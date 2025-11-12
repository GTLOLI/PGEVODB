"""Database utilities for the migration tool."""
from __future__ import annotations

import contextlib
import datetime as dt
from typing import Dict, Iterator, Optional

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from .models import MigrationDefinition, MigrationState


class DatabaseError(RuntimeError):
    """Raised when database operations fail."""


def get_connection(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn)


def _table(schema: str) -> sql.Composed:
    return sql.SQL("{}.{}" ).format(sql.Identifier(schema), sql.Identifier("schema_migrations"))


def ensure_schema_migrations(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}" ).format(sql.Identifier(schema)))
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                    id BIGSERIAL PRIMARY KEY,
                    migration_id TEXT UNIQUE NOT NULL,
                    checksum TEXT NOT NULL,
                    applied_at TIMESTAMPTZ,
                    applied_by TEXT,
                    status TEXT NOT NULL,
                    execution_ms INTEGER,
                    verify_ok BOOLEAN,
                    log_ref TEXT
                )
                """
            ).format(_table(schema))
        )
        cur.execute(
            sql.SQL(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_{0}_migration_id
                    ON {1} (migration_id)
                """
            ).format(sql.Identifier(f"{schema}_schema_migrations_migration_id"), _table(schema))
        )
        cur.execute(
            sql.SQL(
                """
                CREATE INDEX IF NOT EXISTS idx_{0}_status
                    ON {1} (status)
                """
            ).format(sql.Identifier(f"{schema}_schema_migrations_status"), _table(schema))
        )


def fetch_states(conn: psycopg.Connection, schema: str) -> Dict[str, MigrationState]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql.SQL("SELECT * FROM {} ORDER BY migration_id" ).format(_table(schema)))
        results = {}
        for row in cur.fetchall():
            results[row["migration_id"]] = MigrationState(
                migration_id=row["migration_id"],
                checksum=row["checksum"],
                status=row["status"],
                applied_at=row.get("applied_at"),
                applied_by=row.get("applied_by"),
                execution_ms=row.get("execution_ms"),
                verify_ok=row.get("verify_ok"),
                log_ref=row.get("log_ref"),
            )
        return results


def set_status(
    conn: psycopg.Connection,
    schema: str,
    migration: MigrationDefinition,
    status: str,
    applied_by: Optional[str] = None,
    applied_at: Optional[dt.datetime] = None,
    execution_ms: Optional[int] = None,
    verify_ok: Optional[bool] = None,
    log_ref: Optional[str] = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {table} (migration_id, checksum, status, applied_by, applied_at, execution_ms, verify_ok, log_ref)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (migration_id)
                DO UPDATE SET
                    checksum = EXCLUDED.checksum,
                    status = EXCLUDED.status,
                    applied_by = EXCLUDED.applied_by,
                    applied_at = EXCLUDED.applied_at,
                    execution_ms = EXCLUDED.execution_ms,
                    verify_ok = EXCLUDED.verify_ok,
                    log_ref = EXCLUDED.log_ref
                """
            ).format(table=_table(schema)),
            (
                migration.migration_id,
                migration.checksum,
                status,
                applied_by,
                applied_at,
                execution_ms,
                verify_ok,
                log_ref,
            ),
        )


def repair_checksum(conn: psycopg.Connection, schema: str, migration_id: str, checksum: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("UPDATE {} SET checksum = %s WHERE migration_id = %s" ).format(_table(schema)),
            (checksum, migration_id),
        )
        if cur.rowcount == 0:
            raise DatabaseError(f"Migration {migration_id} not found for repair")


def update_status_fields(
    conn: psycopg.Connection,
    schema: str,
    migration_id: str,
    **fields,
) -> None:
    assignments = []
    values = []
    for key, value in fields.items():
        assignments.append(sql.SQL("{} = %s" ).format(sql.Identifier(key)))
        values.append(value)
    if not assignments:
        return
    values.append(migration_id)
    query = sql.SQL("UPDATE {table} SET {assignments} WHERE migration_id = %s").format(
        table=_table(schema), assignments=sql.SQL(", ").join(assignments)
    )
    with conn.cursor() as cur:
        cur.execute(query, values)
        if cur.rowcount == 0:
            raise DatabaseError(f"Migration {migration_id} not found for status update")


@contextlib.contextmanager
def advisory_lock(conn: psycopg.Connection, lock_key: int) -> Iterator[None]:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_key,))
        locked = cur.fetchone()[0]
        if not locked:
            raise DatabaseError("已有迁移在执行 (advisory lock failed)")
    try:
        yield
    finally:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))


def current_database_user(conn: psycopg.Connection) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT current_user")
        return cur.fetchone()[0]
