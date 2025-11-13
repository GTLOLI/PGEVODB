"""Core migration execution logic."""
from __future__ import annotations

import contextlib
import datetime as dt
import subprocess
import time
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

import psycopg

from . import db
from .config import ProfileConfig
from .loader import load_migrations, require_sequential
from .models import MigrationDefinition, MigrationState, PlanResult, VerifyResult
from .logging_utils import migration_log


class MigrationRunnerError(RuntimeError):
    """Raised for expected runtime issues."""


class MigrationRunner:
    def __init__(self, profile: ProfileConfig, app_name: str = "pgmigrate", confirm_override: bool = False) -> None:
        self.profile = profile
        self.app_name = app_name
        self.confirm_override = confirm_override
        self.migrations = load_migrations(profile.migrations_dir)
        require_sequential(self.migrations)
        self._skip_next_confirmation = False

    def _timeout_for(self, migration: MigrationDefinition) -> int:
        if migration.meta.timeout_sec is not None:
            return int(migration.meta.timeout_sec)
        if self.profile.timeout_sec is None:
            raise MigrationRunnerError("No timeout configured for migration execution")
        return int(self.profile.timeout_sec)

    @contextlib.contextmanager
    def connect(self) -> Iterator[psycopg.Connection]:
        from psycopg import sql

        conn = db.get_connection(self.profile.dsn)
        try:
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SET application_name = {}").format(sql.Literal(self.app_name))
                )
            yield conn
        finally:
            conn.close()

    def _ensure(self, conn: psycopg.Connection) -> Dict[str, MigrationState]:
        db.ensure_schema_migrations(conn, self.profile.schema)
        conn.commit()
        return db.fetch_states(conn, self.profile.schema)

    def status(self) -> Tuple[List[MigrationDefinition], Dict[str, MigrationState]]:
        with self.connect() as conn:
            states = self._ensure(conn)
            return self.migrations, states

    def plan_up(self, target: Optional[str]) -> PlanResult:
        with self.connect() as conn:
            states = self._ensure(conn)

        pending: List[MigrationDefinition] = []
        already: List[MigrationDefinition] = []
        target_reached: Optional[str] = None
        for migration in self.migrations:
            state = states.get(migration.migration_id)
            applied = state and state.status == "applied"
            if applied:
                already.append(migration)
            else:
                pending.append(migration)
            if target and migration.migration_id == target:
                target_reached = target
                break
        if target and not target_reached:
            raise MigrationRunnerError(f"Target migration {target} not found")
        return PlanResult(pending=pending, already_applied=already, target_reached=target_reached)

    def apply(self, target: Optional[str], non_interactive: bool = False) -> None:
        with self.connect() as conn:
            states = self._ensure(conn)
            user = db.current_database_user(conn)
            pending = self._pending_for_apply(states, target)
            if not pending:
                print("No pending migrations")
                return
            self._confirm_execution(len(pending), "up", non_interactive)
            with db.advisory_lock(conn, self.profile.lock_key):
                for migration in pending:
                    self._apply_single(conn, migration, user)

    def rollback(self, target: str, non_interactive: bool = False) -> None:
        with self.connect() as conn:
            states = self._ensure(conn)
            user = db.current_database_user(conn)
            to_revert = self._pending_for_down(states, target)
            if not to_revert:
                print("Nothing to rollback")
                return
            self._confirm_execution(len(to_revert), "down", non_interactive)
            with db.advisory_lock(conn, self.profile.lock_key):
                for migration in to_revert:
                    self._revert_single(conn, migration, user)

    def verify(self, latest: bool = False, migration_id: Optional[str] = None) -> List[VerifyResult]:
        with self.connect() as conn:
            states = self._ensure(conn)
            targets = self._select_verifications(states, latest, migration_id)
            if not targets:
                raise MigrationRunnerError("No migrations found for verification")
            results: List[VerifyResult] = []
            for migration in targets:
                ok, details = self._run_verify(conn, migration)
                results.append(VerifyResult(migration_id=migration.migration_id, ok=ok, details=details))
            return results

    def repair(self, migration_id: str, accept: bool) -> None:
        if not accept:
            raise MigrationRunnerError("Checksum repair requires --accept-checksum")
        with self.connect() as conn:
            self._ensure(conn)
            migration = self._find_migration(migration_id)
            db.repair_checksum(conn, self.profile.schema, migration_id, migration.checksum)
            conn.commit()

    def retry(self, migration_id: str, accept_checksum: bool, force: bool, non_interactive: bool) -> None:
        migration = self._find_migration(migration_id)
        with self.connect() as conn:
            states = self._ensure(conn)
            state = states.get(migration_id)
            if not state:
                raise MigrationRunnerError(
                    f"Migration {migration_id} not found in schema_migrations; cannot retry"
                )
            if state.status == "applied":
                print(f"Migration {migration_id} is already applied; nothing to retry")
                return
            if state.status == "running" and not force:
                raise MigrationRunnerError(
                    f"Migration {migration_id} is currently marked running; use --force if you are certain it is safe"
                )
            if state.status == "running" and force:
                print(
                    f"Warning: forcing retry for migration {migration_id} while status is running; ensure no other process is active"
                )
            if state.checksum != migration.checksum:
                if not accept_checksum:
                    raise MigrationRunnerError(
                        "Migration checksum differs from filesystem; rerun with --accept-checksum to repair"
                    )
                db.repair_checksum(conn, self.profile.schema, migration_id, migration.checksum)
                conn.commit()
            message = (
                f"Reset migration {migration_id} to retry? This will mark it as reverted and re-run pending migrations up to it."
            )
            self._confirm_action(
                message,
                non_interactive,
                action_description=f"Reset status for {migration_id} and retry",
            )
            db.update_status_fields(
                conn,
                self.profile.schema,
                migration_id,
                status="reverted",
                applied_at=None,
                applied_by=None,
                execution_ms=None,
                verify_ok=None,
            )
            conn.commit()
        previous_skip = self._skip_next_confirmation
        try:
            self._skip_next_confirmation = True
            self.apply(target=migration_id, non_interactive=non_interactive)
        finally:
            self._skip_next_confirmation = previous_skip

    def reset_failed(self, migration_id: str, delete: bool, non_interactive: bool) -> None:
        with self.connect() as conn:
            states = self._ensure(conn)
            state = states.get(migration_id)
            if not state:
                raise MigrationRunnerError(
                    f"Migration {migration_id} not found in schema_migrations; cannot reset"
                )
            action = "delete" if delete else "reset"
            message = (
                f"About to {action} failure record for {migration_id}. This does not run any migrations. Proceed?"
            )
            self._confirm_action(
                message,
                non_interactive,
                action_description=("Delete record" if delete else "Reset failed status"),
            )
            if delete:
                db.delete_state(conn, self.profile.schema, migration_id)
                conn.commit()
                print(f"Removed migration {migration_id} from schema_migrations")
            else:
                db.update_status_fields(
                    conn,
                    self.profile.schema,
                    migration_id,
                    status="reverted",
                    applied_at=None,
                    applied_by=None,
                    execution_ms=None,
                    verify_ok=None,
                )
                conn.commit()
                print(f"Reset migration {migration_id} status to reverted")

    # --- helpers ---

    def _find_migration(self, migration_id: str) -> MigrationDefinition:
        for migration in self.migrations:
            if migration.migration_id == migration_id:
                return migration
        raise MigrationRunnerError(f"Migration {migration_id} not found in filesystem")

    def _pending_for_apply(self, states: Dict[str, MigrationState], target: Optional[str]) -> List[MigrationDefinition]:
        pending: List[MigrationDefinition] = []
        for migration in self.migrations:
            if target and migration.migration_id > target:
                break
            state = states.get(migration.migration_id)
            if state:
                if state.checksum != migration.checksum:
                    raise MigrationRunnerError(
                        f"Migration {migration.migration_id} checksum mismatch; run repair before applying"
                    )
                if state.status == "running":
                    raise MigrationRunnerError(f"Migration {migration.migration_id} is marked as running")
                if state.status == "failed":
                    raise MigrationRunnerError(f"Migration {migration.migration_id} failed previously; investigate")
                if state.status == "applied":
                    continue
            self._validate_tags(migration)
            pending.append(migration)
        if target and (not pending or pending[-1].migration_id != target):
            raise MigrationRunnerError(f"Target migration {target} not reachable")
        self._validate_dependencies(states, pending)
        return pending

    def _pending_for_down(self, states: Dict[str, MigrationState], target: str) -> List[MigrationDefinition]:
        pending: List[MigrationDefinition] = []
        seen_target = False
        for migration in reversed(self.migrations):
            state = states.get(migration.migration_id)
            if not state or state.status != "applied":
                continue
            pending.append(migration)
            if migration.migration_id == target:
                seen_target = True
                break
        if not seen_target:
            raise MigrationRunnerError(f"Target migration {target} not yet applied; cannot rollback")
        for migration in pending:
            if migration.meta.reversible is False or not migration.down_sql.read_text(encoding="utf-8").strip():
                raise MigrationRunnerError(
                    f"Migration {migration.migration_id} is marked irreversible; cannot rollback"
                )
            self._validate_tags(migration)
        return pending

    def _select_verifications(
        self, states: Dict[str, MigrationState], latest: bool, migration_id: Optional[str]
    ) -> List[MigrationDefinition]:
        if latest:
            applied = [m for m in self.migrations if states.get(m.migration_id, None) and states[m.migration_id].status == "applied"]
            if not applied:
                raise MigrationRunnerError("No applied migrations to verify")
            return [applied[-1]] if applied[-1].verify_sql else []
        if migration_id:
            migration = self._find_migration(migration_id)
            if not migration.verify_sql:
                raise MigrationRunnerError(f"Migration {migration_id} does not have verify.sql")
            return [migration]
        with_verify = [m for m in self.migrations if m.verify_sql]
        return with_verify

    def _run_verify(self, conn: psycopg.Connection, migration: MigrationDefinition) -> Tuple[bool, Optional[str]]:
        sql_text = migration.verify_sql.read_text(encoding="utf-8") if migration.verify_sql else ""
        if not sql_text:
            return False, "No verify.sql provided"
        timeout = self._timeout_for(migration)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT set_config('statement_timeout', %s, true)",
                    (f"{int(max(timeout, 0) * 1000)}ms",),
                )
                cur.execute(sql_text)
            return True, None
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)

    def _confirm_execution(self, count: int, direction: str, non_interactive: bool) -> None:
        env = self.profile.app_env or self.profile.name or "current"
        message = f"About to run {count} migration(s) {direction} in environment {env}."
        self._confirm_action(message, non_interactive, action_description=f"Apply {count} migration(s) {direction}")

    def _confirm_action(
        self,
        message: str,
        non_interactive: bool,
        action_description: Optional[str] = None,
    ) -> None:
        if self._skip_next_confirmation:
            self._skip_next_confirmation = False
            return
        if self.profile.confirm_prod and self.confirm_override:
            return
        if non_interactive or not self.profile.interactive:
            if self.profile.confirm_prod and not self.confirm_override:
                raise MigrationRunnerError("Production execution requires --confirm-prod in non-interactive mode")
            return
        if self.profile.confirm_prod:
            prompt = f"{message} Type the database schema name to confirm: "
            response = input(prompt)
            if response.strip() != self.profile.schema:
                raise MigrationRunnerError("Confirmation failed; aborting")
        else:
            description = action_description or message
            prompt = f"{description}? [y/N]: "
            response = input(prompt)
            if response.strip().lower() not in {"y", "yes"}:
                raise MigrationRunnerError("User aborted execution")

    def _validate_tags(self, migration: MigrationDefinition) -> None:
        allowed = set(self.profile.allow_tags)
        if allowed:
            tags = set(migration.meta.tags or [])
            if not tags.issubset(allowed):
                raise MigrationRunnerError(
                    f"Migration {migration.migration_id} has tags {tags} not allowed for this environment"
                )

    def _validate_dependencies(self, states: Dict[str, MigrationState], migrations: Sequence[MigrationDefinition]) -> None:
        applied = {mid for mid, state in states.items() if state.status == "applied"}
        for migration in migrations:
            for required in migration.meta.requires:
                if required not in applied and required not in {m.migration_id for m in migrations}:
                    raise MigrationRunnerError(
                        f"Migration {migration.migration_id} requires {required} to be applied first"
                    )

    def _apply_single(self, conn: psycopg.Connection, migration: MigrationDefinition, applied_by: str) -> None:
        timeout = self._timeout_for(migration)
        start = time.monotonic()
        log_dir = self.profile.log_dir
        assert log_dir is not None
        with migration_log(log_dir, migration.migration_id) as (log_path, log):
            log(f"-- Applying {migration.migration_id} --")
            db.set_status(
                conn,
                self.profile.schema,
                migration,
                status="running",
                applied_by=applied_by,
                applied_at=dt.datetime.utcnow(),
                log_ref=str(log_path),
            )
            conn.commit()
            try:
                self._run_hooks(migration.meta.pre_hooks, log)
                self._execute_sql(conn, migration.up_sql, timeout, log)
                verify_ok = True
                if migration.verify_sql:
                    verify_ok, verify_details = self._run_verify(conn, migration)
                    if not verify_ok and verify_details:
                        log(f"verify.sql failed: {verify_details}")
                        raise MigrationRunnerError(f"verify.sql failed for {migration.migration_id}: {verify_details}")
                conn.commit()
                self._run_hooks(migration.meta.post_hooks, log)
                duration = int((time.monotonic() - start) * 1000)
                db.set_status(
                    conn,
                    self.profile.schema,
                    migration,
                    status="applied",
                    applied_by=applied_by,
                    applied_at=dt.datetime.utcnow(),
                    execution_ms=duration,
                    verify_ok=verify_ok,
                    log_ref=str(log_path),
                )
                conn.commit()
                log("Migration applied successfully")
            except Exception as exc:  # noqa: BLE001
                conn.rollback()
                duration = int((time.monotonic() - start) * 1000)
                db.set_status(
                    conn,
                    self.profile.schema,
                    migration,
                    status="failed",
                    applied_by=applied_by,
                    applied_at=dt.datetime.utcnow(),
                    execution_ms=duration,
                    verify_ok=False,
                    log_ref=str(log_path),
                )
                conn.commit()
                log(f"Migration failed: {exc}")
                raise MigrationRunnerError(str(exc)) from exc

    def _revert_single(self, conn: psycopg.Connection, migration: MigrationDefinition, applied_by: str) -> None:
        timeout = self._timeout_for(migration)
        start = time.monotonic()
        log_dir = self.profile.log_dir
        assert log_dir is not None
        with migration_log(log_dir, migration.migration_id + "_down") as (log_path, log):
            log(f"-- Reverting {migration.migration_id} --")
            db.set_status(
                conn,
                self.profile.schema,
                migration,
                status="running",
                applied_by=applied_by,
                applied_at=dt.datetime.utcnow(),
                log_ref=str(log_path),
            )
            conn.commit()
            try:
                self._run_hooks(migration.meta.pre_hooks, log)
                self._execute_sql(conn, migration.down_sql, timeout, log)
                conn.commit()
                self._run_hooks(migration.meta.post_hooks, log)
                duration = int((time.monotonic() - start) * 1000)
                db.set_status(
                    conn,
                    self.profile.schema,
                    migration,
                    status="reverted",
                    applied_by=applied_by,
                    applied_at=dt.datetime.utcnow(),
                    execution_ms=duration,
                    verify_ok=None,
                    log_ref=str(log_path),
                )
                conn.commit()
                log("Migration reverted successfully")
            except Exception as exc:  # noqa: BLE001
                conn.rollback()
                duration = int((time.monotonic() - start) * 1000)
                db.set_status(
                    conn,
                    self.profile.schema,
                    migration,
                    status="failed",
                    applied_by=applied_by,
                    applied_at=dt.datetime.utcnow(),
                    execution_ms=duration,
                    verify_ok=False,
                    log_ref=str(log_path),
                )
                conn.commit()
                log(f"Rollback failed: {exc}")
                raise MigrationRunnerError(str(exc)) from exc

    def _execute_sql(self, conn: psycopg.Connection, path: Path, timeout: int, log) -> None:
        sql_text = path.read_text(encoding="utf-8")
        if not sql_text.strip():
            log(f"No SQL to execute in {path}")
            return
        timeout_ms = max(timeout, 0) * 1000
        log(f"Executing {path.name} with timeout {timeout}s")
        with conn.cursor() as cur:
            cur.execute(
                "SELECT set_config('statement_timeout', %s, true)",
                (f"{int(timeout_ms)}ms",),
            )
            cur.execute(sql_text)

    def _run_hooks(self, hooks: Sequence[str], log) -> None:
        for hook in hooks:
            log(f"Running hook: {hook}")
            try:
                subprocess.run(hook, shell=True, check=True)
            except subprocess.CalledProcessError as exc:
                raise MigrationRunnerError(f"Hook failed: {hook}") from exc

