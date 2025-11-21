"""核心迁移执行逻辑。"""
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
    """预期运行时问题时引发。"""


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
            raise MigrationRunnerError("未为迁移执行配置超时时间")
        return int(self.profile.timeout_sec)

    @contextlib.contextmanager
    def connect(self) -> Iterator[psycopg.Connection]:
        from psycopg import sql

        conn = db.get_connection(self.profile.dsn)
        try:
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SET application_name = {}").format(
                        sql.Literal(self.app_name))
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
            raise MigrationRunnerError(f"未找到目标迁移 {target}")
        return PlanResult(pending=pending, already_applied=already, target_reached=target_reached)

    def apply(self, target: Optional[str], non_interactive: bool = False) -> None:
        with self.connect() as conn:
            states = self._ensure(conn)
            user = db.current_database_user(conn)
            pending = self._pending_for_apply(states, target)
            if not pending:
                print("无待处理的迁移")
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
                print("无需回滚")
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
                raise MigrationRunnerError("未找到需要验证的迁移")
            results: List[VerifyResult] = []
            for migration in targets:
                ok, details = self._run_verify(conn, migration)
                results.append(VerifyResult(
                    migration_id=migration.migration_id, ok=ok, details=details))
            return results

    def repair(self, migration_id: str, accept: bool) -> None:
        if not accept:
            raise MigrationRunnerError("校验和修复需要 --accept-checksum")
        with self.connect() as conn:
            self._ensure(conn)
            migration = self._find_migration(migration_id)
            db.repair_checksum(conn, self.profile.schema,
                               migration_id, migration.checksum)
            conn.commit()

    def retry(self, migration_id: str, accept_checksum: bool, force: bool, non_interactive: bool) -> None:
        migration = self._find_migration(migration_id)
        with self.connect() as conn:
            states = self._ensure(conn)
            state = states.get(migration_id)
            if not state:
                raise MigrationRunnerError(
                    f"迁移 {migration_id} 在 schema_migrations 中未找到；无法重试"
                )
            if state.status == "applied":
                print(f"迁移 {migration_id} 已经应用；无需重试")
                return
            if state.status == "running" and not force:
                raise MigrationRunnerError(
                    f"迁移 {migration_id} 当前标记为运行中；如果确认安全，请使用 --force"
                )
            if state.status == "running" and force:
                print(
                    f"警告: 强制重试迁移 {migration_id}，其状态为运行中；请确保没有其他进程正在活动"
                )
            if state.checksum != migration.checksum:
                if not accept_checksum:
                    raise MigrationRunnerError(
                        "迁移校验和与文件系统不同；使用 --accept-checksum 重新运行以修复"
                    )
                db.repair_checksum(conn, self.profile.schema,
                                   migration_id, migration.checksum)
                conn.commit()
            message = (
                f"重置迁移 {migration_id} 以重试？这将将其标记为已回滚，并重新运行到该迁移的待处理迁移。"
            )
            self._confirm_action(
                message,
                non_interactive,
                action_description=f"重置 {migration_id} 的状态并重试",
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
                    f"迁移 {migration_id} 在 schema_migrations 中未找到；无法重置"
                )
            action = "删除" if delete else "重置"
            message = (
                f"即将{action}迁移 {migration_id} 的失败记录。这不会运行任何迁移。是否继续？"
            )
            self._confirm_action(
                message,
                non_interactive,
                action_description=("删除记录" if delete else "重置失败状态"),
            )
            if delete:
                db.delete_state(conn, self.profile.schema, migration_id)
                conn.commit()
                print(f"已从 schema_migrations 中移除迁移 {migration_id}")
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
                print(f"已将迁移 {migration_id} 状态重置为已回滚")

    # --- helpers ---

    def _find_migration(self, migration_id: str) -> MigrationDefinition:
        for migration in self.migrations:
            if migration.migration_id == migration_id:
                return migration
        raise MigrationRunnerError(f"迁移 {migration_id} 在文件系统中未找到")

    def _pending_for_apply(self, states: Dict[str, MigrationState], target: Optional[str]) -> List[MigrationDefinition]:
        pending: List[MigrationDefinition] = []
        for migration in self.migrations:
            if target and migration.migration_id > target:
                break
            state = states.get(migration.migration_id)
            if state:
                if state.checksum != migration.checksum:
                    raise MigrationRunnerError(
                        f"迁移 {migration.migration_id} 校验和不匹配；在应用前运行 repair"
                    )
                if state.status == "running":
                    raise MigrationRunnerError(
                        f"迁移 {migration.migration_id} 标记为运行中")
                if state.status == "failed":
                    raise MigrationRunnerError(
                        f"迁移 {migration.migration_id} 之前失败；请调查")
                if state.status == "applied":
                    continue
            self._validate_tags(migration)
            pending.append(migration)
        if target and (not pending or pending[-1].migration_id != target):
            raise MigrationRunnerError(f"目标迁移 {target} 不可达")
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
            raise MigrationRunnerError(f"目标迁移 {target} 尚未应用；无法回滚")
        for migration in pending:
            if migration.meta.reversible is False or not migration.down_sql.read_text(encoding="utf-8").strip():
                raise MigrationRunnerError(
                    f"迁移 {migration.migration_id} 标记为不可逆；无法回滚"
                )
            self._validate_tags(migration)
        return pending

    def _select_verifications(
        self, states: Dict[str, MigrationState], latest: bool, migration_id: Optional[str]
    ) -> List[MigrationDefinition]:
        if latest:
            applied = [m for m in self.migrations if states.get(
                m.migration_id, None) and states[m.migration_id].status == "applied"]
            if not applied:
                raise MigrationRunnerError("无已应用的迁移可验证")
            return [applied[-1]] if applied[-1].verify_sql else []
        if migration_id:
            migration = self._find_migration(migration_id)
            if not migration.verify_sql:
                raise MigrationRunnerError(f"迁移 {migration_id} 没有 verify.sql")
            return [migration]
        with_verify = [m for m in self.migrations if m.verify_sql]
        return with_verify

    def _run_verify(self, conn: psycopg.Connection, migration: MigrationDefinition) -> Tuple[bool, Optional[str]]:
        sql_text = migration.verify_sql.read_text(
            encoding="utf-8") if migration.verify_sql else ""
        if not sql_text:
            return False, "未提供 verify.sql"
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
        message = f"即将在环境 {env} 中{direction}运行 {count} 个迁移。"
        self._confirm_action(message, non_interactive,
                             action_description=f"应用 {count} 个迁移 ({direction})")

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
                raise MigrationRunnerError("生产环境执行在非交互模式下需要 --confirm-prod")
            return
        if self.profile.confirm_prod:
            prompt = f"{message} 输入数据库模式名称以确认: "
            response = input(prompt)
            if response.strip() != self.profile.schema:
                raise MigrationRunnerError("确认失败；中止")
        else:
            description = action_description or message
            prompt = f"{description}? [是/否]: "
            response = input(prompt)
            if response.strip().lower() not in {"y", "yes", "是"}:
                raise MigrationRunnerError("用户中止执行")

    def _validate_tags(self, migration: MigrationDefinition) -> None:
        allowed = set(self.profile.allow_tags)
        if allowed:
            tags = set(migration.meta.tags or [])
            if not tags.issubset(allowed):
                raise MigrationRunnerError(
                    f"迁移 {migration.migration_id} 的标签 {tags} 不允许在此环境中使用"
                )

    def _validate_dependencies(self, states: Dict[str, MigrationState], migrations: Sequence[MigrationDefinition]) -> None:
        applied = {mid for mid, state in states.items()
                   if state.status == "applied"}
        for migration in migrations:
            for required in migration.meta.requires:
                if required not in applied and required not in {m.migration_id for m in migrations}:
                    raise MigrationRunnerError(
                        f"迁移 {migration.migration_id} 需要先应用 {required}"
                    )

    def _apply_single(self, conn: psycopg.Connection, migration: MigrationDefinition, applied_by: str) -> None:
        timeout = self._timeout_for(migration)
        start = time.monotonic()
        log_dir = self.profile.log_dir
        assert log_dir is not None
        with migration_log(log_dir, migration.migration_id) as (log_path, log):
            log(f"-- 正在应用 {migration.migration_id} --")
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
                    verify_ok, verify_details = self._run_verify(
                        conn, migration)
                    if not verify_ok and verify_details:
                        log(f"verify.sql 失败: {verify_details}")
                        raise MigrationRunnerError(
                            f"{migration.migration_id} 的 verify.sql 失败: {verify_details}")
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
                log("迁移成功应用")
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
                log(f"迁移失败: {exc}")
                raise MigrationRunnerError(str(exc)) from exc

    def _revert_single(self, conn: psycopg.Connection, migration: MigrationDefinition, applied_by: str) -> None:
        timeout = self._timeout_for(migration)
        start = time.monotonic()
        log_dir = self.profile.log_dir
        assert log_dir is not None
        with migration_log(log_dir, migration.migration_id + "_down") as (log_path, log):
            log(f"-- 正在回滚 {migration.migration_id} --")
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
                log("迁移成功回滚")
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
                log(f"回滚失败: {exc}")
                raise MigrationRunnerError(str(exc)) from exc

    def _process_sql_includes(self, sql_path: Path, log, processed_files: Optional[set] = None) -> str:
        """处理 SQL 文件中的 @include 指令，递归展开引用的文件。"""
        if processed_files is None:
            processed_files = set()

        # 防止循环引用
        abs_path = sql_path.resolve()
        if abs_path in processed_files:
            raise MigrationRunnerError(f"检测到循环引用: {sql_path}")
        processed_files.add(abs_path)

        sql_text = sql_path.read_text(encoding="utf-8")
        lines = sql_text.split("\n")
        result_lines = []

        import re
        include_pattern = re.compile(r'^\s*--\s*@include\s+(.+?)\s*$')

        for line in lines:
            match = include_pattern.match(line)
            if match:
                include_file = match.group(1).strip()
                # 相对于当前 SQL 文件所在目录解析路径
                include_path = (sql_path.parent / include_file).resolve()

                if not include_path.exists():
                    raise MigrationRunnerError(
                        f"引用的文件不存在: {include_file} (完整路径: {include_path})")

                if not include_path.is_file():
                    raise MigrationRunnerError(f"引用的路径不是文件: {include_file}")

                log(f"  └─ 引用文件: {include_file}")
                # 递归处理被引用的文件
                included_content = self._process_sql_includes(
                    include_path, log, processed_files)
                result_lines.append(f"-- BEGIN INCLUDE: {include_file}")
                result_lines.append(included_content)
                result_lines.append(f"-- END INCLUDE: {include_file}")
            else:
                result_lines.append(line)

        return "\n".join(result_lines)

    def _execute_sql(self, conn: psycopg.Connection, path: Path, timeout: int, log) -> None:
        # 处理 @include 指令
        sql_text = self._process_sql_includes(path, log)

        if not sql_text.strip():
            log(f"在 {path} 中没有要执行的 SQL")
            return
        timeout_ms = max(timeout, 0) * 1000
        log(f"正在执行 {path.name}，超时时间 {timeout} 秒")
        with conn.cursor() as cur:
            cur.execute(
                "SELECT set_config('statement_timeout', %s, true)",
                (f"{int(timeout_ms)}ms",),
            )
            cur.execute(sql_text)

    def _run_hooks(self, hooks: Sequence[str], log) -> None:
        for hook in hooks:
            log(f"正在运行钩子: {hook}")
            try:
                subprocess.run(hook, shell=True, check=True)
            except subprocess.CalledProcessError as exc:
                raise MigrationRunnerError(f"钩子失败: {hook}") from exc
