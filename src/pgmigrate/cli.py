"""数据库迁移工具的命令行接口。"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

from .config import ConfigError, ProfileConfig, load_config, resolve_profile
from .runner import MigrationRunner, MigrationRunnerError
from .db import DatabaseError
from .loader import MigrationFormatError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="migrate", description="PostgreSQL 数据库迁移管理工具")
    parser.add_argument("--config", default="migrate.yaml", help="配置文件路径")
    parser.add_argument("--env", dest="env", help="使用的配置环境")
    parser.add_argument("--dsn", dest="dsn", help="覆盖 DSN 配置")
    parser.add_argument("--log-dir", dest="log_dir", help="覆盖日志目录")
    parser.add_argument("--migrations-dir",
                        dest="migrations_dir", help="覆盖迁移文件目录")
    parser.add_argument("--timeout-sec", dest="timeout_sec",
                        type=int, help="覆盖语句超时时间（秒）")
    parser.add_argument(
        "--non-interactive",
        dest="non_interactive",
        action="store_true",
        help="禁用交互式确认",
    )
    parser.add_argument(
        "--confirm-prod",
        dest="confirm_prod",
        action="store_true",
        help="明确确认生产环境执行（跳过交互式提示）",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("status", help="显示当前迁移状态")

    plan = sub.add_parser("plan", help="预览待执行的迁移")
    plan.add_argument("--to", dest="target", help="目标迁移 ID")

    up = sub.add_parser("up", help="执行待处理的迁移")
    up.add_argument("--to", dest="target", help="目标迁移 ID")

    down = sub.add_parser("down", help="回滚迁移")
    down.add_argument("--to", dest="target", required=True,
                      help="回滚到的目标迁移 ID（包含）")

    verify = sub.add_parser("verify", help="运行验证脚本")
    verify_mode = verify.add_mutually_exclusive_group()
    verify_mode.add_argument(
        "--latest", action="store_true", help="仅验证最新应用的迁移")
    verify_mode.add_argument("--id", dest="migration_id", help="验证指定的迁移")

    repair = sub.add_parser("repair", help="修复迁移的校验和")
    repair.add_argument("--accept-checksum",
                        dest="migration_id", required=True, help="要修复的迁移 ID")

    retry = sub.add_parser("retry", help="重试失败的迁移")
    retry.add_argument("--id", dest="migration_id",
                       required=True, help="要重试的迁移 ID")
    retry.add_argument(
        "--accept-checksum",
        dest="accept_checksum",
        action="store_true",
        help="当文件系统不同时自动修复校验和",
    )
    retry.add_argument(
        "--force",
        dest="force",
        action="store_true",
        help="覆盖运行状态检查（谨慎使用）",
    )

    reset_failed = sub.add_parser("reset-failed", help="重置或删除失败的迁移记录")
    reset_failed.add_argument(
        "--id", dest="migration_id", required=True, help="要重置的迁移 ID")
    reset_failed.add_argument(
        "--delete",
        dest="delete",
        action="store_true",
        help="删除迁移记录而不是标记为已回滚",
    )

    return parser


def _load_profile(args: argparse.Namespace) -> ProfileConfig:
    config_path = Path(args.config).resolve()
    config = load_config(config_path)
    log_dir = Path(args.log_dir).resolve() if args.log_dir else None
    migrations_dir = Path(args.migrations_dir).resolve(
    ) if args.migrations_dir else None
    profile = resolve_profile(
        config,
        profile_name=args.env,
        dsn_override=args.dsn,
        log_dir_override=log_dir,
        migrations_dir_override=migrations_dir,
        timeout_override=args.timeout_sec,
        interactive_override=False if args.non_interactive else None,
    )
    return profile


def run_status(runner: MigrationRunner) -> int:
    migrations, states = runner.status()
    applied_ids = {mid for mid, state in states.items()
                   if state.status == "applied"}
    pending = [m for m in migrations if m.migration_id not in applied_ids]
    print(f"总迁移数: {len(migrations)}")
    print(f"已应用: {len(applied_ids)}")
    if pending:
        print("待处理:")
        for migration in pending:
            state = states.get(migration.migration_id)
            status = state.status if state else "pending"
            print(f"  - {migration.migration_id} [{status}]")
    else:
        print("无待处理的迁移")
    failed = [s for s in states.values() if s.status == "failed"]
    if failed:
        print("失败的迁移:")
        for state in failed:
            print(f"  - {state.migration_id} (checksum={state.checksum})")
    return 0


def run_plan(runner: MigrationRunner, target: Optional[str]) -> int:
    plan = runner.plan_up(target)
    if not plan.pending:
        print("无待处理的迁移")
        return 0
    print("将要应用的迁移:")
    for migration in plan.pending:
        tags = ",".join(migration.meta.tags) if migration.meta.tags else "-"
        reversible = "是" if migration.meta.reversible else "否"
        print(f"  - {migration.migration_id} [标签={tags} 可回滚={reversible}]")
    return 0


def run_up(runner: MigrationRunner, target: Optional[str], non_interactive: bool) -> int:
    runner.apply(target=target, non_interactive=non_interactive)
    return 0


def run_down(runner: MigrationRunner, target: str, non_interactive: bool) -> int:
    runner.rollback(target=target, non_interactive=non_interactive)
    return 0


def run_verify(runner: MigrationRunner, latest: bool, migration_id: Optional[str]) -> int:
    results = runner.verify(latest=latest, migration_id=migration_id)
    for result in results:
        status = "OK" if result.ok else "FAILED"
        details = f" - {result.details}" if result.details else ""
        print(f"{result.migration_id}: {status}{details}")
    return 0


def run_repair(runner: MigrationRunner, migration_id: str) -> int:
    runner.repair(migration_id=migration_id, accept=True)
    print(f"已修复 {migration_id} 的校验和")
    return 0


def run_retry(
    runner: MigrationRunner,
    migration_id: str,
    accept_checksum: bool,
    force: bool,
    non_interactive: bool,
) -> int:
    runner.retry(
        migration_id=migration_id,
        accept_checksum=accept_checksum,
        force=force,
        non_interactive=non_interactive,
    )
    return 0


def run_reset_failed(
    runner: MigrationRunner,
    migration_id: str,
    delete: bool,
    non_interactive: bool,
) -> int:
    runner.reset_failed(
        migration_id=migration_id,
        delete=delete,
        non_interactive=non_interactive,
    )
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        profile = _load_profile(args)
        runner = MigrationRunner(profile, confirm_override=args.confirm_prod)
        command = args.command
        if command == "status":
            return run_status(runner)
        if command == "plan":
            return run_plan(runner, args.target)
        if command == "up":
            return run_up(runner, args.target, args.non_interactive)
        if command == "down":
            return run_down(runner, args.target, args.non_interactive)
        if command == "verify":
            return run_verify(runner, args.latest, args.migration_id)
        if command == "repair":
            return run_repair(runner, args.migration_id)
        if command == "retry":
            return run_retry(
                runner,
                args.migration_id,
                args.accept_checksum,
                args.force,
                args.non_interactive,
            )
        if command == "reset-failed":
            return run_reset_failed(
                runner,
                args.migration_id,
                args.delete,
                args.non_interactive,
            )
        parser.error("未知命令")
    except (ConfigError, MigrationRunnerError, DatabaseError, MigrationFormatError) as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
