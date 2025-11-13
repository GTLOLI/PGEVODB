"""Command line interface for the migration tool."""
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
    parser = argparse.ArgumentParser(prog="migrate", description="PostgreSQL migration management tool")
    parser.add_argument("--config", default="migrate.yaml", help="Path to configuration file")
    parser.add_argument("--env", dest="env", help="Profile to use from configuration")
    parser.add_argument("--dsn", dest="dsn", help="Override DSN")
    parser.add_argument("--log-dir", dest="log_dir", help="Override log directory")
    parser.add_argument("--migrations-dir", dest="migrations_dir", help="Override migrations directory")
    parser.add_argument("--timeout-sec", dest="timeout_sec", type=int, help="Override statement timeout in seconds")
    parser.add_argument(
        "--non-interactive",
        dest="non_interactive",
        action="store_true",
        help="Disable interactive confirmations",
    )
    parser.add_argument(
        "--confirm-prod",
        dest="confirm_prod",
        action="store_true",
        help="Explicitly confirm production execution (bypass interactive prompt)",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("status", help="Show current migration status")

    plan = sub.add_parser("plan", help="Preview pending migrations")
    plan.add_argument("--to", dest="target", help="Target migration id")

    up = sub.add_parser("up", help="Apply pending migrations")
    up.add_argument("--to", dest="target", help="Target migration id")

    down = sub.add_parser("down", help="Rollback migrations")
    down.add_argument("--to", dest="target", required=True, help="Target migration id to rollback to (inclusive)")

    verify = sub.add_parser("verify", help="Run verification scripts")
    verify_mode = verify.add_mutually_exclusive_group()
    verify_mode.add_argument("--latest", action="store_true", help="Verify only the latest applied migration")
    verify_mode.add_argument("--id", dest="migration_id", help="Verify a specific migration")

    repair = sub.add_parser("repair", help="Repair checksum for a migration")
    repair.add_argument("--accept-checksum", dest="migration_id", required=True, help="Migration id to repair")

    retry = sub.add_parser("retry", help="Retry a failed migration")
    retry.add_argument("--id", dest="migration_id", required=True, help="Migration id to retry")
    retry.add_argument(
        "--accept-checksum",
        dest="accept_checksum",
        action="store_true",
        help="Repair checksum automatically when filesystem differs",
    )
    retry.add_argument(
        "--force",
        dest="force",
        action="store_true",
        help="Override running status check (use with caution)",
    )

    reset_failed = sub.add_parser("reset-failed", help="Reset or delete failed migration records")
    reset_failed.add_argument("--id", dest="migration_id", required=True, help="Migration id to reset")
    reset_failed.add_argument(
        "--delete",
        dest="delete",
        action="store_true",
        help="Delete the migration entry instead of marking it reverted",
    )

    return parser


def _load_profile(args: argparse.Namespace) -> ProfileConfig:
    config_path = Path(args.config).resolve()
    config = load_config(config_path)
    log_dir = Path(args.log_dir).resolve() if args.log_dir else None
    migrations_dir = Path(args.migrations_dir).resolve() if args.migrations_dir else None
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
    applied_ids = {mid for mid, state in states.items() if state.status == "applied"}
    pending = [m for m in migrations if m.migration_id not in applied_ids]
    print(f"Total migrations: {len(migrations)}")
    print(f"Applied: {len(applied_ids)}")
    if pending:
        print("Pending:")
        for migration in pending:
            state = states.get(migration.migration_id)
            status = state.status if state else "pending"
            print(f"  - {migration.migration_id} [{status}]")
    else:
        print("No pending migrations")
    failed = [s for s in states.values() if s.status == "failed"]
    if failed:
        print("Failed migrations:")
        for state in failed:
            print(f"  - {state.migration_id} (checksum={state.checksum})")
    return 0


def run_plan(runner: MigrationRunner, target: Optional[str]) -> int:
    plan = runner.plan_up(target)
    if not plan.pending:
        print("No pending migrations")
        return 0
    print("Migrations to apply:")
    for migration in plan.pending:
        tags = ",".join(migration.meta.tags) if migration.meta.tags else "-"
        reversible = "yes" if migration.meta.reversible else "no"
        print(f"  - {migration.migration_id} [tags={tags} reversible={reversible}]")
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
    print(f"Checksum repaired for {migration_id}")
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
        parser.error("Unknown command")
    except (ConfigError, MigrationRunnerError, DatabaseError, MigrationFormatError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
