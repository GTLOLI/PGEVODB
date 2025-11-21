"""迁移的文件系统加载器。"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable, List, Optional

import yaml

from .models import MigrationDefinition, MigrationMeta


class MigrationFormatError(RuntimeError):
    """当迁移目录无效时引发。"""


def _load_meta(path: Path) -> MigrationMeta:
    if not path.exists():
        return MigrationMeta()

    with path.open("r", encoding="utf-8") as fh:
        meta_raw = yaml.safe_load(fh) or {}

    return MigrationMeta(
        timeout_sec=meta_raw.get("timeout_sec"),
        online_safe=bool(meta_raw.get("online_safe", False)),
        reversible=bool(meta_raw.get("reversible", True)),
        tags=list(meta_raw.get("tags", []) or []),
        requires=list(meta_raw.get("requires", []) or []),
        pre_hooks=list(meta_raw.get("pre_hooks", []) or []),
        post_hooks=list(meta_raw.get("post_hooks", []) or []),
    )


def _read_text(path: Path) -> str:
    with path.open("r", encoding="utf-8") as fh:
        return fh.read()


def _checksum(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def load_migrations(directory: Path) -> List[MigrationDefinition]:
    """从文件系统加载和验证迁移。"""

    if not directory.exists():
        raise MigrationFormatError(f"迁移目录不存在: {directory}")

    migrations: List[MigrationDefinition] = []
    for entry in sorted(directory.iterdir()):
        if not entry.is_dir():
            continue
        migration_id = entry.name
        up_sql = entry / "up.sql"
        down_sql = entry / "down.sql"
        verify_sql = entry / "verify.sql"
        meta_yaml = entry / "meta.yaml"

        if not up_sql.exists():
            raise MigrationFormatError(f"迁移 {migration_id} 缺少 up.sql")
        if not down_sql.exists():
            raise MigrationFormatError(f"迁移 {migration_id} 缺少 down.sql")

        up_content = _read_text(up_sql)
        checksum = _checksum(up_content)

        verify_path = verify_sql if verify_sql.exists() else None

        migrations.append(
            MigrationDefinition(
                migration_id=migration_id,
                path=entry,
                up_sql=up_sql,
                down_sql=down_sql,
                verify_sql=verify_path,
                meta=_load_meta(meta_yaml),
                checksum=checksum,
            )
        )

    return migrations


def require_sequential(migrations: Iterable[MigrationDefinition]) -> None:
    """确保迁移目录名称已经按字典序排序。"""

    previous: Optional[str] = None
    for migration in migrations:
        if previous and migration.migration_id <= previous:
            raise MigrationFormatError("迁移没有按严格升序排列")
        previous = migration.migration_id
