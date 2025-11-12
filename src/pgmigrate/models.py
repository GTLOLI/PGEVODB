"""Data models used across the migration tool."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional


@dataclass
class MigrationMeta:
    timeout_sec: Optional[int] = None
    online_safe: bool = False
    reversible: bool = True
    tags: List[str] = field(default_factory=list)
    requires: List[str] = field(default_factory=list)
    pre_hooks: List[str] = field(default_factory=list)
    post_hooks: List[str] = field(default_factory=list)


@dataclass
class MigrationDefinition:
    migration_id: str
    path: Path
    up_sql: Path
    down_sql: Path
    verify_sql: Optional[Path]
    meta: MigrationMeta
    checksum: str


@dataclass
class MigrationState:
    migration_id: str
    checksum: str
    status: str
    applied_at: Optional[str]
    applied_by: Optional[str]
    execution_ms: Optional[int]
    verify_ok: Optional[bool]
    log_ref: Optional[str]


@dataclass
class PlanResult:
    pending: List[MigrationDefinition]
    already_applied: List[MigrationDefinition]
    target_reached: Optional[str]


@dataclass
class VerifyResult:
    migration_id: str
    ok: bool
    details: Optional[str] = None


@dataclass
class AdvisoryLock:
    lock_key: int


@dataclass
class MigrationFilters:
    allow_tags: Iterable[str] = field(default_factory=list)
    target: Optional[str] = None
