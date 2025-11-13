"""Configuration loading for the PostgreSQL migration tool."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import os

import yaml


@dataclass
class ProfileConfig:
    """Effective configuration for a single profile."""

    name: str
    dsn: str
    schema: str = "public"
    app_env: Optional[str] = None
    confirm_prod: bool = False
    timeout_sec: Optional[int] = None
    log_dir: Optional[Path] = None
    migrations_dir: Path = Path("./migrations")
    lock_key: Optional[int] = None
    allow_tags: Iterable[str] = field(default_factory=list)
    interactive: bool = True


@dataclass
class GlobalConfig:
    """Raw configuration loaded from YAML."""

    profiles: Dict[str, ProfileConfig]
    default_profile: str
    global_overrides: Dict[str, Any]


class ConfigError(RuntimeError):
    """Raised when the configuration file is invalid."""


def _path_from(value: Optional[str], base_dir: Path) -> Optional[Path]:
    if value is None:
        return None
    return (base_dir / value).resolve() if not Path(value).is_absolute() else Path(value)


def load_config(path: Path) -> GlobalConfig:
    """Load and validate the migrate.yaml file."""

    if not path.exists():
        raise ConfigError(f"Configuration file not found: {path}")

    with path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    profiles_raw = raw.get("profiles") or {}
    if not profiles_raw:
        raise ConfigError("No profiles defined in configuration")

    profiles: Dict[str, ProfileConfig] = {}
    base_dir = path.parent

    global_defaults = raw.get("global") or {}

    for name, data in profiles_raw.items():
        if "dsn" not in data:
            raise ConfigError(f"Profile '{name}' missing 'dsn'")

        profile_kwargs = {
            "name": name,
            "dsn": str(data["dsn"]),
            "schema": data.get("schema", global_defaults.get("schema", "public")),
            "app_env": data.get("app_env", global_defaults.get("app_env")),
            "confirm_prod": bool(data.get("confirm_prod", global_defaults.get("confirm_prod", False))),
            "timeout_sec": data.get("timeout_sec", global_defaults.get("timeout_sec")),
            "migrations_dir": _path_from(
                data.get("migrations_dir", global_defaults.get("migrations_dir", "./migrations")),
                base_dir,
            ),
            "log_dir": _path_from(
                data.get("log_dir", global_defaults.get("log_dir")),
                base_dir,
            ),
            "lock_key": data.get("lock_key", global_defaults.get("lock_key")),
            "allow_tags": data.get("allow_tags", global_defaults.get("allow_tags", [])),
            "interactive": bool(data.get("interactive", global_defaults.get("interactive", True))),
        }

        profiles[name] = ProfileConfig(**profile_kwargs)

    default_profile = raw.get("default_profile")
    if not default_profile:
        raise ConfigError("'default_profile' must be defined in configuration")
    if default_profile not in profiles:
        raise ConfigError(f"Default profile '{default_profile}' is not defined in profiles")

    return GlobalConfig(
        profiles=profiles,
        default_profile=default_profile,
        global_overrides=global_defaults,
    )


def resolve_profile(
    config: GlobalConfig,
    profile_name: Optional[str],
    dsn_override: Optional[str],
    log_dir_override: Optional[Path],
    migrations_dir_override: Optional[Path],
    timeout_override: Optional[int],
    interactive_override: Optional[bool],
) -> ProfileConfig:
    """Resolve the effective profile taking CLI overrides into account."""

    if profile_name:
        if profile_name not in config.profiles:
            raise ConfigError(f"Profile '{profile_name}' not found")
        profile = config.profiles[profile_name]
    else:
        profile = config.profiles[config.default_profile]

    effective = ProfileConfig(**profile.__dict__)

    if dsn_override:
        effective.dsn = dsn_override
    if log_dir_override:
        effective.log_dir = log_dir_override
    if migrations_dir_override:
        effective.migrations_dir = migrations_dir_override
    if timeout_override is not None:
        effective.timeout_sec = timeout_override
    if interactive_override is not None:
        effective.interactive = interactive_override

    # Ensure directories are absolute
    effective.migrations_dir = effective.migrations_dir.resolve()
    if effective.log_dir:
        effective.log_dir = effective.log_dir.resolve()

    if not effective.log_dir:
        log_dir_default = config.global_overrides.get("log_dir")
        if log_dir_default:
            effective.log_dir = _path_from(log_dir_default, Path.cwd())
        else:
            effective.log_dir = (Path.cwd() / ".migrate-logs").resolve()

    if effective.timeout_sec is None:
        if config.global_overrides.get("timeout_sec") is not None:
            effective.timeout_sec = int(config.global_overrides["timeout_sec"])
        else:
            effective.timeout_sec = 600

    if effective.lock_key is None:
        lock_key = config.global_overrides.get("lock_key")
        if lock_key is None:
            raise ConfigError("'lock_key' must be specified globally or per profile")
        effective.lock_key = int(lock_key)

    # Allow overriding via environment variables for DSN
    env_dsn = os.getenv("PG_DSN")
    if env_dsn:
        effective.dsn = env_dsn

    return effective


def list_profiles(config: GlobalConfig) -> List[str]:
    return sorted(config.profiles.keys())
