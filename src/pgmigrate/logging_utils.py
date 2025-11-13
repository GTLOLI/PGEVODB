"""Logging helpers for migration runs."""
from __future__ import annotations

import contextlib
import datetime as dt
from pathlib import Path
from typing import Callable, Iterator, Tuple


@contextlib.contextmanager
def migration_log(log_dir: Path, migration_id: str) -> Iterator[Tuple[str, Callable[[str], None]]]:
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"{migration_id}_{timestamp}.log"
    log_path = log_dir / filename
    with log_path.open("w", encoding="utf-8") as fh:
        def log(message: str) -> None:
            fh.write(message + "\n")
            fh.flush()

        yield filename, log
