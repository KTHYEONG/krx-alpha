"""Per-invocation temporary workspace isolation for pytest runs."""

from __future__ import annotations

import contextlib
import os
import shutil
from collections.abc import Mapping
from datetime import timedelta
from pathlib import Path
from typing import Final
from uuid import uuid4

PYTEST_TMP_BASE_NAME: Final[str] = "pytest"

# Longest legitimate run (verify timeout cap 240s, full slow suite minutes)
# is orders of magnitude shorter; a root older than a day can only belong
# to a crashed/killed run, so age alone safely proves it is not live.
STALE_RUN_ROOT_AGE: Final[timedelta] = timedelta(hours=24)


def resolve_run_id(environ: Mapping[str, str]) -> str:
    """Return the run id grouping all processes of one pytest invocation.

    Uses `PYTEST_XDIST_TESTRUNUID` when present (xdist worker); otherwise a fresh `f"{os.getpid()}-{uuid4().hex[:12]}"`.
    Pure except for pid/uuid generation; never reads files.
    """
    testrunuid = environ.get("PYTEST_XDIST_TESTRUNUID")
    if testrunuid:
        return testrunuid
    return f"{os.getpid()}-{uuid4().hex[:12]}"


def run_tmp_root(base: Path, run_id: str, worker: str) -> Path:
    """Return `base / run_id / worker`. Does not touch the filesystem."""
    return base / run_id / worker


def release_run_tmp_root(root: Path) -> None:
    """Remove `root` recursively, then remove its parent run dir only if now empty (sibling workers may be live).

    Missing paths and `OSError` on the parent rmdir are ignored.
    """
    shutil.rmtree(root, ignore_errors=True)
    with contextlib.suppress(OSError):
        root.parent.rmdir()


def prune_stale_run_roots(base: Path, *, now: float, max_age: timedelta, keep: Path) -> list[Path]:
    """Delete direct children of `base` whose `st_mtime` is older than `now - max_age`, skipping any child that is
    `keep` or an ancestor of `keep`. Returns removed paths (sorted). Missing `base` -> `[]`. Races with a concurrent
    pruner are tolerated (`ignore_errors`). Legacy flat dirs (`main`, `gwN`) are pruned by the same age rule.
    """
    try:
        children = sorted(base.iterdir())
    except OSError:
        return []
    cutoff = now - max_age.total_seconds()
    removed: list[Path] = []
    for child in children:
        if child == keep or child in keep.parents:
            continue
        try:
            mtime = child.stat().st_mtime
        except OSError:
            continue
        if mtime >= cutoff:
            continue
        if child.is_symlink() or child.is_file():
            with contextlib.suppress(OSError):
                child.unlink()
        else:
            shutil.rmtree(child, ignore_errors=True)
        removed.append(child)
    return sorted(removed)
