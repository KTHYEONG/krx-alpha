"""Snapshot contracts compatibility shim (canonical modules own the logic)."""

from __future__ import annotations

from src.marketdata.snapshot_plan import (
    SnapshotJob,
    SnapshotJobKind,
    _add_seconds,
    _aware,
    _coverage_series,
    _interval_series,
    _interval_series_inclusive,
    build_session_jobs,
    partition_due_jobs,
)
from src.marketdata.snapshot_schema import (
    COMMON_SNAPSHOT_COLUMNS,
    SNAPSHOT_DEDUP_KEYS,
    SNAPSHOT_LEGACY_COLUMN_RENAMES,
    SNAPSHOT_SCHEMAS,
    SnapshotDataset,
)
from src.marketdata.snapshot_validation import (
    normalize_legacy_snapshot_columns,
    snapshot_row_violations,
)

__all__ = [
    "COMMON_SNAPSHOT_COLUMNS",
    "SNAPSHOT_DEDUP_KEYS",
    "SNAPSHOT_LEGACY_COLUMN_RENAMES",
    "SNAPSHOT_SCHEMAS",
    "SnapshotDataset",
    "SnapshotJob",
    "SnapshotJobKind",
    "_add_seconds",
    "_aware",
    "_coverage_series",
    "_interval_series",
    "_interval_series_inclusive",
    "build_session_jobs",
    "normalize_legacy_snapshot_columns",
    "partition_due_jobs",
    "snapshot_row_violations",
]
