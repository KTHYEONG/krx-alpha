"""Session-scoped append store for REST snapshot datasets (atomic L1 parquet partitions)."""

from __future__ import annotations

import datetime as dt
import logging
import os
from collections.abc import Mapping, Sequence

import polars as pl

from src.core.config import DataPaths
from src.core.errors import KrxAlphaError
from src.marketdata.snapshot_schema import (
    SNAPSHOT_DEDUP_KEYS,
    SNAPSHOT_SCHEMAS,
    SnapshotDataset,
)
from src.marketdata.snapshot_validation import (
    normalize_legacy_snapshot_columns,
    snapshot_row_violations,
)

logger = logging.getLogger(__name__)


class SnapshotStoreError(KrxAlphaError):
    """Snapshot rows violate the dataset contract or a partition cannot be read safely."""


class SnapshotStore:
    """Accumulate snapshot rows per dataset for one session and persist atomically.

    Each append rewrites the whole day partition from memory, which keeps every
    committed row durable across collector restarts without partial files. A
    dataset has exactly one writer process per session; concurrent writers to
    the same dataset are not supported.
    """

    def __init__(self, *, paths: DataPaths, session_date: dt.date) -> None:
        self._paths = paths
        self._session_date = session_date
        self._frames: dict[SnapshotDataset, pl.DataFrame | None] = {}
        self._loaded: set[SnapshotDataset] = set()
        self._keys: dict[SnapshotDataset, set[tuple[object, ...]]] = {}

    @property
    def session_date(self) -> dt.date:
        """Session date this store persists rows for."""
        return self._session_date

    def _ensure_loaded(self, dataset: SnapshotDataset) -> pl.DataFrame:
        if dataset in self._loaded:
            cached = self._frames.get(dataset)
            assert cached is not None
            return cached
        schema = SNAPSHOT_SCHEMAS[dataset]
        empty = pl.DataFrame(schema=schema, strict=True)
        path = self._paths.snapshot_partition(dataset.value, self._session_date)
        if path.exists():
            try:
                loaded = pl.read_parquet(path)
            except Exception as exc:
                raise SnapshotStoreError(f"unreadable snapshot partition: {path} ({exc})") from exc
            try:
                loaded = normalize_legacy_snapshot_columns(dataset, loaded)
            except ValueError as exc:
                raise SnapshotStoreError(f"schema mismatch in snapshot partition: {path} ({exc})") from exc
            if set(loaded.columns) != set(schema):
                raise SnapshotStoreError(f"schema mismatch in snapshot partition: {path}")
            try:
                loaded = loaded.cast(pl.Schema(schema), strict=True).select(list(schema))
            except Exception as exc:
                raise SnapshotStoreError(f"schema mismatch in snapshot partition: {path} ({exc})") from exc
            self._frames[dataset] = loaded
        else:
            self._frames[dataset] = empty
        # 키 집합은 로드 시 1회만 구성한다: append마다 당일 전체를 재스캔하지 않도록.
        self._keys[dataset] = {self._dedup_key(dataset, row) for row in self._frames[dataset].to_dicts()}  # type: ignore[union-attr]
        self._loaded.add(dataset)
        cached = self._frames.get(dataset)
        assert cached is not None
        return cached

    @staticmethod
    def _dedup_key(dataset: SnapshotDataset, row: Mapping[str, object]) -> tuple[object, ...]:
        key: list[object] = []
        for col in SNAPSHOT_DEDUP_KEYS[dataset]:
            val = row[col]
            key.append(tuple(val) if isinstance(val, list) else val)
        return tuple(key)

    def append(self, dataset: SnapshotDataset, rows: Sequence[Mapping[str, object]]) -> int:
        """Validate, deduplicate and persist rows; return the number of new rows.

        Rows violating dataset value identities are dropped before
        deduplication and counted in a ``[DATA] stage=snapshot_dq`` warning; they never abort the append.

        Raises:
            SnapshotStoreError: On column set mismatch, dtype cast failure,
                session_date mismatch, or an unreadable existing partition.
        """
        if len(rows) == 0:
            self._ensure_loaded(dataset)
            return 0
        schema = SNAPSHOT_SCHEMAS[dataset]
        expected = set(schema)
        for row in rows:
            if set(row.keys()) != expected:
                raise SnapshotStoreError(f"column set mismatch for {dataset.value}")
            if row["session_date"] != self._session_date:
                raise SnapshotStoreError(f"session_date mismatch for {dataset.value}")
        existing = self._ensure_loaded(dataset)
        try:
            batch = pl.DataFrame([dict(row) for row in rows], schema=schema, strict=True).select(list(schema))
        except Exception as exc:
            raise SnapshotStoreError(f"dtype cast failure for {dataset.value} ({exc})") from exc
        mask = snapshot_row_violations(dataset, batch)
        rejected = int(mask.sum())
        total = batch.height
        if rejected > 0:
            if rejected == total:
                logger.critical(
                    "[DATA] stage=snapshot_dq dataset=%s rejected=%d total=%d status=FAIL",
                    dataset.value,
                    rejected,
                    total,
                )
            else:
                logger.warning(
                    "[DATA] stage=snapshot_dq dataset=%s rejected=%d total=%d status=WARN",
                    dataset.value,
                    rejected,
                    total,
                )
            batch = batch.filter(~mask)
            if batch.height == 0:
                return 0
        seen = set(self._keys[dataset])
        fresh: list[dict[str, object]] = []
        for row in batch.to_dicts():
            key = self._dedup_key(dataset, row)
            if key in seen:
                continue
            seen.add(key)
            fresh.append(dict(row))
        if not fresh:
            return 0
        fresh_batch = pl.DataFrame(fresh, schema=schema, strict=True).select(list(schema))
        combined = pl.concat([existing, fresh_batch], how="vertical")
        path = self._paths.snapshot_partition(dataset.value, self._session_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.parent / (path.name + ".tmp")
        combined.write_parquet(tmp_path, compression="zstd")
        os.replace(tmp_path, path)
        self._frames[dataset] = combined
        self._keys[dataset] = seen
        return len(fresh)

    def frame(self, dataset: SnapshotDataset) -> pl.DataFrame:
        """Return a copy of all committed rows of ``dataset`` for this session."""
        return self._ensure_loaded(dataset).clone()
