"""Month-partitioned daily stores with bounded reads."""

from __future__ import annotations

import datetime as dt
import logging
import os
import pathlib
import re
import shutil
from dataclasses import dataclass
from typing import cast

import polars as pl

from src.core.errors import KrxAlphaError

logger = logging.getLogger(__name__)

_PARTITION_RE = re.compile(r"^(\d{4})-(\d{2})\.parquet$")
_MONTH_FORMAT = "%Y-%m"


class PartitionedStoreError(KrxAlphaError):
    """Month-partitioned store unreadable, mismatched, or ambiguously migrated."""


def month_partition_path(root: pathlib.Path, day: dt.date) -> pathlib.Path:
    """Return the month file holding rows for ``day``."""
    return pathlib.Path(root) / f"{day:{_MONTH_FORMAT}}.parquet"


def _iter_partition_files(root: pathlib.Path) -> list[tuple[int, int, pathlib.Path]]:
    """List ``YYYY-MM.parquet`` files under ``root`` oldest-first without opening them."""
    found: list[tuple[int, int, pathlib.Path]] = []
    try:
        entries = list(pathlib.Path(root).iterdir())
    except OSError:
        return []
    for entry in entries:
        match = _PARTITION_RE.fullmatch(entry.name)
        if match is None or not entry.is_file():
            continue
        found.append((int(match.group(1)), int(match.group(2)), entry))
    found.sort()
    return found


def _month_in_range(year: int, month: int, min_date: dt.date | None, max_date: dt.date | None) -> bool:
    month_start = dt.date(year, month, 1)
    if month == 12:
        month_end = dt.date(year + 1, 1, 1) - dt.timedelta(days=1)
    else:
        month_end = dt.date(year, month + 1, 1) - dt.timedelta(days=1)
    if min_date is not None and month_end < min_date:
        return False
    return not (max_date is not None and month_start > max_date)


def upsert_month_partitions(
    root: pathlib.Path,
    frame: pl.DataFrame,
    *,
    key_columns: tuple[str, ...],
    sort_columns: tuple[str, ...],
    date_column: str = "date",
) -> int:
    """Upsert rows into ``root/YYYY-MM.parquet`` files, rewriting only touched months.

    Write cost and peak memory are bounded by one month of rows regardless of
    total history, which keeps the long-running daemon inside its fixed
    container memory budget as the store grows.

    Returns:
        Number of incoming key rows that were not present before.

    Raises:
        PartitionedStoreError: If an existing partition cannot be read or its
            schema differs from ``frame``.
    """
    store = pathlib.Path(root)
    keys = tuple(key_columns)
    order = tuple(sort_columns)
    if date_column not in frame.columns:
        raise PartitionedStoreError(f"missing date column {date_column!r}")
    stamped = frame.with_columns(pl.col(date_column).dt.strftime(_MONTH_FORMAT).alias("__month"))
    added = 0
    for month in stamped.get_column("__month").unique().sort().to_list():
        part = stamped.filter(pl.col("__month") == month).drop("__month")
        target = store / f"{month}.parquet"
        if target.exists():
            try:
                existing = pl.read_parquet(target)
            except Exception as exc:
                raise PartitionedStoreError(f"partition unreadable at {target}: {exc}") from exc
            if list(existing.schema.items()) != list(part.schema.items()):
                raise PartitionedStoreError(f"partition schema mismatch at {target}")
            added += part.join(existing.select(list(keys)), on=list(keys), how="anti").height
            kept = existing.join(part.select(list(keys)), on=list(keys), how="anti")
            combined = pl.concat([kept, part]).sort(list(order))
        else:
            added += part.height
            combined = part.sort(list(order))
        store.mkdir(parents=True, exist_ok=True)
        tmp = store / f".{target.name}.tmp"
        combined.write_parquet(tmp, compression="zstd")
        os.replace(tmp, target)
    return added


def scan_month_partitions(
    root: pathlib.Path, *, min_date: dt.date | None = None, max_date: dt.date | None = None
) -> pl.LazyFrame:
    """Scan partitions whose name-derived month overlaps ``[min_date, max_date]``.

    Files wholly outside the range are never opened; the exact row-level date
    filter is then applied lazily.

    Raises:
        PartitionedStoreError: If ``root`` holds no partition files at all, or
            none overlapping the requested range.
    """
    files = _iter_partition_files(root)
    if not files:
        raise PartitionedStoreError(f"no month partitions in {root}")
    selected = [path for year, month, path in files if _month_in_range(year, month, min_date, max_date)]
    if not selected:
        raise PartitionedStoreError(f"no month partitions in range in {root}")
    frame = pl.scan_parquet(selected)
    if min_date is not None:
        frame = frame.filter(pl.col("date") >= pl.lit(min_date))
    if max_date is not None:
        frame = frame.filter(pl.col("date") <= pl.lit(max_date))
    return frame


def latest_partition_date(root: pathlib.Path, *, date_column: str = "date") -> dt.date | None:
    """Return the max ``date_column`` value, reading only the newest-named file."""
    files = _iter_partition_files(root)
    if not files:
        return None
    newest = files[-1][2]
    return cast(
        "dt.date | None",
        pl.scan_parquet(newest).select(pl.col(date_column).max()).collect().item(),
    )


@dataclass(frozen=True)
class MigrationResult:
    migrated: bool
    rows: int
    partitions: int


def migrate_single_file_store(
    legacy_file: pathlib.Path,
    root: pathlib.Path,
    *,
    key_columns: tuple[str, ...],
    sort_columns: tuple[str, ...],
    date_column: str = "date",
) -> MigrationResult:
    """Split a legacy single-file store into month partitions, preserving rows.

    Writes into a sibling temp dir, verifies total row count and distinct key
    set against the legacy file, then renames the temp dir to ``root`` and the
    legacy file to ``<name>.migrated``. The legacy file is never deleted.
    """
    legacy = pathlib.Path(legacy_file)
    store = pathlib.Path(root)
    try:
        return _migrate(legacy, store, tuple(key_columns), tuple(sort_columns), date_column)
    except OSError as exc:
        # 디스크 부족·권한·rename 실패도 데몬을 죽이지 않고 fail-closed 경로(CRITICAL + 오케스트레이션 차단)로 보낸다.
        raise PartitionedStoreError(f"migration filesystem error for {legacy}: {exc}") from exc


def _migrate(
    legacy: pathlib.Path,
    store: pathlib.Path,
    keys: tuple[str, ...],
    order: tuple[str, ...],
    date_column: str,
) -> MigrationResult:
    if not legacy.exists():
        return MigrationResult(migrated=False, rows=0, partitions=0)
    if _iter_partition_files(store):
        raise PartitionedStoreError(f"ambiguous store state: {legacy} and partitions in {store}")
    # 원본을 통째로 올리지 않고 월 단위 lazy 스캔으로 나눈다: 피크 메모리가 전체 이력이 아니라 한 달 분량에 비례한다.
    source = pl.scan_parquet(legacy)
    try:
        schema_names = source.collect_schema().names()
        months = (
            source.select(pl.col(date_column).dt.strftime(_MONTH_FORMAT).unique().sort())
            .collect(engine="streaming")
            .to_series()
            .to_list()
            if date_column in schema_names
            else []
        )
        legacy_rows = int(source.select(pl.len()).collect().item())
    except Exception as exc:
        raise PartitionedStoreError(f"legacy store unreadable at {legacy}: {exc}") from exc
    if date_column not in schema_names:
        raise PartitionedStoreError(f"legacy store missing date column {date_column!r}")
    tmp_dir = store.parent / f".{store.name}.migrate-tmp"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True)
    files: list[pathlib.Path] = []
    for month in months:
        first = dt.date.fromisoformat(f"{month}-01")
        after = dt.date(first.year + (first.month // 12), first.month % 12 + 1, 1)
        target = tmp_dir / f"{month}.parquet"
        source.filter((pl.col(date_column) >= first) & (pl.col(date_column) < after)).sort(list(order)).collect().write_parquet(
            target, compression="zstd"
        )
        files.append(target)
    migrated = pl.concat([pl.scan_parquet(f) for f in files]) if files else None
    migrated_rows = int(migrated.select(pl.len()).collect().item()) if migrated is not None else 0
    if migrated_rows != legacy_rows:
        raise PartitionedStoreError(f"migration row count mismatch: {migrated_rows} != {legacy_rows}")
    if migrated is not None:
        # 키 집합 비교는 polars anti-join 으로 한다: 146만 행을 Python tuple set 으로 만들면 1GiB 컨테이너가 OOM 된다.
        legacy_keys = source.select(list(keys)).unique()
        migrated_keys = migrated.select(list(keys)).unique()
        missing = legacy_keys.join(migrated_keys, on=list(keys), how="anti", nulls_equal=True).select(pl.len()).collect(engine="streaming").item()
        extra = migrated_keys.join(legacy_keys, on=list(keys), how="anti", nulls_equal=True).select(pl.len()).collect(engine="streaming").item()
        if missing or extra:
            raise PartitionedStoreError(f"migration key set mismatch: missing={missing} extra={extra}")
    if store.exists():
        # 이전에 중단된 upsert 가 남긴 *.tmp 만 있는 빈 store 는 정리하고, 그 밖의 내용은 fail-closed.
        leftovers = list(store.iterdir())
        if any(not (entry.is_file() and entry.name.endswith(".tmp")) for entry in leftovers):
            raise PartitionedStoreError(f"unexpected content in {store}; refusing to migrate over it")
        for entry in leftovers:
            entry.unlink()
        store.rmdir()
    os.rename(tmp_dir, store)
    legacy.rename(legacy.with_name(legacy.name + ".migrated"))
    logger.info(
        "[DATA] stage=store_migration status=OK rows=%d partitions=%d",
        legacy_rows,
        len(months),
    )
    return MigrationResult(migrated=True, rows=legacy_rows, partitions=len(months))
