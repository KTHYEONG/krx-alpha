"""Idempotent Parquet persistence for validated KRX daily bars and market map."""

from __future__ import annotations

import datetime as dt
import json
import os
import pathlib
from typing import Literal, cast

import polars as pl

from src.marketdata.bar_validation import KrxBarsError, validate_daily_bars
from src.marketdata.partitioned_store import PartitionedStoreError, upsert_month_partitions
from src.marketdata.schema import BAR_SCHEMA, STORED_BAR_COLUMNS

MIN_ROWCOUNT_RATIO: float = 0.90

_BAR_KEY_COLUMNS: tuple[str, ...] = ("date", "symbol")
_BAR_SORT_COLUMNS: tuple[str, ...] = ("symbol", "date")


class ImplausibleRowCountError(KrxBarsError):
    """직전 거래일 대비 행수 급감(절단 응답) fail-closed 신호."""


def _reference_day_height(store: pathlib.Path, incoming_dates: list[dt.date]) -> int | None:
    """Row count of the latest stored date strictly not among ``incoming_dates``.

    Partitions are examined newest-first, stopping at the first file that
    contains such a date, so the read stays bounded by one month file.
    """
    incoming = set(incoming_dates)
    if store.exists() and not store.is_dir():
        raise PartitionedStoreError(f"bars store is not a directory: {store}")
    files = sorted(store.glob("*.parquet"), reverse=True)
    for path in files:
        try:
            dates = (
                pl.scan_parquet(path)
                .select(pl.col("date").unique())
                .collect()
                .get_column("date")
                .to_list()
            )
        except Exception as exc:
            raise PartitionedStoreError(f"partition unreadable at {path}: {exc}") from exc
        prior = [day for day in dates if day not in incoming]
        if not prior:
            continue
        reference = max(prior)
        return cast(
            "int",
            pl.scan_parquet(path)
            .filter(pl.col("date") == reference)
            .select(pl.len())
            .collect()
            .item(),
        )
    return None


def _all_stored_dates(store: pathlib.Path, exclude: set[dt.date]) -> list[dt.date]:
    """Collect distinct stored dates excluding ``exclude`` across all partitions."""
    if store.exists() and not store.is_dir():
        raise PartitionedStoreError(f"bars store is not a directory: {store}")
    dates: set[dt.date] = set()
    for path in sorted(store.glob("*.parquet")):
        try:
            got = (
                pl.scan_parquet(path)
                .select(pl.col("date").unique())
                .collect()
                .get_column("date")
                .to_list()
            )
        except Exception as exc:
            raise PartitionedStoreError(f"partition unreadable at {path}: {exc}") from exc
        for day in got:
            if day not in exclude:
                dates.add(day)
    return sorted(dates)


def _count_on_date(store: pathlib.Path, day: dt.date) -> int:
    """Row count for one stored date, read from its month file when present."""
    candidate = store / f"{day:%Y-%m}.parquet"
    if candidate.exists():
        return cast(
            "int",
            pl.scan_parquet(candidate)
            .filter(pl.col("date") == day)
            .select(pl.len())
            .collect()
            .item(),
        )
    files = sorted(store.glob("*.parquet"))
    if not files:
        return 0
    return cast(
        "int",
        pl.scan_parquet(files)
        .filter(pl.col("date") == day)
        .select(pl.len())
        .collect()
        .item(),
    )


def _nearest_day_height(store: pathlib.Path, incoming_dates: list[dt.date]) -> int | None:
    """Row count of the stored date nearest to the incoming batch.

    Prefers the closest later date when one exists, else the closest earlier
    date. Listing counts drift over years, so history backfill must compare
    against the temporal neighbor instead of today's latest height.
    """
    if not incoming_dates:
        return None
    anchor = min(incoming_dates)
    stored = _all_stored_dates(store, set(incoming_dates))
    if not stored:
        return None
    later = [day for day in stored if day > anchor]
    if later:
        return _count_on_date(store, min(later))
    return _count_on_date(store, max(day for day in stored if day < anchor))


def append_daily_bars(
    store_path: pathlib.Path, bars: pl.DataFrame, *, reference: Literal["latest", "nearest"] = "latest",
) -> int:
    """Persist validated daily bars idempotently under the existing Parquet schema.

    Args:
        store_path: Month-partition root directory (``YYYY-MM.parquet`` files).
        reference: ``"latest"`` compares against the latest stored date,
            ``"nearest"`` against the stored date nearest the incoming date.
    """
    if reference not in ("latest", "nearest"):
        raise ValueError(f"unknown reference {reference!r}")
    store = pathlib.Path(store_path)
    present = [c for c in STORED_BAR_COLUMNS if c in bars.columns]
    incoming = bars.select(present)
    for column in STORED_BAR_COLUMNS:
        if column not in incoming.columns:
            incoming = incoming.with_columns(pl.lit(None).cast(BAR_SCHEMA[column]).alias(column))
    incoming = incoming.select(STORED_BAR_COLUMNS).with_columns([
        pl.col(column).cast(BAR_SCHEMA[column]) for column in STORED_BAR_COLUMNS
    ])
    validate_daily_bars(incoming)
    incoming_dates = incoming["date"].unique().to_list()
    if reference == "nearest":
        for day in incoming_dates:
            reference_height = _nearest_day_height(store, [day])
            height = incoming.filter(pl.col("date") == day).height
            if reference_height is not None and height < reference_height * MIN_ROWCOUNT_RATIO:
                raise ImplausibleRowCountError(
                    f"implausible row count for {day}: {height} < {reference_height} * {MIN_ROWCOUNT_RATIO}"
                )
    else:
        reference_height = _reference_day_height(store, incoming_dates)
        if reference_height is not None and incoming.height < reference_height * MIN_ROWCOUNT_RATIO:
            raise ImplausibleRowCountError(
                f"implausible row count for {incoming_dates}: {incoming.height} < {reference_height} * {MIN_ROWCOUNT_RATIO}"
            )
    return upsert_month_partitions(store, incoming, key_columns=_BAR_KEY_COLUMNS, sort_columns=_BAR_SORT_COLUMNS)


def write_market_map(
    path: pathlib.Path, market_map: dict[str, str],
) -> None:
    """Atomically persist the current symbol-to-market mapping format."""
    target = pathlib.Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.parent / f".{target.name}.tmp"
    tmp.write_text(json.dumps(market_map, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, target)
