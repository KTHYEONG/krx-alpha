"""Idempotent Parquet persistence for validated KRX daily bars and market map."""

from __future__ import annotations

import json
import os
import pathlib

import polars as pl

from src.marketdata.bar_validation import KrxBarsError, validate_daily_bars
from src.marketdata.schema import BAR_SCHEMA, STORED_BAR_COLUMNS

MIN_ROWCOUNT_RATIO: float = 0.90


class ImplausibleRowCountError(KrxBarsError):
    """직전 거래일 대비 행수 급감(절단 응답) fail-closed 신호."""


def append_daily_bars(
    store_path: pathlib.Path, bars: pl.DataFrame,
) -> int:
    """Persist validated daily bars idempotently under the existing Parquet schema."""
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
    if store.exists():
        existing = pl.read_parquet(store)
        for column in STORED_BAR_COLUMNS:
            if column not in existing.columns:
                existing = existing.with_columns(pl.lit(None).cast(BAR_SCHEMA[column]).alias(column))
        existing = existing.select(STORED_BAR_COLUMNS)
        incoming_dates = incoming["date"].unique().to_list()
        prior = existing.filter(~pl.col("date").is_in(incoming_dates))
        if prior.height > 0:
            reference_date = prior["date"].max()
            reference_height = prior.filter(pl.col("date") == reference_date).height
            if incoming.height < reference_height * MIN_ROWCOUNT_RATIO:
                raise ImplausibleRowCountError(
                    f"implausible row count for {incoming_dates}: {incoming.height} < {reference_height} * {MIN_ROWCOUNT_RATIO}"
                )
        new_rows = incoming.join(
            existing.select(["date", "symbol"]), on=["date", "symbol"], how="anti"
        )
        if new_rows.height == 0:
            return 0
        kept = existing.join(incoming.select(["date", "symbol"]), on=["date", "symbol"], how="anti")
        combined = pl.concat([kept, incoming]).sort(["symbol", "date"])
    else:
        new_rows = incoming
        combined = incoming.sort(["symbol", "date"])
    store.parent.mkdir(parents=True, exist_ok=True)
    tmp = store.parent / f".{store.name}.tmp"
    combined.write_parquet(tmp, compression="zstd")
    os.replace(tmp, store)
    return new_rows.height


def write_market_map(
    path: pathlib.Path, market_map: dict[str, str],
) -> None:
    """Atomically persist the current symbol-to-market mapping format."""
    target = pathlib.Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.parent / f".{target.name}.tmp"
    tmp.write_text(json.dumps(market_map, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, target)
