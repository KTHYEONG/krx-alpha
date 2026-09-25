"""Structural row validation before L1 snapshot persistence."""

from __future__ import annotations

import polars as pl

from src.marketdata.snapshot_schema import SNAPSHOT_LEGACY_COLUMN_RENAMES, SnapshotDataset


def normalize_legacy_snapshot_columns(dataset: SnapshotDataset, frame: pl.DataFrame) -> pl.DataFrame:
    """Rename legacy snapshot columns to their current contract names.

    Partitions written before a column rename stay byte-identical on local
    disk and on the remote archive (their size-verified copies remain valid),
    so compatibility is provided at the read boundary instead of rewriting
    history. Readers of historical partitions must call this before applying
    ``SNAPSHOT_SCHEMAS``.

    Args:
        dataset: Snapshot dataset the frame belongs to.
        frame: Frame as read from a stored partition.

    Returns:
        Frame whose legacy column names are replaced by current names; all
        other columns, their order and values are untouched.

    Raises:
        ValueError: If a legacy column and its current name coexist, because
            choosing one would silently discard observed values.
    """
    renames = SNAPSHOT_LEGACY_COLUMN_RENAMES.get(dataset)
    if not renames:
        return frame
    columns = frame.columns
    for legacy, current in renames.items():
        if legacy in columns and current in columns:
            raise ValueError(f"legacy and current columns coexist: {legacy} and {current}")
    active = {legacy: current for legacy, current in renames.items() if legacy in columns}
    if not active:
        return frame
    return frame.rename(active)


def snapshot_row_violations(dataset: SnapshotDataset, frame: pl.DataFrame) -> pl.Series:
    """Mark structurally invalid vendor rows before L1 snapshot persistence."""
    height = frame.height
    if height == 0:
        return pl.Series([], dtype=pl.Boolean)
    if dataset in (SnapshotDataset.INVESTOR_ESTIMATE, SnapshotDataset.NEWS_TITLE):
        return pl.Series([False] * height, dtype=pl.Boolean)
    if dataset is SnapshotDataset.SECURITY_STATUS:
        expr = (
            (pl.col("last_price").is_not_null() & (pl.col("last_price") <= 0))
            | (
                pl.col("lower_limit").is_not_null()
                & pl.col("upper_limit").is_not_null()
                & (pl.col("lower_limit") >= pl.col("upper_limit"))
            )
            | (
                pl.col("lower_limit").is_not_null()
                & pl.col("last_price").is_not_null()
                & pl.col("upper_limit").is_not_null()
                & ((pl.col("last_price") < pl.col("lower_limit")) | (pl.col("last_price") > pl.col("upper_limit")))
            )
            | (
                pl.col("lower_limit").is_not_null()
                & pl.col("base_price").is_not_null()
                & pl.col("upper_limit").is_not_null()
                & ((pl.col("base_price") < pl.col("lower_limit")) | (pl.col("base_price") > pl.col("upper_limit")))
            )
        )
    elif dataset is SnapshotDataset.AUCTION_BOOK:
        expr = (
            (pl.col("expected_price").is_not_null() & (pl.col("expected_price") < 0))
            | (pl.col("expected_volume").is_not_null() & (pl.col("expected_volume") < 0))
        )
    elif dataset is SnapshotDataset.PROGRAM_TRADE:
        expr = (
            (pl.col("sell_qty").is_not_null() & (pl.col("sell_qty") < 0))
            | (pl.col("buy_qty").is_not_null() & (pl.col("buy_qty") < 0))
            | (pl.col("cum_volume").is_not_null() & (pl.col("cum_volume") < 0))
            | (
                pl.col("net_qty").is_not_null()
                & pl.col("buy_qty").is_not_null()
                & pl.col("sell_qty").is_not_null()
                & (pl.col("net_qty") != pl.col("buy_qty") - pl.col("sell_qty"))
            )
            | (
                pl.col("net_value_krw").is_not_null()
                & pl.col("buy_value_krw").is_not_null()
                & pl.col("sell_value_krw").is_not_null()
                & (pl.col("net_value_krw") != pl.col("buy_value_krw") - pl.col("sell_value_krw"))
            )
        )
    elif dataset is SnapshotDataset.RANKING:
        expr = (pl.col("rank").is_not_null() & (pl.col("rank") < 1)) | (
            pl.col("trade_value_krw").is_not_null() & (pl.col("trade_value_krw") < 0)
        )
    elif dataset is SnapshotDataset.INDEX_SNAPSHOT:
        expr = (
            (pl.col("index_value").is_not_null() & (pl.col("index_value") <= 0))
            | (pl.col("cum_value_mil_krw").is_not_null() & (pl.col("cum_value_mil_krw") < 0))
            | (pl.col("advancers").is_not_null() & (pl.col("advancers") < 0))
            | (pl.col("decliners").is_not_null() & (pl.col("decliners") < 0))
        )
    else:
        expr = (
            (pl.col("open").is_not_null() & (pl.col("open") <= 0))
            | (pl.col("high").is_not_null() & (pl.col("high") <= 0))
            | (pl.col("low").is_not_null() & (pl.col("low") <= 0))
            | (pl.col("close").is_not_null() & (pl.col("close") <= 0))
            | (
                pl.col("high").is_not_null()
                & pl.col("open").is_not_null()
                & pl.col("close").is_not_null()
                & ((pl.col("high") < pl.col("open")) | (pl.col("high") < pl.col("close")))
            )
            | (
                pl.col("low").is_not_null()
                & pl.col("open").is_not_null()
                & pl.col("close").is_not_null()
                & ((pl.col("low") > pl.col("open")) | (pl.col("low") > pl.col("close")))
            )
            | (pl.col("volume").is_not_null() & (pl.col("volume") < 0))
        )
    return frame.select(expr.alias("__violation__")).to_series()
