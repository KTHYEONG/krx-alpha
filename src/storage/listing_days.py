"""Listing-day symbol resolution from the daily-bar store."""

from __future__ import annotations

import datetime as dt
import logging
import pathlib
import re

import polars as pl

logger = logging.getLogger(__name__)


def listing_day_symbols(bars_daily_dir: pathlib.Path, day: dt.date) -> frozenset[str]:
    """Return symbols whose first-ever daily bar is ``day``.

    The daily-bar store is the only point-in-time source of listing dates available to the
    normalizer: the tick stream itself does not flag a listing day. A symbol counts as newly
    listed only when ``day`` is strictly after the earliest bar date of the whole store, so
    symbols present at the store's earliest bar date are never misclassified.

    Args:
        bars_daily_dir: Monthly daily-bar parquet directory (``DataPaths.bars_daily_dir``).
        day: Partition date being normalized.

    Returns:
        Newly listed symbols for ``day``. Empty when the store is missing, empty, unreadable,
        or holds no bar for ``day``; the caller then applies the standard band, which can
        only over-report violations, never hide them.

    Note:
        Reads only the ``date`` and ``symbol`` columns, aggregating one month at a time.
        Future month partitions are not opened. Unknown dates or symbols degrade to the
        standard band instead of granting an exemption from incomplete history. A degraded read logs
        ``[DATA] stage=listing_days status=DEGRADED reason=<exception class>`` at WARNING
        and never raises.
    """
    store = pathlib.Path(bars_daily_dir)
    if not store.is_dir():
        logger.warning(
            "[DATA] stage=listing_days status=DEGRADED reason=%s",
            FileNotFoundError.__name__,
        )
        return frozenset()
    try:
        files = sorted(store.glob("*.parquet"))
        if not files:
            return frozenset()
        first_dates: dict[str, dt.date] = {}
        for path in files:
            month = re.fullmatch(r"(\d{4}-\d{2})\.parquet", path.name)
            if month is not None and dt.date.fromisoformat(f"{month[1]}-01") > day:
                continue
            source = pl.scan_parquet(path).select("date", "symbol")
            schema = source.collect_schema()
            if schema["date"] != pl.Date or schema["symbol"] != pl.String:
                raise ValueError("invalid daily-bar key schema")
            first = (
                source.filter(pl.col("date").is_null() | (pl.col("date") <= pl.lit(day)))
                .group_by("symbol")
                .agg(pl.col("date").min(), pl.col("date").null_count().alias("unknown_dates"))
                .collect(engine="streaming")
            )
            for symbol, first_date, unknown_dates in first.iter_rows():
                if not isinstance(symbol, str) or not symbol or unknown_dates:
                    raise ValueError("incomplete daily-bar key history")
                previous = first_dates.get(symbol)
                if previous is None or first_date < previous:
                    first_dates[symbol] = first_date
        if not first_dates or min(first_dates.values()) >= day:
            return frozenset()
        return frozenset(symbol for symbol, first_date in first_dates.items() if first_date == day)
    except Exception as exc:
        logger.warning(
            "[DATA] stage=listing_days status=DEGRADED reason=%s",
            type(exc).__name__,
        )
        return frozenset()
