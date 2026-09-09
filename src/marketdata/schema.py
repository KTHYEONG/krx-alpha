"""일봉 스키마 단일 소스 (수집/적재 dtype 캐스팅 기준)."""

from __future__ import annotations

import polars as pl

REQUIRED_BAR_COLUMNS: tuple[str, ...] = ("date", "symbol", "close", "volume", "trade_value_100m", "daily_change_pct")

BAR_SCHEMA: dict[str, type[pl.DataType]] = {
    "date": pl.Date,
    "symbol": pl.String,
    "close": pl.Float64,
    "volume": pl.Int64,
    "trade_value_100m": pl.Float64,
    "daily_change_pct": pl.Float64,
    "market": pl.String,
}
