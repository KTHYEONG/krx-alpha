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
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "base_price": pl.Float64,
    "market_cap_krw": pl.Int64,
    "listed_shares": pl.Int64,
    "section": pl.String,
    "stock_cert_kind": pl.String,
    "security_group": pl.String,
}

STORED_BAR_COLUMNS: tuple[str, ...] = (
    *REQUIRED_BAR_COLUMNS,
    "market",
    "open",
    "high",
    "low",
    "base_price",
    "market_cap_krw",
    "listed_shares",
    "section",
    "stock_cert_kind",
    "security_group",
)
