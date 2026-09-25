"""Persisted REST snapshot dataset schemas."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

import polars as pl


class SnapshotDataset(StrEnum):
    """Identify the persisted REST snapshot schema for each dataset."""

    SECURITY_STATUS = "security_status"
    AUCTION_BOOK = "auction_book"
    INVESTOR_ESTIMATE = "investor_estimate"
    PROGRAM_TRADE = "program_trade"
    RANKING = "ranking"
    INDEX_SNAPSHOT = "index_snapshot"
    INDEX_MINUTE_BAR = "index_minute_bar"
    NEWS_TITLE = "news_title"
    STOCK_MINUTE_BAR = "stock_minute_bar"


COMMON_SNAPSHOT_COLUMNS: dict[str, Any] = {
    "session_date": pl.Date,
    "observed_at_ns": pl.Int64,
    "source_tr": pl.String,
    "market_div_code": pl.String,
}

SNAPSHOT_SCHEMAS: dict[SnapshotDataset, dict[str, Any]] = {
    SnapshotDataset.SECURITY_STATUS: {
        **COMMON_SNAPSHOT_COLUMNS,
        "symbol": pl.String,
        "status_code": pl.String,
        "managed": pl.Boolean,
        "market_warning_code": pl.String,
        "short_overheated": pl.Boolean,
        "investment_caution": pl.Boolean,
        "liquidation_trading": pl.Boolean,
        "trading_halted": pl.Boolean,
        "vi_code": pl.String,
        # KIS 필드 `ovtm_vi_cls_code`를 그대로 전달하는 값이다. 2026-09-14 개편 이후
        # 어느 세션을 가리키는지는 미확인이므로 애프터마켓 VI 플래그로 해석해서는 안 된다.
        "ovtm_vi_cls_code": pl.String,
        "credit_available": pl.Boolean,
        "last_price": pl.Int64,
        "base_price": pl.Int64,
        "upper_limit": pl.Int64,
        "lower_limit": pl.Int64,
    },
    SnapshotDataset.AUCTION_BOOK: {
        **COMMON_SNAPSHOT_COLUMNS,
        "phase": pl.String,
        "symbol": pl.String,
        "book_time": pl.String,
        "auction_code": pl.String,
        "expected_price": pl.Int64,
        "expected_volume": pl.Int64,
        "expected_change_pct": pl.Float64,
        "vi_code": pl.String,
        "last_price": pl.Int64,
        "base_price": pl.Int64,
        "ask_prices": pl.List(pl.Int64),
        "ask_sizes": pl.List(pl.Int64),
        "bid_prices": pl.List(pl.Int64),
        "bid_sizes": pl.List(pl.Int64),
        "total_ask_size": pl.Int64,
        "total_bid_size": pl.Int64,
    },
    SnapshotDataset.INVESTOR_ESTIMATE: {
        **COMMON_SNAPSHOT_COLUMNS,
        "symbol": pl.String,
        "bucket": pl.Int64,
        "foreign_net_qty": pl.Int64,
        "institution_net_qty": pl.Int64,
        "total_net_qty": pl.Int64,
    },
    SnapshotDataset.PROGRAM_TRADE: {
        **COMMON_SNAPSHOT_COLUMNS,
        "symbol": pl.String,
        "trade_time": pl.String,
        "cum_volume": pl.Int64,
        "sell_qty": pl.Int64,
        "buy_qty": pl.Int64,
        "net_qty": pl.Int64,
        "sell_value_krw": pl.Int64,
        "buy_value_krw": pl.Int64,
        "net_value_krw": pl.Int64,
    },
    SnapshotDataset.RANKING: {
        **COMMON_SNAPSHOT_COLUMNS,
        "list_kind": pl.String,
        "rank": pl.Int64,
        "symbol": pl.String,
        "change_pct": pl.Float64,
        "trade_value_krw": pl.Int64,
    },
    SnapshotDataset.INDEX_SNAPSHOT: {
        **COMMON_SNAPSHOT_COLUMNS,
        "index_code": pl.String,
        "index_value": pl.Float64,
        "change_pct": pl.Float64,
        "cum_value_mil_krw": pl.Int64,
        "advancers": pl.Int64,
        "decliners": pl.Int64,
    },
    SnapshotDataset.INDEX_MINUTE_BAR: {
        **COMMON_SNAPSHOT_COLUMNS,
        "index_code": pl.String,
        "bar_time": pl.String,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume": pl.Int64,
        "cum_value_mil_krw": pl.Int64,
    },
    SnapshotDataset.NEWS_TITLE: {
        **COMMON_SNAPSHOT_COLUMNS,
        "news_id": pl.String,
        "published_at_ns": pl.Int64,
        "provider": pl.String,
        "provider_code": pl.String,
        "category_code": pl.String,
        "title": pl.String,
        "symbols": pl.List(pl.String),
    },
    SnapshotDataset.STOCK_MINUTE_BAR: {
        **COMMON_SNAPSHOT_COLUMNS,
        "symbol": pl.String,
        "bar_time": pl.String,
        "open": pl.Int64,
        "high": pl.Int64,
        "low": pl.Int64,
        "close": pl.Int64,
        "volume": pl.Int64,
    },
}

SNAPSHOT_DEDUP_KEYS: dict[SnapshotDataset, tuple[str, ...]] = {
    SnapshotDataset.SECURITY_STATUS: ("symbol", "observed_at_ns"),
    SnapshotDataset.AUCTION_BOOK: ("symbol", "observed_at_ns"),
    SnapshotDataset.PROGRAM_TRADE: ("symbol", "observed_at_ns"),
    SnapshotDataset.INVESTOR_ESTIMATE: ("symbol", "bucket", "observed_at_ns"),
    SnapshotDataset.RANKING: ("list_kind", "rank", "observed_at_ns"),
    SnapshotDataset.INDEX_SNAPSHOT: ("index_code", "observed_at_ns"),
    SnapshotDataset.INDEX_MINUTE_BAR: ("index_code", "bar_time"),
    SnapshotDataset.NEWS_TITLE: ("news_id",),
    SnapshotDataset.STOCK_MINUTE_BAR: ("symbol", "bar_time"),
}


SNAPSHOT_LEGACY_COLUMN_RENAMES: dict[SnapshotDataset, dict[str, str]] = {
    SnapshotDataset.SECURITY_STATUS: {"overtime_vi_code": "ovtm_vi_cls_code"},
}
