"""bars-refresh CLI 서브커맨드."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import pathlib

from src.collector.bars import (
    KrxBarsError,
    append_daily_bars,
    backfill_bars,
    derive_market_map,
    latest_trading_day,
    write_market_map,
)

logger = logging.getLogger(__name__)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """'bars-refresh' 서브커맨드를 등록한다."""
    parser = subparsers.add_parser("bars-refresh")
    parser.add_argument("--store-path", required=True)
    parser.add_argument("--market-map-path", required=True)
    parser.add_argument("--ref-date", required=True)
    parser.add_argument("--window-days", type=int, default=90)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """bars store 누적 + market-map 갱신을 수행한다."""
    auth_key = os.environ["KRX_OPENAPI_KEY"]
    ref = dt.date.fromisoformat(str(args.ref_date))
    store = pathlib.Path(str(args.store_path))
    try:
        if not store.exists():
            result = backfill_bars(store, auth_key=auth_key, end_date=ref - dt.timedelta(days=1), window_days=int(args.window_days))
            logger.info(
                "[DATA] stage=bars_backfill trading_days=%d appended=%d status=OK",
                result["trading_days"],
                result["appended_rows"],
            )
        day, bars = latest_trading_day(ref, auth_key=auth_key)
    except KrxBarsError as exc:
        logger.error("[DATA] stage=bars_refresh status=FAIL reason=%s", str(exc))
        return 4
    appended = append_daily_bars(store, bars)
    write_market_map(pathlib.Path(str(args.market_map_path)), derive_market_map(bars))
    logger.info("[DATA] stage=bars_refresh date=%s appended=%d status=OK", day.isoformat(), appended)
    return 0
