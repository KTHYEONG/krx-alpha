"""collect-market-sweep CLI subcommand (full-market auction and investor-estimate REST sweep)."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import pathlib
import time
from zoneinfo import ZoneInfo

import requests

from src.brokers.kis.stack import build_kis_data_slot_stack
from src.core.config import KisTokenSettings, resolve_collector_runtime
from src.core.session_anchors import resolve_session_anchors
from src.marketdata.market_sweep import (
    MarketMapError,
    idle_until_run_end,
    load_market_symbols,
    run_market_sweep_session,
    shift_market_sweep_settings,
)
from src.marketdata.quiet_check import QuietCheck
from src.realtime.kis_sharding import load_kis_data_credentials
from src.storage.snapshot_store import SnapshotStore

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register the 'collect-market-sweep' subcommand."""
    parser = subparsers.add_parser("collect-market-sweep")
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--market-map-path", required=True)
    parser.add_argument("--max-iterations", type=int, default=None)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """Sweep the full listing on the configured data slot; a missing listing idles to run end without calls."""
    session_date = dt.date.fromisoformat(str(args.session_date))
    runtime = resolve_collector_runtime()
    settings = shift_market_sweep_settings(
        runtime.market_sweep, resolve_session_anchors(runtime.paths.session_calendar_dir, session_date)
    )
    now_fn = lambda: dt.datetime.now(_KST)  # noqa: E731
    try:
        symbols = load_market_symbols(pathlib.Path(str(args.market_map_path)), markets=settings.markets)
    except MarketMapError as exc:
        logger.warning("[DATA] stage=market_sweep status=DEGRADED reason=%s", str(exc))
        idle_until_run_end(
            settings, session_date, now_fn=now_fn, sleep_fn=time.sleep, max_iterations=args.max_iterations
        )
        return 0
    stack, _ = build_kis_data_slot_stack(
        snapshot=runtime.market_sweep,
        credentials=load_kis_data_credentials(),
        token_settings=KisTokenSettings(),
        session=requests,
        now=now_fn,
    )
    logger.info(
        "[DATA] stage=market_sweep status=READY symbols=%d concurrency=%d", len(symbols), settings.concurrency
    )
    run_market_sweep_session(
        settings=settings,
        session_date=session_date,
        source=stack.data,
        store=SnapshotStore(paths=runtime.paths, session_date=session_date),
        symbols=symbols,
        now_fn=now_fn,
        sleep_fn=time.sleep,
        max_iterations=args.max_iterations,
        quiet_check=QuietCheck(runtime.paths.quiet_suspects_path(), session_date),
    )
    return 0
