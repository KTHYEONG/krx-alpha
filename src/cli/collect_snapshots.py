"""collect-snapshots CLI subcommand (intraday REST snapshot collector)."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import pathlib
import time
from typing import Any, cast
from zoneinfo import ZoneInfo

import requests

from src.brokers.kis.stack import build_kis_data_slot_stack
from src.core.config import KisTokenSettings, resolve_collector_runtime
from src.core.session_anchors import resolve_session_anchors
from src.marketdata.snapshot_plan import shift_snapshot_settings
from src.marketdata.snapshot_service import run_snapshot_session
from src.realtime.kis_sharding import load_kis_data_credentials
from src.storage.snapshot_store import SnapshotStore
from src.universe.ipc import CandidateFileError, read_candidates

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register the 'collect-snapshots' subcommand."""
    parser = subparsers.add_parser("collect-snapshots")
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--candidates-path", required=True)
    parser.add_argument("--max-iterations", type=int, default=None)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """Build the data-slot KIS client and run the snapshot session to completion."""
    session_date = dt.date.fromisoformat(str(args.session_date))
    runtime = resolve_collector_runtime()
    settings = runtime.snapshot
    stack, _ = build_kis_data_slot_stack(snapshot=settings, credentials=load_kis_data_credentials(),
                                         token_settings=KisTokenSettings(), session=requests, now=lambda: dt.datetime.now(_KST))
    client = stack.data
    try:
        data = read_candidates(pathlib.Path(str(args.candidates_path)))
    except CandidateFileError as exc:
        logger.warning("[DATA] stage=snapshots status=DEGRADED reason=%s", str(exc))
        data = None
    if data is None:
        logger.warning("[DATA] stage=snapshots status=DEGRADED reason=no_candidates")
        symbols: tuple[str, ...] = ()
    else:
        candidates = cast("list[dict[str, Any]]", data.get("candidates") or [])
        symbols = tuple(str(row["symbol"]) for row in candidates)
    store = SnapshotStore(paths=runtime.paths, session_date=session_date)
    run_snapshot_session(
        settings=shift_snapshot_settings(
            settings, resolve_session_anchors(runtime.paths.session_calendar_dir, session_date)
        ),
        session_date=session_date,
        source=client,
        store=store,
        symbols=symbols,
        now_fn=lambda: dt.datetime.now(_KST),
        sleep_fn=time.sleep,
        max_iterations=args.max_iterations,
    )
    return 0
