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

from src.brokers.kis.auth import KisAppAuth, KisTokenProvider, kis_token_cache_path
from src.brokers.kis.data import KisDataClient
from src.brokers.kis.http import KisGetTransport
from src.brokers.kis.rate import RateLimiter
from src.core.config import KisTokenSettings, resolve_collector_runtime
from src.core.errors import MissingCredentialsError
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
    credentials = load_kis_data_credentials()
    cred = next((c for c in credentials if c.slot == settings.kis_data_slot), None)
    if cred is None:
        raise MissingCredentialsError(f"no data credential for slot {settings.kis_data_slot}")
    token_settings = KisTokenSettings()
    auth = KisAppAuth(app_key=cred.app_key, app_secret=cred.app_secret)
    limiter = RateLimiter(settings.rest_rate_per_s)
    tokens = KisTokenProvider(
        auth=auth,
        session=requests,
        cache_path=kis_token_cache_path(token_settings.token_cache_dir, cred.app_key),
        limiter=limiter,
        now=lambda: dt.datetime.now(_KST),
        timeout_s=settings.request_timeout_s,
        base_url="https://openapi.koreainvestment.com:9443",
        allow_issue=token_settings.allow_issue,
    )
    transport = KisGetTransport(
        auth=auth,
        tokens=tokens,
        session=requests,
        limiter=limiter,
        timeout_s=settings.request_timeout_s,
        base_url="https://openapi.koreainvestment.com:9443",
    )
    client = KisDataClient(transport=transport)
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
        settings=settings,
        session_date=session_date,
        source=client,
        store=store,
        symbols=symbols,
        now_fn=lambda: dt.datetime.now(_KST),
        sleep_fn=time.sleep,
        max_iterations=args.max_iterations,
    )
    return 0
