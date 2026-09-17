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

from src.core.config import CollectorSettings, KisCredentials, KisTokenSettings, SnapshotSettings
from src.core.errors import MissingCredentialsError
from src.execution.kis_client import KisRestClient, RateLimiter, kis_token_cache_path
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
    settings = SnapshotSettings()
    credentials = load_kis_data_credentials()
    cred = next((c for c in credentials if c.slot == settings.kis_data_slot), None)
    if cred is None:
        raise MissingCredentialsError(f"no data credential for slot {settings.kis_data_slot}")
    token_settings = KisTokenSettings()
    client = KisRestClient(
        creds=KisCredentials(
            kis_app_key=cred.app_key,
            kis_app_secret=cred.app_secret,
            kis_account_no="",
            kis_account_product_code="",
        ),
        session=requests,
        token_cache_path=kis_token_cache_path(token_settings.token_cache_dir, cred.app_key),
        limiter=RateLimiter(settings.rest_rate_per_s),
        now=lambda: dt.datetime.now(_KST),
        timeout_s=settings.request_timeout_s,
        allow_token_issue=token_settings.allow_issue,
    )
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
    store = SnapshotStore(paths=CollectorSettings().paths, session_date=session_date)
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
