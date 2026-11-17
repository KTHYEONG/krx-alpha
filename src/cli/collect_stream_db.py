"""collect-stream-db CLI 서브커맨드 (DB 증권 워밍 세컨더리 스트리머)."""

from __future__ import annotations

import argparse
import asyncio
import logging

from src.cli import collect_stream
from src.cli.collect_stream import RegularStreamProfile
from src.core.config import CollectorSettings
from src.core.errors import KrxAlphaError
from src.core.observability import EVENT
from src.realtime.contracts import MarketSession, MarketVenue
from src.realtime.session import StreamRoute, bootstrap_session
from src.realtime.streamer import RealtimeStreamer
from src.storage.layout import L0_VENDOR_DB

logger = logging.getLogger(__name__)

_DB_SESSION_INDEXES = (0, 1)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register ``collect-stream-db``: the LS collect-stream arguments minus ``--market-map``, plus the warm-standby knobs."""
    parser = subparsers.add_parser("collect-stream-db")
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--journal-root", required=True)
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--candidates-path", required=True)
    parser.add_argument("--dynamic-plan-path", required=True)
    parser.add_argument("--db-session-index", type=int, choices=_DB_SESSION_INDEXES, default=0)
    parser.add_argument("--hot-symbols", type=int, default=0, help="top-priority symbols subscribed at start; 0 = warm standby")
    parser.add_argument("--ntp-host", default=None)
    parser.add_argument("--max-clock-offset-ns", type=int, default=None)
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--degraded-reason", default=None)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """Run the DB warm-standby streamer and return the exit code (SHARD_DENIED_EXIT when the connection slot is taken)."""
    return asyncio.run(_run_stream(args))


async def _run_stream(args: argparse.Namespace) -> int:
    settings = CollectorSettings()
    hot = int(args.hot_symbols)
    if hot < 0 or hot * len(settings.streams) > settings.db_capacity_pairs:
        raise KrxAlphaError(
            f"hot-symbols {hot} x streams {len(settings.streams)} exceeds db_capacity_pairs {settings.db_capacity_pairs}"
        )
    profile = RegularStreamProfile(
        vendor=L0_VENDOR_DB,
        slot_budget=settings.db_capacity_pairs,
        # Bootstrap requires a positive limit; a zero-hot standby subscribes nothing statically.
        symbol_limit=max(hot, 1),
        route=StreamRoute(MarketVenue.KRX, MarketSession.REGULAR),
        shutdown_log="[DATA] stage=stream_shutdown vendor=db manifest=%s acks=%d gaps=%d boots=%d",
        session_index=int(args.db_session_index),
        warm_standby=True,
        hot_symbols=hot,
    )
    logger.info(
        "[DATA] stage=db_child status=START session=%d hot_symbols=%d", profile.session_index, hot, extra=EVENT
    )
    return await collect_stream.run_regular_stream(
        args, settings, profile, bootstrap=bootstrap_session, streamer_factory=RealtimeStreamer
    )
