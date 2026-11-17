"""collect-stream-toss CLI 서브커맨드 (토스증권 통합 체결 워밍 세컨더리 스트리머)."""

from __future__ import annotations

import argparse
import asyncio
import logging

from src.cli import collect_stream
from src.cli.collect_stream import RegularStreamProfile, vendor_streams
from src.core.config import CollectorSettings
from src.core.errors import KrxAlphaError
from src.core.observability import EVENT
from src.realtime.contracts import MarketSession, MarketVenue
from src.realtime.session import StreamRoute, bootstrap_session
from src.realtime.streamer import RealtimeStreamer
from src.storage.layout import L0_VENDOR_TOSS

logger = logging.getLogger(__name__)

_TOSS_SESSION_INDEXES = (0, 1)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register ``collect-stream-toss``: the DB warm-child arguments with a Toss session index."""
    parser = subparsers.add_parser("collect-stream-toss")
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--journal-root", required=True)
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--candidates-path", required=True)
    parser.add_argument("--dynamic-plan-path", required=True)
    parser.add_argument("--toss-session-index", type=int, choices=_TOSS_SESSION_INDEXES, default=0)
    parser.add_argument("--hot-symbols", type=int, default=0, help="top-priority symbols subscribed at start; 0 = warm standby")
    parser.add_argument("--ntp-host", default=None)
    parser.add_argument("--max-clock-offset-ns", type=int, default=None)
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--degraded-reason", default=None)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """Run the Toss warm-standby streamer and return the exit code (SHARD_DENIED_EXIT when the connection slot is taken)."""
    return asyncio.run(_run_stream(args))


async def _run_stream(args: argparse.Namespace) -> int:
    settings = CollectorSettings()
    hot = int(args.hot_symbols)
    streams = vendor_streams(settings, L0_VENDOR_TOSS)
    if hot < 0 or hot * len(streams) > settings.toss_capacity_pairs:
        raise KrxAlphaError(
            f"hot-symbols {hot} x toss_streams {len(streams)} exceeds toss_capacity_pairs {settings.toss_capacity_pairs}"
        )
    profile = RegularStreamProfile(
        vendor=L0_VENDOR_TOSS,
        slot_budget=settings.toss_capacity_pairs,
        # Bootstrap requires a positive limit; a zero-hot standby subscribes nothing statically.
        symbol_limit=max(hot, 1),
        # Toss trade:kr merges KRX and NXT prints with no venue field, so its journal partition says so.
        route=StreamRoute(MarketVenue.UNIFIED, MarketSession.REGULAR),
        shutdown_log="[DATA] stage=stream_shutdown vendor=toss manifest=%s acks=%d gaps=%d boots=%d",
        session_index=int(args.toss_session_index),
        warm_standby=True,
        hot_symbols=hot,
    )
    logger.info(
        "[DATA] stage=toss_child status=START session=%d hot_symbols=%d", profile.session_index, hot, extra=EVENT
    )
    return await collect_stream.run_regular_stream(
        args, settings, profile, bootstrap=bootstrap_session, streamer_factory=RealtimeStreamer
    )
