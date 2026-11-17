"""collect-stream-ls-nxt CLI 서브커맨드 (LS NXT 온디맨드 세컨더리 스트리머)."""

from __future__ import annotations

import argparse
import asyncio
import logging

from src.cli import collect_stream
from src.cli.collect_stream import RegularStreamProfile
from src.core.config import CollectorSettings
from src.core.observability import EVENT
from src.realtime.adapters.registry import LS_NXT_ADAPTER
from src.realtime.contracts import MarketSession, MarketVenue
from src.realtime.session import StreamRoute, bootstrap_session
from src.realtime.streamer import RealtimeStreamer

logger = logging.getLogger(__name__)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register ``collect-stream-ls-nxt``: the warm-child arguments of the DB child without hot symbols or session index."""
    parser = subparsers.add_parser("collect-stream-ls-nxt")
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--journal-root", required=True)
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--candidates-path", required=True)
    parser.add_argument("--dynamic-plan-path", required=True)
    parser.add_argument("--ntp-host", default=None)
    parser.add_argument("--max-clock-offset-ns", type=int, default=None)
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--degraded-reason", default=None)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """Run the LS NXT streamer and return the exit code (SHARD_DENIED_EXIT when no LS session slot is free)."""
    return asyncio.run(_run_stream(args))


async def _run_stream(args: argparse.Namespace) -> int:
    settings = CollectorSettings()
    profile = RegularStreamProfile(
        vendor="ls",
        adapter=LS_NXT_ADAPTER,
        slot_budget=settings.ls_max_subscriptions_per_session,
        # Bootstrap requires a positive limit; the child subscribes nothing statically.
        symbol_limit=1,
        route=StreamRoute(MarketVenue.NXT, MarketSession.REGULAR),
        shutdown_log="[DATA] stage=stream_shutdown vendor=ls-nxt manifest=%s acks=%d gaps=%d boots=%d",
        session_index=0,
        warm_standby=True,
        hot_symbols=0,
    )
    logger.info("[DATA] stage=ls_nxt_child status=START session=%d", profile.session_index, extra=EVENT)
    return await collect_stream.run_regular_stream(
        args, settings, profile, bootstrap=bootstrap_session, streamer_factory=RealtimeStreamer
    )
