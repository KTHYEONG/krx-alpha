"""collect-stream-kiwoom CLI 서브커맨드 (Kiwoom 크로스체크 정규장 스트리머)."""

from __future__ import annotations

import argparse
import asyncio
import logging

from src.cli import collect_stream
from src.cli.collect_stream import RegularStreamProfile
from src.core.config import CollectorSettings
from src.realtime.contracts import MarketSession, MarketVenue
from src.realtime.session import StreamRoute, bootstrap_session
from src.realtime.streamer import RealtimeStreamer
from src.storage.layout import L0_VENDOR_KIWOOM

logger = logging.getLogger(__name__)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register ``collect-stream-kiwoom`` with the same arguments as ``collect-stream-db``."""
    parser = subparsers.add_parser("collect-stream-kiwoom")
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--journal-root", required=True)
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--candidates-path", required=True)
    parser.add_argument("--ntp-host", default=None)
    parser.add_argument("--max-clock-offset-ns", type=int, default=None)
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--degraded-reason", default=None)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """Run the Kiwoom crosscheck regular-session streamer and return the exit code."""
    return asyncio.run(_run_stream(args))


async def _run_stream(args: argparse.Namespace) -> int:
    settings = CollectorSettings()
    if not settings.kiwoom_crosscheck_enabled:
        logger.info("[DATA] stage=kiwoom_crosscheck status=SKIP reason=disabled")
        return 0
    profile = RegularStreamProfile(
        vendor=L0_VENDOR_KIWOOM,
        slot_budget=settings.kiwoom_capacity_pairs,
        symbol_limit=settings.kiwoom_crosscheck_symbol_budget,
        route=StreamRoute(MarketVenue.KRX, MarketSession.REGULAR),
        shutdown_log="[DATA] stage=stream_shutdown vendor=kiwoom manifest=%s acks=%d gaps=%d boots=%d",
    )
    return await collect_stream.run_regular_stream(
        args, settings, profile, bootstrap=bootstrap_session, streamer_factory=RealtimeStreamer
    )
