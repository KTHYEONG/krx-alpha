"""collect-aftermarket CLI 서브커맨드 (KIS 애프터마켓 venue 수집 구동)."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import logging
import pathlib
import signal
import time
from typing import Any

import aiohttp

from src.core.config import AftermarketSettings, CollectorSettings, KisCredentials, load_credentials
from src.core.errors import SlotBudgetExceededError
from src.core.observability import EVENT
from src.realtime.adapters.kis import KisRealtimeAdapter
from src.realtime.contracts import MarketSession, MarketVenue
from src.realtime.session import SessionConfig, StreamRoute, bootstrap_session
from src.realtime.streamer import RealtimeStreamer, SessionFrameSink, aftermarket_silence_limit_s

logger = logging.getLogger(__name__)


def _route_for_venue(venue: str) -> StreamRoute:
    v = venue.lower()
    return StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER) if v == "nxt" else StreamRoute(MarketVenue.KRX, MarketSession.KRX_AFTER)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """'collect-aftermarket' 서브커맨드를 등록한다."""
    parser = subparsers.add_parser("collect-aftermarket")
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--journal-root", required=True)
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--candidates-path", required=True)
    parser.add_argument("--venue", required=True, choices=["krx", "nxt"])
    parser.add_argument("--ntp-host", default="kr.pool.ntp.org")
    parser.add_argument("--max-clock-offset-ns", type=int, default=2_000_000_000)
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--degraded-reason", default=None)
    parser.set_defaults(handler=lambda args: asyncio.run(_run_stream(args)))


async def _run_stream(args: argparse.Namespace) -> int:
    settings = CollectorSettings()
    after = AftermarketSettings(enabled=settings.after_market_enabled)
    route = _route_for_venue(str(args.venue))
    streams: tuple[str, str] = after.nxt_streams if route.venue == MarketVenue.NXT else after.krx_streams
    creds = load_credentials(KisCredentials)
    app_key = getattr(creds, "kis_app_key", "k")
    app_secret = getattr(creds, "kis_app_secret", "s")
    cfg = SessionConfig(
        session_date=dt.date.fromisoformat(str(args.session_date)),
        journal_root=pathlib.Path(str(args.journal_root)),
        manifest_path=pathlib.Path(str(args.manifest_path)),
        candidates_path=pathlib.Path(str(args.candidates_path)),
        ntp_host=str(args.ntp_host),
        slot_budget=settings.subscription_pair_budget,
        max_clock_offset_ns=int(args.max_clock_offset_ns),
        desired_streams=tuple(streams),
        vendor="kis",
        degraded_reason=getattr(args, "degraded_reason", None),
        ntp_fallback_hosts=settings.ntp_fallback_hosts,
        route=route,
    )
    session = bootstrap_session(cfg)
    pairs = session.replay_pairs()
    capacity = after.pair_capacity_per_connection
    if capacity is None:
        raise SlotBudgetExceededError("aftermarket pair capacity is not configured")
    if len(pairs) > capacity: raise SlotBudgetExceededError(f"planned pairs {len(pairs)} exceed capacity {capacity}")  # noqa: E701
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    http = aiohttp.ClientSession()
    try:
        adapter = KisRealtimeAdapter(
            app_key=app_key,
            app_secret=app_secret,
            http=http,
            route=route,
            allowed_streams=streams,
            capacity_pairs=capacity,
        )
        streamer = RealtimeStreamer(
            adapter=adapter,
            sink=SessionFrameSink(session=session),
            replay_pairs=pairs,
            silence_limit=lambda: aftermarket_silence_limit_s(dt.datetime.now(dt.UTC), route=route),
        )
        await streamer.run_forever(stop, max_cycles=getattr(args, "max_cycles", None))
    finally:
        await http.close()
    persist_fn: Any = getattr(session, "persist", None)
    if callable(persist_fn):
        persist_fn()
    manifest = getattr(session, "manifest", None)
    if manifest is not None and hasattr(manifest, "writer_closed_at_ns"): manifest.writer_closed_at_ns = time.time_ns()  # noqa: E701
    if manifest is not None and callable(persist_fn): persist_fn()  # noqa: E701
    acks = len(getattr(manifest, "subscription_acks", []) or []) if manifest is not None else 0
    gaps = len(getattr(manifest, "gaps", []) or []) if manifest is not None else 0
    logger.info(
        "[DATA] stage=stream_shutdown manifest=%s venue=%s acks=%d gaps=%d",
        str(cfg.manifest_path),
        route.venue.value,
        acks,
        gaps,
        extra=EVENT,
    )
    return 0
