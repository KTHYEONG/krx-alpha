"""collect-stream-db CLI 서브커맨드 (DB 크로스체크 정규장 스트리머)."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import logging
import pathlib
import signal

import aiohttp

from src.core.config import CollectorSettings, DbCredentials, KisTokenSettings, load_credentials
from src.core.observability import EVENT
from src.core.session_anchors import resolve_session_anchors
from src.marketdata.toss_token_store import TossTokenStore, db_token_path
from src.realtime.adapters.db import DbRealtimeAdapter
from src.realtime.contracts import MarketSession, MarketVenue
from src.realtime.session import SessionConfig, StreamRoute, bootstrap_session
from src.realtime.streamer import RealtimeStreamer, SessionFrameSink, regular_session_silence_limit_s

logger = logging.getLogger(__name__)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register ``collect-stream-db`` with the same arguments as ``collect-stream`` minus ``--market-map``."""
    parser = subparsers.add_parser("collect-stream-db")
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
    """Run the DB crosscheck regular-session streamer and return the exit code."""
    return asyncio.run(_run_stream(args))


async def _run_stream(args: argparse.Namespace) -> int:
    settings = CollectorSettings()
    if not settings.db_crosscheck_enabled:
        logger.info("[DATA] stage=db_crosscheck status=SKIP reason=disabled")
        return 0
    creds = load_credentials(DbCredentials)
    session_date = dt.date.fromisoformat(str(args.session_date))
    anchors = resolve_session_anchors(settings.paths.session_calendar_dir, session_date)
    raw_ntp_host = getattr(args, "ntp_host", None)
    raw_max_offset = getattr(args, "max_clock_offset_ns", None)
    ntp_host = str(raw_ntp_host) if raw_ntp_host is not None else settings.ntp_host
    max_clock_offset_ns = int(raw_max_offset) if raw_max_offset is not None else settings.max_clock_offset_ns
    cfg = SessionConfig(
        session_date=session_date,
        journal_root=pathlib.Path(str(args.journal_root)),
        manifest_path=pathlib.Path(str(args.manifest_path)),
        candidates_path=pathlib.Path(str(args.candidates_path)),
        ntp_host=ntp_host,
        slot_budget=settings.db_capacity_pairs,
        max_clock_offset_ns=max_clock_offset_ns,
        desired_streams=settings.streams,
        vendor="db",
        symbol_limit=settings.db_crosscheck_symbol_budget,
        route=StreamRoute(MarketVenue.KRX, MarketSession.REGULAR),
        degraded_reason=getattr(args, "degraded_reason", None),
        ntp_fallback_hosts=settings.ntp_fallback_hosts,
        min_free_disk_gb=settings.min_free_disk_gb,
        journal_retain_days=settings.journal_retain_days,
    )
    session = bootstrap_session(cfg)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    http = aiohttp.ClientSession()
    try:
        token_store = TossTokenStore(
            db_token_path(KisTokenSettings().token_cache_dir, creds.db_app_key),
            rotation_cooldown_s=settings.db_token_rotation_cooldown_s,
        )
        adapter = DbRealtimeAdapter(
            app_key=creds.db_app_key,
            app_secret=creds.db_app_secret,
            http=http,
            token_store=token_store,
            capacity_pairs=settings.db_capacity_pairs,
            ack_timeout_s=settings.db_ack_timeout_s,
            subscribe_spacing_s=settings.db_subscribe_spacing_s,
        )
        streamer = RealtimeStreamer(
            adapter=adapter,
            sink=SessionFrameSink(session=session),
            replay_pairs=session.replay_pairs(),
            silence_limit=lambda: regular_session_silence_limit_s(dt.datetime.now(dt.UTC), anchors=anchors),
        )
        await streamer.run_forever(stop, max_cycles=args.max_cycles)
    finally:
        await http.close()
    session.persist()
    logger.info(
        "[DATA] stage=stream_shutdown vendor=db manifest=%s acks=%d gaps=%d boots=%d",
        str(cfg.manifest_path),
        len(session.manifest.subscription_acks),
        len(session.manifest.gaps),
        len(session.manifest.boots),
        extra=EVENT,
    )
    return 0
