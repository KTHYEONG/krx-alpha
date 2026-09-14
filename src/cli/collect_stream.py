"""collect-stream CLI 서브커맨드 (LS 실시간 수집 구동)."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import logging
import pathlib
import signal

import aiohttp

from src.core.config import CollectorSettings, LsCredentials, load_credentials
from src.core.observability import EVENT
from src.realtime.adapters.ls import LsRealtimeAdapter
from src.realtime.session import SessionConfig, bootstrap_session
from src.realtime.streamer import RealtimeStreamer, SessionFrameSink, regular_session_silence_limit_s

logger = logging.getLogger(__name__)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """'collect-stream' 서브커맨드를 등록한다."""
    parser = subparsers.add_parser("collect-stream")
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--journal-root", required=True)
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--candidates-path", required=True)
    parser.add_argument("--market-map", required=True)
    parser.add_argument("--ntp-host", default="kr.pool.ntp.org")
    parser.add_argument("--max-clock-offset-ns", type=int, default=2_000_000_000)
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--degraded-reason", default=None)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """스트리머를 구동하고 종료 코드를 반환한다."""
    return asyncio.run(_run_stream(args))


async def _run_stream(args: argparse.Namespace) -> int:
    settings = CollectorSettings()
    creds = load_credentials(LsCredentials)
    cfg = SessionConfig(
        session_date=dt.date.fromisoformat(str(args.session_date)),
        journal_root=pathlib.Path(str(args.journal_root)),
        manifest_path=pathlib.Path(str(args.manifest_path)),
        candidates_path=pathlib.Path(str(args.candidates_path)),
        ntp_host=str(args.ntp_host),
        slot_budget=settings.subscription_pair_budget,  # SubscriptionRegistry 는 (symbol, tr_id) 쌍을 계수한다
        max_clock_offset_ns=int(args.max_clock_offset_ns),
        desired_streams=settings.streams,
        vendor=settings.vendor,
        degraded_reason=getattr(args, "degraded_reason", None),
        ntp_fallback_hosts=settings.ntp_fallback_hosts,
    )
    session = bootstrap_session(cfg)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    market_of = json.loads(pathlib.Path(str(args.market_map)).read_text(encoding="utf-8"))  # noqa: ASYNC240 - one-shot startup read
    http = aiohttp.ClientSession()
    try:
        adapter = LsRealtimeAdapter(
            app_key=creds.ls_app_key,
            app_secret=creds.ls_app_secret,
            http=http,
            market_of=market_of,
        )
        streamer = RealtimeStreamer(
            adapter=adapter,
            sink=SessionFrameSink(session=session),
            replay_pairs=session.replay_pairs(),
            silence_limit=lambda: regular_session_silence_limit_s(dt.datetime.now(dt.UTC)),
        )
        await streamer.run_forever(stop, max_cycles=args.max_cycles)
    finally:
        await http.close()
    session.persist()
    logger.info(
        "[DATA] stage=stream_shutdown manifest=%s acks=%d gaps=%d boots=%d",
        str(cfg.manifest_path),
        len(session.manifest.subscription_acks),
        len(session.manifest.gaps),
        len(session.manifest.boots),
        extra=EVENT,
    )
    return 0
