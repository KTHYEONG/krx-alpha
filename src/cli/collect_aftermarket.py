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

from src.core.config import DataPaths, resolve_collector_runtime
from src.core.errors import MissingCredentialsError, SlotBudgetExceededError
from src.core.observability import EVENT
from src.realtime.adapters.kis import KisRealtimeAdapter
from src.realtime.contracts import MarketSession, MarketVenue
from src.realtime.kis_lease import KisWebSocketLease
from src.realtime.kis_sharding import AftermarketShard, load_kis_data_credentials
from src.realtime.session import SessionConfig, StreamRoute, bootstrap_session
from src.realtime.streamer import RealtimeStreamer, SessionFrameSink, aftermarket_silence_limit_s
from src.universe.ipc import read_candidate_snapshot

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
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--credential-slot", required=True)
    parser.add_argument("--credential-key-id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--ntp-host", default="kr.pool.ntp.org")
    parser.add_argument("--max-clock-offset-ns", type=int, default=2_000_000_000)
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--degraded-reason", default=None)
    parser.set_defaults(handler=lambda args: asyncio.run(_run_stream(args)))


async def _run_stream(args: argparse.Namespace) -> int:
    runtime = resolve_collector_runtime()
    settings = runtime.collector
    after = runtime.aftermarket
    route = _route_for_venue(str(args.venue))
    streams: tuple[str, str] = after.nxt_streams if route.venue == MarketVenue.NXT else after.krx_streams
    credentials = load_kis_data_credentials()
    slot = str(args.credential_slot)
    key_id = str(args.credential_key_id)
    cred = next((c for c in credentials if c.slot == slot), None)
    if cred is None or cred.key_id != key_id:
        raise MissingCredentialsError(f"credential fingerprint mismatch for slot {slot}")
    symbols = tuple(part for part in str(args.symbols).split(",") if part)
    snapshot = read_candidate_snapshot(pathlib.Path(str(args.candidates_path)), expected_session_date=dt.date.fromisoformat(str(args.session_date)), expected_session="aftermarket", max_candidates=after.max_symbols)
    rows: list[dict[str, Any]] = list(snapshot.candidates)
    universe = {str(row["symbol"]) for row in rows}
    expected_symbols = tuple(str(row["symbol"]) for row in rows if str(row["symbol"]) in set(symbols))
    if not set(symbols) <= universe:
        raise MissingCredentialsError(f"shard symbols not subset of candidates for slot {slot}")
    if len(set(symbols)) != len(symbols) or symbols != expected_symbols:
        raise MissingCredentialsError(f"shard symbols do not exactly match candidate order for slot {slot}")
    shard = AftermarketShard(route.venue, int(args.shard_index), symbols, streams, cred.slot, cred.key_id)
    cfg = SessionConfig(
        session_date=dt.date.fromisoformat(str(args.session_date)),
        journal_root=pathlib.Path(str(args.journal_root)),
        manifest_path=pathlib.Path(str(args.manifest_path)),
        candidates_path=pathlib.Path(str(args.candidates_path)),
        ntp_host=str(args.ntp_host),
        slot_budget=len(symbols) * len(streams),
        max_clock_offset_ns=int(args.max_clock_offset_ns),
        desired_streams=tuple(streams),
        vendor="kis",
        degraded_reason=getattr(args, "degraded_reason", None),
        ntp_fallback_hosts=settings.ntp_fallback_hosts,
        route=route,
        shard=shard,
        min_free_disk_gb=settings.min_free_disk_gb,
        journal_retain_days=settings.journal_retain_days,
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
        lease = KisWebSocketLease(
            root=DataPaths(pathlib.Path(str(args.journal_root)).parent).kis_ws_lease_dir,
            credential_key_id=cred.key_id,
        )
        adapter = KisRealtimeAdapter(
            app_key=cred.app_key,
            app_secret=cred.app_secret,
            http=http,
            route=route,
            allowed_streams=streams,
            capacity_pairs=capacity,
            lease=lease,
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
