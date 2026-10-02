"""collect-premarket CLI 서브커맨드 (NXT 프리마켓 단일 샤드 수집 구동)."""

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
from src.core.errors import KrxAlphaError, MissingCredentialsError, SlotBudgetExceededError
from src.core.observability import EVENT
from src.core.session_anchors import resolve_session_anchors
from src.realtime.adapters.kis import KisRealtimeAdapter
from src.realtime.contracts import MarketSession, MarketVenue
from src.realtime.kis_lease import KisWebSocketLease
from src.realtime.kis_sharding import AftermarketShard, load_kis_data_credentials
from src.realtime.session import SessionConfig, StreamRoute, bootstrap_session
from src.realtime.streamer import RealtimeStreamer, SessionFrameSink, premarket_silence_limit_s
from src.universe.ipc import read_candidate_snapshot

logger = logging.getLogger(__name__)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register ``collect-premarket``.

    Required: ``--session-date``, ``--journal-root``, ``--manifest-path``, ``--candidates-path``,
    ``--credential-slot``, ``--credential-key-id``, ``--symbols``.
    Optional: ``--ntp-host``, ``--max-clock-offset-ns``, ``--max-cycles`` (settings-derived defaults are
    None so registration reads no environment). There is deliberately no ``--venue`` or
    ``--shard-index``: the route is fixed to (NXT, NXT_PRE) and the shard index to 0.
    """
    parser = subparsers.add_parser("collect-premarket")
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--journal-root", required=True)
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--candidates-path", required=True)
    parser.add_argument("--credential-slot", required=True)
    parser.add_argument("--credential-key-id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--ntp-host", default=None)
    parser.add_argument("--max-clock-offset-ns", type=int, default=None)
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.set_defaults(handler=lambda args: asyncio.run(_run_stream(args)))


def _deadline_ns(expected_close_ns: int, close_grace_s: float) -> int:
    return expected_close_ns + int(close_grace_s * 1_000_000_000)


async def _run_stream(args: argparse.Namespace) -> int:
    """Collect NXT premarket trades and 10-level quotes for one pre-planned symbol shard.

    Returns:
        0 after a graceful stop (SIGTERM or window end) with the manifest persisted and
        ``writer_closed_at_ns`` stamped; also 0 when started after the window already closed
        (no socket is opened).

    Raises:
        KrxAlphaError: Premarket is disabled, or the credential/symbol/pool contract is violated.
        MissingCredentialsError: Slot fingerprint mismatch or missing credential.
        SlotBudgetExceededError: Planned pairs exceed the configured connection capacity.
        ClockUnsyncedError: NTP offset above the configured ceiling (fail-closed, no data written).
        StorageExhaustedError: Free disk below the premarket watermark (this child stops; others keep running).
    """
    runtime = resolve_collector_runtime()
    settings = runtime.collector
    premarket = runtime.premarket
    if not premarket.enabled:
        raise KrxAlphaError("premarket collection is disabled")
    route = StreamRoute(MarketVenue.NXT, MarketSession.NXT_PRE)
    streams: tuple[str, str] = tuple(premarket.nxt_streams)  # type: ignore[assignment]
    credentials = load_kis_data_credentials()
    slot = str(args.credential_slot)
    key_id = str(args.credential_key_id)
    cred = next((c for c in credentials if c.slot == slot), None)
    if cred is None or cred.key_id != key_id:
        raise MissingCredentialsError(f"credential fingerprint mismatch for slot {slot}")
    symbols = tuple(part for part in str(args.symbols).split(",") if part)
    if not symbols:
        raise KrxAlphaError(f"premarket symbols must be non-empty for slot {slot}")
    session_date = dt.date.fromisoformat(str(args.session_date))
    snapshot = read_candidate_snapshot(
        pathlib.Path(str(args.candidates_path)),
        expected_session_date=session_date,
        expected_session="premarket",
        max_candidates=premarket.max_symbols,
    )
    rows: list[dict[str, Any]] = list(snapshot.candidates)
    universe = {str(row["symbol"]) for row in rows}
    expected_symbols = tuple(str(row["symbol"]) for row in rows if str(row["symbol"]) in set(symbols))
    if not set(symbols) <= universe:
        raise MissingCredentialsError(f"shard symbols not subset of candidates for slot {slot}")
    if len(set(symbols)) != len(symbols) or symbols != expected_symbols:
        raise MissingCredentialsError(f"shard symbols do not exactly match candidate order for slot {slot}")
    capacity = premarket.pair_capacity_per_connection
    if capacity is None:
        raise SlotBudgetExceededError("premarket pair capacity is not configured")
    pairs_planned = len(symbols) * len(streams)
    if pairs_planned > capacity:
        raise SlotBudgetExceededError(f"planned pairs {pairs_planned} exceed capacity {capacity}")
    anchors = resolve_session_anchors(
        DataPaths(pathlib.Path(str(args.journal_root)).parent).session_calendar_dir, session_date
    )
    raw_ntp_host = getattr(args, "ntp_host", None)
    raw_max_offset = getattr(args, "max_clock_offset_ns", None)
    ntp_host = str(raw_ntp_host) if raw_ntp_host is not None else str(settings.ntp_host)
    max_clock_offset_ns = int(raw_max_offset) if raw_max_offset is not None else int(settings.max_clock_offset_ns)
    shard = AftermarketShard(MarketVenue.NXT, 0, symbols, streams, cred.slot, cred.key_id)
    cfg = SessionConfig(
        session_date=session_date,
        journal_root=pathlib.Path(str(args.journal_root)),
        manifest_path=pathlib.Path(str(args.manifest_path)),
        candidates_path=pathlib.Path(str(args.candidates_path)),
        ntp_host=ntp_host,
        slot_budget=pairs_planned,
        max_clock_offset_ns=max_clock_offset_ns,
        desired_streams=tuple(streams),
        vendor="kis",
        ntp_fallback_hosts=settings.ntp_fallback_hosts,
        route=route,
        shard=shard,
        min_free_disk_gb=float(settings.min_free_disk_gb) + float(premarket.extra_free_disk_gb),
        journal_retain_days=settings.journal_retain_days,
    )
    session = bootstrap_session(cfg)
    manifest = getattr(session, "manifest", None)
    deadline = _deadline_ns(int(getattr(manifest, "expected_close_ns", 0) or 0), float(premarket.close_grace_s))
    if time.time_ns() >= deadline:
        # Nothing was collected: leave the manifest unclosed so EOD reports NOT_CLOSED instead of OK.
        logger.info(
            "[DATA] stage=premarket_stream status=STOP date=%s symbols=%d pairs=%d credential_key_id=%s reason=window_closed",
            session_date.isoformat(),
            len(symbols),
            pairs_planned,
            cred.key_id,
            extra=EVENT,
        )
        return 0
    logger.info(
        "[DATA] stage=premarket_stream status=START date=%s symbols=%d pairs=%d credential_key_id=%s",
        session_date.isoformat(),
        len(symbols),
        pairs_planned,
        cred.key_id,
        extra=EVENT,
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)

    async def _deadline_watch() -> None:
        delay_s = (deadline - time.time_ns()) / 1_000_000_000
        if delay_s > 0:
            await asyncio.sleep(delay_s)
        stop.set()

    deadline_task = asyncio.ensure_future(_deadline_watch())
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
            replay_pairs=session.replay_pairs(),
            silence_limit=lambda: premarket_silence_limit_s(
                dt.datetime.now(dt.UTC), anchors=anchors, limit_s=premarket.silence_limit_s
            ),
        )
        await streamer.run_forever(stop, max_cycles=getattr(args, "max_cycles", None))
    finally:
        deadline_task.cancel()
        await http.close()
    persist_fn2: Any = getattr(session, "persist", None)
    if callable(persist_fn2):
        persist_fn2()
    if manifest is not None and hasattr(manifest, "writer_closed_at_ns"):
        manifest.writer_closed_at_ns = time.time_ns()
    if manifest is not None and callable(persist_fn2):
        persist_fn2()
    logger.info(
        "[DATA] stage=premarket_stream status=STOP date=%s symbols=%d pairs=%d credential_key_id=%s",
        session_date.isoformat(),
        len(symbols),
        pairs_planned,
        cred.key_id,
        extra=EVENT,
    )
    return 0


__all__ = ["add_parser"]
