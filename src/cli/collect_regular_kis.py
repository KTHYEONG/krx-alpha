"""collect-regular-kis CLI 서브커맨드 (KRX 정규장 단일 KIS 샤드 수집 구동)."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import logging
import pathlib
import time
from typing import Any, cast

from src.core.config import DataPaths, resolve_collector_runtime
from src.core.errors import KrxAlphaError, MissingCredentialsError, SlotBudgetExceededError
from src.core.observability import EVENT
from src.core.session_anchors import resolve_session_anchors
from src.marketdata.toss_calendar import load_trading_day_cache, trading_day_from_cache
from src.realtime.adapters.kis import KisRealtimeAdapter
from src.realtime.contracts import MarketSession, MarketVenue
from src.realtime.kis_lease import KisWebSocketLease
from src.realtime.kis_sharding import AftermarketShard, load_kis_data_credentials, plan_regular_kis_shards
from src.realtime.session import SessionConfig, StreamRoute, bootstrap_session
from src.realtime.streamer import RealtimeStreamer, regular_kis_silence_limit_s
from src.realtime.streaming_child import resolve_clock_overrides, run_streaming_child
from src.universe.ipc import read_candidates

logger = logging.getLogger(__name__)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register ``collect-regular-kis``."""
    parser = subparsers.add_parser("collect-regular-kis")
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--journal-root", required=True)
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--candidates-path", required=True)
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--credential-slot", required=True)
    parser.add_argument("--credential-key-id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--ntp-host", default=None)
    parser.add_argument("--max-clock-offset-ns", type=int, default=None)
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--restart-reason", choices=["stale_child"], default=None)
    parser.set_defaults(handler=lambda args: asyncio.run(_run_stream(args)))


def _deadline_ns(expected_close_ns: int, close_grace_s: float) -> int:
    return expected_close_ns + int(close_grace_s * 1_000_000_000)


def _candidate_symbols(
    candidates_path: pathlib.Path, session_date: dt.date, paths: DataPaths
) -> tuple[str, ...]:
    stored = read_candidates(candidates_path)
    if stored is None:
        raise KrxAlphaError(f"missing candidates file: {candidates_path}")
    rev = stored.get("rev")
    rows = stored.get("candidates")
    if not isinstance(rev, int) or not isinstance(rows, list) or not rows:
        raise KrxAlphaError(f"invalid candidates file: {candidates_path}")
    if any(not isinstance(row, dict) or not isinstance(row.get("symbol"), str) or not row["symbol"] for row in rows):
        raise KrxAlphaError(f"invalid candidate symbols: {candidates_path}")
    try:
        rev_date = dt.datetime.strptime(str(rev), "%Y%m%d").date()
    except ValueError as exc:
        raise KrxAlphaError(f"invalid candidates rev {rev!r}") from exc
    cached = load_trading_day_cache(paths.calendar_cache)
    day = trading_day_from_cache(cached, session_date) if cached is not None else None
    if day is None or not day.is_business_day or rev_date != day.previous_business_day:
        raise KrxAlphaError(f"stale candidates rev {rev} for session {session_date.isoformat()}")
    return tuple(str(cast(dict[str, Any], row)["symbol"]) for row in rows)


async def _run_stream(args: argparse.Namespace) -> int:
    """Collect KRX regular trades and 10-level quotes for one planned KIS shard until window end.

    Returns:
        0 after a graceful stop with the manifest persisted and ``writer_closed_at_ns``
        stamped; also 0 when started after the window already closed (no socket opened).

    Raises:
        KrxAlphaError: Regular KIS is disabled, or the credential/symbol/candidates contract is violated.
        MissingCredentialsError: Slot fingerprint mismatch or shard symbols diverge from the plan.
        SlotBudgetExceededError: Planned pairs exceed the configured connection capacity.
        ClockUnsyncedError: NTP offset above the configured ceiling (fail-closed, no data written).
        StorageExhaustedError: Free disk below the watermark (this child stops; others keep running).
    """
    runtime = resolve_collector_runtime()
    settings = runtime.collector
    regular_kis = runtime.regular_kis
    if not regular_kis.enabled:
        raise KrxAlphaError("regular_kis collection is disabled")
    route = StreamRoute(MarketVenue.KRX, MarketSession.REGULAR)
    streams: tuple[str, str] = tuple(regular_kis.streams)  # type: ignore[assignment]
    credentials = load_kis_data_credentials()
    slot = str(args.credential_slot)
    key_id = str(args.credential_key_id)
    cred = next((c for c in credentials if c.slot == slot), None)
    if cred is None or cred.key_id != key_id:
        raise MissingCredentialsError(f"credential fingerprint mismatch for slot {slot}")
    symbols = tuple(part for part in str(args.symbols).split(",") if part)
    if not symbols:
        raise KrxAlphaError(f"regular_kis symbols must be non-empty for slot {slot}")
    session_date = dt.date.fromisoformat(str(args.session_date))
    paths = DataPaths(pathlib.Path(str(args.journal_root)).parent)
    candidates_order = _candidate_symbols(pathlib.Path(str(args.candidates_path)), session_date, paths)
    capacity = regular_kis.pair_capacity_per_connection
    if capacity is None:
        raise SlotBudgetExceededError("regular_kis pair capacity is not configured")
    slot_budget = len(symbols) * len(streams)
    if slot_budget > capacity:
        raise SlotBudgetExceededError(f"planned pairs {slot_budget} exceed capacity {capacity}")
    plan = plan_regular_kis_shards(
        symbols=candidates_order,
        credentials=credentials,
        credential_slots=tuple(regular_kis.credential_slots),
        pair_capacity_per_connection=capacity,
        streams=streams,
    )
    shard_index = int(args.shard_index)
    expected = next((shard for shard in plan if shard.shard_index == shard_index), None)
    if expected is None or symbols != expected.symbols:
        raise MissingCredentialsError(f"shard symbols do not match the regular_kis plan for shard {shard_index}")
    if slot != expected.credential_slot or key_id != expected.credential_key_id:
        raise MissingCredentialsError(f"credential fingerprint mismatch for slot {slot}")
    anchors = resolve_session_anchors(
        DataPaths(pathlib.Path(str(args.journal_root)).parent).session_calendar_dir, session_date
    )
    overrides = resolve_clock_overrides(getattr(args, "ntp_host", None), getattr(args, "max_clock_offset_ns", None), settings)
    if not 0 < overrides.max_clock_offset_ns <= 2_000_000_000:
        raise ValueError("max_clock_offset_ns must be positive and at most 2 seconds")
    shard = AftermarketShard(MarketVenue.KRX, shard_index, symbols, streams, cred.slot, cred.key_id)
    cfg = SessionConfig(
        session_date=session_date,
        journal_root=pathlib.Path(str(args.journal_root)),
        manifest_path=pathlib.Path(str(args.manifest_path)),
        candidates_path=pathlib.Path(str(args.candidates_path)),
        ntp_host=overrides.ntp_host,
        slot_budget=slot_budget,
        max_clock_offset_ns=overrides.max_clock_offset_ns,
        desired_streams=tuple(streams),
        vendor="kis",
        ntp_fallback_hosts=settings.ntp_fallback_hosts,
        route=route,
        shard=shard,
        min_free_disk_gb=float(settings.min_free_disk_gb),
        journal_retain_days=settings.journal_retain_days,
        restart_reason=getattr(args, "restart_reason", None),
    )
    session = bootstrap_session(cfg)
    manifest = getattr(session, "manifest", None)
    deadline = _deadline_ns(int(getattr(manifest, "expected_close_ns", 0) or 0), float(regular_kis.close_grace_s))
    if time.time_ns() >= deadline:
        logger.info(
            "[DATA] stage=regular_kis_stream status=STOP date=%s shard=%d symbols=%d pairs=%d credential_key_id=%s reason=window_closed",
            session_date.isoformat(),
            shard_index,
            len(symbols),
            slot_budget,
            cred.key_id,
            extra=EVENT,
        )
        return 0
    logger.info(
        "[DATA] stage=regular_kis_stream status=START date=%s shard=%d symbols=%d pairs=%d credential_key_id=%s",
        session_date.isoformat(),
        shard_index,
        len(symbols),
        slot_budget,
        cred.key_id,
        extra=EVENT,
    )

    def _adapter_factory(http: Any) -> Any:
        lease = KisWebSocketLease(
            root=DataPaths(pathlib.Path(str(args.journal_root)).parent).kis_ws_lease_dir,
            credential_key_id=cred.key_id,
        )
        return KisRealtimeAdapter(
            app_key=cred.app_key,
            app_secret=cred.app_secret,
            http=http,
            route=route,
            allowed_streams=streams,
            capacity_pairs=capacity,
            lease=lease,
            ack_timeout_s=settings.kis_ack_timeout_s,
        )

    await run_streaming_child(
        session=session,
        adapter_factory=_adapter_factory,
        replay_pairs=session.replay_pairs(),
        silence_limit=lambda: regular_kis_silence_limit_s(
            dt.datetime.now(dt.UTC), anchors=anchors, limit_s=regular_kis.silence_limit_s
        ),
        max_cycles=getattr(args, "max_cycles", None),
        streamer_factory=RealtimeStreamer,
        stop_deadline_ns=deadline,
        close_writer=True,
    )
    logger.info(
        "[DATA] stage=regular_kis_stream status=STOP date=%s shard=%d symbols=%d pairs=%d credential_key_id=%s",
        session_date.isoformat(),
        shard_index,
        len(symbols),
        slot_budget,
        cred.key_id,
        extra=EVENT,
    )
    return 0


__all__ = ["add_parser"]
