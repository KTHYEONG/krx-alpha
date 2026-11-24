"""collect-regular-kis CLI 서브커맨드 (KRX 정규장 단일 KIS 샤드 수집 구동)."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import logging
import pathlib
import time
from typing import Any, cast

from src.core.config import DataPaths, FeedControlSettings, IntradayAdmissionSettings, resolve_collector_runtime
from src.core.errors import KrxAlphaError, MissingCredentialsError, SlotBudgetExceededError
from src.core.observability import EVENT
from src.core.session_anchors import resolve_session_anchors
from src.marketdata.toss_calendar import load_trading_day_cache, trading_day_from_cache
from src.orchestration.regular_kis import ls_shard_blocks, prior_ls_frame_loads, regular_kis_ls_plan_inputs
from src.realtime.adapters.kis import KisRealtimeAdapter
from src.realtime.contracts import MarketSession, MarketVenue
from src.realtime.feed_health import FeedHealthConfig, build_feed_health_tracker, feed_id_for
from src.realtime.kis_lease import KisWebSocketLease
from src.realtime.kis_sharding import AftermarketShard, load_kis_data_credentials, plan_regular_kis_shards
from src.realtime.session import DynamicFeed, DynamicPairWatcher, SessionConfig, StreamRoute, bootstrap_session
from src.realtime.streamer import RealtimeStreamer, regular_kis_silence_limit_s
from src.realtime.streaming_child import resolve_clock_overrides, run_streaming_child
from src.universe.intraday import read_dynamic_admissions
from src.universe.ipc import CandidateFileError, read_candidate_snapshot, read_candidates

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
    parser.add_argument("--venue", choices=["krx", "nxt"], default="krx")
    parser.add_argument("--ntp-host", default=None)
    parser.add_argument("--max-clock-offset-ns", type=int, default=None)
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--restart-reason", choices=["stale_child"], default=None)
    parser.set_defaults(handler=lambda args: asyncio.run(_run_stream(args)))
    dynamic = subparsers.add_parser("collect-regular-kis-dynamic")
    dynamic.add_argument("--session-date", required=True)
    dynamic.add_argument("--journal-root", required=True)
    dynamic.add_argument("--manifest-path", required=True)
    dynamic.add_argument("--credential-slot", required=True)
    dynamic.add_argument("--credential-key-id", required=True)
    dynamic.add_argument("--ntp-host", default=None)
    dynamic.add_argument("--max-clock-offset-ns", type=int, default=None)
    dynamic.add_argument("--max-cycles", type=int, default=None)
    dynamic.add_argument("--venue", choices=["krx", "nxt"], default="krx")
    dynamic.set_defaults(handler=lambda args: asyncio.run(_run_dynamic_stream(args)))


def _deadline_ns(expected_close_ns: int, close_grace_s: float) -> int:
    return expected_close_ns + int(close_grace_s * 1_000_000_000)


def _nxt_candidate_symbols(
    candidates_path: pathlib.Path, session_date: dt.date, max_candidates: int
) -> tuple[str, ...]:
    try:
        snapshot = read_candidate_snapshot(
            candidates_path,
            expected_session_date=session_date,
            expected_session="regular_nxt",
            max_candidates=max_candidates,
        )
    except CandidateFileError as exc:
        raise KrxAlphaError(str(exc)) from exc
    ordered = sorted(snapshot.candidates, key=lambda row: int(cast(int, row["rank"])))
    return tuple(str(row["symbol"]) for row in ordered)


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
    venue = MarketVenue(str(getattr(args, "venue", "krx")))
    route = StreamRoute(venue, MarketSession.REGULAR)
    streams: tuple[str, str] = tuple(regular_kis.nxt_streams if venue is MarketVenue.NXT else regular_kis.streams)  # type: ignore[assignment]
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
    anchors = resolve_session_anchors(paths.session_calendar_dir, session_date)
    capacity = regular_kis.pair_capacity_per_connection
    if capacity is None:
        raise SlotBudgetExceededError("regular_kis pair capacity is not configured")
    if venue is MarketVenue.NXT:
        if not regular_kis.nxt_credential_slots:
            raise KrxAlphaError("nxt regular collection has no configured credential slots")
        candidates_order = _nxt_candidate_symbols(
            pathlib.Path(str(args.candidates_path)), session_date, regular_kis.nxt_max_symbols
        )
        plan_slots = tuple(regular_kis.nxt_credential_slots)
    else:
        candidates_order = _candidate_symbols(pathlib.Path(str(args.candidates_path)), session_date, paths)
        plan_slots = tuple(regular_kis.credential_slots)
    slot_budget = len(symbols) * len(streams)
    if slot_budget > capacity:
        raise SlotBudgetExceededError(f"planned pairs {slot_budget} exceed capacity {capacity}")
    ls_covered: frozenset[str] | None = None
    ls_blocks: tuple[tuple[str, ...], ...] | None = None
    if venue is MarketVenue.KRX:
        prior = prior_ls_frame_loads(paths, session_date)
        blocks = ls_shard_blocks(
            pathlib.Path(str(args.candidates_path)), settings, loads=prior[1] if prior is not None else None
        )
        if blocks:
            ls_covered, ls_blocks = regular_kis_ls_plan_inputs(blocks)
    plan = plan_regular_kis_shards(
        symbols=candidates_order,
        credentials=credentials,
        credential_slots=plan_slots,
        pair_capacity_per_connection=capacity,
        streams=streams,
        ls_covered_symbols=ls_covered,
        ls_shard_symbols=ls_blocks,
        log_coverage=False,
        venue=venue,
    )
    shard_index = int(args.shard_index)
    expected = next((shard for shard in plan if shard.shard_index == shard_index), None)
    if expected is None or symbols != expected.symbols:
        raise MissingCredentialsError(f"shard symbols do not match the regular_kis plan for shard {shard_index}")
    if slot != expected.credential_slot or key_id != expected.credential_key_id:
        raise MissingCredentialsError(f"credential fingerprint mismatch for slot {slot}")
    overrides = resolve_clock_overrides(getattr(args, "ntp_host", None), getattr(args, "max_clock_offset_ns", None), settings)
    if not 0 < overrides.max_clock_offset_ns <= 2_000_000_000:
        raise ValueError("max_clock_offset_ns must be positive and at most 2 seconds")
    shard = AftermarketShard(venue, shard_index, symbols, streams, cred.slot, cred.key_id)
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

    feed_id = feed_id_for("kis", venue.value, shard_index)
    health_tracker = build_feed_health_tracker(
        feed_id=feed_id, vendor="kis", venue=venue.value, session="regular", session_date=session_date
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
        feed_health=FeedHealthConfig(
            tracker=health_tracker,
            path=paths.feed_health_path(feed_id),
            interval_s=settings.feed_health_interval_s,
        ),
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


async def _run_dynamic_stream(args: argparse.Namespace) -> int:
    """Collect the regular trades the intraday plan assigns to one KIS dynamic slot and venue until window end.

    Returns:
        0 after a graceful stop, or when started after the window already closed (no socket opened).

    Raises:
        KrxAlphaError: Neither intraday admission nor this slot's failover secondary is enabled, regular KIS is
            disabled, the slot is a regular credential slot, or NXT is requested on a shifted session.
        MissingCredentialsError: Dynamic slot credential fingerprint mismatch.
    """
    runtime = resolve_collector_runtime()
    settings = runtime.collector
    regular_kis = runtime.regular_kis
    intraday = IntradayAdmissionSettings()
    slot = str(args.credential_slot)
    if not (intraday.enabled or f"kis:{slot}" in FeedControlSettings().secondary_feeds):
        raise KrxAlphaError("intraday admission is disabled")
    if not regular_kis.enabled:
        raise KrxAlphaError("regular_kis collection is disabled")
    if slot not in intraday.kis_dynamic_slots:
        raise KrxAlphaError(f"credential slot {slot} is not a configured kis_dynamic_slots entry")
    if slot in regular_kis.credential_slots:
        raise KrxAlphaError(f"kis dynamic slot {slot} must not be a regular_kis credential slot")
    cred = next((c for c in load_kis_data_credentials() if c.slot == slot), None)
    if cred is None or cred.key_id != str(args.credential_key_id):
        raise MissingCredentialsError(f"credential fingerprint mismatch for slot {slot}")
    venue = MarketVenue(str(getattr(args, "venue", "krx")))
    capacity = int(regular_kis.pair_capacity_per_connection or 0)
    streams: tuple[str, str] = tuple(
        regular_kis.nxt_streams if venue is MarketVenue.NXT else regular_kis.streams
    )  # type: ignore[assignment]
    route = StreamRoute(venue, MarketSession.REGULAR)
    session_date = dt.date.fromisoformat(str(args.session_date))
    paths = DataPaths(pathlib.Path(str(args.journal_root)).parent)
    anchors = resolve_session_anchors(paths.session_calendar_dir, session_date)
    overrides = resolve_clock_overrides(getattr(args, "ntp_host", None), getattr(args, "max_clock_offset_ns", None), settings)
    if not 0 < overrides.max_clock_offset_ns <= 2_000_000_000:
        raise ValueError("max_clock_offset_ns must be positive and at most 2 seconds")
    feed = f"kis:{slot}"
    ipc_path = paths.dynamic_admissions(session_date)
    initial = [
        (symbol, stream)
        for symbol in dict.fromkeys(read_dynamic_admissions(ipc_path).pairs_for(feed, venue.value))
        for stream in streams
    ]
    shard = AftermarketShard(venue, int(slot), (), streams, cred.slot, cred.key_id)
    cfg = SessionConfig(
        session_date=session_date,
        journal_root=pathlib.Path(str(args.journal_root)),
        manifest_path=pathlib.Path(str(args.manifest_path)),
        candidates_path=paths.regular_nxt_candidates(session_date) if venue is MarketVenue.NXT else paths.candidates,
        ntp_host=overrides.ntp_host,
        slot_budget=capacity,
        max_clock_offset_ns=overrides.max_clock_offset_ns,
        desired_streams=streams,
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
            "[DATA] stage=regular_kis_dynamic status=STOP date=%s slot=%s reason=window_closed",
            session_date.isoformat(),
            slot,
            extra=EVENT,
        )
        return 0
    feed_id = feed_id_for("kis", MarketVenue.KRX.value, 0, slot=slot)
    health_tracker = build_feed_health_tracker(
        feed_id=feed_id, vendor="kis", venue=MarketVenue.KRX.value, session="regular", session_date=session_date
    )
    watch = DynamicPairWatcher(
        feed=DynamicFeed(
            vendor="kis",
            feed=feed,
            streams=streams,
            ipc_path=ipc_path,
            membership_path=paths.membership_path(session_date),
            kis_slot=slot,
            venue=venue.value,
        ),
        session=session,
        static_pairs=(),
        initial_dynamic=initial,
        observer=health_tracker,
    )
    logger.info(
        "[DATA] stage=regular_kis_dynamic status=START date=%s slot=%s initial_pairs=%d credential_key_id=%s",
        session_date.isoformat(),
        slot,
        len(initial),
        cred.key_id,
        extra=EVENT,
    )

    def _adapter_factory(http: Any) -> Any:
        lease = KisWebSocketLease(root=paths.kis_ws_lease_dir, credential_key_id=cred.key_id)
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
        replay_pairs=watch.pairs(),
        live_watch=watch,
        silence_limit=lambda: regular_kis_silence_limit_s(
            dt.datetime.now(dt.UTC), anchors=anchors, limit_s=regular_kis.silence_limit_s
        ),
        max_cycles=getattr(args, "max_cycles", None),
        streamer_factory=RealtimeStreamer,
        stop_deadline_ns=deadline,
        close_writer=True,
        feed_health=FeedHealthConfig(
            tracker=health_tracker,
            path=paths.feed_health_path(feed_id),
            interval_s=settings.feed_health_interval_s,
        ),
    )
    return 0


__all__ = ["add_parser"]
