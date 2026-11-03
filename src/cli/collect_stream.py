"""collect-stream CLI 서브커맨드 (LS 실시간 수집 구동)."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import hashlib
import json
import logging
import os
import pathlib
import re
import time
from collections.abc import Callable
from dataclasses import dataclass

from src.core.config import AuditSettings, CollectorSettings, IntradayAdmissionSettings, KisTokenSettings
from src.core.errors import KrxAlphaError
from src.core.observability import EVENT
from src.core.session_anchors import resolve_session_anchors
from src.realtime.adapters.registry import RegularAdapterContext, regular_vendor
from src.realtime.connection_handover import SwapRequest
from src.realtime.lag_monitor import LagMonitor
from src.realtime.ls_limits import LsGuardConfig
from src.realtime.regular_open_watch import RegularOpenWatch
from src.realtime.session import (
    CollectorSession,
    DynamicFeed,
    DynamicPairWatcher,
    SessionConfig,
    StreamRoute,
    bootstrap_session,
)
from src.realtime.streamer import SILENCE_LIMIT_S, RealtimeStreamer
from src.realtime.streaming_child import HandoverSettings, resolve_clock_overrides, run_streaming_child
from src.realtime.ws_lease import Lease, LeaseRole, LeaseUnavailable, WsLeaseLedger
from src.universe.intraday import read_dynamic_admissions
from src.universe.ipc import CandidateFileError, read_candidates

logger = logging.getLogger(__name__)

SHARD_DENIED_EXIT = 3
_LEASE_RETRY_S = 1.0
_SHARD0_LEASE_WAIT_TTLS = 2
_SHARD_SYMBOL = re.compile(r"[0-9A-Z]{6}")


def ls_guard_config(
    settings: CollectorSettings, candidates_path: pathlib.Path, *, shard_symbols: tuple[str, ...] | None = None
) -> LsGuardConfig:
    """Build the LS limit guard from settings, keeping candidate priority order for truncation."""
    tiers: dict[str, str] | None = None
    order: tuple[str, ...] | None = None
    try:
        stored = read_candidates(candidates_path)
    except (OSError, CandidateFileError, ValueError, KeyError, TypeError, AttributeError):
        stored = None
    rows = stored.get("candidates") if isinstance(stored, dict) else None
    if isinstance(rows, list) and rows:
        symbols = [str(row["symbol"]) for row in rows if isinstance(row, dict) and "symbol" in row]
        if symbols:
            order = tuple(symbols)
            mapped = {
                str(row["symbol"]): str(row["tier"])
                for row in rows
                if isinstance(row, dict) and "symbol" in row and "tier" in row
            }
            tiers = mapped or None
    if shard_symbols is not None:
        allowed = frozenset(shard_symbols)
        if order is not None:
            order = tuple(sym for sym in order if sym in allowed) or None
        if tiers is not None:
            tiers = {sym: tier for sym, tier in tiers.items() if sym in allowed} or None
    return LsGuardConfig(
        max_subscriptions=settings.ls_max_subscriptions_per_session,
        max_sessions=settings.ls_max_sessions,
        tiers=tiers,
        order=order,
    )


def _parse_shard_symbols(raw: object) -> tuple[str, ...] | None:
    if raw is None:
        return None
    symbols = tuple(part.strip() for part in str(raw).split(","))
    if any(_SHARD_SYMBOL.fullmatch(symbol) is None for symbol in symbols) or len(set(symbols)) != len(symbols):
        raise KrxAlphaError("symbols must be a comma-separated list of unique 6-character KRX codes")
    return symbols


def _shard_request(args: argparse.Namespace) -> tuple[int, tuple[str, ...] | None]:
    index = int(getattr(args, "ls_shard_index", None) or 0)
    if index < 0:
        raise KrxAlphaError(f"ls-shard-index {index} must be non-negative")
    symbols = _parse_shard_symbols(getattr(args, "symbols", None))
    if index >= 1 and symbols is None:
        raise KrxAlphaError(f"symbols is required for ls-shard-index {index}")
    return index, symbols


def _credential_key_id(app_key: str) -> str:
    """Credential fingerprint shared with the LS token store, so every process on one key uses one ledger."""
    return hashlib.sha256(app_key.encode("utf-8")).hexdigest()[:12]


def _ls_lease_ledger(
    vendor: str, settings: CollectorSettings, token_cache_dir: pathlib.Path, app_key: str
) -> WsLeaseLedger | None:
    """Host-wide LS slot ledger for multi-session collection; None keeps the single-session path unchanged."""
    if vendor != "ls" or settings.ls_active_sessions < 2:
        return None
    return WsLeaseLedger(
        token_cache_dir,
        vendor=vendor,
        key_id=_credential_key_id(app_key),
        max_connections=settings.ls_max_sessions,
        ttl_s=settings.ws_lease_ttl_s,
    )


async def _claim_primary(ledger: WsLeaseLedger, *, owner: str, shard_index: int, ttl_s: float) -> Lease | None:
    """Acquire the shard's PRIMARY slot; shard 0 proceeds unleased after its retry window, other shards raise."""
    if shard_index >= 1:
        return ledger.acquire(owner, LeaseRole.PRIMARY)
    deadline = time.monotonic() + _SHARD0_LEASE_WAIT_TTLS * ttl_s
    while True:
        try:
            return ledger.acquire(owner, LeaseRole.PRIMARY)
        except LeaseUnavailable as exc:
            if time.monotonic() >= deadline:
                logger.critical("[SYS] stage=ws_lease status=PRIMARY_FORCED reason=%s", exc, extra=EVENT)
                return None
            await asyncio.sleep(_LEASE_RETRY_S)


def _release_lease_quietly(ledger: WsLeaseLedger, lease: Lease) -> None:
    try:
        ledger.release(lease)
    except LeaseUnavailable as exc:
        logger.warning("[SYS] stage=ws_lease status=RELEASE_FAILED reason=%s", exc, extra=EVENT)


@dataclass(frozen=True)
class RegularStreamProfile:
    vendor: str
    slot_budget: int
    symbol_limit: int | None
    route: StreamRoute | None
    shutdown_log: str
    proactive_handover: bool = False


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register 'collect-stream'. Settings-derived flags default to None so registration reads no env."""
    parser = subparsers.add_parser("collect-stream")
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--journal-root", required=True)
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--candidates-path", required=True)
    parser.add_argument("--market-map", required=True)
    parser.add_argument("--ntp-host", default=None)
    parser.add_argument("--max-clock-offset-ns", type=int, default=None)
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--degraded-reason", default=None)
    parser.add_argument("--ls-shard-index", type=int, default=0, help="LS shard number; 0 is the primary shard")
    parser.add_argument(
        "--symbols", default=None, help="comma-separated shard symbols; required when --ls-shard-index >= 1"
    )
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """Run the LS regular-session streamer and return the exit code.

    ``ntp_host``/``max_clock_offset_ns`` left as None (or absent) resolve from
    ``CollectorSettings`` at run time: the daemon launches this command without
    these flags, so settings are the only channel for env overrides to reach
    the child. Explicit flags win.

    Raises:
        ClockUnsyncedError: Clock offset exceeds the resolved ceiling (before any vendor connect).
        MissingCredentialsError: LS credentials are absent.
    """
    return asyncio.run(_run_stream(args))


def _ls_dynamic_watch(
    *,
    session: CollectorSession,
    session_date: dt.date,
    settings: CollectorSettings,
    vendor: str,
    shard_index: int,
    static_pairs: list[tuple[str, str]],
) -> DynamicPairWatcher | None:
    """Watcher for an LS dynamic-reserve shard (index >= 1); None when intraday admission is disabled or for shard 0."""
    if vendor != "ls" or shard_index < 1 or not IntradayAdmissionSettings().enabled:
        return None
    feed = f"ls:{shard_index}"
    ipc_path = settings.paths.dynamic_admissions(session_date)
    initial = [
        (symbol, stream)
        for symbol in dict.fromkeys(read_dynamic_admissions(ipc_path).symbols_for(feed))
        for stream in settings.streams
    ]
    return DynamicPairWatcher(
        feed=DynamicFeed(
            vendor=vendor,
            feed=feed,
            streams=tuple(settings.streams),
            ipc_path=ipc_path,
            membership_path=settings.paths.membership_path(session_date),
            ls_shard=shard_index,
        ),
        session=session,
        static_pairs=static_pairs,
        initial_dynamic=initial,
    )


async def run_regular_stream(
    args: argparse.Namespace,
    settings: CollectorSettings,
    profile: RegularStreamProfile,
    *,
    bootstrap: Callable[[SessionConfig], CollectorSession],
    streamer_factory: Callable[..., RealtimeStreamer],
) -> int:
    """Run one regular-session streamer for profile.vendor; return 0 after a graceful stop, or SHARD_DENIED_EXIT on a denied shard lease."""
    shard_index, shard_symbols = _shard_request(args)
    entry = regular_vendor(profile.vendor)
    creds = entry.load_credentials()
    session_date = dt.date.fromisoformat(str(args.session_date))
    anchors = resolve_session_anchors(settings.paths.session_calendar_dir, session_date)
    overrides = resolve_clock_overrides(
        getattr(args, "ntp_host", None), getattr(args, "max_clock_offset_ns", None), settings
    )
    kwargs: dict[str, object] = {}
    if profile.symbol_limit is not None:
        kwargs["symbol_limit"] = profile.symbol_limit
    if profile.route is not None:
        kwargs["route"] = profile.route
    candidates_path = pathlib.Path(str(args.candidates_path))
    token_cache_dir = KisTokenSettings().token_cache_dir
    ledger = _ls_lease_ledger(profile.vendor, settings, token_cache_dir, creds.app_key)
    owner = f"ls-shard-{shard_index}-{os.getpid()}"
    primary: Lease | None = None
    if ledger is not None:
        try:
            primary = await _claim_primary(ledger, owner=owner, shard_index=shard_index, ttl_s=settings.ws_lease_ttl_s)
        except LeaseUnavailable as exc:
            logger.error(
                "[SYS] stage=ws_lease status=PRIMARY_DENIED index=%d reason=%s",
                shard_index,
                exc,
                extra=EVENT,
            )
            return SHARD_DENIED_EXIT
    try:
        cfg = SessionConfig(
            session_date=session_date,
            journal_root=pathlib.Path(str(args.journal_root)),
            manifest_path=pathlib.Path(str(args.manifest_path)),
            candidates_path=candidates_path,
            ntp_host=overrides.ntp_host,
            slot_budget=profile.slot_budget,
            max_clock_offset_ns=overrides.max_clock_offset_ns,
            desired_streams=settings.streams,
            vendor=profile.vendor,
            degraded_reason=getattr(args, "degraded_reason", None),
            ntp_fallback_hosts=settings.ntp_fallback_hosts,
            min_free_disk_gb=settings.min_free_disk_gb,
            journal_retain_days=settings.journal_retain_days,
            ls_guard=ls_guard_config(settings, candidates_path, shard_symbols=shard_symbols)
            if profile.vendor == "ls"
            else None,
            ls_shard_index=shard_index,
            ls_symbols=shard_symbols,
            **kwargs,  # type: ignore[arg-type]
        )
        session = bootstrap(cfg)
        market_of: dict[str, str] | None = None
        if entry.requires_market_map:
            raw_map = getattr(args, "market_map", None)
            if raw_map is None:
                raise KrxAlphaError(f"market-map is required for vendor {profile.vendor}")
            market_of = json.loads(pathlib.Path(str(raw_map)).read_text(encoding="utf-8"))  # noqa: ASYNC240 - one-shot startup read
        open_watch = RegularOpenWatch(
            anchors=anchors,
            silence_limit_s=SILENCE_LIMIT_S,
            open_grace_s=settings.regular_open_grace_s,
            alert_after_s=settings.regular_open_alert_after_s,
            standby_lead_s=settings.regular_open_standby_lead_s,
            standby_hold_s=settings.regular_open_standby_hold_s,
        )
        swap_request = SwapRequest()
        if shard_symbols is not None:
            logger.info("[DATA] stage=ls_shard index=%d symbols=%d", shard_index, len(shard_symbols), extra=EVENT)
        static_pairs = session.replay_pairs()
        live_watch = _ls_dynamic_watch(
            session=session,
            session_date=session_date,
            settings=settings,
            vendor=profile.vendor,
            shard_index=shard_index,
            static_pairs=static_pairs,
        )
        await run_streaming_child(
            session=session,
            adapter_factory=lambda http: entry.build(
                creds, RegularAdapterContext(http, settings, token_cache_dir, market_of)
            ),
            replay_pairs=live_watch.pairs() if live_watch is not None else static_pairs,
            live_watch=live_watch,
            silence_limit=open_watch.silence_limit,
            max_cycles=getattr(args, "max_cycles", None),
            streamer_factory=streamer_factory,
            lag_monitor=LagMonitor(
                threshold_s=AuditSettings().latency_alert_threshold_s,
                # Only shard 0 may open standbys: the spare LS session is then never contended across shards.
                on_exceeded=swap_request.request if profile.proactive_handover and shard_index == 0 else None,
            ),
            frame_observers=(open_watch,),
            handover=(
                HandoverSettings(
                    swap_request,
                    settings.lag_handover_max_s,
                    settings.lag_handover_cooldown_s,
                    open_watch.standby_window if profile.proactive_handover and shard_index == 0 else None,
                    max_sessions=settings.ls_max_sessions,
                    stall_lag_s=AuditSettings().latency_alert_threshold_s
                    if settings.lag_handover_standby_refresh_enabled
                    else None,
                    refresh_cooldown_s=settings.lag_handover_refresh_cooldown_s,
                    lease_ledger=ledger,
                    owner=owner,
                    primary_lease=primary,
                    lease_ttl_s=settings.ws_lease_ttl_s,
                )
                if profile.proactive_handover or ledger is not None
                else None
            ),
        )
        logger.info(
            profile.shutdown_log,
            str(cfg.manifest_path),
            len(session.manifest.subscription_acks),
            len(session.manifest.gaps),
            len(session.manifest.boots),
            extra=EVENT,
        )
    finally:
        if ledger is not None and primary is not None:
            _release_lease_quietly(ledger, primary)
    return 0


async def _run_stream(args: argparse.Namespace) -> int:
    settings = CollectorSettings()
    profile = RegularStreamProfile(
        vendor=settings.vendor,
        slot_budget=settings.subscription_pair_budget,
        symbol_limit=None,
        route=None,
        shutdown_log="[DATA] stage=stream_shutdown manifest=%s acks=%d gaps=%d boots=%d",
        proactive_handover=settings.lag_handover_enabled and settings.vendor == "ls",
    )
    return await run_regular_stream(
        args, settings, profile, bootstrap=bootstrap_session, streamer_factory=RealtimeStreamer
    )
