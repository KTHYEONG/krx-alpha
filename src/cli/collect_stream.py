"""collect-stream CLI 서브커맨드 (LS 실시간 수집 구동)."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import logging
import pathlib
from collections.abc import Callable
from dataclasses import dataclass

from src.core.config import AuditSettings, CollectorSettings, KisTokenSettings
from src.core.errors import KrxAlphaError
from src.core.observability import EVENT
from src.core.session_anchors import resolve_session_anchors
from src.realtime.adapters.registry import RegularAdapterContext, regular_vendor
from src.realtime.connection_handover import SwapRequest
from src.realtime.lag_monitor import LagMonitor
from src.realtime.regular_open_watch import RegularOpenWatch
from src.realtime.session import CollectorSession, SessionConfig, StreamRoute, bootstrap_session
from src.realtime.streamer import SILENCE_LIMIT_S, RealtimeStreamer
from src.realtime.streaming_child import HandoverSettings, resolve_clock_overrides, run_streaming_child

logger = logging.getLogger(__name__)


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


async def run_regular_stream(
    args: argparse.Namespace,
    settings: CollectorSettings,
    profile: RegularStreamProfile,
    *,
    bootstrap: Callable[[SessionConfig], CollectorSession],
    streamer_factory: Callable[..., RealtimeStreamer],
) -> int:
    """Run one regular-session streamer for profile.vendor and return 0 after a graceful stop."""
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
    cfg = SessionConfig(
        session_date=session_date,
        journal_root=pathlib.Path(str(args.journal_root)),
        manifest_path=pathlib.Path(str(args.manifest_path)),
        candidates_path=pathlib.Path(str(args.candidates_path)),
        ntp_host=overrides.ntp_host,
        slot_budget=profile.slot_budget,
        max_clock_offset_ns=overrides.max_clock_offset_ns,
        desired_streams=settings.streams,
        vendor=profile.vendor,
        degraded_reason=getattr(args, "degraded_reason", None),
        ntp_fallback_hosts=settings.ntp_fallback_hosts,
        min_free_disk_gb=settings.min_free_disk_gb,
        journal_retain_days=settings.journal_retain_days,
        **kwargs,  # type: ignore[arg-type]
    )
    session = bootstrap(cfg)
    market_of: dict[str, str] | None = None
    if entry.requires_market_map:
        raw_map = getattr(args, "market_map", None)
        if raw_map is None:
            raise KrxAlphaError(f"market-map is required for vendor {profile.vendor}")
        market_of = json.loads(pathlib.Path(str(raw_map)).read_text(encoding="utf-8"))  # noqa: ASYNC240 - one-shot startup read
    token_cache_dir = KisTokenSettings().token_cache_dir
    open_watch = RegularOpenWatch(
        anchors=anchors,
        silence_limit_s=SILENCE_LIMIT_S,
        open_grace_s=settings.regular_open_grace_s,
        alert_after_s=settings.regular_open_alert_after_s,
        standby_lead_s=settings.regular_open_standby_lead_s,
        standby_hold_s=settings.regular_open_standby_hold_s,
    )
    swap_request = SwapRequest()
    await run_streaming_child(
        session=session,
        adapter_factory=lambda http: entry.build(
            creds, RegularAdapterContext(http, settings, token_cache_dir, market_of)
        ),
        replay_pairs=session.replay_pairs(),
        silence_limit=open_watch.silence_limit,
        max_cycles=getattr(args, "max_cycles", None),
        streamer_factory=streamer_factory,
        lag_monitor=LagMonitor(
            threshold_s=AuditSettings().latency_alert_threshold_s,
            on_exceeded=swap_request.request if profile.proactive_handover else None,
        ),
        frame_observers=(open_watch,),
        handover=(
            HandoverSettings(
                swap_request, settings.lag_handover_max_s, settings.lag_handover_cooldown_s, open_watch.standby_window
            )
            if profile.proactive_handover
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
