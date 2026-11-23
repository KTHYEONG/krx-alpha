"""collect-stream-ls-after CLI subcommand: one preemptible LS connection for aftermarket duplicates."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import logging
import pathlib
from collections.abc import Mapping, Sequence
from collections.abc import Set as AbstractSet
from zoneinfo import ZoneInfo

from src.cli import collect_stream
from src.cli.collect_stream import RegularStreamProfile
from src.core.config import AftermarketSettings, CollectorSettings
from src.core.observability import EVENT
from src.core.session_anchors import resolve_session_anchors
from src.realtime.adapters.ls import LS_NXT_STREAMS
from src.realtime.adapters.registry import LS_AFTER_ADAPTER
from src.realtime.contracts import MarketSession, MarketVenue
from src.realtime.session import StreamRoute, bootstrap_session
from src.realtime.streamer import RealtimeStreamer
from src.realtime.ws_lease import LeaseRole
from src.universe.aftermarket import aftermarket_candidate_session
from src.universe.intraday import DynamicAdmissionsFileError, read_dynamic_admissions
from src.universe.ipc import CandidateFileError, read_candidate_snapshot
from src.universe.membership import MembershipFileError, effective_tiers, read_membership

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")
_NS_PER_S = 1_000_000_000
_FEED_ROLE = "after"


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register ``collect-stream-ls-after``; ``--end-time`` defaults to the session's after-market end (KST)."""
    parser = subparsers.add_parser("collect-stream-ls-after")
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--journal-root", required=True)
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--market-map", required=True)
    parser.add_argument("--krx-candidates-path", required=True)
    parser.add_argument("--nxt-candidates-path", required=True)
    parser.add_argument("--membership-path", required=True)
    parser.add_argument("--dynamic-plan-path", required=True)
    parser.add_argument("--end-time", default=None, help="KST wall time HH:MM at which the child stops gracefully")
    parser.add_argument("--ntp-host", default=None)
    parser.add_argument("--max-clock-offset-ns", type=int, default=None)
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--degraded-reason", default=None)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """Run the LS aftermarket child; return SHARD_DENIED_EXIT when no lease ledger or slot is available."""
    return asyncio.run(_run_stream(args))


def build_ls_after_pair_plan(
    krx_symbols: Sequence[str],
    nxt_symbols: Sequence[str],
    ls_members: AbstractSet[str],
    capacity_pairs: int,
) -> dict[str, tuple[str, ...]]:
    """Symbol -> streams plan: KRX candidates outside LS members first, then NXT candidates, within `capacity_pairs`."""
    krx_streams = tuple(AftermarketSettings().krx_streams)
    plan: dict[str, tuple[str, ...]] = {}
    used = 0
    for symbol in dict.fromkeys(krx_symbols):
        if symbol in ls_members:
            continue
        if used + len(krx_streams) > capacity_pairs:
            break
        plan[symbol] = krx_streams
        used += len(krx_streams)
    for symbol in dict.fromkeys(nxt_symbols):
        if used + len(LS_NXT_STREAMS) > capacity_pairs:
            break
        plan[symbol] = (*plan.get(symbol, ()), *LS_NXT_STREAMS)
        used += len(LS_NXT_STREAMS)
    return plan


def _read_candidate_symbols(path: pathlib.Path, venue: MarketVenue, session_date: dt.date) -> tuple[str, ...]:
    try:
        snapshot = read_candidate_snapshot(
            path,
            expected_session_date=session_date,
            expected_session=aftermarket_candidate_session(venue),
            max_candidates=AftermarketSettings().max_symbols,
        )
    except CandidateFileError as exc:
        logger.warning(
            "[DATA] stage=ls_after_child status=CANDIDATES_UNREADABLE venue=%s reason=%s",
            venue.value,
            exc,
            extra=EVENT,
        )
        return ()
    return tuple(str(row["symbol"]) for row in snapshot.candidates if "symbol" in row)


def _ls_member_symbols(membership_path: pathlib.Path, plan_path: pathlib.Path, ls_sessions: int) -> frozenset[str]:
    members: set[str] = set()
    try:
        rows = effective_tiers(read_membership(membership_path)).values()
        members.update(row.symbol for row in rows if row.ls_shard is not None)
    except (OSError, MembershipFileError) as exc:
        logger.warning("[DATA] stage=ls_after_child status=MEMBERSHIP_UNREADABLE reason=%s", exc, extra=EVENT)
    try:
        plan = read_dynamic_admissions(plan_path)
    except (OSError, DynamicAdmissionsFileError) as exc:
        logger.warning("[DATA] stage=ls_after_child status=ADMISSIONS_UNREADABLE reason=%s", exc, extra=EVENT)
    else:
        for index in range(ls_sessions):
            members.update(plan.pairs_for(f"ls:{index}", "krx"))
    return frozenset(members)


def _stop_deadline_ns(args: argparse.Namespace, settings: CollectorSettings, session_date: dt.date) -> int:
    raw = getattr(args, "end_time", None)
    if raw is not None:
        end = dt.time.fromisoformat(str(raw))
    else:
        end = resolve_session_anchors(settings.paths.session_calendar_dir, session_date).after_market_end
    return int(dt.datetime.combine(session_date, end, tzinfo=_KST).timestamp()) * _NS_PER_S


async def _run_stream(args: argparse.Namespace) -> int:
    settings = CollectorSettings()
    session_date = dt.date.fromisoformat(str(args.session_date))
    krx_symbols = _read_candidate_symbols(pathlib.Path(args.krx_candidates_path), MarketVenue.KRX, session_date)
    nxt_symbols = _read_candidate_symbols(pathlib.Path(args.nxt_candidates_path), MarketVenue.NXT, session_date)
    if not krx_symbols and not nxt_symbols:
        logger.info("[DATA] stage=ls_after_child status=NO_PAIRS", extra=EVENT)
        return 0
    ls_members = _ls_member_symbols(
        pathlib.Path(args.membership_path), pathlib.Path(args.dynamic_plan_path), settings.ls_active_sessions
    )
    plan = build_ls_after_pair_plan(krx_symbols, nxt_symbols, ls_members, settings.ls_capacity_pairs)
    if not plan:
        logger.info("[DATA] stage=ls_after_child status=NO_PAIRS", extra=EVENT)
        return 0
    requested = len(set(krx_symbols) | set(nxt_symbols))
    if len(plan) < requested:
        logger.warning(
            "[DATA] stage=ls_after_child status=TRUNCATED dropped=%d", requested - len(plan), extra=EVENT
        )
    krx_count = sum(1 for streams in plan.values() if streams[0] in tuple(AftermarketSettings().krx_streams))
    pair_count = sum(len(streams) for streams in plan.values())
    logger.info(
        "[DATA] stage=ls_after_child status=START krx=%d nxt=%d pairs=%d",
        krx_count,
        len(plan) - krx_count,
        pair_count,
        extra=EVENT,
    )
    profile = RegularStreamProfile(
        vendor="ls",
        adapter=LS_AFTER_ADAPTER,
        slot_budget=settings.ls_max_subscriptions_per_session,
        symbol_limit=None,
        route=StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER),
        shutdown_log="[DATA] stage=stream_shutdown vendor=ls-after manifest=%s acks=%d gaps=%d boots=%d",
        session_index=0,
        hot_symbols=0,
        pair_plan=plan,
        stream_routes=_stream_routes(plan),
        lease_role=LeaseRole.STANDBY,
        require_lease=True,
        feed_role=_FEED_ROLE,
    )
    child_args = argparse.Namespace(**vars(args))
    child_args.candidates_path = args.krx_candidates_path
    return await collect_stream.run_regular_stream(
        child_args,
        settings,
        profile,
        bootstrap=bootstrap_session,
        streamer_factory=RealtimeStreamer,
        stop_deadline_ns=_stop_deadline_ns(args, settings, session_date),
    )


def _stream_routes(plan: Mapping[str, tuple[str, ...]]) -> dict[str, StreamRoute]:
    krx = StreamRoute(MarketVenue.KRX, MarketSession.KRX_AFTER)
    nxt = StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER)
    routes: dict[str, StreamRoute] = {}
    for streams in plan.values():
        for stream in streams:
            routes[stream] = nxt if stream in LS_NXT_STREAMS else krx
    return routes
