"""collect-stream-ls-nxt-regular CLI subcommand: spare LS connection for NXT regular ranks after the KIS pool."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import logging
import pathlib
from collections.abc import Sequence
from typing import cast
from zoneinfo import ZoneInfo

from src.cli import collect_stream
from src.cli.collect_stream import RegularStreamProfile
from src.core.config import CollectorSettings, RegularKisSettings
from src.core.observability import EVENT
from src.core.session_anchors import resolve_session_anchors
from src.realtime.adapters.ls import LS_NXT_STREAMS
from src.realtime.adapters.registry import LS_NXT_ADAPTER
from src.realtime.contracts import MarketSession, MarketVenue
from src.realtime.session import StreamRoute, bootstrap_session
from src.realtime.streamer import RealtimeStreamer
from src.realtime.ws_lease import LeaseRole
from src.universe.ipc import CandidateFileError, read_candidate_snapshot

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")
_NS_PER_S = 1_000_000_000
_REGULAR_NXT_SESSION = "regular_nxt"
_FEED_ROLE = "nxt_regular"
_NXT_REGULAR_ROUTE = StreamRoute(MarketVenue.NXT, MarketSession.REGULAR)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register ``collect-stream-ls-nxt-regular``; ``--kis-nxt-symbols`` is the KIS NXT pool prefix this child skips."""
    parser = subparsers.add_parser("collect-stream-ls-nxt-regular")
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--journal-root", required=True)
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--candidates-path", required=True)
    parser.add_argument("--kis-nxt-symbols", type=int, required=True)
    parser.add_argument("--ntp-host", default=None)
    parser.add_argument("--max-clock-offset-ns", type=int, default=None)
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--degraded-reason", default=None)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """Run the LS NXT regular child; return SHARD_DENIED_EXIT when no lease ledger or spare slot is available."""
    return asyncio.run(_run_stream(args))


def _read_ranked_symbols(path: pathlib.Path, session_date: dt.date, max_candidates: int) -> tuple[tuple[str, int], ...]:
    try:
        snapshot = read_candidate_snapshot(
            path,
            expected_session_date=session_date,
            expected_session=_REGULAR_NXT_SESSION,
            max_candidates=max_candidates,
        )
    except CandidateFileError as exc:
        logger.warning(
            "[DATA] stage=ls_nxt_regular_child status=CANDIDATES_UNREADABLE reason=%s", exc, extra=EVENT
        )
        return ()
    ranked = sorted(snapshot.candidates, key=lambda row: int(cast(int, row["rank"])))
    return tuple((str(row["symbol"]), int(cast(int, row["rank"]))) for row in ranked if "symbol" in row)


def build_ls_nxt_regular_plan(
    ranked_symbols: Sequence[tuple[str, int]], kis_nxt_symbols: int, capacity_pairs: int
) -> dict[str, tuple[str, ...]]:
    """Symbol -> streams plan for the ranks the KIS NXT pool does not cover, within `capacity_pairs`."""
    slots = capacity_pairs // len(LS_NXT_STREAMS)
    return {symbol: LS_NXT_STREAMS for symbol, _ in ranked_symbols[kis_nxt_symbols : kis_nxt_symbols + slots]}


def _stop_deadline_ns(settings: CollectorSettings, session_date: dt.date) -> int:
    anchors = resolve_session_anchors(settings.paths.session_calendar_dir, session_date)
    grace = dt.timedelta(seconds=RegularKisSettings().close_grace_s)
    end = dt.datetime.combine(session_date, anchors.closing_auction_start, tzinfo=_KST) + grace
    return int(end.timestamp()) * _NS_PER_S


async def _run_stream(args: argparse.Namespace) -> int:
    settings = CollectorSettings()
    session_date = dt.date.fromisoformat(str(args.session_date))
    kis_nxt_symbols = int(args.kis_nxt_symbols)
    slots = settings.ls_capacity_pairs // len(LS_NXT_STREAMS)
    ranked = _read_ranked_symbols(pathlib.Path(args.candidates_path), session_date, kis_nxt_symbols + slots)
    plan = build_ls_nxt_regular_plan(ranked, kis_nxt_symbols, settings.ls_capacity_pairs)
    if not plan:
        logger.info("[DATA] stage=ls_nxt_regular_child status=NO_PAIRS", extra=EVENT)
        return 0
    first_rank = next(rank for symbol, rank in ranked if symbol in plan)
    logger.info(
        "[DATA] stage=ls_nxt_regular_child status=START symbols=%d pairs=%d first_rank=%d",
        len(plan),
        sum(len(streams) for streams in plan.values()),
        first_rank,
        extra=EVENT,
    )
    profile = RegularStreamProfile(
        vendor="ls",
        adapter=LS_NXT_ADAPTER,
        slot_budget=settings.ls_max_subscriptions_per_session,
        symbol_limit=None,
        route=_NXT_REGULAR_ROUTE,
        shutdown_log="[DATA] stage=stream_shutdown vendor=ls-nxt-regular manifest=%s acks=%d gaps=%d boots=%d",
        session_index=0,
        hot_symbols=0,
        pair_plan=plan,
        stream_routes=dict.fromkeys(LS_NXT_STREAMS, _NXT_REGULAR_ROUTE),
        lease_role=LeaseRole.SPARE,
        require_lease=True,
        feed_role=_FEED_ROLE,
    )
    return await collect_stream.run_regular_stream(
        args,
        settings,
        profile,
        bootstrap=bootstrap_session,
        streamer_factory=RealtimeStreamer,
        stop_deadline_ns=_stop_deadline_ns(settings, session_date),
    )
