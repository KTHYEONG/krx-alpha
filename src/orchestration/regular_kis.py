"""KRX regular-session KIS co-primary shard helpers (pure, no daemon import)."""

from __future__ import annotations

import datetime as dt
import pathlib
import sys
from zoneinfo import ZoneInfo

from src.core.config import CollectorSettings
from src.core.errors import KrxAlphaError
from src.core.paths import DataPaths
from src.core.session_anchors import SessionAnchors
from src.realtime.contracts import MarketVenue
from src.realtime.kis_sharding import AftermarketShard
from src.realtime.ls_sharding import plan_ls_shards
from src.universe.ipc import read_candidates

_KST = ZoneInfo("Asia/Seoul")

__all__ = [
    "candidate_symbols_and_tiers",
    "ls_shard_blocks",
    "regular_kis_collection_due",
    "regular_kis_dynamic_stream_cmd",
    "regular_kis_ls_plan_inputs",
    "regular_kis_stream_cmd",
    "regular_nxt_collection_due",
]


def candidate_symbols_and_tiers(path: pathlib.Path) -> tuple[tuple[str, ...], dict[str, str]]:
    """Return candidate symbols in priority order and their tiers; raises KrxAlphaError when the file is not ready."""
    data = read_candidates(path)
    rows = data.get("candidates") if isinstance(data, dict) else None
    if (
        not isinstance(rows, list)
        or not rows
        or not all(isinstance(row, dict) and isinstance(row.get("symbol"), str) and row["symbol"] for row in rows)
    ):
        raise KrxAlphaError(f"invalid candidates file: {path}")
    symbols = tuple(str(row["symbol"]) for row in rows)
    tiers = {str(row["symbol"]): str(row.get("tier", "event")) for row in rows}
    return symbols, tiers


def ls_shard_blocks(candidates_path: pathlib.Path, collector: CollectorSettings) -> tuple[tuple[str, ...], ...]:
    """LS shard symbol blocks as the daemon plans them; () for non-LS vendors or when planning fails."""
    if collector.vendor != "ls":
        return ()
    try:
        symbols, tiers = candidate_symbols_and_tiers(candidates_path)
        shards, _overflow = plan_ls_shards(
            symbols,
            tiers=tiers,
            streams=collector.streams,
            active_sessions=collector.ls_active_sessions,
            pairs_per_session=collector.ls_max_subscriptions_per_session,
            dynamic_reserve_symbols=collector.ls_dynamic_reserve_symbols,
            max_sessions=collector.ls_max_sessions,
        )
    except (KrxAlphaError, OSError, ValueError):
        return ()
    return tuple(shard.symbols for shard in shards)


def regular_kis_ls_plan_inputs(
    blocks: tuple[tuple[str, ...], ...],
) -> tuple[frozenset[str], tuple[tuple[str, ...], ...] | None]:
    """(ls_covered_symbols, ls_shard_symbols) for ``plan_regular_kis_shards``; blocks only when 2+ LS shards exist."""
    covered = frozenset(symbol for block in blocks for symbol in block)
    return covered, (blocks if len(blocks) >= 2 else None)


def regular_kis_collection_due(
    now: dt.datetime, *, anchors: SessionAnchors, start_lead_s: float, close_grace_s: float
) -> bool:
    """True iff KST weekday and now in [regular_open - start_lead_s, regular_close + close_grace_s)."""
    kst = now.astimezone(_KST)
    if kst.weekday() >= 5:
        return False
    start_dt = dt.datetime.combine(kst.date(), anchors.regular_open, tzinfo=_KST) - dt.timedelta(
        seconds=start_lead_s
    )
    end_dt = dt.datetime.combine(kst.date(), anchors.regular_close, tzinfo=_KST) + dt.timedelta(
        seconds=close_grace_s
    )
    return start_dt <= kst < end_dt


def regular_nxt_collection_due(
    now: dt.datetime, *, anchors: SessionAnchors, start_lead_s: float, close_grace_s: float
) -> bool:
    """True iff KST weekday, unshifted anchors, and now in [regular_open - start_lead_s, closing_auction_start + close_grace_s)."""
    kst = now.astimezone(_KST)
    if kst.weekday() >= 5 or anchors.open_shift != dt.timedelta(0):
        return False
    start_dt = dt.datetime.combine(kst.date(), anchors.regular_open, tzinfo=_KST) - dt.timedelta(
        seconds=start_lead_s
    )
    end_dt = dt.datetime.combine(kst.date(), anchors.closing_auction_start, tzinfo=_KST) + dt.timedelta(
        seconds=close_grace_s
    )
    return start_dt <= kst < end_dt


def regular_kis_stream_cmd(today: dt.date, paths: DataPaths, *, shard: AftermarketShard) -> list[str]:
    """argv for ``collect-regular-kis`` (same interpreter and module entry as the other children)."""
    return [
        sys.executable,
        "-m",
        "src.cli.main",
        "collect-regular-kis",
        "--session-date",
        today.isoformat(),
        "--journal-root",
        str(paths.journal_root),
        "--manifest-path",
        str(paths.regular_kis_manifest_path(today, shard.shard_index, shard.venue)),
        "--candidates-path",
        str(paths.candidates if shard.venue is MarketVenue.KRX else paths.regular_nxt_candidates(today)),
        "--shard-index",
        str(shard.shard_index),
        "--venue",
        shard.venue.value,
        "--credential-slot",
        shard.credential_slot,
        "--credential-key-id",
        shard.credential_key_id,
        "--symbols",
        ",".join(shard.symbols),
    ]


def regular_kis_dynamic_stream_cmd(
    today: dt.date, paths: DataPaths, *, slot: str, key_id: str, venue: str = "krx"
) -> list[str]:
    """argv for ``collect-regular-kis-dynamic`` on a KIS dynamic slot; the default KRX argv omits ``--venue``."""
    venue_args = ["--venue", venue] if venue != "krx" else []
    return [
        sys.executable,
        "-m",
        "src.cli.main",
        "collect-regular-kis-dynamic",
        "--session-date",
        today.isoformat(),
        "--journal-root",
        str(paths.journal_root),
        "--manifest-path",
        str(paths.regular_kis_manifest_path(today, int(slot), venue)),
        "--credential-slot",
        slot,
        "--credential-key-id",
        key_id,
        *venue_args,
    ]
