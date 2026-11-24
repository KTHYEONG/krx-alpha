"""KRX regular-session KIS co-primary shard helpers (pure, no daemon import)."""

from __future__ import annotations

import datetime as dt
import logging
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
from src.realtime.manifest import SessionManifest
from src.universe.ipc import read_candidates

_KST = ZoneInfo("Asia/Seoul")
_logger = logging.getLogger(__name__)

__all__ = [
    "candidate_symbols_and_tiers",
    "disjoint_hot_symbol_sets",
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


def disjoint_hot_symbol_sets(
    priority_symbols: tuple[str, ...], excluded: frozenset[str], counts: tuple[int, ...]
) -> tuple[tuple[str, ...], ...]:
    """Split priority order minus `excluded` into consecutive non-overlapping slices sized by `counts`; short when exhausted."""
    if len(set(priority_symbols)) != len(priority_symbols):
        raise KrxAlphaError("priority_symbols must be unique")
    if any(count < 0 for count in counts):
        raise KrxAlphaError(f"hot symbol counts must be non-negative: {counts}")
    remaining = tuple(symbol for symbol in priority_symbols if symbol not in excluded)
    sets: list[tuple[str, ...]] = []
    start = 0
    for count in counts:
        chunk = remaining[start : start + count]
        sets.append(chunk)
        start += len(chunk)
    return tuple(sets)


# Calendar days searched back for the last LS session with frame counts (covers weekends and a holiday run).
LS_LOAD_LOOKBACK_DAYS = 7


def prior_ls_frame_loads(paths: DataPaths, today: dt.date) -> tuple[dt.date, dict[str, int]] | None:
    """(day, frames per symbol over all LS KRX shards and streams) for the latest day before `today` with counts.

    Point-in-time: only days strictly before `today`, so the daemon, a resumed daemon and the KIS child all read
    the same closed manifests. None when no such day exists or a manifest is unreadable (callers fall back).
    """
    for back in range(1, LS_LOAD_LOOKBACK_DAYS + 1):
        day = today - dt.timedelta(days=back)
        total: dict[str, int] | None = None
        try:
            for path in paths.ls_manifest_paths(day):
                if not path.is_file():
                    continue
                counts = SessionManifest.load(path).frame_counts
                if counts is None:
                    continue
                total = {} if total is None else total
                for symbol, per_stream in counts.items():
                    total[symbol] = total.get(symbol, 0) + sum(per_stream.values())
        except (OSError, ValueError, KeyError, TypeError, KrxAlphaError) as exc:
            _logger.warning("[DATA] stage=ls_shard_load status=SKIP reason=%s day=%s", type(exc).__name__, day.isoformat())
            return None
        if total:
            return day, total
    return None


def ls_shard_blocks(
    candidates_path: pathlib.Path, collector: CollectorSettings, *, loads: dict[str, int] | None = None
) -> tuple[tuple[str, ...], ...]:
    """LS shard symbol blocks as the daemon plans them (same `loads`); () for non-LS vendors or when planning fails."""
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
            loads=loads,
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
    """True iff KST weekday and now in [regular_open - start_lead_s, closing_auction_start + close_grace_s) of the day's anchors."""
    kst = now.astimezone(_KST)
    if kst.weekday() >= 5:
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
