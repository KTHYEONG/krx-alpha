"""NXT regular-session symbol pool: evening selection from a pre-ranked trade-amount list (pure, injected fetch)."""

from __future__ import annotations

import datetime as dt
import pathlib
from collections.abc import Callable, Sequence

from src.core.errors import KrxAlphaError
from src.marketdata.kiwoom_ranking import KiwoomRankingRow
from src.universe.ipc import CandidateFileError, CandidateSnapshot, read_candidate_snapshot, write_candidate_snapshot

_KST_OFFSET: dt.timedelta = dt.timedelta(hours=9)
_POLICY_VERSION: str = "regular_nxt_v1"
_SESSION: str = "regular_nxt"


class RegularNxtUniverseError(KrxAlphaError):
    """NXT regular symbol pool selection fail-closed signal."""


def build_regular_nxt_snapshot(
    *,
    target_date: dt.date,
    generated_at: dt.datetime,
    effective_from: dt.datetime,
    rows: Sequence[KiwoomRankingRow],
    capacity: int,
    excluded_symbols: frozenset[str] = frozenset(),
) -> CandidateSnapshot:
    """Rank ``rows`` by trade value into a ``regular_nxt`` snapshot for ``target_date``.

    Raises:
        RegularNxtUniverseError: Timestamps are not KST-offset, ``generated_at`` is not before
            ``target_date``, ``effective_from`` is not on ``target_date`` or precedes ``generated_at``,
            ``capacity`` is below 1, or nothing remains after exclusions.
    """
    if generated_at.utcoffset() != _KST_OFFSET or generated_at.date() >= target_date:
        raise RegularNxtUniverseError("regular_nxt pool must be generated before the target date")
    if effective_from.utcoffset() != _KST_OFFSET or effective_from.date() != target_date:
        raise RegularNxtUniverseError("regular_nxt effective_from must fall on the target date in KST")
    if capacity < 1:
        raise RegularNxtUniverseError("regular_nxt capacity must be >= 1")
    best: dict[str, KiwoomRankingRow] = {}
    for row in rows:
        if row.symbol in excluded_symbols:
            continue
        current = best.get(row.symbol)
        if current is None or row.trade_value_krw > current.trade_value_krw:
            best[row.symbol] = row
    ordered = sorted(best.values(), key=lambda row: (-row.trade_value_krw, row.symbol))[:capacity]
    if not ordered:
        raise RegularNxtUniverseError("regular_nxt ranking has no eligible symbols")
    return CandidateSnapshot(
        schema_version=1,
        rev=int(target_date.strftime("%Y%m%d")),
        session_date=target_date,
        session=_SESSION,
        generated_at=generated_at,
        source_asof=generated_at,
        effective_from=effective_from,
        policy_version=_POLICY_VERSION,
        capacity=capacity,
        eligible_count=len(best),
        selected_count=len(ordered),
        candidates=tuple(
            {"symbol": row.symbol, "rank": index, "trade_value_krw": row.trade_value_krw}
            for index, row in enumerate(ordered, start=1)
        ),
    )


def refresh_regular_nxt_pool(
    *,
    target_date: dt.date,
    generated_at: dt.datetime,
    effective_from: dt.datetime,
    fetch: Callable[[], Sequence[KiwoomRankingRow]],
    out_path: pathlib.Path,
    capacity: int,
    excluded_symbols: frozenset[str] = frozenset(),
) -> CandidateSnapshot:
    """Fetch the ranking once, build the ``regular_nxt`` snapshot, and persist it atomically.

    A ``fetch`` exception propagates unchanged and nothing is written.

    Raises:
        RegularNxtUniverseError: See ``build_regular_nxt_snapshot``.
    """
    rows = fetch()
    snapshot = build_regular_nxt_snapshot(
        target_date=target_date,
        generated_at=generated_at,
        effective_from=effective_from,
        rows=rows,
        capacity=capacity,
        excluded_symbols=excluded_symbols,
    )
    write_candidate_snapshot(out_path, snapshot)
    return snapshot


def regular_nxt_pool_ready(path: pathlib.Path, *, target_date: dt.date, max_candidates: int) -> bool:
    """True when ``path`` holds a valid regular_nxt snapshot for ``target_date``; never raises on a bad file."""
    try:
        read_candidate_snapshot(
            path,
            expected_session_date=target_date,
            expected_session=_SESSION,
            max_candidates=max_candidates,
        )
    except (CandidateFileError, OSError, ValueError, KeyError, TypeError, AttributeError):
        return False
    return True
