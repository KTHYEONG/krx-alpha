"""NXT premarket pool selection: evening batch from NXT-only rankings (session isolated)."""

from __future__ import annotations

import datetime as dt
import pathlib

from src.brokers.kis.data import KisDataClient
from src.core.errors import KrxAlphaError
from src.universe.aftermarket import AftermarketUniverseError, build_aftermarket_snapshot
from src.universe.ipc import CandidateFileError, CandidateSnapshot, read_candidate_snapshot, write_candidate_snapshot


class PremarketUniverseError(KrxAlphaError):
    """Premarket pool selection fail-closed signal."""


def refresh_premarket_pool(
    *,
    target_date: dt.date,
    generated_at: dt.datetime,
    effective_from: dt.datetime,
    client: KisDataClient,
    out_path: pathlib.Path,
    capacity: int,
    excluded_symbols: frozenset[str] = frozenset(),
) -> CandidateSnapshot:
    """Select the NXT premarket symbol pool for ``target_date`` and persist it atomically.

    Ranks come from the NXT-only (``NX``) trade-amount and fluctuation screens, so every selected
    symbol is NXT-listed. The pool is built from data observable at ``generated_at`` only, which
    must fall on an earlier KST date than ``target_date``; ``effective_from`` is the target-day
    collector start. The file is written with session ``"premarket"`` and ``rev`` = target date.

    Raises:
        PremarketUniverseError: ``generated_at`` is not on a date before ``target_date``;
            ``effective_from`` is not on ``target_date``; or the ranking source is invalid or empty
            after exclusions.
        KisApiError: Vendor transport/API failure (callers retry).
        CandidateFileError: The snapshot file cannot be written.
    """
    if generated_at.tzinfo is None or generated_at.date() >= target_date:
        raise PremarketUniverseError("premarket pool must be generated before the target date")
    if effective_from.tzinfo is None or effective_from.date() != target_date:
        raise PremarketUniverseError("premarket effective_from must fall on the target date")
    trade_amount_rows = client.get_trade_amount_ranking(market_div="NX")
    fluctuation_rows = client.get_fluctuation_ranking(market_div="NX")
    try:
        snapshot = build_aftermarket_snapshot(
            session_date=target_date,
            generated_at=generated_at,
            trade_amount_rows=trade_amount_rows,
            fluctuation_rows=fluctuation_rows,
            capacity=capacity,
            excluded_symbols=excluded_symbols,
            policy_version="premarket_v1",
            session="premarket",
            effective_from=effective_from,
        )
    except AftermarketUniverseError as exc:
        raise PremarketUniverseError(str(exc)) from exc
    write_candidate_snapshot(out_path, snapshot)
    return snapshot


def premarket_pool_ready(path: pathlib.Path, *, target_date: dt.date, max_candidates: int) -> bool:
    """True when ``path`` holds a valid premarket snapshot for ``target_date``; never raises on a bad file."""
    try:
        read_candidate_snapshot(
            path,
            expected_session_date=target_date,
            expected_session="premarket",
            max_candidates=max_candidates,
        )
    except (CandidateFileError, OSError, ValueError, KeyError, TypeError, AttributeError):
        return False
    return True
