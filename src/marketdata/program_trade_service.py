"""Toss program-trade history backfill orchestration (rate-limited, per-symbol isolated)."""

from __future__ import annotations

import datetime as dt
import logging
import pathlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Any

from src.brokers.kis.rate import HostPacedRateLimiter, host_state_path
from src.core.config import KisTokenSettings, TossProgramTradesSettings
from src.core.symbols import is_krx_short_code
from src.marketdata.partitioned_store import PartitionedStoreError, scan_month_partitions
from src.marketdata.toss_calendar import (
    TOSS_STOCK_TRADING_TREND_GROUP,
    TossCalendarError,
    issue_access_token,
)
from src.marketdata.toss_program_trades import (
    TossProgramTradesError,
    _TossProgramTradesAuthRejected,
    append_program_trades,
    backfill_program_trades_history,
    program_trade_coverage,
    symbols_needing_backfill,
)
from src.marketdata.toss_token_store import IssuedToken, TossTokenStore, toss_token_path

logger = logging.getLogger(__name__)


def _resolve_pacing(
    *,
    app_key: str,
    app_secret: str,
    rate_per_s: float,
    cache_dir: pathlib.Path | None,
    max_lead_s: float | None,
    session: Any | None,
) -> tuple[HostPacedRateLimiter, _TokenSession]:
    """Build the host-shared Toss pacing limiter and token session, issuing one token.

    Raises:
        TossProgramTradesError: When the initial token issuance fails.
    """
    if rate_per_s <= 0:
        raise ValueError("rate_per_s must be positive")
    directory = pathlib.Path(cache_dir) if cache_dir is not None else KisTokenSettings().token_cache_dir
    lead = max_lead_s if max_lead_s is not None else TossProgramTradesSettings().rest_max_lead_s
    limiter = HostPacedRateLimiter(
        host_state_path(directory, "toss", app_key, TOSS_STOCK_TRADING_TREND_GROUP),
        rate_per_s,
        max_lead_s=lead,
    )
    tokens = _TokenSession(
        TossTokenStore(toss_token_path(directory, app_key)),
        lambda: IssuedToken(issue_access_token(app_key=app_key, app_secret=app_secret, session=session), None),
    )
    return limiter, tokens


class _TokenIssuanceFailed(TossProgramTradesError):  # noqa: N818 - surfaced as TossProgramTradesError
    """Toss token issuance failure; aborts the whole run since no symbol can succeed."""


class _TokenSession:
    """One run's Toss token, rotated at most once per vendor rejection."""

    def __init__(self, store: TossTokenStore, issue: Callable[[], IssuedToken]) -> None:
        self._store = store
        self._issue = issue
        try:
            self.token = store.get_or_issue(issue)
        except TossCalendarError as exc:
            raise _TokenIssuanceFailed(f"toss program-trades token issuance failed: {exc}") from exc

    def rotate(self) -> None:
        """Replace the rejected token, adopting a peer-rotated token when present."""
        try:
            self.token = self._store.replace_rejected(self.token, self._issue)
        except TossCalendarError as exc:
            raise _TokenIssuanceFailed(f"toss program-trades token issuance failed: {exc}") from exc


def _fetch_symbol(
    symbol: str,
    tokens: _TokenSession,
    fetch: Callable[..., Sequence[dict[str, object]]],
) -> Sequence[dict[str, object]] | None:
    """Fetch one symbol with a single token rotation on auth rejection.

    Returns:
        Fetched rows, or ``None`` when this symbol failed for a symbol-local reason
        (logged and skipped so the remaining symbols still run).

    Raises:
        TossProgramTradesError: When token issuance fails or the rotated token is
            also rejected; credentials are unusable for every symbol, so the run
            aborts fail-closed.
    """
    try:
        try:
            return fetch(access_token=tokens.token)
        except _TossProgramTradesAuthRejected:
            tokens.rotate()
        return fetch(access_token=tokens.token)
    except (_TossProgramTradesAuthRejected, _TokenIssuanceFailed):
        raise
    except TossProgramTradesError as exc:
        logger.warning("[DATA] stage=toss_program_backfill status=SKIP symbol=%s reason=%s", symbol, str(exc))
        return None


@dataclass(frozen=True)
class ProgramTradesBackfillResult:
    symbols_ok: int
    symbols_failed: int
    appended_rows: int


@dataclass(frozen=True)
class ProgramTradesSyncResult:
    forward_targets: tuple[str, ...]
    candidate_targets: tuple[str, ...]
    symbols_ok: int
    symbols_failed: int
    appended_rows: int
    stale_symbols: tuple[str, ...]
    tracked: tuple[str, ...]


def _tracked_program_trade_symbols(store_path: pathlib.Path) -> tuple[str, ...]:
    """Return every symbol present in the store, bounded by the symbol count."""
    store = pathlib.Path(store_path)
    if store.exists() and not store.is_dir():
        raise TossProgramTradesError(f"toss program-trades store unreadable at {store_path}: not a directory")
    try:
        frame = scan_month_partitions(store).select("symbol").unique().collect(engine="streaming")
    except PartitionedStoreError:
        return ()
    except Exception as exc:
        raise TossProgramTradesError(f"toss program-trades store unreadable at {store_path}: {exc}") from exc
    return tuple(sorted(str(code) for code in frame.get_column("symbol").to_list()))


def sync_program_trades_forward(
    *,
    store_root: pathlib.Path,
    session_date: dt.date,
    complete_through: dt.date,
    candidate_symbols: Sequence[str],
    lookback_days: int,
    app_key: str,
    app_secret: str,
    rate_per_s: float,
    session: Any | None = None,
    active_symbols: frozenset[str] | None = None,
    cache_dir: pathlib.Path | None = None,
    max_lead_s: float | None = None,
) -> ProgramTradesSyncResult:
    """Forward-sync tracked symbols to ``session_date`` plus candidate history depth.

    ``active_symbols`` (currently listed codes) limits forward targets and the
    staleness measure; delisted symbols never gain rows, so counting them would
    push the stale ratio past tolerance within about a year of normal delistings.
    ``None`` keeps every stored symbol tracked.

    Tracked symbols behind ``session_date`` are fetched from ``max_date + 1``;
    candidate symbols lacking history back to ``session_date - lookback_days``
    are fetched from that floor. One shared token and rate limiter cover both
    phases, and all rows land with a single store write.
    """
    store = pathlib.Path(store_root)
    tracked = _tracked_program_trade_symbols(store)
    coverage = program_trade_coverage(store, tracked) if tracked else {}
    forward_min: dict[str, dt.date] = {}
    for symbol, (_min_date, max_date) in coverage.items():
        if active_symbols is not None and symbol not in active_symbols:
            continue
        if max_date < session_date:
            forward_min[symbol] = max_date + dt.timedelta(days=1)
    forward_targets = tuple(sorted(forward_min))
    candidate_floor = session_date - dt.timedelta(days=lookback_days)
    clean_candidates = tuple(code for code in candidate_symbols if is_krx_short_code(code))
    skipped = set(candidate_symbols) - set(clean_candidates)
    if skipped:
        logger.warning(
            "[DATA] stage=toss_program_backfill status=SKIP_INVALID_CODE symbols=%s", sorted(skipped)
        )
    candidate_targets = (
        symbols_needing_backfill(store, clean_candidates, candidate_floor) if clean_candidates else ()
    )
    union = tuple(dict.fromkeys((*forward_targets, *candidate_targets)))
    min_date_by_symbol: dict[str, dt.date] = {}
    candidate_set = set(candidate_targets)
    for symbol in union:
        dates = []
        if symbol in forward_min:
            dates.append(forward_min[symbol])
        if symbol in candidate_set:
            dates.append(candidate_floor)
        min_date_by_symbol[symbol] = min(dates)
    limiter, tokens = _resolve_pacing(
        app_key=app_key,
        app_secret=app_secret,
        rate_per_s=rate_per_s,
        cache_dir=cache_dir,
        max_lead_s=max_lead_s,
        session=session,
    )
    symbols_ok = 0
    symbols_failed = 0
    fetched: list[dict[str, object]] = []
    fetched_max: dict[str, dt.date] = {}
    for symbol in union:
        rows = _fetch_symbol(
            symbol,
            tokens,
            partial(
                backfill_program_trades_history,
                symbol,
                min_date=min_date_by_symbol[symbol],
                session=session,
                throttle=limiter.acquire,
            ),
        )
        if rows is None:
            symbols_failed += 1
            continue
        symbols_ok += 1
        for row in rows:
            fetched.append(row)
            day = row["date"]
            assert isinstance(day, dt.date)
            if symbol not in fetched_max or day > fetched_max[symbol]:
                fetched_max[symbol] = day
    appended_rows = append_program_trades(store, fetched) if fetched else 0
    final_max: dict[str, dt.date] = {symbol: max_date for symbol, (_min_date, max_date) in coverage.items()}
    for symbol, day in fetched_max.items():
        if symbol not in final_max or day > final_max[symbol]:
            final_max[symbol] = day
    if active_symbols is not None:
        final_max = {symbol: day for symbol, day in final_max.items() if symbol in active_symbols}
    tracked_final = tuple(sorted(final_max))
    stale_symbols = tuple(sorted(symbol for symbol, max_date in final_max.items() if max_date < complete_through))
    logger.info(
        "[DATA] stage=program_trades_forward status=OK fetched_symbols=%d appended_rows=%d stale=%d",
        symbols_ok,
        appended_rows,
        len(stale_symbols),
    )
    return ProgramTradesSyncResult(
        forward_targets=forward_targets,
        candidate_targets=candidate_targets,
        symbols_ok=symbols_ok,
        symbols_failed=symbols_failed,
        appended_rows=appended_rows,
        stale_symbols=stale_symbols,
        tracked=tracked_final,
    )


def backfill_program_trades(
    *,
    store_path: pathlib.Path,
    symbols: Sequence[str],
    min_date: dt.date,
    app_key: str,
    app_secret: str,
    rate_per_s: float,
    session: Any | None = None,
    cache_dir: pathlib.Path | None = None,
    max_lead_s: float | None = None,
) -> ProgramTradesBackfillResult:
    """Fetch a fixed symbol set under the Toss rate limit and persist successes."""
    unique = list(dict.fromkeys(symbols))
    if not unique or any(not is_krx_short_code(code) for code in unique):
        raise ValueError(f"symbols must be non-empty KRX short codes: {list(symbols)!r}")
    limiter, tokens = _resolve_pacing(
        app_key=app_key,
        app_secret=app_secret,
        rate_per_s=rate_per_s,
        cache_dir=cache_dir,
        max_lead_s=max_lead_s,
        session=session,
    )
    symbols_ok = 0
    symbols_failed = 0
    fetched: list[dict[str, object]] = []
    for code in unique:
        rows = _fetch_symbol(
            code,
            tokens,
            partial(backfill_program_trades_history, code, min_date=min_date, session=session, throttle=limiter.acquire),
        )
        if rows is None:
            symbols_failed += 1
            continue
        fetched.extend(rows)
        symbols_ok += 1
    appended_rows = append_program_trades(store_path, fetched) if fetched else 0
    logger.info(
        "[DATA] stage=toss_program_backfill status=OK symbols_ok=%d symbols_failed=%d appended_rows=%d",
        symbols_ok,
        symbols_failed,
        appended_rows,
    )
    return ProgramTradesBackfillResult(symbols_ok=symbols_ok, symbols_failed=symbols_failed, appended_rows=appended_rows)


def backfill_universe_program_trades(
    *,
    store_path: pathlib.Path,
    symbols: Sequence[str],
    lookback_days: int,
    reference_date: dt.date,
    app_key: str,
    app_secret: str,
    rate_per_s: float,
    session: Any | None = None,
    cache_dir: pathlib.Path | None = None,
    max_lead_s: float | None = None,
) -> ProgramTradesBackfillResult:
    """Backfill only universe symbols lacking the requested historical window."""
    if not symbols:
        return ProgramTradesBackfillResult(symbols_ok=0, symbols_failed=0, appended_rows=0)
    clean_symbols = tuple(code for code in symbols if is_krx_short_code(code))
    skipped = set(symbols) - set(clean_symbols)
    if skipped:
        logger.warning(
            "[DATA] stage=toss_program_backfill status=SKIP_INVALID_CODE symbols=%s", sorted(skipped)
        )
    if not clean_symbols:
        return ProgramTradesBackfillResult(symbols_ok=0, symbols_failed=0, appended_rows=0)
    min_date = reference_date - dt.timedelta(days=lookback_days)
    targets = symbols_needing_backfill(store_path, clean_symbols, min_date)
    if not targets:
        return ProgramTradesBackfillResult(symbols_ok=0, symbols_failed=0, appended_rows=0)
    return backfill_program_trades(
        store_path=store_path,
        symbols=targets,
        min_date=min_date,
        app_key=app_key,
        app_secret=app_secret,
        rate_per_s=rate_per_s,
        session=session,
        cache_dir=cache_dir,
        max_lead_s=max_lead_s,
    )
