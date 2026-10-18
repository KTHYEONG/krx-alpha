"""Toss program-trade daily history: paged backfill via the 'until' cursor (STOCK_TRADING_TREND, 10 req/s)."""

from __future__ import annotations

import datetime as dt
import pathlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import polars as pl
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt

from src.core.errors import KrxAlphaError
from src.marketdata.krx_bars import retry_wait_seconds
from src.marketdata.partitioned_store import (
    PartitionedStoreError,
    scan_month_partitions,
    upsert_month_partitions,
)
from src.marketdata.toss_auth import TOSS_TRANSIENT_EXCEPTIONS, is_auth_rejection, is_invalid_token_envelope

TOSS_PROGRAM_TRADES_URL_TEMPLATE: str = "https://openapi.tossinvest.com/api/v1/stocks/{symbol}/program-trades"
TOSS_PROGRAM_TRADES_PAGE_COUNT: int = 100

@retry(
    stop=stop_after_attempt(3),
    wait=retry_wait_seconds,
    retry=retry_if_exception_type(TOSS_TRANSIENT_EXCEPTIONS),
    reraise=True,
)
def _send_program_trades_request(sess: Any, url: str, headers: dict[str, str], params: dict[str, str]) -> Any:
    resp = sess.get(url, headers=headers, params=params)
    resp.raise_for_status()
    return resp

PROGRAM_TRADE_HISTORY_SCHEMA: dict[str, type[pl.DataType]] = {
    "symbol": pl.String,
    "date": pl.Date,
    "arbitrage_buy_volume": pl.Int64,
    "arbitrage_sell_volume": pl.Int64,
    "arbitrage_net_volume": pl.Int64,
    "non_arbitrage_buy_volume": pl.Int64,
    "non_arbitrage_sell_volume": pl.Int64,
    "non_arbitrage_net_volume": pl.Int64,
}


class TossProgramTradesError(KrxAlphaError):
    """Toss program-trade fetch/backfill fail-closed signal (auth, transport, or schema violation)."""


class _TossProgramTradesAuthRejected(TossProgramTradesError):  # noqa: N818 - internal retry signal, surfaced as TossProgramTradesError
    """Vendor auth rejection (HTTP 401 or invalid-token envelope) warranting one rotation."""


def _parse_int(value: object) -> int:
    return int(str(value))


def _parse_side_volumes(side: object) -> tuple[int, int, int]:
    if not isinstance(side, Mapping):
        raise TossProgramTradesError(f"toss program-trade side envelope invalid: {side!r}")
    try:
        buy = _parse_int(side["buyVolume"])
        sell = _parse_int(side["sellVolume"])
        net = _parse_int(side["netBuyVolume"])
    except (KeyError, TypeError, ValueError) as exc:
        raise TossProgramTradesError(f"toss program-trade volume invalid: {exc}") from exc
    if net != buy - sell:
        raise TossProgramTradesError(f"toss program-trade net identity violated: net={net} buy={buy} sell={sell}")
    return buy, sell, net


def fetch_program_trades_page(
    symbol: str, *, access_token: str, until: dt.date | None = None, session: Any | None = None
) -> tuple[tuple[dict[str, object], ...], dt.date | None]:
    """Fetch one page of Toss program-trade daily records for one symbol.

    Args:
        until: Inclusive upper date bound for backward pagination; ``None``
            requests the newest page.

    Returns:
        ``(rows, next_until)``; ``next_until`` is ``None`` when the vendor has
        no earlier page.

    Raises:
        TossProgramTradesError: On transport failure or a record whose
            reported net volume does not equal buy minus sell.
    """
    sess = session if session is not None else requests
    url = TOSS_PROGRAM_TRADES_URL_TEMPLATE.format(symbol=symbol)
    params: dict[str, str] = {"count": str(TOSS_PROGRAM_TRADES_PAGE_COUNT)}
    if until is not None:
        params["until"] = until.isoformat()
    try:
        resp = _send_program_trades_request(sess, url, {"Authorization": f"Bearer {access_token}"}, params)
        body = resp.json()
    except requests.HTTPError as exc:
        if is_auth_rejection(exc):
            raise _TossProgramTradesAuthRejected(f"toss program-trades auth rejected for {symbol}") from exc
        raise TossProgramTradesError(f"toss program-trades request failed for {symbol}: {exc}") from exc
    except requests.RequestException as exc:
        raise TossProgramTradesError(f"toss program-trades request failed for {symbol}: {exc}") from exc
    if is_invalid_token_envelope(body):
        raise _TossProgramTradesAuthRejected(f"toss program-trades auth rejected for {symbol}: invalid-token")
    try:
        result = body["result"]
        records = result["records"]
    except (KeyError, TypeError) as exc:
        raise TossProgramTradesError(f"toss program-trades envelope invalid for {symbol}: {exc}") from exc
    if not isinstance(records, list):
        raise TossProgramTradesError(f"toss program-trades records invalid for {symbol}")
    rows: list[dict[str, object]] = []
    for record in records:
        try:
            day = dt.date.fromisoformat(str(record["date"]))
            arb = record["arbitrage"]
            non_arb = record["nonArbitrage"]
        except (KeyError, TypeError, ValueError) as exc:
            raise TossProgramTradesError(f"toss program-trades record invalid for {symbol}: {exc}") from exc
        arb_buy, arb_sell, arb_net = _parse_side_volumes(arb)
        non_buy, non_sell, non_net = _parse_side_volumes(non_arb)
        rows.append(
            {
                "symbol": symbol,
                "date": day,
                "arbitrage_buy_volume": arb_buy,
                "arbitrage_sell_volume": arb_sell,
                "arbitrage_net_volume": arb_net,
                "non_arbitrage_buy_volume": non_buy,
                "non_arbitrage_sell_volume": non_sell,
                "non_arbitrage_net_volume": non_net,
            }
        )
    raw_next: object = None
    if isinstance(result, Mapping):
        raw_next = result.get("nextUntil")
    if raw_next is None or (isinstance(raw_next, str) and not raw_next.strip()):
        return tuple(rows), None
    try:
        next_until = dt.date.fromisoformat(str(raw_next))
    except ValueError as exc:
        raise TossProgramTradesError(f"toss program-trades nextUntil invalid for {symbol}: {exc}") from exc
    return tuple(rows), next_until


def backfill_program_trades_history(
    symbol: str,
    *,
    access_token: str,
    min_date: dt.date,
    session: Any | None = None,
    throttle: Callable[[], None] | None = None,
    max_pages: int = 50,
) -> tuple[dict[str, object], ...]:
    """Walk the ``until`` cursor backward until coverage reaches ``min_date``.

    Args:
        throttle: Optional callable invoked before every page request so the
            caller can enforce the vendor's per-second rate limit; this module
            stays limiter-implementation-agnostic to avoid an upward layer
            dependency on the execution-layer rate limiter.

    Returns:
        Rows with ``date >= min_date``, deduplicated by date (first-seen wins)
        and sorted ascending.

    Raises:
        TossProgramTradesError: On a page fetch failure, or when pagination
            does not make progress (repeated or missing cursor) before
            reaching ``min_date`` within ``max_pages``.
    """
    by_date: dict[dt.date, dict[str, object]] = {}
    until: dt.date | None = None
    for _ in range(max_pages):
        if throttle is not None:
            throttle()
        rows, next_until = fetch_program_trades_page(symbol, access_token=access_token, until=until, session=session)
        if not rows:
            break
        days: list[dt.date] = []
        for row in rows:
            day = row["date"]
            assert isinstance(day, dt.date)
            days.append(day)
            if day not in by_date:
                by_date[day] = row
        # 커서는 과거로만 진행하므로 한 페이지에 min_date 이전 날짜가 하나라도 있으면 더 오래된 페이지는 불필요하다.
        if any(day < min_date for day in days):
            break
        if next_until is None:
            break
        if next_until == until:
            raise TossProgramTradesError(f"toss program-trades pagination stalled for {symbol} at {until}")
        until = next_until
    else:
        raise TossProgramTradesError(f"toss program-trades pagination exceeded max_pages={max_pages} for {symbol}")
    return tuple(by_date[day] for day in sorted(by_date) if day >= min_date)


def append_program_trades(store_path: pathlib.Path, rows: Sequence[Mapping[str, object]]) -> int:
    """Idempotently upsert program-trade history rows keyed by (symbol, date).

    Args:
        store_path: Month-partition root directory (``YYYY-MM.parquet`` files).

    Raises:
        TossProgramTradesError: If the store exists but cannot be read as
            a matching Parquet store.
    """
    if not rows:
        return 0
    store = pathlib.Path(store_path)
    if store.exists() and not store.is_dir():
        raise TossProgramTradesError(f"toss program-trades store unreadable at {store_path}: not a directory")
    columns = list(PROGRAM_TRADE_HISTORY_SCHEMA)
    incoming = pl.DataFrame(
        [{key: row[key] for key in columns} for row in rows],
        schema=PROGRAM_TRADE_HISTORY_SCHEMA,
    ).select(columns)
    try:
        return upsert_month_partitions(
            pathlib.Path(store_path),
            incoming,
            key_columns=("symbol", "date"),
            sort_columns=("symbol", "date"),
        )
    except PartitionedStoreError as exc:
        raise TossProgramTradesError(f"toss program-trades store unreadable at {store_path}: {exc}") from exc


def _stored_symbol_dates(store_path: pathlib.Path) -> pl.LazyFrame:
    """Lazily scan stored ``symbol``/``date`` pairs without materializing history."""
    return scan_month_partitions(pathlib.Path(store_path)).select(["symbol", "date"])


def symbols_needing_backfill(
    store_path: pathlib.Path, symbols: Sequence[str], min_date: dt.date
) -> tuple[str, ...]:
    """Return the symbols whose stored history does not yet reach back to ``min_date``.

    A symbol needs backfill when the store has no rows for it at all, or when
    its earliest stored date is later than ``min_date``. Coverage is judged
    solely by the earliest stored date because a symbol only ever gains rows
    from a fully successful backfill run, so a partial or gapped history for
    it cannot exist.

    Raises:
        TossProgramTradesError: If the store exists but cannot be read.
    """
    if not symbols:
        return ()
    store = pathlib.Path(store_path)
    if store.exists() and not store.is_dir():
        raise TossProgramTradesError(f"toss program-trades store unreadable at {store_path}: not a directory")
    wanted = set(symbols)
    try:
        covered = (
            _stored_symbol_dates(store_path)
            .filter(pl.col("symbol").is_in(wanted))
            .group_by("symbol")
            .agg(pl.col("date").min().alias("min_date"))
            .filter(pl.col("min_date") <= min_date)
            .collect(engine="streaming")
            .get_column("symbol")
            .to_list()
        )
    except PartitionedStoreError:
        return tuple(symbols)
    except Exception as exc:
        raise TossProgramTradesError(
            f"toss program-trades store unreadable at {store_path}: {exc}"
        ) from exc
    covered_set = set(covered)
    return tuple(symbol for symbol in symbols if symbol not in covered_set)


def program_trade_coverage(
    store_path: pathlib.Path, symbols: Sequence[str]
) -> dict[str, tuple[dt.date, dt.date]]:
    """Return ``(min_date, max_date)`` per stored symbol with a bounded lazy scan."""
    wanted = set(symbols)
    if not wanted:
        return {}
    store = pathlib.Path(store_path)
    if store.exists() and not store.is_dir():
        raise TossProgramTradesError(f"toss program-trades store unreadable at {store_path}: not a directory")
    try:
        bounds = (
            _stored_symbol_dates(store_path)
            .filter(pl.col("symbol").is_in(wanted))
            .group_by("symbol")
            .agg(pl.col("date").min().alias("min_date"), pl.col("date").max().alias("max_date"))
            .collect(engine="streaming")
        )
    except PartitionedStoreError:
        return {}
    except Exception as exc:
        raise TossProgramTradesError(
            f"toss program-trades store unreadable at {store_path}: {exc}"
        ) from exc
    return {
        str(symbol): (min_date, max_date)
        for symbol, min_date, max_date in bounds.iter_rows()
    }


class ProgramTradeGapStatus(StrEnum):
    """Outcome of an interior gap check against the certified trading calendar."""

    COMPLETE = "COMPLETE"
    GAPS = "GAPS"
    CALENDAR_UNAVAILABLE = "CALENDAR_UNAVAILABLE"


@dataclass(frozen=True)
class ProgramTradeGapReport:
    """Per-symbol missing traded business days inside the checked window."""

    status: ProgramTradeGapStatus
    window: tuple[dt.date, ...]
    missing: Mapping[str, tuple[dt.date, ...]]

    @property
    def missing_pairs(self) -> int:
        return sum(len(days) for days in self.missing.values())

    @property
    def earliest_missing(self) -> dt.date | None:
        if not self.missing:
            return None
        return min(day for days in self.missing.values() for day in days)


def _certified_gap_window(
    bars_daily_dir: pathlib.Path, complete_through: dt.date, window_business_days: int
) -> tuple[dt.date, ...] | None:
    """Return a bounded business-day window ending exactly on ``complete_through``."""
    try:
        dates = (
            scan_month_partitions(pathlib.Path(bars_daily_dir), max_date=complete_through)
            .select("date")
            .unique()
            .sort("date")
            .tail(window_business_days)
            .collect(engine="streaming")
            .get_column("date")
            .to_list()
        )
    except (PartitionedStoreError, OSError, pl.exceptions.PolarsError):
        return None
    ordered = sorted(day for day in dates if isinstance(day, dt.date))
    if not ordered:
        return None
    window = tuple(ordered[-window_business_days:])
    if window[-1] != complete_through:
        return None
    return window


def _expected_traded_pairs(
    bars_daily_dir: pathlib.Path,
    coverage: Mapping[str, tuple[dt.date, dt.date]],
    window: tuple[dt.date, ...],
) -> set[tuple[str, dt.date]]:
    """Bar rows for covered symbols on positive-volume window days at or after each symbol's first stored date."""
    frame = (
        scan_month_partitions(pathlib.Path(bars_daily_dir), min_date=window[0], max_date=window[-1])
        .filter(
            pl.col("symbol").is_in(list(coverage))
            & pl.col("date").is_in(list(window))
            & (pl.col("volume") > 0)
        )
        .select("symbol", "date")
        .join(
            pl.DataFrame(
                {"symbol": list(coverage), "first_date": [bounds[0] for bounds in coverage.values()]},
                schema={"symbol": pl.String, "first_date": pl.Date},
            ).lazy(),
            on="symbol",
        )
        .filter(pl.col("date") >= pl.col("first_date"))
        .select("symbol", "date")
        .unique()
        .collect(engine="streaming")
    )
    expected: set[tuple[str, dt.date]] = set()
    for symbol, day in frame.iter_rows():
        assert isinstance(day, dt.date)
        if day >= coverage[str(symbol)][0]:
            expected.add((str(symbol), day))
    return expected


def _stored_gap_pairs(
    store_path: pathlib.Path,
    coverage: Mapping[str, tuple[dt.date, dt.date]],
    window: tuple[dt.date, ...],
    complete_through: dt.date,
) -> set[tuple[str, dt.date]]:
    """Program-trade rows for covered symbols within the checked span, lazily pruned by month.

    Raises:
        TossProgramTradesError: If the store exists but cannot be read.
    """
    store = pathlib.Path(store_path)
    if store.exists() and not store.is_dir():
        raise TossProgramTradesError(f"toss program-trades store unreadable at {store_path}: not a directory")
    try:
        if store.exists():
            # The shared scanner treats directory-listing failures as absent partitions.
            list(store.iterdir())
        frame = (
            scan_month_partitions(store, min_date=window[0], max_date=complete_through)
            .filter(pl.col("symbol").is_in(list(coverage)) & pl.col("date").is_in(list(window)))
            .select("symbol", "date")
            .unique()
            .collect(engine="streaming")
        )
    except PartitionedStoreError:
        return set()
    except Exception as exc:
        raise TossProgramTradesError(f"toss program-trades store unreadable at {store_path}: {exc}") from exc
    return {(str(symbol), day) for symbol, day in frame.iter_rows()}


def _group_missing_days(pairs: set[tuple[str, dt.date]]) -> dict[str, tuple[dt.date, ...]]:
    grouped: dict[str, list[dt.date]] = {}
    for symbol, day in pairs:
        grouped.setdefault(symbol, []).append(day)
    return {symbol: tuple(sorted(days)) for symbol, days in sorted(grouped.items())}


def find_program_trade_gaps(
    store_path: pathlib.Path,
    *,
    bars_daily_dir: pathlib.Path,
    coverage: Mapping[str, tuple[dt.date, dt.date]],
    complete_through: dt.date,
    window_business_days: int,
) -> ProgramTradeGapReport:
    """Find missing traded business days; raise TossProgramTradesError for an unreadable program store."""
    if window_business_days < 1:
        raise ValueError("window_business_days must be positive")
    window = _certified_gap_window(bars_daily_dir, complete_through, window_business_days)
    if window is None:
        return ProgramTradeGapReport(ProgramTradeGapStatus.CALENDAR_UNAVAILABLE, (), {})
    if not coverage:
        return ProgramTradeGapReport(ProgramTradeGapStatus.COMPLETE, window, {})
    try:
        expected = _expected_traded_pairs(bars_daily_dir, coverage, window)
    except (PartitionedStoreError, OSError, pl.exceptions.PolarsError):
        return ProgramTradeGapReport(ProgramTradeGapStatus.CALENDAR_UNAVAILABLE, (), {})
    stored = _stored_gap_pairs(store_path, coverage, window, complete_through)
    missing = _group_missing_days(expected - stored)
    status = ProgramTradeGapStatus.GAPS if missing else ProgramTradeGapStatus.COMPLETE
    return ProgramTradeGapReport(status, window, missing)
