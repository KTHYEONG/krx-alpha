"""Toss 1-minute candle transport and backward history walk."""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo

import requests

from src.core.errors import KrxAlphaError
from src.marketdata.toss_auth import is_auth_rejection, is_invalid_token_envelope

TOSS_CANDLES_URL: str = "https://openapi.tossinvest.com/api/v1/candles"
TOSS_CANDLES_INTERVAL: str = "1m"
TOSS_CANDLES_COUNT: int = 200
TOSS_MARKET_DATA_CHART_GROUP: str = "MARKET_DATA_CHART"

_KST = ZoneInfo("Asia/Seoul")
_DAY_START = dt.time(9, 0)
_DAY_END = dt.time(15, 30)


class TossCandlesError(KrxAlphaError):
    """Toss candle fetch or walk fail-closed signal."""


class _TossCandlesAuthRejected(TossCandlesError):  # noqa: N818 - internal retry signal
    """Vendor auth rejection warranting one token rotation."""


class MinuteBackfillBoundaryError(TossCandlesError):
    """The caller stopped admission at its deadline or request budget."""


@dataclass(frozen=True)
class MinuteCandle:
    ts: dt.datetime
    open: int
    high: int
    low: int
    close: int
    volume: int


@dataclass(frozen=True)
class MinuteWalk:
    complete: Mapping[dt.date, tuple[MinuteCandle, ...]]
    vendor_empty: frozenset[dt.date]
    empty_reasons: Mapping[dt.date, str]
    discarded_offhours: int
    calls: int


def _parse_ts(value: object) -> dt.datetime:
    if not isinstance(value, str) or not value.strip():
        raise TossCandlesError(f"toss candle timestamp invalid: {value!r}")
    try:
        moment = dt.datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise TossCandlesError(f"toss candle timestamp invalid: {value!r}") from exc
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=_KST)
    return moment.astimezone(_KST)


def _parse_non_negative_int(value: object, name: str) -> int:
    if isinstance(value, bool):
        raise TossCandlesError(f"toss candle field invalid: {name}={value!r}")
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise TossCandlesError(f"toss candle field invalid: {name}={value!r}") from exc
    if parsed < 0:
        raise TossCandlesError(f"toss candle field invalid: {name}={value!r}")
    return parsed


def _parse_candle(symbol: str, row: Any) -> MinuteCandle:
    if not isinstance(row, Mapping):
        raise TossCandlesError(f"toss candle row invalid for {symbol}")
    ts = _parse_ts(row.get("timestamp"))
    try:
        cand = MinuteCandle(
            ts=ts,
            open=_parse_non_negative_int(row["openPrice"], "openPrice"),
            high=_parse_non_negative_int(row["highPrice"], "highPrice"),
            low=_parse_non_negative_int(row["lowPrice"], "lowPrice"),
            close=_parse_non_negative_int(row["closePrice"], "closePrice"),
            volume=_parse_non_negative_int(row["volume"], "volume"),
        )
    except KeyError as exc:
        raise TossCandlesError(f"toss candle record invalid for {symbol}: {exc}") from exc
    return cand


def fetch_minute_candles(
    symbol: str,
    *,
    access_token: str,
    before: dt.datetime | None,
    session: Any,
    timeout_s: float,
) -> tuple[MinuteCandle, ...]:
    """GET /api/v1/candles interval=1m count=200 adjusted=false; returns candles newest first; raises TossCandlesError."""
    if not symbol:
        raise TossCandlesError("toss candles require a symbol")
    params: dict[str, str] = {
        "symbol": symbol,
        "interval": TOSS_CANDLES_INTERVAL,
        "count": str(TOSS_CANDLES_COUNT),
        "adjusted": "false",
    }
    if before is not None:
        moment = before.astimezone(_KST) if before.tzinfo is not None else before.replace(tzinfo=_KST)
        params["before"] = moment.isoformat()
    try:
        resp = session.get(
            TOSS_CANDLES_URL,
            headers={"Authorization": f"Bearer {access_token}"},
            params=params,
            timeout=timeout_s,
        )
        resp.raise_for_status()
        body = resp.json()
    except requests.HTTPError as exc:
        if is_auth_rejection(exc):
            raise _TossCandlesAuthRejected(f"toss candles auth rejected for {symbol}") from exc
        raise TossCandlesError(f"toss candles request failed for {symbol}: {exc}") from exc
    except requests.RequestException as exc:
        raise TossCandlesError(f"toss candles request failed for {symbol}: {exc}") from exc
    except ValueError as exc:
        raise TossCandlesError(f"toss candles response is not JSON for {symbol}") from exc
    if is_invalid_token_envelope(body):
        raise _TossCandlesAuthRejected(f"toss candles auth rejected for {symbol}: invalid-token")
    try:
        result = body["result"]
        rows = result["candles"]
    except (KeyError, TypeError) as exc:
        raise TossCandlesError(f"toss candles envelope invalid for {symbol}: {exc}") from exc
    if not isinstance(rows, list):
        raise TossCandlesError(f"toss candles records invalid for {symbol}")
    return tuple(_parse_candle(symbol, row) for row in rows)


def _in_regular_session(moment: dt.datetime) -> bool:
    wall = moment.astimezone(_KST).time()
    return _DAY_START <= wall <= _DAY_END


def walk_minute_history(
    symbol: str,
    *,
    wanted_days: frozenset[dt.date],
    fetch: Callable[[dt.datetime | None], tuple[MinuteCandle, ...]],
    max_calls: int,
) -> MinuteWalk:
    """Page backward from 15:30:59 KST of max(wanted_days) to below min(wanted_days); return certified days."""
    _ = symbol
    if max_calls < 1:
        raise ValueError("max_calls must be >= 1")
    if not wanted_days:
        return MinuteWalk(complete={}, vendor_empty=frozenset(), empty_reasons={}, discarded_offhours=0, calls=0)
    floor = min(wanted_days)
    start = dt.datetime.combine(max(wanted_days), dt.time(15, 30, 59), tzinfo=_KST)
    per_day: dict[dt.date, list[MinuteCandle]] = {day: [] for day in wanted_days}
    seen: set[dt.datetime] = set()
    discarded = 0
    newest: dt.date | None = None
    oldest: dt.date | None = None
    calls = 0
    before: dt.datetime | None = start
    previous: dt.datetime | None = None
    empty_terminated = False
    while calls < max_calls:
        try:
            page = fetch(before)
        except MinuteBackfillBoundaryError:
            break
        calls += 1
        if not page:
            empty_terminated = True
            break
        rows_added = 0
        page_min: dt.datetime | None = None
        for candle in page:
            if candle.ts in seen:
                continue
            if previous is not None and candle.ts > previous:
                raise TossCandlesError("toss candles are not newest-first")
            previous = candle.ts
            seen.add(candle.ts)
            newest = candle.ts.date() if newest is None else max(newest, candle.ts.date())
            oldest = candle.ts.date() if oldest is None else min(oldest, candle.ts.date())
            page_min = candle.ts if page_min is None else min(page_min, candle.ts)
            if not _in_regular_session(candle.ts):
                discarded += 1
                continue
            if candle.ts.date() in per_day:
                per_day[candle.ts.date()].append(candle)
                rows_added += 1
        if oldest is not None and oldest < floor:
            break
        if page_min is not None and before is not None and page_min >= before and rows_added == 0:
            raise TossCandlesError("toss candles pagination made no progress")
        if page_min is None:
            raise TossCandlesError("toss candles pagination made no progress")
        before = page_min - dt.timedelta(microseconds=1)
    complete: dict[dt.date, tuple[MinuteCandle, ...]] = {}
    vendor_empty: set[dt.date] = set()
    reasons: dict[dt.date, str] = {}
    for day in wanted_days:
        rows = per_day[day]
        older = oldest is not None and oldest < day
        newer = newest is not None and newest > day
        if rows and (older or empty_terminated):
            complete[day] = tuple(sorted(rows, key=lambda c: c.ts))
        elif not rows and ((newer and older) or (empty_terminated and (oldest is None or day <= oldest))):
            vendor_empty.add(day)
            reasons[day] = "before_vendor_history" if empty_terminated and not (newer and older) else "bracketed_empty"
    return MinuteWalk(
        complete=complete,
        vendor_empty=frozenset(vendor_empty),
        empty_reasons=reasons,
        discarded_offhours=discarded,
        calls=calls,
    )
