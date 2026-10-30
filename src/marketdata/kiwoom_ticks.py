"""Kiwoom tick-chart (ka10079) tape transport for history backfill."""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal
from zoneinfo import ZoneInfo

from src.core.errors import KrxAlphaError

logger = logging.getLogger(__name__)

KIWOOM_BASE_URL: str = "https://api.kiwoom.com"
KIWOOM_TOKEN_URL: str = f"{KIWOOM_BASE_URL}/oauth2/token"
KIWOOM_CHART_URL: str = f"{KIWOOM_BASE_URL}/api/dostk/chart"
KIWOOM_TICK_API_ID: str = "ka10079"

_KST = ZoneInfo("Asia/Seoul")

_AUTH_RETURN_CODES: frozenset[int] = frozenset({3})
_THROTTLE_RETURN_CODE: int = 5
_TRANSIENT_RETURN_CODES: frozenset[int] = frozenset({7})  # vendor [1631] server-error fault observed 2026-10-30


class KiwoomTickTapeError(KrxAlphaError):
    """Kiwoom tape transport, envelope or auth failure."""


class KiwoomTransientError(KiwoomTickTapeError):  # noqa: N818 - spec-fixed vendor signal name
    """Vendor-side transient failure; the same cursor may be retried."""


class KiwoomAuthRejected(KiwoomTickTapeError):  # noqa: N818 - spec-fixed vendor signal name
    """Vendor auth rejection (token invalid or expired); the caller re-issues once."""


class KiwoomThrottled(KiwoomTickTapeError):  # noqa: N818 - spec-fixed vendor signal name
    """Vendor rate-limit rejection (HTTP 429 with return_code 5)."""


@dataclass(frozen=True, slots=True)
class TapeTick:
    day: dt.date
    ts_hms: int
    price: int
    volume: int


@dataclass(frozen=True)
class TickPage:
    ticks: tuple[TapeTick, ...]
    next_key: str | None


@dataclass(frozen=True)
class TapeWalk:
    complete: Mapping[dt.date, tuple[TapeTick, ...]]
    vendor_empty: frozenset[dt.date]
    edge_partial: frozenset[dt.date]
    pages: int
    termination: Literal["crossed", "tape_end", "page_budget", "oversize"]
    emitted: frozenset[dt.date] = frozenset()
    oversize_day: dt.date | None = None


class TapeBoundaryError(KiwoomTickTapeError):
    """The caller's deadline or request budget stopped an unbroken tape walk."""


def _headers_case_insensitive(headers: Mapping[str, Any] | None, name: str) -> str | None:
    if not headers:
        return None
    wanted = name.lower()
    for key, value in headers.items():
        if str(key).lower() == wanted:
            text = str(value).strip()
            return text or None
    return None


_RETURN_MSG_CHARS = 120


def _return_msg(body: Any) -> str:
    """Vendor `return_msg` collapsed to one bounded line for error text; "-" when absent."""
    raw = body.get("return_msg") if isinstance(body, Mapping) else None
    text = " ".join(str(raw).split())[:_RETURN_MSG_CHARS] if raw is not None else ""
    return text or "-"


def _return_code(body: Any) -> int | None:
    if not isinstance(body, Mapping):
        return None
    raw = body.get("return_code")
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return None


def issue_kiwoom_token(*, app_key: str, secret_key: str, session: Any, timeout_s: float) -> str:
    """POST /oauth2/token and return the bearer token; raises KiwoomTickTapeError unless return_code == 0."""
    try:
        resp = session.post(
            KIWOOM_TOKEN_URL,
            json={"grant_type": "client_credentials", "appkey": app_key, "secretkey": secret_key},
            headers={"Content-Type": "application/json;charset=UTF-8"},
            timeout=timeout_s,
        )
        body = resp.json()
    except KiwoomTickTapeError:
        raise
    except Exception as exc:
        raise KiwoomTickTapeError(f"kiwoom token request failed: {type(exc).__name__}") from exc
    code = _return_code(body)
    if int(getattr(resp, "status_code", 200)) >= 400:
        raise KiwoomTickTapeError("kiwoom token HTTP failure")
    if code != 0:
        raise KiwoomTickTapeError(f"kiwoom token issuance failed: return_code={code}")
    token = body.get("token") if isinstance(body, Mapping) else None
    if not isinstance(token, str) or not token:
        raise KiwoomTickTapeError("kiwoom token response missing token")
    return token


def _parse_tick(row: Any) -> TapeTick:
    if not isinstance(row, Mapping):
        raise KiwoomTickTapeError("kiwoom tick row is not an object")
    cntr_tm = str(row.get("cntr_tm", ""))
    if len(cntr_tm) != 14 or not cntr_tm.isdigit():
        raise KiwoomTickTapeError(f"kiwoom tick row has malformed cntr_tm: {cntr_tm!r}")
    try:
        day = dt.date(int(cntr_tm[0:4]), int(cntr_tm[4:6]), int(cntr_tm[6:8]))
    except ValueError as exc:
        raise KiwoomTickTapeError(f"kiwoom tick row has invalid date: {cntr_tm!r}") from exc
    ts_hms = int(cntr_tm[8:14])
    try:
        dt.time(int(cntr_tm[8:10]), int(cntr_tm[10:12]), int(cntr_tm[12:14]))
    except ValueError as exc:
        raise KiwoomTickTapeError("kiwoom tick row has invalid time") from exc
    raw_price = str(row.get("cur_prc", "")).strip().lstrip("+-")
    try:
        price = abs(int(raw_price))
    except ValueError as exc:
        raise KiwoomTickTapeError("kiwoom tick row has malformed cur_prc") from exc
    if price == 0:
        raise KiwoomTickTapeError("kiwoom tick row has zero cur_prc")
    try:
        volume = int(str(row.get("trde_qty", "")).strip())
    except ValueError as exc:
        raise KiwoomTickTapeError("kiwoom tick row has malformed trde_qty") from exc
    if volume <= 0:
        raise KiwoomTickTapeError("kiwoom tick row has non-positive trde_qty")
    return TapeTick(day=day, ts_hms=ts_hms, price=price, volume=volume)


def fetch_tick_page(
    symbol: str, *, access_token: str, app_key: str, next_key: str | None, session: Any, timeout_s: float
) -> TickPage:
    """POST /api/dostk/chart api-id ka10079 for one page; raises KiwoomTransientError, KiwoomThrottled, KiwoomAuthRejected or KiwoomTickTapeError."""
    if not symbol:
        raise KiwoomTickTapeError("kiwoom tick page requires a symbol")
    _ = app_key
    head_dt = dt.datetime.now(_KST).date().strftime("%Y%m%d")
    headers: dict[str, str] = {
        "Content-Type": "application/json;charset=UTF-8",
        "authorization": f"Bearer {access_token}",
        "api-id": KIWOOM_TICK_API_ID,
        "cont-yn": "Y" if next_key else "N",
    }
    if next_key:
        headers["next-key"] = next_key
    try:
        resp = session.post(
            KIWOOM_CHART_URL,
            json={"stk_cd": symbol, "base_dt": head_dt, "tic_scope": "1", "upd_stkpc_tp": "1"},
            headers=headers,
            timeout=timeout_s,
        )
    except (KiwoomThrottled, KiwoomAuthRejected):
        raise
    except KiwoomTickTapeError:
        raise
    except Exception as exc:
        import requests as _requests

        if isinstance(exc, (_requests.exceptions.Timeout, _requests.exceptions.ConnectionError)):
            raise KiwoomTransientError(f"kiwoom tick page request failed: {type(exc).__name__}") from exc
        raise KiwoomTickTapeError(f"kiwoom tick page request failed: {type(exc).__name__}") from exc
    status = int(getattr(resp, "status_code", 200) or 200)
    if status >= 500:
        raise KiwoomTransientError(f"kiwoom tick page HTTP {status}")
    try:
        body = resp.json()
    except (KiwoomThrottled, KiwoomAuthRejected):
        raise
    except KiwoomTickTapeError:
        raise
    except Exception as exc:
        raise KiwoomTickTapeError(f"kiwoom tick page request failed: {type(exc).__name__}") from exc
    code = _return_code(body)
    if status == 429 and code == _THROTTLE_RETURN_CODE:
        raise KiwoomThrottled("kiwoom tick page throttled: return_code=5")
    if status == 401:
        raise KiwoomAuthRejected("kiwoom tick page auth rejected: http 401")
    if code is None:
        raise KiwoomTickTapeError("kiwoom tick page response missing return_code")
    if code != 0:
        if code in _AUTH_RETURN_CODES:
            raise KiwoomAuthRejected(f"kiwoom tick page auth rejected: return_code={code}")
        position = "first" if next_key is None else "continued"
        if code in _TRANSIENT_RETURN_CODES:
            raise KiwoomTransientError(
                f"kiwoom tick page failed: return_code={code} return_msg={_return_msg(body)} page={position}"
            )
        raise KiwoomTickTapeError(
            f"kiwoom tick page failed: return_code={code} return_msg={_return_msg(body)} page={position}"
        )
    if status >= 400:
        raise KiwoomTickTapeError("kiwoom tick page HTTP failure")
    rows = body.get("stk_tic_chart_qry")
    if not isinstance(rows, list):
        raise KiwoomTickTapeError("kiwoom tick page rows are not a list")
    ticks = tuple(_parse_tick(row) for row in rows)
    cont = _headers_case_insensitive(getattr(resp, "headers", None), "cont-yn")
    continuation = (
        _headers_case_insensitive(getattr(resp, "headers", None), "next-key") if cont == "Y" else None
    )
    if cont == "Y" and not continuation:
        raise KiwoomTickTapeError("kiwoom continuation missing next-key")
    return TickPage(ticks=ticks, next_key=continuation)


def walk_symbol_tape(
    symbol: str,
    *,
    wanted_days: frozenset[dt.date],
    fetch_page: Callable[[str | None], TickPage],
    max_pages: int,
    on_day_complete: Callable[[dt.date, tuple[TapeTick, ...]], None] | None = None,
    max_ticks_per_day: int | None = None,
) -> TapeWalk:
    """Walk one symbol's tape newest-first until older than min(wanted_days), the tape ends, or max_pages; return certified days.

    With ``on_day_complete`` each certified day is delivered chronologically as soon as an older tick proves it whole,
    then dropped from memory and reported in ``TapeWalk.emitted`` instead of ``complete``.
    """
    _ = symbol
    if max_pages < 1:
        raise ValueError("max_pages must be >= 1")
    if not wanted_days:
        return TapeWalk(complete={}, vendor_empty=frozenset(), edge_partial=frozenset(), pages=0, termination="crossed")
    floor = min(wanted_days)
    per_day: dict[dt.date, list[TapeTick]] = {day: [] for day in wanted_days}
    newest: dt.date | None = None
    oldest: dt.date | None = None
    previous: tuple[dt.date, int] | None = None
    cursors: set[str] = set()
    emitted: set[dt.date] = set()
    pages = 0
    next_key: str | None = None
    termination: Literal["crossed", "tape_end", "page_budget", "oversize"] = "page_budget"
    oversize_day: dt.date | None = None
    while pages < max_pages:
        try:
            page = fetch_page(next_key)
        except TapeBoundaryError:
            break
        pages += 1
        for tick in page.ticks:
            stamp = (tick.day, tick.ts_hms)
            if previous is not None and stamp > previous:
                raise KiwoomTickTapeError("kiwoom tape is not newest-first")
            previous = stamp
            newest = tick.day if newest is None else max(newest, tick.day)
            oldest = tick.day if oldest is None else min(oldest, tick.day)
            if tick.day in per_day:
                per_day[tick.day].append(tick)
        if on_day_complete is not None and oldest is not None:
            for day in sorted(d for d, ticks in per_day.items() if ticks and d > oldest):
                on_day_complete(day, tuple(reversed(per_day[day])))
                per_day[day] = []
                emitted.add(day)
        if max_ticks_per_day is not None:
            breached = [day for day in wanted_days if len(per_day[day]) > max_ticks_per_day]
            if breached:
                oversize_day = max(breached)
                termination = "oversize"
                break
        if oldest is not None and oldest < floor:
            termination = "crossed"
            break
        if page.next_key is None:
            termination = "tape_end"
            break
        if not page.ticks or page.next_key in cursors:
            raise KiwoomTickTapeError("kiwoom tape cursor made no progress")
        cursors.add(page.next_key)
        next_key = page.next_key
    else:
        termination = "page_budget"
    complete: dict[dt.date, tuple[TapeTick, ...]] = {}
    vendor_empty: set[dt.date] = set()
    eligible = {day for day in wanted_days - emitted if oversize_day is None or day > oversize_day}
    for day in eligible:
        rows = per_day[day]
        older = oldest is not None and oldest < day
        newer = newest is not None and newest > day
        if rows and older:
            complete[day] = tuple(reversed(rows))
        elif not rows and newer and older:
            vendor_empty.add(day)
    edge_partial: set[dt.date] = set()
    if termination == "tape_end" and oldest is not None:
        edge = oldest
        edge_partial.add(edge)
        complete.pop(edge, None)
        vendor_empty.discard(edge)
        for day in wanted_days - emitted:
            if not per_day[day] and day < edge:
                vendor_empty.add(day)
    return TapeWalk(
        complete=complete,
        vendor_empty=frozenset(vendor_empty),
        edge_partial=frozenset(edge_partial),
        pages=pages,
        termination=termination,
        emitted=frozenset(emitted),
        oversize_day=oversize_day,
    )
