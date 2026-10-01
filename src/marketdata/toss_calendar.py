"""Toss OpenAPI 영업일 캘린더 (수치 혼입 금지, 메타데이터만 제공)."""

from __future__ import annotations

import datetime as dt
import json
import os
import pathlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo

import requests
from tenacity import retry, retry_if_exception, stop_after_attempt

from src.core.config import KisTokenSettings
from src.core.errors import KrxAlphaError
from src.core.session_anchors import (
    STANDARD_AFTER_MARKET_END,
    AnchorSource,
    SessionAnchorError,
    SessionAnchors,
)
from src.marketdata.krx_bars import retry_wait_seconds
from src.marketdata.toss_auth import is_auth_rejection, is_client_error, is_invalid_token_envelope
from src.marketdata.toss_token_store import IssuedToken, TossTokenStore, toss_token_path

TOSS_TOKEN_URL: str = "https://openapi.tossinvest.com/oauth2/token"  # noqa: S105 - public endpoint, not a secret
TOSS_CALENDAR_URL: str = "https://openapi.tossinvest.com/api/v1/market-calendar/KR"
TOSS_STOCK_TRADING_TREND_GROUP: str = "STOCK_TRADING_TREND"
"""Toss rate group scoping the shared program-trades pacing file."""
TOSS_STOCK_TRADING_TREND_RATE_PER_S: float = 10.0
"""Documented vendor contract rate for the STOCK_TRADING_TREND group."""


class TossCalendarError(KrxAlphaError):
    """Toss 캘린더 인증 실패/전송 실패/봉투 스키마 위반 fail-closed 신호."""


class _TossAuthRejected(TossCalendarError):  # noqa: N818 - internal retry signal, surfaced as TossCalendarError
    """Vendor auth rejection (HTTP 401 or invalid-token envelope) warranting one rotation."""


@dataclass(frozen=True)
class TradingDay:
    date: dt.date
    is_business_day: bool
    previous_business_day: dt.date
    next_business_day: dt.date
    anchors: SessionAnchors | None = None
    next_anchors: SessionAnchors | None = None


_KST = ZoneInfo("Asia/Seoul")


def _parse_vendor_time(day: dt.date, value: Any) -> dt.time | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        moment = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if moment.tzinfo is None:
        return None
    kst = moment.astimezone(_KST)
    if kst.date() != day:
        return None
    return kst.time()


def parse_session_anchors(day: dt.date, integrated: Mapping[str, Any] | None) -> SessionAnchors | None:
    """Parse Toss ``integrated`` session times into anchors for ``day``.

    Returns ``None`` for holidays (``integrated is None``) and for any
    malformed, missing, or cross-date time field, so a partial vendor payload
    degrades to the standard schedule instead of invalidating the business-day
    decision.
    """
    if integrated is None:
        return None
    try:
        regular = integrated.get("regularMarket")
        if not isinstance(regular, Mapping):
            return None
        regular_open = _parse_vendor_time(day, regular.get("startTime"))
        closing_auction_start = _parse_vendor_time(day, regular.get("singlePriceAuctionStartTime"))
        regular_close = _parse_vendor_time(day, regular.get("endTime"))
        if regular_open is None or closing_auction_start is None or regular_close is None:
            return None
        after = integrated.get("afterMarket")
        after_market_end: dt.time | None
        if after is None:
            after_market_end = STANDARD_AFTER_MARKET_END
        else:
            if not isinstance(after, Mapping):
                return None
            after_market_end = _parse_vendor_time(day, after.get("endTime"))
            if after_market_end is None:
                return None
        return SessionAnchors(
            date=day,
            regular_open=regular_open,
            closing_auction_start=closing_auction_start,
            regular_close=regular_close,
            after_market_end=after_market_end,
            source=AnchorSource.VENDOR,
        )
    except (SessionAnchorError, AttributeError, TypeError, ValueError):
        return None


@retry(
    stop=stop_after_attempt(3),
    wait=retry_wait_seconds,
    retry=retry_if_exception(lambda exc: isinstance(exc, requests.RequestException) and not is_client_error(exc)),
    reraise=True,
)
def _send(session: Any, method: str, url: str, **kwargs: Any) -> Any:
    resp = getattr(session, method)(url, **kwargs)
    resp.raise_for_status()
    return resp.json()


def _send_toss(session: Any, method: str, url: str, **kwargs: Any) -> Any:
    try:
        body = _send(session, method, url, **kwargs)
    except requests.RequestException as exc:
        if is_auth_rejection(exc):
            raise _TossAuthRejected(f"toss auth rejected for {url}") from exc
        raise TossCalendarError(f"toss request failed for {url}: {exc}") from exc
    if is_invalid_token_envelope(body):
        raise _TossAuthRejected(f"toss auth rejected for {url}: invalid-token")
    return body


def issue_access_token(*, app_key: str, app_secret: str, session: Any | None = None) -> str:
    sess = session if session is not None else requests
    body = _send_toss(
        sess,
        "post",
        TOSS_TOKEN_URL,
        data={"grant_type": "client_credentials", "client_id": app_key, "client_secret": app_secret},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=10,
    )
    try:
        token = body["access_token"]
    except (KeyError, TypeError) as exc:
        raise TossCalendarError("toss token response missing access_token") from exc
    return str(token)


def _resolve_toss_cache_dir(cache_dir: pathlib.Path | None) -> pathlib.Path:
    if cache_dir is not None:
        return pathlib.Path(cache_dir)
    return KisTokenSettings().token_cache_dir


def _fetch_with_token(sess: Any, ref_date: dt.date, token: str) -> TradingDay:
    body = _send_toss(
        sess,
        "get",
        TOSS_CALENDAR_URL,
        params={"date": ref_date.isoformat()},
        headers={"Authorization": f"Bearer {token}", "Accept-Encoding": "gzip"},
        timeout=10,
    )
    try:
        result = body["result"]
        today = result["today"]
        prev = result["previousBusinessDay"]
        nxt = result["nextBusinessDay"]
        integrated = today["integrated"]
        date = dt.date.fromisoformat(today["date"])
        previous_business_day = dt.date.fromisoformat(prev["date"])
        next_business_day = dt.date.fromisoformat(nxt["date"])
    except (KeyError, TypeError, ValueError) as exc:
        raise TossCalendarError(f"toss calendar envelope invalid for {ref_date}: {exc}") from exc
    next_integrated = nxt.get("integrated") if isinstance(nxt, dict) else None
    return TradingDay(
        date=date,
        is_business_day=integrated is not None,
        previous_business_day=previous_business_day,
        next_business_day=next_business_day,
        anchors=parse_session_anchors(date, integrated if isinstance(integrated, Mapping) or integrated is None else None),
        next_anchors=parse_session_anchors(
            next_business_day, next_integrated if isinstance(next_integrated, Mapping) or next_integrated is None else None
        ),
    )


def fetch_trading_day(
    ref_date: dt.date,
    *,
    app_key: str,
    app_secret: str,
    session: Any | None = None,
    cache_dir: pathlib.Path | None = None,
    token_store: TossTokenStore | None = None,
) -> TradingDay:
    """Fetch the trading-day envelope with a host-shared Toss token.

    Every issuance goes through the shared token store so concurrent KRX and
    KCA processes never revoke each other's live token. A 401 or
    ``invalid-token`` response rotates once via the store and retries the same
    request once; a second rejection raises.
    """
    sess = session if session is not None else requests
    store = (
        token_store
        if token_store is not None
        else TossTokenStore(toss_token_path(_resolve_toss_cache_dir(cache_dir), app_key))
    )

    def _issue() -> IssuedToken:
        return IssuedToken(issue_access_token(app_key=app_key, app_secret=app_secret, session=sess), None)

    token = store.get_or_issue(_issue)
    try:
        return _fetch_with_token(sess, ref_date, token)
    except _TossAuthRejected:
        rotated = store.replace_rejected(token, _issue)
        return _fetch_with_token(sess, ref_date, rotated)

def save_trading_day_cache(path: pathlib.Path, day: TradingDay) -> None:
    target = pathlib.Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "date": day.date.isoformat(),
        "is_business_day": day.is_business_day,
        "previous_business_day": day.previous_business_day.isoformat(),
        "next_business_day": day.next_business_day.isoformat(),
    }
    tmp = target.parent / f".{target.name}.tmp"
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, target)


def load_trading_day_cache(path: pathlib.Path) -> TradingDay | None:
    try:
        raw = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
        return TradingDay(
            date=dt.date.fromisoformat(str(raw["date"])),
            is_business_day=bool(raw["is_business_day"]),
            previous_business_day=dt.date.fromisoformat(str(raw["previous_business_day"])),
            next_business_day=dt.date.fromisoformat(str(raw["next_business_day"])),
        )
    except (FileNotFoundError, json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None


def trading_day_from_cache(cached: TradingDay, today: dt.date) -> TradingDay | None:
    if today == cached.date:
        return cached
    last_business = cached.date if cached.is_business_day else cached.previous_business_day
    if today == cached.next_business_day:
        return TradingDay(date=today, is_business_day=True, previous_business_day=last_business, next_business_day=today)
    if cached.date < today < cached.next_business_day:
        return TradingDay(
            date=today,
            is_business_day=False,
            previous_business_day=last_business,
            next_business_day=cached.next_business_day,
        )
    return None
