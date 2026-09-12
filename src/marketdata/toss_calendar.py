"""Toss OpenAPI 영업일 캘린더 (수치 혼입 금지, 메타데이터만 제공)."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.core.errors import KrxAlphaError

TOSS_TOKEN_URL: str = "https://openapi.tossinvest.com/oauth2/token"  # noqa: S105 - public endpoint, not a secret
TOSS_CALENDAR_URL: str = "https://openapi.tossinvest.com/api/v1/market-calendar/KR"


class TossCalendarError(KrxAlphaError):
    """Toss 캘린더 인증 실패/전송 실패/봉투 스키마 위반 fail-closed 신호."""


@dataclass(frozen=True)
class TradingDay:
    date: dt.date
    is_business_day: bool
    previous_business_day: dt.date
    next_business_day: dt.date


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.01, max=0.1),
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True,
)
def _send(session: Any, method: str, url: str, **kwargs: Any) -> Any:
    resp = getattr(session, method)(url, **kwargs)
    resp.raise_for_status()
    return resp.json()


def _send_toss(session: Any, method: str, url: str, **kwargs: Any) -> Any:
    try:
        return _send(session, method, url, **kwargs)
    except requests.RequestException as exc:
        raise TossCalendarError(f"toss request failed for {url}: {exc}") from exc


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


def fetch_trading_day(ref_date: dt.date, *, app_key: str, app_secret: str, session: Any | None = None) -> TradingDay:
    sess = session if session is not None else requests
    token = issue_access_token(app_key=app_key, app_secret=app_secret, session=sess)
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
    return TradingDay(
        date=date,
        is_business_day=integrated is not None,
        previous_business_day=previous_business_day,
        next_business_day=next_business_day,
    )
