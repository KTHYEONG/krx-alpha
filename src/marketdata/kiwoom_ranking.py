"""Kiwoom ka10024 KRX trade-amount ranking pages for intraday discovery."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from deploy.shared.host_broker_contract import AsyncHostPacer
from src.core.symbols import is_krx_short_code
from src.marketdata.kiwoom_ticks import (
    KIWOOM_CHART_URL,
    KiwoomAuthRejected,
    KiwoomThrottled,
    KiwoomTickTapeError,
    KiwoomTransientError,
    _headers_case_insensitive,
    _return_code,
    _return_msg,
)

logger = logging.getLogger(__name__)

KIWOOM_RANKING_API_ID: str = "ka10024"
KIWOOM_RANKING_URL: str = f"{KIWOOM_CHART_URL.rsplit('/', 1)[0]}/rkinfo"
KIWOOM_RANKING_LIST_KEY: str = "trde_prica_upr"
KIWOOM_RANKING_STEX_TP: str = "1"
KIWOOM_RANKING_MARKET: str = "000"
KIWOOM_TRADE_AMOUNT_UNIT_KRW: int = 1_000_000
KIWOOM_THROTTLE_RETRIES: int = 3
KIWOOM_THROTTLE_BACKOFF_S: float = 1.2
_TRANSIENT_RETURN_CODES: frozenset[int] = frozenset({7})
_AUTH_RETURN_CODES: frozenset[int] = frozenset({3})
_THROTTLE_RETURN_CODE: int = 5


@dataclass(frozen=True)
class KiwoomRankingRow:
    rank: int
    symbol: str
    last_price: int
    change_pct: float
    trade_value_krw: int


@dataclass(frozen=True)
class _RankingPage:
    rows: tuple[KiwoomRankingRow, ...]
    next_key: str | None


def _parse_row(raw: Mapping[str, Any]) -> KiwoomRankingRow | None:
    stk_cd = str(raw.get("stk_cd", "")).split("_", 1)[0].strip().zfill(6)
    if not is_krx_short_code(stk_cd):
        logger.warning("[DATA] stage=intraday_ranking status=ROW_REJECTED reason=symbol")
        return None
    try:
        return KiwoomRankingRow(
            rank=int(str(raw["rank"]).strip()),
            symbol=stk_cd,
            last_price=abs(int(str(raw["cur_prc"]).strip())),
            change_pct=float(str(raw["flu_rt"]).strip()),
            trade_value_krw=int(str(raw["trde_amt"]).strip()) * KIWOOM_TRADE_AMOUNT_UNIT_KRW,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise KiwoomTickTapeError(f"kiwoom ranking row malformed: {type(exc).__name__}") from exc


def _post_page(
    *, session: Any, access_token: str, next_key: str | None, timeout_s: float
) -> _RankingPage:
    headers: dict[str, str] = {
        "Content-Type": "application/json;charset=UTF-8",
        "authorization": f"Bearer {access_token}",
        "api-id": KIWOOM_RANKING_API_ID,
        "cont-yn": "Y" if next_key else "N",
    }
    if next_key:
        headers["next-key"] = next_key
    try:
        resp = session.post(
            KIWOOM_RANKING_URL,
            json={"mrkt_tp": KIWOOM_RANKING_MARKET, "stex_tp": KIWOOM_RANKING_STEX_TP},
            headers=headers,
            timeout=timeout_s,
        )
    except KiwoomTickTapeError:
        raise
    except Exception as exc:
        raise KiwoomTransientError(f"kiwoom ranking request failed: {type(exc).__name__}") from exc
    status = int(getattr(resp, "status_code", 200) or 200)
    if status >= 500:
        raise KiwoomTransientError(f"kiwoom ranking HTTP {status}")
    if status == 401:
        raise KiwoomAuthRejected("kiwoom ranking auth rejected: http 401")
    try:
        body = resp.json()
    except Exception as exc:
        raise KiwoomTickTapeError(f"kiwoom ranking response unreadable: {type(exc).__name__}") from exc
    code = _return_code(body)
    if status == 429 and code == _THROTTLE_RETURN_CODE:
        raise KiwoomThrottled("kiwoom ranking throttled: return_code=5")
    if code is None:
        raise KiwoomTickTapeError("kiwoom ranking response missing return_code")
    if code != 0:
        if code in _AUTH_RETURN_CODES:
            raise KiwoomAuthRejected(f"kiwoom ranking auth rejected: return_code={code}")
        if code in _TRANSIENT_RETURN_CODES:
            raise KiwoomTransientError(
                f"kiwoom ranking failed: return_code={code} return_msg={_return_msg(body)}"
            )
        raise KiwoomTickTapeError(f"kiwoom ranking failed: return_code={code} return_msg={_return_msg(body)}")
    raw_rows = body.get(KIWOOM_RANKING_LIST_KEY) if isinstance(body, Mapping) else None
    if not isinstance(raw_rows, list):
        raise KiwoomTickTapeError("kiwoom ranking rows are not a list")
    rows = tuple(row for row in (_parse_row(item) for item in raw_rows) if row is not None)
    cont = _headers_case_insensitive(getattr(resp, "headers", None), "cont-yn")
    continuation = _headers_case_insensitive(getattr(resp, "headers", None), "next-key") if cont == "Y" else None
    if cont == "Y" and not continuation:
        raise KiwoomTickTapeError("kiwoom ranking continuation missing next-key")
    return _RankingPage(rows=rows, next_key=continuation)


def _post_page_with_throttle_retry(
    *,
    session: Any,
    access_token: str,
    next_key: str | None,
    timeout_s: float,
    pacer: AsyncHostPacer,
    sleep: Callable[[float], None],
) -> _RankingPage:
    retries = 0
    while True:
        asyncio.run(pacer.acquire())
        try:
            return _post_page(session=session, access_token=access_token, next_key=next_key, timeout_s=timeout_s)
        except KiwoomThrottled:
            retries += 1
            if retries > KIWOOM_THROTTLE_RETRIES:
                raise
            sleep(KIWOOM_THROTTLE_BACKOFF_S)


def fetch_trade_amount_ranking(
    *,
    session: Any,
    access_token: str,
    app_key: str,
    pacer: AsyncHostPacer,
    max_pages: int,
    min_trade_value_krw: int,
    timeout_s: float,
) -> tuple[KiwoomRankingRow, ...]:
    """ka10024 KRX-only ranking pages until max_pages or a page tail below min_trade_value_krw; raises KiwoomTickTapeError family on vendor failure."""
    _ = app_key
    if max_pages < 1:
        raise ValueError("max_pages must be >= 1")
    collected: list[KiwoomRankingRow] = []
    seen: set[str] = set()
    last_rank = 0
    next_key: str | None = None
    for _page_index in range(max_pages):
        page = _post_page_with_throttle_retry(
            session=session,
            access_token=access_token,
            next_key=next_key,
            timeout_s=timeout_s,
            pacer=pacer,
            sleep=time.sleep,
        )
        for row in page.rows:
            if row.rank <= last_rank:
                raise KiwoomTickTapeError(f"kiwoom ranking ranks not strictly increasing at rank={row.rank}")
            last_rank = row.rank
            if row.symbol in seen:
                continue
            seen.add(row.symbol)
            collected.append(row)
        tail_below = not page.rows or page.rows[-1].trade_value_krw < min_trade_value_krw
        if tail_below or page.next_key is None:
            break
        next_key = page.next_key
    return tuple(collected)
