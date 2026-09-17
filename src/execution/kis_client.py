"""KIS 실전 REST 클라이언트: 시세·잔고 조회와 주문 POST 경계."""

from __future__ import annotations

import datetime as dt
import fcntl
import hashlib
import json
import logging
import os
import pathlib
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, cast
from zoneinfo import ZoneInfo

import requests

from src.core.config import KisCredentials
from src.execution.contracts import (
    BrokerOrderStatus,
    BrokerOutcome,
    ExecutionError,
    Holding,
    KisApiError,
    Order,
    OrderType,
    OutcomeKind,
    Quote,
    Side,
)

logger = logging.getLogger(__name__)

KIS_LIVE_BASE_URL: str = "https://openapi.koreainvestment.com:9443"

TR_BUY: str = "TTTC0012U"
TR_SELL: str = "TTTC0011U"
TR_CANCEL: str = "TTTC0013U"
TR_DAILY_ORDERS: str = "TTTC0081R"
TR_BALANCE: str = "TTTC8434R"
TR_ORDERABLE: str = "TTTC8408R"
TR_PRICE: str = "FHKST01010100"
TR_ASKING: str = "FHKST01010200"
TR_DAILY_CHART: str = "FHKST03010100"
# 실측 정정(2026-09-17): FHPST01720000/"/ranking/trade-amount"는 공식 KIS 스펙상
# "거래대금순위"가 아니라 "호가잔량순위"이며 해당 경로는 404를 반환한다(공식
# koreainvestment/open-trading-api 재확인). 거래대금순위는 거래량순위 TR에
# FID_BLNG_CLS_CODE="3"(거래금액순)을 지정해 조회한다.
TR_TRADE_AMOUNT: str = "FHPST01710000"
TR_FLUCTUATION: str = "FHPST01700000"
TR_INVESTOR_ESTIMATE: str = "HHPTJ04160200"
TR_PROGRAM_TRADE: str = "FHPPG04650101"
TR_INDEX_PRICE: str = "FHPUP02100000"
TR_INDEX_MINUTE_CHART: str = "FHKUP03500200"
TR_MINUTE_CHART: str = "FHKST03010200"
TR_NEWS_TITLE: str = "FHKST01011800"

ORD_DVSN: dict[OrderType, str] = {OrderType.LIMIT: "00", OrderType.MARKET: "01"}

_PATH_ORDER_CASH = "/uapi/domestic-stock/v1/trading/order-cash"
_PATH_RVSECNCL = "/uapi/domestic-stock/v1/trading/order-rvsecncl"
_PATH_PRICE = "/uapi/domestic-stock/v1/quotations/inquire-price"
_PATH_ASKING = "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn"
_PATH_DAILY_CHART: str = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
_PATH_BALANCE = "/uapi/domestic-stock/v1/trading/inquire-balance"
_PATH_ORDERABLE = "/uapi/domestic-stock/v1/trading/inquire-psbl-order"
_PATH_DAILY = "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
_PATH_TOKEN = "/oauth2/tokenP"  # noqa: S105 - public endpoint, not a secret
_PATH_TRADE_AMOUNT = "/uapi/domestic-stock/v1/quotations/volume-rank"
_PATH_FLUCTUATION = "/uapi/domestic-stock/v1/ranking/fluctuation"
_PATH_INVESTOR_ESTIMATE = "/uapi/domestic-stock/v1/quotations/investor-trend-estimate"
_PATH_PROGRAM_TRADE = "/uapi/domestic-stock/v1/quotations/program-trade-by-stock"
_PATH_INDEX_PRICE = "/uapi/domestic-stock/v1/quotations/inquire-index-price"
_PATH_INDEX_MINUTE_CHART = "/uapi/domestic-stock/v1/quotations/inquire-time-indexchartprice"
_PATH_MINUTE_CHART = "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
_PATH_NEWS_TITLE = "/uapi/domestic-stock/v1/quotations/news-title"

_RATE_LIMIT_CODES: frozenset[str] = frozenset({"EGW00201"})
_EXPIRED_TOKEN_CODES: frozenset[str] = frozenset({"EGW00121", "EGW00123"})
_MAX_SAFE_RETRIES = 2
_MAX_PAGES = 100
_TOKEN_REFRESH_MARGIN: dt.timedelta = dt.timedelta(minutes=10)
_KST: dt.tzinfo = ZoneInfo("Asia/Seoul")


@dataclass(frozen=True)
class KisRankingRow:
    """KIS 랭킹 행 (거래대금·등락률 공용)."""

    symbol: str
    rank: int
    change_pct: float
    trade_value_krw: int


def _parse_ranking_rows(rows: list[dict[str, Any]]) -> tuple[KisRankingRow, ...]:
    try:
        parsed: list[KisRankingRow] = []
        seen: set[str] = set()
        ok = bool(rows)
        for index, row in enumerate(rows, start=1):
            symbol = str(row.get("stck_shrn_iscd", ""))
            change = Decimal(str(row.get("prdy_ctrt", "")))
            # 등락률 랭킹(FHPST01700000)은 acml_tr_pbmn 필드를 아예 반환하지 않는다
            # (실측 확인) -- 선택 순위는 랭크 위치로만 결정되고 이 값은 메타데이터
            # 표시용이라, 미제공을 0으로 취급해도 선정 로직에 영향이 없다.
            trade_value = int(Decimal(str(row.get("acml_tr_pbmn") or "0")))
            if not (symbol.isdigit() and len(symbol) == 6) or not change.is_finite() or trade_value < 0 or symbol in seen:
                ok = False
                break
            seen.add(symbol)
            parsed.append(KisRankingRow(symbol=symbol, rank=index, change_pct=float(change), trade_value_krw=trade_value))
        if not ok:
            raise ValueError("invalid ranking schema")
        return tuple(parsed)
    except (ValueError, KeyError, TypeError, AttributeError, ArithmeticError) as exc:
        raise KisApiError("SCHEMA", "invalid ranking schema") from exc


def _to_int(raw: Any) -> int:
    text = str(raw or "0")
    return int(Decimal(text))


def _snapshot_raw(output: Mapping[str, Any], key: str) -> Any:
    if key not in output:
        raise KisApiError("SCHEMA", f"missing snapshot field: {key}")
    return output[key]


def _snapshot_code(output: Mapping[str, Any], key: str) -> str:
    return str(_snapshot_raw(output, key)).strip()


def _snapshot_int(output: Mapping[str, Any], key: str) -> int:
    text = str(_snapshot_raw(output, key)).strip()
    try:
        value = Decimal(text)
    except (ArithmeticError, ValueError) as exc:
        raise KisApiError("SCHEMA", f"invalid int field: {key}") from exc
    if not value.is_finite() or value != value.to_integral_value():
        raise KisApiError("SCHEMA", f"invalid int field: {key}")
    return int(value)


def _snapshot_float(output: Mapping[str, Any], key: str) -> float:
    text = str(_snapshot_raw(output, key)).strip()
    try:
        value = Decimal(text)
    except (ArithmeticError, ValueError) as exc:
        raise KisApiError("SCHEMA", f"invalid float field: {key}") from exc
    if not value.is_finite():
        raise KisApiError("SCHEMA", f"invalid float field: {key}")
    return float(value)


def _snapshot_yn(output: Mapping[str, Any], key: str) -> bool:
    text = str(_snapshot_raw(output, key)).strip()
    if text == "Y":
        return True
    if text == "N":
        return False
    raise KisApiError("SCHEMA", f"invalid Y/N field: {key}")


def _snapshot_ladder_level(output: Mapping[str, Any], key: str) -> int:
    if str(_snapshot_raw(output, key)).strip() == "":
        return 0
    return _snapshot_int(output, key)


def _snapshot_news_ns(date_text: str, time_text: str) -> int:
    if len(date_text) != 8 or len(time_text) != 6 or not date_text.isdigit() or not time_text.isdigit():
        raise KisApiError("SCHEMA", "malformed news publication time")
    try:
        moment = dt.datetime(
            int(date_text[0:4]),
            int(date_text[4:6]),
            int(date_text[6:8]),
            int(time_text[0:2]),
            int(time_text[2:4]),
            int(time_text[4:6]),
            tzinfo=_KST,
        )
    except ValueError as exc:
        raise KisApiError("SCHEMA", "malformed news publication time") from exc
    delta = moment.astimezone(dt.UTC) - dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000


def kis_app_key_fingerprint(app_key: str) -> str:
    """앱키 지문 (토큰 캐시 파일명·WS lease와 공유하는 규칙)."""
    return hashlib.sha256(app_key.encode("utf-8")).hexdigest()[:12]


def kis_token_cache_path(cache_dir: pathlib.Path, app_key: str) -> pathlib.Path:
    """앱키별 canonical 토큰 캐시 경로 (token_<sha256(app_key)[:12]>.json)."""
    return pathlib.Path(cache_dir) / f"token_{kis_app_key_fingerprint(app_key)}.json"


_TOKEN_KEY_LOCKS: dict[str, threading.Lock] = {}
_TOKEN_KEY_LOCKS_GUARD = threading.Lock()


def _lock_for_token_cache(path: pathlib.Path) -> threading.Lock:
    with _TOKEN_KEY_LOCKS_GUARD:
        return _TOKEN_KEY_LOCKS.setdefault(str(path), threading.Lock())


@contextmanager
def _token_file_lock(path: pathlib.Path) -> Iterator[None]:
    """프로세스 간 토큰 발급을 직렬화하는 advisory lock."""
    lock_path = pathlib.Path(f"{path}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(lock_path, "a+b")  # noqa: SIM115 - lock handle은 context 수명 동안 유지
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


class RateLimiter:
    """토큰버킷 대체: 요청 간 최소 간격을 보장한다."""

    def __init__(
        self,
        rate_per_s: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._interval = 1.0 / rate_per_s
        self._clock = clock
        self._sleep = sleep
        self._next_allowed = float("-inf")

    def acquire(self) -> None:
        now = self._clock()
        if now < self._next_allowed:
            self._sleep(self._next_allowed - now)
            now = self._next_allowed
        self._next_allowed = now + self._interval


def order_tr_id(side: Side) -> str:
    """매수/매도 방향의 실전 주문 TR 을 반환한다."""
    if side is Side.BUY:
        return TR_BUY
    return TR_SELL


def build_order_body(creds: KisCredentials, order: Order) -> dict[str, str]:
    """공식 order-cash 바디를 조립한다."""
    intent = order.intent
    return {
        "CANO": creds.kis_account_no,
        "ACNT_PRDT_CD": creds.kis_account_product_code,
        "PDNO": intent.symbol,
        "ORD_DVSN": ORD_DVSN[intent.order_type],
        "ORD_QTY": str(intent.qty),
        "ORD_UNPR": str(intent.limit_price) if intent.order_type is OrderType.LIMIT else "0",
        "EXCG_ID_DVSN_CD": "KRX",
        "SLL_TYPE": "01" if intent.side is Side.SELL else "",
        "CNDT_PRIC": "",
    }


def build_cancel_body(creds: KisCredentials, order: Order) -> dict[str, str]:
    """전량취소(rvsecncl) 바디를 조립한다."""
    if order.broker_order_no is None or order.broker_org_no is None:
        raise ExecutionError("cancel requires broker order and org numbers")
    return {
        "CANO": creds.kis_account_no,
        "ACNT_PRDT_CD": creds.kis_account_product_code,
        "KRX_FWDG_ORD_ORGNO": order.broker_org_no,
        "ORGN_ODNO": order.broker_order_no,
        "ORD_DVSN": ORD_DVSN[order.intent.order_type],
        "RVSE_CNCL_DVSN_CD": "02",
        "ORD_QTY": "0",
        "ORD_UNPR": "0",
        "QTY_ALL_ORD_YN": "Y",
        "EXCG_ID_DVSN_CD": "KRX",
    }


class KisRestClient:
    """KIS 실전 REST 경계 (조회 GET + 주문 POST)."""

    def __init__(
        self,
        *,
        creds: KisCredentials,
        session: Any,
        token_cache_path: pathlib.Path,
        limiter: RateLimiter,
        now: Callable[[], dt.datetime],
        timeout_s: float,
        base_url: str = KIS_LIVE_BASE_URL,
        allow_token_issue: bool = True,
    ) -> None:
        self._creds = creds
        self._session = session
        self._token_cache_path = token_cache_path
        self._limiter = limiter
        self._now = now
        self._timeout_s = timeout_s
        self._base_url = base_url
        self._allow_token_issue = allow_token_issue
        self._token: str | None = None
        self._token_expires_at: dt.datetime | None = None

    def _read_valid_token(self, now: dt.datetime) -> tuple[str, dt.datetime] | None:
        """유효한 캐시 토큰을 반환하고 무효·만료 임박분은 None으로 fail-closed 처리한다."""
        try:
            cached = json.loads(self._token_cache_path.read_text(encoding="utf-8"))
            expires_at = dt.datetime.fromisoformat(
                str(cached.get("expired_at") or cached.get("expires_at"))
            )
            if cached.get("app_key") != self._creds.kis_app_key:
                return None
            token = cached.get("access_token")
            if not isinstance(token, str) or not token:
                return None
            if expires_at - now <= _TOKEN_REFRESH_MARGIN:
                return None
            return (token, expires_at)
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            return None

    def _cached_issue_day(self) -> dt.date | None:
        """캐시에 기록된 발급일(KST)을 반환한다 (legacy 캐시는 None)."""
        try:
            cached = json.loads(self._token_cache_path.read_text(encoding="utf-8"))
            return dt.datetime.fromisoformat(str(cached.get("issued_at", ""))).date()
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            return None

    def access_token(self, *, force: bool = False) -> str:
        """캐시 토큰을 반환하고 만료 임박 시에만 재발급한다."""
        now = self._now()
        if (
            not force
            and self._token is not None
            and self._token_expires_at is not None
            and self._token_expires_at - now > _TOKEN_REFRESH_MARGIN
        ):
            return self._token
        if not force:
            hit = self._read_valid_token(now)
            if hit is not None:
                self._token, self._token_expires_at = hit
                return self._token
        if not self._allow_token_issue:
            raise KisApiError("TOKEN_CACHE", "token cache missing/expired and issuance disabled")
        return self._issue_token(now, reuse_valid=not force)

    def _issue_token(self, now: dt.datetime, *, reuse_valid: bool) -> str:
        """per-key lock으로 캐시를 재검사한 뒤 atomic 0600 write로 발급한다 (당일 재발급은 force도 거부)."""
        with _token_file_lock(self._token_cache_path), _lock_for_token_cache(self._token_cache_path):
            cached = self._read_valid_token(now) if reuse_valid else None
            if cached is not None:
                self._token, self._token_expires_at = cached
                return self._token
            if self._cached_issue_day() == now.astimezone(_KST).date():
                raise KisApiError("TOKEN_DAILY_LIMIT", "token already issued today (KST)")
            return self._issue_token_unlocked(now)

    def _issue_token_unlocked(self, now: dt.datetime) -> str:
        self._limiter.acquire()
        resp = self._session.post(
            self._base_url + _PATH_TOKEN,
            json={
                "grant_type": "client_credentials",
                "appkey": self._creds.kis_app_key,
                "appsecret": self._creds.kis_app_secret,
            },
            timeout=self._timeout_s,
        )
        body = resp.json()
        if "access_token" not in body:
            raise KisApiError(str(body.get("error_code", "TOKEN")), str(body.get("error_description", "")))
        expires_at = dt.datetime.strptime(
            str(body["access_token_token_expired"]), "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=_KST)
        self._token = str(body["access_token"])
        self._token_expires_at = expires_at
        payload = json.dumps(
            {
                "access_token": self._token,
                "expired_at": expires_at.isoformat(),
                "app_key": self._creds.kis_app_key,
                "issued_at": now.isoformat(),
            }
        )
        self._token_cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._token_cache_path.parent / (self._token_cache_path.name + ".tmp")
        fd = os.open(str(tmp_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
        os.replace(str(tmp_path), str(self._token_cache_path))
        logger.debug("[EXEC] stage=token_issued expires_at=%s", expires_at.isoformat())
        return self._token

    def _headers(self, tr_id: str, tr_cont: str) -> dict[str, str]:
        token = self.access_token()
        return {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": self._creds.kis_app_key,
            "appsecret": self._creds.kis_app_secret,
            "tr_id": tr_id,
            "custtype": "P",
            "tr_cont": tr_cont,
        }

    def _get(
        self, path: str, tr_id: str, params: dict[str, str], tr_cont: str = ""
    ) -> tuple[dict[str, Any], str]:
        refreshed = False
        retries = 0
        while True:
            self._limiter.acquire()
            try:
                resp = self._session.get(
                    self._base_url + path,
                    headers=self._headers(tr_id, tr_cont),
                    params=params,
                    timeout=self._timeout_s,
                )
                body = resp.json()
            except (requests.RequestException, ValueError) as exc:
                raise KisApiError("TRANSPORT", type(exc).__name__) from exc
            msg_cd = str(body.get("msg_cd", ""))
            if msg_cd in _RATE_LIMIT_CODES and retries < _MAX_SAFE_RETRIES:
                retries += 1
                continue
            if msg_cd in _EXPIRED_TOKEN_CODES and not refreshed:
                self.access_token(force=True)
                refreshed = True
                continue
            if body.get("rt_cd") != "0":
                raise KisApiError(msg_cd, str(body.get("msg1", "")).strip())
            return body, str(resp.headers.get("tr_cont", ""))

    def _get_paged(
        self, path: str, tr_id: str, params: dict[str, str], list_key: str = "output1"
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        req_cont = ""
        for _ in range(_MAX_PAGES):
            body, cont = self._get(path, tr_id, params, req_cont)
            rows.extend(body.get(list_key) or [])
            if cont not in ("M", "F"):
                return rows
            params = {
                **params,
                "CTX_AREA_FK100": str(body.get("ctx_area_fk100", "")),
                "CTX_AREA_NK100": str(body.get("ctx_area_nk100", "")),
            }
            req_cont = "N"
        raise KisApiError("PAGINATION", f"exceeded {_MAX_PAGES} pages")

    def get_quote(self, symbol: str) -> Quote:
        """현재가+10단계 호가를 조회한다."""
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol}
        price_body, _ = self._get(_PATH_PRICE, TR_PRICE, params)
        asking_body, _ = self._get(_PATH_ASKING, TR_ASKING, params)
        output = price_body.get("output") or {}
        book = asking_body.get("output1") or {}
        asks: list[tuple[int, int]] = []
        bids: list[tuple[int, int]] = []
        for i in range(1, 11):
            ask_price = _to_int(book.get(f"askp{i}", "0"))
            if ask_price > 0:
                asks.append((ask_price, _to_int(book.get(f"askp_rsqn{i}", "0"))))
            bid_price = _to_int(book.get(f"bidp{i}", "0"))
            if bid_price > 0:
                bids.append((bid_price, _to_int(book.get(f"bidp_rsqn{i}", "0"))))
        quote = Quote(
            symbol=symbol,
            last=_to_int(output.get("stck_prpr", "0")),
            upper_limit=_to_int(output.get("stck_mxpr", "0")),
            lower_limit=_to_int(output.get("stck_llam", "0")),
            tick=_to_int(output.get("aspr_unit", "0")),
            halted=str(output.get("temp_stop_yn", "N")) == "Y",
            asks=tuple(asks),
            bids=tuple(bids),
        )
        logger.debug(
            "[EXEC] stage=quote symbol=%s last=%d halted=%s", symbol, quote.last, str(quote.halted)
        )
        return quote

    def get_daily_bar(self, symbol: str, day: dt.date) -> dict[str, str] | None:
        """일자별 itemchartprice(output2 선두행)를 조회한다 (무효 심볼의 전필드 0 응답은 None)."""
        stamp = day.strftime("%Y%m%d")
        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": symbol,
            "fid_input_date_1": stamp,
            "fid_input_date_2": stamp,
            "fid_period_div_code": "D",
            "fid_org_adj_prc": "1",
        }
        body, _ = self._get(_PATH_DAILY_CHART, TR_DAILY_CHART, params)
        rows = body.get("output2") or []
        if not rows:
            return None
        row = rows[0]
        if str(row.get("stck_clpr", "0")) == "0":
            return None
        return cast("dict[str, str]", row)

    def get_holdings(self) -> list[Holding]:
        """실계좌 보유잔고를 조회한다."""
        params = {
            "CANO": self._creds.kis_account_no,
            "ACNT_PRDT_CD": self._creds.kis_account_product_code,
            "AFHR_FLPR_YN": "N",
            "OFL_YN": "",
            "INQR_DVSN": "02",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "00",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }
        rows = self._get_paged(_PATH_BALANCE, TR_BALANCE, params)
        holdings: list[Holding] = []
        for row in rows:
            qty = _to_int(row.get("hldg_qty", "0"))
            if qty > 0:
                holdings.append(
                    Holding(
                        symbol=str(row.get("pdno", "")),
                        qty=qty,
                        cost_krw=_to_int(row.get("pchs_amt", "0")),
                    )
                )
        logger.debug("[EXEC] stage=holdings count=%d", len(holdings))
        return holdings

    def get_orderable_cash(self, symbol: str, price: int, order_type: OrderType) -> int:
        """미수 없는 매수가능금액을 조회한다."""
        params = {
            "CANO": self._creds.kis_account_no,
            "ACNT_PRDT_CD": self._creds.kis_account_product_code,
            "PDNO": symbol,
            "ORD_UNPR": str(price),
            "ORD_DVSN": ORD_DVSN[order_type],
            "CMA_EVLU_AMT_ICLD_YN": "N",
            "OVRS_ICLD_YN": "N",
        }
        body, _ = self._get(_PATH_ORDERABLE, TR_ORDERABLE, params)
        output = body.get("output") or {}
        return _to_int(output.get("nrcv_buy_amt", "0"))

    def get_daily_orders(self, day: dt.date) -> list[BrokerOrderStatus]:
        """당일주문체결조회를 연속조회로 수집한다."""
        stamp = day.strftime("%Y%m%d")
        params = {
            "CANO": self._creds.kis_account_no,
            "ACNT_PRDT_CD": self._creds.kis_account_product_code,
            "INQR_STRT_DT": stamp,
            "INQR_END_DT": stamp,
            "SLL_BUY_DVSN_CD": "00",
            "PDNO": "",
            "CCLD_DVSN": "00",
            "INQR_DVSN": "00",
            "INQR_DVSN_3": "00",
            "ORD_GNO_BRNO": "",
            "ODNO": "",
            "INQR_DVSN_1": "",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
            "EXCG_ID_DVSN_CD": "KRX",
        }
        rows = self._get_paged(_PATH_DAILY, TR_DAILY_ORDERS, params)
        return [
            BrokerOrderStatus(
                broker_order_no=str(row.get("odno", "")),
                original_order_no=str(row.get("orgn_odno", "")),
                org_no=str(row.get("ord_gno_brno", "")),
                symbol=str(row.get("pdno", "")),
                side=Side.SELL if str(row.get("sll_buy_dvsn_cd", "")) == "01" else Side.BUY,
                ordered_qty=_to_int(row.get("ord_qty", "0")),
                price=_to_int(row.get("ord_unpr", "0")),
                filled_qty=_to_int(row.get("tot_ccld_qty", "0")),
                filled_amount_krw=_to_int(row.get("tot_ccld_amt", "0")),
                remaining_qty=_to_int(row.get("rmn_qty", "0")),
                rejected_qty=_to_int(row.get("rjct_qty", "0")),
                cancelled=str(row.get("cncl_yn", "N")) == "Y",
                order_time=str(row.get("ord_tmd", "")),
            )
            for row in rows
        ]

    def post_order(self, tr_id: str, body: dict[str, str]) -> BrokerOutcome:
        """주문/취소 POST 를 전송하고 결과를 분류한다."""
        path = _PATH_RVSECNCL if tr_id == TR_CANCEL else _PATH_ORDER_CASH
        refreshed = False
        retries = 0
        while True:
            self._limiter.acquire()
            try:
                resp = self._session.post(
                    self._base_url + path,
                    headers=self._headers(tr_id, ""),
                    json=body,
                    timeout=self._timeout_s,
                )
            except requests.ConnectTimeout:
                if retries < _MAX_SAFE_RETRIES:
                    retries += 1
                    continue
                return BrokerOutcome(
                    kind=OutcomeKind.REJECTED,
                    code="connect_timeout",
                    message="not sent: connect timeout retries exhausted",
                )
            except requests.RequestException as exc:
                return BrokerOutcome(kind=OutcomeKind.UNKNOWN, code="transport", message=type(exc).__name__)
            try:
                data = resp.json()
            except ValueError:
                return BrokerOutcome(
                    kind=OutcomeKind.UNKNOWN, code=f"http_{resp.status_code}", message="non_json_response"
                )
            msg_cd = str(data.get("msg_cd", ""))
            message = str(data.get("msg1", "")).strip()
            if msg_cd in _RATE_LIMIT_CODES and retries < _MAX_SAFE_RETRIES:
                retries += 1
                continue
            if msg_cd in _EXPIRED_TOKEN_CODES and not refreshed:
                self.access_token(force=True)
                refreshed = True
                continue
            if data.get("rt_cd") != "0":
                return BrokerOutcome(kind=OutcomeKind.REJECTED, code=msg_cd, message=message)
            output = data.get("output") or {}
            if not output.get("ODNO"):
                return BrokerOutcome(kind=OutcomeKind.UNKNOWN, code="no_odno", message=message)
            return BrokerOutcome(
                kind=OutcomeKind.ACCEPTED,
                broker_order_no=str(output.get("ODNO")),
                broker_org_no=str(output.get("KRX_FWDG_ORD_ORGNO", "")),
                order_time=str(output.get("ORD_TMD", "")),
                code=msg_cd,
                message=message,
            )

    def get_trade_amount_ranking(self) -> tuple[KisRankingRow, ...]:
        """거래대금 랭킹(TR FHPST01710000, 거래량순위 화면의 거래금액순 정렬)을 조회한다."""
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_COND_SCR_DIV_CODE": "20171",
            "FID_INPUT_ISCD": "0000",
            "FID_DIV_CLS_CODE": "0",
            "FID_BLNG_CLS_CODE": "3",  # 3: 거래금액순 (실측 확인)
            "FID_TRGT_CLS_CODE": "0000000000",
            # 10자리: 위험/경고/주의 관리종목 정리매매 불성실공시 우선주 거래정지 ETF ETN
            # 신용주문불가 SPAC 순. ETF/ETN(7,8번째 자리)을 제외하지 않으면 6자리 숫자가
            # 아닌 종목코드(예: 단일종목 레버리지 ETF "0193T0")가 섞여 스키마 검증에서
            # 거부된다(실측 확인).
            "FID_TRGT_EXLS_CLS_CODE": "0000001100",
            "FID_INPUT_PRICE_1": "",
            "FID_INPUT_PRICE_2": "",
            "FID_VOL_CNT": "",
            "FID_INPUT_DATE_1": "",
        }
        body, _ = self._get(_PATH_TRADE_AMOUNT, TR_TRADE_AMOUNT, params)
        # 이 화면의 종목코드 필드는 mksc_shrn_iscd 이며 등락률 화면(stck_shrn_iscd)과
        # 다르다(실측 확인). 공용 파서가 stck_shrn_iscd 만 읽으므로 여기서 정규화한다.
        rows = [
            {**row, "stck_shrn_iscd": row.get("stck_shrn_iscd") or row.get("mksc_shrn_iscd", "")}
            for row in (body.get("output") or [])
        ]
        return _parse_ranking_rows(rows)

    def get_fluctuation_ranking(self) -> tuple[KisRankingRow, ...]:
        """등락률 랭킹(TR FHPST01700000)을 조회한다."""
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_COND_SCR_DIV_CODE": "20170",
            "FID_INPUT_ISCD": "0000",
            "FID_RANK_SORT_CLS_CODE": "0",
            "FID_INPUT_CNT_1": "200",
            "FID_PRC_CLS_CODE": "0",
            "FID_INPUT_PRICE_1": "",
            "FID_INPUT_PRICE_2": "",
            "FID_VOL_CNT": "",
            "FID_TRGT_CLS_CODE": "0",
            "FID_TRGT_EXLS_CLS_CODE": "0",
            "FID_DIV_CLS_CODE": "0",
            "FID_RSFL_RATE1": "0",
            "FID_RSFL_RATE2": "30",
        }
        body, _ = self._get(_PATH_FLUCTUATION, TR_FLUCTUATION, params)
        return _parse_ranking_rows(list(body.get("output") or []))

    def get_security_status(self, symbol: str) -> dict[str, object]:
        """Fetch exchange designation flags and price limits for one stock.

        The flags are a point-in-time observation; KIS offers no history, so the
        caller must persist every observation it relies on.

        Raises:
            KisApiError: On transport/API failure or when a field violates its type.
        """
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol}
        body, _ = self._get(_PATH_PRICE, TR_PRICE, params)
        output = body.get("output") or {}
        row = {
            "source_tr": TR_PRICE,
            "market_div_code": "J",
            "symbol": symbol,
            "status_code": _snapshot_code(output, "iscd_stat_cls_code"),
            "managed": _snapshot_yn(output, "mang_issu_cls_code"),
            "market_warning_code": _snapshot_code(output, "mrkt_warn_cls_code"),
            "short_overheated": _snapshot_yn(output, "short_over_yn"),
            "investment_caution": _snapshot_yn(output, "invt_caful_yn"),
            "liquidation_trading": _snapshot_yn(output, "sltr_yn"),
            "trading_halted": _snapshot_yn(output, "temp_stop_yn"),
            "vi_code": _snapshot_code(output, "vi_cls_code"),
            "overtime_vi_code": _snapshot_code(output, "ovtm_vi_cls_code"),
            "credit_available": _snapshot_yn(output, "crdt_able_yn"),
            "last_price": _snapshot_int(output, "stck_prpr"),
            "base_price": _snapshot_int(output, "stck_sdpr"),
            "upper_limit": _snapshot_int(output, "stck_mxpr"),
            "lower_limit": _snapshot_int(output, "stck_llam"),
        }
        logger.debug("[DATA] stage=kis_snapshot tr=%s symbol=%s rows=1", TR_PRICE, symbol)
        return row

    def get_auction_book(self, symbol: str) -> dict[str, object]:
        """Fetch the 10-level book with the single-price auction expectation.

        Ladder lists always keep ten positional levels (empty levels as 0) so a
        level index means the same depth across observations.

        Raises:
            KisApiError: On transport/API failure or schema violation.
        """
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol}
        body, _ = self._get(_PATH_ASKING, TR_ASKING, params)
        book = body.get("output1") or {}
        expect = body.get("output2") or {}
        row = {
            "source_tr": TR_ASKING,
            "market_div_code": "J",
            "symbol": symbol,
            "book_time": _snapshot_code(book, "aspr_acpt_hour"),
            "auction_code": _snapshot_code(expect, "antc_mkop_cls_code"),
            "expected_price": _snapshot_int(expect, "antc_cnpr"),
            "expected_volume": _snapshot_int(expect, "antc_vol"),
            "expected_change_pct": _snapshot_float(expect, "antc_cntg_prdy_ctrt"),
            "vi_code": _snapshot_code(expect, "vi_cls_code"),
            "last_price": _snapshot_int(expect, "stck_prpr"),
            "base_price": _snapshot_int(expect, "stck_sdpr"),
            "ask_prices": [_snapshot_ladder_level(book, f"askp{i}") for i in range(1, 11)],
            "ask_sizes": [_snapshot_ladder_level(book, f"askp_rsqn{i}") for i in range(1, 11)],
            "bid_prices": [_snapshot_ladder_level(book, f"bidp{i}") for i in range(1, 11)],
            "bid_sizes": [_snapshot_ladder_level(book, f"bidp_rsqn{i}") for i in range(1, 11)],
            "total_ask_size": _snapshot_int(book, "total_askp_rsqn"),
            "total_bid_size": _snapshot_int(book, "total_bidp_rsqn"),
        }
        logger.debug("[DATA] stage=kis_snapshot tr=%s symbol=%s rows=1", TR_ASKING, symbol)
        return row

    def get_investor_estimate(self, symbol: str) -> tuple[dict[str, object], ...]:
        """Fetch provisional foreign/institution cumulative net quantities by bucket.

        Buckets follow the exchange provisional-flow publication slots (1..5); the
        response carries no publication timestamp, so observation time is the only
        point-in-time anchor.

        Raises:
            KisApiError: On transport/API failure, bucket outside 1..5, duplicate
                bucket, or schema violation.
        """
        body, _ = self._get(_PATH_INVESTOR_ESTIMATE, TR_INVESTOR_ESTIMATE, {"MKSC_SHRN_ISCD": symbol})
        raw_rows = body.get("output2") or []
        ranked: list[tuple[int, dict[str, object]]] = []
        seen: set[int] = set()
        for raw in raw_rows:
            bucket = _snapshot_int(raw, "bsop_hour_gb")
            if bucket < 1 or bucket > 5:
                raise KisApiError("SCHEMA", f"bucket out of range: {bucket}")
            if bucket in seen:
                raise KisApiError("SCHEMA", f"duplicate bucket: {bucket}")
            seen.add(bucket)
            ranked.append((
                bucket,
                {
                    "source_tr": TR_INVESTOR_ESTIMATE,
                    "market_div_code": "",
                    "symbol": symbol,
                    "bucket": bucket,
                    "foreign_net_qty": _snapshot_int(raw, "frgn_fake_ntby_qty"),
                    "institution_net_qty": _snapshot_int(raw, "orgn_fake_ntby_qty"),
                    "total_net_qty": _snapshot_int(raw, "sum_fake_ntby_qty"),
                },
            ))
        ranked.sort(key=lambda item: item[0])
        logger.debug(
            "[DATA] stage=kis_snapshot tr=%s symbol=%s rows=%d", TR_INVESTOR_ESTIMATE, symbol, len(ranked)
        )
        return tuple(row for _, row in ranked)

    def get_program_trade_latest(self, symbol: str) -> dict[str, object] | None:
        """Fetch the latest cumulative program-trading totals for one stock.

        Returns:
            The row with the greatest ``trade_time``, or ``None`` if no program
            trade has been reported yet.

        Raises:
            KisApiError: On transport/API failure, schema violation, or when the
                selected row breaks ``net = buy - sell`` for quantity or value.
        """
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol}
        body, _ = self._get(_PATH_PROGRAM_TRADE, TR_PROGRAM_TRADE, params)
        raw_rows = body.get("output") or []
        if not raw_rows:
            logger.debug("[DATA] stage=kis_snapshot tr=%s symbol=%s rows=0", TR_PROGRAM_TRADE, symbol)
            return None
        timed = [(_snapshot_code(raw, "bsop_hour"), raw) for raw in raw_rows]
        trade_time, winner = max(timed, key=lambda item: item[0])
        sell_qty = _snapshot_int(winner, "whol_smtn_seln_vol")
        buy_qty = _snapshot_int(winner, "whol_smtn_shnu_vol")
        net_qty = _snapshot_int(winner, "whol_smtn_ntby_qty")
        sell_value = _snapshot_int(winner, "whol_smtn_seln_tr_pbmn")
        buy_value = _snapshot_int(winner, "whol_smtn_shnu_tr_pbmn")
        net_value = _snapshot_int(winner, "whol_smtn_ntby_tr_pbmn")
        if net_qty != buy_qty - sell_qty or net_value != buy_value - sell_value:
            raise KisApiError("SCHEMA", "program trade net identity broken")
        row = {
            "source_tr": TR_PROGRAM_TRADE,
            "market_div_code": "J",
            "symbol": symbol,
            "trade_time": trade_time,
            "cum_volume": _snapshot_int(winner, "acml_vol"),
            "sell_qty": sell_qty,
            "buy_qty": buy_qty,
            "net_qty": net_qty,
            "sell_value_krw": sell_value,
            "buy_value_krw": buy_value,
            "net_value_krw": net_value,
        }
        logger.debug("[DATA] stage=kis_snapshot tr=%s symbol=%s rows=1", TR_PROGRAM_TRADE, symbol)
        return row

    def get_index_snapshot(self, index_code: str) -> dict[str, object]:
        """Fetch the current value, breadth and cumulative turnover of a market index.

        Raises:
            KisApiError: On transport/API failure or schema violation.
        """
        params = {"FID_COND_MRKT_DIV_CODE": "U", "FID_INPUT_ISCD": index_code}
        body, _ = self._get(_PATH_INDEX_PRICE, TR_INDEX_PRICE, params)
        output = body.get("output") or {}
        row = {
            "source_tr": TR_INDEX_PRICE,
            "market_div_code": "U",
            "index_code": index_code,
            "index_value": _snapshot_float(output, "bstp_nmix_prpr"),
            "change_pct": _snapshot_float(output, "bstp_nmix_prdy_ctrt"),
            "cum_value_mil_krw": _snapshot_int(output, "acml_tr_pbmn"),
            "advancers": _snapshot_int(output, "ascn_issu_cnt"),
            "decliners": _snapshot_int(output, "down_issu_cnt"),
        }
        logger.debug("[DATA] stage=kis_snapshot tr=%s symbol=%s rows=1", TR_INDEX_PRICE, index_code)
        return row

    def get_index_minute_bars(self, index_code: str, *, session_date: dt.date) -> tuple[dict[str, object], ...]:
        """Fetch the vendor's most recent one-minute index bars (fixed 60-second interval).

        The endpoint has no time cursor and no continuation mechanism, so one call
        only ever returns its most recent page; full-day coverage depends on the
        caller polling at an interval no longer than that page's span.

        Args:
            session_date: Trading date bars must belong to; other-day rows are
                dropped rather than misfiled into this session.

        Raises:
            KisApiError: On transport/API failure, schema violation, or a bar
                whose low/high does not bound its open and close.
        """
        stamp = session_date.strftime("%Y%m%d")
        params = {
            "FID_COND_MRKT_DIV_CODE": "U",
            "FID_ETC_CLS_CODE": "0",
            "FID_INPUT_ISCD": index_code,
            "FID_INPUT_HOUR_1": "60",
            "FID_PW_DATA_INCU_YN": "Y",
        }
        body, _ = self._get(_PATH_INDEX_MINUTE_CHART, TR_INDEX_MINUTE_CHART, params)
        rows = body.get("output2") or []
        bars: dict[str, dict[str, object]] = {}
        for raw in rows:
            hour = _snapshot_code(raw, "stck_cntg_hour")
            if hour in ("999999", "888888"):
                continue
            if str(raw.get("stck_bsop_date", "")) != stamp:
                continue
            if hour in bars:
                continue
            open_p = _snapshot_float(raw, "bstp_nmix_oprc")
            high = _snapshot_float(raw, "bstp_nmix_hgpr")
            low = _snapshot_float(raw, "bstp_nmix_lwpr")
            close_p = _snapshot_float(raw, "bstp_nmix_prpr")
            if low > min(open_p, close_p) or max(open_p, close_p) > high:
                raise KisApiError("SCHEMA", f"bar range violated: {hour}")
            bars[hour] = {
                "source_tr": TR_INDEX_MINUTE_CHART,
                "market_div_code": "U",
                "index_code": index_code,
                "bar_time": hour,
                "open": open_p,
                "high": high,
                "low": low,
                "close": close_p,
                "volume": _snapshot_int(raw, "cntg_vol"),
                "cum_value_mil_krw": _snapshot_int(raw, "acml_tr_pbmn"),
            }
        ordered = tuple(bars[key] for key in sorted(bars))
        logger.debug(
            "[DATA] stage=kis_snapshot tr=%s symbol=%s rows=%d", TR_INDEX_MINUTE_CHART, index_code, len(ordered)
        )
        return ordered

    def get_stock_minute_bars(
        self, symbol: str, *, session_date: dt.date, session_open: dt.time, session_close: dt.time
    ) -> tuple[dict[str, object], ...]:
        """Fetch same-day KRX one-minute bars inside the regular session window.

        The endpoint pages backward in 30-bar windows keyed by an inclusive time
        cursor, so the method walks the cursor from ``session_close`` toward
        ``session_open`` and stops on exhaustion.

        Returns:
            Bars sorted by ``bar_time`` ascending, unique per ``bar_time``.

        Raises:
            KisApiError: On transport/API failure, schema violation, a bar whose
                low/high does not bound its open and close, or when the page cap
                is exceeded.
        """
        stamp = session_date.strftime("%Y%m%d")
        open_s = session_open.strftime("%H%M%S")
        close_s = session_close.strftime("%H%M%S")
        bars: dict[str, dict[str, object]] = {}
        cursor = close_s
        for _ in range(_MAX_PAGES):
            params = {
                "FID_ETC_CLS_CODE": "",
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": symbol,
                "FID_INPUT_HOUR_1": cursor,
                "FID_PW_DATA_INCU_YN": "N",
            }
            body, _ = self._get(_PATH_MINUTE_CHART, TR_MINUTE_CHART, params)
            rows = body.get("output2") or []
            if not rows:
                break
            day_rows = [raw for raw in rows if str(raw.get("stck_bsop_date", "")) == stamp]
            fresh = False
            min_hour: str | None = None
            for raw in day_rows:
                hour = _snapshot_code(raw, "stck_cntg_hour")
                if len(hour) != 6 or not hour.isdigit():
                    raise KisApiError("SCHEMA", f"malformed bar time: {hour}")
                if min_hour is None or hour < min_hour:
                    min_hour = hour
                if hour < open_s or hour > close_s or hour in bars:
                    continue
                open_p = _snapshot_int(raw, "stck_oprc")
                high = _snapshot_int(raw, "stck_hgpr")
                low = _snapshot_int(raw, "stck_lwpr")
                close_p = _snapshot_int(raw, "stck_prpr")
                if low > min(open_p, close_p) or max(open_p, close_p) > high:
                    raise KisApiError("SCHEMA", f"bar range violated: {hour}")
                bars[hour] = {
                    "source_tr": TR_MINUTE_CHART,
                    "market_div_code": "J",
                    "symbol": symbol,
                    "bar_time": hour,
                    "open": open_p,
                    "high": high,
                    "low": low,
                    "close": close_p,
                    "volume": _snapshot_int(raw, "cntg_vol"),
                }
                fresh = True
            if not fresh:
                break
            assert min_hour is not None
            if min_hour <= open_s:
                break
            cursor = (dt.datetime.strptime(min_hour, "%H%M%S") - dt.timedelta(seconds=60)).strftime("%H%M%S")
        else:
            raise KisApiError("PAGINATION", f"exceeded {_MAX_PAGES} pages")
        ordered = tuple(bars[key] for key in sorted(bars))
        logger.debug(
            "[DATA] stage=kis_snapshot tr=%s symbol=%s rows=%d", TR_MINUTE_CHART, symbol, len(ordered)
        )
        return ordered

    def get_news_titles(self, *, before: tuple[str, str] | None = None) -> tuple[dict[str, object], ...]:
        """Fetch one page of market news and exchange disclosure headlines.

        Args:
            before: Optional inclusive ``(YYYYMMDD, HHMMSS)`` cursor; ``None``
                requests the newest page.

        Returns:
            Rows in vendor order (newest first) with the KST publication time
            converted to UTC epoch nanoseconds.

        Raises:
            KisApiError: On transport/API failure, empty news id, malformed
                publication date/time, or schema violation.
        """
        params = {
            "FID_NEWS_OFER_ENTP_CODE": "",
            "FID_COND_MRKT_CLS_CODE": "",
            "FID_INPUT_ISCD": "",
            "FID_TITL_CNTT": "",
            "FID_INPUT_DATE_1": "",
            "FID_INPUT_HOUR_1": "",
            "FID_RANK_SORT_CLS_CODE": "",
            "FID_INPUT_SRNO": "",
        }
        if before is not None:
            params["FID_INPUT_DATE_1"] = before[0]
            params["FID_INPUT_HOUR_1"] = before[1]
        body, _ = self._get(_PATH_NEWS_TITLE, TR_NEWS_TITLE, params)
        raw_rows = body.get("output")
        if not raw_rows:
            return ()
        parsed: list[dict[str, object]] = []
        for raw in raw_rows:
            news_id = _snapshot_code(raw, "cntt_usiq_srno")
            if not news_id:
                raise KisApiError("SCHEMA", "empty news id")
            symbols = [
                text for i in range(1, 11) if (text := str(raw.get(f"iscd{i}", "")).strip())
            ]
            parsed.append({
                "source_tr": TR_NEWS_TITLE,
                "market_div_code": "",
                "news_id": news_id,
                "published_at_ns": _snapshot_news_ns(
                    _snapshot_code(raw, "data_dt"), _snapshot_code(raw, "data_tm")
                ),
                "provider": _snapshot_code(raw, "dorg"),
                "provider_code": _snapshot_code(raw, "news_ofer_entp_code"),
                "category_code": _snapshot_code(raw, "news_lrdv_code"),
                "title": _snapshot_code(raw, "hts_pbnt_titl_cntt"),
                "symbols": symbols,
            })
        logger.debug("[DATA] stage=kis_snapshot tr=%s symbol=%s rows=%d", TR_NEWS_TITLE, "", len(parsed))
        return tuple(parsed)
