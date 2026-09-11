"""KIS 실전 REST 클라이언트: 시세·잔고 조회와 주문 POST 경계."""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import pathlib
import time
from collections.abc import Callable
from decimal import Decimal
from typing import Any
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

ORD_DVSN: dict[OrderType, str] = {OrderType.LIMIT: "00", OrderType.MARKET: "01"}

_PATH_ORDER_CASH = "/uapi/domestic-stock/v1/trading/order-cash"
_PATH_RVSECNCL = "/uapi/domestic-stock/v1/trading/order-rvsecncl"
_PATH_PRICE = "/uapi/domestic-stock/v1/quotations/inquire-price"
_PATH_ASKING = "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn"
_PATH_BALANCE = "/uapi/domestic-stock/v1/trading/inquire-balance"
_PATH_ORDERABLE = "/uapi/domestic-stock/v1/trading/inquire-psbl-order"
_PATH_DAILY = "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
_PATH_TOKEN = "/oauth2/tokenP"  # noqa: S105 - public endpoint, not a secret

_RATE_LIMIT_CODES: frozenset[str] = frozenset({"EGW00201"})
_EXPIRED_TOKEN_CODES: frozenset[str] = frozenset({"EGW00121", "EGW00123"})
_MAX_SAFE_RETRIES = 2
_MAX_PAGES = 100
_TOKEN_REFRESH_MARGIN: dt.timedelta = dt.timedelta(minutes=10)
_KST: dt.tzinfo = ZoneInfo("Asia/Seoul")


def _to_int(raw: Any) -> int:
    text = str(raw or "0")
    return int(Decimal(text))


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
    ) -> None:
        self._creds = creds
        self._session = session
        self._token_cache_path = token_cache_path
        self._limiter = limiter
        self._now = now
        self._timeout_s = timeout_s
        self._base_url = base_url
        self._token: str | None = None
        self._token_expires_at: dt.datetime | None = None

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
            try:
                cached = json.loads(self._token_cache_path.read_text(encoding="utf-8"))
                expires_at = dt.datetime.fromisoformat(str(cached["expires_at"]))
                if expires_at - now > _TOKEN_REFRESH_MARGIN:
                    self._token = str(cached["access_token"])
                    self._token_expires_at = expires_at
                    return self._token
            except (OSError, ValueError, KeyError):
                pass
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
        payload = json.dumps({"access_token": self._token, "expires_at": expires_at.isoformat()})
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
