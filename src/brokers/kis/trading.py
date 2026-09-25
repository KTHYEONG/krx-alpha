"""KIS account and order endpoints under an explicit trading credential."""

from __future__ import annotations

import datetime as dt
import logging
from decimal import Decimal
from typing import Any

import requests

from src.brokers.kis.http import (
    _EXPIRED_TOKEN_CODES,
    _MAX_SAFE_RETRIES,
    _RATE_LIMIT_CODES,
    KisGetTransport,
)
from src.brokers.kis.rate import RateLimiter
from src.core.config import KisCredentials
from src.execution.contracts import (
    BrokerOrderStatus,
    BrokerOutcome,
    ExecutionError,
    Holding,
    Order,
    OrderType,
    OutcomeKind,
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

ORD_DVSN: dict[OrderType, str] = {OrderType.LIMIT: "00", OrderType.MARKET: "01"}

_PATH_ORDER_CASH = "/uapi/domestic-stock/v1/trading/order-cash"
_PATH_RVSECNCL = "/uapi/domestic-stock/v1/trading/order-rvsecncl"
_PATH_BALANCE = "/uapi/domestic-stock/v1/trading/inquire-balance"
_PATH_ORDERABLE = "/uapi/domestic-stock/v1/trading/inquire-psbl-order"
_PATH_DAILY = "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"


def _to_int(raw: Any) -> int:
    text = str(raw or "0")
    return int(Decimal(text))


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


class KisTradingClient:
    """Access account and order endpoints under an explicit trading credential.

    Order submission retains ambiguity classification so a possible exchange
    acceptance is never retried as an ordinary safe GET request.
    """

    def __init__(
        self,
        *,
        transport: KisGetTransport,
        session: Any,
        credentials: KisCredentials,
        limiter: RateLimiter,
        timeout_s: float,
        base_url: str,
    ) -> None:
        self._transport = transport
        self._session = session
        self._credentials = credentials
        self._limiter = limiter
        self._timeout_s = timeout_s
        self._base_url = base_url

    def get_holdings(self) -> list[Holding]:
        """실계좌 보유잔고를 조회한다."""
        params = {
            "CANO": self._credentials.kis_account_no,
            "ACNT_PRDT_CD": self._credentials.kis_account_product_code,
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
        rows = self._transport.get_paged(_PATH_BALANCE, TR_BALANCE, params)
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
            "CANO": self._credentials.kis_account_no,
            "ACNT_PRDT_CD": self._credentials.kis_account_product_code,
            "PDNO": symbol,
            "ORD_UNPR": str(price),
            "ORD_DVSN": ORD_DVSN[order_type],
            "CMA_EVLU_AMT_ICLD_YN": "N",
            "OVRS_ICLD_YN": "N",
        }
        body, _ = self._transport.get(_PATH_ORDERABLE, TR_ORDERABLE, params)
        output = body.get("output") or {}
        return _to_int(output.get("nrcv_buy_amt", "0"))

    def get_daily_orders(self, day: dt.date) -> list[BrokerOrderStatus]:
        """당일주문체결조회를 연속조회로 수집한다."""
        stamp = day.strftime("%Y%m%d")
        params = {
            "CANO": self._credentials.kis_account_no,
            "ACNT_PRDT_CD": self._credentials.kis_account_product_code,
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
        rows = self._transport.get_paged(_PATH_DAILY, TR_DAILY_ORDERS, params)
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
                    headers=self._transport.headers(tr_id, ""),
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
                self._transport.refresh_token()
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
