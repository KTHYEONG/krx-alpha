"""KIS account and order endpoints under an explicit trading credential."""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

import requests

from src.brokers.kis.http import (
    MAX_SAFE_RETRIES,
    KisGetTransport,
    KisRetryDecision,
    classify_kis_retry,
)
from src.brokers.kis.rate import Pacer
from src.core.config import KisCredentials, KisVenue
from src.execution.contracts import (
    BrokerOrderStatus,
    BrokerOutcome,
    ExecutionError,
    Holding,
    KisApiError,
    Order,
    OrderType,
    OutcomeKind,
    Side,
)

logger = logging.getLogger(__name__)

KIS_LIVE_BASE_URL: str = "https://openapi.koreainvestment.com:9443"
KIS_MOCK_BASE_URL: str = "https://openapivts.koreainvestment.com:29443"

TR_BUY: str = "TTTC0012U"
TR_SELL: str = "TTTC0011U"
TR_CANCEL: str = "TTTC0013U"
TR_DAILY_ORDERS: str = "TTTC0081R"
TR_BALANCE: str = "TTTC8434R"
TR_ORDERABLE: str = "TTTC8908R"

ORD_DVSN: dict[OrderType, str] = {OrderType.LIMIT: "00", OrderType.MARKET: "01"}

_PATH_ORDER_CASH = "/uapi/domestic-stock/v1/trading/order-cash"
_PATH_RVSECNCL = "/uapi/domestic-stock/v1/trading/order-rvsecncl"
_PATH_BALANCE = "/uapi/domestic-stock/v1/trading/inquire-balance"
_PATH_ORDERABLE = "/uapi/domestic-stock/v1/trading/inquire-psbl-order"
_PATH_DAILY = "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"


def venue_base_url(venue: KisVenue) -> str:
    """Return the REST base URL bound to a venue."""
    return KIS_MOCK_BASE_URL if venue is KisVenue.MOCK else KIS_LIVE_BASE_URL


def wire_tr_id(venue: KisVenue, canonical_tr_id: str) -> str:
    """Map a canonical (live) trading TR id to the id sent on the wire; raises ValueError when unmappable."""
    if venue is KisVenue.LIVE:
        return canonical_tr_id
    if not canonical_tr_id or not canonical_tr_id.startswith("TTTC"):
        raise ValueError(f"unmappable TR id for mock venue: {canonical_tr_id!r}")
    return "V" + canonical_tr_id[1:]


def _strict_int(row: Mapping[str, Any], field: str, endpoint: str) -> int:
    if field not in row or row[field] is None:
        raise KisApiError("SCHEMA", f"{endpoint} missing/invalid {field}")
    raw = row[field]
    if isinstance(raw, str) and raw.strip() == "":
        return 0
    try:
        value = Decimal(str(raw).strip())
    except (ArithmeticError, ValueError):
        raise KisApiError("SCHEMA", f"{endpoint} missing/invalid {field}") from None
    if not value.is_finite() or value != value.to_integral_value():
        raise KisApiError("SCHEMA", f"{endpoint} missing/invalid {field}")
    return int(value)


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
        limiter: Pacer,
        timeout_s: float,
        base_url: str,
        venue: KisVenue = KisVenue.LIVE,
    ) -> None:
        self._transport = transport
        self._session = session
        self._credentials = credentials
        self._limiter = limiter
        self._timeout_s = timeout_s
        self._base_url = base_url
        self.venue: KisVenue = venue

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
        rows = self._transport.get_paged(
            _PATH_BALANCE, wire_tr_id(self.venue, TR_BALANCE), params
        )
        holdings: list[Holding] = []
        for row in rows:
            endpoint = "inquire-balance"
            if not isinstance(row, Mapping) or any(
                f not in row or row[f] is None for f in ("pdno", "hldg_qty", "pchs_amt")
            ):
                missing = next(
                    f for f in ("pdno", "hldg_qty", "pchs_amt") if not isinstance(row, Mapping) or f not in row or row[f] is None
                )
                raise KisApiError("SCHEMA", f"{endpoint} missing/invalid {missing}")
            qty = _strict_int(row, "hldg_qty", endpoint)
            cost = _strict_int(row, "pchs_amt", endpoint)
            if qty > 0:
                holdings.append(
                    Holding(
                        symbol=str(row.get("pdno", "")),
                        qty=qty,
                        cost_krw=cost,
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
        body, _ = self._transport.get(
            _PATH_ORDERABLE, wire_tr_id(self.venue, TR_ORDERABLE), params
        )
        output = body.get("output")
        if not isinstance(output, Mapping) or "nrcvb_buy_amt" not in output or output["nrcvb_buy_amt"] is None:
            raise KisApiError("SCHEMA", "inquire-psbl-order missing/invalid nrcvb_buy_amt")
        return _strict_int(output, "nrcvb_buy_amt", "inquire-psbl-order")

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
        rows = self._transport.get_paged(
            _PATH_DAILY, wire_tr_id(self.venue, TR_DAILY_ORDERS), params
        )
        statuses: list[BrokerOrderStatus] = []
        for row in rows:
            endpoint = "inquire-daily-ccld"
            for field in (
                "odno",
                "orgn_odno",
                "ord_gno_brno",
                "pdno",
                "sll_buy_dvsn_cd",
                "ord_qty",
                "ord_unpr",
                "tot_ccld_qty",
                "tot_ccld_amt",
                "rmn_qty",
                "rjct_qty",
                "cncl_yn",
                "ord_tmd",
            ):
                if not isinstance(row, Mapping) or field not in row or row[field] is None:
                    raise KisApiError("SCHEMA", f"{endpoint} missing/invalid {field}")
            if not str(row["odno"]).strip():
                raise KisApiError("SCHEMA", f"{endpoint} missing/invalid odno")
            dvsn = str(row["sll_buy_dvsn_cd"])
            if dvsn not in ("01", "02"):
                raise KisApiError("SCHEMA", f"{endpoint} missing/invalid sll_buy_dvsn_cd")
            statuses.append(
                BrokerOrderStatus(
                    broker_order_no=str(row.get("odno", "")),
                    original_order_no=str(row.get("orgn_odno", "")),
                    org_no=str(row.get("ord_gno_brno", "")),
                    symbol=str(row.get("pdno", "")),
                    side=Side.SELL if dvsn == "01" else Side.BUY,
                    ordered_qty=_strict_int(row, "ord_qty", endpoint),
                    price=_strict_int(row, "ord_unpr", endpoint),
                    filled_qty=_strict_int(row, "tot_ccld_qty", endpoint),
                    filled_amount_krw=_strict_int(row, "tot_ccld_amt", endpoint),
                    remaining_qty=_strict_int(row, "rmn_qty", endpoint),
                    rejected_qty=_strict_int(row, "rjct_qty", endpoint),
                    cancelled=str(row.get("cncl_yn", "N")) == "Y",
                    order_time=str(row.get("ord_tmd", "")),
                )
            )
        return statuses

    def post_order(self, tr_id: str, body: dict[str, str]) -> BrokerOutcome:
        """주문/취소 POST 를 전송하고 결과를 분류한다."""
        path = _PATH_RVSECNCL if tr_id == TR_CANCEL else _PATH_ORDER_CASH
        wire_id = wire_tr_id(self.venue, tr_id)
        refreshed = False
        retries = 0
        while True:
            self._limiter.acquire()
            sent_headers, sent_token = self._transport.authorized_headers(wire_id, "")
            try:
                resp = self._session.post(
                    self._base_url + path,
                    headers=sent_headers,
                    json=body,
                    timeout=self._timeout_s,
                )
            except requests.ConnectTimeout:
                if retries < MAX_SAFE_RETRIES:
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
            decision = classify_kis_retry(msg_cd, retries=retries, refreshed=refreshed)
            if decision is KisRetryDecision.RETRY_RATE_LIMITED:
                retries += 1
                continue
            if decision is KisRetryDecision.RETRY_AFTER_TOKEN_REFRESH:
                self._transport.refresh_token(sent_token)
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
