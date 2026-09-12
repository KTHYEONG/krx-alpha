"""주문집행 단위테스트용 결정적 가짜 경계 (HTTP 세션/시계/브로커 게이트웨이)."""

from __future__ import annotations

import copy
import dataclasses
import datetime as dt
import json
import pathlib
from typing import Any
from zoneinfo import ZoneInfo

from src.core.config import ExecutionMode, KisCredentials
from src.execution.contracts import BrokerOrderStatus, BrokerOutcome, Order, OrderType, OutcomeKind, Quote
from src.execution.gateways import PaperGateway
from src.execution.journal import OrderJournal
from src.execution.kis_client import KisRestClient, RateLimiter
from src.execution.ledger import CostModel, Ledger
from src.execution.oms import OrderManager
from src.execution.risk import RiskLimits

KST = ZoneInfo("Asia/Seoul")
T0 = dt.datetime(2026, 9, 11, 9, 0, 0, tzinfo=KST)
_CACHE_EXPIRY = dt.datetime(2026, 9, 12, 9, 0, 0, tzinfo=KST)


class FixedClock:
    def __init__(self, start: dt.datetime) -> None:
        self.current = start

    def __call__(self) -> dt.datetime:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += dt.timedelta(seconds=seconds)


class FakeResponse:
    def __init__(
        self, body: Any, *, status: int = 200, headers: dict[str, str] | None = None, raw_text: bool = False
    ) -> None:
        self._body = body
        self.status_code = status
        self.headers = headers or {}
        self._raw_text = raw_text

    def json(self) -> Any:
        if self._raw_text:
            raise ValueError("response is not json")
        return self._body


class FakeSession:
    """스크립트 순서대로 응답을 반환하고 호출 인자를 깊은복사로 기록한다 (예외 인스턴스는 raise)."""

    def __init__(self, script: list[Any]) -> None:
        self.script = list(script)
        self.calls: list[dict[str, Any]] = []

    def _next(self, method: str, url: str, kwargs: dict[str, Any]) -> Any:
        self.calls.append({"method": method, "url": url, **copy.deepcopy(kwargs)})
        if not self.script:
            raise AssertionError(f"unexpected {method} {url}")
        item = self.script.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    def get(self, url: str, **kwargs: Any) -> Any:
        return self._next("GET", url, kwargs)

    def post(self, url: str, **kwargs: Any) -> Any:
        return self._next("POST", url, kwargs)


def make_creds() -> KisCredentials:
    return KisCredentials(
        kis_app_key="app-key", kis_app_secret="app-secret", kis_account_no="12345678", kis_account_product_code="01"
    )


def token_response(expired: str = "2026-09-12 09:00:00", *, token: str = "tok-1") -> FakeResponse:
    return FakeResponse(
        {"access_token": token, "token_type": "Bearer", "expires_in": 86400, "access_token_token_expired": expired}
    )


def write_token_cache(path: pathlib.Path, *, token: str = "tok-1", expires_at: dt.datetime = _CACHE_EXPIRY) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"access_token": token, "expires_at": expires_at.isoformat()}), encoding="utf-8")


def price_body(
    *, last: int = 10_000, upper: int = 13_000, lower: int = 7_000, tick: int = 10, halted: str = "N"
) -> FakeResponse:
    return FakeResponse(
        {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "정상처리 되었습니다.",
            "output": {
                "stck_prpr": str(last),
                "stck_mxpr": str(upper),
                "stck_llam": str(lower),
                "aspr_unit": str(tick),
                "temp_stop_yn": halted,
            },
        }
    )


def asking_body(*, asks: list[tuple[int, int]], bids: list[tuple[int, int]]) -> FakeResponse:
    out: dict[str, str] = {}
    for i in range(1, 11):
        ask_p, ask_q = asks[i - 1] if i <= len(asks) else (0, 0)
        bid_p, bid_q = bids[i - 1] if i <= len(bids) else (0, 0)
        out[f"askp{i}"] = str(ask_p)
        out[f"askp_rsqn{i}"] = str(ask_q)
        out[f"bidp{i}"] = str(bid_p)
        out[f"bidp_rsqn{i}"] = str(bid_q)
    return FakeResponse({"rt_cd": "0", "msg_cd": "MCA00000", "msg1": "정상처리 되었습니다.", "output1": out, "output2": {}})


def daily_chart_body(*, date: str, close: str, volume: str, trade_value: str, prdy_vrss: str = "0") -> FakeResponse:
    return FakeResponse({"rt_cd": "0", "msg_cd": "MCA00000", "msg1": "정상처리 되었습니다.", "output1": {}, "output2": [{"stck_bsop_date": date, "stck_clpr": close, "stck_oprc": close, "stck_hgpr": close, "stck_lwpr": close, "acml_vol": volume, "acml_tr_pbmn": trade_value, "prdy_vrss": prdy_vrss}]})


def balance_body(rows: list[dict[str, str]]) -> FakeResponse:
    return FakeResponse(
        {
            "rt_cd": "0",
            "msg_cd": "KIOK0510" if rows else "KIOK0560",
            "msg1": "조회되었습니다" if rows else "조회할 내용이 없습니다",
            "output1": rows,
            "output2": [{"tot_evlu_amt": "0"}],
            "ctx_area_fk100": "",
            "ctx_area_nk100": "",
        },
        headers={"tr_cont": "D"},
    )


def accepted_body(*, odno: str = "0000117057") -> FakeResponse:
    return FakeResponse(
        {
            "rt_cd": "0",
            "msg_cd": "APBK0013",
            "msg1": "주문 전송 완료 되었습니다.",
            "output": {"KRX_FWDG_ORD_ORGNO": "91252", "ODNO": odno, "ORD_TMD": "090001"},
        }
    )


_DEFAULT_QUOTE = Quote(
    symbol="005930",
    last=10_000,
    upper_limit=13_000,
    lower_limit=7_000,
    tick=10,
    halted=False,
    asks=((10_010, 5), (10_020, 5)),
    bids=((10_000, 5), (9_990, 5)),
)


def make_quote(**overrides: Any) -> Quote:
    return dataclasses.replace(_DEFAULT_QUOTE, **overrides)


class FakeQuotes:
    def __init__(self, quote: Quote) -> None:
        self.quote = quote
        self.requested: list[str] = []

    def get_quote(self, symbol: str) -> Quote:
        self.requested.append(symbol)
        return dataclasses.replace(self.quote, symbol=symbol)


def make_client(root: pathlib.Path, script: list[Any]) -> tuple[KisRestClient, FakeSession, FixedClock]:
    clock = FixedClock(T0)
    session = FakeSession(script)
    cache = root / "kis_token.json"
    write_token_cache(cache)
    limiter = RateLimiter(1000.0, clock=lambda: 0.0, sleep=lambda seconds: None)
    client = KisRestClient(
        creds=make_creds(), session=session, token_cache_path=cache, limiter=limiter, now=clock, timeout_s=5.0
    )
    return client, session, clock


_BASE_LIMITS = RiskLimits(
    max_order_notional_krw=1_000_000,
    max_position_notional_krw=3_000_000,
    max_orders_per_minute=10,
    max_daily_loss_krw=300_000,
)


def make_limits(**overrides: Any) -> RiskLimits:
    return dataclasses.replace(_BASE_LIMITS, **overrides)


class ScriptedGateway:
    mode = ExecutionMode.LIVE

    def __init__(
        self,
        *,
        outcomes: list[BrokerOutcome] | None = None,
        statuses: list[BrokerOrderStatus] | None = None,
        cash: int = 10_000_000,
    ) -> None:
        self.outcomes = list(outcomes or [])
        self.statuses = list(statuses or [])
        self.cash = cash
        self.submitted: list[Order] = []
        self.cancelled: list[Order] = []

    def submit(self, order: Order, quote: Quote) -> BrokerOutcome:
        self.submitted.append(order)
        return self.outcomes.pop(0)

    def cancel(self, order: Order) -> BrokerOutcome:
        self.cancelled.append(order)
        return BrokerOutcome(kind=OutcomeKind.ACCEPTED, broker_order_no=f"C{order.broker_order_no}")

    def fetch_statuses(self) -> list[BrokerOrderStatus]:
        return list(self.statuses)

    def orderable_cash(self, symbol: str, price: int, order_type: OrderType) -> int:
        return self.cash


_COSTS = CostModel(commission_bps=1.5, sell_tax_bps=20.0)


def make_manager(
    root: pathlib.Path,
    gateway: Any,
    *,
    limits: RiskLimits | None = None,
    kill: bool = False,
    quote: Quote | None = None,
) -> tuple[OrderManager, FixedClock]:
    clock = FixedClock(T0)
    journal = OrderJournal(root=root / "journal", mode=gateway.mode, now=clock)
    ledger = Ledger(cash_krw=1_000_000, costs=_COSTS)
    manager = OrderManager(
        gateway=gateway,
        quotes=FakeQuotes(quote or make_quote()),
        ledger=ledger,
        limits=limits or make_limits(),
        costs=_COSTS,
        journal=journal,
        now=clock,
        kill_switch=lambda: kill,
    )
    return manager, clock


def make_paper_manager(root: pathlib.Path, *, quote: Quote | None = None) -> OrderManager:
    clock = FixedClock(T0)
    journal = OrderJournal(root=root / "journal", mode=ExecutionMode.PAPER, now=clock)
    ledger = Ledger(cash_krw=1_000_000, costs=_COSTS)
    gateway = PaperGateway(creds=make_creds(), journal=journal, ledger=ledger)
    return OrderManager(
        gateway=gateway,
        quotes=FakeQuotes(quote or make_quote()),
        ledger=ledger,
        limits=make_limits(),
        costs=_COSTS,
        journal=journal,
        now=clock,
        kill_switch=lambda: False,
    )
