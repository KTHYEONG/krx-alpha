
def test_oms_paper_submit_reconcile_applies_fills_to_ledger(tmp_path) -> None:
    # Given: 실제 PaperGateway + 고정 호가(매도 10,010x5 / 10,020x5, 매수 10,000x5 / 9,990x5)
    import json

    from src.execution.contracts import OrderIntent, OrderStatus, OrderType, Side
    from tests.unit.execution.fakes import make_paper_manager

    manager = make_paper_manager(tmp_path)

    # When: 시장가 7주 매수
    buy = manager.submit(OrderIntent("005930", Side.BUY, 7, OrderType.MARKET))

    # Then: 접수만 되고 체결은 대사로만 반영
    assert buy.status is OrderStatus.ACCEPTED
    assert buy.client_id == "paper-20260911-000001"
    assert buy.broker_order_no == "P00000001"
    assert buy.filled_qty == 0

    # When
    changed = manager.reconcile()

    # Then: 5x10,010 + 2x10,020 = 70,090, 수수료 10
    assert [o.client_id for o in changed] == [buy.client_id]
    assert buy.status is OrderStatus.FILLED
    assert (buy.filled_qty, buy.filled_amount_krw) == (7, 70_090)
    assert manager.ledger.qty("005930") == 7
    assert manager.ledger.cash_krw == 929_900

    # When: 시장성 지정가 3주 매도
    sell = manager.submit(OrderIntent("005930", Side.SELL, 3, OrderType.LIMIT, 10_000))
    manager.reconcile()

    # Then: 원가 70,100*3//7=30,042, 수수료 4, 거래세 60 → 실현 -106
    assert sell.status is OrderStatus.FILLED
    assert sell.filled_amount_krw == 30_000
    assert manager.ledger.qty("005930") == 4
    assert manager.ledger.realized_pnl_krw == -106
    assert manager.ledger.cash_krw == 959_836

    # When: 비시장성 지정가 매수는 대기
    resting = manager.submit(OrderIntent("005930", Side.BUY, 1, OrderType.LIMIT, 9_000))
    assert manager.reconcile() == []
    assert resting.status is OrderStatus.ACCEPTED

    # When: 취소
    manager.cancel(resting.client_id)
    manager.reconcile()

    # Then
    assert resting.status is OrderStatus.CANCELLED
    assert resting.filled_qty == 0
    assert manager.reconcile() == []
    lines = (tmp_path / "journal" / "2026-09-11.jsonl").read_text(encoding="utf-8").splitlines()
    events = [json.loads(line)["event"] for line in lines]
    assert events.count("paper_would_send") == 3
    assert events.count("fill") == 2
    assert "paper_would_cancel" in events

def test_oms_risk_reject_is_journaled_without_gateway_call(tmp_path) -> None:
    # Given: kill-switch 활성
    import json

    from src.execution.contracts import OrderIntent, OrderStatus, OrderType, Side
    from tests.unit.execution.fakes import ScriptedGateway, make_manager

    gateway = ScriptedGateway()
    manager, _ = make_manager(tmp_path, gateway, kill=True)

    # When
    order = manager.submit(OrderIntent("005930", Side.BUY, 1, OrderType.LIMIT, 10_000))

    # Then
    assert order.status is OrderStatus.REJECTED
    assert order.reject_code == "kill_switch"
    assert gateway.submitted == []
    assert manager.orders[order.client_id] is order
    last = json.loads((tmp_path / "journal" / "2026-09-11.jsonl").read_text(encoding="utf-8").splitlines()[-1])
    assert last["event"] == "risk_reject"
    assert last["code"] == "kill_switch"

    # When / Then: 지정가 누락도 거부 주문으로 기록되고 예외로 새지 않는다
    bad = manager.submit(OrderIntent("005930", Side.BUY, 1, OrderType.LIMIT))
    assert bad.status is OrderStatus.REJECTED
    assert gateway.submitted == []

def test_oms_counts_orders_per_minute_window(tmp_path) -> None:
    # Given: 분당 2건 한도
    from src.execution.contracts import BrokerOutcome, OrderIntent, OrderStatus, OrderType, OutcomeKind, Side
    from tests.unit.execution.fakes import ScriptedGateway, make_limits, make_manager

    accepted = [
        BrokerOutcome(kind=OutcomeKind.ACCEPTED, broker_order_no=f"{i:010d}", broker_org_no="91252", order_time="090000")
        for i in range(3)
    ]
    gateway = ScriptedGateway(outcomes=accepted)
    manager, clock = make_manager(tmp_path, gateway, limits=make_limits(max_orders_per_minute=2))
    intent = OrderIntent("005930", Side.BUY, 1, OrderType.LIMIT, 10_000)

    # When / Then
    assert manager.submit(intent).status is OrderStatus.ACCEPTED
    assert manager.submit(intent).status is OrderStatus.ACCEPTED
    third = manager.submit(intent)
    assert third.status is OrderStatus.REJECTED
    assert third.reject_code == "order_rate_limit"

    # When: 61초 경과
    clock.advance(61)

    # Then
    assert manager.submit(intent).status is OrderStatus.ACCEPTED
    assert len(gateway.submitted) == 3

def test_oms_unknown_blocks_same_key_and_adopts_single_match_on_reconcile(tmp_path) -> None:
    # Given: 전송 후 응답 유실
    from src.execution.contracts import BrokerOrderStatus, BrokerOutcome, OrderIntent, OrderStatus, OrderType, OutcomeKind, Side
    from tests.unit.execution.fakes import ScriptedGateway, make_manager

    gateway = ScriptedGateway(outcomes=[BrokerOutcome(kind=OutcomeKind.UNKNOWN, code="transport", message="ReadTimeout")])
    manager, _ = make_manager(tmp_path, gateway)
    intent = OrderIntent("005930", Side.BUY, 3, OrderType.LIMIT, 10_000)

    # When
    first = manager.submit(intent)
    blocked = manager.submit(intent)

    # Then: 대사 전 동일 의도 재전송 금지
    assert first.status is OrderStatus.UNKNOWN
    assert first.broker_order_no is None
    assert blocked.status is OrderStatus.REJECTED
    assert blocked.reject_code == "unknown_pending"
    assert len(gateway.submitted) == 1

    # When: 브로커 당일주문 (09:00:01 일치 1건 + 윈도우 밖 08:50:00 1건)
    gateway.statuses = [
        BrokerOrderStatus(
            broker_order_no="0000117057", original_order_no="", org_no="91252", symbol="005930", side=Side.BUY,
            ordered_qty=3, price=10_000, filled_qty=3, filled_amount_krw=30_000, remaining_qty=0, rejected_qty=0,
            cancelled=False, order_time="090001",
        ),
        BrokerOrderStatus(
            broker_order_no="0000000009", original_order_no="", org_no="91252", symbol="005930", side=Side.BUY,
            ordered_qty=3, price=10_000, filled_qty=0, filled_amount_krw=0, remaining_qty=3, rejected_qty=0,
            cancelled=False, order_time="085000",
        ),
    ]
    changed = manager.reconcile()

    # Then: 단일 후보 채택 후 누적체결 반영
    assert changed == [first]
    assert (first.broker_order_no, first.broker_org_no) == ("0000117057", "91252")
    assert first.status is OrderStatus.FILLED
    assert manager.ledger.qty("005930") == 3

def test_oms_unknown_stays_unknown_on_ambiguous_match(tmp_path, caplog) -> None:
    # Given
    import dataclasses
    import json
    import logging

    from src.execution.contracts import BrokerOrderStatus, BrokerOutcome, OrderIntent, OrderStatus, OrderType, OutcomeKind, Side
    from tests.unit.execution.fakes import ScriptedGateway, make_manager

    gateway = ScriptedGateway(outcomes=[BrokerOutcome(kind=OutcomeKind.UNKNOWN, code="transport")])
    manager, _ = make_manager(tmp_path, gateway)
    order = manager.submit(OrderIntent("005930", Side.BUY, 3, OrderType.LIMIT, 10_000))
    row = BrokerOrderStatus(
        broker_order_no="0000000001", original_order_no="", org_no="91252", symbol="005930", side=Side.BUY,
        ordered_qty=3, price=10_000, filled_qty=0, filled_amount_krw=0, remaining_qty=3, rejected_qty=0,
        cancelled=False, order_time="090001",
    )
    gateway.statuses = [row, dataclasses.replace(row, broker_order_no="0000000002", order_time="090002")]

    # When
    with caplog.at_level(logging.CRITICAL):
        changed = manager.reconcile()

    # Then
    assert changed == []
    assert order.status is OrderStatus.UNKNOWN
    assert order.broker_order_no is None
    lines = (tmp_path / "journal" / "2026-09-11.jsonl").read_text(encoding="utf-8").splitlines()
    assert "unknown_ambiguous" in [json.loads(line)["event"] for line in lines]
    assert any(record.levelno == logging.CRITICAL for record in caplog.records)

    # When / Then: 후보 0건
    gateway.statuses = []
    assert manager.reconcile() == []
    assert order.status is OrderStatus.UNKNOWN

def test_oms_reconcile_raises_on_cumulative_fill_decrease(tmp_path) -> None:
    # Given: 5주 접수 후 2주 부분체결
    import dataclasses

    import pytest

    from src.execution.contracts import (
        BrokerIntegrityError, BrokerOrderStatus, BrokerOutcome, OrderIntent, OrderStatus, OrderType, OutcomeKind, Side,
    )
    from tests.unit.execution.fakes import ScriptedGateway, make_manager

    gateway = ScriptedGateway(outcomes=[
        BrokerOutcome(kind=OutcomeKind.ACCEPTED, broker_order_no="0000000001", broker_org_no="91252", order_time="090001")
    ])
    manager, _ = make_manager(tmp_path, gateway)
    order = manager.submit(OrderIntent("005930", Side.BUY, 5, OrderType.LIMIT, 10_000))
    row = BrokerOrderStatus(
        broker_order_no="0000000001", original_order_no="", org_no="91252", symbol="005930", side=Side.BUY,
        ordered_qty=5, price=10_000, filled_qty=2, filled_amount_krw=20_000, remaining_qty=3, rejected_qty=0,
        cancelled=False, order_time="090001",
    )
    gateway.statuses = [row]
    manager.reconcile()
    assert order.status is OrderStatus.PARTIALLY_FILLED
    assert manager.ledger.qty("005930") == 2

    # When / Then: 누적체결 감소
    gateway.statuses = [dataclasses.replace(row, filled_qty=1, filled_amount_krw=10_000, remaining_qty=4)]
    with pytest.raises(BrokerIntegrityError):
        manager.reconcile()
    assert manager.ledger.qty("005930") == 2

    # When / Then: 주문수량 초과
    gateway.statuses = [dataclasses.replace(row, filled_qty=6, filled_amount_krw=60_000, remaining_qty=0)]
    with pytest.raises(BrokerIntegrityError):
        manager.reconcile()
    assert manager.ledger.qty("005930") == 2

def test_oms_cancel_guards_unknown_and_terminal_orders(tmp_path) -> None:
    # Given: 응답 유실 1건 + 브로커 거부 1건
    import pytest

    from src.execution.contracts import BrokerOutcome, ExecutionError, OrderIntent, OrderStatus, OrderType, OutcomeKind, Side
    from tests.unit.execution.fakes import ScriptedGateway, make_manager

    gateway = ScriptedGateway(outcomes=[
        BrokerOutcome(kind=OutcomeKind.UNKNOWN, code="transport"),
        BrokerOutcome(kind=OutcomeKind.REJECTED, code="APBK0919", message="주문가능금액 초과"),
    ])
    manager, _ = make_manager(tmp_path, gateway)

    # When / Then: 브로커 주문번호 없는 UNKNOWN 은 취소 불가
    unknown = manager.submit(OrderIntent("005930", Side.BUY, 1, OrderType.LIMIT, 10_000))
    with pytest.raises(ExecutionError):
        manager.cancel(unknown.client_id)

    # When / Then: 브로커 거부 사유 보존 + 종결 주문 취소는 무변경
    rejected = manager.submit(OrderIntent("000660", Side.BUY, 1, OrderType.LIMIT, 10_000))
    assert rejected.status is OrderStatus.REJECTED
    assert (rejected.reject_code, rejected.reject_message) == ("APBK0919", "주문가능금액 초과")
    assert manager.cancel(rejected.client_id) is rejected
    assert gateway.cancelled == []

    # Then: 미지 client_id
    with pytest.raises(ExecutionError):
        manager.cancel("missing-id")

def _recovered_session(**overrides):
    from src.execution.recovery import RecoveredSession

    base = {"orders": {}, "next_seq": 0, "submit_times": (), "ledger": None}
    base.update(overrides)
    return RecoveredSession(**base)

def _unknown_buy(client_id="live-20260911-000001", symbol="005930"):
    from src.execution.contracts import Order, OrderIntent, OrderStatus, OrderType, Side
    from tests.unit.execution.fakes import T0

    return Order(
        client_id=client_id,
        intent=OrderIntent(symbol, Side.BUY, 3, OrderType.LIMIT, 10_000),
        status=OrderStatus.UNKNOWN,
        created_at=T0,
    )

def test_restored_unknown_blocks_same_symbol_and_side(tmp_path) -> None:
    # Given
    from src.execution.contracts import BrokerOutcome, OrderIntent, OrderStatus, OrderType, OutcomeKind, Side
    from src.execution.oms import OrderManager
    from tests.unit.execution.fakes import ScriptedGateway, make_limits, FakeQuotes, make_quote
    from tests.unit.execution.fakes import T0, FixedClock
    from src.execution.journal import OrderJournal
    from src.execution.ledger import CostModel, Ledger
    from src.core.config import ExecutionMode

    gateway = ScriptedGateway(outcomes=[
        BrokerOutcome(kind=OutcomeKind.ACCEPTED, broker_order_no="0000000001", broker_org_no="91252", order_time="090001")
    ])
    clock = FixedClock(T0)
    costs = CostModel(commission_bps=1.5, sell_tax_bps=20.0)
    ledger = Ledger(cash_krw=1_000_000, costs=costs)
    journal = OrderJournal(root=tmp_path / "journal", mode=ExecutionMode.LIVE, now=clock)
    manager = OrderManager(
        gateway=gateway, quotes=FakeQuotes(make_quote()), ledger=ledger, limits=make_limits(),
        costs=costs, journal=journal, now=clock, kill_switch=lambda: False,
        recovered=_recovered_session(orders={"live-20260911-000001": _unknown_buy()}, next_seq=1),
    )

    # When
    blocked = manager.submit(OrderIntent("005930", Side.BUY, 1, OrderType.LIMIT, 10_000))

    # Then
    assert blocked.status is OrderStatus.REJECTED
    assert blocked.reject_code == "unknown_pending"
    assert gateway.submitted == []

def test_restored_sequence_never_reuses_client_id(tmp_path) -> None:
    # Given: recovered.next_seq == 7
    from src.execution.contracts import BrokerOutcome, OrderIntent, OrderStatus, OrderType, OutcomeKind, Side
    from src.execution.oms import OrderManager
    from src.core.config import ExecutionMode
    from src.execution.journal import OrderJournal
    from src.execution.ledger import CostModel, Ledger
    from tests.unit.execution.fakes import T0, FakeQuotes, FixedClock, ScriptedGateway, make_limits, make_quote

    gateway = ScriptedGateway(outcomes=[
        BrokerOutcome(kind=OutcomeKind.ACCEPTED, broker_order_no="0000000001", broker_org_no="91252", order_time="090001")
    ])
    clock = FixedClock(T0)
    costs = CostModel(commission_bps=1.5, sell_tax_bps=20.0)
    manager = OrderManager(
        gateway=gateway, quotes=FakeQuotes(make_quote()), ledger=Ledger(cash_krw=1_000_000, costs=costs),
        limits=make_limits(), costs=costs, journal=OrderJournal(root=tmp_path / "journal", mode=ExecutionMode.LIVE, now=clock),
        now=clock, kill_switch=lambda: False, recovered=_recovered_session(next_seq=7),
    )

    # When
    order = manager.submit(OrderIntent("005930", Side.BUY, 1, OrderType.LIMIT, 10_000))

    # Then
    assert order.status is OrderStatus.ACCEPTED
    assert order.client_id.endswith("000008")

def test_restored_rate_window_is_enforced(tmp_path) -> None:
    # Given: 60초 내 10건 복원
    import datetime as dt

    from src.execution.contracts import OrderIntent, OrderStatus, OrderType, Side
    from tests.unit.execution.fakes import T0, ScriptedGateway, make_limits, make_manager

    gateway = ScriptedGateway()
    manager, _ = make_manager(
        tmp_path, gateway, limits=make_limits(max_orders_per_minute=10),
        recovered=_recovered_session(submit_times=tuple(T0 - dt.timedelta(seconds=i) for i in range(10))),
    )

    # When
    order = manager.submit(OrderIntent("005930", Side.BUY, 1, OrderType.LIMIT, 10_000))

    # Then
    assert order.status is OrderStatus.REJECTED
    assert order.reject_code == "order_rate_limit"

def test_restored_realized_loss_is_enforced(tmp_path) -> None:
    # Given
    from src.core.config import ExecutionMode
    from src.execution.contracts import BrokerOutcome, OrderIntent, OrderStatus, OrderType, OutcomeKind, Side
    from src.execution.ledger import CostModel
    from src.execution.recovery import restore_session
    from tests.unit.execution.test_recovery import _intent_rec, _rec
    from tests.unit.execution.fakes import ScriptedGateway, make_limits, make_manager

    records = [
        _rec("session_start", holdings=[{"symbol": "005930", "qty": 4, "cost_krw": 800_000}]),
        _intent_rec("live-20260911-000001", intent={"side": "sell", "qty": 3}),
        _rec("fill", client_id="live-20260911-000001", delta_qty=3, delta_amount_krw=300_000,
             cumulative_qty=3, cumulative_amount_krw=300_000),
        _rec("status", client_id="live-20260911-000001", status="filled", filled_qty=3),
    ]
    recovered = restore_session(records, mode=ExecutionMode.LIVE, costs=CostModel(1.5, 20.0))
    assert recovered.ledger is not None
    assert recovered.ledger.realized_pnl_krw == -300_645
    gateway = ScriptedGateway(outcomes=[BrokerOutcome(kind=OutcomeKind.ACCEPTED, broker_order_no="2")])
    manager, _ = make_manager(
        tmp_path, gateway, limits=make_limits(max_daily_loss_krw=300_000),
        recovered=recovered, ledger=recovered.ledger,
    )

    # When / Then: 매수는 거부, 매도는 통과(리스크 다음 단계로)
    buy = manager.submit(OrderIntent("005930", Side.BUY, 1, OrderType.LIMIT, 10_000))
    assert buy.status is OrderStatus.REJECTED
    assert buy.reject_code == "daily_loss_limit"
    sell = manager.submit(OrderIntent("005930", Side.SELL, 1, OrderType.LIMIT, 10_000))
    assert sell.status is OrderStatus.ACCEPTED
    assert gateway.submitted == [sell]

def test_restored_open_order_reconciled_to_fill(tmp_path) -> None:
    # Given
    import json

    from src.execution.contracts import BrokerOrderStatus, Order, OrderIntent, OrderStatus, OrderType, Side
    from tests.unit.execution.fakes import T0, ScriptedGateway, make_manager

    gateway = ScriptedGateway()
    accepted = Order(
        client_id="live-20260911-000007",
        intent=OrderIntent("005930", Side.BUY, 10, OrderType.LIMIT, 10_000),
        status=OrderStatus.ACCEPTED,
        created_at=T0,
        broker_order_no="0000000007",
        broker_org_no="91252",
        broker_order_time="090001",
    )
    manager, _ = make_manager(tmp_path, gateway, recovered=_recovered_session(orders={accepted.client_id: accepted}))
    gateway.statuses = [
        BrokerOrderStatus(
            broker_order_no="0000000007", original_order_no="", org_no="91252", symbol="005930", side=Side.BUY,
            ordered_qty=10, price=10_000, filled_qty=10, filled_amount_krw=100_000, remaining_qty=0, rejected_qty=0,
            cancelled=False, order_time="090001",
        )
    ]

    # When
    manager.reconcile()

    # Then
    assert accepted.status is OrderStatus.FILLED
    assert manager.ledger.qty("005930") == 10
    lines = (tmp_path / "journal" / "2026-09-11.jsonl").read_text(encoding="utf-8").splitlines()
    fills = [json.loads(line) for line in lines if json.loads(line)["event"] == "fill"]
    assert len(fills) == 1
    assert fills[0]["cumulative_amount_krw"] == 100_000

def test_blocked_manager_rejects_submits_but_still_cancels(tmp_path) -> None:
    # Given
    from src.execution.contracts import BrokerOutcome, OrderIntent, OrderStatus, OrderType, OutcomeKind, Side
    from tests.unit.execution.fakes import ScriptedGateway, make_manager

    gateway = ScriptedGateway(outcomes=[
        BrokerOutcome(kind=OutcomeKind.ACCEPTED, broker_order_no="0000000001", broker_org_no="91252", order_time="090001")
    ])
    manager, _ = make_manager(tmp_path, gateway)
    open_order = manager.submit(OrderIntent("005930", Side.BUY, 1, OrderType.LIMIT, 10_000))
    manager.block_submits("position mismatch")

    # When
    rejected = manager.submit(OrderIntent("005930", Side.BUY, 1, OrderType.LIMIT, 10_000))
    cancelled = manager.cancel(open_order.client_id)

    # Then
    assert rejected.status is OrderStatus.REJECTED
    assert rejected.reject_code == "account_mismatch"
    assert len(gateway.submitted) == 1
    assert cancelled is open_order
    assert len(gateway.cancelled) == 1

def test_submit_journals_latency(tmp_path) -> None:
    # Given
    import json

    from src.execution.contracts import BrokerOutcome, OrderIntent, OrderStatus, OrderType, OutcomeKind, Side
    from tests.unit.execution.fakes import ScriptedGateway, make_manager

    gateway = ScriptedGateway(outcomes=[
        BrokerOutcome(kind=OutcomeKind.ACCEPTED, broker_order_no="0000000001", broker_org_no="91252", order_time="090001")
    ])
    ticks = iter([100.0, 100.25, 100.5])
    manager, _ = make_manager(tmp_path, gateway)
    manager._monotonic = lambda: next(ticks)

    # When
    order = manager.submit(OrderIntent("005930", Side.BUY, 1, OrderType.LIMIT, 10_000))

    # Then
    assert order.status is OrderStatus.ACCEPTED
    lines = (tmp_path / "journal" / "2026-09-11.jsonl").read_text(encoding="utf-8").splitlines()
    results = [json.loads(line) for line in lines if json.loads(line)["event"] == "submit_result"]
    assert len(results) == 1
    assert results[0]["pretrade_ms"] == 250.0
    assert results[0]["gateway_ms"] == 250.0

def test_close_is_idempotent(tmp_path) -> None:
    # Given
    from tests.unit.execution.fakes import ScriptedGateway, make_manager

    manager, _ = make_manager(tmp_path, ScriptedGateway())
    calls = []
    manager._on_close = lambda: calls.append(1)

    # When
    manager.close()
    manager.close()

    # Then
    assert calls == [1]


def test_closed_manager_cannot_bypass_session_lock(tmp_path) -> None:
    import pytest

    from src.execution.contracts import ExecutionError, OrderIntent, OrderType, Side
    from tests.unit.execution.fakes import ScriptedGateway, make_manager

    gateway = ScriptedGateway()
    manager, _ = make_manager(tmp_path, gateway)
    manager.close()
    with pytest.raises(ExecutionError, match="session is closed"):
        manager.submit(OrderIntent("005930", Side.BUY, 1, OrderType.LIMIT, 10_000))
    with pytest.raises(ExecutionError, match="session is closed"):
        manager.cancel("live-20260911-000001")
    with pytest.raises(ExecutionError, match="session is closed"):
        manager.reconcile()
    assert gateway.submitted == []
    assert gateway.cancelled == []


def test_reconcile_journals_cancel_evidence_on_status_change(tmp_path) -> None:
    # Given: 자식 취소 행이 있는 접수 주문
    import json

    from src.execution.contracts import BrokerOrderStatus, Order, OrderIntent, OrderStatus, OrderType, Side
    from tests.unit.execution.fakes import T0, ScriptedGateway, make_manager

    gateway = ScriptedGateway()
    accepted = Order(
        client_id="live-20260911-000009",
        intent=OrderIntent("005930", Side.BUY, 1, OrderType.LIMIT, 10_000),
        status=OrderStatus.ACCEPTED,
        created_at=T0,
        broker_order_no="0000000009",
        broker_org_no="91252",
        broker_order_time="090001",
    )
    manager, _ = make_manager(tmp_path, gateway, recovered=_recovered_session(orders={accepted.client_id: accepted}))
    gateway.statuses = [
        BrokerOrderStatus(
            broker_order_no="0000000009", original_order_no="", org_no="91252", symbol="005930", side=Side.BUY,
            ordered_qty=1, price=10_000, filled_qty=0, filled_amount_krw=0, remaining_qty=1, rejected_qty=0,
            cancelled=False, order_time="090001",
        ),
        BrokerOrderStatus(
            broker_order_no="0000000010", original_order_no="0000000009", org_no="91252", symbol="005930",
            side=Side.BUY, ordered_qty=1, price=10_000, filled_qty=0, filled_amount_krw=0, remaining_qty=0,
            rejected_qty=0, cancelled=False, order_time="090100",
        ),
    ]

    # When
    manager.reconcile()

    # Then: 어떤 증거로 CANCELLED 가 되었는지 저널에 남는다
    assert accepted.status is OrderStatus.CANCELLED
    lines = (tmp_path / "journal" / "2026-09-11.jsonl").read_text(encoding="utf-8").splitlines()
    status = [json.loads(line) for line in lines if json.loads(line)["event"] == "status"]
    assert status[-1]["status"] == "cancelled"
    assert status[-1]["evidence"] == {"cncl_yn": False, "cancel_child": True, "rmn_qty": 1, "rejected_qty": 0}
