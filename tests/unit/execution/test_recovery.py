"""Session recovery invariant guards (Part 2)."""

from __future__ import annotations

import datetime as dt
from typing import Any
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
T0 = dt.datetime(2026, 9, 11, 9, 0, 0, tzinfo=KST)

_COSTS = None


def _costs():
    global _COSTS
    if _COSTS is None:
        from src.execution.ledger import CostModel

        _COSTS = CostModel(commission_bps=1.5, sell_tax_bps=20.0)
    return _COSTS


def _intent(symbol: str = "005930", side: str = "buy", qty: int = 10, price: int = 10_000) -> dict[str, Any]:
    return {"symbol": symbol, "side": side, "qty": qty, "order_type": "limit", "limit_price": price, "tag": ""}


def _rec(event: str, seq: int = 0, **fields: Any) -> dict[str, Any]:
    ts = (T0 + dt.timedelta(seconds=seq)).isoformat()
    base: dict[str, Any] = {"ts": ts, "mode": "mock", "event": event}
    base.update(fields)
    return base


def _intent_rec(client_id: str, seq: int = 0, **overrides: Any) -> dict[str, Any]:
    intent = _intent()
    intent.update(overrides.pop("intent", {}))
    return _rec("intent", seq, client_id=client_id, intent=intent, **overrides)


def _result_rec(client_id: str, status: str, seq: int = 0, odno: str | None = "0000117057") -> dict[str, Any]:
    return _rec(
        "submit_result",
        seq,
        client_id=client_id,
        status=status,
        outcome={
            "kind": "accepted" if status == "accepted" else status,
            "broker_order_no": odno,
            "broker_org_no": "91252" if odno else None,
            "order_time": "090001" if odno else None,
            "code": "",
            "message": "",
        },
    )


def test_sequence_continues_across_processes() -> None:
    from src.core.config import ExecutionMode
    from src.execution.recovery import restore_session

    records = [
        _rec("risk_reject", 0, client_id="mock-20260911-000001", code="kill_switch"),
        _intent_rec("mock-20260911-000002", 1),
    ]
    recovered = restore_session(records, mode=ExecutionMode.MOCK, costs=_costs())
    assert recovered.next_seq == 2


def test_intent_without_result_restored_as_unknown() -> None:
    from src.core.config import ExecutionMode
    from src.execution.contracts import OrderStatus
    from src.execution.recovery import restore_session

    records = [_intent_rec("mock-20260911-000001"), _rec("live_send", 1, client_id="mock-20260911-000001")]
    recovered = restore_session(records, mode=ExecutionMode.MOCK, costs=_costs())
    [order] = recovered.orders.values()
    assert order.status is OrderStatus.UNKNOWN
    assert order.broker_order_no is None


def test_accepted_order_keeps_broker_identifiers() -> None:
    from src.core.config import ExecutionMode
    from src.execution.contracts import OrderStatus
    from src.execution.recovery import restore_session

    records = [_intent_rec("mock-20260911-000001"), _result_rec("mock-20260911-000001", "accepted")]
    recovered = restore_session(records, mode=ExecutionMode.MOCK, costs=_costs())
    order = recovered.orders["mock-20260911-000001"]
    assert order.status is OrderStatus.ACCEPTED
    assert (order.broker_order_no, order.broker_org_no, order.broker_order_time) == ("0000117057", "91252", "090001")


def test_adopted_unknown_restored_with_identifiers() -> None:
    from src.core.config import ExecutionMode
    from src.execution.contracts import OrderStatus
    from src.execution.recovery import restore_session

    records = [
        _intent_rec("mock-20260911-000001"),
        _rec(
            "submit_result", 1, client_id="mock-20260911-000001", status="unknown",
            outcome={"kind": "unknown", "broker_order_no": None, "broker_org_no": None, "order_time": None, "code": "transport", "message": "timeout"},
        ),
        _rec("unknown_adopted", 2, client_id="mock-20260911-000001", broker_order_no="0000117057", broker_org_no="91252", order_time="090001"),
        _rec("status", 3, client_id="mock-20260911-000001", status="accepted", filled_qty=0),
    ]
    recovered = restore_session(records, mode=ExecutionMode.MOCK, costs=_costs())
    order = recovered.orders["mock-20260911-000001"]
    assert order.status is OrderStatus.ACCEPTED
    assert (order.broker_order_no, order.broker_org_no, order.broker_order_time) == ("0000117057", "91252", "090001")


def test_partial_fill_restores_cumulative_quantity_and_amount() -> None:
    from src.core.config import ExecutionMode
    from src.execution.recovery import restore_session

    records = [
        _intent_rec("mock-20260911-000001"),
        _result_rec("mock-20260911-000001", "accepted"),
        _rec("fill", 2, client_id="mock-20260911-000001", delta_qty=4, delta_amount_krw=400_000, cumulative_qty=4, cumulative_amount_krw=400_000),
        _rec("status", 3, client_id="mock-20260911-000001", status="partially_filled", filled_qty=4),
    ]
    recovered = restore_session(records, mode=ExecutionMode.MOCK, costs=_costs())
    order = recovered.orders["mock-20260911-000001"]
    assert (order.filled_qty, order.filled_amount_krw, order.remaining_qty) == (4, 400_000, 6)


def test_ledger_replay_equals_live_ledger(tmp_path) -> None:
    import json

    from src.core.config import ExecutionMode
    from src.execution.contracts import BrokerOrderStatus, BrokerOutcome, OrderIntent, OrderType, OutcomeKind, Side
    from src.execution.journal import OrderJournal
    from src.execution.ledger import Ledger
    from src.execution.oms import OrderManager
    from src.execution.recovery import restore_session
    from tests.unit.execution.fakes import FixedClock, ScriptedGateway, make_limits, make_quote, FakeQuotes

    costs = _costs()
    clock = FixedClock(T0)
    journal = OrderJournal(root=tmp_path / "journal", mode=ExecutionMode.LIVE, now=clock)
    ledger = Ledger(cash_krw=0, costs=costs)
    gateway = ScriptedGateway(
        outcomes=[
            BrokerOutcome(kind=OutcomeKind.ACCEPTED, broker_order_no="0000000001", broker_org_no="91252", order_time="090001"),
            BrokerOutcome(kind=OutcomeKind.ACCEPTED, broker_order_no="0000000002", broker_org_no="91252", order_time="090002"),
        ]
    )
    manager = OrderManager(
        gateway=gateway, quotes=FakeQuotes(make_quote()), ledger=ledger, limits=make_limits(),
        costs=costs, journal=journal, now=clock, kill_switch=lambda: False,
    )
    buy = manager.submit(OrderIntent("005930", Side.BUY, 10, OrderType.LIMIT, 10_000))
    gateway.statuses = [
        BrokerOrderStatus(
            broker_order_no="0000000001", original_order_no="", org_no="91252", symbol="005930", side=Side.BUY,
            ordered_qty=10, price=10_000, filled_qty=10, filled_amount_krw=100_000, remaining_qty=0, rejected_qty=0,
            cancelled=False, order_time="090001",
        )
    ]
    manager.reconcile()
    sell = manager.submit(OrderIntent("005930", Side.SELL, 4, OrderType.LIMIT, 10_000))
    gateway.statuses.append(
        BrokerOrderStatus(
            broker_order_no="0000000002", original_order_no="", org_no="91252", symbol="005930", side=Side.SELL,
            ordered_qty=4, price=10_000, filled_qty=4, filled_amount_krw=40_000, remaining_qty=0, rejected_qty=0,
            cancelled=False, order_time="090002",
        )
    )
    manager.reconcile()
    assert (buy.filled_qty, sell.filled_qty) == (10, 4)

    lines = (tmp_path / "journal" / "2026-09-11.jsonl").read_text(encoding="utf-8").splitlines()
    start = {"ts": T0.isoformat(), "mode": "live", "event": "session_start", "holdings": []}
    recovered = restore_session([start] + [json.loads(line) for line in lines], mode=ExecutionMode.LIVE, costs=costs)
    assert recovered.ledger is not None
    assert {s: (p.qty, p.cost_krw) for s, p in recovered.ledger.positions.items()} == {
        s: (p.qty, p.cost_krw) for s, p in manager.ledger.positions.items()
    }
    assert recovered.ledger.cash_krw == manager.ledger.cash_krw
    assert recovered.ledger.realized_pnl_krw == manager.ledger.realized_pnl_krw
    assert recovered.next_seq == 2


def test_rate_window_survives_restart() -> None:
    from src.core.config import ExecutionMode
    from src.execution.recovery import restore_session

    now = dt.datetime(2026, 9, 11, 9, 0, 30, tzinfo=KST)
    records = []
    for i in range(10):
        ts = (now - dt.timedelta(seconds=i * 5)).isoformat()
        records.append({"ts": ts, "mode": "mock", "event": "intent", "client_id": f"mock-20260911-{i + 1:06d}", "intent": _intent()})
    recovered = restore_session(records, mode=ExecutionMode.MOCK, costs=_costs())
    assert len(recovered.submit_times) == 10
    assert recovered.submit_times[0].isoformat() == records[0]["ts"]


def test_paper_restores_sequence_only() -> None:
    from src.core.config import ExecutionMode
    from src.execution.recovery import restore_session

    records = [
        {"ts": T0.isoformat(), "mode": "paper", "event": "intent", "client_id": "paper-20260911-000001", "intent": _intent()},
        {"ts": T0.isoformat(), "mode": "paper", "event": "fill", "client_id": "paper-20260911-000001", "delta_qty": 1, "delta_amount_krw": 10_000, "cumulative_qty": 1, "cumulative_amount_krw": 10_000},
    ]
    recovered = restore_session(records, mode=ExecutionMode.PAPER, costs=_costs())
    assert recovered.next_seq == 1
    assert len(recovered.submit_times) == 1
    assert recovered.orders == {}
    assert recovered.ledger is None


def test_first_session_of_day_has_no_ledger() -> None:
    from src.core.config import ExecutionMode
    from src.execution.recovery import restore_session

    recovered = restore_session([], mode=ExecutionMode.LIVE, costs=_costs())
    assert (recovered.next_seq, recovered.orders, recovered.ledger) == (0, {}, None)


def test_inconsistent_journal_fails_closed() -> None:
    import pytest

    from src.core.config import ExecutionMode
    from src.execution.contracts import ExecutionError
    from src.execution.recovery import restore_session

    with pytest.raises(ExecutionError):
        restore_session(
            [_rec("fill", 0, client_id="mock-20260911-000009", delta_qty=1, delta_amount_krw=1, cumulative_qty=1, cumulative_amount_krw=1)],
            mode=ExecutionMode.MOCK, costs=_costs(),
        )
    with pytest.raises(ExecutionError):
        restore_session(
            [_rec("session_start", 0, holdings_count=0)],
            mode=ExecutionMode.LIVE, costs=_costs(),
        )


def test_malformed_records_fail_closed() -> None:
    import pytest

    from src.core.config import ExecutionMode
    from src.execution.contracts import ExecutionError
    from src.execution.recovery import restore_session

    bad_ts = [_rec("intent", 0, client_id="mock-20260911-000001", intent=_intent(), ts="not-a-time")]
    with pytest.raises(ExecutionError):
        restore_session(bad_ts, mode=ExecutionMode.MOCK, costs=_costs())

    for bad_id in ("nonsense", "mock-20260911-xyz"):
        with pytest.raises(ExecutionError):
            restore_session([_intent_rec(str(bad_id))], mode=ExecutionMode.MOCK, costs=_costs())

    with pytest.raises(ExecutionError):
        restore_session(
            [_rec("intent", 0, client_id="mock-20260911-000001", intent="oops")],
            mode=ExecutionMode.MOCK, costs=_costs(),
        )
    with pytest.raises(ExecutionError):
        restore_session(
            [_rec("intent", 0, client_id="mock-20260911-000001", intent={"symbol": "005930"})],
            mode=ExecutionMode.MOCK, costs=_costs(),
        )

    base = [_intent_rec("mock-20260911-000001")]
    with pytest.raises(ExecutionError):
        restore_session(
            [*base, _rec("submit_result", 1, client_id="mock-20260911-000001", status="bogus", outcome={"kind": "rejected", "code": "x", "message": "y"})],
            mode=ExecutionMode.MOCK, costs=_costs(),
        )
    with pytest.raises(ExecutionError):
        restore_session(
            [*base, _rec("submit_result", 1, client_id="mock-20260911-000001", status="accepted", outcome="oops")],
            mode=ExecutionMode.MOCK, costs=_costs(),
        )
    with pytest.raises(ExecutionError):
        restore_session(
            [*base, _rec("fill", 1, client_id="mock-20260911-000001", delta_qty=1, delta_amount_krw=1, cumulative_qty="bad", cumulative_amount_krw=1)],
            mode=ExecutionMode.MOCK, costs=_costs(),
        )
    with pytest.raises(ExecutionError):
        restore_session(
            [*base, _rec("fill", 1, client_id="mock-20260911-000001", delta_qty=1, delta_amount_krw=1, cumulative_qty=1)],
            mode=ExecutionMode.MOCK, costs=_costs(),
        )
    with pytest.raises(ExecutionError):
        restore_session(
            [*base, _rec("status", 1, client_id="mock-20260911-000001", status="bogus")],
            mode=ExecutionMode.MOCK, costs=_costs(),
        )
    with pytest.raises(ExecutionError):
        restore_session(
            [*base, _rec("status", 1, client_id="mock-20260911-000001", status="accepted", filled_qty="bad")],
            mode=ExecutionMode.MOCK, costs=_costs(),
        )
    with pytest.raises(ExecutionError):
        restore_session(
            [{"ts": T0.isoformat(), "mode": "live", "event": "session_start", "holdings": "oops"}],
            mode=ExecutionMode.LIVE, costs=_costs(),
        )
    with pytest.raises(ExecutionError):
        restore_session(
            [{"ts": T0.isoformat(), "mode": "live", "event": "session_start", "holdings": ["oops"]}],
            mode=ExecutionMode.LIVE, costs=_costs(),
        )
    with pytest.raises(ExecutionError):
        restore_session(
            [{"ts": T0.isoformat(), "mode": "live", "event": "session_start", "holdings": [{"symbol": "A"}]}],
            mode=ExecutionMode.LIVE, costs=_costs(),
        )
    with pytest.raises(ExecutionError):
        restore_session(
            [{"ts": T0.isoformat(), "mode": "live", "event": "session_start", "holdings": []},
             _intent_rec("mock-20260911-000001"),
             _rec("fill", 1, client_id="mock-20260911-000001", delta_qty="bad", delta_amount_krw=1, cumulative_qty=1, cumulative_amount_krw=1)],
            mode=ExecutionMode.LIVE, costs=_costs(),
        )


def test_rejected_submit_result_keeps_broker_reason() -> None:
    from src.core.config import ExecutionMode
    from src.execution.contracts import OrderStatus
    from src.execution.recovery import restore_session

    records = [
        _intent_rec("mock-20260911-000001"),
        _rec("submit_result", 1, client_id="mock-20260911-000001", status="rejected",
             outcome={"kind": "rejected", "broker_order_no": None, "broker_org_no": None, "order_time": None,
                      "code": "APBK0919", "message": "over limit"}),
    ]
    recovered = restore_session(records, mode=ExecutionMode.MOCK, costs=_costs())
    order = recovered.orders["mock-20260911-000001"]
    assert order.status is OrderStatus.REJECTED
    assert (order.reject_code, order.reject_message) == ("APBK0919", "over limit")


def test_position_diffs_cover_both_directions() -> None:
    from src.execution.contracts import Holding
    from src.execution.ledger import Ledger, Position
    from src.execution.recovery import position_diffs

    ledger = Ledger(cash_krw=0, costs=_costs(), positions={"A": Position(qty=3, cost_krw=30)})
    assert position_diffs(ledger, [Holding("A", 3, 30), Holding("B", 1, 10)]) == [("B", 0, 1)]
    assert position_diffs(ledger, []) == [("A", 3, 0)]
    assert position_diffs(ledger, [Holding("A", 3, 999)]) == []


def test_recovery_rejects_accounting_and_causality_corruption() -> None:
    import pytest

    from src.core.config import ExecutionMode
    from src.execution.contracts import ExecutionError
    from src.execution.recovery import restore_session

    client_id = "mock-20260911-000001"
    intent = _intent_rec(client_id)
    corruptions = [
        [intent, intent],
        [_rec("live_send", client_id=client_id)],
        [{**intent, "ts": "2026-09-11T09:00:00"}],
        [{**intent, "client_id": "mock-20260911-²"}],
        [intent, _rec("status", client_id=client_id, status="filled", filled_qty=10)],
        [_intent_rec(client_id, intent={"qty": 1.9})],
        [_rec("session_start", holdings=[{"symbol": "A", "qty": True, "cost_krw": 10}])],
        [intent, _rec("fill", client_id=client_id, cumulative_qty=1, cumulative_amount_krw=10)],
    ]
    for delta_qty, qty, delta_amount, amount in ((1, 11, 100, 100), (1, 2, 100, 100), (1, 1, 100, -100), (1, 1, 100, 200)):
        corruptions.append([
            intent,
            _rec("fill", client_id=client_id, delta_qty=delta_qty, cumulative_qty=qty,
                 delta_amount_krw=delta_amount, cumulative_amount_krw=amount),
        ])
    corruptions.extend([_rec("session_start", holdings=holdings)] for holdings in (
        [{"symbol": "A", "qty": -1, "cost_krw": 10}],
        [{"symbol": "A", "qty": 1, "cost_krw": -10}],
        [{"symbol": "A", "qty": 1, "cost_krw": 10}] * 2,
    ))
    for records in corruptions:
        with pytest.raises(ExecutionError):
            restore_session(records, mode=ExecutionMode.MOCK, costs=_costs())


def test_unknown_resolved_restores_as_terminal_rejected() -> None:
    from src.core.config import ExecutionMode
    from src.execution.contracts import OrderStatus
    from src.execution.recovery import restore_session

    records = [
        _intent_rec("mock-20260911-000001"),
        _rec("unknown_resolved", 2, client_id="mock-20260911-000001", reason="HTS shows no order"),
    ]
    recovered = restore_session(records, mode=ExecutionMode.MOCK, costs=_costs())
    [order] = recovered.orders.values()
    assert order.status is OrderStatus.REJECTED
    assert order.reject_code == "unknown_resolved"
    assert order.reject_message == "HTS shows no order"
