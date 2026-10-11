"""Part3: await_terminal invariant guards."""

import logging

import pytest

from src.execution.contracts import (
    BrokerIntegrityError,
    BrokerOrderStatus,
    BrokerOutcome,
    ExecutionError,
    KisApiError,
    OrderIntent,
    OrderStatus,
    OrderType,
    OutcomeKind,
    Side,
)
from tests.unit.execution.fakes import ScriptedGateway, make_manager


def _accepted_manager(tmp_path, statuses=None):
    gateway = ScriptedGateway(
        outcomes=[
            BrokerOutcome(
                kind=OutcomeKind.ACCEPTED,
                broker_order_no="0000000001",
                broker_org_no="91252",
                order_time="090001",
            )
        ],
        statuses=statuses or [],
    )
    manager, _ = make_manager(tmp_path, gateway)
    order = manager.submit(OrderIntent("005930", Side.BUY, 5, OrderType.LIMIT, 10_000))
    return manager, gateway, order


def _row(filled, amount=None):
    return BrokerOrderStatus(
        broker_order_no="0000000001",
        original_order_no="",
        org_no="91252",
        symbol="005930",
        side=Side.BUY,
        ordered_qty=5,
        price=10_000,
        filled_qty=filled,
        filled_amount_krw=amount if amount is not None else filled * 10_000,
        remaining_qty=5 - filled,
        rejected_qty=0,
        cancelled=False,
        order_time="090001",
    )


def test_first_poll_is_immediate(tmp_path) -> None:
    manager, _, order = _accepted_manager(tmp_path, [_row(5)])
    sleeps: list[float] = []
    out = manager.await_terminal(order.client_id, timeout_s=10, interval_s=1, sleep=sleeps.append)
    assert out.status is OrderStatus.FILLED
    assert sleeps == []


def test_polls_until_fill_arrives(tmp_path) -> None:
    manager, gateway, order = _accepted_manager(tmp_path, [_row(0)])
    calls = {"n": 0}
    orig = manager.reconcile

    def _scripted():
        calls["n"] += 1
        if calls["n"] == 3:
            gateway.statuses = [_row(5)]
        return orig()

    manager.reconcile = _scripted  # type: ignore[method-assign]
    sleeps: list[float] = []
    out = manager.await_terminal(order.client_id, timeout_s=30, interval_s=2, sleep=sleeps.append)
    assert out.status is OrderStatus.FILLED
    assert sleeps == [2, 2]


def test_timeout_returns_open_untouched(tmp_path) -> None:
    manager, gateway, order = _accepted_manager(tmp_path, [_row(0)])
    ticks = iter([100.0, 100.0, 105.0, 111.0])
    manager._monotonic = lambda: next(ticks)
    sleeps: list[float] = []
    out = manager.await_terminal(order.client_id, timeout_s=10, interval_s=5, sleep=sleeps.append)
    assert out.status is OrderStatus.ACCEPTED
    assert gateway.cancelled == []
    assert len(sleeps) == 2


def test_timeout_zero_means_single_poll(tmp_path) -> None:
    manager, _, order = _accepted_manager(tmp_path, [_row(0)])
    sleeps: list[float] = []
    out = manager.await_terminal(order.client_id, timeout_s=0, interval_s=1, sleep=sleeps.append)
    assert out.status is OrderStatus.ACCEPTED
    assert sleeps == []


def test_transient_broker_error_does_not_abort(tmp_path, caplog) -> None:
    manager, gateway, order = _accepted_manager(tmp_path, [_row(0)])
    calls = {"n": 0}
    orig = manager.reconcile

    def _scripted():
        calls["n"] += 1
        if calls["n"] == 2:
            raise KisApiError("TRANSPORT", "ReadTimeout")
        if calls["n"] >= 3:
            gateway.statuses = [_row(5)]
        return orig()

    manager.reconcile = _scripted  # type: ignore[method-assign]
    with caplog.at_level(logging.WARNING):
        out = manager.await_terminal(order.client_id, timeout_s=30, interval_s=1, sleep=lambda s: None)
    assert out.status is OrderStatus.FILLED
    assert any(r.levelno == logging.WARNING and "TRANSPORT" in r.message for r in caplog.records)


def test_first_poll_error_then_fill(tmp_path, caplog) -> None:
    manager, gateway, order = _accepted_manager(tmp_path, [_row(0)])
    calls = {"n": 0}
    orig = manager.reconcile

    def _scripted():
        calls["n"] += 1
        if calls["n"] == 1:
            raise KisApiError("TRANSPORT", "ReadTimeout")
        gateway.statuses = [_row(5)]
        return orig()

    manager.reconcile = _scripted  # type: ignore[method-assign]
    with caplog.at_level(logging.WARNING):
        out = manager.await_terminal(order.client_id, timeout_s=10, interval_s=1, sleep=lambda s: None)
    assert out.status is OrderStatus.FILLED


def test_integrity_violation_propagates(tmp_path) -> None:
    manager, gateway, order = _accepted_manager(tmp_path, [_row(2)])
    manager.reconcile()
    assert order.status is OrderStatus.PARTIALLY_FILLED
    gateway.statuses = [_row(1)]
    with pytest.raises(BrokerIntegrityError):
        manager.await_terminal(order.client_id, timeout_s=10, interval_s=1, sleep=lambda s: None)


def test_unknown_client_id_is_error_without_broker_request(tmp_path) -> None:
    gateway = ScriptedGateway()
    manager, _ = make_manager(tmp_path, gateway)
    fetches = {"n": 0}
    orig_fetch = gateway.fetch_statuses

    def _count():
        fetches["n"] += 1
        return orig_fetch()

    gateway.fetch_statuses = _count  # type: ignore[method-assign]
    with pytest.raises(ExecutionError):
        manager.await_terminal("missing", timeout_s=10, interval_s=1, sleep=lambda s: None)
    assert fetches["n"] == 0
