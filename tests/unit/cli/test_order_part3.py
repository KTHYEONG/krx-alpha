"""Part3: order/order-cancel/order-status CLI guards."""

import argparse
import logging

import pytest

from src.cli import order as order_cli
from src.execution.contracts import Order, OrderStatus, OrderType, Side
from tests.unit.execution.fakes import T0


def _order(status, client_id="paper-20260911-000001", odno="0000000001"):
    return Order(
        client_id=client_id,
        intent=__import__("src.execution.contracts", fromlist=["OrderIntent"]).OrderIntent(
            "005930", Side.BUY, 1, OrderType.LIMIT, 10_000
        ),
        status=status,
        created_at=T0,
        broker_order_no=odno,
        broker_org_no="91252",
        broker_order_time="090001",
    )


class _Ledger:
    def __init__(self, pnl=0):
        self.realized_pnl_krw = pnl


class _Manager:
    def __init__(self, order=None, *, orders=None, block=None, pnl=0):
        self._order = order
        self.orders = {order.client_id: order} if order is not None else dict(orders or {})
        self.submit_block_reason = block
        self.ledger = _Ledger(pnl)
        self.reconciled = 0
        self.awaited: list[tuple[str, float]] = []
        self.cancelled: list[str] = []
        self.closed = 0
        self.posts = 0
        self._await_script: list[OrderStatus] = []

    def submit(self, intent):
        return self._order

    def reconcile(self):
        self.reconciled += 1
        return []

    def await_terminal(self, client_id, *, timeout_s, interval_s):
        self.awaited.append((client_id, timeout_s))
        if self._await_script:
            self.orders[client_id].status = self._await_script.pop(0)
        return self.orders[client_id]

    def cancel(self, client_id):
        order = self.orders[client_id]
        if order.broker_order_no is None:
            from src.execution.contracts import ExecutionError

            raise ExecutionError("cannot cancel without broker order number; reconcile first")
        self.cancelled.append(client_id)
        self.posts += 1
        return order

    def close(self):
        self.closed += 1


def _env(monkeypatch, tmp_path, mode=None):
    monkeypatch.setenv("KIS_APP_KEY", "k")
    monkeypatch.setenv("KIS_APP_SECRET", "s")
    monkeypatch.setenv("KIS_ACCOUNT_NO", "12345678")
    monkeypatch.setenv("KIS_ACCOUNT_PRODUCT_CODE", "01")
    monkeypatch.setenv("KRX_ALPHA_EXEC_DATA_ROOT", str(tmp_path))
    if mode is None:
        monkeypatch.delenv("KRX_ALPHA_EXEC_MODE", raising=False)
    else:
        monkeypatch.setenv("KRX_ALPHA_EXEC_MODE", mode)
    monkeypatch.delenv("KRX_ALPHA_EXEC_LIVE_ARMED", raising=False)


def test_wait_success_and_timeout(tmp_path, monkeypatch) -> None:
    _env(monkeypatch, tmp_path)
    filled = _order(OrderStatus.ACCEPTED)
    mgr = _Manager(filled)
    mgr._await_script = [OrderStatus.FILLED]
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: mgr)
    args = argparse.Namespace(symbol="005930", side="buy", qty=1, type="limit", price=10_000, wait_s=5.0, cancel_on_timeout=False)
    assert order_cli.run(args) == 0
    assert len(mgr.awaited) == 1
    assert mgr.cancelled == []
    assert mgr.closed == 1

    open_order = _order(OrderStatus.ACCEPTED)
    mgr2 = _Manager(open_order)
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: mgr2)
    assert order_cli.run(args) == 6
    assert mgr2.cancelled == []


def test_cancel_on_timeout_and_race_fill(tmp_path, monkeypatch) -> None:
    _env(monkeypatch, tmp_path)
    order = _order(OrderStatus.ACCEPTED)
    mgr = _Manager(order)
    mgr._await_script = [OrderStatus.ACCEPTED, OrderStatus.CANCELLED]
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: mgr)
    args = argparse.Namespace(symbol="005930", side="buy", qty=1, type="limit", price=10_000, wait_s=5.0, cancel_on_timeout=True)
    assert order_cli.run(args) == 6
    assert mgr.cancelled == [order.client_id]

    order2 = _order(OrderStatus.ACCEPTED)
    mgr2 = _Manager(order2)
    mgr2._await_script = [OrderStatus.ACCEPTED, OrderStatus.FILLED]
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: mgr2)
    assert order_cli.run(args) == 0


def test_invalid_combo_before_io(tmp_path, monkeypatch) -> None:
    _env(monkeypatch, tmp_path)

    def _never(**kwargs):
        raise AssertionError("must not build")

    monkeypatch.setattr(order_cli, "build_order_manager", _never)
    args = argparse.Namespace(symbol="005930", side="buy", qty=1, type="limit", price=10_000, wait_s=0.0, cancel_on_timeout=True)
    assert order_cli.run(args) == 4


def test_unknown_submit_not_waited(tmp_path, monkeypatch) -> None:
    _env(monkeypatch, tmp_path)
    from src.execution.contracts import OrderIntent

    unknown = Order(
        client_id="paper-20260911-000001",
        intent=OrderIntent("005930", Side.BUY, 1, OrderType.LIMIT, 10_000),
        status=OrderStatus.UNKNOWN,
        created_at=T0,
    )
    mgr = _Manager(unknown)
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: mgr)
    args = argparse.Namespace(symbol="005930", side="buy", qty=1, type="limit", price=10_000, wait_s=5.0, cancel_on_timeout=True)
    assert order_cli.run(args) == 5
    assert mgr.awaited == []
    assert mgr.cancelled == []


def _make_build(mgr):
    def _build(**kwargs):
        return mgr

    return _build


def test_unknown_submit_reconcile_branches(tmp_path, monkeypatch) -> None:
    _env(monkeypatch, tmp_path)
    from src.execution.contracts import BrokerIntegrityError, KisApiError, OrderIntent

    def _unknown():
        return Order(
            client_id="paper-20260911-000001",
            intent=OrderIntent("005930", Side.BUY, 1, OrderType.LIMIT, 10_000),
            status=OrderStatus.UNKNOWN,
            created_at=T0,
        )

    class _Warn(_Manager):
        def reconcile(self):
            self.reconciled += 1
            raise KisApiError("TRANSPORT", "timeout")

    mgr = _Warn(_unknown())
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: mgr)
    args = argparse.Namespace(symbol="005930", side="buy", qty=1, type="limit", price=10_000, wait_s=0.0, cancel_on_timeout=False)
    assert order_cli.run(args) == 5

    class _Err(_Manager):
        def reconcile(self):
            self.reconciled += 1
            raise BrokerIntegrityError("boom")

    mgr2 = _Err(_unknown())
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: mgr2)
    with pytest.raises(BrokerIntegrityError):
        order_cli.run(args)
    assert mgr2.closed == 1

    for end, code in [(OrderStatus.REJECTED, 3), (OrderStatus.ACCEPTED, 0)]:
        u = _unknown()

        class _Adopt(_Manager):
            target = end
            key = u.client_id

            def reconcile(self):
                self.reconciled += 1
                self.orders[self.key].status = self.target
                return []

        m = _Adopt(u)
        monkeypatch.setattr(order_cli, "build_order_manager", _make_build(m))
        assert order_cli.run(args) == code


def test_wait_zero_reconcile_outcomes(tmp_path, monkeypatch) -> None:
    _env(monkeypatch, tmp_path)
    base = argparse.Namespace(symbol="005930", side="buy", qty=1, type="limit", price=10_000, wait_s=0.0, cancel_on_timeout=False)

    for end, code in [(OrderStatus.REJECTED, 3), (OrderStatus.FILLED, 0)]:
        o = _order(OrderStatus.ACCEPTED)

        class _Mut(_Manager):
            target = end
            key = o.client_id

            def reconcile(self):
                self.reconciled += 1
                self.orders[self.key].status = self.target
                return []

        monkeypatch.setattr(order_cli, "build_order_manager", _make_build(_Mut(o)))
        assert order_cli.run(base) == code


def test_integrity_errors_after_assembly_propagate_and_release_session(tmp_path, monkeypatch) -> None:
    # A submitted order may be live at the broker: code 4 (nothing sent) would be a lie.
    _env(monkeypatch, tmp_path)
    from src.execution.contracts import BrokerIntegrityError, IllegalTransitionError

    class _ReconcileBoom(_Manager):
        def reconcile(self):
            raise BrokerIntegrityError("cumulative fill moved backwards")

    class _AwaitBoom(_Manager):
        def await_terminal(self, client_id, *, timeout_s, interval_s):
            raise IllegalTransitionError("filled->accepted")

    class _ConfirmBoom(_Manager):
        def await_terminal(self, client_id, *, timeout_s, interval_s):
            self.awaited.append((client_id, timeout_s))
            if len(self.awaited) == 2:
                raise BrokerIntegrityError("cumulative fill moved backwards")
            return self.orders[client_id]

    def _args(wait_s, cancel):
        return argparse.Namespace(symbol="005930", side="buy", qty=1, type="limit", price=10_000, wait_s=wait_s, cancel_on_timeout=cancel)

    cid = _order(OrderStatus.ACCEPTED).client_id
    cases = [
        (_ReconcileBoom, BrokerIntegrityError, lambda: order_cli.run(_args(0.0, False))),
        (_AwaitBoom, IllegalTransitionError, lambda: order_cli.run(_args(5.0, False))),
        (_ConfirmBoom, BrokerIntegrityError, lambda: order_cli.run(_args(5.0, True))),
        (_AwaitBoom, IllegalTransitionError, lambda: order_cli.run_cancel(argparse.Namespace(client_id=cid))),
        (_ReconcileBoom, BrokerIntegrityError, lambda: order_cli.run_status(argparse.Namespace())),
    ]
    for cls, error, call in cases:
        mgr = cls(_order(OrderStatus.ACCEPTED))
        monkeypatch.setattr(order_cli, "build_order_manager", _make_build(mgr))
        with pytest.raises(error):
            call()
        assert mgr.closed == 1


def test_journal_restore_failure_maps_to_4_with_traceback(tmp_path, monkeypatch, caplog) -> None:
    _env(monkeypatch, tmp_path)
    from src.execution.contracts import ExecutionError

    def _corrupt(**kwargs):
        raise ExecutionError("duplicate intent: paper-20260911-000001")

    monkeypatch.setattr(order_cli, "build_order_manager", _corrupt)
    with caplog.at_level(logging.ERROR):
        assert order_cli.run_status(argparse.Namespace()) == 4
    record = next(r for r in caplog.records if "session_restore" in r.message)
    assert record.exc_info is not None


def test_settings_validation_error_maps_to_4(tmp_path, monkeypatch, caplog) -> None:
    import logging

    _env(monkeypatch, tmp_path)
    monkeypatch.setenv("KRX_ALPHA_EXEC_FILL_POLL_INTERVAL_S", "0")
    with caplog.at_level(logging.ERROR):
        assert order_cli.run_status(argparse.Namespace()) == 4
    assert any("missing_config" in r.message for r in caplog.records)
    for name in ("KIS_APP_KEY", "KIS_APP_SECRET", "KIS_ACCOUNT_NO", "KIS_ACCOUNT_PRODUCT_CODE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("KRX_ALPHA_EXEC_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("KRX_ALPHA_EXEC_MODE", raising=False)
    monkeypatch.delenv("KRX_ALPHA_EXEC_LIVE_ARMED", raising=False)
    with caplog.at_level(logging.ERROR):
        assert order_cli.run_cancel(argparse.Namespace(client_id="x")) == 4
        assert order_cli.run_status(argparse.Namespace()) == 4
    assert any("missing_config" in r.message for r in caplog.records)


def test_cancel_request_transport_error_still_confirms(tmp_path, monkeypatch) -> None:
    _env(monkeypatch, tmp_path)
    from src.execution.contracts import KisApiError

    class _WarnCancel(_Manager):
        def cancel(self, client_id):
            raise KisApiError("EGW001", "busy")

    m = _WarnCancel(_order(OrderStatus.ACCEPTED))
    m._await_script = [OrderStatus.ACCEPTED, OrderStatus.CANCELLED]
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: m)
    args = argparse.Namespace(symbol="005930", side="buy", qty=1, type="limit", price=10_000, wait_s=5.0, cancel_on_timeout=True)
    assert order_cli.run(args) == 6
    assert len(m.awaited) == 2

    o = _order(OrderStatus.ACCEPTED)
    w = _WarnCancel(o)
    w._await_script = [OrderStatus.CANCELLED]
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: w)
    assert order_cli.run_cancel(argparse.Namespace(client_id=o.client_id)) == 0


def test_status_fails_closed_when_reconcile_cannot_reach_broker(tmp_path, monkeypatch, caplog) -> None:
    _env(monkeypatch, tmp_path)
    from src.execution.contracts import KisApiError

    class _RWarn(_Manager):
        def reconcile(self):
            self.reconciled += 1
            raise KisApiError("TRANSPORT", "timeout")

    mgr = _RWarn(_order(OrderStatus.ACCEPTED))
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: mgr)
    with caplog.at_level(logging.INFO):
        assert order_cli.run_status(argparse.Namespace()) == 4
    assert any("reason=reconcile:TRANSPORT" in r.message for r in caplog.records)
    assert not any("stage=order_status mode=paper open=" in r.message for r in caplog.records)
    assert mgr.closed == 1


def test_cancel_confirms_terminal(tmp_path, monkeypatch) -> None:
    _env(monkeypatch, tmp_path)
    order = _order(OrderStatus.ACCEPTED)
    mgr = _Manager(order)
    mgr._await_script = [OrderStatus.CANCELLED]
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: mgr)
    assert order_cli.run_cancel(argparse.Namespace(client_id=order.client_id)) == 0
    assert mgr.posts == 1

    order2 = _order(OrderStatus.ACCEPTED)
    mgr2 = _Manager(order2)
    mgr2._await_script = [OrderStatus.FILLED]
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: mgr2)
    assert order_cli.run_cancel(argparse.Namespace(client_id=order2.client_id)) == 6


def test_cancel_sends_nothing_for_unknown_or_terminal(tmp_path, monkeypatch) -> None:
    _env(monkeypatch, tmp_path)
    mgr = _Manager(None, orders={})
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: mgr)
    assert order_cli.run_cancel(argparse.Namespace(client_id="nope")) == 4
    assert mgr.posts == 0

    filled = _order(OrderStatus.FILLED)
    mgr2 = _Manager(filled)
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: mgr2)
    assert order_cli.run_cancel(argparse.Namespace(client_id=filled.client_id)) == 6
    assert mgr2.posts == 0

    cancelled = _order(OrderStatus.CANCELLED)
    mgr3 = _Manager(cancelled)
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: mgr3)
    assert order_cli.run_cancel(argparse.Namespace(client_id=cancelled.client_id)) == 0

    from src.execution.contracts import OrderIntent

    no_odno = Order(
        client_id="paper-20260911-000009",
        intent=OrderIntent("005930", Side.BUY, 1, OrderType.LIMIT, 10_000),
        status=OrderStatus.ACCEPTED,
        created_at=T0,
        broker_order_no=None,
    )
    mgr4 = _Manager(no_odno)
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: mgr4)
    assert order_cli.run_cancel(argparse.Namespace(client_id=no_odno.client_id)) == 5
    assert mgr4.posts == 0


def test_cancel_works_under_kill_and_block(tmp_path, monkeypatch) -> None:
    _env(monkeypatch, tmp_path)
    from src.execution.contracts import BrokerOrderStatus
    from tests.unit.execution.fakes import ScriptedGateway, make_manager

    order = _order(OrderStatus.ACCEPTED)
    gateway = ScriptedGateway(
        statuses=[
            BrokerOrderStatus(
                broker_order_no="0000000001", original_order_no="", org_no="91252", symbol="005930",
                side=Side.BUY, ordered_qty=1, price=10_000, filled_qty=0, filled_amount_krw=0,
                remaining_qty=0, rejected_qty=0, cancelled=True, order_time="090001",
            )
        ]
    )
    manager, _ = make_manager(tmp_path, gateway, kill=True)
    manager.orders[order.client_id] = order
    manager.block_submits("position mismatch")
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: manager)
    assert order_cli.run_cancel(argparse.Namespace(client_id=order.client_id)) == 0
    assert [o.client_id for o in gateway.cancelled] == [order.client_id]
    assert order.status is OrderStatus.CANCELLED


def test_status_ranks_conditions(tmp_path, monkeypatch, caplog) -> None:
    _env(monkeypatch, tmp_path)
    open_order = _order(OrderStatus.ACCEPTED)
    mgr = _Manager(open_order)
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: mgr)
    with caplog.at_level(logging.INFO):
        assert order_cli.run_status(argparse.Namespace()) == 0
    assert sum("stage=order_open" in r.message for r in caplog.records) == 1
    assert sum("stage=order_status mode=paper open=1 unknown=0 blocked=False" in r.message for r in caplog.records) == 1

    caplog.clear()
    from src.execution.contracts import OrderIntent

    unknown = Order(
        client_id="paper-20260911-000002",
        intent=OrderIntent("005930", Side.BUY, 1, OrderType.LIMIT, 10_000),
        status=OrderStatus.UNKNOWN,
        created_at=T0,
        broker_order_no="0000000002",
    )
    mgr2 = _Manager(None, orders={unknown.client_id: unknown})
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: mgr2)
    with caplog.at_level(logging.INFO):
        assert order_cli.run_status(argparse.Namespace()) == 5

    caplog.clear()
    mgr3 = _Manager(None, orders={unknown.client_id: unknown}, block="position mismatch")
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: mgr3)
    assert order_cli.run_status(argparse.Namespace()) == 7


def test_session_failures_map_for_all_handlers(tmp_path, monkeypatch, caplog) -> None:
    import logging

    from src.execution.contracts import AccountCheckError, SessionLockError

    _env(monkeypatch, tmp_path)
    args = argparse.Namespace(symbol="005930", side="buy", qty=1, type="limit", price=10_000, wait_s=0.0, cancel_on_timeout=False)

    def _locked(**kwargs):
        raise SessionLockError("busy")

    monkeypatch.setattr(order_cli, "build_order_manager", _locked)
    with caplog.at_level(logging.ERROR):
        assert order_cli.run(args) == 4
        assert order_cli.run_cancel(argparse.Namespace(client_id="x")) == 4
        assert order_cli.run_status(argparse.Namespace()) == 4
    assert any("session_locked" in r.message for r in caplog.records)

    def _acct(**kwargs):
        raise AccountCheckError("bad")

    monkeypatch.setattr(order_cli, "build_order_manager", _acct)
    caplog.clear()
    with caplog.at_level(logging.ERROR):
        assert order_cli.run(args) == 4
        assert order_cli.run_cancel(argparse.Namespace(client_id="x")) == 4
        assert order_cli.run_status(argparse.Namespace()) == 4
    assert any("account_check" in r.message for r in caplog.records)


def test_logs_carry_no_credentials(tmp_path, monkeypatch, caplog) -> None:
    monkeypatch.setenv("KIS_APP_KEY", "FAKEKEY123")
    monkeypatch.setenv("KIS_APP_SECRET", "FAKESECRET456")
    monkeypatch.setenv("KIS_ACCOUNT_NO", "99998888")
    monkeypatch.setenv("KIS_ACCOUNT_PRODUCT_CODE", "01")
    monkeypatch.setenv("KRX_ALPHA_EXEC_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("KRX_ALPHA_EXEC_MODE", raising=False)
    monkeypatch.delenv("KRX_ALPHA_EXEC_LIVE_ARMED", raising=False)
    order = _order(OrderStatus.ACCEPTED)
    mgr = _Manager(order)
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: mgr)
    with caplog.at_level(logging.INFO):
        order_cli.run(argparse.Namespace(symbol="005930", side="buy", qty=1, type="limit", price=10_000, wait_s=0.0, cancel_on_timeout=False))
        order_cli.run_cancel(argparse.Namespace(client_id=order.client_id))
        order_cli.run_status(argparse.Namespace())
    blob = "\n".join(r.message for r in caplog.records)
    assert "FAKEKEY123" not in blob
    assert "FAKESECRET456" not in blob
    assert "99998888" not in blob


def test_add_parser_registers_all_commands() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    order_cli.add_parser(sub)
    for cmd, extra in [("order", ["--symbol", "005930", "--side", "buy", "--qty", "1"]), ("order-cancel", ["--client-id", "x"]), ("order-status", []), ("order-resolve", ["--client-id", "x", "--reason", "r"])]:
        ns = parser.parse_args([cmd, *extra])
        assert callable(ns.handler)
    ns = parser.parse_args(["order", "--symbol", "005930", "--side", "buy", "--qty", "1", "--wait-s", "5", "--cancel-on-timeout"])
    assert ns.wait_s == 5.0
    assert ns.cancel_on_timeout is True


def test_config_poll_validation() -> None:
    from pydantic import ValidationError

    from src.core.config import ExecutionSettings

    with pytest.raises(ValidationError):
        ExecutionSettings(fill_poll_interval_s=0)
    with pytest.raises(ValidationError):
        ExecutionSettings(cancel_confirm_wait_s=-1)


def test_resolve_requires_confirmation_and_maps_outcomes(tmp_path, monkeypatch) -> None:
    from src.execution.contracts import ExecutionError, KisApiError

    _env(monkeypatch, tmp_path)
    unconfirmed = argparse.Namespace(client_id="x", reason="r", confirm_not_at_broker=False)
    assert order_cli.run_resolve(unconfirmed) == 4

    class _Resolver(_Manager):
        def __init__(self, order, error=None):
            super().__init__(order)
            self.error = error

        def resolve_unknown(self, client_id, *, reason):
            if self.error is not None:
                raise self.error
            self.orders[client_id].status = OrderStatus.REJECTED
            return self.orders[client_id]

    args = argparse.Namespace(client_id="paper-20260911-000001", reason="no order at HTS", confirm_not_at_broker=True)
    order = _order(OrderStatus.UNKNOWN, odno=None)
    ok_mgr = _Resolver(order)
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **k: ok_mgr)
    assert order_cli.run_resolve(args) == 0
    assert ok_mgr.closed == 1

    for exc in (ExecutionError("matched"), KisApiError("EGW00000", "down")):
        failing = _Resolver(_order(OrderStatus.UNKNOWN, odno=None), error=exc)
        monkeypatch.setattr(order_cli, "build_order_manager", lambda *, _m=failing, **k: _m)
        assert order_cli.run_resolve(args) == 4
        assert failing.closed == 1


def test_resolve_assembly_failure_maps_to_4(tmp_path, monkeypatch) -> None:
    from src.execution.contracts import SessionLockError

    _env(monkeypatch, tmp_path)

    def _locked(**kwargs):
        raise SessionLockError("held")

    monkeypatch.setattr(order_cli, "build_order_manager", _locked)
    args = argparse.Namespace(client_id="x", reason="r", confirm_not_at_broker=True)
    assert order_cli.run_resolve(args) == 4
