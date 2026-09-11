
def test_order_cli_submits_intent_and_maps_exit_codes(tmp_path, monkeypatch) -> None:
    # Given: paper 설정 env + 매니저 대체
    import argparse

    from src.cli import order as order_cli
    from src.core.config import ExecutionMode
    from src.execution.contracts import KisApiError, Order, OrderStatus, OrderType, Side
    from tests.unit.execution.fakes import T0

    env = {
        "KIS_APP_KEY": "k", "KIS_APP_SECRET": "s", "KIS_ACCOUNT_NO": "12345678", "KIS_ACCOUNT_PRODUCT_CODE": "01",
        "KRX_ALPHA_EXEC_COMMISSION_BPS": "1.5", "KRX_ALPHA_EXEC_DATA_ROOT": str(tmp_path),
    }
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("KRX_ALPHA_EXEC_MODE", raising=False)
    monkeypatch.delenv("KRX_ALPHA_EXEC_LIVE_ARMED", raising=False)

    class _Manager:
        def __init__(self, status, *, reconcile_error=False) -> None:
            self.status = status
            self.reconcile_error = reconcile_error
            self.intents = []
            self.reconciled = 0

        def submit(self, intent):
            self.intents.append(intent)
            if self.status is None:
                raise KisApiError("EGW00201", "rate")
            return Order(client_id="paper-20260911-000001", intent=intent, status=self.status, created_at=T0)

        def reconcile(self):
            self.reconciled += 1
            if self.reconcile_error:
                raise KisApiError("TRANSPORT", "ReadTimeout")
            return []

    limit_args = argparse.Namespace(symbol="005930", side="buy", qty=3, type="limit", price=10_000)
    cases = [
        (_Manager(OrderStatus.ACCEPTED), 0, 1),
        (_Manager(OrderStatus.FILLED), 0, 1),
        (_Manager(OrderStatus.ACCEPTED, reconcile_error=True), 0, 1),
        (_Manager(OrderStatus.REJECTED), 3, 0),
        (_Manager(OrderStatus.UNKNOWN), 5, 1),
        (_Manager(None), 4, 0),
    ]
    for manager, rc, reconciled in cases:
        seen = {}

        def _build(*, settings, creds, session, now, _manager=manager, _seen=seen):
            _seen.update(settings=settings, creds=creds)
            return _manager

        monkeypatch.setattr(order_cli, "build_order_manager", _build)

        # When
        result = order_cli.run(limit_args)

        # Then
        assert result == rc
        assert manager.reconciled == reconciled
        assert manager.intents[0].side is Side.BUY
        assert manager.intents[0].order_type is OrderType.LIMIT
        assert manager.intents[0].limit_price == 10_000
        assert seen["settings"].mode is ExecutionMode.PAPER
        assert seen["creds"].kis_account_no == "12345678"

    # When / Then: 시장가 매도는 가격 없이 조립
    market = _Manager(OrderStatus.ACCEPTED)
    monkeypatch.setattr(order_cli, "build_order_manager", lambda **kwargs: market)
    assert order_cli.run(argparse.Namespace(symbol="005930", side="sell", qty=1, type="market", price=None)) == 0
    assert market.intents[0].side is Side.SELL
    assert market.intents[0].order_type is OrderType.MARKET
    assert market.intents[0].limit_price is None

def test_order_cli_returns_rc4_on_missing_config_unarmed_live_or_account_failure(tmp_path, monkeypatch, caplog) -> None:
    # Given: KIS 계좌번호 env 누락
    import argparse
    import logging

    from src.cli import order as order_cli
    from src.execution.contracts import AccountCheckError

    env = {
        "KIS_APP_KEY": "k", "KIS_APP_SECRET": "s", "KIS_ACCOUNT_PRODUCT_CODE": "01",
        "KRX_ALPHA_EXEC_DATA_ROOT": str(tmp_path),
    }
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    for name in ("KIS_ACCOUNT_NO", "KRX_ALPHA_EXEC_MODE", "KRX_ALPHA_EXEC_LIVE_ARMED"):
        monkeypatch.delenv(name, raising=False)

    def _never(**kwargs):
        raise AssertionError("manager must not be built")

    monkeypatch.setattr(order_cli, "build_order_manager", _never)
    args = argparse.Namespace(symbol="005930", side="buy", qty=1, type="limit", price=10_000)

    # When / Then: 설정 누락
    with caplog.at_level(logging.ERROR):
        assert order_cli.run(args) == 4
    assert any("missing_config" in record.message for record in caplog.records)

    # When / Then: live 미무장
    monkeypatch.setenv("KIS_ACCOUNT_NO", "12345678")
    monkeypatch.setenv("KRX_ALPHA_EXEC_MODE", "live")
    caplog.clear()
    with caplog.at_level(logging.ERROR):
        assert order_cli.run(args) == 4
    assert any("live_not_armed" in record.message for record in caplog.records)

    # When / Then: 계좌 자기검증 실패
    monkeypatch.delenv("KRX_ALPHA_EXEC_MODE")

    def _account_fail(**kwargs):
        raise AccountCheckError("account self-check failed: EGW02007")

    monkeypatch.setattr(order_cli, "build_order_manager", _account_fail)
    caplog.clear()
    with caplog.at_level(logging.ERROR):
        assert order_cli.run(args) == 4
    assert any("account_check" in record.message for record in caplog.records)
