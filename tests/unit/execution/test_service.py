
def test_build_order_manager_paper_self_checks_account_and_uses_virtual_cash(tmp_path, monkeypatch) -> None:
    # Given: 기본(paper) 설정 + 토큰 캐시 선배치
    import json

    from src.core.config import ExecutionMode, ExecutionSettings
    from src.execution.contracts import OrderIntent, OrderStatus, OrderType, Side
    from src.execution.service import build_order_manager
    from tests.unit.execution.fakes import (
        T0, FakeSession, FixedClock, asking_body, balance_body, make_creds, price_body, write_token_cache,
    )

    monkeypatch.delenv("KRX_ALPHA_EXEC_MODE", raising=False)
    monkeypatch.delenv("KRX_ALPHA_EXEC_LIVE_ARMED", raising=False)
    settings = ExecutionSettings(commission_bps=1.5, data_root=tmp_path)
    write_token_cache(settings.paths.kis_token_cache)
    session = FakeSession([balance_body([]), price_body(), asking_body(asks=[(10_010, 5)], bids=[(10_000, 5)])])

    # When
    manager = build_order_manager(
        settings=settings, creds=make_creds(), session=session, now=FixedClock(T0),
        clock=lambda: 0.0, sleep=lambda seconds: None,
    )

    # Then: 실계좌 자기검증 + 가상현금
    assert manager.gateway.mode is ExecutionMode.PAPER
    assert manager.ledger.cash_krw == 10_000_000
    assert manager.ledger.positions == {}
    assert session.calls[0]["headers"]["tr_id"] == "TTTC8434R"

    # When: kill-switch 파일 생성 후 주문
    settings.paths.kill_switch_file.parent.mkdir(parents=True, exist_ok=True)
    settings.paths.kill_switch_file.touch()
    order = manager.submit(OrderIntent("005930", Side.BUY, 1, OrderType.LIMIT, 10_000))

    # Then
    assert order.status is OrderStatus.REJECTED
    assert order.reject_code == "kill_switch"
    lines = (settings.paths.order_journal_dir / "2026-09-11.jsonl").read_text(encoding="utf-8").splitlines()
    events = [json.loads(line)["event"] for line in lines]
    assert events[0] == "session_start"
    assert events[-1] == "risk_reject"
    assert all(call["method"] == "GET" for call in session.calls)

def test_build_order_manager_live_seeds_positions_and_fails_closed_on_account_error(tmp_path, monkeypatch) -> None:
    # Given: 무장된 live 설정
    import pytest

    from src.core.config import ExecutionMode, ExecutionSettings
    from src.execution.contracts import AccountCheckError
    from src.execution.service import build_order_manager
    from tests.unit.execution.fakes import T0, FakeResponse, FakeSession, FixedClock, balance_body, make_creds, write_token_cache

    monkeypatch.delenv("KRX_ALPHA_EXEC_MODE", raising=False)
    settings = ExecutionSettings(commission_bps=1.5, data_root=tmp_path, mode=ExecutionMode.LIVE, live_armed=True)
    write_token_cache(settings.paths.kis_token_cache)
    session = FakeSession([balance_body([{"pdno": "005930", "hldg_qty": "3", "pchs_amt": "750000"}])])

    # When
    manager = build_order_manager(
        settings=settings, creds=make_creds(), session=session, now=FixedClock(T0),
        clock=lambda: 0.0, sleep=lambda seconds: None,
    )

    # Then
    assert manager.gateway.mode is ExecutionMode.LIVE
    assert manager.ledger.qty("005930") == 3
    assert manager.ledger.positions["005930"].cost_krw == 750_000
    assert manager.ledger.cash_krw == 0

    # When / Then: 계좌/키 불일치는 기동 중단
    bad = FakeSession([
        FakeResponse({"rt_cd": "1", "msg_cd": "EGW02007", "msg1": "해당 앱키는 모의투자용 앱키가 아닙니다."}, status=500)
    ])
    with pytest.raises(AccountCheckError, match="EGW02007"):
        build_order_manager(
            settings=settings, creds=make_creds(), session=bad, now=FixedClock(T0),
            clock=lambda: 0.0, sleep=lambda seconds: None,
        )
