
def test_build_order_manager_paper_self_checks_account_and_uses_virtual_cash(tmp_path, monkeypatch) -> None:
    # Given: 기본(paper) 설정 + 토큰 캐시 선배치
    import json

    from src.core.config import ExecutionMode, ExecutionSettings
    from src.execution.contracts import OrderIntent, OrderStatus, OrderType, Side
    from src.execution.kis_client import kis_token_cache_path
    from src.execution.service import build_order_manager
    from tests.unit.execution.fakes import (
        T0, FakeSession, FixedClock, asking_body, balance_body, make_creds, price_body, write_token_cache,
    )

    monkeypatch.delenv("KRX_ALPHA_EXEC_MODE", raising=False)
    monkeypatch.delenv("KRX_ALPHA_EXEC_LIVE_ARMED", raising=False)
    monkeypatch.setenv("KRX_ALPHA_KIS_TOKEN_CACHE_DIR", str(tmp_path / "kis-tokens"))
    (tmp_path / "kis-tokens").mkdir(parents=True, exist_ok=True)
    (tmp_path / "kis-tokens" / ".host-admission").touch()
    settings = ExecutionSettings(commission_bps=1.5, data_root=tmp_path)
    write_token_cache(kis_token_cache_path(tmp_path / "kis-tokens", "app-key"))
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
    manager.close()

def test_build_order_manager_live_seeds_positions_and_fails_closed_on_account_error(tmp_path, monkeypatch) -> None:
    # Given: 무장된 live 설정
    import pytest

    from src.core.config import ExecutionMode, ExecutionSettings
    from src.execution.contracts import AccountCheckError
    from src.execution.kis_client import kis_token_cache_path
    from src.execution.service import build_order_manager
    from tests.unit.execution.fakes import T0, FakeResponse, FakeSession, FixedClock, balance_body, make_creds, write_token_cache

    monkeypatch.delenv("KRX_ALPHA_EXEC_MODE", raising=False)
    monkeypatch.setenv("KRX_ALPHA_KIS_TOKEN_CACHE_DIR", str(tmp_path / "kis-tokens"))
    (tmp_path / "kis-tokens").mkdir(parents=True, exist_ok=True)
    (tmp_path / "kis-tokens" / ".host-admission").touch()
    settings = ExecutionSettings(commission_bps=1.5, data_root=tmp_path, mode=ExecutionMode.LIVE, live_armed=True)
    write_token_cache(kis_token_cache_path(tmp_path / "kis-tokens", "app-key"))
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
    manager.close()

    # When / Then: 계좌/키 불일치는 기동 중단
    bad = FakeSession([
        FakeResponse({"rt_cd": "1", "msg_cd": "EGW02007", "msg1": "해당 앱키는 모의투자용 앱키가 아닙니다."}, status=500)
    ])
    with pytest.raises(AccountCheckError, match="EGW02007"):
        build_order_manager(
            settings=settings, creds=make_creds(), session=bad, now=FixedClock(T0),
            clock=lambda: 0.0, sleep=lambda seconds: None,
        )


def _stack_env(tmp_path, monkeypatch):
    from src.execution.kis_client import kis_token_cache_path
    from tests.unit.execution.fakes import write_token_cache

    monkeypatch.delenv("KRX_ALPHA_EXEC_MODE", raising=False)
    monkeypatch.delenv("KRX_ALPHA_EXEC_LIVE_ARMED", raising=False)
    monkeypatch.setenv("KRX_ALPHA_KIS_TOKEN_CACHE_DIR", str(tmp_path / "kis-tokens"))
    (tmp_path / "kis-tokens").mkdir(parents=True, exist_ok=True)
    (tmp_path / "kis-tokens" / ".host-admission").touch()
    write_token_cache(kis_token_cache_path(tmp_path / "kis-tokens", "app-key"))


def _orderable_body(amount: int = 10_000_000):
    from tests.unit.execution.fakes import FakeResponse

    return FakeResponse(
        {"rt_cd": "0", "msg_cd": "MCA00000", "msg1": "ok", "output": {"nrcvb_buy_amt": str(amount)}}
    )


def _daily_body(rows: list) -> object:
    from tests.unit.execution.fakes import FakeResponse

    return FakeResponse(
        {"rt_cd": "0", "msg_cd": "MCA00000", "msg1": "ok", "output1": rows,
         "ctx_area_fk100": "", "ctx_area_nk100": ""},
        headers={"tr_cont": "D"},
    )


def _daily_row(odno: str, *, qty: int, filled: int, amount: int, side: str = "02", price: int = 10_000, time: str = "090001") -> dict:
    return {
        "odno": odno, "orgn_odno": "", "ord_gno_brno": "91252", "pdno": "005930",
        "sll_buy_dvsn_cd": side, "ord_qty": str(qty), "ord_unpr": str(price),
        "tot_ccld_qty": str(filled), "tot_ccld_amt": str(amount), "rmn_qty": str(qty - filled),
        "rjct_qty": "0", "cncl_yn": "N", "ord_tmd": time,
    }


def _holding_row(symbol: str, qty: int, cost: int) -> dict:
    return {"pdno": symbol, "hldg_qty": str(qty), "pchs_amt": str(cost)}


def _journal_events(settings, day: str = "2026-09-11") -> list:
    import json

    path = settings.paths.order_journal_dir / f"{day}.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_mock_mode_assembles_entirely_on_mock_venue(tmp_path, monkeypatch) -> None:
    # Given
    import src.execution.service as service_module
    from src.brokers.kis.trading import KIS_MOCK_BASE_URL
    from src.core.config import ExecutionMode, ExecutionSettings, KisVenue
    from src.execution.contracts import OrderIntent, OrderStatus, OrderType, Side
    from src.execution.service import build_order_manager
    from tests.unit.execution.fakes import (
        T0, FakeSession, FixedClock, accepted_body, asking_body, balance_body, make_creds, price_body,
    )

    _stack_env(tmp_path, monkeypatch)
    settings = ExecutionSettings(mode=ExecutionMode.MOCK, data_root=tmp_path, commission_bps=1.5)
    captured: dict = {}
    real_build = service_module.build_kis_rest_stack

    def _spy(**kwargs):
        captured.update(kwargs)
        return real_build(**kwargs)

    monkeypatch.setattr(service_module, "build_kis_rest_stack", _spy)
    session = FakeSession([
        balance_body([]), price_body(), asking_body(asks=[(10_010, 5)], bids=[(10_000, 5)]),
        _orderable_body(), accepted_body(odno="0000117057"),
    ])

    # When
    manager = build_order_manager(
        settings=settings, creds=make_creds(), session=session, now=FixedClock(T0),
        clock=lambda: 0.0, sleep=lambda seconds: None,
    )
    order = manager.submit(OrderIntent("005930", Side.BUY, 1, OrderType.LIMIT, 10_000))
    manager.close()

    # Then
    assert manager.gateway.mode is ExecutionMode.MOCK
    assert captured["venue"] is KisVenue.MOCK
    assert captured["rate_per_s"] == settings.mock_rest_rate_per_s
    assert order.status is OrderStatus.ACCEPTED
    assert session.calls
    assert all(call["url"].startswith(KIS_MOCK_BASE_URL) for call in session.calls)
    assert session.calls[0]["headers"]["tr_id"] == "VTTC8434R"
    assert session.calls[-1]["headers"]["tr_id"] == "VTTC0012U"

def test_live_and_paper_never_touch_mock_url(tmp_path, monkeypatch) -> None:
    from src.brokers.kis.trading import KIS_LIVE_BASE_URL, KIS_MOCK_BASE_URL
    from src.core.config import ExecutionMode, ExecutionSettings
    from src.execution.service import build_order_manager
    from tests.unit.execution.fakes import T0, FakeSession, FixedClock, balance_body, make_creds

    _stack_env(tmp_path, monkeypatch)
    live = ExecutionSettings(mode=ExecutionMode.LIVE, live_armed=True, data_root=tmp_path / "live")
    paper = ExecutionSettings(data_root=tmp_path / "paper")
    live_session = FakeSession([balance_body([])])
    paper_session = FakeSession([balance_body([])])

    live_manager = build_order_manager(
        settings=live, creds=make_creds(), session=live_session, now=FixedClock(T0),
        clock=lambda: 0.0, sleep=lambda seconds: None,
    )
    paper_manager = build_order_manager(
        settings=paper, creds=make_creds(), session=paper_session, now=FixedClock(T0),
        clock=lambda: 0.0, sleep=lambda seconds: None,
    )
    live_manager.close()
    paper_manager.close()

    for call in live_session.calls + paper_session.calls:
        assert call["url"].startswith(KIS_LIVE_BASE_URL)
        assert not call["url"].startswith(KIS_MOCK_BASE_URL)

def test_second_session_refused_while_first_open(tmp_path, monkeypatch) -> None:
    import pytest

    from src.core.config import ExecutionSettings
    from src.execution.contracts import SessionLockError
    from src.execution.service import build_order_manager
    from tests.unit.execution.fakes import T0, FakeSession, FixedClock, balance_body, make_creds

    _stack_env(tmp_path, monkeypatch)
    settings = ExecutionSettings(data_root=tmp_path)
    first = build_order_manager(
        settings=settings, creds=make_creds(), session=FakeSession([balance_body([])]), now=FixedClock(T0),
        clock=lambda: 0.0, sleep=lambda seconds: None,
    )
    idle = FakeSession([])

    with pytest.raises(SessionLockError):
        build_order_manager(
            settings=settings, creds=make_creds(), session=idle, now=FixedClock(T0),
            clock=lambda: 0.0, sleep=lambda seconds: None,
        )
    assert idle.calls == []

    first.close()
    second = build_order_manager(
        settings=settings, creds=make_creds(), session=FakeSession([balance_body([])]), now=FixedClock(T0),
        clock=lambda: 0.0, sleep=lambda seconds: None,
    )
    second.close()

def test_lock_released_when_assembly_fails(tmp_path, monkeypatch) -> None:
    import pytest

    from src.core.config import ExecutionSettings
    from src.execution.contracts import AccountCheckError
    from src.execution.service import build_order_manager
    from tests.unit.execution.fakes import T0, FakeResponse, FakeSession, FixedClock, balance_body, make_creds

    _stack_env(tmp_path, monkeypatch)
    settings = ExecutionSettings(data_root=tmp_path)
    failing = FakeSession([
        FakeResponse({"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "bad"}, status=500)
    ])
    with pytest.raises(AccountCheckError):
        build_order_manager(
            settings=settings, creds=make_creds(), session=failing, now=FixedClock(T0),
            clock=lambda: 0.0, sleep=lambda seconds: None,
        )
    manager = build_order_manager(
        settings=settings, creds=make_creds(), session=FakeSession([balance_body([])]), now=FixedClock(T0),
        clock=lambda: 0.0, sleep=lambda seconds: None,
    )
    manager.close()

def test_restart_restores_day_and_reconciles_downtime_fills(tmp_path, monkeypatch) -> None:
    from src.core.config import ExecutionMode, ExecutionSettings
    from src.execution.contracts import OrderIntent, OrderStatus, OrderType, Side
    from src.execution.service import build_order_manager
    from tests.unit.execution.fakes import (
        T0, FakeSession, FixedClock, accepted_body, asking_body, balance_body, make_creds, price_body,
    )

    _stack_env(tmp_path, monkeypatch)
    settings = ExecutionSettings(mode=ExecutionMode.MOCK, data_root=tmp_path)
    first = build_order_manager(
        settings=settings, creds=make_creds(),
        session=FakeSession([
            balance_body([]), price_body(), asking_body(asks=[(10_010, 10)], bids=[(10_000, 10)]),
            _orderable_body(), accepted_body(odno="0000117057"),
        ]),
        now=FixedClock(T0), clock=lambda: 0.0, sleep=lambda seconds: None,
    )
    order = first.submit(OrderIntent("005930", Side.BUY, 10, OrderType.LIMIT, 10_000))
    assert order.status is OrderStatus.ACCEPTED
    first.close()

    second = build_order_manager(
        settings=settings, creds=make_creds(),
        session=FakeSession([
            _daily_body([_daily_row("0000117057", qty=10, filled=10, amount=100_000)]),
            balance_body([_holding_row("005930", 10, 100_000)]),
        ]),
        now=FixedClock(T0), clock=lambda: 0.0, sleep=lambda seconds: None,
    )

    restored = second.orders[order.client_id]
    assert restored.status is OrderStatus.FILLED
    assert second.ledger.qty("005930") == 10
    assert second.submit_block_reason is None
    fills = [e for e in _journal_events(settings) if e["event"] == "fill" and e["client_id"] == order.client_id]
    assert len(fills) == 1
    second.close()

def _write_mock_day(settings, records: list) -> None:
    import json

    path = settings.paths.order_journal_dir / "2026-09-11.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")

def _mock_open_order_records() -> list:
    return [
        {"ts": "2026-09-11T09:00:00+09:00", "mode": "mock", "event": "session_start",
         "holdings": [{"symbol": "005930", "qty": 10, "cost_krw": 100_000}]},
        {"ts": "2026-09-11T09:00:01+09:00", "mode": "mock", "event": "intent",
         "client_id": "mock-20260911-000001",
         "intent": {"symbol": "005930", "side": "buy", "qty": 10, "order_type": "limit", "limit_price": 10_000, "tag": ""}},
        {"ts": "2026-09-11T09:00:02+09:00", "mode": "mock", "event": "submit_result",
         "client_id": "mock-20260911-000001", "status": "accepted",
         "outcome": {"kind": "accepted", "broker_order_no": "0000000001", "broker_org_no": "91252",
                     "order_time": "090001", "code": "", "message": ""}},
    ]

def test_position_mismatch_blocks_submits_without_raising(tmp_path, monkeypatch) -> None:
    from src.core.config import ExecutionMode, ExecutionSettings
    from src.execution.contracts import OrderIntent, OrderStatus, OrderType, Side
    from src.execution.service import build_order_manager
    from tests.unit.execution.fakes import (
        T0, FakeSession, FixedClock, accepted_body, asking_body, balance_body, make_creds, price_body,
    )

    _stack_env(tmp_path, monkeypatch)
    settings = ExecutionSettings(mode=ExecutionMode.MOCK, data_root=tmp_path)
    _write_mock_day(settings, _mock_open_order_records())
    session = FakeSession([
        _daily_body([]), balance_body([_holding_row("005930", 7, 70_000)]),
        _daily_body([]), balance_body([_holding_row("005930", 7, 70_000)]),
        price_body(), asking_body(asks=[(10_010, 5)], bids=[(10_000, 5)]),
        accepted_body(odno="0000117058"),
    ])
    manager = build_order_manager(
        settings=settings, creds=make_creds(), session=session, now=FixedClock(T0),
        clock=lambda: 0.0, sleep=lambda seconds: None,
    )

    mismatches = [e for e in _journal_events(settings) if e["event"] == "position_mismatch"]
    assert len(mismatches) == 1
    assert mismatches[0]["diffs"] == [{"symbol": "005930", "ledger_qty": 10, "broker_qty": 7}]

    rejected = manager.submit(OrderIntent("005930", Side.SELL, 1, OrderType.LIMIT, 10_000))
    assert rejected.status is OrderStatus.REJECTED
    assert rejected.reject_code == "account_mismatch"
    assert [c for c in session.calls if c["method"] == "POST"] == []

    open_order = manager.orders["mock-20260911-000001"]
    manager.cancel(open_order.client_id)
    posts = [c for c in session.calls if c["method"] == "POST"]
    assert len(posts) == 1
    manager.close()

def test_transient_mismatch_clears_on_second_attempt(tmp_path, monkeypatch) -> None:
    from src.core.config import ExecutionMode, ExecutionSettings
    from src.execution.service import build_order_manager
    from tests.unit.execution.fakes import T0, FakeSession, FixedClock, balance_body, make_creds

    _stack_env(tmp_path, monkeypatch)
    settings = ExecutionSettings(mode=ExecutionMode.MOCK, data_root=tmp_path, position_parity_attempts=2)
    _write_mock_day(settings, [
        {"ts": "2026-09-11T09:00:00+09:00", "mode": "mock", "event": "session_start",
         "holdings": [{"symbol": "005930", "qty": 10, "cost_krw": 100_000}]},
    ])
    manager = build_order_manager(
        settings=settings, creds=make_creds(),
        session=FakeSession([
            balance_body([_holding_row("005930", 7, 70_000)]),
            balance_body([_holding_row("005930", 10, 100_000)]),
        ]),
        now=FixedClock(T0), clock=lambda: 0.0, sleep=lambda seconds: None,
    )

    assert manager.submit_block_reason is None
    assert [e for e in _journal_events(settings) if e["event"] == "position_mismatch"] == []
    manager.close()

def test_first_session_seeds_from_broker_and_journals_snapshot(tmp_path, monkeypatch) -> None:
    from src.brokers.kis.trading import KIS_LIVE_BASE_URL
    from src.core.config import ExecutionMode, ExecutionSettings
    from src.execution.service import build_order_manager
    from tests.unit.execution.fakes import T0, FakeSession, FixedClock, balance_body, make_creds

    _stack_env(tmp_path, monkeypatch)
    settings = ExecutionSettings(mode=ExecutionMode.LIVE, live_armed=True, data_root=tmp_path)
    session = FakeSession([balance_body([_holding_row("005930", 5, 50_000)])])
    manager = build_order_manager(
        settings=settings, creds=make_creds(), session=session, now=FixedClock(T0),
        clock=lambda: 0.0, sleep=lambda seconds: None,
    )

    assert manager.ledger.qty("005930") == 5
    starts = [e for e in _journal_events(settings) if e["event"] == "session_start"]
    assert len(starts) == 1
    assert starts[0]["holdings"] == [{"symbol": "005930", "qty": 5, "cost_krw": 50_000}]
    assert len(session.calls) == 1
    assert session.calls[0]["url"].startswith(KIS_LIVE_BASE_URL)
    manager.close()

def test_unexpected_assembly_error_releases_lock(tmp_path, monkeypatch) -> None:
    import fcntl

    import src.execution.service as service_module
    from src.core.config import ExecutionSettings
    from src.execution.service import build_order_manager
    from tests.unit.execution.fakes import T0, FakeSession, FixedClock, make_creds

    _stack_env(tmp_path, monkeypatch)
    settings = ExecutionSettings(data_root=tmp_path)

    def _boom(**kwargs):
        raise ValueError("stack exploded")

    monkeypatch.setattr(service_module, "build_kis_rest_stack", _boom)
    try:
        build_order_manager(
            settings=settings, creds=make_creds(), session=FakeSession([]), now=FixedClock(T0),
            clock=lambda: 0.0, sleep=lambda seconds: None,
        )
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError")

    with open(settings.paths.order_session_lock_file, "w", encoding="utf-8") as probe:
        fcntl.flock(probe.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(probe.fileno(), fcntl.LOCK_UN)

def test_corrupt_journal_fails_closed_and_releases_lock(tmp_path, monkeypatch) -> None:
    import fcntl

    import pytest

    from src.core.config import ExecutionSettings
    from src.execution.contracts import ExecutionError
    from src.execution.service import build_order_manager
    from tests.unit.execution.fakes import T0, FakeSession, FixedClock, balance_body, make_creds

    _stack_env(tmp_path, monkeypatch)
    settings = ExecutionSettings(data_root=tmp_path)
    path = settings.paths.order_journal_dir / "2026-09-11.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        '{"ts": "2026-09-11T09:00:00+09:00", "mode": "paper", "event": "session_start", "holdings": []}\n'
        "{broken\n"
        '{"ts": "2026-09-11T09:00:02+09:00", "mode": "paper", "event": "intent"}\n',
        encoding="utf-8",
    )
    with pytest.raises(ExecutionError):
        build_order_manager(
            settings=settings, creds=make_creds(), session=FakeSession([balance_body([])]), now=FixedClock(T0),
            clock=lambda: 0.0, sleep=lambda seconds: None,
        )

    with open(settings.paths.order_session_lock_file, "w", encoding="utf-8") as probe:
        fcntl.flock(probe.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(probe.fileno(), fcntl.LOCK_UN)


def test_interrupted_assembly_releases_session_lock(tmp_path, monkeypatch) -> None:
    import fcntl

    import pytest

    import src.execution.service as service_module
    from src.core.config import ExecutionSettings
    from tests.unit.execution.fakes import T0, FakeSession, make_creds

    def interrupt(**kwargs):
        raise KeyboardInterrupt

    settings = ExecutionSettings(data_root=tmp_path)
    monkeypatch.setattr(service_module, "build_kis_rest_stack", interrupt)
    with pytest.raises(KeyboardInterrupt):
        service_module.build_order_manager(
            settings=settings, creds=make_creds(), session=FakeSession([]), now=lambda: T0,
        )
    with settings.paths.order_session_lock_file.open("a") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
