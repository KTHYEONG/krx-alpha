def test_run_session_orchestration_invokes_services_directly(tmp_path, monkeypatch) -> None:
    # Given: service 계층을 대체한 데몬 오케스트레이션
    import datetime as dt
    import pathlib

    import polars as pl

    from src.core.config import CollectorSettings
    from src.orchestration import daemon
    from src.universe.ipc import write_candidates

    monkeypatch.setenv("KRX_OPENAPI_KEY", "k")
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    settings.paths.bars_store.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "date": [dt.date(2026, 9, 9)],
        "symbol": ["000001"],
        "close": [1000.0],
        "volume": [1000],
        "trade_value_100m": [100.0],
        "daily_change_pct": [1.0],
    }).write_parquet(settings.paths.bars_store)

    calls: dict[str, object] = {}

    def _fake_refresh(**kwargs):
        calls["refresh"] = kwargs
        return daemon.BarsRefreshResult(trading_day=dt.date(2026, 9, 9), appended_rows=1, backfilled_days=0)

    def _fake_plan(**kwargs):
        calls["plan"] = kwargs
        settings.paths.candidates.parent.mkdir(parents=True, exist_ok=True)
        write_candidates(settings.paths.candidates, [{"symbol": "000001", "selection_reasons": ["limit_up"]}], rev=1)
        return daemon.UniversePlanResult(
            decision_date=dt.date(2026, 9, 9), selected=1, out_path=kwargs["out_path"], candidates_emitted=1
        )

    monkeypatch.setattr(daemon, "refresh_bars", _fake_refresh)
    monkeypatch.setattr(daemon, "plan_universe", _fake_plan)

    # When
    ready = daemon.run_session_orchestration(today=dt.date(2026, 9, 10), settings=settings)

    # Then: argparse.Namespace 위조 없이 타입드 인자로 service 를 호출하고 후보 준비를 반환한다
    assert ready is True
    assert calls["refresh"]["store_path"] == settings.paths.bars_store
    assert calls["plan"]["decision_date"] == dt.date(2026, 9, 9)
    assert calls["plan"]["slot_budget"] == settings.universe_slot_budget


def test_run_collector_daemon_single_cycle() -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    from src.orchestration.daemon import run_collector_daemon

    mock_sleep = MagicMock()
    night = dt.datetime(2026, 9, 8, 20, 0, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    run_collector_daemon(sleep_fn=mock_sleep, max_cycles=1, now_fn=lambda: night)

    mock_sleep.assert_called_once()


def test_run_collector_daemon_streamer_active_spawns_supervised_process(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    import src.orchestration.daemon as daemon_mod
    from src.orchestration.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, 'run_session_orchestration', lambda **kw: True)

    calls: list[str] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            calls.append('constructed')
        def ensure_running(self):
            calls.append('ensure_running')
            return 'started'

    monkeypatch.setattr(daemon_mod, 'ProcessSupervisor', _FakeSupervisor)

    active = dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    run_collector_daemon(sleep_fn=MagicMock(), max_cycles=1, now_fn=lambda: active)

    assert calls == ['constructed', 'ensure_running']


def test_run_collector_daemon_eod_stops_supervised_process(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    import src.orchestration.daemon as daemon_mod
    from src.orchestration.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, 'run_session_orchestration', lambda **kw: True)

    stop_calls: list[float] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
        def ensure_running(self):
            self.checked = True
            return 'started'
        def stop(self, *, timeout_s=15.0):
            stop_calls.append(timeout_s)
            return 'graceful'

    monkeypatch.setattr(daemon_mod, 'ProcessSupervisor', _FakeSupervisor)

    kst = ZoneInfo('Asia/Seoul')
    times = iter([
        dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=kst),
        dt.datetime(2026, 9, 8, 15, 45, 0, tzinfo=kst),
    ])

    run_collector_daemon(sleep_fn=MagicMock(), max_cycles=2, now_fn=lambda: next(times))

    assert stop_calls == [15.0]


def test_run_collector_daemon_streamer_active_skips_spawn_when_not_ready(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    import src.orchestration.daemon as daemon_mod
    from src.orchestration.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, 'run_session_orchestration', lambda **kw: False)

    constructed: list[str] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            constructed.append('constructed')
        def ensure_running(self):
            constructed.append('ensure_running')
            return 'started'

    monkeypatch.setattr(daemon_mod, 'ProcessSupervisor', _FakeSupervisor)

    active = dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    run_collector_daemon(sleep_fn=MagicMock(), max_cycles=1, now_fn=lambda: active)

    assert constructed == []


def test_run_collector_daemon_streamer_active_logs_circuit_open(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    import src.orchestration.daemon as daemon_mod
    from src.orchestration.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, 'run_session_orchestration', lambda **kw: True)

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
        def ensure_running(self):
            return 'circuit_open'

    monkeypatch.setattr(daemon_mod, 'ProcessSupervisor', _FakeSupervisor)

    active = dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    with caplog.at_level(logging.CRITICAL):
        run_collector_daemon(sleep_fn=MagicMock(), max_cycles=1, now_fn=lambda: active)

    assert any('circuit_open' in r.message for r in caplog.records)


def test_run_collector_daemon_streamer_active_survives_orchestration_exception(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    import src.orchestration.daemon as daemon_mod
    from src.orchestration.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)

    def _raise(**kw):
        raise FileNotFoundError("data/universe/2026-09-08.parquet")

    monkeypatch.setattr(daemon_mod, 'run_session_orchestration', _raise)

    constructed: list[str] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            constructed.append('constructed')
        def ensure_running(self):
            return 'started'

    monkeypatch.setattr(daemon_mod, 'ProcessSupervisor', _FakeSupervisor)

    active = dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    run_collector_daemon(sleep_fn=MagicMock(), max_cycles=1, now_fn=lambda: active)

    assert constructed == []


def test_run_session_orchestration_passes_credentials_when_available(tmp_path, monkeypatch) -> None:
    # Given: KRX 자격증명이 있고 service 계층을 대체한 오케스트레이션
    import datetime as dt
    import pathlib

    import polars as pl

    from src.core.config import CollectorSettings
    from src.orchestration import daemon

    monkeypatch.setenv("KRX_OPENAPI_KEY", "k")
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    settings.paths.bars_store.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "date": [dt.date(2026, 9, 9)],
        "symbol": ["000001"],
        "close": [1000.0],
        "volume": [1000],
        "trade_value_100m": [100.0],
        "daily_change_pct": [1.0],
    }).write_parquet(settings.paths.bars_store)

    calls: dict[str, object] = {}

    def _fake_refresh(**kwargs):
        calls["refresh"] = kwargs
        return daemon.BarsRefreshResult(trading_day=dt.date(2026, 9, 9), appended_rows=0, backfilled_days=0)

    def _fake_plan(**kwargs):
        calls["plan"] = kwargs
        return daemon.UniversePlanResult(
            decision_date=dt.date(2026, 9, 9), selected=0, out_path=kwargs["out_path"], candidates_emitted=0
        )

    monkeypatch.setattr(daemon, "refresh_bars", _fake_refresh)
    monkeypatch.setattr(daemon, "plan_universe", _fake_plan)

    # When
    ready = daemon.run_session_orchestration(today=dt.date(2026, 9, 10), settings=settings)

    # Then: 자격증명이 service 호출에 전달되고 후보가 없어 False 를 반환한다
    assert ready is False
    assert calls["refresh"]["auth_key"] == "k"
    assert calls["refresh"]["window_days"] == settings.bars_window_days


def test_run_session_orchestration_returns_false_when_bars_store_missing(tmp_path, monkeypatch) -> None:
    # Given: 자격증명 없이 refresh 는 성공하지만 store 파일이 없는 상태
    import datetime as dt
    import pathlib

    from src.core.config import CollectorSettings
    from src.orchestration import daemon

    monkeypatch.delenv("KRX_OPENAPI_KEY", raising=False)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")

    def _fake_refresh(**kwargs):
        return daemon.BarsRefreshResult(trading_day=dt.date(2026, 9, 9), appended_rows=0, backfilled_days=0)

    monkeypatch.setattr(daemon, "refresh_bars", _fake_refresh)

    # When / Then: bars store 부재로 fail-closed
    ready = daemon.run_session_orchestration(today=dt.date(2026, 9, 10), settings=settings)

    assert ready is False


def test_run_session_orchestration_continues_when_refresh_raises_but_store_ready(tmp_path, monkeypatch) -> None:
    # Given: refresh 실패 + 기존 store + 기존 후보가 있는 상태
    import datetime as dt
    import pathlib

    import polars as pl

    from src.core.config import CollectorSettings
    from src.orchestration import daemon
    from src.universe.ipc import write_candidates

    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    settings.paths.bars_store.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "date": [dt.date(2026, 9, 9)],
        "symbol": ["000001"],
        "close": [1000.0],
        "volume": [1000],
        "trade_value_100m": [100.0],
        "daily_change_pct": [1.0],
    }).write_parquet(settings.paths.bars_store)
    write_candidates(settings.paths.candidates, [{"symbol": "000001", "selection_reasons": ["limit_up"]}], rev=1)

    def _fail_refresh(**kwargs):
        from src.marketdata.krx_bars import KrxBarsError

        raise KrxBarsError("krx down")

    def _fake_plan(**kwargs):
        return daemon.UniversePlanResult(
            decision_date=dt.date(2026, 9, 9), selected=1, out_path=kwargs["out_path"], candidates_emitted=1
        )

    monkeypatch.setattr(daemon, "refresh_bars", _fail_refresh)
    monkeypatch.setattr(daemon, "plan_universe", _fake_plan)

    # When / Then: refresh 실패를 기록하고 기존 산출물로 계속 진행한다
    assert daemon.run_session_orchestration(today=dt.date(2026, 9, 10), settings=settings) is True


def test_run_session_orchestration_propagates_universe_plan_failure(tmp_path, monkeypatch) -> None:
    # Given: 선정 실패 + 직전 세션의 stale 후보가 남아있는 상태
    import datetime as dt
    import pathlib

    import polars as pl
    import pytest

    from src.core.config import CollectorSettings
    from src.orchestration import daemon
    from src.universe.ipc import write_candidates

    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    settings.paths.bars_store.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "date": [dt.date(2026, 9, 9)],
        "symbol": ["000001"],
        "close": [1000.0],
        "volume": [1000],
        "trade_value_100m": [100.0],
        "daily_change_pct": [1.0],
    }).write_parquet(settings.paths.bars_store)
    write_candidates(settings.paths.candidates, [{"symbol": "000001", "selection_reasons": ["limit_up"]}], rev=1)

    def _fake_refresh(**kwargs):
        return daemon.BarsRefreshResult(trading_day=dt.date(2026, 9, 9), appended_rows=0, backfilled_days=0)

    def _fail_plan(**kwargs):
        raise RuntimeError("plan down")

    monkeypatch.setattr(daemon, "refresh_bars", _fake_refresh)
    monkeypatch.setattr(daemon, "plan_universe", _fail_plan)

    # When / Then: 선정 실패를 흡수하지 않고 표면화한다 (stale 후보로 스트리밍하는 fail-open 차단)
    with pytest.raises(RuntimeError, match="plan down"):
        daemon.run_session_orchestration(today=dt.date(2026, 9, 10), settings=settings)


def test_run_collector_daemon_weekend_sleeps_hourly() -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    from src.orchestration.daemon import run_collector_daemon

    mock_sleep = MagicMock()
    saturday = dt.datetime(2026, 9, 12, 12, 0, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    run_collector_daemon(sleep_fn=mock_sleep, max_cycles=1, now_fn=lambda: saturday)

    mock_sleep.assert_called_once_with(3600.0)


def test_run_collector_daemon_pre_market_sleeps_until_streamer_start() -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    from src.orchestration.daemon import run_collector_daemon

    mock_sleep = MagicMock()
    pre_market = dt.datetime(2026, 9, 10, 8, 0, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    run_collector_daemon(sleep_fn=mock_sleep, max_cycles=1, now_fn=lambda: pre_market)

    mock_sleep.assert_called_once_with(300.0)


def test_run_collector_daemon_main_invokes_runner(monkeypatch) -> None:
    import src.orchestration.daemon as daemon_mod

    calls: list[str] = []
    monkeypatch.setattr(daemon_mod, "run_collector_daemon", lambda **kw: calls.append("ran"))

    daemon_mod.main()

    assert calls == ["ran"]
