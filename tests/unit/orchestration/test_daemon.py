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
    monkeypatch.setattr(daemon_mod, 'resolve_trading_day', lambda ref_date: None)
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
    monkeypatch.setattr(daemon_mod, 'resolve_trading_day', lambda ref_date: None)
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
    monkeypatch.setattr(daemon_mod, 'resolve_trading_day', lambda ref_date: None)
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
    monkeypatch.setattr(daemon_mod, 'resolve_trading_day', lambda ref_date: None)
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
    monkeypatch.setattr(daemon_mod, 'resolve_trading_day', lambda ref_date: None)

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

def test_run_session_orchestration_blocks_stale_bars_against_calendar(tmp_path, monkeypatch, caplog) -> None:
    # Given: store 최신일이 2026-09-09 인데 직전 영업일은 2026-09-11
    import datetime as dt
    import logging
    import pathlib

    import polars as pl

    from src.core.config import CollectorSettings
    from src.marketdata.toss_calendar import TradingDay
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

    def _fake_refresh(**kwargs):
        return daemon.BarsRefreshResult(trading_day=dt.date(2026, 9, 9), appended_rows=0, backfilled_days=0)

    planned: list[object] = []

    def _fake_plan(**kwargs):
        planned.append(kwargs)
        raise AssertionError("stale bars 상태에서 유니버스를 계획하면 안 된다")

    monkeypatch.setattr(daemon, "refresh_bars", _fake_refresh)
    monkeypatch.setattr(daemon, "plan_universe", _fake_plan)
    monkeypatch.setattr(daemon, "_build_kis_client", lambda paths: object())
    monkeypatch.setattr(daemon, "refresh_bars_via_kis_fallback", lambda **kw: (_ for _ in ()).throw(daemon.KisFallbackError("boom")))

    trading_day = TradingDay(
        date=dt.date(2026, 9, 14),
        is_business_day=True,
        previous_business_day=dt.date(2026, 9, 11),
        next_business_day=dt.date(2026, 9, 15),
    )

    # When
    with caplog.at_level(logging.CRITICAL):
        ready = daemon.run_session_orchestration(
            today=dt.date(2026, 9, 14), settings=settings, trading_day=trading_day
        )

    # Then: 후보 발행 없이 fail-closed 종료한다
    assert ready is False
    assert planned == []
    assert settings.paths.candidates.exists() is False
    assert "stale_bars" in caplog.text


def test_run_session_orchestration_proceeds_when_decision_date_matches_previous_business_day(tmp_path, monkeypatch) -> None:
    # Given: store 최신일 == 직전 영업일 (2026-09-11)
    import datetime as dt
    import pathlib

    import polars as pl

    from src.core.config import CollectorSettings
    from src.marketdata.toss_calendar import TradingDay
    from src.orchestration import daemon
    from src.universe.ipc import write_candidates

    monkeypatch.setenv("KRX_OPENAPI_KEY", "k")
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    settings.paths.bars_store.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "date": [dt.date(2026, 9, 11)],
        "symbol": ["000001"],
        "close": [1000.0],
        "volume": [1000],
        "trade_value_100m": [100.0],
        "daily_change_pct": [1.0],
    }).write_parquet(settings.paths.bars_store)

    calls: dict[str, object] = {}

    def _fake_refresh(**kwargs):
        return daemon.BarsRefreshResult(trading_day=dt.date(2026, 9, 11), appended_rows=1, backfilled_days=0)

    def _fake_plan(**kwargs):
        calls["plan"] = kwargs
        settings.paths.candidates.parent.mkdir(parents=True, exist_ok=True)
        write_candidates(settings.paths.candidates, [{"symbol": "000001", "selection_reasons": ["limit_up"]}], rev=1)
        return daemon.UniversePlanResult(
            decision_date=dt.date(2026, 9, 11), selected=1, out_path=kwargs["out_path"], candidates_emitted=1
        )

    monkeypatch.setattr(daemon, "refresh_bars", _fake_refresh)
    monkeypatch.setattr(daemon, "plan_universe", _fake_plan)

    trading_day = TradingDay(
        date=dt.date(2026, 9, 14),
        is_business_day=True,
        previous_business_day=dt.date(2026, 9, 11),
        next_business_day=dt.date(2026, 9, 15),
    )

    # When
    ready = daemon.run_session_orchestration(
        today=dt.date(2026, 9, 14), settings=settings, trading_day=trading_day
    )

    # Then
    assert ready is True
    assert calls["plan"]["decision_date"] == dt.date(2026, 9, 11)


def test_run_collector_daemon_skips_orchestration_on_market_holiday(tmp_path, monkeypatch) -> None:
    # Given: 평일 08:30 이지만 캘린더상 휴장일
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.marketdata.toss_calendar import TradingDay
    from src.orchestration.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)

    holiday = TradingDay(
        date=dt.date(2026, 9, 14),
        is_business_day=False,
        previous_business_day=dt.date(2026, 9, 11),
        next_business_day=dt.date(2026, 9, 15),
    )
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: holiday)

    orchestrated: list[object] = []

    def _fail_orchestration(**kwargs):
        orchestrated.append(kwargs)
        raise AssertionError("휴장일에 오케스트레이션을 호출하면 안 된다")

    monkeypatch.setattr(daemon_mod, "run_session_orchestration", _fail_orchestration)

    mock_sleep = MagicMock()
    weekday_morning = dt.datetime(2026, 9, 14, 8, 30, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    # When
    run_collector_daemon(sleep_fn=mock_sleep, max_cycles=1, now_fn=lambda: weekday_morning)

    # Then: 수집을 건너뛰고 1시간 대기한다
    assert orchestrated == []
    mock_sleep.assert_called_once_with(3600.0)


def test_run_collector_daemon_runs_orchestration_on_business_day(tmp_path, monkeypatch) -> None:
    # Given: 평일 08:30, 캘린더상 영업일
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.marketdata.toss_calendar import TradingDay
    from src.orchestration.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)

    business = TradingDay(
        date=dt.date(2026, 9, 14),
        is_business_day=True,
        previous_business_day=dt.date(2026, 9, 11),
        next_business_day=dt.date(2026, 9, 15),
    )
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: business)

    seen: list[dict] = []

    def _fake_orchestration(**kwargs):
        seen.append(kwargs)
        return False

    monkeypatch.setattr(daemon_mod, "run_session_orchestration", _fake_orchestration)

    mock_sleep = MagicMock()
    weekday_morning = dt.datetime(2026, 9, 14, 8, 30, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    # When
    run_collector_daemon(sleep_fn=mock_sleep, max_cycles=1, now_fn=lambda: weekday_morning)

    # Then: 조회된 영업일 컨텍스트가 오케스트레이션으로 전달된다
    assert len(seen) == 1
    assert seen[0]["today"] == dt.date(2026, 9, 14)
    assert seen[0]["trading_day"] is business
    mock_sleep.assert_called_once_with(10.0)


def test_resolve_trading_day_degrades_to_none_when_credentials_missing(monkeypatch, caplog) -> None:
    # Given: 토스 자격증명 env 부재
    import datetime as dt
    import logging

    from src.orchestration import daemon

    monkeypatch.delenv("TOSS_APP_KEY", raising=False)
    monkeypatch.delenv("TOSS_APP_SECRET", raising=False)

    def _must_not_call(*args, **kwargs):
        raise AssertionError("자격증명 없이 벤더를 호출하면 안 된다")

    monkeypatch.setattr(daemon, "fetch_trading_day", _must_not_call)

    # When
    with caplog.at_level(logging.WARNING):
        out = daemon.resolve_trading_day(dt.date(2026, 9, 14))

    # Then: 게이트 비활성 신호(None) + DEGRADED 로깅
    assert out is None
    assert "DEGRADED" in caplog.text


def test_resolve_trading_day_degrades_to_none_when_vendor_fails(monkeypatch, caplog) -> None:
    # Given: 자격증명은 있으나 벤더가 실패
    import datetime as dt
    import logging

    import pytest

    from src.marketdata.toss_calendar import TossCalendarError
    from src.orchestration import daemon

    monkeypatch.setenv("TOSS_APP_KEY", "tsck_test")
    monkeypatch.setenv("TOSS_APP_SECRET", "tssk_test")

    def _raise_vendor(*args, **kwargs):
        raise TossCalendarError("403 forbidden")

    monkeypatch.setattr(daemon, "fetch_trading_day", _raise_vendor)

    # When
    with caplog.at_level(logging.WARNING):
        out = daemon.resolve_trading_day(dt.date(2026, 9, 14))

    # Then: 도메인 예외만 흡수한다
    assert out is None
    assert "DEGRADED" in caplog.text

    def _raise_unexpected(*args, **kwargs):
        raise ValueError("unexpected")

    monkeypatch.setattr(daemon, "fetch_trading_day", _raise_unexpected)
    with pytest.raises(ValueError, match="unexpected"):
        daemon.resolve_trading_day(dt.date(2026, 9, 14))




def test_run_collector_daemon_eod_passes_quarantine_root(tmp_path, monkeypatch) -> None:
    # Given: EOD 시각에 진입한 데몬
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings
    from src.orchestration.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, 'resolve_trading_day', lambda ref_date: None)

    seen: dict[str, object] = {}

    def _fake_maintenance(journal_root, *, retain_days=3, today=None, archive_root=None, quarantine_root=None):
        seen.update({'quarantine_root': quarantine_root, 'archive_root': archive_root})
        return 0

    monkeypatch.setattr(daemon_mod, 'run_eod_maintenance', _fake_maintenance)
    monkeypatch.setattr(daemon_mod, 'run_eod_offload', lambda *a, **kw: {'uploaded': 0, 'skipped': 0, 'failed': 0, 'purged': 0})

    settings = CollectorSettings()
    mock_sleep = MagicMock()
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    # When
    run_collector_daemon(settings=settings, sleep_fn=mock_sleep, max_cycles=1, now_fn=lambda: eod_time)

    # Then: 설정에서 파생된 격리 경로가 EOD 유지보수로 전달된다
    assert seen['quarantine_root'] == settings.paths.quarantine_root
    assert seen['archive_root'] == settings.paths.archive_root


def test_build_kis_client_wires_shared_token_cache_path(tmp_path, monkeypatch) -> None:
    import pathlib

    from src.core.config import CollectorSettings, ExecutionSettings
    from src.execution.kis_client import KisRestClient
    from src.orchestration import daemon

    monkeypatch.setenv("KIS_APP_KEY", "k")
    monkeypatch.setenv("KIS_APP_SECRET", "s")
    monkeypatch.setenv("KIS_ACCOUNT_NO", "12345678")
    monkeypatch.setenv("KIS_ACCOUNT_PRODUCT_CODE", "01")
    data_root = pathlib.Path(tmp_path) / "data"
    collector = CollectorSettings(data_root=data_root)
    execution = ExecutionSettings(data_root=data_root)

    client = daemon._build_kis_client(collector.paths)

    assert isinstance(client, KisRestClient)
    assert client._token_cache_path == collector.paths.kis_token_cache
    assert client._token_cache_path == execution.paths.kis_token_cache

def test_run_session_orchestration_falls_back_to_kis_when_bars_stale(tmp_path, monkeypatch) -> None:
    # Given: store 최신일이 2026-09-09 인데 직전 영업일은 2026-09-11 (KRX 장애 상황)
    import datetime as dt
    import pathlib

    import polars as pl

    from src.core.config import CollectorSettings
    from src.marketdata.toss_calendar import TradingDay
    from src.orchestration import daemon
    from src.universe.ipc import write_candidates

    monkeypatch.setenv("KRX_OPENAPI_KEY", "k")
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    settings.paths.bars_store.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "date": [dt.date(2026, 9, 9)], "symbol": ["000001"], "close": [1000.0],
        "volume": [1000], "trade_value_100m": [100.0], "daily_change_pct": [1.0],
    }).write_parquet(settings.paths.bars_store)

    def _fake_refresh(**kwargs):
        return daemon.BarsRefreshResult(trading_day=dt.date(2026, 9, 9), appended_rows=0, backfilled_days=0)

    fallback_calls: dict[str, object] = {}

    def _fake_fallback(**kwargs):
        fallback_calls.update(kwargs)
        return daemon.BarsRefreshResult(trading_day=kwargs["target_date"], appended_rows=1, backfilled_days=0)

    def _fake_plan(**kwargs):
        settings.paths.candidates.parent.mkdir(parents=True, exist_ok=True)
        write_candidates(settings.paths.candidates, [{"symbol": "000001", "selection_reasons": ["limit_up"]}], rev=1)
        return daemon.UniversePlanResult(
            decision_date=kwargs["decision_date"], selected=1, out_path=kwargs["out_path"], candidates_emitted=1
        )

    monkeypatch.setattr(daemon, "refresh_bars", _fake_refresh)
    monkeypatch.setattr(daemon, "refresh_bars_via_kis_fallback", _fake_fallback)
    monkeypatch.setattr(daemon, "_build_kis_client", lambda paths: object())
    monkeypatch.setattr(daemon, "plan_universe", _fake_plan)

    trading_day = TradingDay(
        date=dt.date(2026, 9, 14), is_business_day=True,
        previous_business_day=dt.date(2026, 9, 11), next_business_day=dt.date(2026, 9, 15),
    )

    # When
    ready = daemon.run_session_orchestration(today=dt.date(2026, 9, 14), settings=settings, trading_day=trading_day)

    # Then: 폴백 성공 -> 직전영업일로 갱신되어 정상 진행
    assert ready is True
    assert fallback_calls["target_date"] == dt.date(2026, 9, 11)

def test_run_session_orchestration_blocks_stale_bars_when_kis_fallback_also_fails(tmp_path, monkeypatch, caplog) -> None:
    # Given: store 최신일이 2026-09-09 인데 직전 영업일은 2026-09-11, KIS 폴백도 실패
    import datetime as dt
    import logging
    import pathlib

    import polars as pl

    from src.core.config import CollectorSettings
    from src.marketdata.service import KisFallbackError
    from src.marketdata.toss_calendar import TradingDay
    from src.orchestration import daemon

    monkeypatch.setenv("KRX_OPENAPI_KEY", "k")
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    settings.paths.bars_store.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "date": [dt.date(2026, 9, 9)], "symbol": ["000001"], "close": [1000.0],
        "volume": [1000], "trade_value_100m": [100.0], "daily_change_pct": [1.0],
    }).write_parquet(settings.paths.bars_store)

    def _fake_refresh(**kwargs):
        return daemon.BarsRefreshResult(trading_day=dt.date(2026, 9, 9), appended_rows=0, backfilled_days=0)

    def _fail_fallback(**kwargs):
        raise KisFallbackError("no prior market_map for kis fallback")

    planned: list[object] = []

    def _fail_plan(**kwargs):
        planned.append(kwargs)
        raise AssertionError("fallback도 실패한 상태에서 유니버스를 계획하면 안 된다")

    monkeypatch.setattr(daemon, "refresh_bars", _fake_refresh)
    monkeypatch.setattr(daemon, "refresh_bars_via_kis_fallback", _fail_fallback)
    monkeypatch.setattr(daemon, "_build_kis_client", lambda paths: object())
    monkeypatch.setattr(daemon, "plan_universe", _fail_plan)

    trading_day = TradingDay(
        date=dt.date(2026, 9, 14), is_business_day=True,
        previous_business_day=dt.date(2026, 9, 11), next_business_day=dt.date(2026, 9, 15),
    )

    # When
    with caplog.at_level(logging.CRITICAL):
        ready = daemon.run_session_orchestration(today=dt.date(2026, 9, 14), settings=settings, trading_day=trading_day)

    # Then: 여전히 fail-closed
    assert ready is False
    assert planned == []
    assert settings.paths.candidates.exists() is False
    assert "stale_bars_fallback_failed" in caplog.text


def test_run_collector_daemon_eod_logs_session_data_gap(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")

    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0})
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: False)

    eod_time = dt.datetime(2026, 9, 10, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.CRITICAL):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    assert "session_data_gap" in caplog.text
