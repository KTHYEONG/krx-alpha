import logging


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

    def _fake_maintenance(journal_root, *, retain_days=3, today=None, archive_root=None, quarantine_root=None, work_root=None):
        seen.update({'quarantine_root': quarantine_root, 'archive_root': archive_root, 'work_root': work_root})
        return 0

    monkeypatch.setattr(daemon_mod, 'run_eod_maintenance', _fake_maintenance)
    monkeypatch.setattr(daemon_mod, 'run_eod_offload', lambda *a, **kw: {'uploaded': 0, 'skipped': 0, 'failed': 0, 'purged': 0})

    settings = CollectorSettings()
    mock_sleep = MagicMock()
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    # When
    run_collector_daemon(settings=settings, sleep_fn=mock_sleep, max_cycles=1, now_fn=lambda: eod_time)

    # Then: 설정에서 파생된 격리/작업 경로가 EOD 유지보수로 전달된다
    assert seen['quarantine_root'] == settings.paths.quarantine_root
    assert seen['archive_root'] == settings.paths.archive_root
    assert seen['work_root'] == settings.paths.work_root


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

def test_run_collector_daemon_eod_attempts_maintenance_once_per_date(tmp_path, monkeypatch) -> None:
    # Given: 같은 날 EOD 윈도우에서 3사이클 반복
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / 'data')
    counts = {'maintenance': 0, 'offload': 0, 'reconcile': 0}

    def _maintenance(*a, **kw):
        counts['maintenance'] += 1
        return 0

    def _offload(*a, **kw):
        counts['offload'] += 1
        return {'uploaded': 0, 'skipped': 0, 'failed': 0, 'purged': 0}

    def _reconcile(**kw):
        counts['reconcile'] += 1
        return True

    monkeypatch.setattr(daemon_mod, 'run_eod_maintenance', _maintenance)
    monkeypatch.setattr(daemon_mod, 'run_eod_offload', _offload)
    monkeypatch.setattr(daemon_mod, 'check_session_reconciliation', _reconcile)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    # When
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=3, now_fn=lambda: eod_time)

    # Then: 거래일당 1회만 시도
    assert counts == {'maintenance': 1, 'offload': 1, 'reconcile': 1}

def test_run_collector_daemon_eod_attempts_again_on_next_date(tmp_path, monkeypatch) -> None:
    # Given: 이틀 연속 EOD 사이클
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / 'data')
    days = []

    def _maintenance(*a, **kw):
        days.append(kw['today'])
        return 0

    monkeypatch.setattr(daemon_mod, 'run_eod_maintenance', _maintenance)
    monkeypatch.setattr(daemon_mod, 'run_eod_offload', lambda *a, **kw: {'uploaded': 0, 'skipped': 0, 'failed': 0, 'purged': 0})
    monkeypatch.setattr(daemon_mod, 'check_session_reconciliation', lambda **kw: True)
    kst = ZoneInfo('Asia/Seoul')
    times = iter([
        dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=kst),
        dt.datetime(2026, 9, 14, 15, 46, 0, tzinfo=kst),
        dt.datetime(2026, 9, 15, 15, 45, 0, tzinfo=kst),
    ])

    # When
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=3, now_fn=lambda: next(times))

    # Then
    assert days == [dt.date(2026, 9, 14), dt.date(2026, 9, 15)]

def test_run_collector_daemon_eod_runs_offload_and_reconciliation_when_maintenance_fails(tmp_path, monkeypatch, caplog) -> None:
    # Given: 정규화 유지보수가 워커 크래시로 실패
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod
    from src.storage.retention import L1WorkerCrashError

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / 'data')
    counts = {'offload': 0, 'reconcile': 0}

    def _maintenance(*a, **kw):
        raise L1WorkerCrashError('normalize worker crashed: returncode=-9')

    def _offload(*a, **kw):
        counts['offload'] += 1
        return {'uploaded': 2, 'skipped': 0, 'failed': 0, 'purged': 1}

    def _reconcile(**kw):
        counts['reconcile'] += 1
        return True

    monkeypatch.setattr(daemon_mod, 'run_eod_maintenance', _maintenance)
    monkeypatch.setattr(daemon_mod, 'run_eod_offload', _offload)
    monkeypatch.setattr(daemon_mod, 'check_session_reconciliation', _reconcile)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    # When
    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    # Then: 오프로드/정합성 검사는 계속되고 요약은 DEGRADED
    assert counts == {'offload': 1, 'reconcile': 1}
    assert 'stage=eod_maintenance status=FAIL reason=maintenance_error' in caplog.text
    assert 'deleted_partitions=0 uploaded=2 purged=1 status=DEGRADED' in caplog.text

def test_run_collector_daemon_eod_logs_error_when_offload_raises_and_does_not_retry_same_date(tmp_path, monkeypatch, caplog) -> None:
    # Given: 오프로드 단계에서 예기치 못한 예외
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / 'data')
    counts = {'maintenance': 0, 'offload': 0, 'reconcile': 0}

    def _maintenance(*a, **kw):
        counts['maintenance'] += 1
        return 0

    def _offload(*a, **kw):
        counts['offload'] += 1
        raise RuntimeError('rclone lsjson failed')

    def _reconcile(**kw):
        counts['reconcile'] += 1
        return True

    monkeypatch.setattr(daemon_mod, 'run_eod_maintenance', _maintenance)
    monkeypatch.setattr(daemon_mod, 'run_eod_offload', _offload)
    monkeypatch.setattr(daemon_mod, 'check_session_reconciliation', _reconcile)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    # When
    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=2, now_fn=lambda: eod_time)

    # Then: 오프로드 실패와 무관하게 정합성 검사는 수행되고 같은 날 재시도하지 않는다
    assert counts == {'maintenance': 1, 'offload': 1, 'reconcile': 1}
    assert 'stage=eod_maintenance error=rclone lsjson failed' in caplog.text
    assert 'deleted_partitions=0 uploaded=0 purged=0 status=DEGRADED' in caplog.text

def test_run_collector_daemon_configures_logging_with_persistent_dir_flag(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    calls: list[tuple[str, object]] = []
    monkeypatch.setattr(daemon_mod, "configure_logging", lambda component, *, log_dir=None: calls.append((component, log_dir)) or "daemon-77")
    night = dt.datetime(2026, 9, 8, 20, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    monkeypatch.delenv("KRX_ALPHA_PERSISTENT_LOGS", raising=False)
    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: night)
    monkeypatch.setenv("KRX_ALPHA_PERSISTENT_LOGS", "true")
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: night)

    assert calls == [("daemon", None), ("daemon", settings.paths.logs_dir)]
    assert "stage=start status=ONLINE timezone=Asia/Seoul run_id=daemon-77" in caplog.text


def test_run_collector_daemon_logs_state_changes_and_ten_minute_heartbeat_only(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.setattr(daemon_mod, "configure_logging", lambda component, *, log_dir=None: "r")
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    kst = ZoneInfo("Asia/Seoul")
    times = iter([
        dt.datetime(2026, 9, 8, 20, 0, 0, tzinfo=kst),
        dt.datetime(2026, 9, 8, 20, 0, 10, tzinfo=kst),
        dt.datetime(2026, 9, 8, 20, 10, 30, tzinfo=kst),
    ])

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=3, now_fn=lambda: next(times))

    info = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO and r.name == daemon_mod.logger.name]
    assert sum("stage=state_change" in m for m in info) == 1
    assert sum("stage=heartbeat" in m for m in info) == 2
    assert not any("sleeping" in m for m in info)
    assert not any(" cycle=" in m and "stage=" not in m for m in info)


def test_run_collector_daemon_logs_streamer_restart_and_circuit_transition_once(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, "configure_logging", lambda component, *, log_dir=None: "r")
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: None)
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: True)
    results = iter(["started", "restarted", "circuit_open", "circuit_open"])

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
            self.last_exit_code = -9

        def ensure_running(self):
            return next(results)

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    active = dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(sleep_fn=lambda s: None, max_cycles=4, now_fn=lambda: active)

    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    criticals = [r.getMessage() for r in caplog.records if r.levelno == logging.CRITICAL]
    assert warnings.count("[DAEMON] stage=streamer status=RESTARTED exit_code=-9 restarts=1") == 1
    assert sum("reason=circuit_open" in m for m in criticals) == 1
    assert any("stage=streamer status=STARTED" in r.getMessage() for r in caplog.records if r.levelno == logging.INFO)


def test_run_collector_daemon_orchestration_exception_logs_traceback(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, "configure_logging", lambda component, *, log_dir=None: "r")
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: None)

    def _raise(**kw):
        raise FileNotFoundError("data/universe/2026-09-08.parquet")

    monkeypatch.setattr(daemon_mod, "run_session_orchestration", _raise)
    active = dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.CRITICAL):
        daemon_mod.run_collector_daemon(sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: active)

    records = [r for r in caplog.records if "reason=orchestration_error" in r.getMessage()]
    assert len(records) == 1
    assert records[0].exc_info is not None
    assert records[0].exc_info[0] is FileNotFoundError


def test_run_collector_daemon_eod_unexpected_error_logs_traceback(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, "configure_logging", lambda component, *, log_dir=None: "r")
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)

    def _offload(*a, **kw):
        raise RuntimeError("rclone lsjson failed")

    monkeypatch.setattr(daemon_mod, "run_eod_offload", _offload)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.ERROR):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    records = [r for r in caplog.records if "stage=eod_maintenance error=" in r.getMessage()]
    assert len(records) == 1
    assert records[0].exc_info is not None
    assert records[0].exc_info[0] is RuntimeError


def test_run_session_orchestration_blocks_stale_bars_when_calendar_unknown(tmp_path, monkeypatch, caplog) -> None:

    import datetime as dt
    import pathlib

    import polars as pl

    from src.core.config import CollectorSettings
    from src.orchestration import daemon
    from src.universe.ipc import write_candidates

    monkeypatch.setenv("KRX_OPENAPI_KEY", "k")
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    settings.paths.bars_store.parent.mkdir(parents=True, exist_ok=True)

    def _store(last: dt.date) -> None:
        pl.DataFrame({"date": [last], "symbol": ["000001"], "close": [1000.0], "volume": [1000], "trade_value_100m": [100.0], "daily_change_pct": [1.0]}).write_parquet(settings.paths.bars_store)

    planned: list[dt.date] = []

    def _plan(**kwargs):
        planned.append(kwargs["decision_date"])
        write_candidates(settings.paths.candidates, [{"symbol": "000001", "selection_reasons": ["limit_up"]}], rev=1)
        return daemon.UniversePlanResult(decision_date=kwargs["decision_date"], selected=1, out_path=kwargs["out_path"], candidates_emitted=1)

    monkeypatch.setattr(daemon, "plan_universe", _plan)

    _store(dt.date(2026, 9, 3))
    monkeypatch.setattr(daemon, "refresh_bars", lambda **kw: None)

    with caplog.at_level(logging.CRITICAL):
        ready = daemon.run_session_orchestration(today=dt.date(2026, 9, 14), settings=settings, trading_day=None)

    assert ready is False
    assert planned == []
    assert "reason=stale_bars_calendar_unknown decision_date=2026-09-03" in caplog.text


def test_run_session_orchestration_allows_recent_bars_when_calendar_unknown(tmp_path, monkeypatch) -> None:

    import datetime as dt
    import pathlib

    import polars as pl

    from src.core.config import CollectorSettings
    from src.orchestration import daemon
    from src.universe.ipc import write_candidates

    monkeypatch.setenv("KRX_OPENAPI_KEY", "k")
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    settings.paths.bars_store.parent.mkdir(parents=True, exist_ok=True)

    def _store(last: dt.date) -> None:
        pl.DataFrame({"date": [last], "symbol": ["000001"], "close": [1000.0], "volume": [1000], "trade_value_100m": [100.0], "daily_change_pct": [1.0]}).write_parquet(settings.paths.bars_store)

    planned: list[dt.date] = []

    def _plan(**kwargs):
        planned.append(kwargs["decision_date"])
        write_candidates(settings.paths.candidates, [{"symbol": "000001", "selection_reasons": ["limit_up"]}], rev=1)
        return daemon.UniversePlanResult(decision_date=kwargs["decision_date"], selected=1, out_path=kwargs["out_path"], candidates_emitted=1)

    monkeypatch.setattr(daemon, "plan_universe", _plan)

    _store(dt.date(2026, 9, 10))
    monkeypatch.setattr(daemon, "refresh_bars", lambda **kw: None)

    ready = daemon.run_session_orchestration(today=dt.date(2026, 9, 14), settings=settings, trading_day=None)

    assert ready is True
    assert planned == [dt.date(2026, 9, 10)]


def test_run_session_orchestration_uses_kis_fallback_on_incomplete_market(tmp_path, monkeypatch) -> None:

    import datetime as dt
    import pathlib

    import polars as pl

    from src.core.config import CollectorSettings
    from src.orchestration import daemon
    from src.universe.ipc import write_candidates

    monkeypatch.setenv("KRX_OPENAPI_KEY", "k")
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    settings.paths.bars_store.parent.mkdir(parents=True, exist_ok=True)

    def _store(last: dt.date) -> None:
        pl.DataFrame({"date": [last], "symbol": ["000001"], "close": [1000.0], "volume": [1000], "trade_value_100m": [100.0], "daily_change_pct": [1.0]}).write_parquet(settings.paths.bars_store)

    planned: list[dt.date] = []

    def _plan(**kwargs):
        planned.append(kwargs["decision_date"])
        write_candidates(settings.paths.candidates, [{"symbol": "000001", "selection_reasons": ["limit_up"]}], rev=1)
        return daemon.UniversePlanResult(decision_date=kwargs["decision_date"], selected=1, out_path=kwargs["out_path"], candidates_emitted=1)

    monkeypatch.setattr(daemon, "plan_universe", _plan)

    from src.marketdata.krx_bars import IncompleteMarketError
    from src.marketdata.toss_calendar import TradingDay

    _store(dt.date(2026, 9, 10))

    def _partial(**kw):
        raise IncompleteMarketError("partial market data for 2026-09-11: empty KOSDAQ")

    fallback_targets: list[dt.date] = []

    def _fallback(**kw):
        fallback_targets.append(kw["target_date"])
        return daemon.BarsRefreshResult(trading_day=kw["target_date"], appended_rows=1, backfilled_days=0)

    monkeypatch.setattr(daemon, "refresh_bars", _partial)
    monkeypatch.setattr(daemon, "refresh_bars_via_kis_fallback", _fallback)
    monkeypatch.setattr(daemon, "_build_kis_client", lambda paths: object())
    trading_day = TradingDay(date=dt.date(2026, 9, 14), is_business_day=True, previous_business_day=dt.date(2026, 9, 11), next_business_day=dt.date(2026, 9, 15))

    ready = daemon.run_session_orchestration(today=dt.date(2026, 9, 14), settings=settings, trading_day=trading_day)

    assert ready is True
    assert fallback_targets == [dt.date(2026, 9, 11)]
    assert planned == [dt.date(2026, 9, 11)]


def test_run_session_orchestration_fails_closed_when_kis_fallback_rowcount_implausible(tmp_path, monkeypatch, caplog) -> None:

    import datetime as dt
    import pathlib

    import polars as pl

    from src.core.config import CollectorSettings
    from src.orchestration import daemon
    from src.universe.ipc import write_candidates

    monkeypatch.setenv("KRX_OPENAPI_KEY", "k")
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    settings.paths.bars_store.parent.mkdir(parents=True, exist_ok=True)

    def _store(last: dt.date) -> None:
        pl.DataFrame({"date": [last], "symbol": ["000001"], "close": [1000.0], "volume": [1000], "trade_value_100m": [100.0], "daily_change_pct": [1.0]}).write_parquet(settings.paths.bars_store)

    planned: list[dt.date] = []

    def _plan(**kwargs):
        planned.append(kwargs["decision_date"])
        write_candidates(settings.paths.candidates, [{"symbol": "000001", "selection_reasons": ["limit_up"]}], rev=1)
        return daemon.UniversePlanResult(decision_date=kwargs["decision_date"], selected=1, out_path=kwargs["out_path"], candidates_emitted=1)

    monkeypatch.setattr(daemon, "plan_universe", _plan)

    from src.marketdata.krx_bars import ImplausibleRowCountError
    from src.marketdata.toss_calendar import TradingDay

    _store(dt.date(2026, 9, 10))

    def _truncated(**kw):
        raise ImplausibleRowCountError("implausible row count for [2026-09-11]: 10 < 2765 * 0.9")

    monkeypatch.setattr(daemon, "refresh_bars", lambda **kw: None)
    monkeypatch.setattr(daemon, "refresh_bars_via_kis_fallback", _truncated)
    monkeypatch.setattr(daemon, "_build_kis_client", lambda paths: object())
    trading_day = TradingDay(date=dt.date(2026, 9, 14), is_business_day=True, previous_business_day=dt.date(2026, 9, 11), next_business_day=dt.date(2026, 9, 15))

    with caplog.at_level(logging.CRITICAL):
        ready = daemon.run_session_orchestration(today=dt.date(2026, 9, 14), settings=settings, trading_day=trading_day)

    assert ready is False
    assert planned == []
    assert "stale_bars_fallback_failed" in caplog.text


def test_degraded_candidates_rev_accepts_only_recent_readable_candidates(tmp_path) -> None:
    import datetime as dt

    from src.orchestration.daemon import _degraded_candidates_rev
    from src.universe.ipc import write_candidates

    today = dt.date(2026, 9, 14)
    path = tmp_path / "candidates.json"
    row = [{"symbol": "005930", "selection_reasons": ["limit_up"]}]

    assert _degraded_candidates_rev(path, today, max_age_days=7) is None
    path.write_text("{broken", encoding="utf-8")
    assert _degraded_candidates_rev(path, today, max_age_days=7) is None
    write_candidates(path, [], rev=20260911)
    assert _degraded_candidates_rev(path, today, max_age_days=7) is None
    write_candidates(path, row, rev=20260901)
    assert _degraded_candidates_rev(path, today, max_age_days=7) is None
    write_candidates(path, row, rev=20260915)
    assert _degraded_candidates_rev(path, today, max_age_days=7) is None
    write_candidates(path, row, rev=1)
    assert _degraded_candidates_rev(path, today, max_age_days=7) is None
    write_candidates(path, row, rev=20260907)
    assert _degraded_candidates_rev(path, today, max_age_days=7) == 20260907


def test_run_collector_daemon_retries_orchestration_after_backoff_same_day(tmp_path, monkeypatch, caplog) -> None:

    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    settings.paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    kst = ZoneInfo("Asia/Seoul")
    constructed: list[list[str]] = []
    stops: list[float] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
            self.last_exit_code = None
            constructed.append(cmd)

        def ensure_running(self):
            return "started"

        def stop(self, *, timeout_s=15.0):
            stops.append(timeout_s)
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)

    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: None)
    outcomes = iter([False, True])
    attempts: list[dt.date] = []

    def _orch(**kw):
        attempts.append(kw["today"])
        return next(outcomes)

    monkeypatch.setattr(daemon_mod, "run_session_orchestration", _orch)
    times = iter([dt.datetime(2026, 9, 14, 8, 25, tzinfo=kst), dt.datetime(2026, 9, 14, 8, 27, tzinfo=kst),
                  dt.datetime(2026, 9, 14, 8, 31, tzinfo=kst), dt.datetime(2026, 9, 14, 8, 32, tzinfo=kst)])

    with caplog.at_level(logging.CRITICAL):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=4, now_fn=lambda: next(times))

    assert len(attempts) == 2
    assert len(constructed) == 1
    assert "--degraded-reason" not in constructed[0]
    failures = [r.getMessage() for r in caplog.records if "reason=candidates_not_ready" in r.getMessage()]
    assert len(failures) == 1
    assert "attempt=1" in failures[0]


def test_run_collector_daemon_starts_degraded_streamer_with_recent_candidates(tmp_path, monkeypatch, caplog) -> None:

    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod
    from src.universe.ipc import write_candidates

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    settings.paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    kst = ZoneInfo("Asia/Seoul")
    constructed: list[list[str]] = []
    stops: list[float] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
            self.last_exit_code = None
            constructed.append(cmd)

        def ensure_running(self):
            return "started"

        def stop(self, *, timeout_s=15.0):
            stops.append(timeout_s)
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)

    write_candidates(settings.paths.candidates, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=20260911)
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: None)
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: False)

    with caplog.at_level(logging.CRITICAL):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: dt.datetime(2026, 9, 14, 8, 25, tzinfo=kst))

    assert len(constructed) == 1
    cmd = constructed[0]
    assert cmd[cmd.index("--degraded-reason") + 1] == "orchestration_failed"
    assert "[DAEMON] stage=streamer status=DEGRADED reason=orchestration_failed candidates_rev=20260911" in caplog.text


def test_run_collector_daemon_skips_degraded_streamer_when_candidates_too_old(tmp_path, monkeypatch) -> None:

    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod
    from src.universe.ipc import write_candidates

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    settings.paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    kst = ZoneInfo("Asia/Seoul")
    constructed: list[list[str]] = []
    stops: list[float] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
            self.last_exit_code = None
            constructed.append(cmd)

        def ensure_running(self):
            return "started"

        def stop(self, *, timeout_s=15.0):
            stops.append(timeout_s)
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)

    write_candidates(settings.paths.candidates, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=20260901)
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: None)
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: False)

    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: dt.datetime(2026, 9, 14, 8, 25, tzinfo=kst))

    assert constructed == []


def test_run_collector_daemon_replaces_degraded_streamer_after_successful_retry(tmp_path, monkeypatch, caplog) -> None:

    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod
    from src.universe.ipc import write_candidates

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    settings.paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    kst = ZoneInfo("Asia/Seoul")
    constructed: list[list[str]] = []
    stops: list[float] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
            self.last_exit_code = None
            constructed.append(cmd)

        def ensure_running(self):
            return "started"

        def stop(self, *, timeout_s=15.0):
            stops.append(timeout_s)
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)

    write_candidates(settings.paths.candidates, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=20260911)
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: None)
    outcomes = iter([False, True])
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: next(outcomes))
    times = iter([dt.datetime(2026, 9, 14, 8, 25, tzinfo=kst), dt.datetime(2026, 9, 14, 8, 31, tzinfo=kst)])

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=2, now_fn=lambda: next(times))

    assert len(constructed) == 2
    assert "--degraded-reason" in constructed[0]
    assert "--degraded-reason" not in constructed[1]
    assert stops == [15.0]
    assert "stage=streamer status=REPLACE_DEGRADED stop_result=graceful" in caplog.text


def test_run_collector_daemon_uses_calendar_cache_when_toss_unavailable(tmp_path, monkeypatch) -> None:

    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    settings.paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    kst = ZoneInfo("Asia/Seoul")
    constructed: list[list[str]] = []
    stops: list[float] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
            self.last_exit_code = None
            constructed.append(cmd)

        def ensure_running(self):
            return "started"

        def stop(self, *, timeout_s=15.0):
            stops.append(timeout_s)
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)

    from src.marketdata.toss_calendar import TradingDay

    friday = TradingDay(date=dt.date(2026, 9, 11), is_business_day=True, previous_business_day=dt.date(2026, 9, 10), next_business_day=dt.date(2026, 9, 14))
    lookups = iter([friday, None])
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: next(lookups))
    seen: list[object] = []

    def _orch(**kw):
        seen.append(kw["trading_day"])
        return True

    monkeypatch.setattr(daemon_mod, "run_session_orchestration", _orch)
    times = iter([dt.datetime(2026, 9, 11, 8, 30, tzinfo=kst), dt.datetime(2026, 9, 14, 8, 30, tzinfo=kst)])

    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=2, now_fn=lambda: next(times))

    assert settings.paths.calendar_cache.exists()
    assert seen[0] is friday
    assert seen[1] == TradingDay(date=dt.date(2026, 9, 14), is_business_day=True, previous_business_day=dt.date(2026, 9, 11), next_business_day=dt.date(2026, 9, 14))


def test_run_collector_daemon_skips_day_when_calendar_cache_marks_holiday(tmp_path, monkeypatch) -> None:

    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    settings.paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    kst = ZoneInfo("Asia/Seoul")
    constructed: list[list[str]] = []
    stops: list[float] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
            self.last_exit_code = None
            constructed.append(cmd)

        def ensure_running(self):
            return "started"

        def stop(self, *, timeout_s=15.0):
            stops.append(timeout_s)
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)

    from src.marketdata.toss_calendar import TradingDay

    friday = TradingDay(date=dt.date(2026, 9, 11), is_business_day=True, previous_business_day=dt.date(2026, 9, 10), next_business_day=dt.date(2026, 9, 15))
    lookups = iter([friday, None])
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: next(lookups))
    orchestrated: list[dt.date] = []
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: orchestrated.append(kw["today"]) or True)
    sleeps: list[float] = []
    times = iter([dt.datetime(2026, 9, 11, 8, 30, tzinfo=kst), dt.datetime(2026, 9, 14, 8, 30, tzinfo=kst)])

    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=sleeps.append, max_cycles=2, now_fn=lambda: next(times))

    assert orchestrated == [dt.date(2026, 9, 11)]
    assert sleeps[-1] == 3600.0


def test_run_collector_daemon_eod_reconciliation_failure_is_critical_and_degraded(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 1, "skipped": 0, "failed": 0, "purged": 0})

    def _broken(**kw):
        raise OSError("bars parquet unreadable")

    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", _broken)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    records = [r for r in caplog.records if "stage=eod_reconciliation status=FAIL" in r.getMessage()]
    assert len(records) == 1
    assert records[0].levelno == logging.CRITICAL
    assert records[0].exc_info is not None
    assert "deleted_partitions=0 uploaded=1 purged=0 status=DEGRADED" in caplog.text


def test_run_collector_daemon_clears_supervisor_after_eod_so_next_day_never_restarts_stale_session(tmp_path, monkeypatch) -> None:
    # Given: 전일 스트리머가 EOD 에서 정지된 뒤 다음 날 오케스트레이션이 실패
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    kst = ZoneInfo("Asia/Seoul")
    ensured_dates: list[str] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
            self.last_exit_code = 0

        def ensure_running(self):
            ensured_dates.append(self.cmd[self.cmd.index("--session-date") + 1])
            return "restarted"

        def stop(self, *, timeout_s=15.0):
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: None)
    outcomes = iter([True, False])
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: next(outcomes))
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0})
    times = iter([
        dt.datetime(2026, 9, 14, 9, 0, tzinfo=kst),
        dt.datetime(2026, 9, 14, 15, 45, tzinfo=kst),
        dt.datetime(2026, 9, 15, 9, 0, tzinfo=kst),
    ])

    # When
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=3, now_fn=lambda: next(times))

    # Then: 다음 날에는 전일 session-date 스트리머를 재기동하지 않는다
    assert ensured_dates == ["2026-09-14"]


def test_run_collector_daemon_stops_stale_day_streamer_when_eod_was_missed(tmp_path, monkeypatch, caplog) -> None:
    # Given: EOD 윈도우를 거치지 못한 채 날짜가 바뀐 경우 (데몬이 15:40-16:00 사이 중단 등)
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    kst = ZoneInfo("Asia/Seoul")
    stops: list[str] = []
    ensured_dates: list[str] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
            self.last_exit_code = None

        def ensure_running(self):
            ensured_dates.append(self.cmd[self.cmd.index("--session-date") + 1])
            return "running"

        def stop(self, *, timeout_s=15.0):
            stops.append(self.cmd[self.cmd.index("--session-date") + 1])
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: None)
    outcomes = iter([True, False])
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: next(outcomes))
    times = iter([dt.datetime(2026, 9, 14, 9, 0, tzinfo=kst), dt.datetime(2026, 9, 15, 9, 0, tzinfo=kst)])

    # When
    with caplog.at_level(logging.WARNING):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=2, now_fn=lambda: next(times))

    # Then: 전일 스트리머를 정지하고 재기동하지 않는다
    assert stops == ["2026-09-14"]
    assert ensured_dates == ["2026-09-14"]
    assert "stage=streamer status=STOP_STALE_DAY stop_result=graceful" in caplog.text


def test_journal_age_s_returns_newest_mtime_age_or_none(tmp_path) -> None:
    import datetime as dt
    import os

    from src.orchestration.daemon import _journal_age_s

    now = dt.datetime.fromtimestamp(1_000_250, tz=dt.UTC)
    day = dt.date(2026, 9, 14)
    assert _journal_age_s(tmp_path, "ls", day, now) is None

    for stream, mtime in (("H0STCNT0", 1_000_100), ("H0STASP0", 1_000_200)):
        part = tmp_path / "ls" / stream / "dt=2026-09-14"
        part.mkdir(parents=True)
        f = part / "10.jsonl.zst"
        f.write_bytes(b"x")
        os.utime(f, (mtime, mtime))
    other_day = tmp_path / "ls" / "H0STCNT0" / "dt=2026-09-13"
    other_day.mkdir(parents=True)
    (other_day / "15.jsonl.zst").write_bytes(b"x")

    assert _journal_age_s(tmp_path, "ls", day, now) == 50.0

def test_run_collector_daemon_ingest_watchdog_alerts_stale_journal_once_and_recovers(tmp_path, monkeypatch, caplog) -> None:

    import datetime as dt
    import os
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    kst = ZoneInfo("Asia/Seoul")

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
            self.last_exit_code = None

        def ensure_running(self):
            return "started"

        def stop(self, *, timeout_s=15.0):
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: None)
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: True)

    times = [dt.datetime(2026, 9, 14, 10, m, tzinfo=kst) for m in (0, 1, 2, 3)]
    calls = {"n": 0}

    def _now():
        t = times[calls["n"]]
        calls["n"] += 1
        if calls["n"] == 3:
            part = settings.paths.journal_root / "ls" / "H0STCNT0" / "dt=2026-09-14"
            part.mkdir(parents=True, exist_ok=True)
            f = part / "10.jsonl.zst"
            f.write_bytes(b"x")
            os.utime(f, (t.timestamp() - 10, t.timestamp() - 10))
        return t

    with caplog.at_level(logging.WARNING):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=4, now_fn=_now)

    stale = [r.getMessage() for r in caplog.records if "stage=ingest_watchdog status=STALE" in r.getMessage()]
    recovered = [r for r in caplog.records if "stage=ingest_watchdog status=RECOVERED" in r.getMessage()]
    assert stale == ["[DAEMON] stage=ingest_watchdog status=STALE date=2026-09-14 age_s=none"]
    assert [r.levelno for r in caplog.records if "status=STALE" in r.getMessage()] == [logging.CRITICAL]
    assert len(recovered) == 1
    assert recovered[0].levelno == logging.WARNING

def test_run_collector_daemon_ingest_watchdog_skips_open_and_close_auction_windows(tmp_path, monkeypatch, caplog) -> None:

    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    kst = ZoneInfo("Asia/Seoul")

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
            self.last_exit_code = None

        def ensure_running(self):
            return "started"

        def stop(self, *, timeout_s=15.0):
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: None)
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: True)

    times = iter([dt.datetime(2026, 9, 14, 9, 0, tzinfo=kst), dt.datetime(2026, 9, 14, 9, 4, tzinfo=kst), dt.datetime(2026, 9, 14, 15, 26, tzinfo=kst)])

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=3, now_fn=lambda: next(times))

    assert "stage=ingest_watchdog" not in caplog.text

def test_run_collector_daemon_eod_offload_remote_auth_failure_is_critical(tmp_path, monkeypatch, caplog) -> None:

    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod
    from src.storage.remote import RemoteArchiveError

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    digests: list[tuple[str, str]] = []
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)) or True)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    def _offload(*a, **kw):
        raise RemoteArchiveError("lsjson failed: l1/ couldn't fetch token: invalid_grant: maybe token expired?")

    monkeypatch.setattr(daemon_mod, "run_eod_offload", _offload)
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    criticals = [r.getMessage() for r in caplog.records if r.levelno == logging.CRITICAL]
    assert any(m.startswith("[DAEMON] stage=eod_offload status=FAIL reason=auth_expired hint=rclone_config_reconnect_gdrive") for m in criticals)
    assert "deleted_partitions=0 uploaded=0 purged=0 status=DEGRADED" in caplog.text

def test_run_collector_daemon_eod_backup_freshness_stale_is_critical(tmp_path, monkeypatch, caplog) -> None:

    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    digests: list[tuple[str, str]] = []
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)) or True)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    seen: dict[str, object] = {}
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0})

    def _freshness(**kw):
        seen.update(kw)
        return ["2026-09-11.json", "2026-09-12.json"]

    monkeypatch.setattr(daemon_mod, "check_backup_freshness", _freshness)

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    assert seen == {"manifest_dir": settings.paths.manifest_dir, "today": dt.date(2026, 9, 14)}
    assert "[DAEMON] stage=backup_freshness status=STALE missing=2 oldest=2026-09-11.json" in caplog.text
    assert [r.levelno for r in caplog.records if "stage=backup_freshness" in r.getMessage()] == [logging.CRITICAL]
    assert "status=DEGRADED" in caplog.text

def test_run_collector_daemon_eod_backup_freshness_remote_failure_is_critical(tmp_path, monkeypatch, caplog) -> None:

    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod
    from src.storage.remote import RemoteArchiveError

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    digests: list[tuple[str, str]] = []
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)) or True)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0})

    def _freshness(**kw):
        raise RemoteArchiveError("lsjson failed: manifest/ dial tcp: i/o timeout")

    monkeypatch.setattr(daemon_mod, "check_backup_freshness", _freshness)

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    assert "[DAEMON] stage=backup_freshness status=FAIL reason=remote_error" in caplog.text
    assert "status=DEGRADED" in caplog.text

def test_run_collector_daemon_eod_passes_substantive_reconciliation_inputs(tmp_path, monkeypatch) -> None:

    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    digests: list[tuple[str, str]] = []
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)) or True)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    seen: dict[str, object] = {}
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0})
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])

    def _reconcile(**kw):
        seen.update(kw)
        return True

    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", _reconcile)

    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    assert seen["journal_root"] == settings.paths.journal_root
    assert seen["streams"] == settings.streams
    assert seen["vendor"] == settings.vendor
    assert seen["manifest_path"] == settings.paths.manifest_path(dt.date(2026, 9, 14))

def test_run_collector_daemon_eod_sends_daily_digest_once_per_date(tmp_path, monkeypatch) -> None:

    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    digests: list[tuple[str, str]] = []
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)) or True)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 2, "skipped": 0, "failed": 0, "purged": 1})
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])

    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=2, now_fn=lambda: eod_time)

    assert len(digests) == 1
    subject, body = digests[0]
    assert subject == "[krx-alpha] EOD 2026-09-14 OK"
    lines = body.splitlines()
    assert "status=OK" in lines
    assert "uploaded=2" in lines
    assert "purged=1" in lines
    assert "reconciled=True" in lines
    assert "backup_missing=0" in lines

