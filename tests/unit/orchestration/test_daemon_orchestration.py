"""Daemon orchestration cases split from test_daemon.py (bars/universe orchestration, retry and degraded fallback, trading-day calendar, token preflight, and program-trade backfill)."""

from __future__ import annotations

import logging

import pytest
from tests.unit.orchestration.daemon_fixtures import _business_trading_day


@pytest.fixture(autouse=True)
def _verified_aftermarket_capacity(monkeypatch) -> None:
    """Daemon tests simulate a deployed host with verified aftermarket capacity."""
    monkeypatch.setenv("KRX_ALPHA_AFTERMARKET_PAIR_CAPACITY_PER_CONNECTION", "4")


@pytest.fixture(autouse=True)


def _verified_aftermarket_capacity(monkeypatch) -> None:
    """Daemon tests simulate a deployed host with verified aftermarket capacity."""
    monkeypatch.setenv("KRX_ALPHA_AFTERMARKET_PAIR_CAPACITY_PER_CONNECTION", "4")


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

    # Then: 수집을 건너뛰고 상태 경계(08:50)까지 대기한다
    assert orchestrated == []
    mock_sleep.assert_called_once_with(1200.0)


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


def test_build_kis_client_wires_shared_token_cache_path(tmp_path, monkeypatch) -> None:
    import hashlib
    import pathlib

    from src.brokers.kis.data import KisDataClient
    from src.core.config import CollectorSettings, ExecutionSettings
    from src.orchestration import daemon

    monkeypatch.setenv("KIS_APP_KEY", "k")
    monkeypatch.setenv("KIS_APP_SECRET", "s")
    monkeypatch.setenv("KIS_ACCOUNT_NO", "12345678")
    monkeypatch.setenv("KIS_ACCOUNT_PRODUCT_CODE", "01")
    monkeypatch.setenv("KRX_ALPHA_KIS_TOKEN_CACHE_DIR", str(tmp_path / "kis-tokens"))
    data_root = pathlib.Path(tmp_path) / "data"
    collector = CollectorSettings(data_root=data_root)
    execution = ExecutionSettings(data_root=data_root)

    client = daemon._build_kis_client(collector.paths)

    assert isinstance(client, KisDataClient)
    expected = pathlib.Path(tmp_path) / "kis-tokens" / f"token_{hashlib.sha256(b'k').hexdigest()[:12]}.json"
    assert client._transport._tokens._token_cache_path == expected
    assert client._transport._tokens._allow_token_issue is True
    assert expected.parent == pathlib.Path(tmp_path) / "kis-tokens"
    assert execution.paths.root == collector.paths.root


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
    assert sleeps[-1] == 1200.0


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


def test_journal_age_s_finds_routed_regular_partition(tmp_path) -> None:
    import datetime as dt
    import os

    from src.orchestration.daemon import _journal_age_s

    day = dt.date(2026, 9, 14)
    now = dt.datetime(2026, 9, 14, 10, 0, tzinfo=dt.UTC)
    path = tmp_path / 'ls' / 'krx' / 'regular' / 'H0STCNT0' / 'dt=2026-09-14' / '09.jsonl.zst'
    path.parent.mkdir(parents=True)
    path.write_bytes(b'x')
    os.utime(path, (now.timestamp() - 50, now.timestamp() - 50))
    assert _journal_age_s(tmp_path, 'ls', day, now) == 50.0


def test_run_session_orchestration_passes_status_source_and_store(tmp_path, monkeypatch) -> None:
    # Given: KIS 자격증명 없이 plan_universe를 가짜로 둔 오케스트레이션
    import datetime as dt
    import pathlib

    import polars as pl

    from src.core.config import CollectorSettings
    from src.orchestration import daemon
    from src.storage.snapshot_store import SnapshotStore
    from src.universe.ipc import write_candidates

    monkeypatch.setenv("KRX_OPENAPI_KEY", "k")
    for name in ("KIS_APP_KEY", "KIS_APP_SECRET", "KIS_ACCOUNT_NO", "KIS_ACCOUNT_PRODUCT_CODE"):
        monkeypatch.delenv(name, raising=False)
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
        return daemon.BarsRefreshResult(trading_day=dt.date(2026, 9, 9), appended_rows=0, backfilled_days=0)

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

    # Then
    assert ready is True
    assert calls["plan"]["status_source"] is None
    assert calls["plan"]["session_date"] == dt.date(2026, 9, 10)
    assert isinstance(calls["plan"]["snapshot_store"], SnapshotStore)


def _clear_program_backfill_env(monkeypatch) -> None:
    import os

    for name in [n for n in os.environ if n.startswith("KRX_ALPHA_TOSS_PROGRAM_")]:
        monkeypatch.delenv(name, raising=False)


def _ready_orchestration_setup(tmp_path, monkeypatch, *, symbols=("005930", "000660")):
    import datetime as dt
    import pathlib

    import polars as pl

    from src.core.config import CollectorSettings
    from src.orchestration import daemon
    from src.universe.ipc import write_candidates

    monkeypatch.setenv("KRX_OPENAPI_KEY", "k")
    monkeypatch.setenv("TOSS_APP_KEY", "tsck_test")
    monkeypatch.setenv("TOSS_APP_SECRET", "tssk_test")
    _clear_program_backfill_env(monkeypatch)
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

    def _fake_plan(**kwargs):
        settings.paths.candidates.parent.mkdir(parents=True, exist_ok=True)
        write_candidates(
            settings.paths.candidates,
            [{"symbol": symbol, "selection_reasons": ["limit_up"]} for symbol in symbols],
            rev=1,
        )
        return daemon.UniversePlanResult(
            decision_date=dt.date(2026, 9, 9), selected=len(symbols), out_path=kwargs["out_path"], candidates_emitted=len(symbols)
        )

    monkeypatch.setattr(daemon, "refresh_bars", _fake_refresh)
    monkeypatch.setattr(daemon, "plan_universe", _fake_plan)
    return settings


def test_run_session_orchestration_triggers_auto_backfill_when_ready(tmp_path, monkeypatch) -> None:
    # Given: 정상 bars/universe/candidates 준비 경로와 가짜 백필
    import datetime as dt

    from src.marketdata.service import ProgramTradesBackfillResult
    from src.orchestration import daemon

    settings = _ready_orchestration_setup(tmp_path, monkeypatch)
    seen: dict[str, object] = {}

    def _fake_backfill(**kwargs):
        seen.update(kwargs)
        return ProgramTradesBackfillResult(symbols_ok=2, symbols_failed=0, appended_rows=10)

    monkeypatch.setattr(daemon, "backfill_universe_program_trades", _fake_backfill)

    # When
    ready = daemon.run_session_orchestration(today=dt.date(2026, 9, 10), settings=settings)

    # Then: 오늘 candidates 심볼 튜플과 reference_date=today로 호출되고 반환은 그대로 True
    assert ready is True
    assert seen["symbols"] == ("005930", "000660")
    assert seen["reference_date"] == dt.date(2026, 9, 10)
    assert seen["store_path"] == settings.paths.program_trades_store


def test_run_session_orchestration_skips_auto_backfill_when_not_ready(tmp_path, monkeypatch) -> None:
    # Given: plan_universe가 빈 candidates를 발행하는 준비 실패 경로
    import datetime as dt
    import pathlib

    import polars as pl

    from src.core.config import CollectorSettings
    from src.orchestration import daemon

    monkeypatch.setenv("KRX_OPENAPI_KEY", "k")
    monkeypatch.setenv("TOSS_APP_KEY", "tsck_test")
    monkeypatch.setenv("TOSS_APP_SECRET", "tssk_test")
    _clear_program_backfill_env(monkeypatch)
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
    monkeypatch.setattr(
        daemon, "refresh_bars", lambda **kw: daemon.BarsRefreshResult(trading_day=dt.date(2026, 9, 9), appended_rows=0, backfilled_days=0)
    )
    monkeypatch.setattr(
        daemon,
        "plan_universe",
        lambda **kw: daemon.UniversePlanResult(
            decision_date=dt.date(2026, 9, 9), selected=0, out_path=kw["out_path"], candidates_emitted=0
        ),
    )

    def _must_not_call(**kwargs):
        raise AssertionError("backfill must not run when universe is not ready")

    monkeypatch.setattr(daemon, "backfill_universe_program_trades", _must_not_call)

    # When
    ready = daemon.run_session_orchestration(today=dt.date(2026, 9, 10), settings=settings)

    # Then: 백필 미호출
    assert ready is False


def test_run_session_orchestration_isolates_missing_toss_credentials(tmp_path, monkeypatch, caplog) -> None:
    # Given: TOSS 자격증명 미설정, 그 외 정상 경로
    import datetime as dt
    import logging

    from src.orchestration import daemon

    settings = _ready_orchestration_setup(tmp_path, monkeypatch)
    monkeypatch.delenv("TOSS_APP_KEY", raising=False)
    monkeypatch.delenv("TOSS_APP_SECRET", raising=False)

    def _must_not_call(**kwargs):
        raise AssertionError("backfill must not run without credentials")

    monkeypatch.setattr(daemon, "backfill_universe_program_trades", _must_not_call)

    # When
    with caplog.at_level(logging.WARNING):
        ready = daemon.run_session_orchestration(today=dt.date(2026, 9, 10), settings=settings)

    # Then: 스트리밍 준비에 영향 없이 True + WARNING 로그
    assert ready is True
    assert "program_trades_auto_backfill" in caplog.text
    assert any(r.levelno == logging.WARNING for r in caplog.records)


def test_run_session_orchestration_isolates_program_trades_backfill_failure(tmp_path, monkeypatch, caplog) -> None:
    # Given: 백필이 TossProgramTradesError를 던지는 경로
    import datetime as dt
    import logging

    from src.marketdata.toss_program_trades import TossProgramTradesError
    from src.orchestration import daemon

    settings = _ready_orchestration_setup(tmp_path, monkeypatch)

    def _fail_backfill(**kwargs):
        raise TossProgramTradesError("store unreadable")

    monkeypatch.setattr(daemon, "backfill_universe_program_trades", _fail_backfill)

    # When
    with caplog.at_level(logging.ERROR):
        ready = daemon.run_session_orchestration(today=dt.date(2026, 9, 10), settings=settings)

    # Then: 예외 전파 없이 True + ERROR 로그
    assert ready is True
    assert "program_trades_auto_backfill" in caplog.text
    assert any(r.levelno == logging.ERROR for r in caplog.records)


def test_program_trades_auto_backfill_skips_when_disabled(tmp_path, monkeypatch) -> None:
    # Given: 자동 백필 비활성화 설정
    import datetime as dt
    import pathlib

    from src.core.config import CollectorSettings
    from src.orchestration import daemon

    monkeypatch.setenv("TOSS_APP_KEY", "tsck_test")
    monkeypatch.setenv("TOSS_APP_SECRET", "tssk_test")
    monkeypatch.setenv("KRX_ALPHA_TOSS_PROGRAM_AUTO_BACKFILL_ENABLED", "false")
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")

    def _must_not_call(**kwargs):
        raise AssertionError("backfill must not run when disabled")

    monkeypatch.setattr(daemon, "backfill_universe_program_trades", _must_not_call)

    # When / Then: 후보 파일 유무와 무관하게 조용히 반환
    daemon._run_program_trades_auto_backfill(settings.paths, dt.date(2026, 9, 10))


def test_program_trades_auto_backfill_skips_when_no_candidates(tmp_path, monkeypatch) -> None:
    # Given: 자격증명은 있으나 후보 파일이 없는 경로
    import datetime as dt
    import pathlib

    from src.core.config import CollectorSettings
    from src.orchestration import daemon

    monkeypatch.setenv("TOSS_APP_KEY", "tsck_test")
    monkeypatch.setenv("TOSS_APP_SECRET", "tssk_test")
    _clear_program_backfill_env(monkeypatch)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")

    def _must_not_call(**kwargs):
        raise AssertionError("backfill must not run without candidates")

    monkeypatch.setattr(daemon, "backfill_universe_program_trades", _must_not_call)

    # When / Then: 예외 없이 반환
    daemon._run_program_trades_auto_backfill(settings.paths, dt.date(2026, 9, 10))


def test_program_trades_auto_backfill_isolates_unexpected_failure(tmp_path, monkeypatch, caplog) -> None:
    # Given: 손상된 후보 파일(읽기 시 예외 발생)
    import datetime as dt
    import logging
    import pathlib

    from src.core.config import CollectorSettings
    from src.orchestration import daemon

    monkeypatch.setenv("TOSS_APP_KEY", "tsck_test")
    monkeypatch.setenv("TOSS_APP_SECRET", "tssk_test")
    _clear_program_backfill_env(monkeypatch)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    settings.paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    settings.paths.candidates.write_text("{not-json", encoding="utf-8")

    def _must_not_call(**kwargs):
        raise AssertionError("backfill must not run on corrupt candidates")

    monkeypatch.setattr(daemon, "backfill_universe_program_trades", _must_not_call)

    # When / Then: 호출자에게 전파하지 않고 ERROR 로그만 남긴다
    with caplog.at_level(logging.ERROR):
        daemon._run_program_trades_auto_backfill(settings.paths, dt.date(2026, 9, 10))
    assert "program_trades_auto_backfill" in caplog.text


def test_daemon_business_day_passes_trading_day_through(tmp_path, monkeypatch) -> None:
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    business = _business_trading_day(dt.date(2026, 9, 14))
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: business)
    seen: list[dict] = []
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: seen.append(kw) or False)

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            pass

        def ensure_running(self):
            return "started"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    now = dt.datetime(2026, 9, 14, 8, 30, tzinfo=ZoneInfo("Asia/Seoul"))
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now)
    assert len(seen) == 1
    assert seen[0]["trading_day"] is business


def test_kis_token_preflight_runs_once_before_orchestration_on_retry(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.marketdata.toss_calendar import TradingDay
    from src.orchestration.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    business = TradingDay(
        date=dt.date(2026, 9, 14), is_business_day=True,
        previous_business_day=dt.date(2026, 9, 11), next_business_day=dt.date(2026, 9, 15),
    )
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: business)

    order: list[str] = []
    preflight_calls: list[object] = []

    def _fake_preflight(paths, today, *args, **kwargs):
        preflight_calls.append(today)
        order.append("preflight")
        return {}

    orch_calls: list[int] = []

    def _fake_orchestration(**kwargs):
        order.append("orchestration")
        orch_calls.append(1)
        return False

    monkeypatch.setattr(daemon_mod, "_kis_token_preflight", _fake_preflight)
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", _fake_orchestration)

    kst = ZoneInfo("Asia/Seoul")
    times = iter([
        dt.datetime(2026, 9, 14, 8, 30, 0, tzinfo=kst),
        dt.datetime(2026, 9, 14, 8, 36, 0, tzinfo=kst),
    ])

    run_collector_daemon(sleep_fn=MagicMock(), max_cycles=2, now_fn=lambda: next(times))

    assert len(preflight_calls) == 1
    assert preflight_calls[0] == dt.date(2026, 9, 14)
    assert order[0] == "preflight"
    assert order.count("orchestration") == 2


def test_kis_token_preflight_skipped_on_holiday(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.marketdata.toss_calendar import TradingDay
    from src.orchestration.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    holiday = TradingDay(
        date=dt.date(2026, 9, 14), is_business_day=False,
        previous_business_day=dt.date(2026, 9, 11), next_business_day=dt.date(2026, 9, 15),
    )
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: holiday)
    called: list[object] = []
    monkeypatch.setattr(daemon_mod, "_kis_token_preflight", lambda paths, today, *a, **k: called.append(today) or {})
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: (_ for _ in ()).throw(AssertionError("must not orchestrate")))

    morning = dt.datetime(2026, 9, 14, 8, 25, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    run_collector_daemon(sleep_fn=MagicMock(), max_cycles=1, now_fn=lambda: morning)

    assert called == []


def test_kis_token_preflight_failure_does_not_block_other_key(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    import pathlib
    from types import SimpleNamespace

    from src.core.config import CollectorSettings
    from src.execution.contracts import KisApiError
    from src.execution.kis_client import TokenSource
    from src.orchestration import daemon as daemon_mod

    import hashlib

    primary_key = "primary-app-key-1"
    snapshot_key = "snapshot-app-key-2"
    primary_fp = hashlib.sha256(primary_key.encode()).hexdigest()[:12]
    snapshot_fp = hashlib.sha256(snapshot_key.encode()).hexdigest()[:12]

    def _raise_token():
        raise KisApiError("EGW00103", "bad")

    primary_client = SimpleNamespace(
        _creds=SimpleNamespace(kis_app_key=primary_key), ensure_token=_raise_token,
    )
    snapshot_client = SimpleNamespace(
        _creds=SimpleNamespace(kis_app_key=snapshot_key), ensure_token=lambda: TokenSource.CACHE,
    )
    monkeypatch.setattr(daemon_mod, "_build_kis_client", lambda paths: primary_client)
    monkeypatch.setattr(
        daemon_mod, "_build_snapshot_preflight_client", lambda paths, *a, **k: (snapshot_client, snapshot_fp)
    )
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")

    with caplog.at_level(logging.CRITICAL):
        outcomes = daemon_mod._kis_token_preflight(settings.paths, dt.date(2026, 9, 14))

    assert outcomes[primary_fp] == "fail:EGW00103"
    assert outcomes[snapshot_fp] == "cache"
    assert "result=fail reason=EGW00103" in caplog.text


def test_kis_token_preflight_logs_fingerprints_only(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import hashlib
    import logging
    import pathlib
    from types import SimpleNamespace

    from src.core.config import CollectorSettings
    from src.execution.kis_client import TokenSource
    from src.orchestration import daemon as daemon_mod

    primary_key = "primary-secret-key-abc"
    snapshot_key = "snapshot-secret-key-xyz"
    primary_fp = hashlib.sha256(primary_key.encode()).hexdigest()[:12]
    snapshot_fp = hashlib.sha256(snapshot_key.encode()).hexdigest()[:12]
    primary_client = SimpleNamespace(
        _creds=SimpleNamespace(kis_app_key=primary_key), ensure_token=lambda: TokenSource.CACHE,
    )
    snapshot_client = SimpleNamespace(
        _creds=SimpleNamespace(kis_app_key=snapshot_key), ensure_token=lambda: TokenSource.ISSUED,
    )
    monkeypatch.setattr(daemon_mod, "_build_kis_client", lambda paths: primary_client)
    monkeypatch.setattr(
        daemon_mod, "_build_snapshot_preflight_client", lambda paths, *a, **k: (snapshot_client, snapshot_fp)
    )
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")

    with caplog.at_level(logging.INFO):
        outcomes = daemon_mod._kis_token_preflight(settings.paths, dt.date(2026, 9, 14))

    assert primary_fp in caplog.text
    assert snapshot_fp in caplog.text
    assert primary_key not in caplog.text
    assert snapshot_key not in caplog.text
    assert outcomes[primary_fp] == "cache"
    assert outcomes[snapshot_fp] == "issued"


def test_kis_token_preflight_tolerates_unexpected_client_shape(tmp_path, monkeypatch) -> None:
    import datetime as dt
    import pathlib

    from src.core.config import CollectorSettings
    from src.core.errors import MissingCredentialsError
    from src.orchestration import daemon as daemon_mod

    def _missing_data(paths, *args, **kwargs):
        raise MissingCredentialsError("no data credential")

    monkeypatch.setattr(daemon_mod, "_build_kis_client", lambda paths: object())
    monkeypatch.setattr(daemon_mod, "_build_snapshot_preflight_client", _missing_data)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")

    outcomes = daemon_mod._kis_token_preflight(settings.paths, dt.date(2026, 9, 14))

    assert outcomes["unknown"] == "fail:AttributeError"
    assert outcomes["unknown-data"] == "fail:missing_credentials"


def _raise_missing():
    from src.core.errors import MissingCredentialsError

    raise MissingCredentialsError("no data credential")


def test_kis_token_preflight_maps_unexpected_ensure_error(tmp_path, monkeypatch) -> None:
    import datetime as dt
    import hashlib
    import pathlib
    from types import SimpleNamespace

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    primary_key = "primary-key-unexpected"
    primary_fp = hashlib.sha256(primary_key.encode()).hexdigest()[:12]

    def _boom():
        raise RuntimeError("boom")

    primary_client = SimpleNamespace(
        _creds=SimpleNamespace(kis_app_key=primary_key), ensure_token=_boom,
    )
    snapshot_client = SimpleNamespace(ensure_token=_boom)
    monkeypatch.setattr(daemon_mod, "_build_kis_client", lambda paths: primary_client)
    monkeypatch.setattr(
        daemon_mod, "_build_snapshot_preflight_client", lambda paths, *a, **k: (snapshot_client, "snapfp123456")
    )
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")

    outcomes = daemon_mod._kis_token_preflight(settings.paths, dt.date(2026, 9, 14))

    assert outcomes[primary_fp] == "fail:RuntimeError"
    assert outcomes["snapfp123456"] == "fail:RuntimeError"


def test_kis_token_preflight_maps_missing_credentials_for_both_keys(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    import pathlib

    from src.core.config import CollectorSettings
    from src.core.errors import MissingCredentialsError
    from src.orchestration import daemon as daemon_mod

    def _missing(paths, *args, **kwargs):
        raise MissingCredentialsError("no creds")

    monkeypatch.setattr(daemon_mod, "_build_kis_client", _missing)
    monkeypatch.setattr(daemon_mod, "_build_snapshot_preflight_client", _missing)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")

    with caplog.at_level(logging.CRITICAL):
        outcomes = daemon_mod._kis_token_preflight(settings.paths, dt.date(2026, 9, 14))

    assert outcomes == {"unknown": "fail:missing_credentials", "unknown-data": "fail:missing_credentials"}
    assert caplog.text.count("reason=missing_credentials") == 2


def test_build_snapshot_preflight_client_uses_configured_data_slot_without_network(tmp_path, monkeypatch) -> None:
    # Given: 가짜 데이터 슬롯 2개와 스냅샷 슬롯 2 지정 (실키·네트워크 없음)
    import pathlib

    from src.brokers.kis.auth import kis_app_key_fingerprint
    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    for slot in ("1", "2"):
        monkeypatch.setenv(f"KIS_DATA_{slot}_APP_KEY", f"fake-key-{slot}")
        monkeypatch.setenv(f"KIS_DATA_{slot}_APP_SECRET", f"fake-secret-{slot}")
        monkeypatch.setenv(f"KIS_DATA_{slot}_HTS_ID", f"hts{slot}")
    monkeypatch.setenv("KIS_DATA_SLOTS", "1,2")
    monkeypatch.setenv("KRX_ALPHA_SNAPSHOT_KIS_DATA_SLOT", "2")
    monkeypatch.setenv("KRX_ALPHA_KIS_TOKEN_CACHE_DIR", str(tmp_path / "kis"))
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")

    # When
    client, key_id = daemon_mod._build_snapshot_preflight_client(settings.paths)

    # Then: 슬롯 2 키로 구성되고 캐시 경로는 설정 디렉터리 아래 지문 파일이다
    assert client._transport._auth.app_key == "fake-key-2"
    assert not hasattr(client, "_creds")
    assert key_id == kis_app_key_fingerprint("fake-key-2")
    assert client._transport._tokens._token_cache_path == tmp_path / "kis" / f"token_{kis_app_key_fingerprint('fake-key-2')}.json"


def test_kis_token_preflight_records_snapshot_key_vendor_error(tmp_path, monkeypatch, caplog) -> None:
    # Given: 메인 키는 캐시 적중, 스냅샷 키는 벤더 거부
    import datetime as dt
    import logging
    import pathlib
    from types import SimpleNamespace

    from src.core.config import CollectorSettings
    from src.execution.contracts import KisApiError
    from src.execution.kis_client import TokenSource
    from src.orchestration import daemon as daemon_mod

    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    primary = SimpleNamespace(_creds=SimpleNamespace(kis_app_key="fake-primary"), ensure_token=lambda: TokenSource.CACHE)

    def _reject() -> TokenSource:
        raise KisApiError("EGW00103", "invalid appkey")

    monkeypatch.setattr(daemon_mod, "_build_kis_client", lambda paths: primary)
    monkeypatch.setattr(
        daemon_mod, "_build_snapshot_preflight_client", lambda paths, *a, **k: (SimpleNamespace(ensure_token=_reject), "snapfp000001")
    )

    # When
    with caplog.at_level(logging.INFO):
        outcomes = daemon_mod._kis_token_preflight(settings.paths, dt.date(2026, 9, 14))

    # Then: 스냅샷 키 실패만 CRITICAL로 기록되고 메인 키 결과는 유지된다
    assert outcomes["snapfp000001"] == "fail:EGW00103"
    assert "cache" in outcomes.values()
    assert any(
        r.levelno == logging.CRITICAL and "key_id=snapfp000001" in r.getMessage() and "reason=EGW00103" in r.getMessage()
        for r in caplog.records
    )
