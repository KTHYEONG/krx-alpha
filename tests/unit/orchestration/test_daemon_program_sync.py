"""Nightly program-trades sync scheduling (supervised one-shot child off the critical path)."""

from __future__ import annotations

import datetime as dt
import threading
from typing import ClassVar
from zoneinfo import ZoneInfo

_KST = ZoneInfo("Asia/Seoul")


class _FakePopen:
    """Non-blocking Popen double recording launch args and lifecycle signals."""

    instances: ClassVar[list[_FakePopen]] = []

    def __init__(self, cmd, *, env=None, exit_code=None) -> None:
        self.cmd = list(cmd)
        self.env = dict(env) if env else {}
        self._exit_code = exit_code
        self.terminated = False
        self.killed = False
        self.wait_calls: list[object] = []
        _FakePopen.instances.append(self)

    def poll(self):
        return self._exit_code

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True

    def wait(self, timeout=None):
        self.wait_calls.append(timeout)
        return self._exit_code


def _business_view(day: dt.date):
    from src.marketdata.toss_calendar import TradingDay
    from src.orchestration.trading_day_gate import TradingDayStatus, TradingDayView

    trading_day = TradingDay(
        date=day,
        is_business_day=True,
        previous_business_day=day - dt.timedelta(days=1),
        next_business_day=day + dt.timedelta(days=1),
    )
    return TradingDayView(date=day, status=TradingDayStatus.BUSINESS, trading_day=trading_day)


def _holiday_view(day: dt.date):
    from src.orchestration.trading_day_gate import TradingDayStatus, TradingDayView

    return TradingDayView(date=day, status=TradingDayStatus.HOLIDAY, trading_day=None)


def _runner(tmp_path, monkeypatch, *, now: dt.datetime):
    import pathlib

    from src.core.config import CollectorSettings, resolve_collector_runtime
    from src.orchestration.daemon import DaemonRunner

    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    runtime = resolve_collector_runtime(collector=settings)
    return DaemonRunner(runtime=runtime, shutdown=threading.Event(), now=lambda: now, sleep=lambda s: None)


def test_orchestration_no_longer_calls_toss_program_trades(tmp_path, monkeypatch) -> None:
    # Given: bars/universe 준비 경로와 호출되면 실패하는 프로그램매매 서비스
    import datetime as dt
    import pathlib

    import polars as pl

    from src.core.config import CollectorSettings
    from src.marketdata import program_trade_service as service
    from src.orchestration import daemon
    from src.universe.ipc import write_candidates

    monkeypatch.setenv("KRX_OPENAPI_KEY", "k")
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    settings.paths.bars_daily_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "date": [dt.date(2026, 9, 9)],
        "symbol": ["000001"],
        "close": [1000.0],
        "volume": [1000],
        "trade_value_100m": [100.0],
        "daily_change_pct": [1.0],
    }).write_parquet(settings.paths.bars_daily_dir / "2026-09.parquet")

    def _fake_refresh(**kwargs):
        return daemon.BarsRefreshResult(trading_day=dt.date(2026, 9, 9), appended_rows=0, backfilled_days=0)

    def _fake_plan(**kwargs):
        settings.paths.candidates.parent.mkdir(parents=True, exist_ok=True)
        write_candidates(settings.paths.candidates, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=1)
        return daemon.UniversePlanResult(
            decision_date=dt.date(2026, 9, 9), selected=1, out_path=kwargs["out_path"], candidates_emitted=1
        )

    def _must_not_call(*args, **kwargs):
        raise AssertionError("orchestration must not invoke program-trade fetch")

    monkeypatch.setattr(daemon, "refresh_bars", _fake_refresh)
    monkeypatch.setattr(daemon, "plan_universe", _fake_plan)
    monkeypatch.setattr(service, "backfill_program_trades_history", _must_not_call)
    monkeypatch.setattr(service, "append_program_trades", _must_not_call)
    assert not hasattr(daemon, "_run_program_trades_auto_backfill")

    # When / Then: 08:20 경로에서 Toss 호출 없이 후보 준비를 반환한다
    assert daemon.run_session_orchestration(today=dt.date(2026, 9, 10), settings=settings) is True


def test_sync_launched_once_after_business_day_eod(tmp_path, monkeypatch) -> None:
    # Given: 영업일 EOD가 끝난 심야 상태
    import datetime as dt

    from src.orchestration import daemon as daemon_mod

    _FakePopen.instances.clear()
    day = dt.date(2026, 9, 24)
    now = dt.datetime(2026, 9, 25, 2, 0, tzinfo=_KST)
    runner = _runner(tmp_path, monkeypatch, now=now)
    runner._state.eod_attempted_for = day
    monkeypatch.setattr(runner._gate, "view", lambda today, at: _business_view(day))
    monkeypatch.setattr(daemon_mod.subprocess, "Popen", _FakePopen)

    # When: 여러 스텝이 지나도
    runner.step(now)
    runner.step(now)

    # Then: 자식은 한 번만, 논블로킹으로 실행되고 인자는 세션일/직전영업일이다
    assert len(_FakePopen.instances) == 1
    cmd = _FakePopen.instances[0].cmd
    assert cmd[cmd.index("--session-date") + 1] == "2026-09-24"
    assert cmd[cmd.index("--complete-through") + 1] == "2026-09-23"
    assert "-m" in cmd
    assert "src.cli.toss_program_trades_sync" in cmd
    assert runner._state.program_sync_day == day
    assert runner._children.program_sync is _FakePopen.instances[0]


def test_sync_not_launched_on_holidays(tmp_path, monkeypatch) -> None:
    # Given: 휴장일 EOD 상태
    import datetime as dt

    from src.orchestration import daemon as daemon_mod

    _FakePopen.instances.clear()
    day = dt.date(2026, 9, 24)
    now = dt.datetime(2026, 9, 25, 2, 0, tzinfo=_KST)
    runner = _runner(tmp_path, monkeypatch, now=now)
    runner._state.eod_attempted_for = day
    monkeypatch.setattr(runner._gate, "view", lambda today, at: _holiday_view(day))
    monkeypatch.setattr(daemon_mod.subprocess, "Popen", _FakePopen)

    # When / Then
    runner.step(now)
    assert _FakePopen.instances == []
    assert runner._state.program_sync_day is None


def test_sync_not_launched_when_disabled(tmp_path, monkeypatch) -> None:
    # Given: 자동 백필 비활성화 설정
    import datetime as dt

    from src.orchestration import daemon as daemon_mod

    _FakePopen.instances.clear()
    monkeypatch.setenv("KRX_ALPHA_TOSS_PROGRAM_AUTO_BACKFILL_ENABLED", "false")
    day = dt.date(2026, 9, 24)
    now = dt.datetime(2026, 9, 25, 2, 0, tzinfo=_KST)
    runner = _runner(tmp_path, monkeypatch, now=now)
    runner._state.eod_attempted_for = day
    monkeypatch.setattr(runner._gate, "view", lambda today, at: _business_view(day))
    monkeypatch.setattr(daemon_mod.subprocess, "Popen", _FakePopen)

    # When / Then
    runner.step(now)
    assert _FakePopen.instances == []


def test_sync_launch_failure_alerted(tmp_path, monkeypatch, caplog) -> None:
    # Given: Popen 자체가 실패하는 환경
    import datetime as dt
    import logging

    from src.orchestration import daemon as daemon_mod

    day = dt.date(2026, 9, 24)
    now = dt.datetime(2026, 9, 25, 2, 0, tzinfo=_KST)
    runner = _runner(tmp_path, monkeypatch, now=now)
    runner._state.eod_attempted_for = day
    monkeypatch.setattr(runner._gate, "view", lambda today, at: _business_view(day))

    def _raise(cmd, *, env=None):
        raise OSError("fork down")

    monkeypatch.setattr(daemon_mod.subprocess, "Popen", _raise)

    # When / Then: CRITICAL 후 다음 스텝에서 재시도할 수 있다
    with caplog.at_level(logging.CRITICAL):
        runner.step(now)
    assert "stage=program_trades_sync status=FAIL" in caplog.text
    assert runner._children.program_sync is None
    assert runner._state.program_sync_day is None


def test_child_success_logged_and_reaped(tmp_path, monkeypatch, caplog) -> None:
    # Given: exit 0으로 끝난 자식
    import datetime as dt
    import logging

    now = dt.datetime(2026, 9, 25, 2, 0, tzinfo=_KST)
    runner = _runner(tmp_path, monkeypatch, now=now)
    runner._children.program_sync = _FakePopen(["sync"], exit_code=0)
    runner._children.program_sync_started_at = now

    # When
    with caplog.at_level(logging.INFO):
        runner._poll_program_sync(now)

    # Then
    assert runner._children.program_sync is None
    assert "stage=program_trades_sync status=OK exit_code=0" in caplog.text


def test_child_stale_exit_stays_silent(tmp_path, monkeypatch, caplog) -> None:
    # Given: exit 2(자식이 이미 CRITICAL 보고)로 끝난 자식
    import datetime as dt
    import logging

    now = dt.datetime(2026, 9, 25, 2, 0, tzinfo=_KST)
    runner = _runner(tmp_path, monkeypatch, now=now)
    runner._children.program_sync = _FakePopen(["sync"], exit_code=2)
    runner._children.program_sync_started_at = now

    # When
    with caplog.at_level(logging.INFO):
        runner._poll_program_sync(now)

    # Then: 데몬은 추가로 로그하지 않고 회수만 한다
    assert runner._children.program_sync is None
    assert "program_trades_sync" not in caplog.text


def test_child_failure_alerted(tmp_path, monkeypatch, caplog) -> None:
    # Given: exit 1로 죽은 자식
    import datetime as dt
    import logging

    now = dt.datetime(2026, 9, 25, 2, 0, tzinfo=_KST)
    runner = _runner(tmp_path, monkeypatch, now=now)
    runner._children.program_sync = _FakePopen(["sync"], exit_code=1)
    runner._children.program_sync_started_at = now

    # When
    with caplog.at_level(logging.CRITICAL):
        runner._poll_program_sync(now)

    # Then
    assert runner._children.program_sync is None
    assert "stage=program_trades_sync status=FAIL exit_code=1" in caplog.text


def test_child_killed_by_signal_alerted(tmp_path, monkeypatch, caplog) -> None:
    # Given: 시그널로 죽은 자식(음수 코드)
    import datetime as dt
    import logging

    now = dt.datetime(2026, 9, 25, 2, 0, tzinfo=_KST)
    runner = _runner(tmp_path, monkeypatch, now=now)
    runner._children.program_sync = _FakePopen(["sync"], exit_code=-15)
    runner._children.program_sync_started_at = now

    # When
    with caplog.at_level(logging.CRITICAL):
        runner._poll_program_sync(now)

    # Then
    assert "stage=program_trades_sync status=FAIL exit_code=-15" in caplog.text


def test_timeout_kills_child_and_alerts(tmp_path, monkeypatch, caplog) -> None:
    # Given: 타임아웃을 넘겨 계속 도는 자식 (wait는 한 번 타임아웃 후 종료)
    import datetime as dt
    import logging

    monkeypatch.setenv("KRX_ALPHA_TOSS_PROGRAM_SYNC_TIMEOUT_S", "60")
    now = dt.datetime(2026, 9, 25, 2, 0, tzinfo=_KST)
    runner = _runner(tmp_path, monkeypatch, now=now)
    proc = _FakePopen(["sync"], exit_code=None)

    calls = {"wait": 0}
    real_wait = proc.wait

    def _flaky_wait(timeout=None):
        calls["wait"] += 1
        if calls["wait"] == 1:
            raise TimeoutError("still running")
        return real_wait(timeout)

    proc.wait = _flaky_wait  # type: ignore[method-assign]
    runner._children.program_sync = proc
    runner._children.program_sync_started_at = now - dt.timedelta(hours=3)

    # When
    with caplog.at_level(logging.CRITICAL):
        runner._poll_program_sync(now)

    # Then: terminate 후 kill, CRITICAL reason=timeout
    assert proc.terminated is True
    assert proc.killed is True
    assert runner._children.program_sync is None
    assert "stage=program_trades_sync status=FAIL reason=timeout" in caplog.text


def test_sync_child_kill_failure_stays_nonblocking(tmp_path, monkeypatch, caplog) -> None:
    # Given: kill마저 실패하는 자식
    import logging

    from src.orchestration.daemon import _stop_sync_child

    proc = _FakePopen(["sync"], exit_code=None)

    def _hanging_wait(timeout=None):
        raise TimeoutError("still running")

    def _failing_kill() -> None:
        raise OSError("kill down")

    proc.wait = _hanging_wait  # type: ignore[method-assign]
    proc.kill = _failing_kill  # type: ignore[method-assign]

    # When / Then: 예외 없이 경고만 남기고 반환한다
    with caplog.at_level(logging.WARNING):
        _stop_sync_child(proc, grace_s=5.0)
    assert "status=KILL_FAIL" in caplog.text


def test_shutdown_stops_running_sync_child(tmp_path, monkeypatch) -> None:
    # Given: 실행 중인 sync 자식과 종료 신호
    import datetime as dt

    now = dt.datetime(2026, 9, 25, 2, 0, tzinfo=_KST)
    runner = _runner(tmp_path, monkeypatch, now=now)
    proc = _FakePopen(["sync"], exit_code=None)

    calls = {"wait": 0}

    def _flaky_wait(timeout=None):
        calls["wait"] += 1
        if calls["wait"] == 1:
            raise TimeoutError("still running")
        return None

    proc.wait = _flaky_wait  # type: ignore[method-assign]
    runner._children.program_sync = proc
    runner._children.program_sync_started_at = now
    runner._shutdown.set()

    # When
    runner.stop_children()

    # Then: 데드라인 안에 terminate/kill로 정리된다
    assert proc.terminated is True
    assert proc.killed is True
    assert runner._children.program_sync is None


def test_shutdown_signals_sync_child_before_waiting_supervisors(tmp_path, monkeypatch) -> None:
    # Given: 실행 중인 sync 자식, 감독 대상 정리가 오래 걸리는 상황
    import datetime as dt

    import src.orchestration.daemon as daemon_mod

    now = dt.datetime(2026, 9, 25, 2, 0, tzinfo=_KST)
    runner = _runner(tmp_path, monkeypatch, now=now)
    proc = _FakePopen(["sync"], exit_code=None)
    runner._children.program_sync = proc
    runner._children.program_sync_started_at = now
    seen: dict[str, bool] = {}

    def _stop_supervisors(targets, *, deadline_s):
        seen["sync_terminated_before_wait"] = proc.terminated
        return {"graceful": 0, "killed": 0, "not_running": 0}

    monkeypatch.setattr(daemon_mod, "stop_supervisors", _stop_supervisors)

    # When
    runner.stop_children()

    # Then: 대기를 겹치기 위해 supervisor 대기 전에 sync 자식에 SIGTERM 이 이미 나가 있다
    assert seen["sync_terminated_before_wait"] is True
    assert runner._children.program_sync is None


def test_night_sleep_shortened_while_sync_child_runs(tmp_path, monkeypatch) -> None:
    # Given: 야간(02:00)에 실행 중인 sync 자식
    import datetime as dt

    from src.orchestration.daemon import PROGRAM_SYNC_POLL_S

    now = dt.datetime(2026, 9, 25, 2, 0, tzinfo=_KST)
    runner = _runner(tmp_path, monkeypatch, now=now)
    runner._children.program_sync = _FakePopen(["sync"], exit_code=None)
    runner._children.program_sync_started_at = now

    # When
    delay = runner.step(now)

    # Then: 30분 야간 수면 대신 짧게 깨어나 타임아웃을 제때 점검한다
    assert delay <= PROGRAM_SYNC_POLL_S
