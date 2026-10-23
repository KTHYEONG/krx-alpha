"""Nightly backfill job scheduling (tick tape first, then minute bars, one child at a time)."""

from __future__ import annotations

import datetime as dt
import threading
from types import SimpleNamespace
from typing import ClassVar
from zoneinfo import ZoneInfo

import pytest

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
        _FakePopen.instances.append(self)

    def poll(self):
        return self._exit_code

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True

    def wait(self, timeout=None):
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
    from src.orchestration.trading_day_gate import TradingDayView
    from src.orchestration.trading_day_gate import TradingDayStatus

    return TradingDayView(date=day, status=TradingDayStatus.HOLIDAY, trading_day=None)


def _runner(tmp_path, monkeypatch, *, now: dt.datetime, tick_enabled=True, minute_enabled=False):
    import pathlib

    from src.core.config import (
        CollectorSettings,
        TickTapeBackfillSettings,
        TossProgramTradesSettings,
        resolve_collector_runtime,
    )
    from src.orchestration.daemon import DaemonRunner

    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    runtime = resolve_collector_runtime(collector=settings)
    return DaemonRunner(
        runtime=runtime,
        shutdown=threading.Event(),
        now=lambda: now,
        sleep=lambda s: None,
        program_sync_settings=TossProgramTradesSettings(auto_backfill_enabled=False),
        tick_tape_settings=TickTapeBackfillSettings(enabled=tick_enabled),
        minute_backfill_settings=SimpleNamespace(enabled=minute_enabled),
    )


def _at(day: dt.date, hour: int, minute: int, second: int = 0) -> dt.datetime:
    return dt.datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=_KST)


def test_window_crossing_midnight() -> None:
    # Given: 자정을 가로지르는 23:30-05:30 윈도우
    from src.orchestration.daemon import nightly_window_open

    day = dt.date(2026, 9, 25)
    start, end = dt.time(23, 30), dt.time(5, 30)

    # When / Then: 경계는 [start, end) 로 닫힘/열림이다
    assert nightly_window_open(_at(day, 23, 29), start=start, end=end) is False
    assert nightly_window_open(_at(day, 23, 30), start=start, end=end) is True
    assert nightly_window_open(_at(day + dt.timedelta(days=1), 2, 0), start=start, end=end) is True
    assert nightly_window_open(_at(day + dt.timedelta(days=1), 5, 30), start=start, end=end) is False

    # Given: 자정을 넘지 않는 01:00-05:30 윈도우
    plain_start, plain_end = dt.time(1, 0), dt.time(5, 30)

    # When / Then: 같은 날 안에서 [start, end) 로 판정한다
    assert nightly_window_open(_at(day, 0, 59), start=plain_start, end=plain_end) is False
    assert nightly_window_open(_at(day, 1, 0), start=plain_start, end=plain_end) is True
    assert nightly_window_open(_at(day, 5, 30), start=plain_start, end=plain_end) is False


def test_nightly_window_identity_and_end() -> None:
    # Given: 자정을 가로지르는 윈도우와 넘지 않는 윈도우
    from src.orchestration.daemon import _nightly_window_end, _nightly_window_identity

    day = dt.date(2026, 9, 25)
    start, end = dt.time(23, 30), dt.time(5, 30)

    # When / Then: 식별자는 윈도우 시작일의 KST 날짜다
    assert _nightly_window_identity(_at(day, 23, 45), start=start, end=end) == day
    assert _nightly_window_identity(_at(day + dt.timedelta(days=1), 2, 0), start=start, end=end) == day
    assert _nightly_window_end(day, start=start, end=end) == _at(
        day + dt.timedelta(days=1), 5, 30
    )

    # When / Then: 넘지 않는 윈도우의 식별자와 종료는 같은 날이다
    plain_start, plain_end = dt.time(1, 0), dt.time(5, 30)
    assert _nightly_window_identity(_at(day, 2, 0), start=plain_start, end=plain_end) == day
    assert _nightly_window_end(day, start=plain_start, end=plain_end) == _at(day, 5, 30)


def test_child_alive_covers_supervisor_doubles() -> None:
    # Given: poll이 없고 is_running만 있는 감독자 더블
    from src.orchestration.daemon import _child_alive

    class _Sup:
        def __init__(self, running: bool) -> None:
            self._running = running

        def is_running(self) -> bool:
            return self._running

    # When / Then: 실행 여부가 그대로 반영되고, 둘 다 없으면 존재 자체로 판단한다
    assert _child_alive(_Sup(True)) is True
    assert _child_alive(_Sup(False)) is False
    assert _child_alive(object()) is True
    assert _child_alive(None) is False


def test_nightly_job_cmd_carries_deadline() -> None:
    # Given: 윈도우 종료 시각의 deadline
    import datetime as dt

    from src.orchestration.daemon import NightlyJob, _nightly_job_cmd

    deadline = dt.datetime(2026, 9, 26, 5, 30, tzinfo=_KST)

    # When / Then: 잡별 one-shot 모듈과 deadline 인자를 싣는다
    tick = _nightly_job_cmd(NightlyJob.TICK_TAPE, deadline=deadline)
    assert "backfill-tick-tape" in tick
    assert tick[tick.index("--deadline") + 1] == deadline.isoformat()
    minute = _nightly_job_cmd(NightlyJob.MINUTE_BARS, deadline=deadline)
    assert "backfill-minute-bars" in minute
    assert minute[minute.index("--deadline") + 1] == deadline.isoformat()


def test_disabled_never_launches(tmp_path, monkeypatch) -> None:
    # Given: 두 잡 모두 비활성, EOD는 완료된 심야
    from src.orchestration import daemon as daemon_mod

    _FakePopen.instances.clear()
    day = dt.date(2026, 9, 25)
    now = _at(day, 23, 0)
    runner = _runner(tmp_path, monkeypatch, now=now, tick_enabled=False, minute_enabled=False)
    runner._state.eod_attempted_for = day
    def forbidden(*args, **kwargs):
        raise AssertionError("disabled nightly jobs must have no calendar or command side effects")

    monkeypatch.setattr(runner._gate, "view", forbidden)
    monkeypatch.setattr(daemon_mod, "_nightly_job_cmd", forbidden)
    monkeypatch.setattr(daemon_mod.subprocess, "Popen", _FakePopen)

    # When: 23:00-06:00 사이클을 순회
    moment = now
    while moment <= _at(day + dt.timedelta(days=1), 6, 0):
        runner._poll_nightly(moment)
        runner._maybe_launch_nightly(moment)
        moment += dt.timedelta(minutes=30)

    # Then: 자식 명령이 한 번도 만들어지지 않는다
    assert _FakePopen.instances == []


def test_waits_for_eod_and_program_sync(tmp_path, monkeypatch) -> None:
    # Given: 윈도우 안(23:45)이지만 EOD 미시도 상태
    from src.orchestration import daemon as daemon_mod

    _FakePopen.instances.clear()
    day = dt.date(2026, 9, 25)
    now = _at(day, 23, 45)
    runner = _runner(tmp_path, monkeypatch, now=now)
    monkeypatch.setattr(runner._gate, "view", lambda today, at: _business_view(day))
    monkeypatch.setattr(daemon_mod.subprocess, "Popen", _FakePopen)

    # When / Then: EOD 없이는 실행되지 않는다
    runner._maybe_launch_nightly(now)
    assert _FakePopen.instances == []

    # Given: EOD는 됐지만 program_sync 자식이 도는 중
    runner._state.eod_attempted_for = day
    runner._children.program_sync = _FakePopen(["sync"], exit_code=None)

    # When / Then: sync이 살아있는 동안은 실행되지 않는다
    runner._maybe_launch_nightly(now)
    assert len(_FakePopen.instances) == 1  # program_sync 더미만 존재
    assert runner._children.nightly is None

    # When: 두 조건이 모두 해소된 첫 사이클
    runner._children.program_sync = None
    runner._poll_nightly(now)
    runner._maybe_launch_nightly(now)

    # Then: tick tape 자식이 한 번 실행된다
    assert len(_FakePopen.instances) == 2
    proc = _FakePopen.instances[1]
    assert runner._children.nightly is proc
    assert "backfill-tick-tape" in proc.cmd
    assert proc.cmd[proc.cmd.index("--deadline") + 1] == _at(day + dt.timedelta(days=1), 5, 30).isoformat()


def test_non_business_window_skips_eod_gate(tmp_path, monkeypatch) -> None:
    # Given: 윈도우 시작일이 휴장일이라 EOD가 없는 심야
    from src.orchestration import daemon as daemon_mod

    _FakePopen.instances.clear()
    day = dt.date(2026, 9, 27)
    now = _at(day, 23, 45)
    runner = _runner(tmp_path, monkeypatch, now=now)
    monkeypatch.setattr(runner._gate, "view", lambda today, at: _holiday_view(day))
    monkeypatch.setattr(daemon_mod.subprocess, "Popen", _FakePopen)

    # When / Then: EOD 없이도 실행된다
    runner._maybe_launch_nightly(now)
    assert len(_FakePopen.instances) == 1
    assert "backfill-tick-tape" in _FakePopen.instances[0].cmd


def test_tick_tape_before_minute_bars_never_concurrent(tmp_path, monkeypatch, caplog) -> None:
    # Given: 두 잡 활성, tick tape 실행 중
    import logging

    from src.orchestration import daemon as daemon_mod
    from src.orchestration.daemon import NightlyJob

    _FakePopen.instances.clear()
    day = dt.date(2026, 9, 25)
    launch_at = _at(day, 23, 45)
    runner = _runner(tmp_path, monkeypatch, now=launch_at, minute_enabled=True)
    runner._state.eod_attempted_for = day
    monkeypatch.setattr(runner._gate, "view", lambda today, at: _business_view(day))
    monkeypatch.setattr(daemon_mod.subprocess, "Popen", _FakePopen)

    # When: tick tape 자식이 살아있는 동안 사이클이 지나도
    runner._maybe_launch_nightly(launch_at)
    runner._poll_nightly(_at(day, 23, 46))
    runner._maybe_launch_nightly(_at(day, 23, 46))

    # Then: tick tape 하나만 돌고 minute bars는 대기한다
    assert len(_FakePopen.instances) == 1
    assert runner._children.nightly_job is NightlyJob.TICK_TAPE

    # When: tick tape이 exit 0으로 끝나고 다음 사이클이 오면
    _FakePopen.instances[0]._exit_code = 0
    with caplog.at_level(logging.INFO):
        runner._poll_nightly(_at(day, 23, 50))
    runner._maybe_launch_nightly(_at(day, 23, 50))

    # Then: 같은 윈도우 안에서 minute bars가 이어서 실행된다
    assert "stage=nightly_tick_tape status=OK" in caplog.text
    assert len(_FakePopen.instances) == 2
    assert runner._children.nightly_job is NightlyJob.MINUTE_BARS
    assert "backfill-minute-bars" in _FakePopen.instances[1].cmd


def test_one_attempt_per_window(tmp_path, monkeypatch, caplog) -> None:
    # Given: tick tape이 exit 4로 실패한 윈도우
    import logging

    from src.orchestration import daemon as daemon_mod

    _FakePopen.instances.clear()
    day = dt.date(2026, 9, 25)
    launch_at = _at(day, 23, 45)
    runner = _runner(tmp_path, monkeypatch, now=launch_at)
    runner._state.eod_attempted_for = day
    monkeypatch.setattr(runner._gate, "view", lambda today, at: _business_view(day))
    monkeypatch.setattr(daemon_mod.subprocess, "Popen", _FakePopen)
    runner._maybe_launch_nightly(launch_at)
    _FakePopen.instances[0]._exit_code = 4

    # When: 같은 윈도우(다음날 00:10)에 회수하고 재시도해도
    with caplog.at_level(logging.WARNING):
        runner._poll_nightly(_at(day + dt.timedelta(days=1), 0, 10))
    runner._maybe_launch_nightly(_at(day + dt.timedelta(days=1), 0, 10))

    # Then: 실패가 기록되고 재실행되지 않는다
    assert "stage=nightly_tick_tape status=FAIL exit_code=4" in caplog.text
    assert len(_FakePopen.instances) == 1
    # When: 다음 윈도우 식별자(다음날 23:31)가 오면
    runner._state.eod_attempted_for = day + dt.timedelta(days=1)
    monkeypatch.setattr(
        runner._gate, "view", lambda today, at: _business_view(day + dt.timedelta(days=1))
    )
    runner._maybe_launch_nightly(_at(day + dt.timedelta(days=1), 23, 31))

    # Then: 다시 한 번 시도한다
    assert len(_FakePopen.instances) == 2


def test_child_exit_two_logged_as_noop(tmp_path, monkeypatch, caplog) -> None:
    # Given: 할 일 없이 exit 2로 끝난 자식
    import logging

    day = dt.date(2026, 9, 25)
    now = _at(day, 23, 45)
    runner = _runner(tmp_path, monkeypatch, now=now)
    proc = _FakePopen(["backfill-tick-tape"], exit_code=2)
    runner._children.nightly = proc
    from src.orchestration.daemon import NightlyJob

    runner._children.nightly_job = NightlyJob.TICK_TAPE
    runner._children.nightly_started_at = now

    # When
    with caplog.at_level(logging.INFO):
        runner._poll_nightly(_at(day, 23, 50))

    # Then: 회수만 하고 INFO NOOP으로 남긴다
    assert runner._children.nightly is None
    assert "stage=nightly_tick_tape status=NOOP" in caplog.text


def test_deadline_enforcement(tmp_path, monkeypatch, caplog) -> None:
    # Given: 윈도우 종료를 넘겨도 도는 자식
    import logging

    from src.orchestration import daemon as daemon_mod

    _FakePopen.instances.clear()
    day = dt.date(2026, 9, 25)
    launch_at = _at(day, 23, 45)
    runner = _runner(tmp_path, monkeypatch, now=launch_at)
    runner._state.eod_attempted_for = day
    monkeypatch.setattr(runner._gate, "view", lambda today, at: _business_view(day))
    monkeypatch.setattr(daemon_mod.subprocess, "Popen", _FakePopen)
    runner._maybe_launch_nightly(launch_at)
    proc = _FakePopen.instances[0]

    # When: window_end + 121 s 에 폴링하면
    with caplog.at_level(logging.WARNING):
        runner._poll_nightly(_at(day + dt.timedelta(days=1), 5, 32, 1))

    # Then: 자식이 정리되고 deadline WARNING이 정확히 한 번 남는다
    assert proc.terminated is True
    assert runner._children.nightly is None
    assert caplog.text.count("reason=deadline") == 1
    assert "stage=nightly_tick_tape status=FAIL" in caplog.text


def test_holiday_and_shutdown_stop_the_child(tmp_path, monkeypatch) -> None:
    # Given: 실행 중인 nightly 자식
    from src.orchestration import daemon as daemon_mod

    _FakePopen.instances.clear()
    day = dt.date(2026, 9, 25)
    now = _at(day, 23, 45)
    runner = _runner(tmp_path, monkeypatch, now=now)
    runner._state.eod_attempted_for = day
    monkeypatch.setattr(runner._gate, "view", lambda today, at: _business_view(day))
    monkeypatch.setattr(daemon_mod.subprocess, "Popen", _FakePopen)
    runner._maybe_launch_nightly(now)
    proc = _FakePopen.instances[0]

    # When: 데몬 종료가 오면
    runner.stop_children()

    # Then: 자식이 정리된다
    assert proc.terminated is True
    assert runner._children.nightly is None

    # Given: 다시 실행 중인 nightly 자식과 휴장 전환
    runner._children.nightly = _FakePopen(["backfill-tick-tape"], exit_code=None)
    revived = runner._children.nightly

    # When: 휴장 자식 정리가 오면
    runner._stop_holiday_children()

    # Then: 자식이 정리된다
    assert revived.terminated is True
    assert runner._children.nightly is None


def test_never_during_sessions(tmp_path, monkeypatch) -> None:
    # Given: 이동된 세션일로 윈도우 안에 STREAMER_ACTIVE 상태
    from src.core.calendar import SessionState
    from src.orchestration import daemon as daemon_mod

    _FakePopen.instances.clear()
    day = dt.date(2026, 9, 25)
    now = _at(day, 2, 0)
    runner = _runner(tmp_path, monkeypatch, now=now)
    runner._state.eod_attempted_for = day - dt.timedelta(days=1)
    monkeypatch.setattr(runner._gate, "view", lambda today, at: _business_view(day - dt.timedelta(days=1)))
    monkeypatch.setattr(daemon_mod.subprocess, "Popen", _FakePopen)

    # When / Then: 세션 상태에서는 절대 실행되지 않는다
    for state in (
        SessionState.STREAMER_ACTIVE,
        SessionState.FULL_ACTIVE,
        SessionState.AFTER_MARKET_ACTIVE,
    ):
        runner._state.prev_state = state
        runner._maybe_launch_nightly(now)
    assert _FakePopen.instances == []
    assert runner._children.nightly is None


def test_launch_failure_marks_window_done(tmp_path, monkeypatch, caplog) -> None:
    # Given: Popen 자체가 실패하는 환경
    import logging

    from src.orchestration import daemon as daemon_mod

    day = dt.date(2026, 9, 25)
    now = _at(day, 23, 45)
    runner = _runner(tmp_path, monkeypatch, now=now)
    runner._state.eod_attempted_for = day
    monkeypatch.setattr(runner._gate, "view", lambda today, at: _business_view(day))

    def _raise(cmd, *, env=None):
        raise OSError("fork down")

    monkeypatch.setattr(daemon_mod.subprocess, "Popen", _raise)

    # When / Then: CRITICAL 후 같은 윈도우에는 재시도하지 않는다
    with caplog.at_level(logging.CRITICAL):
        runner._maybe_launch_nightly(now)
    assert "stage=nightly_tick_tape status=FAIL exit_code=launch reason=" in caplog.text
    assert runner._children.nightly is None
    runner._maybe_launch_nightly(_at(day, 23, 50))
    assert runner._children.nightly is None


def test_step_reaps_and_hands_over_to_minute_bars(tmp_path, monkeypatch) -> None:
    # Given: exit 0으로 끝난 tick tape 자식을 물고 NIGHT_SLEEP 심야에 들어선 스텝
    from src.orchestration import daemon as daemon_mod
    from src.orchestration.daemon import NightlyJob

    _FakePopen.instances.clear()
    day = dt.date(2026, 9, 25)
    now = _at(day + dt.timedelta(days=1), 0, 10)
    runner = _runner(tmp_path, monkeypatch, now=now, minute_enabled=True)
    runner._state.eod_attempted_for = day
    runner._state.nightly_done[NightlyJob.TICK_TAPE] = day
    dead = _FakePopen(["backfill-tick-tape"], exit_code=0)
    runner._children.nightly = dead
    runner._children.nightly_job = NightlyJob.TICK_TAPE
    runner._children.nightly_started_at = _at(day, 23, 45)
    monkeypatch.setattr(runner._gate, "view", lambda today, at: _business_view(day))
    monkeypatch.setattr(daemon_mod.subprocess, "Popen", _FakePopen)

    # When
    runner.step(now)

    # Then: 같은 스텝에서 회수 후 minute bars로 인계된다
    assert len(_FakePopen.instances) == 2
    assert runner._children.nightly_job is NightlyJob.MINUTE_BARS
    assert "backfill-minute-bars" in _FakePopen.instances[1].cmd


def test_unknown_calendar_never_bypasses_eod(tmp_path, monkeypatch) -> None:
    from src.orchestration import daemon
    from src.orchestration.trading_day_gate import TradingDayStatus, TradingDayView

    day = dt.date(2026, 9, 25)
    now = _at(day, 23, 45)
    runner = _runner(tmp_path, monkeypatch, now=now)
    monkeypatch.setattr(
        runner._gate, "view",
        lambda date, at: TradingDayView(date=date, status=TradingDayStatus.UNKNOWN, trading_day=None),
    )
    _FakePopen.instances.clear()
    monkeypatch.setattr(daemon.subprocess, "Popen", _FakePopen)
    runner._maybe_launch_nightly(now)
    assert _FakePopen.instances == []


def test_program_sync_cannot_overlap_nightly(tmp_path, monkeypatch) -> None:
    from src.core.config import TossProgramTradesSettings
    from src.orchestration import daemon

    day = dt.date(2026, 9, 25)
    now = _at(day, 23, 45)
    runner = _runner(tmp_path, monkeypatch, now=now)
    runner._program_sync_cfg = TossProgramTradesSettings(auto_backfill_enabled=True)
    runner.state.eod_attempted_for = day
    runner.children.nightly = _FakePopen(["backfill-tick-tape"])
    _FakePopen.instances.clear()
    monkeypatch.setattr(runner._gate, "view", lambda date, at: _business_view(date))
    monkeypatch.setattr(daemon.subprocess, "Popen", _FakePopen)
    runner._maybe_launch_program_sync(now)
    assert _FakePopen.instances == []
    assert runner.children.program_sync is None


def test_session_step_stops_nightly_before_session_work(tmp_path, monkeypatch) -> None:
    from src.core.calendar import SessionState

    day = dt.date(2026, 9, 25)
    now = _at(day, 9, 0)
    runner = _runner(tmp_path, monkeypatch, now=now)
    proc = _FakePopen(["backfill-tick-tape"])
    runner.children.nightly = proc
    monkeypatch.setattr(runner._gate, "view", lambda date, at: _business_view(date))

    def session_work(at, state, view, anchors):
        assert state is SessionState.FULL_ACTIVE
        assert proc.terminated
        assert runner.children.nightly is None
        return 10.0

    monkeypatch.setattr(runner, "_step_active_session", session_work)
    runner.step(now)


@pytest.mark.parametrize("shutdown", [False, True])
def test_exited_child_cleanup_never_signals(tmp_path, monkeypatch, shutdown) -> None:
    day = dt.date(2026, 9, 25)
    runner = _runner(tmp_path, monkeypatch, now=_at(day, 23, 45))
    proc = _FakePopen(["backfill-tick-tape"], exit_code=0)
    runner.children.nightly = proc
    if shutdown:
        runner.stop_children()
    else:
        runner._stop_nightly_child(reason="session")
    assert not proc.terminated
    assert runner.children.nightly is None


def test_enabled_idle_scheduler_does_not_sleep_past_window_start(tmp_path, monkeypatch) -> None:
    day = dt.date(2026, 9, 25)
    runner = _runner(tmp_path, monkeypatch, now=_at(day, 23, 29))
    assert runner.step(_at(day, 23, 29)) <= 60.0


def test_blocked_cycle_cannot_launch_after_window_end(tmp_path, monkeypatch) -> None:
    from src.orchestration import daemon

    day = dt.date(2026, 9, 25)
    runner = _runner(tmp_path, monkeypatch, now=_at(day, 6, 0))
    _FakePopen.instances.clear()
    monkeypatch.setattr(daemon.subprocess, "Popen", _FakePopen)
    runner._maybe_launch_nightly(_at(day, 5, 0))
    assert runner.children.nightly is None
    assert _FakePopen.instances == []


@pytest.mark.parametrize("slot", ["regular", "premarket", "aftermarket", "regular_kis"])
def test_live_session_child_blocks_nightly_in_sleep_state(tmp_path, monkeypatch, slot) -> None:
    from src.core.calendar import SessionState
    from src.orchestration import daemon

    day = dt.date(2026, 9, 25)
    now = _at(day, 23, 45)
    runner = _runner(tmp_path, monkeypatch, now=now)
    runner.state.prev_state = SessionState.NIGHT_SLEEP
    runner.state.eod_attempted_for = day
    supervisor = SimpleNamespace(is_running=lambda: True)
    if slot in ("aftermarket", "regular_kis"):
        getattr(runner.children, slot)["shard"] = supervisor
    else:
        setattr(runner.children, slot, supervisor)
    _FakePopen.instances.clear()
    monkeypatch.setattr(daemon.subprocess, "Popen", _FakePopen)
    monkeypatch.setattr(runner._gate, "view", lambda date, at: _business_view(date))
    runner._maybe_launch_nightly(now)
    assert runner.children.nightly is None
    assert _FakePopen.instances == []


def test_restart_after_eod_still_launches_when_digest_archived(tmp_path, monkeypatch) -> None:
    from src.orchestration import daemon as daemon_mod

    _FakePopen.instances.clear()
    day = dt.date(2026, 9, 25)
    now = _at(day, 23, 45)
    runner = _runner(tmp_path, monkeypatch, now=now)
    monkeypatch.setattr(runner._gate, "view", lambda today, at: _business_view(day))
    monkeypatch.setattr(daemon_mod.subprocess, "Popen", _FakePopen)
    assert runner._state.eod_attempted_for is None

    runner._maybe_launch_nightly(now)
    assert _FakePopen.instances == []

    digest = runner._paths.eod_digest_path(day)
    digest.parent.mkdir(parents=True, exist_ok=True)
    digest.write_text("subject\n\nbody\n", encoding="utf-8")
    runner._maybe_launch_nightly(now)

    assert len(_FakePopen.instances) == 1
    assert "backfill-tick-tape" in _FakePopen.instances[0].cmd


def test_digest_of_another_day_does_not_open_the_gate(tmp_path, monkeypatch) -> None:
    from src.orchestration import daemon as daemon_mod

    _FakePopen.instances.clear()
    day = dt.date(2026, 9, 25)
    now = _at(day, 23, 45)
    runner = _runner(tmp_path, monkeypatch, now=now)
    monkeypatch.setattr(runner._gate, "view", lambda today, at: _business_view(day))
    monkeypatch.setattr(daemon_mod.subprocess, "Popen", _FakePopen)
    other = runner._paths.eod_digest_path(day - dt.timedelta(days=1))
    other.parent.mkdir(parents=True, exist_ok=True)
    other.write_text("x", encoding="utf-8")

    runner._maybe_launch_nightly(now)

    assert _FakePopen.instances == []
