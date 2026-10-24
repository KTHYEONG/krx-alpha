from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

import pytest

_KST = ZoneInfo("Asia/Seoul")


def test_repair_cmd_and_enabled(tmp_path, monkeypatch) -> None:
    import threading
    from types import SimpleNamespace

    from src.core.config import RepairSettings, TickTapeBackfillSettings
    from src.orchestration.daemon import DaemonRunner, NightlyJob, _nightly_job_cmd

    deadline = dt.datetime(2026, 9, 26, 5, 30, tzinfo=_KST)
    cmd = _nightly_job_cmd(NightlyJob.REPAIR, deadline=deadline)
    assert "repair-ticks" in cmd
    try:
        _nightly_job_cmd("bogus", deadline=deadline)  # type: ignore[arg-type]
    except ValueError:
        pass
    else:
        raise AssertionError("unknown job must raise")
    from src.core.config import CollectorSettings, resolve_collector_runtime

    settings = CollectorSettings(data_root=tmp_path / "data")
    runtime = resolve_collector_runtime(collector=settings)
    runner = DaemonRunner(
        runtime=runtime,
        shutdown=threading.Event(),
        now=lambda: dt.datetime(2026, 9, 25, 23, 45, tzinfo=_KST),
        sleep=lambda s: None,
        tick_tape_settings=TickTapeBackfillSettings(enabled=False),
        minute_backfill_settings=SimpleNamespace(enabled=False),
        repair_settings=RepairSettings(job_enabled=True),
    )
    assert runner._nightly_job_enabled(NightlyJob.REPAIR) is True
    assert runner._nightly_job_enabled(NightlyJob.TICK_TAPE) is False


@pytest.mark.parametrize(("tick", "repair", "expected"), [
    (True, True, ["backfill-tick-tape", "repair-ticks", "backfill-minute-bars"]),
    (False, True, ["repair-ticks", "backfill-minute-bars"]),
    (True, False, ["backfill-tick-tape", "backfill-minute-bars"]),
])
def test_launch_order_and_skip_finishes_window(tmp_path, monkeypatch, tick, repair, expected) -> None:
    from types import SimpleNamespace

    from src.core.config import CollectorSettings, RepairSettings, TickTapeBackfillSettings, resolve_collector_runtime
    from src.orchestration import daemon
    from src.orchestration.trading_day_gate import TradingDayStatus, TradingDayView

    now = dt.datetime(2026, 10, 24, 23, 45, tzinfo=_KST)
    runtime = resolve_collector_runtime(collector=CollectorSettings(data_root=tmp_path),
                                        repair=RepairSettings(job_enabled=repair))
    runner = daemon.DaemonRunner(
        runtime=runtime, shutdown=None, now=lambda: now, sleep=lambda s: None,
        tick_tape_settings=TickTapeBackfillSettings(enabled=tick),
        minute_backfill_settings=SimpleNamespace(enabled=True),
    )
    monkeypatch.setattr(runner._gate, "view", lambda day, at: TradingDayView(day, TradingDayStatus.HOLIDAY, None))
    launched = []

    class Process:
        code = None

        def __init__(self, cmd, **kwargs):
            self.cmd = cmd
            launched.append(self)

        def poll(self):
            return self.code

    monkeypatch.setattr(daemon.subprocess, "Popen", Process)
    for command in expected:
        runner._maybe_launch_nightly(now)
        process = launched[-1]
        assert process.cmd[3] == command
        assert process.cmd[-1] == "2026-10-25T05:30:00+09:00"
        assert len(launched) == expected.index(command) + 1
        runner._maybe_launch_nightly(now)
        assert launched[-1] is process
        process.code = 2
        runner._poll_nightly(now)
    runner._maybe_launch_nightly(now)
    assert len(launched) == len(expected)
