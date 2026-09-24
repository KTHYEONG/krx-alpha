"""Deploy session gate invariant guards."""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

_KST = ZoneInfo("Asia/Seoul")


def _kst(hour: int, minute: int = 0, second: int = 0, *, day: int = 23) -> dt.datetime:
    return dt.datetime(2026, 9, day, hour, minute, second, tzinfo=_KST)


def test_deploy_decision_defers_during_session() -> None:
    from src.orchestration.deploy_gate import deploy_decision

    decision = deploy_decision(_kst(10, 0))

    assert decision.action == "defer"
    assert decision.reason == "session_busy"


def test_deploy_decision_lead_boundary_is_inclusive() -> None:
    from src.orchestration.deploy_gate import deploy_decision

    assert deploy_decision(_kst(8, 10, 0)).action == "defer"
    assert deploy_decision(_kst(8, 9, 59)).action == "proceed"


def test_deploy_decision_deferred_time_boundary_is_exclusive() -> None:
    from src.orchestration.deploy_gate import deploy_decision

    assert deploy_decision(_kst(21, 59, 59)).action == "defer"
    assert deploy_decision(_kst(22, 0, 0)).action == "proceed"


def test_deploy_decision_proceeds_on_weekend() -> None:
    from src.orchestration.deploy_gate import deploy_decision

    saturday = dt.datetime(2026, 9, 26, 11, 0, 0, tzinfo=_KST)
    assert saturday.weekday() == 5

    decision = deploy_decision(saturday)

    assert decision.action == "proceed"
    assert decision.reason == "weekend"


def test_deploy_decision_converts_utc_input_to_kst() -> None:
    from src.orchestration.deploy_gate import deploy_decision

    utc = dt.datetime(2026, 9, 23, 1, 0, 0, tzinfo=dt.UTC)

    assert deploy_decision(utc).action == "defer"


def test_deploy_decision_rejects_naive_datetime() -> None:
    import pytest

    from src.orchestration.deploy_gate import deploy_decision

    with pytest.raises(ValueError, match="timezone-aware"):
        deploy_decision(dt.datetime(2026, 9, 23, 10, 0, 0))


def test_deploy_decision_honors_custom_schedule() -> None:
    from src.core.calendar import SessionSchedule
    from src.orchestration.deploy_gate import deploy_decision

    schedule = SessionSchedule(
        streamer_start=dt.time(9, 0),
        scanner_start=dt.time(9, 30),
        market_close=dt.time(15, 40),
        eod_done=dt.time(16, 0),
    )

    assert deploy_decision(_kst(8, 45), schedule).action == "proceed"


def test_deploy_gate_runs_under_bare_python() -> None:
    import shutil
    import subprocess

    exe = shutil.which("python3") or "python3"
    saturday = subprocess.run(  # noqa: S603 - hermetic bare-interpreter check
        [exe, "-S", "-m", "src.orchestration.deploy_gate", "--now", "2026-09-26T10:00:00+09:00"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert saturday.returncode == 0
    assert "action=proceed" in saturday.stdout

    session = subprocess.run(  # noqa: S603 - hermetic bare-interpreter check
        [exe, "-S", "-m", "src.orchestration.deploy_gate", "--now", "2026-09-23T10:00:00+09:00"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert session.returncode == 10
    assert "action=defer" in session.stdout


def test_deploy_gate_main_reports_proceed_and_defer(capsys) -> None:
    from src.orchestration.deploy_gate import EXIT_DEFER, EXIT_PROCEED, main

    assert main(["--now", "2026-09-26T10:00:00+09:00"]) == EXIT_PROCEED
    assert "action=proceed" in capsys.readouterr().out

    assert main(["--now", "2026-09-23T10:00:00+09:00"]) == EXIT_DEFER
    assert "action=defer" in capsys.readouterr().out


def test_deploy_gate_main_accepts_zulu_and_current_time(capsys) -> None:
    from src.orchestration.deploy_gate import EXIT_DEFER, main

    assert main(["--now", "2026-09-23T01:00:00Z"]) == EXIT_DEFER
    assert "reason=session_busy" in capsys.readouterr().out

    assert main([]) in (0, 10)
