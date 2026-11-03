from __future__ import annotations

import datetime as dt
from typing import ClassVar
from zoneinfo import ZoneInfo

import pytest

import src.orchestration.daemon as daemon_mod
from src.core.config import CollectorSettings, IntradayAdmissionSettings, RegularKisSettings, resolve_collector_runtime
from src.core.errors import KrxAlphaError
from src.realtime.kis_sharding import KisDataCredential
from src.universe.intraday import DynamicEntry, DynamicPlan, write_dynamic_admissions

_DAY = dt.date(2026, 11, 3)


class _FakeSupervisor:
    def __init__(self, cmd, breaker=None) -> None:
        self.cmd = cmd
        self.last_exit_code = None
        self.started = 0

    def ensure_running(self) -> str:
        self.started += 1
        return "started"

    def stop(self, timeout_s: float) -> str:
        return "stopped"

    def is_running(self) -> bool:
        return True

    def request_stop(self) -> None:
        return None

    def wait_stopped(self, timeout_s: float) -> str:
        return "not_running"


def _runner(monkeypatch, tmp_path, *, intraday_enabled: bool, credential_slots=("1", "2", "3", "4")):
    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    monkeypatch.setattr(
        daemon_mod,
        "load_kis_data_credentials",
        lambda: (KisDataCredential("5", "app-5", "secret-5", "hts-5", "kid5"),),
    )
    regular = RegularKisSettings(
        enabled=True, credential_slots=credential_slots, pair_capacity_per_connection=41
    )
    intraday = IntradayAdmissionSettings(enabled=intraday_enabled)
    settings = CollectorSettings(data_root=tmp_path / "data")
    runtime = resolve_collector_runtime(collector=settings, regular_kis=regular, intraday=intraday)
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)
    return runner


def test_intraday_child_is_absent_from_session_specs_when_disabled(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path, intraday_enabled=False)

    keys = [spec.key for spec in runner._session_specs()]

    assert daemon_mod.StreamChildKey.INTRADAY_DISCOVERY not in keys


def test_intraday_child_is_a_session_spec_when_enabled(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path, intraday_enabled=True)

    spec = runner._stream_spec(daemon_mod.StreamChildKey.INTRADAY_DISCOVERY)

    assert spec.lifecycle is daemon_mod.ChildLifecycle.SESSION
    assert spec.cmd is not None
    assert spec.cmd(_DAY, runner._paths, None)[-2:] == ["--session-date", _DAY.isoformat()]


def test_dynamic_slot_shared_with_regular_slots_fails_at_plan_time(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path, intraday_enabled=True, credential_slots=("1", "2", "3", "5"))

    with pytest.raises(KrxAlphaError, match="must not be a regular_kis credential slot"):
        runner._ensure_regular_kis_plan(_DAY)


def test_dynamic_kis_child_waits_for_admission_and_free_lease(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path, intraday_enabled=True)
    runner._supervise_regular_kis_dynamic(_DAY)
    assert runner.children.regular_kis_dynamic is None

    write_dynamic_admissions(
        runner._paths.dynamic_admissions(_DAY),
        DynamicPlan(rev=1, entries=(DynamicEntry("005930", "kis:5", 1),)),
    )
    runner._supervise_regular_kis_dynamic(_DAY)

    sup = runner.children.regular_kis_dynamic
    assert sup is not None
    assert "collect-regular-kis-dynamic" in sup.cmd
    assert sup.started == 1

    runner._stop_regular_kis_dynamic()
    assert runner.children.regular_kis_dynamic is None


class _ScriptedSupervisor(_FakeSupervisor):
    results: ClassVar[list[str]] = []

    def ensure_running(self) -> str:
        self.started += 1
        return self.results.pop(0)


def test_discovery_window_runs_from_open_to_post_close_plus_one_minute() -> None:
    import types

    runtime = types.SimpleNamespace(intraday=IntradayAdmissionSettings(enabled=True))
    anchors = types.SimpleNamespace(regular_open=dt.time(9, 0))

    def at(hour: int, minute: int, second: int = 0) -> dt.datetime:
        return dt.datetime.combine(_DAY, dt.time(hour, minute, second), tzinfo=ZoneInfo("Asia/Seoul"))

    assert daemon_mod._intraday_discovery_active(runtime, at(8, 59, 59), anchors) is False  # type: ignore[arg-type]
    assert daemon_mod._intraday_discovery_active(runtime, at(9, 0), anchors) is True  # type: ignore[arg-type]
    assert daemon_mod._intraday_discovery_active(runtime, at(15, 31, 59), anchors) is True  # type: ignore[arg-type]
    assert daemon_mod._intraday_discovery_active(runtime, at(15, 32), anchors) is False  # type: ignore[arg-type]


def test_dynamic_child_waits_while_slot_lease_is_held(monkeypatch, tmp_path) -> None:
    import fcntl

    runner = _runner(monkeypatch, tmp_path, intraday_enabled=True)
    write_dynamic_admissions(
        runner._paths.dynamic_admissions(_DAY),
        DynamicPlan(rev=1, entries=(DynamicEntry("005930", "kis:5", 1),)),
    )
    lease_path = runner._paths.kis_ws_lease_dir / "kid5.lock"
    lease_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lease_path, "a+b") as holder:
        fcntl.flock(holder.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        runner._supervise_regular_kis_dynamic(_DAY)

    assert runner.children.regular_kis_dynamic is None


def test_dynamic_child_without_credential_fails_closed(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path, intraday_enabled=True)
    write_dynamic_admissions(
        runner._paths.dynamic_admissions(_DAY),
        DynamicPlan(rev=1, entries=(DynamicEntry("005930", "kis:5", 1),)),
    )
    monkeypatch.setattr(daemon_mod, "load_kis_data_credentials", lambda: ())

    with pytest.raises(KrxAlphaError, match="no KIS data credential"):
        runner._supervise_regular_kis_dynamic(_DAY)


def test_dynamic_child_restart_and_circuit_are_reported(monkeypatch, tmp_path, caplog) -> None:
    import logging

    caplog.set_level(logging.WARNING)
    runner = _runner(monkeypatch, tmp_path, intraday_enabled=True)
    sup = _ScriptedSupervisor(cmd=["x"])
    sup.results = ["restarted", "circuit_open"]
    runner.children.regular_kis_dynamic = sup  # type: ignore[assignment]

    runner._supervise_regular_kis_dynamic(_DAY)
    runner._supervise_regular_kis_dynamic(_DAY)

    assert "status=RESTARTED slot=5" in caplog.text
    assert "reason=circuit_open slot=5" in caplog.text


def test_stop_children_includes_dynamic_kis_shard(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path, intraday_enabled=True)
    stopped: list[str] = []

    class _Recording(_FakeSupervisor):
        def request_stop(self) -> None:
            stopped.append("dynamic")

    runner.children.regular_kis_dynamic = _Recording(cmd=["x"])  # type: ignore[assignment]

    runner.stop_children()

    assert stopped == ["dynamic"]
