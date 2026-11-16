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


def _runner(monkeypatch, tmp_path, *, intraday_enabled: bool, credential_slots=("1", "2"), dynamic_slots=("3", "4")):
    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    monkeypatch.setattr(
        daemon_mod,
        "load_kis_data_credentials",
        lambda: (
            KisDataCredential("3", "app-3", "secret-3", "hts-3", "kid3"),
            KisDataCredential("4", "app-4", "secret-4", "hts-4", "kid4"),
        ),
    )
    regular = RegularKisSettings(
        enabled=True, credential_slots=credential_slots, pair_capacity_per_connection=41
    )
    intraday = IntradayAdmissionSettings(
        enabled=intraday_enabled, kis_dynamic_slots=dynamic_slots
    )
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


def test_dynamic_slots_overlapping_regular_fail_at_plan_time(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path, intraday_enabled=True, credential_slots=("1", "2", "3"))

    with pytest.raises(KrxAlphaError, match="must not be a regular_kis credential slot"):
        runner._ensure_regular_kis_plan(_DAY)


def test_dynamic_kis_child_waits_for_admission_and_free_lease(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path, intraday_enabled=True)
    runner._supervise_regular_kis_dynamic(_DAY)
    assert runner.children.regular_kis_dynamic == {}

    write_dynamic_admissions(
        runner._paths.dynamic_admissions(_DAY),
        DynamicPlan(rev=1, entries=(DynamicEntry("005930", "kis:3", 1),)),
    )
    runner._supervise_regular_kis_dynamic(_DAY)

    sup = runner.children.regular_kis_dynamic["3"]
    assert "collect-regular-kis-dynamic" in sup.cmd
    assert sup.started == 1
    assert set(runner.children.regular_kis_dynamic) == {"3"}

    runner._stop_regular_kis_dynamic()
    assert runner.children.regular_kis_dynamic == {}


def test_dynamic_child_per_slot_waits_for_its_own_admission(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path, intraday_enabled=True)
    write_dynamic_admissions(
        runner._paths.dynamic_admissions(_DAY),
        DynamicPlan(rev=1, entries=(DynamicEntry("005930", "kis:3", 1),)),
    )

    runner._supervise_regular_kis_dynamic(_DAY)

    assert list(runner.children.regular_kis_dynamic) == ["3"]
    assert "collect-regular-kis-dynamic" in runner.children.regular_kis_dynamic["3"].cmd


def test_second_slot_starts_after_its_admission(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path, intraday_enabled=True)
    write_dynamic_admissions(
        runner._paths.dynamic_admissions(_DAY),
        DynamicPlan(rev=1, entries=(DynamicEntry("005930", "kis:3", 1),)),
    )
    runner._supervise_regular_kis_dynamic(_DAY)
    first = runner.children.regular_kis_dynamic["3"]

    write_dynamic_admissions(
        runner._paths.dynamic_admissions(_DAY),
        DynamicPlan(
            rev=2,
            entries=(DynamicEntry("005930", "kis:3", 1), DynamicEntry("000660", "kis:4", 2)),
        ),
    )
    runner._supervise_regular_kis_dynamic(_DAY)

    assert set(runner.children.regular_kis_dynamic) == {"3", "4"}
    assert runner.children.regular_kis_dynamic["3"] is first


def test_busy_lease_on_one_slot_does_not_block_other(monkeypatch, tmp_path) -> None:
    import fcntl

    runner = _runner(monkeypatch, tmp_path, intraday_enabled=True)
    write_dynamic_admissions(
        runner._paths.dynamic_admissions(_DAY),
        DynamicPlan(
            rev=1,
            entries=(DynamicEntry("005930", "kis:3", 1), DynamicEntry("000660", "kis:4", 2)),
        ),
    )
    lease_path = runner._paths.kis_ws_lease_dir / "kid3.lock"
    lease_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lease_path, "a+b") as holder:
        fcntl.flock(holder.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        runner._supervise_regular_kis_dynamic(_DAY)

    assert list(runner.children.regular_kis_dynamic) == ["4"]


def test_stop_clears_all_dynamic_supervisors(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path, intraday_enabled=True)
    stopped: list[str] = []

    class _Recording(_FakeSupervisor):
        def stop(self, timeout_s: float) -> str:
            stopped.append(self.cmd[-1])
            return "stopped"

    runner.children.regular_kis_dynamic = {
        "3": _Recording(cmd=["x", "3"]),  # type: ignore[dict-item]
        "4": _Recording(cmd=["x", "4"]),  # type: ignore[dict-item]
    }

    runner._stop_regular_kis_dynamic()

    assert runner.children.regular_kis_dynamic == {}
    assert sorted(stopped) == ["3", "4"]


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
        DynamicPlan(rev=1, entries=(DynamicEntry("005930", "kis:3", 1),)),
    )
    lease_path = runner._paths.kis_ws_lease_dir / "kid3.lock"
    lease_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lease_path, "a+b") as holder:
        fcntl.flock(holder.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        runner._supervise_regular_kis_dynamic(_DAY)

    assert runner.children.regular_kis_dynamic == {}


def test_dynamic_slot_without_credential_is_skipped_and_logged(monkeypatch, tmp_path, caplog) -> None:
    import logging

    caplog.set_level(logging.WARNING)
    runner = _runner(monkeypatch, tmp_path, intraday_enabled=True)
    write_dynamic_admissions(
        runner._paths.dynamic_admissions(_DAY),
        DynamicPlan(rev=1, entries=(DynamicEntry("005930", "kis:3", 1),)),
    )
    monkeypatch.setattr(daemon_mod, "load_kis_data_credentials", lambda: ())

    runner._supervise_regular_kis_dynamic(_DAY)

    assert runner.children.regular_kis_dynamic == {}
    assert "stage=regular_kis_dynamic status=SUPERVISE_FAIL slot=3 reason=no KIS data credential" in caplog.text


def test_failing_slot_does_not_block_the_other_slot(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path, intraday_enabled=True)
    write_dynamic_admissions(
        runner._paths.dynamic_admissions(_DAY),
        DynamicPlan(rev=1, entries=(DynamicEntry("005930", "kis:3", 1), DynamicEntry("000660", "kis:4", 1))),
    )
    monkeypatch.setattr(
        daemon_mod,
        "load_kis_data_credentials",
        lambda: (KisDataCredential("4", "app-4", "secret-4", "hts-4", "kid4"),),
    )

    runner._supervise_regular_kis_dynamic(_DAY)

    assert set(runner.children.regular_kis_dynamic) == {"4"}


def test_dynamic_slot_number_colliding_with_static_shard_index_fails_at_plan_time(monkeypatch, tmp_path) -> None:
    runner = _runner(
        monkeypatch, tmp_path, intraday_enabled=True, credential_slots=("1", "3", "4"), dynamic_slots=("2",)
    )

    with pytest.raises(KrxAlphaError, match=r"collide with regular_kis shard indices 0\.\.2"):
        runner._ensure_regular_kis_plan(_DAY)


def test_dynamic_child_restart_and_circuit_are_reported(monkeypatch, tmp_path, caplog) -> None:
    import logging

    caplog.set_level(logging.WARNING)
    runner = _runner(monkeypatch, tmp_path, intraday_enabled=True)
    sup = _ScriptedSupervisor(cmd=["x"])
    sup.results = ["restarted", "circuit_open"]
    write_dynamic_admissions(
        runner._paths.dynamic_admissions(_DAY),
        DynamicPlan(rev=1, entries=(DynamicEntry("005930", "kis:3", 1),)),
    )
    runner.children.regular_kis_dynamic = {"3": sup}  # type: ignore[assignment]

    runner._supervise_regular_kis_dynamic(_DAY)
    runner._supervise_regular_kis_dynamic(_DAY)

    assert "status=RESTARTED slot=3" in caplog.text
    assert "reason=circuit_open slot=3" in caplog.text
    from src.core.alerts import PageKind, page_kind

    assert {page_kind(r) for r in caplog.records if "reason=circuit_open" in r.getMessage()} == {
        PageKind.REGULAR_FEED_DOWN
    }


def test_stop_children_includes_dynamic_kis_shard(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path, intraday_enabled=True)
    stopped: list[str] = []

    class _Recording(_FakeSupervisor):
        def request_stop(self) -> None:
            stopped.append("dynamic")

    runner.children.regular_kis_dynamic = {"3": _Recording(cmd=["x"])}  # type: ignore[dict-item]

    runner.stop_children()

    assert stopped == ["dynamic"]
