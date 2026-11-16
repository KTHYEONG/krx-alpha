from __future__ import annotations

import datetime as dt
from typing import ClassVar
from zoneinfo import ZoneInfo

import pytest

import src.orchestration.daemon as daemon_mod
from src.core.config import CollectorSettings, IntradayAdmissionSettings, RegularKisSettings, resolve_collector_runtime
from src.core.errors import KrxAlphaError
from src.realtime.contracts import MarketVenue
from src.realtime.kis_sharding import AftermarketShard, KisDataCredential
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


def _failover_runner(monkeypatch, tmp_path, *, secondary: bool):
    from src.core.config import FeedControlSettings

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    monkeypatch.setattr(
        daemon_mod,
        "load_kis_data_credentials",
        lambda: (KisDataCredential("5", "app-5", "secret-5", "hts-5", "kid5"),),
    )
    regular = RegularKisSettings(enabled=True, credential_slots=("1", "2"), pair_capacity_per_connection=41)
    intraday = IntradayAdmissionSettings(enabled=False, kis_dynamic_slots=("5",))
    feed_control = FeedControlSettings(
        watch_groups=("ls:0",), secondary_feeds=("kis:5",) if secondary else ()
    )
    settings = CollectorSettings(data_root=tmp_path / "data")
    runtime = resolve_collector_runtime(
        collector=settings, regular_kis=regular, intraday=intraday, feed_control=feed_control
    )
    return daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)


def test_failover_secondary_starts_dynamic_child_with_intraday_off(monkeypatch, tmp_path) -> None:
    runner = _failover_runner(monkeypatch, tmp_path, secondary=True)
    write_dynamic_admissions(
        runner._paths.dynamic_admissions(_DAY),
        DynamicPlan(rev=1, entries=(DynamicEntry("005930", "kis:5", 1),)),
    )
    runner._supervise_regular_kis_dynamic(_DAY)
    assert set(runner.children.regular_kis_dynamic) == {"5"}
    assert "collect-regular-kis-dynamic" in runner.children.regular_kis_dynamic["5"].cmd
    runner._stop_regular_kis_dynamic()


def test_dynamic_child_stays_absent_by_default_with_intraday_off(monkeypatch, tmp_path) -> None:
    runner = _failover_runner(monkeypatch, tmp_path, secondary=False)
    write_dynamic_admissions(
        runner._paths.dynamic_admissions(_DAY),
        DynamicPlan(rev=1, entries=(DynamicEntry("005930", "kis:5", 1),)),
    )
    runner._supervise_regular_kis_dynamic(_DAY)
    assert runner.children.regular_kis_dynamic == {}


def test_failover_primary_views_and_secondary_stop(monkeypatch, tmp_path) -> None:
    runner = _failover_runner(monkeypatch, tmp_path, secondary=True)
    runner._state.regular_kis_plan = (
        AftermarketShard(MarketVenue.KRX, 0, ("005930",), ("H0STCNT0", "H0STASP0"), "1", "kid1"),
    )
    runner._state.regular_nxt_plan = (
        AftermarketShard(MarketVenue.NXT, 0, ("005930",), ("H0NXCNT0", "H0NXASP0"), "1", "kid1"),
    )
    runner.children.regular_kis["krx-00"] = _FakeSupervisor(cmd=["x"])
    runner.children.regular_nxt["nxt-00"] = _FakeSupervisor(cmd=["x"])
    runner.children.regular_kis_dynamic["5"] = _FakeSupervisor(cmd=["collect-regular-kis-dynamic"])

    views = runner._primary_views()
    assert set(views) == {"kis-krx:0", "kis-nxt:0", "kis:5"}
    assert views["kis:5"].running is True

    runner._stop_feed_secondary("kis:5")
    assert "5" not in runner.children.regular_kis_dynamic

    runner._run_feed_failover(dt.datetime(2026, 11, 3, 9, 10, tzinfo=ZoneInfo("Asia/Seoul")), _DAY, _anchors())


def _anchors():
    from src.core.session_anchors import standard_session_anchors

    return standard_session_anchors(_DAY)


def _nxt_dynamic_runner(
    monkeypatch, tmp_path, *, nxt_slots=("3", "4"), regular_slots=("1", "2"), dynamic_slots=("5",)
):
    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    monkeypatch.setattr(
        daemon_mod,
        "load_kis_data_credentials",
        lambda: (KisDataCredential("5", "app-5", "secret-5", "hts-5", "kid5"),),
    )
    regular = RegularKisSettings(
        enabled=True,
        credential_slots=regular_slots,
        pair_capacity_per_connection=41,
        nxt_credential_slots=nxt_slots,
    )
    intraday = IntradayAdmissionSettings(enabled=True, kis_dynamic_slots=dynamic_slots)
    runtime = resolve_collector_runtime(
        collector=CollectorSettings(data_root=tmp_path / "data"), regular_kis=regular, intraday=intraday
    )
    return daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)


def _nxt_entry_plan(runner) -> None:
    write_dynamic_admissions(
        runner._paths.dynamic_admissions(_DAY),
        DynamicPlan(rev=1, entries=(DynamicEntry("005930", "kis:5", 1, venue="nxt"),)),
    )


_KST = ZoneInfo("Asia/Seoul")


def test_nxt_dynamic_child_builds_venue_cmd_and_starts_only_in_nxt_window(monkeypatch, tmp_path) -> None:
    from src.core.session_anchors import standard_session_anchors

    runner = _nxt_dynamic_runner(monkeypatch, tmp_path)
    _nxt_entry_plan(runner)
    anchors = standard_session_anchors(_DAY)
    before_open = dt.datetime(2026, 11, 3, 7, 0, tzinfo=_KST)
    in_window = dt.datetime(2026, 11, 3, 10, 0, tzinfo=_KST)

    runner._supervise_regular_kis_dynamic(_DAY, before_open, anchors)
    assert runner.children.regular_kis_dynamic == {}

    runner._supervise_regular_kis_dynamic(_DAY, in_window, anchors)
    cmd = runner.children.regular_kis_dynamic["5"].cmd
    assert cmd[cmd.index("--venue") + 1] == "nxt"
    assert cmd[cmd.index("--manifest-path") + 1].endswith("2026-11-03.nxt.shard-05.json")

    runner._supervise_regular_kis_dynamic(_DAY, dt.datetime(2026, 11, 3, 16, 0, tzinfo=_KST), anchors)
    assert runner.children.regular_kis_dynamic == {}


def test_nxt_dynamic_child_waits_while_slot_lease_is_held(monkeypatch, tmp_path) -> None:
    from src.core.session_anchors import standard_session_anchors

    runner = _nxt_dynamic_runner(monkeypatch, tmp_path)
    _nxt_entry_plan(runner)
    monkeypatch.setattr(daemon_mod.KisWebSocketLease, "is_free", lambda self: False)

    runner._supervise_regular_kis_dynamic(
        _DAY, dt.datetime(2026, 11, 3, 10, 0, tzinfo=_KST), standard_session_anchors(_DAY)
    )

    assert runner.children.regular_kis_dynamic == {}


def test_nxt_dynamic_child_without_now_is_never_started(monkeypatch, tmp_path) -> None:
    runner = _nxt_dynamic_runner(monkeypatch, tmp_path)
    _nxt_entry_plan(runner)

    runner._supervise_regular_kis_dynamic(_DAY)

    assert runner.children.regular_kis_dynamic == {}


def test_krx_dynamic_cmd_argv_is_unchanged(tmp_path) -> None:
    from src.core.paths import DataPaths
    from src.orchestration.regular_kis import regular_kis_dynamic_stream_cmd

    cmd = regular_kis_dynamic_stream_cmd(_DAY, DataPaths(tmp_path / "data"), slot="5", key_id="kid5")

    assert "--venue" not in cmd
    assert cmd[cmd.index("--manifest-path") + 1].endswith("2026-11-03.krx.shard-05.json")


def test_dynamic_nxt_manifest_path_does_not_collide_with_static_or_krx(tmp_path) -> None:
    from src.core.paths import DataPaths

    paths = DataPaths(tmp_path / "data")
    nxt_dynamic = paths.regular_kis_manifest_path(_DAY, 5, "nxt")
    assert nxt_dynamic not in {paths.regular_kis_manifest_path(_DAY, i, "nxt") for i in (0, 1)}
    assert nxt_dynamic != paths.regular_kis_manifest_path(_DAY, 5, "krx")


def test_dynamic_slot_colliding_with_nxt_static_index_fails_at_plan_time(monkeypatch, tmp_path) -> None:
    runner = _nxt_dynamic_runner(
        monkeypatch, tmp_path, nxt_slots=("3", "4", "5"), regular_slots=("2",), dynamic_slots=("1",)
    )

    with pytest.raises(KrxAlphaError, match=r"collide with regular_nxt shard indices 0\.\.2"):
        runner._ensure_regular_kis_plan(_DAY)
