"""LS-NXT secondary: NXT-only route, secondary order preference and on-demand daemon child."""

from __future__ import annotations

import dataclasses
import datetime as dt
import logging

import pytest

from src.core.config import CollectorSettings, FeedControlSettings, RegularKisSettings, resolve_collector_runtime
from src.core.paths import DataPaths
from src.orchestration import daemon as daemon_mod
from src.orchestration import feed_failover as ff
from src.orchestration.feed_control.model import GroupState
from src.orchestration.feed_failover import PrimaryView
from src.universe.intraday import DynamicEntry, DynamicPlan, read_dynamic_admissions, write_dynamic_admissions
from tests.unit.orchestration.test_feed_failover import (
    ANCHORS,
    DAY,
    KST,
    NXT_PLAN,
    T0,
    _Clock,
    _settings,
    _state,
    _Stops,
    _supervisor,
    _write_health,
    _write_nxt_pool,
    paths,  # noqa: F401 - fixture
)

_ALIVE = PrimaryView(running=True, circuit_open=False, auth_rejected=False)
_SPECS = {spec.key: spec for spec in daemon_mod.DaemonRunner._STREAM_SPECS}
_SPEC = _SPECS[daemon_mod.StreamChildKey.LS_NXT_SECONDARY]


def test_ls_nxt_route_is_nxt_only_with_the_session_pair_cap() -> None:
    route = ff._secondary_routes(RegularKisSettings(), CollectorSettings(), FeedControlSettings())["ls-nxt:0"]
    assert (route.vendor, route.venues, route.unified_venue) == ("ls", frozenset({"nxt"}), False)
    assert route.pair_capacity == CollectorSettings().ls_max_subscriptions_per_session == 200


def _run_nxt(paths, secondary: tuple[str, ...]) -> None:  # noqa: F811
    _write_nxt_pool(paths, ("005930",))
    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(watch_groups=("kis-nxt:0",), secondary_feeds=secondary), clock, _Stops())
    wall = T0
    while wall <= T0 + 60:
        clock.t = wall
        _write_health(paths, "kis-nxt:0", wall, last_frame=wall - 60)
        for feed in secondary:
            _write_health(paths, feed, wall, last_frame=wall)
        sup.tick(
            now=dt.datetime.fromtimestamp(wall, KST),
            today=DAY,
            anchors=ANCHORS,
            ls_plan=(),
            kis_plan=(),
            nxt_plan=NXT_PLAN,
            primaries={"kis-nxt:0": _ALIVE, **dict.fromkeys(secondary, _ALIVE)},
        )
        wall += 10


def test_ls_nxt_failover_admits_the_nxt_group(paths) -> None:  # noqa: F811
    _run_nxt(paths, ("ls-nxt:0",))
    plan = read_dynamic_admissions(paths.dynamic_admissions(DAY))
    assert [(e.symbol, e.feed, e.venue) for e in plan.entries] == [("005930", "ls-nxt:0", "nxt")]
    assert _state(paths).groups["kis-nxt:0"].state is GroupState.SECONDARY_ACTIVE


@pytest.mark.parametrize(("order", "chosen"), [(("ls-nxt:0", "kis:5"), "ls-nxt:0"), (("kis:5", "ls-nxt:0"), "kis:5")])
def test_secondary_order_decides_between_ls_nxt_and_slot5(paths, order, chosen) -> None:  # noqa: F811
    _run_nxt(paths, order)
    plan = read_dynamic_admissions(paths.dynamic_admissions(DAY))
    assert {e.feed for e in plan.entries} == {chosen}


def test_ls_nxt_is_never_a_krx_candidate(paths) -> None:  # noqa: F811
    sup = _supervisor(paths, _settings(secondary_feeds=("ls-nxt:0",)), _Clock(T0), _Stops())
    assert sup._spec("ls:0", "ls:0", "krx", ("005930",), ("H0STCNT0",), {}).secondaries == ()


def _runtime(tmp_path, secondary: tuple[str, ...]):
    return resolve_collector_runtime(
        collector=CollectorSettings(data_root=tmp_path / "data"),
        feed_control=FeedControlSettings(watch_groups=("kis-nxt:0",), secondary_feeds=secondary) if secondary else None,
    )


def test_ls_nxt_child_is_enabled_only_by_secondary_feeds(tmp_path) -> None:
    assert not _SPEC.enabled(_runtime(tmp_path, ()))
    assert not _SPEC.enabled(_runtime(tmp_path, ("kis:5",)))
    assert _SPEC.enabled(_runtime(tmp_path, ("ls-nxt:0",)))
    assert _SPEC.role is daemon_mod.StreamRole.CROSSCHECK
    assert _SPEC.requires_primary
    assert _SPEC.journal_vendor(CollectorSettings()) == "ls"  # type: ignore[misc]


def test_ls_nxt_child_starts_only_with_admissions(tmp_path) -> None:
    paths_ = DataPaths(tmp_path / "data")
    day = dt.date(2026, 11, 17)
    assert _SPEC.demand is not None
    assert not _SPEC.demand(paths_, day)
    write_dynamic_admissions(
        paths_.dynamic_admissions(day), DynamicPlan(rev=1, entries=(DynamicEntry("005930", "kis:5", 1, "nxt"),))
    )
    assert not _SPEC.demand(paths_, day)
    write_dynamic_admissions(
        paths_.dynamic_admissions(day),
        DynamicPlan(rev=2, entries=(DynamicEntry("005930", "kis:5", 1, "nxt"), DynamicEntry("000660", "ls-nxt:0", 2, "nxt"))),
    )
    assert _SPEC.demand(paths_, day)
    paths_.dynamic_admissions(day).write_text("{broken", encoding="utf-8")
    assert not _SPEC.demand(paths_, day)


def test_ls_nxt_follows_krx_shifted_open(tmp_path) -> None:
    runtime = _runtime(tmp_path, ("ls-nxt:0",))
    shifted = dataclasses.replace(
        ANCHORS, regular_open=dt.time(10, 0), closing_auction_start=dt.time(15, 20), regular_close=dt.time(16, 30)
    )
    assert _SPEC.active(runtime, dt.datetime(2026, 11, 16, 10, 15, tzinfo=KST), shifted)
    assert not _SPEC.active(runtime, dt.datetime(2026, 11, 16, 9, 30, tzinfo=KST), shifted)
    assert not _SPEC.active(runtime, dt.datetime(2026, 11, 16, 8, 0, tzinfo=KST), ANCHORS)


def test_ls_nxt_command_targets_the_nxt_plan_and_manifest(tmp_path) -> None:
    paths_ = DataPaths(tmp_path / "data")
    day = dt.date(2026, 11, 17)
    cmd = daemon_mod._ls_nxt_cmd(day, paths_)
    assert "collect-stream-ls-nxt" in cmd
    assert cmd[cmd.index("--manifest-path") + 1] == str(paths_.ls_nxt_manifest_path(day))
    assert cmd[cmd.index("--dynamic-plan-path") + 1] == str(paths_.dynamic_admissions(day))
    assert cmd[cmd.index("--candidates-path") + 1] == str(paths_.regular_nxt_candidates(day))


def test_crosscheck_on_demand_spec_rejects_non_crosscheck_roles() -> None:
    kwargs = {
        "key": daemon_mod.StreamChildKey.SNAPSHOT,
        "role": daemon_mod.StreamRole.SNAPSHOT,
        "lifecycle": daemon_mod.ChildLifecycle.SESSION,
        "enabled": lambda runtime: True,
        "cmd": lambda today, p, reason: ["cmd"],
        "active": lambda runtime, now, anchors: True,
        "logs": daemon_mod.SupervisionLogs(restarted="r", circuit_open="c", circuit_level=logging.WARNING),
        "demand": lambda p, day: True,
    }
    with pytest.raises(ValueError, match="on-demand"):
        daemon_mod.StreamChildSpec(**kwargs)


def test_runner_installs_the_ls_nxt_child_only_when_the_plan_demands_it(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    runtime = _runtime(tmp_path, ("ls-nxt:0",))
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)
    installed: list[daemon_mod.StreamChildKey] = []
    monkeypatch.setattr(runner, "_install_stream_child", lambda key, cmd, **kw: installed.append(key))
    now = dt.datetime(2026, 11, 16, 10, 0, tzinfo=KST)
    runner.children.regular = object()  # type: ignore[assignment]
    runner._state.orchestrated_for = DAY
    runner._start_on_demand_child(_SPEC, now, ANCHORS)
    assert installed == []
    write_dynamic_admissions(
        runner._paths.dynamic_admissions(DAY), DynamicPlan(rev=1, entries=(DynamicEntry("005930", "ls-nxt:0", 1, "nxt"),))
    )
    runner._start_on_demand_child(_SPEC, dt.datetime(2026, 11, 16, 16, 0, tzinfo=KST), ANCHORS)
    assert installed == []  # outside the NXT window
    runner._start_on_demand_child(_SPEC, now, ANCHORS)
    assert installed == [daemon_mod.StreamChildKey.LS_NXT_SECONDARY]


def test_runner_loops_skip_on_demand_install_at_orchestration_and_start_it_in_supervision(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    runtime = _runtime(tmp_path, ("ls-nxt:0",))
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)
    installed: list[daemon_mod.StreamChildKey] = []
    supervised: list[daemon_mod.StreamChildKey] = []
    monkeypatch.setattr(runner, "_ensure_ls_shard_plan", lambda today: None)
    monkeypatch.setattr(runner, "_install_stream_child", lambda key, cmd, **kw: installed.append(key))
    monkeypatch.setattr(runner, "_supervise_stream_child", lambda key, now, anchors: supervised.append(key))
    runner._start_ready_children(DAY)
    assert daemon_mod.StreamChildKey.LS_NXT_SECONDARY not in installed
    assert daemon_mod.StreamChildKey.REGULAR in installed
    runner.children.regular = object()  # type: ignore[assignment]
    runner.children.set(daemon_mod.StreamChildKey.LS_NXT_SECONDARY, object())  # type: ignore[arg-type]
    installed.clear()
    runner._supervise_session_children(dt.datetime(2026, 11, 16, 10, 0, tzinfo=KST), ANCHORS)
    assert installed == []  # already running: no second install
    assert daemon_mod.StreamChildKey.LS_NXT_SECONDARY in supervised
    runner.children.set(daemon_mod.StreamChildKey.LS_NXT_SECONDARY, None)
    runner._state.orchestrated_for = DAY
    write_dynamic_admissions(
        runner._paths.dynamic_admissions(DAY), DynamicPlan(rev=1, entries=(DynamicEntry("005930", "ls-nxt:0", 1, "nxt"),))
    )
    runner._supervise_session_children(dt.datetime(2026, 11, 16, 10, 0, tzinfo=KST), ANCHORS)
    assert installed == [daemon_mod.StreamChildKey.LS_NXT_SECONDARY]
