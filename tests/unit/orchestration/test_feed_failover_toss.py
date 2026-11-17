"""Toss secondary routes: unified venue admission, capacity units and daemon child specs."""

from __future__ import annotations

import datetime as dt

from src.core.config import CollectorSettings, FeedControlSettings, RegularKisSettings
from src.core.paths import DataPaths
from src.orchestration import daemon as daemon_mod
from src.orchestration import feed_failover as ff
from src.orchestration.feed_control.model import GroupState
from src.orchestration.feed_failover import PrimaryView
from src.universe.intraday import read_dynamic_admissions
from tests.unit.orchestration.test_feed_failover import (
    _MIXED,
    ALIVE,
    ANCHORS,
    DAY,
    KST,
    LS_SYMBOLS,
    NXT_PLAN,
    LS_PLAN,
    T0,
    _Clock,
    _settings,
    _state,
    _Stops,
    _supervisor,
    _tick,
    _write_health,
    _write_nxt_pool,
    paths,  # noqa: F401 - fixture
)

_ALIVE = PrimaryView(running=True, circuit_open=False, auth_rejected=False)


def test_toss_routes_cover_both_venues_without_a_day_lock() -> None:
    routes = ff._secondary_routes(RegularKisSettings(), CollectorSettings(), FeedControlSettings())
    for feed in ("toss:0", "toss:1"):
        assert routes[feed].vendor == "toss"
        assert routes[feed].venues == frozenset({"krx", "nxt"})
        assert routes[feed].unified_venue
    assert not routes["kis:5"].unified_venue
    assert not routes["db:0"].unified_venue


def test_toss_capacity_counts_symbols_not_book_streams() -> None:
    default = ff._secondary_routes(RegularKisSettings(), CollectorSettings(), FeedControlSettings())
    assert default["toss:0"].pair_capacity == 100 * 2  # 100 trade-only symbols, counted at 2 controller pairs each
    hot = FeedControlSettings(watch_groups=("ls:0",), secondary_feeds=("toss:0",), hot_symbols=(("toss:0", 10),))
    assert ff._secondary_routes(RegularKisSettings(), CollectorSettings(), hot)["toss:0"].pair_capacity == 90 * 2
    with_book = CollectorSettings(toss_streams=("H0STCNT0", "H0STASP0"))
    assert ff._secondary_routes(RegularKisSettings(), with_book, FeedControlSettings())["toss:0"].pair_capacity == 50 * 2


def test_toss_failover_admits_ls_symbols_with_full_state(paths) -> None:  # noqa: F811
    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(secondary_feeds=("toss:0",)), clock, _Stops())
    primaries = {**ALIVE, "toss:0": _ALIVE}
    wall = T0
    while wall <= T0 + 40:
        _write_health(paths, "toss:0", wall, last_frame=wall)
        _tick(sup, paths, clock, wall, ls_frame=wall - 40, primaries=primaries)
        wall += 10
    plan = read_dynamic_admissions(paths.dynamic_admissions(DAY))
    assert plan.symbols_for("toss:0") == LS_SYMBOLS
    assert _state(paths).groups["ls:0"].state is GroupState.SECONDARY_ACTIVE


def test_toss_serves_both_venues_in_one_tick_where_slot5_would_lock(paths) -> None:  # noqa: F811
    _write_nxt_pool(paths, ("005930",))
    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(watch_groups=_MIXED, secondary_feeds=("toss:0",)), clock, _Stops())
    wall = T0
    while wall <= T0 + 60:
        clock.t = wall
        _write_health(paths, "ls:0", wall, last_frame=wall - 40)
        _write_health(paths, "kis-nxt:0", wall, last_frame=wall - 60)
        _write_health(paths, "toss:0", wall, last_frame=wall)
        sup.tick(
            now=dt.datetime.fromtimestamp(wall, KST),
            today=DAY,
            anchors=ANCHORS,
            ls_plan=LS_PLAN,
            kis_plan=(),
            nxt_plan=NXT_PLAN,
            primaries={"ls:0": ALIVE["ls:0"], "kis-nxt:0": _ALIVE, "toss:0": _ALIVE},
        )
        wall += 10
    plan = read_dynamic_admissions(paths.dynamic_admissions(DAY))
    assert {entry.venue for entry in plan.entries if entry.feed == "toss:0"} == {"krx", "nxt"}
    assert _state(paths).groups["kis-nxt:0"].state is GroupState.SECONDARY_ACTIVE


def test_toss_child_specs_follow_secondary_feeds_and_build_toss_commands(tmp_path) -> None:
    paths_ = DataPaths(tmp_path / "data")
    specs = {spec.key: spec for spec in daemon_mod.DaemonRunner._STREAM_SPECS}
    from src.core.config import resolve_collector_runtime

    def enabled(secondary: tuple[str, ...]) -> tuple[bool, bool]:
        runtime = resolve_collector_runtime(
            collector=CollectorSettings(data_root=tmp_path / "data"),
            feed_control=FeedControlSettings(watch_groups=("ls:0",), secondary_feeds=secondary) if secondary else None,
        )
        return (
            specs[daemon_mod.StreamChildKey.TOSS_SECONDARY_A].enabled(runtime),
            specs[daemon_mod.StreamChildKey.TOSS_SECONDARY_B].enabled(runtime),
        )

    assert enabled(()) == (False, False)
    assert enabled(("toss:0",)) == (True, False)
    assert enabled(("toss:1",)) == (False, True)
    assert enabled(("db:0", "toss:0", "toss:1")) == (True, True)

    day = dt.date(2026, 11, 17)
    cmd = daemon_mod._toss_cmd(day, paths_, session_index=1, hot_symbols=5)
    assert "collect-stream-toss" in cmd
    assert cmd[cmd.index("--toss-session-index") + 1] == "1"
    assert cmd[cmd.index("--hot-symbols") + 1] == "5"
    assert cmd[cmd.index("--manifest-path") + 1] == str(paths_.toss_manifest_path(day, 1))
    spec = specs[daemon_mod.StreamChildKey.TOSS_SECONDARY_B]
    assert spec.role is daemon_mod.StreamRole.CROSSCHECK
    assert spec.journal_vendor(CollectorSettings()) == "toss"  # type: ignore[misc]
    assert spec.requires_primary


def test_session_child_cmd_carries_the_toss_hot_count(tmp_path, monkeypatch) -> None:
    from src.core.config import resolve_collector_runtime

    monkeypatch.chdir(tmp_path)
    runtime = resolve_collector_runtime(
        collector=CollectorSettings(data_root=tmp_path / "data"),
        feed_control=FeedControlSettings(
            watch_groups=("ls:0",), secondary_feeds=("toss:0", "toss:1"), hot_symbols=(("toss:1", 7),)
        ),
    )
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)
    specs = {spec.key: spec for spec in daemon_mod.DaemonRunner._STREAM_SPECS}
    day = dt.date(2026, 11, 17)
    first = runner._session_child_cmd(specs[daemon_mod.StreamChildKey.TOSS_SECONDARY_A], day, None)
    second = runner._session_child_cmd(specs[daemon_mod.StreamChildKey.TOSS_SECONDARY_B], day, None)
    assert first[first.index("--hot-symbols") + 1] == "0"
    assert second[second.index("--hot-symbols") + 1] == "7"
    assert second[second.index("--toss-session-index") + 1] == "1"


def test_toss_child_handles_round_trip_through_children_properties() -> None:
    children = daemon_mod.DaemonChildren()
    sentinel = object()
    children.toss_secondary_a = sentinel  # type: ignore[assignment]
    children.toss_secondary_b = sentinel  # type: ignore[assignment]
    assert children.get(daemon_mod.StreamChildKey.TOSS_SECONDARY_A) is sentinel
    assert children.toss_secondary_b is sentinel
    children.toss_secondary_a = None
    children.toss_secondary_b = None
    assert children.toss_secondary_a is None
