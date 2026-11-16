"""Feed failover supervisor: inertness, observe/active admission, failback, recycle, and persistence invariants."""

from __future__ import annotations

import datetime as dt
import json
import logging
import pathlib
from zoneinfo import ZoneInfo

import pytest

from src.core.alerts import PAGE_ATTR
from src.core.config import (
    CollectorSettings,
    FeedControlSettings,
    IntradayAdmissionSettings,
    RegularKisSettings,
    resolve_collector_runtime,
)
from src.core.paths import DataPaths
from src.core.session_anchors import standard_session_anchors
from src.orchestration import feed_failover as ff
from src.orchestration.feed_control.model import ControllerState, GroupState
from src.orchestration.feed_failover import FeedFailoverSupervisor, PrimaryView
from src.realtime.contracts import MarketVenue
from src.realtime.kis_sharding import AftermarketShard
from src.realtime.ls_sharding import LsShard
from src.universe.intraday import DynamicEntry, DynamicPlan, read_dynamic_admissions, write_dynamic_admissions

KST = ZoneInfo("Asia/Seoul")
DAY = dt.date(2026, 11, 16)
DAY2 = dt.date(2026, 11, 17)
ANCHORS = standard_session_anchors(DAY)
T0 = dt.datetime(2026, 11, 16, 9, 10, tzinfo=KST).timestamp()
LS_SYMBOLS = ("005930", "000660", "035420")
LS_PLAN = (
    LsShard(
        shard_index=0,
        symbols=LS_SYMBOLS,
        streams=("H0STCNT0", "H0STASP0"),
        dynamic_reserve_symbols=0,
        window_standby=True,
    ),
)
NXT_PLAN = (
    AftermarketShard(MarketVenue.NXT, 0, ("005930",), ("H0NXCNT0", "H0NXASP0"), "1", "kid1"),
)
ALIVE = {
    "ls:0": PrimaryView(running=True, circuit_open=False, auth_rejected=False),
    "kis:5": PrimaryView(running=True, circuit_open=False, auth_rejected=False),
}


class _Clock:
    def __init__(self, t: float) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


class _Stops:
    def __init__(self) -> None:
        self.feeds: list[str] = []

    def __call__(self, feed_id: str) -> None:
        self.feeds.append(feed_id)


def _settings(**overrides) -> FeedControlSettings:
    base = {"watch_groups": ("ls:0",), "secondary_feeds": ("kis:5",)}
    base.update(overrides)
    return FeedControlSettings(**base)


def _supervisor(paths: DataPaths, settings: FeedControlSettings, clock: _Clock, stops: _Stops):
    return FeedFailoverSupervisor(
        paths=paths,
        settings=settings,
        collector=CollectorSettings(data_root=paths.root),
        regular_kis=RegularKisSettings(pair_capacity_per_connection=41),
        stop_secondary=stops,
        wall=clock,
    )


def _write_health(paths: DataPaths, feed: str, wall: float, *, last_frame: float | None) -> None:
    streams = ("H0STCNT0", "H0STASP0")
    payload = {
        "schema": "feed-health.v1",
        "feed_id": feed,
        "vendor": "kis" if feed.startswith("kis") else "ls",
        "venue": "krx",
        "session": "regular",
        "session_date": DAY.isoformat(),
        "pid": 1,
        "started_wall": wall - 100,
        "updated_wall": wall,
        "connected": True,
        "disconnects_total": 0,
        "last_disconnect_wall": None,
        "last_disconnect_reason": None,
        "frames_total": 1,
        "last_frame_wall": last_frame,
        "stream_last_frame_wall": dict.fromkeys(streams, last_frame) if last_frame is not None else {},
        "window_s": 60,
        "window_frames": 1,
        "lag_p50_s": None,
        "lag_p99_s": None,
        "lag_samples": 0,
        "planned_pairs": 0,
        "accepted_pairs": 0,
        "rejected_pairs": 0,
        "pairs_pending_since_wall": None,
    }
    path = paths.feed_health_path(feed)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _tick(
    sup: FeedFailoverSupervisor,
    paths: DataPaths,
    clock: _Clock,
    wall: float,
    *,
    ls_frame: float | None,
    kis_frame: float | None = None,
    primaries: dict[str, PrimaryView] | None = None,
    today: dt.date = DAY,
) -> None:
    clock.t = wall
    _write_health(paths, "ls:0", wall, last_frame=ls_frame)
    if kis_frame is not None:
        _write_health(paths, "kis:5", wall, last_frame=kis_frame)
    now = dt.datetime.fromtimestamp(wall, KST)
    sup.tick(
        now=now,
        today=today,
        anchors=ANCHORS if today == DAY else standard_session_anchors(today),
        ls_plan=LS_PLAN,
        kis_plan=(),
        nxt_plan=(),
        primaries=ALIVE if primaries is None else primaries,
    )


def _run_silent_ls(sup, paths, clock, start: float, seconds: float, *, kis_alive: bool = True) -> float:
    wall = start
    end = start + seconds
    while wall <= end:
        _tick(
            sup,
            paths,
            clock,
            wall,
            ls_frame=wall - 40,
            kis_frame=wall if kis_alive else None,
            primaries=ALIVE if kis_alive else {"ls:0": ALIVE["ls:0"]},
        )
        wall += 10
    return wall


def _run_healthy_ls(sup, paths, clock, start: float, seconds: float) -> float:
    wall = start
    end = start + seconds
    while wall <= end:
        _tick(sup, paths, clock, wall, ls_frame=wall, kis_frame=wall)
        wall += 10
    return wall


def _state(paths: DataPaths) -> ControllerState:
    return ControllerState.from_json(json.loads(paths.feed_controller_state_path().read_text(encoding="utf-8")))


def _journal(paths: DataPaths) -> list[dict]:
    path = paths.feed_control_log_path(DAY)
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


@pytest.fixture
def paths(tmp_path) -> DataPaths:
    return DataPaths(tmp_path / "data")


def test_inert_by_default(monkeypatch, paths, caplog) -> None:
    def _boom(self, **kwargs):
        raise AssertionError("tick body must not run with default settings")

    monkeypatch.setattr(FeedFailoverSupervisor, "_tick", _boom)
    sup = FeedFailoverSupervisor(
        paths=paths,
        settings=FeedControlSettings(),
        collector=CollectorSettings(data_root=paths.root),
        regular_kis=RegularKisSettings(),
        stop_secondary=_Stops(),
        wall=_Clock(T0),
    )
    with caplog.at_level(logging.DEBUG):
        sup.tick(
            now=dt.datetime.fromtimestamp(T0, KST),
            today=DAY,
            anchors=ANCHORS,
            ls_plan=LS_PLAN,
            kis_plan=(),
            nxt_plan=(),
            primaries=ALIVE,
        )
    assert not paths.root.exists()
    assert caplog.records == []


def test_observe_only_writes_nothing_but_state_and_journal(paths, caplog) -> None:
    clock = _Clock(T0)
    stops = _Stops()
    sup = _supervisor(paths, _settings(secondary_feeds=()), clock, stops)
    with caplog.at_level(logging.INFO):
        _run_silent_ls(sup, paths, clock, T0, 40)
    assert not paths.dynamic_admissions(DAY).exists()
    assert any("status=WOULD_ADMIT" in r.getMessage() for r in caplog.records)
    assert paths.feed_controller_state_path().exists()
    journal = _journal(paths)
    assert journal
    assert all(row["mode"] == "observe" for row in journal)
    assert stops.feeds == []


def test_ls_silence_admits_to_slot5(paths) -> None:
    clock = _Clock(T0)
    stops = _Stops()
    sup = _supervisor(paths, _settings(), clock, stops)
    _run_silent_ls(sup, paths, clock, T0, 40)
    plan = read_dynamic_admissions(paths.dynamic_admissions(DAY))
    assert plan.rev == 1
    assert plan.symbols_for("kis:5") == LS_SYMBOLS
    assert _state(paths).groups["ls:0"].state is GroupState.SECONDARY_ACTIVE
    assert any(row["kind"] == "ADMIT" and row["feed"] == "kis:5" for row in _journal(paths))

    _run_silent_ls(sup, paths, clock, T0 + 50, 20)
    later = read_dynamic_admissions(paths.dynamic_admissions(DAY))
    assert later.rev == 1
    assert later.entries == plan.entries
    assert stops.feeds == []


def test_plan_rev_continues_after_restart(paths) -> None:
    write_dynamic_admissions(
        paths.dynamic_admissions(DAY),
        DynamicPlan(rev=7, entries=(DynamicEntry("000660", "kis:5", 1),)),
    )
    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(), clock, _Stops())
    _run_silent_ls(sup, paths, clock, T0, 40)
    assert read_dynamic_admissions(paths.dynamic_admissions(DAY)).rev == 8


def test_restart_resumes_state_without_duplicate_admission(paths) -> None:
    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(), clock, _Stops())
    wall = _run_silent_ls(sup, paths, clock, T0, 40)
    before = _state(paths)
    before_plan = read_dynamic_admissions(paths.dynamic_admissions(DAY))

    fresh = _supervisor(paths, _settings(), clock, _Stops())
    _run_silent_ls(fresh, paths, clock, wall, 20)

    after = _state(paths)
    assert after.groups["ls:0"].state is GroupState.SECONDARY_ACTIVE
    assert after.groups["ls:0"].admitted == before.groups["ls:0"].admitted
    assert read_dynamic_admissions(paths.dynamic_admissions(DAY)) == before_plan


def test_failback_after_hold_and_recycle_after_close(paths) -> None:
    clock = _Clock(T0)
    stops = _Stops()
    sup = _supervisor(paths, _settings(), clock, stops)
    wall = _run_silent_ls(sup, paths, clock, T0, 40)
    wall = _run_healthy_ls(sup, paths, clock, wall, 700)
    assert _state(paths).groups["ls:0"].state is GroupState.PRIMARY_OK
    assert stops.feeds == []

    close = ANCHORS.regular_close
    close_wall = dt.datetime.combine(DAY, close, tzinfo=KST).timestamp()
    _tick(sup, paths, clock, close_wall + 1, ls_frame=close_wall + 1, kis_frame=close_wall + 1)
    _tick(sup, paths, clock, close_wall + 11, ls_frame=close_wall + 11, kis_frame=close_wall + 11)
    assert stops.feeds == ["kis:5"]


def test_day_demotion_persisted_blocks_failback(paths) -> None:
    settings = _settings(failback_hold_s=60, demote_degraded_s=120)
    clock = _Clock(T0)
    sup = _supervisor(paths, settings, clock, _Stops())
    wall = _run_silent_ls(sup, paths, clock, T0, 200)
    assert _state(paths).groups["ls:0"].state is GroupState.DAY_DEMOTED

    fresh = _supervisor(paths, settings, clock, _Stops())
    wall = _run_healthy_ls(fresh, paths, clock, wall, 300)
    assert _state(paths).groups["ls:0"].state is GroupState.DAY_DEMOTED

    _tick(fresh, paths, clock, wall + 10, ls_frame=wall + 10, kis_frame=wall + 10, today=DAY2)
    assert _state(paths).groups["ls:0"].state is GroupState.PRIMARY_OK
    assert _state(paths).day == DAY2


def test_corrupt_state_moved_aside_not_deleted(paths, caplog) -> None:
    path = paths.feed_controller_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not json", encoding="utf-8")
    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(), clock, _Stops())
    with caplog.at_level(logging.INFO):
        _tick(sup, paths, clock, T0, ls_frame=T0)
    moved = sorted(path.parent.glob("feed_controller.json.corrupt-*"))
    assert len(moved) == 1
    assert moved[0].read_text(encoding="utf-8") == "{not json"
    assert any("state_corrupt_moved_aside" in r.getMessage() for r in caplog.records)
    assert _state(paths).day == DAY


def test_tick_never_raises_and_pages_stuck_once(paths, caplog) -> None:
    state_path = paths.feed_controller_state_path()
    state_path.mkdir(parents=True)
    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(), clock, _Stops())
    with caplog.at_level(logging.WARNING):
        for index in range(40):
            _tick(sup, paths, clock, T0 + index * 10, ls_frame=T0)
    stuck = [r for r in caplog.records if getattr(r, PAGE_ATTR, None) == "feed_controller_stuck"]
    assert len(stuck) == 1


def test_no_secondary_healthy_pages_group_down_once(paths, caplog) -> None:
    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(), clock, _Stops())
    primaries = {"ls:0": ALIVE["ls:0"]}
    with caplog.at_level(logging.CRITICAL):
        _run_silent_ls(sup, paths, clock, T0, 340, kis_alive=False)
    group_down = [
        r for r in caplog.records if getattr(r, PAGE_ATTR, None) == "feed_group_down" and "reason=group_down" in r.getMessage()
    ]
    assert len(group_down) == 1
    assert "shard=ls:0" in group_down[0].getMessage()
    assert primaries


def test_intraday_and_slot5_exclusive_rejected_before_start(paths) -> None:
    with pytest.raises(ValueError, match="conflicts with intraday admission"):
        resolve_collector_runtime(
            collector=CollectorSettings(data_root=paths.root),
            intraday=IntradayAdmissionSettings(enabled=True),
            feed_control=_settings(),
        )


def test_nxt_group_watched_without_secondary(paths, caplog) -> None:
    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(watch_groups=("kis-nxt:0",)), clock, _Stops())
    nxt_plan = NXT_PLAN
    wall = T0
    with caplog.at_level(logging.INFO):
        while wall <= T0 + 340:
            clock.t = wall
            _write_health(paths, "kis-nxt:0", wall, last_frame=wall - 60)
            sup.tick(
                now=dt.datetime.fromtimestamp(wall, KST),
                today=DAY,
                anchors=ANCHORS,
                ls_plan=(),
                kis_plan=(),
                nxt_plan=nxt_plan,
                primaries={"kis-nxt:0": PrimaryView(True, False, False)},
            )
            wall += 10
    messages = [r.getMessage() for r in caplog.records]
    assert not [m for m in messages if "status=ADMITTED" in m or "status=WOULD_ADMIT" in m]
    assert any(row["kind"] == "TRANSITION" for row in _journal(paths))
    group_down = [
        r for r in caplog.records if getattr(r, PAGE_ATTR, None) == "feed_group_down" and "reason=group_down" in r.getMessage()
    ]
    assert len(group_down) == 1


def test_single_writer_round_trip_through_real_reader(paths) -> None:
    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(), clock, _Stops())
    _run_silent_ls(sup, paths, clock, T0, 40)
    plan = read_dynamic_admissions(paths.dynamic_admissions(DAY))
    assert plan.entries
    assert all(entry.feed == "kis:5" for entry in plan.entries)
    assert plan.symbols_for("kis:5") == LS_SYMBOLS


def test_read_feed_health_rejects_other_session(paths) -> None:
    _write_health(paths, "ls:0", T0, last_frame=T0)
    assert ff.read_feed_health(paths.feed_health_path("ls:0"), DAY) is not None
    assert ff.read_feed_health(paths.feed_health_path("ls:0"), DAY2) is None
    assert ff.read_feed_health(paths.feed_health_path("missing:0"), DAY) is None


def test_health_reader_rejects_malformed_files(paths) -> None:
    path = paths.feed_health_path("ls:0")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x" * (70 * 1024), encoding="utf-8")
    assert ff.read_feed_health(path, DAY) is None
    path.write_text("[]", encoding="utf-8")
    assert ff.read_feed_health(path, DAY) is None
    _write_health(paths, "ls:0", T0, last_frame=T0)
    payload = json.loads(path.read_text(encoding="utf-8"))
    del payload["lag_p50_s"]
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert ff.read_feed_health(path, DAY) is None
    payload["lag_p50_s"] = None
    payload["updated_wall"] = True
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert ff.read_feed_health(path, DAY) is None


def test_state_from_other_day_starts_fresh(paths) -> None:
    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(), clock, _Stops())
    _run_silent_ls(sup, paths, clock, T0, 40)
    assert _state(paths).day == DAY

    fresh = _supervisor(paths, _settings(), clock, _Stops())
    _tick(fresh, paths, clock, T0 + 60, ls_frame=T0 + 60, kis_frame=T0 + 60, today=DAY2)
    state = _state(paths)
    assert state.day == DAY2
    assert state.plan_rev == 0


def test_move_aside_failure_is_logged_and_state_starts_fresh(paths, monkeypatch, caplog) -> None:
    path = paths.feed_controller_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{broken", encoding="utf-8")

    def _deny(self, target):
        raise OSError("read-only")

    monkeypatch.setattr(pathlib.Path, "rename", _deny)
    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(), clock, _Stops())
    with caplog.at_level(logging.WARNING):
        _tick(sup, paths, clock, T0, ls_frame=T0)
    assert any("state_move_failed" in r.getMessage() for r in caplog.records)
    assert _state(paths).day == DAY


def test_krx_shard_group_is_watched_with_slot5_route(paths) -> None:
    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(watch_groups=("kis-krx:0",)), clock, _Stops())
    krx_plan = (AftermarketShard(MarketVenue.KRX, 0, ("005930",), ("H0STCNT0", "H0STASP0"), "1", "kid1"),)
    _write_health(paths, "kis-krx:0", T0, last_frame=T0)
    sup.tick(
        now=dt.datetime.fromtimestamp(T0, KST),
        today=DAY,
        anchors=ANCHORS,
        ls_plan=(),
        kis_plan=krx_plan,
        nxt_plan=(),
        primaries={"kis-krx:0": PrimaryView(True, False, False)},
    )
    assert "kis-krx:0" in _state(paths).groups


def test_secondary_without_supervisor_is_never_admitted(paths) -> None:
    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(), clock, _Stops())
    wall = T0
    while wall <= T0 + 40:
        _tick(
            sup,
            paths,
            clock,
            wall,
            ls_frame=wall - 40,
            kis_frame=wall,
            primaries={"ls:0": ALIVE["ls:0"]},
        )
        wall += 10
    assert not paths.dynamic_admissions(DAY).exists()
    assert _state(paths).groups["ls:0"].admitted == {}


def _write_nxt_pool(paths: DataPaths, symbols: tuple[str, ...]) -> None:
    path = paths.regular_nxt_candidates(DAY)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"rev": 1, "candidates": [{"symbol": s} for s in symbols]}), encoding="utf-8")


def _tick_mixed(sup, paths, clock, wall, *, ls_alive: bool, nxt_alive: bool) -> None:
    clock.t = wall
    _write_health(paths, "ls:0", wall, last_frame=wall if ls_alive else wall - 40)
    _write_health(paths, "kis-nxt:0", wall, last_frame=wall if nxt_alive else wall - 60)
    _write_health(paths, "kis:5", wall, last_frame=wall)
    sup.tick(
        now=dt.datetime.fromtimestamp(wall, KST),
        today=DAY,
        anchors=ANCHORS,
        ls_plan=LS_PLAN,
        kis_plan=(),
        nxt_plan=NXT_PLAN,
        primaries={
            "ls:0": ALIVE["ls:0"],
            "kis-nxt:0": PrimaryView(True, False, False),
            "kis:5": ALIVE["kis:5"],
        },
    )


def _run_mixed(sup, paths, clock, start: float, seconds: float, *, ls_alive: bool, nxt_alive: bool) -> None:
    wall = start
    while wall <= start + seconds:
        _tick_mixed(sup, paths, clock, wall, ls_alive=ls_alive, nxt_alive=nxt_alive)
        wall += 10


_MIXED = ("ls:0", "kis-nxt:0")


def test_nxt_group_failover_admits_nxt_venue_on_slot5(paths, monkeypatch) -> None:
    from src.realtime.contracts import MarketSession, MarketVenue
    from src.storage.layout import L0PartitionKey, l0_partition_relpath

    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(watch_groups=("kis-nxt:0",)), clock, _Stops())
    _write_nxt_pool(paths, ("005930",))
    wall = T0
    while wall <= T0 + 60:
        clock.t = wall
        _write_health(paths, "kis-nxt:0", wall, last_frame=wall - 60)
        _write_health(paths, "kis:5", wall, last_frame=wall)
        sup.tick(
            now=dt.datetime.fromtimestamp(wall, KST),
            today=DAY,
            anchors=ANCHORS,
            ls_plan=(),
            kis_plan=(),
            nxt_plan=NXT_PLAN,
            primaries={"kis-nxt:0": PrimaryView(True, False, False), "kis:5": ALIVE["kis:5"]},
        )
        wall += 10

    plan = read_dynamic_admissions(paths.dynamic_admissions(DAY))
    assert [(e.symbol, e.feed, e.venue) for e in plan.entries] == [("005930", "kis:5", "nxt")]
    assert plan.pairs_for("kis:5", "nxt") == ("005930",)
    assert _state(paths).groups["kis-nxt:0"].state is GroupState.SECONDARY_ACTIVE
    rel = l0_partition_relpath(
        L0PartitionKey("kis", MarketVenue.NXT, MarketSession.REGULAR, "H0NXCNT0", DAY)
    )
    assert str(rel).startswith("kis/nxt/regular/H0NXCNT0")


def test_krx_locked_slot5_refuses_failing_nxt_group(paths) -> None:
    write_dynamic_admissions(
        paths.dynamic_admissions(DAY),
        DynamicPlan(rev=1, entries=(DynamicEntry("000660", "kis:5", 1, venue="krx"),)),
    )
    _write_nxt_pool(paths, ("005930",))
    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(watch_groups=_MIXED), clock, _Stops())
    _run_mixed(sup, paths, clock, T0, 60, ls_alive=True, nxt_alive=False)

    plan = read_dynamic_admissions(paths.dynamic_admissions(DAY))
    assert plan.rev == 1
    assert {e.venue for e in plan.entries} == {"krx"}
    group = _state(paths).groups["kis-nxt:0"]
    assert group.state is GroupState.SECONDARY_ACTIVE
    assert group.admitted == {}


def test_nxt_locked_slot5_refuses_failing_krx_group(paths) -> None:
    write_dynamic_admissions(
        paths.dynamic_admissions(DAY),
        DynamicPlan(rev=1, entries=(DynamicEntry("005930", "kis:5", 1, venue="nxt"),)),
    )
    _write_nxt_pool(paths, ("005930",))
    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(watch_groups=_MIXED), clock, _Stops())
    _run_mixed(sup, paths, clock, T0, 60, ls_alive=False, nxt_alive=True)

    plan = read_dynamic_admissions(paths.dynamic_admissions(DAY))
    assert plan.rev == 1
    assert [e.venue for e in plan.entries] == ["nxt"]


def test_same_tick_venue_conflict_admits_only_the_first_venue(paths, caplog) -> None:
    _write_nxt_pool(paths, ("005930",))
    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(watch_groups=_MIXED), clock, _Stops())
    with caplog.at_level(logging.INFO):
        _run_mixed(sup, paths, clock, T0, 60, ls_alive=False, nxt_alive=False)

    plan = read_dynamic_admissions(paths.dynamic_admissions(DAY))
    assert plan.entries
    assert {e.venue for e in plan.entries} == {"krx"}
    assert "status=REFUSED group=kis-nxt:0" in caplog.text


def test_nxt_symbols_outside_pool_are_dropped_and_logged(paths, caplog) -> None:
    _write_nxt_pool(paths, ("000660",))
    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(watch_groups=("kis-nxt:0",)), clock, _Stops())
    with caplog.at_level(logging.INFO):
        wall = T0
        while wall <= T0 + 60:
            clock.t = wall
            _write_health(paths, "kis-nxt:0", wall, last_frame=wall - 60)
            _write_health(paths, "kis:5", wall, last_frame=wall)
            sup.tick(
                now=dt.datetime.fromtimestamp(wall, KST),
                today=DAY,
                anchors=ANCHORS,
                ls_plan=(),
                kis_plan=(),
                nxt_plan=NXT_PLAN,
                primaries={"kis-nxt:0": PrimaryView(True, False, False), "kis:5": ALIVE["kis:5"]},
            )
            wall += 10
    assert not paths.dynamic_admissions(DAY).exists()
    assert "reason=outside_nxt_pool dropped_n=1 dropped=005930" in caplog.text


def test_corrupt_nxt_pool_admits_nothing_and_never_raises(paths) -> None:
    path = paths.regular_nxt_candidates(DAY)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{broken", encoding="utf-8")
    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(watch_groups=("kis-nxt:0",)), clock, _Stops())
    _run_mixed(sup, paths, clock, T0, 60, ls_alive=True, nxt_alive=False)

    assert not paths.dynamic_admissions(DAY).exists()
    assert _state(paths).groups["kis-nxt:0"].admitted == {}


def test_journal_rows_carry_full_symbols_and_group_plan(paths) -> None:
    many = tuple(f"{index:06d}" for index in range(1, 8))
    plan = (
        LsShard(
            shard_index=0,
            symbols=many,
            streams=("H0STCNT0", "H0STASP0"),
            dynamic_reserve_symbols=0,
            window_standby=True,
        ),
    )
    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(), clock, _Stops())
    wall = T0
    while wall <= T0 + 40:
        clock.t = wall
        _write_health(paths, "ls:0", wall, last_frame=wall - 40)
        _write_health(paths, "kis:5", wall, last_frame=wall)
        sup.tick(
            now=dt.datetime.fromtimestamp(wall, KST),
            today=DAY,
            anchors=ANCHORS,
            ls_plan=plan,
            kis_plan=(),
            nxt_plan=(),
            primaries=ALIVE,
        )
        wall += 10
    journal = _journal(paths)
    admits = [row for row in journal if row["kind"] == "ADMIT"]
    assert admits
    assert all(row["symbols"] == list(many) for row in admits)
    assert admits[0]["symbols_n"] == len(many)
    assert admits[0]["symbols_head"] == list(many[:5])
    plan_rows = [row for row in journal if row["kind"] == "group_plan"]
    assert [row["group"] for row in plan_rows] == ["ls:0"]
    assert plan_rows[0]["symbols"] == list(many)


def test_group_plan_rewritten_on_change_and_each_day(paths) -> None:
    clock = _Clock(T0)
    sup = _supervisor(paths, _settings(), clock, _Stops())
    _run_silent_ls(sup, paths, clock, T0, 20)
    _run_silent_ls(sup, paths, clock, T0 + 30, 20)
    assert len([row for row in _journal(paths) if row["kind"] == "group_plan"]) == 1

    _tick(sup, paths, clock, T0 + 60, ls_frame=T0 + 60, kis_frame=T0 + 60, today=DAY2)
    day2 = paths.feed_control_log_path(DAY2)
    rows = [json.loads(line) for line in day2.read_text(encoding="utf-8").splitlines()]
    assert [row["kind"] for row in rows if row["kind"] == "group_plan"] == ["group_plan"]
