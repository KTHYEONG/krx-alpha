from __future__ import annotations

import datetime as dt
import random
from dataclasses import replace

import pytest

from src.core.config import FeedControlSettings
from src.orchestration.feed_control.controller import classify_source, initial_state, step
from src.orchestration.feed_control.model import (
    ActionKind,
    FeedHealth,
    FeedStateError,
    GroupState,
    SecondaryCapacity,
    SourceStatus,
)
from tests.unit.orchestration.feed_control.replay import (
    DAY,
    STREAMS,
    SYMBOLS,
    T0,
    build_health,
    default_capacity,
    group_spec,
    run_scenario,
    silent_since,
    window,
)

GROUP = group_spec()
GROUPS = (GROUP,)
SETTINGS = FeedControlSettings(watch_groups=("ls:0",), secondary_feeds=("db:0", "toss:0"))
DEAD = {"circuit_open": True}


def _settings(**overrides: float) -> FeedControlSettings:
    return FeedControlSettings(watch_groups=("ls:0",), secondary_feeds=("db:0", "toss:0"), **overrides)


def _first(items, index: int, value: object):
    for item in items:
        if item[index] == value:
            return item
    raise AssertionError(f"no item with [{index}] == {value!r}")


def test_healthy_day_no_actions() -> None:
    trace = run_scenario(GROUPS, SETTINGS, [], end=T0 + 6 * 3600)
    assert trace.transitions() == []
    assert trace.actions() == []


def test_primary_dead_failover_immediate() -> None:
    trace = run_scenario(GROUPS, SETTINGS, [(T0 + 10, {"ls:0": DEAD}, {}, {})], end=T0 + 20)
    transitions = trace.transitions()
    assert transitions[0] == (T0 + 10, "ls:0", "PRIMARY_OK", "DEGRADED", "primary_dead")
    assert transitions[1] == (T0 + 10, "ls:0", "DEGRADED", "SECONDARY_ACTIVE", "failover")
    admits = trace.actions(ActionKind.ADMIT)
    assert admits == [(T0 + 10, ActionKind.ADMIT, "ls:0", "db:0", SYMBOLS, "admitted_by_priority")]


def test_silence_degrade_then_failover_timing() -> None:
    settings = SETTINGS
    trace = run_scenario(GROUPS, settings, [(T0 + 100, {"ls:0": silent_since(T0 + 100)}, {}, {})], end=T0 + 200)
    degraded = _first(trace.transitions(), 3, "DEGRADED")[0]
    secondary = _first(trace.transitions(), 3, "SECONDARY_ACTIVE")[0]
    start = T0 + 100
    lower = start + settings.ls_silence_s + settings.degrade_after_s
    assert lower <= degraded <= lower + 1.0
    assert degraded + settings.failover_after_s <= secondary <= degraded + settings.failover_after_s + 1.0


def test_short_blip_heals_without_admission() -> None:
    script = [(T0 + 100, {"ls:0": silent_since(T0 + 100)}, {}, {}), (T0 + 120, {"ls:0": {}}, {}, {})]
    trace = run_scenario(GROUPS, SETTINGS, script, end=T0 + 200)
    states = [item[3] for item in trace.transitions()]
    assert "SECONDARY_ACTIVE" not in states
    assert trace.actions(ActionKind.ADMIT) == []
    assert trace.final_state.groups["ls:0"].state is GroupState.PRIMARY_OK
    assert trace.final_state.groups["ls:0"].relapses == 0


def test_one_stream_silence_detected() -> None:
    now = T0 + 1000
    ctx = window(now, session_open_wall=T0)
    health = _fh(now, stream_last_frame_wall={"H0STCNT0": now, "H0STASP0": now - 40}, last_frame_wall=now)
    assert classify_source(health, now_wall=now, ctx=ctx, settings=SETTINGS) is SourceStatus.SILENT
    early = window(T0 + 10, session_open_wall=T0)
    assert classify_source(health, now_wall=T0 + 10, ctx=early, settings=SETTINGS) is not SourceStatus.SILENT


def test_outside_session_is_inert() -> None:
    script = [(T0 + 100, {"ls:0": silent_since(T0 + 100)}, {}, {"in_session": False})]
    trace = run_scenario(GROUPS, SETTINGS, script, end=T0 + 300)
    assert trace.transitions() == []
    assert trace.actions() == []


def test_failback_after_hold_not_in_high_risk() -> None:
    script = [
        (T0 + 10, {"ls:0": DEAD}, {}, {}),
        (T0 + 20, {"ls:0": {}}, {}, {"in_high_risk": True}),
        (T0 + 700, {}, {}, {"in_high_risk": False}),
    ]
    trace = run_scenario(GROUPS, SETTINGS, script, end=T0 + 800)
    holds = [item for item in trace.transitions() if item[3] == "FAILBACK_HOLD"]
    assert holds == [(T0 + 700, "ls:0", "SECONDARY_ACTIVE", "FAILBACK_HOLD", "failback_hold")]
    assert (T0 + 760, "ls:0", "FAILBACK_HOLD", "PRIMARY_OK", "failback_confirmed") in trace.transitions()


def test_relapse_during_hold_counts() -> None:
    script = [
        (T0 + 10, {"ls:0": DEAD}, {}, {}),
        (T0 + 100, {"ls:0": {}}, {}, {}),
        (T0 + 710, {"ls:0": DEAD}, {}, {}),
    ]
    trace = run_scenario(GROUPS, SETTINGS, script, end=T0 + 720)
    assert (T0 + 700, "ls:0", "SECONDARY_ACTIVE", "FAILBACK_HOLD", "failback_hold") in trace.transitions()
    assert (T0 + 710, "ls:0", "FAILBACK_HOLD", "SECONDARY_ACTIVE", "relapse") in trace.transitions()
    assert trace.final_state.groups["ls:0"].relapses == 1


def test_day_demotion_by_relapses_then_admits_new_capacity() -> None:
    settings = _settings(demote_degraded_s=100_000.0)
    script = [
        (T0 + 10, {"ls:0": DEAD}, {"db:0": SecondaryCapacity("db:0", 2, frozenset()), "toss:0": None}, {}),
        (T0 + 100, {"ls:0": {}}, {}, {}),
        (T0 + 710, {"ls:0": DEAD}, {}, {}),
        (T0 + 720, {"ls:0": {}}, {}, {}),
        (T0 + 1330, {"ls:0": DEAD}, {}, {}),
        (T0 + 1340, {"ls:0": {}}, {}, {}),
        (T0 + 1400, {}, {"db:0": SecondaryCapacity("db:0", 1000, frozenset())}, {}),
    ]
    trace = run_scenario(GROUPS, settings, script, end=T0 + 1500)
    transitions = trace.transitions()
    assert (T0 + 1330, "ls:0", "SECONDARY_ACTIVE", "DAY_DEMOTED", "demoted_relapses") in transitions
    assert not [item for item in transitions if item[0] > T0 + 1330 and item[3] == "FAILBACK_HOLD"]
    assert trace.final_state.groups["ls:0"].state is GroupState.DAY_DEMOTED
    later = trace.actions(ActionKind.ADMIT)[-1]
    assert later[0] == T0 + 1400
    assert later[3] == "db:0"
    assert later[4] == SYMBOLS[1:]


def test_day_demotion_by_degraded_time() -> None:
    script = [(T0 + 10, {"ls:0": DEAD}, {}, {}), (T0 + 950, {"ls:0": {}}, {}, {})]
    trace = run_scenario(GROUPS, SETTINGS, script, end=T0 + 1000)
    assert (T0 + 910, "ls:0", "SECONDARY_ACTIVE", "DAY_DEMOTED", "demoted_degraded_time") in trace.transitions()
    assert not [item for item in trace.transitions() if item[3] == "FAILBACK_HOLD"]
    assert trace.final_state.groups["ls:0"].state is GroupState.DAY_DEMOTED


def test_admission_respects_capacity_and_priority() -> None:
    script = [(T0 + 10, {"ls:0": DEAD}, {"db:0": SecondaryCapacity("db:0", 5, frozenset()), "toss:0": None}, {})]
    trace = run_scenario(GROUPS, SETTINGS, script, end=T0 + 20)
    admits = trace.actions(ActionKind.ADMIT)
    assert [(item[3], item[4]) for item in admits] == [("db:0", SYMBOLS[:2])]
    assert trace.final_state.groups["ls:0"].admitted["db:0"] == SYMBOLS[:2]


def test_admission_skips_unhealthy_secondary_and_uses_next() -> None:
    script = [(T0 + 10, {"ls:0": DEAD, "db:0": DEAD}, {}, {})]
    trace = run_scenario(GROUPS, SETTINGS, script, end=T0 + 20)
    assert [(item[3], item[4]) for item in trace.actions(ActionKind.ADMIT)] == [("toss:0", SYMBOLS)]


def test_no_healthy_secondary_pages_once() -> None:
    dead_all = {"ls:0": DEAD, "db:0": DEAD, "toss:0": DEAD}
    trace = run_scenario(GROUPS, SETTINGS, [(T0 + 10, dead_all, {}, {})], end=T0 + 1000)
    group_down = [item for item in trace.actions(ActionKind.PAGE) if item[5] == "group_down"]
    assert [item[0] for item in group_down] == [T0 + 310]


def test_capacity_exhausted_page() -> None:
    script = [(T0 + 10, {"ls:0": DEAD}, {"db:0": SecondaryCapacity("db:0", 2, frozenset()), "toss:0": None}, {})]
    trace = run_scenario(GROUPS, SETTINGS, script, end=T0 + 1000)
    pages = trace.actions(ActionKind.PAGE)
    assert pages == [(T0 + 310, ActionKind.PAGE, "ls:0", None, SYMBOLS[1:], "capacity_exhausted")]


def test_page_reasons_latch_independently() -> None:
    script = [
        (T0 + 10, {"ls:0": DEAD, "db:0": DEAD, "toss:0": DEAD}, {}, {}),
        (T0 + 400, {"db:0": {}, "toss:0": {}}, {}, {}),
        (T0 + 600, {"db:0": DEAD}, {}, {}),
    ]
    trace = run_scenario(GROUPS, SETTINGS, script, end=T0 + 1000)
    reasons = [item[5] for item in trace.actions(ActionKind.PAGE)]
    assert sorted(reasons) == ["capacity_exhausted", "group_down"]


def test_idempotent_step() -> None:
    now = T0 + 20
    health = build_health(GROUPS, now, {"ls:0": DEAD})
    capacity = default_capacity(GROUPS)
    ctx = window(now)
    state = initial_state(DAY, GROUPS, now_wall=T0 + 10)
    first = step(state, GROUPS, health, capacity, ctx, SETTINGS)
    assert first.actions
    second = step(first.state, GROUPS, health, capacity, ctx, SETTINGS)
    assert second.actions == ()
    assert second.transitions == ()
    assert second.state.plan_rev == first.state.plan_rev


def test_recycle_after_close_once() -> None:
    script = [
        (T0 + 10, {"ls:0": DEAD}, {}, {}),
        (T0 + 100, {}, {}, {"in_session": False, "session_close_wall": T0 + 100}),
    ]
    trace = run_scenario(GROUPS, SETTINGS, script, end=T0 + 300)
    recycles = trace.actions(ActionKind.RECYCLE)
    assert recycles == [(T0 + 100, ActionKind.RECYCLE, "ls:0", "db:0", (), "session_closed")]


def test_degraded_time_accumulates_with_gap_cap() -> None:
    ticks = [T0 + index for index in range(15)] + [T0 + 614]
    script = [(T0, {"ls:0": silent_since(T0)}, {}, {})]
    trace = run_scenario(GROUPS, SETTINGS, script, ticks=ticks)
    final = trace.final_state.groups["ls:0"]
    assert final.degraded_s == pytest.approx(60.0)
    assert final.state is GroupState.SECONDARY_ACTIVE


def test_unknown_grace_anchor() -> None:
    settings = _settings(degrade_after_s=0.0)
    trace = run_scenario(GROUPS, settings, [(T0, {"ls:0": None}, {}, {})], end=T0 + 40)
    assert _first(trace.transitions(), 3, "DEGRADED")[0] == T0 + 30
    script = [(T0, {"ls:0": None}, {}, {}), (T0 + 20, {"ls:0": {}}, {}, {}), (T0 + 25, {"ls:0": None}, {}, {})]
    reset = run_scenario(GROUPS, settings, script, end=T0 + 60)
    assert _first(reset.transitions(), 3, "DEGRADED")[0] == T0 + 55


def test_unknown_health_never_heals() -> None:
    settings = _settings(demote_degraded_s=100_000.0)
    script = [(T0 + 10, {"ls:0": DEAD}, {}, {}), (T0 + 20, {"ls:0": None}, {}, {})]
    trace = run_scenario(GROUPS, settings, script, end=T0 + 1000)
    assert not [item for item in trace.transitions() if item[3] == "FAILBACK_HOLD"]
    assert trace.final_state.groups["ls:0"].state is GroupState.SECONDARY_ACTIVE


def test_missing_group_initialized_and_removed_group_dropped() -> None:
    state = initial_state(DAY, GROUPS, now_wall=T0)
    other = group_spec("ls:1", primary="ls:1")
    health = build_health((other,), T0 + 1, {})
    result = step(state, (other,), health, default_capacity((other,)), window(T0 + 1), SETTINGS)
    assert set(result.state.groups) == {"ls:1"}
    assert result.state.groups["ls:1"].state is GroupState.PRIMARY_OK


def test_day_mismatch_raises() -> None:
    state = initial_state(DAY, GROUPS, now_wall=T0)
    ctx = replace(window(T0 + 1), day=dt.date(2026, 11, 17))
    with pytest.raises(FeedStateError):
        step(state, GROUPS, build_health(GROUPS, T0 + 1, {}), default_capacity(GROUPS), ctx, SETTINGS)


def test_pending_acks_lag_only_when_pending() -> None:
    now = T0 + 1000
    ctx = window(now, session_open_wall=T0)
    assert classify_source(_fh(now, pairs_pending_since_wall=now - 30), now_wall=now, ctx=ctx, settings=SETTINGS) is (
        SourceStatus.LAGGING
    )
    assert classify_source(_fh(now, pairs_pending_since_wall=now - 5), now_wall=now, ctx=ctx, settings=SETTINGS) is (
        SourceStatus.HEALTHY
    )
    rejected = _fh(now, rejected_pairs=3, pairs_pending_since_wall=None)
    assert classify_source(rejected, now_wall=now, ctx=ctx, settings=SETTINGS) is SourceStatus.HEALTHY


def test_unknown_when_missing_or_stale() -> None:
    now = T0 + 1000
    ctx = window(now, session_open_wall=T0)
    assert classify_source(None, now_wall=now, ctx=ctx, settings=SETTINGS) is SourceStatus.UNKNOWN
    stale = _fh(now, updated_wall=now - SETTINGS.health_max_age_s - 1)
    assert classify_source(stale, now_wall=now, ctx=ctx, settings=SETTINGS) is SourceStatus.UNKNOWN


def test_step_does_not_mutate_inputs() -> None:
    now = T0 + 20
    state = initial_state(DAY, GROUPS, now_wall=T0 + 10)
    health = build_health(GROUPS, now, {"ls:0": DEAD})
    capacity = default_capacity(GROUPS)
    before_state = state.to_json()
    before_health = dict(health)
    before_capacity = dict(capacity)
    step(state, GROUPS, health, capacity, window(now), SETTINGS)
    assert state.to_json() == before_state
    assert health == before_health
    assert capacity == before_capacity


def test_determinism_property_under_random_scripts() -> None:
    rng = random.Random(7)  # noqa: S311 seeded replay script, not cryptographic
    options = [{}, DEAD, {"lag_p50_s": 9.0}, None, {"last_frame_wall": T0 + 1}]
    script = []
    for _ in range(40):
        when = T0 + rng.randrange(0, 1200)
        feed = rng.choice(["ls:0", "db:0", "toss:0"])
        choice = rng.choice(options)
        window_row = {"in_session": rng.random() > 0.1, "in_high_risk": rng.random() > 0.8}
        script.append((float(when), {feed: choice}, {}, window_row))
    first = run_scenario(GROUPS, SETTINGS, script, end=T0 + 1300)
    second = run_scenario(GROUPS, SETTINGS, script, end=T0 + 1300)
    assert first.transitions() == second.transitions()
    assert first.actions() == second.actions()
    assert first.final_state.to_json() == second.final_state.to_json()


def _fh(now: float, **overrides) -> FeedHealth:
    fields = {
        "feed_id": "ls:0",
        "vendor": "ls",
        "updated_wall": now,
        "running": True,
        "connected": True,
        "circuit_open": False,
        "auth_rejected": False,
        "last_frame_wall": now,
        "stream_last_frame_wall": dict.fromkeys(STREAMS, now),
        "lag_p50_s": 0.1,
        "lag_p99_s": 0.5,
        "accepted_pairs": 0,
        "planned_pairs": 0,
        "rejected_pairs": 0,
        "pairs_pending_since_wall": None,
    }
    fields.update(overrides)
    return FeedHealth(**fields)


def test_classify_outside_session_is_healthy_even_when_silent() -> None:
    now = T0 + 1000
    ctx = window(now, in_session=False, session_open_wall=T0)
    silent = _fh(now, last_frame_wall=now - 500, stream_last_frame_wall=dict.fromkeys(STREAMS, now - 500))
    assert classify_source(silent, now_wall=now, ctx=ctx, settings=SETTINGS) is SourceStatus.HEALTHY


def test_classify_missing_frames_respect_open_grace() -> None:
    now = T0 + 1000
    no_frames = _fh(now, last_frame_wall=None, stream_last_frame_wall={})
    in_grace = window(now, session_open_wall=now - 10)
    after_grace = window(now, session_open_wall=now - 500)
    assert classify_source(no_frames, now_wall=now, ctx=in_grace, settings=SETTINGS) is SourceStatus.HEALTHY
    assert classify_source(no_frames, now_wall=now, ctx=after_grace, settings=SETTINGS) is SourceStatus.SILENT


def test_classify_uses_vendor_specific_silence_limit() -> None:
    now = T0 + 1000
    ctx = window(now, session_open_wall=T0)
    quiet = {"last_frame_wall": now - 20, "stream_last_frame_wall": dict.fromkeys(STREAMS, now - 20)}
    kis = _fh(now, vendor="kis", feed_id="kis:1", **quiet)
    other = _fh(now, vendor="db", feed_id="db:0", **quiet)
    ls = _fh(now, vendor="ls", **quiet)
    assert classify_source(kis, now_wall=now, ctx=ctx, settings=SETTINGS) is SourceStatus.HEALTHY
    assert classify_source(other, now_wall=now, ctx=ctx, settings=SETTINGS) is SourceStatus.HEALTHY
    assert classify_source(ls, now_wall=now, ctx=ctx, settings=SETTINGS) is SourceStatus.SILENT


def test_classify_disconnected_feed_is_silent() -> None:
    now = T0 + 1000
    ctx = window(now, session_open_wall=T0)
    assert classify_source(_fh(now, connected=False), now_wall=now, ctx=ctx, settings=SETTINGS) is SourceStatus.SILENT
