"""Pure feed-failover controller: classifies feed health and advances per-group failover state."""

from __future__ import annotations

import dataclasses
import datetime as dt
from collections.abc import Mapping, Sequence

from src.core.config import FeedControlSettings
from src.orchestration.feed_control.model import (
    Action,
    ActionKind,
    ControllerState,
    FeedHealth,
    FeedStateError,
    GroupRuntime,
    GroupSpec,
    GroupState,
    SecondaryCapacity,
    SourceStatus,
    StepResult,
    Transition,
    WindowContext,
)

MAX_STEP_GAP_S = 60.0
_MAX_TRANSITIONS_PER_STEP = len(GroupState)
_ACTIVE_DEGRADED = frozenset({GroupState.DEGRADED, GroupState.SECONDARY_ACTIVE, GroupState.FAILBACK_HOLD})
_ADMITTING = frozenset({GroupState.SECONDARY_ACTIVE, GroupState.FAILBACK_HOLD, GroupState.DAY_DEMOTED})
_ABNORMAL = frozenset({SourceStatus.SILENT, SourceStatus.LAGGING, SourceStatus.DEAD})


def classify_source(
    health: FeedHealth | None,
    *,
    now_wall: float,
    ctx: WindowContext,
    settings: FeedControlSettings,
) -> SourceStatus:
    if health is None or health.updated_wall is None or now_wall - health.updated_wall > settings.health_max_age_s:
        return SourceStatus.UNKNOWN
    if health.circuit_open or health.auth_rejected or not health.running:
        return SourceStatus.DEAD
    if not health.connected:
        return SourceStatus.SILENT
    if not ctx.in_session:
        return SourceStatus.HEALTHY
    limit = _silence_limit_s(health.vendor, settings)
    in_open_grace = now_wall - ctx.session_open_wall <= settings.first_frame_grace_s
    if health.last_frame_wall is None:
        if not in_open_grace:
            return SourceStatus.SILENT
    elif now_wall - health.last_frame_wall > limit:
        return SourceStatus.SILENT
    if not in_open_grace and health.stream_last_frame_wall:
        ages = [now_wall - ts for ts in health.stream_last_frame_wall.values()]
        if any(age > limit for age in ages) and any(age <= limit for age in ages):
            return SourceStatus.SILENT
    if health.pairs_pending_since_wall is not None and now_wall - health.pairs_pending_since_wall > (
        settings.pairs_shortfall_after_s
    ):
        return SourceStatus.LAGGING
    if health.lag_p50_s is not None and health.lag_p50_s > settings.lag_limit_s:
        return SourceStatus.LAGGING
    return SourceStatus.HEALTHY


def initial_state(day: dt.date, groups: Sequence[GroupSpec], *, now_wall: float) -> ControllerState:
    runtimes = {group.group_id: _fresh_runtime(now_wall) for group in groups}
    return ControllerState(day=day, plan_rev=0, groups=runtimes, last_step_wall=None)


def step(
    state: ControllerState,
    groups: Sequence[GroupSpec],
    health: Mapping[str, FeedHealth],
    capacity: Mapping[str, SecondaryCapacity],
    ctx: WindowContext,
    settings: FeedControlSettings,
) -> StepResult:
    if state.day != ctx.day:
        raise FeedStateError("controller state day does not match window day")
    gap_s = _step_gap_s(state.last_step_wall, ctx.now_wall)
    remaining = {feed: _net_free_pairs(feed, cap, state, groups) for feed, cap in capacity.items()}
    runtimes: dict[str, GroupRuntime] = {}
    actions: list[Action] = []
    transitions: list[Transition] = []
    for spec in groups:
        runtime = state.groups.get(spec.group_id)
        if runtime is None:
            runtime = _fresh_runtime(ctx.now_wall)
        if ctx.in_session:
            runtime, group_actions, group_transitions = _advance_group(
                runtime, spec, health, capacity, remaining, gap_s, ctx, settings
            )
            actions.extend(group_actions)
            transitions.extend(group_transitions)
        recycle_actions, runtime = _recycle(runtime, spec.group_id, ctx)
        actions.extend(recycle_actions)
        runtimes[spec.group_id] = runtime
    plan_rev = state.plan_rev + (1 if any(action.kind is ActionKind.ADMIT for action in actions) else 0)
    new_state = ControllerState(day=ctx.day, plan_rev=plan_rev, groups=runtimes, last_step_wall=ctx.now_wall)
    return StepResult(state=new_state, actions=tuple(actions), transitions=tuple(transitions))


def _silence_limit_s(vendor: str, settings: FeedControlSettings) -> float:
    if vendor == "ls":
        return settings.ls_silence_s
    if vendor == "kis":
        return settings.kis_silence_s
    return settings.other_silence_s


def _fresh_runtime(now_wall: float) -> GroupRuntime:
    return GroupRuntime(
        state=GroupState.PRIMARY_OK,
        since_wall=now_wall,
        healthy_since_wall=None,
        abnormal_since_wall=None,
        unknown_since_wall=None,
        relapses=0,
        degraded_s=0.0,
        admitted={},
        paged_reasons=(),
        recycled_feeds=(),
    )


def _step_gap_s(last_step_wall: float | None, now_wall: float) -> float:
    if last_step_wall is None:
        return 0.0
    return min(max(now_wall - last_step_wall, 0.0), MAX_STEP_GAP_S)


def _net_free_pairs(
    feed: str,
    capacity: SecondaryCapacity,
    state: ControllerState,
    groups: Sequence[GroupSpec],
) -> int:
    held = 0
    for spec in groups:
        runtime = state.groups.get(spec.group_id)
        if runtime is None:
            continue
        for symbol in runtime.admitted.get(feed, ()):
            held += sum(1 for stream in spec.streams if (symbol, stream) not in capacity.admitted)
    return max(0, capacity.free_pairs - held)


def _advance_group(
    runtime: GroupRuntime,
    spec: GroupSpec,
    health: Mapping[str, FeedHealth],
    capacity: Mapping[str, SecondaryCapacity],
    remaining: dict[str, int],
    gap_s: float,
    ctx: WindowContext,
    settings: FeedControlSettings,
) -> tuple[GroupRuntime, list[Action], list[Transition]]:
    status = classify_source(health.get(spec.primary_feed), now_wall=ctx.now_wall, ctx=ctx, settings=settings)
    if runtime.state in _ACTIVE_DEGRADED:
        runtime = dataclasses.replace(runtime, degraded_s=runtime.degraded_s + gap_s)
    runtime = _observe(runtime, status, ctx.now_wall, settings)
    runtime, moves = _advance(runtime, status, ctx, settings)
    transitions = [Transition(spec.group_id, ctx.now_wall, old, new, reason) for old, new, reason in moves]
    actions = [Action(ActionKind.DIGEST, spec.group_id, None, (), transition.reason) for transition in transitions]
    if runtime.state in _ADMITTING:
        runtime, admit_actions = _plan_admission(runtime, spec, health, capacity, remaining, ctx, settings)
        actions.extend(admit_actions)
    page_actions, runtime = _pages(runtime, spec, status, health, ctx, settings)
    actions.extend(page_actions)
    return runtime, actions, transitions


def _observe(runtime: GroupRuntime, status: SourceStatus, now: float, settings: FeedControlSettings) -> GroupRuntime:
    unknown_since = None
    if status is SourceStatus.UNKNOWN:
        unknown_since = runtime.unknown_since_wall if runtime.unknown_since_wall is not None else now
    if status is SourceStatus.HEALTHY:
        healthy_since = runtime.healthy_since_wall if runtime.healthy_since_wall is not None else now
        abnormal_since = None
    else:
        healthy_since = None
        abnormal_since = runtime.abnormal_since_wall
        if _is_abnormal(status, unknown_since, now, settings):
            abnormal_since = abnormal_since if abnormal_since is not None else now
    return dataclasses.replace(
        runtime,
        healthy_since_wall=healthy_since,
        abnormal_since_wall=abnormal_since,
        unknown_since_wall=unknown_since,
    )


def _is_abnormal(
    status: SourceStatus,
    unknown_since: float | None,
    now: float,
    settings: FeedControlSettings,
) -> bool:
    if status is SourceStatus.UNKNOWN:
        return unknown_since is not None and now - unknown_since >= settings.unknown_grace_s
    return status in _ABNORMAL


def _held_for(now: float, since: float | None, duration_s: float) -> bool:
    return since is not None and now - since >= duration_s


def _advance(
    runtime: GroupRuntime,
    status: SourceStatus,
    ctx: WindowContext,
    settings: FeedControlSettings,
) -> tuple[GroupRuntime, list[tuple[GroupState, GroupState, str]]]:
    moves: list[tuple[GroupState, GroupState, str]] = []
    for _ in range(_MAX_TRANSITIONS_PER_STEP):
        move = _next_move(runtime, status, ctx, settings)
        if move is None:
            break
        target, reason = move
        old = runtime.state
        relapses = runtime.relapses
        if old is GroupState.FAILBACK_HOLD and target is GroupState.SECONDARY_ACTIVE:
            relapses += 1
        runtime = dataclasses.replace(runtime, state=target, since_wall=ctx.now_wall, relapses=relapses)
        moves.append((old, target, reason))
    return runtime, moves


def _next_move(
    runtime: GroupRuntime,
    status: SourceStatus,
    ctx: WindowContext,
    settings: FeedControlSettings,
) -> tuple[GroupState, str] | None:
    now = ctx.now_wall
    state = runtime.state
    if state in _ACTIVE_DEGRADED:
        if runtime.relapses >= settings.demote_relapses:
            return GroupState.DAY_DEMOTED, "demoted_relapses"
        if runtime.degraded_s >= settings.demote_degraded_s:
            return GroupState.DAY_DEMOTED, "demoted_degraded_time"
    abnormal = _is_abnormal(status, runtime.unknown_since_wall, now, settings)
    healthy = status is SourceStatus.HEALTHY
    if state is GroupState.PRIMARY_OK:
        if abnormal and (status is SourceStatus.DEAD or _held_for(now, runtime.abnormal_since_wall, settings.degrade_after_s)):
            return GroupState.DEGRADED, f"primary_{status.value.lower()}"
        return None
    if state is GroupState.DEGRADED:
        if healthy and _held_for(now, runtime.healthy_since_wall, settings.heal_after_s):
            return GroupState.PRIMARY_OK, "primary_recovered"
        if abnormal and (status is SourceStatus.DEAD or now - runtime.since_wall >= settings.failover_after_s):
            return GroupState.SECONDARY_ACTIVE, "failover"
        return None
    if state is GroupState.SECONDARY_ACTIVE:
        if healthy and not ctx.in_high_risk and _held_for(now, runtime.healthy_since_wall, settings.failback_hold_s):
            return GroupState.FAILBACK_HOLD, "failback_hold"
        return None
    if state is GroupState.FAILBACK_HOLD:
        if abnormal:
            return GroupState.SECONDARY_ACTIVE, "relapse"
        confirm_from = None
        if runtime.healthy_since_wall is not None:
            confirm_from = max(runtime.healthy_since_wall, runtime.since_wall)
        if healthy and _held_for(now, confirm_from, settings.failback_confirm_s):
            return GroupState.PRIMARY_OK, "failback_confirmed"
        return None
    return None


def _plan_admission(
    runtime: GroupRuntime,
    spec: GroupSpec,
    health: Mapping[str, FeedHealth],
    capacity: Mapping[str, SecondaryCapacity],
    remaining: dict[str, int],
    ctx: WindowContext,
    settings: FeedControlSettings,
) -> tuple[GroupRuntime, list[Action]]:
    taken = {symbol for symbols in runtime.admitted.values() for symbol in symbols}
    admitted = {feed: tuple(symbols) for feed, symbols in runtime.admitted.items()}
    actions: list[Action] = []
    width = len(spec.streams)
    for feed in spec.secondaries:
        cap = capacity.get(feed)
        if cap is None:
            continue
        if classify_source(health.get(feed), now_wall=ctx.now_wall, ctx=ctx, settings=settings) is not (
            SourceStatus.HEALTHY
        ):
            continue
        free = remaining[feed]
        chosen: list[str] = []
        for symbol in spec.symbols:
            if symbol in taken or any((symbol, stream) in cap.admitted for stream in spec.streams):
                continue
            if free < width:
                break
            chosen.append(symbol)
            taken.add(symbol)
            free -= width
        remaining[feed] = free
        if chosen:
            admitted[feed] = admitted.get(feed, ()) + tuple(chosen)
            actions.append(Action(ActionKind.ADMIT, spec.group_id, feed, tuple(chosen), "admitted_by_priority"))
    return dataclasses.replace(runtime, admitted=admitted), actions


def _pages(
    runtime: GroupRuntime,
    spec: GroupSpec,
    status: SourceStatus,
    health: Mapping[str, FeedHealth],
    ctx: WindowContext,
    settings: FeedControlSettings,
) -> tuple[list[Action], GroupRuntime]:
    now = ctx.now_wall
    if not _held_for(now, runtime.abnormal_since_wall, settings.page_after_s):
        return [], runtime
    paged = set(runtime.paged_reasons)
    actions: list[Action] = []
    healthy_secondary = any(
        runtime.admitted.get(feed)
        and classify_source(health.get(feed), now_wall=now, ctx=ctx, settings=settings) is SourceStatus.HEALTHY
        for feed in spec.secondaries
    )
    if status is not SourceStatus.HEALTHY and not healthy_secondary and "group_down" not in paged:
        actions.append(Action(ActionKind.PAGE, spec.group_id, None, (), "group_down"))
        paged.add("group_down")
    admitted_symbols = {symbol for symbols in runtime.admitted.values() for symbol in symbols}
    unadmitted = tuple(symbol for symbol in spec.symbols if symbol not in admitted_symbols)
    if _is_abnormal(status, runtime.unknown_since_wall, now, settings) and unadmitted and (
        "capacity_exhausted" not in paged
    ):
        actions.append(Action(ActionKind.PAGE, spec.group_id, None, unadmitted, "capacity_exhausted"))
        paged.add("capacity_exhausted")
    return actions, dataclasses.replace(runtime, paged_reasons=tuple(sorted(paged)))


def _recycle(runtime: GroupRuntime, group_id: str, ctx: WindowContext) -> tuple[list[Action], GroupRuntime]:
    if ctx.now_wall < ctx.session_close_wall:
        return [], runtime
    recycled = set(runtime.recycled_feeds)
    actions: list[Action] = []
    for feed in sorted(runtime.admitted):
        if runtime.admitted[feed] and feed not in recycled:
            actions.append(Action(ActionKind.RECYCLE, group_id, feed, (), "session_closed"))
            recycled.add(feed)
    return actions, dataclasses.replace(runtime, recycled_feeds=tuple(sorted(recycled)))
