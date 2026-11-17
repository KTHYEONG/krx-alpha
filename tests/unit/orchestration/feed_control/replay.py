"""Deterministic replay harness: drives the pure controller over an explicit tick grid."""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from src.core.config import FeedControlSettings
from src.orchestration.feed_control.controller import initial_state, step
from src.orchestration.feed_control.model import (
    ActionKind,
    ControllerState,
    FeedHealth,
    GroupSpec,
    SecondaryCapacity,
    StepResult,
    WindowContext,
)

T0 = 1_800_000_000.0
DAY = dt.date(2026, 11, 16)
SESSION_CLOSE = T0 + 6 * 3600.0
STREAMS = ("H0STCNT0", "H0STASP0")
SYMBOLS = ("005930", "000660", "035420", "051910", "005380")
DEFAULT_FREE_PAIRS = 1000

HealthRow = Mapping[str, Mapping[str, Any] | None]
CapacityRow = Mapping[str, SecondaryCapacity | None]
Row = tuple[float, HealthRow, CapacityRow, Mapping[str, Any]]


@dataclass(frozen=True)
class ScenarioTrace:
    steps: tuple[tuple[float, StepResult], ...]

    @property
    def final_state(self) -> ControllerState:
        return self.steps[-1][1].state

    def transitions(self) -> list[tuple[float, str, str, str, str]]:
        return [
            (transition.wall, transition.group_id, transition.old.value, transition.new.value, transition.reason)
            for _, result in self.steps
            for transition in result.transitions
        ]

    def actions(self, kind: ActionKind | None = None) -> list[tuple[float, ActionKind, str, str | None, tuple[str, ...], str]]:
        return [
            (t, action.kind, action.group_id, action.feed_id, action.symbols, action.reason)
            for t, result in self.steps
            for action in result.actions
            if kind is None or action.kind is kind
        ]


def group_spec(
    group_id: str = "ls:0",
    *,
    primary: str = "ls:0",
    secondaries: tuple[str, ...] = ("db:0", "toss:0"),
    symbols: tuple[str, ...] = SYMBOLS,
) -> GroupSpec:
    return GroupSpec(
        group_id=group_id,
        primary_feed=primary,
        venue="krx",
        session="regular",
        symbols=symbols,
        streams=STREAMS,
        secondaries=secondaries,
    )


def window(now: float, **overrides: Any) -> WindowContext:
    fields: dict[str, Any] = {
        "day": DAY,
        "now_wall": now,
        "in_session": True,
        "in_high_risk": False,
        "session_open_wall": T0,
        "session_close_wall": SESSION_CLOSE,
        "continuous_close_wall": SESSION_CLOSE,
    }
    fields.update(overrides)
    return WindowContext(**fields)


def healthy_fields(feed_id: str, streams: Sequence[str], now: float) -> dict[str, Any]:
    return {
        "feed_id": feed_id,
        "vendor": feed_id.split(":")[0],
        "updated_wall": now,
        "running": True,
        "connected": True,
        "circuit_open": False,
        "auth_rejected": False,
        "last_frame_wall": now,
        "stream_last_frame_wall": dict.fromkeys(streams, now),
        "lag_p50_s": 0.1,
        "lag_p99_s": 0.5,
        "accepted_pairs": 0,
        "planned_pairs": 0,
        "rejected_pairs": 0,
        "pairs_pending_since_wall": None,
    }


def silent_since(since: float, streams: Sequence[str] = STREAMS) -> dict[str, Any]:
    return {"last_frame_wall": since, "stream_last_frame_wall": dict.fromkeys(streams, since)}


def feed_streams(groups: Sequence[GroupSpec]) -> dict[str, tuple[str, ...]]:
    streams: dict[str, tuple[str, ...]] = {}
    for group in groups:
        streams[group.primary_feed] = group.streams
        for feed in group.secondaries:
            streams[feed] = group.streams
    return streams


def build_health(
    groups: Sequence[GroupSpec],
    now: float,
    overrides: Mapping[str, Mapping[str, Any] | None],
) -> dict[str, FeedHealth]:
    health: dict[str, FeedHealth] = {}
    for feed, streams in feed_streams(groups).items():
        if feed in overrides and overrides[feed] is None:
            continue
        fields = healthy_fields(feed, streams, now)
        fields.update(overrides.get(feed, {}) or {})
        health[feed] = FeedHealth(**fields)
    return health


def default_capacity(groups: Sequence[GroupSpec]) -> dict[str, SecondaryCapacity]:
    feeds = {feed for group in groups for feed in group.secondaries}
    return {feed: SecondaryCapacity(feed, DEFAULT_FREE_PAIRS, frozenset()) for feed in sorted(feeds)}


def run_scenario(
    groups: Sequence[GroupSpec],
    settings: FeedControlSettings,
    script: Sequence[Row],
    *,
    start: float = T0,
    end: float | None = None,
    ticks: Sequence[float] | None = None,
) -> ScenarioTrace:
    rows = sorted(script, key=lambda row: row[0])
    if ticks is None:
        last = end if end is not None else (rows[-1][0] if rows else start) + 60.0
        count = round(last - start)
        ticks = [start + index for index in range(count + 1)]
    health_overrides: dict[str, Mapping[str, Any] | None] = {}
    capacity_overrides: dict[str, SecondaryCapacity | None] = {}
    window_overrides: dict[str, Any] = {}
    cursor = 0
    state: ControllerState | None = None
    out: list[tuple[float, StepResult]] = []
    for tick in ticks:
        while cursor < len(rows) and rows[cursor][0] <= tick:
            _, health_row, capacity_row, window_row = rows[cursor]
            health_overrides.update(health_row)
            capacity_overrides.update(capacity_row)
            window_overrides.update(window_row)
            cursor += 1
        if state is None:
            state = initial_state(DAY, groups, now_wall=tick)
        health = build_health(groups, tick, health_overrides)
        capacity = {
            feed: capacity_overrides.get(feed, base)
            for feed, base in default_capacity(groups).items()
            if capacity_overrides.get(feed, base) is not None
        }
        ctx = window(tick, **window_overrides)
        result = step(state, groups, health, capacity, ctx, settings)
        state = result.state
        out.append((tick, result))
    return ScenarioTrace(steps=tuple(out))
