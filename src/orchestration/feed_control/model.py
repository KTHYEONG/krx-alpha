"""Feed health snapshots, group runtime state and controller I/O contracts (pure data)."""

from __future__ import annotations

import datetime as dt
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Any

from src.core.errors import KrxAlphaError

_SCHEMA_VERSION = 1
_TOKEN_RE = re.compile(r"[a-z0-9:_-]+")
_REASON_RE = re.compile(r"[a-z_]+")
_SYMBOL_RE = re.compile(r"[A-Za-z0-9]{1,32}")


class FeedStateError(KrxAlphaError, ValueError):
    """Malformed controller state or a state/window day mismatch; the caller reinitializes the day."""


class GroupState(StrEnum):
    PRIMARY_OK = "PRIMARY_OK"
    DEGRADED = "DEGRADED"
    SECONDARY_ACTIVE = "SECONDARY_ACTIVE"
    FAILBACK_HOLD = "FAILBACK_HOLD"
    DAY_DEMOTED = "DAY_DEMOTED"


class SourceStatus(StrEnum):
    HEALTHY = "HEALTHY"
    LAGGING = "LAGGING"
    SILENT = "SILENT"
    DEAD = "DEAD"
    UNKNOWN = "UNKNOWN"


class ActionKind(StrEnum):
    ADMIT = "ADMIT"
    RECYCLE = "RECYCLE"
    PAGE = "PAGE"
    DIGEST = "DIGEST"


@dataclass(frozen=True, slots=True)
class FeedHealth:
    feed_id: str
    vendor: str
    updated_wall: float | None
    running: bool
    connected: bool
    circuit_open: bool
    auth_rejected: bool
    last_frame_wall: float | None
    stream_last_frame_wall: Mapping[str, float]
    lag_p50_s: float | None
    lag_p99_s: float | None
    accepted_pairs: int
    planned_pairs: int
    rejected_pairs: int
    pairs_pending_since_wall: float | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "stream_last_frame_wall", MappingProxyType(dict(self.stream_last_frame_wall)))


@dataclass(frozen=True, slots=True)
class GroupSpec:
    group_id: str
    primary_feed: str
    venue: str
    session: str
    symbols: tuple[str, ...]
    streams: tuple[str, ...]
    secondaries: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SecondaryCapacity:
    feed_id: str
    free_pairs: int
    admitted: frozenset[tuple[str, str]]


@dataclass(frozen=True, slots=True)
class WindowContext:
    day: dt.date
    now_wall: float
    in_session: bool
    in_high_risk: bool
    session_open_wall: float
    session_close_wall: float
    continuous_close_wall: float


@dataclass(frozen=True, slots=True)
class GroupRuntime:
    state: GroupState
    since_wall: float
    healthy_since_wall: float | None
    abnormal_since_wall: float | None
    unknown_since_wall: float | None
    relapses: int
    degraded_s: float
    admitted: Mapping[str, tuple[str, ...]]
    paged_reasons: tuple[str, ...]
    recycled_feeds: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "admitted",
            MappingProxyType({feed: tuple(symbols) for feed, symbols in sorted(dict(self.admitted).items())}),
        )
        object.__setattr__(self, "paged_reasons", tuple(sorted(set(self.paged_reasons))))
        object.__setattr__(self, "recycled_feeds", tuple(sorted(set(self.recycled_feeds))))


@dataclass(frozen=True, slots=True)
class ControllerState:
    day: dt.date
    plan_rev: int
    groups: Mapping[str, GroupRuntime]
    last_step_wall: float | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "groups", MappingProxyType(dict(self.groups)))

    def to_json(self) -> dict[str, Any]:
        return {
            "day": self.day.isoformat(),
            "groups": {group_id: _group_to_json(self.groups[group_id]) for group_id in sorted(self.groups)},
            "last_step_wall": self.last_step_wall,
            "plan_rev": self.plan_rev,
            "schema_version": _SCHEMA_VERSION,
        }

    @staticmethod
    def from_json(raw: Mapping[str, Any]) -> ControllerState:
        data = _mapping(raw)
        _exact_keys(data, ("day", "groups", "last_step_wall", "plan_rev", "schema_version"))
        if _int(data["schema_version"]) != _SCHEMA_VERSION:
            raise FeedStateError("unsupported controller state schema_version")
        groups_raw = _mapping(data["groups"])
        groups: dict[str, GroupRuntime] = {}
        for group_id, group_raw in groups_raw.items():
            _token(group_id)
            groups[group_id] = _group_from_json(group_raw)
        return ControllerState(
            day=_day(data["day"]),
            plan_rev=_int(data["plan_rev"]),
            groups=groups,
            last_step_wall=_optional_number(data["last_step_wall"]),
        )


@dataclass(frozen=True, slots=True)
class Action:
    kind: ActionKind
    group_id: str
    feed_id: str | None
    symbols: tuple[str, ...]
    reason: str


@dataclass(frozen=True, slots=True)
class Transition:
    group_id: str
    wall: float
    old: GroupState
    new: GroupState
    reason: str


@dataclass(frozen=True, slots=True)
class StepResult:
    state: ControllerState
    actions: tuple[Action, ...]
    transitions: tuple[Transition, ...]


def _group_to_json(runtime: GroupRuntime) -> dict[str, Any]:
    return {
        "abnormal_since_wall": runtime.abnormal_since_wall,
        "admitted": {feed: list(symbols) for feed, symbols in runtime.admitted.items()},
        "degraded_s": runtime.degraded_s,
        "healthy_since_wall": runtime.healthy_since_wall,
        "paged_reasons": list(runtime.paged_reasons),
        "recycled_feeds": list(runtime.recycled_feeds),
        "relapses": runtime.relapses,
        "since_wall": runtime.since_wall,
        "state": runtime.state.value,
        "unknown_since_wall": runtime.unknown_since_wall,
    }


_GROUP_KEYS = (
    "abnormal_since_wall",
    "admitted",
    "degraded_s",
    "healthy_since_wall",
    "paged_reasons",
    "recycled_feeds",
    "relapses",
    "since_wall",
    "state",
    "unknown_since_wall",
)


def _group_from_json(raw: Any) -> GroupRuntime:
    data = _mapping(raw)
    _exact_keys(data, _GROUP_KEYS)
    state_raw = data["state"]
    if not isinstance(state_raw, str):
        raise FeedStateError("group state must be a string")
    try:
        state = GroupState(state_raw)
    except ValueError:
        raise FeedStateError("unknown group state") from None
    admitted: dict[str, tuple[str, ...]] = {}
    for feed_id, symbols_raw in _mapping(data["admitted"]).items():
        _token(feed_id)
        if not isinstance(symbols_raw, list | tuple):
            raise FeedStateError("admitted symbols must be a list")
        admitted[feed_id] = tuple(_symbol(symbol) for symbol in symbols_raw)
    paged = _string_list(data["paged_reasons"], _REASON_RE, "paged reason")
    recycled = _string_list(data["recycled_feeds"], _TOKEN_RE, "recycled feed")
    return GroupRuntime(
        state=state,
        since_wall=_number(data["since_wall"]),
        healthy_since_wall=_optional_number(data["healthy_since_wall"]),
        abnormal_since_wall=_optional_number(data["abnormal_since_wall"]),
        unknown_since_wall=_optional_number(data["unknown_since_wall"]),
        relapses=_int(data["relapses"]),
        degraded_s=_number(data["degraded_s"]),
        admitted=admitted,
        paged_reasons=paged,
        recycled_feeds=recycled,
    )


def _mapping(raw: Any) -> Mapping[str, Any]:
    if not isinstance(raw, Mapping):
        raise FeedStateError("controller state section must be an object")
    return raw


def _exact_keys(data: Mapping[str, Any], expected: Sequence[str]) -> None:
    if set(data) != set(expected):
        raise FeedStateError("controller state has missing or unexpected fields")


def _int(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FeedStateError("controller state integer field is malformed")
    return value


def _optional_number(value: Any) -> float | None:
    if value is None:
        return None
    return _number(value)


def _number(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise FeedStateError("controller state numeric field is malformed")
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise FeedStateError("controller state numeric field must be finite and non-negative")
    return number


def _day(value: Any) -> dt.date:
    if not isinstance(value, str):
        raise FeedStateError("controller state day is malformed")
    try:
        return dt.date.fromisoformat(value)
    except ValueError:
        raise FeedStateError("controller state day is malformed") from None


def _token(value: Any) -> str:
    if not isinstance(value, str) or _TOKEN_RE.fullmatch(value) is None:
        raise FeedStateError("controller state identifier is malformed")
    return value


def _symbol(value: Any) -> str:
    if not isinstance(value, str) or _SYMBOL_RE.fullmatch(value) is None:
        raise FeedStateError("controller state symbol is malformed")
    return value


def _string_list(value: Any, pattern: re.Pattern[str], label: str) -> tuple[str, ...]:
    if not isinstance(value, list | tuple):
        raise FeedStateError(f"{label} list is malformed")
    for item in value:
        if not isinstance(item, str) or pattern.fullmatch(item) is None:
            raise FeedStateError(f"{label} is malformed")
    return tuple(value)
