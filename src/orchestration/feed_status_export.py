"""Feed controller status export: builds the dashboard `status-checks.v1` payload and writes it atomically."""

from __future__ import annotations

import datetime as dt
import json
import os
import pathlib
from collections.abc import Mapping, Sequence
from typing import Any, Final

from src.core.config import FeedControlSettings
from src.orchestration.feed_control.model import ControllerState, FeedHealth, GroupRuntime, GroupState

STATUS_SCHEMA: Final[str] = "status-checks.v1"
PRODUCER: Final[str] = "krx-feed-controller"
PROJECT: Final[str] = "krx"
STALE_AFTER_S: Final[int] = 120
_FILE_MODE: Final[int] = 0o644


def build_status_checks(
    state: ControllerState,
    health: Mapping[str, FeedHealth | None],
    *,
    now_wall: float,
    secondaries: Sequence[str],
    settings: FeedControlSettings,
) -> dict[str, Any]:
    """Status payload for the watched groups, configured secondaries and controller liveness; pure."""
    observed_at = _iso(now_wall)
    checks: list[dict[str, Any]] = [_controller_check(state, observed_at, observe_only=not secondaries)]
    checks.extend(_group_check(group_id, state, health, now_wall, observed_at, settings) for group_id in settings.watch_groups)
    checks.extend(_secondary_check(feed, health, now_wall, observed_at, settings) for feed in secondaries)
    return {"schema": STATUS_SCHEMA, "producer": PRODUCER, "generated_at": observed_at, "checks": checks}


def write_status_file(path: pathlib.Path, payload: Mapping[str, Any]) -> None:
    """Replace `path` atomically with the JSON payload (mode 0o644); raises OSError on failure."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    os.chmod(tmp, _FILE_MODE)
    os.replace(tmp, path)


def _iso(wall: float) -> str:
    return dt.datetime.fromtimestamp(wall, tz=dt.UTC).isoformat(timespec="seconds")


def _check(
    check_id: str,
    title: str,
    level: str,
    detail: str,
    observed_at: str,
    value: str | None = None,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": check_id,
        "project": PROJECT,
        "title": title,
        "level": level,
        "detail": detail,
        "observed_at": observed_at,
        "stale_after_s": STALE_AFTER_S,
    }
    if value is not None:
        row["value"] = value
    return row


def _controller_check(state: ControllerState, observed_at: str, *, observe_only: bool) -> dict[str, Any]:
    if state.last_step_wall is None:
        return _check("krx.feed_controller", "Feed controller", "UNKNOWN", "no tick recorded", observed_at)
    detail = f"last tick {_iso(state.last_step_wall)}"
    if observe_only:
        detail = f"{detail} observe_only"
    return _check("krx.feed_controller", "Feed controller", "OK", detail, observed_at)


def _group_check(
    group_id: str,
    state: ControllerState,
    health: Mapping[str, FeedHealth | None],
    now_wall: float,
    observed_at: str,
    settings: FeedControlSettings,
) -> dict[str, Any]:
    check_id = f"krx.feed_{group_id.replace(':', '_')}"
    title = f"Feed group {group_id}"
    runtime = state.groups.get(group_id)
    primary = health.get(group_id)
    if runtime is None or primary is None:
        reason = "no controller state" if runtime is None else "no health"
        return _check(check_id, title, "UNKNOWN", reason, observed_at)
    admitted_n = sum(len(symbols) for symbols in runtime.admitted.values())
    if not _is_up(primary, now_wall, settings) and not _covered(runtime, health, now_wall, settings):
        return _check(check_id, title, "FAIL", f"primary down no healthy secondary state={runtime.state}", observed_at, str(admitted_n))
    if runtime.state is GroupState.PRIMARY_OK:
        return _check(check_id, title, "OK", "primary healthy", observed_at, str(admitted_n))
    if runtime.state is GroupState.DEGRADED:
        return _check(check_id, title, "WARN", f"degraded admitted={admitted_n}", observed_at, str(admitted_n))
    return _check(
        check_id,
        title,
        "WARN",
        f"{runtime.state} secondary={_active_secondary(runtime)} admitted={admitted_n}",
        observed_at,
        str(admitted_n),
    )


def _secondary_check(
    feed: str,
    health: Mapping[str, FeedHealth | None],
    now_wall: float,
    observed_at: str,
    settings: FeedControlSettings,
) -> dict[str, Any]:
    check_id = f"krx.feed_secondary_{feed.replace(':', '_')}"
    title = f"Feed secondary {feed}"
    found = health.get(feed)
    if found is None:
        return _check(check_id, title, "UNKNOWN", "no health", observed_at)
    if _is_up(found, now_wall, settings):
        return _check(check_id, title, "OK", "up", observed_at)
    return _check(check_id, title, "WARN", "protection_lost", observed_at)


def _is_up(health: FeedHealth | None, now_wall: float, settings: FeedControlSettings) -> bool:
    return (
        health is not None
        and health.updated_wall is not None
        and now_wall - health.updated_wall <= settings.health_max_age_s
        and health.running
        and health.connected
        and not health.circuit_open
        and not health.auth_rejected
    )


def _covered(
    runtime: GroupRuntime,
    health: Mapping[str, FeedHealth | None],
    now_wall: float,
    settings: FeedControlSettings,
) -> bool:
    return any(
        symbols and _is_up(health.get(feed), now_wall, settings) for feed, symbols in runtime.admitted.items()
    )


def _active_secondary(runtime: GroupRuntime) -> str:
    feeds = sorted(feed for feed, symbols in runtime.admitted.items() if symbols)
    return feeds[0] if feeds else "none"
