"""Feed status export: level mapping, secondary protection, contract shape, and atomic file write."""

from __future__ import annotations

import datetime as dt
import json
import os
import stat

import pytest

from src.core.config import FeedControlSettings
from src.orchestration.feed_control.model import ControllerState, FeedHealth, GroupRuntime, GroupState
from src.orchestration.feed_status_export import STALE_AFTER_S, build_status_checks, write_status_file

DAY = dt.date(2026, 11, 16)
NOW = dt.datetime(2026, 11, 16, 10, 0, tzinfo=dt.UTC).timestamp()
SETTINGS = FeedControlSettings(watch_groups=("ls:0",), secondary_feeds=("kis:5",))
LEVELS = {"OK", "WARN", "UNKNOWN", "FAIL"}
CHECK_KEYS = {"id", "project", "title", "level", "detail", "observed_at", "stale_after_s"}


def _health(feed: str, *, wall: float = NOW, up: bool = True, vendor: str = "ls") -> FeedHealth:
    return FeedHealth(
        feed_id=feed,
        vendor=vendor,
        updated_wall=wall,
        running=up,
        connected=up,
        circuit_open=False,
        auth_rejected=False,
        last_frame_wall=wall,
        stream_last_frame_wall={"H0STCNT0": wall},
        lag_p50_s=0.1,
        lag_p99_s=0.2,
        accepted_pairs=2,
        planned_pairs=2,
        rejected_pairs=0,
        pairs_pending_since_wall=None,
    )


def _runtime(state: GroupState, admitted: dict[str, tuple[str, ...]] | None = None) -> GroupRuntime:
    return GroupRuntime(
        state=state,
        since_wall=NOW - 100,
        healthy_since_wall=None,
        abnormal_since_wall=None,
        unknown_since_wall=None,
        relapses=0,
        degraded_s=0.0,
        admitted=admitted or {},
        paged_reasons=(),
        recycled_feeds=(),
    )


def _state(runtime: GroupRuntime, *, last_step: float | None = NOW) -> ControllerState:
    return ControllerState(day=DAY, plan_rev=0, groups={"ls:0": runtime}, last_step_wall=last_step)


def _group_check(payload: dict) -> dict:
    return next(check for check in payload["checks"] if check["id"] == "krx.feed_ls_0")


def _status(state: ControllerState, health: dict[str, FeedHealth | None], secondaries=("kis:5",)) -> dict:
    return build_status_checks(state, health, now_wall=NOW, secondaries=secondaries, settings=SETTINGS)


@pytest.mark.parametrize(
    ("runtime", "health", "level"),
    [
        (_runtime(GroupState.PRIMARY_OK), {"ls:0": _health("ls:0")}, "OK"),
        (_runtime(GroupState.DEGRADED), {"ls:0": _health("ls:0")}, "WARN"),
        (
            _runtime(GroupState.SECONDARY_ACTIVE, {"kis:5": ("005930",)}),
            {"ls:0": _health("ls:0", up=False), "kis:5": _health("kis:5", vendor="kis")},
            "WARN",
        ),
        (
            _runtime(GroupState.FAILBACK_HOLD, {"kis:5": ("005930",)}),
            {"ls:0": _health("ls:0"), "kis:5": _health("kis:5", vendor="kis")},
            "WARN",
        ),
        (
            _runtime(GroupState.DAY_DEMOTED, {"kis:5": ("005930",)}),
            {"ls:0": _health("ls:0"), "kis:5": _health("kis:5", vendor="kis")},
            "WARN",
        ),
        (
            _runtime(GroupState.SECONDARY_ACTIVE, {"kis:5": ("005930",)}),
            {"ls:0": _health("ls:0", up=False), "kis:5": _health("kis:5", up=False, vendor="kis")},
            "FAIL",
        ),
    ],
)
def test_levels_map_from_group_state(runtime, health, level) -> None:
    group = _group_check(_status(_state(runtime), health))
    assert group["level"] == level
    assert group["level"] in LEVELS


def test_missing_health_or_state_is_unknown() -> None:
    assert _group_check(_status(_state(_runtime(GroupState.PRIMARY_OK)), {}))["level"] == "UNKNOWN"
    assert _group_check(_status(ControllerState(DAY, 0, {}, NOW), {"ls:0": _health("ls:0")}))["level"] == "UNKNOWN"


def test_group_detail_names_secondary_and_admitted_count() -> None:
    runtime = _runtime(GroupState.SECONDARY_ACTIVE, {"kis:5": ("005930", "000660")})
    health = {"ls:0": _health("ls:0", up=False), "kis:5": _health("kis:5", vendor="kis")}
    group = _group_check(_status(_state(runtime), health))
    assert group["detail"] == "SECONDARY_ACTIVE secondary=kis:5 admitted=2"
    assert group["value"] == "2"


@pytest.mark.parametrize(
    ("health", "level", "detail"),
    [
        (_health("kis:5", vendor="kis"), "OK", "up"),
        (_health("kis:5", up=False, vendor="kis"), "WARN", "protection_lost"),
        (None, "UNKNOWN", "no health"),
    ],
)
def test_secondary_protection_never_fails(health, level, detail) -> None:
    payload = _status(_state(_runtime(GroupState.PRIMARY_OK)), {"ls:0": _health("ls:0"), "kis:5": health})
    check = next(c for c in payload["checks"] if c["id"] == "krx.feed_secondary_kis_5")
    assert (check["level"], check["detail"]) == (level, detail)
    assert check["level"] != "FAIL"


def test_controller_liveness_and_observe_only_detail() -> None:
    state = _state(_runtime(GroupState.PRIMARY_OK))
    active = _status(state, {"ls:0": _health("ls:0")})
    observe = _status(state, {"ls:0": _health("ls:0")}, secondaries=())
    live = next(c for c in active["checks"] if c["id"] == "krx.feed_controller")
    assert live["level"] == "OK"
    assert "observe_only" not in live["detail"]
    observe_live = next(c for c in observe["checks"] if c["id"] == "krx.feed_controller")
    assert observe_live["detail"].endswith("observe_only")
    unknown = _status(_state(_runtime(GroupState.PRIMARY_OK), last_step=None), {})
    assert next(c for c in unknown["checks"] if c["id"] == "krx.feed_controller")["level"] == "UNKNOWN"


def test_ids_prefixed_and_schema_exact() -> None:
    payload = _status(_state(_runtime(GroupState.PRIMARY_OK)), {"ls:0": _health("ls:0"), "kis:5": _health("kis:5", vendor="kis")})
    assert set(payload) == {"schema", "producer", "generated_at", "checks"}
    assert payload["schema"] == "status-checks.v1"
    ids = [check["id"] for check in payload["checks"]]
    assert len(ids) == len(set(ids))
    for check in payload["checks"]:
        assert check["id"].startswith("krx.")
        assert ":" not in check["id"]
        assert check["project"] == "krx"
        assert check["level"] in LEVELS
        assert set(check) >= CHECK_KEYS
        assert set(check) - CHECK_KEYS <= {"value"}
        assert check["stale_after_s"] == STALE_AFTER_S == 120
        assert check["observed_at"] == payload["generated_at"]


def test_no_secrets_or_paths_in_payload() -> None:
    payload = _status(
        _state(_runtime(GroupState.SECONDARY_ACTIVE, {"kis:5": ("005930",)})),
        {"ls:0": _health("ls:0", up=False), "kis:5": _health("kis:5", vendor="kis")},
    )
    assert "/" not in json.dumps(payload, ensure_ascii=False)


def test_write_status_file_is_atomic_and_world_readable(tmp_path) -> None:
    target = tmp_path / "status" / "feed_control.json"
    write_status_file(target, {"schema": "status-checks.v1", "checks": []})
    write_status_file(target, {"schema": "status-checks.v1", "checks": [1]})
    assert json.loads(target.read_text(encoding="utf-8"))["checks"] == [1]
    assert stat.S_IMODE(os.stat(target).st_mode) == 0o644
    assert [p.name for p in target.parent.iterdir()] == ["feed_control.json"]
