from __future__ import annotations

import json

import pytest

from src.orchestration.feed_control.model import (
    ControllerState,
    FeedStateError,
    GroupRuntime,
    GroupState,
)
from tests.unit.orchestration.feed_control.replay import DAY, T0


def test_state_json_roundtrip() -> None:
    state = ControllerState(
        day=DAY,
        plan_rev=3,
        groups={
            "ls:0": GroupRuntime(
                state=GroupState.SECONDARY_ACTIVE,
                since_wall=T0,
                healthy_since_wall=None,
                abnormal_since_wall=T0 + 1,
                unknown_since_wall=None,
                relapses=1,
                degraded_s=12.5,
                admitted={"db:0": ("005930", "000660")},
                paged_reasons=("group_down",),
                recycled_feeds=("db:0",),
            )
        },
        last_step_wall=T0 + 2,
    )
    assert ControllerState.from_json(json.loads(json.dumps(state.to_json()))) == state


_GARBAGE = {
    "schema": lambda d: d.update(schema_version=2),
    "unknown_enum": lambda d: d["groups"]["ls:0"].update(state="BOGUS_STATE"),
    "negative_number": lambda d: d["groups"]["ls:0"].update(degraded_s=-1.0),
    "bool_as_int": lambda d: d.update(plan_rev=True),
    "unsafe_group_id": lambda d: d["groups"].update({"LS:0 x": d["groups"]["ls:0"]}),
    "symbol_type": lambda d: d["groups"]["ls:0"]["admitted"].update({"db:0": [42]}),
    "reason_token": lambda d: d["groups"]["ls:0"].update(paged_reasons=["GROUP_DOWN!"]),
    "bad_day": lambda d: d.update(day="2026-13-40"),
    "missing_field": lambda d: d["groups"]["ls:0"].pop("relapses"),
    "state_type": lambda d: d["groups"]["ls:0"].update(state=5),
    "symbols_not_list": lambda d: d["groups"]["ls:0"]["admitted"].update({"db:0": "005930"}),
    "groups_not_object": lambda d: d.update(groups="LS:0 x"),
    "bool_as_number": lambda d: d["groups"]["ls:0"].update(since_wall=True),
    "day_type": lambda d: d.update(day=20261116),
    "reasons_not_list": lambda d: d["groups"]["ls:0"].update(paged_reasons="GROUP_DOWN!"),
}


@pytest.mark.parametrize("mutate", list(_GARBAGE.values()), ids=list(_GARBAGE))
def test_state_json_rejects_garbage_without_echo(mutate) -> None:
    raw = json.loads(json.dumps(_valid_raw()))
    mutate(raw)
    with pytest.raises(FeedStateError) as info:
        ControllerState.from_json(raw)
    message = str(info.value)
    for leaked in ("BOGUS_STATE", "LS:0 x", "GROUP_DOWN!", "2026-13-40", "-1.0", "42"):
        assert leaked not in message


def _valid_raw() -> dict:
    state = ControllerState(
        day=DAY,
        plan_rev=0,
        groups={
            "ls:0": GroupRuntime(
                state=GroupState.PRIMARY_OK,
                since_wall=T0,
                healthy_since_wall=T0,
                abnormal_since_wall=None,
                unknown_since_wall=None,
                relapses=0,
                degraded_s=0.0,
                admitted={},
                paged_reasons=(),
                recycled_feeds=(),
            )
        },
        last_step_wall=None,
    )
    return state.to_json()
