from __future__ import annotations

import datetime as dt
import json

import pytest
from pydantic import ValidationError

from src.core.config import IntradayAdmissionSettings
from src.marketdata.kiwoom_ranking import KiwoomRankingRow
from src.universe.intraday import (
    AdmissionState,
    DynamicEntry,
    DynamicAdmissionsFileError,
    DynamicPlan,
    decide_admissions,
    feed_capacities,
    read_dynamic_admissions,
    state_from_ledger,
    write_dynamic_admissions,
)
from src.universe.membership import MembershipRow

_TV_200_EOK = 20_000_000_000


def _cfg(**overrides: object) -> IntradayAdmissionSettings:
    return IntradayAdmissionSettings(enabled=True, **overrides)


def _row(rank: int, symbol: str, change_pct: float, trade_value: int = _TV_200_EOK) -> KiwoomRankingRow:
    return KiwoomRankingRow(rank=rank, symbol=symbol, last_price=10_000, change_pct=change_pct, trade_value_krw=trade_value)


def _state(
    *,
    collected: frozenset[str] = frozenset(),
    ineligible: frozenset[str] = frozenset(),
    ls: int = 10,
    kis: int = 10,
) -> AdmissionState:
    return AdmissionState(
        collected=collected,
        ineligible=ineligible,
        capacity_left={"ls:1": ls, "kis:5": kis},
    )


def test_symmetric_qualification_admits_up_and_down_moves() -> None:
    decision = decide_admissions([_row(1, "000001", 6.0), _row(2, "000002", -6.0)], _state(), _cfg())

    assert {(a.symbol, a.direction) for a in decision.admitted} == {("000001", "up"), ("000002", "down")}
    assert decision.rejected == ()


def test_thresholds_are_inclusive() -> None:
    decision = decide_admissions(
        [_row(1, "000001", 5.0, trade_value=10_000_000_000)], _state(), _cfg(min_trade_value_krw=10_000_000_000)
    )

    assert [a.symbol for a in decision.admitted] == ["000001"]


def test_collected_symbols_are_skipped_silently() -> None:
    decision = decide_admissions(
        [_row(1, "000001", 6.0)], _state(collected=frozenset({"000001"})), _cfg()
    )

    assert decision.admitted == ()
    assert decision.rejected == ()


def test_fill_order_is_ls_then_kis_by_trade_value() -> None:
    rows = [
        _row(1, "000001", 6.0, trade_value=30_000_000_000),
        _row(2, "000002", 6.0, trade_value=20_000_000_000),
        _row(3, "000003", 6.0, trade_value=10_000_000_000),
    ]
    decision = decide_admissions(rows, _state(ls=1, kis=1), _cfg(min_trade_value_krw=10_000_000_000))

    assert [(a.symbol, a.feed) for a in decision.admitted] == [("000001", "ls:1"), ("000002", "kis:5")]
    assert decision.rejected == (("000003", "capacity"),)


def test_decision_is_deterministic_under_row_shuffle() -> None:
    rows = [_row(i + 1, f"00000{i}", 6.0, trade_value=_TV_200_EOK + (i % 2)) for i in range(6)]
    shuffled = rows[3:] + rows[:3][::-1]

    first = decide_admissions(rows, _state(ls=2, kis=2), _cfg())
    second = decide_admissions(shuffled, _state(ls=2, kis=2), _cfg())

    assert first == second


def test_ineligible_symbols_are_rejected_not_admitted() -> None:
    decision = decide_admissions(
        [_row(1, "000001", 6.0)], _state(ineligible=frozenset({"000001"})), _cfg()
    )

    assert decision.admitted == ()
    assert decision.rejected == (("000001", "ineligible"),)


def test_duplicate_symbols_keep_first_row() -> None:
    decision = decide_admissions([_row(1, "000001", 6.0), _row(2, "000001", -6.0)], _state(), _cfg())

    assert [(a.symbol, a.direction) for a in decision.admitted] == [("000001", "up")]


def test_feed_capacities_orders_ls_then_kis_slots_in_order() -> None:
    capacity = feed_capacities(ls_reserve_symbols=40, kis_slot_symbols=20, kis_slots=("3", "4"))

    assert list(capacity) == ["ls:1", "kis:3", "kis:4"]
    assert capacity == {"ls:1": 40, "kis:3": 20, "kis:4": 20}


def test_feed_capacities_without_kis_slots() -> None:
    assert feed_capacities(ls_reserve_symbols=40, kis_slot_symbols=20, kis_slots=()) == {"ls:1": 40}


def test_feed_capacities_clamps_negative() -> None:
    capacity = feed_capacities(ls_reserve_symbols=-1, kis_slot_symbols=-5, kis_slots=("3",))

    assert capacity == {"ls:1": 0, "kis:3": 0}


def test_admission_spills_to_second_kis_feed() -> None:
    rows = [
        _row(1, "000001", 6.0, trade_value=30_000_000_000),
        _row(2, "000002", 6.0, trade_value=20_000_000_000),
        _row(3, "000003", 6.0, trade_value=10_000_000_000),
    ]
    state = AdmissionState(
        collected=frozenset(),
        ineligible=frozenset(),
        capacity_left={"ls:1": 0, "kis:3": 1, "kis:4": 1},
    )
    decision = decide_admissions(rows, state, _cfg(min_trade_value_krw=10_000_000_000))

    assert [(a.symbol, a.feed) for a in decision.admitted] == [("000001", "kis:3"), ("000002", "kis:4")]
    assert decision.rejected == (("000003", "capacity"),)


def test_state_from_ledger_reduces_capacity_by_dynamic_rows_only() -> None:
    def dynamic(symbol: str) -> MembershipRow:
        return MembershipRow(
            symbol=symbol, tier="B", source="dynamic", decided_at_ns=1, direction="up", warning_level=None,
            ls_shard=1, kis_slot=None, trade_value_krw=1, reason=None,
        )

    rows = [
        MembershipRow("000100", "A", "static", 1, None, None, 0, None, None, None),
        *(dynamic(f"00020{i}") for i in range(5)),
        MembershipRow("000300", "C", "intraday_capacity", 1, "up", None, None, None, 1, None),
        MembershipRow("000400", "B", "ack", 1, None, None, 1, None, None, "accepted:00000"),
    ]

    state = state_from_ledger(rows, ineligible=frozenset(), capacity={"ls:1": 40, "kis:5": 20})

    assert state.capacity_left == {"ls:1": 35, "kis:5": 20}
    assert {"000100", "000200", "000201", "000202", "000203", "000204"} <= state.collected
    assert "000300" not in state.collected


def test_dynamic_plan_roundtrip_is_strictly_revisioned(tmp_path) -> None:
    path = tmp_path / "2026-11-03.dynamic.json"
    assert read_dynamic_admissions(path) == DynamicPlan(rev=0, entries=())
    plan = DynamicPlan(rev=1, entries=(DynamicEntry("005930", "ls:1", 10),))

    write_dynamic_admissions(path, plan)

    assert read_dynamic_admissions(path) == plan
    assert plan.symbols_for("ls:1") == ("005930",)
    with pytest.raises(DynamicAdmissionsFileError):
        write_dynamic_admissions(path, DynamicPlan(rev=1, entries=()))


def test_dynamic_plan_rejects_malformed_file(tmp_path) -> None:
    path = tmp_path / "bad.json"
    path.write_text(json.dumps({"rev": 1, "entries": [{"symbol": "abc", "feed": "ls:1", "decided_at_ns": 1}]}))

    with pytest.raises(DynamicAdmissionsFileError):
        read_dynamic_admissions(path)


def test_settings_defaults_are_inert() -> None:
    settings = IntradayAdmissionSettings()

    assert settings.enabled is False
    assert settings.admission_cutoff == dt.time(15, 15)
    assert settings.post_close_screen_at == dt.time(15, 31)
    assert settings.kis_dynamic_slots == ("5",)


@pytest.mark.parametrize("slots", [("3", "3"), ("6",)])
def test_settings_reject_invalid_kis_dynamic_slots(slots) -> None:
    with pytest.raises(ValidationError):
        IntradayAdmissionSettings(kis_dynamic_slots=slots)


def test_settings_cutoff_must_precede_closing_auction() -> None:
    with pytest.raises(ValidationError):
        IntradayAdmissionSettings(admission_cutoff=dt.time(15, 20))
    with pytest.raises(ValidationError):
        IntradayAdmissionSettings(admission_cutoff=dt.time(15, 16), post_close_screen_at=dt.time(15, 15))


def test_dynamic_row_without_feed_fails_closed() -> None:
    orphan = MembershipRow(
        symbol="000700", tier="B", source="dynamic", decided_at_ns=1, direction="up", warning_level=None,
        ls_shard=None, kis_slot=None, trade_value_krw=1, reason=None,
    )

    with pytest.raises(DynamicAdmissionsFileError, match="no feed"):
        state_from_ledger([orphan], ineligible=frozenset(), capacity={"ls:1": 1})


@pytest.mark.parametrize(
    "payload",
    [
        {"rev": -1, "entries": []},
        {"rev": True, "entries": []},
        {"rev": 1, "entries": "not-a-list"},
    ],
)
def test_dynamic_plan_rejects_invalid_envelope(tmp_path, payload) -> None:
    path = tmp_path / "bad-envelope.json"
    path.write_text(json.dumps(payload))

    with pytest.raises(DynamicAdmissionsFileError):
        read_dynamic_admissions(path)


def test_kis_dynamic_rows_reduce_only_the_kis_feed() -> None:
    kis_row = MembershipRow(
        symbol="005930", tier="B", source="dynamic", decided_at_ns=1, direction="down", warning_level=None,
        ls_shard=None, kis_slot="5", trade_value_krw=1, reason=None,
    )

    state = state_from_ledger([kis_row], ineligible=frozenset(), capacity={"ls:1": 40, "kis:5": 20})

    assert state.capacity_left == {"ls:1": 40, "kis:5": 19}
    assert "005930" in state.collected


def test_dynamic_entry_venue_roundtrip_and_default(tmp_path) -> None:
    path = tmp_path / "dyn.json"
    write_dynamic_admissions(
        path,
        DynamicPlan(
            rev=3,
            entries=(
                DynamicEntry("005930", "kis:5", 1, venue="nxt"),
                DynamicEntry("000660", "kis:5", 2),
            ),
        ),
    )
    plan = read_dynamic_admissions(path)
    assert [entry.venue for entry in plan.entries] == ["nxt", "krx"]
    assert plan.pairs_for("kis:5", "nxt") == ("005930",)
    assert plan.pairs_for("kis:5", "krx") == ("000660",)
    assert plan.symbols_for("kis:5") == ("005930", "000660")

    legacy = tmp_path / "legacy.json"
    legacy.write_text(
        json.dumps({"rev": 1, "entries": [{"symbol": "005930", "feed": "kis:5", "decided_at_ns": 1}]}),
        encoding="utf-8",
    )
    assert read_dynamic_admissions(legacy).entries == (DynamicEntry("005930", "kis:5", 1, venue="krx"),)


@pytest.mark.parametrize("venue", ["KRX", "kosdaq", ""])
def test_dynamic_plan_rejects_unknown_venue(tmp_path, venue) -> None:
    path = tmp_path / "dyn.json"
    path.write_text(
        json.dumps({"rev": 1, "entries": [{"symbol": "005930", "feed": "kis:5", "decided_at_ns": 1, "venue": venue}]}),
        encoding="utf-8",
    )
    with pytest.raises(DynamicAdmissionsFileError, match="invalid venue"):
        read_dynamic_admissions(path)
