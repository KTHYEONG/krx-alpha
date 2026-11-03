from __future__ import annotations

import datetime as dt

import polars as pl
import pytest

from src.core.config import UniverseTierSettings
from src.universe.policy import (
    compute_selection_features,
    select_follow_up,
    select_universe_detailed,
    select_universe_tiered,
)

BASE = dt.date(2026, 1, 5)
SESSIONS = 60
DECISION = BASE + dt.timedelta(days=SESSIONS - 1)


def _frame(
    symbol: str,
    changes: list[float],
    *,
    tv: float | list[float] = 100.0,
    cert: str = "보통주",
    closes: list[float] | None = None,
) -> pl.DataFrame:
    n = len(changes)
    if closes is None:
        closes = [1000.0]
        for pct in changes[1:]:
            closes.append(closes[-1] * (1 + pct / 100))
    tvs = [float(tv)] * n if isinstance(tv, (int, float)) else list(tv)
    return pl.DataFrame({
        "date": [BASE + dt.timedelta(days=i) for i in range(n)],
        "symbol": [symbol] * n,
        "close": closes,
        "volume": [1000] * n,
        "trade_value_100m": tvs,
        "daily_change_pct": changes,
        "stock_cert_kind": [cert] * n,
        "section": [""] * n,
    })


def _decision_only(symbol: str, change: float, *, tv: float = 100.0, cert: str = "보통주") -> pl.DataFrame:
    changes = [0.0] * (SESSIONS - 1) + [change]
    tvs = [100.0] * (SESSIONS - 1) + [tv]
    return _frame(symbol, changes, tv=tvs, cert=cert)


def _market(*frames: pl.DataFrame) -> pl.DataFrame:
    return compute_selection_features(pl.concat(frames))


def _reasons(selected: pl.DataFrame, symbol: str) -> set[str]:
    row = selected.filter(pl.col("symbol") == symbol)
    assert row.height == 1
    return set(row["selection_reasons"][0])


def test_downside_disabled_is_a_no_op_regression() -> None:
    featured = _market(
        _decision_only("UP1", 12.0),
        _decision_only("UP2", 29.9, tv=1000.0),
        _decision_only("DN1", -12.0),
    )

    result = select_universe_detailed(featured, DECISION, slot_budget=100, downside=False)

    assert result.selected["symbol"].to_list() == ["UP1", "UP2"]
    assert result.dropped.height == 0
    assert result.selected.filter(pl.col("symbol") == "UP1")["selection_reasons"][0].to_list() == [
        "surge10",
        "newhigh60",
    ]
    assert result.selected.filter(pl.col("symbol") == "UP2")["selection_reasons"][0].to_list() == [
        "limit_up",
        "surge10",
        "volsurge",
        "newhigh60",
    ]
    assert set(result.selected["direction"].to_list()) <= {"up", None}


def test_plunge_qualifies_symmetrically() -> None:
    featured = _market(_decision_only("DN1", -12.0))

    result = select_universe_detailed(featured, DECISION, slot_budget=100, downside=True)

    assert result.selected["symbol"].to_list() == ["DN1"]
    assert "plunge10" in _reasons(result.selected, "DN1")
    assert result.selected["direction"].to_list() == ["down"]


def test_limit_down_carries_both_down_reasons() -> None:
    featured = _market(_decision_only("DN1", -29.5))

    result = select_universe_detailed(featured, DECISION, slot_budget=100, downside=True)

    assert {"limit_down", "plunge10"} <= _reasons(result.selected, "DN1")


def test_downside_respects_liquidity_floor() -> None:
    featured = _market(_decision_only("THIN", -15.0, tv=40.0))

    result = select_universe_detailed(featured, DECISION, slot_budget=100, downside=True)

    assert result.selected.height == 0


def test_preferred_share_never_qualifies_on_downside() -> None:
    featured = _market(_decision_only("PREF", -20.0, cert="우선주"))

    result = select_universe_detailed(featured, DECISION, slot_budget=100, downside=True)

    assert result.selected.height == 0


def test_new_low_uses_split_adjusted_history() -> None:
    # Reverse split at row 40 doubles the price basis: raw history is 500 before and 1000 after.
    # On the adjusted basis every prior close is 1000, so a 940 close is a genuine 60-session low.
    closes = [500.0] * 40 + [1000.0] * 19 + [940.0]
    changes = [0.0] * 59 + [-6.0]
    featured = _market(_frame("SPLIT", changes, closes=closes))

    result = select_universe_detailed(featured, DECISION, slot_budget=100, downside=True)

    assert "newlow60" in _reasons(result.selected, "SPLIT")
    assert featured.filter(pl.col("date") == DECISION)["close_min_60"][0] == pytest.approx(940.0)
    raw_prior_min = min(closes[:-1])
    assert not (closes[-1] <= raw_prior_min)


def test_close_min_is_causal_under_future_perturbation() -> None:
    base_changes = [0.0] + [((i * 37) % 19 - 9) * 0.3 for i in range(1, SESSIONS)]
    cut = 40
    perturbed_changes = base_changes[: cut + 1] + [((i * 53) % 21 - 10) * 0.9 for i in range(cut + 1, SESSIONS)]
    day = BASE + dt.timedelta(days=cut)

    before = compute_selection_features(_frame("X", base_changes))
    after = compute_selection_features(_frame("X", perturbed_changes))

    assert before.filter(pl.col("date") == day)["close_min_60"][0] == pytest.approx(
        after.filter(pl.col("date") == day)["close_min_60"][0]
    )


def test_row_qualifying_both_directions_is_rejected(monkeypatch) -> None:
    from src.universe import policy

    monkeypatch.setattr(policy, "SURGE_PCT", -5.0)
    featured = _market(_decision_only("FLAT", 0.0))

    with pytest.raises(ValueError, match="both upside and downside"):
        policy._event_qualifying(featured.filter(pl.col("date") == DECISION), downside=True)


def test_direction_neutral_ranking_under_budget() -> None:
    frames = [
        _frame("CORE", [0.0] * SESSIONS, tv=10000.0),
        _decision_only("UP_A", 12.0, tv=600.0),
        _decision_only("UP_B", 12.0, tv=300.0),
        _decision_only("UP_C", 12.0, tv=150.0),
        _decision_only("DN_A", -12.0, tv=500.0),
        _decision_only("DN_B", -12.0, tv=400.0),
        _decision_only("DN_C", -12.0, tv=250.0),
    ]
    featured = _market(*frames)
    tiers = UniverseTierSettings(
        core_enabled=True,
        core_enter_rank=1,
        core_exit_rank=1,
        core_warmup_sessions=1,
        follow_sessions=0,
        downside_enabled=True,
    )

    result = select_universe_tiered(featured, DECISION, slot_budget=5, tiers=tiers)

    events = result.selected.filter(pl.col("tier") == "event")
    assert events.sort("trade_value_100m", descending=True)["symbol"].to_list() == ["UP_A", "DN_A", "DN_B", "UP_B"]
    assert set(result.dropped["symbol"].to_list()) == {"DN_C", "UP_C"}


def test_follow_inherits_latest_direction() -> None:
    changes = [0.0] * SESSIONS
    changes[SESSIONS - 4] = 12.0
    changes[SESSIONS - 2] = -12.0
    featured = _market(_frame("FLIP", changes), _decision_only("TODAY", 0.0))

    got = select_follow_up(featured, DECISION, follow_sessions=3, downside=True)

    row = got.filter(pl.col("symbol") == "FLIP")
    assert row["follow_age"].to_list() == [1]
    assert row["direction"].to_list() == ["down"]
