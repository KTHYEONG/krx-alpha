from __future__ import annotations

import datetime as dt

import polars as pl
import pytest

from src.core.config import CollectorSettings, UniverseTierSettings, resolve_collector_runtime
from src.core.errors import SlotBudgetExceededError
from src.universe.policy import (
    compute_selection_features,
    select_core_panel,
    select_follow_up,
    select_universe_detailed,
    select_universe_tiered,
)


def _core_frame(ranks_by_date: dict[dt.date, dict[str, float]]) -> pl.DataFrame:
    rows = []
    for day, mapping in ranks_by_date.items():
        for symbol, tv_med in mapping.items():
            rows.append({"date": day, "symbol": symbol, "tv_median_20": tv_med})
    return pl.DataFrame(rows)


def _rank_map(symbols: list[str], target: str, rank: int, top_tv: float = 1000.0) -> dict[str, float]:
    mapping: dict[str, float] = {}
    others = [s for s in symbols if s != target]
    for i, s in enumerate(others[: rank - 1]):
        mapping[s] = top_tv - i
    mapping[target] = top_tv - (rank - 1) - 0.5
    for i, s in enumerate(others[rank - 1 :]):
        mapping[s] = top_tv - (rank - 1) - 1.0 - i
    return mapping


def test_core_hysteresis_keeps_boundary_names() -> None:
    symbols = [f"S{i:03d}" for i in range(80)]
    symbols.append("TGT")
    dates = [dt.date(2026, 3, 2) + dt.timedelta(days=i) for i in range(4)]
    wanted = [58, 70, 74, 76]
    frame = _core_frame({d: _rank_map(symbols, "TGT", r) for d, r in zip(dates, wanted, strict=True)})
    for i, day in enumerate(dates):
        got = select_core_panel(
            frame.filter(pl.col("date") <= day), day, enter_rank=60, exit_rank=75, warmup_sessions=4
        )
        assert ("TGT" in got["symbol"].to_list()) == (i < 3)


def test_core_entry_requires_enter_rank() -> None:
    symbols = [f"S{i:03d}" for i in range(80)]
    symbols.append("TGT")
    dates = [dt.date(2026, 3, 2) + dt.timedelta(days=i) for i in range(4)]
    frame = _core_frame({d: _rank_map(symbols, "TGT", 70) for d in dates})
    for day in dates:
        got = select_core_panel(
            frame.filter(pl.col("date") <= day), day, enter_rank=60, exit_rank=75, warmup_sessions=4
        )
        assert "TGT" not in got["symbol"].to_list()


def test_core_is_point_in_time() -> None:
    frame = pl.DataFrame({
        "date": [dt.date(2026, 3, 3)],
        "symbol": ["000001"],
        "tv_median_20": [100.0],
    })
    with pytest.raises(ValueError, match="lookahead"):
        select_core_panel(frame, dt.date(2026, 3, 2), enter_rank=60, exit_rank=75, warmup_sessions=4)


def test_core_rejects_bad_ranks() -> None:
    frame = pl.DataFrame({
        "date": [dt.date(2026, 3, 3)],
        "symbol": ["000001"],
        "tv_median_20": [100.0],
    })
    with pytest.raises(ValueError, match="core_exit_rank"):
        select_core_panel(frame, dt.date(2026, 3, 3), enter_rank=60, exit_rank=59, warmup_sessions=4)
    with pytest.raises(ValueError, match=">= 1"):
        select_core_panel(frame, dt.date(2026, 3, 3), enter_rank=0, exit_rank=75, warmup_sessions=4)
    with pytest.raises(ValueError, match="warmup_sessions"):
        select_core_panel(frame, dt.date(2026, 3, 3), enter_rank=60, exit_rank=75, warmup_sessions=0)
    with pytest.raises(ValueError, match="missing required columns"):
        select_core_panel(
            pl.DataFrame({"date": [dt.date(2026, 3, 3)]}),
            dt.date(2026, 3, 3),
            enter_rank=60,
            exit_rank=75,
            warmup_sessions=4,
        )


def test_future_perturbation_invariance() -> None:
    n = 65
    base = dt.date(2026, 1, 5)
    decision = base + dt.timedelta(days=n - 2)
    bars = pl.DataFrame({
        "date": [base + dt.timedelta(days=i) for i in range(n)],
        "symbol": ["000001"] * n,
        "close": [1000.0] * (n - 1) + [1300.0],
        "volume": [1000] * n,
        "trade_value_100m": [100.0] * (n - 1) + [1000.0],
        "daily_change_pct": [0.0] * (n - 1) + [29.9],
    })
    perturbed = bars.with_columns([
        pl.when(pl.col("date") > decision)
        .then(pl.lit(9999.0))
        .otherwise(pl.col("trade_value_100m"))
        .alias("trade_value_100m")
    ])
    tiers = UniverseTierSettings(core_enabled=True)
    before = select_universe_tiered(
        compute_selection_features(bars).filter(pl.col("date") <= decision),
        decision,
        slot_budget=100,
        tiers=tiers,
    )
    after = select_universe_tiered(
        compute_selection_features(perturbed).filter(pl.col("date") <= decision),
        decision,
        slot_budget=100,
        tiers=tiers,
    )
    assert before.selected.height == 1
    assert before.selected.to_dicts() == after.selected.to_dicts()


def test_core_size_bound() -> None:
    symbols = [f"S{i:03d}" for i in range(100)]
    dates = [dt.date(2026, 3, 2) + dt.timedelta(days=i) for i in range(20)]
    rows = []
    for day_idx, day in enumerate(dates):
        for i, s in enumerate(symbols):
            rows.append({"date": day, "symbol": s, "tv_median_20": float((i * 37 + day_idx * 11) % 100)})
    frame = pl.DataFrame(rows)
    got = select_core_panel(frame, dates[-1], enter_rank=60, exit_rank=75, warmup_sessions=20)
    assert got.height <= 75


def test_core_short_warmup_warns(caplog) -> None:
    import logging

    frame = pl.DataFrame({
        "date": [dt.date(2026, 3, 3)],
        "symbol": ["000001"],
        "tv_median_20": [100.0],
    })
    with caplog.at_level(logging.WARNING, logger="src.universe.policy"):
        got = select_core_panel(frame, dt.date(2026, 3, 3), enter_rank=60, exit_rank=75, warmup_sessions=20)
    assert got.height == 1
    assert any("SHORT_WARMUP" in rec.message for rec in caplog.records)
    empty = select_core_panel(
        frame.filter(pl.col("date") > dt.date(2026, 3, 3)),
        dt.date(2026, 3, 3),
        enter_rank=60,
        exit_rank=75,
        warmup_sessions=4,
    )
    assert empty.height == 0


def _event_featured(entries: list[tuple[dt.date, str, float, float]]) -> pl.DataFrame:
    rows = []
    for day, symbol, tv, change in entries:
        rows.append({
            "date": day,
            "symbol": symbol,
            "close": 1300.0 if change >= 29.0 else 1000.0,
            "volume": 1000,
            "trade_value_100m": tv,
            "daily_change_pct": change,
            "tv_median_20": 100.0,
            "close_max_60": 1000.0,
            "tv_ratio": 6.0 if tv >= 500 else 1.0,
            "section": "",
            "stock_cert_kind": "보통주",
        })
    return pl.DataFrame(rows)


def test_rank_key_value_branches() -> None:
    from src.universe.policy import _rank_key_value

    assert _rank_key_value(True) == float("-inf")
    assert _rank_key_value(None) == float("-inf")
    assert _rank_key_value(float("nan")) == float("-inf")
    assert _rank_key_value(5.0) == 5.0


def test_follow_up_ages_and_drop_outs() -> None:
    t3, t2, t1, t0 = (dt.date(2026, 3, 2) + dt.timedelta(days=i) for i in range(4))
    frame = _event_featured([
        (t3, "A", 1000.0, 29.9),
        (t1, "A", 1000.0, 29.9),
        (t2, "B", 1000.0, 29.9),
        (t0, "A", 100.0, 0.0),
    ])
    got = select_follow_up(frame, t0, follow_sessions=3)
    assert got.filter(pl.col("symbol") == "A")["follow_age"].to_list() == [1]
    assert "B" not in got["symbol"].to_list()
    assert select_follow_up(frame, t0, follow_sessions=0).height == 0
    with pytest.raises(ValueError, match="follow_sessions"):
        select_follow_up(frame, t0, follow_sessions=-1)


def test_follow_up_empty_branches() -> None:
    t0 = dt.date(2026, 3, 10)
    only_today = _event_featured([(t0, "A", 100.0, 0.0)])
    assert select_follow_up(only_today, t0, follow_sessions=3).height == 0
    t1 = t0 - dt.timedelta(days=1)
    no_event = _event_featured([(t1, "A", 100.0, 0.0), (t0, "A", 100.0, 0.0)])
    assert select_follow_up(no_event, t0, follow_sessions=1).height == 0
    thin_today = _event_featured([(t1, "A", 1000.0, 29.9), (t0, "A", 10.0, 0.0)])
    assert select_follow_up(thin_today, t0, follow_sessions=1).height == 0


def test_core_empty_day_resets_membership() -> None:
    t1 = dt.date(2026, 3, 9)
    t0 = dt.date(2026, 3, 10)
    frame = pl.DataFrame([
        {"date": t1, "symbol": "A", "tv_median_20": None},
        {"date": t0, "symbol": "A", "tv_median_20": 100.0},
        {"date": t0, "symbol": "B", "tv_median_20": 50.0},
    ])
    got = select_core_panel(frame, t0, enter_rank=60, exit_rank=75, warmup_sessions=2)
    assert "A" in got["symbol"].to_list()
    all_null = pl.DataFrame([
        {"date": t1, "symbol": "A", "tv_median_20": None},
        {"date": t0, "symbol": "A", "tv_median_20": None},
    ])
    assert select_core_panel(all_null, t0, enter_rank=60, exit_rank=75, warmup_sessions=2).height == 0


def test_follow_up_lookahead() -> None:
    frame = _event_featured([(dt.date(2026, 3, 4), "A", 1000.0, 29.9)])
    with pytest.raises(ValueError, match="lookahead"):
        select_follow_up(frame, dt.date(2026, 3, 3), follow_sessions=3)


def test_tier_precedence_and_reason_union() -> None:
    t0 = dt.date(2026, 3, 5)
    t1 = t0 - dt.timedelta(days=1)
    symbols = [f"S{i:03d}" for i in range(70)]
    rows = []
    for day in (t1, t0):
        for i, s in enumerate(symbols):
            rows.append({"date": day, "symbol": s, "tv_median_20": 1000.0 - i,
                         "close": 1000.0, "volume": 1000, "trade_value_100m": 100.0,
                         "daily_change_pct": 0.0, "close_max_60": 1000.0, "tv_ratio": 1.0})
    rows.append({
        "date": t0, "symbol": "A", "tv_median_20": 5000.0,
        "close": 1300.0, "volume": 1000, "trade_value_100m": 900.0,
        "daily_change_pct": 29.9, "close_max_60": 1000.0, "tv_ratio": 9.0,
    })
    rows.append({
        "date": t1, "symbol": "A", "tv_median_20": 5000.0,
        "close": 1000.0, "volume": 1000, "trade_value_100m": 100.0,
        "daily_change_pct": 0.0, "close_max_60": 1000.0, "tv_ratio": 1.0,
    })
    frame = pl.DataFrame(rows)
    tiers = UniverseTierSettings(
        core_enabled=True, core_enter_rank=60, core_exit_rank=75, core_warmup_sessions=2, follow_sessions=0
    )
    got = select_universe_tiered(frame, t0, slot_budget=100, tiers=tiers)
    row = got.selected.filter(pl.col("symbol") == "A").to_dicts()[0]
    assert row["tier"] == "core"
    assert "core" in row["selection_reasons"]
    assert "limit_up" in row["selection_reasons"]


def test_priority_order_and_truncation(caplog) -> None:
    import logging

    t0 = dt.date(2026, 3, 10)
    t1 = t0 - dt.timedelta(days=1)
    t2 = t0 - dt.timedelta(days=2)
    core_syms = [f"C{i:03d}" for i in range(60)]
    event_syms = [f"E{i:03d}" for i in range(50)]
    follow_syms = [f"F{i:03d}" for i in range(10)]
    rows = []
    for day in (t2, t1, t0):
        for i, s in enumerate(core_syms):
            rows.append({"date": day, "symbol": s, "tv_median_20": 5000.0 - i,
                         "close": 1000.0, "volume": 1000, "trade_value_100m": 100.0,
                         "daily_change_pct": 0.0, "close_max_60": 1000.0, "tv_ratio": 1.0})
        for i, s in enumerate(event_syms):
            med = 100.0 - i * 0.01
            if day == t0:
                rows.append({"date": day, "symbol": s, "tv_median_20": med,
                             "close": 1300.0, "volume": 1000, "trade_value_100m": 900.0 - i,
                             "daily_change_pct": 29.9, "close_max_60": 1000.0, "tv_ratio": 9.0})
            else:
                rows.append({"date": day, "symbol": s, "tv_median_20": med,
                             "close": 1000.0, "volume": 1000, "trade_value_100m": 100.0,
                             "daily_change_pct": 0.0, "close_max_60": 1000.0, "tv_ratio": 1.0})
        for s in follow_syms:
            if day == t1:
                rows.append({"date": day, "symbol": s, "tv_median_20": 10.0,
                             "close": 1300.0, "volume": 1000, "trade_value_100m": 800.0,
                             "daily_change_pct": 29.9, "close_max_60": 1000.0, "tv_ratio": 8.0})
            else:
                rows.append({"date": day, "symbol": s, "tv_median_20": 10.0,
                             "close": 1000.0, "volume": 1000, "trade_value_100m": 100.0,
                             "daily_change_pct": 0.0, "close_max_60": 1000.0, "tv_ratio": 1.0})
    frame = pl.DataFrame(rows)
    tiers = UniverseTierSettings(
        core_enabled=True, core_enter_rank=60, core_exit_rank=60, core_warmup_sessions=2, follow_sessions=3
    )
    with caplog.at_level(logging.WARNING, logger="src.universe.policy"):
        got = select_universe_tiered(frame, t0, slot_budget=100, tiers=tiers)
    assert got.selected.height == 100
    assert got.selected["tier"].to_list()[:60] == ["core"] * 60
    assert got.selected["core_rank"].to_list()[:60] == list(range(1, 61))
    assert got.selected["tier"].to_list()[60:] == ["event"] * 40
    assert got.dropped.height == 20
    assert sorted(got.dropped["tier"].to_list()) == ["event"] * 10 + ["follow"] * 10
    warnings = [rec for rec in caplog.records if "TRUNCATED" in rec.message]
    assert len(warnings) == 1
    assert "tier_dropped=event:10,follow:10" in warnings[0].message


def test_disabled_path_is_legacy_identical() -> None:
    from polars.testing import assert_frame_equal

    t0 = dt.date(2026, 3, 10)
    frame = _event_featured([(t0, "A", 900.0, 29.9), (t0, "B", 100.0, 1.0)])
    tiers = UniverseTierSettings(core_enabled=False)
    assert_frame_equal(
        select_universe_tiered(frame, t0, slot_budget=100, tiers=tiers).selected,
        select_universe_detailed(frame, t0, slot_budget=100).selected,
    )


def test_excluded_securities_never_rank() -> None:
    t1 = dt.date(2026, 3, 9)
    t0 = dt.date(2026, 3, 10)
    rows = []
    for day in (t1, t0):
        rows.append({"date": day, "symbol": "BAD1", "tv_median_20": 99999.0,
                     "close": 1300.0, "volume": 1000, "trade_value_100m": 99999.0,
                     "daily_change_pct": 29.9, "close_max_60": 1000.0, "tv_ratio": 9.0,
                     "section": "관리종목(소속부없음)", "stock_cert_kind": "보통주"})
        rows.append({"date": day, "symbol": "BAD2", "tv_median_20": 99998.0,
                     "close": 1300.0, "volume": 1000, "trade_value_100m": 99998.0,
                     "daily_change_pct": 29.9, "close_max_60": 1000.0, "tv_ratio": 9.0,
                     "section": "", "stock_cert_kind": "구형우선주"})
        rows.append({"date": day, "symbol": "GOOD", "tv_median_20": 1.0,
                     "close": 1000.0, "volume": 1000, "trade_value_100m": 100.0,
                     "daily_change_pct": 0.0, "close_max_60": 1000.0, "tv_ratio": 1.0,
                     "section": "", "stock_cert_kind": "보통주"})
    frame = pl.DataFrame(rows)
    assert "BAD1" not in select_core_panel(frame, t0, enter_rank=60, exit_rank=75, warmup_sessions=2)["symbol"].to_list()
    tiers = UniverseTierSettings(
        core_enabled=True, core_enter_rank=60, core_exit_rank=75, core_warmup_sessions=2, follow_sessions=1,
    )
    got = select_universe_tiered(frame, t0, slot_budget=100, tiers=tiers)
    assert "BAD1" not in got.selected["symbol"].to_list()
    assert "BAD2" not in got.selected["symbol"].to_list()


def test_tiered_rejects_bad_budget_and_ranks() -> None:
    frame = _event_featured([(dt.date(2026, 3, 10), "A", 900.0, 29.9)])
    tiers = UniverseTierSettings(
        core_enabled=True, core_enter_rank=60, core_exit_rank=75, core_warmup_sessions=2, follow_sessions=1
    )
    with pytest.raises(ValueError, match="slot_budget"):
        select_universe_tiered(frame, dt.date(2026, 3, 10), slot_budget=0, tiers=tiers)
    with pytest.raises(SlotBudgetExceededError, match="exceeds"):
        select_universe_tiered(frame, dt.date(2026, 3, 10), slot_budget=70, tiers=tiers)
    with pytest.raises(ValueError, match="lookahead"):
        select_universe_tiered(frame, dt.date(2026, 3, 9), slot_budget=100, tiers=tiers)
    with pytest.raises(ValueError, match="missing required columns"):
        select_universe_tiered(
            frame.drop("close"), dt.date(2026, 3, 10), slot_budget=100, tiers=tiers
        )
    bad = UniverseTierSettings.model_construct(
        core_enabled=True, core_enter_rank=60, core_exit_rank=59,
        core_warmup_sessions=2, follow_sessions=1,
    )
    with pytest.raises(ValueError, match="core_exit_rank"):
        select_universe_tiered(frame, dt.date(2026, 3, 10), slot_budget=100, tiers=bad)


def test_tiered_empty_selection() -> None:
    t0 = dt.date(2026, 3, 10)
    t1 = t0 - dt.timedelta(days=1)
    frame = pl.DataFrame([
        {"date": t1, "symbol": "A", "tv_median_20": 100.0, "close": 1000.0, "volume": 1000,
         "trade_value_100m": 100.0, "daily_change_pct": 0.0, "close_max_60": 1000.0, "tv_ratio": 1.0,
         "section": "관리종목(소속부없음)", "stock_cert_kind": "보통주"},
        {"date": t0, "symbol": "A", "tv_median_20": 100.0, "close": 1000.0, "volume": 1000,
         "trade_value_100m": 100.0, "daily_change_pct": 0.0, "close_max_60": 1000.0, "tv_ratio": 1.0,
         "section": "관리종목(소속부없음)", "stock_cert_kind": "보통주"},
    ])
    tiers = UniverseTierSettings(
        core_enabled=True, core_enter_rank=60, core_exit_rank=75, core_warmup_sessions=2, follow_sessions=1
    )
    got = select_universe_tiered(frame, t0, slot_budget=100, tiers=tiers)
    assert got.selected.height == 0
    assert got.selected.columns == [
        "decision_date", "symbol", "selection_reasons", "daily_change_pct",
        "trade_value_100m", "tv_ratio", "tier", "core_rank",
    ]


def test_event_follow_overlap_adds_follow_age() -> None:
    t1 = dt.date(2026, 3, 9)
    t0 = dt.date(2026, 3, 10)
    frame = _event_featured([
        (t1, "Z9", 1000.0, 29.9),
        (t0, "Z9", 900.0, 29.9),
        (t1, "A", 100.0, 0.0),
        (t0, "A", 100.0, 0.0),
    ])
    tiers = UniverseTierSettings(
        core_enabled=True, core_enter_rank=1, core_exit_rank=1, core_warmup_sessions=1, follow_sessions=1
    )
    got = select_universe_tiered(frame, t0, slot_budget=100, tiers=tiers)
    row = got.selected.filter(pl.col("symbol") == "Z9").to_dicts()[0]
    assert row["tier"] == "event"
    assert any(str(r).startswith("follow:") for r in row["selection_reasons"])


def test_tier_settings_validation_and_runtime_crosscheck() -> None:
    with pytest.raises(ValueError, match="core_exit_rank"):
        UniverseTierSettings(core_enabled=True, core_enter_rank=60, core_exit_rank=59)
    collector = CollectorSettings(universe_slot_budget=70)
    tiers = UniverseTierSettings(
        core_enabled=True, core_enter_rank=60, core_exit_rank=75, core_warmup_sessions=2, follow_sessions=1
    )
    with pytest.raises(SlotBudgetExceededError, match="core_exit_rank"):
        resolve_collector_runtime(collector=collector, universe_tiers=tiers)
    ok = resolve_collector_runtime(
        collector=CollectorSettings(universe_slot_budget=80),
        universe_tiers=UniverseTierSettings(core_enabled=True, core_enter_rank=60, core_exit_rank=75),
    )
    assert ok.universe_tiers.core_enabled is True


def test_service_tiered_persists_priority_order_and_ipc_tier(tmp_path) -> None:
    import json

    from src.marketdata.bar_store import append_daily_bars
    from src.universe.service import plan_universe

    n = 65
    base = dt.date(2026, 1, 5)
    decision = base + dt.timedelta(days=n - 1)
    core_syms = [f"9{i:05d}" for i in range(5)]
    rows = {"date": [], "symbol": [], "close": [], "volume": [], "trade_value_100m": [], "daily_change_pct": []}
    for s in core_syms:
        for i in range(n):
            rows["date"].append(base + dt.timedelta(days=i))
            rows["symbol"].append(s)
            rows["close"].append(1000.0)
            rows["volume"].append(1000)
            rows["trade_value_100m"].append(100.0)
            rows["daily_change_pct"].append(0.0)
    rows["date"].append(decision)
    rows["symbol"].append("999999")
    rows["close"].append(1300.0)
    rows["volume"].append(1000)
    rows["trade_value_100m"].append(900.0)
    rows["daily_change_pct"].append(29.9)
    for i in range(n - 1):
        rows["date"].append(base + dt.timedelta(days=i))
        rows["symbol"].append("999999")
        rows["close"].append(1000.0)
        rows["volume"].append(1000)
        rows["trade_value_100m"].append(100.0)
        rows["daily_change_pct"].append(0.0)
    bars_root = tmp_path / "bars"
    append_daily_bars(bars_root, pl.DataFrame(rows))
    tiers = UniverseTierSettings(
        core_enabled=True, core_enter_rank=5, core_exit_rank=5, core_warmup_sessions=20, follow_sessions=0
    )
    out = tmp_path / "universe.parquet"
    cand = tmp_path / "candidates.json"
    result = plan_universe(
        bars_root=bars_root,
        decision_date=decision,
        lookback_calendar_days=150,
        out_path=out,
        slot_budget=40,
        candidates_path=cand,
        tiers=tiers,
    )
    assert result.selected == 6
    stored = pl.read_parquet(out)
    assert "tier" in stored.columns
    assert "core_rank" in stored.columns
    payload = json.loads(cand.read_text(encoding="utf-8"))
    assert [c["symbol"] for c in payload["candidates"]] == stored["symbol"].to_list()
    assert all("tier" in c for c in payload["candidates"])
    assert payload["candidates"][0]["tier"] == "core"


def test_service_legacy_payload_unchanged(tmp_path) -> None:
    import json

    from src.marketdata.bar_store import append_daily_bars
    from src.universe.service import plan_universe

    n = 60
    base = dt.date(2026, 1, 5)
    decision = base + dt.timedelta(days=n - 1)
    bars_root = tmp_path / "bars"
    append_daily_bars(bars_root, pl.DataFrame({
        "date": [base + dt.timedelta(days=i) for i in range(n)],
        "symbol": ["000001"] * n,
        "close": [1000.0] * (n - 1) + [1300.0],
        "volume": [1000] * n,
        "trade_value_100m": [100.0] * (n - 1) + [1000.0],
        "daily_change_pct": [0.0] * (n - 1) + [29.9],
    }))
    plan_universe(
        bars_root=bars_root,
        decision_date=decision,
        lookback_calendar_days=150,
        out_path=tmp_path / "universe.parquet",
        slot_budget=40,
        candidates_path=tmp_path / "candidates.json",
        tiers=UniverseTierSettings(core_enabled=False),
    )
    payload = json.loads((tmp_path / "candidates.json").read_text(encoding="utf-8"))
    assert all("tier" not in c and "core_rank" not in c for c in payload["candidates"])
    plan_universe(
        bars_root=bars_root,
        decision_date=decision,
        lookback_calendar_days=150,
        out_path=tmp_path / "legacy.parquet",
        slot_budget=40,
        candidates_path=tmp_path / "legacy.json",
    )
    assert (tmp_path / "candidates.json").read_bytes() == (tmp_path / "legacy.json").read_bytes()


def test_all_layer_overlap_preserves_reasons_and_priority() -> None:
    from polars.testing import assert_frame_equal

    decision = dt.date(2026, 3, 10)
    prior = decision - dt.timedelta(days=1)
    frame = _event_featured([
        (prior, "A", 900.0, 29.9), (decision, "A", 900.0, 29.9),
        (prior, "B", 900.0, 29.9), (decision, "B", 900.0, 29.9),
        (prior, "C", 900.0, 29.9), (decision, "C", 100.0, 0.0),
    ])
    tiers = UniverseTierSettings(
        core_enabled=True, core_enter_rank=1, core_exit_rank=1,
        core_warmup_sessions=2, follow_sessions=1,
    )
    result = select_universe_tiered(frame, decision, slot_budget=3, tiers=tiers)
    assert result.selected["symbol"].to_list() == ["A", "B", "C"]
    assert result.selected["tier"].to_list() == ["core", "event", "follow"]
    reasons = result.selected["selection_reasons"].to_list()[0]
    assert set(reasons) == {"limit_up", "surge10", "volsurge", "newhigh60", "core", "follow:1"}
    shuffled = select_universe_tiered(
        frame.sample(fraction=1.0, shuffle=True, seed=7), decision, slot_budget=3, tiers=tiers,
    )
    assert_frame_equal(result.selected, shuffled.selected)
    assert_frame_equal(result.dropped, shuffled.dropped)


def test_tiered_schema_preserves_all_null_feature_dtype() -> None:
    decision = dt.date(2026, 3, 10)
    frame = _event_featured([(decision, "A", 100.0, 0.0)]).with_columns(
        pl.lit(None, dtype=pl.Float64).alias("tv_ratio"),
    )
    tiers = UniverseTierSettings(
        core_enabled=True, core_enter_rank=1, core_exit_rank=1,
        core_warmup_sessions=1, follow_sessions=0,
    )
    result = select_universe_tiered(frame, decision, slot_budget=1, tiers=tiers)
    assert result.selected.schema["tv_ratio"] == pl.Float64
    assert result.selected.schema["core_rank"] == pl.Int32
    assert result.selected["selection_reasons"].to_list() == [["core"]]
