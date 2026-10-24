from __future__ import annotations

import datetime as dt
import json
import pathlib

from src.core.config import RepairSettings
from src.core.paths import DataPaths
from src.marketdata.tick_tape_backfill import pending_repair_symbol_days, plan_tick_tape_targets


def _audit(paths: DataPaths, day: dt.date, gaps: dict[str, int]) -> None:
    path = paths.session_audit_path(day, "ls")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"symbols": [{"symbol": s, "confirmed_missing": m} for s, m in gaps.items()]}),
        encoding="utf-8",
    )


def test_pending_repair_symbol_days_and_order(tmp_path: pathlib.Path) -> None:
    from src.marketdata import tick_tape_backfill as mod

    paths = DataPaths(tmp_path)
    day = dt.date(2026, 10, 21)
    prev = day - dt.timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= dt.timedelta(days=1)
    import polars as pl

    from src.marketdata.toss_calendar import TradingDay, save_trading_day_cache

    save_trading_day_cache(paths.calendar_cache, TradingDay(day, True, prev, day + dt.timedelta(days=1)))
    paths.universe_out(prev).parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["AAA", "BBB"]}, schema={"symbol": pl.String}).write_parquet(paths.universe_out(prev))
    paths.manifest_path(day).parent.mkdir(parents=True, exist_ok=True)
    paths.manifest_path(day).write_text(json.dumps({"subscription_acks": []}), encoding="utf-8")
    _audit(paths, day, {"BBB": 3})
    settings = RepairSettings()
    pending = pending_repair_symbol_days(paths, [day], settings=settings, tape_done=frozenset())
    assert pending == frozenset({("BBB", day)})
    settled = pending_repair_symbol_days(paths, [day], settings=settings, tape_done=frozenset({("BBB", day)}))
    assert settled == frozenset()
    missing = pending_repair_symbol_days(paths, [dt.date(2026, 10, 22)], settings=settings, tape_done=frozenset())
    assert missing == frozenset()
    assert pending_repair_symbol_days(
        paths, [day], settings=RepairSettings(repair_vendors=("kis",)), tape_done=frozenset(),
    ) == frozenset()
    targets = plan_tick_tape_targets(
        sessions=[day], paths=paths, ledger_done=frozenset(), repair_symbol_days=pending
    )
    assert [t.symbol for t in targets][:1] == ["BBB"]
    plain = plan_tick_tape_targets(sessions=[day], paths=paths, ledger_done=frozenset())
    assert {t.symbol for t in plain} == {t.symbol for t in targets}
    outside = plan_tick_tape_targets(
        sessions=[day],
        paths=paths,
        ledger_done=frozenset(),
        repair_symbol_days=frozenset({("BBB", dt.date(2020, 1, 1))}),
    )
    assert [t.symbol for t in outside] == [t.symbol for t in plain]
    assert mod.pending_repair_symbol_days is pending_repair_symbol_days
