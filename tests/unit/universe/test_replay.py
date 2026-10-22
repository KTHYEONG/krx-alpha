"""Universe replay guards: point-in-time selection without live side effects."""

from __future__ import annotations

import datetime as dt

import polars as pl


def _seed(root, frame: pl.DataFrame) -> None:
    from src.marketdata.bar_store import append_daily_bars

    append_daily_bars(root, frame)


def _bars(dates: list[dt.date], symbols: list[str], *, change_last: float = 29.9, tv_last: float = 1000.0) -> pl.DataFrame:
    rows = []
    for symbol in symbols:
        for i, day in enumerate(dates):
            last = i == len(dates) - 1
            rows.append({
                "date": day,
                "symbol": symbol,
                "close": 1300.0 if last else 1000.0,
                "volume": 1000,
                "trade_value_100m": tv_last if last else 100.0,
                "daily_change_pct": change_last if last else 0.0,
            })
    return pl.DataFrame(rows)


def _tiers(**overrides):
    from src.core.config import UniverseTierSettings

    base = {"core_enabled": False, "core_enter_rank": 2, "core_exit_rank": 3, "core_warmup_sessions": 20, "follow_sessions": 3}
    base.update(overrides)
    return UniverseTierSettings(**base)


_SLOT = 100


def test_replay_never_writes_live_log(tmp_path) -> None:
    from src.core.config import DataPaths
    from src.universe.replay import replay_universe_history

    paths = DataPaths(tmp_path / "data")
    dates = [dt.date(2026, 1, 5) + dt.timedelta(days=i) for i in range(60)]
    _seed(paths.bars_daily_dir, _bars(dates, ["000001"]))

    tiers = _tiers()
    summary = replay_universe_history(
        bars_root=paths.bars_daily_dir,
        out_dir=paths.universe_replay_dir,
        start=dates[-1],
        end=dates[-1],
        lookback_calendar_days=150,
        slot_budget=100,
        tiers=tiers,
    )

    assert summary.core_enabled is True
    assert tiers.core_enabled is False
    assert (paths.universe_replay_dir / f"{dates[-1].isoformat()}.parquet").exists()
    assert not paths.universe_dir.exists() or list(paths.universe_dir.glob("*.parquet")) == []


def test_point_in_time_perturbation_invariance(tmp_path) -> None:
    from src.universe.replay import replay_universe_history

    dates = [dt.date(2026, 1, 5) + dt.timedelta(days=i) for i in range(60)]
    target = dates[30]
    root_a = tmp_path / "a"
    root_b = tmp_path / "b"
    _seed(root_a, _bars(dates, ["000001"]))
    frame_b = _bars(dates, ["000001"])
    frame_b = frame_b.with_columns(
        pl.when(pl.col("date") > target)
        .then(pl.lit(9999.0))
        .otherwise(pl.col("close"))
        .alias("close")
    )
    _seed(root_b, frame_b)

    kwargs = {"start": target, "end": target, "lookback_calendar_days": 150, "slot_budget": 40, "tiers": _tiers()}
    replay_universe_history(bars_root=root_a, out_dir=tmp_path / "out_a", **kwargs)
    replay_universe_history(bars_root=root_b, out_dir=tmp_path / "out_b", **kwargs)

    a = pl.read_parquet(tmp_path / "out_a" / f"{target.isoformat()}.parquet")
    b = pl.read_parquet(tmp_path / "out_b" / f"{target.isoformat()}.parquet")
    assert a.sort("symbol").to_dicts() == b.sort("symbol").to_dicts()


def test_matches_production_selection(tmp_path) -> None:
    from polars.testing import assert_frame_equal

    from src.marketdata.partitioned_store import scan_month_partitions
    from src.universe.policy import compute_selection_features, select_universe_tiered
    from src.universe.replay import replay_universe_history

    dates = [dt.date(2026, 1, 5) + dt.timedelta(days=i) for i in range(60)]
    decision = dates[-1]
    root = tmp_path / "bars"
    _seed(root, _bars(dates, ["000001", "000002"]))
    tiers = _tiers(core_enabled=True)

    replay_universe_history(
        bars_root=root, out_dir=tmp_path / "replay", start=decision, end=decision,
        lookback_calendar_days=150, slot_budget=100, tiers=tiers,
    )

    replayed = pl.read_parquet(tmp_path / "replay" / f"{decision.isoformat()}.parquet")
    assert (replayed["managed_filter"].to_list()) == ["unavailable"] * replayed.height
    bars = scan_month_partitions(root, min_date=decision - dt.timedelta(days=150), max_date=decision).collect()
    expected = select_universe_tiered(compute_selection_features(bars), decision, slot_budget=100, tiers=tiers).selected
    replay_only = {"managed_filter", "short_warmup", "replay_run_id"}
    assert_frame_equal(
        replayed.select(list(expected.columns)),
        expected,
    )
    assert set(replayed.columns) == set(expected.columns) | replay_only


def test_delisted_symbol_appears_while_listed(tmp_path) -> None:
    from src.universe.replay import replay_universe_history

    dates = [dt.date(2026, 1, 5) + dt.timedelta(days=i) for i in range(60)]
    early = dates[:50]
    root = tmp_path / "bars"
    rows = []
    for i, day in enumerate(dates):
        last_early = i == len(early) - 1
        rows.append({
            "date": day, "symbol": "LIVE", "close": 1300.0 if day == dates[-1] else 1000.0,
            "volume": 1000, "trade_value_100m": 1000.0 if day in (early[-1], dates[-1]) else 100.0,
            "daily_change_pct": 29.9 if day in (early[-1], dates[-1]) else 0.0,
        })
    for i, day in enumerate(early):
        rows.append({
            "date": day, "symbol": "GONE", "close": 1300.0 if i == len(early) - 1 else 1000.0,
            "volume": 1000, "trade_value_100m": 1000.0 if i == len(early) - 1 else 100.0,
            "daily_change_pct": 29.9 if i == len(early) - 1 else 0.0,
        })
    _seed(root, pl.DataFrame(rows))

    replay_universe_history(
        bars_root=root, out_dir=tmp_path / "replay", start=early[-1], end=dates[-1],
        lookback_calendar_days=150, slot_budget=100, tiers=_tiers(),
    )

    while_listed = pl.read_parquet(tmp_path / "replay" / f"{early[-1].isoformat()}.parquet")
    after_delist = pl.read_parquet(tmp_path / "replay" / f"{dates[-1].isoformat()}.parquet")
    assert "GONE" in while_listed["symbol"].to_list()
    assert "GONE" not in after_delist["symbol"].to_list()


def test_idempotent_skip(tmp_path) -> None:
    from src.universe.replay import replay_universe_history

    dates = [dt.date(2026, 1, 5) + dt.timedelta(days=i) for i in range(60)]
    decision = dates[-1]
    root = tmp_path / "bars"
    _seed(root, _bars(dates, ["000001"]))
    out = tmp_path / "replay"

    first = replay_universe_history(
        bars_root=root, out_dir=out, start=decision, end=decision,
        lookback_calendar_days=150, slot_budget=100, tiers=_tiers(),
    )
    target = out / f"{decision.isoformat()}.parquet"
    before = target.read_bytes()
    second = replay_universe_history(
        bars_root=root, out_dir=out, start=decision, end=decision,
        lookback_calendar_days=150, slot_budget=100, tiers=_tiers(),
    )

    assert first.written == 1
    assert second.written == 0
    assert second.skipped == 1
    assert target.read_bytes() == before
    replay_universe_history(
        bars_root=root, out_dir=out, start=decision, end=decision,
        lookback_calendar_days=150, slot_budget=100, tiers=_tiers(), overwrite=True,
    )
    assert target.read_bytes() == before


def test_short_warmup_flag_and_progress_logging(tmp_path, caplog) -> None:
    import logging

    from src.universe.replay import replay_universe_history

    dates = [dt.date(2026, 1, 5) + dt.timedelta(days=i) for i in range(40)]
    root = tmp_path / "bars"
    _seed(root, _bars(dates, ["000001"]))
    out = tmp_path / "replay"

    with caplog.at_level(logging.INFO, logger="src.universe.replay"):
        summary = replay_universe_history(
            bars_root=root, out_dir=out, start=dates[0], end=dates[-1],
            lookback_calendar_days=150, slot_budget=100, tiers=_tiers(),
        )

    assert summary.written == 40
    assert len(summary.short_warmup_dates) == 38
    frame = pl.read_parquet(out / f"{dates[19].isoformat()}.parquet")
    assert frame.height > 0
    assert frame["short_warmup"].to_list() == [True] * frame.height
    ready = pl.read_parquet(out / f"{dates[38].isoformat()}.parquet")
    assert ready["short_warmup"].to_list() == [False] * ready.height
    assert any("stage=universe_replay progress=20/40" in rec.message for rec in caplog.records)


def test_replay_rejects_invalid_parameters_and_live_paths(tmp_path) -> None:
    import pytest

    from src.core.paths import DataPaths
    from src.universe.replay import replay_universe_history

    paths = DataPaths(tmp_path)
    day = dt.date(2026, 9, 1)
    kwargs = {
        "bars_root": paths.bars_daily_dir, "out_dir": paths.universe_replay_dir,
        "start": day, "end": day, "lookback_calendar_days": 150,
        "slot_budget": 100, "tiers": _tiers(),
    }
    cases = [
        {"start": day + dt.timedelta(days=1)},
        {"lookback_calendar_days": 0},
        {"slot_budget": 1},
        {"out_dir": paths.universe_dir},
        {"out_dir": paths.universe_dir / "nested"},
        {"out_dir": paths.bars_daily_dir},
    ]
    for changes in cases:
        with pytest.raises(ValueError, match="must"):
            replay_universe_history(**(kwargs | changes))
    assert not paths.universe_dir.exists()
    assert not paths.universe_replay_dir.exists()


def test_failed_overwrite_preserves_existing_replay(tmp_path, monkeypatch) -> None:
    import pytest

    from src.universe.replay import replay_universe_history

    day = dt.date(2026, 9, 1)
    root = tmp_path / "bars"
    _seed(root, _bars([day], ["000001"]))
    kwargs = {
        "bars_root": root, "out_dir": tmp_path / "replay", "start": day, "end": day,
        "lookback_calendar_days": 150, "slot_budget": 100, "tiers": _tiers(),
    }
    replay_universe_history(**kwargs)
    target = tmp_path / "replay" / f"{day}.parquet"
    before = target.read_bytes()

    def _interrupted(self, file, **kw):
        file.write_bytes(b"partial")
        raise OSError("disk full")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", _interrupted)
    with pytest.raises(OSError, match="disk full"):
        replay_universe_history(**kwargs, overwrite=True)
    assert target.read_bytes() == before
    summary = replay_universe_history(**kwargs)
    assert summary.skipped == 1
