"""Bars partition-store guards: cross-month reference check and bounded rewrites."""

from __future__ import annotations

import datetime as dt

import polars as pl


def _valid_day(day: dt.date, count: int) -> pl.DataFrame:
    return pl.DataFrame({
        "date": [day] * count,
        "symbol": [f"{index:06d}" for index in range(count)],
        "close": [100.0] * count,
        "volume": [100] * count,
        "trade_value_100m": [10.0] * count,
        "daily_change_pct": [0.5] * count,
    })


def test_append_daily_bars_rejects_truncated_day_across_month_boundary(tmp_path) -> None:
    import pytest

    from src.marketdata.bar_store import ImplausibleRowCountError, append_daily_bars

    store = tmp_path / "daily"
    assert append_daily_bars(store, _valid_day(dt.date(2026, 8, 31), 2800)) == 2800

    with pytest.raises(ImplausibleRowCountError):
        append_daily_bars(store, _valid_day(dt.date(2026, 9, 1), 100))

    assert sorted(path.name for path in store.glob("*.parquet")) == ["2026-08.parquet"]


def test_append_daily_bars_rewrites_only_current_month(tmp_path) -> None:
    import polars as pl

    from src.marketdata.bar_store import append_daily_bars
    from src.marketdata.partitioned_store import scan_month_partitions

    store = tmp_path / "daily"
    history = pl.concat([
        _valid_day(dt.date(2024 + (8 + month) // 12, (8 + month) % 12 + 1, 15), 10)
        for month in range(24)
    ])
    assert append_daily_bars(store, history) == 240

    before = {path.name: (path.stat().st_mtime_ns, path.read_bytes()) for path in sorted(store.glob("*.parquet"))}
    assert len(before) == 24
    assert max(before) == "2026-08.parquet"

    added = append_daily_bars(store, _valid_day(dt.date(2026, 9, 30), 10))

    assert added == 10
    assert (store / "2026-09.parquet").exists()
    after = {path.name: (path.stat().st_mtime_ns, path.read_bytes()) for path in sorted(store.glob("*.parquet"))}
    assert set(after) == set(before) | {"2026-09.parquet"}
    for name in before:
        assert after[name] == before[name]
    assert scan_month_partitions(store).collect().height == 250


def test_append_daily_bars_rejects_file_path_store(tmp_path) -> None:
    import pytest

    from src.marketdata.bar_store import append_daily_bars
    from src.marketdata.partitioned_store import PartitionedStoreError

    store = tmp_path / "daily.parquet"
    store.write_bytes(b"legacy bytes")

    with pytest.raises(PartitionedStoreError):
        append_daily_bars(store, _valid_day(dt.date(2026, 9, 1), 10))
    assert store.read_bytes() == b"legacy bytes"


def test_append_daily_bars_fails_closed_on_unreadable_newer_partition(tmp_path) -> None:
    import pytest

    from src.marketdata.bar_store import append_daily_bars
    from src.marketdata.partitioned_store import PartitionedStoreError

    store = tmp_path / "daily"
    store.mkdir(parents=True)
    (store / "2026-10.parquet").write_bytes(b"not-a-parquet")

    with pytest.raises(PartitionedStoreError, match="unreadable"):
        append_daily_bars(store, _valid_day(dt.date(2026, 9, 1), 10))
    assert list(store.glob("2026-09.parquet")) == []
