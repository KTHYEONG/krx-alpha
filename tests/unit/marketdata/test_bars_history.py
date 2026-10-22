"""History backfill guards: nearest reference and backward extension."""

from __future__ import annotations

import datetime as dt

import polars as pl


def _day(day: dt.date, count: int) -> pl.DataFrame:
    return pl.DataFrame({
        "date": [day] * count,
        "symbol": [f"{i:06d}" for i in range(count)],
        "close": [100.0] * count,
        "volume": [100] * count,
        "trade_value_100m": [10.0] * count,
        "daily_change_pct": [0.5] * count,
    })


def test_nearest_reference_accepts_genuine_older_listing_counts(tmp_path) -> None:
    import pytest

    from src.marketdata.bar_store import ImplausibleRowCountError, append_daily_bars

    store = tmp_path / "daily"
    append_daily_bars(store, _day(dt.date(2026, 9, 2), 900))
    append_daily_bars(store, _day(dt.date(2026, 9, 3), 1000))

    assert append_daily_bars(store, _day(dt.date(2026, 9, 1), 880), reference="nearest") == 880
    with pytest.raises(ImplausibleRowCountError):
        append_daily_bars(store, _day(dt.date(2026, 8, 31), 880), reference="latest")


def test_nearest_reference_falls_back_to_earlier_date(tmp_path) -> None:
    from src.marketdata.bar_store import append_daily_bars

    store = tmp_path / "daily"
    append_daily_bars(store, _day(dt.date(2026, 9, 3), 900))
    assert append_daily_bars(store, _day(dt.date(2026, 9, 4), 850), reference="nearest") == 850


def test_truncated_history_still_fails_closed(tmp_path) -> None:
    import pytest

    from src.marketdata.bar_store import ImplausibleRowCountError, append_daily_bars

    store = tmp_path / "daily"
    append_daily_bars(store, _day(dt.date(2026, 9, 3), 900))

    with pytest.raises(ImplausibleRowCountError):
        append_daily_bars(store, _day(dt.date(2026, 9, 1), 500), reference="nearest")


def test_extension_skips_non_trading_days_and_resumes(tmp_path, monkeypatch) -> None:
    import src.marketdata.krx_bars as bars_mod
    from src.marketdata.bar_store import append_daily_bars
    from src.marketdata.krx_bars import KrxNoTradingDataError, extend_bars_history
    from src.marketdata.partitioned_store import scan_month_partitions

    store = tmp_path / "daily"
    append_daily_bars(store, _day(dt.date(2026, 9, 10), 10))

    holiday = dt.date(2026, 9, 7)

    def _fake(date: dt.date, *, auth_key: str, session=None) -> pl.DataFrame:
        if date == holiday:
            raise KrxNoTradingDataError(f"no trading data for {date}")
        return _day(date, 10)

    monkeypatch.setattr(bars_mod, "fetch_daily_bars", _fake)

    first = extend_bars_history(store, auth_key="k", start_date=dt.date(2026, 9, 1), max_trading_days=2)
    second = extend_bars_history(store, auth_key="k", start_date=dt.date(2026, 9, 1), max_trading_days=2)

    assert first["appended_days"] == 2
    assert second["appended_days"] == 2
    dates = sorted(scan_month_partitions(store).select(pl.col("date").unique()).collect().get_column("date").to_list())
    assert holiday not in dates
    assert len(dates) == 5
    assert dates[-1] == dt.date(2026, 9, 10)


def test_empty_store_rejected(tmp_path) -> None:
    import pytest

    from src.marketdata.krx_bars import KrxBarsError, extend_bars_history

    with pytest.raises(KrxBarsError):
        extend_bars_history(tmp_path / "daily", auth_key="k", start_date=dt.date(2026, 9, 1), max_trading_days=2)


def test_latest_refresh_path_unchanged(tmp_path) -> None:
    from src.marketdata.bar_store import append_daily_bars
    from src.marketdata.partitioned_store import scan_month_partitions

    store = tmp_path / "daily"
    assert append_daily_bars(store, _day(dt.date(2026, 9, 7), 10)) == 10
    assert append_daily_bars(store, _day(dt.date(2026, 9, 8), 9)) == 9
    assert scan_month_partitions(store).collect().height == 19


def test_nearest_reference_defensive_branches(tmp_path) -> None:
    import pytest

    from src.marketdata.bar_store import _count_on_date, _nearest_day_height, append_daily_bars
    from src.marketdata.partitioned_store import PartitionedStoreError

    assert _nearest_day_height(tmp_path / "missing", []) is None
    assert _nearest_day_height(tmp_path / "missing", [dt.date(2026, 9, 1)]) is None
    assert _count_on_date(tmp_path / "missing", dt.date(2026, 9, 1)) == 0

    store = tmp_path / "daily"
    append_daily_bars(store, _day(dt.date(2026, 9, 3), 10))
    assert _nearest_day_height(store, [dt.date(2026, 9, 3)]) is None

    same = tmp_path / "same.parquet"
    same.write_bytes(b"legacy bytes")
    with pytest.raises(PartitionedStoreError):
        append_daily_bars(same, _day(dt.date(2026, 9, 1), 10), reference="nearest")

    broken = tmp_path / "broken"
    broken.mkdir()
    (broken / "2026-09.parquet").write_bytes(b"not-a-parquet")
    with pytest.raises(PartitionedStoreError):
        append_daily_bars(broken, _day(dt.date(2026, 9, 1), 10), reference="nearest")

    with pytest.raises(ValueError, match="unknown reference"):
        append_daily_bars(store, _day(dt.date(2026, 9, 4), 10), reference="later")  # type: ignore[arg-type]


def test_count_on_date_falls_back_to_full_scan(tmp_path) -> None:
    import polars as pl

    from src.marketdata.bar_store import _count_on_date, append_daily_bars

    store = tmp_path / "daily"
    append_daily_bars(store, _day(dt.date(2026, 9, 3), 7))
    misnamed = store / "2026-10.parquet"
    (store / "2026-09.parquet").rename(misnamed)
    assert _count_on_date(store, dt.date(2026, 9, 3)) == 7
    assert pl.scan_parquet(misnamed).filter(pl.col("date") == dt.date(2026, 9, 3)).select(pl.len()).collect().item() == 7


def test_earliest_rejects_unreadable_partition(tmp_path) -> None:
    import pytest

    from src.marketdata.krx_bars import KrxBarsError
    from src.marketdata.krx_bars import _earliest_stored_date

    store = tmp_path / "daily"
    store.mkdir()
    (store / "2026-08.parquet").write_bytes(b"not-a-parquet")
    with pytest.raises(KrxBarsError, match="partition unreadable"):
        _earliest_stored_date(store)


def test_extension_logs_progress_every_twenty_days(tmp_path, monkeypatch, caplog) -> None:
    import logging

    import src.marketdata.krx_bars as bars_mod
    from src.marketdata.bar_store import append_daily_bars
    from src.marketdata.krx_bars import extend_bars_history

    store = tmp_path / "daily"
    append_daily_bars(store, _day(dt.date(2026, 9, 30), 10))

    def _fake(date: dt.date, *, auth_key: str, session=None) -> pl.DataFrame:
        return _day(date, 10)

    monkeypatch.setattr(bars_mod, "fetch_daily_bars", _fake)
    with caplog.at_level(logging.INFO, logger="src.marketdata.krx_bars"):
        result = extend_bars_history(
            store, auth_key="k", start_date=dt.date(2026, 9, 1), max_trading_days=21
        )
    assert result["appended_days"] == 21
    assert any("progress=20/21" in rec.message for rec in caplog.records)


def test_bars_refresh_history_paths(tmp_path, monkeypatch) -> None:
    import argparse

    import src.cli.bars_refresh as cli_mod
    from src.core.config import DataPaths
    from src.marketdata.bar_store import append_daily_bars
    from src.marketdata.krx_bars import KrxBarsError

    store = tmp_path / "daily"
    append_daily_bars(store, _day(dt.date(2026, 9, 10), 10))
    market_map = tmp_path / "market_map.json"
    market_map.write_text("{}", encoding="utf-8")
    paths = DataPaths(tmp_path / "data")
    assert paths.universe_replay_dir == tmp_path / "data" / "universe_replay"
    assert paths.universe_replay_out(dt.date(2026, 9, 10)) == paths.universe_replay_dir / "2026-09-10.parquet"

    monkeypatch.setattr(cli_mod, "refresh_bars", lambda **kw: type("R", (), {"trading_day": dt.date(2026, 9, 10), "appended_rows": 0})())
    seen: dict = {}

    def _ok(path, *, auth_key, start_date, max_trading_days, session=None):
        seen["days"] = max_trading_days
        return {"appended_days": 1, "appended_rows": 10}

    monkeypatch.setattr(cli_mod, "extend_bars_history", _ok)
    monkeypatch.setenv("KRX_OPENAPI_KEY", "k")
    args = argparse.Namespace(
        ref_date="2026-09-11", store_path=str(store), market_map_path=str(market_map),
        window_days=90, history_start="2026-09-01", history_max_days=5,
    )
    assert cli_mod.run(args) == 0
    assert seen["days"] == 5

    def _fail(*a, **k):
        raise KrxBarsError("boom")

    monkeypatch.setattr(cli_mod, "extend_bars_history", _fail)
    assert cli_mod.run(args) == 4


def test_nearest_batch_cannot_hide_truncated_day(tmp_path) -> None:
    import pytest

    from src.marketdata.bar_store import ImplausibleRowCountError, append_daily_bars

    store = tmp_path / "daily"
    append_daily_bars(store, _day(dt.date(2026, 9, 3), 10))
    before = (store / "2026-09.parquet").read_bytes()
    batch = pl.concat([_day(dt.date(2026, 9, 2), 10), _day(dt.date(2026, 9, 1), 5)])
    with pytest.raises(ImplausibleRowCountError):
        append_daily_bars(store, batch, reference="nearest")
    assert (store / "2026-09.parquet").read_bytes() == before


def test_history_invalid_budget_store_and_response_fail_closed(tmp_path, monkeypatch) -> None:
    import pytest

    import src.marketdata.krx_bars as bars_mod
    from src.marketdata.bar_store import append_daily_bars

    store = tmp_path / "daily"
    day = dt.date(2026, 9, 3)
    append_daily_bars(store, _day(day, 10))
    kwargs = {"auth_key": "k", "start_date": day - dt.timedelta(days=2)}
    for budget in (0, -1):
        with pytest.raises(bars_mod.KrxBarsError, match="max_trading_days"):
            bars_mod.extend_bars_history(store, max_trading_days=budget, **kwargs)
    not_dir = tmp_path / "file"
    not_dir.write_bytes(b"legacy")
    with pytest.raises(bars_mod.KrxBarsError, match="not a directory"):
        bars_mod.extend_bars_history(not_dir, max_trading_days=1, **kwargs)
    before = (store / "2026-09.parquet").read_bytes()
    for response in (_day(day, 10), _day(day, 0)):
        monkeypatch.setattr(bars_mod, "fetch_daily_bars", lambda *a, response=response, **kw: response)
        with pytest.raises(bars_mod.KrxBarsError, match="unexpected bar dates"):
            bars_mod.extend_bars_history(store, max_trading_days=1, **kwargs)
        assert (store / "2026-09.parquet").read_bytes() == before


def test_history_cli_storage_failure_preserves_refreshed_artifacts(tmp_path, monkeypatch) -> None:
    import argparse

    import src.cli.bars_refresh as cli_mod
    from src.marketdata.bar_store import append_daily_bars
    from src.marketdata.partitioned_store import PartitionedStoreError

    store = tmp_path / "daily"
    market_map = tmp_path / "market_map.json"
    day = dt.date(2026, 9, 3)

    def _refresh(**kw):
        append_daily_bars(store, _day(day, 10))
        market_map.write_text('{"000001":"KOSPI"}', encoding="utf-8")
        return cli_mod.BarsRefreshResult(trading_day=day, appended_rows=10, backfilled_days=0)

    def _fail(*a, **kw):
        raise PartitionedStoreError("history partition corrupt")

    monkeypatch.setenv("KRX_OPENAPI_KEY", "k")
    monkeypatch.setattr(cli_mod, "refresh_bars", _refresh)
    monkeypatch.setattr(cli_mod, "extend_bars_history", _fail)
    args = argparse.Namespace(
        ref_date="2026-09-04", store_path=str(store), market_map_path=str(market_map),
        window_days=90, history_start="2023-01-02", history_max_days=400,
    )
    assert cli_mod.run(args) == 4
    assert pl.read_parquet(store / "2026-09.parquet")["date"].to_list() == [day] * 10
    assert market_map.read_text(encoding="utf-8") == '{"000001":"KOSPI"}'
    args.history_start = "invalid"
    assert cli_mod.run(args) == 4
