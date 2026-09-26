def test_plan_universe_writes_parquet_and_emits_candidates(tmp_path) -> None:
    # Given: 60거래일 일봉 + 판정일 상한가 1종목
    import datetime as dt

    import polars as pl

    from src.universe.ipc import read_candidates
    from src.universe.service import plan_universe

    n = 60
    base = dt.date(2026, 1, 5)
    decision = base + dt.timedelta(days=n - 1)
    bars_root = tmp_path / "bars"
    _seed_bars(bars_root, pl.DataFrame({
        "date": [base + dt.timedelta(days=i) for i in range(n)],
        "symbol": ["000001"] * n,
        "close": [1000.0] * (n - 1) + [1300.0],
        "volume": [1000] * n,
        "trade_value_100m": [100.0] * (n - 1) + [1000.0],
        "daily_change_pct": [0.0] * (n - 1) + [29.9],
    }))
    out_path = tmp_path / "universe.parquet"
    candidates_path = tmp_path / "candidates.json"

    # When
    result = plan_universe(
        bars_root=bars_root,
        decision_date=decision,
        lookback_calendar_days=150,
        out_path=out_path,
        slot_budget=40,
        candidates_path=candidates_path,
    )

    # Then
    assert result.decision_date == decision
    assert result.selected == 1
    assert result.candidates_emitted == 1
    assert pl.read_parquet(out_path)["symbol"].to_list() == ["000001"]
    stored = read_candidates(candidates_path)
    assert stored is not None
    assert stored["candidates"][0]["symbol"] == "000001"


def _seed_bars(root: object, frame: object) -> None:
    from src.marketdata.bar_store import append_daily_bars

    append_daily_bars(root, frame)


def _two_symbol_bars(root: object) -> object:
    import datetime as dt

    import polars as pl

    n = 60
    base = dt.date(2026, 1, 5)
    symbols = ["000001", "000002"]
    _seed_bars(root, pl.DataFrame({
        "date": [base + dt.timedelta(days=i) for i in range(n)] * len(symbols),
        "symbol": [s for s in symbols for _ in range(n)],
        "close": ([1000.0] * (n - 1) + [1300.0]) * len(symbols),
        "volume": [1000] * (n * len(symbols)),
        "trade_value_100m": ([100.0] * (n - 1) + [1000.0]) * len(symbols),
        "daily_change_pct": ([0.0] * (n - 1) + [29.9]) * len(symbols),
    }))
    return base + dt.timedelta(days=n - 1)


def _status_row(symbol, managed) -> dict:
    return {
        "source_tr": "FHKST01010100",
        "market_div_code": "J",
        "symbol": symbol,
        "status_code": "00",
        "managed": managed,
        "market_warning_code": "00",
        "short_overheated": False,
        "investment_caution": False,
        "liquidation_trading": False,
        "trading_halted": False,
        "vi_code": "",
        "ovtm_vi_cls_code": "",
        "credit_available": True,
        "last_price": 1300,
        "base_price": 1000,
        "upper_limit": 1300,
        "lower_limit": 700,
    }


class _FakeStatusSource:
    def __init__(self, managed_by_symbol) -> None:
        self.managed_by_symbol = managed_by_symbol
        self.requested: list = []

    def get_security_status(self, symbol: str) -> dict:
        self.requested.append(symbol)
        return _status_row(symbol, self.managed_by_symbol[symbol])


def test_plan_universe_drops_managed_status_and_persists_observations(tmp_path) -> None:

    import polars as pl

    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore
    from src.universe.ipc import read_candidates
    from src.universe.service import plan_universe

    bars_root = tmp_path / "bars"
    decision = _two_symbol_bars(bars_root)
    source = _FakeStatusSource({"000001": True, "000002": False})
    store = SnapshotStore(paths=DataPaths(tmp_path / "data"), session_date=decision)

    result = plan_universe(
        bars_root=bars_root,
        decision_date=decision,
        lookback_calendar_days=150,
        out_path=tmp_path / "universe.parquet",
        slot_budget=40,
        candidates_path=tmp_path / "candidates.json",
        session_date=decision,
        status_source=source,
        snapshot_store=store,
        wall_ns=lambda: 123,
    )

    assert result.selected == 1
    assert result.status_excluded == 1
    assert result.status_unknown == 0
    assert source.requested == ["000001", "000002"]
    stored = read_candidates(tmp_path / "candidates.json")
    assert stored is not None
    assert [c["symbol"] for c in stored["candidates"]] == ["000002"]
    part = tmp_path / "data" / "l1" / "snapshot" / "security_status" / f"dt={decision.isoformat()}.parquet"
    frame = pl.read_parquet(part)
    assert frame.height == 2
    assert frame["observed_at_ns"].to_list() == [123, 123]
    assert frame["session_date"].to_list() == [decision, decision]


def test_plan_universe_keeps_symbol_when_status_lookup_fails(tmp_path, caplog) -> None:
    import logging

    from src.execution.contracts import KisApiError
    from src.universe.service import plan_universe

    bars_root = tmp_path / "bars"
    decision = _two_symbol_bars(bars_root)

    class _FailingSource:
        def get_security_status(self, symbol: str) -> dict:
            raise KisApiError("EGW00201", "limit")

    with caplog.at_level(logging.WARNING, logger="src.universe.service"):
        result = plan_universe(
            bars_root=bars_root,
            decision_date=decision,
            lookback_calendar_days=150,
            out_path=tmp_path / "universe.parquet",
            slot_budget=40,
            session_date=decision,
            status_source=_FailingSource(),
        )

    assert result.selected == 2
    assert result.status_unknown == 2
    assert result.status_excluded == 0
    assert any("stage=universe_status" in rec.message for rec in caplog.records)


def test_plan_universe_without_status_source_is_unchanged(tmp_path) -> None:
    from src.universe.service import plan_universe

    bars_root = tmp_path / "bars"
    decision = _two_symbol_bars(bars_root)

    result = plan_universe(
        bars_root=bars_root,
        decision_date=decision,
        lookback_calendar_days=150,
        out_path=tmp_path / "universe.parquet",
        slot_budget=40,
        candidates_path=tmp_path / "candidates.json",
    )

    assert result.selected == 2
    assert result.status_excluded == 0
    assert result.status_unknown == 0
    assert not list((tmp_path / "data").rglob("*.parquet"))


def test_plan_universe_continues_when_snapshot_append_fails(tmp_path, caplog) -> None:
    import logging

    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore
    from src.universe.service import plan_universe

    bars_root = tmp_path / "bars"
    decision = _two_symbol_bars(bars_root)
    part = tmp_path / "data" / "l1" / "snapshot" / "security_status" / f"dt={decision.isoformat()}.parquet"
    part.parent.mkdir(parents=True, exist_ok=True)
    part.write_bytes(b"\x00\x01broken-parquet")
    store = SnapshotStore(paths=DataPaths(tmp_path / "data"), session_date=decision)

    with caplog.at_level(logging.ERROR, logger="src.universe.service"):
        result = plan_universe(
            bars_root=bars_root,
            decision_date=decision,
            lookback_calendar_days=150,
            out_path=tmp_path / "universe.parquet",
            slot_budget=40,
            session_date=decision,
            status_source=_FakeStatusSource({"000001": False, "000002": False}),
            snapshot_store=store,
        )

    assert result.selected == 2
    assert any("stage=universe_status status=FAIL" in rec.message for rec in caplog.records)


def test_plan_universe_requires_session_date_with_status_source(tmp_path) -> None:

    import pytest

    from src.universe.service import plan_universe

    bars_root = tmp_path / "bars"
    decision = _two_symbol_bars(bars_root)

    with pytest.raises(ValueError, match="session_date"):
        plan_universe(
            bars_root=bars_root,
            decision_date=decision,
            lookback_calendar_days=150,
            out_path=tmp_path / "universe.parquet",
            slot_budget=40,
            status_source=_FakeStatusSource({}),
        )


def _long_bars_frame(*, days: int, decision: object) -> object:
    import datetime as dt

    import polars as pl

    base = decision - dt.timedelta(days=days - 1)
    dates = [base + dt.timedelta(days=i) for i in range(days)]
    frames = []
    for symbol, selected in (("000001", True), ("000002", False)):
        closes = [10000.0] * days
        bases = [10000.0] * days
        changes = [0.0] * days
        # 2:1 split inside the last 60 rows: price halves against a halved base.
        split_at = days - 30
        for i in range(split_at, days):
            closes[i] = 5100.0
            bases[i] = 5000.0
            changes[i] = 2.0
        if selected:
            closes[-1] = 6495.0
            changes[-1] = 29.9
        frames.append(pl.DataFrame({
            "date": dates,
            "symbol": [symbol] * days,
            "close": closes,
            "volume": [1000] * days,
            "trade_value_100m": ([100.0] * (days - 1) + [1000.0]) if selected else [100.0] * days,
            "daily_change_pct": changes,
            "market": ["KOSPI"] * days,
            "open": closes,
            "high": closes,
            "low": closes,
            "base_price": bases,
            "market_cap_krw": [100000000] * days,
            "listed_shares": [10000] * days,
            # Stale classification change 200 days ago: settled long before any window.
            "section": ["관리종목(소속부없음)" if i < 100 else "주권" for i in range(days)],
            "stock_cert_kind": ["구형우선주" if i < 100 else "보통주" for i in range(days)],
            "security_group": ["주권"] * days,
        }))
    return pl.concat(frames)


def test_plan_universe_windowed_selection_equals_full_history_selection(tmp_path) -> None:
    import datetime as dt

    import polars as pl
    from polars.testing import assert_frame_equal

    from src.marketdata.partitioned_store import scan_month_partitions
    from src.universe.policy import compute_selection_features
    from src.universe.service import plan_universe

    decision = dt.date(2026, 9, 10)
    frame = _long_bars_frame(days=300, decision=decision)
    full_root = tmp_path / "full"
    _seed_bars(full_root, frame)
    window = frame.filter(pl.col("date") >= decision - dt.timedelta(days=150))
    window_root = tmp_path / "window"
    _seed_bars(window_root, window)

    full = plan_universe(
        bars_root=full_root,
        decision_date=decision,
        lookback_calendar_days=400,
        out_path=tmp_path / "full.parquet",
        slot_budget=40,
    )
    windowed = plan_universe(
        bars_root=window_root,
        decision_date=decision,
        lookback_calendar_days=150,
        out_path=tmp_path / "window.parquet",
        slot_budget=40,
    )

    assert full.selected == windowed.selected == 1
    assert pl.read_parquet(tmp_path / "full.parquet").sort("symbol").to_dicts() == (
        pl.read_parquet(tmp_path / "window.parquet").sort("symbol").to_dicts()
    )
    full_features = compute_selection_features(
        scan_month_partitions(full_root, min_date=decision - dt.timedelta(days=400), max_date=decision).collect()
    ).filter(pl.col("date") == decision)
    window_features = compute_selection_features(
        scan_month_partitions(window_root, min_date=decision - dt.timedelta(days=150), max_date=decision).collect()
    ).filter(pl.col("date") == decision)
    assert_frame_equal(
        full_features.sort("symbol").select(["symbol", "tv_median_20", "close_max_60", "tv_ratio"]),
        window_features.sort("symbol").select(["symbol", "tv_median_20", "close_max_60", "tv_ratio"]),
    )
