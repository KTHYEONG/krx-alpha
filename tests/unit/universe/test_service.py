def test_plan_universe_writes_parquet_and_emits_candidates(tmp_path) -> None:
    # Given: 60거래일 일봉 + 판정일 상한가 1종목
    import datetime as dt

    import polars as pl

    from src.universe.ipc import read_candidates
    from src.universe.service import plan_universe

    n = 60
    base = dt.date(2026, 1, 5)
    decision = base + dt.timedelta(days=n - 1)
    bars_path = tmp_path / "bars.parquet"
    pl.DataFrame({
        "date": [base + dt.timedelta(days=i) for i in range(n)],
        "symbol": ["000001"] * n,
        "close": [1000.0] * (n - 1) + [1300.0],
        "volume": [1000] * n,
        "trade_value_100m": [100.0] * (n - 1) + [1000.0],
        "daily_change_pct": [0.0] * (n - 1) + [29.9],
    }).write_parquet(bars_path)
    out_path = tmp_path / "universe.parquet"
    candidates_path = tmp_path / "candidates.json"

    # When
    result = plan_universe(
        bars_path=bars_path,
        decision_date=decision,
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


def _two_symbol_bars(path: object) -> object:
    import datetime as dt

    import polars as pl

    n = 60
    base = dt.date(2026, 1, 5)
    symbols = ["000001", "000002"]
    pl.DataFrame({
        "date": [base + dt.timedelta(days=i) for i in range(n)] * len(symbols),
        "symbol": [s for s in symbols for _ in range(n)],
        "close": ([1000.0] * (n - 1) + [1300.0]) * len(symbols),
        "volume": [1000] * (n * len(symbols)),
        "trade_value_100m": ([100.0] * (n - 1) + [1000.0]) * len(symbols),
        "daily_change_pct": ([0.0] * (n - 1) + [29.9]) * len(symbols),
    }).write_parquet(path)
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
        "overtime_vi_code": "",
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

    bars_path = tmp_path / "bars.parquet"
    decision = _two_symbol_bars(bars_path)
    source = _FakeStatusSource({"000001": True, "000002": False})
    store = SnapshotStore(paths=DataPaths(tmp_path / "data"), session_date=decision)

    result = plan_universe(
        bars_path=bars_path,
        decision_date=decision,
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

    bars_path = tmp_path / "bars.parquet"
    decision = _two_symbol_bars(bars_path)

    class _FailingSource:
        def get_security_status(self, symbol: str) -> dict:
            raise KisApiError("EGW00201", "limit")

    with caplog.at_level(logging.WARNING, logger="src.universe.service"):
        result = plan_universe(
            bars_path=bars_path,
            decision_date=decision,
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

    bars_path = tmp_path / "bars.parquet"
    decision = _two_symbol_bars(bars_path)

    result = plan_universe(
        bars_path=bars_path,
        decision_date=decision,
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

    bars_path = tmp_path / "bars.parquet"
    decision = _two_symbol_bars(bars_path)
    part = tmp_path / "data" / "l1" / "snapshot" / "security_status" / f"dt={decision.isoformat()}.parquet"
    part.parent.mkdir(parents=True, exist_ok=True)
    part.write_bytes(b"\x00\x01broken-parquet")
    store = SnapshotStore(paths=DataPaths(tmp_path / "data"), session_date=decision)

    with caplog.at_level(logging.ERROR, logger="src.universe.service"):
        result = plan_universe(
            bars_path=bars_path,
            decision_date=decision,
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

    bars_path = tmp_path / "bars.parquet"
    decision = _two_symbol_bars(bars_path)

    with pytest.raises(ValueError, match="session_date"):
        plan_universe(
            bars_path=bars_path,
            decision_date=decision,
            out_path=tmp_path / "universe.parquet",
            slot_budget=40,
            status_source=_FakeStatusSource({}),
        )
