from __future__ import annotations

import datetime as dt

import polars as pl

from src.core.config import DataPaths, UniverseTierSettings
from src.execution.contracts import KisApiError
from src.storage.snapshot_store import SnapshotStore
from src.universe.ipc import read_candidates
from src.universe.service import plan_universe

BASE = dt.date(2026, 1, 5)
SESSIONS = 60


def _seed(root, symbols: list[str], *, decision_change: float = 29.9) -> dt.date:
    from src.marketdata.bar_store import append_daily_bars

    n = SESSIONS
    decision = BASE + dt.timedelta(days=n - 1)
    frame = pl.DataFrame({
        "date": [BASE + dt.timedelta(days=i) for i in range(n)] * len(symbols),
        "symbol": [s for s in symbols for _ in range(n)],
        "close": ([1000.0] * (n - 1) + [1000.0 * (1 + decision_change / 100)]) * len(symbols),
        "volume": [1000] * (n * len(symbols)),
        "trade_value_100m": ([100.0] * (n - 1) + [1000.0]) * len(symbols),
        "daily_change_pct": ([0.0] * (n - 1) + [decision_change]) * len(symbols),
    })
    append_daily_bars(root, frame)
    return decision


def _status_row(symbol: str, *, short_overheated: bool = False) -> dict[str, object]:
    return {
        "source_tr": "FHKST01010100",
        "market_div_code": "J",
        "symbol": symbol,
        "status_code": "59" if short_overheated else "00",
        "managed": False,
        "market_warning_code": "00",
        "short_overheated": short_overheated,
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


class _Source:
    def __init__(self, rows: dict[str, dict[str, object] | Exception]) -> None:
        self.rows = rows

    def get_security_status(self, symbol: str) -> dict[str, object]:
        row = self.rows[symbol]
        if isinstance(row, Exception):
            raise row
        return row


def test_lookup_failure_keeps_symbol_as_unknown(tmp_path) -> None:
    bars_root = tmp_path / "bars"
    decision = _seed(bars_root, ["000001", "000002"])
    source = _Source({
        "000001": KisApiError("EGW00201", "rate limited"),
        "000002": _status_row("000002"),
    })
    out = tmp_path / "universe.parquet"

    result = plan_universe(
        bars_root=bars_root,
        decision_date=decision,
        lookback_calendar_days=150,
        out_path=out,
        slot_budget=40,
        session_date=decision,
        status_source=source,
        wall_ns=lambda: 1,
    )

    assert result.status_unknown == 1
    assert result.status_excluded == 0
    stored = pl.read_parquet(out).sort("symbol")
    assert stored["symbol"].to_list() == ["000001", "000002"]
    assert stored["warning_level"].to_list() == ["unknown", "none"]
    assert stored["status_flags"].to_list() == [[], []]


def test_excluded_slot_is_not_back_filled(tmp_path) -> None:
    bars_root = tmp_path / "bars"
    decision = _seed(bars_root, ["000001", "000002", "000003", "000004"])
    source = _Source({
        "000001": _status_row("000001"),
        "000002": _status_row("000002", short_overheated=True),
        "000003": _status_row("000003"),
        "000004": _status_row("000004"),
    })
    out = tmp_path / "universe.parquet"
    candidates = tmp_path / "candidates.json"

    result = plan_universe(
        bars_root=bars_root,
        decision_date=decision,
        lookback_calendar_days=150,
        out_path=out,
        slot_budget=3,
        candidates_path=candidates,
        session_date=decision,
        status_source=source,
        snapshot_store=SnapshotStore(paths=DataPaths(tmp_path / "data"), session_date=decision),
        wall_ns=lambda: 1,
    )

    assert result.selected == 2
    assert result.status_excluded == 1
    assert result.excluded_by_flag["short_overheated"] == 1
    assert result.dropped == 1
    stored = pl.read_parquet(out)
    assert sorted(stored["symbol"].to_list()) == ["000001", "000003"]
    assert "000004" not in stored["symbol"].to_list()


def test_universe_parquet_carries_tags_and_policy_version(tmp_path) -> None:
    bars_root = tmp_path / "bars"
    decision = _seed(bars_root, ["000001"], decision_change=-12.0)
    out = tmp_path / "universe.parquet"
    candidates = tmp_path / "candidates.json"

    plan_universe(
        bars_root=bars_root,
        decision_date=decision,
        lookback_calendar_days=150,
        out_path=out,
        slot_budget=40,
        candidates_path=candidates,
        session_date=decision,
        status_source=_Source({"000001": _status_row("000001")}),
        wall_ns=lambda: 1,
        tiers=UniverseTierSettings(downside_enabled=True),
    )

    stored = pl.read_parquet(out)
    for column in ("direction", "warning_level", "status_flags", "policy_version"):
        assert column in stored.columns
    assert stored["policy_version"].unique().to_list() == ["tiered_v2_symmetric"]
    assert stored["direction"].to_list() == ["down"]
    payload = read_candidates(candidates)
    assert payload is not None
    row = payload["candidates"][0]
    assert row["direction"] == "down"
    assert row["warning_level"] == "none"



def test_policy_dropped_rows_written_without_status_columns(tmp_path) -> None:
    bars_root = tmp_path / "bars"
    symbols = ["000001", "000002", "000003", "000004"]
    decision = _seed(bars_root, symbols)
    out = tmp_path / "universe.parquet"
    paths = DataPaths(tmp_path / "data")

    result = plan_universe(
        bars_root=bars_root,
        decision_date=decision,
        lookback_calendar_days=150,
        out_path=out,
        slot_budget=2,
        session_date=decision,
        status_source=_Source({symbol: _status_row(symbol) for symbol in symbols}),
        wall_ns=lambda: 1,
        paths=paths,
    )

    dropped = pl.read_parquet(paths.universe_dropped_out(decision))
    universe = pl.read_parquet(out)
    assert paths.universe_dropped_out(decision).parent == paths.universe_dropped_dir
    assert result.dropped == 2
    assert dropped.height == 2
    assert sorted(dropped["symbol"].to_list()) == sorted(result.dropped_symbols)
    assert set(dropped["symbol"].to_list()).isdisjoint(universe["symbol"].to_list())
    assert dropped.columns == [c for c in universe.columns if c not in ("warning_level", "status_flags")]
    assert dropped["policy_version"].unique().to_list() == universe["policy_version"].unique().to_list()
    assert not list(paths.universe_dropped_dir.glob("*.tmp"))


def test_empty_policy_dropped_writes_schema_only_file(tmp_path) -> None:
    bars_root = tmp_path / "bars"
    decision = _seed(bars_root, ["000001", "000002"])
    paths = DataPaths(tmp_path / "data")

    result = plan_universe(
        bars_root=bars_root,
        decision_date=decision,
        lookback_calendar_days=150,
        out_path=tmp_path / "universe.parquet",
        slot_budget=40,
        paths=paths,
    )

    dropped = pl.read_parquet(paths.universe_dropped_out(decision))
    assert result.dropped == 0
    assert dropped.height == 0
    assert "policy_version" in dropped.columns
    assert "warning_level" not in dropped.columns
    assert "status_flags" not in dropped.columns
