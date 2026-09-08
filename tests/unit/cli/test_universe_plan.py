"""Universe-plan CLI unit tests."""

from __future__ import annotations


def test_universe_plan_run_writes_selection_output(tmp_path):
    # Given: 일봉 parquet 과 CLI 인자
    import argparse
    import datetime as dt
    import polars as pl
    from src.cli.universe_plan import run

    n = 60
    base = dt.date(2026, 1, 5)
    decision = base + dt.timedelta(days=n - 1)
    bars_path = tmp_path / "bars.parquet"
    out_path = tmp_path / "universe.parquet"
    pl.DataFrame({
        "date": [base + dt.timedelta(days=i) for i in range(n)],
        "symbol": ["000001"] * n,
        "close": [1000.0] * (n - 1) + [1300.0],
        "volume": [1000] * n,
        "trade_value_100m": [100.0] * (n - 1) + [1000.0],
        "daily_change_pct": [0.0] * (n - 1) + [29.9],
    }).write_parquet(bars_path)

    args = argparse.Namespace(
        bars_path=str(bars_path),
        decision_date=decision.isoformat(),
        out_path=str(out_path),
        slot_budget=40,
    )

    # When
    exit_code = run(args)

    # Then: 종료코드 0 과 선정 결과 parquet 이 생성된다
    assert exit_code == 0
    written = pl.read_parquet(out_path)
    assert written["symbol"].to_list() == ["000001"]
    assert "limit_up" in written["selection_reasons"].to_list()[0]
