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


def test_universe_plan_run_emits_candidates_when_path_given(tmp_path):
    # Given: 상한가 1종목이 나오는 일봉 + --candidates-path 지정
    import argparse
    import datetime as dt

    import polars as pl

    from src.cli.universe_plan import run
    from src.collector.ipc import read_candidates

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

    cand_path = tmp_path / "candidates.json"
    args = argparse.Namespace(
        bars_path=str(bars_path), decision_date=decision.isoformat(),
        out_path=str(tmp_path / "u.parquet"), slot_budget=40,
        candidates_path=str(cand_path),
    )

    # When
    rc = run(args)

    # Then: parquet + candidates.json 동시 생성
    assert rc == 0
    got = read_candidates(cand_path)
    assert got["candidates"][0]["symbol"] == "000001"


def test_universe_plan_run_creates_missing_out_dir(tmp_path) -> None:
    import argparse
    import datetime as dt
    import polars as pl
    from src.cli.universe_plan import run

    n = 60
    base = dt.date(2026, 1, 5)
    decision = base + dt.timedelta(days=n - 1)
    bars = pl.DataFrame({
        'date': [base + dt.timedelta(days=i) for i in range(n)],
        'symbol': ['005930'] * n,
        'close': [1000.0 + i for i in range(n)],
        'volume': [1000] * n,
        'trade_value_100m': [100.0] * (n - 1) + [900.0],
        'daily_change_pct': [1.0] * (n - 2) + [30.0, 15.0],
    })
    bars_path = tmp_path / 'bars.parquet'
    bars.write_parquet(bars_path)
    out_path = tmp_path / 'nested' / 'does' / 'not' / 'exist' / 'out.parquet'

    args = argparse.Namespace(bars_path=str(bars_path), decision_date=decision.isoformat(),
                              out_path=str(out_path), slot_budget=40, candidates_path=None)

    rc = run(args)

    assert rc == 0
    assert out_path.exists()
