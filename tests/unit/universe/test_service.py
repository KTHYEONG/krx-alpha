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
