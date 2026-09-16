def test_build_aftermarket_snapshot_orders_limit_up_and_preserves_audit_fields() -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.execution.kis_client import KisRankingRow
    from src.universe.aftermarket import build_aftermarket_snapshot

    kst = ZoneInfo('Asia/Seoul')
    generated = dt.datetime(2026, 9, 16, 15, 31, tzinfo=kst)
    snapshot = build_aftermarket_snapshot(
        session_date=generated.date(),
        generated_at=generated,
        trade_amount_rows=(
            KisRankingRow(symbol='000660', rank=1, change_pct=3.0, trade_value_krw=900_000_000_000),
            KisRankingRow(symbol='005930', rank=2, change_pct=1.0, trade_value_krw=800_000_000_000),
        ),
        fluctuation_rows=(
            KisRankingRow(symbol='123456', rank=1, change_pct=29.9, trade_value_krw=10_000_000_000),
            KisRankingRow(symbol='000660', rank=2, change_pct=3.0, trade_value_krw=900_000_000_000),
        ),
        capacity=2,
    )

    assert snapshot.session == 'aftermarket'
    assert snapshot.session_date == dt.date(2026, 9, 16)
    assert snapshot.source_asof == generated == snapshot.effective_from
    assert (snapshot.eligible_count, snapshot.selected_count, snapshot.capacity) == (3, 2, 2)
    assert [row['symbol'] for row in snapshot.candidates] == ['123456', '000660']
    assert [row['rank'] for row in snapshot.candidates] == [1, 2]
    assert snapshot.candidates[0]['selection_reasons'] == ['limit_up', 'fluctuation']
    assert snapshot.candidates[1]['source_ranks'] == {'trade_amount': 1, 'fluctuation': 2}
    assert snapshot.candidates[1]['metrics']['trade_value_krw'] == 900_000_000_000


def test_build_aftermarket_snapshot_rejects_empty_or_duplicate_source() -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    import pytest

    from src.execution.kis_client import KisRankingRow
    from src.universe.aftermarket import AftermarketUniverseError, build_aftermarket_snapshot

    generated = dt.datetime(2026, 9, 16, 15, 31, tzinfo=ZoneInfo('Asia/Seoul'))
    row = KisRankingRow(symbol='005930', rank=1, change_pct=1.0, trade_value_krw=1)

    with pytest.raises(AftermarketUniverseError):
        build_aftermarket_snapshot(
            session_date=generated.date(), generated_at=generated, trade_amount_rows=(),
            fluctuation_rows=(row,), capacity=40,
        )
    with pytest.raises(AftermarketUniverseError):
        build_aftermarket_snapshot(
            session_date=generated.date(), generated_at=generated, trade_amount_rows=(row, row),
            fluctuation_rows=(row,), capacity=40,
        )


def test_build_aftermarket_snapshot_uses_fluctuation_change_for_limit_up() -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.execution.kis_client import KisRankingRow
    from src.universe.aftermarket import build_aftermarket_snapshot

    generated = dt.datetime(2026, 9, 16, 15, 31, tzinfo=ZoneInfo("Asia/Seoul"))
    snapshot = build_aftermarket_snapshot(
        session_date=generated.date(), generated_at=generated,
        trade_amount_rows=(KisRankingRow("005930", 1, 1.0, 100),),
        fluctuation_rows=(KisRankingRow("005930", 1, 29.9, 100),), capacity=1,
    )

    assert snapshot.candidates[0]["selection_reasons"] == ["limit_up", "trade_amount", "fluctuation"]
    assert snapshot.candidates[0]["metrics"]["change_pct"] == 29.9


def test_refresh_aftermarket_candidates_writes_snapshot(tmp_path) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.execution.kis_client import KisRankingRow
    from src.universe.aftermarket import refresh_aftermarket_candidates
    from src.universe.ipc import read_candidate_snapshot

    kst = ZoneInfo('Asia/Seoul')
    generated = dt.datetime(2026, 9, 16, 15, 31, tzinfo=kst)

    class FakeClient:
        def get_trade_amount_ranking(self):
            return (KisRankingRow(symbol='005930', rank=1, change_pct=1.0, trade_value_krw=1),)

        def get_fluctuation_ranking(self):
            return (KisRankingRow(symbol='005930', rank=1, change_pct=1.0, trade_value_krw=1),)

    out = tmp_path / 'aftermarket.json'
    snapshot = refresh_aftermarket_candidates(session_date=generated.date(), generated_at=generated, client=FakeClient(), out_path=out, capacity=40)

    assert snapshot.selected_count == 1
    assert read_candidate_snapshot(out, expected_session_date=generated.date(), expected_session='aftermarket', max_candidates=40) == snapshot
