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
    from src.realtime.contracts import MarketVenue
    from src.universe.aftermarket import refresh_aftermarket_candidates
    from src.universe.ipc import read_candidate_snapshot

    kst = ZoneInfo('Asia/Seoul')
    generated = dt.datetime(2026, 9, 16, 15, 31, tzinfo=kst)

    class FakeClient:
        def get_trade_amount_ranking(self, *, market_div="J"):
            assert market_div == "J"
            return (KisRankingRow(symbol='005930', rank=1, change_pct=1.0, trade_value_krw=1),)

        def get_fluctuation_ranking(self, *, market_div="J"):
            assert market_div == "J"
            return (KisRankingRow(symbol='005930', rank=1, change_pct=1.0, trade_value_krw=1),)

    out = tmp_path / 'aftermarket.json'
    snapshot = refresh_aftermarket_candidates(session_date=generated.date(), generated_at=generated, client=FakeClient(), out_path=out, capacity=40, venue=MarketVenue.KRX)

    assert snapshot.selected_count == 1
    assert read_candidate_snapshot(out, expected_session_date=generated.date(), expected_session='aftermarket', max_candidates=40) == snapshot


def _aftermarket_rows(symbols: tuple[str, ...], change: float = 1.0):
    from src.execution.kis_client import KisRankingRow

    return tuple(KisRankingRow(symbol=symbol, rank=index + 1, change_pct=change, trade_value_krw=100) for index, symbol in enumerate(symbols))


def test_build_aftermarket_snapshot_excludes_ineligible_symbols() -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.universe.aftermarket import build_aftermarket_snapshot

    generated = dt.datetime(2026, 9, 16, 15, 31, tzinfo=ZoneInfo('Asia/Seoul'))
    snapshot = build_aftermarket_snapshot(
        session_date=generated.date(), generated_at=generated,
        trade_amount_rows=_aftermarket_rows(('005930', '005935')),
        fluctuation_rows=_aftermarket_rows(('005930', '005935')),
        capacity=40, excluded_symbols=frozenset({'005935'}),
    )

    assert [row['symbol'] for row in snapshot.candidates] == ['005930']
    assert snapshot.eligible_count == 1
    assert snapshot.candidates[0]['source_ranks'] == {'trade_amount': 1, 'fluctuation': 1}


def test_build_aftermarket_snapshot_selects_alphanumeric_listing() -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.universe.aftermarket import build_aftermarket_snapshot

    generated = dt.datetime(2026, 9, 16, 15, 31, tzinfo=ZoneInfo('Asia/Seoul'))
    snapshot = build_aftermarket_snapshot(
        session_date=generated.date(), generated_at=generated,
        trade_amount_rows=_aftermarket_rows(('0007J0',), change=29.9),
        fluctuation_rows=_aftermarket_rows(('0007J0',), change=29.9),
        capacity=40,
    )

    assert [row['symbol'] for row in snapshot.candidates] == ['0007J0']
    assert snapshot.candidates[0]['rank'] == 1


def test_build_aftermarket_snapshot_fails_closed_when_exclusion_empties_union() -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    import pytest

    from src.universe.aftermarket import AftermarketUniverseError, build_aftermarket_snapshot

    generated = dt.datetime(2026, 9, 16, 15, 31, tzinfo=ZoneInfo('Asia/Seoul'))
    with pytest.raises(AftermarketUniverseError, match="no eligible aftermarket candidates"):
        build_aftermarket_snapshot(
            session_date=generated.date(), generated_at=generated,
            trade_amount_rows=_aftermarket_rows(('005930',)),
            fluctuation_rows=_aftermarket_rows(('005930',)),
            capacity=40, excluded_symbols=frozenset({'005930'}),
        )


def test_refresh_aftermarket_candidates_round_trips_alphanumeric_symbol(tmp_path) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.universe.aftermarket import refresh_aftermarket_candidates
    from src.universe.ipc import read_candidate_snapshot

    generated = dt.datetime(2026, 9, 16, 15, 31, tzinfo=ZoneInfo('Asia/Seoul'))

    from src.realtime.contracts import MarketVenue

    class FakeClient:
        def get_trade_amount_ranking(self, *, market_div="J"):
            return _aftermarket_rows(('0007J0',), change=29.9)

        def get_fluctuation_ranking(self, *, market_div="J"):
            return _aftermarket_rows(('0007J0',), change=29.9)

    out = tmp_path / 'aftermarket.json'
    snapshot = refresh_aftermarket_candidates(session_date=generated.date(), generated_at=generated, client=FakeClient(), out_path=out, capacity=40, venue=MarketVenue.KRX)

    assert [row['symbol'] for row in snapshot.candidates] == ['0007J0']
    assert read_candidate_snapshot(out, expected_session_date=generated.date(), expected_session='aftermarket', max_candidates=40) == snapshot


def test_defaults_preserve_aftermarket_snapshots() -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.execution.kis_client import KisRankingRow
    from src.universe.aftermarket import build_aftermarket_snapshot

    generated = dt.datetime(2026, 9, 16, 15, 31, tzinfo=ZoneInfo('Asia/Seoul'))
    kwargs: dict = {
        'session_date': generated.date(),
        'generated_at': generated,
        'trade_amount_rows': (
            KisRankingRow(symbol='000660', rank=1, change_pct=3.0, trade_value_krw=900_000_000_000),
            KisRankingRow(symbol='005930', rank=2, change_pct=1.0, trade_value_krw=800_000_000_000),
        ),
        'fluctuation_rows': (
            KisRankingRow(symbol='123456', rank=1, change_pct=29.9, trade_value_krw=10_000_000_000),
            KisRankingRow(symbol='000660', rank=2, change_pct=3.0, trade_value_krw=900_000_000_000),
        ),
        'capacity': 2,
    }

    snapshot = build_aftermarket_snapshot(**kwargs)

    assert snapshot.session == 'aftermarket'
    assert snapshot.effective_from == generated
    assert [row['symbol'] for row in snapshot.candidates] == ['123456', '000660']
    assert [row['rank'] for row in snapshot.candidates] == [1, 2]


def test_session_and_effective_from_propagate() -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.universe.aftermarket import build_aftermarket_snapshot

    generated = dt.datetime(2026, 9, 16, 15, 31, tzinfo=ZoneInfo('Asia/Seoul'))
    effective = dt.datetime(2026, 9, 17, 8, 0, tzinfo=ZoneInfo('Asia/Seoul'))
    rows = _aftermarket_rows(('005930', '000660', '123456'))

    snapshot = build_aftermarket_snapshot(
        session_date=dt.date(2026, 9, 17), generated_at=generated,
        trade_amount_rows=rows, fluctuation_rows=rows, capacity=2,
        session='premarket', effective_from=effective,
    )

    assert snapshot.session == 'premarket'
    assert snapshot.effective_from == effective
    assert snapshot.rev == 20260917
    assert [row['symbol'] for row in snapshot.candidates] == ['005930', '000660']


def test_effective_time_before_generation_is_rejected() -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    import pytest

    from src.execution.kis_client import KisRankingRow
    from src.universe.aftermarket import AftermarketUniverseError, build_aftermarket_snapshot

    generated = dt.datetime(2026, 9, 16, 15, 31, tzinfo=ZoneInfo('Asia/Seoul'))
    row = KisRankingRow(symbol='005930', rank=1, change_pct=1.0, trade_value_krw=1)

    with pytest.raises(AftermarketUniverseError, match='invalid aftermarket source'):
        build_aftermarket_snapshot(
            session_date=generated.date(), generated_at=generated,
            trade_amount_rows=(row,), fluctuation_rows=(row,), capacity=40,
            effective_from=generated - dt.timedelta(seconds=1),
        )


def _venue_fake_client(calls, *, trade_rows, fluc_rows=None, fluc_error=None):
    class FakeClient:
        def get_trade_amount_ranking(self, *, market_div="J"):
            calls.append(("trade_amount", market_div))
            return trade_rows[market_div]

        def get_fluctuation_ranking(self, *, market_div="J"):
            calls.append(("fluctuation", market_div))
            if fluc_error is not None and market_div in fluc_error:
                raise fluc_error[market_div]
            return (fluc_rows or trade_rows)[market_div]

    return FakeClient()


def test_nxt_refresh_uses_nx_screens_only(tmp_path) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.realtime.contracts import MarketVenue
    from src.universe.aftermarket import refresh_aftermarket_candidates

    generated = dt.datetime(2026, 9, 16, 15, 31, tzinfo=ZoneInfo('Asia/Seoul'))
    calls: list = []
    client = _venue_fake_client(calls, trade_rows={"NX": _aftermarket_rows(('000001', '000002')), "J": _aftermarket_rows(('000003',))})
    out = tmp_path / 'aftermarket.nxt.json'

    snapshot = refresh_aftermarket_candidates(
        session_date=generated.date(), generated_at=generated, client=client,
        out_path=out, capacity=40, venue=MarketVenue.NXT,
    )

    assert calls == [("trade_amount", "NX"), ("fluctuation", "NX")]
    assert snapshot.session == 'aftermarket_nxt'
    assert snapshot.policy_version == 'aftermarket_nxt_v1'
    assert out.exists()


def test_krx_refresh_keeps_j_screens(tmp_path) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.realtime.contracts import MarketVenue
    from src.universe.aftermarket import refresh_aftermarket_candidates

    generated = dt.datetime(2026, 9, 16, 15, 31, tzinfo=ZoneInfo('Asia/Seoul'))
    calls: list = []
    client = _venue_fake_client(calls, trade_rows={"NX": _aftermarket_rows(('000001',)), "J": _aftermarket_rows(('000003', '000004'))})
    out = tmp_path / 'aftermarket.json'

    snapshot = refresh_aftermarket_candidates(
        session_date=generated.date(), generated_at=generated, client=client,
        out_path=out, capacity=40, venue=MarketVenue.KRX,
    )

    assert calls == [("trade_amount", "J"), ("fluctuation", "J")]
    assert snapshot.session == 'aftermarket'
    assert snapshot.policy_version == 'aftermarket_v1'


def test_nxt_candidates_subset_of_nx_screens(tmp_path) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.realtime.contracts import MarketVenue
    from src.universe.aftermarket import refresh_aftermarket_candidates

    generated = dt.datetime(2026, 9, 16, 15, 31, tzinfo=ZoneInfo('Asia/Seoul'))
    nx_rows = _aftermarket_rows(('000001', '000002', '000003'))
    client = _venue_fake_client(
        [], trade_rows={"NX": nx_rows, "J": _aftermarket_rows(('000004', '000005'))},
        fluc_rows={"NX": nx_rows, "J": _aftermarket_rows(('000004', '000005'))},
    )

    snapshot = refresh_aftermarket_candidates(
        session_date=generated.date(), generated_at=generated, client=client,
        out_path=tmp_path / 'aftermarket.nxt.json', capacity=40, venue=MarketVenue.NXT,
    )

    assert {row['symbol'] for row in snapshot.candidates} <= {'000001', '000002', '000003'}


def test_nx_ranking_failure_writes_nothing(tmp_path) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    import pytest

    from src.execution.contracts import KisApiError
    from src.realtime.contracts import MarketVenue
    from src.universe.aftermarket import refresh_aftermarket_candidates

    generated = dt.datetime(2026, 9, 16, 15, 31, tzinfo=ZoneInfo('Asia/Seoul'))
    krx_path = tmp_path / 'aftermarket.json'
    krx_path.write_bytes(b'{"pinned": true}')
    before = krx_path.read_bytes()
    nxt_path = tmp_path / 'aftermarket.nxt.json'
    error = KisApiError('SCHEMA', 'nx fluctuation unavailable')
    client = _venue_fake_client(
        [], trade_rows={"NX": _aftermarket_rows(('000001',)), "J": _aftermarket_rows(('000002',))},
        fluc_error={"NX": error},
    )

    with pytest.raises(KisApiError):
        refresh_aftermarket_candidates(
            session_date=generated.date(), generated_at=generated, client=client,
            out_path=nxt_path, capacity=40, venue=MarketVenue.NXT,
        )

    assert not nxt_path.exists()
    assert krx_path.read_bytes() == before


def test_nx_empty_after_exclusions_fails_closed(tmp_path) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    import pytest

    from src.realtime.contracts import MarketVenue
    from src.universe.aftermarket import AftermarketUniverseError, refresh_aftermarket_candidates

    generated = dt.datetime(2026, 9, 16, 15, 31, tzinfo=ZoneInfo('Asia/Seoul'))
    nx_rows = _aftermarket_rows(('000001',))
    client = _venue_fake_client([], trade_rows={"NX": nx_rows, "J": _aftermarket_rows(('000002',))})
    out = tmp_path / 'aftermarket.nxt.json'

    with pytest.raises(AftermarketUniverseError):
        refresh_aftermarket_candidates(
            session_date=generated.date(), generated_at=generated, client=client,
            out_path=out, capacity=40, venue=MarketVenue.NXT,
            excluded_symbols=frozenset({'000001'}),
        )

    assert not out.exists()


def test_candidate_session_per_venue() -> None:
    from src.realtime.contracts import MarketVenue
    from src.universe.aftermarket import aftermarket_candidate_session

    assert aftermarket_candidate_session(MarketVenue.KRX) == 'aftermarket'
    assert aftermarket_candidate_session(MarketVenue.NXT) == 'aftermarket_nxt'


def test_unknown_venue_rejected(tmp_path) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    import pytest

    from src.universe.aftermarket import aftermarket_candidate_session, refresh_aftermarket_candidates

    with pytest.raises(ValueError, match="unknown aftermarket venue"):
        aftermarket_candidate_session("xyz")  # type: ignore[arg-type]

    generated = dt.datetime(2026, 9, 16, 15, 31, tzinfo=ZoneInfo('Asia/Seoul'))
    with pytest.raises(ValueError, match="unknown aftermarket venue"):
        refresh_aftermarket_candidates(
            session_date=generated.date(), generated_at=generated, client=object(),  # type: ignore[arg-type]
            out_path=tmp_path / "aftermarket.json", capacity=40, venue="xyz",  # type: ignore[arg-type]
        )
