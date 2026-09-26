"""Forward-sync invariants for nightly program-trades convergence."""

from __future__ import annotations

import datetime as dt


def _pt_row(symbol: str, day: dt.date) -> dict[str, object]:
    return {
        "symbol": symbol,
        "date": day,
        "arbitrage_buy_volume": 10,
        "arbitrage_sell_volume": 4,
        "arbitrage_net_volume": 6,
        "non_arbitrage_buy_volume": 20,
        "non_arbitrage_sell_volume": 5,
        "non_arbitrage_net_volume": 15,
    }


def _seed(store, entries: dict[str, list[dt.date]]) -> None:
    from src.marketdata.toss_program_trades import append_program_trades

    rows = [_pt_row(symbol, day) for symbol, days in entries.items() for day in days]
    append_program_trades(store, rows)


def _range_rows(symbol: str, start: dt.date, end: dt.date) -> tuple[dict[str, object], ...]:
    day = start
    out: list[dict[str, object]] = []
    while day <= end:
        out.append(_pt_row(symbol, day))
        day += dt.timedelta(days=1)
    return tuple(out)


def test_forward_sync_fetches_only_symbols_behind_session_date(tmp_path, monkeypatch) -> None:
    # Given: A는 세션일보다 뒤처지고 B는 이미 최신인 스토어
    import datetime as dt

    from src.marketdata import program_trade_service as service

    store = tmp_path / "program_trades"
    _seed(store, {"005930": [dt.date(2026, 9, 17)], "000660": [dt.date(2026, 9, 25)]})
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: "tok")
    calls: dict[str, dt.date] = {}

    def _fake_history(symbol: str, **kwargs) -> tuple:
        calls[symbol] = kwargs["min_date"]
        return ()

    monkeypatch.setattr(service, "backfill_program_trades_history", _fake_history)

    # When
    result = service.sync_program_trades_forward(
        store_root=store,
        session_date=dt.date(2026, 9, 25),
        complete_through=dt.date(2026, 9, 24),
        candidate_symbols=(),
        lookback_days=120,
        app_key="k",
        app_secret="s",
        rate_per_s=1000.0,
    )

    # Then: 뒤처진 A만 min_date=max+1일로 조회한다
    assert result.forward_targets == ("005930",)
    assert calls == {"005930": dt.date(2026, 9, 18)}
    assert (result.symbols_ok, result.symbols_failed, result.appended_rows) == (1, 0, 0)


def test_forward_sync_closes_gap_in_one_run(tmp_path, monkeypatch) -> None:
    # Given: 09-17에 멈춘 A와 09-18..09-25를 serving하는 가짜 벤더
    import datetime as dt

    from src.marketdata import program_trade_service as service
    from src.marketdata.toss_program_trades import program_trade_coverage

    store = tmp_path / "program_trades"
    _seed(store, {"005930": [dt.date(2026, 9, 17)]})
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: "tok")

    def _fake_history(symbol: str, **kwargs) -> tuple:
        assert kwargs["min_date"] == dt.date(2026, 9, 18)
        return _range_rows(symbol, dt.date(2026, 9, 18), dt.date(2026, 9, 25))

    monkeypatch.setattr(service, "backfill_program_trades_history", _fake_history)

    # When
    result = service.sync_program_trades_forward(
        store_root=store,
        session_date=dt.date(2026, 9, 25),
        complete_through=dt.date(2026, 9, 24),
        candidate_symbols=(),
        lookback_days=120,
        app_key="k",
        app_secret="s",
        rate_per_s=1000.0,
    )

    # Then: 한 번의 실행으로 최신일까지 수렴하고 신규 일자만큼 추가된다
    assert program_trade_coverage(store, ("005930",))["005930"][1] == dt.date(2026, 9, 25)
    assert result.appended_rows == 8
    assert result.stale_symbols == ()


def test_forward_sync_backfills_candidate_lacking_history(tmp_path, monkeypatch) -> None:
    # Given: 스토어에 없는 후보 C와 최신인 기존 종목
    import datetime as dt

    from src.marketdata import program_trade_service as service
    from src.marketdata.toss_program_trades import program_trade_coverage

    store = tmp_path / "program_trades"
    _seed(store, {"005930": [dt.date(2026, 9, 25)]})
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: "tok")
    seen: dict[str, dt.date] = {}

    def _fake_history(symbol: str, **kwargs) -> tuple:
        seen[symbol] = kwargs["min_date"]
        return (_pt_row(symbol, dt.date(2026, 9, 25)),)

    monkeypatch.setattr(service, "backfill_program_trades_history", _fake_history)

    # When
    result = service.sync_program_trades_forward(
        store_root=store,
        session_date=dt.date(2026, 9, 25),
        complete_through=dt.date(2026, 9, 24),
        candidate_symbols=("000660", "BAD!"),
        lookback_days=10,
        app_key="k",
        app_secret="s",
        rate_per_s=1000.0,
    )

    # Then: C는 세션일-lookback부터 조회되고 malformed 코드는 제외된다
    assert result.candidate_targets == ("000660",)
    assert seen == {"000660": dt.date(2026, 9, 15)}
    assert program_trade_coverage(store, ("000660",))["000660"][1] == dt.date(2026, 9, 25)


def test_forward_sync_writes_store_once(tmp_path, monkeypatch) -> None:
    # Given: 5개 뒤처진 종목과 행을 반환하는 가짜 벤더
    import datetime as dt

    from src.marketdata import program_trade_service as service

    symbols = tuple(f"{i + 1:06d}" for i in range(5))
    store = tmp_path / "program_trades"
    _seed(store, {symbol: [dt.date(2026, 9, 17)] for symbol in symbols})
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: "tok")
    monkeypatch.setattr(
        service,
        "backfill_program_trades_history",
        lambda symbol, **kwargs: (_pt_row(symbol, dt.date(2026, 9, 18)),),
    )
    writes: list[object] = []
    real_append = service.append_program_trades

    def _counting_append(store_path, rows) -> int:
        writes.append(list(rows))
        return real_append(store_path, rows)

    monkeypatch.setattr(service, "append_program_trades", _counting_append)

    # When
    result = service.sync_program_trades_forward(
        store_root=store,
        session_date=dt.date(2026, 9, 25),
        complete_through=dt.date(2026, 9, 17),
        candidate_symbols=(),
        lookback_days=120,
        app_key="k",
        app_secret="s",
        rate_per_s=1000.0,
    )

    # Then: 5개 종목분이 단 한 번의 쓰기로 적재된다
    assert len(writes) == 1
    assert sorted(row["symbol"] for row in writes[0]) == sorted(symbols)
    assert result.appended_rows == 5


def test_forward_sync_isolates_symbol_failure(tmp_path, monkeypatch, caplog) -> None:
    # Given: B의 조회가 실패하는 벤더
    import datetime as dt
    import logging

    from src.marketdata import program_trade_service as service
    from src.marketdata.partitioned_store import scan_month_partitions
    from src.marketdata.toss_program_trades import TossProgramTradesError

    store = tmp_path / "program_trades"
    _seed(store, {"005930": [dt.date(2026, 9, 17)], "000660": [dt.date(2026, 9, 17)]})

    def _fake_history(symbol: str, **kwargs) -> tuple:
        if symbol == "000660":
            raise TossProgramTradesError("404 gone")
        return (_pt_row(symbol, dt.date(2026, 9, 18)),)

    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: "tok")
    monkeypatch.setattr(service, "backfill_program_trades_history", _fake_history)

    # When
    with caplog.at_level(logging.WARNING):
        result = service.sync_program_trades_forward(
            store_root=store,
            session_date=dt.date(2026, 9, 25),
            complete_through=dt.date(2026, 9, 17),
            candidate_symbols=(),
            lookback_days=120,
            app_key="k",
            app_secret="s",
            rate_per_s=1000.0,
        )

    # Then: 나머지 종목은 저장되고 실패만 격리된다
    assert (result.symbols_ok, result.symbols_failed) == (1, 1)
    saved = scan_month_partitions(store).collect()
    assert saved.filter(saved["symbol"] == "005930").height == 2
    assert saved.filter(saved["symbol"] == "000660").height == 1
    assert sum("status=SKIP" in record.message for record in caplog.records) == 1


def test_forward_sync_measures_staleness_against_complete_through(tmp_path, monkeypatch) -> None:
    # Given: 100개 추적 종목 중 3개 상장폐지 종목이 과거에 멈춘 스토어
    import datetime as dt

    from src.marketdata import program_trade_service as service

    symbols = [f"{i + 1:06d}" for i in range(100)]
    stuck = set(symbols[:3])
    store = tmp_path / "program_trades"
    _seed(
        store,
        {symbol: [dt.date(2026, 9, 10)] if symbol in stuck else [dt.date(2026, 9, 24)] for symbol in symbols},
    )
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: "tok")
    monkeypatch.setattr(service, "backfill_program_trades_history", lambda symbol, **kwargs: ())

    # When
    result = service.sync_program_trades_forward(
        store_root=store,
        session_date=dt.date(2026, 9, 25),
        complete_through=dt.date(2026, 9, 24),
        candidate_symbols=(),
        lookback_days=120,
        app_key="k",
        app_secret="s",
        rate_per_s=1000.0,
    )

    # Then: 3개만 stale이고 비율 0.03은 허용치 이내이다
    assert result.stale_symbols == tuple(sorted(stuck))
    assert len(result.tracked) == 100
    assert len(result.stale_symbols) / len(result.tracked) == 0.03


def test_forward_sync_starts_from_empty_store_with_candidates(tmp_path, monkeypatch) -> None:
    # Given: 파티션이 하나도 없는 빈 스토어와 후보 1개
    import datetime as dt

    from src.marketdata import program_trade_service as service

    store = tmp_path / "program_trades"
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: "tok")
    monkeypatch.setattr(
        service,
        "backfill_program_trades_history",
        lambda symbol, **kwargs: (_pt_row(symbol, dt.date(2026, 9, 24)),),
    )

    # When
    result = service.sync_program_trades_forward(
        store_root=store,
        session_date=dt.date(2026, 9, 25),
        complete_through=dt.date(2026, 9, 24),
        candidate_symbols=("005930",),
        lookback_days=120,
        app_key="k",
        app_secret="s",
        rate_per_s=1000.0,
    )

    # Then: 후보가 곧 추적 집합이 되고 stale이 없다
    assert result.forward_targets == ()
    assert result.candidate_targets == ("005930",)
    assert result.tracked == ("005930",)
    assert result.stale_symbols == ()
    assert result.appended_rows == 1


def test_forward_sync_wraps_token_issuance_failure(tmp_path, monkeypatch) -> None:
    # Given: 토큰 발급이 실패하는 벤더
    import datetime as dt

    import pytest

    from src.marketdata import program_trade_service as service
    from src.marketdata.toss_calendar import TossCalendarError
    from src.marketdata.toss_program_trades import TossProgramTradesError

    store = tmp_path / "program_trades"
    _seed(store, {"005930": [dt.date(2026, 9, 17)]})
    monkeypatch.setattr(
        service, "issue_access_token", lambda **kwargs: (_ for _ in ()).throw(TossCalendarError("auth down"))
    )

    # When / Then
    with pytest.raises(TossProgramTradesError, match="token issuance"):
        service.sync_program_trades_forward(
            store_root=store,
            session_date=dt.date(2026, 9, 25),
            complete_through=dt.date(2026, 9, 24),
            candidate_symbols=(),
            lookback_days=120,
            app_key="k",
            app_secret="s",
            rate_per_s=1000.0,
        )


def test_forward_sync_rejects_file_path_store(tmp_path) -> None:
    # Given: 디렉터리가 아닌 레거시 파일 경로
    import datetime as dt

    import pytest

    from src.marketdata import program_trade_service as service
    from src.marketdata.toss_program_trades import TossProgramTradesError

    store = tmp_path / "program_trades.parquet"
    store.write_bytes(b"legacy bytes")

    # When / Then
    with pytest.raises(TossProgramTradesError, match="unreadable"):
        service.sync_program_trades_forward(
            store_root=store,
            session_date=dt.date(2026, 9, 25),
            complete_through=dt.date(2026, 9, 24),
            candidate_symbols=(),
            lookback_days=120,
            app_key="k",
            app_secret="s",
            rate_per_s=1000.0,
        )
    assert store.read_bytes() == b"legacy bytes"


def test_forward_sync_rejects_corrupt_store(tmp_path) -> None:
    # Given: 손상된 parquet 바이트 파티션
    import datetime as dt

    import pytest

    from src.marketdata import program_trade_service as service
    from src.marketdata.toss_program_trades import TossProgramTradesError

    store = tmp_path / "program_trades"
    store.mkdir(parents=True, exist_ok=True)
    (store / "2026-09.parquet").write_bytes(b"not-a-parquet")

    # When / Then: fail-closed
    with pytest.raises(TossProgramTradesError, match="unreadable"):
        service.sync_program_trades_forward(
            store_root=store,
            session_date=dt.date(2026, 9, 25),
            complete_through=dt.date(2026, 9, 24),
            candidate_symbols=("005930",),
            lookback_days=120,
            app_key="k",
            app_secret="s",
            rate_per_s=1000.0,
        )


def test_forward_sync_excludes_delisted_symbols_from_targets_and_staleness(tmp_path, monkeypatch) -> None:
    # Given: 상장 종목 A(뒤처짐)와 상장폐지 종목 D(과거에 멈춤)
    import datetime as dt

    from src.marketdata import program_trade_service as service

    store = tmp_path / "program_trades"
    _seed(store, {"005930": [dt.date(2026, 9, 17)], "999990": [dt.date(2025, 3, 1)]})
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: "tok")
    calls: list[str] = []

    def _fake_history(symbol: str, **kwargs) -> tuple:
        calls.append(symbol)
        return _range_rows(symbol, dt.date(2026, 9, 18), dt.date(2026, 9, 25))

    monkeypatch.setattr(service, "backfill_program_trades_history", _fake_history)

    # When
    result = service.sync_program_trades_forward(
        store_root=store,
        session_date=dt.date(2026, 9, 25),
        complete_through=dt.date(2026, 9, 24),
        candidate_symbols=(),
        lookback_days=120,
        app_key="k",
        app_secret="s",
        rate_per_s=1000.0,
        active_symbols=frozenset({"005930"}),
    )

    # Then: 상장폐지 종목은 조회하지도, 누락으로 세지도 않는다
    assert calls == ["005930"]
    assert result.tracked == ("005930",)
    assert result.stale_symbols == ()
