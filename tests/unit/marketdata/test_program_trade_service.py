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


def test_forward_sync_rotates_once_on_auth_rejection_then_retries(tmp_path, monkeypatch) -> None:
    # Given: 첫 조회가 인증 거부되는 벤더
    import datetime as dt

    from src.marketdata import program_trade_service as service
    from src.marketdata.toss_program_trades import _TossProgramTradesAuthRejected

    store = tmp_path / "program_trades"
    _seed(store, {"005930": [dt.date(2026, 9, 17)]})
    tokens: list[str] = []
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: tokens.append("oauth") or f"tok-{len(tokens)}")
    attempts: list[str] = []

    def _flaky_history(symbol: str, **kwargs) -> tuple:
        attempts.append(kwargs["access_token"])
        if len(attempts) == 1:
            raise _TossProgramTradesAuthRejected("stale token")
        return (_pt_row(symbol, dt.date(2026, 9, 18)),)

    monkeypatch.setattr(service, "backfill_program_trades_history", _flaky_history)

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

    # Then: 같은 요청이 회전된 토큰으로 1회 재시도되고 oauth는 2회 호출된다
    assert attempts == ["tok-1", "tok-2"]
    assert tokens == ["oauth", "oauth"]
    assert result.symbols_ok == 1


def test_forward_sync_second_rejection_raises(tmp_path, monkeypatch) -> None:
    # Given: 계속 거부되는 벤더
    import datetime as dt

    import pytest

    from src.marketdata import program_trade_service as service
    from src.marketdata.toss_program_trades import TossProgramTradesError, _TossProgramTradesAuthRejected

    store = tmp_path / "program_trades"
    _seed(store, {"005930": [dt.date(2026, 9, 17)]})
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: "tok")
    monkeypatch.setattr(
        service,
        "backfill_program_trades_history",
        lambda symbol, **kwargs: (_ for _ in ()).throw(_TossProgramTradesAuthRejected("stale")),
    )

    # When / Then: 두 번째 거부는 기존 TossProgramTradesError로 표면화된다
    with pytest.raises(TossProgramTradesError):
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


def test_backfill_rejects_non_positive_rate_before_any_call(tmp_path, monkeypatch) -> None:
    # Given: 토큰 발급 감시
    import datetime as dt

    import pytest

    from src.marketdata import program_trade_service as service

    calls: list[str] = []
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: calls.append("token") or "tok")

    # When / Then
    with pytest.raises(ValueError, match="positive"):
        service.backfill_program_trades(
            store_path=tmp_path / "program_trades",
            symbols=("005930",),
            min_date=dt.date(2026, 9, 1),
            app_key="k",
            app_secret="s",
            rate_per_s=0.0,
        )
    assert calls == []


def test_forward_sync_rotation_issuance_failure_wraps(tmp_path, monkeypatch) -> None:
    # Given: 거부 후 회전 발급마저 실패하는 벤더
    import datetime as dt

    import pytest

    from src.marketdata import program_trade_service as service
    from src.marketdata.toss_calendar import TossCalendarError
    from src.marketdata.toss_program_trades import TossProgramTradesError, _TossProgramTradesAuthRejected

    store = tmp_path / "program_trades"
    _seed(store, {"005930": [dt.date(2026, 9, 17)]})
    calls: list[str] = []

    def _issue(**kwargs: object) -> str:
        calls.append("oauth")
        if len(calls) == 1:
            return "tok-1"
        raise TossCalendarError("auth down")

    monkeypatch.setattr(service, "issue_access_token", _issue)
    monkeypatch.setattr(
        service,
        "backfill_program_trades_history",
        lambda symbol, **kwargs: (_ for _ in ()).throw(_TossProgramTradesAuthRejected("stale")),
    )

    # When / Then: 회전 발급 실패는 TossProgramTradesError로 표면화된다
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


def test_backfill_rotates_once_on_auth_rejection_then_retries(tmp_path, monkeypatch) -> None:
    # Given: 첫 조회가 인증 거부되는 벤더
    import datetime as dt

    from src.marketdata import program_trade_service as service
    from src.marketdata.toss_program_trades import _TossProgramTradesAuthRejected

    tokens: list[str] = []
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: tokens.append("oauth") or f"tok-{len(tokens)}")
    attempts: list[str] = []

    def _flaky_history(symbol: str, **kwargs) -> tuple:
        attempts.append(kwargs["access_token"])
        if len(attempts) == 1:
            raise _TossProgramTradesAuthRejected("stale token")
        return (_pt_row(symbol, dt.date(2026, 9, 18)),)

    monkeypatch.setattr(service, "backfill_program_trades_history", _flaky_history)
    monkeypatch.setattr(service, "append_program_trades", lambda *a, **k: 1)

    # When
    result = service.backfill_program_trades(
        store_path=tmp_path / "program_trades",
        symbols=("005930",),
        min_date=dt.date(2026, 9, 1),
        app_key="k",
        app_secret="s",
        rate_per_s=1000.0,
    )

    # Then: 같은 요청이 회전된 토큰으로 1회 재시도된다
    assert attempts == ["tok-1", "tok-2"]
    assert result.symbols_ok == 1


def test_forward_sync_skips_symbol_failing_after_rotation_and_keeps_others(tmp_path, monkeypatch) -> None:
    # Given: 005930 은 회전 후 재시도에서 404, 000660 은 정상
    from src.marketdata import program_trade_service as service
    from src.marketdata.toss_program_trades import (
        TossProgramTradesError,
        _TossProgramTradesAuthRejected,
        program_trade_coverage,
    )

    store = tmp_path / "program_trades"
    _seed(store, {"005930": [dt.date(2026, 9, 17)], "000660": [dt.date(2026, 9, 17)]})
    issued = iter(["tok-1", "tok-2"])
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: next(issued))

    def _fetch(symbol, *, access_token, min_date, **kwargs):
        if symbol == "005930":
            if access_token == "tok-1":
                raise _TossProgramTradesAuthRejected("stale")
            raise TossProgramTradesError("404 delisted")
        return _range_rows(symbol, min_date, dt.date(2026, 9, 25))

    monkeypatch.setattr(service, "backfill_program_trades_history", _fetch)

    # When
    out = service.sync_program_trades_forward(
        store_root=store,
        session_date=dt.date(2026, 9, 25),
        complete_through=dt.date(2026, 9, 24),
        candidate_symbols=(),
        lookback_days=120,
        app_key="k",
        app_secret="s",
        rate_per_s=1000.0,
        cache_dir=tmp_path / "cache",
    )

    # Then: 실패 종목만 SKIP 되고 다른 종목 행은 저장된다
    assert (out.symbols_ok, out.symbols_failed) == (1, 1)
    assert program_trade_coverage(store, ("000660",))["000660"][1] == dt.date(2026, 9, 25)


def test_backfill_aborts_when_rotation_issuance_fails(tmp_path, monkeypatch) -> None:
    # Given: 첫 발급은 성공, 회전 발급은 실패
    import pytest

    from src.marketdata import program_trade_service as service
    from src.marketdata.toss_calendar import TossCalendarError
    from src.marketdata.toss_program_trades import TossProgramTradesError, _TossProgramTradesAuthRejected

    issued = iter(["tok-1"])

    def _issue(**kwargs):
        try:
            return next(issued)
        except StopIteration:
            raise TossCalendarError("issuance down") from None

    monkeypatch.setattr(service, "issue_access_token", _issue)
    monkeypatch.setattr(
        service,
        "backfill_program_trades_history",
        lambda symbol, **kwargs: (_ for _ in ()).throw(_TossProgramTradesAuthRejected("stale")),
    )

    # When / Then: 자격 증명 장애는 종목 SKIP 이 아니라 실행 전체 중단이다
    with pytest.raises(TossProgramTradesError, match="token issuance failed"):
        service.backfill_program_trades(
            store_path=tmp_path / "program_trades",
            symbols=("005930", "000660"),
            min_date=dt.date(2026, 9, 1),
            app_key="k",
            app_secret="s",
            rate_per_s=1000.0,
            cache_dir=tmp_path / "cache",
        )
