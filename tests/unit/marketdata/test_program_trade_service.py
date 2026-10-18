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


_SD1 = dt.date(2026, 9, 1)
_SD2 = dt.date(2026, 9, 2)
_SD3 = dt.date(2026, 9, 3)
_SD4 = dt.date(2026, 9, 4)


def _seed_bars(bars_dir, entries: dict[str, list[dt.date]]) -> None:
    import polars as pl

    from src.marketdata.partitioned_store import upsert_month_partitions

    rows = [(symbol, day) for symbol, days in entries.items() for day in days]
    frame = pl.DataFrame(
        {
            "date": [row[1] for row in rows],
            "symbol": [row[0] for row in rows],
            "close": [1000.0] * len(rows),
            "volume": [1000] * len(rows),
            "trade_value_100m": [100.0] * len(rows),
            "daily_change_pct": [1.0] * len(rows),
        },
        schema={
            "date": pl.Date,
            "symbol": pl.String,
            "close": pl.Float64,
            "volume": pl.Int64,
            "trade_value_100m": pl.Float64,
            "daily_change_pct": pl.Float64,
        },
    )
    upsert_month_partitions(bars_dir, frame, key_columns=("date", "symbol"), sort_columns=("date", "symbol"))


def _forward(service, store, bars, monkeypatch, **overrides):
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: "tok")
    kwargs = {
        "store_root": store,
        "session_date": _SD4,
        "complete_through": _SD3,
        "candidate_symbols": (),
        "lookback_days": 120,
        "app_key": "k",
        "app_secret": "s",
        "rate_per_s": 1000.0,
        "bars_daily_dir": bars,
        "gap_window_business_days": 20,
    }
    kwargs.update(overrides)
    return service.sync_program_trades_forward(**kwargs)


def test_forward_sync_lowers_floor_for_interior_gap(tmp_path, monkeypatch) -> None:
    # Given: A가 D1,D3,D4를 가지며 D2 내부 갭이 있고 일봉은 D1..D3
    from src.marketdata import program_trade_service as service
    from src.marketdata.toss_program_trades import ProgramTradeGapStatus

    store = tmp_path / "program_trades"
    bars = tmp_path / "bars" / "daily"
    _seed(store, {"A": [_SD1, _SD3, _SD4]})
    _seed_bars(bars, {"A": [_SD1, _SD2, _SD3]})
    calls: dict[str, dt.date] = {}

    def _fake_history(symbol: str, **kwargs) -> tuple:
        calls[symbol] = kwargs["min_date"]
        return (_pt_row(symbol, _SD2),)

    monkeypatch.setattr(service, "backfill_program_trades_history", _fake_history)

    # When
    result = _forward(service, store, bars, monkeypatch)

    # Then: 내부 갭 날짜로 바닥이 낮아져 한 번에 봉합된다
    assert calls == {"A": _SD2}
    assert result.gap_targets == ("A",)
    assert result.forward_targets == ()
    assert result.gap_report is not None
    assert result.gap_report.status is ProgramTradeGapStatus.COMPLETE


def test_forward_sync_vendor_hole_persists_as_post_gap(tmp_path, monkeypatch) -> None:
    # Given: 벤더가 D2를 끝내 주지 않는 상태
    from src.marketdata import program_trade_service as service

    store = tmp_path / "program_trades"
    bars = tmp_path / "bars" / "daily"
    _seed(store, {"A": [_SD1, _SD3, _SD4]})
    _seed_bars(bars, {"A": [_SD1, _SD2, _SD3]})
    monkeypatch.setattr(service, "backfill_program_trades_history", lambda symbol, **kwargs: ())

    # When
    result = _forward(service, store, bars, monkeypatch)

    # Then: 사후 보고서가 잔여 갭을 유지한다
    assert result.gap_report is not None
    assert result.gap_report.missing == {"A": (_SD2,)}


def test_forward_sync_tail_gap_floor_stays_max_plus_one(tmp_path, monkeypatch) -> None:
    # Given: 내부 갭 없이 세션일보다 뒤처진 A
    from src.marketdata import program_trade_service as service

    store = tmp_path / "program_trades"
    bars = tmp_path / "bars" / "daily"
    _seed(store, {"A": [_SD1, _SD2, _SD3]})
    _seed_bars(bars, {"A": [_SD1, _SD2, _SD3]})
    calls: dict[str, dt.date] = {}

    def _fake_history(symbol: str, **kwargs) -> tuple:
        calls[symbol] = kwargs["min_date"]
        return ()

    monkeypatch.setattr(service, "backfill_program_trades_history", _fake_history)

    # When
    result = _forward(service, store, bars, monkeypatch)

    # Then: 꼬리 갭은 max+1, 갭 대상 아님
    assert calls == {"A": _SD4}
    assert result.gap_targets == ()


def test_forward_sync_does_not_gap_check_delisted_symbol(tmp_path, monkeypatch) -> None:
    # Given: 상장폐지 B가 D2 내부 갭을 갖지만 active_symbols에 없음
    from src.marketdata import program_trade_service as service

    store = tmp_path / "program_trades"
    bars = tmp_path / "bars" / "daily"
    _seed(store, {"A": [_SD1, _SD2, _SD3], "B": [_SD1, _SD3]})
    _seed_bars(bars, {"A": [_SD1, _SD2, _SD3], "B": [_SD1, _SD2, _SD3]})
    calls: list[str] = []

    def _fake_history(symbol: str, **kwargs) -> tuple:
        calls.append(symbol)
        return ()

    monkeypatch.setattr(service, "backfill_program_trades_history", _fake_history)

    # When
    result = _forward(service, store, bars, monkeypatch, active_symbols=frozenset({"A"}))

    # Then: B는 갭 대상도, 누락 목록도 아니다
    assert calls == ["A"]
    assert result.gap_targets == ()
    assert result.gap_report is not None
    assert "B" not in result.gap_report.missing


def test_forward_sync_keeps_candidate_lookback_floor(tmp_path, monkeypatch) -> None:
    # Given: 이력이 없는 후보 C
    from src.marketdata import program_trade_service as service

    store = tmp_path / "program_trades"
    bars = tmp_path / "bars" / "daily"
    _seed(store, {"A": [_SD1, _SD2, _SD3]})
    _seed_bars(bars, {"A": [_SD1, _SD2, _SD3]})
    calls: dict[str, dt.date] = {}

    def _fake_history(symbol: str, **kwargs) -> tuple:
        calls[symbol] = kwargs["min_date"]
        return ()

    monkeypatch.setattr(service, "backfill_program_trades_history", _fake_history)

    # When
    result = _forward(service, store, bars, monkeypatch, candidate_symbols=("000660",), lookback_days=10)

    # Then: 후보 바닥은 세션일-lookback 그대로다
    assert result.candidate_targets == ("000660",)
    assert calls["000660"] == _SD4 - dt.timedelta(days=10)


def test_forward_sync_gap_check_is_idempotent_on_complete_store(tmp_path, monkeypatch) -> None:
    # Given: 갭이 없는 완전한 스토어
    from src.marketdata import program_trade_service as service

    store = tmp_path / "program_trades"
    bars = tmp_path / "bars" / "daily"
    _seed(store, {"A": [_SD1, _SD2, _SD3]})
    _seed_bars(bars, {"A": [_SD1, _SD2, _SD3]})
    monkeypatch.setattr(service, "backfill_program_trades_history", lambda symbol, **kwargs: ())
    from src.marketdata.partitioned_store import scan_month_partitions

    before = scan_month_partitions(store).collect().sort("symbol", "date")

    # When
    first = _forward(service, store, bars, monkeypatch)
    second = _forward(service, store, bars, monkeypatch)

    # Then: 재실행은 갭 대상을 만들지 않고 스토어를 바꾸지 않는다
    assert first.gap_targets == ()
    assert second.gap_targets == ()
    assert second.appended_rows == 0
    assert scan_month_partitions(store).collect().sort("symbol", "date").equals(before)


def test_forward_sync_gap_check_disabled_matches_legacy(tmp_path, monkeypatch) -> None:
    # Given: 새 파라미터를 모두 생략한 레거시 호출
    from src.marketdata import program_trade_service as service

    store = tmp_path / "program_trades"
    _seed(store, {"A": [_SD1]})
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: "tok")
    monkeypatch.setattr(service, "backfill_program_trades_history", lambda symbol, **kwargs: ())

    # When
    result = service.sync_program_trades_forward(
        store_root=store,
        session_date=_SD4,
        complete_through=_SD3,
        candidate_symbols=(),
        lookback_days=120,
        app_key="k",
        app_secret="s",
        rate_per_s=1000.0,
    )

    # Then: 갭 필드는 기본값 그대로다
    assert result.gap_targets == ()
    assert result.gap_report is None


def test_forward_sync_rejects_half_configured_gap_check(tmp_path, monkeypatch) -> None:
    # Given: 두 파라미터 중 하나만 지정
    import pytest

    from src.marketdata import program_trade_service as service

    store = tmp_path / "program_trades"
    _seed(store, {"A": [_SD1]})
    calls: list[str] = []
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: calls.append("token") or "tok")
    base = {
        "store_root": store,
        "session_date": _SD4,
        "complete_through": _SD3,
        "candidate_symbols": (),
        "lookback_days": 120,
        "app_key": "k",
        "app_secret": "s",
        "rate_per_s": 1000.0,
    }

    # When / Then: 벤더 호출 전에 fail-closed
    with pytest.raises(ValueError, match="together"):
        service.sync_program_trades_forward(**base, bars_daily_dir=tmp_path / "bars")
    with pytest.raises(ValueError, match="together"):
        service.sync_program_trades_forward(**base, gap_window_business_days=20)
    assert calls == []


def test_forward_sync_unverified_calendar_keeps_max_plus_one_floor(tmp_path, monkeypatch) -> None:
    # Given: 일봉 스토어가 없어 캘린더를 인증할 수 없음
    from src.marketdata import program_trade_service as service
    from src.marketdata.toss_program_trades import ProgramTradeGapStatus

    store = tmp_path / "program_trades"
    _seed(store, {"A": [_SD1, _SD2, _SD3]})
    calls: dict[str, dt.date] = {}

    def _fake_history(symbol: str, **kwargs) -> tuple:
        calls[symbol] = kwargs["min_date"]
        return ()

    monkeypatch.setattr(service, "backfill_program_trades_history", _fake_history)

    # When
    result = _forward(service, store, tmp_path / "missing-bars", monkeypatch)

    # Then: 바닥 변경 없이 기존 max+1 로직이 돈다
    assert calls == {"A": _SD4}
    assert result.gap_targets == ()
    assert result.gap_report is not None
    assert result.gap_report.status is ProgramTradeGapStatus.CALENDAR_UNAVAILABLE


def test_tail_missing_day_does_not_count_as_lowered_gap_floor(tmp_path, monkeypatch) -> None:
    from src.marketdata import program_trade_service as service

    store = tmp_path / "program"
    bars = tmp_path / "daily"
    _seed(store, {"A": [_SD1, _SD2]})
    _seed_bars(bars, {"A": [_SD1, _SD2, _SD3]})
    calls = {}

    def fetch(symbol, **kwargs):
        calls[symbol] = kwargs["min_date"]
        return ()

    monkeypatch.setattr(service, "backfill_program_trades_history", fetch)
    result = _forward(service, store, bars, monkeypatch)
    assert calls == {"A": _SD3}
    assert result.forward_targets == ("A",)
    assert result.gap_targets == ()
    assert result.gap_report.missing == {"A": (_SD3,)}


def test_healed_store_rerun_does_not_fetch_again(tmp_path, monkeypatch) -> None:
    from src.marketdata import program_trade_service as service
    from src.marketdata.partitioned_store import scan_month_partitions
    from src.marketdata.toss_program_trades import ProgramTradeGapStatus

    store = tmp_path / "program"
    bars = tmp_path / "daily"
    _seed(store, {"A": [_SD1, _SD3, _SD4]})
    _seed_bars(bars, {"A": [_SD1, _SD2, _SD3]})
    calls = []

    def fetch(symbol, **kwargs):
        calls.append((symbol, kwargs["min_date"]))
        return (_pt_row(symbol, _SD2),)

    monkeypatch.setattr(service, "backfill_program_trades_history", fetch)
    first = _forward(service, store, bars, monkeypatch)
    before = scan_month_partitions(store).collect().sort("symbol", "date")
    second = _forward(service, store, bars, monkeypatch)
    assert calls == [("A", _SD2)]
    assert first.gap_report.status is ProgramTradeGapStatus.COMPLETE
    assert second.gap_targets == ()
    assert second.symbols_ok == 0
    assert scan_month_partitions(store).collect().sort("symbol", "date").equals(before)
