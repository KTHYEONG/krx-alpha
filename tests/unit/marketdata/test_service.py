def test_refresh_bars_appends_store_and_writes_market_map(tmp_path, monkeypatch) -> None:
    # Given: KRX 호출을 대체한 최신 거래일 응답
    import datetime as dt
    import json

    import polars as pl

    from src.marketdata import service

    def _fake_latest(ref_date, *, auth_key, max_lookback=10, session=None):
        bars = pl.DataFrame({
            "date": [dt.date(2026, 9, 9)],
            "symbol": ["005930"],
            "close": [70000.0],
            "volume": [1000],
            "trade_value_100m": [700.0],
            "daily_change_pct": [1.5],
            "market": ["KOSPI"],
        })
        return dt.date(2026, 9, 9), bars

    monkeypatch.setattr(service, "latest_trading_day", _fake_latest)
    monkeypatch.setattr(service, "backfill_bars", lambda *a, **k: {"trading_days": 1, "appended_rows": 0})

    store = tmp_path / "bars" / "daily"
    market_map = tmp_path / "market_map.json"

    # When
    result = service.refresh_bars(
        store_path=store,
        market_map_path=market_map,
        ref_date=dt.date(2026, 9, 10),
        window_days=1,
        auth_key="dummy",
    )

    # Then
    assert result.trading_day == dt.date(2026, 9, 9)
    assert result.appended_rows == 1
    assert store.exists()
    assert json.loads(market_map.read_text(encoding="utf-8")) == {"005930": "KOSPI"}


def test_refresh_bars_preserves_artifacts_on_krx_failure(tmp_path, monkeypatch) -> None:
    # Given: 기존 산출물이 있고 KRX 조회가 실패
    import datetime as dt

    import pytest

    from src.marketdata import service
    from src.marketdata.krx_bars import KrxBarsError

    def _fail(*args, **kwargs):
        raise KrxBarsError("down")

    monkeypatch.setattr(service, "latest_trading_day", _fail)

    store = tmp_path / "bars" / "daily"
    store.mkdir(parents=True, exist_ok=True)
    (store / "2026-09.parquet").write_bytes(b"existing")
    market_map = tmp_path / "market_map.json"
    market_map.write_text('{"005930": "KOSPI"}', encoding="utf-8")

    # When / Then: 예외를 삼키지 않고 표면화하며 기존 산출물은 보존된다
    monkeypatch.setattr(service, "latest_partition_date", lambda root: dt.date(2026, 9, 9))
    with pytest.raises(KrxBarsError, match="down"):
        service.refresh_bars(
            store_path=store,
            market_map_path=market_map,
            ref_date=dt.date(2026, 9, 10),
            window_days=1,
            auth_key="dummy",
        )
    assert market_map.read_text(encoding="utf-8") == '{"005930": "KOSPI"}'
    assert (store / "2026-09.parquet").read_bytes() == b"existing"


def test_refresh_bars_via_kis_fallback_raises_when_market_map_missing(tmp_path) -> None:
    import datetime as dt

    import pytest

    from src.marketdata.service import KisFallbackError, refresh_bars_via_kis_fallback

    with pytest.raises(KisFallbackError, match="market_map"):
        refresh_bars_via_kis_fallback(
            store_path=tmp_path / "bars" / "daily",
            market_map_path=tmp_path / "market_map.json",
            target_date=dt.date(2026, 9, 10),
            kis_client=object(),
        )

def test_refresh_bars_via_kis_fallback_raises_when_market_map_empty(tmp_path) -> None:
    import datetime as dt
    import json

    import pytest

    from src.marketdata.service import KisFallbackError, refresh_bars_via_kis_fallback

    map_path = tmp_path / "market_map.json"
    map_path.write_text(json.dumps({}), encoding="utf-8")

    with pytest.raises(KisFallbackError, match="empty"):
        refresh_bars_via_kis_fallback(
            store_path=tmp_path / "bars" / "daily",
            market_map_path=map_path,
            target_date=dt.date(2026, 9, 10),
            kis_client=object(),
        )

def test_refresh_bars_via_kis_fallback_writes_valid_rows_and_skips_invalid(tmp_path) -> None:
    import datetime as dt
    import json

    import polars as pl

    from src.execution.contracts import KisApiError
    from src.marketdata.service import refresh_bars_via_kis_fallback

    map_path = tmp_path / "market_map.json"
    map_path.write_text(json.dumps({"005930": "KOSPI", "000660": "KOSPI"}), encoding="utf-8")
    store_path = tmp_path / "bars" / "daily"

    class _FakeKisClient:
        def get_daily_bar(self, symbol: str, day: dt.date) -> dict[str, str] | None:
            if symbol == "000660":
                raise KisApiError("EGW00000", "boom")
            return {
                "stck_bsop_date": "20260910",
                "stck_clpr": "269000", "acml_vol": "22517075",
                "acml_tr_pbmn": "6028310398800", "prdy_vrss": "-500",
                "stck_oprc": "268000", "stck_hgpr": "270000", "stck_lwpr": "267000",
            }

    result = refresh_bars_via_kis_fallback(
        store_path=store_path,
        market_map_path=map_path,
        target_date=dt.date(2026, 9, 10),
        kis_client=_FakeKisClient(),
    )

    assert result.trading_day == dt.date(2026, 9, 10)
    assert result.appended_rows == 1
    saved = pl.read_parquet(store_path / "2026-09.parquet")
    assert saved["symbol"].to_list() == ["005930"]
    assert saved["close"].to_list() == [269000.0]
    assert saved["volume"].to_list() == [22517075]

def test_refresh_bars_via_kis_fallback_raises_when_zero_rows_survive(tmp_path) -> None:
    import datetime as dt
    import json

    import pytest

    from src.marketdata.service import KisFallbackError, refresh_bars_via_kis_fallback

    map_path = tmp_path / "market_map.json"
    map_path.write_text(json.dumps({"005930": "KOSPI"}), encoding="utf-8")

    class _AllNoneClient:
        def get_daily_bar(self, symbol: str, day: dt.date) -> dict[str, str] | None:
            return None

    with pytest.raises(KisFallbackError, match="0 rows"):
        refresh_bars_via_kis_fallback(
            store_path=tmp_path / "bars" / "daily",
            market_map_path=map_path,
            target_date=dt.date(2026, 9, 10),
            kis_client=_AllNoneClient(),
        )

def test_refresh_bars_via_kis_fallback_zeroes_change_pct_when_prev_close_is_zero(tmp_path) -> None:
    import datetime as dt
    import json

    import polars as pl

    from src.marketdata.service import refresh_bars_via_kis_fallback

    map_path = tmp_path / "market_map.json"
    map_path.write_text(json.dumps({"900001": "KOSDAQ"}), encoding="utf-8")
    store_path = tmp_path / "bars" / "daily"

    class _IpoClient:
        def get_daily_bar(self, symbol: str, day: dt.date) -> dict[str, str] | None:
            return {"stck_bsop_date": "20260910", "stck_clpr": "1000", "acml_vol": "500", "acml_tr_pbmn": "500000", "prdy_vrss": "1000",
                    "stck_oprc": "1000", "stck_hgpr": "1000", "stck_lwpr": "1000"}

    result = refresh_bars_via_kis_fallback(
        store_path=store_path, market_map_path=map_path, target_date=dt.date(2026, 9, 10), kis_client=_IpoClient(),
    )

    assert result.appended_rows == 1
    saved = pl.read_parquet(store_path / "2026-09.parquet")
    assert saved["daily_change_pct"].to_list() == [0.0]


def test_kis_fallback_populates_ohl_and_base_price(tmp_path) -> None:
    import datetime as dt
    import json

    import polars as pl

    from src.marketdata.service import refresh_bars_via_kis_fallback

    map_path = tmp_path / "market_map.json"
    map_path.write_text(json.dumps({"005930": "KOSPI"}), encoding="utf-8")
    store_path = tmp_path / "bars" / "daily"

    class _OhlClient:
        def get_daily_bar(self, symbol: str, day: dt.date) -> dict[str, str] | None:
            return {"stck_bsop_date": "20260910", "stck_clpr": "1000", "acml_vol": "500", "acml_tr_pbmn": "500000", "prdy_vrss": "50",
                    "stck_oprc": "960", "stck_hgpr": "1010", "stck_lwpr": "950"}

    result = refresh_bars_via_kis_fallback(
        store_path=store_path, market_map_path=map_path, target_date=dt.date(2026, 9, 10), kis_client=_OhlClient(),
    )

    assert result.appended_rows == 1
    saved = pl.read_parquet(store_path / "2026-09.parquet")
    assert saved["open"].to_list() == [960.0]
    assert saved["high"].to_list() == [1010.0]
    assert saved["low"].to_list() == [950.0]
    assert saved["base_price"].to_list() == [950.0]
    assert saved["stock_cert_kind"].to_list() == [None]
    assert saved["section"].to_list() == [None]


def _program_trade_row(symbol: str, day) -> dict:
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


def test_backfill_program_trades_persists_per_symbol_and_survives_one_failure(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging

    import polars as pl

    from src.marketdata import program_trade_service as service
    from src.marketdata.toss_program_trades import TossProgramTradesError

    rows_a = (_program_trade_row("005930", dt.date(2026, 9, 16)), _program_trade_row("005930", dt.date(2026, 9, 17)))
    rows_c = (_program_trade_row("000660", dt.date(2026, 9, 17)),)

    def _fake_history(symbol, **kwargs):
        if symbol == "035720":
            raise TossProgramTradesError("bad page")
        return rows_a if symbol == "005930" else rows_c

    monkeypatch.setattr(service, "backfill_program_trades_history", _fake_history)
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: "tok")

    store = tmp_path / "bars" / "program_trades"

    # When
    with caplog.at_level(logging.WARNING):
        result = service.backfill_program_trades(
            store_path=store,
            symbols=("005930", "035720", "000660"),
            min_date=dt.date(2026, 9, 1),
            app_key="k",
            app_secret="s",
            rate_per_s=1000.0,
        )

    # Then
    assert (result.symbols_ok, result.symbols_failed, result.appended_rows) == (2, 1, 3)
    saved = pl.read_parquet(store / "2026-09.parquet")
    assert sorted(saved["symbol"].unique().to_list()) == ["000660", "005930"]
    assert saved.height == 3
    assert sum("status=SKIP" in record.message for record in caplog.records) == 1


def test_backfill_program_trades_rejects_invalid_symbol_before_any_call(monkeypatch) -> None:
    import datetime as dt

    import pytest

    from src.marketdata import program_trade_service as service

    calls: list[str] = []
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: calls.append("token") or "tok")

    class _ExplodingSession:
        def get(self, *args, **kwargs):
            raise AssertionError("no vendor call allowed before symbol validation")

    # When / Then
    with pytest.raises(ValueError, match="KRX short codes"):
        service.backfill_program_trades(
            store_path="x.parquet",
            symbols=("12345",),
            min_date=dt.date(2026, 9, 1),
            app_key="k",
            app_secret="s",
            rate_per_s=8.0,
            session=_ExplodingSession(),
        )
    assert calls == []


def test_backfill_program_trades_issues_token_exactly_once(monkeypatch) -> None:
    import datetime as dt

    from src.marketdata import program_trade_service as service

    calls: list[dict] = []
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: calls.append(kwargs) or "tok")
    monkeypatch.setattr(service, "backfill_program_trades_history", lambda *a, **k: ())
    monkeypatch.setattr(service, "append_program_trades", lambda *a, **k: 0)

    # When
    service.backfill_program_trades(
        store_path="x.parquet",
        symbols=("005930", "000660", "035720"),
        min_date=dt.date(2026, 9, 1),
        app_key="k",
        app_secret="s",
        rate_per_s=8.0,
    )

    # Then
    assert len(calls) == 1


def test_backfill_program_trades_wraps_token_issuance_failure(monkeypatch) -> None:
    import datetime as dt

    import pytest

    from src.marketdata import program_trade_service as service
    from src.marketdata.toss_calendar import TossCalendarError
    from src.marketdata.toss_program_trades import TossProgramTradesError

    def _fail_token(**kwargs):
        raise TossCalendarError("auth down")

    monkeypatch.setattr(service, "issue_access_token", _fail_token)

    # When / Then: 초기 토큰 발급 실패는 TossProgramTradesError로 표면화된다
    with pytest.raises(TossProgramTradesError, match="token issuance"):
        service.backfill_program_trades(
            store_path="x.parquet",
            symbols=("005930",),
            min_date=dt.date(2026, 9, 1),
            app_key="k",
            app_secret="s",
            rate_per_s=8.0,
        )

def test_backfill_universe_program_trades_targets_only_uncovered_symbols(tmp_path, monkeypatch) -> None:
    # Given: 3개 중 1개만 커버리지가 부족한 스토어
    import datetime as dt

    from src.marketdata import program_trade_service as service
    from src.marketdata.toss_program_trades import append_program_trades

    store = tmp_path / "bars" / "program_trades"
    reference_date = dt.date(2026, 9, 17)
    min_date = reference_date - dt.timedelta(days=120)
    append_program_trades(store, [_program_trade_row("005930", min_date - dt.timedelta(days=1))])
    append_program_trades(store, [_program_trade_row("000660", min_date - dt.timedelta(days=1))])
    append_program_trades(store, [_program_trade_row("035720", min_date + dt.timedelta(days=1))])

    seen: dict[str, object] = {}

    def _fake_backfill(**kwargs):
        seen.update(kwargs)
        return service.ProgramTradesBackfillResult(symbols_ok=1, symbols_failed=0, appended_rows=5)

    monkeypatch.setattr(service, "backfill_program_trades", _fake_backfill)

    # When
    result = service.backfill_universe_program_trades(
        store_path=store,
        symbols=("005930", "000660", "035720"),
        lookback_days=120,
        reference_date=reference_date,
        app_key="k",
        app_secret="s",
        rate_per_s=8.0,
    )

    # Then: 미커버 1개에만 위임하고 min_date가 lookback 기준으로 계산된다
    assert seen["symbols"] == ("035720",)
    assert seen["min_date"] == min_date
    assert (result.symbols_ok, result.symbols_failed, result.appended_rows) == (1, 0, 5)


def test_backfill_universe_program_trades_skips_vendor_call_when_all_covered(tmp_path, monkeypatch) -> None:
    # Given: 모든 심볼이 이미 커버됨
    import datetime as dt

    from src.marketdata import program_trade_service as service
    from src.marketdata.toss_program_trades import append_program_trades

    store = tmp_path / "bars" / "program_trades"
    reference_date = dt.date(2026, 9, 17)
    min_date = reference_date - dt.timedelta(days=120)
    append_program_trades(store, [_program_trade_row("005930", min_date - dt.timedelta(days=1))])

    class _ExplodingSession:
        def get(self, *args: object, **kwargs: object) -> object:
            raise AssertionError("no vendor call allowed when all covered")

    calls: list[dict] = []

    def _must_not_call(**kwargs):
        calls.append(kwargs)
        raise AssertionError("backfill_program_trades must not be called")

    monkeypatch.setattr(service, "backfill_program_trades", _must_not_call)

    # When
    result = service.backfill_universe_program_trades(
        store_path=store,
        symbols=("005930",),
        lookback_days=120,
        reference_date=reference_date,
        app_key="k",
        app_secret="s",
        rate_per_s=8.0,
        session=_ExplodingSession(),
    )

    # Then: 벤더 호출 없이 전부 0인 결과
    assert calls == []
    assert (result.symbols_ok, result.symbols_failed, result.appended_rows) == (0, 0, 0)


def test_backfill_universe_program_trades_returns_zero_result_for_empty_symbols(monkeypatch) -> None:
    # Given: 빈 심볼 목록
    import datetime as dt

    from src.marketdata import program_trade_service as service

    def _must_not_call(**kwargs):
        raise AssertionError("backfill_program_trades must not be called for empty symbols")

    monkeypatch.setattr(service, "backfill_program_trades", _must_not_call)

    # When
    result = service.backfill_universe_program_trades(
        store_path="x.parquet",
        symbols=(),
        lookback_days=120,
        reference_date=dt.date(2026, 9, 17),
        app_key="k",
        app_secret="s",
        rate_per_s=8.0,
    )

    # Then: 예외 없이 0 결과 반환
    assert (result.symbols_ok, result.symbols_failed, result.appended_rows) == (0, 0, 0)


def test_backfill_universe_program_trades_keeps_alphanumeric_and_skips_malformed(tmp_path, monkeypatch, caplog) -> None:
    """형태가 올바른 alphanumeric 단축코드는 백필 대상에 유지하고, 깨진 코드만 건너뛴다."""
    import datetime as dt
    import logging

    from src.marketdata import program_trade_service as service

    store = tmp_path / "bars" / "program_trades"
    reference_date = dt.date(2026, 9, 21)
    seen: dict[str, object] = {}

    def _fake_backfill(**kwargs):
        seen.update(kwargs)
        return service.ProgramTradesBackfillResult(symbols_ok=1, symbols_failed=0, appended_rows=10)

    monkeypatch.setattr(service, "backfill_program_trades", _fake_backfill)

    # When: alphanumeric 신규상장과 깨진 코드가 포함된 유니버스 전달
    with caplog.at_level(logging.WARNING):
        result = service.backfill_universe_program_trades(
            store_path=store,
            symbols=("005930", "0004V0", "12"),
            lookback_days=120,
            reference_date=reference_date,
            app_key="k",
            app_secret="s",
            rate_per_s=8.0,
        )

    # Then: alphanumeric 코드는 유지되고 깨진 코드만 건너뛴다
    assert seen["symbols"] == ("005930", "0004V0")
    assert (result.symbols_ok, result.symbols_failed, result.appended_rows) == (1, 0, 10)
    assert "status=SKIP_INVALID_CODE" in caplog.text


def test_backfill_program_trades_accepts_alphanumeric_codes(tmp_path, monkeypatch) -> None:
    import datetime as dt

    from src.marketdata import program_trade_service as service

    attempted: list[str] = []

    def _fake_history(symbol, **kwargs):
        attempted.append(symbol)
        return (_program_trade_row(symbol, dt.date(2026, 9, 17)),)

    monkeypatch.setattr(service, "backfill_program_trades_history", _fake_history)
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: "tok")

    result = service.backfill_program_trades(
        store_path=tmp_path / "bars" / "program_trades",
        symbols=("005930", "0004V0"),
        min_date=dt.date(2026, 9, 1),
        app_key="k",
        app_secret="s",
        rate_per_s=1000.0,
    )

    assert sorted(attempted) == ["0004V0", "005930"]
    assert (result.symbols_ok, result.symbols_failed) == (2, 0)


def test_backfill_program_trades_rejects_malformed_code(monkeypatch) -> None:
    import datetime as dt

    import pytest

    from src.marketdata import program_trade_service as service

    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: "tok")

    with pytest.raises(ValueError, match="KRX short codes"):
        service.backfill_program_trades(
            store_path="x.parquet",
            symbols=("5930",),
            min_date=dt.date(2026, 9, 1),
            app_key="k",
            app_secret="s",
            rate_per_s=8.0,
        )


def test_backfill_program_trades_isolates_single_vendor_rejection(tmp_path, monkeypatch) -> None:
    import datetime as dt

    import polars as pl

    from src.marketdata import program_trade_service as service
    from src.marketdata.toss_program_trades import TossProgramTradesError

    def _fake_history(symbol, **kwargs):
        if symbol == "0004V0":
            raise TossProgramTradesError("rejected")
        return (_program_trade_row(symbol, dt.date(2026, 9, 17)),)

    monkeypatch.setattr(service, "backfill_program_trades_history", _fake_history)
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: "tok")

    store = tmp_path / "bars" / "program_trades"
    result = service.backfill_program_trades(
        store_path=store,
        symbols=("005930", "0004V0"),
        min_date=dt.date(2026, 9, 1),
        app_key="k",
        app_secret="s",
        rate_per_s=1000.0,
    )

    assert (result.symbols_ok, result.symbols_failed) == (1, 1)
    assert pl.read_parquet(store / "2026-09.parquet")["symbol"].unique().to_list() == ["005930"]


def _kis_row(bsop_date="20260910", close="70000", vol="1000", value="70000000", vrss="700", oprc="69500", hgpr="70500", lwpr="69000"):    return {
        "stck_bsop_date": bsop_date, "stck_clpr": close, "acml_vol": vol, "acml_tr_pbmn": value,
        "prdy_vrss": vrss, "stck_oprc": oprc, "stck_hgpr": hgpr, "stck_lwpr": lwpr,
    }


def test_refresh_bars_via_kis_fallback_skips_bar_from_another_session(tmp_path, caplog) -> None:
    import datetime as dt
    import json
    import logging

    import polars as pl

    from src.marketdata.service import refresh_bars_via_kis_fallback

    target = dt.date(2026, 9, 10)
    map_path = tmp_path / "market_map.json"
    map_path.write_text(json.dumps({"005930": "KOSPI", "000660": "KOSPI"}), encoding="utf-8")
    store_path = tmp_path / "bars" / "daily"

    class _MixedDateClient:
        def get_daily_bar(self, symbol: str, day: dt.date):
            if symbol == "000660":
                return _kis_row(bsop_date="20260909")
            return _kis_row(bsop_date="20260910")

    with caplog.at_level(logging.WARNING):
        result = refresh_bars_via_kis_fallback(
            store_path=store_path, market_map_path=map_path, target_date=target, kis_client=_MixedDateClient(),
        )

    assert result.trading_day == target
    assert result.appended_rows == 1
    saved = pl.read_parquet(store_path / "2026-09.parquet")
    assert saved["symbol"].to_list() == ["005930"]
    assert saved["date"].to_list() == [target]
    assert "date_mismatch" in caplog.text


def test_refresh_bars_via_kis_fallback_fails_closed_when_all_dates_mismatched(tmp_path) -> None:
    import datetime as dt
    import json

    import pytest

    from src.marketdata.service import KisFallbackError, refresh_bars_via_kis_fallback

    target = dt.date(2026, 9, 10)
    map_path = tmp_path / "market_map.json"
    map_path.write_text(json.dumps({"005930": "KOSPI"}), encoding="utf-8")
    store_path = tmp_path / "bars" / "daily"

    class _StaleClient:
        def get_daily_bar(self, symbol: str, day: dt.date):
            return _kis_row(bsop_date="20260909")

    with pytest.raises(KisFallbackError, match="0 rows"):
        refresh_bars_via_kis_fallback(
            store_path=store_path, market_map_path=map_path, target_date=target, kis_client=_StaleClient(),
        )
    assert not list(store_path.glob("*.parquet"))


def test_backfill_universe_program_trades_returns_zero_when_all_symbols_malformed(tmp_path, monkeypatch) -> None:
    import datetime as dt

    from src.marketdata import program_trade_service as service

    def _must_not_call(**kwargs):
        raise AssertionError("backfill_program_trades must not be called when no clean symbols")

    monkeypatch.setattr(service, "backfill_program_trades", _must_not_call)

    result = service.backfill_universe_program_trades(
        store_path=tmp_path / "program_trades",
        symbols=("12", "ab!"),
        lookback_days=120,
        reference_date=dt.date(2026, 9, 21),
        app_key="k",
        app_secret="s",
        rate_per_s=8.0,
    )

    assert (result.symbols_ok, result.symbols_failed, result.appended_rows) == (0, 0, 0)


def test_backfill_program_trades_writes_store_once(tmp_path, monkeypatch) -> None:
    import datetime as dt

    from src.marketdata import program_trade_service as service

    histories = {
        "005930": (
            _program_trade_row("005930", dt.date(2026, 9, 16)),
            _program_trade_row("005930", dt.date(2026, 9, 17)),
        ),
        "000660": (_program_trade_row("000660", dt.date(2026, 9, 17)),),
        "035720": (
            _program_trade_row("035720", dt.date(2026, 9, 16)),
            _program_trade_row("035720", dt.date(2026, 9, 17)),
        ),
    }
    monkeypatch.setattr(service, "backfill_program_trades_history", lambda symbol, **kwargs: histories[symbol])
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: "tok")
    calls: list[tuple] = []
    real_append = service.append_program_trades

    def _counting_append(store_path, rows):
        calls.append((store_path, [dict(row) for row in rows]))
        return real_append(store_path, rows)

    monkeypatch.setattr(service, "append_program_trades", _counting_append)
    store = tmp_path / "bars" / "program_trades"

    result = service.backfill_program_trades(
        store_path=store,
        symbols=("005930", "000660", "035720", "005930"),
        min_date=dt.date(2026, 9, 1),
        app_key="k",
        app_secret="s",
        rate_per_s=1000.0,
    )

    assert (result.symbols_ok, result.symbols_failed, result.appended_rows) == (3, 0, 5)
    assert len(calls) == 1
    assert sorted(row["symbol"] for row in calls[0][1]) == ["000660", "005930", "005930", "035720", "035720"]


def test_program_trade_coverage_returns_min_max_per_symbol(tmp_path) -> None:
    import datetime as dt

    from src.marketdata.toss_program_trades import append_program_trades, program_trade_coverage

    store = tmp_path / "bars" / "program_trades"
    append_program_trades(store, [
        _program_trade_row("005930", dt.date(2026, 5, 19)),
        _program_trade_row("005930", dt.date(2026, 9, 17)),
        _program_trade_row("000660", dt.date(2026, 8, 1)),
        _program_trade_row("000660", dt.date(2026, 8, 2)),
    ])

    coverage = program_trade_coverage(store, ("005930", "000660", "035720"))

    assert coverage == {
        "005930": (dt.date(2026, 5, 19), dt.date(2026, 9, 17)),
        "000660": (dt.date(2026, 8, 1), dt.date(2026, 8, 2)),
    }
    assert program_trade_coverage(store, ()) == {}
