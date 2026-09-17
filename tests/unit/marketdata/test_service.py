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

    store = tmp_path / "bars" / "daily.parquet"
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

    store = tmp_path / "bars.parquet"
    store.write_bytes(b"existing")
    market_map = tmp_path / "market_map.json"
    market_map.write_text('{"005930": "KOSPI"}', encoding="utf-8")

    # When / Then: 예외를 삼키지 않고 표면화하며 기존 산출물은 보존된다
    with pytest.raises(KrxBarsError, match="down"):
        service.refresh_bars(
            store_path=store,
            market_map_path=market_map,
            ref_date=dt.date(2026, 9, 10),
            window_days=1,
            auth_key="dummy",
        )
    assert market_map.read_text(encoding="utf-8") == '{"005930": "KOSPI"}'
    assert store.read_bytes() == b"existing"


def test_refresh_bars_via_kis_fallback_raises_when_market_map_missing(tmp_path) -> None:
    import datetime as dt

    import pytest

    from src.marketdata.service import KisFallbackError, refresh_bars_via_kis_fallback

    with pytest.raises(KisFallbackError, match="market_map"):
        refresh_bars_via_kis_fallback(
            store_path=tmp_path / "bars" / "daily.parquet",
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
            store_path=tmp_path / "bars" / "daily.parquet",
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
    store_path = tmp_path / "bars" / "daily.parquet"

    class _FakeKisClient:
        def get_daily_bar(self, symbol: str, day: dt.date) -> dict[str, str] | None:
            if symbol == "000660":
                raise KisApiError("EGW00000", "boom")
            return {
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
    saved = pl.read_parquet(store_path)
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
            store_path=tmp_path / "bars" / "daily.parquet",
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
    store_path = tmp_path / "bars" / "daily.parquet"

    class _IpoClient:
        def get_daily_bar(self, symbol: str, day: dt.date) -> dict[str, str] | None:
            return {"stck_clpr": "1000", "acml_vol": "500", "acml_tr_pbmn": "500000", "prdy_vrss": "1000",
                    "stck_oprc": "1000", "stck_hgpr": "1000", "stck_lwpr": "1000"}

    result = refresh_bars_via_kis_fallback(
        store_path=store_path, market_map_path=map_path, target_date=dt.date(2026, 9, 10), kis_client=_IpoClient(),
    )

    assert result.appended_rows == 1
    saved = pl.read_parquet(store_path)
    assert saved["daily_change_pct"].to_list() == [0.0]


def test_kis_fallback_populates_ohl_and_base_price(tmp_path) -> None:
    import datetime as dt
    import json

    import polars as pl

    from src.marketdata.service import refresh_bars_via_kis_fallback

    map_path = tmp_path / "market_map.json"
    map_path.write_text(json.dumps({"005930": "KOSPI"}), encoding="utf-8")
    store_path = tmp_path / "bars" / "daily.parquet"

    class _OhlClient:
        def get_daily_bar(self, symbol: str, day: dt.date) -> dict[str, str] | None:
            return {"stck_clpr": "1000", "acml_vol": "500", "acml_tr_pbmn": "500000", "prdy_vrss": "50",
                    "stck_oprc": "960", "stck_hgpr": "1010", "stck_lwpr": "950"}

    result = refresh_bars_via_kis_fallback(
        store_path=store_path, market_map_path=map_path, target_date=dt.date(2026, 9, 10), kis_client=_OhlClient(),
    )

    assert result.appended_rows == 1
    saved = pl.read_parquet(store_path)
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

    from src.marketdata import service
    from src.marketdata.toss_program_trades import TossProgramTradesError

    rows_a = (_program_trade_row("005930", dt.date(2026, 9, 16)), _program_trade_row("005930", dt.date(2026, 9, 17)))
    rows_c = (_program_trade_row("000660", dt.date(2026, 9, 17)),)

    def _fake_history(symbol, **kwargs):
        if symbol == "035720":
            raise TossProgramTradesError("bad page")
        return rows_a if symbol == "005930" else rows_c

    monkeypatch.setattr(service, "backfill_program_trades_history", _fake_history)
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: "tok")

    store = tmp_path / "bars" / "program_trades.parquet"

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
    saved = pl.read_parquet(store)
    assert sorted(saved["symbol"].unique().to_list()) == ["000660", "005930"]
    assert saved.height == 3
    assert sum("status=SKIP" in record.message for record in caplog.records) == 1


def test_backfill_program_trades_rejects_invalid_symbol_before_any_call(monkeypatch) -> None:
    import datetime as dt

    import pytest

    from src.marketdata import service

    calls: list[str] = []
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: calls.append("token") or "tok")

    class _ExplodingSession:
        def get(self, *args, **kwargs):
            raise AssertionError("no vendor call allowed before symbol validation")

    # When / Then
    with pytest.raises(ValueError, match="6-digit"):
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

    from src.marketdata import service

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

    from src.marketdata import service
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
