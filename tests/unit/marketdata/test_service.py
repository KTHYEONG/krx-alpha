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
