def test_bars_refresh_run_backfills_when_store_missing_and_writes_market_map(tmp_path, monkeypatch) -> None:
    import argparse
    import datetime as dt
    import json
    import polars as pl
    from src.cli import bars_refresh

    monkeypatch.setenv('KRX_OPENAPI_KEY', 'k')

    def _fake_backfill(store_path, *, auth_key, end_date, window_days, session=None):
        return {'trading_days': window_days, 'appended_rows': window_days}

    def _fake_latest(ref_date, *, auth_key, max_lookback=10, session=None):
        bars = pl.DataFrame({'date': [ref_date], 'symbol': ['005930'], 'close': [100.0],
                             'volume': [1], 'trade_value_100m': [1.0], 'daily_change_pct': [0.1], 'market': ['KOSPI']})
        return ref_date - dt.timedelta(days=1), bars

    monkeypatch.setattr(bars_refresh, 'backfill_bars', _fake_backfill)
    monkeypatch.setattr(bars_refresh, 'latest_trading_day', _fake_latest)

    store = tmp_path / 'bars.parquet'
    mm = tmp_path / 'market_map.json'
    args = argparse.Namespace(store_path=str(store), market_map_path=str(mm),
                              ref_date='2026-09-08', window_days=90)

    rc = bars_refresh.run(args)

    assert rc == 0
    assert json.loads(mm.read_text()) == {'005930': 'KOSPI'}


def test_bars_refresh_run_preserves_market_map_on_krx_failure(tmp_path, monkeypatch) -> None:
    import argparse
    from src.cli import bars_refresh
    from src.collector.bars import KrxBarsError

    monkeypatch.setenv('KRX_OPENAPI_KEY', 'k')

    def _fail(*a, **k):
        raise KrxBarsError('down')

    monkeypatch.setattr(bars_refresh, 'latest_trading_day', _fail)

    store = tmp_path / 'bars.parquet'
    store.write_bytes(b'existing')
    mm = tmp_path / 'market_map.json'
    mm.write_text('{"005930": "KOSPI"}', encoding='utf-8')
    args = argparse.Namespace(store_path=str(store), market_map_path=str(mm),
                              ref_date='2026-09-08', window_days=90)

    rc = bars_refresh.run(args)

    assert rc == 4
    assert mm.read_text() == '{"005930": "KOSPI"}'


def test_bars_refresh_run_returns_rc4_when_krx_key_missing(tmp_path, monkeypatch, caplog) -> None:
    import argparse
    import logging
    from src.cli import bars_refresh

    monkeypatch.delenv('KRX_OPENAPI_KEY', raising=False)

    mm = tmp_path / 'market_map.json'
    mm.write_text('{"005930": "KOSPI"}', encoding='utf-8')
    store = tmp_path / 'bars.parquet'
    store.write_bytes(b'existing')
    args = argparse.Namespace(store_path=str(store), market_map_path=str(mm),
                              ref_date='2026-09-08', window_days=90)

    with caplog.at_level(logging.ERROR):
        rc = bars_refresh.run(args)

    assert rc == 4
    assert mm.read_text() == '{"005930": "KOSPI"}'
    assert any('missing_env' in r.message for r in caplog.records)
