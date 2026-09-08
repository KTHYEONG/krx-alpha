def test_fetch_daily_bars_maps_krx_response_to_required_columns() -> None:
    import datetime as dt
    from src.collector.bars import fetch_daily_bars

    kospi_payload = {'OutBlock_1': [{'BAS_DD': '20260907', 'ISU_CD': '005930', 'ISU_NM': '삼성전자', 'MKT_NM': 'KOSPI',
                                     'TDD_CLSPRC': '270000', 'ACC_TRDVOL': '18314016', 'ACC_TRDVAL': '4900114076282', 'FLUC_RT': '5.68'}]}
    kosdaq_payload = {'OutBlock_1': []}

    class _Resp:
        def __init__(self, payload):
            self._payload = payload
        def raise_for_status(self):
            self._checked = True
        def json(self):
            return self._payload

    class _Session:
        def __init__(self):
            self.urls: list[str] = []
        def post(self, url, **kw):
            self.urls.append(url)
            return _Resp(kospi_payload if 'stk_bydd_trd' in url else kosdaq_payload)

    session = _Session()

    out = fetch_daily_bars(dt.date(2026, 9, 7), auth_key='k', session=session)

    assert out['symbol'].to_list() == ['005930']
    assert out['close'].to_list() == [270000.0]
    assert out['volume'].to_list() == [18314016]
    assert round(out['trade_value_100m'][0], 5) == round(4900114076282 / 1e8, 5)
    assert out['daily_change_pct'].to_list() == [5.68]
    assert out['market'].to_list() == ['KOSPI']
    assert len(session.urls) == 2


def test_fetch_daily_bars_raises_on_empty_response() -> None:
    import datetime as dt
    import pytest
    from src.collector.bars import KrxBarsError, fetch_daily_bars

    class _Resp:
        def raise_for_status(self):
            self._ok = True
        def json(self):
            return {'OutBlock_1': []}

    class _Session:
        def post(self, url, **kw):
            return _Resp()

    with pytest.raises(KrxBarsError, match='no trading data'):
        fetch_daily_bars(dt.date(2026, 9, 6), auth_key='k', session=_Session())


def test_fetch_daily_bars_wraps_request_exception() -> None:
    import datetime as dt
    import pytest
    import requests
    from src.collector.bars import KrxBarsError, fetch_daily_bars

    class _Session:
        def post(self, url, **kw):
            raise requests.ConnectionError('boom')

    with pytest.raises(KrxBarsError):
        fetch_daily_bars(dt.date(2026, 9, 7), auth_key='k', session=_Session())


def test_latest_trading_day_walks_backward_on_krx_bars_error(monkeypatch) -> None:
    import datetime as dt
    import polars as pl
    import src.collector.bars as bars_mod
    from src.collector.bars import latest_trading_day

    calls: list[dt.date] = []

    def _fake(date, *, auth_key, session=None):
        calls.append(date)
        if date != dt.date(2026, 9, 4):
            raise bars_mod.KrxBarsError('non-trading')
        return pl.DataFrame({'date': [date], 'symbol': ['005930'], 'close': [1.0],
                             'volume': [1], 'trade_value_100m': [1.0], 'daily_change_pct': [0.1], 'market': ['KOSPI']})

    monkeypatch.setattr(bars_mod, 'fetch_daily_bars', _fake)

    day, bars = latest_trading_day(dt.date(2026, 9, 8), auth_key='k')

    assert day == dt.date(2026, 9, 4)
    assert calls == [dt.date(2026, 9, 7), dt.date(2026, 9, 6), dt.date(2026, 9, 5), dt.date(2026, 9, 4)]


def test_latest_trading_day_never_fetches_ref_date_itself(monkeypatch) -> None:
    import datetime as dt
    import polars as pl
    import src.collector.bars as bars_mod
    from src.collector.bars import latest_trading_day

    seen: list[dt.date] = []

    def _fake(date, *, auth_key, session=None):
        seen.append(date)
        return pl.DataFrame({'date': [date], 'symbol': ['005930'], 'close': [1.0],
                             'volume': [1], 'trade_value_100m': [1.0], 'daily_change_pct': [0.1], 'market': ['KOSPI']})

    monkeypatch.setattr(bars_mod, 'fetch_daily_bars', _fake)

    latest_trading_day(dt.date(2026, 9, 8), auth_key='k')

    assert dt.date(2026, 9, 8) not in seen
    assert seen[0] == dt.date(2026, 9, 7)


def test_latest_trading_day_raises_after_max_lookback_exhausted(monkeypatch) -> None:
    import datetime as dt
    import pytest
    import src.collector.bars as bars_mod
    from src.collector.bars import KrxBarsError, latest_trading_day

    def _always_fail(date, *, auth_key, session=None):
        raise KrxBarsError('none')

    monkeypatch.setattr(bars_mod, 'fetch_daily_bars', _always_fail)

    with pytest.raises(KrxBarsError):
        latest_trading_day(dt.date(2026, 9, 8), auth_key='k', max_lookback=3)


def test_derive_market_map_builds_symbol_to_market_dict() -> None:
    import datetime as dt
    import polars as pl
    from src.collector.bars import derive_market_map

    bars = pl.DataFrame({'date': [dt.date(2026, 9, 7)] * 2, 'symbol': ['005930', '247540'],
                         'close': [1.0, 2.0], 'volume': [1, 1], 'trade_value_100m': [1.0, 1.0],
                         'daily_change_pct': [0.1, 0.1], 'market': ['KOSPI', 'KOSDAQ']})

    out = derive_market_map(bars)

    assert out == {'005930': 'KOSPI', '247540': 'KOSDAQ'}


def test_append_daily_bars_creates_new_store_file(tmp_path) -> None:
    import datetime as dt
    import polars as pl
    from src.collector.bars import append_daily_bars

    store = tmp_path / 'bars.parquet'
    bars = pl.DataFrame({'date': [dt.date(2026, 9, 7)], 'symbol': ['005930'], 'close': [270000.0],
                         'volume': [100], 'trade_value_100m': [1.0], 'daily_change_pct': [5.68], 'market': ['KOSPI']})

    appended = append_daily_bars(store, bars)

    assert appended == 1
    assert store.exists()
    stored = pl.read_parquet(store)
    assert stored.columns == ['date', 'symbol', 'close', 'volume', 'trade_value_100m', 'daily_change_pct']


def test_append_daily_bars_is_idempotent_for_existing_date(tmp_path) -> None:
    import datetime as dt
    import polars as pl
    from src.collector.bars import append_daily_bars

    store = tmp_path / 'bars.parquet'
    bars = pl.DataFrame({'date': [dt.date(2026, 9, 7)], 'symbol': ['005930'], 'close': [270000.0],
                         'volume': [100], 'trade_value_100m': [1.0], 'daily_change_pct': [5.68]})

    first = append_daily_bars(store, bars)
    second = append_daily_bars(store, bars)

    assert first == 1
    assert second == 0
    assert pl.read_parquet(store).height == 1


def test_append_daily_bars_appends_new_date_alongside_existing(tmp_path) -> None:
    import datetime as dt
    import polars as pl
    from src.collector.bars import append_daily_bars

    store = tmp_path / 'bars.parquet'
    day1 = pl.DataFrame({'date': [dt.date(2026, 9, 7)], 'symbol': ['005930'], 'close': [1.0],
                         'volume': [1], 'trade_value_100m': [1.0], 'daily_change_pct': [0.1]})
    day2 = pl.DataFrame({'date': [dt.date(2026, 9, 8)], 'symbol': ['005930'], 'close': [2.0],
                         'volume': [2], 'trade_value_100m': [2.0], 'daily_change_pct': [0.2]})

    append_daily_bars(store, day1)
    second = append_daily_bars(store, day2)

    assert second == 1
    assert pl.read_parquet(store).height == 2


def test_backfill_bars_accumulates_until_window_days_reached(tmp_path, monkeypatch) -> None:
    import datetime as dt
    import polars as pl
    import src.collector.bars as bars_mod
    from src.collector.bars import KrxBarsError, backfill_bars

    def _fake_fetch(date, *, auth_key, session=None):
        if date.weekday() >= 5:
            raise KrxBarsError('weekend')
        return pl.DataFrame({'date': [date], 'symbol': ['005930'], 'close': [100.0],
                             'volume': [1], 'trade_value_100m': [1.0], 'daily_change_pct': [0.1]})

    monkeypatch.setattr(bars_mod, 'fetch_daily_bars', _fake_fetch)

    result = backfill_bars(tmp_path / 'bars.parquet', auth_key='k', end_date=dt.date(2026, 9, 7), window_days=5)

    assert result['trading_days'] == 5
    assert result['appended_rows'] == 5
    assert pl.read_parquet(tmp_path / 'bars.parquet').height == 5


def test_backfill_bars_raises_when_calendar_cap_exhausted_before_window(tmp_path, monkeypatch) -> None:
    import datetime as dt
    import pytest
    import src.collector.bars as bars_mod
    from src.collector.bars import KrxBarsError, backfill_bars

    def _always_fail(date, *, auth_key, session=None):
        raise KrxBarsError('no data')

    monkeypatch.setattr(bars_mod, 'fetch_daily_bars', _always_fail)

    with pytest.raises(KrxBarsError):
        backfill_bars(tmp_path / 'bars.parquet', auth_key='k', end_date=dt.date(2026, 9, 7), window_days=3)


def test_write_market_map_writes_atomic_json(tmp_path) -> None:
    import json
    from src.collector.bars import write_market_map

    path = tmp_path / 'nested' / 'market_map.json'

    write_market_map(path, {'005930': 'KOSPI', '247540': 'KOSDAQ'})

    assert json.loads(path.read_text(encoding='utf-8')) == {'005930': 'KOSPI', '247540': 'KOSDAQ'}
    assert list(path.parent.glob('.*.tmp')) == []
