def test_fetch_daily_bars_minimal_payload_keeps_required_mapping() -> None:
    # Given: 선택 필드가 없는 기존 payload + 빈 base info (보조 소스 부재)
    import datetime as dt

    from src.marketdata.krx_bars import fetch_daily_bars
    from src.marketdata.schema import STORED_BAR_COLUMNS

    kospi_payload = {'OutBlock_1': [{'BAS_DD': '20260907', 'ISU_CD': '005930', 'ISU_NM': '삼성전자', 'MKT_NM': 'KOSPI',
                                     'TDD_CLSPRC': '270000', 'ACC_TRDVOL': '18314016', 'ACC_TRDVAL': '4900114076282', 'FLUC_RT': '5.68'}]}
    kosdaq_payload = {'OutBlock_1': [{'BAS_DD': '20260907', 'ISU_CD': '035720', 'ISU_NM': '카카오', 'MKT_NM': 'KOSDAQ',
                                      'TDD_CLSPRC': '50000', 'ACC_TRDVOL': '1000', 'ACC_TRDVAL': '50000000', 'FLUC_RT': '-1.20'}]}

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
            if 'isu_base_info' in url:
                return _Resp({'OutBlock_1': []})
            return _Resp(kospi_payload if 'stk_bydd_trd' in url else kosdaq_payload)

    session = _Session()

    # When
    out = fetch_daily_bars(dt.date(2026, 9, 7), auth_key='k', session=session)

    # Then: 필수 컬럼 값은 불변, 선택 컬럼은 null, bydd 2 → base info 2 순서
    assert out['symbol'].to_list() == ['005930', '035720']
    assert out['close'].to_list() == [270000.0, 50000.0]
    assert out['volume'].to_list() == [18314016, 1000]
    assert round(out['trade_value_100m'][0], 5) == round(4900114076282 / 1e8, 5)
    assert out['daily_change_pct'].to_list() == [5.68, -1.20]
    assert out['market'].to_list() == ['KOSPI', 'KOSDAQ']
    assert out['open'].to_list() == [None, None]
    assert out['base_price'].to_list() == [None, None]
    assert out['stock_cert_kind'].to_list() == [None, None]
    assert out.columns == list(STORED_BAR_COLUMNS)
    assert len(session.urls) == 4
    assert 'bydd_trd' in session.urls[0]
    assert 'bydd_trd' in session.urls[1]
    assert 'isu_base_info' in session.urls[2]
    assert 'isu_base_info' in session.urls[3]


def _four_way_session(bydd: dict, base: dict):
    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            pass

        def json(self):
            return self._payload

    class _Session:
        def __init__(self):
            self.urls: list[str] = []

        def post(self, url, **kw):
            self.urls.append(url)
            if 'isu_base_info' in url:
                payload = base['kospi'] if '/sto/stk_' in url else base['kosdaq']
            else:
                payload = bydd['kospi'] if 'stk_bydd_trd' in url else bydd['kosdaq']
            if isinstance(payload, BaseException):
                raise payload
            return _Resp(payload)

    return _Session()


def test_fetch_daily_bars_maps_ohl_base_price_and_security_class() -> None:
    # Given: OHL·부호 포함 전일대비·시총·상장주식수·증권구분 + base info 종류주권
    import datetime as dt

    import polars as pl

    from src.marketdata.krx_bars import fetch_daily_bars

    bydd_row = {'ISU_CD': '005930', 'TDD_CLSPRC': '70000', 'ACC_TRDVOL': '1000', 'ACC_TRDVAL': '70000000',
                'FLUC_RT': '1.0', 'TDD_OPNPRC': '69500', 'TDD_HGPRC': '70500', 'TDD_LWPRC': '69000',
                'CMPPREVDD_PRC': '-14', 'MKTCAP': '400000000000', 'LIST_SHRS': '5900000', 'SECT_TP_NM': ' '}
    bydd = {'kospi': {'OutBlock_1': [bydd_row]}, 'kosdaq': {'OutBlock_1': [dict(bydd_row, ISU_CD='035720')]}}
    base_row = {'ISU_SRT_CD': '005930', 'KIND_STKCERT_TP_NM': '신형우선주', 'SECUGRP_NM': '주권'}
    base = {'kospi': {'OutBlock_1': [base_row]}, 'kosdaq': {'OutBlock_1': [dict(base_row, ISU_SRT_CD='035720')]}}
    session = _four_way_session(bydd, base)

    # When
    out = fetch_daily_bars(dt.date(2026, 9, 7), auth_key='k', session=session)

    # Then
    assert out.filter(pl.col('symbol') == '005930')['base_price'].to_list() == [70014.0]
    assert out.filter(pl.col('symbol') == '005930')['stock_cert_kind'].to_list() == ['신형우선주']
    assert out['open'].to_list() == [69500.0, 69500.0]
    assert out['market_cap_krw'].to_list() == [400000000000, 400000000000]
    assert out['listed_shares'].to_list() == [5900000, 5900000]
    assert out['section'].to_list() == [None, None]
    assert len(session.urls) == 4


def test_fetch_daily_bars_nulls_ohl_for_zero_volume_rows() -> None:
    # Given: 거래량 0 행의 시·고가가 "0"
    import datetime as dt

    from src.marketdata.krx_bars import fetch_daily_bars

    row = {'ISU_CD': '005930', 'TDD_CLSPRC': '70000', 'ACC_TRDVOL': '0', 'ACC_TRDVAL': '0',
           'FLUC_RT': '0.0', 'TDD_OPNPRC': '0', 'TDD_HGPRC': '0', 'TDD_LWPRC': '0'}
    bydd = {'kospi': {'OutBlock_1': [row]}, 'kosdaq': {'OutBlock_1': [dict(row, ISU_CD='035720')]}}
    base = {'kospi': {'OutBlock_1': []}, 'kosdaq': {'OutBlock_1': []}}
    session = _four_way_session(bydd, base)

    # When
    out = fetch_daily_bars(dt.date(2026, 9, 7), auth_key='k', session=session)

    # Then: 가격은 null, 종가는 유지
    assert out['open'].to_list() == [None, None]
    assert out['high'].to_list() == [None, None]
    assert out['low'].to_list() == [None, None]
    assert out['close'].to_list() == [70000.0, 70000.0]


def test_fetch_daily_bars_degrades_class_columns_when_base_info_fails(caplog) -> None:
    # Given: KOSPI base info가 전송 실패, KOSDAQ base info는 정상
    import datetime as dt
    import logging

    import requests

    from src.marketdata.krx_bars import fetch_daily_bars

    row = {'ISU_CD': '005930', 'TDD_CLSPRC': '70000', 'ACC_TRDVOL': '1000', 'ACC_TRDVAL': '70000000', 'FLUC_RT': '1.0'}
    bydd = {'kospi': {'OutBlock_1': [row]}, 'kosdaq': {'OutBlock_1': [dict(row, ISU_CD='035720')]}}
    base = {
        'kospi': requests.ConnectionError('boom'),
        'kosdaq': {'OutBlock_1': [{'ISU_SRT_CD': '035720', 'KIND_STKCERT_TP_NM': '보통주', 'SECUGRP_NM': '주권'}]},
    }
    session = _four_way_session(bydd, base)

    # When
    with caplog.at_level(logging.WARNING, logger='src.marketdata.krx_bars'):
        out = fetch_daily_bars(dt.date(2026, 9, 7), auth_key='k', session=session)

    # Then: 예외 없이 반환, 실패 시장 클래스 null, DEGRADED 경고
    assert out['stock_cert_kind'].to_list() == [None, '보통주']
    assert any('stage=krx_base_info status=DEGRADED' in rec.message for rec in caplog.records)


def test_fetch_daily_bars_degrades_class_columns_when_base_info_empty(caplog) -> None:
    # Given: 빈 OutBlock_1 / ISU_SRT_CD 없는 행만 반환하는 base info
    import datetime as dt
    import logging

    from src.marketdata.krx_bars import fetch_daily_bars

    row = {'ISU_CD': '005930', 'TDD_CLSPRC': '70000', 'ACC_TRDVOL': '1000', 'ACC_TRDVAL': '70000000', 'FLUC_RT': '1.0'}
    bydd = {'kospi': {'OutBlock_1': [row]}, 'kosdaq': {'OutBlock_1': [dict(row, ISU_CD='035720')]}}
    base = {'kospi': {'OutBlock_1': []}, 'kosdaq': {'OutBlock_1': [{'KIND_STKCERT_TP_NM': '보통주'}]}}
    session = _four_way_session(bydd, base)

    # When
    with caplog.at_level(logging.WARNING, logger='src.marketdata.krx_bars'):
        out = fetch_daily_bars(dt.date(2026, 9, 7), auth_key='k', session=session)

    # Then
    assert out['stock_cert_kind'].to_list() == [None, None]
    assert sum('stage=krx_base_info status=DEGRADED' in rec.message for rec in caplog.records) == 2


def test_append_daily_bars_coerces_narrow_frame_without_touching_old_rows(tmp_path) -> None:
    # Given: 필수 6컬럼만 가진 신규 일자 + 확장 컬럼 포함 다음 일자
    import datetime as dt

    import polars as pl

    from src.marketdata.krx_bars import append_daily_bars
    from src.marketdata.schema import BAR_SCHEMA, STORED_BAR_COLUMNS

    store_path = tmp_path / 'daily'
    narrow = pl.DataFrame({
        'date': [dt.date(2026, 9, 4)],
        'symbol': ['005930'],
        'close': [70000.0],
        'volume': [1000],
        'trade_value_100m': [700.0],
        'daily_change_pct': [1.0],
    })
    assert append_daily_bars(store_path, narrow) == 1
    incoming = pl.DataFrame([{
        'date': dt.date(2026, 9, 7),
        'symbol': '005930',
        'close': 71000.0,
        'volume': 2000,
        'trade_value_100m': 1420.0,
        'daily_change_pct': 1.4,
        'market': 'KOSPI',
        'open': 70500.0,
        'high': 71500.0,
        'low': 70000.0,
        'base_price': 70000.0,
        'market_cap_krw': 400000000000,
        'listed_shares': 5900000,
        'section': None,
        'stock_cert_kind': '보통주',
        'security_group': '주권',
    }], schema=BAR_SCHEMA)

    # When
    added = append_daily_bars(store_path, incoming)

    # Then
    assert added == 1
    saved = pl.read_parquet(store_path / '2026-09.parquet')
    assert saved.columns == list(STORED_BAR_COLUMNS)
    old = saved.filter(pl.col('date') == dt.date(2026, 9, 4))
    assert old['close'].to_list() == [70000.0]
    assert old['open'].to_list() == [None]
    assert old['stock_cert_kind'].to_list() == [None]
    new = saved.filter(pl.col('date') == dt.date(2026, 9, 7))
    assert new['open'].to_list() == [70500.0]
    assert new['stock_cert_kind'].to_list() == ['보통주']


def test_fetch_daily_bars_raises_on_empty_response() -> None:
    import datetime as dt
    import pytest
    from src.marketdata.krx_bars import KrxBarsError, fetch_daily_bars

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
    from src.marketdata.krx_bars import KrxBarsError, fetch_daily_bars

    class _Session:
        def post(self, url, **kw):
            raise requests.ConnectionError('boom')

    with pytest.raises(KrxBarsError):
        fetch_daily_bars(dt.date(2026, 9, 7), auth_key='k', session=_Session())


def test_latest_trading_day_walks_backward_on_krx_bars_error(monkeypatch) -> None:
    import datetime as dt
    import polars as pl
    import src.marketdata.krx_bars as bars_mod
    from src.marketdata.krx_bars import latest_trading_day

    calls: list[dt.date] = []

    def _fake(date, *, auth_key, session=None):
        calls.append(date)
        if date != dt.date(2026, 9, 4):
            raise bars_mod.KrxNoTradingDataError('non-trading')
        return pl.DataFrame({'date': [date], 'symbol': ['005930'], 'close': [1.0],
                             'volume': [1], 'trade_value_100m': [1.0], 'daily_change_pct': [0.1], 'market': ['KOSPI']})

    monkeypatch.setattr(bars_mod, 'fetch_daily_bars', _fake)

    day, bars = latest_trading_day(dt.date(2026, 9, 8), auth_key='k')

    assert day == dt.date(2026, 9, 4)
    assert calls == [dt.date(2026, 9, 7), dt.date(2026, 9, 6), dt.date(2026, 9, 5), dt.date(2026, 9, 4)]

def test_latest_trading_day_never_fetches_ref_date_itself(monkeypatch) -> None:
    import datetime as dt
    import polars as pl
    import src.marketdata.krx_bars as bars_mod
    from src.marketdata.krx_bars import latest_trading_day

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
    import src.marketdata.krx_bars as bars_mod
    from src.marketdata.krx_bars import KrxBarsError, latest_trading_day

    def _always_fail(date, *, auth_key, session=None):
        raise KrxBarsError('none')

    monkeypatch.setattr(bars_mod, 'fetch_daily_bars', _always_fail)

    with pytest.raises(KrxBarsError):
        latest_trading_day(dt.date(2026, 9, 8), auth_key='k', max_lookback=3)


def test_derive_market_map_builds_symbol_to_market_dict() -> None:
    import datetime as dt
    import polars as pl
    from src.marketdata.krx_bars import derive_market_map

    bars = pl.DataFrame({'date': [dt.date(2026, 9, 7)] * 2, 'symbol': ['005930', '247540'],
                         'close': [1.0, 2.0], 'volume': [1, 1], 'trade_value_100m': [1.0, 1.0],
                         'daily_change_pct': [0.1, 0.1], 'market': ['KOSPI', 'KOSDAQ']})

    out = derive_market_map(bars)

    assert out == {'005930': 'KOSPI', '247540': 'KOSDAQ'}


def test_append_daily_bars_creates_new_store_file(tmp_path) -> None:
    import datetime as dt
    import polars as pl
    from src.marketdata.krx_bars import append_daily_bars
    from src.marketdata.schema import STORED_BAR_COLUMNS

    store = tmp_path / 'bars'
    bars = pl.DataFrame({'date': [dt.date(2026, 9, 7)], 'symbol': ['005930'], 'close': [270000.0],
                         'volume': [100], 'trade_value_100m': [1.0], 'daily_change_pct': [5.68], 'market': ['KOSPI']})

    appended = append_daily_bars(store, bars)

    assert appended == 1
    assert (store / '2026-09.parquet').exists()
    stored = pl.read_parquet(store / '2026-09.parquet')
    assert stored.columns == list(STORED_BAR_COLUMNS)




def test_append_daily_bars_appends_new_date_alongside_existing(tmp_path) -> None:
    import datetime as dt
    import polars as pl
    from src.marketdata.krx_bars import append_daily_bars

    store = tmp_path / 'bars'
    day1 = pl.DataFrame({'date': [dt.date(2026, 9, 7)], 'symbol': ['005930'], 'close': [1.0],
                         'volume': [1], 'trade_value_100m': [1.0], 'daily_change_pct': [0.1]})
    day2 = pl.DataFrame({'date': [dt.date(2026, 9, 8)], 'symbol': ['005930'], 'close': [2.0],
                         'volume': [2], 'trade_value_100m': [2.0], 'daily_change_pct': [0.2]})

    append_daily_bars(store, day1)
    second = append_daily_bars(store, day2)

    assert second == 1
    assert pl.read_parquet(store / '2026-09.parquet').height == 2


def test_backfill_bars_accumulates_until_window_days_reached(tmp_path, monkeypatch) -> None:
    import datetime as dt
    import polars as pl
    import src.marketdata.krx_bars as bars_mod
    from src.marketdata.krx_bars import KrxNoTradingDataError, backfill_bars

    def _fake_fetch(date, *, auth_key, session=None):
        if date.weekday() >= 5:
            raise KrxNoTradingDataError('weekend')
        return pl.DataFrame({'date': [date], 'symbol': ['005930'], 'close': [100.0],
                             'volume': [1], 'trade_value_100m': [1.0], 'daily_change_pct': [0.1]})

    monkeypatch.setattr(bars_mod, 'fetch_daily_bars', _fake_fetch)

    result = backfill_bars(tmp_path / 'bars', auth_key='k', end_date=dt.date(2026, 9, 7), window_days=5)

    assert result['trading_days'] == 5
    assert result['appended_rows'] == 5
    assert pl.read_parquet(tmp_path / 'bars' / '2026-09.parquet').height == 5

def test_backfill_bars_raises_when_calendar_cap_exhausted_before_window(tmp_path, monkeypatch) -> None:
    import datetime as dt
    import pytest
    import src.marketdata.krx_bars as bars_mod
    from src.marketdata.krx_bars import KrxBarsError, backfill_bars

    def _always_fail(date, *, auth_key, session=None):
        raise KrxBarsError('no data')

    monkeypatch.setattr(bars_mod, 'fetch_daily_bars', _always_fail)

    with pytest.raises(KrxBarsError):
        backfill_bars(tmp_path / 'bars', auth_key='k', end_date=dt.date(2026, 9, 7), window_days=3)


def test_write_market_map_writes_atomic_json(tmp_path) -> None:
    import json
    from src.marketdata.krx_bars import write_market_map

    path = tmp_path / 'nested' / 'market_map.json'

    write_market_map(path, {'005930': 'KOSPI', '247540': 'KOSDAQ'})

    assert json.loads(path.read_text(encoding='utf-8')) == {'005930': 'KOSPI', '247540': 'KOSDAQ'}
    assert list(path.parent.glob('.*.tmp')) == []


def test_fetch_daily_bars_raises_incomplete_market_when_one_market_empty() -> None:
    # Given: KOSPI 만 응답하고 KOSDAQ 은 빈 배열인 부분 수집
    import datetime as dt

    import pytest

    from src.marketdata.krx_bars import IncompleteMarketError, fetch_daily_bars

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            self._checked = True

        def json(self):
            return self._payload

    class _Session:
        def post(self, url, **kw):
            rows = [{'ISU_CD': '005930', 'MKT_NM': 'KOSPI', 'TDD_CLSPRC': '100', 'ACC_TRDVOL': '1',
                     'ACC_TRDVAL': '100', 'FLUC_RT': '0.0'}] if 'stk_bydd_trd' in url else []
            return _Resp({'OutBlock_1': rows})

    # When / Then: 부분 저장 대신 fail-closed
    with pytest.raises(IncompleteMarketError, match='KOSDAQ'):
        fetch_daily_bars(dt.date(2026, 9, 7), auth_key='k', session=_Session())


def test_incomplete_market_error_is_not_swallowed_by_latest_trading_day(monkeypatch) -> None:
    # Given: 특정 일자에서 부분 수집이 발생
    import datetime as dt

    import pytest

    import src.marketdata.krx_bars as bars_mod
    from src.marketdata.krx_bars import IncompleteMarketError, latest_trading_day

    def _partial(date, *, auth_key, session=None):
        raise IncompleteMarketError(f'partial market data for {date}')

    monkeypatch.setattr(bars_mod, 'fetch_daily_bars', _partial)

    # When / Then: 비영업일 워크백으로 오인해 삼키지 않고 그대로 전파한다
    with pytest.raises(IncompleteMarketError):
        latest_trading_day(dt.date(2026, 9, 8), auth_key='k')


def test_append_daily_bars_recovers_missing_symbols_for_existing_date(tmp_path) -> None:
    # Given: 과거에 KOSPI 만 저장된 거래일
    import datetime as dt

    import polars as pl

    from src.marketdata.krx_bars import append_daily_bars

    store = tmp_path / 'bars'
    partial = pl.DataFrame({'date': [dt.date(2026, 9, 7)], 'symbol': ['005930'], 'close': [1.0],
                            'volume': [1], 'trade_value_100m': [1.0], 'daily_change_pct': [0.1]})
    full = pl.DataFrame({'date': [dt.date(2026, 9, 7), dt.date(2026, 9, 7)], 'symbol': ['005930', '035720'],
                         'close': [1.0, 2.0], 'volume': [1, 2], 'trade_value_100m': [1.0, 2.0],
                         'daily_change_pct': [0.1, 0.2]})

    first = append_daily_bars(store, partial)

    # When: 동일 날짜의 완전 데이터로 재적재
    second = append_daily_bars(store, full)

    # Then: date 단위 락인 없이 누락 심볼이 복구된다
    assert first == 1
    assert second == 1
    stored = pl.read_parquet(store / '2026-09.parquet')
    assert sorted(stored['symbol'].to_list()) == ['005930', '035720']
    assert stored.height == 2


def test_append_daily_bars_is_idempotent_for_identical_rows(tmp_path) -> None:
    # Given: 동일 (date, symbol) 데이터를 두 번 적재
    import datetime as dt

    import polars as pl

    from src.marketdata.krx_bars import append_daily_bars

    store = tmp_path / 'bars'
    bars = pl.DataFrame({'date': [dt.date(2026, 9, 7)], 'symbol': ['005930'], 'close': [270000.0],
                         'volume': [100], 'trade_value_100m': [1.0], 'daily_change_pct': [5.68]})

    # When
    first = append_daily_bars(store, bars)
    second = append_daily_bars(store, bars)

    # Then: 신규 키가 없으면 재기록하지 않는다
    assert first == 1
    assert second == 0
    assert pl.read_parquet(store / '2026-09.parquet').height == 1


def test_append_daily_bars_raises_on_implausible_rowcount(tmp_path) -> None:
    # Given: 직전 거래일 10행이 저장된 스토어
    import datetime as dt

    import polars as pl

    import pytest

    from src.marketdata.krx_bars import ImplausibleRowCountError, append_daily_bars

    store = tmp_path / 'bars'
    prev = pl.DataFrame({
        'date': [dt.date(2026, 9, 7)] * 10,
        'symbol': [f'{i:06d}' for i in range(10)],
        'close': [1.0] * 10, 'volume': [1] * 10,
        'trade_value_100m': [1.0] * 10, 'daily_change_pct': [0.1] * 10,
    })
    append_daily_bars(store, prev)

    truncated = pl.DataFrame({'date': [dt.date(2026, 9, 8)], 'symbol': ['000000'], 'close': [1.0],
                              'volume': [1], 'trade_value_100m': [1.0], 'daily_change_pct': [0.1]})

    # When / Then: 직전 거래일 대비 하한 비율 미만이면 절단 응답으로 판정해 거부한다
    with pytest.raises(ImplausibleRowCountError):
        append_daily_bars(store, truncated)

    assert pl.read_parquet(store / '2026-09.parquet').height == 10


def test_append_daily_bars_accepts_rowcount_within_ratio(tmp_path) -> None:
    # Given: 직전 거래일 10행, 신규일 9행 (비율 0.9 = 하한 경계)
    import datetime as dt

    import polars as pl

    from src.marketdata.krx_bars import MIN_ROWCOUNT_RATIO, append_daily_bars

    store = tmp_path / 'bars'
    prev = pl.DataFrame({
        'date': [dt.date(2026, 9, 7)] * 10,
        'symbol': [f'{i:06d}' for i in range(10)],
        'close': [1.0] * 10, 'volume': [1] * 10,
        'trade_value_100m': [1.0] * 10, 'daily_change_pct': [0.1] * 10,
    })
    append_daily_bars(store, prev)
    nxt = pl.DataFrame({
        'date': [dt.date(2026, 9, 8)] * 9,
        'symbol': [f'{i:06d}' for i in range(9)],
        'close': [1.0] * 9, 'volume': [1] * 9,
        'trade_value_100m': [1.0] * 9, 'daily_change_pct': [0.1] * 9,
    })

    # When
    appended = append_daily_bars(store, nxt)

    # Then: 경계값은 정상 상장폐지/거래정지 변동으로 수용한다
    assert MIN_ROWCOUNT_RATIO == 0.90
    assert appended == 9
    assert pl.read_parquet(store / '2026-09.parquet').height == 19


def test_fetch_daily_bars_raises_transport_error_on_request_exception() -> None:
    import datetime as dt

    import pytest
    import requests

    from src.marketdata.krx_bars import KrxBarsError, KrxTransportError, fetch_daily_bars

    class _Session:
        def post(self, url, **kw):
            raise requests.ConnectionError("boom")

    with pytest.raises(KrxTransportError, match="krx request failed") as excinfo:
        fetch_daily_bars(dt.date(2026, 9, 11), auth_key="k", session=_Session())
    assert isinstance(excinfo.value, KrxBarsError)


def test_fetch_daily_bars_raises_no_trading_data_when_both_markets_empty() -> None:
    import datetime as dt

    import pytest

    from src.marketdata.krx_bars import KrxNoTradingDataError, fetch_daily_bars

    class _Resp:
        def raise_for_status(self):
            return None

        def json(self):
            return {"OutBlock_1": []}

    class _Session:
        def post(self, url, **kw):
            return _Resp()

    with pytest.raises(KrxNoTradingDataError, match="no trading data"):
        fetch_daily_bars(dt.date(2026, 9, 12), auth_key="k", session=_Session())


def test_latest_trading_day_propagates_transport_error_without_walking_back(monkeypatch) -> None:
    import datetime as dt

    import pytest

    import src.marketdata.krx_bars as bars_mod
    from src.marketdata.krx_bars import KrxTransportError, latest_trading_day

    calls: list[dt.date] = []

    def _fake(date, *, auth_key, session=None):
        calls.append(date)
        raise KrxTransportError(f"krx request failed for {date}: timeout")

    monkeypatch.setattr(bars_mod, "fetch_daily_bars", _fake)

    with pytest.raises(KrxTransportError):
        latest_trading_day(dt.date(2026, 9, 14), auth_key="k")
    assert calls == [dt.date(2026, 9, 13)]


def test_backfill_bars_propagates_transport_error(tmp_path, monkeypatch) -> None:
    import datetime as dt

    import polars as pl
    import pytest

    import src.marketdata.krx_bars as bars_mod
    from src.marketdata.krx_bars import KrxNoTradingDataError, KrxTransportError, backfill_bars

    def _fake(date, *, auth_key, session=None):
        if date == dt.date(2026, 9, 7):
            return pl.DataFrame({"date": [date], "symbol": ["005930"], "close": [1.0], "volume": [1], "trade_value_100m": [1.0], "daily_change_pct": [0.1]})
        if date.weekday() >= 5:
            raise KrxNoTradingDataError("weekend")
        raise KrxTransportError("krx request failed")

    monkeypatch.setattr(bars_mod, "fetch_daily_bars", _fake)

    with pytest.raises(KrxTransportError):
        backfill_bars(tmp_path / "bars", auth_key="k", end_date=dt.date(2026, 9, 7), window_days=3)


def test_partial_and_implausible_errors_are_krx_bars_errors() -> None:
    from src.marketdata.krx_bars import (
        ImplausibleRowCountError,
        IncompleteMarketError,
        KrxBarsError,
        KrxNoTradingDataError,
        KrxTransportError,
    )

    for cls in (IncompleteMarketError, ImplausibleRowCountError, KrxNoTradingDataError, KrxTransportError):
        assert issubclass(cls, KrxBarsError)
    assert not issubclass(IncompleteMarketError, KrxNoTradingDataError)
    assert not issubclass(KrxTransportError, KrxNoTradingDataError)


def test_retry_wait_seconds_grows_exponentially_and_caps(monkeypatch) -> None:
    import types

    import src.marketdata.krx_bars as bars_mod

    monkeypatch.setattr(bars_mod, "RETRY_WAIT_BASE_S", 1.0)
    waits = [bars_mod.retry_wait_seconds(types.SimpleNamespace(attempt_number=n)) for n in (1, 2, 3, 4)]

    assert bars_mod.RETRY_WAIT_MAX_S == 4.0
    assert waits == [1.0, 2.0, 4.0, 4.0]


def test_post_krx_sleeps_between_attempts_using_retry_wait(monkeypatch) -> None:
    import datetime as dt

    import pytest
    import requests

    import src.marketdata.krx_bars as bars_mod

    slept: list[float] = []
    monkeypatch.setattr(bars_mod, "RETRY_WAIT_BASE_S", 0.5)
    monkeypatch.setattr(bars_mod._post_krx.retry, "sleep", slept.append)
    attempts = {"n": 0}

    class _Session:
        def post(self, url, **kw):
            attempts["n"] += 1
            raise requests.ConnectionError("down")

    with pytest.raises(bars_mod.KrxTransportError):
        bars_mod.fetch_daily_bars(dt.date(2026, 9, 11), auth_key="k", session=_Session())
    assert attempts["n"] == 3
    assert slept == [0.5, 1.0]


def _valid_bar_row(**overrides):
    import datetime as dt

    row = {
        "date": dt.date(2026, 9, 8),
        "symbol": "005930",
        "close": 70000.0,
        "volume": 1000,
        "trade_value_100m": 700.0,
        "daily_change_pct": 1.01,
        "market": "KOSPI",
        "open": 69500.0,
        "high": 70500.0,
        "low": 69000.0,
        "base_price": 69300.0,
        "market_cap_krw": None,
        "listed_shares": None,
        "section": None,
        "stock_cert_kind": None,
        "security_group": None,
    }
    row.update(overrides)
    return row


def _bars_frame(rows) -> object:
    import polars as pl

    from src.marketdata.schema import BAR_SCHEMA

    return pl.DataFrame(rows, schema=BAR_SCHEMA)


def test_validate_daily_bars_accepts_clean_traded_and_halted_rows() -> None:
    import datetime as dt

    from src.marketdata.krx_bars import validate_daily_bars

    halted = _valid_bar_row(
        symbol="000660", close=50000.0, volume=0, trade_value_100m=0.0, daily_change_pct=0.0,
        open=None, high=None, low=None, base_price=None,
    )
    validate_daily_bars(_bars_frame([_valid_bar_row(), halted]))
    assert dt.date(2026, 9, 8).isoformat() == "2026-09-08"


def test_validate_daily_bars_accepts_extreme_but_genuine_change() -> None:
    from src.marketdata.krx_bars import validate_daily_bars

    row = _valid_bar_row(close=1000.0, volume=100, trade_value_100m=1.0, daily_change_pct=-94.95, base_price=None, open=1000.0, high=1100.0, low=900.0)
    validate_daily_bars(_bars_frame([row]))


def test_validate_daily_bars_rejects_zero_close() -> None:
    import pytest

    from src.marketdata.krx_bars import ImplausibleBarValuesError, validate_daily_bars

    with pytest.raises(ImplausibleBarValuesError, match="close_positive"):
        validate_daily_bars(_bars_frame([_valid_bar_row(close=0.0, low=0.0, open=0.0, high=1.0)]))


def test_validate_daily_bars_rejects_inverted_high_low() -> None:
    import pytest

    from src.marketdata.krx_bars import ImplausibleBarValuesError, validate_daily_bars

    with pytest.raises(ImplausibleBarValuesError, match="low_high_order"):
        validate_daily_bars(_bars_frame([_valid_bar_row(high=69000.0, low=70000.0, open=69500.0, close=69500.0)]))


def test_validate_daily_bars_rejects_close_outside_range() -> None:
    import pytest

    from src.marketdata.krx_bars import ImplausibleBarValuesError, validate_daily_bars

    with pytest.raises(ImplausibleBarValuesError, match="close_in_range"):
        validate_daily_bars(_bars_frame([_valid_bar_row(close=71000.0)]))


def test_validate_daily_bars_rejects_volume_without_value() -> None:
    import pytest

    from src.marketdata.krx_bars import ImplausibleBarValuesError, validate_daily_bars

    with pytest.raises(ImplausibleBarValuesError, match="volume_trade_value_consistency"):
        validate_daily_bars(_bars_frame([_valid_bar_row(trade_value_100m=0.0)]))


def test_validate_daily_bars_rejects_change_pct_disagreement() -> None:
    import pytest

    from src.marketdata.krx_bars import ImplausibleBarValuesError, validate_daily_bars

    row = _valid_bar_row(close=1100.0, open=1050.0, high=1150.0, low=1000.0, base_price=1000.0, daily_change_pct=5.0)
    with pytest.raises(ImplausibleBarValuesError, match="change_pct_agreement"):
        validate_daily_bars(_bars_frame([row]))


def test_validate_daily_bars_rejects_duplicate_key() -> None:
    import pytest

    from src.marketdata.krx_bars import ImplausibleBarValuesError, validate_daily_bars

    with pytest.raises(ImplausibleBarValuesError, match="duplicate_key"):
        validate_daily_bars(_bars_frame([_valid_bar_row(), _valid_bar_row()]))


def test_validate_daily_bars_skips_null_ohl_on_traded_legacy_row() -> None:
    from src.marketdata.krx_bars import validate_daily_bars

    row = _valid_bar_row(open=None, high=None, low=None, base_price=None)
    validate_daily_bars(_bars_frame([row]))


def test_append_daily_bars_leaves_store_untouched_on_implausible_batch(tmp_path) -> None:
    import polars as pl
    import pytest

    from src.marketdata.krx_bars import ImplausibleBarValuesError, append_daily_bars

    store = tmp_path / "daily"
    append_daily_bars(store, _bars_frame([_valid_bar_row()]))
    before = (store / "2026-09.parquet").read_bytes()
    with pytest.raises(ImplausibleBarValuesError):
        append_daily_bars(store, _bars_frame([_valid_bar_row(close=0.0, low=0.0, open=0.0, high=1.0)]))
    assert (store / "2026-09.parquet").read_bytes() == before
    assert pl.read_parquet(store / "2026-09.parquet").height == 1
