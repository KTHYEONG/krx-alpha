def test_decode_and_flag_ticks_returns_none_when_no_tick_rows() -> None:
    import polars as pl
    from src.storage.quality import decode_and_flag_ticks

    # Given: 호가(H0STASP0) 행만 존재하는 L1 DataFrame
    df = pl.DataFrame({
        'raw': ['anything-not-parsed'],
        'tr_id': ['H0STASP0'],
        'recv_wall_ns': [100],
    })

    # When
    summary = decode_and_flag_ticks(df)

    # Then
    assert summary is None


def test_decode_and_flag_ticks_clean_tick_not_flagged() -> None:
    import json
    import polars as pl
    from src.storage.quality import decode_and_flag_ticks

    # Given: 보합(sign=3, change=0)인 정상 체결 틱 1건
    body = {'shcode': '005930', 'price': '70000', 'cvolume': '10', 'volume': '100', 'change': '0', 'sign': '3'}
    raw = json.dumps({'header': {'tr_cd': 'S3_', 'tr_key': '005930'}, 'body': body}, ensure_ascii=False)
    df = pl.DataFrame({'raw': [raw], 'tr_id': ['H0STCNT0'], 'recv_wall_ns': [100]})

    # When
    summary = decode_and_flag_ticks(df)

    # Then
    assert summary is not None
    assert summary.rows == 1
    assert summary.decode_fail == 0
    assert summary.zero_volume == 0
    assert summary.price_band_violation == 0
    assert summary.cum_volume_regression == 0


def test_decode_and_flag_ticks_flags_decode_fail_for_malformed_raw() -> None:
    import polars as pl
    from src.storage.quality import decode_and_flag_ticks

    # Given: JSON이 아닌 raw 문자열 (zstd 손상/부분쓰기 시나리오 근사)
    df = pl.DataFrame({'raw': ['not-json'], 'tr_id': ['H0STCNT0'], 'recv_wall_ns': [100]})

    # When: 예외를 던지지 않고 처리되어야 한다
    summary = decode_and_flag_ticks(df)

    # Then
    assert summary is not None
    assert summary.rows == 1
    assert summary.decode_fail == 1
    assert summary.zero_volume == 0
    assert summary.price_band_violation == 0
    assert summary.cum_volume_regression == 0


def test_decode_and_flag_ticks_flags_nan_price_string() -> None:
    import json
    import polars as pl
    from src.storage.quality import decode_and_flag_ticks

    # Given: price 필드가 literal 'NaN' 문자열인 손상 틱
    body = {'shcode': '005930', 'price': 'NaN', 'cvolume': '10', 'volume': '100', 'change': '0', 'sign': '3'}
    raw = json.dumps({'header': {'tr_cd': 'S3_', 'tr_key': '005930'}, 'body': body}, ensure_ascii=False)
    df = pl.DataFrame({'raw': [raw], 'tr_id': ['H0STCNT0'], 'recv_wall_ns': [100]})

    # When
    summary = decode_and_flag_ticks(df)

    # Then
    assert summary is not None
    assert summary.decode_fail == 1


def test_decode_and_flag_ticks_flags_zero_cvolume() -> None:
    import json
    import polars as pl
    from src.storage.quality import decode_and_flag_ticks

    # Given: 체결량 0인 무의미한 체결 틱
    body = {'shcode': '005930', 'price': '70000', 'cvolume': '0', 'volume': '100', 'change': '0', 'sign': '3'}
    raw = json.dumps({'header': {'tr_cd': 'S3_', 'tr_key': '005930'}, 'body': body}, ensure_ascii=False)
    df = pl.DataFrame({'raw': [raw], 'tr_id': ['H0STCNT0'], 'recv_wall_ns': [100]})

    # When
    summary = decode_and_flag_ticks(df)

    # Then
    assert summary is not None
    assert summary.decode_fail == 0
    assert summary.zero_volume == 1


def test_decode_and_flag_ticks_flags_price_band_violation() -> None:
    import json
    import polars as pl
    from src.storage.quality import decode_and_flag_ticks

    # Given: ref_price=10000(=price-change), price=15000 -> +50% (가격제한폭 +30% 초과)
    body = {'shcode': '005930', 'price': '15000', 'cvolume': '10', 'volume': '100', 'change': '5000', 'sign': '2'}
    raw = json.dumps({'header': {'tr_cd': 'S3_', 'tr_key': '005930'}, 'body': body}, ensure_ascii=False)
    df = pl.DataFrame({'raw': [raw], 'tr_id': ['H0STCNT0'], 'recv_wall_ns': [100]})

    # When
    summary = decode_and_flag_ticks(df)

    # Then
    assert summary is not None
    assert summary.decode_fail == 0
    assert summary.price_band_violation == 1


def test_decode_and_flag_ticks_fallback_batch_survives_non_numeric_body_field() -> None:
    import json
    import polars as pl
    from src.storage.quality import decode_and_flag_ticks

    # Given: 배치 내 비-JSON 행이 하나 있어 폴백 경로로 전환되고,
    # 같은 배치의 다른 행은 JSON은 유효하나 price가 숫자로 캐스팅 불가한 문자열('N/A')
    body = {'shcode': '005930', 'price': 'N/A', 'cvolume': '10', 'volume': '100', 'change': '0', 'sign': '3'}
    raw_ok_json_bad_number = json.dumps({'header': {'tr_cd': 'S3_', 'tr_key': '005930'}, 'body': body}, ensure_ascii=False)
    df = pl.DataFrame({
        'raw': ['not-json', raw_ok_json_bad_number],
        'tr_id': ['H0STCNT0', 'H0STCNT0'],
        'recv_wall_ns': [100, 200],
    })

    # When: 예외 없이 두 행 모두 decode_fail로 집계되어야 한다 (ValueError 미발생)
    summary = decode_and_flag_ticks(df)

    # Then
    assert summary is not None
    assert summary.rows == 2
    assert summary.decode_fail == 2
    assert summary.zero_volume == 0
    assert summary.price_band_violation == 0


def test_decode_and_flag_ticks_flags_cum_volume_regression() -> None:
    import json
    import polars as pl
    from src.storage.quality import decode_and_flag_ticks

    # Given: 같은 심볼, 같은 recv_wall_ns 순서로 누적거래량이 500 -> 300 으로 역행
    body1 = {'shcode': '005930', 'price': '70000', 'cvolume': '10', 'volume': '500', 'change': '0', 'sign': '3'}
    body2 = {'shcode': '005930', 'price': '70000', 'cvolume': '10', 'volume': '300', 'change': '0', 'sign': '3'}
    raw1 = json.dumps({'header': {'tr_cd': 'S3_', 'tr_key': '005930'}, 'body': body1}, ensure_ascii=False)
    raw2 = json.dumps({'header': {'tr_cd': 'S3_', 'tr_key': '005930'}, 'body': body2}, ensure_ascii=False)
    df = pl.DataFrame({'raw': [raw1, raw2], 'tr_id': ['H0STCNT0', 'H0STCNT0'], 'recv_wall_ns': [100, 200]})

    # When
    summary = decode_and_flag_ticks(df)

    # Then: 첫 틱은 volume_prev가 없어 미탐지, 두번째 틱만 역행 탐지
    assert summary is not None
    assert summary.decode_fail == 0
    assert summary.cum_volume_regression == 1
