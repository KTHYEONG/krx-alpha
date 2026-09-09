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


def test_decode_and_flag_ticks_flags_tick_loss_and_quantifies_lost_volume() -> None:
    import json
    import polars as pl
    from src.storage.quality import decode_and_flag_ticks

    def _tick(volume, cvolume, mdchecnt, mschecnt):
        b = {
            'shcode': '005930', 'price': '70000', 'cvolume': str(cvolume), 'volume': str(volume),
            'change': '0', 'sign': '3', 'drate': '0.00', 'mdchecnt': str(mdchecnt), 'mschecnt': str(mschecnt),
        }
        return json.dumps({'header': {'tr_cd': 'S3_', 'tr_key': '005930'}, 'body': b}, ensure_ascii=False)

    # Given: volume 500 -> 520 (델타 20) 인데 cvolume=10, 실제 체결건수(mdchecnt+mschecnt) 델타는 1건 뿐
    # -> 벤더 배치 전송(여러 체결이 한 메시지로 묶임)이 아닌 진짜 물량 불일치
    df = pl.DataFrame({
        'raw': [_tick(500, 10, 100, 50), _tick(520, 10, 101, 50)],
        'tr_id': ['H0STCNT0', 'H0STCNT0'],
        'recv_wall_ns': [100, 200],
    })

    # When
    summary = decode_and_flag_ticks(df)

    # Then
    assert summary is not None
    assert summary.decode_fail == 0
    assert summary.tick_loss == 1
    assert summary.lost_volume == 10


def test_decode_and_flag_ticks_ignores_vendor_batched_ticks() -> None:
    """실측(ADR_20260909_stream_integrity_v2 정정) 회귀 테스트.

    실데이터 검증 결과 volume-cvolume 델타 불일치의 99.99%는 벤더가 짧은 시간 내
    여러 체결을 한 메시지로 배치 전송(mdchecnt+mschecnt 델타>1)한 정상 현상이었다 —
    이걸 tick_loss로 오탐하면 안 된다.
    """
    import json
    import polars as pl
    from src.storage.quality import decode_and_flag_ticks

    def _tick(volume, cvolume, mdchecnt, mschecnt):
        b = {
            'shcode': '005930', 'price': '70000', 'cvolume': str(cvolume), 'volume': str(volume),
            'change': '0', 'sign': '3', 'drate': '0.00', 'mdchecnt': str(mdchecnt), 'mschecnt': str(mschecnt),
        }
        return json.dumps({'header': {'tr_cd': 'S3_', 'tr_key': '005930'}, 'body': b}, ensure_ascii=False)

    # Given: volume 500 -> 520 (델타 20) 이고 cvolume=10 이지만,
    # 실제 체결건수(mdchecnt+mschecnt)가 3건 늘어 여러 체결이 배치로 묶인 정상 상황
    df = pl.DataFrame({
        'raw': [_tick(500, 10, 100, 50), _tick(520, 10, 102, 51)],
        'tr_id': ['H0STCNT0', 'H0STCNT0'],
        'recv_wall_ns': [100, 200],
    })

    # When
    summary = decode_and_flag_ticks(df)

    # Then: 배치 전송이므로 오탐 없음
    assert summary is not None
    assert summary.decode_fail == 0
    assert summary.tick_loss == 0
    assert summary.lost_volume == 0


def test_decode_and_flag_ticks_conservation_law_skips_zero_volume_rows() -> None:
    import json
    import polars as pl
    from src.storage.quality import decode_and_flag_ticks

    def _tick(volume, cvolume, mdchecnt, mschecnt):
        b = {
            'shcode': '005930', 'price': '70000', 'cvolume': str(cvolume), 'volume': str(volume),
            'change': '0', 'sign': '3', 'drate': '0.00', 'mdchecnt': str(mdchecnt), 'mschecnt': str(mschecnt),
        }
        return json.dumps({'header': {'tr_cd': 'S3_', 'tr_key': '005930'}, 'body': b}, ensure_ascii=False)

    # Given: 두번째 행이 cvolume=0 이면서 volume이 동일(변화 없음), 체결건수도 불변(checnt_delta=0)
    df = pl.DataFrame({
        'raw': [_tick(500, 10, 100, 50), _tick(500, 0, 100, 50)],
        'tr_id': ['H0STCNT0', 'H0STCNT0'],
        'recv_wall_ns': [100, 200],
    })

    # When
    summary = decode_and_flag_ticks(df)

    # Then: zero_volume=1 이지만 보존법칙 판정 대상에서는 제외되어 tick_loss는 0
    assert summary is not None
    assert summary.zero_volume == 1
    assert summary.tick_loss == 0


def test_decode_and_flag_ticks_flags_schema_disagree_on_sign_drate_mismatch() -> None:
    import json
    import polars as pl
    from src.storage.quality import decode_and_flag_ticks

    # Given: sign='2'(상승) 인데 drate가 반전되어 -5.00% -> 두 유도식이 어긋남
    body = {'shcode': '005930', 'price': '71000', 'cvolume': '10', 'volume': '100', 'change': '1000', 'sign': '2', 'drate': '-5.00'}
    raw = json.dumps({'header': {'tr_cd': 'S3_', 'tr_key': '005930'}, 'body': body}, ensure_ascii=False)
    df = pl.DataFrame({'raw': [raw], 'tr_id': ['H0STCNT0'], 'recv_wall_ns': [100]})

    # When
    summary = decode_and_flag_ticks(df)

    # Then
    assert summary is not None
    assert summary.decode_fail == 0
    assert summary.schema_disagree == 1


def test_decode_and_flag_ticks_no_schema_disagree_when_consistent() -> None:
    import json
    import polars as pl
    from src.storage.quality import decode_and_flag_ticks

    # Given: price=71000, change=1000, ref=70000 -> drate = 1000/70000*100 ≈ 1.43(sign='2'과 정합)
    body = {'shcode': '005930', 'price': '71000', 'cvolume': '10', 'volume': '100', 'change': '1000', 'sign': '2', 'drate': '1.43'}
    raw = json.dumps({'header': {'tr_cd': 'S3_', 'tr_key': '005930'}, 'body': body}, ensure_ascii=False)
    df = pl.DataFrame({'raw': [raw], 'tr_id': ['H0STCNT0'], 'recv_wall_ns': [100]})

    # When
    summary = decode_and_flag_ticks(df)

    # Then
    assert summary is not None
    assert summary.schema_disagree == 0


def test_decode_and_flag_quotes_returns_none_when_no_quote_rows() -> None:
    import polars as pl
    from src.storage.quality import decode_and_flag_quotes

    # Given: 체결(H0STCNT0) 행만 존재
    df = pl.DataFrame({'raw': ['x'], 'tr_id': ['H0STCNT0'], 'recv_wall_ns': [100]})

    # When
    summary = decode_and_flag_quotes(df)

    # Then
    assert summary is None


def test_decode_and_flag_quotes_clean_quote_not_flagged() -> None:
    import json
    import polars as pl
    from src.storage.quality import decode_and_flag_quotes

    def _quote_body(hotime, offerho, bidho, offerrem=None, bidrem=None, tot_o=None, tot_b=None):
        offerrem = offerrem or [100] * 10
        bidrem = bidrem or [100] * 10
        b = {'shcode': '005930', 'hotime': str(hotime).zfill(6)}
        for k in range(10):
            b[f'offerho{k + 1}'] = str(offerho[k])
            b[f'bidho{k + 1}'] = str(bidho[k])
            b[f'offerrem{k + 1}'] = str(offerrem[k])
            b[f'bidrem{k + 1}'] = str(bidrem[k])
        b['totofferrem'] = str(tot_o if tot_o is not None else sum(offerrem))
        b['totbidrem'] = str(tot_b if tot_b is not None else sum(bidrem))
        return b

    clean_offer = [70100 + k * 100 for k in range(10)]
    clean_bid = [69900 - k * 100 for k in range(10)]

    # Given: 정상 사다리/잔량을 가진 호가 1건 (연속매매 시간대)
    raw = json.dumps({'header': {'tr_cd': 'H1_', 'tr_key': '005930'}, 'body': _quote_body(101500, clean_offer, clean_bid)}, ensure_ascii=False)
    df = pl.DataFrame({'raw': [raw], 'tr_id': ['H0STASP0'], 'recv_wall_ns': [100]})

    # When
    summary = decode_and_flag_quotes(df)

    # Then
    assert summary is not None
    assert summary.rows == 1
    assert summary.decode_fail == 0
    assert summary.ladder_disorder == 0
    assert summary.crossed_book == 0
    assert summary.negative_remain == 0
    assert summary.total_remain_short == 0


def test_decode_and_flag_quotes_flags_ladder_disorder() -> None:
    import json
    import polars as pl
    from src.storage.quality import decode_and_flag_quotes

    def _quote_body(hotime, offerho, bidho, offerrem=None, bidrem=None, tot_o=None, tot_b=None):
        offerrem = offerrem or [100] * 10
        bidrem = bidrem or [100] * 10
        b = {'shcode': '005930', 'hotime': str(hotime).zfill(6)}
        for k in range(10):
            b[f'offerho{k + 1}'] = str(offerho[k])
            b[f'bidho{k + 1}'] = str(bidho[k])
            b[f'offerrem{k + 1}'] = str(offerrem[k])
            b[f'bidrem{k + 1}'] = str(bidrem[k])
        b['totofferrem'] = str(tot_o if tot_o is not None else sum(offerrem))
        b['totbidrem'] = str(tot_b if tot_b is not None else sum(bidrem))
        return b

    clean_offer = [70100 + k * 100 for k in range(10)]
    clean_bid = [69900 - k * 100 for k in range(10)]

    # Given: offerho3, offerho4를 뒤바꿔 오름차순 위반 주입
    bad_offer = list(clean_offer)
    bad_offer[2], bad_offer[3] = bad_offer[3], bad_offer[2]
    raw = json.dumps({'header': {'tr_cd': 'H1_', 'tr_key': '005930'}, 'body': _quote_body(101500, bad_offer, clean_bid)}, ensure_ascii=False)
    df = pl.DataFrame({'raw': [raw], 'tr_id': ['H0STASP0'], 'recv_wall_ns': [100]})

    # When
    summary = decode_and_flag_quotes(df)

    # Then
    assert summary is not None
    assert summary.decode_fail == 0
    assert summary.ladder_disorder == 1


def test_decode_and_flag_quotes_flags_crossed_book_outside_auction() -> None:
    import json
    import polars as pl
    from src.storage.quality import decode_and_flag_quotes

    def _quote_body(hotime, offerho, bidho, offerrem=None, bidrem=None, tot_o=None, tot_b=None):
        offerrem = offerrem or [100] * 10
        bidrem = bidrem or [100] * 10
        b = {'shcode': '005930', 'hotime': str(hotime).zfill(6)}
        for k in range(10):
            b[f'offerho{k + 1}'] = str(offerho[k])
            b[f'bidho{k + 1}'] = str(bidho[k])
            b[f'offerrem{k + 1}'] = str(offerrem[k])
            b[f'bidrem{k + 1}'] = str(bidrem[k])
        b['totofferrem'] = str(tot_o if tot_o is not None else sum(offerrem))
        b['totbidrem'] = str(tot_b if tot_b is not None else sum(bidrem))
        return b

    clean_offer = [70100 + k * 100 for k in range(10)]
    clean_bid = [69900 - k * 100 for k in range(10)]

    # Given: 연속매매 시간대(10:15:00)에 offerho1을 bidho1보다 낮게 만들어 크로스북 주입
    crossed_offer = [69800, *clean_offer[1:]]
    raw = json.dumps({'header': {'tr_cd': 'H1_', 'tr_key': '005930'}, 'body': _quote_body(101500, crossed_offer, clean_bid)}, ensure_ascii=False)
    df = pl.DataFrame({'raw': [raw], 'tr_id': ['H0STASP0'], 'recv_wall_ns': [100]})

    # When
    summary = decode_and_flag_quotes(df)

    # Then
    assert summary is not None
    assert summary.crossed_book == 1


def test_decode_and_flag_quotes_ignores_crossed_book_during_auction_window() -> None:
    import json
    import polars as pl
    from src.storage.quality import decode_and_flag_quotes

    def _quote_body(hotime, offerho, bidho, offerrem=None, bidrem=None, tot_o=None, tot_b=None):
        offerrem = offerrem or [100] * 10
        bidrem = bidrem or [100] * 10
        b = {'shcode': '005930', 'hotime': str(hotime).zfill(6)}
        for k in range(10):
            b[f'offerho{k + 1}'] = str(offerho[k])
            b[f'bidho{k + 1}'] = str(bidho[k])
            b[f'offerrem{k + 1}'] = str(offerrem[k])
            b[f'bidrem{k + 1}'] = str(bidrem[k])
        b['totofferrem'] = str(tot_o if tot_o is not None else sum(offerrem))
        b['totbidrem'] = str(tot_b if tot_b is not None else sum(bidrem))
        return b

    clean_offer = [70100 + k * 100 for k in range(10)]
    clean_bid = [69900 - k * 100 for k in range(10)]

    # Given: 동시호가 시간대(08:45:00, _AUCTION_WINDOWS 첫 구간)에 동일한 크로스북 패턴
    crossed_offer = [69800, *clean_offer[1:]]
    raw = json.dumps({'header': {'tr_cd': 'H1_', 'tr_key': '005930'}, 'body': _quote_body(84500, crossed_offer, clean_bid)}, ensure_ascii=False)
    df = pl.DataFrame({'raw': [raw], 'tr_id': ['H0STASP0'], 'recv_wall_ns': [100]})

    # When
    summary = decode_and_flag_quotes(df)

    # Then: 동시호가 구간은 체결 유예로 호가 역전이 정상 -> 오탐 없음
    assert summary is not None
    assert summary.crossed_book == 0


def test_decode_and_flag_quotes_flags_negative_remain() -> None:
    import json
    import polars as pl
    from src.storage.quality import decode_and_flag_quotes

    def _quote_body(hotime, offerho, bidho, offerrem=None, bidrem=None, tot_o=None, tot_b=None):
        offerrem = offerrem or [100] * 10
        bidrem = bidrem or [100] * 10
        b = {'shcode': '005930', 'hotime': str(hotime).zfill(6)}
        for k in range(10):
            b[f'offerho{k + 1}'] = str(offerho[k])
            b[f'bidho{k + 1}'] = str(bidho[k])
            b[f'offerrem{k + 1}'] = str(offerrem[k])
            b[f'bidrem{k + 1}'] = str(bidrem[k])
        b['totofferrem'] = str(tot_o if tot_o is not None else sum(offerrem))
        b['totbidrem'] = str(tot_b if tot_b is not None else sum(bidrem))
        return b

    clean_offer = [70100 + k * 100 for k in range(10)]
    clean_bid = [69900 - k * 100 for k in range(10)]

    # Given: bidrem2를 음수로 손상
    bad_bidrem = [100] * 10
    bad_bidrem[1] = -5
    raw = json.dumps({'header': {'tr_cd': 'H1_', 'tr_key': '005930'}, 'body': _quote_body(101500, clean_offer, clean_bid, bidrem=bad_bidrem)}, ensure_ascii=False)
    df = pl.DataFrame({'raw': [raw], 'tr_id': ['H0STASP0'], 'recv_wall_ns': [100]})

    # When
    summary = decode_and_flag_quotes(df)

    # Then
    assert summary is not None
    assert summary.negative_remain == 1


def test_decode_and_flag_quotes_flags_total_remain_short() -> None:
    import json
    import polars as pl
    from src.storage.quality import decode_and_flag_quotes

    def _quote_body(hotime, offerho, bidho, offerrem=None, bidrem=None, tot_o=None, tot_b=None):
        offerrem = offerrem or [100] * 10
        bidrem = bidrem or [100] * 10
        b = {'shcode': '005930', 'hotime': str(hotime).zfill(6)}
        for k in range(10):
            b[f'offerho{k + 1}'] = str(offerho[k])
            b[f'bidho{k + 1}'] = str(bidho[k])
            b[f'offerrem{k + 1}'] = str(offerrem[k])
            b[f'bidrem{k + 1}'] = str(bidrem[k])
        b['totofferrem'] = str(tot_o if tot_o is not None else sum(offerrem))
        b['totbidrem'] = str(tot_b if tot_b is not None else sum(bidrem))
        return b

    clean_offer = [70100 + k * 100 for k in range(10)]
    clean_bid = [69900 - k * 100 for k in range(10)]

    # Given: totofferrem을 상위10 offerrem 합(1000)보다 작은 50으로 손상
    raw = json.dumps({'header': {'tr_cd': 'H1_', 'tr_key': '005930'}, 'body': _quote_body(101500, clean_offer, clean_bid, tot_o=50)}, ensure_ascii=False)
    df = pl.DataFrame({'raw': [raw], 'tr_id': ['H0STASP0'], 'recv_wall_ns': [100]})

    # When
    summary = decode_and_flag_quotes(df)

    # Then
    assert summary is not None
    assert summary.total_remain_short == 1


def test_decode_and_flag_quotes_chunks_and_aggregates_across_chunk_boundary() -> None:
    import json
    import polars as pl
    from src.storage.quality import decode_and_flag_quotes

    def _quote_body(hotime, offerho, bidho, offerrem=None, bidrem=None, tot_o=None, tot_b=None):
        offerrem = offerrem or [100] * 10
        bidrem = bidrem or [100] * 10
        b = {'shcode': '005930', 'hotime': str(hotime).zfill(6)}
        for k in range(10):
            b[f'offerho{k + 1}'] = str(offerho[k])
            b[f'bidho{k + 1}'] = str(bidho[k])
            b[f'offerrem{k + 1}'] = str(offerrem[k])
            b[f'bidrem{k + 1}'] = str(bidrem[k])
        b['totofferrem'] = str(tot_o if tot_o is not None else sum(offerrem))
        b['totbidrem'] = str(tot_b if tot_b is not None else sum(bidrem))
        return b

    clean_offer = [70100 + k * 100 for k in range(10)]
    clean_bid = [69900 - k * 100 for k in range(10)]

    # Given: chunk_rows=2, 5행 -> 3개 청크(2,2,1)로 나뉘고 3번째 행(2번째 청크)에 사다리역전 주입
    bad_offer = list(clean_offer)
    bad_offer[2], bad_offer[3] = bad_offer[3], bad_offer[2]
    rows = []
    for i in range(5):
        offer = bad_offer if i == 2 else clean_offer
        rows.append(json.dumps({'header': {'tr_cd': 'H1_', 'tr_key': f'{i:06d}'}, 'body': _quote_body(101500, offer, clean_bid)}, ensure_ascii=False))
    df = pl.DataFrame({'raw': rows, 'tr_id': ['H0STASP0'] * 5, 'recv_wall_ns': list(range(5))})

    # When
    summary = decode_and_flag_quotes(df, chunk_rows=2)

    # Then: 청크 경계와 무관하게 전체 5행, 사다리역전 1건만 정확히 집계
    assert summary is not None
    assert summary.rows == 5
    assert summary.ladder_disorder == 1


def test_decode_and_flag_quotes_fallback_batch_survives_non_numeric_body_field() -> None:
    import json
    import polars as pl
    from src.storage.quality import decode_and_flag_quotes

    def _quote_body(hotime, offerho, bidho, offerrem=None, bidrem=None, tot_o=None, tot_b=None):
        offerrem = offerrem or [100] * 10
        bidrem = bidrem or [100] * 10
        b = {'shcode': '005930', 'hotime': str(hotime).zfill(6)}
        for k in range(10):
            b[f'offerho{k + 1}'] = str(offerho[k])
            b[f'bidho{k + 1}'] = str(bidho[k])
            b[f'offerrem{k + 1}'] = str(offerrem[k])
            b[f'bidrem{k + 1}'] = str(bidrem[k])
        b['totofferrem'] = str(tot_o if tot_o is not None else sum(offerrem))
        b['totbidrem'] = str(tot_b if tot_b is not None else sum(bidrem))
        return b

    clean_offer = [70100 + k * 100 for k in range(10)]
    clean_bid = [69900 - k * 100 for k in range(10)]

    # Given: 배치 내 한 행이 비-JSON이라 폴백 경로로 전환되고,
    # 다른 행은 JSON은 유효하나 hotime이 숫자로 캐스팅 불가한 'N/A'
    bad_body = _quote_body(101500, clean_offer, clean_bid)
    bad_body['hotime'] = 'N/A'
    raw_ok_json_bad_number = json.dumps({'header': {'tr_cd': 'H1_', 'tr_key': '005930'}, 'body': bad_body}, ensure_ascii=False)
    df = pl.DataFrame({
        'raw': ['not-json', raw_ok_json_bad_number],
        'tr_id': ['H0STASP0', 'H0STASP0'],
        'recv_wall_ns': [100, 200],
    })

    # When: 예외 없이 두 행 모두 decode_fail로 집계되어야 한다
    summary = decode_and_flag_quotes(df)

    # Then
    assert summary is not None
    assert summary.rows == 2
    assert summary.decode_fail == 2
