
def test_rate_limiter_spaces_requests_by_interval() -> None:
    # Given: 2 req/s (모노토닉 시계/수면 주입)
    from src.execution.kis_client import RateLimiter

    now = [0.0]
    slept: list[float] = []

    def fake_sleep(seconds: float) -> None:
        slept.append(seconds)
        now[0] += seconds

    limiter = RateLimiter(2.0, clock=lambda: now[0], sleep=fake_sleep)

    # When
    limiter.acquire()
    limiter.acquire()
    limiter.acquire()

    # Then: 0.5초 간격 강제
    assert slept == [0.5, 0.5]

    # When: 충분히 유휴한 뒤
    now[0] += 10.0
    limiter.acquire()

    # Then: 추가 대기 없음
    assert slept == [0.5, 0.5]

def test_access_token_is_cached_and_reissued_only_when_near_expiry(tmp_path) -> None:
    # Given
    import json
    import stat

    import pytest

    from src.execution.contracts import KisApiError
    from src.execution.kis_client import KisRestClient, RateLimiter
    from tests.unit.execution.fakes import T0, FakeResponse, FakeSession, FixedClock, make_creds, token_response

    clock = FixedClock(T0)
    session = FakeSession([
        token_response("2026-09-12 09:00:00", token="tok-1"),
        token_response("2026-09-13 08:55:00", token="tok-2"),
    ])
    cache = tmp_path / "execution" / "kis_token.json"
    limiter = RateLimiter(1000.0, clock=lambda: 0.0, sleep=lambda seconds: None)
    client = KisRestClient(
        creds=make_creds(), session=session, token_cache_path=cache, limiter=limiter, now=clock, timeout_s=5.0
    )

    # When / Then: 최초 1회 발급 후 재사용
    assert client.access_token() == "tok-1"
    assert client.access_token() == "tok-1"
    assert len(session.calls) == 1
    assert session.calls[0]["url"] == "https://openapi.koreainvestment.com:9443/oauth2/tokenP"
    assert session.calls[0]["json"] == {"grant_type": "client_credentials", "appkey": "app-key", "appsecret": "app-secret"}

    # Then: 캐시 파일 형식 + 소유자 전용 권한
    assert json.loads(cache.read_text(encoding="utf-8")) == {
        "access_token": "tok-1", "expires_at": "2026-09-12T09:00:00+09:00",
    }
    assert stat.S_IMODE(cache.stat().st_mode) == 0o600

    # Then: 새 인스턴스도 파일 캐시를 재사용 (발급 1분 1회 제한 회피)
    other = KisRestClient(
        creds=make_creds(), session=session, token_cache_path=cache, limiter=limiter, now=clock, timeout_s=5.0
    )
    assert other.access_token() == "tok-1"
    assert len(session.calls) == 1

    # When: 만료 10분 이내 진입
    clock.advance(86_400 - 5 * 60)

    # Then: 재발급
    assert client.access_token() == "tok-2"
    assert len(session.calls) == 2

    # Then: 발급 거부(EGW00133)는 타입드 예외
    failing = KisRestClient(
        creds=make_creds(),
        session=FakeSession([FakeResponse({"error_code": "EGW00133", "error_description": "1분당 1회"}, status=403)]),
        token_cache_path=tmp_path / "other" / "kis_token.json",
        limiter=limiter, now=clock, timeout_s=5.0,
    )
    with pytest.raises(KisApiError) as excinfo:
        failing.access_token()
    assert excinfo.value.msg_cd == "EGW00133"

def test_get_quote_parses_price_and_ten_level_book(tmp_path) -> None:
    # Given: 실측 응답 형태(삼성전자 2026-09-11 종가 기준)
    from tests.unit.execution.fakes import asking_body, make_client, price_body

    client, session, _ = make_client(tmp_path, [
        price_body(last=259_500, upper=349_500, lower=188_500, tick=500),
        asking_body(asks=[(260_000, 101_111), (260_500, 10)], bids=[(259_500, 31_936)]),
    ])

    # When
    quote = client.get_quote("005930")

    # Then
    assert quote.symbol == "005930"
    assert quote.last == 259_500
    assert quote.upper_limit == 349_500
    assert quote.lower_limit == 188_500
    assert quote.tick == 500
    assert quote.halted is False
    assert quote.asks == ((260_000, 101_111), (260_500, 10))
    assert quote.bids == ((259_500, 31_936),)
    assert [c["headers"]["tr_id"] for c in session.calls] == ["FHKST01010100", "FHKST01010200"]
    assert session.calls[0]["url"].endswith("/uapi/domestic-stock/v1/quotations/inquire-price")
    assert session.calls[1]["url"].endswith("/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn")
    assert session.calls[0]["params"] == {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": "005930"}
    assert session.calls[0]["headers"]["authorization"] == "Bearer tok-1"
    assert session.calls[0]["headers"]["custtype"] == "P"
    assert session.calls[0]["timeout"] == 5.0

def test_reads_refresh_token_once_retry_rate_limit_and_raise_on_error(tmp_path) -> None:
    # Given
    import pytest
    import requests

    from src.execution.contracts import KisApiError
    from tests.unit.execution.fakes import FakeResponse, asking_body, make_client, price_body, token_response

    expired = FakeResponse({"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "기간이 만료된 token 입니다."}, status=500)
    rate = FakeResponse({"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다."}, status=500)
    book = asking_body(asks=[(10_010, 5)], bids=[(10_000, 5)])

    # When / Then: 만료 → 재발급 → 재요청
    client, session, _ = make_client(tmp_path / "a", [expired, token_response(token="tok-2"), price_body(), book])
    assert client.get_quote("005930").last == 10_000
    assert session.calls[1]["url"].endswith("/oauth2/tokenP")
    assert session.calls[2]["headers"]["authorization"] == "Bearer tok-2"

    # When / Then: 레이트 거절은 재시도
    client, session, _ = make_client(tmp_path / "b", [rate, price_body(), book])
    assert client.get_quote("005930").last == 10_000
    assert len(session.calls) == 3

    # When / Then: 업무 오류는 msg_cd 보존 예외
    client, _, _ = make_client(tmp_path / "c", [FakeResponse({"rt_cd": "1", "msg_cd": "APBK1234", "msg1": "조회 오류 "})])
    with pytest.raises(KisApiError) as excinfo:
        client.get_quote("005930")
    assert excinfo.value.msg_cd == "APBK1234"

    # When / Then: 전송 오류/비JSON 응답은 TRANSPORT
    client, _, _ = make_client(tmp_path / "d", [requests.ConnectionError("down")])
    with pytest.raises(KisApiError) as excinfo:
        client.get_quote("005930")
    assert excinfo.value.msg_cd == "TRANSPORT"
    client, _, _ = make_client(tmp_path / "e", [FakeResponse(None, status=502, raw_text=True)])
    with pytest.raises(KisApiError) as excinfo:
        client.get_quote("005930")
    assert excinfo.value.msg_cd == "TRANSPORT"

def test_get_daily_orders_follows_pagination_and_parses_rows(tmp_path) -> None:
    # Given: 2페이지 응답 (원주문 + 취소 자식주문)
    import datetime as dt

    import pytest

    from src.execution.contracts import KisApiError, Side
    from tests.unit.execution.fakes import FakeResponse, make_client

    row1 = {
        "odno": "0000000001", "orgn_odno": "", "ord_gno_brno": "91252", "pdno": "005930", "sll_buy_dvsn_cd": "02",
        "ord_qty": "10", "ord_unpr": "10000", "tot_ccld_qty": "4", "tot_ccld_amt": "40000", "rmn_qty": "6",
        "rjct_qty": "0", "cncl_yn": "N", "ord_tmd": "090001",
    }
    row2 = {
        "odno": "0000000002", "orgn_odno": "0000000001", "ord_gno_brno": "91252", "pdno": "005930",
        "sll_buy_dvsn_cd": "02", "ord_qty": "6", "ord_unpr": "0.00", "tot_ccld_qty": "0", "tot_ccld_amt": "0",
        "rmn_qty": "0", "rjct_qty": "0", "cncl_yn": "Y", "ord_tmd": "090210",
    }
    page1 = FakeResponse(
        {"rt_cd": "0", "msg_cd": "KIOK0460", "msg1": "ok", "output1": [row1], "ctx_area_fk100": "FK1", "ctx_area_nk100": "NK1"},
        headers={"tr_cont": "M"},
    )
    page2 = FakeResponse(
        {"rt_cd": "0", "msg_cd": "KIOK0460", "msg1": "ok", "output1": [row2], "ctx_area_fk100": "FK2", "ctx_area_nk100": "NK2"},
        headers={"tr_cont": "D"},
    )
    client, session, _ = make_client(tmp_path / "a", [page1, page2])

    # When
    rows = client.get_daily_orders(dt.date(2026, 9, 11))

    # Then: 행 파싱
    assert [r.broker_order_no for r in rows] == ["0000000001", "0000000002"]
    first = rows[0]
    assert first.side is Side.BUY
    assert (first.ordered_qty, first.price, first.filled_qty, first.filled_amount_krw) == (10, 10_000, 4, 40_000)
    assert (first.remaining_qty, first.rejected_qty, first.cancelled) == (6, 0, False)
    assert (first.org_no, first.original_order_no, first.order_time) == ("91252", "", "090001")
    assert rows[1].cancelled is True
    assert rows[1].original_order_no == "0000000001"
    assert rows[1].price == 0

    # Then: 연속조회 헤더/키
    c0, c1 = session.calls
    assert c0["headers"]["tr_id"] == "TTTC0081R"
    assert c0["headers"]["tr_cont"] == ""
    assert c0["params"]["INQR_STRT_DT"] == "20260911"
    assert c0["params"]["INQR_END_DT"] == "20260911"
    assert c0["params"]["CANO"] == "12345678"
    assert c0["params"]["ACNT_PRDT_CD"] == "01"
    assert c0["params"]["CTX_AREA_NK100"] == ""
    assert c1["headers"]["tr_cont"] == "N"
    assert c1["params"]["CTX_AREA_FK100"] == "FK1"
    assert c1["params"]["CTX_AREA_NK100"] == "NK1"

    # Then: 무한 연속조회는 상한에서 fail-closed
    client, _, _ = make_client(tmp_path / "b", [page1] * 100)
    with pytest.raises(KisApiError) as excinfo:
        client.get_daily_orders(dt.date(2026, 9, 11))
    assert excinfo.value.msg_cd == "PAGINATION"

def test_get_holdings_and_orderable_cash_parse_account_trs(tmp_path) -> None:
    # Given
    from src.execution.contracts import OrderType
    from tests.unit.execution.fakes import FakeResponse, balance_body, make_client

    client, session, _ = make_client(tmp_path, [
        balance_body([
            {"pdno": "005930", "hldg_qty": "3", "pchs_amt": "750000"},
            {"pdno": "000660", "hldg_qty": "0", "pchs_amt": "0"},
        ]),
        FakeResponse({
            "rt_cd": "0", "msg_cd": "KIOK0000", "msg1": "ok",
            "output": {"ord_psbl_cash": "900000", "nrcv_buy_amt": "850000", "max_buy_amt": "5"},
        }),
    ])

    # When / Then: 잔고
    holdings = client.get_holdings()
    assert [(h.symbol, h.qty, h.cost_krw) for h in holdings] == [("005930", 3, 750_000)]
    assert session.calls[0]["headers"]["tr_id"] == "TTTC8434R"
    assert session.calls[0]["params"] == {
        "CANO": "12345678", "ACNT_PRDT_CD": "01", "AFHR_FLPR_YN": "N", "OFL_YN": "", "INQR_DVSN": "02",
        "UNPR_DVSN": "01", "FUND_STTL_ICLD_YN": "N", "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "00",
        "CTX_AREA_FK100": "", "CTX_AREA_NK100": "",
    }

    # When / Then: 매수가능금액 (미수 없는 매수금액)
    assert client.get_orderable_cash("005930", 259_500, OrderType.LIMIT) == 850_000
    assert session.calls[1]["headers"]["tr_id"] == "TTTC8408R"
    assert session.calls[1]["params"] == {
        "CANO": "12345678", "ACNT_PRDT_CD": "01", "PDNO": "005930", "ORD_UNPR": "259500", "ORD_DVSN": "00",
        "CMA_EVLU_AMT_ICLD_YN": "N", "OVRS_ICLD_YN": "N",
    }

def test_post_order_classifies_accepted_rejected_unknown(tmp_path) -> None:
    # Given
    import requests

    from src.execution.contracts import OutcomeKind
    from src.execution.kis_client import TR_BUY
    from tests.unit.execution.fakes import FakeResponse, accepted_body, make_client

    body = {"PDNO": "005930"}

    # When / Then: 접수
    client, session, _ = make_client(tmp_path / "a", [accepted_body()])
    out = client.post_order(TR_BUY, body)
    assert out.kind is OutcomeKind.ACCEPTED
    assert (out.broker_order_no, out.broker_org_no, out.order_time) == ("0000117057", "91252", "090001")
    assert session.calls[0]["url"] == "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/order-cash"
    assert session.calls[0]["json"] == body
    assert session.calls[0]["headers"]["tr_id"] == "TTTC0012U"

    # When / Then: 브로커 거부는 HTTP 코드와 무관하게 rt_cd 로 판정
    client, _, _ = make_client(
        tmp_path / "b",
        [FakeResponse({"rt_cd": "1", "msg_cd": "APBK0919", "msg1": "주문가능금액을 초과 했습니다 "}, status=500)],
    )
    out = client.post_order(TR_BUY, body)
    assert out.kind is OutcomeKind.REJECTED
    assert out.code == "APBK0919"
    assert out.message == "주문가능금액을 초과 했습니다"

    # When / Then: 전송 여부가 불확실하면 재시도 없이 UNKNOWN (중복주문 방지)
    ambiguous = [
        (requests.ReadTimeout("read"), "transport", "a1"),
        (requests.ConnectionError("reset"), "transport", "a2"),
        (FakeResponse(None, status=502, raw_text=True), "http_502", "a3"),
        (FakeResponse({"rt_cd": "0", "msg_cd": "APBK0013", "msg1": "ok", "output": {}}), "no_odno", "a4"),
    ]
    for item, code, sub in ambiguous:
        client, session, _ = make_client(tmp_path / sub, [item])
        out = client.post_order(TR_BUY, body)
        assert out.kind is OutcomeKind.UNKNOWN
        assert out.code == code
        assert len(session.calls) == 1

def test_post_order_retries_only_when_definitely_not_sent(tmp_path) -> None:
    # Given
    import requests

    from src.execution.contracts import OutcomeKind
    from src.execution.kis_client import TR_CANCEL, TR_SELL
    from tests.unit.execution.fakes import FakeResponse, accepted_body, make_client, token_response

    # When / Then: 연결 수립 전 타임아웃은 미전송 확정 → 재시도
    client, session, _ = make_client(tmp_path / "a", [requests.ConnectTimeout("connect"), accepted_body()])
    assert client.post_order(TR_SELL, {"PDNO": "005930"}).kind is OutcomeKind.ACCEPTED
    assert len(session.calls) == 2

    # When / Then: 레이트 거절은 브로커 미처리 → 재시도
    rate = FakeResponse({"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다."}, status=500)
    client, session, _ = make_client(tmp_path / "b", [rate, accepted_body()])
    assert client.post_order(TR_SELL, {}).kind is OutcomeKind.ACCEPTED
    assert len(session.calls) == 2

    # When / Then: 만료 토큰은 1회 재발급 후 재전송 (취소는 정정취소 경로)
    expired = FakeResponse({"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "expired"}, status=500)
    client, session, _ = make_client(tmp_path / "c", [expired, token_response(token="tok-2"), accepted_body()])
    assert client.post_order(TR_CANCEL, {}).kind is OutcomeKind.ACCEPTED
    assert session.calls[0]["url"].endswith("/uapi/domestic-stock/v1/trading/order-rvsecncl")
    assert session.calls[2]["headers"]["authorization"] == "Bearer tok-2"
    assert session.calls[2]["headers"]["tr_id"] == "TTTC0013U"

    # When / Then: 안전 재시도 소진은 미전송 확정이므로 REJECTED
    client, session, _ = make_client(tmp_path / "d", [requests.ConnectTimeout("c")] * 3)
    out = client.post_order(TR_SELL, {})
    assert out.kind is OutcomeKind.REJECTED
    assert out.code == "connect_timeout"
    assert len(session.calls) == 3

def test_build_order_body_matches_official_fields() -> None:
    # Given
    import pytest

    from src.execution.contracts import ExecutionError, Order, OrderIntent, OrderStatus, OrderType, Side
    from src.execution.kis_client import TR_BUY, TR_SELL, build_cancel_body, build_order_body, order_tr_id
    from tests.unit.execution.fakes import T0, make_creds

    creds = make_creds()
    buy = Order(
        client_id="c1", intent=OrderIntent("005930", Side.BUY, 3, OrderType.LIMIT, 259_500),
        status=OrderStatus.PENDING_SUBMIT, created_at=T0,
    )
    sell = Order(
        client_id="c2", intent=OrderIntent("005930", Side.SELL, 2, OrderType.MARKET),
        status=OrderStatus.PENDING_SUBMIT, created_at=T0,
    )

    # When / Then: 지정가 매수
    assert build_order_body(creds, buy) == {
        "CANO": "12345678", "ACNT_PRDT_CD": "01", "PDNO": "005930", "ORD_DVSN": "00", "ORD_QTY": "3",
        "ORD_UNPR": "259500", "EXCG_ID_DVSN_CD": "KRX", "SLL_TYPE": "", "CNDT_PRIC": "",
    }

    # Then: 시장가 매도
    body = build_order_body(creds, sell)
    assert (body["ORD_DVSN"], body["ORD_UNPR"], body["SLL_TYPE"]) == ("01", "0", "01")
    assert order_tr_id(Side.BUY) == TR_BUY == "TTTC0012U"
    assert order_tr_id(Side.SELL) == TR_SELL == "TTTC0011U"

    # Then: 취소는 원주문번호/조직번호 필수
    with pytest.raises(ExecutionError):
        build_cancel_body(creds, buy)
    buy.broker_order_no = "0000117057"
    buy.broker_org_no = "91252"
    assert build_cancel_body(creds, buy) == {
        "CANO": "12345678", "ACNT_PRDT_CD": "01", "KRX_FWDG_ORD_ORGNO": "91252", "ORGN_ODNO": "0000117057",
        "ORD_DVSN": "00", "RVSE_CNCL_DVSN_CD": "02", "ORD_QTY": "0", "ORD_UNPR": "0", "QTY_ALL_ORD_YN": "Y",
        "EXCG_ID_DVSN_CD": "KRX",
    }


def test_get_daily_bar_parses_valid_response(tmp_path) -> None:
    import datetime as dt

    from tests.unit.execution.fakes import daily_chart_body, make_client

    client, session, _ = make_client(
        tmp_path,
        [daily_chart_body(date="20260910", close="269000", volume="22517075", trade_value="6028310398811", prdy_vrss="-500")],
    )

    row = client.get_daily_bar("005930", dt.date(2026, 9, 10))

    # Then: 실측(broker_kis.md 3.3, 005930 실계정 검증)과 동일한 소문자 파라미터 + 시작/종료일 동일값을 그대로 보낸다
    assert session.calls[-1]["params"] == {
        "fid_cond_mrkt_div_code": "J",
        "fid_input_iscd": "005930",
        "fid_input_date_1": "20260910",
        "fid_input_date_2": "20260910",
        "fid_period_div_code": "D",
        "fid_org_adj_prc": "1",
    }
    assert row is not None
    assert row["stck_clpr"] == "269000"
    assert row["acml_vol"] == "22517075"
    assert row["acml_tr_pbmn"] == "6028310398811"
    assert row["prdy_vrss"] == "-500"

def test_get_daily_bar_returns_none_when_output2_empty(tmp_path) -> None:
    import datetime as dt

    from tests.unit.execution.fakes import FakeResponse, make_client

    empty = FakeResponse({"rt_cd": "0", "msg_cd": "MCA00000", "msg1": "ok", "output1": {}, "output2": []})
    client, session, _ = make_client(tmp_path, [empty])

    row = client.get_daily_bar("999999", dt.date(2026, 9, 10))

    assert row is None

def test_get_daily_bar_returns_none_on_zero_close_price(tmp_path) -> None:
    import datetime as dt

    from tests.unit.execution.fakes import daily_chart_body, make_client

    client, session, _ = make_client(
        tmp_path,
        [daily_chart_body(date="20260910", close="0", volume="0", trade_value="0")],
    )

    row = client.get_daily_bar("999999", dt.date(2026, 9, 10))

    assert row is None

def test_get_daily_bar_keeps_zero_volume_when_price_valid(tmp_path) -> None:
    import datetime as dt

    from tests.unit.execution.fakes import daily_chart_body, make_client

    client, session, _ = make_client(
        tmp_path,
        [daily_chart_body(date="20260910", close="11700", volume="0", trade_value="0", prdy_vrss="0")],
    )

    row = client.get_daily_bar("0011A0", dt.date(2026, 9, 10))

    assert row is not None
    assert row["stck_clpr"] == "11700"
    assert row["acml_vol"] == "0"
