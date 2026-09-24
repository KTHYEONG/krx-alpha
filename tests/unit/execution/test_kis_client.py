
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
        "access_token": "tok-1", "expired_at": "2026-09-12T09:00:00+09:00",
        "app_key": "app-key", "issued_at": "2026-09-11T09:00:00+09:00",
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


def test_access_token_rechecks_cache_after_cross_process_lock(monkeypatch, tmp_path) -> None:
    import datetime as dt

    from src.execution.kis_client import KisRestClient, RateLimiter
    from tests.unit.execution.fakes import FixedClock, make_creds

    client = KisRestClient(
        creds=make_creds(), session=object(), token_cache_path=tmp_path / "token.json",
        limiter=RateLimiter(1000.0, sleep=lambda _: None), now=lambda: FixedClock(dt.datetime(2026, 9, 15, 9, tzinfo=dt.UTC))(), timeout_s=1.0,
    )
    results = iter([None, ("cached-after-lock", dt.datetime(2026, 9, 16, 9, tzinfo=dt.UTC))])
    monkeypatch.setattr(client, "_read_valid_token", lambda _now: next(results))
    assert client.access_token() == "cached-after-lock"

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
    client, session, clock = make_client(tmp_path / "a", [expired, token_response(token="tok-2"), price_body(), book])
    assert client.access_token() == "tok-1"
    clock.advance(15 * 3_600)
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
    client, session, clock = make_client(tmp_path / "c", [expired, token_response(token="tok-2"), accepted_body()])
    assert client.access_token() == "tok-1"
    clock.advance(15 * 3_600)
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


def test_kis_rest_client_readonly_cache_and_same_day_guard(tmp_path) -> None:
    import datetime as dt
    import pytest
    from zoneinfo import ZoneInfo
    from src.core.config import KisCredentials
    from src.execution.contracts import KisApiError
    from src.execution.kis_client import KisRestClient, RateLimiter
    class Session:
        def post(self, *args, **kwargs): raise AssertionError('must not issue token')
    client=KisRestClient(creds=KisCredentials(kis_app_key='key',kis_app_secret='secret',kis_account_no='12345678',kis_account_product_code='01'),session=Session(),token_cache_path=tmp_path/'missing.json',limiter=RateLimiter(1.0,sleep=lambda _:None),now=lambda:dt.datetime(2026,9,15,9,tzinfo=ZoneInfo('Asia/Seoul')),timeout_s=1.0,allow_token_issue=False)
    with pytest.raises(KisApiError,match='TOKEN_CACHE'):
        client.access_token()


def test_kis_rest_client_rejects_invalid_cache_and_same_day_reissue_without_http(tmp_path) -> None:
    import datetime as dt
    import json

    import pytest
    from zoneinfo import ZoneInfo

    from src.core.config import KisCredentials
    from src.execution.contracts import KisApiError
    from src.execution.kis_client import KisRestClient, RateLimiter

    now = dt.datetime(2026, 9, 15, 9, tzinfo=ZoneInfo("Asia/Seoul"))

    def client_for(cache, **kwargs):
        class Session:
            def post(self, *args, **kwargs):
                raise AssertionError("must not issue token")

        return KisRestClient(
            creds=KisCredentials(kis_app_key="key", kis_app_secret="secret", kis_account_no="12345678", kis_account_product_code="01"),
            session=Session(), token_cache_path=cache, limiter=RateLimiter(1000.0, sleep=lambda _: None),
            now=lambda: now, timeout_s=1.0, **kwargs,
        )

    wrong = tmp_path / "wrong.json"
    wrong.write_text(json.dumps({"access_token": "tok", "expired_at": "2026-09-16T09:00:00+09:00", "app_key": "other", "issued_at": "2026-09-15T08:00:00+09:00"}), encoding="utf-8")
    with pytest.raises(KisApiError, match="TOKEN_CACHE"):
        client_for(wrong, allow_token_issue=False).access_token()

    empty = tmp_path / "empty.json"
    empty.write_text(json.dumps({"access_token": "", "expired_at": "2026-09-16T09:00:00+09:00", "app_key": "key", "issued_at": "2026-09-15T08:00:00+09:00"}), encoding="utf-8")
    with pytest.raises(KisApiError, match="TOKEN_CACHE"):
        client_for(empty, allow_token_issue=False).access_token()

    issued_today = tmp_path / "today.json"
    issued_today.write_text(json.dumps({"access_token": "old", "expired_at": "2026-09-15T08:00:00+09:00", "app_key": "key", "issued_at": "2026-09-15T07:00:00+09:00"}), encoding="utf-8")
    with pytest.raises(KisApiError, match="TOKEN_DAILY_LIMIT"):
        client_for(issued_today, allow_token_issue=True).access_token(force=True)


def test_kis_rankings_use_correct_trs_and_reject_schema(tmp_path) -> None:
    import pytest

    from src.execution.contracts import KisApiError
    from tests.unit.execution.fakes import FakeResponse, make_client

    # 거래대금순위 화면(FHPST01710000)의 종목코드 필드는 mksc_shrn_iscd 다(실측
    # 확인: FHPST01720000/"/ranking/trade-amount"는 404를 반환하는 잘못된 TR이었음).
    amount = FakeResponse({'rt_cd': '0', 'msg_cd': '0', 'msg1': 'ok', 'output': [{'mksc_shrn_iscd': '005930', 'prdy_ctrt': '1.25', 'acml_tr_pbmn': '123456789'}]})
    fluctuation = FakeResponse({'rt_cd': '0', 'msg_cd': '0', 'msg1': 'ok', 'output': [{'stck_shrn_iscd': '000660', 'prdy_ctrt': '29.90', 'acml_tr_pbmn': '987654321'}]})
    client, session, _ = make_client(tmp_path, [amount, fluctuation])

    assert client.get_trade_amount_ranking()[0].symbol == '005930'
    assert client.get_fluctuation_ranking()[0].change_pct == pytest.approx(29.90)
    assert [call['headers']['tr_id'] for call in session.calls] == ['FHPST01710000', 'FHPST01700000']
    assert session.calls[0]['url'].endswith('/uapi/domestic-stock/v1/quotations/volume-rank')
    assert session.calls[0]['params']['FID_BLNG_CLS_CODE'] == '3'
    assert session.calls[0]['params']['FID_TRGT_EXLS_CLS_CODE'] == '0000101100'
    assert session.calls[1]['url'].endswith('/uapi/domestic-stock/v1/ranking/fluctuation')
    assert session.calls[1]['params']['FID_PRC_CLS_CODE'] == '0'
    assert session.calls[1]['params']['FID_INPUT_CNT_1'] == '200'

    bad = FakeResponse({'rt_cd': '0', 'msg_cd': '0', 'msg1': 'ok', 'output': [{'stck_shrn_iscd': 'BAD', 'prdy_ctrt': '1', 'acml_tr_pbmn': '1'}]})
    client, _, _ = make_client(tmp_path / 'bad', [bad])
    with pytest.raises(KisApiError) as excinfo:
        client.get_trade_amount_ranking()
    assert excinfo.value.msg_cd == 'SCHEMA'


def test_ranking_parser_keeps_alphanumeric_new_listing_in_input_order(tmp_path) -> None:
    """신규 상장 alphanumeric 단축코드(예: 0007J0)도 형태가 올바르면 랭킹에 유지하고 순위를 연속 부여한다."""
    from tests.unit.execution.fakes import FakeResponse, make_client

    mixed = FakeResponse({
        'rt_cd': '0',
        'msg_cd': '0',
        'msg1': 'ok',
        'output': [
            {'mksc_shrn_iscd': '005930', 'prdy_ctrt': '1.25', 'acml_tr_pbmn': '100000'},
            {'mksc_shrn_iscd': '0007J0', 'prdy_ctrt': '29.50', 'acml_tr_pbmn': '50000'},
            {'mksc_shrn_iscd': '000660', 'prdy_ctrt': '3.40', 'acml_tr_pbmn': '80000'},
        ],
    })
    client, _, _ = make_client(tmp_path, [mixed])
    ranking = client.get_trade_amount_ranking()
    assert [row.symbol for row in ranking] == ['005930', '0007J0', '000660']
    assert [row.rank for row in ranking] == [1, 2, 3]


def test_ranking_parser_skips_malformed_code_without_voiding_list(tmp_path) -> None:
    from tests.unit.execution.fakes import FakeResponse, make_client

    mixed = FakeResponse({
        'rt_cd': '0',
        'msg_cd': '0',
        'msg1': 'ok',
        'output': [
            {'mksc_shrn_iscd': '00593', 'prdy_ctrt': '1.25', 'acml_tr_pbmn': '100000'},
            {'mksc_shrn_iscd': '005930', 'prdy_ctrt': '1.25', 'acml_tr_pbmn': '100000'},
        ],
    })
    client, _, _ = make_client(tmp_path, [mixed])
    ranking = client.get_trade_amount_ranking()
    assert [row.symbol for row in ranking] == ['005930']
    assert ranking[0].rank == 1


def test_fluctuation_ranking_defaults_missing_trade_value_to_zero(tmp_path) -> None:
    """실측 회귀: 등락률 랭킹은 acml_tr_pbmn 을 반환하지 않는다. 선정 순위는 랭크
    위치로 결정되고 이 값은 메타데이터라 0 기본값이 선정 로직에 영향을 주지 않는다."""
    from tests.unit.execution.fakes import FakeResponse, make_client

    no_trade_value = FakeResponse(
        {'rt_cd': '0', 'msg_cd': '0', 'msg1': 'ok', 'output': [{'stck_shrn_iscd': '042040', 'prdy_ctrt': '10.83'}]}
    )
    client, _, _ = make_client(tmp_path, [no_trade_value])

    row = client.get_fluctuation_ranking()[0]

    assert row.symbol == '042040'
    assert row.trade_value_krw == 0


def _status_output(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        'iscd_stat_cls_code': '51',
        'mang_issu_cls_code': 'Y',
        'mrkt_warn_cls_code': '00',
        'short_over_yn': 'N',
        'invt_caful_yn': 'N',
        'sltr_yn': 'N',
        'temp_stop_yn': 'N',
        'vi_cls_code': '',
        'ovtm_vi_cls_code': '',
        'crdt_able_yn': 'Y',
        'stck_prpr': '70000',
        'stck_sdpr': '69500',
        'stck_mxpr': '90350',
        'stck_llam': '48650',
    }
    base.update(overrides)
    return base


def _ok(body: dict[str, object]) -> object:
    from tests.unit.execution.fakes import FakeResponse

    return FakeResponse({'rt_cd': '0', 'msg_cd': 'MCA00000', 'msg1': 'ok', **body})


def test_get_security_status_maps_flags_and_limits(tmp_path) -> None:
    from src.execution.contracts import KisApiError  # noqa: F401
    from tests.unit.execution.fakes import make_client

    client, session, _ = make_client(tmp_path, [_ok({'output': _status_output()})])

    row = client.get_security_status('005930')

    assert row == {
        'source_tr': 'FHKST01010100',
        'market_div_code': 'J',
        'symbol': '005930',
        'status_code': '51',
        'managed': True,
        'market_warning_code': '00',
        'short_overheated': False,
        'investment_caution': False,
        'liquidation_trading': False,
        'trading_halted': False,
        'vi_code': '',
        'ovtm_vi_cls_code': '',
        'credit_available': True,
        'last_price': 70000,
        'base_price': 69500,
        'upper_limit': 90350,
        'lower_limit': 48650,
    }
    assert session.calls[0]['params'] == {'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': '005930'}


def test_get_security_status_emits_vendor_verbatim_vi_key(tmp_path) -> None:
    from tests.unit.execution.fakes import make_client

    client, _, _ = make_client(tmp_path, [_ok({'output': _status_output(ovtm_vi_cls_code='2')})])

    row = client.get_security_status('005930')

    assert row['ovtm_vi_cls_code'] == '2'
    assert 'overtime_vi_code' not in row


def test_get_security_status_rejects_unknown_flag_value(tmp_path) -> None:
    import pytest

    from src.execution.contracts import KisApiError
    from tests.unit.execution.fakes import make_client

    client, _, _ = make_client(tmp_path, [_ok({'output': _status_output(temp_stop_yn='X')})])

    with pytest.raises(KisApiError) as excinfo:
        client.get_security_status('005930')
    assert excinfo.value.msg_cd == 'SCHEMA'


def _asking_output1(levels: int) -> dict[str, str]:
    out: dict[str, str] = {'aspr_acpt_hour': '085900'}
    for i in range(1, 11):
        fill = str(1000 + i) if i <= levels else ''
        out[f'askp{i}'] = fill
        out[f'askp_rsqn{i}'] = str(10 * i) if i <= levels else ''
        out[f'bidp{i}'] = str(990 - i) if i <= levels else ''
        out[f'bidp_rsqn{i}'] = str(5 * i) if i <= levels else ''
    out['total_askp_rsqn'] = '300'
    out['total_bidp_rsqn'] = '150'
    return out


def _asking_output2() -> dict[str, str]:
    return {
        'antc_mkop_cls_code': '1',
        'antc_cnpr': '70100',
        'antc_vol': '1234',
        'antc_cntg_prdy_ctrt': '1.45',
        'vi_cls_code': '',
        'stck_prpr': '70000',
        'stck_sdpr': '69500',
    }


def test_get_auction_book_keeps_ten_positional_levels(tmp_path) -> None:
    from tests.unit.execution.fakes import make_client

    client, _, _ = make_client(tmp_path, [_ok({'output1': _asking_output1(3), 'output2': _asking_output2()})])

    row = client.get_auction_book('005930')

    assert len(row['ask_prices']) == 10
    assert len(row['ask_sizes']) == 10
    assert len(row['bid_prices']) == 10
    assert len(row['bid_sizes']) == 10
    assert row['ask_prices'][3:] == [0] * 7
    assert row['bid_sizes'][3:] == [0] * 7
    assert all(isinstance(v, int) for v in row['ask_prices'])
    assert row['expected_price'] == 70100
    assert row['expected_change_pct'] == 1.45
    assert 'phase' not in row


def _estimate_row(bucket: str, foreign: str, orgn: str, total: str) -> dict[str, str]:
    return {
        'bsop_hour_gb': bucket,
        'frgn_fake_ntby_qty': foreign,
        'orgn_fake_ntby_qty': orgn,
        'sum_fake_ntby_qty': total,
    }


def test_get_investor_estimate_parses_signed_padded_quantities(tmp_path) -> None:
    from tests.unit.execution.fakes import make_client

    rows = [_estimate_row(str(b), '-00000000000718000', '00000000000010000', '-00000000000708000') for b in (5, 4, 3, 2, 1)]
    client, session, _ = make_client(tmp_path, [_ok({'output2': rows})])

    got = client.get_investor_estimate('005930')

    assert [r['bucket'] for r in got] == [1, 2, 3, 4, 5]
    assert got[0]['foreign_net_qty'] == -718000
    assert got[0]['institution_net_qty'] == 10000
    assert got[0]['total_net_qty'] == -708000
    assert got[0]['market_div_code'] == ''
    assert session.calls[0]['params'] == {'MKSC_SHRN_ISCD': '005930'}


def test_get_investor_estimate_rejects_out_of_range_bucket(tmp_path) -> None:
    import pytest

    from src.execution.contracts import KisApiError
    from tests.unit.execution.fakes import make_client

    client, _, _ = make_client(tmp_path, [_ok({'output2': [_estimate_row('6', '0', '0', '0')]})])

    with pytest.raises(KisApiError) as excinfo:
        client.get_investor_estimate('005930')
    assert excinfo.value.msg_cd == 'SCHEMA'


def test_get_investor_estimate_handles_empty_and_duplicate_buckets(tmp_path) -> None:
    import pytest

    from src.execution.contracts import KisApiError
    from tests.unit.execution.fakes import make_client

    client, _, _ = make_client(tmp_path, [_ok({'output2': []})])
    assert client.get_investor_estimate('005930') == ()

    dup = [_estimate_row('1', '0', '0', '0'), _estimate_row('1', '0', '0', '0')]
    client, _, _ = make_client(tmp_path / 'dup', [_ok({'output2': dup})])
    with pytest.raises(KisApiError) as excinfo:
        client.get_investor_estimate('005930')
    assert excinfo.value.msg_cd == 'SCHEMA'


def _program_row(hour: str, sell: str, buy: str, net: str, sell_v: str, buy_v: str, net_v: str) -> dict[str, str]:
    return {
        'bsop_hour': hour,
        'acml_vol': '5000',
        'whol_smtn_seln_vol': sell,
        'whol_smtn_shnu_vol': buy,
        'whol_smtn_ntby_qty': net,
        'whol_smtn_seln_tr_pbmn': sell_v,
        'whol_smtn_shnu_tr_pbmn': buy_v,
        'whol_smtn_ntby_tr_pbmn': net_v,
    }


def test_get_program_trade_latest_selects_max_time_and_checks_net_identity(tmp_path) -> None:
    import pytest

    from src.execution.contracts import KisApiError
    from tests.unit.execution.fakes import make_client

    rows = [
        _program_row('150000', '100', '150', '50', '1000', '1500', '500'),
        _program_row('093000', '10', '12', '2', '100', '120', '20'),
    ]
    client, _, _ = make_client(tmp_path, [_ok({'output': rows})])

    row = client.get_program_trade_latest('005930')

    assert row is not None
    assert row['trade_time'] == '150000'
    assert row['net_qty'] == 50
    assert row['market_div_code'] == 'J'

    broken = [_program_row('150000', '100', '150', '51', '1000', '1500', '500')]
    client, _, _ = make_client(tmp_path / 'broken', [_ok({'output': broken})])
    with pytest.raises(KisApiError) as excinfo:
        client.get_program_trade_latest('005930')
    assert excinfo.value.msg_cd == 'SCHEMA'

    client, _, _ = make_client(tmp_path / 'empty', [_ok({'output': []})])
    assert client.get_program_trade_latest('005930') is None


def test_get_index_snapshot_maps_breadth_and_turnover(tmp_path) -> None:
    from tests.unit.execution.fakes import make_client

    output = {
        'bstp_nmix_prpr': '822.18',
        'bstp_nmix_prdy_ctrt': '0.87',
        'acml_tr_pbmn': '123456',
        'ascn_issu_cnt': '945',
        'down_issu_cnt': '123',
    }
    client, session, _ = make_client(tmp_path, [_ok({'output': output})])

    row = client.get_index_snapshot('1001')

    assert row['index_value'] == 822.18
    assert row['advancers'] == 945
    assert row['decliners'] == 123
    assert row['cum_value_mil_krw'] == 123456
    assert row['market_div_code'] == 'U'
    assert session.calls[0]['params']['FID_COND_MRKT_DIV_CODE'] == 'U'


def _minute_row(hour: str, price: str = '70000', volume: str = '10', date: str = '20260917') -> dict[str, str]:
    return {
        'stck_bsop_date': date,
        'stck_cntg_hour': hour,
        'stck_oprc': price,
        'stck_hgpr': price,
        'stck_lwpr': price,
        'stck_prpr': price,
        'cntg_vol': volume,
    }


def _minute_hours(start: str, end: str) -> list[str]:
    import datetime as dt

    cur = dt.datetime.strptime(start, '%H%M%S')
    stop = dt.datetime.strptime(end, '%H%M%S')
    out: list[str] = []
    while cur >= stop:
        out.append(cur.strftime('%H%M%S'))
        cur -= dt.timedelta(minutes=1)
    return out


def test_get_stock_minute_bars_walks_cursor_and_filters_session(tmp_path) -> None:
    import datetime as dt

    from tests.unit.execution.fakes import make_client

    page1 = [_minute_row(h, volume='0') if h > '153000' else _minute_row(h) for h in _minute_hours('153500', '150600')]
    page2 = [_minute_row(h) for h in _minute_hours('150500', '143600')]
    client, session, _ = make_client(
        tmp_path, [_ok({'output2': page1}), _ok({'output2': page2}), _ok({'output2': []})]
    )

    bars = client.get_stock_minute_bars(
        '005930', session_date=dt.date(2026, 9, 17), session_open=dt.time(9, 0), session_close=dt.time(15, 30)
    )

    assert [call['params']['FID_INPUT_HOUR_1'] for call in session.calls] == ['153000', '150500', '143500']
    assert session.calls[0]['params']['FID_COND_MRKT_DIV_CODE'] == 'J'
    times = [b['bar_time'] for b in bars]
    assert times == sorted(set(times))
    assert all(t <= '153000' for t in times)
    assert len(bars) == 55
    assert bars[0]['volume'] == 10


def test_get_stock_minute_bars_drops_other_date_and_rejects_broken_bar(tmp_path) -> None:
    import datetime as dt

    import pytest

    from src.execution.contracts import KisApiError
    from tests.unit.execution.fakes import make_client

    page = [_minute_row('150000', date='20260916'), _minute_row('145900')]
    client, _, _ = make_client(tmp_path, [_ok({'output2': page}), _ok({'output2': []})])

    bars = client.get_stock_minute_bars(
        '005930', session_date=dt.date(2026, 9, 17), session_open=dt.time(9, 0), session_close=dt.time(15, 30)
    )

    assert [b['bar_time'] for b in bars] == ['145900']

    broken = dict(_minute_row('145800'))
    broken['stck_hgpr'] = '69000'
    client, _, _ = make_client(tmp_path / 'broken', [_ok({'output2': [broken]}), _ok({'output2': []})])
    with pytest.raises(KisApiError) as excinfo:
        client.get_stock_minute_bars(
            '005930', session_date=dt.date(2026, 9, 17), session_open=dt.time(9, 0), session_close=dt.time(15, 30)
        )
    assert excinfo.value.msg_cd == 'SCHEMA'


def test_get_stock_minute_bars_stops_on_stale_and_open_cursor(tmp_path) -> None:
    import datetime as dt

    import pytest

    from src.execution.contracts import KisApiError
    from tests.unit.execution.fakes import make_client

    dup = [_minute_row('145900')]
    client, session, _ = make_client(tmp_path, [_ok({'output2': dup}), _ok({'output2': dup})])

    bars = client.get_stock_minute_bars(
        '005930', session_date=dt.date(2026, 9, 17), session_open=dt.time(9, 0), session_close=dt.time(15, 30)
    )

    assert [b['bar_time'] for b in bars] == ['145900']
    assert len(session.calls) == 2

    reaching_open = [_minute_row('150000'), _minute_row('145900')]
    client, session, _ = make_client(tmp_path / 'open', [_ok({'output2': reaching_open})])
    bars = client.get_stock_minute_bars(
        '005930', session_date=dt.date(2026, 9, 17), session_open=dt.time(14, 59), session_close=dt.time(15, 30)
    )

    assert [b['bar_time'] for b in bars] == ['145900', '150000']
    assert len(session.calls) == 1

    malformed = [_minute_row('25AB00')]
    client, _, _ = make_client(tmp_path / 'malformed', [_ok({'output2': malformed})])
    with pytest.raises(KisApiError) as excinfo:
        client.get_stock_minute_bars(
            '005930', session_date=dt.date(2026, 9, 17), session_open=dt.time(9, 0), session_close=dt.time(15, 30)
        )
    assert excinfo.value.msg_cd == 'SCHEMA'


def test_get_stock_minute_bars_rejects_page_cap_overflow(tmp_path) -> None:
    import datetime as dt

    import pytest

    from src.execution.contracts import KisApiError
    from tests.unit.execution.fakes import make_client

    pages = []
    cur = dt.datetime.strptime('153000', '%H%M%S')
    for _ in range(100):
        pages.append(_ok({'output2': [_minute_row(cur.strftime('%H%M%S'))]}))
        cur -= dt.timedelta(minutes=1)
    client, _, _ = make_client(tmp_path, pages)

    with pytest.raises(KisApiError) as excinfo:
        client.get_stock_minute_bars(
            '005930', session_date=dt.date(2026, 9, 17), session_open=dt.time(9, 0), session_close=dt.time(15, 30)
        )
    assert excinfo.value.msg_cd == 'PAGINATION'


def _news_row(news_id: str = '202609170001', **overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        'cntt_usiq_srno': news_id,
        'data_dt': '20260917',
        'data_tm': '203306',
        'dorg': '거래소 공시',
        'news_ofer_entp_code': '1',
        'news_lrdv_code': 'A',
        'hts_pbnt_titl_cntt': '제목',
        'iscd1': '066570',
        'iscd2': ' ',
    }
    for i in range(3, 11):
        base[f'iscd{i}'] = ''
    base.update(overrides)
    return base


def test_get_news_titles_converts_kst_time_and_collects_symbols(tmp_path) -> None:
    import datetime as dt

    from tests.unit.execution.fakes import make_client

    client, session, _ = make_client(tmp_path, [_ok({'output': [_news_row()]})])

    (row,) = client.get_news_titles()

    pub = dt.datetime(2026, 9, 17, 11, 33, 6, tzinfo=dt.UTC)
    epoch = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
    assert row['published_at_ns'] == int((pub - epoch).total_seconds()) * 1_000_000_000
    assert row['symbols'] == ['066570']
    assert row['market_div_code'] == ''
    assert set(session.calls[0]['params']) == {
        'FID_NEWS_OFER_ENTP_CODE',
        'FID_COND_MRKT_CLS_CODE',
        'FID_INPUT_ISCD',
        'FID_TITL_CNTT',
        'FID_INPUT_DATE_1',
        'FID_INPUT_HOUR_1',
        'FID_RANK_SORT_CLS_CODE',
        'FID_INPUT_SRNO',
    }
    assert all(v == '' for v in session.calls[0]['params'].values())


def test_get_news_titles_sends_inclusive_cursor(tmp_path) -> None:
    from tests.unit.execution.fakes import make_client

    client, session, _ = make_client(tmp_path, [_ok({'output': []})])

    assert client.get_news_titles(before=('20260917', '201739')) == ()
    assert session.calls[0]['params']['FID_INPUT_DATE_1'] == '20260917'
    assert session.calls[0]['params']['FID_INPUT_HOUR_1'] == '201739'


def test_get_news_titles_rejects_empty_news_id(tmp_path) -> None:
    import pytest

    from src.execution.contracts import KisApiError
    from tests.unit.execution.fakes import make_client

    client, _, _ = make_client(tmp_path, [_ok({'output': [_news_row(news_id='  ')]})])

    with pytest.raises(KisApiError) as excinfo:
        client.get_news_titles()
    assert excinfo.value.msg_cd == 'SCHEMA'


def test_snapshot_parsing_rejects_missing_and_malformed_fields(tmp_path) -> None:
    import pytest

    from src.execution.contracts import KisApiError
    from tests.unit.execution.fakes import make_client

    cases = [
        ('missing key', _status_output(), ['stck_prpr'], 'get_security_status', ('005930',), {}),
        ('bad int', _status_output(stck_prpr='abc'), [], 'get_security_status', ('005930',), {}),
        ('fractional int', _status_output(stck_prpr='1.5'), [], 'get_security_status', ('005930',), {}),
    ]
    for label, output, drop, method, args, kwargs in cases:
        for key in drop:
            del output[key]
        client, _, _ = make_client(tmp_path / label.replace(' ', '_'), [_ok({'output': output})])
        with pytest.raises(KisApiError) as excinfo:
            getattr(client, method)(*args, **kwargs)
        assert excinfo.value.msg_cd == 'SCHEMA', label

    for bad_float in ('xyz', 'Infinity'):
        output2 = _asking_output2()
        output2['antc_cntg_prdy_ctrt'] = bad_float
        client, _, _ = make_client(
            tmp_path / f'float_{bad_float}', [_ok({'output1': _asking_output1(1), 'output2': output2})]
        )
        with pytest.raises(KisApiError) as excinfo:
            client.get_auction_book('005930')
        assert excinfo.value.msg_cd == 'SCHEMA'

    for bad_dt, bad_tm in (('20260917', '2033'), ('20260230', '203306')):
        client, _, _ = make_client(
            tmp_path / f'news_{bad_dt}_{bad_tm}', [_ok({'output': [_news_row(data_dt=bad_dt, data_tm=bad_tm)]})]
        )
        with pytest.raises(KisApiError) as excinfo:
            client.get_news_titles()
        assert excinfo.value.msg_cd == 'SCHEMA'


def _index_minute_row(hour: str, *, date: str = '20260917', price: str = '822.18') -> dict[str, str]:
    return {
        'stck_bsop_date': date,
        'stck_cntg_hour': hour,
        'bstp_nmix_oprc': price,
        'bstp_nmix_hgpr': '823.00',
        'bstp_nmix_lwpr': '821.00',
        'bstp_nmix_prpr': '822.50',
        'cntg_vol': '1234',
        'acml_tr_pbmn': '567890',
    }


def test_get_index_minute_bars_drops_summary_rows_and_maps_float_ohlc(tmp_path) -> None:
    import datetime as dt

    from tests.unit.execution.fakes import make_client

    rows = [
        _index_minute_row('999999'),
        _index_minute_row('888888'),
        _index_minute_row('090000'),
    ]
    client, session, _ = make_client(tmp_path, [_ok({'output2': rows})])

    (bar,) = client.get_index_minute_bars('1001', session_date=dt.date(2026, 9, 17))

    assert bar['bar_time'] == '090000'
    assert isinstance(bar['open'], float)
    assert isinstance(bar['high'], float)
    assert isinstance(bar['low'], float)
    assert isinstance(bar['close'], float)
    assert bar['market_div_code'] == 'U'
    params = session.calls[0]['params']
    assert params['FID_INPUT_HOUR_1'] == '60'
    assert params['FID_INPUT_ISCD'] == '1001'
    assert params['FID_COND_MRKT_DIV_CODE'] == 'U'


def test_get_index_minute_bars_drops_other_date_and_rejects_broken_bar(tmp_path) -> None:
    import datetime as dt

    import pytest

    from src.execution.contracts import KisApiError
    from tests.unit.execution.fakes import make_client

    rows = [_index_minute_row('090000', date='20260916'), _index_minute_row('090100')]
    client, _, _ = make_client(tmp_path, [_ok({'output2': rows})])

    bars = client.get_index_minute_bars('1001', session_date=dt.date(2026, 9, 17))

    assert [b['bar_time'] for b in bars] == ['090100']

    broken = _index_minute_row('090200')
    broken['bstp_nmix_hgpr'] = '820.00'
    client, _, _ = make_client(tmp_path / 'broken', [_ok({'output2': [broken]})])
    with pytest.raises(KisApiError) as excinfo:
        client.get_index_minute_bars('1001', session_date=dt.date(2026, 9, 17))
    assert excinfo.value.msg_cd == 'SCHEMA'


def test_get_index_minute_bars_deduplicates_and_sorts_ascending(tmp_path) -> None:
    import datetime as dt

    from tests.unit.execution.fakes import make_client

    rows = [_index_minute_row('090200'), _index_minute_row('090100'), _index_minute_row('090100')]
    client, _, _ = make_client(tmp_path, [_ok({'output2': rows})])

    bars = client.get_index_minute_bars('1001', session_date=dt.date(2026, 9, 17))

    assert [b['bar_time'] for b in bars] == ['090100', '090200']
