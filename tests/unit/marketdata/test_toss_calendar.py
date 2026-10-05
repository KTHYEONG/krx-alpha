def test_fetch_trading_day_parses_business_day_envelope() -> None:
    # Given: 영업일 2026-09-14 캘린더 봉투
    import datetime as dt

    from src.marketdata.toss_calendar import TOSS_CALENDAR_URL, fetch_trading_day

    payload = {
        "result": {
            "today": {"date": "2026-09-14", "integrated": {"regularMarket": {"startTime": "2026-09-14T09:00:00.000+09:00"}}},
            "previousBusinessDay": {"date": "2026-09-11", "integrated": {}},
            "nextBusinessDay": {"date": "2026-09-15", "integrated": {}},
        }
    }

    class _Resp:
        def __init__(self, body):
            self._body = body

        def raise_for_status(self):
            self._checked = True

        def json(self):
            return self._body

    class _Session:
        def __init__(self):
            self.get_calls = []
            self.post_calls = []

        def post(self, url, **kw):
            self.post_calls.append((url, kw))
            return _Resp({"access_token": "tok", "expires_in": 86400})

        def get(self, url, **kw):
            self.get_calls.append((url, kw))
            return _Resp(payload)

    session = _Session()

    # When
    out = fetch_trading_day(dt.date(2026, 9, 14), app_key="k", app_secret="s", session=session)

    # Then: 영업일 플래그와 전/후 영업일이 date 로 정규화된다
    assert out.date == dt.date(2026, 9, 14)
    assert out.is_business_day is True
    assert out.previous_business_day == dt.date(2026, 9, 11)
    assert out.next_business_day == dt.date(2026, 9, 15)
    assert session.get_calls[0][0] == TOSS_CALENDAR_URL
    assert session.get_calls[0][1]["params"] == {"date": "2026-09-14"}
    assert session.get_calls[0][1]["headers"]["Authorization"] == "Bearer tok"


def test_fetch_trading_day_flags_holiday_when_integrated_is_null() -> None:
    # Given: 비영업일 봉투 (integrated=null)
    import datetime as dt

    from src.marketdata.toss_calendar import fetch_trading_day

    payload = {
        "result": {
            "today": {"date": "2026-09-12", "integrated": None},
            "previousBusinessDay": {"date": "2026-09-11"},
            "nextBusinessDay": {"date": "2026-09-14"},
        }
    }

    class _Resp:
        def __init__(self, body):
            self._body = body

        def raise_for_status(self):
            self._checked = True

        def json(self):
            return self._body

    class _Session:
        def post(self, url, **kw):
            return _Resp({"access_token": "tok"})

        def get(self, url, **kw):
            return _Resp(payload)

    # When
    out = fetch_trading_day(dt.date(2026, 9, 12), app_key="k", app_secret="s", session=_Session())

    # Then: 휴장으로 판정하고 직전 영업일을 노출한다
    assert out.is_business_day is False
    assert out.previous_business_day == dt.date(2026, 9, 11)
    assert out.next_business_day == dt.date(2026, 9, 14)


def test_fetch_trading_day_raises_on_missing_envelope_keys() -> None:
    # Given: previousBusinessDay 가 없는 손상 봉투
    import datetime as dt

    import pytest

    from src.marketdata.toss_calendar import TossCalendarError, fetch_trading_day

    payload = {"result": {"today": {"date": "2026-09-14", "integrated": {}}}}

    class _Resp:
        def __init__(self, body):
            self._body = body

        def raise_for_status(self):
            self._checked = True

        def json(self):
            return self._body

    class _Session:
        def post(self, url, **kw):
            return _Resp({"access_token": "tok"})

        def get(self, url, **kw):
            return _Resp(payload)

    # When / Then
    with pytest.raises(TossCalendarError):
        fetch_trading_day(dt.date(2026, 9, 14), app_key="k", app_secret="s", session=_Session())


def test_fetch_trading_day_wraps_request_exception() -> None:
    # Given: 전송 계층이 계속 실패하는 세션
    import datetime as dt

    import pytest
    import requests

    from src.marketdata.toss_calendar import TossCalendarError, fetch_trading_day

    class _Session:
        def __init__(self):
            self.attempts = 0

        def post(self, url, **kw):
            self.attempts += 1
            raise requests.RequestException("boom")

        def get(self, url, **kw):
            raise AssertionError("토큰 발급 실패 시 캘린더를 호출하면 안 된다")

    session = _Session()

    # When / Then: 도메인 예외로 변환되고 재시도가 수행된다
    with pytest.raises(TossCalendarError):
        fetch_trading_day(dt.date(2026, 9, 14), app_key="k", app_secret="s", session=session)
    assert session.attempts >= 1


def test_issue_access_token_raises_when_access_token_missing() -> None:
    # Given: access_token 이 없는 오류 봉투
    import pytest

    from src.marketdata.toss_calendar import TOSS_TOKEN_URL, TossCalendarError, issue_access_token

    class _Resp:
        def raise_for_status(self):
            self._checked = True

        def json(self):
            return {"error": {"code": "invalid-client", "message": "bad key"}}

    class _Session:
        def __init__(self):
            self.calls = []

        def post(self, url, **kw):
            self.calls.append((url, kw))
            return _Resp()

    session = _Session()

    # When / Then
    with pytest.raises(TossCalendarError) as excinfo:
        issue_access_token(app_key="k", app_secret="super-secret", session=session)

    assert session.calls[0][0] == TOSS_TOKEN_URL
    assert session.calls[0][1]["data"]["grant_type"] == "client_credentials"
    assert "super-secret" not in str(excinfo.value)




def test_trading_day_cache_roundtrip_and_unreadable_cache(tmp_path) -> None:
    import datetime as dt

    from src.marketdata.toss_calendar import TradingDay, load_trading_day_cache, save_trading_day_cache

    path = tmp_path / "data" / "calendar_cache.json"
    day = TradingDay(date=dt.date(2026, 9, 11), is_business_day=True, previous_business_day=dt.date(2026, 9, 10), next_business_day=dt.date(2026, 9, 14))

    assert load_trading_day_cache(path) is None
    save_trading_day_cache(path, day)
    assert load_trading_day_cache(path) == day
    assert list(path.parent.glob("*.tmp")) == []
    path.write_text("{not json", encoding="utf-8")
    assert load_trading_day_cache(path) is None


def test_trading_day_from_cache_derives_only_decidable_days() -> None:
    import datetime as dt

    from src.marketdata.toss_calendar import TradingDay, trading_day_from_cache

    friday = TradingDay(date=dt.date(2026, 9, 11), is_business_day=True, previous_business_day=dt.date(2026, 9, 10), next_business_day=dt.date(2026, 9, 15))
    holiday = TradingDay(date=dt.date(2026, 9, 14), is_business_day=False, previous_business_day=dt.date(2026, 9, 11), next_business_day=dt.date(2026, 9, 15))

    assert trading_day_from_cache(friday, dt.date(2026, 9, 11)) == friday
    assert trading_day_from_cache(friday, dt.date(2026, 9, 15)) == TradingDay(date=dt.date(2026, 9, 15), is_business_day=True, previous_business_day=dt.date(2026, 9, 11), next_business_day=dt.date(2026, 9, 15))
    assert trading_day_from_cache(holiday, dt.date(2026, 9, 15)).previous_business_day == dt.date(2026, 9, 11)
    assert trading_day_from_cache(friday, dt.date(2026, 9, 14)) == TradingDay(date=dt.date(2026, 9, 14), is_business_day=False, previous_business_day=dt.date(2026, 9, 11), next_business_day=dt.date(2026, 9, 15))
    assert trading_day_from_cache(friday, dt.date(2026, 9, 16)) is None
    assert trading_day_from_cache(friday, dt.date(2026, 9, 10)) is None


def _calendar_session(payload):
    class _Resp:
        def __init__(self, body):
            self._body = body

        def raise_for_status(self):
            pass

        def json(self):
            return self._body

    class _Session:
        def post(self, url, **kw):
            return _Resp({"access_token": "tok"})

        def get(self, url, **kw):
            return _Resp(payload)

    return _Session()


def _integrated(day, open_hhmm, auction_hhmm, close_hhmm, after_hhmm, *, pre_null=True):
    def _ts(hhmm):
        return f"{day}T{hhmm[:2]}:{hhmm[2:]}:00.000+09:00"

    return {
        "regularMarket": {
            "startTime": _ts(open_hhmm),
            "singlePriceAuctionStartTime": _ts(auction_hhmm),
            "endTime": _ts(close_hhmm),
        },
        "preMarket": None if pre_null else {},
        "afterMarket": None if after_hhmm is None else {"endTime": _ts(after_hhmm)},
    }


def test_fetch_trading_day_parses_normal_day_anchors() -> None:
    import datetime as dt

    from src.core.session_anchors import AnchorSource
    from src.marketdata.toss_calendar import fetch_trading_day

    payload = {
        "result": {
            "today": {"date": "2026-10-01", "integrated": _integrated("2026-10-01", "0900", "1520", "1530", "2000")},
            "previousBusinessDay": {"date": "2026-09-30", "integrated": {}},
            "nextBusinessDay": {"date": "2026-10-02", "integrated": _integrated("2026-10-02", "0900", "1520", "1530", "2000")},
        }
    }
    out = fetch_trading_day(dt.date(2026, 10, 1), app_key="k", app_secret="s", session=_calendar_session(payload))

    assert out.is_business_day is True
    assert out.anchors is not None
    assert (out.anchors.regular_open, out.anchors.closing_auction_start, out.anchors.regular_close, out.anchors.after_market_end) == (
        dt.time(9, 0),
        dt.time(15, 20),
        dt.time(15, 30),
        dt.time(20, 0),
    )
    assert out.anchors.source is AnchorSource.VENDOR


def test_fetch_trading_day_parses_csat_payload_with_null_premarket() -> None:
    import datetime as dt

    from src.marketdata.toss_calendar import fetch_trading_day

    payload = {
        "result": {
            "today": {"date": "2025-11-13", "integrated": _integrated("2025-11-13", "1000", "1620", "1630", "2000")},
            "previousBusinessDay": {"date": "2025-11-12", "integrated": {}},
            "nextBusinessDay": {"date": "2025-11-14", "integrated": None},
        }
    }
    out = fetch_trading_day(dt.date(2025, 11, 13), app_key="k", app_secret="s", session=_calendar_session(payload))

    assert out.anchors is not None
    assert (out.anchors.regular_open, out.anchors.closing_auction_start, out.anchors.regular_close, out.anchors.after_market_end) == (
        dt.time(10, 0),
        dt.time(16, 20),
        dt.time(16, 30),
        dt.time(20, 0),
    )


def test_fetch_trading_day_holiday_has_no_anchors() -> None:
    import datetime as dt

    from src.marketdata.toss_calendar import fetch_trading_day

    payload = {
        "result": {
            "today": {"date": "2026-10-04", "integrated": None},
            "previousBusinessDay": {"date": "2026-10-02"},
            "nextBusinessDay": {"date": "2026-10-05"},
        }
    }
    out = fetch_trading_day(dt.date(2026, 10, 4), app_key="k", app_secret="s", session=_calendar_session(payload))

    assert out.is_business_day is False
    assert out.anchors is None


def test_fetch_trading_day_parses_next_business_day_anchors() -> None:
    import datetime as dt

    from src.marketdata.toss_calendar import fetch_trading_day

    payload = {
        "result": {
            "today": {"date": "2026-10-01", "integrated": _integrated("2026-10-01", "0900", "1520", "1530", "2000")},
            "previousBusinessDay": {"date": "2026-09-30", "integrated": {}},
            "nextBusinessDay": {"date": "2026-10-02", "integrated": _integrated("2026-10-02", "1000", "1620", "1630", "2000")},
        }
    }
    out = fetch_trading_day(dt.date(2026, 10, 1), app_key="k", app_secret="s", session=_calendar_session(payload))

    assert out.next_anchors is not None
    assert out.next_anchors.date == dt.date(2026, 10, 2)
    assert out.next_anchors.regular_open == dt.time(10, 0)


def test_fetch_trading_day_malformed_time_degrades_to_none() -> None:
    import datetime as dt

    from src.marketdata.toss_calendar import fetch_trading_day

    integrated = _integrated("2026-10-01", "0900", "1520", "1530", "2000")
    integrated["regularMarket"]["endTime"] = "bad"
    payload = {
        "result": {
            "today": {"date": "2026-10-01", "integrated": integrated},
            "previousBusinessDay": {"date": "2026-09-30", "integrated": {}},
            "nextBusinessDay": {"date": "2026-10-02", "integrated": None},
        }
    }
    out = fetch_trading_day(dt.date(2026, 10, 1), app_key="k", app_secret="s", session=_calendar_session(payload))

    assert out.anchors is None
    assert out.is_business_day is True


def test_parse_session_anchors_converts_utc_offset_to_kst() -> None:
    import datetime as dt

    from src.marketdata.toss_calendar import parse_session_anchors

    integrated = {
        "regularMarket": {
            "startTime": "2026-11-19T01:00:00.000Z",
            "singlePriceAuctionStartTime": "2026-11-19T07:20:00.000Z",
            "endTime": "2026-11-19T07:30:00.000Z",
        },
        "afterMarket": {"endTime": "2026-11-19T11:00:00.000Z"},
    }

    anchors = parse_session_anchors(dt.date(2026, 11, 19), integrated)

    assert anchors is not None
    assert anchors.regular_open == dt.time(10, 0)


def test_trading_day_cache_round_trip_keeps_legacy_format(tmp_path) -> None:
    import datetime as dt
    import json

    from src.marketdata.toss_calendar import TradingDay, load_trading_day_cache, save_trading_day_cache

    day = TradingDay(
        date=dt.date(2026, 10, 1),
        is_business_day=True,
        previous_business_day=dt.date(2026, 9, 30),
        next_business_day=dt.date(2026, 10, 2),
    )

    def _path(tmp_path):
        return tmp_path / "calendar_cache.json"

    path = _path(tmp_path)
    save_trading_day_cache(path, day)
    raw = json.loads(path.read_text(encoding="utf-8"))

    assert sorted(raw.keys()) == ["date", "is_business_day", "next_business_day", "previous_business_day"]
    assert load_trading_day_cache(path) is not None
    assert load_trading_day_cache(path).anchors is None


def test_parse_session_anchors_rejects_naive_timestamp() -> None:
    import datetime as dt

    from src.marketdata.toss_calendar import parse_session_anchors

    integrated = {
        "regularMarket": {
            "startTime": "2026-10-01T09:00:00.000",
            "singlePriceAuctionStartTime": "2026-10-01T15:20:00.000+09:00",
            "endTime": "2026-10-01T15:30:00.000+09:00",
        },
        "afterMarket": {"endTime": "2026-10-01T20:00:00.000+09:00"},
    }

    assert parse_session_anchors(dt.date(2026, 10, 1), integrated) is None


def test_parse_session_anchors_rejects_cross_date_timestamp() -> None:
    import datetime as dt

    from src.marketdata.toss_calendar import parse_session_anchors

    integrated = {
        "regularMarket": {
            "startTime": "2026-10-01T09:00:00.000+09:00",
            "singlePriceAuctionStartTime": "2026-10-01T15:20:00.000+09:00",
            "endTime": "2026-10-01T15:30:00.000+09:00",
        },
        "afterMarket": {"endTime": "2026-10-01T20:00:00.000+09:00"},
    }

    assert parse_session_anchors(dt.date(2026, 10, 2), integrated) is None


def test_parse_session_anchors_uses_standard_aftermarket_when_null() -> None:
    import datetime as dt

    from src.core.session_anchors import STANDARD_AFTER_MARKET_END
    from src.marketdata.toss_calendar import parse_session_anchors

    integrated = {
        "regularMarket": {
            "startTime": "2026-10-01T09:00:00.000+09:00",
            "singlePriceAuctionStartTime": "2026-10-01T15:20:00.000+09:00",
            "endTime": "2026-10-01T15:30:00.000+09:00",
        },
        "afterMarket": None,
    }

    anchors = parse_session_anchors(dt.date(2026, 10, 1), integrated)

    assert anchors is not None
    assert anchors.after_market_end == STANDARD_AFTER_MARKET_END


def test_parse_session_anchors_rejects_non_mapping_sections() -> None:
    import datetime as dt

    from src.marketdata.toss_calendar import parse_session_anchors

    day = dt.date(2026, 10, 1)
    regular = {
        "startTime": "2026-10-01T09:00:00.000+09:00",
        "singlePriceAuctionStartTime": "2026-10-01T15:20:00.000+09:00",
        "endTime": "2026-10-01T15:30:00.000+09:00",
    }

    assert parse_session_anchors(day, {"regularMarket": None, "afterMarket": None}) is None
    assert parse_session_anchors(day, {"regularMarket": regular, "afterMarket": "bad"}) is None
    assert parse_session_anchors(day, {"regularMarket": regular, "afterMarket": {"endTime": "bad"}}) is None


def test_parse_session_anchors_rejects_out_of_order_vendor_times() -> None:
    import datetime as dt

    from src.marketdata.toss_calendar import parse_session_anchors

    integrated = {
        "regularMarket": {
            "startTime": "2026-10-01T12:30:00.000+09:00",
            "singlePriceAuctionStartTime": "2026-10-01T12:20:00.000+09:00",
            "endTime": "2026-10-01T15:30:00.000+09:00",
        },
        "afterMarket": {"endTime": "2026-10-01T20:00:00.000+09:00"},
    }

    assert parse_session_anchors(dt.date(2026, 10, 1), integrated) is None


def _auth_rejecting_session(kind: str, calls: list):
    import requests

    business = {
        "result": {
            "today": {"date": "2026-09-14", "integrated": {"regularMarket": {"startTime": "2026-09-14T09:00:00.000+09:00"}}},
            "previousBusinessDay": {"date": "2026-09-11", "integrated": {}},
            "nextBusinessDay": {"date": "2026-09-15", "integrated": {}},
        }
    }

    class _Resp:
        def __init__(self, body):
            self._body = body

        def raise_for_status(self):
            pass

        def json(self):
            return self._body

    class _Session:
        def __init__(self):
            self.tokens = 0

        def post(self, url, **kw):
            self.tokens += 1
            calls.append(("token", self.tokens))
            return _Resp({"access_token": f"tok-{self.tokens}"})

        def get(self, url, **kw):
            auth = kw.get("headers", {}).get("Authorization", "")
            if kind == "always-401":
                err = requests.HTTPError("401 Unauthorized")
                err.response = type("R", (), {"status_code": 401})()
                raise err
            if kind == "once-401" and auth == "Bearer tok-1":
                err = requests.HTTPError("401 Unauthorized")
                err.response = type("R", (), {"status_code": 401})()
                raise err
            if kind == "invalid-token" and auth == "Bearer tok-1":
                return _Resp({"error": {"code": "invalid-token", "message": "stale"}})
            return _Resp(business)

    return _Session()


def test_fetch_trading_day_rotates_once_on_401_and_retries(tmp_path) -> None:
    # Given: 첫 토큰으로 401이 나는 세션과 격리된 저장소
    import datetime as dt

    from src.marketdata.toss_calendar import fetch_trading_day
    from src.marketdata.toss_token_store import TossTokenStore, toss_token_path

    calls: list = []
    session = _auth_rejecting_session("once-401", calls)
    store = TossTokenStore(toss_token_path(tmp_path, "k"))

    # When
    out = fetch_trading_day(dt.date(2026, 9, 14), app_key="k", app_secret="s", session=session, token_store=store)

    # Then: 1회 회전 후 같은 요청을 재시도하고 oauth는 2회 호출된다
    assert out.date == dt.date(2026, 9, 14)
    assert calls == [("token", 1), ("token", 2)]
    assert store.read().access_token == "tok-2"
    assert store.read().generation == 2


def test_fetch_trading_day_reuses_stored_token_without_issuance(tmp_path) -> None:
    # Given: 저장소가 이미 유효한 B를 쥐고 있는 세션
    import datetime as dt
    import json

    from src.marketdata.toss_calendar import fetch_trading_day
    from src.marketdata.toss_token_store import TossTokenStore, toss_token_path

    path = toss_token_path(tmp_path, "k")
    path.write_text(json.dumps({
        "schema_version": 1, "access_token": "tok-B", "issued_at": __import__("datetime").datetime.now(__import__("datetime").UTC).isoformat(),
        "expires_at": None, "generation": 5,
    }), encoding="utf-8")

    business = {
        "result": {
            "today": {"date": "2026-09-14", "integrated": {"regularMarket": {"startTime": "2026-09-14T09:00:00.000+09:00"}}},
            "previousBusinessDay": {"date": "2026-09-11", "integrated": {}},
            "nextBusinessDay": {"date": "2026-09-15", "integrated": {}},
        }
    }

    class _Resp:
        def __init__(self, body):
            self._body = body

        def raise_for_status(self):
            pass

        def json(self):
            return self._body

    class _PeerSession:
        def post(self, url, **kw):
            raise AssertionError("stored token is usable; must not issue")

        def get(self, url, **kw):
            assert kw["headers"]["Authorization"] == "Bearer tok-B"
            return _Resp(business)

    # When: 저장소 토큰으로 곧바로 조회한다
    store = TossTokenStore(path)
    out = fetch_trading_day(
        dt.date(2026, 9, 14), app_key="k", app_secret="s", session=_PeerSession(), token_store=store
    )

    # Then: 발급 없이 조회하고 세대가 유지된다
    assert out.previous_business_day == dt.date(2026, 9, 11)
    assert store.read().generation == 5


def test_fetch_trading_day_second_rejection_raises(tmp_path) -> None:
    # Given: 항상 401인 세션
    import datetime as dt

    import pytest

    from src.marketdata.toss_calendar import TossCalendarError, fetch_trading_day

    calls: list = []
    session = _auth_rejecting_session("always-401", calls)

    # When / Then: 두 번째 거부도 기존 TossCalendarError로 표면화된다
    with pytest.raises(TossCalendarError):
        fetch_trading_day(dt.date(2026, 9, 14), app_key="k", app_secret="s", session=session, cache_dir=tmp_path)
    assert calls == [("token", 1), ("token", 2)]


def test_fetch_trading_day_retries_once_on_invalid_token_envelope(tmp_path) -> None:
    # Given: 첫 토큰에 invalid-token 봉투를 돌려주는 세션
    import datetime as dt

    from src.marketdata.toss_calendar import fetch_trading_day
    from src.marketdata.toss_token_store import TossTokenStore, toss_token_path

    calls: list = []
    session = _auth_rejecting_session("invalid-token", calls)
    store = TossTokenStore(toss_token_path(tmp_path, "k"))

    # When
    out = fetch_trading_day(dt.date(2026, 9, 14), app_key="k", app_secret="s", session=session, token_store=store)

    # Then: 회전 후 재시도가 성공한다
    assert out.date == dt.date(2026, 9, 14)
    assert calls == [("token", 1), ("token", 2)]
    assert store.read().access_token == "tok-2"


def test_issue_access_token_with_non_mapping_body_raises() -> None:
    # Given: 리스트 본문을 돌려주는 토큰 세션
    import pytest

    from src.marketdata.toss_calendar import TossCalendarError, issue_access_token

    class _Resp:
        def raise_for_status(self):
            pass

        def json(self):
            return ["not", "a", "mapping"]

    class _Session:
        def post(self, url, **kw):
            return _Resp()

    # When / Then
    with pytest.raises(TossCalendarError, match="missing access_token"):
        issue_access_token(app_key="k", app_secret="s", session=_Session())


def test_fetch_trading_day_does_not_resend_401_before_rotation(tmp_path) -> None:
    # Given: 항상 401인 세션의 GET 호출 횟수 감시
    import datetime as dt

    import pytest

    from src.marketdata.toss_calendar import TossCalendarError, fetch_trading_day

    calls: list = []
    session = _auth_rejecting_session("always-401", calls)
    gets: list[str] = []
    original_get = session.get

    def _counting_get(url, **kw):
        gets.append(url)
        return original_get(url, **kw)

    session.get = _counting_get

    # When / Then: 4xx 는 재전송 없이 회전 1회 후 바로 실패한다
    with pytest.raises(TossCalendarError):
        fetch_trading_day(dt.date(2026, 9, 14), app_key="k", app_secret="s", session=session, cache_dir=tmp_path)
    assert len(gets) == 2
