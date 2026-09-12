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


