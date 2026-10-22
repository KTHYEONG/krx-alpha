"""Invariant tests for Toss 1-minute candle transport and history walk."""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

import pytest

from src.marketdata.toss_candles import (
    MinuteCandle,
    TossCandlesError,
    fetch_minute_candles,
    walk_minute_history,
)

_KST = ZoneInfo("Asia/Seoul")


def _candle(day: dt.date, h: int, m: int, s: int = 0) -> MinuteCandle:
    return MinuteCandle(
        ts=dt.datetime(day.year, day.month, day.day, h, m, s, tzinfo=_KST),
        open=100,
        high=101,
        low=99,
        close=100,
        volume=10,
    )


class _Resp:
    def __init__(self, body, *, status: int = 200):
        self._body = body
        self.status_code = status

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise _HttpError(self.status_code)

    def json(self):
        return self._body


class _HttpError(Exception):
    def __init__(self, status: int):
        super().__init__(f"http {status}")
        self.response = type("R", (), {"status_code": status})()


class _Session:
    def __init__(self, body):
        self._body = body
        self.calls: list[dict] = []

    def get(self, url, **kwargs):
        self.calls.append({"url": url, "params": kwargs.get("params", {})})
        return _Resp(self._body)


def _row(ts: str, volume: str | int = 10) -> dict:
    return {"timestamp": ts, "openPrice": 100, "highPrice": 101, "lowPrice": 99, "closePrice": 100, "volume": volume}


def test_raw_prices_requested() -> None:
    day = dt.date(2026, 10, 20)
    session = _Session({"result": {"candles": [_row(f"{day.isoformat()}T09:00:00+09:00")]}})
    candles = fetch_minute_candles(
        "005930", access_token="tok", before=None, session=session, timeout_s=10.0
    )
    assert len(candles) == 1
    params = session.calls[0]["params"]
    assert params["adjusted"] == "false"
    assert params["interval"] == "1m"
    assert params["count"] == "200"


def test_offhours_candles_discarded() -> None:
    day = dt.date(2026, 10, 20)
    prev_day = dt.date(2026, 10, 17)
    pages = [
        (
            _candle(day, 16, 10),
            _candle(day, 15, 30),
            _candle(day, 9, 0),
            _candle(day, 8, 30),
        ),
        (_candle(prev_day, 15, 30),),
    ]
    calls = {"n": 0}

    def _fetch(before):
        page = pages[calls["n"]]
        calls["n"] += 1
        return page

    walk = walk_minute_history("A", wanted_days=frozenset({day}), fetch=_fetch, max_calls=10)
    assert walk.discarded_offhours == 2
    assert sorted(c.ts.time() for c in walk.complete[day]) == [dt.time(9, 0), dt.time(15, 30)]


def test_crossing_certifies_the_day() -> None:
    day = dt.date(2026, 10, 20)
    prev_day = dt.date(2026, 10, 17)
    bars = tuple(
        MinuteCandle(
            ts=dt.datetime.combine(day, dt.time(9, 0), tzinfo=_KST) + dt.timedelta(minutes=idx),
            open=100,
            high=101,
            low=99,
            close=100,
            volume=10,
        )
        for idx in range(390)
    )
    pages = [tuple(reversed(bars)), (_candle(prev_day, 15, 30),)]
    calls = {"n": 0}

    def _fetch(before):
        page = pages[calls["n"]]
        calls["n"] += 1
        return page

    walk = walk_minute_history("A", wanted_days=frozenset({day}), fetch=_fetch, max_calls=10)
    assert len(walk.complete[day]) == 390
    assert prev_day not in walk.complete
    assert walk.vendor_empty == frozenset()


def test_before_vendor_history_is_vendor_empty() -> None:
    day = dt.date(2026, 10, 20)

    def _fetch(before):
        return ()

    walk = walk_minute_history("A", wanted_days=frozenset({day}), fetch=_fetch, max_calls=10)
    assert walk.vendor_empty == frozenset({day})
    assert walk.empty_reasons[day] == "before_vendor_history"


def test_page_boundary_duplicates_removed() -> None:
    day = dt.date(2026, 10, 20)
    prev_day = dt.date(2026, 10, 17)
    shared = _candle(day, 10, 0)
    pages = [
        (_candle(day, 11, 0), shared),
        (shared, _candle(prev_day, 15, 30)),
    ]
    calls = {"n": 0}

    def _fetch(before):
        page = pages[calls["n"]]
        calls["n"] += 1
        return page

    walk = walk_minute_history("A", wanted_days=frozenset({day}), fetch=_fetch, max_calls=10)
    assert len(walk.complete[day]) == 2


def test_malformed_numeric_raises() -> None:
    day = dt.date(2026, 10, 20)
    session = _Session({"result": {"candles": [_row(f"{day.isoformat()}T09:00:00+09:00", volume="-1")]}})
    with pytest.raises(TossCandlesError):
        fetch_minute_candles("005930", access_token="tok", before=None, session=session, timeout_s=10.0)


def test_empty_symbol_raises() -> None:
    session = _Session({"result": {"candles": []}})
    with pytest.raises(TossCandlesError):
        fetch_minute_candles("", access_token="tok", before=None, session=session, timeout_s=10.0)


def test_bad_timestamp_type_raises() -> None:
    row = _row("2026-10-20T09:00:00+09:00")
    row["timestamp"] = 20261020
    session = _Session({"result": {"candles": [row]}})
    with pytest.raises(TossCandlesError):
        fetch_minute_candles("005930", access_token="tok", before=None, session=session, timeout_s=10.0)


def test_bad_timestamp_iso_raises() -> None:
    session = _Session({"result": {"candles": [_row("not-a-time")]}})
    with pytest.raises(TossCandlesError):
        fetch_minute_candles("005930", access_token="tok", before=None, session=session, timeout_s=10.0)


def test_naive_timestamp_assumed_kst() -> None:
    session = _Session({"result": {"candles": [_row("2026-10-20T09:00:00")]}})
    (candle,) = fetch_minute_candles("005930", access_token="tok", before=None, session=session, timeout_s=10.0)
    assert candle.ts.tzinfo is not None
    assert candle.ts.utcoffset() == dt.timedelta(hours=9)


def test_bool_field_raises() -> None:
    row = _row("2026-10-20T09:00:00+09:00")
    row["openPrice"] = True
    session = _Session({"result": {"candles": [row]}})
    with pytest.raises(TossCandlesError):
        fetch_minute_candles("005930", access_token="tok", before=None, session=session, timeout_s=10.0)


def test_non_numeric_field_raises() -> None:
    row = _row("2026-10-20T09:00:00+09:00")
    row["closePrice"] = "abc"
    session = _Session({"result": {"candles": [row]}})
    with pytest.raises(TossCandlesError):
        fetch_minute_candles("005930", access_token="tok", before=None, session=session, timeout_s=10.0)


def test_non_mapping_row_raises() -> None:
    session = _Session({"result": {"candles": [42]}})
    with pytest.raises(TossCandlesError):
        fetch_minute_candles("005930", access_token="tok", before=None, session=session, timeout_s=10.0)


def test_missing_key_raises() -> None:
    row = _row("2026-10-20T09:00:00+09:00")
    del row["closePrice"]
    session = _Session({"result": {"candles": [row]}})
    with pytest.raises(TossCandlesError):
        fetch_minute_candles("005930", access_token="tok", before=None, session=session, timeout_s=10.0)


def _http_error(status: int):
    import requests

    exc = requests.HTTPError(f"http {status}")
    exc.response = type("R", (), {"status_code": status})()
    return exc


class _RaisingSession:
    def __init__(self, exc):
        self._exc = exc

    def get(self, url, **kwargs):
        raise self._exc


def test_http_auth_rejection_surfaced() -> None:
    with pytest.raises(TossCandlesError):
        fetch_minute_candles(
            "005930", access_token="tok", before=None,
            session=_RaisingSession(_http_error(401)), timeout_s=10.0,
        )


def test_http_error_wrapped() -> None:
    with pytest.raises(TossCandlesError):
        fetch_minute_candles(
            "005930", access_token="tok", before=None,
            session=_RaisingSession(_http_error(500)), timeout_s=10.0,
        )


def test_request_exception_wrapped() -> None:
    import requests

    with pytest.raises(TossCandlesError):
        fetch_minute_candles(
            "005930", access_token="tok", before=None,
            session=_RaisingSession(requests.ConnectionError("down")), timeout_s=10.0,
        )


def test_invalid_token_envelope_rejected() -> None:
    session = _Session({"code": "invalid-token"})
    with pytest.raises(TossCandlesError):
        fetch_minute_candles("005930", access_token="tok", before=None, session=session, timeout_s=10.0)


def test_envelope_without_candles_raises() -> None:
    for body in ({"nope": 1}, {"result": {}}, {"result": {"candles": {}}}):
        with pytest.raises(TossCandlesError):
            fetch_minute_candles("005930", access_token="tok", before=None, session=_Session(body), timeout_s=10.0)


def test_before_param_sent() -> None:
    day = dt.date(2026, 10, 20)
    session = _Session({"result": {"candles": []}})
    before = dt.datetime(2026, 10, 20, 15, 30, 59, tzinfo=_KST)
    fetch_minute_candles("005930", access_token="tok", before=before, session=session, timeout_s=10.0)
    assert session.calls[0]["params"]["before"] == before.isoformat()
    naive = dt.datetime(2026, 10, 20, 15, 30, 59)
    fetch_minute_candles("005930", access_token="tok", before=naive, session=session, timeout_s=10.0)
    assert "+09:00" in session.calls[1]["params"]["before"]


def test_walk_rejects_bad_budget() -> None:
    with pytest.raises(ValueError, match="max_calls"):
        walk_minute_history("A", wanted_days=frozenset({dt.date(2026, 10, 20)}), fetch=lambda before: (), max_calls=0)


def test_walk_empty_wanted() -> None:
    walk = walk_minute_history("A", wanted_days=frozenset(), fetch=lambda before: (), max_calls=10)
    assert walk.calls == 0
    assert walk.complete == {}
    assert walk.vendor_empty == frozenset()


def test_walk_rejects_unordered_pages() -> None:
    day = dt.date(2026, 10, 20)
    with pytest.raises(TossCandlesError):
        walk_minute_history(
            "A",
            wanted_days=frozenset({day}),
            fetch=lambda before: (_candle(day, 9, 0), _candle(day, 9, 1)),
            max_calls=10,
        )


def test_walk_no_progress_raises() -> None:
    day = dt.date(2026, 10, 20)
    with pytest.raises(TossCandlesError):
        walk_minute_history(
            "A",
            wanted_days=frozenset({day}),
            fetch=lambda before: (_candle(day, 16, 10),),
            max_calls=3,
        )


def test_documented_vendor_price_fields_are_parsed() -> None:
    session = _Session({"result": {"candles": [{
        "timestamp": "2026-10-20T00:00:00Z", "openPrice": "100", "highPrice": "102",
        "lowPrice": "99", "closePrice": "101", "volume": "10", "currency": "KRW",
    }]}})
    (bar,) = fetch_minute_candles("005930", access_token="tok", before=None, session=session, timeout_s=1)
    assert (bar.open, bar.high, bar.low, bar.close, bar.volume) == (100, 102, 99, 101, 10)
    assert bar.ts == dt.datetime(2026, 10, 20, 9, tzinfo=_KST)


def test_inclusive_singleton_tail_can_reach_empty_page() -> None:
    day = dt.date(2026, 10, 20)
    bar = _candle(day, 9, 0)
    cursors = []

    def fetch(before):
        cursors.append(before)
        return (bar,) if before >= bar.ts else ()

    walk = walk_minute_history("A", wanted_days=frozenset({day}), fetch=fetch, max_calls=3)
    assert walk.complete[day] == (bar,)
    assert walk.calls == 2
    assert cursors[1] < bar.ts


def test_overlapping_pages_keep_first_values() -> None:
    day = dt.date(2026, 10, 20)
    newest = _candle(day, 10, 0)
    oldest = _candle(day, 9, 0)
    altered = MinuteCandle(newest.ts, 999, 999, 999, 999, 999)
    pages = iter([(newest, oldest), (altered, oldest, _candle(day - dt.timedelta(days=1), 15, 30))])
    walk = walk_minute_history("A", wanted_days=frozenset({day}), fetch=lambda before: next(pages), max_calls=2)
    assert walk.complete[day] == (oldest, newest)


def test_closing_filter_rejects_seconds_after_153000() -> None:
    day = dt.date(2026, 10, 20)
    pages = iter([(_candle(day, 15, 30, 1), _candle(day, 15, 30)), ()])
    walk = walk_minute_history("A", wanted_days=frozenset({day}), fetch=lambda before: next(pages), max_calls=2)
    assert walk.complete[day] == (_candle(day, 15, 30),)
    assert walk.discarded_offhours == 1


def test_non_json_response_is_domain_error() -> None:
    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            raise ValueError("invalid JSON")

    class Session:
        def get(self, *args, **kwargs):
            return Response()

    with pytest.raises(TossCandlesError, match="not JSON"):
        fetch_minute_candles("A", access_token="tok", before=None, session=Session(), timeout_s=1)


def test_vendor_ignoring_cursor_cannot_loop_on_duplicates() -> None:
    day = dt.date(2026, 10, 20)
    with pytest.raises(TossCandlesError, match="no progress"):
        walk_minute_history(
            "A", wanted_days=frozenset({day}), fetch=lambda before: (_candle(day, 9, 0),), max_calls=3,
        )
