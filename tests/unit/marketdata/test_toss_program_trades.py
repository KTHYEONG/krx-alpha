import datetime as dt

import pytest
import requests

from src.marketdata import toss_program_trades as tpt
from src.marketdata.toss_program_trades import TossProgramTradesError


def _record(date: str, buy: str = "100", sell: str = "40", non_buy: str = "200", non_sell: str = "50") -> dict:
    return {
        "date": date,
        "arbitrage": {"buyVolume": buy, "sellVolume": sell, "netBuyVolume": str(int(buy) - int(sell))},
        "nonArbitrage": {"buyVolume": non_buy, "sellVolume": non_sell, "netBuyVolume": str(int(non_buy) - int(non_sell))},
    }


class _Resp:
    def __init__(self, body: dict) -> None:
        self._body = body

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._body


class _Session:
    def __init__(self, body: dict) -> None:
        self._body = body
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, **kwargs: object) -> _Resp:
        self.calls.append((url, kwargs))
        return _Resp(self._body)


def _row(symbol: str, day: dt.date) -> dict[str, object]:
    return {
        "symbol": symbol,
        "date": day,
        "arbitrage_buy_volume": 10,
        "arbitrage_sell_volume": 4,
        "arbitrage_net_volume": 6,
        "non_arbitrage_buy_volume": 20,
        "non_arbitrage_sell_volume": 5,
        "non_arbitrage_net_volume": 15,
    }


def test_fetch_program_trades_page_maps_rows_and_parses_cursor() -> None:
    # Given: 2개 레코드와 nextUntil을 담은 봉투
    session = _Session({"result": {"records": [_record("2026-09-17"), _record("2026-09-16")], "nextUntil": "2026-09-15"}})

    # When
    rows, next_until = tpt.fetch_program_trades_page("005930", access_token="tok", session=session)

    # Then
    assert len(rows) == 2
    assert next_until == dt.date(2026, 9, 15)
    assert rows[0]["arbitrage_net_volume"] == 60
    assert rows[0]["non_arbitrage_net_volume"] == 150
    url, kwargs = session.calls[0]
    assert url == "https://openapi.tossinvest.com/api/v1/stocks/005930/program-trades"
    assert kwargs["headers"] == {"Authorization": "Bearer tok"}
    assert kwargs["params"] == {"count": "100"}


def test_fetch_program_trades_page_rejects_net_identity_violation() -> None:
    # Given: netBuyVolume이 buy-sell과 다른 손상 레코드
    bad = _record("2026-09-17")
    bad["arbitrage"]["netBuyVolume"] = "999"
    session = _Session({"result": {"records": [bad], "nextUntil": None}})

    # When / Then
    with pytest.raises(TossProgramTradesError):
        tpt.fetch_program_trades_page("005930", access_token="tok", session=session)


def test_fetch_program_trades_page_returns_none_cursor_when_absent() -> None:
    # Given: nextUntil 키가 없는 봉투
    session = _Session({"result": {"records": [_record("2026-09-17")]}})

    # When
    rows, next_until = tpt.fetch_program_trades_page("005930", access_token="tok", session=session)

    # Then
    assert len(rows) == 1
    assert next_until is None


def test_backfill_program_trades_history_stops_at_min_date_boundary(monkeypatch) -> None:
    # Given: 페이지1 10행 + 전부 min_date 미만인 페이지2
    page1 = tuple(_row("005930", dt.date(2026, 9, 8) + dt.timedelta(days=i)) for i in range(10))
    page2 = tuple(_row("005930", dt.date(2025, 1, 1) + dt.timedelta(days=i)) for i in range(5))
    pages = [(page1, dt.date(2026, 9, 3)), (page2, None)]
    monkeypatch.setattr(tpt, "fetch_program_trades_page", lambda *a, **k: pages.pop(0))

    # When
    out = tpt.backfill_program_trades_history("005930", access_token="tok", min_date=dt.date(2025, 6, 1))

    # Then: 페이지2 기여분 없이 페이지1 10행만 오름차순
    assert len(out) == 10
    assert [r["date"] for r in out] == sorted(r["date"] for r in out)
    assert all(r["date"] >= dt.date(2025, 6, 1) for r in out)


def test_backfill_program_trades_history_calls_throttle_before_each_page(monkeypatch) -> None:
    # Given: 2페이지 응답과 순서 기록 스파이
    events: list[str] = []
    page1 = (_row("005930", dt.date(2026, 9, 17)),)
    page2 = (_row("005930", dt.date(2026, 9, 16)),)

    def _fake(symbol: str, **kwargs: object) -> tuple[tuple[dict, ...], dt.date | None]:
        events.append("fetch")
        if len(events) == 2:
            return page1, dt.date(2026, 9, 16)
        return page2, None

    monkeypatch.setattr(tpt, "fetch_program_trades_page", _fake)

    # When
    tpt.backfill_program_trades_history(
        "005930",
        access_token="tok",
        min_date=dt.date(2026, 9, 1),
        throttle=lambda: events.append("throttle"),
    )

    # Then: 매 페이지 요청 직전 스로틀 실행
    assert events == ["throttle", "fetch", "throttle", "fetch"]


def test_backfill_program_trades_history_raises_on_stalled_cursor(monkeypatch) -> None:
    # Given: nextUntil이 요청 until과 동일하게 반복되는 커서
    def _fake(symbol: str, **kwargs: object) -> tuple[tuple[dict, ...], dt.date | None]:
        until = kwargs.get("until")
        if until is None:
            return ((_row("005930", dt.date(2026, 9, 17)),), dt.date(2026, 9, 1))
        return ((_row("005930", dt.date(2026, 9, 1)),), dt.date(2026, 9, 1))

    monkeypatch.setattr(tpt, "fetch_program_trades_page", _fake)

    # When / Then
    with pytest.raises(TossProgramTradesError):
        tpt.backfill_program_trades_history("005930", access_token="tok", min_date=dt.date(2020, 1, 1))


def test_backfill_program_trades_history_raises_when_max_pages_exceeded(monkeypatch) -> None:
    # Given: min_date에 닿지 않고 계속 진행 가능한 커서
    def _fake(symbol: str, **kwargs: object) -> tuple[tuple[dict, ...], dt.date | None]:
        until = kwargs.get("until")
        assert isinstance(until, dt.date | None)
        base = until or dt.date(2026, 9, 17)
        return ((_row("005930", base),), base - dt.timedelta(days=1))

    monkeypatch.setattr(tpt, "fetch_program_trades_page", _fake)

    # When / Then
    with pytest.raises(TossProgramTradesError):
        tpt.backfill_program_trades_history("005930", access_token="tok", min_date=dt.date(2020, 1, 1), max_pages=2)


def test_append_program_trades_upserts_by_symbol_and_date(tmp_path) -> None:
    # Given: 같은 키의 구값이 들어있는 스토어
    import polars as pl

    store = tmp_path / "bars" / "program_trades.parquet"
    old = _row("005930", dt.date(2026, 9, 1))
    old["arbitrage_buy_volume"] = 1
    tpt.append_program_trades(store, [old])

    # When: 같은 키 신값 + 새 날짜 1건
    new = _row("005930", dt.date(2026, 9, 1))
    new["arbitrage_buy_volume"] = 999
    added = tpt.append_program_trades(store, [new, _row("005930", dt.date(2026, 9, 2))])

    # Then: 신규 조합만 카운트, 구값은 신값으로 교체
    assert added == 1
    saved = pl.read_parquet(store).sort("date")
    assert saved.height == 2
    assert saved.filter(pl.col("date") == dt.date(2026, 9, 1))["arbitrage_buy_volume"].to_list() == [999]


def test_append_program_trades_empty_rows_writes_nothing(tmp_path) -> None:
    # Given: 파일이 없는 상태의 빈 배치
    store = tmp_path / "bars" / "program_trades.parquet"

    # When
    assert tpt.append_program_trades(store, []) == 0

    # Then: 파일 미생성
    assert not store.exists()


def test_fetch_program_trades_page_sends_until_cursor() -> None:
    # Given: until 상한이 지정된 요청
    session = _Session({"result": {"records": [], "nextUntil": None}})

    # When
    rows, next_until = tpt.fetch_program_trades_page(
        "005930", access_token="tok", until=dt.date(2026, 9, 3), session=session
    )

    # Then: until이 ISO 문자열로 쿼리에 실린다
    assert rows == ()
    assert next_until is None
    assert session.calls[0][1]["params"] == {"count": "100", "until": "2026-09-03"}


def test_fetch_program_trades_page_wraps_transport_failure() -> None:
    # Given: 전송 계층이 실패하는 세션
    import requests

    class _BrokenSession:
        def get(self, *args: object, **kwargs: object) -> object:
            raise requests.RequestException("boom")

    # When / Then
    with pytest.raises(TossProgramTradesError, match="request failed"):
        tpt.fetch_program_trades_page("005930", access_token="tok", session=_BrokenSession())


def test_fetch_program_trades_page_rejects_malformed_envelope_and_records() -> None:
    # Given: 봉투/레코드 변형 케이스들
    bad_bodies = [
        {"no_result": {}},
        {"result": {"records": {"not": "a-list"}}},
        {"result": {"records": [{"date": "2026-09-17"}]}},
        {"result": {"records": [{"date": "2026-09-17", "arbitrage": None, "nonArbitrage": None}]}},
        {"result": {"records": [{**_record("2026-09-17"), "date": "not-a-date"}]}},
    ]
    corrupt = _record("2026-09-17")
    corrupt["nonArbitrage"]["buyVolume"] = "lots"
    bad_bodies.append({"result": {"records": [corrupt]}})

    # When / Then: 모두 fail-closed
    for body in bad_bodies:
        with pytest.raises(TossProgramTradesError):
            tpt.fetch_program_trades_page("005930", access_token="tok", session=_Session(body))


def test_fetch_program_trades_page_rejects_unparsable_next_cursor() -> None:
    # Given: nextUntil이 ISO 날짜가 아닌 봉투
    session = _Session({"result": {"records": [_record("2026-09-17")], "nextUntil": "yesterday"}})

    # When / Then
    with pytest.raises(TossProgramTradesError, match="nextUntil"):
        tpt.fetch_program_trades_page("005930", access_token="tok", session=session)


def test_backfill_program_trades_history_returns_accumulated_on_empty_page(monkeypatch) -> None:
    # Given: 첫 페이지만 데이터, 두 번째 페이지는 빈 페이지
    page1 = (_row("005930", dt.date(2026, 9, 17)), _row("005930", dt.date(2026, 9, 16)))
    pages: list = [(page1, dt.date(2026, 9, 15)), ((), dt.date(2026, 9, 14))]
    monkeypatch.setattr(tpt, "fetch_program_trades_page", lambda *a, **k: pages.pop(0))

    # When
    out = tpt.backfill_program_trades_history("005930", access_token="tok", min_date=dt.date(2026, 9, 1))

    # Then: 누적분만 오름차순 반환
    assert [r["date"] for r in out] == [dt.date(2026, 9, 16), dt.date(2026, 9, 17)]


def test_append_program_trades_preserves_corrupt_store_and_raises(tmp_path) -> None:
    # Given: 손상된 기존 스토어 파일
    store = tmp_path / "bars" / "program_trades.parquet"
    store.parent.mkdir(parents=True, exist_ok=True)
    store.write_bytes(b"not-a-parquet")

    # When / Then: 원본 보존 + fail-closed
    with pytest.raises(TossProgramTradesError, match="unreadable"):
        tpt.append_program_trades(store, [_row("005930", dt.date(2026, 9, 17))])
    assert store.read_bytes() == b"not-a-parquet"

class _FlakySession:
    """지정한 횟수만큼 전송 계층 예외를 던진 뒤 정상 응답으로 전환되는 가짜 세션."""

    def __init__(self, exc: Exception, *, fail_times: int, body: dict) -> None:
        self._exc = exc
        self._fail_times = fail_times
        self._body = body
        self.calls = 0

    def get(self, url: str, **kwargs: object) -> _Resp:
        self.calls += 1
        if self.calls <= self._fail_times:
            raise self._exc
        return _Resp(self._body)


class _RaisingResp:
    """raise_for_status()에서 HTTPError를 던지는 가짜 응답 (4xx/5xx 재현용)."""

    def __init__(self, status: int) -> None:
        self.status_code = status

    def raise_for_status(self) -> None:
        raise requests.exceptions.HTTPError(f"{self.status_code} Client Error", response=self)

    def json(self) -> dict:
        raise AssertionError("json() must not be called when raise_for_status raises")


class _AlwaysHttpErrorSession:
    def __init__(self, status: int) -> None:
        self._status = status
        self.calls = 0

    def get(self, url: str, **kwargs: object) -> _RaisingResp:
        self.calls += 1
        return _RaisingResp(self._status)


def test_fetch_program_trades_page_retries_transient_connection_error_and_succeeds() -> None:
    # Given: 연결 리셋 2회 후 정상 응답으로 회복되는 세션
    exc = requests.exceptions.ConnectionError("Connection aborted.")
    session = _FlakySession(exc, fail_times=2, body={"result": {"records": [_record("2026-09-17")], "nextUntil": None}})

    # When
    rows, next_until = tpt.fetch_program_trades_page("199730", access_token="tok", session=session)

    # Then: 재시도 끝에 정상 결과, 총 3회 호출
    assert len(rows) == 1
    assert next_until is None
    assert session.calls == 3


def test_fetch_program_trades_page_raises_after_exhausting_retries() -> None:
    # Given: 계속 연결이 끊기는 세션(3회 시도 모두 실패)
    exc = requests.exceptions.ConnectionError("Connection aborted.")
    session = _FlakySession(exc, fail_times=99, body={"result": {"records": [], "nextUntil": None}})

    # When / Then: 재시도 소진 후 fail-closed, 정확히 3회 시도
    with pytest.raises(TossProgramTradesError, match="request failed"):
        tpt.fetch_program_trades_page("199730", access_token="tok", session=session)
    assert session.calls == 3


def test_fetch_program_trades_page_does_not_retry_permanent_http_error() -> None:
    # Given: 상장폐지 종목의 404 (동일 요청을 반복해도 결과가 같은 영구 오류)
    session = _AlwaysHttpErrorSession(404)

    # When / Then: 재시도 없이 즉시 실패, 호출 1회뿐
    with pytest.raises(TossProgramTradesError, match="request failed"):
        tpt.fetch_program_trades_page("094800", access_token="tok", session=session)
    assert session.calls == 1


def test_symbols_needing_backfill_returns_all_when_store_missing(tmp_path) -> None:
    # Given: 존재하지 않는 store 경로
    store = tmp_path / "bars" / "program_trades.parquet"

    # When
    out = tpt.symbols_needing_backfill(store, ("005930", "000660"), dt.date(2026, 5, 20))

    # Then: 입력 순서 그대로 전부 반환
    assert out == ("005930", "000660")


def test_symbols_needing_backfill_returns_empty_for_no_symbols(tmp_path) -> None:
    # Given: 빈 심볼 목록
    store = tmp_path / "bars" / "program_trades.parquet"

    # When / Then: 벤더 호출 없이 즉시 빈 튜플
    assert tpt.symbols_needing_backfill(store, (), dt.date(2026, 5, 20)) == ()


def test_symbols_needing_backfill_skips_fully_covered_symbols(tmp_path) -> None:
    # Given: 005930은 min_date 이전부터, 000660은 이후부터만 존재
    import polars as pl

    store = tmp_path / "bars" / "program_trades.parquet"
    min_date = dt.date(2026, 5, 20)
    tpt.append_program_trades(store, [_row("005930", dt.date(2026, 5, 19)), _row("005930", dt.date(2026, 9, 17))])
    tpt.append_program_trades(store, [_row("000660", dt.date(2026, 5, 21)), _row("000660", dt.date(2026, 9, 17))])
    assert pl.read_parquet(store).height == 4

    # When
    out = tpt.symbols_needing_backfill(store, ("005930", "000660"), min_date)

    # Then: 커버리지 부족분만 반환
    assert out == ("000660",)


def test_symbols_needing_backfill_treats_absent_symbol_as_needing_backfill(tmp_path) -> None:
    # Given: 스토어에 다른 심볼만 존재
    store = tmp_path / "bars" / "program_trades.parquet"
    tpt.append_program_trades(store, [_row("005930", dt.date(2026, 1, 5))])

    # When
    out = tpt.symbols_needing_backfill(store, ("000660",), dt.date(2026, 5, 20))

    # Then: 이력 없는 심볼은 백필 대상
    assert out == ("000660",)


def test_symbols_needing_backfill_preserves_requested_order(tmp_path) -> None:
    # Given: 스토어에 아무 이력도 없음(대상 심볼 부재)
    store = tmp_path / "bars" / "program_trades.parquet"
    tpt.append_program_trades(store, [_row("005930", dt.date(2026, 1, 5))])

    # When
    out = tpt.symbols_needing_backfill(store, ("B", "A"), dt.date(2026, 5, 20))

    # Then: 정렬하지 않고 요청 순서 유지
    assert out == ("B", "A")


def test_symbols_needing_backfill_raises_on_corrupt_store(tmp_path) -> None:
    # Given: 손상된 parquet 바이트 파일
    store = tmp_path / "bars" / "program_trades.parquet"
    store.parent.mkdir(parents=True, exist_ok=True)
    store.write_bytes(b"not-a-parquet")

    # When / Then: fail-closed
    with pytest.raises(TossProgramTradesError):
        tpt.symbols_needing_backfill(store, ("005930",), dt.date(2026, 5, 20))
