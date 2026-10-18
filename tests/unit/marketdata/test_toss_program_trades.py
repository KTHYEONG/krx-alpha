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

    store = tmp_path / "bars" / "program_trades"
    old = _row("005930", dt.date(2026, 9, 1))
    old["arbitrage_buy_volume"] = 1
    tpt.append_program_trades(store, [old])

    # When: 같은 키 신값 + 새 날짜 1건
    new = _row("005930", dt.date(2026, 9, 1))
    new["arbitrage_buy_volume"] = 999
    added = tpt.append_program_trades(store, [new, _row("005930", dt.date(2026, 9, 2))])

    # Then: 신규 조합만 카운트, 구값은 신값으로 교체
    from src.marketdata.partitioned_store import scan_month_partitions

    assert added == 1
    saved = scan_month_partitions(store).collect().sort("date")
    assert saved.height == 2
    assert saved.filter(pl.col("date") == dt.date(2026, 9, 1))["arbitrage_buy_volume"].to_list() == [999]


def test_append_program_trades_empty_rows_writes_nothing(tmp_path) -> None:
    # Given: 파일이 없는 상태의 빈 배치
    store = tmp_path / "bars" / "program_trades"

    # When
    assert tpt.append_program_trades(store, []) == 0

    # Then: 파일 미생성
    assert list(store.glob("*.parquet")) == []


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
    # Given: 손상된 기존 스토어 파티션
    store = tmp_path / "bars" / "program_trades"
    store.mkdir(parents=True, exist_ok=True)
    (store / "2026-09.parquet").write_bytes(b"not-a-parquet")

    # When / Then: 원본 보존 + fail-closed
    with pytest.raises(TossProgramTradesError, match="unreadable"):
        tpt.append_program_trades(store, [_row("005930", dt.date(2026, 9, 17))])
    assert (store / "2026-09.parquet").read_bytes() == b"not-a-parquet"

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
    store = tmp_path / "bars" / "program_trades"

    # When
    out = tpt.symbols_needing_backfill(store, ("005930", "000660"), dt.date(2026, 5, 20))

    # Then: 입력 순서 그대로 전부 반환
    assert out == ("005930", "000660")


def test_symbols_needing_backfill_returns_empty_for_no_symbols(tmp_path) -> None:
    # Given: 빈 심볼 목록
    store = tmp_path / "bars" / "program_trades"

    # When / Then: 벤더 호출 없이 즉시 빈 튜플
    assert tpt.symbols_needing_backfill(store, (), dt.date(2026, 5, 20)) == ()


def test_symbols_needing_backfill_skips_fully_covered_symbols(tmp_path) -> None:
    # Given: 005930은 min_date 이전부터, 000660은 이후부터만 존재
    from src.marketdata.partitioned_store import scan_month_partitions

    store = tmp_path / "bars" / "program_trades"
    min_date = dt.date(2026, 5, 20)
    tpt.append_program_trades(store, [_row("005930", dt.date(2026, 5, 19)), _row("005930", dt.date(2026, 9, 17))])
    tpt.append_program_trades(store, [_row("000660", dt.date(2026, 5, 21)), _row("000660", dt.date(2026, 9, 17))])
    assert scan_month_partitions(store).collect().height == 4

    # When
    out = tpt.symbols_needing_backfill(store, ("005930", "000660"), min_date)

    # Then: 커버리지 부족분만 반환
    assert out == ("000660",)


def test_symbols_needing_backfill_treats_absent_symbol_as_needing_backfill(tmp_path) -> None:
    # Given: 스토어에 다른 심볼만 존재
    store = tmp_path / "bars" / "program_trades"
    tpt.append_program_trades(store, [_row("005930", dt.date(2026, 1, 5))])

    # When
    out = tpt.symbols_needing_backfill(store, ("000660",), dt.date(2026, 5, 20))

    # Then: 이력 없는 심볼은 백필 대상
    assert out == ("000660",)


def test_symbols_needing_backfill_preserves_requested_order(tmp_path) -> None:
    # Given: 스토어에 아무 이력도 없음(대상 심볼 부재)
    store = tmp_path / "bars" / "program_trades"
    tpt.append_program_trades(store, [_row("005930", dt.date(2026, 1, 5))])

    # When
    out = tpt.symbols_needing_backfill(store, ("B", "A"), dt.date(2026, 5, 20))

    # Then: 정렬하지 않고 요청 순서 유지
    assert out == ("B", "A")


def test_symbols_needing_backfill_raises_on_corrupt_store(tmp_path) -> None:
    # Given: 손상된 parquet 바이트 파티션
    store = tmp_path / "bars" / "program_trades"
    store.mkdir(parents=True, exist_ok=True)
    (store / "2026-09.parquet").write_bytes(b"not-a-parquet")

    # When / Then: fail-closed
    with pytest.raises(TossProgramTradesError):
        tpt.symbols_needing_backfill(store, ("005930",), dt.date(2026, 5, 20))


def test_append_program_trades_rejects_file_path_store(tmp_path) -> None:
    store = tmp_path / "program_trades.parquet"
    store.write_bytes(b"legacy bytes")

    with pytest.raises(TossProgramTradesError, match="unreadable"):
        tpt.append_program_trades(store, [_row("005930", dt.date(2026, 9, 17))])
    assert store.read_bytes() == b"legacy bytes"


def test_symbols_needing_backfill_rejects_file_path_store(tmp_path) -> None:
    store = tmp_path / "program_trades.parquet"
    store.write_bytes(b"legacy bytes")

    with pytest.raises(TossProgramTradesError, match="unreadable"):
        tpt.symbols_needing_backfill(store, ("005930",), dt.date(2026, 5, 20))


def test_program_trade_coverage_without_store_is_empty(tmp_path) -> None:
    from src.marketdata.toss_program_trades import program_trade_coverage

    assert program_trade_coverage(tmp_path / "missing", ("005930",)) == {}


def test_program_trade_coverage_rejects_unreadable_store(tmp_path) -> None:
    from src.marketdata.toss_program_trades import program_trade_coverage

    store = tmp_path / "program_trades"
    store.mkdir(parents=True)
    (store / "2026-09.parquet").write_bytes(b"not-a-parquet")

    with pytest.raises(TossProgramTradesError, match="unreadable"):
        program_trade_coverage(store, ("005930",))


def test_program_trade_coverage_rejects_file_path_store(tmp_path) -> None:
    from src.marketdata.toss_program_trades import program_trade_coverage

    store = tmp_path / "program_trades.parquet"
    store.write_bytes(b"legacy bytes")

    with pytest.raises(TossProgramTradesError, match="unreadable"):
        program_trade_coverage(store, ("005930",))


def test_backfill_program_trades_history_uses_one_page_when_gap_fits(monkeypatch) -> None:
    # Given: 첫 페이지가 min_date 이후(신규 8일)와 이전(이미 저장된 날짜)을 함께 담는 증분 동기화 상황
    page1 = tuple(_row("005930", dt.date(2026, 9, 25) - dt.timedelta(days=i)) for i in range(20))
    calls: list[int] = []

    def _fake(symbol: str, **kwargs: object) -> tuple[tuple[dict, ...], dt.date | None]:
        calls.append(1)
        return page1, dt.date(2026, 9, 5)

    monkeypatch.setattr(tpt, "fetch_program_trades_page", _fake)

    # When
    out = tpt.backfill_program_trades_history("005930", access_token="tok", min_date=dt.date(2026, 9, 18))

    # Then: 더 오래된 페이지는 요청하지 않고 신규 날짜만 반환한다
    assert len(calls) == 1
    assert [r["date"] for r in out] == [dt.date(2026, 9, 18) + dt.timedelta(days=i) for i in range(8)]


def test_fetch_program_trades_page_raises_auth_rejected_on_401() -> None:
    # Given: 401을 돌려주는 세션
    import requests

    import pytest

    from src.marketdata.toss_program_trades import _TossProgramTradesAuthRejected

    class _Resp:
        status_code = 401

        def raise_for_status(self) -> None:
            err = requests.HTTPError("401 Unauthorized")
            err.response = self
            raise err

        def json(self) -> dict:
            raise AssertionError("no body on 401")

    class _Session:
        def get(self, url: str, **kwargs: object) -> _Resp:
            return _Resp()

    # When / Then: 회전 신호로 표면화된다
    with pytest.raises(_TossProgramTradesAuthRejected):
        tpt.fetch_program_trades_page("005930", access_token="stale", session=_Session())


def test_fetch_program_trades_page_wraps_non_401_http_error() -> None:
    # Given: 500을 돌려주는 세션
    import requests

    import pytest

    class _Resp:
        status_code = 500

        def raise_for_status(self) -> None:
            err = requests.HTTPError("500 Server Error")
            err.response = self
            raise err

        def json(self) -> dict:
            raise AssertionError("no body on 500")

    class _Session:
        def get(self, url: str, **kwargs: object) -> _Resp:
            return _Resp()

    # When / Then: 일반 전송 실패로 표면화된다
    with pytest.raises(TossProgramTradesError, match="request failed"):
        tpt.fetch_program_trades_page("005930", access_token="tok", session=_Session())


def test_fetch_program_trades_page_raises_auth_rejected_on_invalid_token_envelope() -> None:
    # Given: invalid-token 봉투를 돌려주는 세션
    import pytest

    from src.marketdata.toss_program_trades import _TossProgramTradesAuthRejected

    session = _Session({"error": {"code": "invalid-token", "message": "stale"}})

    # When / Then
    with pytest.raises(_TossProgramTradesAuthRejected):
        tpt.fetch_program_trades_page("005930", access_token="stale", session=session)


def test_fetch_program_trades_page_rejects_non_mapping_body() -> None:
    # Given: 리스트 본문을 돌려주는 세션
    import pytest

    class _ListResp:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> list:
            return ["not", "a", "mapping"]

    class _ListSession:
        def get(self, url: str, **kwargs: object) -> _ListResp:
            return _ListResp()

    # When / Then: 봉투 위반으로 표면화된다
    with pytest.raises(TossProgramTradesError, match="envelope invalid"):
        tpt.fetch_program_trades_page("005930", access_token="tok", session=_ListSession())


_D1 = dt.date(2026, 9, 1)
_D2 = dt.date(2026, 9, 2)
_D3 = dt.date(2026, 9, 3)
_D4 = dt.date(2026, 9, 4)
_D5 = dt.date(2026, 9, 7)


def _seed_bars(bars_dir, entries: dict[str, list[tuple[dt.date, int]]]) -> None:
    import polars as pl

    from src.marketdata.partitioned_store import upsert_month_partitions

    rows = [(symbol, day, volume) for symbol, days in entries.items() for day, volume in days]
    frame = pl.DataFrame(
        {
            "date": [row[1] for row in rows],
            "symbol": [row[0] for row in rows],
            "close": [1000.0] * len(rows),
            "volume": [row[2] for row in rows],
            "trade_value_100m": [100.0] * len(rows),
            "daily_change_pct": [1.0] * len(rows),
        },
        schema={
            "date": pl.Date,
            "symbol": pl.String,
            "close": pl.Float64,
            "volume": pl.Int64,
            "trade_value_100m": pl.Float64,
            "daily_change_pct": pl.Float64,
        },
    )
    upsert_month_partitions(bars_dir, frame, key_columns=("date", "symbol"), sort_columns=("date", "symbol"))


def test_find_program_trade_gaps_detects_interior_gap(tmp_path) -> None:
    # Given: A가 D2에만 프로그램매매 행이 없는 저장/일봉 스토어
    bars = tmp_path / "bars" / "daily"
    store = tmp_path / "bars" / "program_trades"
    _seed_bars(bars, {"A": [(_D1, 1000), (_D2, 1000), (_D3, 1000)]})
    tpt.append_program_trades(store, [_row("A", _D1), _row("A", _D3)])

    # When
    report = tpt.find_program_trade_gaps(
        store, bars_daily_dir=bars, coverage={"A": (_D1, _D3)}, complete_through=_D3, window_business_days=20
    )

    # Then
    assert report.status is tpt.ProgramTradeGapStatus.GAPS
    assert report.window == (_D1, _D2, _D3)
    assert report.missing == {"A": (_D2,)}
    assert report.missing_pairs == 1
    assert report.earliest_missing == _D2


def test_find_program_trade_gaps_holiday_is_never_expected(tmp_path) -> None:
    # Given: 휴장일 D2에는 일봉 자체가 없는 캘린더
    bars = tmp_path / "bars" / "daily"
    store = tmp_path / "bars" / "program_trades"
    _seed_bars(bars, {"A": [(_D1, 1000), (_D3, 1000)]})
    tpt.append_program_trades(store, [_row("A", _D1), _row("A", _D3)])

    # When
    report = tpt.find_program_trade_gaps(
        store, bars_daily_dir=bars, coverage={"A": (_D1, _D3)}, complete_through=_D3, window_business_days=20
    )

    # Then
    assert report.status is tpt.ProgramTradeGapStatus.COMPLETE
    assert report.missing == {}
    assert report.earliest_missing is None


def test_find_program_trade_gaps_halted_day_is_never_expected(tmp_path) -> None:
    # Given: D2 일봉 거래량이 0인 정지일
    bars = tmp_path / "bars" / "daily"
    store = tmp_path / "bars" / "program_trades"
    _seed_bars(bars, {"A": [(_D1, 1000), (_D2, 0), (_D3, 1000)]})
    tpt.append_program_trades(store, [_row("A", _D1), _row("A", _D3)])

    # When
    report = tpt.find_program_trade_gaps(
        store, bars_daily_dir=bars, coverage={"A": (_D1, _D3)}, complete_through=_D3, window_business_days=20
    )

    # Then
    assert report.status is tpt.ProgramTradeGapStatus.COMPLETE


def test_find_program_trade_gaps_history_starts_inside_window(tmp_path) -> None:
    # Given: A의 첫 프로그램매매 행이 D3라 D1/D2는 기대 대상이 아니다
    bars = tmp_path / "bars" / "daily"
    store = tmp_path / "bars" / "program_trades"
    _seed_bars(bars, {"A": [(_D1, 1000), (_D2, 1000), (_D3, 1000)]})
    tpt.append_program_trades(store, [_row("A", _D3)])

    # When
    report = tpt.find_program_trade_gaps(
        store, bars_daily_dir=bars, coverage={"A": (_D3, _D3)}, complete_through=_D3, window_business_days=20
    )

    # Then
    assert report.status is tpt.ProgramTradeGapStatus.COMPLETE


def test_find_program_trade_gaps_detects_gap_at_window_edge(tmp_path) -> None:
    # Given: K=3에서 window[0]인 D3가 비어 있는 스토어
    bars = tmp_path / "bars" / "daily"
    store = tmp_path / "bars" / "program_trades"
    _seed_bars(bars, {"A": [(_D1, 1000), (_D2, 1000), (_D3, 1000), (_D4, 1000), (_D5, 1000)]})
    tpt.append_program_trades(store, [_row("A", _D1), _row("A", _D2), _row("A", _D4), _row("A", _D5)])

    # When
    report = tpt.find_program_trade_gaps(
        store, bars_daily_dir=bars, coverage={"A": (_D1, _D5)}, complete_through=_D5, window_business_days=3
    )

    # Then
    assert report.window == (_D3, _D4, _D5)
    assert report.missing == {"A": (_D3,)}


def test_find_program_trade_gaps_ignores_gap_outside_window(tmp_path) -> None:
    # Given: K=3에서 window[0] 바로 이전 영업일 D2가 비어 있는 스토어
    bars = tmp_path / "bars" / "daily"
    store = tmp_path / "bars" / "program_trades"
    _seed_bars(bars, {"A": [(_D1, 1000), (_D2, 1000), (_D3, 1000), (_D4, 1000), (_D5, 1000)]})
    tpt.append_program_trades(store, [_row("A", _D1), _row("A", _D3), _row("A", _D4), _row("A", _D5)])

    # When
    report = tpt.find_program_trade_gaps(
        store, bars_daily_dir=bars, coverage={"A": (_D1, _D5)}, complete_through=_D5, window_business_days=3
    )

    # Then
    assert report.status is tpt.ProgramTradeGapStatus.COMPLETE


def test_find_program_trade_gaps_short_calendar_uses_available_days(tmp_path) -> None:
    # Given: 영업일이 5일뿐이고 K=20
    bars = tmp_path / "bars" / "daily"
    store = tmp_path / "bars" / "program_trades"
    _seed_bars(bars, {"A": [(_D1, 1000), (_D2, 1000), (_D3, 1000), (_D4, 1000), (_D5, 1000)]})
    tpt.append_program_trades(store, [_row("A", _D1), _row("A", _D2), _row("A", _D4), _row("A", _D5)])

    # When
    report = tpt.find_program_trade_gaps(
        store, bars_daily_dir=bars, coverage={"A": (_D1, _D5)}, complete_through=_D5, window_business_days=20
    )

    # Then
    assert report.window == (_D1, _D2, _D3, _D4, _D5)
    assert report.missing == {"A": (_D3,)}


def test_find_program_trade_gaps_uncertified_without_bars_store(tmp_path) -> None:
    # Given: 일봉 스토어 자체가 없음
    store = tmp_path / "bars" / "program_trades"
    tpt.append_program_trades(store, [_row("A", _D1), _row("A", _D3)])

    # When
    report = tpt.find_program_trade_gaps(
        store, bars_daily_dir=tmp_path / "missing", coverage={"A": (_D1, _D3)}, complete_through=_D3, window_business_days=20
    )

    # Then: 미인증 캘린더는 COMPLETE로 위장하지 않는다
    assert report.status is tpt.ProgramTradeGapStatus.CALENDAR_UNAVAILABLE
    assert report.window == ()
    assert report.missing == {}


def test_find_program_trade_gaps_uncertified_when_calendar_lags(tmp_path) -> None:
    # Given: 일봉 최신일 D3 < complete_through D4
    bars = tmp_path / "bars" / "daily"
    store = tmp_path / "bars" / "program_trades"
    _seed_bars(bars, {"A": [(_D1, 1000), (_D2, 1000), (_D3, 1000)]})
    tpt.append_program_trades(store, [_row("A", _D1), _row("A", _D3)])

    # When
    report = tpt.find_program_trade_gaps(
        store, bars_daily_dir=bars, coverage={"A": (_D1, _D3)}, complete_through=_D4, window_business_days=20
    )

    # Then
    assert report.status is tpt.ProgramTradeGapStatus.CALENDAR_UNAVAILABLE


def test_find_program_trade_gaps_uncertified_when_no_bar_in_range(tmp_path) -> None:
    # Given: 일봉이 complete_through 이후 날짜만 존재
    bars = tmp_path / "bars" / "daily"
    store = tmp_path / "bars" / "program_trades"
    _seed_bars(bars, {"A": [(_D4, 1000)]})
    tpt.append_program_trades(store, [_row("A", _D4)])

    # When
    report = tpt.find_program_trade_gaps(
        store, bars_daily_dir=bars, coverage={"A": (_D4, _D4)}, complete_through=_D3, window_business_days=20
    )

    # Then
    assert report.status is tpt.ProgramTradeGapStatus.CALENDAR_UNAVAILABLE


def test_find_program_trade_gaps_missing_complete_through_is_a_gap(tmp_path) -> None:
    # Given: A의 마지막 행이 D2인데 complete_through D3는 거래일
    bars = tmp_path / "bars" / "daily"
    store = tmp_path / "bars" / "program_trades"
    _seed_bars(bars, {"A": [(_D1, 1000), (_D2, 1000), (_D3, 1000)]})
    tpt.append_program_trades(store, [_row("A", _D1), _row("A", _D2)])

    # When
    report = tpt.find_program_trade_gaps(
        store, bars_daily_dir=bars, coverage={"A": (_D1, _D2)}, complete_through=_D3, window_business_days=20
    )

    # Then
    assert report.missing == {"A": (_D3,)}


def test_find_program_trade_gaps_ignores_dates_after_complete_through(tmp_path) -> None:
    # Given: D4 일봉이 이미 발행됐지만 complete_through는 D3
    bars = tmp_path / "bars" / "daily"
    store = tmp_path / "bars" / "program_trades"
    _seed_bars(bars, {"A": [(_D1, 1000), (_D2, 1000), (_D3, 1000), (_D4, 1000)]})
    tpt.append_program_trades(store, [_row("A", _D1), _row("A", _D2), _row("A", _D3)])

    # When
    report = tpt.find_program_trade_gaps(
        store, bars_daily_dir=bars, coverage={"A": (_D1, _D3)}, complete_through=_D3, window_business_days=20
    )

    # Then: D4는 기대 대상이 아니다
    assert report.status is tpt.ProgramTradeGapStatus.COMPLETE


def test_find_program_trade_gaps_rejects_non_positive_window(tmp_path) -> None:
    import pytest

    with pytest.raises(ValueError, match="positive"):
        tpt.find_program_trade_gaps(
            tmp_path / "store", bars_daily_dir=tmp_path / "bars", coverage={}, complete_through=_D3, window_business_days=0
        )


def test_find_program_trade_gaps_wraps_corrupt_program_store(tmp_path) -> None:
    # Given: 손상된 프로그램매매 파티션
    import pytest

    bars = tmp_path / "bars" / "daily"
    store = tmp_path / "bars" / "program_trades"
    _seed_bars(bars, {"A": [(_D1, 1000), (_D2, 1000), (_D3, 1000)]})
    store.mkdir(parents=True)
    (store / "2026-09.parquet").write_bytes(b"not-a-parquet")

    # When / Then: fail-closed
    with pytest.raises(TossProgramTradesError, match="unreadable"):
        tpt.find_program_trade_gaps(
            store, bars_daily_dir=bars, coverage={"A": (_D1, _D3)}, complete_through=_D3, window_business_days=20
        )


def test_find_program_trade_gaps_rejects_file_path_store(tmp_path) -> None:
    # Given: 디렉터리가 아닌 레거시 파일 경로
    import pytest

    bars = tmp_path / "bars" / "daily"
    store = tmp_path / "program_trades.parquet"
    _seed_bars(bars, {"A": [(_D1, 1000), (_D2, 1000), (_D3, 1000)]})
    store.write_bytes(b"legacy bytes")

    # When / Then
    with pytest.raises(TossProgramTradesError, match="not a directory"):
        tpt.find_program_trade_gaps(
            store, bars_daily_dir=bars, coverage={"A": (_D1, _D3)}, complete_through=_D3, window_business_days=20
        )


def test_find_program_trade_gaps_treats_absent_store_as_all_missing(tmp_path) -> None:
    # Given: 프로그램매매 스토어가 아직 없음
    bars = tmp_path / "bars" / "daily"
    _seed_bars(bars, {"A": [(_D1, 1000), (_D2, 1000), (_D3, 1000)]})

    # When
    report = tpt.find_program_trade_gaps(
        tmp_path / "bars" / "program_trades",
        bars_daily_dir=bars,
        coverage={"A": (_D1, _D3)},
        complete_through=_D3,
        window_business_days=20,
    )

    # Then: 저장분이 없으니 기대 쌍 전부가 누락이다
    assert report.status is tpt.ProgramTradeGapStatus.GAPS
    assert report.missing == {"A": (_D1, _D2, _D3)}


def test_find_program_trade_gaps_empty_coverage_is_complete(tmp_path) -> None:
    # Given: 점검 대상 종목이 없지만 캘린더는 인증된다
    bars = tmp_path / "bars" / "daily"
    _seed_bars(bars, {"A": [(_D1, 1000), (_D2, 1000), (_D3, 1000)]})

    # When
    report = tpt.find_program_trade_gaps(
        tmp_path / "bars" / "program_trades",
        bars_daily_dir=bars,
        coverage={},
        complete_through=_D3,
        window_business_days=20,
    )

    # Then
    assert report.status is tpt.ProgramTradeGapStatus.COMPLETE
    assert report.window == (_D1, _D2, _D3)


@pytest.mark.parametrize("failure", ["missing_volume", "corrupt_volume", "corrupt_parquet"])
def test_gap_calendar_read_failure_is_unverified(tmp_path, failure) -> None:
    import polars as pl

    bars = tmp_path / "daily"
    bars.mkdir()
    path = bars / "2026-09.parquet"
    if failure == "corrupt_parquet":
        path.write_bytes(b"invalid parquet")
    else:
        frame = pl.DataFrame({"symbol": ["A"], "date": [_D3]})
        if failure == "corrupt_volume":
            frame = frame.with_columns(pl.lit("invalid").alias("volume"))
        frame.write_parquet(path)
    report = tpt.find_program_trade_gaps(
        tmp_path / "program", bars_daily_dir=bars, coverage={"A": (_D1, _D3)},
        complete_through=_D3, window_business_days=20,
    )
    assert report.status is tpt.ProgramTradeGapStatus.CALENDAR_UNAVAILABLE
    assert report.window == ()
    assert report.missing == {}


def test_gap_check_prunes_future_and_old_partitions(tmp_path) -> None:
    bars = tmp_path / "daily"
    store = tmp_path / "program"
    _seed_bars(bars, {"A": [(_D1, 1), (_D2, 1), (_D3, 1)]})
    tpt.append_program_trades(store, [_row("A", _D1), _row("A", _D3)])
    for root in (bars, store):
        (root / "2026-10.parquet").write_bytes(b"future corruption")
    (store / "2026-08.parquet").write_bytes(b"old corruption")
    report = tpt.find_program_trade_gaps(
        store, bars_daily_dir=bars, coverage={"A": (_D1, _D3)},
        complete_through=_D3, window_business_days=2,
    )
    assert report.window == (_D2, _D3)
    assert report.missing == {"A": (_D2,)}


def test_gap_check_rejects_unreadable_program_directory(tmp_path, monkeypatch) -> None:
    import pathlib

    bars = tmp_path / "daily"
    store = tmp_path / "program"
    _seed_bars(bars, {"A": [(_D3, 1)]})
    store.mkdir()
    original = pathlib.Path.iterdir

    def iterdir(path):
        if path == store:
            raise PermissionError("denied")
        return original(path)

    monkeypatch.setattr(pathlib.Path, "iterdir", iterdir)
    with pytest.raises(TossProgramTradesError, match="unreadable"):
        tpt.find_program_trade_gaps(
            store, bars_daily_dir=bars, coverage={"A": (_D1, _D3)},
            complete_through=_D3, window_business_days=20,
        )
