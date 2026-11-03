from __future__ import annotations

import pytest

from src.marketdata import kiwoom_ranking as ranking
from src.marketdata.kiwoom_ticks import (
    KiwoomAuthRejected,
    KiwoomThrottled,
    KiwoomTickTapeError,
    KiwoomTransientError,
)


class _Resp:
    def __init__(self, body: dict, *, status: int = 200, headers: dict | None = None) -> None:
        self.status_code = status
        self._body = body
        self.headers = headers or {}

    def json(self) -> dict:
        return self._body


class _Session:
    def __init__(self, responses: list[_Resp] | None = None, *, repeat: _Resp | None = None) -> None:
        self._responses = list(responses or [])
        self._repeat = repeat
        self.calls: list[dict] = []

    def post(self, url: str, *, json: dict, headers: dict, timeout: float) -> _Resp:
        self.calls.append({"url": url, "json": json, "headers": headers})
        if self._repeat is not None:
            return self._repeat
        return self._responses.pop(0)


class _Pacer:
    def __init__(self) -> None:
        self.booked = 0

    async def acquire(self) -> None:
        self.booked += 1


def _item(rank: int, stk_cd: str = "004490_AL", trde_prica: str = "12345", flu_rt: str = "+6.10") -> dict:
    return {
        "now_rank": str(rank),
        "stk_cd": stk_cd,
        "cur_prc": "-12000",
        "flu_rt": flu_rt,
        "trde_prica": trde_prica,
    }


def _page(items: list[dict], *, cont: str = "N", next_key: str | None = None) -> _Resp:
    headers = {"cont-yn": cont}
    if next_key is not None:
        headers["next-key"] = next_key
    return _Resp({"return_code": 0, "trde_prica_upper": items}, headers=headers)


def _fetch(session: _Session, pacer: _Pacer, *, max_pages: int = 3, min_tv: int = 1) -> tuple:
    return ranking.fetch_trade_amount_ranking(
        session=session,
        access_token="TOKEN",
        app_key="APPKEY",
        pacer=pacer,  # type: ignore[arg-type]
        max_pages=max_pages,
        min_trade_value_krw=min_tv,
        timeout_s=5.0,
    )


def test_suffix_stripped_and_unit_converted() -> None:
    session = _Session([_page([_item(1)])])

    rows = _fetch(session, _Pacer())

    assert len(rows) == 1
    assert rows[0].symbol == "004490"
    assert rows[0].trade_value_krw == 12_345_000_000
    assert rows[0].change_pct == pytest.approx(6.10)
    assert rows[0].last_price == 12_000
    headers = session.calls[0]["headers"]
    assert headers["api-id"] == "ka10032"
    assert session.calls[0]["json"] == {"mrkt_tp": "000", "mang_stk_incls": "0", "stex_tp": "1"}


def test_paging_stops_below_threshold_after_one_call() -> None:
    session = _Session([_page([_item(1, trde_prica="100"), _item(2, "000020", trde_prica="1")], cont="Y", next_key="K")])
    pacer = _Pacer()

    rows = _fetch(session, pacer, min_tv=50_000_000)

    assert len(session.calls) == 1
    assert pacer.booked == 1
    assert [row.symbol for row in rows] == ["004490", "000020"]


def test_paging_stops_at_max_pages_and_books_each_call() -> None:
    pages = [
        _page([_item(n, f"00000{n}")], cont="Y", next_key=f"K{n}") for n in range(1, 6)
    ]
    session = _Session(pages)
    pacer = _Pacer()

    rows = _fetch(session, pacer, max_pages=3)

    assert len(session.calls) == 3
    assert pacer.booked == 3
    assert [row.rank for row in rows] == [1, 2, 3]
    assert [call["headers"].get("next-key") for call in session.calls] == [None, "K1", "K2"]


def test_non_monotonic_ranks_are_rejected() -> None:
    session = _Session([_page([_item(5, "000020"), _item(3, "000030")])])

    with pytest.raises(KiwoomTickTapeError):
        _fetch(session, _Pacer())


def test_duplicate_symbols_keep_higher_rank() -> None:
    session = _Session([_page([_item(1), _item(2, flu_rt="-9.00")], cont="Y", next_key="K"), _page([])])

    rows = _fetch(session, _Pacer(), max_pages=2)

    assert [(row.rank, row.symbol, row.change_pct) for row in rows] == [(1, "004490", pytest.approx(6.10))]


def test_nonzero_return_code_is_vendor_error() -> None:
    session = _Session([_Resp({"return_code": 99, "return_msg": "bad"})])

    with pytest.raises(KiwoomTickTapeError):
        _fetch(session, _Pacer())


def test_throttle_is_retried_with_backoff(monkeypatch) -> None:
    slept: list[float] = []
    monkeypatch.setattr(ranking.time, "sleep", slept.append)
    session = _Session(
        [
            _Resp({"return_code": 5}, status=429),
            _page([_item(1)]),
        ]
    )

    rows = _fetch(session, _Pacer())

    assert len(rows) == 1
    assert slept == [ranking.KIWOOM_THROTTLE_BACKOFF_S]


def test_persistent_throttle_raises_after_retries(monkeypatch) -> None:
    monkeypatch.setattr(ranking.time, "sleep", lambda _s: None)
    session = _Session(repeat=_Resp({"return_code": 5}, status=429))

    with pytest.raises(KiwoomThrottled):
        _fetch(session, _Pacer())
    assert len(session.calls) == ranking.KIWOOM_THROTTLE_RETRIES + 1


def test_invalid_symbol_row_is_dropped_not_fatal() -> None:
    session = _Session([_page([_item(1, "!!"), _item(2, "000020")])])

    rows = _fetch(session, _Pacer())

    assert [(row.rank, row.symbol) for row in rows] == [(2, "000020")]


def test_row_missing_field_is_vendor_error() -> None:
    broken = {"stk_cd": "004490", "cur_prc": "1", "flu_rt": "1.0", "trde_prica": "1"}
    session = _Session([_page([broken])])

    with pytest.raises(KiwoomTickTapeError, match="malformed"):
        _fetch(session, _Pacer())


def test_http_5xx_is_transient() -> None:
    session = _Session([_Resp({}, status=503)])

    with pytest.raises(KiwoomTransientError):
        _fetch(session, _Pacer())


def test_http_401_is_auth_rejected() -> None:
    session = _Session([_Resp({}, status=401)])

    with pytest.raises(KiwoomAuthRejected):
        _fetch(session, _Pacer())


def test_transport_exception_is_transient() -> None:
    class _Broken:
        def post(self, url, **kwargs):
            raise OSError("socket reset")

    with pytest.raises(KiwoomTransientError, match="request failed"):
        _fetch(_Broken(), _Pacer())  # type: ignore[arg-type]


def test_unreadable_body_is_vendor_error() -> None:
    class _BadJson(_Resp):
        def json(self) -> dict:
            raise ValueError("not json")

    with pytest.raises(KiwoomTickTapeError, match="unreadable"):
        _fetch(_Session([_BadJson({})]), _Pacer())


def test_missing_return_code_is_vendor_error() -> None:
    with pytest.raises(KiwoomTickTapeError, match="missing return_code"):
        _fetch(_Session([_Resp({"trde_prica_upper": []})]), _Pacer())


@pytest.mark.parametrize(
    ("code", "error"),
    [(3, KiwoomAuthRejected), (7, KiwoomTransientError), (99, KiwoomTickTapeError)],
)
def test_return_code_classification(code: int, error: type[Exception]) -> None:
    with pytest.raises(error):
        _fetch(_Session([_Resp({"return_code": code, "return_msg": "x"})]), _Pacer())


def test_rows_must_be_a_list() -> None:
    with pytest.raises(KiwoomTickTapeError, match="not a list"):
        _fetch(_Session([_Resp({"return_code": 0, "trde_prica_upper": "nope"})]), _Pacer())


def test_continuation_requires_next_key() -> None:
    with pytest.raises(KiwoomTickTapeError, match="missing next-key"):
        _fetch(_Session([_page([_item(1)], cont="Y")]), _Pacer())


def test_max_pages_must_be_positive() -> None:
    with pytest.raises(ValueError, match="max_pages"):
        _fetch(_Session([]), _Pacer(), max_pages=0)


def test_vendor_error_raised_by_transport_propagates_unchanged() -> None:
    class _AuthSession:
        def post(self, url, **kwargs):
            raise KiwoomAuthRejected("token revoked")

    with pytest.raises(KiwoomAuthRejected, match="token revoked"):
        _fetch(_AuthSession(), _Pacer())  # type: ignore[arg-type]


def test_alphanumeric_short_code_is_kept() -> None:
    rows = _fetch(_Session([_page([_item(1, "0147B0")])]), _Pacer())

    assert [row.symbol for row in rows] == ["0147B0"]
