"""Invariant tests for Kiwoom tick-tape transport and tape walk."""

from __future__ import annotations

import datetime as dt

import pytest

from src.marketdata.kiwoom_ticks import (
    KiwoomAuthRejected,
    KiwoomThrottled,
    KiwoomTickTapeError,
    TapeTick,
    TickPage,
    fetch_tick_page,
    issue_kiwoom_token,
    walk_symbol_tape,
)

_D = dt.date(2026, 10, 21)


class _Resp:
    def __init__(self, body, *, status=200, headers=None):
        self._body = body
        self.status_code = status
        self.headers = headers or {}

    def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


class _Session:
    def __init__(self, responses):
        self._responses = list(responses)
        self.posts: list[dict] = []

    def post(self, url, **kwargs):
        self.posts.append({"url": url, **kwargs})
        return self._responses.pop(0)


def _tick(day, ts_hms, price=1000, volume=5):
    return TapeTick(day=day, ts_hms=ts_hms, price=price, volume=volume)


def _page_body(rows):
    return {"return_code": 0, "return_msg": "ok", "stk_tic_chart_qry": rows}


def test_page_parse_strips_signs_and_splits_cntr_tm() -> None:
    body = _page_body([{"cntr_tm": "20261021090001", "cur_prc": "-71200", "trde_qty": "3"}])
    session = _Session([_Resp(body, headers={"cont-yn": "N"})])
    page = fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)
    assert page.ticks == (TapeTick(day=dt.date(2026, 10, 21), ts_hms=90001, price=71200, volume=3),)
    assert page.next_key is None


def test_malformed_row_raises() -> None:
    bad_tm = _page_body([{"cntr_tm": "202610210900", "cur_prc": "100", "trde_qty": "1"}])
    session = _Session([_Resp(bad_tm, headers={"cont-yn": "N"})])
    with pytest.raises(KiwoomTickTapeError):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)
    zero_price = _page_body([{"cntr_tm": "20261021090001", "cur_prc": "0", "trde_qty": "1"}])
    session = _Session([_Resp(zero_price, headers={"cont-yn": "N"})])
    with pytest.raises(KiwoomTickTapeError):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)


def test_continuation_only_on_cont_yn_y() -> None:
    body = _page_body([{"cntr_tm": "20261021090001", "cur_prc": "100", "trde_qty": "1"}])
    session = _Session([_Resp(body, headers={"cont-yn": "N", "next-key": "stale"})])
    page = fetch_tick_page("005930", access_token="t", app_key="k", next_key="prev", session=session, timeout_s=5.0)
    assert page.next_key is None


def test_auth_and_throttle_classes() -> None:
    denied = {"return_code": 3, "return_msg": "token expired"}
    session = _Session([_Resp(denied, headers={"cont-yn": "N"})])
    with pytest.raises(KiwoomAuthRejected):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)
    throttled = {"return_code": 5, "return_msg": "over limit"}
    session = _Session([_Resp(throttled, status=429, headers={"cont-yn": "N"})])
    with pytest.raises(KiwoomThrottled):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)


def test_revoke_never_called() -> None:
    session = _Session(
        [
            _Resp({"return_code": 3, "return_msg": "expired"}, headers={"cont-yn": "N"}),
        ]
    )
    with pytest.raises(KiwoomAuthRejected):
        fetch_tick_page("005930", access_token="tok", app_key="k", next_key=None, session=session, timeout_s=5.0)
    assert all("/oauth2/revoke" not in call["url"] for call in session.posts)


def test_issue_token_success_and_failure() -> None:
    session = _Session([_Resp({"return_code": 0, "return_msg": "ok", "token": "tok"})])
    assert issue_kiwoom_token(app_key="k", secret_key="s", session=session, timeout_s=5.0) == "tok"
    session = _Session([_Resp({"return_code": 1, "return_msg": "bad"})])
    with pytest.raises(KiwoomTickTapeError):
        issue_kiwoom_token(app_key="k", secret_key="s", session=session, timeout_s=5.0)


def _walk_pages(pages: list[TickPage]):
    calls: list[str | None] = []
    queue = list(pages)

    def fetch_page(next_key: str | None) -> TickPage:
        calls.append(next_key)
        return queue.pop(0)

    return fetch_page, calls


def test_crossed_day_is_complete_and_chronological() -> None:
    d0, d1, d2 = _D, _D - dt.timedelta(days=1), _D - dt.timedelta(days=2)
    pages = [
        TickPage(ticks=(_tick(d0, 150000), _tick(d1, 150000), _tick(d1, 145959)), next_key="k1"),
        TickPage(ticks=(_tick(d1, 90001), _tick(d2, 150000)), next_key=None),
    ]
    fetch_page, _ = _walk_pages(pages)
    walk = walk_symbol_tape("005930", wanted_days=frozenset({d1}), fetch_page=fetch_page, max_pages=10)
    assert walk.termination == "crossed"
    assert set(walk.complete) == {d1}
    ordered = [tick.ts_hms for tick in walk.complete[d1]]
    assert ordered == sorted(ordered) == [90001, 145959, 150000]


def test_retention_edge_is_partial() -> None:
    edge = _D - dt.timedelta(days=20)
    pages = [TickPage(ticks=(_tick(edge, 120000), _tick(edge, 110000)), next_key=None)]
    fetch_page, _ = _walk_pages(pages)
    walk = walk_symbol_tape("005930", wanted_days=frozenset({edge}), fetch_page=fetch_page, max_pages=10)
    assert walk.termination == "tape_end"
    assert edge in walk.edge_partial
    assert edge not in walk.complete


def test_bracketed_zero_trade_day_is_vendor_empty() -> None:
    newer, day, older = _D, _D - dt.timedelta(days=1), _D - dt.timedelta(days=2)
    pages = [
        TickPage(ticks=(_tick(newer, 150000),), next_key="k1"),
        TickPage(ticks=(_tick(older, 150000),), next_key=None),
    ]
    fetch_page, _ = _walk_pages(pages)
    walk = walk_symbol_tape("005930", wanted_days=frozenset({day}), fetch_page=fetch_page, max_pages=10)
    assert day in walk.vendor_empty
    assert day not in walk.complete


def test_page_budget_certifies_only_crossed_days() -> None:
    d0 = _D
    d2 = _D - dt.timedelta(days=2)
    d3 = _D - dt.timedelta(days=3)
    pages = [
        TickPage(ticks=(_tick(d0, 150000),), next_key="k1"),
        TickPage(ticks=(_tick(d2, 150000),), next_key="k2"),
    ]
    fetch_page, _ = _walk_pages(pages)
    walk = walk_symbol_tape(
        "005930", wanted_days=frozenset({d0, d3}), fetch_page=fetch_page, max_pages=2
    )
    assert walk.termination == "page_budget"
    assert d0 in walk.complete
    assert d3 not in walk.complete
    assert d3 not in walk.vendor_empty
    assert d3 not in walk.edge_partial


def test_row_shape_errors_raise() -> None:
    not_list = {"return_code": 0, "return_msg": "ok", "stk_tic_chart_qry": {"cntr_tm": "x"}}
    session = _Session([_Resp(not_list, headers={"cont-yn": "N"})])
    with pytest.raises(KiwoomTickTapeError):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)
    non_mapping = _page_body(["not-a-mapping"])
    session = _Session([_Resp(non_mapping, headers={"cont-yn": "N"})])
    with pytest.raises(KiwoomTickTapeError):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)
    bad_date = _page_body([{"cntr_tm": "20261301090001", "cur_prc": "100", "trde_qty": "1"}])
    session = _Session([_Resp(bad_date, headers={"cont-yn": "N"})])
    with pytest.raises(KiwoomTickTapeError):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)
    bad_qty = _page_body([{"cntr_tm": "20261021090001", "cur_prc": "100", "trde_qty": "0"}])
    session = _Session([_Resp(bad_qty, headers={"cont-yn": "N"})])
    with pytest.raises(KiwoomTickTapeError):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)


def test_string_return_code_and_missing_continuation_key() -> None:
    body = {
        "return_code": "0",
        "return_msg": "ok",
        "stk_tic_chart_qry": [{"cntr_tm": "20261021090001", "cur_prc": "+100", "trde_qty": "1"}],
    }
    session = _Session([_Resp(body, headers={"Cont-Yn": "N"})])
    page = fetch_tick_page("005930", access_token="t", app_key="k", next_key="prev", session=session, timeout_s=5.0)
    assert page.next_key is None
    assert page.ticks[0].price == 100
    session = _Session([_Resp(body, headers={"Cont-Yn": "Y"})])
    with pytest.raises(KiwoomTickTapeError, match="missing next-key"):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key="prev", session=session, timeout_s=5.0)
    denied = {"return_code": True, "return_msg": "weird"}
    session = _Session([_Resp(denied, headers={"cont-yn": "N"})])
    with pytest.raises(KiwoomTickTapeError):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)


def test_token_transport_failure_and_missing_token() -> None:
    class _Boom:
        def post(self, url, **kwargs):
            raise ConnectionError("down")

    with pytest.raises(KiwoomTickTapeError):
        issue_kiwoom_token(app_key="k", secret_key="s", session=_Boom(), timeout_s=5.0)
    session = _Session([_Resp({"return_code": 0, "return_msg": "ok"})])
    with pytest.raises(KiwoomTickTapeError):
        issue_kiwoom_token(app_key="k", secret_key="s", session=session, timeout_s=5.0)
    session = _Session([_Resp(KiwoomTickTapeError("broken envelope"))])
    with pytest.raises(KiwoomTickTapeError):
        issue_kiwoom_token(app_key="k", secret_key="s", session=session, timeout_s=5.0)


def test_http_401_is_auth_rejection() -> None:
    body = {"return_code": 0, "return_msg": "ok", "stk_tic_chart_qry": []}
    session = _Session([_Resp(body, status=401, headers={"cont-yn": "N"})])
    with pytest.raises(KiwoomAuthRejected):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)


def test_falsy_headers_and_non_mapping_body() -> None:
    body = {"return_code": 0, "return_msg": "ok", "stk_tic_chart_qry": []}

    class _NoHeaders(_Resp):
        def __init__(self, payload):
            super().__init__(payload, headers={})
            self.headers = None

    session = _Session([_NoHeaders(body)])
    page = fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)
    assert page.next_key is None
    session = _Session([_Resp(["not", "a", "mapping"])])
    with pytest.raises(KiwoomTickTapeError):
        issue_kiwoom_token(app_key="k", secret_key="s", session=session, timeout_s=5.0)
    session = _Session([_Resp({"return_code": "bad", "return_msg": "weird"})])
    with pytest.raises(KiwoomTickTapeError):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)


def test_malformed_price_and_quantity_strings_raise() -> None:
    bad_price = _page_body([{"cntr_tm": "20261021090001", "cur_prc": "abc", "trde_qty": "1"}])
    session = _Session([_Resp(bad_price, headers={"cont-yn": "N"})])
    with pytest.raises(KiwoomTickTapeError):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)
    bad_qty = _page_body([{"cntr_tm": "20261021090001", "cur_prc": "100", "trde_qty": "many"}])
    session = _Session([_Resp(bad_qty, headers={"cont-yn": "N"})])
    with pytest.raises(KiwoomTickTapeError):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)


def test_empty_symbol_and_passthrough_rejection() -> None:
    session = _Session([])
    with pytest.raises(KiwoomTickTapeError):
        fetch_tick_page("", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)

    class _Rejecting:
        def post(self, url, **kwargs):
            raise KiwoomThrottled("busy")

    with pytest.raises(KiwoomThrottled):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=_Rejecting(), timeout_s=5.0)


def test_base_error_passthrough_and_transport_wrap() -> None:
    class _BaseBoom:
        def post(self, url, **kwargs):
            raise KiwoomTickTapeError("base transport failure")

    with pytest.raises(KiwoomTickTapeError):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=_BaseBoom(), timeout_s=5.0)

    class _ConnBoom:
        def post(self, url, **kwargs):
            raise ConnectionError("down")

    with pytest.raises(KiwoomTickTapeError):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=_ConnBoom(), timeout_s=5.0)


def test_nonzero_return_code_and_null_rows() -> None:
    body = {"return_code": 1, "return_msg": "busy", "stk_tic_chart_qry": []}
    session = _Session([_Resp(body, headers={"cont-yn": "N"})])
    with pytest.raises(KiwoomTickTapeError):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)
    null_rows = {"return_code": 0, "return_msg": "ok", "stk_tic_chart_qry": None}
    session = _Session([_Resp(null_rows, headers={"cont-yn": "N"})])
    with pytest.raises(KiwoomTickTapeError, match="rows are not a list"):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)


def test_walk_guards() -> None:
    with pytest.raises(ValueError, match="max_pages"):
        walk_symbol_tape("005930", wanted_days=frozenset({_D}), fetch_page=lambda key: TickPage((), None), max_pages=0)
    walk = walk_symbol_tape("005930", wanted_days=frozenset(), fetch_page=lambda key: TickPage((), None), max_pages=5)
    assert walk.pages == 0
    assert walk.termination == "crossed"


def test_older_than_edge_is_vendor_empty() -> None:
    newer, mid, old = _D, _D - dt.timedelta(days=2), _D - dt.timedelta(days=5)
    pages = [
        TickPage(ticks=(_tick(newer, 150000),), next_key="k1"),
        TickPage(ticks=(_tick(mid, 150000),), next_key=None),
    ]
    fetch_page, _ = _walk_pages(pages)
    walk = walk_symbol_tape("005930", wanted_days=frozenset({old, newer}), fetch_page=fetch_page, max_pages=10)
    assert walk.termination == "tape_end"
    assert newer in walk.complete
    assert old in walk.vendor_empty
    assert mid in walk.edge_partial


def _many(day, count, base=90000):
    return tuple(_tick(day, base + count - 1 - idx) for idx in range(count))


def test_oversize_stops_without_certifying() -> None:
    newer = _D
    big = _D - dt.timedelta(days=1)
    older = _D - dt.timedelta(days=2)
    pages = [
        TickPage(ticks=(_tick(newer, 150000),), next_key="k1"),
        TickPage(ticks=_many(big, 5, 90000), next_key="k2"),
        TickPage(ticks=(_tick(older, 150000),), next_key=None),
    ]
    fetch_page, _ = _walk_pages(pages)
    emitted: list = []
    walk = walk_symbol_tape(
        "005930", wanted_days=frozenset({newer, big, older}), fetch_page=fetch_page,
        max_pages=10, max_ticks_per_day=3,
        on_day_complete=lambda day, ticks: emitted.append(day),
    )
    assert walk.termination == "oversize"
    assert walk.oversize_day == big
    assert walk.emitted == frozenset({newer})
    for bucket in (walk.complete, walk.vendor_empty, walk.edge_partial):
        assert big not in bucket
        assert older not in bucket


def test_cap_off_is_identical() -> None:
    newer = _D
    big = _D - dt.timedelta(days=1)
    pages = [
        TickPage(ticks=(_tick(newer, 150000), _tick(big, 90003), _tick(big, 90002)), next_key="k1"),
        TickPage(ticks=(_tick(big, 90001), _tick(big, 90000)), next_key=None),
    ]
    fetch_page_a, _ = _walk_pages([TickPage(t, k) for t, k in [(p.ticks, p.next_key) for p in pages]])
    fetch_page_b, _ = _walk_pages([TickPage(t, k) for t, k in [(p.ticks, p.next_key) for p in pages]])
    baseline = walk_symbol_tape("005930", wanted_days=frozenset({big}), fetch_page=fetch_page_a, max_pages=10)
    capped = walk_symbol_tape(
        "005930", wanted_days=frozenset({big}), fetch_page=fetch_page_b, max_pages=10, max_ticks_per_day=None,
    )
    assert capped == baseline


def test_cap_ignores_unwanted_days() -> None:
    wanted = _D - dt.timedelta(days=1)
    huge = _D
    pages = [TickPage(ticks=_many(huge, 10, 90000), next_key="k1"),
             TickPage(ticks=(_tick(wanted, 150000), _tick(wanted, 90000)), next_key=None)]
    fetch_page, _ = _walk_pages(pages)
    walk = walk_symbol_tape(
        "005930", wanted_days=frozenset({wanted}), fetch_page=fetch_page,
        max_pages=10, max_ticks_per_day=3,
    )
    assert walk.termination != "oversize"
    assert walk.oversize_day is None
