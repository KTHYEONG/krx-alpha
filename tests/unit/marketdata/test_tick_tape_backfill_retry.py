"""Invariant guards for tick-tape transient retry, retry pass and attempt ledger (Part 1)."""

from __future__ import annotations

import datetime as dt
import json

import pytest
import requests

from src.core.paths import DataPaths
from src.marketdata import tick_tape_backfill as backfill_mod
from src.marketdata.kiwoom_ticks import (
    KiwoomAuthRejected,
    KiwoomThrottled,
    KiwoomTickTapeError,
    KiwoomTransientError,
    fetch_tick_page,
)
from src.marketdata.tick_tape_backfill import (
    TickTapeTarget,
    read_ledger_attempts,
    read_ledger_done,
    run_tick_tape_backfill,
)
from tests.unit.marketdata.test_tick_tape_backfill import (
    _ChartResp,
    _Session,
    _creds,
    _deadline,
    _evening_now,
    _rows,
    _settings,
    _walk_day_pages,
)

_KST = dt.timezone(dt.timedelta(hours=9))
_EVENING = dt.datetime(2026, 10, 21, 23, 45, tzinfo=_KST)
_DAY = dt.date(2026, 10, 20)
_PREV = dt.date(2026, 10, 17)


class _Resp:
    def __init__(self, body, *, status=200, headers=None):
        self._body = body
        self.status_code = status
        self.headers = headers or {}

    def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


class _PostSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.posts: list[dict] = []

    def post(self, url, **kwargs):
        self.posts.append({"url": url, **kwargs})
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def test_server_error_code_is_transient() -> None:
    body = {"return_code": 7, "return_msg": "서비스를 처리하는 중에 오류가 발생했습니다[1631]"}
    session = _PostSession([_Resp(body, headers={"cont-yn": "N"})])
    with pytest.raises(KiwoomTransientError) as raised:
        fetch_tick_page("005930", access_token="t", app_key="k", next_key="k1", session=session, timeout_s=5.0)
    text = str(raised.value)
    assert "return_code=7" in text
    assert "[1631]" in text
    assert "page=continued" in text
    assert isinstance(raised.value, KiwoomTickTapeError)


def test_http_5xx_unparseable_is_transient() -> None:
    session = _PostSession([_Resp(ValueError("no json"), status=503)])
    with pytest.raises(KiwoomTransientError):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)


def test_transport_timeout_and_connection_are_transient() -> None:
    session = _PostSession([requests.exceptions.ReadTimeout("slow")])
    with pytest.raises(KiwoomTransientError):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)
    session = _PostSession([requests.exceptions.ConnectionError("down")])
    with pytest.raises(KiwoomTransientError):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)


def test_non_transient_failures_keep_class() -> None:
    session = _PostSession([_Resp({"return_code": 9, "return_msg": "bad"}, headers={"cont-yn": "N"})])
    with pytest.raises(KiwoomTickTapeError) as raised:
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)
    assert not isinstance(raised.value, KiwoomTransientError)
    bad = {"return_code": 0, "return_msg": "ok",
           "stk_tic_chart_qry": [{"cntr_tm": "bad", "cur_prc": "1", "trde_qty": "1"}]}
    session = _PostSession([_Resp(bad, headers={"cont-yn": "N"})])
    with pytest.raises(KiwoomTickTapeError) as raised:
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)
    assert not isinstance(raised.value, KiwoomTransientError)
    session = _PostSession([RuntimeError("boom")])
    with pytest.raises(KiwoomTickTapeError) as raised:
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)
    assert not isinstance(raised.value, KiwoomTransientError)


def test_auth_and_throttle_unchanged() -> None:
    session = _PostSession([_Resp({"return_code": 3, "return_msg": "expired"}, headers={"cont-yn": "N"})])
    with pytest.raises(KiwoomAuthRejected):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)
    session = _PostSession([_Resp({"return_code": 5, "return_msg": "limit"}, status=429)])
    with pytest.raises(KiwoomThrottled):
        fetch_tick_page("005930", access_token="t", app_key="k", next_key=None, session=session, timeout_s=5.0)


def _transient_resp():
    return _ChartResp({"return_code": 7, "return_msg": "err[1631]"}, cont="N")


def test_transient_page_retried_on_same_cursor(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    day, prev = _DAY, _PREV
    session = _Session(pages={
        "A": [
            _ChartResp(_rows(day, 2, 150000), cont="Y", next_key="k1"),
            _transient_resp(),
            _transient_resp(),
            _ChartResp(_rows(prev, 1, 150000), cont="N"),
        ]
    })
    sleeps: list[float] = []
    summary = run_tick_tape_backfill(
        targets=[TickTapeTarget(symbol="A", days=(day,), cost_hint=1.0)],
        paths=paths, settings=_settings(transient_retries=3, transient_backoff_s=0.01),
        credentials=_creds(), deadline=_deadline(), session=session,
        now=_evening_now, run_id="retry1", sleep=sleeps.append,
    )
    assert summary.status == "OK"
    assert summary.failed == 0
    assert read_ledger_done(paths.tick_tape_ledger) == frozenset({("A", day)})
    keys = [key for _, key in session.chart_calls]
    assert keys == [None, "k1", "k1", "k1"]
    assert summary.pages == 4


def test_exhausted_retries_fail_only_that_symbol(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    day, prev = _DAY, _PREV
    session = _Session(pages={
        "A": [_transient_resp() for _ in range(10)],
        "B": _walk_day_pages(day, prev),
    })
    summary = run_tick_tape_backfill(
        targets=[
            TickTapeTarget(symbol="A", days=(day,), cost_hint=1.0),
            TickTapeTarget(symbol="B", days=(day,), cost_hint=2.0),
        ],
        paths=paths, settings=_settings(retry_passes=0, transient_backoff_s=0.0),
        credentials=_creds(), deadline=_deadline(), session=session,
        now=_evening_now, run_id="retry2", sleep=lambda s: None,
    )
    assert summary.failed == 1
    assert read_ledger_done(paths.tick_tape_ledger) == frozenset({("B", day)})
    records = [json.loads(line) for line in paths.tick_tape_ledger.read_text().splitlines()]
    failed = [r for r in records if r["status"] == "failed"]
    assert len(failed) == 1
    assert failed[0]["symbol"] == "A"
    assert failed[0]["reason"] == "_SymbolTransientError"
    assert failed[0]["run_id"] == "retry2"


def test_retry_pass_recovers_transient_symbol(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    day, prev = _DAY, _PREV
    calls = {"n": 0}

    class _Flaky(_Session):
        def post(self, url, **kwargs):
            if url.endswith("/oauth2/token"):
                return super().post(url, **kwargs)
            calls["n"] += 1
            if calls["n"] <= 4:
                return _transient_resp()
            body = kwargs.get("json", {})
            key = kwargs.get("headers", {}).get("next-key")
            self.chart_calls.append((str(body.get("stk_cd", "")), key))
            queue = self.pages.get("A", [])
            if queue:
                return queue.pop(0)
            return _ChartResp([], cont="N")

    session = _Flaky(pages={"A": []})
    session.pages = {"A": _walk_day_pages(day, prev)}
    sleeps: list[float] = []
    summary = run_tick_tape_backfill(
        targets=[TickTapeTarget(symbol="A", days=(day,), cost_hint=1.0)],
        paths=paths, settings=_settings(retry_passes=1, retry_pass_delay_s=7.0, transient_backoff_s=0.0),
        credentials=_creds(), deadline=_deadline(), session=session,
        now=_evening_now, run_id="retry3", sleep=sleeps.append,
    )
    assert summary.failed == 0
    assert summary.retried == 1
    assert read_ledger_done(paths.tick_tape_ledger) == frozenset({("A", day)})
    records = [json.loads(line) for line in paths.tick_tape_ledger.read_text().splitlines()]
    assert not [r for r in records if r["status"] == "failed"]
    assert 7.0 in sleeps


def test_non_retryable_failure_not_retried(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    page = _ChartResp(_rows(_DAY, 1), cont="Y", next_key="same")
    session = _Session(pages={"A": [page, page]})
    sleeps: list[float] = []
    summary = run_tick_tape_backfill(
        targets=[TickTapeTarget(symbol="A", days=(_DAY,), cost_hint=1.0)],
        paths=paths, settings=_settings(retry_passes=1, transient_backoff_s=0.0),
        credentials=_creds(), deadline=_deadline(), session=session,
        now=_evening_now, run_id="retry4", sleep=sleeps.append,
    )
    assert summary.failed == 1
    assert summary.retried == 0
    assert len(session.chart_calls) == 2
    records = [json.loads(line) for line in paths.tick_tape_ledger.read_text().splitlines()]
    assert any(r["status"] == "failed" for r in records)


def test_retries_never_overrun_deadline(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    day, prev = _DAY, _PREV
    evening = _EVENING
    deadline = evening + dt.timedelta(seconds=5)
    clock = {"t": evening}
    sleeps: list[float] = []

    def now():
        return clock["t"]

    def fake_sleep(s: float) -> None:
        sleeps.append(s)
        clock["t"] = deadline

    session = _Session(pages={
        "A": [
            _ChartResp(_rows(day, 1, 150000), cont="Y", next_key="k1"),
            _transient_resp(),
        ]
    })
    summary = run_tick_tape_backfill(
        targets=[TickTapeTarget(symbol="A", days=(day,), cost_hint=1.0)],
        paths=paths,
        settings=_settings(transient_retries=3, transient_backoff_s=60.0, retry_passes=0),
        credentials=_creds(), deadline=deadline, session=session,
        now=now, run_id="retry5", sleep=fake_sleep,
    )
    assert sleeps
    assert max(sleeps) <= 5.0 + 1e-9
    assert len(session.chart_calls) == 2
    assert summary.failed == 0
    assert read_ledger_done(paths.tick_tape_ledger) == frozenset()


def test_boundary_interruption_writes_no_failed_record(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    day = _DAY
    older = _DAY - dt.timedelta(days=3)
    session = _Session(pages={
        "A": [
            _ChartResp(_rows(day, 1, 150000), cont="Y", next_key="k1"),
            _ChartResp(_rows(older, 1, 150000), cont="N"),
        ]
    })
    summary = run_tick_tape_backfill(
        targets=[TickTapeTarget(symbol="A", days=(older,), cost_hint=1.0)],
        paths=paths, settings=_settings(max_pages_per_run=1),
        credentials=_creds(), deadline=_deadline(), session=session,
        now=_evening_now, run_id="retry6", sleep=lambda s: None,
    )
    assert summary.failed == 0
    assert read_ledger_done(paths.tick_tape_ledger) == frozenset()
    if paths.tick_tape_ledger.exists():
        records = [json.loads(line) for line in paths.tick_tape_ledger.read_text().splitlines()]
        assert not [r for r in records if r["status"] in ("complete", "failed")]


def test_attempt_reader_counts_distinct_base_runs(tmp_path) -> None:
    ledger = tmp_path / "ledger.jsonl"
    day = _DAY.isoformat()
    lines = [
        {"date": day, "symbol": "005930", "status": "failed", "rows": 0, "run_id": "aaa"},
        {"date": day, "symbol": "005930", "status": "edge_partial", "rows": 0, "run_id": "aaa-1"},
        {"date": day, "symbol": "005930", "status": "failed", "rows": 0, "run_id": "bbb"},
        {"date": day, "symbol": "005930", "status": "complete", "rows": 2, "run_id": "ccc"},
    ]
    ledger.write_text("\n".join(json.dumps(r) for r in lines) + "\n")
    assert read_ledger_attempts(ledger) == {(("005930", _DAY)): 2}
    assert read_ledger_done(ledger) == frozenset({("005930", _DAY)})


def test_attempt_reader_torn_tail_tolerance(tmp_path) -> None:
    ledger = tmp_path / "ledger.jsonl"
    day = _DAY.isoformat()
    good = {"date": day, "symbol": "A", "status": "failed", "rows": 0, "run_id": "aaa"}
    ledger.write_bytes((json.dumps(good) + "\n{truncated").encode())
    assert read_ledger_attempts(ledger) == {(("A", _DAY)): 1}
    ledger.write_bytes((json.dumps(good) + "\n{broken}\n" + json.dumps(good) + "\n").encode())
    with pytest.raises(ValueError, match="corrupt"):
        read_ledger_attempts(ledger)
    assert backfill_mod.read_ledger_attempts is read_ledger_attempts


def test_attempt_reader_skips_noise_and_missing_file(tmp_path) -> None:
    assert read_ledger_attempts(tmp_path / "absent.jsonl") == {}
    ledger = tmp_path / "ledger.jsonl"
    day = _DAY.isoformat()
    good = {"date": day, "symbol": "A", "status": "failed", "rows": 0, "run_id": "aaa"}
    ledger.write_text(
        "\n".join(
            [
                "",
                json.dumps([1, 2]),
                json.dumps({"date": "not-a-date", "symbol": "A", "status": "failed", "run_id": "aaa"}),
                json.dumps({"date": day, "symbol": "A", "status": "complete", "run_id": "aaa"}),
                json.dumps(good),
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    assert read_ledger_attempts(ledger) == {(("A", _DAY)): 1}


def test_retry_pass_stops_when_boundary_fails(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    day, prev = _DAY, _PREV
    session = _Session(pages={
        "A": [_transient_resp()],
        "B": [_transient_resp()],
    })
    session.pages["A"].append(_ChartResp([], cont="N"))
    calls_before = 2

    def should_stop() -> bool:
        return len(session.chart_calls) >= calls_before

    summary = run_tick_tape_backfill(
        targets=[
            TickTapeTarget(symbol="A", days=(day,), cost_hint=1.0),
            TickTapeTarget(symbol="B", days=(day,), cost_hint=2.0),
        ],
        paths=paths,
        settings=_settings(retry_passes=1, retry_pass_delay_s=0.0, transient_retries=0, transient_backoff_s=0.0),
        credentials=_creds(), deadline=_deadline(), session=session,
        now=_evening_now, run_id="retrystop", should_stop=should_stop, sleep=lambda s: None,
    )
    assert summary.failed >= 1
    assert summary.retried == 0


def test_retry_stops_before_late_symbol_on_stop(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    day, prev = _DAY, _PREV
    session = _Session(pages={
        "A": [_transient_resp(), *_walk_day_pages(day, prev)],
        "B": [_transient_resp(), *_walk_day_pages(day, prev)],
    })
    settings = _settings(retry_passes=1, retry_pass_delay_s=0.0, transient_retries=0, transient_backoff_s=0.0)

    def should_stop() -> bool:
        return len(session.chart_calls) >= 4

    summary = run_tick_tape_backfill(
        targets=[
            TickTapeTarget(symbol="A", days=(day,), cost_hint=1.0),
            TickTapeTarget(symbol="B", days=(day,), cost_hint=2.0),
        ],
        paths=paths, settings=settings,
        credentials=_creds(), deadline=_deadline(), session=session,
        now=_evening_now, run_id="retrystop2", should_stop=should_stop, sleep=lambda s: None,
    )
    assert summary.retried == 1
    assert summary.failed == 1


def test_retry_auth_abort(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    day = _DAY
    denied = _ChartResp({"return_code": 3, "return_msg": "expired"}, cont="N")
    session = _Session(pages={"A": [_transient_resp(), denied, denied]})
    summary = run_tick_tape_backfill(
        targets=[TickTapeTarget(symbol="A", days=(day,), cost_hint=1.0)],
        paths=paths,
        settings=_settings(retry_passes=1, retry_pass_delay_s=0.0, transient_retries=0, transient_backoff_s=0.0),
        credentials=_creds(), deadline=_deadline(), session=session,
        now=_evening_now, run_id="retryauth", sleep=lambda s: None,
    )
    assert summary.status == "FAIL"
    assert summary.reason == "auth"


def test_retry_persistence_abort(tmp_path, monkeypatch) -> None:
    paths = DataPaths(tmp_path)
    day, prev = _DAY, _PREV
    session = _Session(pages={"A": [_transient_resp(), *_walk_day_pages(day, prev)]})

    def _fail(*args, **kwargs) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(backfill_mod, "_write_day_chunk", _fail)
    summary = run_tick_tape_backfill(
        targets=[TickTapeTarget(symbol="A", days=(day,), cost_hint=1.0)],
        paths=paths,
        settings=_settings(retry_passes=1, retry_pass_delay_s=0.0, transient_retries=0, transient_backoff_s=0.0),
        credentials=_creds(), deadline=_deadline(), session=session,
        now=_evening_now, run_id="retrypersist", sleep=lambda s: None,
    )
    assert summary.status == "FAIL"
    assert summary.reason == "persistence"


def test_retry_still_fails_keeps_failed_record(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    day = _DAY
    session = _Session(pages={"A": [_transient_resp(), _transient_resp()]})
    summary = run_tick_tape_backfill(
        targets=[TickTapeTarget(symbol="A", days=(day,), cost_hint=1.0)],
        paths=paths,
        settings=_settings(retry_passes=1, retry_pass_delay_s=0.0, transient_retries=0, transient_backoff_s=0.0),
        credentials=_creds(), deadline=_deadline(), session=session,
        now=_evening_now, run_id="retrystill", sleep=lambda s: None,
    )
    assert summary.failed == 1
    assert summary.retried == 0
    records = [json.loads(line) for line in paths.tick_tape_ledger.read_text().splitlines()]
    assert any(r["status"] == "failed" and r["reason"] == "_SymbolTransientError" for r in records)


def test_retry_flush_failure_aborts_persistence(tmp_path, monkeypatch) -> None:
    paths = DataPaths(tmp_path)
    day, prev = _DAY, _PREV
    session = _Session(pages={"A": [_transient_resp(), *_walk_day_pages(day, prev)]})
    monkeypatch.setattr(backfill_mod, "_FLUSH_ROW_LIMIT", 0)

    def _fail(path, lines) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(backfill_mod, "_append_ledger_lines", _fail)
    summary = run_tick_tape_backfill(
        targets=[TickTapeTarget(symbol="A", days=(day,), cost_hint=1.0)],
        paths=paths,
        settings=_settings(retry_passes=1, retry_pass_delay_s=0.0, transient_retries=0, transient_backoff_s=0.0),
        credentials=_creds(), deadline=_deadline(), session=session,
        now=_evening_now, run_id="retryflush", sleep=lambda s: None,
    )
    assert summary.status == "FAIL"
    assert summary.reason == "persistence"


def test_retry_non_retryable_clears_pending(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    day = _DAY
    page = _ChartResp(_rows(day, 1), cont="Y", next_key="same")
    session = _Session(pages={"A": [_transient_resp(), page, page]})
    summary = run_tick_tape_backfill(
        targets=[TickTapeTarget(symbol="A", days=(day,), cost_hint=1.0)],
        paths=paths,
        settings=_settings(retry_passes=1, retry_pass_delay_s=0.0, transient_retries=0, transient_backoff_s=0.0),
        credentials=_creds(), deadline=_deadline(), session=session,
        now=_evening_now, run_id="retrynonret", sleep=lambda s: None,
    )
    assert summary.failed == 1
    assert summary.retried == 0
    records = [json.loads(line) for line in paths.tick_tape_ledger.read_text().splitlines()]
    assert any(r["status"] == "failed" and r["reason"] == "KiwoomTickTapeError" for r in records)


def test_retry_interrupted_by_budget_is_not_recovered(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(backfill_mod.HostPacedRateLimiter, "acquire", lambda self: None)
    paths = DataPaths(tmp_path)
    session = _Session(pages={"A": [_transient_resp(), _ChartResp(_rows(_DAY, 1), cont="Y", next_key="k1")]})
    sleeps: list[float] = []
    summary = run_tick_tape_backfill(
        targets=[TickTapeTarget("A", (_DAY,), 1.0)], paths=paths,
        settings=_settings(max_pages_per_run=2, transient_retries=0, retry_passes=1, retry_pass_delay_s=0),
        credentials=_creds(), deadline=_deadline(), session=session, now=_evening_now,
        run_id="retrybudget", sleep=sleeps.append,
    )
    assert summary.retried == summary.failed == 0
    assert read_ledger_done(paths.tick_tape_ledger) == frozenset()
    assert not paths.tick_tape_ledger.exists()
