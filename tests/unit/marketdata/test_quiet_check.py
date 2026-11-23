"""Quiet-suspect file reading, book-time classification and REST confirmation invariants."""

from __future__ import annotations

import datetime as dt
import json
from zoneinfo import ZoneInfo

import pytest

from src.core.config import QUIET_CHECK_BATCH, QUIET_CHECK_COOLDOWN_S
from src.execution.contracts import KisApiError
from src.marketdata.quiet_check import QuietCheck, classify_book_time, read_quiet_suspects

KST = ZoneInfo("Asia/Seoul")
DAY = dt.date(2026, 11, 24)
WALL = dt.datetime(2026, 11, 24, 10, 0, 0, tzinfo=KST).timestamp()


def _entry(symbol: str, *, kind: str = "quote", venue: str = "krx") -> dict[str, object]:
    return {
        "symbol": symbol,
        "venue": venue,
        "kind": kind,
        "last_frame_wall": WALL,
        "silent_s": 120.0,
    }


def _write(
    path,
    *,
    updated: float = WALL,
    session: str = DAY.isoformat(),
    schema: str = "quiet-suspects.v1",
    suspects: list[dict[str, object]] | None = None,
) -> None:
    payload = {
        "schema": schema,
        "session_date": session,
        "updated_wall": updated,
        "suspects": suspects if suspects is not None else [_entry("000001")],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


class _Clock:
    def __init__(self, start: float) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


class _BookSource:
    def __init__(self, *, book_time: str = "100002", fail: frozenset[str] = frozenset()) -> None:
        self.book_time = book_time
        self.fail = fail
        self.calls: list[str] = []

    def get_auction_book(self, symbol: str) -> dict[str, object]:
        self.calls.append(symbol)
        if symbol in self.fail:
            raise KisApiError("EGW00201", "rate")
        return {"symbol": symbol, "book_time": self.book_time}


def test_book_time_after_last_frame_is_loss_beyond_margin() -> None:
    last = dt.datetime(2026, 11, 24, 10, 0, 0, tzinfo=KST).timestamp()

    assert classify_book_time("100010", last, DAY) == "LOSS"
    assert classify_book_time("100002", last, DAY) == "QUIET"
    assert classify_book_time("100003", last, DAY) == "QUIET"
    assert classify_book_time("100004", last, DAY) == "LOSS"


@pytest.mark.parametrize("book_time", ["", "10000", "1000021", "100060", "250000", "10:00:02", "ab0002"])
def test_invalid_book_time_is_unknown(book_time: str) -> None:
    assert classify_book_time(book_time, WALL, DAY) == "UNKNOWN"


def test_stale_or_foreign_file_ignored(tmp_path) -> None:
    path = tmp_path / "quiet.json"

    _write(path, updated=WALL - 31.0)
    assert read_quiet_suspects(path, DAY, WALL) == ()

    _write(path, session="2026-11-23")
    assert read_quiet_suspects(path, DAY, WALL) == ()

    _write(path, schema="quiet-suspects.v0")
    assert read_quiet_suspects(path, DAY, WALL) == ()

    path.write_text("{not json", encoding="utf-8")
    assert read_quiet_suspects(path, DAY, WALL) == ()
    assert read_quiet_suspects(tmp_path / "absent.json", DAY, WALL) == ()


def test_filters_kind_venue_and_code(tmp_path) -> None:
    path = tmp_path / "quiet.json"
    _write(
        path,
        suspects=[
            _entry("000001"),
            _entry("000002", kind="trade"),
            _entry("000003", venue="nxt"),
            _entry("bad"),
            _entry("000004"),
        ],
    )

    assert [s.symbol for s in read_quiet_suspects(path, DAY, WALL)] == ["000001", "000004"]


def test_batch_and_cooldown(tmp_path) -> None:
    path = tmp_path / "quiet.json"
    symbols = [f"{index:06d}" for index in range(1, 16)]
    _write(path, suspects=[_entry(symbol) for symbol in symbols])
    clock = _Clock(WALL)
    source = _BookSource()
    check = QuietCheck(path, DAY, wall=clock)

    assert check.run(source) == QUIET_CHECK_BATCH
    assert len(source.calls) == 10
    assert check.run(source) == 5
    assert check.run(source) == 0

    clock.now = WALL + QUIET_CHECK_COOLDOWN_S
    _write(path, updated=clock.now, suspects=[_entry(symbol) for symbol in symbols])
    assert check.run(source) == QUIET_CHECK_BATCH


def test_loss_and_unknown_verdicts_do_not_change_call_counts(tmp_path) -> None:
    path = tmp_path / "quiet.json"
    _write(path, suspects=[_entry("000001"), _entry("000002")])
    source = _BookSource(book_time="100010")
    check = QuietCheck(path, DAY, wall=_Clock(WALL))

    assert check.run(source) == 2
    assert source.calls == ["000001", "000002"]


def test_source_error_counted_not_raised(tmp_path) -> None:
    path = tmp_path / "quiet.json"
    _write(path, suspects=[_entry("000001"), _entry("000002")])
    clock = _Clock(WALL)
    source = _BookSource(fail=frozenset({"000001"}))
    check = QuietCheck(path, DAY, wall=clock)

    assert check.run(source) == 2
    assert check.run(source) == 0
    assert source.calls == ["000001", "000002"]


def test_malformed_payload_entries_are_skipped(tmp_path) -> None:
    path = tmp_path / "quiet.json"
    missing_suspects = {"schema": "quiet-suspects.v1", "session_date": DAY.isoformat(), "updated_wall": WALL}
    path.write_text(json.dumps(missing_suspects), encoding="utf-8")
    assert read_quiet_suspects(path, DAY, WALL) == ()

    broken = _entry("000002")
    del broken["silent_s"]
    _write(path, suspects=["not-an-object", broken, {**_entry("000003"), "last_frame_wall": "x"}, _entry("000004")])
    assert [s.symbol for s in read_quiet_suspects(path, DAY, WALL)] == ["000004"]
