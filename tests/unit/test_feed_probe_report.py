"""Invariant tests for tools/feed_probe_report.py; inputs are small synthetic distributions."""

from __future__ import annotations

import datetime as dt
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from src.storage.journal import L0JournalWriter
from src.storage.l0_audit import ReceivedTick

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_NAME = "feed_probe_report_under_test"
KST = ZoneInfo("Asia/Seoul")


def _load() -> Any:
    if MODULE_NAME in sys.modules:
        return sys.modules[MODULE_NAME]
    spec = importlib.util.spec_from_file_location(MODULE_NAME, REPO_ROOT / "tools" / "feed_probe_report.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


rep = _load()


def _recv_ns(hour: int, minute: int, second: int) -> int:
    return int(dt.datetime(2026, 11, 16, hour, minute, second, tzinfo=KST).timestamp() * 1_000_000_000)


def test_lag_stats_known_distribution() -> None:
    recv = _recv_ns(10, 0, 5)  # second of day 36005
    frames = [
        rep.DecodedFrame(recv, "005930", 36000),  # lag 5
        rep.DecodedFrame(recv, "005930", 36003),  # lag 2
        rep.DecodedFrame(recv, "005930", 36004),  # lag 1
        rep.DecodedFrame(recv, "005930", 36010),  # negative lag clamped to 0
        rep.DecodedFrame(recv, "005930", None),  # missing event time is counted, not fatal
    ]
    stats = rep.lag_stats(frames)
    assert stats.count == 4
    assert stats.missing == 1
    assert stats.p50_s == 1.0
    assert stats.p99_s == 5.0
    assert stats.max_s == 5.0


def test_lag_stats_counts_event_holes() -> None:
    recv = _recv_ns(10, 0, 5)
    stats = rep.lag_stats([rep.DecodedFrame(recv, "005930", 36000), rep.DecodedFrame(recv, "005930", 36002)])
    assert stats.event_hole_seconds == 1


def test_lag_stats_empty_is_none() -> None:
    stats = rep.lag_stats([])
    assert stats.count == 0
    assert stats.p50_s is None
    assert stats.p99_s is None


def _ticks(cums: list[int], *, symbol: str = "005930", chetime: int = 100000) -> list[ReceivedTick]:
    return [ReceivedTick(symbol, chetime, cum, 1000, 1) for cum in cums]


def test_conflation_identical_is_one() -> None:
    ref = _ticks(list(range(1, 11)))
    stats = rep.conflation_ratio(ref, ref)
    assert stats.p50_ratio == pytest.approx(1.0)
    assert stats.share_below_threshold == 0.0


def test_conflation_missing_one_of_ten_is_point_nine() -> None:
    ref = _ticks(list(range(1, 11)))
    cand = _ticks([c for c in range(1, 11) if c != 4])
    stats = rep.conflation_ratio(cand, ref)
    assert stats.p50_ratio == pytest.approx(0.9)
    assert stats.share_below_threshold == 1.0


def test_conflation_duplicate_print_is_flagged_extra() -> None:
    ref = _ticks(list(range(1, 11)))
    cand = _ticks([*range(1, 11), 5])
    stats = rep.conflation_ratio(cand, ref)
    assert stats.extra_minutes == 1
    assert stats.p50_ratio == pytest.approx(1.0)


def test_conflation_conflated_pair_is_below_threshold() -> None:
    ref = _ticks(list(range(1, 11)))
    cand = _ticks([c for c in range(1, 11) if c not in (2, 3)])
    stats = rep.conflation_ratio(cand, ref)
    assert stats.p50_ratio == pytest.approx(0.8)
    assert stats.share_below_threshold == 1.0


def _book(symbol: str, asks: list[int], bids: list[int], *, ask_px: int = 100, bid_px: int = 99) -> Any:
    ask_levels = tuple((ask_px + i, 10) if i < len(asks) else (0, 0) for i in range(10))
    bid_levels = tuple((bid_px - i, 10) if i < len(bids) else (0, 0) for i in range(10))
    return rep.DecodedBook(0, symbol, ask_levels, bid_levels)


def test_book_full_ten_by_ten_is_one() -> None:
    stats = rep.book_completeness([_book("005930", list(range(10)), list(range(10)))])
    assert stats.completeness == pytest.approx(1.0)


def test_book_six_asks_ten_bids_is_point_eight() -> None:
    stats = rep.book_completeness([_book("005930", list(range(6)), list(range(10)))])
    assert stats.completeness == pytest.approx(0.8)


def test_book_crossed_is_counted() -> None:
    stats = rep.book_completeness([_book("005930", [0], [0], ask_px=100, bid_px=101)])
    assert stats.crossed == 1


def test_quiet_symbol_excluded_from_liquid_subset() -> None:
    books = [_book("005930", list(range(10)), list(range(10))), _book("999999", [], [])]
    liquid = [b for b in books if b.symbol == "005930"]
    assert rep.book_completeness(books).completeness == pytest.approx(0.5)
    assert rep.book_completeness(liquid).completeness == pytest.approx(1.0)


def test_rate_peak_in_one_second_window() -> None:
    base = _recv_ns(10, 0, 0)
    frames = [rep.DecodedFrame(base + i * 100_000_000, "005930", None) for i in range(3)]
    stats = rep.rate_stats(frames, window_s=1.0)
    assert stats.max_per_window == 3
    assert stats.frames == 3


def test_decide_gate_requires_every_criterion() -> None:
    assert rep.decide_gate({"a": True, "b": True})
    assert not rep.decide_gate({"a": True, "b": False})
    assert not rep.decide_gate({})


def test_db_without_decoder_fails_gate_not_silently() -> None:
    gates = rep._gates("db", {"decode": "raw_only"}, rep.ProbeCriteria())
    assert gates["pass"] is False
    assert gates["name"] == "G-PROBE-DB"


def test_bid_book_descending_is_monotone() -> None:
    stats = rep.book_completeness([_book("005930", list(range(10)), list(range(10)))])
    assert stats.monotone_violations == 0


def test_bid_book_ascending_is_monotone_violation() -> None:
    bad = rep.DecodedBook(
        0,
        "005930",
        tuple((100 + i, 10) for i in range(10)),
        tuple((90 + i, 10) for i in range(10)),
    )
    assert rep.book_completeness([bad]).monotone_violations == 1


SYMBOL = "005930"
DAY = dt.date(2026, 11, 16)
EVENT_STAMP = "100005"
REFERENCE_LAG_S = 1
CANDIDATE_LAG_S = 1


def _ls_trade_raw(tr_cd: str, tr_key: str, symbol: str, stamp: str, cum: int) -> str:
    body = {"shcode": symbol, "chetime": stamp, "price": "100", "cvolume": "1", "mdchecnt": str(cum), "mschecnt": "0"}
    return json.dumps({"header": {"tr_cd": tr_cd, "tr_key": tr_key}, "body": body})


def _db_trade_raw(symbol: str, stamp: str, cum: int, *, drop: str | None = None) -> str:
    body: dict[str, str] = {
        "ShrnIscd": symbol,
        "StckCntghour": stamp,
        "StckPrpr": "100",
        "CntgVol": "1",
        "SelnCntgcsnu": str(cum),
        "ShnuCntgcsnu": "0",
    }
    if drop is not None:
        body.pop(drop)
    return json.dumps({"header": {"tr_cd": "S00", "tr_key": f"J {symbol}"}, "body": body})


def _db_book_raw(symbol: str, stamp: str) -> str:
    body: dict[str, str] = {"ShrnIscd": symbol, "BsopHour": stamp}
    for n in range(1, 11):
        body[f"Askp{n}"] = str(100 + n)
        body[f"AskpRsqn{n}"] = "10"
        body[f"Bidp{n}"] = str(100 - n)
        body[f"BidpRsqn{n}"] = "10"
    return json.dumps({"header": {"tr_cd": "S01", "tr_key": f"J {symbol}"}, "body": body})


def _ls_nxt_book_raw(symbol: str, stamp: str) -> str:
    body: dict[str, str] = {"shcode": symbol, "hotime": stamp}
    for n in range(1, 11):
        body[f"offerho{n}"] = str(100 + n)
        body[f"offerrem{n}"] = "10"
        body[f"bidho{n}"] = str(100 - n)
        body[f"bidrem{n}"] = "10"
    return json.dumps({"header": {"tr_cd": "NH1", "tr_key": f"N{symbol}   "}, "body": body})


def _write_capture(capture_dir: Path, vendor: str, rows: list[tuple[int, str]], cap_pairs: int) -> None:
    capture_dir.mkdir(parents=True, exist_ok=True)
    with (capture_dir / f"capture.{vendor}.100.jsonl").open("w", encoding="utf-8") as handle:
        for recv, raw in rows:
            handle.write(json.dumps({"recv_wall_ns": recv, "recv_mono_ns": 0, "raw": raw}) + "\n")
    (capture_dir / f"summary.{vendor}.json").write_text(json.dumps({"cap_pairs": cap_pairs}), encoding="utf-8")


def _write_reference(journal_root: Path, rows: list[tuple[int, str, str]]) -> None:
    writer = L0JournalWriter(journal_root, vendor="ls", venue="krx", session="regular", stream="H0STCNT0")
    for seq, (recv, stamp, raw) in enumerate(rows):
        writer.append(
            raw=raw,
            recv_mono_ns=seq,
            recv_wall_ns=recv,
            conn_id="ref",
            conn_seq=seq,
            exchange_event_time=stamp,
            symbol=SYMBOL,
        )
    writer.flush()


def _run(
    tmp_path: Path,
    vendor: str,
    *,
    candidate_lag_s: int = CANDIDATE_LAG_S,
    candidate_cums: range = range(1, 11),
    drop: str | None = None,
    cap_pairs: int = 50,
    with_reference: bool = True,
) -> dict[str, Any]:
    capture_dir = tmp_path / "capture"
    journal_root = tmp_path / "journal"
    if with_reference:
        reference = [
            (
                _recv_ns(10, 0, 5 + REFERENCE_LAG_S),
                EVENT_STAMP,
                _ls_trade_raw("H0STCNT0", SYMBOL, SYMBOL, EVENT_STAMP, cum),
            )
            for cum in range(1, 11)
        ]
        _write_reference(journal_root, reference)
    recv = _recv_ns(10, 0, 5 + candidate_lag_s)
    if vendor == "db":
        rows = [(recv, _db_trade_raw(SYMBOL, EVENT_STAMP, cum, drop=drop)) for cum in candidate_cums]
        rows.append((recv, _db_book_raw(SYMBOL, EVENT_STAMP)))
    else:
        rows = [(recv, _ls_trade_raw("NS3", f"N{SYMBOL}   ", SYMBOL, EVENT_STAMP, cum)) for cum in candidate_cums]
        rows.append((recv, _ls_nxt_book_raw(SYMBOL, EVENT_STAMP)))
    _write_capture(capture_dir, vendor, rows, cap_pairs)
    references = ["ls-krx"] if with_reference else []
    return rep.analyze(capture_dir, journal_root, DAY, vendor=vendor, reference_vendors=references)


def _criteria(result: dict[str, Any]) -> dict[str, Any]:
    return result["gates"]["criteria"]


def test_db_gate_passes_on_clean_capture(tmp_path: Path) -> None:
    result = _run(tmp_path, "db")
    assert result["gates"]["name"] == "G-PROBE-DB"
    assert result["reference_lag"]["count"] == 10
    assert all(_criteria(result).values())
    assert result["gates"]["pass"] is True


def test_db_gate_fails_when_lag_exceeds_reference(tmp_path: Path) -> None:
    result = _run(tmp_path, "db", candidate_lag_s=4)
    assert _criteria(result)["lag_within_reference"] is False
    assert _criteria(result)["conflation_p50"] is True
    assert result["gates"]["pass"] is False


def test_db_gate_fails_on_conflated_trades(tmp_path: Path) -> None:
    result = _run(tmp_path, "db", candidate_cums=range(1, 6))
    assert result["conflation"]["p50_ratio"] == pytest.approx(0.5)
    assert _criteria(result)["conflation_p50"] is False
    assert result["gates"]["pass"] is False


def test_db_missing_trade_count_field_is_counted_and_fails_gate(tmp_path: Path) -> None:
    result = _run(tmp_path, "db", drop="SelnCntgcsnu")
    assert result["field_missing"] == {"field_missing:SelnCntgcsnu": 10}
    assert result["gates"]["pass"] is False


def test_ls_nxt_gate_passes_on_clean_capture(tmp_path: Path) -> None:
    result = _run(tmp_path, "ls-nxt")
    assert result["gates"]["name"] == "G-PROBE-LSNXT"
    assert result["reference_lag"]["count"] == 10
    assert all(_criteria(result).values())
    assert result["gates"]["pass"] is True


def test_ls_nxt_gate_fails_when_lag_exceeds_reference(tmp_path: Path) -> None:
    result = _run(tmp_path, "ls-nxt", candidate_lag_s=4)
    assert _criteria(result)["lag_within_reference"] is False
    assert result["gates"]["pass"] is False


def test_ls_nxt_gate_fails_without_reference_samples(tmp_path: Path) -> None:
    result = _run(tmp_path, "ls-nxt", with_reference=False)
    assert result["reference_lag"]["count"] == 0
    assert _criteria(result)["lag_within_reference"] is False
    assert result["gates"]["pass"] is False


def test_conflation_judges_only_reference_trades_inside_the_candidate_span() -> None:
    cand = [rep.ReceivedTick(SYMBOL, 100100, 5, 100, 1)]
    ref = [
        rep.ReceivedTick(SYMBOL, 100000, 4, 100, 1),
        rep.ReceivedTick(SYMBOL, 100100, 5, 100, 1),
        rep.ReceivedTick("000660", 100100, 9, 100, 1),
    ]
    stats = rep.conflation_ratio(cand, ref, scope=rep._trade_spans(cand))
    assert stats.symbol_minutes == 1
    assert stats.p50_ratio == pytest.approx(1.0)


def _toss_trade_raw(symbol: str, timestamp: str, price: str, volume: str) -> str:
    data = {"price": price, "volume": volume, "timestamp": timestamp, "currency": "KRW"}
    return json.dumps({"type": "message", "topic": f"trade:kr:{symbol}", "data": data})


def _toss_book_raw(symbol: str, timestamp: str, levels: int = 10) -> str:
    asks = [{"price": str(100 + n), "volume": "10"} for n in range(levels)]
    bids = [{"price": str(99 - n), "volume": "10"} for n in range(levels)]
    data = {"timestamp": timestamp, "currency": "KRW", "asks": asks, "bids": bids}
    return json.dumps({"type": "message", "topic": f"orderbook:kr:{symbol}", "data": data})


def test_toss_decoder_parses_trade_and_book_frames() -> None:
    recv = _recv_ns(10, 0, 6)
    rows = [
        rep._Capture(recv, "PONG"),
        rep._Capture(recv, '{"type":"subscriptions","subscribed":["trade:kr:005930"]}'),
        rep._Capture(recv, _toss_trade_raw(SYMBOL, "2026-11-16T10:00:05.123+09:00", "70000", "10")),
        rep._Capture(recv, _toss_trade_raw(SYMBOL, "2026-11-16T01:00:05.500Z", "70100", "3")),
        rep._Capture(recv, _toss_book_raw(SYMBOL, "2026-11-16T10:00:05.123+09:00", levels=11)),
    ]
    frames, books, trades = rep._decode_toss(rows)
    assert [f.event_second for f in frames] == [36005, 36005]
    assert trades == [
        rep.ReceivedTick(SYMBOL, 100005, 0, 70000, 10),
        rep.ReceivedTick(SYMBOL, 100005, 0, 70100, 3),
    ]
    assert len(books) == 1
    assert len(books[0].asks) == 10
    assert len(books[0].bids) == 10
    assert rep.book_completeness(books).completeness == pytest.approx(1.0)


def test_toss_conflation_uses_second_price_volume_multiset() -> None:
    candidate = [
        rep.ReceivedTick(SYMBOL, 100005, 0, 100, 1),
        rep.ReceivedTick(SYMBOL, 100010, 0, 100, 9),
        rep.ReceivedTick(SYMBOL, 100020, 0, 101, 1),
    ]
    reference = [
        rep.ReceivedTick(SYMBOL, 100005, 7, 100, 1),
        rep.ReceivedTick(SYMBOL, 100010, 7, 100, 2),
        rep.ReceivedTick(SYMBOL, 100030, 7, 100, 3),
        rep.ReceivedTick(SYMBOL, 100020, 7, 101, 1),
        rep.ReceivedTick(SYMBOL, 100040, 7, 102, 5),
    ]
    stats = rep.trade_match(candidate, reference, scope=rep._trade_spans(candidate))
    assert stats.symbol_minutes == 1
    assert stats.p50_matched_share == pytest.approx(2 / 3)
    assert stats.unexplained == 1


def _toss_run(tmp_path: Path, *, matched: int) -> dict[str, Any]:
    capture_dir = tmp_path / "capture"
    journal_root = tmp_path / "journal"
    reference = [
        (_recv_ns(10, 0, 6), EVENT_STAMP, _ls_trade_raw("H0STCNT0", SYMBOL, SYMBOL, EVENT_STAMP, cum))
        for cum in range(1, 11)
    ]
    _write_reference(journal_root, reference)
    stamp = "2026-11-16T10:00:05.000+09:00"
    recv = _recv_ns(10, 0, 6)
    trades = [_toss_trade_raw(SYMBOL, stamp, "100", "1")] * matched
    trades += [_toss_trade_raw(SYMBOL, stamp, "999", "1")] * (10 - matched)
    rows = [(recv, raw) for raw in trades] + [(recv, _toss_book_raw(SYMBOL, stamp))]
    _write_capture(capture_dir, "toss", rows, 50)
    return rep.analyze(capture_dir, journal_root, DAY, vendor="toss", reference_vendors=["ls-krx"])


def test_toss_gate_passes_when_match_share_meets_threshold(tmp_path: Path) -> None:
    result = _toss_run(tmp_path, matched=10)
    assert result["decode"] == "decoded"
    assert result["gates"]["name"] == "G-PROBE-TOSS"
    assert result["trade_match"]["p50_matched_share"] == pytest.approx(1.0)
    assert all(_criteria(result).values())
    assert result["gates"]["pass"] is True


def test_toss_gate_fails_below_match_share_threshold(tmp_path: Path) -> None:
    result = _toss_run(tmp_path, matched=5)
    assert result["trade_match"]["p50_matched_share"] == pytest.approx(0.5)
    assert _criteria(result)["trade_match_p50"] is False
    assert result["gates"]["pass"] is False


def test_db_split_gates_book_pass_tick_fail(tmp_path: Path) -> None:
    result = _run(tmp_path, "db", candidate_cums=range(1, 6))
    assert result["gates_split"]["G-PROBE-DB-BOOK"]["pass"] is True
    assert result["gates_split"]["G-PROBE-DB-TICK"]["pass"] is False
    assert result["gates"]["pass"] is False
    assert set(result["gates"]) == {"name", "pass", "criteria", "note"}
    assert set(result["gates"]["criteria"]) == {
        "accepted_pairs",
        "conflation_p50",
        "book_completeness",
        "lag_within_reference",
    }


def _toss_split_run(tmp_path: Path, *, locked_books: int, normal_books: int) -> dict[str, Any]:
    """Trades match the reference fully; books mirror the 2026-11-17 live mix (normal, locked, crossed)."""
    capture_dir = tmp_path / "capture"
    journal_root = tmp_path / "journal"
    _write_reference(
        journal_root,
        [
            (_recv_ns(10, 0, 6), EVENT_STAMP, _ls_trade_raw("H0STCNT0", SYMBOL, SYMBOL, EVENT_STAMP, cum))
            for cum in range(1, 11)
        ],
    )
    stamp = "2026-11-16T10:00:05.000+09:00"
    recv = _recv_ns(10, 0, 6)
    rows = [(recv, _toss_trade_raw(SYMBOL, stamp, "100", "1")) for _ in range(10)]
    rows += [(recv, _toss_book_raw(SYMBOL, stamp)) for _ in range(normal_books)]
    locked = json.loads(_toss_book_raw(SYMBOL, stamp))
    locked["data"]["bids"][0]["price"] = locked["data"]["asks"][0]["price"]
    rows += [(recv, json.dumps(locked)) for _ in range(locked_books)]
    _write_capture(capture_dir, "toss", rows, 50)
    return rep.analyze(capture_dir, journal_root, DAY, vendor="toss", reference_vendors=["ls-krx"])


def test_toss_report_split_gates_trade_pass_book_fail_on_live_mix(tmp_path: Path) -> None:
    result = _toss_split_run(tmp_path, locked_books=25, normal_books=75)
    gates = result["gates_split"]
    assert gates["G-PROBE-TOSS-TRADE"]["pass"] is True
    assert gates["G-PROBE-TOSS-BOOK"]["pass"] is False
    assert gates["G-PROBE-TOSS-BOOK"]["criteria"]["crossed_share"] is False
    assert gates["G-PROBE-TOSS-BOOK"]["criteria"]["book_completeness"] is True
    assert result["gates"]["pass"] is False


def test_toss_report_book_gate_passes_without_crossed_frames(tmp_path: Path) -> None:
    result = _toss_split_run(tmp_path, locked_books=0, normal_books=50)
    assert result["gates_split"]["G-PROBE-TOSS-BOOK"]["pass"] is True
    assert result["gates"]["pass"] is True
