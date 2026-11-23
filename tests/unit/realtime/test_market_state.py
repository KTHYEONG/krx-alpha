"""Market-state lane: side-journal divergence, ack filtering, fail-soft writes and halt replay."""

from __future__ import annotations

import json
import pathlib

from src.realtime.contracts import L0Frame, MarketVenue, VendorAck
from src.realtime.market_state import MarketStateSink, read_market_state

_T0_NS = 1_800_000_000_000_000_000
_NS = 1_000_000_000


class _Inner:
    def __init__(self) -> None:
        self.frames: list[L0Frame] = []
        self.acks: list[VendorAck] = []

    def record(self, frame: L0Frame) -> None:
        self.frames.append(frame)

    def note_ack(self, vendor: str, ack: VendorAck) -> None:
        self.acks.append(ack)

    def note_gap(self, symbol: str, start_ns: int, end_ns: int, reason: str) -> None:
        return None

    def flush(self) -> int:
        return 0


def _frame(stream: str, symbol: str, raw: str, recv_s: float) -> L0Frame:
    return L0Frame(
        "ls",
        stream,
        symbol,
        raw,
        0,
        _T0_NS + int(recv_s * _NS),
        1,
        conn_id="c1",
        venue=MarketVenue.KRX,
    )


def _vi(ref: str, gubun: str) -> str:
    return json.dumps({"header": {"tr_cd": "VI_"}, "body": {"ref_shcode": ref, "vi_gubun": gubun, "time": "151206"}})


def _jif(jangubun: str, jstatus: str) -> str:
    return json.dumps({"header": {"tr_cd": "JIF"}, "body": {"jangubun": jangubun, "jstatus": jstatus}})


def _sink(inner: _Inner, path: pathlib.Path) -> MarketStateSink:
    return MarketStateSink(inner, path, wall_clock_ns=lambda: _T0_NS)


def test_market_frames_diverted(tmp_path) -> None:
    inner = _Inner()
    path = tmp_path / "market.jsonl"
    sink = _sink(inner, path)
    vi_raw = _vi("088340", "1")
    sink.record(_frame("LS_VI", "000000", vi_raw, 0))
    trade = _frame("H0STCNT0", "005930", "{}", 0)
    sink.record(trade)

    assert inner.frames == [trade]
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["raw"] == vi_raw


def test_market_ack_not_forwarded(tmp_path, caplog) -> None:
    inner = _Inner()
    sink = _sink(inner, tmp_path / "market.jsonl")
    caplog.set_level("INFO")
    sink.note_ack("ls", VendorAck("000000", "LS_VI", True, "00000"))
    sink.note_ack("ls", VendorAck("005930", "H0STCNT0", True, "00000"))

    assert [ack.symbol for ack in inner.acks] == ["005930"]
    assert "stage=market_state status=ACK stream=LS_VI" in caplog.text


def test_write_failure_never_raises(tmp_path, caplog) -> None:
    blocker = tmp_path / "file"
    blocker.write_text("x", encoding="utf-8")
    inner = _Inner()
    sink = _sink(inner, blocker / "market.jsonl")
    trade = _frame("H0STCNT0", "005930", "{}", 0)
    sink.record(_frame("LS_JIF", "0", _jif("1", "30"), 0))
    sink.record(trade)

    assert inner.frames == [trade]
    assert "status=WRITE_FAIL" in caplog.text


def test_halted_symbols_window(tmp_path) -> None:
    path = tmp_path / "market.jsonl"
    sink = _sink(_Inner(), path)
    sink.record(_frame("LS_VI", "000000", _vi("088340", "1"), 100))
    sink.record(_frame("LS_VI", "000000", _vi("088340", "0"), 220))
    sink.record(_frame("LS_VI", "000000", _vi("099999", "2"), 100))
    view = read_market_state(path)
    t0 = _T0_NS / _NS

    assert "088340" in view.halted_symbols(t0 + 160)
    assert "088340" not in view.halted_symbols(t0 + 230)
    assert "099999" in view.halted_symbols(t0 + 650)
    assert "099999" not in view.halted_symbols(t0 + 701)


def test_market_cb_state(tmp_path) -> None:
    path = tmp_path / "market.jsonl"
    sink = _sink(_Inner(), path)
    sink.record(_frame("LS_JIF", "0", _jif("2", "61"), 100))
    t0 = _T0_NS / _NS
    assert read_market_state(path).market_halted(t0 + 200) == {"2"}

    sink.record(_frame("LS_JIF", "0", _jif("2", "62"), 300))
    assert read_market_state(path).market_halted(t0 + 400) == {"2"}

    sink.record(_frame("LS_JIF", "0", _jif("2", "63"), 500))
    assert read_market_state(path).market_halted(t0 + 600) == frozenset()


def test_missing_journal_reads_as_empty(tmp_path) -> None:
    view = read_market_state(tmp_path / "absent.jsonl")
    assert view.halted_symbols(_T0_NS / _NS) == frozenset()
    assert view.market_halted(_T0_NS / _NS) == frozenset()


def test_market_gap_not_forwarded(tmp_path) -> None:
    inner = _Inner()
    sink = _sink(inner, tmp_path / "market.jsonl")
    recorded: list[str] = []
    inner.note_gap = lambda symbol, start_ns, end_ns, reason: recorded.append(symbol)  # type: ignore[method-assign]
    sink.note_gap("0", 0, 1, "stream_gap")
    sink.note_gap("000000", 0, 1, "stream_gap")
    sink.note_gap("005930", 0, 1, "stream_gap")
    assert recorded == ["005930"]


def test_flush_forwards_to_inner(tmp_path) -> None:
    assert _sink(_Inner(), tmp_path / "market.jsonl").flush() == 0


def test_write_failure_warns_once_per_interval(tmp_path, caplog) -> None:
    blocker = tmp_path / "file"
    blocker.write_text("x", encoding="utf-8")
    clock = [0.0]
    sink = MarketStateSink(_Inner(), blocker / "market.jsonl", wall_clock_ns=lambda: _T0_NS, monotonic=lambda: clock[0])
    caplog.set_level("WARNING")
    sink.record(_frame("LS_JIF", "0", _jif("1", "30"), 0))
    sink.record(_frame("LS_JIF", "0", _jif("1", "30"), 0))
    assert caplog.text.count("status=WRITE_FAIL") == 1
    clock[0] = 61.0
    sink.record(_frame("LS_JIF", "0", _jif("1", "30"), 0))
    assert caplog.text.count("status=WRITE_FAIL") == 2


def test_malformed_and_unrelated_lines_are_skipped(tmp_path) -> None:
    path = tmp_path / "market.jsonl"
    good = json.dumps(
        {"recv_wall_ns": _T0_NS, "stream": "LS_VI", "symbol": "000000", "raw": _vi("088340", "1")}, sort_keys=True
    )
    no_ref = json.dumps(
        {"recv_wall_ns": _T0_NS, "stream": "LS_VI", "symbol": "000000", "raw": json.dumps({"body": {"vi_gubun": "1"}})},
        sort_keys=True,
    )
    no_jangubun = json.dumps(
        {"recv_wall_ns": _T0_NS, "stream": "LS_JIF", "symbol": "0", "raw": json.dumps({"body": {"jstatus": "61"}})},
        sort_keys=True,
    )
    trade = json.dumps({"recv_wall_ns": _T0_NS, "stream": "H0STCNT0", "symbol": "005930", "raw": "{}"}, sort_keys=True)
    path.write_text("not json\n" + "\n".join([good, no_ref, no_jangubun, trade]) + "\n", encoding="utf-8")
    view = read_market_state(path)
    t0 = _T0_NS / _NS

    assert view.halted_symbols(t0 + 1) == {"088340"}
    assert view.market_halted(t0 + 1) == frozenset()
    assert view.halted_symbols(t0 - 1) == frozenset()
