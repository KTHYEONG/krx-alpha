import datetime as dt
import itertools
import json
from zoneinfo import ZoneInfo

import pytest
import zstandard as zstd

KST = ZoneInfo("Asia/Seoul")
DAY = dt.date(2026, 10, 15)
TICK = "H0STCNT0"
QUOTE = "H0STASP0"
_SEQ = itertools.count(1)


@pytest.fixture(autouse=True)
def _clean_audit_env(monkeypatch):
    import os

    for name in [n for n in os.environ if n.startswith("KRX_ALPHA_AUDIT_")]:
        monkeypatch.delenv(name, raising=False)


def _ns(hour, minute, second=0):
    return int(dt.datetime(DAY.year, DAY.month, DAY.day, hour, minute, second, tzinfo=KST).timestamp() * 1_000_000_000)


def _tick_body(sym, md, ms, volume, cvolume, chetime, price="70000"):
    return {
        "shcode": sym, "price": price, "cvolume": str(cvolume), "volume": str(volume),
        "change": "0", "sign": "3", "drate": "0.00", "chetime": chetime,
        "mdchecnt": str(md), "mschecnt": str(ms),
    }


def _env(raw, recv_ns, conn="c1", seq=None, stream=TICK, exch="", symbol="", vendor="ls", **kwargs):
    record = {
        "raw": raw, "recv_mono_ns": recv_ns, "recv_wall_ns": recv_ns, "conn_id": conn,
        "conn_seq": next(_SEQ) if seq is None else seq, "vendor": vendor, "tr_id": stream, "venue": "krx",
        "session": "regular", "stream": stream, "symbol": symbol, "exchange_event_time": exch,
    }
    record.update(kwargs)
    return record


def _tick(sym, md, ms, volume, cvolume, chetime, recv_ns, conn="c1", seq=None, price="70000", exch=""):
    raw = json.dumps({"header": {"tr_cd": "S3_", "tr_key": sym}, "body": _tick_body(sym, md, ms, volume, cvolume, chetime, price)})
    return _env(raw, recv_ns, conn=conn, seq=seq, exch=exch)


def _quote(sym, hotime, recv_ns, exch="", seq=None):
    body = {"shcode": sym, "hotime": hotime, "totofferrem": "100", "totbidrem": "100"}
    raw = json.dumps({"header": {"tr_cd": "S3_", "tr_key": sym}, "body": body})
    return _env(raw, recv_ns, stream=QUOTE, exch=exch, seq=seq)


def _write(root, stream, records, name="09.jsonl.zst", vendor="ls", raw_frames=None):
    part = root / vendor / "krx" / "regular" / stream / f"dt={DAY.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    if raw_frames is None:
        payload = "".join(json.dumps(record) + "\n" for record in records).encode()
        (part / name).write_bytes(zstd.ZstdCompressor(level=3).compress(payload))
    else:
        (part / name).write_bytes(b"".join(raw_frames))
    return part


def _frames_payload(records, trailing_newline=True):
    payload = "".join(json.dumps(record) + "\n" for record in records).encode()
    return payload if trailing_newline else payload.rstrip(b"\n")


def _derive(root, settings=None, **overrides):
    from src.core.config import AuditSettings
    from src.core.session_anchors import standard_session_anchors
    from src.storage.l0_audit import derive_session_audit

    params = {
        "vendor": "ls", "venue": "krx", "session": "regular", "day": DAY,
        "anchors": standard_session_anchors(DAY), "settings": settings or AuditSettings(),
    }
    params.update(overrides)
    return derive_session_audit(root, **params)


def _sample_settings(monkeypatch, budget, **kwargs):
    from src.core.config import AuditSettings

    settings = AuditSettings(**kwargs)
    monkeypatch.setattr(settings, "latency_sample_budget_bytes", budget)
    return settings


def _ok_quote(recv_ns=None):
    return _quote("005930", "090100", recv_ns or _ns(9, 1, 1))


def test_received_count_preserves_counter_span_and_loss_ratio(tmp_path):
    from src.storage.l0_audit import _audit_to_jsonable, iter_received_ticks

    _write(tmp_path, TICK, [
        _tick("005930", 1, 0, 10, 10, "090000", _ns(9, 0)),
        _tick("005930", 3, 0, 30, 10, "090002", _ns(9, 0, 2)),
    ])
    audit = _derive(tmp_path)
    assert audit.totals.trades_observed == 3
    assert audit.totals.confirmed_missing == 1
    assert audit.totals.loss_ratio == pytest.approx(1 / 3)
    assert audit.totals.trades_received == 2
    assert audit.totals.trades_received == len(list(
        iter_received_ticks(tmp_path, vendor="ls", venue="krx", session="regular", day=DAY)
    ))
    assert _audit_to_jsonable(audit)["totals"]["trades_received"] == 2


def test_received_count_matches_decoder_before_dedup_and_phase_filtering(tmp_path):
    from src.storage.l0_audit import iter_received_ticks

    first = _tick("005930", 1, 0, 10, 10, "090000", _ns(9, 0), seq=1)
    bad_volume = _tick("000001", 7, 0, "bad", 10, "090001", _ns(9, 0, 1))
    bad_price = _tick("005930", 2, 0, 20, 10, "090001", _ns(9, 0, 1), price="bad")
    _write(tmp_path, TICK, [
        first, first, bad_volume, bad_price,
        _tick("005930", 3, 0, 30, 10, "153000", _ns(15, 30)),
        _tick("000002", 1, 0, 10, 10, "090000", _ns(9, 0)),
    ])
    audit = _derive(tmp_path)
    ticks = list(iter_received_ticks(tmp_path, vendor="ls", venue="krx", session="regular", day=DAY))
    assert audit.totals.trades_received == len(ticks) == 5
    assert audit.totals.symbols_without_counter > 0
    assert sum(symbol.duplicates for symbol in audit.symbols) == 1


def _as_kis(record):
    body = json.loads(record["raw"])["body"]
    fields = [""] * 17
    for index, key in ((0, "shcode"), (1, "chetime"), (2, "price"), (12, "cvolume"),
                       (13, "volume"), (15, "mdchecnt"), (16, "mschecnt")):
        fields[index] = body[key]
    return {**record, "vendor": "kis", "raw": "^".join(fields)}


@pytest.mark.parametrize(("counters", "volumes", "expected"), [
    ([100, 101, 105], [1000, 1010, 1050], (3, 0, 0, 0)),
    ([100, 101, 105], [1000, 1010, 1020], (0, 3, 0, 0)),
    ([100, 100, 101], [1000, 1000, 1010], (0, 0, 1, 0)),
    ([500, 2, 3, 4, 5], [5000, 20, 30, 40, 50], (0, 0, 0, 1)),
    ([500, 9, 8, 7, 501], [5000, 90, 80, 70, 5010], (0, 0, 0, 0)),
])
def test_kis_ledger_matches_ls(tmp_path, counters, volumes, expected):
    from src.core.config import AuditSettings
    from src.storage.l0_audit import AuditStatus

    records = [
        _tick("005930", counter - 1, 1, volume, 10, "090001", _ns(9, 0, i + 3), seq=i + 1)
        for i, (counter, volume) in enumerate(zip(counters, volumes, strict=True))
    ]
    _write(tmp_path, TICK, records)
    _write(tmp_path, TICK, [_as_kis(record) for record in records], vendor="kis")
    settings = AuditSettings(latency_streams=(TICK,), counter_reset_confirm_frames=3)
    ls = _derive(tmp_path, settings=settings, wall_ns=lambda: 42)
    kis = _derive(tmp_path, settings=settings, vendor="kis", wall_ns=lambda: 42)

    assert kis.status is ls.status is AuditStatus.OK
    assert kis.symbols == ls.symbols
    assert kis.totals == ls.totals
    assert kis.latency == ls.latency
    ledger = kis.symbols[0]
    assert (ledger.confirmed_missing, ledger.count_only_missing, ledger.duplicates, ledger.resets) == expected


def test_kis_short_record_skipped_and_received_ticks_normalized(tmp_path):
    from src.core.config import AuditSettings
    from src.storage.l0_audit import AuditStatus, ReceivedTick, iter_received_ticks

    records = [_as_kis(_tick("005930", counter, 0, counter * 10, 10, "090001", _ns(9, 0, 3)))
               for counter in (100, 101)]
    short = {**records[0], "raw": "^".join(records[0]["raw"].split("^")[:16])}
    _write(tmp_path, TICK, [records[0], short, records[1]], vendor="kis")
    audit = _derive(tmp_path, settings=AuditSettings(latency_streams=(TICK,)), vendor="kis")

    assert audit.status is AuditStatus.OK
    assert audit.symbols[0].frames == audit.symbols[0].trades_observed == 2
    assert audit.latency[0].event_time_missing == 1
    assert list(iter_received_ticks(tmp_path, vendor="kis", venue="krx", session="regular", day=DAY)) == [
        ReceivedTick("005930", 90001, 100, 70000, 10),
        ReceivedTick("005930", 90001, 101, 70000, 10),
    ]


def test_kis_latency_and_read_only_determinism(tmp_path):
    from src.core.config import AuditSettings
    from src.storage.l0_audit import _audit_to_jsonable

    record = _as_kis(_tick("005930", 100, 0, 1000, 10, "090001", _ns(9, 0, 3)))
    _write(tmp_path, TICK, [record], vendor="kis")
    before = {p: (p.stat().st_mtime_ns, p.read_bytes()) for p in tmp_path.rglob("*.zst")}
    settings = AuditSettings(latency_streams=(TICK,))
    first = _derive(tmp_path, settings=settings, vendor="kis", wall_ns=lambda: 42)
    second = _derive(tmp_path, settings=settings, vendor="kis", wall_ns=lambda: 42)

    assert json.dumps(_audit_to_jsonable(first)) == json.dumps(_audit_to_jsonable(second))
    assert before == {p: (p.stat().st_mtime_ns, p.read_bytes()) for p in tmp_path.rglob("*.zst")}
    (minute,) = first.latency[0].minutes
    assert (minute.minute_kst, minute.count) == ("09:00", 1)
    assert minute.max_s == pytest.approx(2.0, abs=1e-3)


def test_kis_latency_replays_noncontiguous_receive_minute(tmp_path):
    from src.core.config import AuditSettings

    records = [_as_kis(_tick("005930", 100 + i, 0, 1000 + i * 10, 10, "090001", recv))
               for i, recv in enumerate((_ns(9, 0, 3), _ns(9, 1, 3), _ns(9, 0, 5)))]
    _write(tmp_path, TICK, records, vendor="kis")
    audit = _derive(tmp_path, settings=AuditSettings(latency_streams=(TICK,)), vendor="kis")
    first, second = audit.latency[0].minutes
    assert (first.minute_kst, first.count, first.min_s, first.max_s) == ("09:00", 2, 2.0, 4.0)
    assert (second.minute_kst, second.count, second.max_s) == ("09:01", 1, 62.0)


@pytest.mark.parametrize("with_records", [False, True])
def test_db_ledger_stays_unsupported(tmp_path, with_records):
    from src.core.config import AuditSettings
    from src.storage.l0_audit import AuditStatus

    if with_records:
        _write(tmp_path, TICK, [_tick("005930", 100, 0, 1000, 10, "090001", _ns(9, 0, 3))], vendor="db")
    audit = _derive(tmp_path, settings=AuditSettings(latency_streams=(TICK,)), vendor="db")
    assert audit.status is AuditStatus.UNAVAILABLE
    assert audit.reason == "unsupported_vendor"
    assert audit.symbols == ()


def test_recv_minute_bucketing(tmp_path) -> None:
    from src.storage.l0_audit import AuditStatus

    _write(tmp_path / "l0", TICK, [
        _tick("005930", 1, 0, 100, 10, "090100", _ns(9, 1, 10)),
        _tick("005930", 2, 0, 110, 10, "090101", _ns(9, 1, 50)),
        _tick("005930", 3, 0, 120, 10, "090102", _ns(9, 2, 5)),
    ])

    audit = _derive(tmp_path / "l0")

    (entry,) = [item for item in audit.latency if item.stream == TICK]
    assert [(m.minute_kst, m.count) for m in entry.minutes] == [("09:01", 2), ("09:02", 1)]
    assert audit.status is AuditStatus.PARTIAL


def test_open_backlog_is_visible_and_breaches(tmp_path) -> None:
    import datetime as dt

    from src.storage.l0_audit import latency_breaches

    _write(tmp_path / "l0", TICK, [
        _tick("005930", 100, 0, 1000, 7, "090010", _ns(9, 0, 10)),
        _tick("005930", 101, 0, 1007, 7, "090045", _ns(9, 1, 10)),
    ])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    (entry,) = [item for item in audit.latency if item.stream == TICK]
    (late,) = [m for m in entry.minutes if m.minute_kst == "09:01"]
    assert 25.0 <= late.max_s < 26.0
    breaches = latency_breaches(audit, threshold_s=10.0, window_start=dt.time(9, 0), window_end=dt.time(15, 30))
    assert [(b.stream, b.minute_kst) for b in breaches] == [(TICK, "09:01")]


def test_median_is_lower_sample_and_deterministic(tmp_path) -> None:
    _write(tmp_path / "l0", TICK, [
        _tick("005930", 1, 0, 100, 10, "090109", _ns(9, 1, 10)),
        _tick("005930", 2, 0, 110, 10, "090109", _ns(9, 1, 11)),
        _tick("005930", 3, 0, 120, 10, "090109", _ns(9, 1, 12)),
        _tick("005930", 4, 0, 130, 10, "090109", _ns(9, 1, 13)),
    ])

    first = _derive(tmp_path / "l0")
    second = _derive(tmp_path / "l0")

    (entry,) = [item for item in first.latency if item.stream == TICK]
    (minute,) = entry.minutes
    assert minute.p50_s == pytest.approx(2.0, abs=1e-6)
    assert minute.max_s == pytest.approx(4.0, abs=1e-6)
    assert minute.min_s == pytest.approx(1.0, abs=1e-6)
    assert first.latency == second.latency


def test_invalid_event_time_excluded_and_counted(tmp_path) -> None:
    from src.storage.l0_audit import AuditStatus

    _write(tmp_path / "l0", TICK, [
        _tick("005930", 1, 0, 100, 10, "", _ns(9, 1, 10)),
        _tick("005930", 2, 0, 110, 10, "9999", _ns(9, 1, 11)),
        _tick("005930", 3, 0, 120, 10, "250000", _ns(9, 1, 12)),
    ])

    audit = _derive(tmp_path / "l0")

    (entry,) = [item for item in audit.latency if item.stream == TICK]
    assert entry.rows == 3
    assert entry.event_time_missing == 3
    assert entry.minutes == ()
    assert audit.status is AuditStatus.UNAVAILABLE
    assert audit.reason == "no_decodable_records"


def test_negative_latency_retained_without_breach(tmp_path) -> None:
    import datetime as dt

    from src.storage.l0_audit import latency_breaches

    _write(tmp_path / "l0", TICK, [_tick("005930", 1, 0, 100, 10, "090001", _ns(9, 0, 0))])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    (entry,) = [item for item in audit.latency if item.stream == TICK]
    (minute,) = entry.minutes
    assert minute.min_s < 0
    assert latency_breaches(audit, threshold_s=10.0, window_start=dt.time(9, 0), window_end=dt.time(15, 30)) == ()


def test_equality_is_not_a_breach(tmp_path) -> None:
    import datetime as dt

    from src.storage.l0_audit import latency_breaches

    _write(tmp_path / "l0", TICK, [_tick("005930", 1, 0, 100, 10, "090200", _ns(9, 2, 10))])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")
    (entry,) = [item for item in audit.latency if item.stream == TICK]
    threshold = entry.minutes[0].max_s
    window = {"window_start": dt.time(9, 0), "window_end": dt.time(15, 30)}

    assert latency_breaches(audit, threshold_s=threshold, **window) == ()

    (breach,) = latency_breaches(audit, threshold_s=threshold - 1e-9, **window)
    assert (breach.stream, breach.minute_kst) == (TICK, "09:02")


def test_alert_window_excludes_preopen(tmp_path) -> None:
    import datetime as dt

    from src.storage.l0_audit import latency_breaches

    _write(tmp_path / "l0", TICK, [
        _tick("005930", 1, 0, 100, 10, "085456", _ns(8, 55, 10)),
        _tick("005930", 2, 0, 110, 10, "090056", _ns(9, 1, 10)),
    ])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    breaches = latency_breaches(audit, threshold_s=10.0, window_start=dt.time(9, 0), window_end=dt.time(15, 30))
    assert [(b.stream, b.minute_kst) for b in breaches] == [(TICK, "09:01")]
    assert audit.totals.trades_observed == 1


def test_streams_are_independent(tmp_path) -> None:
    from src.storage.l0_audit import AuditStatus

    _write(tmp_path / "l0", TICK, [
        _tick("005930", 1, 0, 100, 10, "090100", _ns(9, 1, 10)),
        _tick("005930", 2, 0, 110, 10, "090101", _ns(9, 1, 20)),
    ])
    part = tmp_path / "l0" / "ls" / "krx" / "regular" / QUOTE / f"dt={DAY.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    (part / "09.jsonl.zst").write_bytes(b"x")

    audit = _derive(tmp_path / "l0")

    by_stream = {item.stream: item for item in audit.latency}
    assert by_stream[TICK].status is AuditStatus.OK
    assert len(by_stream[TICK].minutes) == 1
    assert by_stream[QUOTE].status is AuditStatus.UNAVAILABLE
    assert audit.status is AuditStatus.PARTIAL


def test_quote_hotime_path_and_envelope_precedence(tmp_path) -> None:
    from src.storage.l0_audit import AuditStatus

    _write(tmp_path / "l0", QUOTE, [
        _quote("005930", "090100", _ns(9, 1, 10)),
        _env("bogus", _ns(9, 1, 10), stream=QUOTE, exch="090100"),
        _env(json.dumps({"header": {}, "body": {"shcode": "005930"}}), _ns(9, 1, 30), stream=QUOTE),
    ])

    audit = _derive(tmp_path / "l0")

    (entry,) = [item for item in audit.latency if item.stream == QUOTE]
    assert entry.status is AuditStatus.OK
    assert entry.rows == 3
    assert entry.event_time_missing == 1
    (minute,) = entry.minutes
    assert (minute.count, minute.max_s) == (2, pytest.approx(10.0, abs=1e-6))
    assert audit.status is AuditStatus.UNAVAILABLE
    assert audit.reason == "no_decodable_records"


def test_malformed_kis_record_uses_envelope_time_without_ledger(tmp_path) -> None:
    from src.storage.l0_audit import AuditStatus

    _write(tmp_path / "l0", TICK, [
        _env("KIS|H0STCNT0|unrelated", _ns(15, 40, 10), exch="154001", vendor="kis"),
    ], vendor="kis")

    audit = _derive(tmp_path / "l0", vendor="kis")

    assert audit.status is AuditStatus.UNAVAILABLE
    assert audit.reason == "no_decodable_records"
    (entry,) = [item for item in audit.latency if item.stream == TICK]
    assert entry.status is AuditStatus.OK
    (minute,) = entry.minutes
    assert minute.minute_kst == "15:40"
    assert 9.0 <= minute.max_s < 10.0
    assert audit.symbols == ()
    assert audit.totals.loss_ratio == 0.0


def test_contiguous_counters_are_clean(tmp_path) -> None:
    from src.storage.l0_audit import AuditStatus

    _write(tmp_path / "l0", TICK, [
        _tick("005930", 10, 0, 100, 5, "090100", _ns(9, 1, 10)),
        _tick("005930", 11, 0, 105, 5, "090101", _ns(9, 1, 20)),
        _tick("005930", 12, 0, 110, 5, "090102", _ns(9, 1, 30)),
        _tick("005930", 13, 0, 115, 5, "090103", _ns(9, 1, 40)),
    ])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    assert audit.status is AuditStatus.OK
    (ledger,) = audit.symbols
    assert ledger.symbol == "005930"
    assert ledger.status == "OK"
    assert ledger.intervals == ()
    assert (ledger.net_missing, ledger.confirmed_missing, ledger.trades_observed) == (0, 0, 4)
    assert audit.totals.loss_ratio == 0.0
    assert audit.totals.symbols == 1


def test_single_cut_is_quantified(tmp_path) -> None:
    _write(tmp_path / "l0", TICK, [
        _tick("005930", 60, 40, 1000, 7, "090010", _ns(9, 0, 10)),
        _tick("005930", 63, 42, 1407, 7, "090045", _ns(9, 1, 10)),
    ])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    (ledger,) = audit.symbols
    (interval,) = ledger.intervals
    assert interval.kind == "confirmed"
    assert (interval.missing_trades, interval.volume_deficit) == (4, 400)
    assert (interval.chetime_prev, interval.chetime_next) == ("090010", "090045")
    assert interval.conn_changed is False
    assert ledger.net_missing == 4
    assert audit.totals.confirmed_missing == 4


def test_count_only_jump_is_not_budgeted(tmp_path) -> None:
    _write(tmp_path / "l0", TICK, [
        _tick("005930", 10, 0, 100, 3, "090010", _ns(9, 0, 10)),
        _tick("005930", 13, 0, 103, 3, "090020", _ns(9, 0, 20)),
    ])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    (ledger,) = audit.symbols
    (interval,) = ledger.intervals
    assert interval.kind == "count_only"
    assert (ledger.count_only_missing, ledger.confirmed_missing) == (2, 0)
    assert audit.totals.loss_ratio == 0.0


def test_duplicates_do_not_shift_baseline(tmp_path) -> None:
    frame = {"md": 5, "ms": 0, "volume": 100, "cvolume": 10, "chetime": "090100", "recv": _ns(9, 0, 10)}
    _write(tmp_path / "l0", TICK, [
        _tick("005930", frame["md"], frame["ms"], frame["volume"], frame["cvolume"], frame["chetime"], frame["recv"], seq=1),
        _tick("005930", frame["md"], frame["ms"], frame["volume"], frame["cvolume"], frame["chetime"], frame["recv"], seq=2),
        _tick("005930", frame["md"], frame["ms"], frame["volume"], frame["cvolume"], frame["chetime"], frame["recv"], seq=3),
        _tick("005930", 6, 0, 110, 10, "090101", _ns(9, 0, 20), seq=4),
        _tick("005930", 99, 0, 999, 1, "090102", _ns(9, 0, 30), seq=4),
        _tick("005930", 7, 0, 120, 10, "090103", _ns(9, 0, 40), seq=5),
    ])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    (ledger,) = [item for item in audit.symbols if item.symbol == "005930"]
    assert ledger.duplicates == 3
    assert ledger.frames == 3
    assert ledger.intervals == ()
    assert ledger.net_missing == 0


def test_out_of_order_frame_is_ignored_safely(tmp_path) -> None:
    _write(tmp_path / "l0", TICK, [
        _tick("005930", 10, 0, 100, 10, "090100", _ns(9, 0, 10)),
        _tick("005930", 12, 0, 120, 10, "090101", _ns(9, 0, 20)),
        _tick("005930", 11, 0, 110, 10, "090059", _ns(9, 0, 30)),
        _tick("005930", 13, 0, 130, 10, "090103", _ns(9, 0, 40)),
    ])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    (ledger,) = audit.symbols
    assert ledger.regressions == 1
    assert [(iv.cum_prev, iv.cum_next, iv.missing_trades) for iv in ledger.intervals] == [(10, 12, 1)]
    assert (ledger.net_missing, ledger.confirmed_missing) == (1, 1)


def test_counter_reset_needs_confirmation(tmp_path) -> None:
    from src.core.config import AuditSettings

    settings = AuditSettings(counter_reset_confirm_frames=3)
    _write(tmp_path / "l0", TICK, [
        _tick("005930", 500, 0, 5000, 10, "090100", _ns(9, 0, 10)),
        _tick("005930", 2, 0, 20, 10, "090101", _ns(9, 0, 20)),
        _tick("005930", 3, 0, 30, 10, "090102", _ns(9, 0, 30)),
        _tick("005930", 4, 0, 40, 10, "090103", _ns(9, 0, 40)),
        _tick("005930", 5, 0, 50, 10, "090104", _ns(9, 0, 50)),
    ])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0", settings=settings)

    (ledger,) = audit.symbols
    assert ledger.resets == 1
    assert ledger.intervals == ()
    assert (ledger.net_missing, ledger.trades_observed, ledger.frames) == (0, 5, 5)


def test_decreasing_run_is_not_a_reset(tmp_path) -> None:
    _write(tmp_path / "l0", TICK, [
        _tick("005930", 500, 0, 5000, 10, "090100", _ns(9, 0, 10)),
        _tick("005930", 9, 0, 90, 10, "090101", _ns(9, 0, 20)),
        _tick("005930", 8, 0, 80, 10, "090102", _ns(9, 0, 30)),
        _tick("005930", 7, 0, 70, 10, "090103", _ns(9, 0, 40)),
        _tick("005930", 501, 0, 5010, 10, "090104", _ns(9, 0, 50)),
    ])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    (ledger,) = audit.symbols
    assert (ledger.resets, ledger.regressions) == (0, 3)
    assert ledger.intervals == ()
    assert ledger.net_missing == 0


def test_single_regression_is_not_a_reset(tmp_path) -> None:
    _write(tmp_path / "l0", TICK, [
        _tick("005930", 500, 0, 5000, 10, "090100", _ns(9, 0, 10)),
        _tick("005930", 450, 0, 4500, 10, "090101", _ns(9, 0, 20)),
        _tick("005930", 501, 0, 5010, 10, "090102", _ns(9, 0, 30)),
    ])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    (ledger,) = audit.symbols
    assert (ledger.resets, ledger.regressions) == (0, 1)
    assert ledger.intervals == ()


def test_first_frame_per_segment_is_baseline(tmp_path) -> None:
    _write(tmp_path / "l0", TICK, [
        _tick("005930", 10, 0, 100, 10, "151959", _ns(15, 19, 59)),
        _tick("005930", 11, 0, 110, 10, "160001", _ns(16, 0, 1)),
    ])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    (ledger,) = audit.symbols
    assert ledger.intervals == ()
    assert ledger.frames == 2
    assert audit.totals.trades_observed == 1


def test_shifted_anchors_move_phases(tmp_path) -> None:
    import datetime as dt

    from src.core.session_anchors import AnchorSource, SessionAnchors

    shifted = SessionAnchors(
        date=DAY, regular_open=dt.time(10, 0), closing_auction_start=dt.time(16, 20),
        regular_close=dt.time(16, 30), after_market_end=dt.time(21, 0), source=AnchorSource.DEFAULT,
    )
    records = [
        _tick("005930", 10, 0, 100, 10, "093000", _ns(9, 30, 0)),
        _tick("005930", 11, 0, 110, 10, "100030", _ns(10, 0, 30)),
    ]
    _write(tmp_path / "l0", TICK, records)
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    shifted_audit = _derive(tmp_path / "l0", anchors=shifted)
    (shifted_ledger,) = shifted_audit.symbols
    assert shifted_ledger.intervals == ()
    assert shifted_audit.totals.trades_observed == 1

    default_audit = _derive(tmp_path / "l0")
    (default_ledger,) = default_audit.symbols
    assert default_ledger.intervals == ()
    assert default_audit.totals.trades_observed == 2


def test_symbols_are_independent(tmp_path) -> None:
    _write(tmp_path / "l0", TICK, [
        _tick("000001", 1, 0, 100, 10, "090100", _ns(9, 0, 10)),
        _tick("000002", 1, 0, 100, 10, "090100", _ns(9, 0, 11)),
        _tick("000001", 2, 0, 110, 10, "090101", _ns(9, 0, 20)),
        _tick("000002", 5, 0, 140, 10, "090101", _ns(9, 0, 21)),
        _tick("000001", 3, 0, 120, 10, "090102", _ns(9, 0, 30)),
        _tick("000001", 4, 0, 130, 10, "090103", _ns(9, 0, 40)),
    ])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    by_symbol = {item.symbol: item for item in audit.symbols}
    assert by_symbol["000001"].intervals == ()
    assert by_symbol["000001"].net_missing == 0
    (interval,) = by_symbol["000002"].intervals
    assert interval.missing_trades == 3
    assert audit.totals.symbols_with_loss == 1


def test_symbol_without_counter_is_excluded_from_budget(tmp_path) -> None:
    _write(tmp_path / "l0", TICK, [
        _tick("000001", 1, 0, 100, 10, "090100", _ns(9, 0, 10)),
        _tick("000001", 2, 0, 110, 10, "090101", _ns(9, 0, 20)),
        _tick("000002", 0, 0, 100, 10, "090100", _ns(9, 0, 11)),
        _tick("000002", 0, 0, 110, 10, "090101", _ns(9, 0, 21)),
    ])
    bad = _tick_body("000002", "", "", 120, 10, "090102")
    raw = json.dumps({"header": {"tr_cd": "S3_", "tr_key": "000002"}, "body": bad})
    part = tmp_path / "l0" / "ls" / "krx" / "regular" / TICK / f"dt={DAY.isoformat()}"
    with open(part / "09.jsonl.zst", "ab") as handle:
        handle.write(zstd.ZstdCompressor(level=3).compress((json.dumps(_env(raw, _ns(9, 0, 22))) + "\n").encode()))
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    by_symbol = {item.symbol: item for item in audit.symbols}
    assert by_symbol["000002"].status == "NO_COUNTER"
    assert audit.totals.symbols == 1
    assert audit.totals.symbols_without_counter == 1
    assert audit.totals.trades_observed == 2
    assert audit.totals.loss_ratio == 0.0


def test_partial_decode_failure_skips_frames_without_imputation(tmp_path) -> None:
    records = [
        _tick("005930", 10, 0, 100, 10, "090100", _ns(9, 0, 10)),
        _tick("005930", 0, 0, 0, 0, "090101", _ns(9, 0, 11)),
        _tick("005930", 0, 0, 0, 0, "090102", _ns(9, 0, 12)),
        _tick("005930", 0, 0, 0, 0, "090103", _ns(9, 0, 13)),
        _tick("005930", 12, 0, 120, 10, "090104", _ns(9, 0, 14)),
    ]
    records[1] = _env(json.dumps({"header": {}, "body": {**_tick_body("005930", 0, 0, 0, 0, "090101"), "mdchecnt": "abc"}}), _ns(9, 0, 11))
    records[2] = _env(json.dumps({"header": {}, "body": {**_tick_body("005930", 0, 0, 0, 0, "090102"), "mdchecnt": True}}), _ns(9, 0, 12))
    records[3] = _env(json.dumps({"header": {}, "body": {**_tick_body("005930", 0, 0, 0, 0, "090103"), "mdchecnt": "9" * 5000}}), _ns(9, 0, 13))
    _write(tmp_path / "l0", TICK, records)
    no_symbol = _env(json.dumps({"header": {}, "body": {**_tick_body("005930", 1, 0, 50, 5, "090105"), "shcode": "", "mdchecnt": "zz"}}), _ns(9, 0, 15), symbol="999999")
    untracked = _env(json.dumps({"header": {}, "body": {**_tick_body("005930", 1, 0, 50, 5, "090106"), "shcode": ""}}), _ns(9, 0, 16))
    part = tmp_path / "l0" / "ls" / "krx" / "regular" / TICK / f"dt={DAY.isoformat()}"
    with open(part / "09.jsonl.zst", "ab") as handle:
        handle.write(zstd.ZstdCompressor(level=3).compress(_frames_payload([no_symbol, untracked])))
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    by_symbol = {item.symbol: item for item in audit.symbols}
    assert by_symbol["005930"].decode_fail == 3
    (interval,) = by_symbol["005930"].intervals
    assert (interval.cum_prev, interval.cum_next, interval.missing_trades) == (10, 12, 1)
    assert by_symbol["999999"].status == "NO_COUNTER"
    assert by_symbol["999999"].decode_fail == 1
    assert "" not in by_symbol


def test_price_missing_frame_is_still_accepted_and_singletons_are_ok(tmp_path) -> None:
    body = _tick_body("005930", 7, 0, 100, 10, "090100")
    del body["price"]
    raw = json.dumps({"header": {"tr_cd": "S3_", "tr_key": "005930"}, "body": body})
    _write(tmp_path / "l0", TICK, [_env(raw, _ns(9, 0, 10))])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    (ledger,) = audit.symbols
    assert (ledger.status, ledger.frames, ledger.trades_observed) == ("OK", 1, 1)


def test_stuck_counter_is_no_counter(tmp_path) -> None:
    _write(tmp_path / "l0", TICK, [
        _tick("005930", 7, 0, 100, 5, "090100", _ns(9, 0, 10)),
        _tick("005930", 7, 0, 105, 5, "090101", _ns(9, 0, 20)),
    ])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    (ledger,) = audit.symbols
    assert ledger.status == "NO_COUNTER"
    assert audit.totals.symbols == 0
    assert audit.totals.symbols_without_counter == 1
    assert audit.totals.net_missing_positive == 0


def test_interval_cap_keeps_aggregates_exact(tmp_path) -> None:
    from src.core.config import AuditSettings

    records = []
    cum = volume = 0
    for idx in range(52):
        recv = _ns(9, idx, 0)
        chetime = f"09{idx:02d}00"
        records.append(_tick("005930", cum, 0, volume, 1, chetime, recv))
        cum += idx + 1
        volume += idx + 1
    _write(tmp_path / "l0", TICK, records)
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0", settings=AuditSettings(max_intervals_per_symbol=20))

    (ledger,) = audit.symbols
    assert [iv.missing_trades for iv in ledger.intervals] == list(range(50, 30, -1))
    assert ledger.intervals_truncated == 30
    assert ledger.confirmed_missing == sum(range(1, 51))
    assert audit.totals.loss_ratio == pytest.approx(1275 / 1327, abs=1e-12)


def test_probe_shaped_cut(tmp_path) -> None:
    symbols = [f"{idx:06d}" for idx in range(95)]
    records = []
    for seq, sym in enumerate(symbols):
        records.append(_tick(sym, 60, 40, 1000, 7, "090010", _ns(9, 0, 10), conn="c1", seq=seq * 2 + 1))
        records.append(_tick(sym, 84, 56, 1407, 7, "090045", _ns(9, 1, 10), conn="c2", seq=seq * 2 + 2))
    _write(tmp_path / "l0", TICK, records)
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    assert audit.totals.symbols_with_loss == 95
    assert audit.totals.symbols == 95
    for ledger in audit.symbols:
        (interval,) = ledger.intervals
        assert interval.conn_changed is True
        assert (interval.chetime_prev, interval.chetime_next) == ("090010", "090045")
        assert (interval.missing_trades, interval.volume_deficit) == (39, 400)
    assert audit.totals.loss_ratio == pytest.approx(95 * 39 / (95 * 41), abs=1e-12)


def test_tail_silence_uses_closing_auction_start(tmp_path) -> None:
    _write(tmp_path / "l0", TICK, [
        _tick("005930", 1, 0, 100, 10, "090000", _ns(9, 0, 5)),
        _tick("005930", 2, 0, 200, 10, "143000", _ns(14, 30, 0)),
    ])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    assert audit.last_regular_tick_recv_ns == _ns(14, 30, 0)
    assert audit.tail_silence_s == pytest.approx(3000.0, abs=1e-6)


def test_tail_silence_near_close_is_small(tmp_path) -> None:
    _write(tmp_path / "l0", TICK, [_tick("005930", 1, 0, 100, 10, "151959", _ns(15, 19, 59))])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    assert audit.tail_silence_s == pytest.approx(1.0, abs=1e-6)


def test_aftermarket_intervals_are_informational_only(tmp_path) -> None:
    from src.storage.l0_audit import AuditStatus

    _write(tmp_path / "l0", TICK, [
        _tick("005930", 10, 0, 100, 10, "160001", _ns(16, 0, 1)),
        _tick("005930", 15, 0, 210, 10, "160002", _ns(16, 0, 2)),
    ])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    assert audit.status is AuditStatus.UNAVAILABLE
    assert audit.reason == "no_decodable_records"
    assert audit.tail_silence_s is None
    assert audit.last_regular_tick_recv_ns is None
    (ledger,) = audit.symbols
    assert ledger.status == "OK"
    (interval,) = ledger.intervals
    assert interval.phase == "aftermarket"
    assert interval.missing_trades == 4
    assert audit.totals.confirmed_missing == 0
    assert audit.totals.trades_observed == 0


def test_manifest_frame_count_is_informational(tmp_path) -> None:
    _write(tmp_path / "l0", TICK, [
        _tick("005930", 1, 0, 100, 10, "090100", _ns(9, 0, 10)),
        _tick("005930", 2, 0, 110, 10, "090101", _ns(9, 0, 20)),
        _tick("000001", 1, 0, 50, 5, "090100", _ns(9, 0, 11)),
    ])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0", manifest_frame_counts={"005930": {TICK: 999}, "ghost": {TICK: 1}})

    by_symbol = {item.symbol: item for item in audit.symbols}
    assert by_symbol["005930"].manifest_frames == 999
    assert by_symbol["000001"].manifest_frames is None
    assert by_symbol["005930"].status == "OK"

    plain = _derive(tmp_path / "l0")
    assert {item.symbol: item for item in plain.symbols}["005930"].manifest_frames is None


def test_missing_journal_is_unavailable(tmp_path) -> None:
    from src.storage.l0_audit import AuditStatus

    calls = []
    audit = _derive(tmp_path / "l0", progress=lambda: calls.append(1))

    assert audit.status is AuditStatus.UNAVAILABLE
    assert audit.reason == "journal_missing"
    assert audit.sources == ()
    assert calls == [1]


def test_placeholder_journal_is_unavailable(tmp_path) -> None:
    from src.storage.l0_audit import AuditStatus

    part = tmp_path / "l0" / "ls" / "krx" / "regular" / TICK / f"dt={DAY.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    (part / "10.jsonl.zst").write_bytes(b"x")

    audit = _derive(tmp_path / "l0")

    assert audit.status is AuditStatus.UNAVAILABLE
    assert audit.reason == "io_error"
    (source,) = audit.sources
    assert (source.rows, source.truncated) == (0, True)


def test_truncated_tail_keeps_prefix_and_is_partial(tmp_path) -> None:
    from src.storage.l0_audit import AuditStatus

    first = [_tick("005930", idx + 1, 0, 100 + idx * 10, 10, f"09010{idx}", _ns(9, 1, 10 + idx)) for idx in range(3)]
    second = [_tick("005930", idx + 4, 0, 130 + idx * 10, 10, f"09010{idx + 3}", _ns(9, 1, 13 + idx)) for idx in range(2)]
    blob = zstd.ZstdCompressor(level=3).compress(_frames_payload(first)) + zstd.ZstdCompressor(level=3).compress(_frames_payload(second))
    _write(tmp_path / "l0", TICK, [], name="09.jsonl.zst", raw_frames=[blob[:-12]])

    audit = _derive(tmp_path / "l0")

    (source,) = audit.sources
    assert source.truncated is True
    assert source.rows == 3
    by_stream = {item.stream: item for item in audit.latency}
    assert by_stream[TICK].status is AuditStatus.PARTIAL
    assert audit.status is AuditStatus.PARTIAL
    (ledger,) = audit.symbols
    assert ledger.net_missing == 0


def test_multi_frame_file_decodes_every_frame(tmp_path) -> None:
    from src.storage.l0_audit import AuditStatus

    first = [_tick("005930", 1, 0, 100, 10, "090100", _ns(9, 1, 10))]
    second = [_tick("005930", 2, 0, 110, 10, "090101", _ns(9, 1, 20))]
    frames = [
        zstd.ZstdCompressor(level=3).compress(_frames_payload(first)),
        zstd.ZstdCompressor(level=3).compress(_frames_payload(second)),
        zstd.ZstdCompressor(level=3).compress(b""),
    ]
    _write(tmp_path / "l0", TICK, [], name="09.jsonl.zst", raw_frames=[b"".join(frames)])

    audit = _derive(tmp_path / "l0")

    (source,) = audit.sources
    assert (source.rows, source.truncated) == (2, False)
    by_stream = {item.stream: item for item in audit.latency}
    assert by_stream[TICK].status is AuditStatus.OK
    assert audit.status is AuditStatus.PARTIAL


def test_trailing_garbage_after_a_frame_is_truncated(tmp_path) -> None:
    from src.storage.l0_audit import AuditStatus

    good = [_tick("005930", 1, 0, 100, 10, "090100", _ns(9, 1, 10))]
    blob = zstd.ZstdCompressor(level=3).compress(_frames_payload(good)) + b"GARBAGE!!!"
    _write(tmp_path / "l0", TICK, [], name="09.jsonl.zst", raw_frames=[blob])

    audit = _derive(tmp_path / "l0")

    (source,) = audit.sources
    assert (source.rows, source.truncated) == (1, True)
    by_stream = {item.stream: item for item in audit.latency}
    assert by_stream[TICK].status is AuditStatus.PARTIAL


def test_empty_file_reports_no_decodable_records(tmp_path) -> None:
    from src.storage.l0_audit import AuditStatus

    part = tmp_path / "l0" / "ls" / "krx" / "regular" / TICK / f"dt={DAY.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    (part / "09.jsonl.zst").write_bytes(b"")

    audit = _derive(tmp_path / "l0")

    assert audit.status is AuditStatus.UNAVAILABLE
    assert audit.reason == "no_decodable_records"


def test_unreadable_and_missing_files_mark_stream_partial(tmp_path) -> None:
    from src.storage.l0_audit import AuditStatus

    part = _write(tmp_path / "l0", TICK, [
        _tick("005930", 1, 0, 100, 10, "090100", _ns(9, 1, 10)),
        _tick("005930", 2, 0, 110, 10, "090101", _ns(9, 1, 20)),
    ], name="10.jsonl.zst")
    (part / "09.jsonl.zst").mkdir()
    dangling = part / "11.jsonl.zst"
    dangling.symlink_to(part / "gone.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    by_source = {source.path.rsplit("/", 1)[-1]: source for source in audit.sources}
    assert by_source["09.jsonl.zst"].rows == 0
    assert by_source["09.jsonl.zst"].truncated is True
    assert by_source["10.jsonl.zst"].rows == 2
    assert by_source["11.jsonl.zst"].bytes == 0
    by_stream = {item.stream: item for item in audit.latency}
    assert by_stream[TICK].status is AuditStatus.PARTIAL
    assert audit.status is AuditStatus.PARTIAL


def test_garbage_lines_are_skipped(tmp_path) -> None:
    recv = _ns(9, 1, 10)
    good = _tick("005930", 1, 0, 100, 10, "090100", recv)
    lines = [
        b"{bad json",
        b"[1, 2]",
        json.dumps({"nope": 1}).encode(),
        json.dumps({"raw": 5, "recv_wall_ns": recv}).encode(),
        json.dumps({"raw": "x", "recv_wall_ns": "soon"}).encode(),
        json.dumps({"raw": "[1]", "recv_wall_ns": recv}).encode(),
        json.dumps({"raw": '{"body": 5}', "recv_wall_ns": recv}).encode(),
        json.dumps({"raw": "definitely-not-json", "recv_wall_ns": recv}).encode(),
    ]
    payload = b"\n".join(lines) + b"\n" + (json.dumps(good) + "\n").encode()
    part = tmp_path / "l0" / "ls" / "krx" / "regular" / TICK / f"dt={DAY.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    (part / "09.jsonl.zst").write_bytes(zstd.ZstdCompressor(level=3).compress(payload))
    envelope_tick = _tick("005930", 2, 0, 110, 10, "090101", _ns(9, 2, 10), exch="090200")
    with open(part / "09.jsonl.zst", "ab") as handle:
        handle.write(zstd.ZstdCompressor(level=3).compress(_frames_payload([envelope_tick])))

    audit = _derive(tmp_path / "l0")

    (entry,) = [item for item in audit.latency if item.stream == TICK]
    assert entry.rows == 5
    assert entry.event_time_missing == 3
    assert [m.minute_kst for m in entry.minutes] == ["09:01", "09:02"]


def test_unsupported_vendor_reports_ledger_unavailable(tmp_path) -> None:
    _write(tmp_path / "l0", TICK, [_tick("005930", 1, 0, 100, 10, "090100", _ns(9, 0, 10))], vendor="db")

    audit = _derive(tmp_path / "l0", vendor="db")

    assert audit.status.value == "UNAVAILABLE"
    assert audit.reason == "unsupported_vendor"
    assert audit.symbols == ()


def test_time_budget_discards_partials(tmp_path) -> None:
    from src.core.config import AuditSettings
    from src.storage.l0_audit import AuditStatus

    _write(tmp_path / "l0", TICK, [_tick("005930", 1, 0, 100, 10, "090100", _ns(9, 0, 10))])
    calls = {"count": 0}

    def _mono():
        calls["count"] += 1
        return 0.0 if calls["count"] <= 2 else 1000.0

    audit = _derive(tmp_path / "l0", settings=AuditSettings(time_budget_s=0.001), monotonic=_mono)

    assert audit.status is AuditStatus.UNAVAILABLE
    assert audit.reason == "time_budget"
    assert audit.latency == ()
    assert audit.sources == ()
    assert audit.symbols == ()


def test_progress_fires_per_file_and_for_tiny_inputs(tmp_path) -> None:
    for name in ("09.jsonl.zst", "10.jsonl.zst", "11.jsonl.zst"):
        _write(tmp_path / "l0", TICK, [
            _tick("005930", 1, 0, 100, 10, "090100", _ns(9, 0, 10)),
            _tick("005930", 2, 0, 110, 10, "090101", _ns(9, 0, 20)),
        ], name=name)

    calls = []
    _derive(tmp_path / "l0", progress=lambda: calls.append(1))
    assert len(calls) >= 3


def test_derivation_is_read_only(tmp_path) -> None:
    _write(tmp_path / "l0", TICK, [_tick("005930", 1, 0, 100, 10, "090100", _ns(9, 0, 10))])
    part = tmp_path / "l0" / "ls" / "krx" / "regular" / TICK / f"dt={DAY.isoformat()}"
    target = part / "09.jsonl.zst"
    before = (target.read_bytes(), target.stat().st_mtime_ns)

    _derive(tmp_path / "l0")

    assert (target.read_bytes(), target.stat().st_mtime_ns) == before


def test_causal_invariance_for_finished_intervals(tmp_path) -> None:
    base = [
        _tick("005930", 10, 0, 100, 10, "090010", _ns(9, 0, 10)),
        _tick("005930", 12, 0, 120, 10, "090050", _ns(9, 0, 50)),
        _tick("005930", 13, 0, 130, 10, "090130", _ns(9, 1, 30)),
        _tick("005930", 20, 0, 200, 10, "090500", _ns(9, 5, 0)),
    ]
    variant = [
        _tick("005930", 10, 0, 100, 10, "090010", _ns(9, 0, 10)),
        _tick("005930", 12, 0, 120, 10, "090050", _ns(9, 0, 50)),
        _tick("005930", 15, 0, 999, 99, "090131", _ns(9, 1, 31)),
        _tick("005930", 25, 0, 5, 1, "090501", _ns(9, 5, 1)),
    ]
    _write(tmp_path / "l0", TICK, base)
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")
    first = _derive(tmp_path / "l0")

    import shutil

    shutil.rmtree(tmp_path / "l0")
    _write(tmp_path / "l0", TICK, variant)
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")
    second = _derive(tmp_path / "l0")

    cutoff = _ns(9, 1, 0)

    def _early(audit):
        return [iv for item in audit.symbols for iv in item.intervals if iv.recv_next_ns <= cutoff]

    assert _early(first) == _early(second) != []


def test_deterministic_artifact_bytes(tmp_path) -> None:
    from src.storage.l0_audit import _audit_to_jsonable, write_session_audit

    _write(tmp_path / "l0", TICK, [
        _tick("005930", 100, 0, 1000, 7, "090010", _ns(9, 0, 10)),
        _tick("005930", 105, 0, 1407, 7, "090045", _ns(9, 1, 10)),
    ])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    first = _derive(tmp_path / "l0")
    second = _derive(tmp_path / "l0")

    assert [item.symbol for item in first.symbols] == sorted(item.symbol for item in first.symbols)
    payload_first = dict(_audit_to_jsonable(first))
    payload_second = dict(_audit_to_jsonable(second))
    del payload_first["generated_at_ns"]
    del payload_second["generated_at_ns"]
    assert payload_first == payload_second

    out = tmp_path / "manifest" / "audit"
    write_session_audit(first, out / "a.json")
    write_session_audit(second, out / "b.json")
    first_bytes = json.loads((out / "a.json").read_text(encoding="utf-8"))
    second_bytes = json.loads((out / "b.json").read_text(encoding="utf-8"))
    del first_bytes["generated_at_ns"]
    del second_bytes["generated_at_ns"]
    assert first_bytes == second_bytes


def test_atomic_write_keeps_previous_artifact(tmp_path, monkeypatch) -> None:
    import os

    from src.storage.l0_audit import AuditStatus, write_session_audit

    _write(tmp_path / "l0", TICK, [_tick("005930", 1, 0, 100, 10, "090100", _ns(9, 0, 10))])
    audit = _derive(tmp_path / "l0")
    assert audit.status is AuditStatus.PARTIAL
    target = tmp_path / "manifest" / "audit" / "2026-10-15.ls.regular.audit.json"
    write_session_audit(audit, target)
    before = target.read_bytes()

    def _boom(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", _boom)
    try:
        write_session_audit(audit, target)
        raise AssertionError("expected OSError")
    except OSError:
        pass

    assert target.read_bytes() == before
    assert [path for path in target.parent.iterdir() if path.name.startswith(f".{target.name}.")] == []


def test_payload_hygiene(tmp_path) -> None:
    body = _tick_body("005930", 1, 0, 100, 10, "090100")
    body["note"] = "SECRET-TOKEN-xyz"
    raw = json.dumps({"header": {"tr_cd": "S3_", "tr_key": "005930"}, "body": body})
    _write(tmp_path / "l0", TICK, [_env(raw, _ns(9, 0, 10), conn="conn-9")])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    from src.storage.l0_audit import _audit_to_jsonable

    text = json.dumps(_audit_to_jsonable(audit))
    assert "SECRET-TOKEN-xyz" not in text
    assert "conn-9" not in text
    assert "raw" not in json.loads(text)["symbols"][0]


def test_records_without_connection_tracking_are_accepted(tmp_path) -> None:
    record = _tick("005930", 1, 0, 100, 10, "090100", _ns(9, 0, 10))
    del record["conn_id"]
    del record["conn_seq"]
    _write(tmp_path / "l0", TICK, [record])
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    (ledger,) = audit.symbols
    assert (ledger.status, ledger.frames) == ("OK", 1)


def test_oversized_recv_wall_is_skipped(tmp_path) -> None:
    _write(tmp_path / "l0", TICK, [
        _tick("005930", 1, 0, 100, 10, "090100", 10**30),
        _tick("005930", 2, 0, 110, 10, "090101", _ns(9, 0, 20)),
    ])

    audit = _derive(tmp_path / "l0")

    (entry,) = [item for item in audit.latency if item.stream == TICK]
    assert entry.rows == 1


def test_late_file_merges_into_earlier_minute(tmp_path) -> None:
    _write(tmp_path / "l0", TICK, [_tick("005930", 10, 0, 100, 10, "090200", _ns(9, 2, 10))], name="09.jsonl.zst")
    _write(tmp_path / "l0", TICK, [_tick("005930", 11, 0, 110, 10, "090100", _ns(9, 1, 10))], name="10.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    (entry,) = [item for item in audit.latency if item.stream == TICK]
    assert [(m.minute_kst, m.count) for m in entry.minutes] == [("09:01", 1), ("09:02", 1)]
    assert audit.last_regular_tick_recv_ns == _ns(9, 2, 10)


def test_line_batch_reader_streams_without_loading(tmp_path) -> None:
    from src.storage.l0_audit import _iter_line_batches

    target = tmp_path / "09.jsonl.zst"
    lines = [b"x" * 100 + b"\n", b"y" * 50 + b"\n"]
    target.write_bytes(zstd.ZstdCompressor(level=3).compress(b"".join(lines)))
    truncated: list[bool] = [False]

    assert b"".join(_iter_line_batches(target, 8, truncated)) == b"".join(lines)
    assert truncated == [False]

    empty = tmp_path / "empty.jsonl.zst"
    empty.write_bytes(b"")
    assert list(_iter_line_batches(empty, 8, [False])) == []


def test_reader_flags_mid_frame_truncation_with_prefix(tmp_path) -> None:
    from src.storage.l0_audit import _iter_line_batches

    records = [
        (json.dumps({"raw": f"r{i:08d}", "recv_wall_ns": 1700000000000000000 + i}) + "\n").encode()
        for i in range(60000)
    ]
    payload = b"".join(records)
    frame = zstd.ZstdCompressor(level=3).compress(payload)
    target = tmp_path / "cut.jsonl.zst"
    target.write_bytes(frame[: len(frame) // 2])
    truncated: list[bool] = [False]

    blobs = list(_iter_line_batches(target, 8192, truncated))

    assert truncated == [True]
    decoded = b"".join(blobs)
    assert decoded
    assert len(decoded) < len(payload)
    assert payload.startswith(decoded)
    assert decoded.count(b"\n") >= 1


def test_invalid_route_is_unavailable_without_raising(tmp_path) -> None:
    from src.storage.l0_audit import AuditStatus

    venue_audit = _derive(tmp_path / "l0", venue="xx")
    assert venue_audit.status is AuditStatus.UNAVAILABLE
    assert venue_audit.reason == "journal_missing"
    assert all(item.status is AuditStatus.UNAVAILABLE for item in venue_audit.latency)

    session_audit = _derive(tmp_path / "l0", session="xx")
    assert session_audit.status is AuditStatus.UNAVAILABLE
    assert session_audit.reason == "journal_missing"


def test_wall_clock_failure_maps_to_io_error(tmp_path) -> None:
    from src.storage.l0_audit import AuditStatus

    _write(tmp_path / "l0", TICK, [_tick("005930", 1, 0, 100, 10, "090100", _ns(9, 0, 10))])

    def _boom() -> int:
        raise OSError("clock")

    audit = _derive(tmp_path / "l0", wall_ns=_boom)

    assert audit.status is AuditStatus.UNAVAILABLE
    assert audit.reason == "io_error"
    assert audit.generated_at_ns == 0


def test_revisited_receive_minute_is_merged_exactly(tmp_path) -> None:
    _write(tmp_path, TICK, [
        _tick("005930", 1, 0, 10, 10, "090100", _ns(9, 1, 1)),
        _tick("005930", 2, 0, 20, 10, "090200", _ns(9, 2, 2)),
        _tick("005930", 3, 0, 30, 10, "090100", _ns(9, 1, 3)),
        _tick("005930", 4, 0, 40, 10, "090200", _ns(9, 2, 4)),
    ])
    audit = _derive(tmp_path)
    tick = next(entry for entry in audit.latency if entry.stream == TICK)
    assert [(m.minute_kst, m.count, m.p50_s, m.max_s) for m in tick.minutes] == [
        ("09:01", 2, 1.0, 3.0), ("09:02", 2, 2.0, 4.0),
    ]


def test_nanosecond_latency_and_bucket_boundary_are_preserved(tmp_path) -> None:
    _write(tmp_path, TICK, [
        _tick("005930", 1, 0, 10, 10, "090100", _ns(9, 1, 10) + 1),
        _tick("005930", 2, 0, 20, 10, "090159", _ns(9, 2) - 1),
    ])
    audit = _derive(tmp_path)
    tick = next(entry for entry in audit.latency if entry.stream == TICK)
    assert len(tick.minutes) == 1
    assert tick.minutes[0].count == 2
    assert tick.minutes[0].max_s == 10.000000001


def test_reset_keeps_pre_reset_conservation_and_interval_totals(tmp_path) -> None:
    _write(tmp_path, TICK, [
        _tick("005930", cum, 0, volume, 10, f"09010{i}", _ns(9, 1, i))
        for i, (cum, volume) in enumerate([(100, 1000), (105, 1050), (2, 20), (3, 30), (4, 40), (7, 70)])
    ])
    audit = _derive(tmp_path)
    ledger, = audit.symbols
    assert (ledger.resets, ledger.frames, ledger.trades_observed, ledger.net_missing) == (1, 6, 12, 6)
    assert ledger.confirmed_missing == 6
    assert sum(iv.missing_trades for iv in ledger.intervals) == 6


def test_repeated_regression_does_not_confirm_reset(tmp_path) -> None:
    low = _tick("005930", 2, 0, 20, 10, "090101", _ns(9, 1, 1))
    _write(tmp_path, TICK, [
        _tick("005930", 500, 0, 5000, 10, "090100", _ns(9, 1)),
        low, low, low,
        _tick("005930", 501, 0, 5010, 10, "090102", _ns(9, 1, 2)),
    ])
    ledger, = _derive(tmp_path).symbols
    assert (ledger.resets, ledger.regressions, ledger.duplicates) == (0, 1, 2)
    assert ledger.net_missing == 0


def test_corrupt_envelope_cannot_leave_ok_status(tmp_path) -> None:
    part = _write(tmp_path, TICK, [_tick("005930", 1, 0, 10, 10, "090100", _ns(9, 1))])
    with (part / "09.jsonl.zst").open("ab") as handle:
        handle.write(zstd.ZstdCompressor().compress(b"broken\n"))
    audit = _derive(tmp_path)
    assert audit.status.value == "PARTIAL"
    assert audit.sources[0].truncated
    assert audit.latency[0].status.value == "PARTIAL"


def test_latency_selection_does_not_disable_trade_ledger(tmp_path) -> None:
    from src.core.config import AuditSettings

    _write(tmp_path, TICK, [
        _tick("005930", 1, 0, 10, 10, "090100", _ns(9, 1)),
        _tick("005930", 4, 0, 40, 10, "090101", _ns(9, 1, 1)),
    ])
    _write(tmp_path, QUOTE, [_ok_quote()])
    audit = _derive(tmp_path, settings=AuditSettings(latency_streams=(QUOTE,)))
    assert audit.status.value == "OK"
    assert [entry.stream for entry in audit.latency] == [QUOTE]
    assert audit.totals.confirmed_missing == 2


def test_zstd_frame_split_inside_json_line_is_lossless(tmp_path) -> None:
    from src.storage.l0_audit import _iter_line_batches

    payload = _frames_payload([_tick("005930", 1, 0, 10, 10, "090100", _ns(9, 1))])
    cut = len(payload) // 2
    path = tmp_path / "split.zst"
    path.write_bytes(zstd.ZstdCompressor().compress(payload[:cut]) + zstd.ZstdCompressor().compress(payload[cut:]))
    truncated = [False]
    assert b"".join(_iter_line_batches(path, 32, truncated)) == payload
    assert not truncated[0]


def test_compressed_expansion_is_batched_and_progresses(tmp_path) -> None:
    from src.storage.l0_audit import _iter_line_batches

    path = tmp_path / "large.zst"
    payload = b"x\n" * 1000000
    path.write_bytes(zstd.ZstdCompressor(write_checksum=True).compress(payload))
    chunks = list(_iter_line_batches(path, 32768, [False]))
    assert len(chunks) > 50
    assert max(map(len, chunks)) <= 32768
    assert b"".join(chunks) == payload


def test_budget_checked_even_without_files(tmp_path) -> None:
    from src.core.config import AuditSettings

    ticks = iter([0.0, 0.0, 301.0, 301.0])
    audit = _derive(tmp_path, settings=AuditSettings(time_budget_s=300.0), monotonic=lambda: next(ticks))
    assert audit.reason == "time_budget"


def test_revisited_quote_minute_ignores_bad_times_and_preserves_envelope(tmp_path) -> None:
    _write(tmp_path, QUOTE, [
        _quote("005930", "090100", _ns(9, 1, 1)),
        _quote("005930", "090200", _ns(9, 2, 1)),
        _quote("005930", "090100", _ns(9, 1, 3)),
        _quote("005930", "", _ns(9, 1, 4)),
        _quote("005930", "", _ns(9, 1, 5), exch="090100"),
        _quote("005930", "090100", 10**30),
    ])
    audit = _derive(tmp_path)
    quote = next(entry for entry in audit.latency if entry.stream == QUOTE)
    assert [(m.minute_kst, m.count, m.p50_s) for m in quote.minutes] == [("09:01", 3, 3.0), ("09:02", 1, 1.0)]
    assert quote.event_time_missing == 1
    assert quote.status.value == "PARTIAL"


@pytest.mark.parametrize("fault", ["block_header", "block_kind", "checksum"])
def test_incomplete_or_invalid_zstd_block_fails_closed(tmp_path, fault) -> None:
    from src.storage.l0_audit import _iter_line_batches

    frame = zstd.ZstdCompressor(write_checksum=True).compress(b"valid\n")
    header_size = zstd.frame_header_size(frame[:18])
    if fault == "block_header":
        frame = frame[:header_size + 1]
    elif fault == "block_kind":
        frame = frame[:header_size] + b"\x07\x00\x00"
    else:
        frame = frame[:-2]
    path = tmp_path / "bad.zst"
    path.write_bytes(frame)
    truncated = [False]
    list(_iter_line_batches(path, 64, truncated))
    assert truncated[0]


def test_oversized_unterminated_record_stops_reader(tmp_path, monkeypatch) -> None:
    import src.storage.l0_audit as module

    monkeypatch.setattr(module, "_BATCH_BYTES", 64)
    path = tmp_path / "oversized.zst"
    path.write_bytes(zstd.ZstdCompressor().compress(b"x" * 128))
    truncated = [False]
    assert list(module._iter_line_batches(path, 64, truncated)) == []
    assert truncated[0]


def test_sequence_replay_is_exact_after_cache_eviction(tmp_path, monkeypatch) -> None:
    import src.storage.l0_audit as module

    monkeypatch.setattr(module, "_SEQUENCE_CACHE_SIZE", 1)
    first = _tick("005930", 1, 0, 10, 10, "090100", _ns(9, 1), conn="a", seq=1)
    part = _write(tmp_path, TICK, [first])
    with (part / "09.jsonl.zst").open("ab") as handle:
        handle.write(zstd.ZstdCompressor().compress(b"\n"))
    _write(tmp_path, TICK, [
        _tick("005930", 2, 0, 20, 10, "090101", _ns(9, 1, 1), conn="b", seq=1),
        first,
        _tick("005930", 3, 0, 30, 10, "090102", _ns(9, 1, 2), conn="a", seq=2),
    ], name="10.jsonl.zst")
    ledger, = _derive(tmp_path).symbols
    assert (ledger.frames, ledger.duplicates, ledger.regressions) == (3, 1, 0)
    assert ledger.net_missing == 0


def test_event_holes_helper_boundaries() -> None:
    from src.storage.l0_audit import EventHole, _event_holes

    start, end = 9 * 3600, 9 * 3600 + 60
    seconds = {start, start + 1, start + 20, start + 21, end - 1}
    holes, total = _event_holes(seconds, start_s=start, end_s=end, min_hole_s=10, cap=20)
    assert holes == (EventHole("090002", "090019", 18), EventHole("090022", "090058", 37))
    assert total == 55
    assert _event_holes(seconds, start_s=start, end_s=end, min_hole_s=10, cap=1)[0] == (EventHole("090022", "090058", 37),)
    assert _event_holes({start + 5}, start_s=start, end_s=end, min_hole_s=60, cap=5) == ((), 0)
    assert _event_holes({start - 100}, start_s=start, end_s=end, min_hole_s=1, cap=5) == ((), 0)
    assert _event_holes(set(range(start, end)), start_s=start, end_s=end, min_hole_s=1, cap=5) == ((), 0)


def test_leading_hole_marks_a_late_first_event() -> None:
    from src.storage.l0_audit import EventHole, _event_holes

    start, end = 9 * 3600, 9 * 3600 + 30
    holes, total = _event_holes({start + 12}, start_s=start, end_s=end, min_hole_s=10, cap=5)
    assert holes == (EventHole("090000", "090011", 12), EventHole("090013", "090029", 17))
    assert total == 29


def test_derivation_reports_event_time_hole_across_symbols(tmp_path) -> None:
    records = [
        _tick(symbol, second, 0, 100 + second, 1, f"0900{second:02d}", _ns(9, 0, second))
        for second in (0, 1, 2, 3, 20, 21)
        for symbol in ("005930", "000660")
    ]
    _write(tmp_path / "l0", TICK, records)
    _write(tmp_path / "l0", QUOTE, [_quote("005930", "090005", _ns(9, 0, 5))])

    audit = _derive(tmp_path / "l0")

    (entry,) = [item for item in audit.latency if item.stream == TICK]
    assert (entry.event_holes[0].start, entry.event_holes[0].end, entry.event_holes[0].seconds) == ("090004", "090019", 16)
    assert entry.event_hole_seconds >= 16


def test_audit_json_carries_event_holes_and_default_cap_keeps_all_intervals(tmp_path) -> None:
    import json as _json

    from src.core.config import AuditSettings
    from src.storage.l0_audit import write_session_audit

    records = []
    cum = volume = 0
    for idx in range(52):
        records.append(_tick("005930", cum, 0, volume, 1, f"09{idx:02d}00", _ns(9, idx, 0)))
        cum += 2
        volume += 2
    _write(tmp_path / "l0", TICK, records)
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    assert AuditSettings().max_intervals_per_symbol >= 500
    (ledger,) = audit.symbols
    assert ledger.intervals_truncated == 0
    out = tmp_path / "audit.json"
    write_session_audit(audit, out)
    payload = _json.loads(out.read_text(encoding="utf-8"))
    tick_entry = next(item for item in payload["latency"] if item["stream"] == TICK)
    assert tick_entry["event_holes"]
    assert tick_entry["event_hole_seconds"] > 0


def test_late_frames_from_a_handed_over_connection_shrink_the_loss_interval(tmp_path) -> None:
    records = [
        _tick("005930", 0, 1, 10, 10, "090000", _ns(9, 0, 1), conn="A"),
        _tick("005930", 0, 2, 20, 10, "090001", _ns(9, 0, 2), conn="A"),
        _tick("005930", 0, 6, 60, 10, "090005", _ns(9, 0, 5), conn="B"),
        _tick("005930", 0, 3, 30, 10, "090002", _ns(9, 0, 6), conn="A"),
        _tick("005930", 0, 4, 40, 10, "090003", _ns(9, 0, 7), conn="A"),
        _tick("005930", 0, 2, 20, 10, "090001", _ns(9, 0, 8), conn="A"),
        _tick("005930", 0, 7, 70, 10, "090006", _ns(9, 0, 9), conn="B"),
    ]
    _write(tmp_path / "l0", TICK, records)
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    (ledger,) = audit.symbols
    assert ledger.trades_observed == 7
    assert ledger.confirmed_missing == 1
    assert ledger.volume_deficit == 10
    assert ledger.regressions == 0
    assert ledger.resets == 0
    assert ledger.duplicates >= 1
    assert [(iv.cum_prev, iv.cum_next, iv.missing_trades) for iv in ledger.intervals] == [(2, 6, 1)]


def test_overlap_fill_removes_an_interval_completely(tmp_path) -> None:
    records = [
        _tick("005930", 0, 1, 10, 10, "090000", _ns(9, 0, 1), conn="A"),
        _tick("005930", 0, 3, 30, 10, "090002", _ns(9, 0, 2), conn="B"),
        _tick("005930", 0, 2, 20, 20, "090001", _ns(9, 0, 3), conn="A"),
    ]
    _write(tmp_path / "l0", TICK, records)
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    (ledger,) = audit.symbols
    assert ledger.intervals == ()
    assert ledger.confirmed_missing == 0
    assert ledger.count_only_missing == 0
    assert ledger.volume_deficit == 0
    assert ledger.net_missing == 0


def test_overlap_fill_of_a_count_only_interval(tmp_path) -> None:
    records = [
        _tick("005930", 0, 1, 10, 10, "090000", _ns(9, 0, 1), conn="A"),
        _tick("005930", 0, 4, 40, 30, "090003", _ns(9, 0, 2), conn="B"),
        _tick("005930", 0, 2, 20, 10, "090001", _ns(9, 0, 3), conn="A"),
    ]
    _write(tmp_path / "l0", TICK, records)
    _write(tmp_path / "l0", QUOTE, [_ok_quote()], name="09.jsonl.zst")

    audit = _derive(tmp_path / "l0")

    (ledger,) = audit.symbols
    assert ledger.count_only_missing == 1
    assert ledger.confirmed_missing == 0
    assert [(iv.kind, iv.missing_trades) for iv in ledger.intervals] == [("count_only", 1)]


def _hand_fixture_records():
    a1 = _tick("000001", 1, 0, 100, 10, "090001", _ns(9, 0, 3), seq=1)
    a2 = _tick("000001", 2, 0, 110, 10, "090002", _ns(9, 2, 10), seq=2)
    b1 = _tick("000002", 1, 0, 100, 10, "090001", _ns(9, 1, 3), seq=3)
    b2 = _tick("000002", 2, 0, 110, 10, "090003", _ns(9, 1, 5), seq=4)
    return a1, a2, b1, b2


def test_interleaved_shards_equal_single_stream(tmp_path):
    from src.core.config import AuditSettings

    a1, a2, b1, b2 = _hand_fixture_records()
    single = tmp_path / "single"
    _write(single, TICK, [a1, a2, b1, b2])
    shards = tmp_path / "shards"
    _write(shards, TICK, [a1], name="09.s0.jsonl.zst")
    _write(shards, TICK, [b1], name="09.s1.jsonl.zst")
    _write(shards, TICK, [a2], name="09.s2.jsonl.zst")
    _write(shards, TICK, [b2], name="09.s3.jsonl.zst")

    settings = AuditSettings(latency_streams=(TICK,))
    first = _derive(single, settings=settings, wall_ns=lambda: 42)
    second = _derive(shards, settings=settings, wall_ns=lambda: 42)

    assert second.latency == first.latency
    assert second.totals == first.totals
    assert second.symbols == first.symbols
    tick = next(entry for entry in second.latency if entry.stream == TICK)
    assert [(m.minute_kst, m.count, m.p50_s, m.max_s, m.min_s) for m in tick.minutes] == [
        ("09:00", 1, 2.0, 2.0, 2.0),
        ("09:01", 2, 62.0, 62.0, 62.0),
        ("09:02", 1, 128.0, 128.0, 128.0),
    ]
    assert second.totals.trades_observed == 4
    assert second.totals.confirmed_missing == 0
    assert second.totals.symbols == 2


@pytest.mark.parametrize("n_minutes", [10, 390])
def test_file_passes_independent_of_dirty_minutes(tmp_path, monkeypatch, n_minutes):
    import datetime as dt

    import src.storage.l0_audit as module
    from src.core.config import AuditSettings

    calls: list[str] = []
    real = module._iter_line_batches

    def counting(path, batch_bytes, truncated):
        calls.append(path.name)
        yield from real(path, batch_bytes, truncated)

    monkeypatch.setattr(module, "_iter_line_batches", counting)
    names = [f"09.s{i}.jsonl.zst" for i in range(4)]
    for idx, name in enumerate(names):
        records = []
        for pos in range(n_minutes):
            recv = int(dt.datetime(DAY.year, DAY.month, DAY.day, 9 + pos // 60, pos % 60, 10, tzinfo=KST).timestamp() * 1e9)
            records.append(_tick("005930", idx * n_minutes + pos + 1, 0, 100, 10, "090001", recv, seq=idx * n_minutes + pos + 1))
        _write(tmp_path, TICK, records, name=name)

    _derive(tmp_path, settings=AuditSettings(latency_streams=(TICK,)))

    assert sorted(calls) == sorted(names)


def test_median_is_lower_median_over_merged_samples(tmp_path):
    from src.core.config import AuditSettings

    first = [
        _tick("005930", 1, 0, 100, 10, "090100", _ns(9, 1, 1), seq=1),
        _tick("005930", 2, 0, 110, 10, "090100", _ns(9, 1, 4), seq=2),
    ]
    second = [
        _tick("005930", 3, 0, 120, 10, "090100", _ns(9, 1, 2), seq=3),
        _tick("005930", 4, 0, 130, 10, "090100", _ns(9, 1, 3), seq=4),
    ]
    _write(tmp_path, TICK, first, name="09.jsonl.zst")
    _write(tmp_path, TICK, second, name="10.jsonl.zst")

    audit = _derive(tmp_path, settings=AuditSettings(latency_streams=(TICK,)))
    (entry,) = [item for item in audit.latency if item.stream == TICK]
    (minute,) = entry.minutes
    ordered = sorted([1.0, 4.0, 2.0, 3.0])
    assert minute.count == 4
    assert minute.p50_s == ordered[(len(ordered) - 1) // 2] == 2.0
    assert (minute.min_s, minute.max_s) == (1.0, 4.0)


def test_budget_overflow_preserves_exactness(tmp_path, caplog, monkeypatch):
    import logging
    import re

    from src.core.config import AuditSettings

    records = [
        _tick("005930", 1, 0, 100, 10, "090001", _ns(9, 0, 3), seq=1),
        _tick("005930", 2, 0, 110, 10, "090001", _ns(9, 0, 4), seq=2),
        _tick("005930", 3, 0, 120, 10, "090001", _ns(9, 0, 5), seq=3),
        _tick("005930", 4, 0, 130, 10, "090001", _ns(9, 1, 3), seq=4),
        _tick("005930", 5, 0, 140, 10, "090001", _ns(9, 2, 10), seq=5),
        _tick("005930", 6, 0, 150, 10, "090001", _ns(9, 2, 11), seq=6),
        _tick("005930", 7, 0, 160, 10, "090001", _ns(9, 2, 12), seq=7),
        _tick("005930", 8, 0, 170, 10, "090001", _ns(9, 2, 13), seq=8),
        _tick("005930", 9, 0, 180, 10, "090001", _ns(9, 2, 14), seq=9),
    ]
    _write(tmp_path, TICK, records)

    plain = _derive(tmp_path, settings=AuditSettings(latency_streams=(TICK,)), wall_ns=lambda: 42)
    with caplog.at_level(logging.INFO, logger="src.storage.l0_audit"):
        caplog.clear()
        tight = _derive(
            tmp_path,
            settings=_sample_settings(monkeypatch, 104, latency_streams=(TICK,)),
            wall_ns=lambda: 42,
        )
    assert tight.latency == plain.latency
    assert tight.totals == plain.totals
    assert tight.symbols == plain.symbols
    tick = next(entry for entry in tight.latency if entry.stream == TICK)
    assert [(m.minute_kst, m.count, m.p50_s) for m in tick.minutes] == [
        ("09:00", 3, 3.0), ("09:01", 1, 62.0), ("09:02", 5, 131.0),
    ]

    lines = [r.getMessage() for r in caplog.records if "session_audit_timing" in r.getMessage()]
    assert len(lines) == 1
    passes = int(re.search(r"file_passes=(\d+)", lines[0]).group(1))
    total_bytes = 9 * 8 + 3 * 80
    import math

    assert passes <= 1 + min(2, math.ceil(2 * total_bytes / 104))
    assert "sample_bytes_peak=120" in lines[0]


def test_sample_storage_is_compact():
    import pathlib
    import time
    import tracemalloc
    from array import array

    from src.core.config import AuditSettings
    from src.core.session_anchors import standard_session_anchors
    from src.storage.l0_audit import _Derivation, _StreamAccumulator

    run = _Derivation(
        journal_root=pathlib.Path("/nonexistent"),
        vendor="ls", venue="krx", session="regular", day=DAY,
        anchors=standard_session_anchors(DAY), settings=AuditSettings(),
        manifest_frame_counts=None, progress=None, monotonic=time.monotonic,
    )
    acc = _StreamAccumulator(stream=TICK)
    tracemalloc.start()
    try:
        for idx in range(200_000):
            run.add_sample(acc, "09:00", float(idx))
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert acc.counts == {"09:00": 200_000}
    assert isinstance(acc.samples["09:00"], array)
    assert acc.samples["09:00"].itemsize == 8
    assert peak <= 2 * 8 * 200_000


def test_sequence_duplicate_without_rescan(tmp_path, monkeypatch):
    import src.storage.l0_audit as module
    from src.core.config import AuditSettings

    monkeypatch.setattr(module, "_SEQUENCE_CACHE_SIZE", 1)
    calls: list[str] = []
    real = module._iter_line_batches

    def counting(path, batch_bytes, truncated):
        calls.append(path.name)
        yield from real(path, batch_bytes, truncated)

    monkeypatch.setattr(module, "_iter_line_batches", counting)

    def rec(cum, conn, seq, second):
        return _tick("005930", cum, 0, cum * 10, 10, "090001", _ns(9, 0, second), conn=conn, seq=seq)

    first = [rec(10, "a", 1, 1), rec(11, "b", 1, 2), rec(12, "a", 2, 3)]
    replay_a = rec(10, "a", 1, 4)
    second = [replay_a, rec(13, "b", 2, 5), rec(14, "c", 1, 6), rec(15, "a", 3, 7), rec(14, "c", 1, 8)]
    _write(tmp_path, TICK, first, name="09.jsonl.zst")
    _write(tmp_path, TICK, second, name="10.jsonl.zst")

    audit = _derive(tmp_path, settings=AuditSettings(latency_streams=(TICK,)))
    (ledger,) = audit.symbols
    assert (ledger.frames, ledger.duplicates, ledger.regressions) == (6, 2, 0)
    assert ledger.net_missing == 0
    assert sorted(calls) == ["09.jsonl.zst", "10.jsonl.zst"]


def test_add_sample_evicts_newest_when_retained_minute_overflows(monkeypatch):
    import pathlib
    import time

    from src.core.session_anchors import standard_session_anchors
    from src.storage.l0_audit import _Derivation, _StreamAccumulator

    settings = _sample_settings(monkeypatch, 96)
    run = _Derivation(
        journal_root=pathlib.Path("/nonexistent"),
        vendor="ls", venue="krx", session="regular", day=DAY,
        anchors=standard_session_anchors(DAY), settings=settings,
        manifest_frame_counts=None, progress=None, monotonic=time.monotonic,
    )
    acc = _StreamAccumulator(stream=TICK)
    run.add_sample(acc, "09:00", 1.0)
    run.add_sample(acc, "09:00", 2.0)
    run.add_sample(acc, "09:01", 3.0)
    assert acc.samples.keys() == {"09:00"}
    run.add_sample(acc, "09:00", 4.0)
    assert acc.samples == {}
    assert acc.evicted == {"09:00", "09:01"}
    assert acc.counts == {"09:00": 3, "09:01": 1}


def test_add_sample_evicts_newer_minutes_to_keep_earliest_prefix(monkeypatch):
    import pathlib
    import time

    from src.core.session_anchors import standard_session_anchors
    from src.storage.l0_audit import _Derivation, _StreamAccumulator

    settings = _sample_settings(monkeypatch, 264)
    run = _Derivation(
        journal_root=pathlib.Path("/nonexistent"),
        vendor="ls", venue="krx", session="regular", day=DAY,
        anchors=standard_session_anchors(DAY), settings=settings,
        manifest_frame_counts=None, progress=None, monotonic=time.monotonic,
    )
    acc = _StreamAccumulator(stream=TICK)
    run.add_sample(acc, "09:00", 1.0)
    run.add_sample(acc, "09:01", 2.0)
    run.add_sample(acc, "09:02", 3.0)
    run.add_sample(acc, "09:00", 4.0)
    assert set(acc.samples) == {"09:00", "09:01"}
    assert acc.evicted == {"09:02"}
    assert acc.counts == {"09:00": 2, "09:01": 1, "09:02": 1}
    assert acc.stored_bytes == 24 + 2 * 80


def test_latency_sample_maps_oversized_recv_to_none():
    from src.storage.l0_audit import _Envelope, _latency_sample

    record = _Envelope(raw="{}", recv_ns=10**30, conn_id="", conn_seq=None, exchange_event_time="090001", symbol="")
    assert _latency_sample(TICK, "ls", DAY, record) is None


def test_quote_stream_extra_pass_uses_hotime_and_skips_undecodable(tmp_path, monkeypatch):
    import zstandard as zstd

    from src.core.config import AuditSettings

    _write(tmp_path, QUOTE, [
        _quote("005930", "090100", _ns(9, 1, 10)),
        _quote("005930", "090100", _ns(9, 1, 20)),
        _quote("005930", "090200", _ns(9, 2, 10)),
        _quote("005930", "", _ns(9, 2, 20)),
    ])
    part = tmp_path / "ls" / "krx" / "regular" / QUOTE / f"dt={DAY.isoformat()}"
    with (part / "09.jsonl.zst").open("ab") as handle:
        handle.write(zstd.ZstdCompressor(level=3).compress(b"bad line\n"))

    settings = _sample_settings(monkeypatch, 96, latency_streams=(QUOTE,))
    tight = _derive(tmp_path, settings=settings, wall_ns=lambda: 42)
    plain = _derive(tmp_path, settings=AuditSettings(latency_streams=(QUOTE,)), wall_ns=lambda: 42)
    assert tight.latency == plain.latency
    quote = next(entry for entry in tight.latency if entry.stream == QUOTE)
    assert [(m.minute_kst, m.count, m.p50_s) for m in quote.minutes] == [
        ("09:01", 2, 10.0), ("09:02", 1, 10.0),
    ]
    assert quote.event_time_missing == 1


def test_seen_sequences_matches_set_oracle():
    from src.storage.l0_audit import _SeenSequences

    values = [(idx * 37 + 11) % 51 for idx in range(200)] + [5, 6, 7, 4, 3, 20, 19, 18, 20, 0, 50, 49]
    oracle: set[int] = set()
    seen = _SeenSequences()
    for value in values:
        assert (value in seen) == (value in oracle)
        seen.add(value)
        oracle.add(value)
    for value in range(-1, 53):
        assert (value in seen) == (value in oracle)
    assert ("x" in seen) is False
    assert (True in seen) is False
    runs, prev = 0, None
    for value in sorted(oracle):
        if prev is None or value != prev + 1:
            runs += 1
        prev = value
    assert len(seen) == runs


def test_timing_line_emitted_on_ok(tmp_path, caplog):
    import logging

    from src.core.config import AuditSettings
    from src.storage.l0_audit import AuditStatus

    _write(tmp_path, TICK, [_tick("005930", 1, 0, 100, 10, "090100", _ns(9, 0, 10))])
    with caplog.at_level(logging.INFO, logger="src.storage.l0_audit"):
        audit = _derive(tmp_path, settings=AuditSettings(latency_streams=(TICK,)), monotonic=lambda: 7.0)
    lines = [r for r in caplog.records if "session_audit_timing" in r.getMessage()]
    assert len(lines) == 1
    assert lines[0].levelno == logging.INFO
    assert audit.status is AuditStatus.OK
    assert "elapsed_s=" in lines[0].getMessage()
    assert "file_passes=" in lines[0].getMessage()


def test_timing_line_emitted_on_time_budget(tmp_path, caplog):
    import logging

    from src.core.config import AuditSettings
    from src.storage.l0_audit import AuditStatus

    _write(tmp_path, TICK, [_tick("005930", 1, 0, 100, 10, "090100", _ns(9, 0, 10))], name="09.jsonl.zst")
    _write(tmp_path, TICK, [_tick("005930", 2, 0, 110, 10, "090101", _ns(9, 0, 20))], name="10.jsonl.zst")
    ticks = iter([0.0, 0.0, 0.0, 0.0, 5.0, 5.0])
    with caplog.at_level(logging.INFO, logger="src.storage.l0_audit"):
        audit = _derive(
            tmp_path,
            settings=AuditSettings(latency_streams=(TICK,), time_budget_s=0.001),
            monotonic=lambda: next(ticks),
        )
    assert audit.status is AuditStatus.UNAVAILABLE
    assert audit.reason == "time_budget"
    lines = [r for r in caplog.records if "session_audit_timing" in r.getMessage()]
    assert len(lines) == 1
    assert lines[0].levelno == logging.WARNING
    assert "elapsed_s=" in lines[0].getMessage()
    assert "file_passes=" in lines[0].getMessage()


def test_budget_guard_raises_mid_pass_without_partials(tmp_path):
    from src.core.config import AuditSettings
    from src.storage.l0_audit import AuditStatus

    _write(tmp_path, TICK, [_tick("005930", 1, 0, 100, 10, "090100", _ns(9, 0, 10))], name="09.jsonl.zst")
    _write(tmp_path, TICK, [_tick("005930", 2, 0, 110, 10, "090101", _ns(9, 0, 20))], name="10.jsonl.zst")
    ticks = iter([0.0, 0.0, 0.0, 0.0, 5.0, 5.0])
    audit = _derive(
        tmp_path,
        settings=AuditSettings(latency_streams=(TICK,), time_budget_s=0.001),
        monotonic=lambda: next(ticks),
    )
    assert audit.status is AuditStatus.UNAVAILABLE
    assert audit.reason == "time_budget"
    assert audit.symbols == ()


def test_linear_scale_decode_bound(tmp_path, monkeypatch):
    import datetime as dt

    import src.storage.l0_audit as module
    from src.core.config import AuditSettings

    decodes = []
    real = module._decode_envelope

    def counting(line):
        decodes.append(1)
        return real(line)

    monkeypatch.setattr(module, "_decode_envelope", counting)
    n_files, per_file, n_minutes = 4, 2000, 390
    for shard in range(n_files):
        records = []
        for idx in range(per_file):
            pos = idx % n_minutes
            recv = int(dt.datetime(DAY.year, DAY.month, DAY.day, 9 + pos // 60, pos % 60, 10, tzinfo=KST).timestamp() * 1e9)
            cum = shard * per_file + idx + 1
            records.append(_tick(f"{idx % 50:06d}", cum, 0, cum * 10, 10, "090001", recv, conn=f"c{shard}", seq=idx + 1))
        _write(tmp_path, TICK, records, name=f"09.s{shard}.jsonl.zst")

    _derive(tmp_path, settings=AuditSettings(latency_streams=(TICK,)))

    assert len(decodes) <= 2 * n_files * per_file


def test_finalization_keeps_sample_memory_compact():
    import tracemalloc
    from array import array

    from src.storage.l0_audit import _finalize_minute

    tracemalloc.start()
    try:
        samples = array("d", (float(i) for i in range(200_000, 0, -1)))
        minute = _finalize_minute("09:00", samples)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert (minute.count, minute.p50_s, minute.min_s, minute.max_s) == (200_000, 100_000.0, 1.0, 200_000.0)
    assert peak <= 2 * 8 * 200_000


def test_sequence_duplicate_includes_earlier_undecodable_tick(tmp_path):
    from src.core.config import AuditSettings

    _write(tmp_path, TICK, [
        _env("{}", _ns(9, 0, 1), seq=1),
        _tick("005930", 10, 0, 100, 10, "090001", _ns(9, 0, 2), seq=1),
        _tick("005930", 11, 0, 110, 10, "090001", _ns(9, 0, 3), seq=2),
    ])
    audit = _derive(tmp_path, settings=AuditSettings(latency_streams=(TICK,)))
    ledger, = audit.symbols
    assert (ledger.frames, ledger.duplicates, ledger.trades_observed) == (1, 1, 1)


def test_latency_recollection_contains_disappearing_file(tmp_path, monkeypatch):
    part = _write(tmp_path, TICK, [
        _tick("005930", i, 0, i * 10, 10, "090001", _ns(9, i, 3), seq=i)
        for i in range(1, 4)
    ])

    def disappear():
        (part / "09.jsonl.zst").unlink(missing_ok=True)

    audit = _derive(tmp_path, settings=_sample_settings(monkeypatch, 88, latency_streams=(TICK,)), progress=disappear)
    assert audit.status.value == "UNAVAILABLE"
    assert audit.reason == "io_error"
    assert audit.symbols == ()


@pytest.mark.parametrize(("counts", "budget", "expected_passes", "deferred"), [
    ([3] * 8, 160, 8, 7),
    ([10, 2, 1], 112, 3, 2),
    ([2, 3, 1], 208, 2, 1),
    ([1, 1], 176, 1, 0),
    ([1], 8, 2, 1),
])
def test_whole_minute_budget_exceptions_and_fragmentation(
    tmp_path, monkeypatch, caplog, counts, budget, expected_passes, deferred,
):
    import logging
    import math
    import re

    import src.storage.l0_audit as module
    from src.core.config import AuditSettings

    records = []
    for minute, count in enumerate(counts):
        for idx in range(count):
            seq = len(records) + 1
            records.append(_tick(
                "005930", seq, 0, seq * 10, 10, f"09{minute:02d}01",
                _ns(9, minute, idx + 2) + seq, seq=seq,
            ))
    _write(tmp_path, TICK, records)
    before = {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    plain = _derive(tmp_path, settings=AuditSettings(latency_streams=(TICK,)), wall_ns=lambda: 42)
    calls = []
    reader = module._iter_line_batches

    def counting(path, batch_bytes, truncated):
        calls.append(path)
        yield from reader(path, batch_bytes, truncated)

    monkeypatch.setattr(module, "_iter_line_batches", counting)
    caplog.clear()
    with caplog.at_level(logging.INFO, logger=module.logger.name):
        tight = _derive(
            tmp_path, settings=_sample_settings(monkeypatch, budget, latency_streams=(TICK,)),
            wall_ns=lambda: 42,
        )
    assert module._audit_to_jsonable(tight) == module._audit_to_jsonable(plain)
    assert before == {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    timing, = [r.getMessage() for r in caplog.records if "session_audit_timing" in r.getMessage()]
    passes = int(re.search(r"file_passes=(\d+)", timing).group(1))
    peak = int(re.search(r"sample_bytes_peak=(\d+)", timing).group(1))
    sizes = [8 * count + 80 for count in counts]
    assert passes == len(calls) == expected_passes
    assert passes <= 1 + min(deferred, math.ceil(2 * sum(sizes) / budget))
    assert peak <= max(budget, max(sizes))
    if max(sizes) > budget:
        assert peak == max(sizes)


def test_sample_accounting_resets_between_streams(tmp_path, monkeypatch, caplog):
    import logging

    _write(tmp_path, TICK, [_tick("005930", 1, 0, 10, 10, "090001", _ns(9, 0, 2))])
    _write(tmp_path, QUOTE, [_quote("005930", "090001", _ns(9, 0, 2))])
    with caplog.at_level(logging.INFO, logger="src.storage.l0_audit"):
        audit = _derive(tmp_path, settings=_sample_settings(monkeypatch, 88))
    assert audit.status.value == "OK"
    assert [len(stream.minutes) for stream in audit.latency] == [1, 1]
    timing, = [r.getMessage() for r in caplog.records if "session_audit_timing" in r.getMessage()]
    assert "file_passes=2" in timing
    assert "sample_bytes_peak=88" in timing
