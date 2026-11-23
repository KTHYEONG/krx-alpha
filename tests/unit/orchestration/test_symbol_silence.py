from __future__ import annotations

import datetime as dt
import json
import logging
from zoneinfo import ZoneInfo

from src.core.config import SYMBOL_SILENCE_OPEN_GRACE_S
from src.core.paths import DataPaths
from src.core.session_anchors import standard_session_anchors
from src.orchestration import symbol_silence as ss
from src.orchestration.symbol_silence import (
    SymbolFreshness,
    SymbolSilenceWatch,
    evaluate_symbol_silence,
    expected_gaps,
    read_symbol_freshness,
)
from src.realtime.feed_health import write_feed_health

KST = ZoneInfo("Asia/Seoul")
DAY = dt.date(2026, 11, 23)
DAY2 = dt.date(2026, 11, 24)
ANCHORS = standard_session_anchors(DAY)
NOW_BASE = dt.datetime(2026, 11, 23, 10, 0, tzinfo=KST).timestamp()
NO_HALT = lambda symbol: False  # noqa: E731


def _feed(
    feed_id: str, updated: float, last: dict[tuple[str, str, str], float], connected: bool = True
) -> SymbolFreshness:
    return SymbolFreshness(feed_id=feed_id, updated_wall=updated, connected=connected, last=last)


def _eval(feeds, now: float, expected: dict[str, float] | None = None, halted=NO_HALT):
    return evaluate_symbol_silence(feeds, now_wall=now, expected_gap_s=expected or {}, halted=halted)


def test_gap_when_peer_fresh() -> None:
    key = ("quote", "krx", "005930")
    a = _feed("a", 150.0, {key: 100.0})
    b = _feed("b", 150.0, {key: 130.0})
    findings, halted = _eval([a, b], now=150.0)
    assert halted == 0
    assert [(f.status, f.feed_id, f.silent_s, f.last_frame_wall) for f in findings] == [("GAP", "a", 30.0, 100.0)]


def test_no_gap_when_snapshot_older() -> None:
    key = ("quote", "krx", "005930")
    a = _feed("a", 125.0, {key: 100.0})
    b = _feed("b", 131.0, {key: 130.0})
    assert _eval([a, b], now=131.0) == ((), 0)


def test_no_gap_while_lag_is_below_threshold() -> None:
    # A's snapshot postdates the peer frame by 19 s < 20 s: the frame may still be in flight (open lag), not lost.
    key = ("quote", "krx", "005930")
    a = _feed("a", 149.0, {key: 100.0})
    b = _feed("b", 149.0, {key: 130.0})
    assert _eval([a, b], now=149.0) == ((), 0)


def test_no_gap_under_threshold() -> None:
    key = ("quote", "krx", "005930")
    a = _feed("a", 131.0, {key: 120.0})
    b = _feed("b", 131.0, {key: 130.0})
    assert _eval([a, b], now=131.0) == ((), 0)


def test_suspect_scaled_by_activity() -> None:
    busy = ("quote", "krx", "000001")
    quiet = ("quote", "krx", "000002")
    now = 1000.0
    feed = _feed("a", now, {busy: now - 10.0, quiet: now - 300.0})
    findings, _ = _eval([feed], now=now, expected={"000001": 2.0, "000002": 60.0})
    assert findings == ()
    feed = _feed("a", now, {busy: now - 100.0, quiet: now - 300.0})
    findings, _ = _eval([feed], now=now, expected={"000001": 2.0, "000002": 60.0})
    assert [(f.status, f.symbol, f.silent_s) for f in findings] == [("SUSPECT", "000001", 100.0)]


def test_no_baseline_no_suspect() -> None:
    feed = _feed("a", 1000.0, {("quote", "krx", "000001"): 100.0})
    assert _eval([feed], now=1000.0, expected={}) == ((), 0)


def test_halted_symbol_excluded_and_counted() -> None:
    key = ("quote", "krx", "005930")
    a = _feed("a", 150.0, {key: 100.0})
    b = _feed("b", 150.0, {key: 130.0})
    findings, halted = _eval([a, b], now=150.0, halted=lambda symbol: symbol == "005930")
    assert findings == ()
    assert halted == 1


def test_stale_or_disconnected_feed_skipped() -> None:
    key = ("quote", "krx", "005930")
    fresh = _feed("b", 131.0, {key: 130.0})
    disconnected = _feed("a", 131.0, {key: 100.0}, connected=False)
    stale = _feed("c", 90.0, {key: 100.0})
    assert _eval([disconnected, fresh], now=131.0) == ((), 0)
    assert _eval([stale, fresh], now=131.0) == ((), 0)


def test_expected_gaps_omit_empty_symbols() -> None:
    assert expected_gaps({"005930": 4, "000660": 0}, 100.0) == {"005930": 25.0}


def _health(
    feed_id: str, *, updated: float, quotes: dict[str, float], session: dt.date = DAY, schema: str = "feed-health.v1"
) -> dict:
    return {
        "schema": schema,
        "feed_id": feed_id,
        "session_date": session.isoformat(),
        "updated_wall": updated,
        "connected": True,
        "symbol_last_frame_wall": {"trade": {}, "quote": {f"krx|{s}": t for s, t in quotes.items()}},
    }


def test_read_failures_return_none(tmp_path) -> None:
    path = tmp_path / "f.json"
    path.write_text(json.dumps(_health("a", updated=1.0, quotes={"005930": 1.0}, session=DAY2)), encoding="utf-8")
    assert read_symbol_freshness(path, DAY) is None
    path.write_text(json.dumps(_health("a", updated=1.0, quotes={"005930": 1.0}, schema="other")), encoding="utf-8")
    assert read_symbol_freshness(path, DAY) is None
    missing = _health("a", updated=1.0, quotes={})
    del missing["symbol_last_frame_wall"]
    path.write_text(json.dumps(missing), encoding="utf-8")
    assert read_symbol_freshness(path, DAY) is None
    path.write_text("x" * (ss._MAX_HEALTH_BYTES + 1), encoding="utf-8")
    assert read_symbol_freshness(path, DAY) is None
    assert read_symbol_freshness(tmp_path / "absent.json", DAY) is None


def test_watch_window_and_file(tmp_path, caplog) -> None:
    paths = DataPaths(tmp_path / "data")
    clock = {"wall": 0.0}
    watch = SymbolSilenceWatch(paths, wall=lambda: clock["wall"])
    open_s = dt.datetime.combine(DAY, ANCHORS.regular_open, tzinfo=KST).timestamp()
    end_s = dt.datetime.combine(DAY, ANCHORS.closing_auction_start, tzinfo=KST).timestamp()
    feed_dir = paths.feed_health_path("x").parent
    feed_dir.mkdir(parents=True)
    write_feed_health(feed_dir / "a.json", _health("a", updated=open_s + 200, quotes={"005930": open_s + 100}))
    write_feed_health(feed_dir / "b.json", _health("b", updated=open_s + 200, quotes={"005930": open_s + 150}))
    write_feed_health(feed_dir / "c.json", _health("c", updated=open_s + 200, quotes={}))

    early = dt.datetime.fromtimestamp(open_s + SYMBOL_SILENCE_OPEN_GRACE_S - 1, tz=KST)
    clock["wall"] = open_s + 200
    watch.tick(early, DAY, ANCHORS)
    assert not paths.quiet_suspects_path().exists()

    inside = dt.datetime.fromtimestamp(open_s + 200, tz=KST)
    with caplog.at_level(logging.WARNING, logger="src.orchestration.symbol_silence"):
        watch.tick(inside, DAY, ANCHORS)
        watch.tick(inside, DAY, ANCHORS)
    gap_lines = [r for r in caplog.records if "status=GAP" in r.getMessage()]
    assert len(gap_lines) == 1
    payload = json.loads(paths.quiet_suspects_path().read_text(encoding="utf-8"))
    assert payload["schema"] == "quiet-suspects.v1"
    assert payload["suspects"] == []

    after = dt.datetime.fromtimestamp(end_s + 1, tz=KST)
    paths.quiet_suspects_path().unlink()
    watch.tick(after, DAY, ANCHORS)
    assert not paths.quiet_suspects_path().exists()


def test_tick_failure_never_raises(tmp_path, caplog) -> None:
    paths = DataPaths(tmp_path / "data")
    paths.feed_health_path("x").parent.mkdir(parents=True)
    paths.feed_health_path("x").parent.joinpath("bad.json").write_text("{", encoding="utf-8")
    watch = SymbolSilenceWatch(paths, wall=lambda: 1.0)

    def boom(*args, **kwargs):
        raise RuntimeError("boom")

    watch._read_feeds = boom  # type: ignore[method-assign]
    inside = dt.datetime.combine(DAY, ANCHORS.regular_open, tzinfo=KST) + dt.timedelta(
        seconds=SYMBOL_SILENCE_OPEN_GRACE_S + 5
    )
    with caplog.at_level(logging.WARNING, logger="src.orchestration.symbol_silence"):
        watch.tick(inside, DAY, ANCHORS)
        watch.tick(inside, DAY, ANCHORS)
    fails = [r for r in caplog.records if "status=TICK_FAIL reason=RuntimeError" in r.getMessage()]
    assert len(fails) == 1  # throttled to once a minute


def _market_line(stream: str, symbol: str, body: dict, wall_s: float) -> str:
    return json.dumps(
        {
            "recv_wall_ns": int(wall_s * 1_000_000_000),
            "stream": stream,
            "symbol": symbol,
            "raw": json.dumps({"body": body}),
        }
    )


def test_watch_labels_halts_and_reports_suspects(tmp_path, monkeypatch, caplog) -> None:
    paths = DataPaths(tmp_path / "data")
    open_s = dt.datetime.combine(DAY, ANCHORS.regular_open, tzinfo=KST).timestamp()
    now_s = open_s + 3600
    # Expected gap 1 s for every name: 200 s of book silence is far beyond max(90, 12 x 1).
    monkeypatch.setattr(
        ss,
        "prior_ls_frame_loads",
        lambda _paths, _day: (
            DAY - dt.timedelta(days=1),
            dict.fromkeys(("000001", "000002", "000003", "000004"), 22200),
        ),
    )
    paths.market_map.parent.mkdir(parents=True, exist_ok=True)
    paths.market_map.write_text(json.dumps({"000002": "KOSDAQ", "000003": "KOSPI"}), encoding="utf-8")
    market = paths.market_state_path(DAY)
    market.parent.mkdir(parents=True)
    market.write_text(
        "\n".join(
            (
                _market_line(
                    "LS_VI", "000000", {"vi_gubun": "1", "ref_shcode": "000001", "time": "100000"}, now_s - 100
                ),
                _market_line("LS_JIF", "0", {"jangubun": "2", "jstatus": "61"}, now_s - 100),
            )
        )
        + "\n",
        encoding="utf-8",
    )
    feed_dir = paths.feed_health_path("x").parent
    feed_dir.mkdir(parents=True)
    quotes = dict.fromkeys(("000001", "000002", "000003", "000004"), now_s - 200)
    write_feed_health(feed_dir / "a.json", _health("a", updated=now_s, quotes=quotes))
    watch = SymbolSilenceWatch(paths, wall=lambda: now_s)

    with caplog.at_level(logging.INFO, logger="src.orchestration.symbol_silence"):
        watch.tick(dt.datetime.fromtimestamp(now_s, tz=KST), DAY, ANCHORS)

    # 000001 is under VI, 000002 is KOSDAQ under CB, 000004 has no market entry while a CB is active: all halted.
    payload = json.loads(paths.quiet_suspects_path().read_text(encoding="utf-8"))
    assert [item["symbol"] for item in payload["suspects"]] == ["000003"]
    lines = [r.getMessage() for r in caplog.records if "status=SUSPECT" in r.getMessage()]
    assert lines == [
        "[DATA] stage=symbol_silence status=SUSPECT kind=quote symbols=1 max_silent_s=200.0 head=000003 halted=3"
    ]


def test_watch_without_feed_dir_or_market_map_writes_empty_suspects(tmp_path) -> None:
    paths = DataPaths(tmp_path / "data")
    paths.market_map.parent.mkdir(parents=True, exist_ok=True)
    paths.market_map.write_text("[]", encoding="utf-8")
    open_s = dt.datetime.combine(DAY, ANCHORS.regular_open, tzinfo=KST).timestamp()
    watch = SymbolSilenceWatch(paths, wall=lambda: open_s + 600)
    watch.tick(dt.datetime.fromtimestamp(open_s + 600, tz=KST), DAY, ANCHORS)
    payload = json.loads(paths.quiet_suspects_path().read_text(encoding="utf-8"))
    assert payload["suspects"] == []
