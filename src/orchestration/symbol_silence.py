"""Per-symbol silence watch (observe-only): cross-source gaps and activity-scaled quiet suspects from feed health files."""

from __future__ import annotations

import datetime as dt
import json
import logging
import pathlib
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal
from zoneinfo import ZoneInfo

from src.core.config import (
    SYMBOL_SILENCE_CROSS_GAP_S,
    SYMBOL_SILENCE_FEED_STALE_S,
    SYMBOL_SILENCE_OPEN_GRACE_S,
    SYMBOL_SILENCE_REPORT_S,
    SYMBOL_SILENCE_SINGLE_K,
    SYMBOL_SILENCE_SINGLE_MIN_S,
    SYMBOL_SILENCE_SUSPECT_CAP,
)
from src.core.paths import DataPaths
from src.core.session_anchors import SessionAnchors
from src.orchestration.feed_failover import _MAX_HEALTH_BYTES
from src.orchestration.regular_kis import prior_ls_frame_loads
from src.realtime.feed_health import SCHEMA as FEED_HEALTH_SCHEMA
from src.realtime.feed_health import write_feed_health
from src.realtime.market_state import MarketStateView, read_market_state

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")
_SUSPECT_SCHEMA = "quiet-suspects.v1"
_HEAD_SYMBOLS = 5
_KRX = "krx"
_QUOTE = "quote"
_MARKET_JANGUBUN: Mapping[str, str] = {"KOSPI": "1", "KOSDAQ": "2"}
_FAIL_WARN_EVERY_S = 60.0


@dataclass(frozen=True)
class SymbolFreshness:
    feed_id: str
    updated_wall: float
    connected: bool
    last: Mapping[tuple[str, str, str], float]


@dataclass(frozen=True)
class SilenceFinding:
    status: Literal["GAP", "SUSPECT"]
    feed_id: str
    kind: str
    venue: str
    symbol: str
    silent_s: float
    last_frame_wall: float | None


def read_symbol_freshness(path: pathlib.Path, session_date: dt.date) -> SymbolFreshness | None:
    """Parse the per-symbol last-frame map of one feed-health file; None when unusable or for another session."""
    try:
        if path.stat().st_size > _MAX_HEALTH_BYTES:
            return None
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(raw, dict) or raw.get("schema") != FEED_HEALTH_SCHEMA:
        return None
    if raw.get("session_date") != session_date.isoformat():
        return None
    try:
        symbol_last = raw["symbol_last_frame_wall"]
        last: dict[tuple[str, str, str], float] = {}
        for kind in ("trade", _QUOTE):
            for key, value in dict(symbol_last[kind]).items():
                venue, symbol = str(key).split("|", 1)
                last[(kind, venue, symbol)] = float(value)
        return SymbolFreshness(
            feed_id=str(raw["feed_id"]),
            updated_wall=float(raw["updated_wall"]),
            connected=bool(raw["connected"]),
            last=last,
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        return None


def evaluate_symbol_silence(
    feeds: Sequence[SymbolFreshness],
    *,
    now_wall: float,
    expected_gap_s: Mapping[str, float],
    halted: Callable[[str], bool],
) -> tuple[tuple[SilenceFinding, ...], int]:
    """Return (findings, halted_count) for connected, fresh feeds; halted keys are counted and never reported."""
    usable = tuple(
        feed
        for feed in feeds
        if feed.connected and now_wall - feed.updated_wall <= SYMBOL_SILENCE_FEED_STALE_S
    )
    by_key: dict[tuple[str, str, str], list[tuple[SymbolFreshness, float]]] = {}
    for feed in usable:
        for key, last_t in feed.last.items():
            by_key.setdefault(key, []).append((feed, last_t))

    findings: list[SilenceFinding] = []
    halted_count = 0
    for key in sorted(by_key):
        kind, venue, symbol = key
        entries = by_key[key]
        candidates: list[SilenceFinding] = []
        if len(entries) >= 2:
            for feed, t_f in entries:
                t_p = max(t for other, t in entries if other is not feed)
                gap_s = SYMBOL_SILENCE_CROSS_GAP_S[kind]
                # f's snapshot must postdate the peer frame by the full threshold, so delivery lag below it is no GAP.
                if t_p - t_f > gap_s and feed.updated_wall >= t_p + gap_s:
                    candidates.append(
                        SilenceFinding("GAP", feed.feed_id, kind, venue, symbol, t_p - t_f, t_f)
                    )
        if kind == _QUOTE and venue == _KRX and symbol in expected_gap_s:
            t_star = max(t for _, t in entries)
            threshold = max(SYMBOL_SILENCE_SINGLE_MIN_S, SYMBOL_SILENCE_SINGLE_K * expected_gap_s[symbol])
            if now_wall - t_star > threshold:
                candidates.append(SilenceFinding("SUSPECT", "*", kind, venue, symbol, now_wall - t_star, t_star))
        if not candidates:
            continue
        if halted(symbol):
            halted_count += 1
            continue
        findings.extend(candidates)

    findings.sort(key=lambda f: (f.status, f.feed_id, f.kind, f.venue, -f.silent_s, f.symbol))
    return tuple(findings), halted_count


def expected_gaps(loads: Mapping[str, int], continuous_s: float) -> dict[str, float]:
    """Mean seconds between frames per symbol over the continuous session; symbols without frames are omitted."""
    return {symbol: continuous_s / count for symbol, count in loads.items() if count > 0}


class SymbolSilenceWatch:
    """Daemon-side per-symbol silence observer; logs and writes the quiet-suspects file, never affects feeds."""

    def __init__(self, paths: DataPaths, *, wall: Callable[[], float] = time.time) -> None:
        self._paths = paths
        self._wall = wall
        self._baseline_day: dt.date | None = None
        self._expected: dict[str, float] = {}
        self._market_map: dict[str, str] = {}
        self._last_log: dict[tuple[str, str, str], float] = {}
        self._last_fail_warn: float | None = None

    def tick(self, now: dt.datetime, today: dt.date, anchors: SessionAnchors) -> None:
        try:
            self._tick(now, today, anchors)
        except Exception as exc:  # noqa: BLE001 - an observe-only watch must never break the daemon loop
            self._warn_tick_fail(exc)

    def _tick(self, now: dt.datetime, today: dt.date, anchors: SessionAnchors) -> None:
        regular_open = dt.datetime.combine(today, anchors.regular_open, tzinfo=_KST)
        close_dt = dt.datetime.combine(today, anchors.closing_auction_start, tzinfo=_KST)
        if not regular_open + dt.timedelta(seconds=SYMBOL_SILENCE_OPEN_GRACE_S) <= now < close_dt:
            return
        if self._baseline_day != today:
            self._load_day(today, (close_dt - regular_open).total_seconds())
        wall_now = self._wall()
        feeds = self._read_feeds(today)
        findings, halted_count = evaluate_symbol_silence(
            feeds,
            now_wall=wall_now,
            expected_gap_s=self._expected,
            halted=self._halted_predicate(today, wall_now),
        )
        self._report(findings, halted_count, wall_now)
        self._write_suspects(findings, today, wall_now)

    def _load_day(self, today: dt.date, continuous_s: float) -> None:
        baseline = prior_ls_frame_loads(self._paths, today)
        self._expected = expected_gaps(baseline[1], continuous_s) if baseline is not None else {}
        self._market_map = _read_market_map(self._paths.market_map)
        self._baseline_day = today

    def _read_feeds(self, today: dt.date) -> list[SymbolFreshness]:
        root = self._paths.feed_health_path("x").parent
        if not root.is_dir():
            return []
        feeds: list[SymbolFreshness] = []
        for path in sorted(root.glob("*.json")):
            freshness = read_symbol_freshness(path, today)
            if freshness is not None:
                feeds.append(freshness)
        return feeds

    def _halted_predicate(self, today: dt.date, wall_now: float) -> Callable[[str], bool]:
        view: MarketStateView = read_market_state(self._paths.market_state_path(today))
        vi_halted = view.halted_symbols(wall_now)
        market_halted = view.market_halted(wall_now)

        def halted(symbol: str) -> bool:
            if symbol in vi_halted:
                return True
            if not market_halted:
                return False
            market = self._market_map.get(symbol)
            if market is None:
                return True
            return _MARKET_JANGUBUN.get(market) in market_halted

        return halted

    def _report(self, findings: tuple[SilenceFinding, ...], halted_count: int, wall_now: float) -> None:
        groups: dict[tuple[str, str, str], list[SilenceFinding]] = {}
        for finding in findings:
            groups.setdefault((finding.status, finding.feed_id, finding.kind), []).append(finding)
        for key in sorted(groups):
            last = self._last_log.get(key)
            if last is not None and wall_now - last < SYMBOL_SILENCE_REPORT_S:
                continue
            self._last_log[key] = wall_now
            items = groups[key]
            max_silent = max(item.silent_s for item in items)
            head = ",".join(item.symbol for item in items[:_HEAD_SYMBOLS])
            status, feed_id, kind = key
            if status == "GAP":
                venues = ",".join(sorted({item.venue for item in items}))
                logger.warning(
                    "[DATA] stage=symbol_silence status=GAP feed=%s peer_kind=%s venue=%s symbols=%d max_silent_s=%.1f head=%s halted=%d",
                    feed_id,
                    kind,
                    venues,
                    len(items),
                    max_silent,
                    head,
                    halted_count,
                )
            else:
                logger.info(
                    "[DATA] stage=symbol_silence status=SUSPECT kind=%s symbols=%d max_silent_s=%.1f head=%s halted=%d",
                    kind,
                    len(items),
                    max_silent,
                    head,
                    halted_count,
                )

    def _write_suspects(self, findings: tuple[SilenceFinding, ...], today: dt.date, wall_now: float) -> None:
        suspects = sorted((f for f in findings if f.status == "SUSPECT"), key=lambda f: -f.silent_s)
        payload = {
            "schema": _SUSPECT_SCHEMA,
            "session_date": today.isoformat(),
            "updated_wall": wall_now,
            "suspects": [
                {
                    "symbol": f.symbol,
                    "venue": f.venue,
                    "kind": f.kind,
                    "last_frame_wall": f.last_frame_wall,
                    "silent_s": round(f.silent_s, 3),
                }
                for f in suspects[:SYMBOL_SILENCE_SUSPECT_CAP]
            ],
        }
        write_feed_health(self._paths.quiet_suspects_path(), payload)

    def _warn_tick_fail(self, exc: Exception) -> None:
        now = self._wall()
        if self._last_fail_warn is not None and now - self._last_fail_warn < _FAIL_WARN_EVERY_S:
            return
        self._last_fail_warn = now
        logger.warning("[SYS] stage=symbol_silence status=TICK_FAIL reason=%s", type(exc).__name__)


def _read_market_map(path: pathlib.Path) -> dict[str, str]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(raw, dict):
        return {}
    return {str(symbol): str(market) for symbol, market in raw.items()}
