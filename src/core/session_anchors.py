"""KRX session boundary anchors for one KST business date."""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import pathlib
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from src.core.errors import KrxAlphaError

logger = logging.getLogger(__name__)

STANDARD_REGULAR_OPEN: dt.time = dt.time(9, 0)
STANDARD_CLOSING_AUCTION: dt.time = dt.time(15, 20)
STANDARD_REGULAR_CLOSE: dt.time = dt.time(15, 30)
STANDARD_AFTER_MARKET_END: dt.time = dt.time(20, 0)
MAX_SESSION_SHIFT: dt.timedelta = dt.timedelta(hours=3)

_BASE_DATE: dt.date = dt.date(2000, 1, 1)


class AnchorSource(StrEnum):
    VENDOR = "vendor"
    DEFAULT = "default"


class SessionAnchorError(KrxAlphaError):
    """Session anchor invariant violation fail-closed signal."""


def _shift_between(standard: dt.time, actual: dt.time) -> dt.timedelta:
    return dt.datetime.combine(_BASE_DATE, actual) - dt.datetime.combine(_BASE_DATE, standard)


def _shift_time(standard: dt.time, shift: dt.timedelta) -> dt.time:
    moment = dt.datetime.combine(_BASE_DATE, standard) + shift
    if moment.date() != _BASE_DATE:
        raise SessionAnchorError(f"shifted time leaves calendar day: {standard!r} shift={shift}")
    return moment.time()


@dataclass(frozen=True)
class SessionAnchors:
    """KRX session boundary times for one KST business date.

    KRX shifts the whole trading day on announced special days (CSAT day,
    first trading day of the year). Every schedule-dependent behavior (phase
    labels, silence watchdogs, gap reconciliation, snapshot timing, aftermarket
    start) is derived from these anchors instead of fixed clock literals, so a
    shifted day behaves exactly like a normal day moved in time.

    Pre-open boundaries follow ``open_shift`` and post-close boundaries follow
    ``close_shift``. Those are the only two degrees of freedom KRX has used for
    special days.
    """

    date: dt.date
    regular_open: dt.time
    closing_auction_start: dt.time
    regular_close: dt.time
    after_market_end: dt.time
    source: AnchorSource

    def __post_init__(self) -> None:
        if not (self.regular_open < self.closing_auction_start < self.regular_close < self.after_market_end):
            raise SessionAnchorError(f"anchors out of order for {self.date}: {self.regular_open!r} {self.closing_auction_start!r} {self.regular_close!r} {self.after_market_end!r}")
        if abs(self.open_shift) > MAX_SESSION_SHIFT or abs(self.close_shift) > MAX_SESSION_SHIFT:
            raise SessionAnchorError(f"session shift beyond cap for {self.date}: open={self.open_shift} close={self.close_shift}")

    @property
    def open_shift(self) -> dt.timedelta:
        return _shift_between(STANDARD_REGULAR_OPEN, self.regular_open)

    @property
    def close_shift(self) -> dt.timedelta:
        return _shift_between(STANDARD_REGULAR_CLOSE, self.regular_close)

    def shift_pre_open(self, standard: dt.time) -> dt.time:
        return _shift_time(standard, self.open_shift)

    def shift_post_close(self, standard: dt.time) -> dt.time:
        return _shift_time(standard, self.close_shift)


def standard_session_anchors(day: dt.date) -> SessionAnchors:
    return SessionAnchors(
        date=day,
        regular_open=STANDARD_REGULAR_OPEN,
        closing_auction_start=STANDARD_CLOSING_AUCTION,
        regular_close=STANDARD_REGULAR_CLOSE,
        after_market_end=STANDARD_AFTER_MARKET_END,
        source=AnchorSource.DEFAULT,
    )


def _anchor_file(directory: pathlib.Path, day: dt.date) -> pathlib.Path:
    return pathlib.Path(directory) / f"dt={day.isoformat()}.json"


def save_session_anchors(directory: pathlib.Path, anchors: SessionAnchors) -> None:
    target = _anchor_file(directory, anchors.date)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "date": anchors.date.isoformat(),
        "regular_open": anchors.regular_open.strftime("%H:%M:%S"),
        "closing_auction_start": anchors.closing_auction_start.strftime("%H:%M:%S"),
        "regular_close": anchors.regular_close.strftime("%H:%M:%S"),
        "after_market_end": anchors.after_market_end.strftime("%H:%M:%S"),
        "source": anchors.source.value,
    }
    tmp = target.parent / f".{target.name}.tmp"
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, target)


def load_session_anchors(directory: pathlib.Path, day: dt.date) -> SessionAnchors | None:
    try:
        raw: Any = json.loads(_anchor_file(directory, day).read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return None
        if str(raw.get("date")) != day.isoformat():
            return None
        return SessionAnchors(
            date=day,
            regular_open=dt.time.fromisoformat(str(raw["regular_open"])),
            closing_auction_start=dt.time.fromisoformat(str(raw["closing_auction_start"])),
            regular_close=dt.time.fromisoformat(str(raw["regular_close"])),
            after_market_end=dt.time.fromisoformat(str(raw["after_market_end"])),
            source=AnchorSource(str(raw["source"])),
        )
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError, SessionAnchorError):
        return None


def resolve_session_anchors(directory: pathlib.Path, day: dt.date) -> SessionAnchors:
    loaded = load_session_anchors(directory, day)
    if loaded is not None:
        return loaded
    if _anchor_file(directory, day).exists():
        logger.warning("[DATA] stage=session_anchors status=DEFAULT date=%s", day.isoformat())
    else:
        logger.debug("[DATA] stage=session_anchors status=DEFAULT date=%s", day.isoformat())
    return standard_session_anchors(day)
