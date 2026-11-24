"""KRX session boundary anchors for one KST business date."""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import pathlib
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Final

from src.core.errors import KrxAlphaError

logger = logging.getLogger(__name__)

STANDARD_REGULAR_OPEN: dt.time = dt.time(9, 0)
STANDARD_CLOSING_AUCTION: dt.time = dt.time(15, 20)
STANDARD_REGULAR_CLOSE: dt.time = dt.time(15, 30)
STANDARD_POST_CLOSING_PRICE_OPEN: dt.time = dt.time(15, 40)
STANDARD_NXT_AFTER_MARKET_OPEN: dt.time = dt.time(15, 40)
STANDARD_KRX_AFTER_MARKET_OPEN: dt.time = dt.time(16, 0)
# The two 15:40 constants are distinct exchange rules that coincide, kept separate so one can move without the other.
STANDARD_NXT_PREMARKET_OPEN: dt.time = dt.time(8, 0)
STANDARD_NXT_PREMARKET_END: dt.time = dt.time(8, 50)
# NXT premarket (08:00~08:50) is a distinct NXT rule, kept separate from the KRX 08:30/08:40 closing-price window.
STANDARD_AFTER_MARKET_END: dt.time = dt.time(20, 0)
MAX_SESSION_SHIFT: dt.timedelta = dt.timedelta(hours=3)

_BASE_DATE: dt.date = dt.date(2000, 1, 1)
# Sentinel: premarket window derived from the standard schedule when the anchors carry no vendor premarket data.
DERIVE_NXT_PREMARKET: Final[object] = object()


class AnchorSource(StrEnum):
    VENDOR = "vendor"
    DEFAULT = "default"
    VERIFIED = "verified"


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
    nxt_premarket: tuple[dt.time, dt.time] | object | None = DERIVE_NXT_PREMARKET

    def __post_init__(self) -> None:
        if not (self.regular_open < self.closing_auction_start < self.regular_close < self.after_market_end):
            raise SessionAnchorError(f"anchors out of order for {self.date}: {self.regular_open!r} {self.closing_auction_start!r} {self.regular_close!r} {self.after_market_end!r}")
        if isinstance(self.nxt_premarket, tuple):
            premarket_open, premarket_end = self.nxt_premarket
            if not (premarket_open < premarket_end <= self.regular_open):
                raise SessionAnchorError(f"nxt premarket out of order for {self.date}: {premarket_open!r} {premarket_end!r}")
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

    def nxt_premarket_window(self) -> tuple[dt.time, dt.time] | None:
        if self.nxt_premarket is DERIVE_NXT_PREMARKET:
            if self.open_shift != dt.timedelta(0):
                return None
            return (STANDARD_NXT_PREMARKET_OPEN, STANDARD_NXT_PREMARKET_END)
        if self.nxt_premarket is None:
            return None
        return self.nxt_premarket  # type: ignore[return-value]


def standard_session_anchors(day: dt.date) -> SessionAnchors:
    return SessionAnchors(
        date=day,
        regular_open=STANDARD_REGULAR_OPEN,
        closing_auction_start=STANDARD_CLOSING_AUCTION,
        regular_close=STANDARD_REGULAR_CLOSE,
        after_market_end=STANDARD_AFTER_MARKET_END,
        source=AnchorSource.DEFAULT,
    )


@dataclass(frozen=True)
class VerifiedShift:
    """KRX-announced regular-session clock of one shifted day (after-market end is not asserted)."""

    regular_open: dt.time
    closing_auction_start: dt.time
    regular_close: dt.time
    provenance: str


# The vendor calendar is the only other source of shifted clocks and has never been observed on a shifted day;
# a missing or standard vendor answer would otherwise drop the shifted hour silently (DEFAULT/standard anchors).
VERIFIED_SHIFTED_SESSIONS: Mapping[dt.date, VerifiedShift] = MappingProxyType(
    {
        dt.date(2026, 11, 19): VerifiedShift(
            regular_open=dt.time(10, 0),
            closing_auction_start=dt.time(16, 20),
            regular_close=dt.time(16, 30),
            provenance="KRX 2026 CSAT-day session notice",
        ),
    }
)


def reconcile_verified_shift(day: dt.date, anchors: SessionAnchors | None) -> SessionAnchors | None:
    """Return ``anchors`` with ``VERIFIED_SHIFTED_SESSIONS`` enforced for ``day``; unlisted days pass through unchanged."""
    shift = VERIFIED_SHIFTED_SESSIONS.get(day)
    if shift is None:
        return anchors
    expected = (shift.regular_open, shift.closing_auction_start, shift.regular_close)
    if anchors is not None and (anchors.regular_open, anchors.closing_auction_start, anchors.regular_close) == expected:
        return anchors
    logger.warning(
        "[DATA] stage=session_anchors status=VERIFIED_OVERRIDE date=%s vendor=%s",
        day.isoformat(),
        "missing" if anchors is None else f"{anchors.source.value}:{anchors.regular_open}-{anchors.regular_close}",
    )
    return SessionAnchors(
        date=day,
        regular_open=shift.regular_open,
        closing_auction_start=shift.closing_auction_start,
        regular_close=shift.regular_close,
        after_market_end=(
            anchors.after_market_end
            if anchors is not None and anchors.after_market_end > shift.regular_close
            else STANDARD_AFTER_MARKET_END
        ),
        source=AnchorSource.VERIFIED,
        nxt_premarket=anchors.nxt_premarket if anchors is not None else None,
    )


def _anchor_file(directory: pathlib.Path, day: dt.date) -> pathlib.Path:
    return pathlib.Path(directory) / f"dt={day.isoformat()}.json"


def _premarket_payload(value: tuple[dt.time, dt.time] | None) -> list[str] | None:
    if value is None:
        return None
    return [value[0].strftime("%H:%M:%S"), value[1].strftime("%H:%M:%S")]


def _load_premarket(raw: Mapping[str, Any]) -> tuple[dt.time, dt.time] | object | None:
    if "nxt_premarket" not in raw:
        return DERIVE_NXT_PREMARKET
    value = raw["nxt_premarket"]
    if value is None:
        return None
    if not isinstance(value, list) or len(value) != 2:
        raise TypeError("nxt_premarket must be null or a 2-item list")
    return (dt.time.fromisoformat(str(value[0])), dt.time.fromisoformat(str(value[1])))


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
    if anchors.nxt_premarket is not DERIVE_NXT_PREMARKET:
        payload["nxt_premarket"] = _premarket_payload(anchors.nxt_premarket)  # type: ignore[arg-type]
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
            nxt_premarket=_load_premarket(raw),
        )
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError, SessionAnchorError):
        return None


def resolve_session_anchors(directory: pathlib.Path, day: dt.date) -> SessionAnchors:
    loaded = reconcile_verified_shift(day, load_session_anchors(directory, day))
    if loaded is not None:
        return loaded
    if _anchor_file(directory, day).exists():
        logger.warning("[DATA] stage=session_anchors status=DEFAULT date=%s", day.isoformat())
    else:
        logger.debug("[DATA] stage=session_anchors status=DEFAULT date=%s", day.isoformat())
    return standard_session_anchors(day)
