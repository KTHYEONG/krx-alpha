"""REST confirmation of quiet-suspect symbols on the market-sweep child (observe-only)."""

from __future__ import annotations

import datetime as dt
import json
import logging
import pathlib
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, Protocol
from zoneinfo import ZoneInfo

from src.core.config import (
    QUIET_CHECK_BATCH,
    QUIET_CHECK_COOLDOWN_S,
    QUIET_CHECK_FILE_STALE_S,
    QUIET_CHECK_MARGIN_S,
)
from src.core.symbols import is_krx_short_code

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")
_SUSPECT_SCHEMA = "quiet-suspects.v1"
_KRX = "krx"
_QUOTE = "quote"


class BookQuoteSource(Protocol):
    def get_auction_book(self, symbol: str) -> dict[str, object]: ...


@dataclass(frozen=True)
class QuietSuspect:
    symbol: str
    last_frame_wall: float
    silent_s: float


def read_quiet_suspects(path: pathlib.Path, session_date: dt.date, now_wall: float) -> tuple[QuietSuspect, ...]:
    """Return the fresh quote-side KRX suspects of ``session_date`` in file order; empty when unusable or stale."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return ()
    if not isinstance(raw, dict) or raw.get("schema") != _SUSPECT_SCHEMA:
        return ()
    if raw.get("session_date") != session_date.isoformat():
        return ()
    try:
        if now_wall - float(raw["updated_wall"]) > QUIET_CHECK_FILE_STALE_S:
            return ()
        entries = list(raw["suspects"])
    except (KeyError, TypeError, ValueError):
        return ()
    suspects: list[QuietSuspect] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        if entry.get("kind") != _QUOTE or entry.get("venue") != _KRX:
            continue
        symbol = entry.get("symbol")
        if not isinstance(symbol, str) or not is_krx_short_code(symbol):
            continue
        try:
            suspects.append(
                QuietSuspect(
                    symbol=symbol,
                    last_frame_wall=float(entry["last_frame_wall"]),
                    silent_s=float(entry["silent_s"]),
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    return tuple(suspects)


def classify_book_time(
    book_time: str, last_frame_wall: float, session_date: dt.date
) -> Literal["LOSS", "QUIET", "UNKNOWN"]:
    """Classify a KIS book acceptance time (HHMMSS KST) against our last quote frame on ``session_date``."""
    if len(book_time) != 6 or not book_time.isdecimal():
        return "UNKNOWN"
    try:
        accepted = dt.time(int(book_time[:2]), int(book_time[2:4]), int(book_time[4:]))
    except ValueError:
        return "UNKNOWN"
    accepted_wall = dt.datetime.combine(session_date, accepted, tzinfo=_KST).timestamp()
    return "LOSS" if accepted_wall > last_frame_wall + QUIET_CHECK_MARGIN_S else "QUIET"


class QuietCheck:
    """Confirm quiet suspects with the read-only book quotation during idle sweep iterations."""

    def __init__(
        self,
        path: pathlib.Path,
        session_date: dt.date,
        *,
        wall: Callable[[], float] = time.time,
    ) -> None:
        self._path = path
        self._session_date = session_date
        self._wall = wall
        self._checked_at: dict[str, float] = {}

    def run(self, source: BookQuoteSource) -> int:
        """Query up to one batch of cooled-down suspects; return the number of quotation calls made."""
        now = self._wall()
        due = [
            suspect
            for suspect in read_quiet_suspects(self._path, self._session_date, now)
            if suspect.symbol not in self._checked_at
            or now - self._checked_at[suspect.symbol] >= QUIET_CHECK_COOLDOWN_S
        ][:QUIET_CHECK_BATCH]
        counts = {"LOSS": 0, "QUIET": 0, "UNKNOWN": 0}
        failed = 0
        for suspect in due:
            self._checked_at[suspect.symbol] = now
            try:
                row = source.get_auction_book(suspect.symbol)
            except Exception:
                failed += 1
                continue
            book_time = str(row.get("book_time", ""))
            verdict = classify_book_time(book_time, suspect.last_frame_wall, self._session_date)
            counts[verdict] += 1
            if verdict == "LOSS":
                logger.warning(
                    "[DATA] stage=quiet_check status=LOSS symbol=%s kind=quote book_time=%s "
                    "last_frame_wall=%.3f silent_s=%.1f",
                    suspect.symbol,
                    book_time,
                    suspect.last_frame_wall,
                    suspect.silent_s,
                )
        if due:
            logger.info(
                "[DATA] stage=quiet_check status=OK checked=%d loss=%d quiet=%d unknown=%d failed=%d",
                len(due),
                counts["LOSS"],
                counts["QUIET"],
                counts["UNKNOWN"],
                failed,
            )
        return len(due)


__all__ = ["BookQuoteSource", "QuietCheck", "QuietSuspect", "classify_book_time", "read_quiet_suspects"]
