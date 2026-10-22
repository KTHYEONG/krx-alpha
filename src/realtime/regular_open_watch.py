"""Regular-session silence limit that tolerates a late first trade after the scheduled open."""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Callable
from zoneinfo import ZoneInfo

from src.core.session_anchors import SessionAnchors
from src.realtime.contracts import L0Frame

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")
_TRADE_STREAM = "H0STCNT0"
_NS = 1_000_000_000


class RegularOpenWatch:
    """Silence limit for the regular session plus a once-per-day delayed-open alert.

    Until the first trade frame of the day arrives, the limit stretches to `open_grace_s` (counted
    from the scheduled open) so an unannounced late open or a volatility halt at the open does not
    trigger a reconnect storm; afterwards the normal `silence_limit_s` applies. Receiving a frame
    never proves the open happened, so only trade frames received at or after the scheduled open
    end the grace. `standby_window` marks the open window in which a hot standby connection is kept. `silence_limit` logs the alert as a side effect because the streamer polls it
    every second; `observe` never raises.
    """

    def __init__(
        self,
        *,
        anchors: SessionAnchors,
        silence_limit_s: float,
        open_grace_s: float,
        alert_after_s: float,
        standby_lead_s: float,
        standby_hold_s: float,
        now: Callable[[], dt.datetime] = lambda: dt.datetime.now(dt.UTC),
    ) -> None:
        if silence_limit_s <= 0 or alert_after_s <= 0:
            raise ValueError("silence_limit_s and alert_after_s must be positive")
        if open_grace_s < alert_after_s:
            raise ValueError("open_grace_s must be >= alert_after_s")
        if standby_lead_s <= 0 or standby_hold_s <= 0:
            raise ValueError("standby_lead_s and standby_hold_s must be positive")
        self._anchors = anchors
        self._silence_limit_s = silence_limit_s
        self._open_grace_s = open_grace_s
        self._alert_after_s = alert_after_s
        self._standby_lead_s = standby_lead_s
        self._standby_hold_s = standby_hold_s
        self._now = now
        self._first_trade_seen = False
        self._alerted = False

    def _since_open_s(self, moment: dt.datetime) -> float:
        local = moment.astimezone(_KST)
        open_at = dt.datetime.combine(local.date(), self._anchors.regular_open, tzinfo=_KST)
        return (local - open_at).total_seconds()

    def observe(self, frame: L0Frame) -> None:
        """End the open grace on the first trade frame received at or after the scheduled open."""
        if self._first_trade_seen or frame.stream != _TRADE_STREAM:
            return
        try:
            since_open = self._since_open_s(dt.datetime.fromtimestamp(frame.recv_wall_ns / _NS, tz=dt.UTC))
        except (OverflowError, OSError, ValueError):
            return
        if since_open < 0:
            return
        self._first_trade_seen = True
        if self._alerted:
            logger.warning("[DATA] stage=open_delay status=RESOLVED first_trade_after_s=%.0f", since_open)

    def standby_window(self) -> bool:
        """True from `standby_lead_s` before the scheduled open until `standby_hold_s` after it."""
        since_open = self._since_open_s(self._now())
        return -self._standby_lead_s <= since_open < self._standby_hold_s

    def silence_limit(self) -> float | None:
        """Return the silence limit in seconds, or None outside the regular session."""
        moment = self._now()
        local = moment.astimezone(_KST).time()
        if not (self._anchors.regular_open <= local < self._anchors.regular_close):
            return None
        if self._first_trade_seen:
            return self._silence_limit_s
        since_open = self._since_open_s(moment)
        if since_open >= self._open_grace_s:
            return self._silence_limit_s
        if since_open >= self._alert_after_s and not self._alerted:
            self._alerted = True
            logger.critical(
                "[DATA] stage=open_delay status=CRITICAL reason=open_delay_suspected since_open_s=%.0f grace_s=%.0f",
                since_open,
                self._open_grace_s,
            )
        return max(self._silence_limit_s, self._open_grace_s - since_open)
