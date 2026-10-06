"""실시간 스트리머 코어 루프 (재접속/replay/gap/flush)."""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import random
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from src.core.errors import KrxAlphaError
from src.core.observability import EVENT
from src.core.session_anchors import (
    STANDARD_KRX_AFTER_MARKET_OPEN,
    STANDARD_NXT_AFTER_MARKET_OPEN,
    STANDARD_NXT_PREMARKET_END,
    STANDARD_NXT_PREMARKET_OPEN,
    SessionAnchors,
)
from src.realtime.contracts import (
    HeartbeatProbe,
    L0Frame,
    MarketVenue,
    VendorAck,
    VendorAdapter,
    VendorAuthRejected,
    VendorDisconnected,
)
from src.realtime.session import CollectorSession
from src.storage.journal import JournalWriteError

logger = logging.getLogger(__name__)

SILENCE_LIMIT_S: float = 30.0
# Bounds watchdog re-arm latency after a watch window opens; 1 Hz wakeups are negligible vs tick rate.
SILENCE_POLL_INTERVAL_S: float = 1.0
OUTAGE_CRITICAL_S: float = 300.0
AUTH_BACKOFF_MAX_S: float = 300.0
_KST = ZoneInfo("Asia/Seoul")


def regular_session_silence_limit_s(
    now: dt.datetime, *, anchors: SessionAnchors, limit_s: float = SILENCE_LIMIT_S
) -> float | None:
    # 2026-09-14 L0 실측: 장중(09:00-15:30) 프레임 간격 최대 0.4s에 불과해 30s 침묵은 장애다.
    # 반면 정규장 밖 구간(장전 시간외종가·동시호가·장후 시간외종가·애프터마켓) 침묵은 22-450s까지 정상이므로 정규 세션 시간대에만 침묵 감시를 켠다.
    if anchors.regular_open <= now.astimezone(_KST).time() < anchors.regular_close:
        return limit_s
    return None


def aftermarket_silence_limit_s(
    now: dt.datetime, *, route: Any, anchors: SessionAnchors, limit_s: float = SILENCE_LIMIT_S
) -> float | None:
    venue = route.venue if hasattr(route, "venue") else route
    if MarketVenue(venue) == MarketVenue.NXT:
        open_t = anchors.shift_post_close(STANDARD_NXT_AFTER_MARKET_OPEN)
    else:
        open_t = anchors.shift_post_close(STANDARD_KRX_AFTER_MARKET_OPEN)
    if open_t <= now.astimezone(_KST).time() < anchors.after_market_end:
        return limit_s
    return None


def premarket_silence_limit_s(
    now: dt.datetime, *, anchors: SessionAnchors, limit_s: float = SILENCE_LIMIT_S
) -> float | None:
    """Silence limit for the NXT premarket collector, or None outside the premarket window.

    The watchdog arms only inside [shift_pre_open(08:00), shift_pre_open(08:50)) so the quiet period
    before the first print and after the window never trips a reconnect storm.
    """
    start = anchors.shift_pre_open(STANDARD_NXT_PREMARKET_OPEN)
    end = anchors.shift_pre_open(STANDARD_NXT_PREMARKET_END)
    if start <= now.astimezone(_KST).time() < end:
        return limit_s
    return None


class FrameSink(Protocol):
    def record(self, frame: L0Frame) -> None: ...
    def note_ack(self, vendor: str, ack: VendorAck) -> None: ...
    def note_gap(self, symbol: str, start_ns: int, end_ns: int, reason: str) -> None: ...
    def flush(self) -> int: ...


@dataclass
class SessionFrameSink:
    session: CollectorSession
    checkpoint_interval_s: float = 60.0
    monotonic: Callable[[], float] = time.monotonic
    _last_checkpoint: float | None = field(default=None, init=False)

    def record(self, frame: L0Frame) -> None:
        self.session.record_frame(
            vendor=frame.vendor,
            venue=frame.venue,
            session=frame.session,
            stream=frame.stream,
            raw=frame.raw,
            exchange_event_time=frame.exchange_event_time,
            recv_mono_ns=frame.recv_mono_ns,
            recv_wall_ns=frame.recv_wall_ns,
            conn_id=frame.conn_id,
            conn_seq=frame.conn_seq,
        )

    def note_ack(self, vendor: str, ack: VendorAck) -> None:
        self.session.note_ack(
            vendor=vendor, tr_id=ack.stream, symbol=ack.symbol, rt_cd=ack.code, accepted=ack.accepted
        )

    def note_gap(self, symbol: str, start_ns: int, end_ns: int, reason: str) -> None:
        self.session.note_gap(symbol=symbol, gap_start_ns=start_ns, gap_end_ns=end_ns, reason=reason)
        self.session.persist()
        self._last_checkpoint = self.monotonic()

    def flush(self) -> int:
        written = self.session.flush_journals()
        now = self.monotonic()
        if self._last_checkpoint is None or now - self._last_checkpoint >= self.checkpoint_interval_s:
            self.session.persist()
            self._last_checkpoint = now
        return written


class RealtimeStreamer:
    """Reconnect/flush loop driving one vendor adapter into one frame sink.

    Outage start is the ``recv_wall_ns`` of the last frame received by this
    streamer (any connection), or the wall time ``run_forever`` began if no frame
    was ever received, for every non-``"stopped"`` reason. An already-open outage
    keeps its start. The closing gap passed to ``note_gap`` is clamped so
    ``start <= end``.
    """

    def __init__(
        self,
        *,
        adapter: VendorAdapter,
        sink: FrameSink,
        replay_pairs: list[tuple[str, str]],
        flush_every: int = 200,
        flush_interval_s: float = 1.0,
        silence_limit: Callable[[], float | None] | None = None,
        silence_poll_interval_s: float = SILENCE_POLL_INTERVAL_S,
        backoff_max_s: float = 60.0,
        rng: Callable[[], float] = random.random,
        wall_ns: Callable[[], int] = time.time_ns,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if silence_poll_interval_s <= 0:
            raise ValueError(f"silence_poll_interval_s must be positive, got {silence_poll_interval_s}")
        self._adapter = adapter
        self._sink = sink
        self._replay_pairs = replay_pairs
        self._flush_every = flush_every
        self._flush_interval_s = flush_interval_s
        self._silence_limit = silence_limit
        self._silence_poll_interval_s = silence_poll_interval_s
        self._backoff_max_s = backoff_max_s
        self._rng = rng
        self._wall_ns = wall_ns
        self._monotonic = monotonic
        self._outage: tuple[int, str] | None = None
        self._failures: int = 0
        self._outage_alerted: bool = False
        self._last_pump_frames: int = 0
        self._last_disconnect_detail: str | None = None
        self._last_frame_wall_ns: int | None = None
        self._last_pingpong: tuple[int, int | None] | None = None

    def _snapshot_pingpong(self) -> tuple[int, int | None] | None:
        adapter = self._adapter
        if not isinstance(adapter, HeartbeatProbe):
            return None
        return (adapter.pingpong_count, adapter.last_pingpong_wall_ns)

    def _disconnect_log_fields(self) -> tuple[str, str, str]:
        now_ns = self._wall_ns()

        def _age(at_ns: int | None) -> str:
            return "na" if at_ns is None else f"{(now_ns - at_ns) / 1e9:.2f}"

        if self._last_pingpong is None:
            return (_age(self._last_frame_wall_ns), "na", "na")
        count, last_pingpong_ns = self._last_pingpong
        return (_age(self._last_frame_wall_ns), _age(last_pingpong_ns), str(count))

    def _close_outage(self, end_ns: int, *, recovered: bool) -> None:
        if self._outage is None:
            return
        start_ns, reason = self._outage
        end_ns = max(start_ns, end_ns)
        symbols = sorted({s for s, _ in self._replay_pairs})
        for sym in symbols:
            self._sink.note_gap(sym, start_ns, end_ns, reason)
        logger.warning(
            "[DATA] stage=stream_gap reason=%s gap_ms=%d symbols=%d",
            reason,
            (end_ns - start_ns) // 1_000_000,
            len(symbols),
        )
        if self._outage_alerted:
            logger.warning(
                "[DATA] stage=stream_outage status=%s reason=%s outage_s=%d",
                "RECOVERED" if recovered else "UNRESOLVED",
                reason,
                (end_ns - start_ns) // 1_000_000_000,
            )
            self._outage_alerted = False
        self._outage = None

    async def pump(self, stop: asyncio.Event) -> str:
        """Run one connection until stop, vendor failure, or silence watchdog.

        Silence is measured on the injected monotonic clock from the later of the
        last frame received on this connection, subscribe completion, and the
        moment the watchdog became armed (limit None -> value), so a window
        opening after a long legitimate quiet period does not fire immediately.
        The limit is re-evaluated at least every ``silence_poll_interval_s``; the
        watchdog therefore arms within one poll interval of window open and fires
        no later than ``limit`` (+ scheduler jitter) after the silence baseline.

        A single ``adapter.recv()`` task stays in flight across poll wakeups and is
        cancelled only on return (stop, watchdog, failure): cancelling a pending
        recv may discard a frame already read from the socket but not yet returned.

        Any other ``Exception`` raised while connecting, subscribing, receiving or recording is a
        connection-level failure: it is logged at ERROR with traceback and reported as ``"disconnect"``
        so the reconnect/backoff loop in ``run_forever`` keeps the process alive. ``JournalWriteError``
        and ``KrxAlphaError`` are fail-closed domain errors and still propagate.

        Returns:
            "stopped" | "watchdog" | "disconnect" | "auth_rejected".
        """
        frames = 0
        try:
            await self._adapter.connect()
            acks = await self._adapter.subscribe(self._replay_pairs)
            for ack in acks:
                self._sink.note_ack(self._adapter.name, ack)
            accepted = sum(1 for a in acks if a.accepted)
            logger.info(
                "[DATA] stage=stream_connect pairs=%d accepted=%d rejected=%d",
                len(acks),
                accepted,
                len(acks) - accepted,
                extra=EVENT,
            )
            if len(acks) - accepted:
                logger.warning(
                    "[DATA] stage=stream_subscribe status=PARTIAL rejected=%d codes=%s",
                    len(acks) - accepted,
                    ",".join(sorted({a.code for a in acks if not a.accepted})),
                )
            last_flush = self._monotonic()
            silent_since = last_flush
            armed = False
            recv_task = asyncio.ensure_future(self._adapter.recv())
            stop_task = asyncio.ensure_future(stop.wait())
            try:
                while not stop.is_set():
                    limit = self._silence_limit() if self._silence_limit is not None else None
                    timeout = self._silence_poll_interval_s
                    if limit is None:
                        armed = False
                    else:
                        if not armed:
                            # Window just opened: an arming baseline later than the last
                            # frame or subscribe keeps a long legitimate quiet period
                            # from tripping the watchdog on the first poll after it opens.
                            silent_since = self._monotonic()
                            armed = True
                        remaining = limit - (self._monotonic() - silent_since)
                        if remaining <= 0:
                            return "watchdog"
                        timeout = min(timeout, remaining)
                    done, _ = await asyncio.wait(
                        {recv_task, stop_task}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
                    )
                    if recv_task in done:
                        frame = recv_task.result()
                        recv_task = asyncio.ensure_future(self._adapter.recv())
                        if frames == 0:
                            self._close_outage(frame.recv_wall_ns, recovered=True)
                        self._sink.record(frame)
                        self._last_frame_wall_ns = frame.recv_wall_ns
                        frames += 1
                        silent_since = self._monotonic()
                        if frames % self._flush_every == 0 or self._monotonic() - last_flush >= self._flush_interval_s:
                            self._sink.flush()
                            last_flush = self._monotonic()
            finally:
                # Cancelling a pending recv may discard a frame already read from the
                # socket, so it happens only once pump is returning for good.
                for pending in (recv_task, stop_task):
                    pending.cancel()
                primary_error = sys.exception()
                outcomes = await asyncio.gather(recv_task, stop_task, return_exceptions=True)
                for outcome in outcomes:
                    if (
                        isinstance(outcome, KrxAlphaError)
                        and not isinstance(outcome, VendorDisconnected)
                        and not isinstance(primary_error, (KrxAlphaError, asyncio.CancelledError))
                    ):
                        raise outcome
            return "stopped"
        except VendorAuthRejected as exc:
            self._last_disconnect_detail = str(exc)
            return "auth_rejected"
        except VendorDisconnected as exc:
            self._last_disconnect_detail = str(exc)
            return "disconnect"
        except (JournalWriteError, KrxAlphaError):
            raise
        except Exception as exc:
            self._last_disconnect_detail = f"unexpected:{type(exc).__name__}"
            logger.error("[DATA] stage=stream_pump status=ERROR error_type=%s", type(exc).__name__, exc_info=True)
            return "disconnect"
        finally:
            self._last_pump_frames = frames
            self._last_pingpong = self._snapshot_pingpong()
            primary_error = sys.exception()
            cleanup_error: KrxAlphaError | None = None
            try:
                self._sink.flush()
            except (KrxAlphaError, OSError) as flush_exc:
                logger.critical("[DATA] stage=stream_flush status=FAIL error=%s", str(flush_exc), exc_info=True)
                cleanup_error = flush_exc if isinstance(flush_exc, KrxAlphaError) else JournalWriteError(str(flush_exc))
            try:
                await self._adapter.aclose()
            except VendorDisconnected as close_exc:
                logger.debug("[DATA] stage=stream_close status=SUPPRESSED error_type=%s", type(close_exc).__name__)
            except KrxAlphaError as close_exc:
                if cleanup_error is None:
                    cleanup_error = close_exc
            except Exception as close_exc:
                logger.debug("[DATA] stage=stream_close status=SUPPRESSED error_type=%s", type(close_exc).__name__)
            if cleanup_error is not None and not isinstance(primary_error, (KrxAlphaError, asyncio.CancelledError)):
                raise cleanup_error

    async def run_forever(
        self,
        stop: asyncio.Event,
        *,
        max_cycles: int | None = None,
        backoff_s: float = 1.0,
        sleep: Any = None,
    ) -> None:
        cycle = 0
        run_start_ns = self._wall_ns()
        while not stop.is_set():
            reason = await self.pump(stop)
            cycle += 1
            if reason == "stopped":
                break
            if self._last_pump_frames > 0:
                self._failures = 0
            self._failures += 1
            if self._outage is None:
                # Opening at the last frame's reception time, not the trigger time,
                # keeps every recorded gap covering the real no-data interval.
                self._outage = (self._last_frame_wall_ns if self._last_frame_wall_ns is not None else run_start_ns, reason)
            cap = AUTH_BACKOFF_MAX_S if reason == "auth_rejected" else self._backoff_max_s
            delay = min(cap, backoff_s * 2 ** (self._failures - 1)) * (0.5 + self._rng() / 2)
            logger.warning(
                "[DATA] stage=stream_disconnect reason=%s frames=%d consecutive_failures=%d backoff_s=%.2f "
                "last_frame_age_s=%s last_pingpong_age_s=%s pingpongs=%s",
                reason,
                self._last_pump_frames,
                self._failures,
                delay,
                *self._disconnect_log_fields(),
            )
            if not self._outage_alerted:
                outage_s = (self._wall_ns() - self._outage[0]) / 1e9
                regular = self._silence_limit is not None and self._silence_limit() is not None
                if reason == "auth_rejected" or (regular and outage_s >= OUTAGE_CRITICAL_S):
                    logger.critical(
                        "[DATA] stage=stream_outage status=CRITICAL reason=%s detail=%s outage_s=%d consecutive_failures=%d",
                        reason,
                        self._last_disconnect_detail,
                        int(outage_s),
                        self._failures,
                    )
                    self._outage_alerted = True
            if (max_cycles is not None and cycle >= max_cycles) or stop.is_set():
                break
            await (sleep or asyncio.sleep)(delay)
        self._close_outage(self._wall_ns(), recovered=False)
