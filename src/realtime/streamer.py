"""실시간 스트리머 코어 루프 (재접속/replay/gap/flush)."""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from src.core.observability import EVENT
from src.realtime.contracts import (
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
OUTAGE_CRITICAL_S: float = 300.0
AUTH_BACKOFF_MAX_S: float = 300.0
_REGULAR_OPEN = dt.time(9, 0)
_REGULAR_CLOSE = dt.time(15, 30)
_KST = ZoneInfo("Asia/Seoul")


def regular_session_silence_limit_s(now: dt.datetime, *, limit_s: float = SILENCE_LIMIT_S) -> float | None:
    # 2026-09-14 L0 실측: 장중(09:00-15:30) 프레임 간격 최대 0.4s에 불과해 30s 침묵은 장애다.
    # 반면 정규장 밖 구간(장전 시간외종가·동시호가·장후 시간외종가·애프터마켓) 침묵은 22-450s까지 정상이므로 정규 세션 시간대에만 침묵 감시를 켠다.
    if _REGULAR_OPEN <= now.astimezone(_KST).time() < _REGULAR_CLOSE:
        return limit_s
    return None


_NXT_AFTER_OPEN = dt.time(15, 40)
_KRX_AFTER_OPEN = dt.time(16, 0)
_AFTER_CLOSE = dt.time(20, 0)


def aftermarket_silence_limit_s(
    now: dt.datetime, *, route: Any, limit_s: float = SILENCE_LIMIT_S
) -> float | None:
    venue = route.venue if hasattr(route, "venue") else route
    open_t = _NXT_AFTER_OPEN if MarketVenue(venue) == MarketVenue.NXT else _KRX_AFTER_OPEN
    if open_t <= now.astimezone(_KST).time() < _AFTER_CLOSE:
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
    def __init__(
        self,
        *,
        adapter: VendorAdapter,
        sink: FrameSink,
        replay_pairs: list[tuple[str, str]],
        flush_every: int = 200,
        flush_interval_s: float = 1.0,
        silence_limit: Callable[[], float | None] | None = None,
        backoff_max_s: float = 60.0,
        rng: Callable[[], float] = random.random,
        wall_ns: Callable[[], int] = time.time_ns,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._adapter = adapter
        self._sink = sink
        self._replay_pairs = replay_pairs
        self._flush_every = flush_every
        self._flush_interval_s = flush_interval_s
        self._silence_limit = silence_limit
        self._backoff_max_s = backoff_max_s
        self._rng = rng
        self._wall_ns = wall_ns
        self._monotonic = monotonic
        self._outage: tuple[int, str] | None = None
        self._failures: int = 0
        self._outage_alerted: bool = False
        self._last_pump_frames: int = 0
        self._last_disconnect_detail: str | None = None

    def _close_outage(self, end_ns: int, *, recovered: bool) -> None:
        if self._outage is None:
            return
        start_ns, reason = self._outage
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
            while not stop.is_set():
                limit = self._silence_limit() if self._silence_limit is not None else None
                recv_task = asyncio.ensure_future(self._adapter.recv())
                stop_task = asyncio.ensure_future(stop.wait())
                done, pending = await asyncio.wait(
                    {recv_task, stop_task}, timeout=limit, return_when=asyncio.FIRST_COMPLETED
                )
                for p in pending:
                    p.cancel()
                for p in pending:
                    with contextlib.suppress(asyncio.CancelledError):
                        await p
                if not done:
                    return "watchdog"
                if recv_task in done:
                    frame = recv_task.result()
                    if frames == 0:
                        self._close_outage(frame.recv_wall_ns, recovered=True)
                    self._sink.record(frame)
                    frames += 1
                    if frames % self._flush_every == 0 or self._monotonic() - last_flush >= self._flush_interval_s:
                        self._sink.flush()
                        last_flush = self._monotonic()
            return "stopped"
        except VendorAuthRejected as exc:
            self._last_disconnect_detail = str(exc)
            return "auth_rejected"
        except VendorDisconnected as exc:
            self._last_disconnect_detail = str(exc)
            return "disconnect"
        finally:
            self._last_pump_frames = frames
            try:
                self._sink.flush()
            except (JournalWriteError, OSError) as flush_exc:
                logger.critical("[DATA] stage=stream_flush status=FAIL error=%s", str(flush_exc), exc_info=True)
            await self._adapter.aclose()

    async def run_forever(
        self,
        stop: asyncio.Event,
        *,
        max_cycles: int | None = None,
        backoff_s: float = 1.0,
        sleep: Any = None,
    ) -> None:
        cycle = 0
        while not stop.is_set():
            reason = await self.pump(stop)
            cycle += 1
            if reason == "stopped":
                break
            if self._last_pump_frames > 0:
                self._failures = 0
            self._failures += 1
            if self._outage is None:
                self._outage = (self._wall_ns(), reason)
            cap = AUTH_BACKOFF_MAX_S if reason == "auth_rejected" else self._backoff_max_s
            delay = min(cap, backoff_s * 2 ** (self._failures - 1)) * (0.5 + self._rng() / 2)
            logger.warning(
                "[DATA] stage=stream_disconnect reason=%s frames=%d consecutive_failures=%d backoff_s=%.2f",
                reason,
                self._last_pump_frames,
                self._failures,
                delay,
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
