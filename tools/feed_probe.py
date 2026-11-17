"""Read-only feed probe for the DB, Toss and LS NXT secondaries (feed resilience Part 1).

Subscribes a candidate feed in ascending ramp steps, holds each step, and writes raw frames plus a summary.
It never trades, never issues a Toss token, never writes into the L0 journal and is not registered with the daemon.

Recipe (no deploy needed): run inside the running collector container with `docker exec <collector> uv run python
tools/feed_probe.py ...`, or with a one-off `docker run` of the deployed image that bind-mounts this tools/ folder
read-only. Output goes to --out, which must lie outside the L0 journal root.

Exit codes: 0 completed, 2 usage, 3 no token, 4 forbidden window or lease denied,
5 vendor rejected immediately, 6 clock unsynced.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import dataclasses
import datetime as dt
import itertools
import json
import os
import pathlib
import re
import sys
import time
from collections.abc import Callable, Sequence
from typing import IO, Any, Literal, Protocol
from zoneinfo import ZoneInfo

import aiohttp

from deploy.shared.host_broker_contract import token_record_path
from src.cli.collect_stream import _credential_key_id
from src.cli.probe_db_stream import _out_inside_l0
from src.core.config import CollectorSettings, KisTokenSettings
from src.core.errors import KrxAlphaError
from src.core.session_anchors import SessionAnchors, resolve_session_anchors
from src.marketdata.host_token_store import AsyncRotatingTokenStore
from src.marketdata.toss_token_store import TossTokenStore, toss_token_path
from src.realtime.adapters.ls import LsRealtimeAdapter
from src.realtime.adapters.registry import RegularAdapterContext, regular_vendor
from src.realtime.clock import ClockUnsyncedError, measure_ntp_offset_ns
from src.realtime.contracts import L0Frame, VendorAuthRejected, VendorDisconnected
from src.realtime.ws_lease import Lease, LeaseRole, LeaseUnavailable, WsLeaseLedger

Pair = tuple[str, str]
Stream = Literal["trade", "book"]

VENDORS: tuple[str, ...] = ("db", "toss", "ls-nxt")
STREAMS: tuple[str, ...] = ("trade", "book")

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_NO_TOKEN = 3
EXIT_REFUSED = 4
EXIT_REJECTED = 5
EXIT_CLOCK = 6

_KST = ZoneInfo("Asia/Seoul")
_KRX_STREAM = {"trade": "H0STCNT0", "book": "H0STASP0"}
_KRX_TO_STREAM = {v: k for k, v in _KRX_STREAM.items()}
_NXT_TR_BY_KRX = {"H0STCNT0": "NS3", "H0STASP0": "NH1"}
_KRX_BY_NXT_TR = {v: k for k, v in _NXT_TR_BY_KRX.items()}
_NXT_KEY_RE = re.compile(r"\d{6}")

_TOSS_WS_URL = "wss://openapi-ws.tossinvest.com/ws/v1"
_TOSS_TYPE = {"trade": "trade:kr", "book": "orderbook:kr"}
_TOSS_TYPE_WHITELIST = frozenset(_TOSS_TYPE.values())
_TOSS_TOPIC_CAP = 100
_TOSS_MIN_DECLARATION_GAP_S = 0.25
_TOSS_ACK_TIMEOUT_S = 10.0
_PING_EVERY_S = 30.0
_LEASE_RENEW_EVERY_S = 5.0

_KCA_BLACKOUT = ((dt.time(8, 30), dt.time(9, 40)), (dt.time(15, 10), dt.time(15, 43)))

_SECRET_FIELD_RE = re.compile(
    r'("(?:access_token|token|authorization|app_?key|app_?secret|secret_?key|account_?no|acnt_?no|'
    r'accountNo|accountSeq|acntNo)"\s*:\s*")[^"]*(")',
    re.IGNORECASE,
)
_BEARER_RE = re.compile(r"(Bearer\s+)[A-Za-z0-9._~+/=-]+", re.IGNORECASE)


class ProbeUsageError(ValueError):
    """Invalid arguments or an output path inside the journal root; maps to exit code 2."""


class _NoTokenError(KrxAlphaError):
    """No usable shared credential; the probe refuses instead of issuing one (exit 3)."""


class _RefusedError(KrxAlphaError):
    """A gate refused the connection (lease denied); carries its exit code."""

    def __init__(self, exit_code: int, message: str) -> None:
        super().__init__(message)
        self.exit_code = exit_code


@dataclasses.dataclass(frozen=True)
class Frame:
    recv_wall_ns: int
    recv_mono_ns: int
    raw: str
    pair: Pair | None


class ProbeIO(Protocol):
    capacity: int

    @property
    def secrets(self) -> tuple[str, ...]: ...

    async def connect(self) -> None: ...

    async def add(self, pairs: list[Pair]) -> list[tuple[Pair, bool, str]]: ...

    async def next_frame(self) -> Frame: ...

    def renew(self) -> bool: ...

    async def close(self) -> None: ...


@dataclasses.dataclass
class StepRecord:
    total_pairs: int
    accepted: int = 0
    rejected: dict[str, int] = dataclasses.field(default_factory=dict)
    frames: int = 0
    first_frame_latency_ms: dict[str, float] = dataclasses.field(default_factory=dict)
    disconnect: str | None = None
    capture: str | None = None


@dataclasses.dataclass
class ProbeSummary:
    vendor: str
    exit_code: int
    cap_pairs: int | None = None
    stop_reason: str | None = None
    second_session_tried: bool = False
    steps: list[StepRecord] = dataclasses.field(default_factory=list)
    detail: str | None = None
    second_session: dict[str, Any] | None = None

    def as_json(self) -> dict[str, Any]:
        data = dataclasses.asdict(self)
        if self.second_session is None:
            del data["second_session"]
        return data


def _append_detail(summary: ProbeSummary, text: str) -> None:
    summary.detail = text if summary.detail is None else f"{summary.detail}; {text}"


def redact(text: str, secrets: Sequence[str]) -> str:
    """Mask known secret values, credential-named JSON fields and Bearer tokens."""
    for secret in secrets:
        if len(secret) >= 8:
            text = text.replace(secret, "<redacted>")
    text = _SECRET_FIELD_RE.sub(r"\1<redacted>\2", text)
    return _BEARER_RE.sub(r"\1<redacted>", text)


def build_pairs(symbols: Sequence[str], streams: Sequence[str]) -> list[Pair]:
    """Expand symbols x streams in symbol-major order; unknown streams raise ProbeUsageError."""
    unknown = set(streams) - set(STREAMS)
    if unknown:
        raise ProbeUsageError(f"unsupported streams: {sorted(unknown)}")
    return [(symbol, stream) for symbol in symbols for stream in streams]


def validate_ramp(ramp: Sequence[int], total_pairs: int) -> None:
    if not ramp or ramp[0] < 1 or any(b <= a for a, b in itertools.pairwise(ramp)):
        raise ProbeUsageError("ramp must be a non-empty strictly ascending list of positive pair counts")
    if ramp[-1] > total_pairs:
        raise ProbeUsageError(f"ramp top {ramp[-1]} exceeds available pairs {total_pairs}")


def toss_declaration(entries: Sequence[tuple[str, Sequence[str]]]) -> str:
    """Build the state-replacing Toss declaration; types outside the read-only whitelist raise ValueError."""
    body: list[dict[str, object]] = []
    for kind, codes in entries:
        if kind not in _TOSS_TYPE_WHITELIST:
            raise ValueError(f"toss subscription type not permitted for the probe: {kind}")
        if codes:
            body.append({"type": kind, "codes": list(codes)})
    return json.dumps(body)


def _toss_topic(pair: Pair) -> str:
    symbol, stream = pair
    return f"{_TOSS_TYPE[stream]}:{symbol}"


def _pair_of_toss_topic(topic: object) -> Pair | None:
    if not isinstance(topic, str):
        return None
    parts = topic.split(":")
    if len(parts) != 3:
        return None
    kind = f"{parts[0]}:{parts[1]}"
    for stream, toss_type in _TOSS_TYPE.items():
        if toss_type == kind:
            return (parts[2], stream)
    return None


def _topic_of_ack_item(item: object) -> str:
    if isinstance(item, str):
        return item
    if isinstance(item, dict) and isinstance(item.get("topic"), str):
        return str(item["topic"])
    return json.dumps(item, sort_keys=True)


def forbidden_reason(vendor: str, now_kst: dt.datetime, anchors: SessionAnchors) -> str | None:
    """Return why the probe must not run now, or None. Windows follow the day's SessionAnchors."""
    at = now_kst.time()
    if vendor == "ls-nxt":
        handovers = (
            (anchors.shift_pre_open(dt.time(8, 58)), anchors.shift_pre_open(dt.time(9, 6))),
            (anchors.shift_post_close(dt.time(15, 15)), anchors.shift_post_close(dt.time(15, 35))),
        )
        for lo, hi in handovers:
            if lo <= at < hi:
                return f"ls-nxt refused in spare-handover window {lo}-{hi}"
        return None
    if now_kst.weekday() >= 5:
        return "weekend: no regular or NXT-after session"
    if vendor == "toss":
        for lo, hi in _KCA_BLACKOUT:
            if lo <= at < hi:
                return "toss refused in kca blackout window (shared token read)"
    regular = (anchors.shift_pre_open(dt.time(9, 10)), anchors.shift_post_close(dt.time(15, 15)))
    nxt_after = (anchors.shift_post_close(dt.time(15, 45)), anchors.shift_post_close(dt.time(19, 45)))
    for lo, hi in (regular, nxt_after):
        if lo <= at < hi:
            return None
    return f"outside probe windows at {at.isoformat()}"


def _session_anchors(day: dt.date) -> SessionAnchors:
    return resolve_session_anchors(CollectorSettings().paths.session_calendar_dir, day)


class _AdapterIO:
    """Wraps a JSON-WS OAuth adapter (DB or LS NXT); subscription acks flow through the adapter's recv()."""

    def __init__(self, adapter: Any, http: aiohttp.ClientSession, secrets: tuple[str, ...]) -> None:
        self._adapter = adapter
        self._http = http
        self._static_secrets = secrets

    @property
    def capacity(self) -> int:
        return int(self._adapter.capacity_pairs)

    @property
    def secrets(self) -> tuple[str, ...]:
        token = getattr(self._adapter, "_token", None)
        return (*self._static_secrets, token) if isinstance(token, str) and token else self._static_secrets

    async def connect(self) -> None:
        await self._adapter.connect()

    async def add(self, pairs: list[Pair]) -> list[tuple[Pair, bool, str]]:
        acks = await self._adapter.subscribe_live([(symbol, _KRX_STREAM[stream]) for symbol, stream in pairs])
        return [((ack.symbol, _KRX_TO_STREAM[ack.stream]), ack.accepted, ack.code) for ack in acks]

    async def next_frame(self) -> Frame:
        frame: L0Frame = await self._adapter.recv()
        pair = (frame.symbol, _KRX_TO_STREAM[frame.stream]) if frame.stream in _KRX_TO_STREAM else None
        return Frame(frame.recv_wall_ns, frame.recv_mono_ns, frame.raw, pair)

    def renew(self) -> bool:
        return True

    async def close(self) -> None:
        with contextlib.suppress(aiohttp.ClientError, OSError, ValueError, KeyError):
            await self._adapter.aclose()
        await self._http.close()


class _LsNxtAdapter(LsRealtimeAdapter):
    """LS adapter with NXT trade (NS3) and orderbook (NH1) TR codes; KRX frames are ignored."""

    def _subscribe_body(self, symbol: str, stream: str) -> dict[str, str]:
        if _NXT_KEY_RE.fullmatch(symbol) is None:
            raise ValueError(f"NXT symbol must be 6 digits: {symbol!r}")
        return {"tr_cd": _NXT_TR_BY_KRX[stream], "tr_key": f"N{symbol}   "}

    def _data_frame(self, o: dict[str, Any], header: dict[str, Any], raw: str) -> L0Frame | None:
        if "rsp_cd" in header or header.get("tr_cd") not in _KRX_BY_NXT_TR:
            return None
        key = str(header.get("tr_key", "")).strip()
        symbol = key[1:] if key[:1] in ("N", "U") else key
        return L0Frame(
            "ls",
            _KRX_BY_NXT_TR[str(header["tr_cd"])],
            symbol,
            raw,
            time.monotonic_ns(),
            time.time_ns(),
            self._next_seq(),
            conn_id=self._conn_id,
        )


class _LsNxtIO(_AdapterIO):
    """Holds a STANDBY lease on the host-wide LS ledger for the whole probe; the lease is released on close."""

    def __init__(
        self, adapter: Any, http: aiohttp.ClientSession, secrets: tuple[str, ...], ledger: WsLeaseLedger, owner: str
    ) -> None:
        super().__init__(adapter, http, secrets)
        self._ledger = ledger
        self._owner = owner
        self._lease: Lease | None = None

    async def connect(self) -> None:
        try:
            self._lease = self._ledger.acquire(self._owner, LeaseRole.STANDBY)
        except LeaseUnavailable as exc:
            raise _RefusedError(EXIT_REFUSED, f"ls lease unavailable: {exc}") from exc
        try:
            await self._adapter.connect()
        except BaseException:
            self._release()
            raise

    def renew(self) -> bool:
        return self._lease is not None and self._ledger.renew(self._lease)

    async def close(self) -> None:
        try:
            await super().close()
        finally:
            self._release()

    def _release(self) -> None:
        if self._lease is not None:
            self._ledger.release(self._lease)
            self._lease = None


class _TossIO:
    """Standalone read-only Toss client; it reuses the shared token and never issues one."""

    capacity = _TOSS_TOPIC_CAP

    def __init__(
        self,
        token: str,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        self._token = token
        self._clock = clock
        self._sleep = sleep
        self._session: aiohttp.ClientSession | None = None
        self._ws: Any = None
        self._declared: list[Pair] = []
        self._last_declared: float | None = None
        self._ack: asyncio.Future[tuple[set[str], set[str]]] | None = None
        self._keepalive: asyncio.Task[None] | None = None

    @property
    def secrets(self) -> tuple[str, ...]:
        return (self._token,)

    async def connect(self) -> None:
        self._session = aiohttp.ClientSession()
        try:
            self._ws = await self._session.ws_connect(
                _TOSS_WS_URL,
                headers={"Authorization": f"Bearer {self._token}"},
                heartbeat=None,
                max_msg_size=0,
            )
        except aiohttp.WSServerHandshakeError as exc:
            await self.close()
            if exc.status in (401, 403):
                raise VendorAuthRejected(f"auth_rejected:{exc.status}") from exc
            raise VendorDisconnected(f"handshake_failed:{exc.status}") from exc
        except (aiohttp.ClientError, OSError) as exc:
            await self.close()
            raise VendorDisconnected(f"connect_failed:{type(exc).__name__}") from exc
        self._keepalive = asyncio.create_task(self._ping_loop())

    async def add(self, pairs: list[Pair]) -> list[tuple[Pair, bool, str]]:
        if len(self._declared) + len(pairs) > _TOSS_TOPIC_CAP:
            raise ValueError("toss topic cap exceeded")
        self._declared.extend(pairs)
        loop = asyncio.get_running_loop()
        self._ack = loop.create_future()
        await self._declare()
        try:
            subscribed, rejected = await asyncio.wait_for(self._ack, timeout=_TOSS_ACK_TIMEOUT_S)
        except TimeoutError:
            return [(pair, False, "timeout") for pair in pairs]
        results: list[tuple[Pair, bool, str]] = []
        for pair in pairs:
            topic = _toss_topic(pair)
            if topic in subscribed:
                results.append((pair, True, "subscribed"))
            elif topic in rejected:
                results.append((pair, False, "rejected"))
            else:
                results.append((pair, False, "not_acked"))
        return results

    async def _declare(self) -> None:
        now = self._clock()
        if self._last_declared is not None:
            wait = self._last_declared + _TOSS_MIN_DECLARATION_GAP_S - now
            if wait > 0:
                await self._sleep(wait)
        self._last_declared = self._clock()
        grouped: dict[str, list[str]] = {}
        for symbol, stream in self._declared:
            grouped.setdefault(_TOSS_TYPE[stream], []).append(symbol)
        await self._ws.send_str(toss_declaration(list(grouped.items())))

    async def next_frame(self) -> Frame:
        while True:
            msg = await self._ws.receive()
            recv_wall, recv_mono = time.time_ns(), time.monotonic_ns()
            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.ERROR):
                raise VendorDisconnected(f"closed:{msg.type.name}")
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue
            text = str(msg.data)
            if text == "PONG":
                return Frame(recv_wall, recv_mono, text, None)
            try:
                obj = json.loads(text)
            except ValueError:
                return Frame(recv_wall, recv_mono, text, None)
            if isinstance(obj, dict) and ("subscribed" in obj or "rejected" in obj):
                self._resolve_ack(obj)
                return Frame(recv_wall, recv_mono, text, None)
            pair = _pair_of_toss_topic(obj.get("topic")) if isinstance(obj, dict) else None
            return Frame(recv_wall, recv_mono, text, pair)

    def _resolve_ack(self, obj: dict[str, Any]) -> None:
        if self._ack is None or self._ack.done():
            return
        subscribed = {_topic_of_ack_item(item) for item in obj.get("subscribed", [])}
        rejected = {_topic_of_ack_item(item) for item in obj.get("rejected", [])}
        self._ack.set_result((subscribed, rejected))

    async def _ping_loop(self) -> None:
        while True:
            await asyncio.sleep(_PING_EVERY_S)
            await self._ws.send_str("PING")

    def renew(self) -> bool:
        return True

    async def close(self) -> None:
        keepalive, self._keepalive = self._keepalive, None
        if keepalive is not None:
            keepalive.cancel()
        ws, self._ws = self._ws, None
        session, self._session = self._session, None
        if ws is not None:
            with contextlib.suppress(aiohttp.ClientError, OSError):
                await ws.close()
        if session is not None:
            await session.close()


def _load_toss_token(cache_dir: pathlib.Path) -> str:
    app_key = os.environ.get("TOSS_APP_KEY", "")
    if not app_key:
        raise _NoTokenError("TOSS_APP_KEY is unset; no shared Toss token can be located")
    store = TossTokenStore(toss_token_path(cache_dir, app_key))
    try:
        record = store.read()
    except (OSError, ValueError) as exc:
        raise _NoTokenError(f"shared Toss token unreadable: {type(exc).__name__}") from exc
    if record is None or not store._is_usable(record, dt.datetime.now(dt.UTC)):
        raise _NoTokenError("no valid shared Toss token; this probe never issues one")
    return record.access_token


async def _open_io(vendor: str) -> ProbeIO:
    """Build the vendor IO without any network call; raises _NoTokenError when credentials or shared tokens are missing."""
    settings = CollectorSettings()
    cache_dir = KisTokenSettings().token_cache_dir
    if vendor == "toss":
        return _TossIO(_load_toss_token(cache_dir))
    try:
        app = regular_vendor("ls" if vendor == "ls-nxt" else "db").load_credentials()
    except (ValueError, KrxAlphaError) as exc:
        raise _NoTokenError(f"{vendor} credentials unavailable: {type(exc).__name__}") from exc
    http = aiohttp.ClientSession()
    secrets = (app.app_key, app.app_secret)
    if vendor == "db":
        ctx = RegularAdapterContext(http=http, settings=settings, token_cache_dir=cache_dir, market_of=None)
        return _AdapterIO(regular_vendor("db").build(app, ctx), http, secrets)
    store = AsyncRotatingTokenStore(
        token_record_path(cache_dir, "ls", app.app_key),
        vendor="ls",
        rotation_cooldown_s=settings.ls_token_rotation_cooldown_s,
        clock=lambda: dt.datetime.now(dt.UTC),
    )
    adapter = _LsNxtAdapter(
        app_key=app.app_key,
        app_secret=app.app_secret,
        http=http,
        market_of={},
        token_store=store,
        capacity_pairs=settings.ls_max_subscriptions_per_session,
    )
    ledger = WsLeaseLedger(
        cache_dir,
        vendor="ls",
        key_id=_credential_key_id(app.app_key),
        max_connections=settings.ls_max_sessions,
        ttl_s=settings.ws_lease_ttl_s,
    )
    return _LsNxtIO(adapter, http, secrets, ledger, owner=f"feed-probe-{os.getpid()}")


class _Recorder:
    """Writes every redacted frame to the current step's capture file and tracks per-pair first-frame latency."""

    def __init__(self, out_dir: pathlib.Path, vendor: str, secrets: Callable[[], tuple[str, ...]]) -> None:
        self._out_dir = out_dir
        self._vendor = vendor
        self._secrets = secrets
        self._handle: IO[str] | None = None
        self._step: StepRecord | None = None
        self._sent_ns: dict[Pair, int] = {}
        self.disconnect: str | None = None
        self.captures: list[str] = []

    def start_step(self, step: StepRecord, new_pairs: list[Pair]) -> None:
        self.close_step()
        self._out_dir.mkdir(parents=True, exist_ok=True)
        name = f"capture.{self._vendor}.{step.total_pairs}.jsonl"
        self._handle = (self._out_dir / name).open("w", encoding="utf-8")
        self._step = step
        step.capture = name
        self.captures.append(name)
        now = time.monotonic_ns()
        self._sent_ns.update(dict.fromkeys(new_pairs, now))

    def close_step(self) -> None:
        if self._handle is not None:
            self._handle.close()
        self._handle = None

    def on_frame(self, frame: Frame) -> None:
        if self._handle is None or self._step is None:
            return
        self._step.frames += 1
        raw = redact(frame.raw, self._secrets())
        self._handle.write(
            json.dumps({"recv_wall_ns": frame.recv_wall_ns, "recv_mono_ns": frame.recv_mono_ns, "raw": raw}) + "\n"
        )
        self._handle.flush()
        pair = frame.pair
        if pair is not None and pair in self._sent_ns:
            key = f"{pair[0]}|{pair[1]}"
            if key not in self._step.first_frame_latency_ms:
                self._step.first_frame_latency_ms[key] = (frame.recv_mono_ns - self._sent_ns[pair]) / 1e6

    async def pump(self, io: ProbeIO) -> None:
        while True:
            try:
                frame = await io.next_frame()
            except VendorDisconnected as exc:
                self.disconnect = f"{type(exc).__name__}:{exc}"
                return
            self.on_frame(frame)


async def _hold(io: ProbeIO, recorder: _Recorder, hold_s: float) -> str | None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + hold_s
    while True:
        if recorder.disconnect is not None:
            return "disconnect"
        remaining = deadline - loop.time()
        if remaining <= 0:
            return None
        await asyncio.sleep(min(remaining, _LEASE_RENEW_EVERY_S))
        if not io.renew():
            return "lease_preempted"


async def _ramp(
    io: ProbeIO,
    pairs: list[Pair],
    ramp: Sequence[int],
    hold_s: float,
    recorder: _Recorder,
    summary: ProbeSummary,
    *,
    second_session: bool = False,
) -> None:
    accepted_total = 0
    prev_total = 0
    for total in ramp:
        if total > io.capacity:
            summary.stop_reason = "local_capacity"
            tail = "second session follows" if second_session else "second session not tried"
            _append_detail(summary, f"session capacity {io.capacity} reached; {tail}")
            break
        step = StepRecord(total_pairs=total)
        summary.steps.append(step)
        new_pairs = pairs[prev_total:total]
        prev_total = total
        recorder.start_step(step, new_pairs)
        try:
            results = await io.add(new_pairs)
        except VendorDisconnected as exc:
            step.disconnect = f"{type(exc).__name__}:{exc}"
            summary.stop_reason = "disconnect"
            summary.cap_pairs = accepted_total
            break
        first_rejected: int | None = None
        for index, (_, accepted, code) in enumerate(results):
            if accepted:
                step.accepted += 1
            else:
                step.rejected[code] = step.rejected.get(code, 0) + 1
                if first_rejected is None:
                    first_rejected = index
        if first_rejected is not None:
            summary.cap_pairs = accepted_total + first_rejected
            summary.stop_reason = "rejected"
            accepted_total += step.accepted
            break
        accepted_total += step.accepted
        summary.cap_pairs = accepted_total
        reason = await _hold(io, recorder, hold_s)
        if reason is not None:
            step.disconnect = recorder.disconnect
            summary.stop_reason = reason
            break
    recorder.close_step()


async def _hold_pair(
    first_io: ProbeIO,
    first_recorder: _Recorder,
    second_io: ProbeIO,
    second_recorder: _Recorder,
    result: dict[str, Any],
    hold_s: float,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + hold_s
    while True:
        await asyncio.sleep(max(0.0, min(deadline - loop.time(), _LEASE_RENEW_EVERY_S)))
        if second_recorder.disconnect is not None:
            result["disconnect"] = second_recorder.disconnect
            return
        if not second_io.renew():
            result["disconnect"] = "lease_preempted"
            return
        if not first_io.renew():
            first_recorder.disconnect = first_recorder.disconnect or "lease_preempted"
        if loop.time() >= deadline:
            return


async def _run_second_session(
    vendor: str,
    pairs: list[Pair],
    offset: int,
    hold_s: float,
    out_dir: pathlib.Path,
    first_io: ProbeIO,
    first_recorder: _Recorder,
    summary: ProbeSummary,
    secrets_out: list[str],
) -> None:
    """Subscribe a disjoint pair block on a second IO while the first session keeps holding; results go to summary.second_session."""
    result: dict[str, Any] = {
        "tried": True,
        "accepted": 0,
        "rejected": {},
        "disconnect": None,
        "first_session_disconnect": None,
    }
    summary.second_session_tried = True
    summary.second_session = result
    try:
        second_io = await _open_io(vendor)
    except _NoTokenError as exc:
        result["disconnect"] = f"no_token:{exc}"
        return
    recorder = _Recorder(out_dir, vendor, lambda: second_io.secrets)
    pump: asyncio.Task[None] | None = None
    try:
        await second_io.connect()
        secrets_out.extend(second_io.secrets)
        block = pairs[offset : min(len(pairs), offset + second_io.capacity, 2 * second_io.capacity)]
        step = StepRecord(total_pairs=offset + len(block))
        summary.steps.append(step)
        recorder.start_step(step, block)
        pump = asyncio.create_task(recorder.pump(second_io))
        for _, accepted, code in await second_io.add(block):
            if accepted:
                result["accepted"] += 1
                step.accepted += 1
            else:
                result["rejected"][code] = result["rejected"].get(code, 0) + 1
                step.rejected[code] = step.rejected.get(code, 0) + 1
        await _hold_pair(first_io, first_recorder, second_io, recorder, result, hold_s)
    except (_RefusedError, VendorAuthRejected, VendorDisconnected) as exc:
        result["disconnect"] = f"{type(exc).__name__}:{exc}"
    finally:
        if pump is not None:
            pump.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await pump
        recorder.close_step()
        await second_io.close()
        if first_recorder.disconnect is not None:
            result["first_session_disconnect"] = first_recorder.disconnect


async def run_probe(
    vendor: Literal["db", "toss", "ls-nxt"],
    *,
    symbols: Sequence[str],
    streams: Sequence[Literal["trade", "book"]],
    ramp: Sequence[int],
    hold_s: float,
    out_dir: pathlib.Path,
    journal_root: pathlib.Path,
    now: Callable[[], float],
    second_session: bool = False,
) -> ProbeSummary:
    """Run one ramp for a candidate vendor and return the summary; the caller maps summary.exit_code to the process exit."""
    summary = ProbeSummary(vendor=vendor, exit_code=EXIT_OK)
    try:
        if vendor not in VENDORS:
            raise ProbeUsageError(f"unknown vendor: {vendor}")
        if hold_s < 0:
            raise ProbeUsageError("hold_s must be non-negative")
        pairs = build_pairs(symbols, streams)
        validate_ramp(ramp, len(pairs))
        if _out_inside_l0(out_dir, journal_root):
            raise ProbeUsageError(f"out must be outside {journal_root}")
    except ProbeUsageError as exc:
        summary.exit_code = EXIT_USAGE
        summary.detail = str(exc)
        return summary

    if second_session and vendor == "ls-nxt":
        _append_detail(summary, "second session ignored for ls-nxt: LS keeps the third session as handover spare")
        second_session = False

    now_kst = dt.datetime.fromtimestamp(now(), _KST)
    refusal = forbidden_reason(vendor, now_kst, _session_anchors(now_kst.date()))
    if refusal is not None:
        summary.exit_code = EXIT_REFUSED
        summary.detail = refusal
        return summary

    try:
        io = await _open_io(vendor)
    except _NoTokenError as exc:
        summary.exit_code = EXIT_NO_TOKEN
        summary.detail = str(exc)
        return summary

    recorder = _Recorder(out_dir, vendor, lambda: io.secrets)
    pump: asyncio.Task[None] | None = None
    second_secrets: list[str] = []
    try:
        await io.connect()
        pump = asyncio.create_task(recorder.pump(io))
        await _ramp(io, pairs, ramp, hold_s, recorder, summary, second_session=second_session)
        if summary.stop_reason in ("rejected", "disconnect") and not summary.cap_pairs:
            summary.exit_code = EXIT_REJECTED
        elif second_session and summary.stop_reason in (None, "local_capacity"):
            offset = summary.cap_pairs or 0
            if offset < len(pairs):
                await _run_second_session(
                    vendor, pairs, offset, hold_s, out_dir, io, recorder, summary, second_secrets
                )
            else:
                _append_detail(summary, "second session not tried: no pairs left")
    except _RefusedError as exc:
        summary.exit_code = exc.exit_code
        summary.detail = str(exc)
    except (VendorAuthRejected, VendorDisconnected) as exc:
        summary.exit_code = EXIT_REJECTED
        summary.detail = f"{type(exc).__name__}:{exc}"
    finally:
        if pump is not None:
            pump.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await pump
        await io.close()
        _write_summary(out_dir, summary, io_secrets=(*io.secrets, *second_secrets))
    return summary


def _write_summary(out_dir: pathlib.Path, summary: ProbeSummary, *, io_secrets: Sequence[str]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    text = redact(json.dumps(summary.as_json(), indent=2, sort_keys=True), io_secrets)
    (out_dir / f"summary.{summary.vendor}.json").write_text(text + "\n", encoding="utf-8")


def _read_symbols(args: argparse.Namespace) -> list[str]:
    raw = pathlib.Path(args.symbols_file).read_text(encoding="utf-8") if args.symbols_file else args.symbols or ""
    symbols = [token.strip() for token in re.split(r"[,\s]+", raw) if token.strip()]
    if not symbols:
        raise ProbeUsageError("no symbols given")
    bad = [s for s in symbols if re.fullmatch(r"\d{6}", s) is None]
    if bad:
        raise ProbeUsageError(f"symbols must be 6-digit codes: {bad[:5]}")
    return symbols


def _parse_ramp(raw: str) -> list[int]:
    try:
        return [int(part) for part in raw.split(",") if part.strip()]
    except ValueError as exc:
        raise ProbeUsageError(f"ramp must be comma-separated integers: {raw}") from exc


def _clock_gate(settings: CollectorSettings) -> str | None:
    try:
        offset_ns = measure_ntp_offset_ns(settings.ntp_host)
    except ClockUnsyncedError as exc:
        return f"ntp unmeasured: {exc}"
    if abs(offset_ns) > settings.max_clock_offset_ns:
        return f"clock offset {offset_ns} ns exceeds {settings.max_clock_offset_ns} ns"
    return None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="feed_probe.py",
        description="Read-only feed capacity probe for DB, Toss and LS NXT. Never trades.",
        epilog=(
            "Run from the repo root with PYTHONPATH=. (inside the collector container: docker exec <collector> sh -c \"PYTHONPATH=. python tools/feed_probe.py ...\"), or with "
            "a one-off docker run of the deployed image that bind-mounts tools/ read-only. No deploy is needed. "
            "Exit codes: 0 completed, 2 usage, 3 no token, 4 forbidden window or lease denied, "
            "5 vendor rejected immediately, 6 clock unsynced."
        ),
    )
    parser.add_argument("--vendor", required=True, choices=VENDORS)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--symbols", help="comma-separated 6-digit codes")
    group.add_argument("--symbols-file", help="file of whitespace or comma separated 6-digit codes")
    parser.add_argument("--streams", default=",".join(STREAMS), help="trade,book (default both)")
    parser.add_argument("--ramp", default="10,25,50,100,200", help="ascending total pair counts")
    parser.add_argument("--hold-s", type=float, default=60.0, help="seconds held at each ramp step")
    parser.add_argument("--out", required=True, help="output directory, outside the L0 journal root")
    parser.add_argument(
        "--second-session",
        action="store_true",
        help="after a full or capacity-limited ramp, open a second session on disjoint pairs (DB, Toss; ignored for ls-nxt)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = CollectorSettings()
    try:
        symbols = _read_symbols(args)
        streams = tuple(s.strip() for s in args.streams.split(",") if s.strip())
        ramp = _parse_ramp(args.ramp)
        out_dir = pathlib.Path(args.out)
        if _out_inside_l0(out_dir, settings.paths.journal_root):
            raise ProbeUsageError(f"out must be outside {settings.paths.journal_root}")
        build_pairs(symbols, streams)
    except (ProbeUsageError, OSError) as exc:
        print(json.dumps({"error": str(exc)}))  # noqa: T201 - probe result goes to stdout
        return EXIT_USAGE

    clock_problem = _clock_gate(settings)
    if clock_problem is not None:
        print(json.dumps({"exit_code": EXIT_CLOCK, "detail": clock_problem}))  # noqa: T201
        return EXIT_CLOCK

    summary = asyncio.run(
        run_probe(
            args.vendor,
            symbols=symbols,
            streams=streams,  # type: ignore[arg-type]
            ramp=ramp,
            hold_s=args.hold_s,
            out_dir=out_dir,
            journal_root=settings.paths.journal_root,
            now=time.time,
            second_session=args.second_session,
        )
    )
    print(json.dumps(summary.as_json(), sort_keys=True))  # noqa: T201 - probe result goes to stdout
    return summary.exit_code


if __name__ == "__main__":
    sys.exit(main())
