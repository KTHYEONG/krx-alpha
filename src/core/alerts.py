"""CRITICAL email alerts and digest rendering with bounded sending."""

import contextlib
import datetime as dt
import fcntl
import json
import logging
import math
import os
import pathlib
import re
import smtplib
import tempfile
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Literal, Protocol
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field

from src.core.config import AlertSettings
from src.core.log_format import format_record_timestamp

ALERT_COOLDOWN_S: float = 1800.0
ALERT_DAILY_CAP: int = 20
# One mail per failure kind per KST day: repeats add no signal (they stay in events jsonl and the dashboard).
ALERT_PER_KEY_DAILY_CAP: int = 1
ALERT_FIRST_OCCURRENCE_RESERVE: int = 5
ALERT_SEND_ATTEMPTS: int = 3
ALERT_RETRY_BACKOFF_S: tuple[float, ...] = (2.0, 8.0)
ALERT_LINE_MAX_CHARS: int = 300
# Host-relative (~/krx-alpha) path of the persistent events log the card points the reader to.
ALERT_EVENTS_LOG_TEMPLATE: str = "data/logs/events-{component}.jsonl"
DIGEST_STATUS_OK: str = "OK"

_KST = ZoneInfo("Asia/Seoul")
_KV_RE = re.compile(r"(\w+)=(\S+)")
_NUMERIC_RE = re.compile(r"(?<![A-Za-z0-9_])[0-9][0-9\.,/:\-]*(?![A-Za-z0-9_])")
_ALERT_LEDGER_SCHEMA_VERSION = 2
_DEGRADED_PID: int | None = None
_DEGRADED_LOCK = threading.Lock()


def alert_dedup_key(message: str) -> str:
    """Return the stable dedup key of a CRITICAL message.

    Numeric literals vary between occurrences of one failure kind (row counts, ratios,
    dates, ids) and must not split its budget. A numeric literal is a maximal run that starts
    with a digit, is not immediately preceded by a letter, digit or underscore, and continues
    over digits and the characters ``. , / : -``; each is replaced by ``#``. Identifiers that
    embed digits (``H0STCNT0``, ``0035S0``) are preserved so distinct streams keep distinct keys.
    The result is truncated to 200 characters after substitution.
    """
    return _NUMERIC_RE.sub("#", message)[:200]


@dataclass(frozen=True)
class AlertLimits:
    cooldown_s: float = ALERT_COOLDOWN_S
    daily_cap: int = ALERT_DAILY_CAP
    per_key_daily_cap: int = ALERT_PER_KEY_DAILY_CAP
    first_occurrence_reserve: int = ALERT_FIRST_OCCURRENCE_RESERVE

    def __post_init__(self) -> None:
        if self.per_key_daily_cap < 1:
            raise ValueError("per_key_daily_cap must be >= 1")
        if not 0 <= self.first_occurrence_reserve < self.daily_cap:
            raise ValueError("first_occurrence_reserve must be in [0, daily_cap)")


@dataclass(frozen=True)
class AlertDecision:
    send: bool
    suppressed_reason: str | None
    previous_last_sent_s: float | None
    reservation_id: str | None = None


class AlertLedger(Protocol):
    def reserve(self, key: str, *, day: dt.date, now_s: float, limits: AlertLimits) -> AlertDecision: ...
    def commit(self, key: str, *, day: dt.date, reservation_id: str | None) -> None: ...
    def release(
        self, key: str, *, day: dt.date, previous_last_sent_s: float | None, reservation_id: str | None = None
    ) -> None: ...


class _LedgerModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


class _KeyBudget(_LedgerModel):
    count: int = Field(default=0, ge=0)
    last_sent_s: float | None = None
    confirmed_count: int = Field(default=0, ge=0)
    last_success_s: float | None = None


class _Reservation(_LedgerModel):
    key: str
    now_s: float
    owner_pid: int = Field(gt=0)
    owner_start: str = Field(min_length=1)


class _LedgerState(_LedgerModel):
    version: Literal[2] = 2
    day: str
    sent_today: int = Field(default=0, ge=0)
    keys: dict[str, _KeyBudget] = Field(default_factory=dict)
    reservations: dict[str, _Reservation] = Field(default_factory=dict)

    def refresh_key(self, key: str) -> None:
        budget = self.keys[key]
        pending = [r.now_s for r in self.reservations.values() if r.key == key]
        budget.count = budget.confirmed_count + len(pending)
        if budget.last_success_s is not None:
            pending.append(budget.last_success_s)
        budget.last_sent_s = max(pending) if pending else None
        if budget.count == 0:
            del self.keys[key]
        self.sent_today = sum(b.count for b in self.keys.values())

    def finish(self, key: str, reservation_id: str, *, success: bool) -> bool:
        reservation = self.reservations.get(reservation_id)
        if reservation is None or reservation.key != key:
            return False
        del self.reservations[reservation_id]
        budget = self.keys[key]
        if success:
            budget.confirmed_count += 1
            budget.last_success_s = max(
                reservation.now_s, budget.last_success_s if budget.last_success_s is not None else reservation.now_s
            )
        self.refresh_key(key)
        return True


def _reserve_slot(
    state: _LedgerState, key: str, *, now_s: float, limits: AlertLimits, owner_start: str
) -> AlertDecision:
    budget = state.keys.get(key, _KeyBudget())
    last = budget.last_sent_s
    reason = _decide_suppression(state.sent_today, budget.count, last, now_s, limits)
    if reason is not None:
        return AlertDecision(False, reason, last)
    reservation_id = uuid.uuid4().hex
    state.keys[key] = budget
    state.reservations[reservation_id] = _Reservation(
        key=key, now_s=now_s, owner_pid=os.getpid(), owner_start=owner_start
    )
    state.refresh_key(key)
    return AlertDecision(True, None, last, reservation_id)


def _process_identity(pid: int) -> tuple[str, str]:
    # Linux start ticks distinguish PID reuse; zombies cannot complete an SMTP send.
    fields = pathlib.Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
    return fields[0], fields[19]


def _reap_abandoned(state: _LedgerState) -> bool:
    changed = False
    owners: dict[tuple[int, str], bool] = {}
    for reservation_id, reservation in list(state.reservations.items()):
        owner = (reservation.owner_pid, reservation.owner_start)
        if owner not in owners:
            try:
                status, start = _process_identity(reservation.owner_pid)
                owners[owner] = status != "Z" and start == reservation.owner_start
            except FileNotFoundError:
                owners[owner] = False
        if not owners[owner]:
            state.finish(reservation.key, reservation_id, success=False)
            changed = True
    return changed


def _decide_suppression(
    sent_today: int, key_count: int, last: float | None, now_s: float, limits: AlertLimits
) -> str | None:
    if sent_today >= limits.daily_cap:
        return "daily_cap"
    if key_count >= limits.per_key_daily_cap:
        return "per_key_cap"
    if key_count > 0 and sent_today >= limits.daily_cap - limits.first_occurrence_reserve:
        return "repeat_budget"
    if last is not None and (now_s - last) < limits.cooldown_s:
        return "cooldown"
    return None


class InMemoryAlertLedger:
    """Process-local ledger; reproduces the pre-existing per-process budget semantics."""

    def __init__(self) -> None:
        self._state: _LedgerState | None = None
        self._lock = threading.Lock()

    @property
    def _sent_today(self) -> int:
        return self._state.sent_today if self._state is not None else 0

    @property
    def _per_key_counts(self) -> dict[str, int]:
        return {k: b.count for k, b in self._state.keys.items()} if self._state is not None else {}

    def reserve(self, key: str, *, day: dt.date, now_s: float, limits: AlertLimits) -> AlertDecision:
        with self._lock:
            if self._state is None or self._state.day != day.isoformat():
                self._state = _LedgerState(day=day.isoformat())
            return _reserve_slot(self._state, key, now_s=now_s, limits=limits, owner_start="memory")

    def commit(self, key: str, *, day: dt.date, reservation_id: str | None) -> None:
        self._finish(key, day=day, reservation_id=reservation_id, success=True)

    def release(
        self, key: str, *, day: dt.date, previous_last_sent_s: float | None, reservation_id: str | None = None
    ) -> None:
        """Release only the identified reservation; missing IDs never change a budget."""
        self._finish(key, day=day, reservation_id=reservation_id, success=False)

    def _finish(self, key: str, *, day: dt.date, reservation_id: str | None, success: bool) -> None:
        with self._lock:
            if reservation_id is not None and self._state is not None and self._state.day == day.isoformat():
                self._state.finish(key, reservation_id, success=success)


class FileAlertLedger:
    """JSON ledger shared by every process of one deployment.

    ``reserve`` is an atomic read-modify-write under an exclusive advisory lock, so
    concurrent processes can never jointly exceed a limit. ``now_s`` MUST be wall-clock
    epoch seconds (monotonic clocks are not comparable across processes). The ledger
    degrades open: it must never silence a CRITICAL because its own storage failed.

    Successful sends must call ``commit``; failed sends must call ``release`` with
    the returned reservation ID. Pending slots belonging to exited processes are
    reclaimed on the next reserve. SMTP delivery and JSON persistence cannot be
    committed atomically: a crash after delivery but before commit is ambiguous.
    """

    def __init__(self, path: pathlib.Path, *, lock_timeout_s: float = 2.0) -> None:
        if not math.isfinite(lock_timeout_s) or lock_timeout_s < 0:
            raise ValueError("lock_timeout_s must be finite and >= 0")
        self._path = pathlib.Path(path)
        self._lock_timeout_s = lock_timeout_s
        self._lock_path = pathlib.Path(str(self._path) + ".lock")

    def _note_degraded(self, exc: BaseException) -> None:
        global _DEGRADED_PID
        pid = os.getpid()
        with _DEGRADED_LOCK:
            if pid == _DEGRADED_PID:
                return
            _DEGRADED_PID = pid
        logging.getLogger(__name__).warning(
            "[SYS] stage=alert_ledger status=DEGRADED reason=%s", type(exc).__name__
        )

    @contextlib.contextmanager
    def _locked(self) -> Iterator[None]:
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock_path.open("a") as lock_file:
            deadline = time.monotonic() + self._lock_timeout_s
            while True:
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError("alert ledger lock timeout") from None
                    time.sleep(min(0.02, remaining))
            yield

    def _load_unlocked(self, day: dt.date) -> _LedgerState:
        try:
            raw: object = json.loads(self._path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return _LedgerState(day=day.isoformat())
        except (ValueError, UnicodeDecodeError) as exc:
            self._note_degraded(exc)
            return _LedgerState(day=day.isoformat())
        try:
            if isinstance(raw, dict) and type(raw.get("version")) is int and raw.get("version") == 1 and isinstance(raw.get("keys"), dict):
                raw["version"] = _ALERT_LEDGER_SCHEMA_VERSION
                for value in raw["keys"].values():
                    if isinstance(value, dict):
                        value.update(confirmed_count=value.get("count"), last_success_s=value.get("last_sent_s"))
            state = _LedgerState.model_validate(raw)
            dt.date.fromisoformat(state.day)
            if any(r.key not in state.keys for r in state.reservations.values()):
                raise ValueError("orphan reservation")
            for key, budget in list(state.keys.items()):
                if (budget.confirmed_count == 0) != (budget.last_success_s is None):
                    raise ValueError("invalid confirmed timestamp")
                count, last = budget.count, budget.last_sent_s
                state.refresh_key(key)
                if budget.count != count or budget.last_sent_s != last:
                    raise ValueError("inconsistent key budget")
            state.sent_today = sum(b.count for b in state.keys.values())
            if not isinstance(raw, dict) or state.sent_today != raw.get("sent_today"):
                raise ValueError("inconsistent daily budget")
        except ValueError as exc:
            self._note_degraded(exc)
            return _LedgerState(day=day.isoformat())
        return state

    def _save_unlocked(self, state: _LedgerState) -> None:
        parent = self._path.parent
        parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=str(parent), prefix=self._path.name + ".tmp.")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as tmp:
                tmp.write(state.model_dump_json())
                tmp.flush()
                os.fsync(tmp.fileno())
            os.replace(tmp_name, self._path)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp_name)
            raise

    def reserve(self, key: str, *, day: dt.date, now_s: float, limits: AlertLimits) -> AlertDecision:
        try:
            with self._locked():
                state = self._load_unlocked(day)
                if state.day != day.isoformat():
                    state = _LedgerState(day=day.isoformat())
                reaped = _reap_abandoned(state)
                _, owner_start = _process_identity(os.getpid())
                decision = _reserve_slot(state, key, now_s=now_s, limits=limits, owner_start=owner_start)
                if decision.send or reaped:
                    self._save_unlocked(state)
                return decision
        except (OSError, ValueError) as exc:
            self._note_degraded(exc)
            return AlertDecision(True, None, None)

    def commit(self, key: str, *, day: dt.date, reservation_id: str | None) -> None:
        self._finish(key, day=day, reservation_id=reservation_id, success=True)

    def release(
        self, key: str, *, day: dt.date, previous_last_sent_s: float | None, reservation_id: str | None = None
    ) -> None:
        """Release only the identified reservation; previous timestamps are informational."""
        self._finish(key, day=day, reservation_id=reservation_id, success=False)

    def _finish(self, key: str, *, day: dt.date, reservation_id: str | None, success: bool) -> None:
        if reservation_id is None:
            return
        try:
            with self._locked():
                state = self._load_unlocked(day)
                if state.day == day.isoformat() and state.finish(key, reservation_id, success=success):
                    self._save_unlocked(state)
        except OSError as exc:
            self._note_degraded(exc)

_STAGE_LABELS: dict[str, str] = {
    "backup_freshness": "백업점검",
    "eod_offload": "백업전송",
    "eod_maintenance": "마감정리",
    "eod_reconciliation": "정합성검증",
    "eod_readiness": "마감준비검증",
    "streamer": "시세수집",
    "stream_flush": "시세저장",
    "stream_outage": "시세단절장애",
    "snapshots": "스냅샷수집",
    "journal_flush": "저널저장",
    "ingest_watchdog": "수집감시",
    "ingest_latency": "수집지연",
    "ingest_lag": "실시간수집지연",
    "open_delay": "개장지연",
    "session_audit": "세션감사",
    "session_reconciliation": "세션정합성",
    "rclone_offload": "원격저장",
    "prune": "데이터정리",
    "quarantine": "데이터격리",
    "order": "주문실행",
    "oms": "주문실행",
    "session": "세션관리",
    "orchestration": "데몬관리",
    "aftermarket_reselection": "애프터마켓종목선정",
    "aftermarket_plan": "애프터마켓수집계획",
    "bootstrap": "초기화",
    "unknown_ambiguous": "주문매핑모호",
    "integrity_violation": "체결정합성위반",
    "shard_watchdog": "샤드감시",
}

_REASON_LABELS: dict[str, str] = {
    "auth_expired": "GDrive 인증 만료",
    "circuit_open": "소켓 단절 (서킷오픈)",
    "remote_error": "원격 스토리지 오류",
    "size_mismatch": "파일 크기 불일치",
    "unverified": "검증 실패",
    "stale": "수집 지연",
    "worker_crash": "프로세스 비정상 종료",
    "no_bars_store": "일봉 스토어 없음",
    "rclone_settings_missing": "rclone 설정 누락",
    "maintenance_error": "정리 작업 오류",
    "stale_bars_calendar_unknown": "일봉 지연 (캘린더 불명)",
    "stale_bars_fallback_failed": "일봉 KIS 대체 갱신 실패",
    "orchestration_error": "데몬 오케스트레이션 오류",
    "candidates_not_ready": "수집 대상 종목 준비 실패",
    "orchestration_failed": "오케스트레이션 실패 (임시 가동)",
    "aftermarket_not_ready": "애프터마켓 수집 마감 미완료",
    "session_data_gap": "세션 데이터 누락 감지",
    "latency_exceeded": "전달 지연 임계 초과",
    "live_lag_exceeded": "실시간 전달 지연 임계 초과",
    "open_delay_suspected": "개장 지연 의심 (첫 체결 미수신)",
    "trade_loss_exceeded": "거래 손실 임계 초과",
    "auth_rejected": "증권사 인증 거부",
    "ntp_unmeasured": "NTP 시간 동기화 측정 실패",
    "quarantine_exists": "격리 대상 중복",
    "stale_budget_exhausted": "스태일 재시작 예산 소진",
    "venue_wide_silence": "베뉴 전체 침묵",
}


def format_alert_subject(
    component: str,
    msg: str,
    fields: dict[str, str],
    *,
    bot_name: str = "krx-alpha",
) -> str:
    """Build a labeled alert subject from stage/reason fields."""
    stage = fields.get("stage", "")
    reason = fields.get("reason", "")
    stage_label = _STAGE_LABELS.get(stage, stage)
    reason_label = _REASON_LABELS.get(reason, reason)

    if stage_label and reason_label:
        core = f"[{stage_label}] {reason_label}"
    elif stage_label:
        status = fields.get("status", "FAIL")
        core = f"[{stage_label}] {status}"
    elif reason_label:
        core = f"[{component}] {reason_label}"
    else:
        clean = re.sub(r"\[\w+\]\s*", "", msg).strip()
        core = f"[{component}] {clean[:40]}"

    return f"[{bot_name}][실패] {core}"


def _one_line(text: str) -> str:
    flat = " ".join(text.split())
    return flat if len(flat) <= ALERT_LINE_MAX_CHARS else flat[: ALERT_LINE_MAX_CHARS - 1] + "…"


def format_alert_text(
    record: logging.LogRecord,
    component: str,
    run_id: str,
    fields: dict[str, str],
    *,
    bot_name: str = "krx-alpha",
) -> str:
    """Render the short CRITICAL alert card; the full record stays in the component's events jsonl."""
    stage = fields.get("stage", "")
    stage_label = _STAGE_LABELS.get(stage, stage) or "일반"
    status = fields.get("status", "FAIL")
    reason = fields.get("reason", "")
    reason_label = _REASON_LABELS.get(reason, reason)
    clean_msg = re.sub(r"\[\w+\]\s*", "", record.getMessage()).strip()
    cause = f"{reason_label}: {clean_msg}" if reason_label else clean_msg
    return "\n".join(
        [
            f"• 무엇: {component} > {stage_label} {status}",
            f"• 언제: {format_record_timestamp(record)} (KST)",
            f"• 원인: {_one_line(cause) or '알 수 없음'}",
            f"• 증거: {ALERT_EVENTS_LOG_TEMPLATE.format(component=component)} (run_id={run_id})",
        ]
    )


def _digest_fields(body: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in body.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            fields[k.strip()] = v.strip()
    return fields


def format_digest_text(subject: str, body: str, *, bot_name: str = "krx-alpha") -> str:
    """Render the short EOD warning card from the key=value digest; the full digest is archived on disk."""
    fields = _digest_fields(body)
    what = subject.removeprefix(f"[{bot_name}] ").strip() or "EOD"
    lines = [
        f"• 무엇: {what}",
        f"• 언제: {dt.datetime.now(_KST).strftime('%Y-%m-%d %H:%M')} (KST)",
        f"• 원인: {_one_line(fields.get('degraded_reasons', '')) or '알 수 없음'}",
    ]
    if fields.get("digest_path"):
        lines.append(f"• 증거: {fields['digest_path']} (run_id={fields.get('run_id', '-')})")
    return "\n".join(lines)


class EmailAlertHandler(logging.Handler):
    """Send bounded CRITICAL alerts with per-day, per-key and cooldown limits.

    A single failure that keeps re-alerting (e.g. a restart circuit re-opening every window)
    must not exhaust the daily budget and hide unrelated CRITICAL events raised later the same
    day. Each alert key may be mailed at most ``per_key_daily_cap`` times per day, and repeat
    sends of an already-mailed key may only use ``daily_cap - first_occurrence_reserve`` slots,
    so at least ``first_occurrence_reserve`` slots always remain for keys first seen that day.
    All counters reset on the KST day rollover. Suppressed alerts stay in the regular logs;
    the handler only limits email delivery. Sender failures remain logged without recursively
    escalating the same alert.

    Limits are enforced by an ``AlertLedger``; pass a shared ``FileAlertLedger`` so child
    processes draw from one budget.

    Raises:
        ValueError: ``per_key_daily_cap < 1`` or ``first_occurrence_reserve`` outside
            ``[0, daily_cap)``.
    """

    def __init__(
        self,
        *,
        component: str,
        run_id: str,
        sender: Callable[..., None],
        cooldown_s: float = ALERT_COOLDOWN_S,
        daily_cap: int = ALERT_DAILY_CAP,
        per_key_daily_cap: int = ALERT_PER_KEY_DAILY_CAP,
        first_occurrence_reserve: int = ALERT_FIRST_OCCURRENCE_RESERVE,
        clock: Callable[[], float] = time.monotonic,
        today: Callable[[], dt.date] | None = None,
        send_attempts: int = ALERT_SEND_ATTEMPTS,
        retry_backoff_s: tuple[float, ...] = ALERT_RETRY_BACKOFF_S,
        sleep: Callable[[float], None] = time.sleep,
        ledger: AlertLedger | None = None,
    ) -> None:
        if per_key_daily_cap < 1:
            raise ValueError("per_key_daily_cap must be >= 1")
        if not 0 <= first_occurrence_reserve < daily_cap:
            raise ValueError("first_occurrence_reserve must be in [0, daily_cap)")
        super().__init__(level=logging.CRITICAL)
        self._component = component
        self._run_id = run_id
        self._sender = sender
        self._cooldown_s = cooldown_s
        self._daily_cap = daily_cap
        self._per_key_daily_cap = per_key_daily_cap
        self._first_occurrence_reserve = first_occurrence_reserve
        self._clock = time.time if isinstance(ledger, FileAlertLedger) and clock is time.monotonic else clock
        self._today = today if today is not None else (lambda: dt.datetime.now(_KST).date())
        self._send_attempts = send_attempts
        self._retry_backoff_s = retry_backoff_s
        self._sleep = sleep
        self._limits = AlertLimits(
            cooldown_s=cooldown_s,
            daily_cap=daily_cap,
            per_key_daily_cap=per_key_daily_cap,
            first_occurrence_reserve=first_occurrence_reserve,
        )
        self._ledger: AlertLedger = ledger if ledger is not None else InMemoryAlertLedger()
        self._current_day: dt.date | None = None
        self._suppressed_logged: set[str] = set()

    @property
    def _sent_today(self) -> int:
        return self._ledger._sent_today if isinstance(self._ledger, InMemoryAlertLedger) else 0

    @property
    def _per_key_counts(self) -> dict[str, int]:
        return self._ledger._per_key_counts if isinstance(self._ledger, InMemoryAlertLedger) else {}

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._emit_guarded(record)
        except Exception as exc:
            logging.getLogger(__name__).warning("[SYS] stage=alert status=FAIL reason=%s", type(exc).__name__)

    def _emit_guarded(self, record: logging.LogRecord) -> None:
        key = alert_dedup_key(record.getMessage())
        day = self._today()
        if self._current_day is None or day != self._current_day:
            self._current_day = day
            self._suppressed_logged.clear()
        now = self._clock()
        decision = self._ledger.reserve(key, day=day, now_s=now, limits=self._limits)
        if not decision.send:
            if decision.suppressed_reason in ("per_key_cap", "repeat_budget"):
                self._log_suppressed_once(key, str(decision.suppressed_reason))
            return
        try:
            msg = record.getMessage()
            fields = dict(_KV_RE.findall(msg))
            subject = format_alert_subject(self._component, msg, fields)
            text_body = format_alert_text(record, self._component, self._run_id, fields)
            attempts = max(1, self._send_attempts)
            for attempt in range(attempts):
                try:
                    self._sender(subject, text_body)
                    break
                except (smtplib.SMTPException, OSError):
                    if attempt + 1 >= attempts:
                        raise
                    backoff = self._retry_backoff_s
                    self._sleep(backoff[attempt] if attempt < len(backoff) else backoff[-1])
        except Exception:
            self._ledger.release(
                key, day=day, previous_last_sent_s=decision.previous_last_sent_s,
                reservation_id=decision.reservation_id,
            )
            raise
        self._ledger.commit(key, day=day, reservation_id=decision.reservation_id)

    def _log_suppressed_once(self, key: str, reason: str) -> None:
        if key in self._suppressed_logged:
            return
        self._suppressed_logged.add(key)
        logging.getLogger(__name__).warning(
            "[SYS] stage=alert status=SUPPRESSED reason=%s key=%r", reason, key
        )


def gmail_sender(settings: AlertSettings) -> Callable[..., None]:
    """Build a Gmail SMTP sender bound to the given alert settings."""

    def send(subject: str, body: str) -> None:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = settings.alert_gmail_user
        msg["To"] = settings.alert_gmail_to
        msg.set_content(body)
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as smtp:
            smtp.login(settings.alert_gmail_user, settings.alert_gmail_app_password)
            smtp.send_message(msg)

    return send


def send_digest(
    subject: str,
    body: str,
    *,
    settings: AlertSettings | None = None,
    sender: Callable[..., None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    bot_name: str = "krx-alpha",
) -> bool:
    """Mail the EOD digest only when its ``status`` is not OK; returns whether a mail was sent.

    A healthy EOD is visible on the dashboard and in the archived digest, so it never mails.
    """
    s = settings if settings is not None else AlertSettings()
    sys_logger = logging.getLogger(__name__)
    if _digest_fields(body).get("status") == DIGEST_STATUS_OK:
        sys_logger.info("[SYS] stage=digest status=SKIPPED reason=status_ok")
        return False
    if not s.enabled:
        sys_logger.info("[SYS] stage=digest status=DISABLED")
        return False
    mail_subject = f"[{bot_name}][경고] {subject.removeprefix(f'[{bot_name}] ')}"
    text_body = format_digest_text(subject, body, bot_name=bot_name)
    actual_sender = sender or gmail_sender(s)
    attempts = ALERT_SEND_ATTEMPTS
    for attempt in range(attempts):
        try:
            actual_sender(mail_subject, text_body)
            sys_logger.info("[SYS] stage=digest status=SENT")
            return True
        except (smtplib.SMTPException, OSError) as exc:
            if attempt + 1 >= attempts:
                sys_logger.warning("[SYS] stage=digest status=FAIL reason=%s", type(exc).__name__)
                return False
            sleep(ALERT_RETRY_BACKOFF_S[attempt] if attempt < len(ALERT_RETRY_BACKOFF_S) else ALERT_RETRY_BACKOFF_S[-1])
    return False  # pragma: no cover - loop above always returns; satisfies the type checker


__all__ = [
    "ALERT_COOLDOWN_S",
    "ALERT_DAILY_CAP",
    "ALERT_FIRST_OCCURRENCE_RESERVE",
    "ALERT_PER_KEY_DAILY_CAP",
    "ALERT_RETRY_BACKOFF_S",
    "ALERT_SEND_ATTEMPTS",
    "AlertDecision",
    "AlertLedger",
    "AlertLimits",
    "EmailAlertHandler",
    "FileAlertLedger",
    "InMemoryAlertLedger",
    "alert_dedup_key",
    "format_alert_subject",
    "format_alert_text",
    "format_digest_text",
    "gmail_sender",
    "send_digest",
]
