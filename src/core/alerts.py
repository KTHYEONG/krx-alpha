"""Operator page emails: explicit opt-in pages only, bounded by a shared dedup ledger."""

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
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from email.message import EmailMessage
from enum import StrEnum
from types import MappingProxyType
from typing import Literal, Protocol
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field

from src.core.config import AlertSettings

ALERT_COOLDOWN_S: float = 1800.0
ALERT_DAILY_CAP: int = 20
# One mail per page key per KST day: repeats add no signal (they stay in events jsonl and the dashboard).
ALERT_PER_KEY_DAILY_CAP: int = 1
ALERT_FIRST_OCCURRENCE_RESERVE: int = 5
ALERT_SEND_ATTEMPTS: int = 3
ALERT_RETRY_BACKOFF_S: tuple[float, ...] = (2.0, 8.0)

_KST = ZoneInfo("Asia/Seoul")
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

class PageKind(StrEnum):
    """Incidents that need a human now; every other CRITICAL stays log-only."""

    REGULAR_FEED_DOWN = "regular_feed_down"
    REGULAR_SHARD_STALE = "regular_shard_stale"
    NO_FIRST_FRAME = "no_first_frame"
    STREAM_AUTH_REJECTED = "stream_auth_rejected"
    STREAM_OUTAGE = "stream_outage"
    JOURNAL_WRITE_FAIL = "journal_write_fail"
    DISK_LOW = "disk_low"
    FEED_COVERAGE_HOLE = "feed_coverage_hole"
    KIS_PREFLIGHT_FAIL = "kis_preflight_fail"
    STORE_MIGRATION = "store_migration"
    OFFLOAD_AUTH_EXPIRED = "offload_auth_expired"
    OFFLOAD_SETTINGS_MISSING = "offload_settings_missing"
    REPAIR_EXHAUSTED = "repair_exhausted"
    REPAIR_HOLD_EXPIRING = "repair_hold_expiring"
    DAEMON_CRASH_LOOP = "daemon_crash_loop"
    BACKUP_STALE = "backup_stale"
    AFTERMARKET_PLAN_MISSING = "aftermarket_plan_missing"


@dataclass(frozen=True)
class PageText:
    summary: str
    auto_action: str
    operator_action: str


PAGE_TEXTS: Mapping[PageKind, PageText] = MappingProxyType(
    {
        PageKind.REGULAR_FEED_DOWN: PageText(
            "정규장 시세 수집 프로세스 중단",
            "재시작을 한도까지 시도했으나 복구되지 않아 재시작을 멈춤",
            "지금 VPS에서 데몬 로그를 확인하고 원인 해소 후 데몬 재기동 (장 마감 전)",
        ),
        PageKind.REGULAR_SHARD_STALE: PageText(
            "정규장 KIS 샤드 시세 정지",
            "정지 샤드 재시작을 오늘 한도까지 시도했으나 계속 정지",
            "지금 KIS 데이터 키와 웹소켓 상태를 확인하고 데몬 재기동 (장 마감 전)",
        ),
        PageKind.NO_FIRST_FRAME: PageText(
            "정규장 개장 후 첫 시세 미수신",
            "수집 프로세스는 재접속을 계속하나 원본이 저장되지 않음",
            "지금 증권사 접속 상태와 수집 로그를 확인 (장 마감 전)",
        ),
        PageKind.AFTERMARKET_PLAN_MISSING: PageText(
            "애프터마켓 수집 대상 선정 실패 (시세 미수집)",
            "선정을 계속 재시도 중이며 성공하는 즉시 해당 장 수집을 시작함",
            "지금 KIS 키와 랭킹 API 상태를 확인 (애프터마켓 종료 전)",
        ),
        PageKind.STREAM_AUTH_REJECTED: PageText(
            "증권사 시세 인증 거부",
            "인증 백오프로 재접속을 계속 시도 중",
            "지금 증권사 앱키·토큰 상태를 확인하고 필요 시 재발급 (장 마감 전)",
        ),
        PageKind.STREAM_OUTAGE: PageText(
            "정규장 시세 연결 장기 단절",
            "자동 재접속을 반복 중이며 공백 구간은 기록됨",
            "지금 증권사 장애 공지와 VPS 네트워크를 확인 (장 마감 전)",
        ),
        PageKind.JOURNAL_WRITE_FAIL: PageText(
            "시세 원본 저장 실패 (유실 위험)",
            "해당 수집 프로세스를 종료하고 재시작에 맡김",
            "지금 VPS 디스크 용량과 쓰기 권한을 확인",
        ),
        PageKind.DISK_LOW: PageText(
            "디스크 여유 공간 부족",
            "정규화를 건너뛰고 원격 검증된 원본만 정리",
            "다음 장 시작(08:10) 전에 디스크 공간 확보",
        ),
        PageKind.FEED_COVERAGE_HOLE: PageText(
            "정규장 일부 종목 수집 공백",
            "없음 (LS·KIS 어디에도 배정되지 않은 종목은 수집되지 않음)",
            "지금 KIS 데이터 키 슬롯 설정을 확인하고 데몬 재기동",
        ),
        PageKind.KIS_PREFLIGHT_FAIL: PageText(
            "KIS 주 키 토큰 발급 실패",
            "LS 수집은 계속하나 KIS 의존 수집·보정은 중단",
            "지금 KIS 앱키 상태를 확인하고 재발급 (장 시작 전)",
        ),
        PageKind.STORE_MIGRATION: PageText(
            "일봉 저장소 이관 실패로 세션 준비 중단",
            "세션 준비를 중단(fail-closed)하고 대기",
            "지금 레거시 저장소 파일을 점검하고 데몬 재기동 (장 시작 전)",
        ),
        PageKind.OFFLOAD_AUTH_EXPIRED: PageText(
            "원격 백업 인증 만료",
            "원격 검증 전까지 원본 삭제를 보류",
            "오늘 안에 rclone config reconnect gdrive: 실행",
        ),
        PageKind.OFFLOAD_SETTINGS_MISSING: PageText(
            "원격 백업 설정 누락",
            "원격 검증 전까지 원본 삭제를 보류",
            "오늘 안에 rclone 원격 설정을 복구하고 데몬 재기동",
        ),
        PageKind.REPAIR_EXHAUSTED: PageText(
            "누락 체결 자동 복구 재시도 소진",
            "원본 삭제를 보류(hold)하고 매일 밤 재시도",
            "원본 보존 만료 전에 수동 복구(repair-ticks) 실행",
        ),
        PageKind.REPAIR_HOLD_EXPIRING: PageText(
            "미복구 원본 보존 만료 임박 (곧 영구 삭제)",
            "보존 연장 한도에 도달하면 원본이 삭제됨",
            "만료 전에 수동 복구 실행 또는 보존 연장 설정 상향",
        ),
        PageKind.DAEMON_CRASH_LOOP: PageText(
            "수집 데몬 반복 비정상 종료",
            "Docker가 매번 자동 재시작 중",
            "지금 데몬 로그에서 크래시 원인을 확인 (장중이면 즉시)",
        ),
        PageKind.BACKUP_STALE: PageText(
            "백업 2일 이상 실패",
            "매일 예정된 백업을 계속 재시도",
            "오늘 안에 백업 작업 로그를 확인하고 수동 백업 실행",
        ),
    }
)

PAGE_ATTR: str = "krx_page"
PAGE_DEADLINE_ATTR: str = "krx_page_deadline"
RESOLVE_ATTR: str = "krx_resolve"
PAGE_BOT_NAME: str = "krx"


def page(kind: PageKind, *, deadline: dt.date | None = None) -> dict[str, object]:
    """Return logging ``extra`` marking a CRITICAL record as an operator page (the only records that mail)."""
    extra: dict[str, object] = {PAGE_ATTR: kind}
    if deadline is not None:
        extra[PAGE_DEADLINE_ATTR] = deadline
    return extra


def page_resolved(kind: PageKind) -> dict[str, object]:
    """Return logging ``extra`` clearing ``kind``; mails one line only if this process paged ``kind`` earlier."""
    return {RESOLVE_ATTR: kind}


def page_kind(record: logging.LogRecord) -> PageKind | None:
    kind = getattr(record, PAGE_ATTR, None)
    return kind if isinstance(kind, PageKind) else None


def resolved_kind(record: logging.LogRecord) -> PageKind | None:
    kind = getattr(record, RESOLVE_ATTR, None)
    return kind if isinstance(kind, PageKind) else None


def is_alert_record(record: logging.LogRecord) -> bool:
    """Return whether the record is a page or a page clearance; every other record never mails."""
    return page_kind(record) is not None or resolved_kind(record) is not None


def _kst_clock(record: logging.LogRecord) -> str:
    return dt.datetime.fromtimestamp(record.created, tz=_KST).strftime("%m-%d %H:%M")


def format_page_subject(kind: PageKind, *, bot_name: str = PAGE_BOT_NAME) -> str:
    return f"[긴급][{bot_name}] {PAGE_TEXTS[kind].summary}"


def format_page_body(kind: PageKind, record: logging.LogRecord) -> str:
    """Render the three-line Korean page body; it never carries the raw message, paths or run ids."""
    text = PAGE_TEXTS[kind]
    deadline = getattr(record, PAGE_DEADLINE_ATTR, None)
    action = text.operator_action
    if isinstance(deadline, dt.date):
        action = f"{action} (기한: {deadline.isoformat()})"
    return "\n".join(
        [
            f"무슨 일: {text.summary} ({_kst_clock(record)} KST)",
            f"자동 조치: {text.auto_action}",
            f"지금 할 일: {action}",
        ]
    )


def format_resolved_subject(kind: PageKind, *, bot_name: str = PAGE_BOT_NAME) -> str:
    return f"[해결됨][{bot_name}] {PAGE_TEXTS[kind].summary}"


def format_resolved_body(kind: PageKind, record: logging.LogRecord) -> str:
    return f"해결됨: {PAGE_TEXTS[kind].summary} 상태가 복구됨 ({_kst_clock(record)} KST)"


class EmailAlertHandler(logging.Handler):
    """Mail operator pages (records marked by ``page``) under per-day, per-key and cooldown limits.

    Records without a page or clearance marker are ignored, whatever their level. A clearance
    (``page_resolved``) mails one line only when this handler delivered a page of that kind
    earlier in its process. One recurring page key cannot starve other pages:
    each key mails at most ``per_key_daily_cap`` times per KST day, and repeats may only use
    ``daily_cap - first_occurrence_reserve`` slots. Limits live in an ``AlertLedger``; a shared
    ``FileAlertLedger`` gives child processes one budget. Sender failures are logged, never raised.

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
        super().__init__(level=logging.NOTSET)
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
        self._open_pages: set[PageKind] = set()

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
        kind = page_kind(record)
        if kind is not None:
            if self._deliver(
                alert_dedup_key(record.getMessage()),
                format_page_subject(kind),
                format_page_body(kind, record),
            ):
                self._open_pages.add(kind)
                logging.getLogger(__name__).info(
                    "[SYS] stage=alert status=SENT kind=%s component=%s run_id=%s",
                    kind.value, self._component, self._run_id,
                )
            return
        cleared = resolved_kind(record)
        if cleared is None or cleared not in self._open_pages:
            return
        self._open_pages.discard(cleared)
        self._deliver(
            f"resolved:{cleared.value}", format_resolved_subject(cleared), format_resolved_body(cleared, record)
        )

    def _deliver(self, key: str, subject: str, body: str) -> bool:
        day = self._today()
        if self._current_day is None or day != self._current_day:
            self._current_day = day
            self._suppressed_logged.clear()
        decision = self._ledger.reserve(key, day=day, now_s=self._clock(), limits=self._limits)
        if not decision.send:
            if decision.suppressed_reason in ("per_key_cap", "repeat_budget"):
                self._log_suppressed_once(key, str(decision.suppressed_reason))
            return False
        try:
            attempts = max(1, self._send_attempts)
            for attempt in range(attempts):
                try:
                    self._sender(subject, body)
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
        return True

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


__all__ = [
    "ALERT_COOLDOWN_S",
    "ALERT_DAILY_CAP",
    "ALERT_FIRST_OCCURRENCE_RESERVE",
    "ALERT_PER_KEY_DAILY_CAP",
    "ALERT_RETRY_BACKOFF_S",
    "ALERT_SEND_ATTEMPTS",
    "PAGE_TEXTS",
    "AlertDecision",
    "AlertLedger",
    "AlertLimits",
    "EmailAlertHandler",
    "FileAlertLedger",
    "InMemoryAlertLedger",
    "PageKind",
    "PageText",
    "alert_dedup_key",
    "format_page_body",
    "format_page_subject",
    "format_resolved_body",
    "format_resolved_subject",
    "gmail_sender",
    "is_alert_record",
    "page",
    "page_kind",
    "page_resolved",
    "resolved_kind",
]
