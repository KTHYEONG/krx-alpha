"""관측 가능성 기반: 컴포넌트별 로깅·영속 JSONL 이벤트·CRITICAL 이메일 알림."""

import datetime as dt
import html
import json
import logging
import pathlib
import queue
import re
import smtplib
import sys
import time
import traceback
from collections.abc import Callable
from email.message import EmailMessage
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from typing import IO
from zoneinfo import ZoneInfo

from src.core.config import AlertSettings, ObservabilitySettings, export_run_id

EVENT: dict[str, bool] = {"krx_event": True}
LOG_FILE_MAX_BYTES: int = 5 * 2**20
LOG_FILE_BACKUPS: int = 5
ALERT_COOLDOWN_S: float = 1800.0
ALERT_DAILY_CAP: int = 20
_MANAGED_ATTR = "_krx_alpha_managed"
_KV_RE = re.compile(r"(\w+)=(\S+)")
_KST = ZoneInfo("Asia/Seoul")
_LISTENERS: list[QueueListener] = []


def new_run_id(component: str, *, now_ns: int | None = None) -> str:
    ns = time.time_ns() if now_ns is None else now_ns
    return f"{component}-{ns // 1_000_000}"


def _format_ts(record: logging.LogRecord) -> str:
    return dt.datetime.fromtimestamp(record.created, tz=_KST).isoformat(timespec="milliseconds")


def _format_exc(record: logging.LogRecord) -> str | None:
    if record.exc_info:
        return "".join(traceback.format_exception(*record.exc_info))
    return record.exc_text or None


class KeyValueFormatter(logging.Formatter):
    def __init__(self, *, component: str, run_id: str) -> None:
        super().__init__()
        self._component = component
        self._run_id = run_id

    def format(self, record: logging.LogRecord) -> str:
        ts = _format_ts(record)
        base = f"{ts} level={record.levelname} comp={self._component} run={self._run_id} {record.getMessage()}"
        tb = _format_exc(record)
        if tb is not None:
            base += " exc=" + json.dumps(tb, ensure_ascii=False)
        return base


class JsonlEventFormatter(logging.Formatter):
    def __init__(self, *, component: str, run_id: str) -> None:
        super().__init__()
        self._component = component
        self._run_id = run_id

    def format(self, record: logging.LogRecord) -> str:
        msg = record.getMessage()
        payload = {
            "ts": _format_ts(record),
            "level": record.levelname,
            "component": self._component,
            "run_id": self._run_id,
            "logger": record.name,
            "msg": msg,
            "fields": dict(_KV_RE.findall(msg)),
            "exc": _format_exc(record),
        }
        return json.dumps(payload, ensure_ascii=False)


class EventFileFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno >= logging.WARNING or bool(getattr(record, "krx_event", False))


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
    "auth_rejected": "증권사 인증 거부",
    "ntp_unmeasured": "NTP 시간 동기화 측정 실패",
    "quarantine_exists": "격리 대상 중복",
}

_ACTION_HINTS: dict[str, str] = {
    "auth_expired": "rclone config reconnect gdrive: (VPS에서 Google Drive 재인증)",
    "circuit_open": "증권사 API/소켓 상태 확인 및 docker compose restart krx-collector",
    "remote_error": "VPS 네트워크 상태 및 원격 스토리지 가용성 점검",
    "size_mismatch": "로컬 및 원격 데이터 파일 손상 여부 확인",
    "stale": "실시간 데이터 수신 상태 및 세션 점검",
    "rclone_settings_missing": ".env 내 rclone 관련 환경변수 설정 확인",
    "no_bars_store": "일봉 데이터 파켓 파일 존재 여부 확인",
    "candidates_not_ready": "유니버스 선정 및 일봉 데이터 상태 확인 (docker compose restart krx-collector)",
    "auth_rejected": "증권사 API 키 및 계좌 설정 유효성 확인",
    "session_data_gap": "당일 수집 저널 및 일봉 데이터 누락 여부 확인",
    "maintenance_error": "VPS 디스크 공간 및 권한 확인 (docker compose logs -n 100 krx-collector)",
}


def _call_sender(
    sender: Callable[..., None],
    subject: str,
    body: str,
    *,
    html_body: str | None = None,
) -> None:
    if html_body:
        try:
            sender(subject, body, html_body=html_body)
            return
        except TypeError:
            pass
    sender(subject, body)


def format_alert_subject(
    component: str,
    msg: str,
    fields: dict[str, str],
    *,
    bot_name: str = "krx-alpha",
) -> str:
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

    return f"[{bot_name}] 🚨 {core}"


def format_alert_text(
    record: logging.LogRecord,
    component: str,
    run_id: str,
    fields: dict[str, str],
    *,
    bot_name: str = "krx-alpha",
) -> str:
    ts = _format_ts(record)
    stage = fields.get("stage", "")
    stage_label = _STAGE_LABELS.get(stage, stage)
    reason = fields.get("reason", "")
    reason_label = _REASON_LABELS.get(reason, reason)

    msg = record.getMessage()
    clean_msg = re.sub(r"\[\w+\]\s*", "", msg).strip()

    lines = [
        f"🚨 [{bot_name}] 장애 발생 알림",
        "=" * 50,
        f"• 발생일시: {ts} (KST)",
        f"• 문제위치: {component} > {stage_label or stage or '일반'}",
        f"• 장애원인: {reason_label or reason or '알 수 없음'}",
        f"• 상세내용: {clean_msg}",
        f"• Run ID:   {run_id}",
    ]

    if fields:
        lines.append("")
        lines.append("▼ 주요 컨텍스트 (Fields):")
        for k, v in fields.items():
            lines.append(f"  • {k}: {v}")

    lines.append("=" * 50)
    return "\n".join(lines)


def format_alert_html(
    record: logging.LogRecord,
    component: str,
    run_id: str,
    fields: dict[str, str],
    *,
    bot_name: str = "krx-alpha",
) -> str:
    ts = html.escape(_format_ts(record))
    stage = fields.get("stage", "")
    stage_label = html.escape(_STAGE_LABELS.get(stage, stage))
    reason = fields.get("reason", "")
    reason_label = html.escape(_REASON_LABELS.get(reason, reason))

    msg = record.getMessage()
    clean_msg = html.escape(re.sub(r"\[\w+\]\s*", "", msg).strip())

    context_html = ""
    if fields:
        kv_rows = "".join(
            f"<tr><td style='padding:3px 6px; color:#64748b; font-family:monospace; width:90px;'>{html.escape(k)}</td>"
            f"<td style='padding:3px 6px; font-family:monospace; color:#0f172a;'>{html.escape(v)}</td></tr>"
            for k, v in fields.items()
        )
        context_html = (
            f"<details style='margin-top:12px; background:#f8fafc; border:1px solid #e2e8f0; border-radius:6px; padding:8px 12px;'>"
            f"<summary style='font-size:12px; color:#475569; cursor:pointer; font-weight:600;'>▼ 상세 파라미터 (Fields)</summary>"
            f"<table style='width:100%; border-collapse:collapse; font-size:11px; margin-top:6px;'>{kv_rows}</table>"
            f"</details>"
        )

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif; margin:0; padding:16px; background-color:#f1f5f9; color:#1e293b;">
<div style="max-width:540px; margin:0 auto; background:#ffffff; border-radius:12px; border:1px solid #e2e8f0; overflow:hidden; box-shadow:0 4px 6px -1px rgba(0,0,0,0.05);">
  <div style="background:#dc2626; color:#ffffff; padding:14px 20px; font-size:16px; font-weight:700; display:flex; justify-content:space-between; align-items:center;">
    <span>🚨 [{html.escape(bot_name)}] 장애 알림</span>
    <span style="font-size:11px; background:rgba(255,255,255,0.25); padding:2px 8px; border-radius:12px; letter-spacing:0.5px;">CRITICAL</span>
  </div>
  <div style="padding:20px;">
    <table style="width:100%; font-size:13px; margin-bottom:14px; border-collapse:collapse;">
      <tr>
        <td style="color:#64748b; width:75px; padding:3px 0;">발생일시</td>
        <td style="font-weight:600; color:#1e293b; padding:3px 0;">{ts} (KST)</td>
      </tr>
      <tr>
        <td style="color:#64748b; padding:3px 0;">문제위치</td>
        <td style="color:#1e293b; padding:3px 0;"><code style="background:#f1f5f9; padding:2px 6px; border-radius:4px; font-size:12px;">{html.escape(component)}</code> &gt; <strong style="color:#0f172a;">{stage_label or stage or '일반'}</strong></td>
      </tr>
    </table>

    <div style="background:#fef2f2; border-left:4px solid #ef4444; padding:14px 16px; border-radius:6px; margin-bottom:14px;">
      <div style="color:#991b1b; font-weight:700; font-size:15px; margin-bottom:6px;">⚠️ {reason_label or reason or '장애 발생'}</div>
      <div style="color:#475569; font-size:13px; line-height:1.5; word-break:break-all;">{clean_msg}</div>
    </div>

    {context_html}
  </div>
  <div style="border-top:1px solid #f1f5f9; padding:10px 20px; font-size:11px; color:#94a3b8; text-align:right; background:#f8fafc;">
    Run ID: {html.escape(run_id)}
  </div>
</div>
</body>
</html>"""


def format_digest_html(subject: str, body: str, *, bot_name: str = "krx-alpha") -> str:
    lines = [ln.strip() for ln in body.splitlines() if ln.strip()]
    fields: dict[str, str] = {}
    for line in lines:
        if "=" in line:
            k, v = line.split("=", 1)
            fields[k.strip()] = v.strip()

    status = fields.get("status", "UNKNOWN")
    is_ok = status == "OK"
    status_bg = "#16a34a" if is_ok else "#dc2626"
    status_badge = "OK" if is_ok else "DEGRADED"

    uploaded = fields.get("uploaded", "-")
    deleted = fields.get("deleted_partitions", "-")
    purged = fields.get("purged", "-")
    reconciled = fields.get("reconciled", "-")
    backup_missing = fields.get("backup_missing", "0")
    streamer_restarts = fields.get("streamer_restarts", "0")
    run_id = fields.get("run_id", "")

    reconciled_ok = reconciled in ("True", "true", "OK")
    reconciled_color = "#16a34a" if reconciled_ok else "#dc2626"
    reconciled_text = "정상 (True)" if reconciled_ok else f"불일치 ({reconciled})"

    backup_ok = backup_missing == "0"
    backup_color = "#16a34a" if backup_ok else "#dc2626"
    backup_text = "0건 (정상)" if backup_ok else f"{backup_missing}건 (누락)"

    kpi_cards = f"""
    <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-bottom:14px;">
      <div style="background:#f8fafc; border:1px solid #e2e8f0; border-radius:8px; padding:10px 12px;">
        <div style="font-size:11px; color:#64748b; margin-bottom:4px;">📦 L1 원격 업로드</div>
        <div style="font-size:16px; font-weight:700; color:#0f172a; font-family:ui-monospace,Menlo,monospace;">{html.escape(uploaded)}<span style="font-size:12px; font-weight:normal; color:#64748b;"> 건</span></div>
      </div>
      <div style="background:#f8fafc; border:1px solid #e2e8f0; border-radius:8px; padding:10px 12px;">
        <div style="font-size:11px; color:#64748b; margin-bottom:4px;">🧹 파티션 정리 (영구삭제)</div>
        <div style="font-size:16px; font-weight:700; color:#0f172a; font-family:ui-monospace,Menlo,monospace;">{html.escape(deleted)}<span style="font-size:12px; font-weight:normal; color:#64748b;"> 개 ({html.escape(purged)}건)</span></div>
      </div>
      <div style="background:#f8fafc; border:1px solid #e2e8f0; border-radius:8px; padding:10px 12px;">
        <div style="font-size:11px; color:#64748b; margin-bottom:4px;">🔍 데이터 정합성</div>
        <div style="font-size:14px; font-weight:700; color:{reconciled_color};">{html.escape(reconciled_text)}</div>
      </div>
      <div style="background:#f8fafc; border:1px solid #e2e8f0; border-radius:8px; padding:10px 12px;">
        <div style="font-size:11px; color:#64748b; margin-bottom:4px;">☁️ 원격 백업 누락</div>
        <div style="font-size:14px; font-weight:700; color:{backup_color};">{html.escape(backup_text)}</div>
      </div>
    </div>
    """

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif; margin:0; padding:16px; background-color:#f1f5f9; color:#1e293b;">
<div style="max-width:540px; margin:0 auto; background:#ffffff; border-radius:12px; border:1px solid #e2e8f0; overflow:hidden; box-shadow:0 4px 6px -1px rgba(0,0,0,0.05);">
  <div style="background:{status_bg}; color:#ffffff; padding:14px 20px; font-size:16px; font-weight:700; display:flex; justify-content:space-between; align-items:center;">
    <span>📊 [{html.escape(bot_name)}] 일일 마감 리포트</span>
    <span style="font-size:11px; background:rgba(255,255,255,0.25); padding:2px 8px; border-radius:12px; letter-spacing:0.5px;">{html.escape(status_badge)}</span>
  </div>
  <div style="padding:20px;">
    <div style="margin-bottom:14px; font-size:14px; font-weight:600; color:{'#16a34a' if is_ok else '#dc2626'};">
      {'✅ 모든 EOD 마감 작업이 정상 완료되었습니다.' if is_ok else '⚠️ 정합성 또는 백업 항목에서 이상이 감지되었습니다.'}
    </div>
    {kpi_cards}
    <div style="font-size:12px; color:#64748b; background:#f8fafc; padding:8px 12px; border-radius:6px; border:1px solid #e2e8f0;">
      • 스트리머 재시작: <strong style="color:#0f172a;">{html.escape(streamer_restarts)}회</strong> &nbsp;|&nbsp; 오케스트레이션 시도: <strong style="color:#0f172a;">{html.escape(fields.get("orchestration_attempts", "1"))}회</strong>
    </div>
  </div>
  <div style="border-top:1px solid #f1f5f9; padding:10px 20px; font-size:11px; color:#94a3b8; text-align:right; background:#f8fafc;">
    {html.escape(subject)} {f'| Run ID: {html.escape(run_id)}' if run_id else ''}
  </div>
</div>
</body>
</html>"""


class EmailAlertHandler(logging.Handler):
    def __init__(
        self,
        *,
        component: str,
        run_id: str,
        sender: Callable[..., None],
        cooldown_s: float = ALERT_COOLDOWN_S,
        daily_cap: int = ALERT_DAILY_CAP,
        clock: Callable[[], float] = time.monotonic,
        today: Callable[[], dt.date] | None = None,
    ) -> None:
        super().__init__(level=logging.CRITICAL)
        self._component = component
        self._run_id = run_id
        self._sender = sender
        self._cooldown_s = cooldown_s
        self._daily_cap = daily_cap
        self._clock = clock
        self._today = today if today is not None else (lambda: dt.datetime.now(_KST).date())
        self._sent_today = 0
        self._current_day: dt.date | None = None
        self._last_sent: dict[str, float] = {}

    def emit(self, record: logging.LogRecord) -> None:
        key = str(record.getMessage())[:80]
        day = self._today()
        if self._current_day is None:
            self._current_day = day
        if day != self._current_day:
            self._current_day = day
            self._sent_today = 0
            self._last_sent.clear()
        if self._sent_today >= self._daily_cap:
            return
        now = self._clock()
        last = self._last_sent.get(key)
        if last is not None and (now - last) < self._cooldown_s:
            return
        msg = record.getMessage()
        fields = dict(_KV_RE.findall(msg))
        subject = format_alert_subject(self._component, msg, fields)
        text_body = format_alert_text(record, self._component, self._run_id, fields)
        html_body = format_alert_html(record, self._component, self._run_id, fields)
        try:
            _call_sender(self._sender, subject, text_body, html_body=html_body)
        except (smtplib.SMTPException, OSError) as exc:
            logging.getLogger(__name__).warning("[SYS] stage=alert status=FAIL reason=%s", type(exc).__name__)
            return
        self._last_sent[key] = now
        self._sent_today += 1


def gmail_sender(settings: AlertSettings) -> Callable[..., None]:
    def send(subject: str, body: str, *, html_body: str | None = None) -> None:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = settings.alert_gmail_user
        msg["To"] = settings.alert_gmail_to
        msg.set_content(body)
        if html_body:
            msg.add_alternative(html_body, subtype="html")
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as smtp:
            smtp.login(settings.alert_gmail_user, settings.alert_gmail_app_password)
            smtp.send_message(msg)

    return send


def format_digest_text(subject: str, body: str, *, bot_name: str = "krx-alpha") -> str:
    lines = [ln.strip() for ln in body.splitlines() if ln.strip()]
    fields: dict[str, str] = {}
    for line in lines:
        if "=" in line:
            k, v = line.split("=", 1)
            fields[k.strip()] = v.strip()

    if not fields:
        return body

    status = fields.get("status", "UNKNOWN")
    is_ok = status == "OK"
    run_id = fields.get("run_id", "")
    uploaded = fields.get("uploaded", "-")
    deleted = fields.get("deleted_partitions", "-")
    purged = fields.get("purged", "-")
    reconciled = fields.get("reconciled", "-")
    backup_missing = fields.get("backup_missing", "0")
    streamer_restarts = fields.get("streamer_restarts", "0")
    attempts = fields.get("orchestration_attempts", "1")

    status_icon = "✅ OK (정상 완료)" if is_ok else f"⚠️ {status} (확인 필요)"
    reconciled_text = "정상 (True)" if reconciled in ("True", "true", "OK") else f"불일치 ({reconciled})"
    backup_text = "0건 (정상)" if backup_missing == "0" else f"{backup_missing}건 (누락)"

    out = [
        f"📊 [{bot_name}] 일일 마감 리포트",
        "=" * 50,
        f"• 마감상태: {status_icon}",
        f"• L1 업로드: {uploaded}건 | 파티션 정리: {deleted}개 ({purged}건 삭제)",
        f"• 정합성 검증: {reconciled_text} | 원격 백업 누락: {backup_text}",
        f"• 스트리머 재시작: {streamer_restarts}회 | 오케스트레이션 시도: {attempts}회",
    ]
    if run_id:
        out.append(f"• Run ID: {run_id}")

    out.append("=" * 50)
    return "\n".join(out)


def send_digest(
    subject: str, body: str, *, settings: AlertSettings | None = None, sender: Callable[..., None] | None = None
) -> bool:
    s = settings if settings is not None else AlertSettings()
    sys_logger = logging.getLogger(__name__)
    if not s.enabled:
        sys_logger.info("[SYS] stage=digest status=DISABLED")
        return False
    html_body = format_digest_html(subject, body)
    text_body = format_digest_text(subject, body)
    actual_sender = sender or gmail_sender(s)
    try:
        _call_sender(actual_sender, subject, text_body, html_body=html_body)
    except (smtplib.SMTPException, OSError) as exc:
        sys_logger.warning("[SYS] stage=digest status=FAIL reason=%s", type(exc).__name__)
        return False
    sys_logger.info("[SYS] stage=digest status=SENT")
    return True


def configure_logging(
    component: str,
    *,
    log_dir: pathlib.Path | None = None,
    level: int | str = logging.INFO,
    stream: IO[str] | None = None,
    alert_settings: AlertSettings | None = None,
    alert_sender: Callable[[str, str], None] | None = None,
) -> str:
    run_id = ObservabilitySettings().run_id or new_run_id(component)
    export_run_id(run_id)
    shutdown_logging()
    root = logging.getLogger()
    root.setLevel(level)
    stream_handler = logging.StreamHandler(stream or sys.stderr)
    stream_handler.setFormatter(KeyValueFormatter(component=component, run_id=run_id))
    setattr(stream_handler, _MANAGED_ATTR, True)
    root.addHandler(stream_handler)
    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            log_dir / f"events-{component}.jsonl",
            maxBytes=LOG_FILE_MAX_BYTES,
            backupCount=LOG_FILE_BACKUPS,
            encoding="utf-8",
        )
        file_handler.setFormatter(JsonlEventFormatter(component=component, run_id=run_id))
        file_handler.addFilter(EventFileFilter())
        setattr(file_handler, _MANAGED_ATTR, True)
        root.addHandler(file_handler)
    alerts = alert_settings if alert_settings is not None else AlertSettings()
    sys_logger = logging.getLogger(__name__)
    if alerts.enabled:
        q: queue.SimpleQueue[logging.LogRecord] = queue.SimpleQueue()
        queue_handler = QueueHandler(q)
        queue_handler.setLevel(logging.CRITICAL)
        setattr(queue_handler, _MANAGED_ATTR, True)
        email_handler = EmailAlertHandler(
            component=component,
            run_id=run_id,
            sender=alert_sender or gmail_sender(alerts),
        )
        listener = QueueListener(q, email_handler, respect_handler_level=False)
        listener.start()
        _LISTENERS.append(listener)
        root.addHandler(queue_handler)
        sys_logger.info("[SYS] stage=alert status=ENABLED")
    else:
        sys_logger.info("[SYS] stage=alert status=DISABLED")
    return run_id


def shutdown_logging() -> None:
    for listener in _LISTENERS:
        listener.stop()
    _LISTENERS.clear()
    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, _MANAGED_ATTR, False):
            root.removeHandler(handler)
            handler.close()
