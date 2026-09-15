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
    "streamer": "시세수집",
    "stream_flush": "시세저장",
    "journal_flush": "저널저장",
    "ingest_watchdog": "수집감시",
    "rclone_offload": "원격저장",
    "prune": "데이터정리",
    "order": "주문실행",
    "oms": "주문실행",
    "session": "세션관리",
    "orchestration": "데몬관리",
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
}

_ACTION_HINTS: dict[str, str] = {
    "auth_expired": "rclone config reconnect gdrive: (VPS에서 Google Drive 재인증)",
    "circuit_open": "증권사 API/소켓 상태 확인 및 docker compose restart krx-collector",
    "remote_error": "VPS 네트워크 상태 및 원격 스토리지 가용성 점검",
    "size_mismatch": "로컬 및 원격 데이터 파일 손상 여부 확인",
    "stale": "실시간 데이터 수신 상태 및 세션 점검",
    "rclone_settings_missing": ".env 내 rclone 관련 환경변수 설정 확인",
    "no_bars_store": "일봉 데이터 파켓 파일 존재 여부 확인",
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
    hint = fields.get("hint", "")

    action = _ACTION_HINTS.get(reason)
    if not action and "rclone_config_reconnect_gdrive" in hint:
        action = "rclone config reconnect gdrive: (VPS에서 Google Drive 재인증)"
    if not action:
        action = "docker compose logs -n 100 krx-collector (VPS에서 로그 확인)"

    msg = record.getMessage()
    clean_msg = re.sub(r"\[\w+\]\s*", "", msg).strip()
    exc = _format_exc(record)

    lines = [
        f"🤖 [{bot_name}] CRITICAL 장애 알림",
        "=" * 50,
        f"• 일시: {ts} (KST)",
        f"• 위치: {component} > {stage_label or stage or '일반'}",
        f"• 원인: {reason_label or reason or '알 수 없음'}",
        f"• 내용: {clean_msg}",
        "",
        "-" * 50,
        "🛠️ 즉시 조치 (Action):",
        f"{action}",
        "-" * 50,
    ]

    if fields:
        lines.append("")
        lines.append("▼ 컨텍스트 (Fields):")
        for k, v in fields.items():
            lines.append(f"  - {k}: {v}")

    if exc:
        lines.append("")
        lines.append("▼ Traceback:")
        lines.append(exc.strip())

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
    hint = fields.get("hint", "")

    action = _ACTION_HINTS.get(reason)
    if not action and "rclone_config_reconnect_gdrive" in hint:
        action = "rclone config reconnect gdrive: (VPS에서 Google Drive 재인증)"
    if not action:
        action = "docker compose logs -n 100 krx-collector (VPS에서 로그 확인)"
    action_escaped = html.escape(action)

    msg = record.getMessage()
    clean_msg = html.escape(re.sub(r"\[\w+\]\s*", "", msg).strip())
    exc = _format_exc(record)

    details_parts: list[str] = []
    if fields:
        kv_rows = "".join(
            f"<tr><td style='padding:4px 8px; color:#64748b; font-family:monospace;'>{html.escape(k)}</td>"
            f"<td style='padding:4px 8px; font-family:monospace; color:#1e293b;'>{html.escape(v)}</td></tr>"
            for k, v in fields.items()
        )
        details_parts.append(
            f"<table style='width:100%; border-collapse:collapse; font-size:12px; margin-top:8px;'>{kv_rows}</table>"
        )
    if exc:
        details_parts.append(
            f"<pre style='margin-top:10px; background:#1e1e1e; color:#d4d4d4; padding:12px; border-radius:4px; font-size:11px; overflow-x:auto; white-space:pre-wrap; word-break:break-all;'>{html.escape(exc.strip())}</pre>"
        )

    details_html = ""
    if details_parts:
        inner = "".join(details_parts)
        details_html = (
            f"<details style='margin-top:14px; background:#f8fafc; border:1px solid #e2e8f0; border-radius:6px; padding:10px 12px;'>"
            f"<summary style='font-size:12px; color:#475569; cursor:pointer; font-weight:600;'>▼ 상세 로그 및 Traceback 확인 (클릭)</summary>"
            f"{inner}"
            f"</details>"
        )

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; margin:0; padding:12px; background-color:#f4f6f8; color:#212529;">
<div style="max-width:540px; margin:0 auto; background:#ffffff; border-radius:8px; border:1px solid #e2e8f0; overflow:hidden; box-shadow:0 1px 3px rgba(0,0,0,0.08);">
  <div style="background:#dc2626; color:#ffffff; padding:12px 18px; font-size:15px; font-weight:bold; display:flex; justify-content:space-between; align-items:center;">
    <span>🚨 [{html.escape(bot_name)}] 장애 알림</span>
    <span style="font-size:11px; background:rgba(255,255,255,0.25); padding:2px 8px; border-radius:10px;">CRITICAL</span>
  </div>
  <div style="padding:16px 18px;">
    <div style="font-size:13px; line-height:1.6; margin-bottom:12px;">
      <div><span style="color:#64748b; width:70px; display:inline-block;">• 발생일시:</span><strong>{ts} (KST)</strong></div>
      <div><span style="color:#64748b; width:70px; display:inline-block;">• 문제위치:</span><code>{html.escape(component)}</code> &gt; <code>{stage_label or stage or '일반'}</code></div>
    </div>
    <div style="background:#fef2f2; border-left:4px solid #ef4444; padding:10px 14px; border-radius:4px; margin-bottom:14px;">
      <div style="color:#991b1b; font-weight:bold; font-size:14px; margin-bottom:4px;">⚠️ {reason_label or reason or '장애 발생'}</div>
      <div style="color:#374151; font-size:13px; word-break:break-all;">{clean_msg}</div>
    </div>
    <div style="background:#eff6ff; border-left:4px solid #3b82f6; padding:10px 14px; border-radius:4px; margin-bottom:12px;">
      <div style="color:#1e40af; font-weight:bold; font-size:12px; margin-bottom:4px;">🛠️ 즉시 조치 (Action)</div>
      <div style="font-family:monospace; background:#ffffff; border:1px solid #bfdbfe; padding:6px 10px; border-radius:4px; font-size:12px; color:#1e3a8a; word-break:break-all;">{action_escaped}</div>
    </div>
    {details_html}
  </div>
  <div style="border-top:1px solid #f1f5f9; padding:8px 18px; font-size:11px; color:#94a3b8; text-align:right; background:#f8fafc;">
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

    rows = (
        "".join(
            f"<tr><td style='padding:6px 12px; color:#475569; font-size:13px;'>{html.escape(k)}</td>"
            f"<td style='padding:6px 12px; font-weight:600; font-family:monospace; font-size:13px; color:{'#dc2626' if (k in ('backup_missing', 'status') and v not in ('0', 'OK')) or (k == 'reconciled' and v == 'False') else '#1e293b'};'>{html.escape(v)}</td></tr>"
            for k, v in fields.items()
        )
        if fields
        else f"<tr><td style='padding:10px;'><pre style='margin:0; font-size:12px;'>{html.escape(body)}</pre></td></tr>"
    )

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; margin:0; padding:12px; background-color:#f4f6f8; color:#212529;">
<div style="max-width:540px; margin:0 auto; background:#ffffff; border-radius:8px; border:1px solid #e2e8f0; overflow:hidden; box-shadow:0 1px 3px rgba(0,0,0,0.08);">
  <div style="background:{status_bg}; color:#ffffff; padding:12px 18px; font-size:15px; font-weight:bold; display:flex; justify-content:space-between; align-items:center;">
    <span>📊 [{html.escape(bot_name)}] 일일 마감 리포트</span>
    <span style="font-size:11px; background:rgba(255,255,255,0.25); padding:2px 8px; border-radius:10px;">{html.escape(status_badge)}</span>
  </div>
  <div style="padding:16px 18px;">
    <div style="margin-bottom:12px; font-size:14px; font-weight:600; color:{'#16a34a' if is_ok else '#dc2626'};">
      {'✅ 모든 EOD 마감 작업이 정상 완료되었습니다.' if is_ok else '⚠️ 정합성 또는 백업 항목에서 이상이 감지되었습니다.'}
    </div>
    <table style="width:100%; border-collapse:collapse; background:#f8fafc; border-radius:6px; overflow:hidden; border:1px solid #e2e8f0;">
      {rows}
    </table>
  </div>
  <div style="border-top:1px solid #f1f5f9; padding:8px 18px; font-size:11px; color:#94a3b8; text-align:right; background:#f8fafc;">
    {html.escape(subject)}
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


def send_digest(
    subject: str, body: str, *, settings: AlertSettings | None = None, sender: Callable[..., None] | None = None
) -> bool:
    s = settings if settings is not None else AlertSettings()
    sys_logger = logging.getLogger(__name__)
    if not s.enabled:
        sys_logger.info("[SYS] stage=digest status=DISABLED")
        return False
    html_body = format_digest_html(subject, body)
    actual_sender = sender or gmail_sender(s)
    try:
        _call_sender(actual_sender, subject, body, html_body=html_body)
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
