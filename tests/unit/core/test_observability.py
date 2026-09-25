

def test_new_run_id_uses_component_and_epoch_ms() -> None:
    from src.core.observability import new_run_id

    assert new_run_id("daemon", now_ns=1_789_000_000_123_456_789) == "daemon-1789000000123"


def test_key_value_formatter_renders_single_line_with_context() -> None:
    import logging

    from src.core.observability import KeyValueFormatter

    record = logging.LogRecord("x", logging.WARNING, __file__, 1, "[DATA] stage=prune part=%s", ("p",), None)
    record.created = 1789000000.5

    line = KeyValueFormatter(component="daemon", run_id="daemon-1").format(record)

    assert line == "2026-09-10T09:26:40.500+09:00 level=WARNING comp=daemon run=daemon-1 [DATA] stage=prune part=p"


def test_key_value_formatter_escapes_traceback_into_one_line() -> None:
    import logging
    import sys

    from src.core.observability import KeyValueFormatter

    try:
        raise ValueError("boom")
    except ValueError:
        exc_info = sys.exc_info()
    record = logging.LogRecord("x", logging.ERROR, __file__, 1, "[DAEMON] stage=eod_maintenance error=%s", ("boom",), exc_info)

    line = KeyValueFormatter(component="daemon", run_id="r").format(record)

    assert "\n" not in line
    assert ' exc="Traceback' in line
    assert "ValueError: boom" in line


def test_jsonl_event_formatter_parses_key_value_fields() -> None:
    import json
    import logging

    from src.core.observability import JsonlEventFormatter

    record = logging.LogRecord("src.storage.retention", logging.CRITICAL, __file__, 1, "[DATA] stage=prune status=FAIL part=%s", ("p1",), None)
    record.created = 1789000000.5

    payload = json.loads(JsonlEventFormatter(component="normalize-worker", run_id="daemon-9").format(record))

    assert payload == {
        "ts": "2026-09-10T09:26:40.500+09:00",
        "level": "CRITICAL",
        "component": "normalize-worker",
        "run_id": "daemon-9",
        "logger": "src.storage.retention",
        "msg": "[DATA] stage=prune status=FAIL part=p1",
        "fields": {"stage": "prune", "status": "FAIL", "part": "p1"},
        "exc": None,
    }


def test_event_file_filter_passes_warning_and_marked_events_only() -> None:
    import logging

    from src.core.observability import EVENT, EventFileFilter

    def rec(level: int) -> logging.LogRecord:
        return logging.LogRecord("x", level, __file__, 1, "m", (), None)

    marked = rec(logging.INFO)
    for key, value in EVENT.items():
        setattr(marked, key, value)
    flt = EventFileFilter()

    assert flt.filter(rec(logging.WARNING)) is True
    assert flt.filter(marked) is True
    assert flt.filter(rec(logging.INFO)) is False
    assert flt.filter(rec(logging.DEBUG)) is False


def test_configure_logging_writes_stream_and_persistent_event_file(tmp_path, monkeypatch) -> None:
    import io
    import json
    import logging
    from logging.handlers import RotatingFileHandler

    from src.core.config import AlertSettings
    from src.core.observability import EVENT, configure_logging, shutdown_logging

    monkeypatch.setenv("KRX_ALPHA_RUN_ID", "run-fixed")
    stream = io.StringIO()
    log_dir = tmp_path / "logs"

    run_id = configure_logging("daemon", log_dir=log_dir, stream=stream, alert_settings=AlertSettings(alert_gmail_user="", alert_gmail_app_password="", alert_gmail_to=""))
    log = logging.getLogger("src.orchestration.daemon")
    log.info("[DAEMON] stage=start status=ONLINE", extra=EVENT)
    log.info("[DAEMON] plain info")
    log.warning("[DATA] stage=prune status=FAIL part=p")
    file_handlers = [h for h in logging.getLogger().handlers if isinstance(h, RotatingFileHandler)]
    shutdown_logging()

    events = [json.loads(line) for line in (log_dir / "events-daemon.jsonl").read_text(encoding="utf-8").splitlines()]
    assert run_id == "run-fixed"
    assert [e["msg"] for e in events] == ["[DAEMON] stage=start status=ONLINE", "[DATA] stage=prune status=FAIL part=p"]
    assert all(e["run_id"] == "run-fixed" for e in events)
    assert len(file_handlers) == 1
    assert file_handlers[0].maxBytes == 5 * 2**20
    assert file_handlers[0].backupCount == 5
    text = stream.getvalue()
    assert "comp=daemon run=run-fixed [DAEMON] plain info" in text
    assert "stage=alert status=DISABLED" in text
    assert not any(isinstance(h, RotatingFileHandler) for h in logging.getLogger().handlers)


def test_configure_logging_is_idempotent_and_keeps_foreign_handlers(monkeypatch) -> None:
    import io
    import logging

    from src.core.config import AlertSettings
    from src.core.observability import configure_logging, shutdown_logging

    monkeypatch.setenv("KRX_ALPHA_RUN_ID", "r")
    foreign = logging.NullHandler()
    root = logging.getLogger()
    root.addHandler(foreign)
    disabled = AlertSettings(alert_gmail_user="", alert_gmail_app_password="", alert_gmail_to="")
    try:
        configure_logging("daemon", stream=io.StringIO(), alert_settings=disabled)
        configure_logging("daemon", stream=io.StringIO(), alert_settings=disabled)
        managed = [h for h in root.handlers if getattr(h, "_krx_alpha_managed", False)]
        assert len(managed) == 1
        assert foreign in root.handlers
    finally:
        shutdown_logging()
        root.removeHandler(foreign)
    assert [h for h in root.handlers if getattr(h, "_krx_alpha_managed", False)] == []


def test_configure_logging_exports_generated_run_id(monkeypatch) -> None:
    import io

    from src.core.config import AlertSettings, ObservabilitySettings
    from src.core.observability import configure_logging, shutdown_logging

    monkeypatch.delenv("KRX_ALPHA_RUN_ID", raising=False)
    try:
        run_id = configure_logging("daemon", stream=io.StringIO(), alert_settings=AlertSettings(alert_gmail_user="", alert_gmail_app_password="", alert_gmail_to=""))
    finally:
        shutdown_logging()

    assert run_id.startswith("daemon-")
    assert run_id.split("-")[1].isdigit()
    assert ObservabilitySettings().run_id == run_id


def test_configure_logging_sends_critical_email_via_background_queue(monkeypatch) -> None:
    import io
    import logging
    from logging.handlers import QueueHandler

    from src.core.config import AlertSettings
    from src.core.observability import configure_logging, shutdown_logging

    monkeypatch.setenv("KRX_ALPHA_RUN_ID", "r")
    sent: list[tuple[str, str]] = []
    stream = io.StringIO()
    configure_logging(
        "daemon",
        stream=stream,
        alert_settings=AlertSettings(alert_gmail_user="u@x", alert_gmail_app_password="pw", alert_gmail_to="t@x"),
        alert_sender=lambda subject, body: sent.append((subject, body)),
    )
    queue_handlers = [h for h in logging.getLogger().handlers if isinstance(h, QueueHandler)]
    log = logging.getLogger("src.orchestration.daemon")
    log.error("[DAEMON] stage=eod_maintenance error=x")
    log.critical("[DAEMON] stage=streamer status=FAIL reason=circuit_open")
    shutdown_logging()

    assert len(queue_handlers) == 1
    assert queue_handlers[0].level == logging.CRITICAL
    assert sent[0][0] == "[krx-alpha] 🚨 [시세수집] 소켓 단절 (서킷오픈)"
    assert "circuit_open" in sent[0][1]
    assert "stage=alert status=ENABLED" in stream.getvalue()


def test_email_alert_handler_applies_cooldown_and_daily_cap() -> None:
    import datetime as dt
    import logging

    from src.core.observability import EmailAlertHandler

    sent: list[str] = []
    now = {"t": 0.0, "day": dt.date(2026, 9, 14)}
    handler = EmailAlertHandler(
        component="daemon", run_id="r", sender=lambda s, b: sent.append(s),
        cooldown_s=1800.0, daily_cap=2, clock=lambda: now["t"], today=lambda: now["day"],
    )

    def rec(msg: str) -> logging.LogRecord:
        return logging.LogRecord("x", logging.CRITICAL, __file__, 1, msg, (), None)

    handler.handle(rec("A failure"))
    now["t"] = 10.0
    handler.handle(rec("A failure"))
    assert len(sent) == 1
    now["t"] = 1811.0
    handler.handle(rec("A failure"))
    assert len(sent) == 2
    handler.handle(rec("B failure"))
    assert len(sent) == 2
    now["day"] = dt.date(2026, 9, 15)
    handler.handle(rec("B failure"))
    assert len(sent) == 3


def test_email_alert_handler_logs_warning_and_does_not_raise_on_send_failure(caplog) -> None:
    import logging
    import smtplib

    from src.core.observability import EmailAlertHandler

    calls: list[str] = []

    def failing(subject: str, body: str) -> None:
        calls.append(subject)
        raise smtplib.SMTPAuthenticationError(535, b"bad credentials")

    handler = EmailAlertHandler(component="daemon", run_id="r", sender=failing, clock=lambda: 0.0)
    record = logging.LogRecord("x", logging.CRITICAL, __file__, 1, "[DAEMON] down", (), None)

    with caplog.at_level(logging.WARNING):
        handler.handle(record)
        handler.handle(record)

    assert len(calls) == 2
    assert "stage=alert status=FAIL reason=SMTPAuthenticationError" in caplog.text


def test_gmail_sender_sends_via_smtp_ssl(monkeypatch) -> None:
    import smtplib

    from src.core.config import AlertSettings
    from src.core.observability import gmail_sender

    seen: dict[str, object] = {}

    class FakeSmtp:
        def __init__(self, host: str, port: int, timeout: float) -> None:
            seen["conn"] = (host, port, timeout)

        def __enter__(self) -> "FakeSmtp":
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        def login(self, user: str, password: str) -> None:
            seen["login"] = (user, password)

        def send_message(self, msg: object) -> None:
            seen["msg"] = msg

    monkeypatch.setattr(smtplib, "SMTP_SSL", FakeSmtp)
    send = gmail_sender(AlertSettings(alert_gmail_user="u@x", alert_gmail_app_password="pw", alert_gmail_to="t@x"))

    send("subject-1", "body-1")

    msg = seen["msg"]
    assert seen["conn"] == ("smtp.gmail.com", 465, 10)
    assert seen["login"] == ("u@x", "pw")
    assert msg["Subject"] == "subject-1"
    assert msg["From"] == "u@x"
    assert msg["To"] == "t@x"
    assert msg.get_content().strip() == "body-1"


def test_send_digest_sends_via_sender_when_alerts_enabled(caplog) -> None:
    import logging

    from src.core.config import AlertSettings
    from src.core.observability import send_digest

    sent: list[tuple[str, str]] = []
    settings = AlertSettings(alert_gmail_user="u@x", alert_gmail_app_password="pw", alert_gmail_to="t@x")

    with caplog.at_level(logging.INFO):
        ok = send_digest("[krx-alpha] EOD 2026-09-14 OK", "status=OK", settings=settings, sender=lambda s, b: sent.append((s, b)))

    assert ok is True
    assert len(sent) == 1
    assert sent[0][0] == "[krx-alpha] EOD 2026-09-14 OK"
    assert "일일 마감 리포트" in sent[0][1]
    assert "정상 완료" in sent[0][1]
    assert "stage=digest status=SENT" in caplog.text

def test_send_digest_skips_when_alerts_disabled(caplog) -> None:
    import logging

    from src.core.config import AlertSettings
    from src.core.observability import send_digest

    def _never(subject, body):
        raise AssertionError("must not send")

    with caplog.at_level(logging.INFO):
        ok = send_digest("s", "b", settings=AlertSettings(alert_gmail_user="", alert_gmail_app_password="", alert_gmail_to=""), sender=_never)

    assert ok is False
    assert "stage=digest status=DISABLED" in caplog.text

def test_send_digest_logs_warning_and_returns_false_on_smtp_failure(caplog) -> None:
    import logging
    import smtplib

    from src.core.config import AlertSettings
    from src.core.observability import send_digest

    def _broken(subject, body):
        raise smtplib.SMTPServerDisconnected("closed")

    settings = AlertSettings(alert_gmail_user="u@x", alert_gmail_app_password="pw", alert_gmail_to="t@x")
    with caplog.at_level(logging.WARNING):
        ok = send_digest("s", "b", settings=settings, sender=_broken)

    assert ok is False
    assert "stage=digest status=FAIL reason=SMTPServerDisconnected" in caplog.text


def test_format_alert_subject_mappings() -> None:
    from src.core.observability import format_alert_subject

    # 1. stage & reason mapped
    s1 = format_alert_subject("daemon", "[DAEMON] stage=backup_freshness status=FAIL reason=auth_expired", {"stage": "backup_freshness", "reason": "auth_expired"})
    assert s1 == "[krx-alpha] 🚨 [백업점검] GDrive 인증 만료"

    # 2. stage only
    s2 = format_alert_subject("streamer", "[STREAM] stage=stream_flush status=TIMEOUT", {"stage": "stream_flush", "status": "TIMEOUT"})
    assert s2 == "[krx-alpha] 🚨 [시세저장] TIMEOUT"

    # 3. reason only
    s3 = format_alert_subject("daemon", "reason=circuit_open", {"reason": "circuit_open"})
    assert s3 == "[krx-alpha] 🚨 [daemon] 소켓 단절 (서킷오픈)"

    # 4. fallback unmapped
    s4 = format_alert_subject("daemon", "[DAEMON] unexpected crash at startup", {})
    assert s4 == "[krx-alpha] 🚨 [daemon] unexpected crash at startup"


def test_format_alert_text_and_html() -> None:
    import logging

    from src.core.observability import format_alert_html, format_alert_text

    record = logging.LogRecord(
        name="src.orchestration.daemon",
        level=logging.CRITICAL,
        pathname="daemon.py",
        lineno=10,
        msg="[DAEMON] stage=backup_freshness status=FAIL reason=auth_expired hint=rclone_config_reconnect_gdrive",
        args=(),
        exc_info=None,
    )
    fields = {
        "stage": "backup_freshness",
        "status": "FAIL",
        "reason": "auth_expired",
        "hint": "rclone_config_reconnect_gdrive",
    }

    text = format_alert_text(record, "daemon", "daemon-123", fields)
    assert "🚨 [krx-alpha] 장애 발생 알림" in text
    assert "• 문제위치: daemon > 백업점검" in text
    assert "• 장애원인: GDrive 인증 만료" in text
    assert "• Run ID:   daemon-123" in text
    assert "stage: backup_freshness" in text

    html = format_alert_html(record, "daemon", "daemon-123", fields)
    assert "🚨 [krx-alpha] 장애 알림" in html
    assert "GDrive 인증 만료" in html
    assert "Run ID: daemon-123" in html


def test_format_digest_html_renders_ok_and_degraded() -> None:
    from src.core.observability import format_digest_html

    ok_body = "status=OK\nuploaded=2\npurged=1\nreconciled=True\nbackup_missing=0"
    html_ok = format_digest_html("[krx-alpha] EOD 2026-09-14 OK", ok_body)
    assert "정상 완료되었습니다" in html_ok
    assert "#16a34a" in html_ok
    assert "L1 원격 업로드" in html_ok
    assert "파티션 정리" in html_ok

    degraded_body = "status=DEGRADED\nuploaded=0\npurged=0\nreconciled=False\nbackup_missing=2\nrun_id=daemon-999"
    html_degraded = format_digest_html("[krx-alpha] EOD 2026-09-14 DEGRADED", degraded_body)
    assert "이상이 감지되었습니다" in html_degraded
    assert "#dc2626" in html_degraded
    assert "DEGRADED" in html_degraded
    assert "2건 (누락)" in html_degraded
    assert "daemon-999" in html_degraded


def test_gmail_sender_sends_multipart_when_html_body_provided(monkeypatch) -> None:
    import smtplib

    from src.core.config import AlertSettings
    from src.core.observability import gmail_sender

    seen: dict[str, object] = {}

    class FakeSmtp:
        def __init__(self, host: str, port: int, timeout: float) -> None:
            pass

        def __enter__(self) -> "FakeSmtp":
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        def login(self, user: str, password: str) -> None:
            pass

        def send_message(self, msg: object) -> None:
            seen["msg"] = msg

    monkeypatch.setattr(smtplib, "SMTP_SSL", FakeSmtp)
    send = gmail_sender(AlertSettings(alert_gmail_user="u@x", alert_gmail_app_password="pw", alert_gmail_to="t@x"))
    send("subj", "plain-body", html_body="<b>html-body</b>")

    msg = seen["msg"]
    assert msg.is_multipart() is True
    assert msg.get_body(preferencelist=("plain",)).get_content().strip() == "plain-body"
    assert "<b>html-body</b>" in msg.get_body(preferencelist=("html",)).get_content()


def test_aftermarket_alert_labels_use_canonical_term() -> None:
    from src.core.observability import _REASON_LABELS, _STAGE_LABELS

    for key in ("aftermarket_reselection", "aftermarket_plan"):
        label = _STAGE_LABELS[key]
        assert "애프터마켓" in label
        assert "시간외" not in label
    reason = _REASON_LABELS["aftermarket_not_ready"]
    assert "애프터마켓" in reason
    assert "시간외" not in reason


def test_send_digest_uses_html_capable_sender_without_fallback(caplog) -> None:
    import logging

    from src.core.config import AlertSettings
    from src.core.observability import send_digest

    calls: list[tuple[str, str, str | None]] = []

    def _html_sender(subject: str, body: str, html_body: str | None = None) -> None:
        calls.append((subject, body, html_body))

    settings = AlertSettings(alert_gmail_user="u@x", alert_gmail_app_password="pw", alert_gmail_to="t@x")
    with caplog.at_level(logging.INFO):
        ok = send_digest("s", "status=OK", settings=settings, sender=_html_sender)

    assert ok is True
    assert len(calls) == 1
    assert calls[0][2] is not None
    assert "마감" in calls[0][2]


def test_format_digest_text_includes_run_id_when_present() -> None:
    from src.core.observability import format_digest_text

    text = format_digest_text("s", "status=OK\nuploaded=2\nrun_id=daemon-1")

    assert "• Run ID: daemon-1" in text
