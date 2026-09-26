def _record(msg="boom"):
    import logging

    return logging.LogRecord("x", logging.CRITICAL, __file__, 1, msg, (), None)


def test_transient_smtp_failure_retried_then_delivered() -> None:
    import datetime as dt
    import smtplib

    from src.core.alerts import EmailAlertHandler

    calls: list[str] = []
    sleeps: list[float] = []
    now = {"t": 0.0, "day": dt.date(2026, 9, 25)}

    def _flaky(subject: str, body: str) -> None:
        calls.append(subject)
        if len(calls) <= 2:
            raise smtplib.SMTPServerDisconnected("closed")

    handler = EmailAlertHandler(
        component="daemon",
        run_id="r",
        sender=_flaky,
        clock=lambda: now["t"],
        today=lambda: now["day"],
        sleep=sleeps.append,
    )

    handler.handle(_record())

    assert len(calls) == 3
    assert sleeps == [2.0, 8.0]

    handler.handle(_record())
    assert len(calls) == 3
    assert sleeps == [2.0, 8.0]


def test_permanent_smtp_failure_does_not_consume_budget(caplog) -> None:
    import logging
    import smtplib

    from src.core.alerts import EmailAlertHandler

    calls: list[str] = []

    def _broken(subject: str, body: str) -> None:
        calls.append(subject)
        raise smtplib.SMTPServerDisconnected("closed")

    handler = EmailAlertHandler(
        component="daemon", run_id="r", sender=_broken, sleep=lambda s: None
    )

    with caplog.at_level(logging.WARNING):
        handler.handle(_record())

    assert len(calls) == 3
    assert handler._sent_today == 0
    assert len([r for r in caplog.records if "stage=alert status=FAIL" in r.getMessage()]) == 1

    with caplog.at_level(logging.WARNING):
        handler.handle(_record())

    assert len(calls) == 6
    assert handler._sent_today == 0


def test_unexpected_formatting_error_keeps_listener_alive() -> None:
    import logging

    from src.core.config import AlertSettings
    from src.core.observability import configure_logging, shutdown_logging

    calls: list[str] = []

    def _flaky(subject: str, body: str, **kw: object) -> None:
        calls.append(subject)
        if len(calls) == 1:
            raise ValueError("formatter exploded")

    configure_logging(
        "daemon",
        alert_settings=AlertSettings(
            alert_gmail_user="u@x", alert_gmail_app_password="pw", alert_gmail_to="t@x"
        ),
        alert_sender=_flaky,
    )
    try:
        log = logging.getLogger("src.orchestration.daemon")
        log.critical("[DAEMON] stage=streamer status=FAIL reason=circuit_open")
        log.critical("[DAEMON] stage=streamer status=FAIL reason=circuit_open")
    finally:
        shutdown_logging()

    assert len(calls) == 2
