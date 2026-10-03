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


def test_html_sender_internal_type_error_not_resent(caplog) -> None:
    import logging

    from src.core.alerts import EmailAlertHandler

    calls: list[tuple] = []

    def _html_sender(subject: str, body: str, *, html_body: str | None = None) -> None:
        calls.append((subject, body, html_body))
        raise TypeError("sender bug after transmit")

    handler = EmailAlertHandler(component="daemon", run_id="r", sender=_html_sender, sleep=lambda s: None)

    with caplog.at_level(logging.WARNING):
        handler.handle(_record())

    assert len(calls) == 1
    assert any("stage=alert status=FAIL" in r.getMessage() for r in caplog.records)


def test_plain_sender_receives_plain_once() -> None:
    from src.core.alerts import _call_sender

    calls: list[tuple] = []

    def _plain(subject, body):
        calls.append((subject, body))

    _call_sender(_plain, "s", "b", html_body="<p>")

    assert calls == [("s", "b")]


def test_kwargs_sender_receives_html() -> None:
    from src.core.alerts import _call_sender

    calls: list[tuple] = []

    def _kwargs(subject, body, **kw):
        calls.append((subject, body, kw))

    _call_sender(_kwargs, "s", "b", html_body="<p>")

    assert calls == [("s", "b", {"html_body": "<p>"})]


def test_unintrospectable_sender_falls_back_plain(monkeypatch) -> None:
    import inspect

    from src.core.alerts import _call_sender

    calls: list[tuple] = []

    def _plain(subject, body):
        calls.append((subject, body))

    monkeypatch.setattr(inspect, "signature", lambda fn: (_ for _ in ()).throw(ValueError("no signature")))
    _call_sender(_plain, "s", "b", html_body="<p>")

    assert calls == [("s", "b")]


def test_positional_only_html_sender_gets_plain() -> None:
    from src.core.alerts import _call_sender

    calls: list[tuple] = []

    def _sender(subject, body, html_body=None, /):
        calls.append((subject, body, html_body))

    _call_sender(_sender, "s", "b", html_body="<p>")

    assert calls == [("s", "b", None)]


def test_keyword_only_html_sender_gets_html() -> None:
    from src.core.alerts import _call_sender

    calls: list[tuple] = []

    def _sender(subject, body, *, html_body=None):
        calls.append((subject, body, html_body))

    _call_sender(_sender, "s", "b", html_body="<p>")

    assert calls == [("s", "b", "<p>")]


def test_per_key_cap_limits_repeating_key(caplog) -> None:
    import datetime as dt
    import logging

    from src.core.alerts import EmailAlertHandler

    calls: list = []
    now = {"t": 0.0, "day": dt.date(2026, 9, 25)}

    def _rec(subject: str, body: str, **kw: object) -> None:
        calls.append(subject)

    handler = EmailAlertHandler(
        component="daemon",
        run_id="r",
        sender=_rec,
        cooldown_s=0,
        clock=lambda: now["t"],
        today=lambda: now["day"],
        sleep=lambda s: None,
    )
    with caplog.at_level(logging.WARNING):
        for _ in range(5):
            handler.handle(_record("same-key"))

    assert len(calls) == 3
    suppressed = [r for r in caplog.records if "status=SUPPRESSED reason=per_key_cap" in r.getMessage()]
    assert len(suppressed) == 1


def test_repeat_budget_preserves_first_occurrence_slots() -> None:
    import datetime as dt

    from src.core.alerts import EmailAlertHandler

    calls: list = []
    now = {"t": 0.0, "day": dt.date(2026, 9, 25)}

    def _rec(subject: str, body: str, **kw: object) -> None:
        calls.append(subject)

    handler = EmailAlertHandler(
        component="daemon",
        run_id="r",
        sender=_rec,
        cooldown_s=0,
        daily_cap=20,
        per_key_daily_cap=20,
        first_occurrence_reserve=5,
        clock=lambda: now["t"],
        today=lambda: now["day"],
        sleep=lambda s: None,
    )
    for _ in range(30):
        handler.handle(_record("repeat-key"))
    for i in range(5):
        handler.handle(_record(f"new-key-{i}"))

    assert handler._per_key_counts.get("repeat-key") == 15
    assert handler._sent_today == 20
    assert len(calls) == 20


def test_first_occurrences_may_use_reserved_slots(caplog) -> None:
    import datetime as dt
    import logging

    from src.core.alerts import EmailAlertHandler

    calls: list = []
    now = {"t": 0.0, "day": dt.date(2026, 9, 25)}

    def _rec(subject: str, body: str, **kw: object) -> None:
        calls.append(subject)

    handler = EmailAlertHandler(
        component="daemon",
        run_id="r",
        sender=_rec,
        cooldown_s=0,
        daily_cap=20,
        per_key_daily_cap=20,
        first_occurrence_reserve=5,
        clock=lambda: now["t"],
        today=lambda: now["day"],
        sleep=lambda s: None,
    )
    for _ in range(30):
        handler.handle(_record("repeat-key"))
    assert handler._sent_today == 15
    with caplog.at_level(logging.WARNING):
        caplog.clear()
        for i in range(5):
            handler.handle(_record(f"fresh-{i}"))
        assert handler._sent_today == 20
        assert len(calls) == 20
        handler.handle(_record("fresh-overflow"))
        assert handler._sent_today == 20
        assert len(calls) == 20
    assert not [r for r in caplog.records if "status=SUPPRESSED" in r.getMessage()]


def test_outage_replay_keeps_later_critical_deliverable() -> None:
    import datetime as dt

    from src.core.alerts import ALERT_PER_KEY_DAILY_CAP, EmailAlertHandler

    calls: list = []
    day = dt.date(2026, 9, 25)
    now = {"t": 0.0}

    def _rec(subject: str, body: str, **kw: object) -> None:
        calls.append(body)

    handler = EmailAlertHandler(
        component="daemon",
        run_id="r",
        sender=_rec,
        clock=lambda: now["t"],
        today=lambda: day,
        sleep=lambda s: None,
    )
    key_a = "[DAEMON] stage=streamer status=FAIL reason=circuit_open lane=regular"
    key_b = "[DAEMON] stage=snapshots status=FAIL reason=circuit_open lane=snapshot"
    delivered = {key_a: 0, key_b: 0}
    for i in range(29):
        now["t"] = float(i * 1850)
        key = key_a if i % 2 == 0 else key_b
        before = len(calls)
        handler.handle(_record(key))
        delivered[key] += len(calls) - before
    assert 1 <= delivered[key_a] <= ALERT_PER_KEY_DAILY_CAP
    assert 1 <= delivered[key_b] <= ALERT_PER_KEY_DAILY_CAP

    now["t"] = 60000.0
    eod_key = "[DAEMON] stage=eod_reconciliation status=FAIL reason=unverified"
    before = len(calls)
    handler.handle(_record(eod_key))
    assert len(calls) == before + 1

    now["t"] = 80000.0
    after_key = "[DAEMON] stage=aftermarket_plan status=FAIL reason=circuit_open"
    before = len(calls)
    handler.handle(_record(after_key))
    assert len(calls) == before + 1


def test_day_rollover_resets_per_key_state(caplog) -> None:
    import datetime as dt
    import logging

    from src.core.alerts import EmailAlertHandler

    calls: list = []
    now = {"t": 0.0, "day": dt.date(2026, 9, 25)}

    def _rec(subject: str, body: str, **kw: object) -> None:
        calls.append(subject)

    handler = EmailAlertHandler(
        component="daemon",
        run_id="r",
        sender=_rec,
        cooldown_s=0,
        clock=lambda: now["t"],
        today=lambda: now["day"],
        sleep=lambda s: None,
    )
    with caplog.at_level(logging.WARNING):
        for _ in range(4):
            handler.handle(_record("roll-key"))
        assert len(calls) == 3
        assert len([r for r in caplog.records if "status=SUPPRESSED" in r.getMessage()]) == 1
        now["day"] = dt.date(2026, 9, 26)
        handler.handle(_record("roll-key"))
        assert len(calls) == 4
        handler.handle(_record("roll-key"))
        handler.handle(_record("roll-key"))
        assert len(calls) == 6
        handler.handle(_record("roll-key"))
        assert len(calls) == 6
        assert len([r for r in caplog.records if "status=SUPPRESSED" in r.getMessage()]) == 2


def test_failed_sends_consume_no_per_key_budget() -> None:
    import datetime as dt
    import smtplib

    from src.core.alerts import ALERT_PER_KEY_DAILY_CAP, EmailAlertHandler

    calls: list = []
    state = {"fail": True}
    now = {"t": 0.0, "day": dt.date(2026, 9, 25)}

    def _rec(subject: str, body: str, **kw: object) -> None:
        calls.append(subject)
        if state["fail"]:
            raise smtplib.SMTPServerDisconnected("closed")

    handler = EmailAlertHandler(
        component="daemon",
        run_id="r",
        sender=_rec,
        cooldown_s=0,
        clock=lambda: now["t"],
        today=lambda: now["day"],
        sleep=lambda s: None,
    )
    for _ in range(ALERT_PER_KEY_DAILY_CAP + 1):
        handler.handle(_record("flaky-key"))
    assert handler._sent_today == 0
    assert handler._per_key_counts.get("flaky-key", 0) == 0
    state["fail"] = False
    handler.handle(_record("flaky-key"))
    assert handler._per_key_counts.get("flaky-key") == 1
    assert handler._sent_today == 1


def test_cooldown_still_applies_under_caps(caplog) -> None:
    import datetime as dt
    import logging

    from src.core.alerts import EmailAlertHandler

    calls: list = []
    now = {"t": 0.0, "day": dt.date(2026, 9, 25)}

    def _rec(subject: str, body: str, **kw: object) -> None:
        calls.append(subject)

    handler = EmailAlertHandler(
        component="daemon",
        run_id="r",
        sender=_rec,
        clock=lambda: now["t"],
        today=lambda: now["day"],
        sleep=lambda s: None,
    )
    with caplog.at_level(logging.WARNING):
        handler.handle(_record("cool-key"))
        now["t"] = 10.0
        handler.handle(_record("cool-key"))

    assert len(calls) == 1
    assert not [r for r in caplog.records if "status=SUPPRESSED" in r.getMessage()]


def test_invalid_limits_fail_fast() -> None:
    import pytest

    from src.core.alerts import EmailAlertHandler

    def _rec(subject: str, body: str, **kw: object) -> None:
        pass

    with pytest.raises(ValueError, match="per_key_daily_cap"):
        EmailAlertHandler(component="d", run_id="r", sender=_rec, per_key_daily_cap=0)
    with pytest.raises(ValueError, match="first_occurrence_reserve"):
        EmailAlertHandler(component="d", run_id="r", sender=_rec, first_occurrence_reserve=-1)
    with pytest.raises(ValueError, match="first_occurrence_reserve"):
        EmailAlertHandler(component="d", run_id="r", sender=_rec, daily_cap=20, first_occurrence_reserve=20)
