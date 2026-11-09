def _record(msg="boom", kind=None):
    import logging

    from src.core.alerts import PageKind

    record = logging.LogRecord("x", logging.CRITICAL, __file__, 1, msg, (), None)
    record.krx_page = kind or PageKind.REGULAR_FEED_DOWN
    return record


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

    from src.core.alerts import PageKind, page

    configure_logging(
        "daemon",
        alert_settings=AlertSettings(
            alert_gmail_user="u@x", alert_gmail_app_password="pw", alert_gmail_to="t@x"
        ),
        alert_sender=_flaky,
    )
    try:
        log = logging.getLogger("src.orchestration.daemon")
        log.critical("[DAEMON] stage=streamer status=FAIL reason=circuit_open", extra=page(PageKind.REGULAR_FEED_DOWN))
        log.critical("[DAEMON] stage=streamer status=FAIL reason=circuit_open", extra=page(PageKind.REGULAR_FEED_DOWN))
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
        per_key_daily_cap=3,
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
        handler.handle(_record(f"new-key-{chr(97 + i)}"))

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
            handler.handle(_record(f"fresh-{chr(97 + i)}"))
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
        per_key_daily_cap=3,
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
        per_key_daily_cap=3,
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


def _shard_budget_record(key="nxt:1", silent=240):

    msg = (
        "[DATA] stage=shard_watchdog status=FAIL reason=stale_budget_exhausted "
        f"key={key} silent_s={silent} limit_s=120 max_restarts=3"
    )
    return _record(msg)


def test_page_subject_and_three_line_body_hide_raw_message() -> None:
    from src.core.alerts import PAGE_TEXTS, PageKind, format_page_body, format_page_subject

    record = _record("[DATA] stage=x path=/srv/data/secret run_id=daemon-123 token=abc", PageKind.DISK_LOW)
    subject = format_page_subject(PageKind.DISK_LOW)
    lines = format_page_body(PageKind.DISK_LOW, record).splitlines()

    assert subject == f"[긴급][krx] {PAGE_TEXTS[PageKind.DISK_LOW].summary}"
    assert [line.split(": ", 1)[0] for line in lines] == ["무슨 일", "자동 조치", "지금 할 일"]
    assert lines[0].endswith(" KST)")
    assert not any(token in "\n".join(lines) for token in ("/srv", "daemon-123", "abc", "stage="))


def test_page_deadline_appended_to_operator_action() -> None:
    import datetime as dt

    from src.core.alerts import PAGE_TEXTS, PageKind, format_page_body, page

    record = _record("m", PageKind.REPAIR_HOLD_EXPIRING)
    record.__dict__.update(page(PageKind.REPAIR_HOLD_EXPIRING, deadline=dt.date(2026, 11, 5)))
    action = format_page_body(PageKind.REPAIR_HOLD_EXPIRING, record).splitlines()[2]
    assert action == f"지금 할 일: {PAGE_TEXTS[PageKind.REPAIR_HOLD_EXPIRING].operator_action} (기한: 2026-11-05)"


def test_page_text_table_covers_every_kind() -> None:
    from src.core.alerts import PAGE_TEXTS, PageKind

    assert set(PAGE_TEXTS) == set(PageKind)
    assert all(t.summary and t.auto_action and t.operator_action for t in PAGE_TEXTS.values())


def test_resolution_mails_once_only_after_own_page() -> None:
    import logging

    from src.core.alerts import EmailAlertHandler, PageKind, page_resolved

    calls: list[tuple[str, str]] = []
    handler = EmailAlertHandler(
        component="daemon", run_id="r", sender=lambda s, b: calls.append((s, b)), cooldown_s=0, sleep=lambda s: None
    )

    def _clear() -> logging.LogRecord:
        rec = logging.LogRecord("x", logging.WARNING, __file__, 1, "[DATA] recovered", (), None)
        rec.__dict__.update(page_resolved(PageKind.NO_FIRST_FRAME))
        return rec

    handler.handle(_clear())
    assert calls == []
    handler.handle(_record("[DAEMON] stage=ingest_watchdog status=NO_FIRST_FRAME", PageKind.NO_FIRST_FRAME))
    handler.handle(_clear())
    handler.handle(_clear())
    assert [s for s, _ in calls] == ["[긴급][krx] 정규장 개장 후 첫 시세 미수신", "[해결됨][krx] 정규장 개장 후 첫 시세 미수신"]
    assert len(calls[1][1].splitlines()) == 1
    assert calls[1][1].startswith("해결됨: ")


def test_shard_dedup_key_by_venue_not_index_or_timing() -> None:
    from src.core.alerts import alert_dedup_key

    a = alert_dedup_key(_shard_budget_record(key="nxt:0", silent=240).getMessage())
    b = alert_dedup_key(_shard_budget_record(key="nxt:1", silent=980).getMessage())
    c = alert_dedup_key(_shard_budget_record(key="krx:0", silent=240).getMessage())
    assert a == b
    assert a != c


def test_handler_second_shard_reserves_its_own_mail() -> None:
    import datetime as dt

    from src.core.alerts import EmailAlertHandler, PageKind, page

    calls: list[str] = []
    handler = EmailAlertHandler(
        component="daemon",
        run_id="r",
        sender=lambda s, b: calls.append(b),
        cooldown_s=0,
        today=lambda: dt.date(2026, 11, 9),
        sleep=lambda s: None,
        per_key_daily_cap=1,
    )

    def _shard(shard: str) -> None:
        import logging

        record = logging.LogRecord(
            "x",
            logging.CRITICAL,
            __file__,
            1,
            f"[DATA] stage=regular_kis_stream status=FAIL reason=circuit_open shard={shard} date=2026-11-09",
            (),
            None,
        )
        record.__dict__.update(page(PageKind.REGULAR_FEED_DOWN))
        handler.handle(record)

    _shard("krx-00")
    _shard("krx-02")
    _shard("krx-00")
    assert len(calls) == 2
    assert "[krx-00]" in calls[0]
    assert "[krx-02]" in calls[1]


def test_body_names_identifier_and_stays_three_lines() -> None:
    import logging

    from src.core.alerts import PageKind, format_page_body, page

    record = logging.LogRecord(
        "x",
        logging.CRITICAL,
        __file__,
        1,
        "[DATA] stage=regular_kis_stream status=FAIL reason=circuit_open shard=krx-02 path=/srv/x",
        (),
        None,
    )
    record.__dict__.update(page(PageKind.REGULAR_FEED_DOWN))
    lines = format_page_body(PageKind.REGULAR_FEED_DOWN, record).splitlines()
    assert len(lines) == 3
    assert [line.split(": ", 1)[0] for line in lines] == ["무슨 일", "자동 조치", "지금 할 일"]
    assert "[krx-02] (" in lines[0]
    assert lines[0].endswith(" KST)")
    assert "/srv" not in "\n".join(lines)


def test_body_without_identifier_is_unchanged() -> None:
    import logging

    from src.core.alerts import PAGE_TEXTS, PageKind, format_page_body, page

    record = logging.LogRecord("x", logging.CRITICAL, __file__, 1, "[DATA] stage=x rows=123", (), None)
    record.__dict__.update(page(PageKind.DISK_LOW))
    lines = format_page_body(PageKind.DISK_LOW, record).splitlines()
    assert lines[0].startswith(f"무슨 일: {PAGE_TEXTS[PageKind.DISK_LOW].summary} (")
    assert "[" not in lines[0]


def test_shard_budget_daily_cap_respected(caplog) -> None:
    import datetime as dt
    import logging

    from src.core.alerts import EmailAlertHandler, InMemoryAlertLedger

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
        per_key_daily_cap=3,
        ledger=InMemoryAlertLedger(),
    )
    with caplog.at_level(logging.WARNING):
        for i in range(4):
            now["t"] = float(i * 2000)
            handler.handle(_shard_budget_record())
    assert len(calls) == 3


def _latency_breach_record(stream="H0STCNT0", minute="09:01", max_s=32.0):
    import logging

    msg = (
        "[DATA] stage=ingest_latency status=FAIL reason=latency_exceeded "
        f"date=2026-10-15 stream={stream} minute={minute} max_s={max_s:.1f} p50_s=12.0 threshold_s=10.0 breach_minutes=1"
    )
    return logging.LogRecord("x", logging.CRITICAL, __file__, 1, msg, (), None)


def test_latency_dedup_key_by_stream_not_minute_or_value() -> None:
    from src.core.alerts import alert_dedup_key

    a = alert_dedup_key(_latency_breach_record(stream="H0STCNT0", minute="09:01", max_s=32.0).getMessage())
    b = alert_dedup_key(_latency_breach_record(stream="H0STCNT0", minute="09:03", max_s=15.0).getMessage())
    c = alert_dedup_key(_latency_breach_record(stream="H0STASP0", minute="09:01", max_s=32.0).getMessage())
    assert a == b
    assert a != c


def test_unmarked_critical_records_never_mail() -> None:
    import datetime as dt
    import logging

    from src.core.alerts import EmailAlertHandler, InMemoryAlertLedger

    calls: list = []
    now = {"t": 0.0, "day": dt.date(2026, 10, 15)}

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
        ledger=InMemoryAlertLedger(),
    )
    gap_msg = "[DAEMON] stage=eod_maintenance status=FAIL reason=session_data_gap date=2026-10-15"
    gap_record = logging.LogRecord("x", logging.CRITICAL, __file__, 1, gap_msg, (), None)
    handler.handle(_latency_breach_record("H0STCNT0"))
    handler.handle(_latency_breach_record("H0STASP0"))
    handler.handle(gap_record)
    assert calls == []
    assert handler._sent_today == 0


def test_default_policy_mails_each_failure_kind_once_per_day() -> None:
    import datetime as dt

    from src.core.alerts import ALERT_PER_KEY_DAILY_CAP, EmailAlertHandler

    calls: list[str] = []
    now = {"t": 0.0, "day": dt.date(2026, 9, 25)}
    handler = EmailAlertHandler(
        component="daemon",
        run_id="r",
        sender=lambda subject, body: calls.append(subject),
        clock=lambda: now["t"],
        today=lambda: now["day"],
        sleep=lambda s: None,
    )
    for i in range(4):
        now["t"] = float(i * 4000)
        handler.handle(_record("repeat-key"))
    handler.handle(_record("other-key"))
    assert ALERT_PER_KEY_DAILY_CAP == 1
    assert len(calls) == 2
    now["day"] = dt.date(2026, 9, 26)
    handler.handle(_record("repeat-key"))
    assert len(calls) == 3


def _ls_shard_record(msg):
    from src.core.alerts import PageKind

    return _record(msg, kind=PageKind.LS_SHARD_STALE)


def test_ls_shard_stale_page_text_present() -> None:
    from src.core.alerts import PAGE_TEXTS, PageKind

    text = PAGE_TEXTS[PageKind.LS_SHARD_STALE]
    assert "시간외" not in " ".join([text.summary, text.auto_action, text.operator_action])


def test_ls_shard_pages_dedup_per_shard() -> None:
    from src.core.alerts import alert_dedup_key

    base = "[DATA] stage=shard_watchdog status=FAIL reason=ls_shard_silent shard=%s silent_s=%d limit_s=900 date=2026-11-09"
    a = alert_dedup_key(base % ("ls-00", 1000))
    b = alert_dedup_key(base % ("ls-01", 1100))
    assert a != b
    assert alert_dedup_key(base % ("ls-01", 1100)) == alert_dedup_key(base % ("ls-01", 2400))


def test_ls_venue_wide_key_distinct() -> None:
    from src.core.alerts import alert_dedup_key

    venue = alert_dedup_key(
        "[DATA] stage=shard_watchdog status=FAIL reason=ls_venue_wide_silent venue=krx silent_s=2000 limit_s=900 date=2026-11-09"
    )
    shard = alert_dedup_key(
        "[DATA] stage=shard_watchdog status=FAIL reason=ls_shard_silent shard=ls-01 silent_s=1000 limit_s=900 date=2026-11-09"
    )
    assert venue != shard



def test_ls_shard_pages_reserve_per_shard_and_repeat_suppressed() -> None:
    import datetime as dt
    import logging

    from src.core.alerts import EmailAlertHandler, PageKind, page

    calls: list[str] = []
    handler = EmailAlertHandler(
        component="daemon",
        run_id="r",
        sender=lambda s, b: calls.append(b),
        cooldown_s=0,
        today=lambda: dt.date(2026, 11, 9),
        sleep=lambda s: None,
        per_key_daily_cap=1,
    )

    def _page(shard: str, silent: int) -> None:
        record = logging.LogRecord(
            "x",
            logging.CRITICAL,
            __file__,
            1,
            f"[DATA] stage=shard_watchdog status=FAIL reason=ls_shard_silent shard={shard} silent_s={silent} limit_s=900 date=2026-11-09",
            (),
            None,
        )
        record.__dict__.update(page(PageKind.LS_SHARD_STALE))
        handler.handle(record)

    _page("ls-00", 1000)
    _page("ls-01", 1100)
    _page("ls-01", 2400)
    assert len(calls) == 2
    assert "[ls-00]" in calls[0]
    assert "[ls-01]" in calls[1]
