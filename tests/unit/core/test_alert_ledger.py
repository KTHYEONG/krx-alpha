import datetime as dt
import json
import logging
import pathlib
import select
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager

import pytest

import src.core.alerts as alerts_mod

from src.core.alerts import (
    AlertLimits,
    AlertDecision,
    EmailAlertHandler,
    FileAlertLedger,
    InMemoryAlertLedger,
    alert_dedup_key,
)

DAY = dt.date(2026, 10, 5)
LIMITS = AlertLimits(cooldown_s=0, daily_cap=20, per_key_daily_cap=20, first_occurrence_reserve=0)


@pytest.fixture(autouse=True)
def _isolated_degraded_warning(monkeypatch) -> None:
    monkeypatch.setattr(alerts_mod, "_DEGRADED_PID", None)


def _release(ledger, key: str, decision: AlertDecision, *, day: dt.date = DAY) -> None:
    ledger.release(
        key, day=day, previous_last_sent_s=decision.previous_last_sent_s,
        reservation_id=decision.reservation_id,
    )


def _commit(ledger, key: str, decision: AlertDecision, *, day: dt.date = DAY) -> None:
    ledger.commit(key, day=day, reservation_id=decision.reservation_id)


def _state(path: pathlib.Path):
    return json.loads(path.read_text(encoding="utf-8"))


@contextmanager
def _concurrent_children(code: str, ledger: pathlib.Path, *, count: int = 2) -> Iterator[list[subprocess.Popen[str]]]:
    children: list[subprocess.Popen[str]] = []
    try:
        for _ in range(count):
            children.append(subprocess.Popen(  # noqa: PERF401, S603 - retain started children if a later spawn fails
                [sys.executable, "-c", code, str(ledger)], stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            ))
        for child in children:
            assert child.stdout is not None
            ready, _, _ = select.select([child.stdout], [], [], 10)
            assert ready, "child did not reach the start barrier"
            assert child.stdout.readline().strip() == "READY"
        for child in children:
            assert child.stdin is not None
            child.stdin.write("GO\n")
            child.stdin.flush()
        yield children
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
            child.communicate(timeout=10)


def _rec(msg: str) -> logging.LogRecord:
    return logging.LogRecord("x", logging.CRITICAL, __file__, 1, msg, (), None)


def test_dedup_ignores_numeric_variation() -> None:
    m1 = "[DATA] stage=quality tr_id=H0STCNT0 vendor=kis rows=123 fail=3/10 ratio=0.30 dt=2026-10-04"
    m2 = "[DATA] stage=quality tr_id=H0STCNT0 vendor=kis rows=4567 fail=11/20 ratio=0.15 dt=2026-10-05"
    assert alert_dedup_key(m1) == alert_dedup_key(m2)


def test_dedup_keeps_distinct_kinds_apart() -> None:
    base = "[DATA] stage=quality tr_id=H0STCNT0 vendor=kis rows=123 fail=3 cnt_a=1"
    other_tr = "[DATA] stage=quality tr_id=H0NXCNT0 vendor=kis rows=123 fail=3 cnt_a=1"
    other_vendor = "[DATA] stage=quality tr_id=H0STCNT0 vendor=ls rows=123 fail=3 cnt_a=1"
    other_counter = "[DATA] stage=quality tr_id=H0STCNT0 vendor=kis rows=123 fail=3 cnt_b=1"
    assert alert_dedup_key(base) != alert_dedup_key(other_tr)
    assert alert_dedup_key(base) != alert_dedup_key(other_vendor)
    assert alert_dedup_key(base) != alert_dedup_key(other_counter)
    assert "H0STCNT0" in alert_dedup_key(base)
    assert "0035S0" in alert_dedup_key("tr_id=0035S0 rows=1")


def test_dedup_bounded() -> None:
    assert len(alert_dedup_key("x" * 5000)) <= 200


def test_limits_validate() -> None:
    import pytest

    with pytest.raises(ValueError, match="per_key_daily_cap"):
        AlertLimits(per_key_daily_cap=0)
    with pytest.raises(ValueError, match="first_occurrence_reserve"):
        AlertLimits(daily_cap=20, first_occurrence_reserve=20)
    with pytest.raises(ValueError, match="first_occurrence_reserve"):
        AlertLimits(daily_cap=20, first_occurrence_reserve=-1)


def test_two_handlers_share_one_budget(tmp_path: pathlib.Path) -> None:
    p = tmp_path / "ledger.json"
    ledger = FileAlertLedger(p)
    day = dt.date(2026, 10, 5)
    c1: list[str] = []
    c2: list[str] = []
    h1 = EmailAlertHandler(
        component="d", run_id="r", sender=lambda s, b, **kw: c1.append(s),
        cooldown_s=0, clock=lambda: 1000.0, today=lambda: day, sleep=lambda s: None, ledger=ledger,
    )
    h2 = EmailAlertHandler(
        component="d", run_id="r", sender=lambda s, b, **kw: c2.append(s),
        cooldown_s=0, clock=lambda: 1000.0, today=lambda: day, sleep=lambda s: None, ledger=ledger,
    )
    for _ in range(5):
        h1.handle(_rec("[DATA] stage=quality tr_id=H0STCNT0 vendor=kis rows=1"))
        h2.handle(_rec("[DATA] stage=quality tr_id=H0STCNT0 vendor=kis rows=999"))
    assert len(c1) + len(c2) == 3
    assert _state(p)["sent_today"] == 3
    assert _state(p)["reservations"] == {}


@pytest.mark.parametrize(("distinct_keys", "expected"), [(False, 3), (True, 5)])
def test_cross_process_budget(tmp_path: pathlib.Path, distinct_keys: bool, expected: int) -> None:
    ledger = tmp_path / "shared.json"
    code = f"""
import datetime as dt, json, logging, pathlib, sys, time
from src.core.alerts import EmailAlertHandler, FileAlertLedger
deliveries = []
handler = EmailAlertHandler(
    component="child", run_id="r", ledger=FileAlertLedger(pathlib.Path(sys.argv[1])),
    sender=lambda subject, body: deliveries.append(body), clock=time.time,
    today=lambda: dt.date(2026,10,5), cooldown_s=0, daily_cap=5,
    per_key_daily_cap=3, first_occurrence_reserve=0,
)
print("READY", flush=True)
sys.stdin.readline()
for i in range(20):
    kind = chr(97+i) if {distinct_keys!r} else "shared"
    handler.handle(logging.LogRecord("x", logging.CRITICAL, "f", 1, f"stage=quality kind={{kind}} rows={{i}}", (), None))
print(json.dumps({{"delivered":len(deliveries)}}), flush=True)
"""
    with _concurrent_children(code, ledger) as children:
        total = 0
        for child in children:
            stdout, stderr = child.communicate(timeout=15)
            assert child.returncode == 0, stderr
            total += json.loads(stdout)["delivered"]
    data = _state(ledger)
    assert total == expected
    assert data["sent_today"] == expected
    assert all(b["confirmed_count"] <= 3 for b in data["keys"].values())
    assert data["reservations"] == {}


def test_cooldown_wall_time(tmp_path: pathlib.Path) -> None:
    p = tmp_path / "c.json"
    day = dt.date(2026, 10, 5)
    lim = AlertLimits(cooldown_s=10, daily_cap=20, per_key_daily_cap=20, first_occurrence_reserve=0)
    led = FileAlertLedger(p)
    assert led.reserve("k", day=day, now_s=100.0, limits=lim).send is True
    led2 = FileAlertLedger(p)
    d2 = led2.reserve("k", day=day, now_s=109.0, limits=lim)
    assert d2.send is False
    assert d2.suppressed_reason == "cooldown"
    d3 = FileAlertLedger(p).reserve("k", day=day, now_s=111.0, limits=lim)
    assert d3.send is True


def test_day_rollover_resets(tmp_path: pathlib.Path) -> None:
    p = tmp_path / "r.json"
    lim = AlertLimits(cooldown_s=0, daily_cap=20, per_key_daily_cap=20, first_occurrence_reserve=0)
    led = FileAlertLedger(p)
    led.reserve("k", day=dt.date(2026, 10, 5), now_s=100.0, limits=lim)
    d = led.reserve("k", day=dt.date(2026, 10, 6), now_s=200.0, limits=lim)
    assert d.send is True
    data = json.loads(p.read_text(encoding="utf-8"))
    assert data["day"] == "2026-10-06"
    assert data["sent_today"] == 1


def test_corrupt_degrades_to_empty(tmp_path: pathlib.Path, caplog) -> None:
    p = tmp_path / "corr.json"
    p.write_text("garbage!!!", encoding="utf-8")
    led = FileAlertLedger(p)
    lim = AlertLimits(cooldown_s=0, daily_cap=20, per_key_daily_cap=20, first_occurrence_reserve=0)
    with caplog.at_level(logging.WARNING):
        d = led.reserve("k", day=dt.date(2026, 10, 5), now_s=300.0, limits=lim)
    assert d.send is True
    assert json.loads(p.read_text(encoding="utf-8"))["sent_today"] == 1
    assert len([r for r in caplog.records if "stage=alert_ledger status=DEGRADED" in r.getMessage()]) == 1
    p.write_text(json.dumps({"version": 999, "day": "2026-10-05", "sent_today": 0, "keys": {}}))
    led2 = FileAlertLedger(p)
    with caplog.at_level(logging.WARNING):
        caplog.clear()
        assert led2.reserve("k", day=dt.date(2026, 10, 5), now_s=400.0, limits=lim).send is True
    assert not [r for r in caplog.records if "stage=alert_ledger status=DEGRADED" in r.getMessage()]


def test_unwritable_degrades_open(tmp_path: pathlib.Path, caplog) -> None:
    blocker = tmp_path / "file"
    blocker.write_text("x")
    bad = blocker / "ledger.json"
    led = FileAlertLedger(bad, lock_timeout_s=0.3)
    lim = AlertLimits(cooldown_s=0, daily_cap=20, per_key_daily_cap=20, first_occurrence_reserve=0)
    t0 = time.monotonic()
    with caplog.at_level(logging.WARNING):
        d = led.reserve("k", day=dt.date(2026, 10, 5), now_s=600.0, limits=lim)
    assert d.send is True
    assert time.monotonic() - t0 <= 0.3 + 0.5
    assert any("stage=alert_ledger status=DEGRADED" in r.getMessage() for r in caplog.records)
    with caplog.at_level(logging.WARNING):
        caplog.clear()
        assert led.reserve("k", day=dt.date(2026, 10, 5), now_s=601.0, limits=lim).send is True
    assert not [r for r in caplog.records if "stage=alert_ledger status=DEGRADED" in r.getMessage()]


def test_lock_timeout_degrades_open(tmp_path: pathlib.Path) -> None:
    import fcntl

    p = tmp_path / "lock.json"
    led = FileAlertLedger(p, lock_timeout_s=0.3)
    lock_path = pathlib.Path(str(p) + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "w") as lf:
        fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
        t0 = time.monotonic()
        d = led.reserve(
            "k", day=dt.date(2026, 10, 5), now_s=700.0,
            limits=AlertLimits(cooldown_s=0, daily_cap=20, per_key_daily_cap=20, first_occurrence_reserve=0),
        )
        assert d.send is True
        assert time.monotonic() - t0 <= 0.3 + 0.5


def test_release_restores(tmp_path: pathlib.Path) -> None:
    p = tmp_path / "rel.json"
    day = dt.date(2026, 10, 5)
    lim = AlertLimits(cooldown_s=0, daily_cap=20, per_key_daily_cap=20, first_occurrence_reserve=0)
    led = FileAlertLedger(p)
    r1 = led.reserve("rk", day=day, now_s=400.0, limits=lim)
    _release(led, "rk", r1)
    data = json.loads(p.read_text(encoding="utf-8"))
    assert data["sent_today"] == 0
    assert data["keys"].get("rk", {}).get("count", 0) == 0
    mem = InMemoryAlertLedger()
    m1 = mem.reserve("rk", day=day, now_s=400.0, limits=lim)
    _release(mem, "rk", m1)
    assert mem._sent_today == 0
    assert mem._per_key_counts.get("rk", 0) == 0
    a = mem.reserve("multi", day=day, now_s=500.0, limits=lim)
    assert a.previous_last_sent_s is None
    _commit(mem, "multi", a)
    b = mem.reserve("multi", day=day, now_s=510.0, limits=lim)
    assert b.previous_last_sent_s == 500.0
    _release(mem, "multi", b)
    assert mem._per_key_counts.get("multi") == 1
    check = mem.reserve("multi", day=day, now_s=520.0, limits=lim)
    assert check.previous_last_sent_s == 500.0
    initial = led.reserve("existing", day=day, now_s=600.0, limits=lim)
    _commit(led, "existing", initial)
    before = _state(p)
    repeated = led.reserve("existing", day=day, now_s=610.0, limits=lim)
    _release(FileAlertLedger(p), "existing", repeated)
    assert _state(p) == before


def test_ledger_io_failures_degrade(tmp_path: pathlib.Path, caplog) -> None:
    lim = AlertLimits(cooldown_s=0, daily_cap=20, per_key_daily_cap=20, first_occurrence_reserve=0)
    day = dt.date(2026, 10, 5)
    # Lock open failure: sidecar path is a directory.
    p = tmp_path / "locked.json"
    pathlib.Path(str(p) + ".lock").mkdir()
    with caplog.at_level(logging.WARNING):
        assert FileAlertLedger(p).reserve("k", day=day, now_s=100.0, limits=lim).send is True
    assert any("stage=alert_ledger status=DEGRADED" in r.getMessage() for r in caplog.records)
    # Ledger path is a directory: read raises OSError.
    d = tmp_path / "dir-ledger.json"
    d.mkdir()
    with caplog.at_level(logging.WARNING):
        caplog.clear()
        assert FileAlertLedger(d).reserve("k", day=day, now_s=200.0, limits=lim).send is True
    # Non-dict JSON treated as empty.
    j = tmp_path / "list.json"
    j.write_text("[]", encoding="utf-8")
    led = FileAlertLedger(j)
    with caplog.at_level(logging.WARNING):
        caplog.clear()
        assert led.reserve("k", day=day, now_s=300.0, limits=lim).send is True
    # Every malformed shape degrades to empty state and recovers.
    bad_shapes = [
        {"version": 1, "day": 5, "sent_today": 0, "keys": {}},
        {"version": 1, "day": "not-a-date", "sent_today": 0, "keys": {}},
        {"version": 1, "day": "2026-10-05", "sent_today": -1, "keys": {}},
        {"version": 1, "day": "2026-10-05", "sent_today": "1", "keys": {}},
        {"version": 1, "day": "2026-10-05", "sent_today": 0, "keys": []},
        {"version": 1, "day": "2026-10-05", "sent_today": 0, "keys": {"k": []}},
        {"version": 1, "day": "2026-10-05", "sent_today": 0, "keys": {"k": {"count": -1, "last_sent_s": 1.0}}},
        {"version": 1, "day": "2026-10-05", "sent_today": 0, "keys": {"k": {"count": 1, "last_sent_s": "x"}}},
    ]
    for i, shape in enumerate(bad_shapes):
        fp = tmp_path / f"bad-{i}.json"
        fp.write_text(json.dumps(shape), encoding="utf-8")
        assert FileAlertLedger(fp).reserve("k", day=day, now_s=400.0 + i, limits=lim).send is True
        assert json.loads(fp.read_text(encoding="utf-8"))["sent_today"] == 1


def test_ledger_save_failure_degrades(tmp_path: pathlib.Path, monkeypatch, caplog) -> None:
    import os as _os

    p = tmp_path / "save-fail.json"
    led = FileAlertLedger(p)
    lim = AlertLimits(cooldown_s=0, daily_cap=20, per_key_daily_cap=20, first_occurrence_reserve=0)
    monkeypatch.setattr(_os, "replace", lambda *a, **kw: (_ for _ in ()).throw(OSError("disk full")))
    with caplog.at_level(logging.WARNING):
        assert led.reserve("k", day=dt.date(2026, 10, 5), now_s=500.0, limits=lim).send is True
    assert any("stage=alert_ledger status=DEGRADED" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("operation", ["commit", "release"])
@pytest.mark.parametrize("failure", ["lock", "read", "save"])
def test_ledger_release_failures_degrade(tmp_path: pathlib.Path, monkeypatch, caplog, operation: str, failure: str) -> None:
    path = tmp_path / "finish.json"
    ledger = FileAlertLedger(path)
    decision = ledger.reserve("k", day=DAY, now_s=100.0, limits=LIMITS)
    before = path.read_bytes()
    method = {"lock": "_locked", "read": "_load_unlocked", "save": "_save_unlocked"}[failure]
    monkeypatch.setattr(ledger, method, lambda *a, **kw: (_ for _ in ()).throw(OSError("unavailable")))
    with caplog.at_level(logging.WARNING):
        if operation == "commit":
            _commit(ledger, "k", decision)
        else:
            _release(ledger, "k", decision)
    assert path.read_bytes() == before
    assert len([r for r in caplog.records if "stage=alert_ledger status=DEGRADED" in r.getMessage()]) == 1


def test_no_secrets_persisted(tmp_path: pathlib.Path, monkeypatch) -> None:
    import smtplib

    from src.core.alerts import gmail_sender
    from src.core.config import AlertSettings

    settings = AlertSettings(
        alert_gmail_user="ledger-user@example.test", alert_gmail_app_password="private-app-token-xyz",
        alert_gmail_to="ledger-recipient@example.test",
    )
    logins = []

    class FakeSMTP:
        def __init__(self, *args, **kwargs):
            self.sent = []

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return None

        def login(self, user, password):
            logins.append((user, password))

        def send_message(self, message):
            self.sent.append(message)

    monkeypatch.setattr(smtplib, "SMTP_SSL", FakeSMTP)
    p = tmp_path / "s.json"
    handler = EmailAlertHandler(
        component="daemon", run_id="private-run-id", sender=gmail_sender(settings), ledger=FileAlertLedger(p),
        clock=lambda: 800.0, today=lambda: DAY,
    )
    handler.handle(_rec("[DATA] stage=quality rows=123"))
    text = p.read_text(encoding="utf-8")
    assert logins == [(settings.alert_gmail_user, settings.alert_gmail_app_password)]
    for secret in (settings.alert_gmail_user, settings.alert_gmail_app_password, settings.alert_gmail_to, "private-run-id"):
        assert secret not in text
    data = _state(p)
    assert set(data["keys"]) == {"[DATA] stage=quality rows=#"}
    assert data["reservations"] == {}
    assert set(data) == {"version", "day", "sent_today", "keys", "reservations"}


def test_handler_dedups_numeric_repeats() -> None:
    calls: list[str] = []
    day = dt.date(2026, 10, 5)
    h = EmailAlertHandler(
        component="d", run_id="r", sender=lambda s, b, **kw: calls.append(s),
        cooldown_s=0, clock=lambda: 900.0, today=lambda: day, sleep=lambda s: None,
    )
    for rows in (1, 22, 333, 4444, 55555):
        h.handle(_rec(f"[DATA] stage=quality tr_id=H0STCNT0 vendor=kis rows={rows}"))
    assert len(calls) == 3


def test_failed_send_releases() -> None:
    import smtplib

    state = {"fail": True}
    calls: list[str] = []

    def _sender(subject: str, body: str, **kw: object) -> None:
        calls.append(subject)
        if state["fail"]:
            raise smtplib.SMTPServerDisconnected("closed")

    day = dt.date(2026, 10, 5)
    h = EmailAlertHandler(
        component="d", run_id="r", sender=_sender,
        cooldown_s=0, clock=lambda: 500.0, today=lambda: day, sleep=lambda s: None,
    )
    h.handle(_rec("boom-release"))
    assert h._sent_today == 0
    state["fail"] = False
    h.handle(_rec("boom-release"))
    assert h._sent_today == 1
    assert len(calls) == 4


@pytest.fixture(params=["memory", "file"])
def ledger(request, tmp_path):
    if request.param == "memory":
        return InMemoryAlertLedger()
    return FileAlertLedger(tmp_path / "budget.json")


def _snapshot(ledger):
    if isinstance(ledger, FileAlertLedger):
        return _state(ledger._path)
    return ledger._state.model_dump()


def test_late_failure_preserves_latest_success_cooldown(ledger) -> None:
    limits = AlertLimits(cooldown_s=10, daily_cap=20, per_key_daily_cap=20, first_occurrence_reserve=0)
    initial = ledger.reserve("k", day=DAY, now_s=100.0, limits=limits)
    _commit(ledger, "k", initial)
    failed = ledger.reserve("k", day=DAY, now_s=111.0, limits=limits)
    delivered = ledger.reserve("k", day=DAY, now_s=122.0, limits=limits)
    _commit(ledger, "k", delivered)
    _release(ledger, "k", failed)
    data = _snapshot(ledger)
    assert data["sent_today"] == 2
    assert data["keys"]["k"]["last_sent_s"] == 122.0
    assert data["keys"]["k"]["last_success_s"] == 122.0
    next_decision = ledger.reserve("k", day=DAY, now_s=123.0, limits=limits)
    assert next_decision.send is False
    assert next_decision.suppressed_reason == "cooldown"


@pytest.mark.parametrize("reverse", [False, True])
def test_overlapping_failures_restore_exact_pre_reservation_state(ledger, reverse: bool) -> None:
    initial = ledger.reserve("k", day=DAY, now_s=100.0, limits=LIMITS)
    _commit(ledger, "k", initial)
    before = _snapshot(ledger)
    first = ledger.reserve("k", day=DAY, now_s=110.0, limits=LIMITS)
    second = ledger.reserve("k", day=DAY, now_s=120.0, limits=LIMITS)
    decisions = [second, first] if reverse else [first, second]
    for decision in decisions:
        _release(ledger, "k", decision)
    assert _snapshot(ledger) == before


def test_out_of_order_successes_keep_latest_send_time(ledger) -> None:
    limits = AlertLimits(cooldown_s=10, daily_cap=20, per_key_daily_cap=20, first_occurrence_reserve=0)
    first = ledger.reserve("k", day=DAY, now_s=100.0, limits=limits)
    second = ledger.reserve("k", day=DAY, now_s=111.0, limits=limits)
    _commit(ledger, "k", second)
    _commit(ledger, "k", first)
    data = _snapshot(ledger)
    assert data["keys"]["k"]["count"] == 2
    assert data["keys"]["k"]["last_sent_s"] == 111.0
    assert ledger.reserve("k", day=DAY, now_s=112.0, limits=limits).suppressed_reason == "cooldown"


def test_finalization_requires_owned_token_and_is_idempotent(ledger) -> None:
    initial = ledger.reserve("k", day=DAY, now_s=100.0, limits=LIMITS)
    _commit(ledger, "k", initial)
    pending = ledger.reserve("k", day=DAY, now_s=110.0, limits=LIMITS)
    before = _snapshot(ledger)
    _release(ledger, "other", pending)
    _release(ledger, "k", pending, day=DAY + dt.timedelta(days=1))
    _commit(ledger, "other", pending)
    ledger.release("k", day=DAY, previous_last_sent_s=999.0)
    ledger.commit("k", day=DAY, reservation_id=None)
    ledger.release("k", day=DAY, previous_last_sent_s=None, reservation_id="unknown-token")
    assert _snapshot(ledger) == before
    _release(ledger, "k", pending)
    restored = _snapshot(ledger)
    _release(ledger, "k", pending)
    _commit(ledger, "k", pending)
    _commit(ledger, "k", initial)
    assert _snapshot(ledger) == restored
    assert restored["sent_today"] == 1


def test_stale_day_token_cannot_change_new_day_budget(ledger) -> None:
    old = ledger.reserve("k", day=DAY, now_s=100.0, limits=LIMITS)
    new_day = DAY + dt.timedelta(days=1)
    current = ledger.reserve("k", day=new_day, now_s=200.0, limits=LIMITS)
    _commit(ledger, "k", current, day=new_day)
    before = _snapshot(ledger)
    _release(ledger, "k", old)
    _commit(ledger, "k", old)
    assert _snapshot(ledger) == before


@pytest.mark.parametrize("failure", ["_locked", "_load_unlocked", "_save_unlocked"])
def test_unrecorded_failed_send_never_refunds_existing_success(tmp_path, monkeypatch, failure: str) -> None:
    path = tmp_path / "degraded-send.json"
    shared = FileAlertLedger(path)
    delivered = []
    failing = False

    def sender(subject, body):
        if failing:
            raise OSError("SMTP unavailable")
        delivered.append(body)

    handler = EmailAlertHandler(
        component="daemon", run_id="r", sender=sender, ledger=shared,
        cooldown_s=0, clock=lambda: 100.0, today=lambda: DAY, sleep=lambda _: None,
    )
    handler.handle(_rec("stage=quality rows=1"))
    before = _state(path)
    failing = True
    with monkeypatch.context() as patch:
        patch.setattr(shared, failure, lambda *a, **kw: (_ for _ in ()).throw(OSError("storage unavailable")))
        decision = shared.reserve("stage=quality rows=#", day=DAY, now_s=110.0, limits=LIMITS)
        assert decision.send is True
        assert decision.reservation_id is None
        handler.handle(_rec("stage=quality rows=22"))
    assert _state(path) == before
    failing = False
    handler.handle(_rec("stage=quality rows=333"))
    assert len(delivered) == 2
    assert _state(path)["sent_today"] == 2
    assert _state(path)["reservations"] == {}


def test_dead_child_reservation_is_reclaimed_without_touching_successes(tmp_path) -> None:
    path = tmp_path / "crash.json"
    shared = FileAlertLedger(path)
    limits = AlertLimits(cooldown_s=0, daily_cap=2, per_key_daily_cap=3, first_occurrence_reserve=0)
    kept = shared.reserve("kept", day=DAY, now_s=100.0, limits=limits)
    _commit(shared, "kept", kept)
    code = """
import datetime as dt, pathlib, sys
from src.core.alerts import AlertLimits, FileAlertLedger
ledger = FileAlertLedger(pathlib.Path(sys.argv[1]))
decision = ledger.reserve("abandoned", day=dt.date(2026,10,5), now_s=200.0,
    limits=AlertLimits(cooldown_s=0,daily_cap=2,per_key_daily_cap=3,first_occurrence_reserve=0))
assert decision.send
print("READY", flush=True)
sys.stdin.readline()
sys.stdin.readline()
"""
    with _concurrent_children(code, path, count=1) as children:
        blocked = shared.reserve("new", day=DAY, now_s=300.0, limits=limits)
        assert blocked.send is False
        assert blocked.suppressed_reason == "daily_cap"
        assert _state(path)["keys"]["abandoned"]["count"] == 1
        children[0].kill()
        children[0].wait(timeout=10)
        recovered = FileAlertLedger(path).reserve("new", day=DAY, now_s=301.0, limits=limits)
        assert recovered.send is True
        _commit(shared, "new", recovered)
    data = _state(path)
    assert data["sent_today"] == 2
    assert set(data["keys"]) == {"kept", "new"}
    assert data["keys"]["kept"]["confirmed_count"] == 1
    assert data["reservations"] == {}


@pytest.mark.parametrize("dead_owner", ["pid_reused", "zombie"])
def test_dead_owner_reaping_preserves_successful_key_cap(tmp_path, monkeypatch, dead_owner: str) -> None:
    import os

    path = tmp_path / "owner.json"
    shared = FileAlertLedger(path)
    limits = AlertLimits(cooldown_s=0, daily_cap=20, per_key_daily_cap=1, first_occurrence_reserve=0)
    kept = shared.reserve("kept", day=DAY, now_s=100.0, limits=limits)
    _commit(shared, "kept", kept)
    pending = shared.reserve("dead", day=DAY, now_s=200.0, limits=limits)
    data = _state(path)
    if dead_owner == "pid_reused":
        data["reservations"][pending.reservation_id]["owner_start"] = "0"
        path.write_text(json.dumps(data), encoding="utf-8")
    else:
        _, start = alerts_mod._process_identity(os.getpid())
        identities = iter([("Z", start), ("R", start)])
        monkeypatch.setattr(alerts_mod, "_process_identity", lambda pid: next(identities))
    denied = shared.reserve("kept", day=DAY, now_s=300.0, limits=limits)
    assert denied.suppressed_reason == "per_key_cap"
    assert _state(path)["sent_today"] == 1
    assert _state(path)["reservations"] == {}
    assert set(_state(path)["keys"]) == {"kept"}


def test_legacy_schema_migration_preserves_budget_and_cooldown(tmp_path, caplog) -> None:
    path = tmp_path / "legacy.json"
    path.write_text(json.dumps({
        "version": 1, "day": DAY.isoformat(), "sent_today": 2,
        "keys": {"k": {"count": 2, "last_sent_s": 100.0}},
    }), encoding="utf-8")
    limits = AlertLimits(cooldown_s=10, daily_cap=20, per_key_daily_cap=3, first_occurrence_reserve=0)
    shared = FileAlertLedger(path)
    assert shared.reserve("k", day=DAY, now_s=109.0, limits=limits).suppressed_reason == "cooldown"
    decision = shared.reserve("k", day=DAY, now_s=111.0, limits=limits)
    assert decision.send is True
    _commit(shared, "k", decision)
    assert shared.reserve("k", day=DAY, now_s=200.0, limits=limits).suppressed_reason == "per_key_cap"
    data = _state(path)
    assert data["version"] == 2
    assert data["sent_today"] == 3
    assert data["keys"]["k"]["confirmed_count"] == 3
    assert not [r for r in caplog.records if "status=DEGRADED" in r.getMessage()]


@pytest.mark.parametrize("corruption", ["orphan", "timestamp", "count", "last", "total", "empty_total", "nan", "boolean", "extra"])
def test_inconsistent_state_is_replaced_not_trusted(tmp_path, caplog, corruption: str) -> None:
    path = tmp_path / "inconsistent.json"
    shared = FileAlertLedger(path)
    initial = shared.reserve("k", day=DAY, now_s=100.0, limits=LIMITS)
    _commit(shared, "k", initial)
    data = _state(path)
    if corruption == "orphan":
        data["reservations"]["missing"] = {"key": "orphan", "now_s": 110.0, "owner_pid": 1, "owner_start": "1"}
    elif corruption == "timestamp":
        data["keys"]["k"]["last_success_s"] = None
    elif corruption == "count":
        data["keys"]["k"]["count"] = 2
    elif corruption == "last":
        data["keys"]["k"]["last_sent_s"] = 99.0
    elif corruption == "total":
        data["sent_today"] = 20
    elif corruption == "empty_total":
        data["keys"] = {}
    elif corruption == "nan":
        data["keys"]["k"]["last_success_s"] = float("nan")
    elif corruption == "boolean":
        data["keys"]["k"]["count"] = True
    else:
        data["secret"] = "must not be retained"
    path.write_text(json.dumps(data), encoding="utf-8")
    with caplog.at_level(logging.WARNING):
        assert shared.reserve("new", day=DAY, now_s=200.0, limits=LIMITS).send is True
    assert _state(path)["sent_today"] == 1
    assert set(_state(path)["keys"]) == {"new"}
    assert len([r for r in caplog.records if "status=DEGRADED" in r.getMessage()]) == 1


@pytest.mark.parametrize("stage", ["fsync", "replace"])
def test_atomic_write_failure_preserves_prior_state(tmp_path, monkeypatch, stage: str) -> None:
    import os

    path = tmp_path / "atomic.json"
    shared = FileAlertLedger(path)
    initial = shared.reserve("kept", day=DAY, now_s=100.0, limits=LIMITS)
    _commit(shared, "kept", initial)
    before = path.read_bytes()
    monkeypatch.setattr(os, stage, lambda *a, **kw: (_ for _ in ()).throw(OSError("disk unavailable")))
    decision = shared.reserve("new", day=DAY, now_s=200.0, limits=LIMITS)
    assert decision.send is True
    assert decision.reservation_id is None
    assert path.read_bytes() == before
    assert list(tmp_path.glob("*.tmp.*")) == []


@pytest.mark.parametrize("timeout", [-1.0, float("nan"), float("inf")])
def test_invalid_lock_timeout_fails_fast(tmp_path, timeout: float) -> None:
    with pytest.raises(ValueError, match="lock_timeout_s"):
        FileAlertLedger(tmp_path / "ledger.json", lock_timeout_s=timeout)


def test_shared_handler_default_clock_is_wall_time(tmp_path) -> None:
    path = tmp_path / "wall.json"
    delivered = []
    handler = EmailAlertHandler(
        component="d", run_id="r", sender=lambda s, b: delivered.append(b), ledger=FileAlertLedger(path),
    )
    before = time.time()
    handler.handle(_rec("stage=quality rows=1"))
    assert len(delivered) == 1
    assert before <= _state(path)["keys"]["stage=quality rows=#"]["last_sent_s"] <= time.time()


def test_empty_in_memory_ledger_has_no_consumed_budget() -> None:
    ledger = InMemoryAlertLedger()
    assert ledger._sent_today == 0
    assert ledger._per_key_counts == {}
    ledger.commit("missing", day=DAY, reservation_id="missing")
    ledger.release("missing", day=DAY, previous_last_sent_s=None, reservation_id="missing")
