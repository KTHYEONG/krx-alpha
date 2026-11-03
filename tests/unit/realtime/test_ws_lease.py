"""Invariant guard tests for the host-wide WebSocket connection lease ledger."""

from __future__ import annotations

import fcntl
import json
import logging
import multiprocessing
import os
import pathlib
import re
import stat
import tempfile
from collections.abc import Callable
from typing import Any

import pytest

from src.realtime.ws_lease import LeaseRole, LeaseUnavailable, WsLeaseLedger

_KEY = "0123456789ab"
_VENDOR = "ls"
_LEDGER_LOGGER = "src.realtime.ws_lease"
_CAP = 3
_TTL_S = 10.0
_TTL_NS = 10_000_000_000
_START_NS = 1_700_000_000_000_000_000
_RACERS = 6
_LEDGER_FILE = f"wslease_{_VENDOR}_{_KEY}.json"
_LOCK_FILE = f"wslease_{_VENDOR}_{_KEY}.lock"
_LINE_RE = re.compile(
    rf"\[SYS\] stage=ws_lease vendor={_VENDOR} key={_KEY} owner=\S+ role=(primary|standby) "
    r"action=(GRANT|DENY|PREEMPT|RELEASE|EXPIRE) live=\d+ cap=\d+"
)


def _clock() -> tuple[Callable[[], int], Callable[[int], None]]:
    now_ns = _START_NS

    def read() -> int:
        return now_ns

    def advance(delta_ns: int) -> None:
        nonlocal now_ns
        now_ns += delta_ns

    return read, advance


def _ledger(directory: pathlib.Path, now: Callable[[], int], *, cap: int = _CAP) -> WsLeaseLedger:
    return WsLeaseLedger(directory, vendor=_VENDOR, key_id=_KEY, max_connections=cap, ttl_s=_TTL_S, clock_ns=now)


def _race_for_primary(directory: str, barrier: Any, outcomes: Any) -> None:
    ledger = WsLeaseLedger(pathlib.Path(directory), vendor=_VENDOR, key_id=_KEY, max_connections=_CAP, ttl_s=600.0)
    barrier.wait(timeout=60)
    try:
        ledger.acquire(f"racer-{os.getpid()}", LeaseRole.PRIMARY)
    except LeaseUnavailable:
        outcomes.put("denied")
    except Exception:
        outcomes.put("error")
    else:
        outcomes.put("granted")


def test_grant_up_to_cap_then_deny(tmp_path: pathlib.Path) -> None:
    now, _ = _clock()
    ledger = _ledger(tmp_path, now)
    leases = [ledger.acquire(f"primary-{i}", LeaseRole.PRIMARY) for i in range(_CAP)]
    assert len({lease.token for lease in leases}) == _CAP

    with pytest.raises(LeaseUnavailable, match="cap_reached"):
        ledger.acquire("standby-0", LeaseRole.STANDBY)


def test_primary_preempts_standby(tmp_path: pathlib.Path) -> None:
    now, _ = _clock()
    ledger = _ledger(tmp_path, now)
    primary_a = ledger.acquire("a", LeaseRole.PRIMARY)
    primary_b = ledger.acquire("b", LeaseRole.PRIMARY)
    standby = ledger.acquire("s", LeaseRole.STANDBY)

    with pytest.raises(LeaseUnavailable, match="preempt_pending"):
        ledger.acquire("c", LeaseRole.PRIMARY)

    assert ledger.renew(standby) is False
    assert ledger.renew(primary_a) is True
    assert ledger.renew(primary_b) is True
    with pytest.raises(LeaseUnavailable, match="preempt_pending"):
        ledger.acquire("c", LeaseRole.PRIMARY)

    ledger.release(standby)
    granted = ledger.acquire("c", LeaseRole.PRIMARY)
    assert granted.role is LeaseRole.PRIMARY


def test_standby_never_preempts(tmp_path: pathlib.Path) -> None:
    now, _ = _clock()
    ledger = _ledger(tmp_path, now)
    primaries = [ledger.acquire(f"p{i}", LeaseRole.PRIMARY) for i in range(_CAP)]

    with pytest.raises(LeaseUnavailable, match="cap_reached"):
        ledger.acquire("s", LeaseRole.STANDBY)

    assert all(ledger.renew(lease) for lease in primaries)
    with pytest.raises(LeaseUnavailable, match="cap_reached"):
        ledger.acquire("p-extra", LeaseRole.PRIMARY)


def test_oldest_standby_is_marked_once_per_pending_preemption(tmp_path: pathlib.Path) -> None:
    now, advance = _clock()
    ledger = _ledger(tmp_path, now)
    primary = ledger.acquire("p", LeaseRole.PRIMARY)
    older = ledger.acquire("s-old", LeaseRole.STANDBY)
    advance(1)
    newer = ledger.acquire("s-new", LeaseRole.STANDBY)

    with pytest.raises(LeaseUnavailable, match="preempt_pending"):
        ledger.acquire("p2", LeaseRole.PRIMARY)
    with pytest.raises(LeaseUnavailable, match="preempt_pending"):
        ledger.acquire("p3", LeaseRole.PRIMARY)

    assert ledger.renew(older) is False
    assert ledger.renew(newer) is True
    assert ledger.renew(primary) is True


def test_expired_lease_is_reclaimed(tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger=_LEDGER_LOGGER)
    now, advance = _clock()
    ledger = _ledger(tmp_path, now, cap=1)
    stale = ledger.acquire("stale", LeaseRole.PRIMARY)

    advance(_TTL_NS - 1)
    with pytest.raises(LeaseUnavailable, match="cap_reached"):
        ledger.acquire("blocked", LeaseRole.PRIMARY)

    advance(1)
    fresh = ledger.acquire("fresh", LeaseRole.PRIMARY)

    assert fresh.token != stale.token
    assert ledger.renew(stale) is False
    expired = [r.getMessage() for r in caplog.records if "action=EXPIRE" in r.getMessage()]
    assert len(expired) == 1
    assert "owner=stale" in expired[0]


def test_renew_extends_lease_by_one_ttl(tmp_path: pathlib.Path) -> None:
    now, advance = _clock()
    ledger = _ledger(tmp_path, now, cap=1)
    lease = ledger.acquire("owner", LeaseRole.PRIMARY)

    advance(_TTL_NS - 1)
    assert ledger.renew(lease) is True

    advance(_TTL_NS - 1)
    with pytest.raises(LeaseUnavailable, match="cap_reached"):
        ledger.acquire("other", LeaseRole.PRIMARY)

    advance(1)
    assert ledger.renew(lease) is False


def test_release_is_idempotent_and_frees_slot(tmp_path: pathlib.Path) -> None:
    now, _ = _clock()
    ledger = _ledger(tmp_path, now, cap=1)
    lease = ledger.acquire("a", LeaseRole.PRIMARY)

    ledger.release(lease)
    ledger.release(lease)

    assert ledger.renew(lease) is False
    successor = ledger.acquire("b", LeaseRole.PRIMARY)
    assert successor.owner == "b"


def test_promote_converts_standby_in_place_without_changing_count(tmp_path: pathlib.Path) -> None:
    now, _ = _clock()
    ledger = _ledger(tmp_path, now, cap=2)
    primary = ledger.acquire("p", LeaseRole.PRIMARY)
    standby = ledger.acquire("s", LeaseRole.STANDBY)

    promoted = ledger.promote(standby)

    assert promoted.role is LeaseRole.PRIMARY
    assert promoted.token == standby.token
    assert ledger.renew(promoted) is True
    assert ledger.renew(primary) is True
    with pytest.raises(LeaseUnavailable, match="cap_reached"):
        ledger.acquire("extra", LeaseRole.STANDBY)


def test_promote_rejects_preempted_standby_and_primary(tmp_path: pathlib.Path) -> None:
    now, _ = _clock()
    ledger = _ledger(tmp_path, now, cap=2)
    primary = ledger.acquire("p", LeaseRole.PRIMARY)
    standby = ledger.acquire("s", LeaseRole.STANDBY)
    with pytest.raises(LeaseUnavailable, match="preempt_pending"):
        ledger.acquire("p2", LeaseRole.PRIMARY)

    with pytest.raises(LeaseUnavailable, match="standby_not_promotable"):
        ledger.promote(standby)
    with pytest.raises(LeaseUnavailable, match="standby_not_promotable"):
        ledger.promote(primary)


def test_log_lines_follow_format_and_never_carry_tokens(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=_LEDGER_LOGGER)
    now, _ = _clock()
    ledger = _ledger(tmp_path, now, cap=2)
    primary = ledger.acquire("p", LeaseRole.PRIMARY)
    standby = ledger.acquire("s", LeaseRole.STANDBY)
    with pytest.raises(LeaseUnavailable):
        ledger.acquire("p2", LeaseRole.PRIMARY)
    ledger.release(standby)
    ledger.release(primary)

    messages = [r.getMessage() for r in caplog.records if r.name == _LEDGER_LOGGER]
    assert messages == [
        f"[SYS] stage=ws_lease vendor={_VENDOR} key={_KEY} owner=p role=primary action=GRANT live=1 cap=2",
        f"[SYS] stage=ws_lease vendor={_VENDOR} key={_KEY} owner=s role=standby action=GRANT live=2 cap=2",
        f"[SYS] stage=ws_lease vendor={_VENDOR} key={_KEY} owner=s role=standby action=PREEMPT live=2 cap=2",
        f"[SYS] stage=ws_lease vendor={_VENDOR} key={_KEY} owner=s role=standby action=RELEASE live=1 cap=2",
        f"[SYS] stage=ws_lease vendor={_VENDOR} key={_KEY} owner=p role=primary action=RELEASE live=0 cap=2",
    ]
    assert all(_LINE_RE.fullmatch(message) for message in messages)
    joined = "\n".join(messages)
    for lease in (primary, standby):
        assert lease.token not in joined
        assert lease.token not in repr(lease)


def test_ledger_file_is_private_and_leaves_no_temp_files(tmp_path: pathlib.Path) -> None:
    directory = tmp_path / "ledger"
    now, _ = _clock()
    ledger = _ledger(directory, now)
    ledger.acquire("p", LeaseRole.PRIMARY)
    ledger.acquire("s", LeaseRole.STANDBY)

    assert stat.S_IMODE((directory / _LEDGER_FILE).stat().st_mode) == 0o600
    assert sorted(path.name for path in directory.iterdir()) == sorted([_LEDGER_FILE, _LOCK_FILE])


@pytest.mark.parametrize(
    "payload",
    [
        b"{not json",
        b'{"schema_version": 1, "leases": "oops"}',
        b"\xff\xfe\x00garbage",
    ],
)
def test_corrupt_ledger_fails_closed_and_is_quarantined(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture, payload: bytes
) -> None:
    caplog.set_level(logging.INFO, logger=_LEDGER_LOGGER)
    now, _ = _clock()
    ledger = _ledger(tmp_path, now)
    (tmp_path / _LEDGER_FILE).write_bytes(payload)

    with pytest.raises(LeaseUnavailable, match="ledger_corrupt"):
        ledger.acquire("p", LeaseRole.PRIMARY)

    assert json.loads((tmp_path / _LEDGER_FILE).read_text())["leases"] == []
    quarantined = list(tmp_path.glob(f".{_LEDGER_FILE}.corrupt-*"))
    assert [path.read_bytes() for path in quarantined] == [payload]
    critical = [r for r in caplog.records if r.levelno == logging.CRITICAL]
    assert len(critical) == 1
    assert "[SYS] stage=ws_lease status=CORRUPT" in critical[0].getMessage()


@pytest.mark.parametrize(
    ("key_id", "cap"),
    [("../escape", _CAP), ("a/b", _CAP), ("", _CAP), (_KEY, 0)],
)
def test_invalid_identifiers_or_cap_are_rejected(tmp_path: pathlib.Path, key_id: str, cap: int) -> None:
    with pytest.raises(ValueError, match="must"):
        WsLeaseLedger(tmp_path, vendor=_VENDOR, key_id=key_id, max_connections=cap, ttl_s=_TTL_S)


def test_concurrent_processes_respect_cap(tmp_path: pathlib.Path) -> None:
    barrier = multiprocessing.Barrier(_RACERS)
    outcomes = multiprocessing.Queue()
    procs = [
        multiprocessing.Process(target=_race_for_primary, args=(str(tmp_path), barrier, outcomes))
        for _ in range(_RACERS)
    ]
    for proc in procs:
        proc.start()
    results = [outcomes.get(timeout=120) for _ in range(_RACERS)]
    for proc in procs:
        proc.join(timeout=120)

    assert all(proc.exitcode == 0 for proc in procs)
    assert "error" not in results
    assert results.count("granted") == _CAP
    assert results.count("denied") == _RACERS - _CAP


def test_unusable_ledger_directory_fails_closed(tmp_path: pathlib.Path) -> None:
    blocker = tmp_path / "occupied"
    blocker.write_text("not a directory")
    now, _ = _clock()
    ledger = _ledger(blocker, now)

    with pytest.raises(LeaseUnavailable, match="ledger_unavailable"):
        ledger.acquire("p", LeaseRole.PRIMARY)
    assert blocker.read_text() == "not a directory"


def test_lock_failure_fails_closed_without_writing_ledger(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse_lock(fd: int, operation: int) -> None:
        raise OSError("lock refused")

    monkeypatch.setattr(fcntl, "flock", refuse_lock)
    now, _ = _clock()
    ledger = _ledger(tmp_path, now)

    with pytest.raises(LeaseUnavailable, match="ledger_unavailable"):
        ledger.acquire("p", LeaseRole.PRIMARY)
    assert not (tmp_path / _LEDGER_FILE).exists()


def test_unreadable_ledger_path_fails_closed_without_quarantine(tmp_path: pathlib.Path) -> None:
    (tmp_path / _LEDGER_FILE).mkdir()
    now, _ = _clock()
    ledger = _ledger(tmp_path, now)

    with pytest.raises(LeaseUnavailable, match="ledger_unavailable"):
        ledger.acquire("p", LeaseRole.PRIMARY)
    assert (tmp_path / _LEDGER_FILE).is_dir()
    assert list(tmp_path.glob(f".{_LEDGER_FILE}.corrupt-*")) == []


def test_quarantine_rename_failure_still_fails_closed(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    caplog.set_level(logging.INFO, logger=_LEDGER_LOGGER)
    payload = b"{not json"
    (tmp_path / _LEDGER_FILE).write_bytes(payload)

    def refuse_rename(src: object, dst: object) -> None:
        raise OSError("rename refused")

    monkeypatch.setattr(os, "replace", refuse_rename)
    now, _ = _clock()
    ledger = _ledger(tmp_path, now)

    with pytest.raises(LeaseUnavailable, match="ledger_corrupt"):
        ledger.acquire("p", LeaseRole.PRIMARY)
    assert (tmp_path / _LEDGER_FILE).read_bytes() == payload
    critical = [r.getMessage() for r in caplog.records if r.levelno == logging.CRITICAL]
    assert len(critical) == 1
    assert "renamed=failed" in critical[0]


def test_temp_file_creation_failure_leaves_ledger_unchanged(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse_temp(*args: object, **kwargs: object) -> tuple[int, str]:
        raise OSError("no temp space")

    monkeypatch.setattr(tempfile, "mkstemp", refuse_temp)
    directory = tmp_path / "ledger"
    now, _ = _clock()
    ledger = _ledger(directory, now)

    with pytest.raises(LeaseUnavailable, match="ledger_unwritable"):
        ledger.acquire("p", LeaseRole.PRIMARY)
    assert sorted(path.name for path in directory.iterdir()) == [_LOCK_FILE]


def test_failed_publish_removes_temp_and_keeps_previous_ledger(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = tmp_path / "ledger"
    now, _ = _clock()
    ledger = _ledger(directory, now)
    primary = ledger.acquire("p", LeaseRole.PRIMARY)
    before = (directory / _LEDGER_FILE).read_bytes()

    def refuse_replace(src: object, dst: object) -> None:
        raise OSError("replace refused")

    monkeypatch.setattr(os, "replace", refuse_replace)
    with pytest.raises(LeaseUnavailable, match="ledger_unwritable"):
        ledger.acquire("s", LeaseRole.STANDBY)
    monkeypatch.undo()

    assert (directory / _LEDGER_FILE).read_bytes() == before
    assert sorted(path.name for path in directory.iterdir()) == sorted([_LEDGER_FILE, _LOCK_FILE])
    assert ledger.renew(primary) is True


@pytest.mark.parametrize(
    "payload",
    [
        b'{"schema_version": 2, "leases": []}',
        b'{"schema_version": 1, "leases": ["not-an-object"]}',
        b'{"schema_version": 1, "leases": [{"owner": "p", "role": "primary", "token": "t", "preempt": false, '
        b'"expires_ns": true, "granted_ns": 0}]}',
    ],
)
def test_malformed_ledger_entries_fail_closed(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture, payload: bytes
) -> None:
    caplog.set_level(logging.INFO, logger=_LEDGER_LOGGER)
    now, _ = _clock()
    ledger = _ledger(tmp_path, now)
    (tmp_path / _LEDGER_FILE).write_bytes(payload)

    with pytest.raises(LeaseUnavailable, match="ledger_corrupt"):
        ledger.acquire("p", LeaseRole.PRIMARY)
    assert json.loads((tmp_path / _LEDGER_FILE).read_text())["leases"] == []
    assert [path.read_bytes() for path in tmp_path.glob(f".{_LEDGER_FILE}.corrupt-*")] == [payload]


def test_corrupt_ledger_blocks_standbys_for_one_ttl_but_admits_primaries(tmp_path: pathlib.Path) -> None:
    now, advance = _clock()
    ledger = _ledger(tmp_path, now)
    (tmp_path / _LEDGER_FILE).write_bytes(b"{not json")
    with pytest.raises(LeaseUnavailable, match="ledger_corrupt"):
        ledger.acquire("p", LeaseRole.PRIMARY)

    other_process = _ledger(tmp_path, now)
    with pytest.raises(LeaseUnavailable, match="quarantine"):
        other_process.acquire("s", LeaseRole.STANDBY)
    primary = other_process.acquire("p", LeaseRole.PRIMARY)
    advance(_TTL_NS // 2)
    assert other_process.renew(primary) is True
    with pytest.raises(LeaseUnavailable, match="quarantine"):
        other_process.acquire("s", LeaseRole.STANDBY)
    advance(_TTL_NS // 2 + 1)
    standby = other_process.acquire("s", LeaseRole.STANDBY)

    assert standby.role is LeaseRole.STANDBY


def test_non_integer_standby_block_is_treated_as_corrupt(tmp_path: pathlib.Path) -> None:
    now, _ = _clock()
    (tmp_path / _LEDGER_FILE).write_text(
        json.dumps({"schema_version": 1, "leases": [], "standby_blocked_until_ns": "soon"}), encoding="utf-8"
    )

    with pytest.raises(LeaseUnavailable, match="ledger_corrupt"):
        _ledger(tmp_path, now).acquire("p", LeaseRole.PRIMARY)
