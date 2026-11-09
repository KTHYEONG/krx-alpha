"""Hermetic guards for the bounded repair-lock wait before a collector recreate."""

from __future__ import annotations

import os
import subprocess
import threading
import time
from pathlib import Path

SCRIPT = Path("deploy/host/krx-wait-repair-idle.sh")
TIMEOUT_TITLE = "::warning title=vps-deploy/krx-alpha/repair_lock_wait::reason=repair_lock_wait_timeout"


def _key(lock: Path) -> str:
    st = os.stat(lock)
    return f"{os.major(st.st_dev):02x}:{os.minor(st.st_dev):02x}:{st.st_ino}"


def _locks_line(lock: Path, *, pid: int = 4242, kind: str = "FLOCK") -> str:
    return f"1: {kind}  ADVISORY  WRITE {pid} {_key(lock)} 0 EOF\n"


def _run(tmp_path: Path, *, locks: str | None, lock_file: bool = True, **overrides: str) -> subprocess.CompletedProcess[str]:
    lock = tmp_path / "repair.lock"
    if lock_file and not lock.exists():
        lock.write_text("", encoding="utf-8")
    proc_locks = tmp_path / "locks"
    if locks is not None:
        proc_locks.write_text(locks, encoding="utf-8")
    env = {
        **os.environ,
        "KRX_REPAIR_LOCK": str(lock),
        "KRX_PROC_LOCKS": str(proc_locks),
        "KRX_REPAIR_WAIT_MAX_S": "3",
        "KRX_REPAIR_WAIT_POLL_S": "1",
        **overrides,
    }
    return subprocess.run(  # noqa: S603 - fixed argv: repo script with a tmp lock and locks file
        ["bash", str(SCRIPT)], env=env, capture_output=True, text=True, check=False  # noqa: S607
    )


def test_idle_when_no_lock_entry(tmp_path: Path) -> None:
    started = time.monotonic()
    result = _run(tmp_path, locks="1: FLOCK  ADVISORY  WRITE 99 00:01:1 0 EOF\n")

    assert result.returncode == 0, result.stdout + result.stderr
    assert "status=idle" in result.stdout
    assert time.monotonic() - started < 2


def test_missing_lock_file_is_idle(tmp_path: Path) -> None:
    result = _run(tmp_path, locks="", lock_file=False)

    assert result.returncode == 0
    assert "reason=no_lock_file" in result.stdout


def test_waits_then_proceeds_when_released(tmp_path: Path) -> None:
    lock = tmp_path / "repair.lock"
    lock.write_text("", encoding="utf-8")
    proc_locks = tmp_path / "locks"
    proc_locks.write_text(_locks_line(lock), encoding="utf-8")
    release = threading.Timer(1.0, proc_locks.write_text, args=("",), kwargs={"encoding": "utf-8"})
    release.start()
    started = time.monotonic()
    try:
        result = _run(tmp_path, locks=None, lock_file=False)
    finally:
        release.cancel()
    elapsed = time.monotonic() - started

    assert result.returncode == 0, result.stdout + result.stderr
    assert "status=waiting" in result.stdout
    assert "holder_pids=4242" in result.stdout
    assert result.stdout.rindex("status=waiting") < result.stdout.rindex("status=idle")
    assert 1 <= elapsed < 3


def test_timeout_proceeds_with_warning(tmp_path: Path) -> None:
    lock = tmp_path / "repair.lock"
    lock.write_text("", encoding="utf-8")
    started = time.monotonic()
    result = _run(tmp_path, locks=_locks_line(lock))
    elapsed = time.monotonic() - started

    assert result.returncode == 0
    assert "status=timeout" in result.stdout
    assert TIMEOUT_TITLE in result.stdout
    assert 3 <= elapsed < 6


def test_posix_or_other_inode_entries_are_ignored(tmp_path: Path) -> None:
    lock = tmp_path / "repair.lock"
    lock.write_text("", encoding="utf-8")
    locks = _locks_line(lock, kind="POSIX") + "2: FLOCK  ADVISORY  WRITE 7 00:01:1 0 EOF\n"
    result = _run(tmp_path, locks=locks)

    assert result.returncode == 0
    assert "status=idle" in result.stdout
    assert "status=waiting" not in result.stdout


def test_unreadable_locks_is_idle(tmp_path: Path) -> None:
    result = _run(tmp_path, locks=None, KRX_PROC_LOCKS=str(tmp_path / "absent"))

    assert result.returncode == 0
    assert "reason=locks_unreadable" in result.stdout


def test_bad_numeric_config_falls_back(tmp_path: Path) -> None:
    result = _run(tmp_path, locks="", KRX_REPAIR_WAIT_MAX_S="abc")

    assert result.returncode == 0
    assert "status=bad_config var=KRX_REPAIR_WAIT_MAX_S using=900" in result.stdout
    assert "status=idle" in result.stdout


def test_run_has_no_side_effects(tmp_path: Path) -> None:
    lock = tmp_path / "repair.lock"
    lock.write_text("", encoding="utf-8")
    (tmp_path / "locks").write_text(_locks_line(lock), encoding="utf-8")
    before = (lock.stat().st_mtime_ns, lock.stat().st_size, sorted(p.name for p in tmp_path.iterdir()))
    result = _run(tmp_path, locks=None, KRX_REPAIR_WAIT_MAX_S="1")
    after = (lock.stat().st_mtime_ns, lock.stat().st_size, sorted(p.name for p in tmp_path.iterdir()))

    assert result.returncode == 0
    assert before == after
