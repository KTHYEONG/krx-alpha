"""Host backup script hermetic tests (fake rclone, tmp lock/log/root)."""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

SCRIPT = Path("deploy/host/krx-host-backup.sh")
FILTER = Path("deploy/host/krx-alpha.rclone-filter")


def _write_fake_rclone(path: Path, argv_log: Path) -> Path:
    bin_path = path
    bin_path.write_text(
        "#!/usr/bin/env bash\n"
        f'printf \'%s\\n\' "$*" >> "{argv_log}"\n'
        'cmd="$1"\n'
        'if [ "$cmd" = "copy" ]; then exit "${FAKE_COPY_RC:-0}"; fi\n'
        'if [ "$cmd" = "lsf" ]; then\n'
        '  printf \'%s\' "${FAKE_LSF_OUTPUT:-}"\n'
        "  exit \"${FAKE_LSF_RC:-0}\"\n"
        "fi\n"
        'if [ "$cmd" = "purge" ]; then exit "${FAKE_PURGE_RC:-0}"; fi\n'
        "exit 0\n",
        encoding="utf-8",
    )
    bin_path.chmod(bin_path.stat().st_mode | stat.S_IEXEC)
    return bin_path


def _base_env(tmp_path: Path, argv_log: Path) -> dict[str, str]:
    krx_root = tmp_path / "krx"
    (krx_root / "data").mkdir(parents=True)
    fake_bin = _write_fake_rclone(tmp_path / "fake-rclone.sh", argv_log)
    env = dict(os.environ)
    env.update(
        {
            "KRX_ROOT": str(krx_root),
            "RCLONE_BIN": str(fake_bin),
            "REMOTE_ROOT": "gdrive:test-root",
            "QUANT_GDRIVE_LOCK": str(tmp_path / "quant-gdrive.lock"),
            "LOCK_WAIT_SEC": "5",
            "VERSION_RETENTION_DAYS": "30",
            "BACKUP_TODAY_UTC": "2026-09-23",
            "LOG_DIR": str(tmp_path / "logs"),
            "FAKE_COPY_RC": "0",
            "FAKE_LSF_RC": "3",
            "FAKE_LSF_OUTPUT": "",
        }
    )
    return env


def _read_argv(argv_log: Path) -> list[str]:
    if not argv_log.exists():
        return []
    return argv_log.read_text(encoding="utf-8").splitlines()


def test_host_copy_excludes_container_owned_trees(tmp_path) -> None:
    argv_log = tmp_path / "argv.log"
    env = _base_env(tmp_path, argv_log)

    result = subprocess.run(  # noqa: S603 - hermetic bash harness with fixed argv
        ["bash", str(SCRIPT)],  # noqa: S607
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0
    lines = _read_argv(argv_log)
    copies = [line for line in lines if line.startswith("copy ")]
    assert len(copies) == 1
    copy = copies[0]
    assert "--filter-from" in copy
    assert "krx-alpha.rclone-filter" in copy
    assert "--backup-dir gdrive:test-root/_versions/2026-09-23/data" in copy
    text = FILTER.read_text(encoding="utf-8")
    assert "- /l1/**" in text
    assert "- /manifest/**" in text
    assert "- /work/**" in text
    assert text.index("- /l1/**") < text.index("+ **")
    assert text.index("- /manifest/**") < text.index("+ **")
    assert text.index("- /work/**") < text.index("+ **")


def test_version_prune_keys_on_folder_date_only(tmp_path) -> None:
    argv_log = tmp_path / "argv.log"
    env = _base_env(tmp_path, argv_log)
    env["FAKE_LSF_RC"] = "0"
    env["FAKE_LSF_OUTPUT"] = "2026-08-23/\n2026-08-24/\njunk/\n"

    result = subprocess.run(  # noqa: S603 - hermetic bash harness with fixed argv
        ["bash", str(SCRIPT)],  # noqa: S607
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0
    lines = _read_argv(argv_log)
    purges = [line for line in lines if line.startswith("purge ")]
    assert purges == ["purge gdrive:test-root/_versions/2026-08-23"]


def test_held_lock_blocks_all_drive_calls(tmp_path) -> None:
    import fcntl

    argv_log = tmp_path / "argv.log"
    env = _base_env(tmp_path, argv_log)
    env["LOCK_WAIT_SEC"] = "1"
    lock_path = Path(env["QUANT_GDRIVE_LOCK"])
    lock_path.touch()
    with open(lock_path, "w", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = subprocess.run(  # noqa: S603 - hermetic bash harness with fixed argv
            ["bash", str(SCRIPT)],  # noqa: S607
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

    assert result.returncode == 75
    assert _read_argv(argv_log) == []


def test_failed_copy_still_prunes_and_exits_nonzero(tmp_path) -> None:
    argv_log = tmp_path / "argv.log"
    env = _base_env(tmp_path, argv_log)
    env["FAKE_COPY_RC"] = "1"
    env["FAKE_LSF_RC"] = "0"
    env["FAKE_LSF_OUTPUT"] = "2026-08-23/\n"

    result = subprocess.run(  # noqa: S603 - hermetic bash harness with fixed argv
        ["bash", str(SCRIPT)],  # noqa: S607
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 1
    lines = _read_argv(argv_log)
    assert any(line.startswith("lsf ") for line in lines)
    assert any(line.startswith("purge ") for line in lines)


def test_no_destructive_sync_semantics(tmp_path) -> None:
    argv_log = tmp_path / "argv.log"
    env = _base_env(tmp_path, argv_log)
    env["FAKE_LSF_RC"] = "0"
    env["FAKE_LSF_OUTPUT"] = "2026-08-23/\n"

    result = subprocess.run(  # noqa: S603 - hermetic bash harness with fixed argv
        ["bash", str(SCRIPT)],  # noqa: S607
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0
    for line in _read_argv(argv_log):
        parts = line.split()
        assert "sync" not in parts
        assert "move" not in parts
        assert "delete" not in parts
        assert "--min-age" not in parts
        assert "--max-age" not in parts
        if parts and parts[0] == "purge":
            assert "_versions/" in line


def _status_path(tmp_path: Path) -> Path:
    return Path(tmp_path / "krx" / "data" / "work" / "host_backup_status.json")


def _read_status(tmp_path: Path) -> dict:
    import json

    return json.loads(_status_path(tmp_path).read_text(encoding="utf-8"))


def test_successful_run_writes_status_with_last_ok(tmp_path) -> None:
    argv_log = tmp_path / "argv.log"
    env = _base_env(tmp_path, argv_log)

    result = subprocess.run(  # noqa: S603 - hermetic bash harness with fixed argv
        ["bash", str(SCRIPT)],  # noqa: S607
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0
    body = _read_status(tmp_path)
    assert body["schema_version"] == 1
    assert body["rc"] == 0
    assert body["data_rc"] == 0
    assert body["prune_rc"] == 0
    assert body["last_ok_at"] == body["attempt_finished_at"]
    assert isinstance(body["lock_holders"], list)
    assert isinstance(body["lock_wait_s"], int)
    leftovers = list((_status_path(tmp_path).parent).glob("*.tmp"))
    assert leftovers == []


def test_lock_timeout_records_holders_and_keeps_previous_last_ok(tmp_path) -> None:
    import fcntl
    import json

    argv_log = tmp_path / "argv.log"
    env = _base_env(tmp_path, argv_log)
    env["LOCK_WAIT_SEC"] = "1"
    work = tmp_path / "krx" / "data" / "work"
    work.mkdir(parents=True, exist_ok=True)
    _status_path(tmp_path).write_text(
        json.dumps({"last_ok_at": "2026-09-22T14:30:00+00:00"}), encoding="utf-8"
    )
    lock_path = Path(env["QUANT_GDRIVE_LOCK"])
    lock_path.touch()
    holder = subprocess.Popen(  # noqa: S603 - hermetic flock holder
        ["flock", str(lock_path), "sleep", "30"],  # noqa: S607
    )
    try:
        import time

        deadline = time.monotonic() + 10
        while True:
            try:
                with open(lock_path, encoding="utf-8") as handle:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                break
            if time.monotonic() > deadline:
                raise AssertionError("holder did not acquire the lock")
            time.sleep(0.05)
        result = subprocess.run(  # noqa: S603 - hermetic bash harness with fixed argv
            ["bash", str(SCRIPT)],  # noqa: S607
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
    finally:
        holder.kill()
        holder.wait()

    assert result.returncode == 75
    body = _read_status(tmp_path)
    assert body["rc"] == 75
    assert body["data_rc"] is None
    assert body["prune_rc"] is None
    assert body["last_ok_at"] == "2026-09-22T14:30:00+00:00"
    assert any(str(holder.pid) == str(entry).split(" ")[0] for entry in body["lock_holders"])
    assert f"holders={len(body['lock_holders'])}" in result.stdout
    assert "environ" not in result.stdout
    assert _read_argv(argv_log) == []


def test_copy_failure_keeps_previous_last_ok(tmp_path) -> None:
    import json

    argv_log = tmp_path / "argv.log"
    env = _base_env(tmp_path, argv_log)
    env["FAKE_COPY_RC"] = "1"
    work = tmp_path / "krx" / "data" / "work"
    work.mkdir(parents=True, exist_ok=True)
    _status_path(tmp_path).write_text(
        json.dumps({"last_ok_at": "2026-09-22T14:30:00+00:00"}), encoding="utf-8"
    )

    result = subprocess.run(  # noqa: S603 - hermetic bash harness with fixed argv
        ["bash", str(SCRIPT)],  # noqa: S607
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 1
    body = _read_status(tmp_path)
    assert body["rc"] == 1
    assert body["data_rc"] == 1
    assert body["last_ok_at"] == "2026-09-22T14:30:00+00:00"


def test_corrupt_previous_status_tolerated(tmp_path) -> None:
    argv_log = tmp_path / "argv.log"
    env = _base_env(tmp_path, argv_log)
    work = tmp_path / "krx" / "data" / "work"
    work.mkdir(parents=True, exist_ok=True)
    _status_path(tmp_path).write_text("not-json{", encoding="utf-8")

    result = subprocess.run(  # noqa: S603 - hermetic bash harness with fixed argv
        ["bash", str(SCRIPT)],  # noqa: S607
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0
    body = _read_status(tmp_path)
    assert body["rc"] == 0
    assert body["last_ok_at"] == body["attempt_finished_at"]
