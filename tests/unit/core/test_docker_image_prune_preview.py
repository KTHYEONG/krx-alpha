"""Invariant guards for the read-only docker image prune preview script."""

from __future__ import annotations

import datetime as dt
import re
import subprocess
from pathlib import Path

SCRIPT = Path("deploy/host/docker_image_prune_preview.sh")
MANAGED = "ghcr.io/kthyeong/krx-collector"


def _created(days_ago: int) -> str:
    stamp = dt.datetime.now(dt.UTC) - dt.timedelta(days=days_ago)
    return stamp.strftime("%Y-%m-%d %H:%M:%S +0000 UTC")


def _images() -> str:
    rows = [
        (MANAGED, "latest", "id0", _created(1), "1GB"),
        (MANAGED, "aaaaaaa", "id1", _created(1), "1GB"),
        (MANAGED, "bbbbbbb", "id2", _created(2), "1GB"),
        (MANAGED, "ccccccc", "id3", _created(3), "1GB"),
        (MANAGED, "ddddddd", "id4", _created(4), "1GB"),
        (MANAGED, "eeeeeee", "id5", _created(5), "1GB"),
        ("ghcr.io/kthyeong/k-stock-engine", "fffffff", "id6", _created(6), "1GB"),
        ("crypto-pilot-live", "latest", "id7", _created(90), "1GB"),
        ("alpine", "3", "id8", _created(90), "1GB"),
        ("kse-backup-memory", "20261013", "id9", _created(40), "1GB"),
        ("kse-backup-memory", "20261117", "id10", _created(2), "1GB"),
    ]
    return "\n".join("\t".join(row) for row in rows)


def _run_preview(tmp_path: Path, *, used: str) -> tuple[str, list[str]]:
    log = tmp_path / "docker_calls.log"
    images = tmp_path / "images.tsv"
    images.write_text(_images() + "\n", encoding="utf-8")
    stub = tmp_path / "bin" / "docker"
    stub.parent.mkdir()
    stub.write_text(
        "#!/usr/bin/env bash\n"
        f'echo "$@" >> "{log}"\n'
        'case "$1" in\n'
        f'  images) cat "{images}" ;;\n'
        f'  ps) printf "%s\\n" "{used}" ;;\n'
        "  *) exit 99 ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    stub.chmod(0o755)
    env = {"PATH": f"{stub.parent}:/usr/bin:/bin", "LC_ALL": "C"}
    proc = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, env=env, check=True)  # noqa: S603,S607
    return proc.stdout, log.read_text(encoding="utf-8").splitlines()


def test_preview_classifies_and_executes_nothing(tmp_path: Path) -> None:
    out, docker_calls = _run_preview(tmp_path, used=f"{MANAGED}:ddddddd")

    lines = out.splitlines()
    assert f"KEEP(in_use) {MANAGED}:ddddddd 1GB" in lines
    assert f"KEEP(latest) {MANAGED}:latest 1GB" in lines
    assert [line for line in lines if line.startswith(f"KEEP(rollback) {MANAGED}:")] == [
        f"KEEP(rollback) {MANAGED}:aaaaaaa 1GB",
        f"KEEP(rollback) {MANAGED}:bbbbbbb 1GB",
        f"KEEP(rollback) {MANAGED}:ccccccc 1GB",
    ]
    assert f"CANDIDATE(old_sha) {MANAGED}:eeeeeee 1GB" in lines
    assert "docker rmi kse-backup-memory:20261013" in lines
    assert "docker rmi kse-backup-memory:20261117" not in lines
    assert "SKIP(other_tenant) crypto-pilot-live:latest 1GB" in lines
    assert "SKIP(other_tenant) alpine:3 1GB" in lines
    assert not any(line.startswith("docker rmi") and ("crypto-pilot-live" in line or "alpine" in line) for line in lines)
    assert all(call.split()[0] in {"images", "ps"} for call in docker_calls)


def test_preview_script_is_read_only() -> None:
    for number, raw in enumerate(SCRIPT.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.strip()
        if line.startswith("#") or not re.search(r"\b(rmi|prune|rm)\b", line):
            continue
        assert line.startswith("printf "), f"line {number} may execute a removal: {line}"
