"""Invariant guards for the shared VPS image garbage collector."""

from __future__ import annotations

import datetime as dt
import fcntl
import importlib.util
import os
import sys
from pathlib import Path
from typing import Any

import pytest


def _load_gc() -> Any:
    spec = importlib.util.spec_from_file_location(
        "vps_image_gc", "deploy/host/vps_image_gc.py"
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["vps_image_gc"] = module
    spec.loader.exec_module(module)
    return module


def _tag(gc: Any, repo: str, tag: str, day: int, image: str = "") -> Any:
    return gc.ImageTag(
        repository=repo,
        tag=tag,
        image_id=image or f"id-{tag}",
        created=dt.datetime(2026, 9, day, tzinfo=dt.UTC),
    )


def test_keeps_newest_n_sha_tags_per_repository(tmp_path: Path) -> None:
    gc = _load_gc()

    tags = [_tag(gc, "repo-a", f"sha-{i:04d}", i) for i in range(1, 8)]
    doomed = gc.select_tag_removals(
        tags,
        repositories=frozenset({"repo-a"}),
        protected_image_ids=frozenset(),
        keep=5,
    )

    assert [t.tag for t in doomed] == ["sha-0001", "sha-0002"]


def test_never_removes_protected_image(tmp_path: Path) -> None:
    gc = _load_gc()

    tags = [_tag(gc, "repo-a", f"sha-{i:04d}", i) for i in range(1, 8)]
    plain = gc.select_tag_removals(
        tags,
        repositories=frozenset({"repo-a"}),
        protected_image_ids=frozenset(),
        keep=5,
    )
    guarded = gc.select_tag_removals(
        tags,
        repositories=frozenset({"repo-a"}),
        protected_image_ids=frozenset({"id-sha-0001"}),
        keep=5,
    )

    assert [t.tag for t in plain] == ["sha-0001", "sha-0002"]
    assert [t.tag for t in guarded] == ["sha-0002"]


def test_ignores_latest_and_foreign_repositories(tmp_path: Path) -> None:
    gc = _load_gc()

    tags = [
        _tag(gc, "repo-a", "latest", 9, image="id-latest"),
        _tag(gc, "repo-a", "sha-0001", 1),
        _tag(gc, "repo-b", "sha-0002", 2),
    ]
    doomed = gc.select_tag_removals(
        tags,
        repositories=frozenset({"repo-a"}),
        protected_image_ids=frozenset(),
        keep=5,
    )

    assert doomed == []


def test_tie_break_is_deterministic(tmp_path: Path) -> None:
    gc = _load_gc()

    first = [_tag(gc, "repo-a", "sha-0002", 1), _tag(gc, "repo-a", "sha-0001", 1)]
    second = [_tag(gc, "repo-a", "sha-0001", 1), _tag(gc, "repo-a", "sha-0002", 1)]
    kwargs: dict[str, Any] = {
        "repositories": frozenset({"repo-a"}),
        "protected_image_ids": frozenset(),
        "keep": 1,
    }

    assert gc.select_tag_removals(first, **kwargs) == gc.select_tag_removals(second, **kwargs)
    assert [t.tag for t in gc.select_tag_removals(first, **kwargs)] == ["sha-0001"]


def test_rejects_non_positive_keep(tmp_path: Path) -> None:
    gc = _load_gc()

    tags = [_tag(gc, "repo-a", "sha-0001", 1)]
    with pytest.raises(ValueError, match="keep"):
        gc.select_tag_removals(
            tags,
            repositories=frozenset({"repo-a"}),
            protected_image_ids=frozenset(),
            keep=0,
        )
    with pytest.raises(ValueError, match="keep"):
        gc.select_evidence_removals([tmp_path / "evidence" / "a" / "f.log"], keep=0)


def test_evidence_retention_per_project(tmp_path: Path) -> None:
    gc = _load_gc()

    a_files = [tmp_path / "evidence" / "a" / f"202609{i:02d}T000000Z-deadbeef1234.log" for i in range(23)]
    b_files = [tmp_path / "evidence" / "b" / f"202609{i:02d}T000000Z-deadbeef1234.log" for i in range(3)]
    doomed = gc.select_evidence_removals([*a_files, *b_files], keep=20)

    assert sorted(p.name for p in doomed) == sorted(p.name for p in a_files[:3])


_FAKE_DOCKER = """#!/bin/bash
printf 'docker %s\\n' "$*" >>"$FAKE_DOCKER_LOG"
cmd=$1; shift
case "$cmd" in
  images)
    if [ -n "${FAKE_SHORT_IDS:-}" ] && [[ " $* " != *" --no-trunc "* ]]; then
      sed -E 's/\tsha256:([0-9a-f]{12})[0-9a-f]*\t/\t\1\t/' "$FAKE_IMAGES_OUTPUT"
    else
      cat "$FAKE_IMAGES_OUTPUT"
    fi
    ;;
  ps) printf '%s\\n' ${FAKE_PS_IDS:-} ;;
  inspect) printf '%s\\n' "$FAKE_CONTAINER_IMAGE" ;;
  image)
    sub=$1; shift
    case "$sub" in
      inspect)
        if [ "${FAKE_LATEST_FAIL:-1}" = "1" ]; then echo "no such image" >&2; exit 1; fi
        printf '%s\\n' "$FAKE_LATEST_IMAGE"
        ;;
      prune)
        python3 - "$FAKE_LOCK" "$FAKE_DOCKER_LOG" <<'PYEOF'
import fcntl
import sys
lock, log = sys.argv[1], sys.argv[2]
try:
    with open(lock, "a") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    held = "free"
except BlockingIOError:
    held = "exclusive"
with open(log, "a") as handle:
    handle.write(f"probe prune-lock={held}\\n")
PYEOF
        exit "${FAKE_PRUNE_RC:-0}"
        ;;
    esac
    ;;
  rmi)
    if [ "${FAKE_FAIL_RMI:-0}" = "1" ]; then echo "rmi failed" >&2; exit 1; fi
    ;;
esac
exit 0
"""


def _install_fake_docker(
    tmp_path: Path,
    monkeypatch: Any,
    *,
    image_lines: list[str],
    ps_ids: str = "",
    fail_rmi: bool = False,
    lock: Path | None = None,
) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    images_output = tmp_path / "images.txt"
    images_output.write_text("".join(line + "\n" for line in image_lines), encoding="utf-8")
    log = tmp_path / "docker.log"
    log.write_text("", encoding="utf-8")
    docker = bin_dir / "docker"
    docker.write_text(_FAKE_DOCKER, encoding="utf-8")
    docker.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_IMAGES_OUTPUT", str(images_output))
    monkeypatch.setenv("FAKE_DOCKER_LOG", str(log))
    monkeypatch.setenv("FAKE_PS_IDS", ps_ids)
    monkeypatch.setenv("FAKE_CONTAINER_IMAGE", "sha256:unused")
    monkeypatch.setenv("FAKE_LATEST_FAIL", "1")
    monkeypatch.setenv("FAKE_FAIL_RMI", "1" if fail_rmi else "0")
    monkeypatch.setenv("FAKE_LOCK", str(lock) if lock is not None else str(tmp_path / "image.lock"))
    return log


def _image_lines(repo: str, count: int) -> list[str]:
    return [f"{repo}\tsha-{i:04d}\tid-{i:04d}\t2026-09-{i:02d}T00:00:00+00:00" for i in range(1, count + 1)]


def test_skips_when_deploy_holds_shared_lock(
    tmp_path: Path, monkeypatch: Any, capsys: Any
) -> None:
    gc = _load_gc()
    state_root = tmp_path / "state"
    lock = state_root / "image.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.touch()
    log = _install_fake_docker(tmp_path, monkeypatch, image_lines=_image_lines("repo-a", 7), lock=lock)

    handle = lock.open("a")
    fcntl.flock(handle.fileno(), fcntl.LOCK_SH)
    try:
        rc = gc.main(
            ["--repo", "repo-a", "--keep", "5", "--state-root", str(state_root), "--lock-wait-s", "1"]
        )
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()

    assert rc == 0
    assert "status=SKIPPED" in capsys.readouterr().out
    recorded = log.read_text(encoding="utf-8")
    assert "docker rmi" not in recorded
    assert "docker image prune" not in recorded


def test_dry_run_mutates_nothing(tmp_path: Path, monkeypatch: Any, capsys: Any) -> None:
    gc = _load_gc()
    state_root = tmp_path / "state"
    evidence = state_root / "evidence" / "a"
    evidence.mkdir(parents=True, exist_ok=True)
    for i in range(21):
        (evidence / f"202609{i:02d}T000000Z-deadbeef1234.log").write_text("x", encoding="utf-8")
    lines = _image_lines("repo-a", 7)
    lines.append("repo-a\tlatest\tid-latest\t2026-09-09T00:00:00+00:00")
    lines.append("repo-foreign\tsha-0001\tid-foreign\t2026-09-01T00:00:00+00:00")
    log = _install_fake_docker(tmp_path, monkeypatch, image_lines=lines)

    rc = gc.main(
        ["--repo", "repo-a", "--keep", "5", "--state-root", str(state_root), "--dry-run"]
    )

    assert rc == 0
    assert "status=OK" in capsys.readouterr().out
    recorded = log.read_text(encoding="utf-8")
    assert "docker rmi" not in recorded
    assert "docker image prune" not in recorded
    assert len(list(evidence.iterdir())) == 21


def test_rmi_failure_degrades_and_alerts(tmp_path: Path, monkeypatch: Any, capsys: Any) -> None:
    gc = _load_gc()
    state_root = tmp_path / "state"
    log = _install_fake_docker(
        tmp_path, monkeypatch, image_lines=_image_lines("repo-a", 7), fail_rmi=True
    )

    rc = gc.main(["--repo", "repo-a", "--keep", "5", "--state-root", str(state_root)])

    assert rc == 1
    assert "status=DEGRADED" in capsys.readouterr().out
    assert "docker rmi" in log.read_text(encoding="utf-8")


def test_prune_runs_only_under_exclusive_lock(tmp_path: Path, monkeypatch: Any) -> None:
    gc = _load_gc()
    state_root = tmp_path / "state"
    log = _install_fake_docker(
        tmp_path,
        monkeypatch,
        image_lines=_image_lines("repo-a", 7),
        lock=state_root / "image.lock",
    )

    assert (
        gc.main(["--repo", "repo-a", "--keep", "5", "--state-root", str(state_root)]) == 0
    )
    assert "probe prune-lock=exclusive" in log.read_text(encoding="utf-8")


def test_in_use_image_protected_with_real_docker_id_formats(
    tmp_path: Path, monkeypatch: Any, capsys: Any
) -> None:
    gc = _load_gc()
    state_root = tmp_path / "state"
    full = [f"sha256:{i:02d}" + "a" * 62 for i in range(1, 8)]
    lines = [f"repo-a\tsha-{i:04d}\t{full[i - 1]}\t2026-09-{i:02d} 00:00:00 +0000 UTC" for i in range(1, 8)]
    log = _install_fake_docker(tmp_path, monkeypatch, image_lines=lines, ps_ids="c1")
    # Real docker: `images` truncates IDs unless --no-trunc; container inspect returns the full ID.
    monkeypatch.setenv("FAKE_SHORT_IDS", "1")
    monkeypatch.setenv("FAKE_CONTAINER_IMAGE", full[0])

    rc = gc.main(["--repo", "repo-a", "--keep", "5", "--state-root", str(state_root)])

    assert rc == 0
    recorded = log.read_text(encoding="utf-8")
    assert "docker rmi repo-a:sha-0001" not in recorded
    assert "docker rmi repo-a:sha-0002" in recorded
    assert "protected=1" in capsys.readouterr().out
