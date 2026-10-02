from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

import pytest

from tests.tmp_isolation import (
    prune_stale_run_roots,
    release_run_tmp_root,
    resolve_run_id,
    run_tmp_root,
)


def test_xdist_workers_share_run_id() -> None:
    env = {"PYTEST_XDIST_TESTRUNUID": "abc"}
    assert resolve_run_id(env) == "abc"
    assert resolve_run_id(env) == "abc"


def test_independent_runs_differ() -> None:
    first = resolve_run_id({})
    second = resolve_run_id({})
    assert first != second
    assert "/" not in first
    assert "/" not in second
    assert os.sep not in first
    assert os.sep not in second


def test_root_layout(tmp_path: Path) -> None:
    base = tmp_path / "pytest"
    root = run_tmp_root(base, "R", "gw0")
    assert root == base / "R" / "gw0"
    assert not root.exists()
    assert not (base / "R").exists()


def test_release_own_only(tmp_path: Path) -> None:
    base = tmp_path / "pytest"
    gw0 = base / "R" / "gw0"
    gw1 = base / "R" / "gw1"
    other = base / "S" / "main"
    for d in (gw0, gw1, other):
        d.mkdir(parents=True)
        (d / "f.txt").write_text("x")
    release_run_tmp_root(gw0)
    assert not gw0.exists()
    assert (gw1 / "f.txt").read_text() == "x"
    assert (other / "f.txt").read_text() == "x"
    assert (base / "R").exists()


def test_release_last_removes_run_dir(tmp_path: Path) -> None:
    base = tmp_path / "pytest"
    root = base / "R" / "main"
    root.mkdir(parents=True)
    (root / "f.txt").write_text("x")
    release_run_tmp_root(root)
    assert not (base / "R").exists()
    assert base.exists()


def test_release_missing_is_noop(tmp_path: Path) -> None:
    release_run_tmp_root(tmp_path / "pytest" / "R" / "gw0")


def test_prune_age_boundary(tmp_path: Path) -> None:
    base = tmp_path / "pytest"
    base.mkdir()
    old = base / "old-run"
    young = base / "young-run"
    old.mkdir()
    young.mkdir()
    now = time.time()
    max_age = timedelta(hours=24)
    os.utime(old, (now - max_age.total_seconds() - 1, now - max_age.total_seconds() - 1))
    os.utime(young, (now - max_age.total_seconds() + 1, now - max_age.total_seconds() + 1))
    keep = base / "keep" / "main"
    removed = prune_stale_run_roots(base, now=now, max_age=max_age, keep=keep)
    assert removed == [old]
    assert not old.exists()
    assert young.exists()


def test_prune_never_keep_ancestor(tmp_path: Path) -> None:
    base = tmp_path / "pytest"
    run_dir = base / "R"
    keep = run_dir / "gw0"
    keep.mkdir(parents=True)
    now = time.time()
    max_age = timedelta(hours=24)
    stale = now - max_age.total_seconds() - 10
    os.utime(run_dir, (stale, stale))
    os.utime(keep, (stale, stale))
    removed = prune_stale_run_roots(base, now=now, max_age=max_age, keep=keep)
    assert removed == []
    assert run_dir.exists()


def test_prune_legacy_flat_dirs(tmp_path: Path) -> None:
    base = tmp_path / "pytest"
    base.mkdir()
    legacy = base / "main"
    fresh = base / "gw0"
    legacy.mkdir()
    fresh.mkdir()
    now = time.time()
    max_age = timedelta(hours=24)
    os.utime(legacy, (now - max_age.total_seconds() - 10, now - max_age.total_seconds() - 10))
    removed = prune_stale_run_roots(base, now=now, max_age=max_age, keep=base / "keep" / "main")
    assert removed == [legacy]
    assert not legacy.exists()
    assert fresh.exists()


def test_prune_missing_base(tmp_path: Path) -> None:
    assert prune_stale_run_roots(tmp_path / "nope", now=time.time(), max_age=timedelta(hours=24), keep=tmp_path / "k") == []


@pytest.mark.slow
def test_concurrent_runs_survive(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    work = tmp_path / "synth"
    work.mkdir()
    (work / "conftest.py").write_text(
        "import os, tempfile, time\n"
        "from pathlib import Path\n"
        "import pytest\n"
        "from tests.tmp_isolation import (\n"
        "    PYTEST_TMP_BASE_NAME, STALE_RUN_ROOT_AGE, prune_stale_run_roots,\n"
        "    release_run_tmp_root, resolve_run_id, run_tmp_root,\n"
        ")\n"
        "_RUN_ID = resolve_run_id(os.environ)\n"
        "PROJECT_TMP_BASE = Path(__file__).resolve().parent / 'tmp' / PYTEST_TMP_BASE_NAME\n"
        "PROJECT_TMP = run_tmp_root(PROJECT_TMP_BASE, _RUN_ID, os.environ.get('PYTEST_XDIST_WORKER', 'main'))\n"
        "@pytest.fixture(scope='session', autouse=True)\n"
        "def _pin_tmp_root():\n"
        "    prune_stale_run_roots(PROJECT_TMP_BASE, now=time.time(), max_age=STALE_RUN_ROOT_AGE, keep=PROJECT_TMP)\n"
        "    PROJECT_TMP.mkdir(parents=True, exist_ok=True)\n"
        "    for var in ('TMPDIR', 'TEMP', 'TMP'):\n"
        "        os.environ[var] = str(PROJECT_TMP)\n"
        "    tempfile.tempdir = str(PROJECT_TMP)\n"
        "    yield\n"
        "    release_run_tmp_root(PROJECT_TMP)\n",
        encoding="utf-8",
    )
    (work / "test_sample.py").write_text(
        "import os, time\n"
        "from pathlib import Path\n"
        "def test_tmp_survives(tmp_path):\n"
        "    p = tmp_path / 'probe.txt'\n"
        "    p.write_text('hello')\n"
        "    time.sleep(0.5)\n"
        "    assert p.read_text() == 'hello'\n"
        "    out = os.environ.get('KRX_ALPHA_TMP_PROBE_OUT', '')\n"
        "    if out:\n"
        "        Path(out).write_text(os.environ.get('TMPDIR', ''))\n",
        encoding="utf-8",
    )
    probe_a = tmp_path / "probe_a.txt"
    probe_b = tmp_path / "probe_b.txt"

    def _base_env(probe: Path) -> dict[str, str]:
        env = {k: v for k, v in os.environ.items() if not k.startswith("PYTEST_XDIST_")}
        env["KRX_ALPHA_TMP_PROBE_OUT"] = str(probe)
        env["PYTHONPATH"] = str(repo_root) + os.pathsep + env.get("PYTHONPATH", "")
        return env

    proc_a = subprocess.Popen(
        [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "-q"],
        cwd=str(work),
        env=_base_env(probe_a),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    time.sleep(0.1)
    proc_b = subprocess.Popen(
        [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "-q"],
        cwd=str(work),
        env=_base_env(probe_b),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    out_a, err_a = proc_a.communicate(timeout=120)
    out_b, err_b = proc_b.communicate(timeout=120)
    assert proc_a.returncode == 0, out_a + err_a
    assert proc_b.returncode == 0, out_b + err_b
    root_a = probe_a.read_text().strip()
    root_b = probe_b.read_text().strip()
    assert root_a
    assert root_b
    assert root_a != root_b
