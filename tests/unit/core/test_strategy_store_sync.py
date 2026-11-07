"""Hermetic replay guards for the strategy store sync (local bare remote, real git, tmp STORE_HOME)."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path("deploy/host/strategy-store-sync.sh")

# Stands in for the store's validator (versioned in the store repo); only the [meta] contract matters here.
_STUB_VALIDATOR = """\
import sys
import tomllib
from pathlib import Path

root = Path(sys.argv[1])
meta = tomllib.loads((root / "k-closing-alpha/closing/params.toml").read_text(encoding="utf-8"))["meta"]
missing = [k for k in ("schema", "version", "created_at", "source", "validated_by") if k not in meta]
if missing:
    print("missing " + ",".join(missing), file=sys.stderr)
    sys.exit(1)
"""

_META = """\
[meta]
schema = "closing-alpha/v1"
version = "{version}"
created_at = "2026-11-07T09:00:00+09:00"
source = "research/2026-11-07"
validated_by = "oos-2026-11"

[params]
threshold = 0.25
"""


class _Store:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.remote = root / "remote.git"
        self.seed = root / "seed"
        self.home = root / "home"
        self.store_home = root / "store-home"
        self.key = root / "strategy_store_ro"
        self.status = root / "state" / "strategy_store_status.json"
        self.home.mkdir()
        subprocess.run(["git", "init", "-q", "--bare", "-b", "main", str(self.remote)], check=True)  # noqa: S603, S607
        subprocess.run(["git", "init", "-q", "-b", "main", str(self.seed)], check=True)  # noqa: S603, S607
        self.counter = 0

    def _git(self, *args: str) -> str:
        env = {
            "PATH": os.environ["PATH"],
            "HOME": str(self.home),
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_AUTHOR_NAME": "store",
            "GIT_AUTHOR_EMAIL": "store@example.invalid",
            "GIT_COMMITTER_NAME": "store",
            "GIT_COMMITTER_EMAIL": "store@example.invalid",
        }
        result = subprocess.run(  # noqa: S603 - hermetic git with fixed argv
            ["git", "-C", str(self.seed), *args],  # noqa: S607
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()

    def commit(self, version: str, *, drop_key: str | None = None) -> str:
        self.counter += 1
        (self.seed / "tools").mkdir(exist_ok=True)
        (self.seed / "tools" / "validate_store.py").write_text(_STUB_VALIDATOR, encoding="utf-8")
        (self.seed / "store.toml").write_text("format = 1\n", encoding="utf-8")
        strategy = self.seed / "k-closing-alpha" / "closing"
        strategy.mkdir(parents=True, exist_ok=True)
        meta = _META.format(version=version)
        if drop_key is not None:
            meta = "\n".join(line for line in meta.splitlines() if not line.startswith(f"{drop_key} ")) + "\n"
        (strategy / "params.toml").write_text(meta, encoding="utf-8")
        self._git("add", "-A")
        self._git("commit", "-q", "-m", f"store {self.counter}")
        self._git("push", "-q", str(self.remote), "main")
        return self._git("rev-parse", "HEAD")

    def remove_remote(self) -> None:
        shutil.rmtree(self.remote)

    def run(self, **overrides: str) -> subprocess.CompletedProcess[str]:
        env = {
            "PATH": os.environ["PATH"],
            "HOME": str(self.home),
            "TMPDIR": str(self.root),
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_NOSYSTEM": "1",
            "STORE_HOME": str(self.store_home),
            "STORE_REMOTE": f"file://{self.remote}",
            "STORE_BRANCH": "main",
            "STORE_KEEP": "10",
            "STORE_KEY": str(self.key),
            "STORE_STATUS": str(self.status),
        }
        env.update(overrides)
        return subprocess.run(  # noqa: S603 - hermetic bash harness with fixed argv
            ["bash", str(SCRIPT)],  # noqa: S607
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    def add_key(self) -> None:
        self.key.write_text("not-a-real-deploy-key\n", encoding="utf-8")

    def status_json(self) -> dict[str, str]:
        return json.loads(self.status.read_text(encoding="utf-8"))

    @property
    def current(self) -> Path:
        return self.store_home / "current"

    def current_target(self) -> str:
        return os.readlink(self.current)

    def release_names(self) -> list[str]:
        return sorted(p.name for p in (self.store_home / "releases").iterdir())


@pytest.fixture
def store(tmp_path: Path) -> _Store:
    return _Store(tmp_path)


def test_not_configured_without_key(store: _Store) -> None:
    store.commit("1.0.0")

    result = store.run()

    assert result.returncode == 0, result.stderr
    assert store.status_json()["status"] == "not_configured"
    assert not (store.store_home / "repo").exists()


def test_first_sync_promotes_validated_tree(store: _Store) -> None:
    store.add_key()
    sha = store.commit("1.0.0")

    result = store.run()

    assert result.returncode == 0, result.stderr
    assert store.current_target() == f"releases/{sha}"
    assert (store.current / "STORE_REVISION").read_text(encoding="utf-8") == f"{sha}\n"
    assert store.status_json()["status"] == "updated"
    assert store.status_json()["revision"] == sha


def test_unchanged_is_noop(store: _Store) -> None:
    store.add_key()
    store.commit("1.0.0")
    assert store.run().returncode == 0
    inode = os.lstat(store.current).st_ino

    result = store.run()

    assert result.returncode == 0, result.stderr
    assert store.status_json()["status"] == "unchanged"
    assert os.lstat(store.current).st_ino == inode


def test_invalid_commit_keeps_previous_current(store: _Store) -> None:
    store.add_key()
    good = store.commit("1.0.0")
    assert store.run().returncode == 0
    bad = store.commit("1.0.1", drop_key="validated_by")

    result = store.run()

    assert result.returncode == 65, result.stderr
    assert store.current_target() == f"releases/{good}"
    assert store.status_json()["status"] == "invalid"
    assert store.status_json()["revision"] == bad
    assert not any(name.endswith(".tmp") for name in store.release_names())


def test_fetch_failure_keeps_current(store: _Store) -> None:
    store.add_key()
    good = store.commit("1.0.0")
    assert store.run().returncode == 0
    store.remove_remote()

    result = store.run()

    assert result.returncode == 75, result.stderr
    assert store.current_target() == f"releases/{good}"
    assert store.status_json()["status"] == "fetch_failed"


def test_prune_keeps_current_and_newest(store: _Store) -> None:
    store.add_key()
    last = ""
    for i in range(12):
        last = store.commit(f"1.0.{i}")
        assert store.run().returncode == 0

    names = store.release_names()

    assert len(names) == 10
    assert last in names
    assert store.current_target() == f"releases/{last}"
    assert (store.store_home / "releases" / last).is_dir()
