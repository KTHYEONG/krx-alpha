"""Host image garbage collector for shared VPS deploys.

Runs on the VPS host (not in a container) under an exclusive image lock, so it
never races an in-flight deploy pull. Untags superseded ``sha-*`` images while
protecting every in-use image, then prunes dangling layers. Stdlib only.
"""

from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import subprocess
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

IMAGES_FORMAT = "{{.Repository}}\t{{.Tag}}\t{{.ID}}\t{{.CreatedAt}}"
CONTAINER_IMAGE_FORMAT = "{{.Image}}"
LATEST_ID_FORMAT = "{{.Id}}"


@dataclass(frozen=True, slots=True)
class ImageTag:
    """One repository tag pointing at an image."""

    repository: str
    tag: str
    image_id: str
    created: dt.datetime


class _DockerError(Exception):
    """A read-only docker query failed; carries the failing ref for evidence."""

    def __init__(self, ref: str) -> None:
        super().__init__(ref)
        self.ref = ref


def select_tag_removals(
    tags: Sequence[ImageTag],
    *,
    repositories: frozenset[str],
    protected_image_ids: frozenset[str],
    keep: int,
) -> list[ImageTag]:
    """Return the sha-* tags that are safe to untag.

    Only tags whose repository is listed and whose tag starts with ``sha-``
    are candidates. Per repository the newest ``keep`` by image ``created``
    (ties broken by tag descending) are kept, and a tag whose image is
    protected is never selected.
    """
    if keep < 1:
        raise ValueError(f"keep must be >= 1, got {keep}")
    by_repo: dict[str, list[ImageTag]] = {}
    for tag in tags:
        if tag.repository not in repositories or not tag.tag.startswith("sha-"):
            continue
        by_repo.setdefault(tag.repository, []).append(tag)
    doomed: list[ImageTag] = []
    for repo_tags in by_repo.values():
        ordered = sorted(repo_tags, key=lambda item: (item.created, item.tag), reverse=True)
        doomed.extend(item for item in ordered[keep:] if item.image_id not in protected_image_ids)
    return sorted(doomed, key=lambda item: (item.repository, item.tag))


def select_evidence_removals(files: Sequence[Path], *, keep: int) -> list[Path]:
    """Return the evidence files beyond the newest ``keep`` per project directory."""
    if keep < 1:
        raise ValueError(f"keep must be >= 1, got {keep}")
    by_parent: dict[Path, list[Path]] = {}
    for file in files:
        by_parent.setdefault(file.parent, []).append(file)
    doomed: list[Path] = []
    for siblings in by_parent.values():
        ordered = sorted(siblings, key=lambda item: item.name, reverse=True)
        doomed.extend(ordered[keep:])
    return sorted(doomed, key=lambda item: (item.parent.as_posix(), item.name))


def _parse_created(raw: str) -> dt.datetime:
    text = raw.strip()
    if text.endswith(" UTC"):
        text = text[: -len(" UTC")]
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        parsed = dt.datetime.strptime(text, "%Y-%m-%d %H:%M:%S %z")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def _run(argv: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed argv without shell
        ["docker", *argv],  # noqa: S607 - host PATH docker is the deploy contract
        capture_output=True,
        text=True,
        check=False,
    )


def _check(proc: subprocess.CompletedProcess[str], ref: str) -> str:
    if proc.returncode != 0:
        raise _DockerError(ref)
    return proc.stdout


def _list_tags() -> list[ImageTag]:
    # --no-trunc: default output is a 12-char short ID, while container and
    # :latest inspect return full sha256 IDs; protection compares these sets.
    out = _check(_run(["images", "--no-trunc", "--format", IMAGES_FORMAT]), "images")
    tags: list[ImageTag] = []
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) != 4:
            continue
        repository, tag, image_id, created_raw = (part.strip() for part in parts)
        if not repository or not tag:
            continue
        try:
            created = _parse_created(created_raw)
        except ValueError:
            continue
        tags.append(ImageTag(repository=repository, tag=tag, image_id=image_id, created=created))
    return tags


def _protected_image_ids(repositories: Sequence[str]) -> set[str]:
    protected: set[str] = set()
    containers = _check(_run(["ps", "-aq"]), "ps").split()
    for container in containers:
        proc = _run(["inspect", "--format", CONTAINER_IMAGE_FORMAT, container])
        if proc.returncode == 0 and proc.stdout.strip():
            protected.add(proc.stdout.strip().split()[0])
    for repo in repositories:
        proc = _run(["image", "inspect", "--format", LATEST_ID_FORMAT, f"{repo}:latest"])
        if proc.returncode == 0 and proc.stdout.strip():
            protected.add(proc.stdout.strip().split()[0])
    return protected


def _acquire_exclusive(lock: Path, wait_s: float) -> TextIO | None:
    lock.parent.mkdir(parents=True, exist_ok=True)
    handle = lock.open("a")
    deadline = time.monotonic() + wait_s
    while True:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return handle
        except BlockingIOError:
            if time.monotonic() >= deadline:
                handle.close()
                return None
            time.sleep(0.05)


def _collect_evidence_files(state_root: Path) -> dict[str, list[Path]]:
    base = state_root / "evidence"
    grouped: dict[str, list[Path]] = {}
    if not base.is_dir():
        return grouped
    for project_dir in sorted((p for p in base.iterdir() if p.is_dir()), key=lambda p: p.name):
        grouped[project_dir.name] = sorted(
            (p for p in project_dir.iterdir() if p.is_file()), key=lambda p: p.name
        )
    return grouped


def main(argv: Sequence[str] | None = None) -> int:
    """Untag superseded images under the exclusive lock and trim evidence."""
    parser = argparse.ArgumentParser(description="Shared VPS image garbage collector.")
    parser.add_argument("--repo", action="append", required=True, help="Image repository to collect (repeatable).")
    parser.add_argument("--keep", type=int, default=5, help="Newest sha-* tags to keep per repository.")
    parser.add_argument("--evidence-keep", type=int, default=20, help="Newest evidence files to keep per project.")
    parser.add_argument("--state-root", default="~/.local/state/vps-deploy")
    parser.add_argument("--lock-wait-s", type=float, default=1800)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    keep: int = args.keep
    evidence_keep: int = args.evidence_keep
    if keep < 1:
        raise ValueError(f"--keep must be >= 1, got {keep}")
    if evidence_keep < 1:
        raise ValueError(f"--evidence-keep must be >= 1, got {evidence_keep}")
    repositories: list[str] = list(args.repo or [])
    dry_run: bool = bool(args.dry_run)
    state_root = Path(str(args.state_root)).expanduser()
    wait_s = float(str(args.lock_wait_s))

    start = time.monotonic()
    handle = _acquire_exclusive(state_root / "image.lock", wait_s)
    if handle is None:
        print("[SYS] stage=image_gc status=SKIPPED reason=lock_busy")  # noqa: T201 - runner stdout protocol
        return 0
    with handle:
        try:
            tags = _list_tags()
            protected = _protected_image_ids(repositories)
        except _DockerError as exc:
            elapsed = int(time.monotonic() - start)
            print(  # noqa: T201 - runner stdout protocol
                f"[SYS] stage=image_gc status=DEGRADED failed={exc.ref} elapsed_s={elapsed}"
            )
            return 1
        repos = frozenset(repositories)
        candidates = [tag for tag in tags if tag.repository in repos and tag.tag.startswith("sha-")]
        removals = select_tag_removals(
            tags,
            repositories=repos,
            protected_image_ids=frozenset(protected),
            keep=keep,
        )
        kept = len(candidates) - len(removals)
        failed: list[str] = []
        removed = 0
        if not dry_run:
            for tag in removals:
                ref = f"{tag.repository}:{tag.tag}"
                if _run(["rmi", ref]).returncode != 0:
                    failed.append(ref)
                else:
                    removed += 1
            if _run(["image", "prune", "-f"]).returncode != 0:
                failed.append("prune")
        evidence_removed = 0
        for files in _collect_evidence_files(state_root).values():
            for victim in select_evidence_removals(files, keep=evidence_keep):
                if dry_run:
                    continue
                try:
                    victim.unlink()
                except OSError:
                    continue
                evidence_removed += 1
        elapsed = int(time.monotonic() - start)
        if failed:
            print(  # noqa: T201 - runner stdout protocol
                f"[SYS] stage=image_gc status=DEGRADED removed_tags={removed} kept={kept}"
                f" protected={len(protected)} evidence_removed={evidence_removed}"
                f" failed={','.join(failed)} elapsed_s={elapsed}"
            )
            return 1
        print(  # noqa: T201 - runner stdout protocol
            f"[SYS] stage=image_gc status=OK removed_tags={removed} kept={kept}"
            f" protected={len(protected)} evidence_removed={evidence_removed} elapsed_s={elapsed}"
        )
        return 0


if __name__ == "__main__":
    sys.exit(main())
