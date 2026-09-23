"""one-time, evidence-gated removal of superseded krx-alpha objects from `gdrive:quant-lake/live/krx-alpha/data`. Why: host copy and container offload historically wrote overlapping trees, and `rclone copy` never deletes. Dry-run by default; runs from the workstation."""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from src.core.config import RcloneArchiveSettings
from src.storage.remote import l0_partition_for_l1

logger = logging.getLogger(__name__)

CleanupRule = Literal["l0_superseded_by_l1", "l0_quarantine_duplicate", "manifest_duplicate", "work_transient"]


@dataclass(frozen=True, slots=True)
class RemoteObject:
    path: str
    size: int


@dataclass(frozen=True, slots=True)
class CleanupCandidate:
    rule: CleanupRule
    target: str
    bytes: int
    evidence: str


def build_cleanup_plan(
    objects: Sequence[RemoteObject],
) -> tuple[list[CleanupCandidate], list[tuple[str, str]]]:
    """Classify remote objects into deletion candidates and kept (path, reason) entries.

    Rules:
        l0_superseded_by_l1: every l0 partition dir D such that some remote L1 object with
            size > 0 maps to D via ``l0_partition_for_l1``.
        l0_quarantine_duplicate: l0 files whose ``quarantine/<same relative path>`` exists with equal size
            (only when the partition is not already covered by the previous rule).
        manifest_duplicate: ``manifest/<rel>`` whose ``manifests/<rel>`` exists with equal size.
        work_transient: every object under ``work/``.

    Returns:
        (candidates sorted by target, kept entries for all other l0/manifest objects).
    """
    by_path: dict[str, int] = {obj.path: obj.size for obj in objects}
    l1_evidence: dict[str, str] = {}
    for obj in objects:
        if obj.size <= 0 or not obj.path.startswith("l1/"):
            continue
        mapped = l0_partition_for_l1(obj.path)
        if mapped is None:
            continue
        if mapped not in l1_evidence or obj.path < l1_evidence[mapped]:
            l1_evidence[mapped] = obj.path
    superseded = set(l1_evidence)
    candidates: list[CleanupCandidate] = []
    covered_l0: set[str] = set()
    for target in sorted(superseded):
        files = [obj for obj in objects if obj.path.startswith(target + "/")]
        if not files:
            continue
        total = sum(obj.size for obj in files)
        candidates.append(
            CleanupCandidate(
                rule="l0_superseded_by_l1",
                target=target,
                bytes=total,
                evidence=l1_evidence[target],
            )
        )
        covered_l0.update(obj.path for obj in files)
    for obj in objects:
        if not obj.path.startswith("l0/") or obj.path in covered_l0:
            continue
        counterpart = "quarantine/" + obj.path[len("l0/") :]
        if counterpart in by_path and by_path[counterpart] == obj.size:
            candidates.append(
                CleanupCandidate(
                    rule="l0_quarantine_duplicate",
                    target=obj.path,
                    bytes=obj.size,
                    evidence=counterpart,
                )
            )
            covered_l0.add(obj.path)
    candidate_targets = {item.target for item in candidates}
    for obj in objects:
        if not obj.path.startswith("manifest/") or obj.path in candidate_targets:
            continue
        counterpart = "manifests/" + obj.path[len("manifest/") :]
        if counterpart in by_path and by_path[counterpart] == obj.size:
            candidates.append(
                CleanupCandidate(
                    rule="manifest_duplicate",
                    target=obj.path,
                    bytes=obj.size,
                    evidence=counterpart,
                )
            )
    candidates.extend(
        CleanupCandidate(rule="work_transient", target=obj.path, bytes=obj.size, evidence="transient")
        for obj in objects
        if obj.path.startswith("work/")
    )
    candidates.sort(key=lambda item: item.target)
    candidate_targets = {item.target for item in candidates}
    kept: list[tuple[str, str]] = []
    for obj in objects:
        if obj.path.startswith("l0/"):
            if obj.path in covered_l0:
                continue
            counterpart = "quarantine/" + obj.path[len("l0/") :]
            if counterpart in by_path:
                kept.append((obj.path, "quarantine_size_mismatch"))
            else:
                kept.append((obj.path, "no_l1"))
        elif obj.path.startswith("manifest/"):
            if obj.path in candidate_targets:
                continue
            counterpart = "manifests/" + obj.path[len("manifest/") :]
            if counterpart in by_path:
                kept.append((obj.path, "manifests_size_mismatch"))
            else:
                kept.append((obj.path, "no_manifests_match"))
    return candidates, kept


def _remote_data_root() -> str:
    settings = RcloneArchiveSettings()
    return f"{settings.remote_name}:{settings.remote_path}"


def _list_remote_objects(remote: str) -> list[RemoteObject]:
    result = subprocess.run(  # noqa: S603 - fixed rclone argv without shell
        ["rclone", "lsjson", remote, "-R", "--fast-list", "--files-only"],  # noqa: S607
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"lsjson failed: {result.stderr}")
    try:
        entries = json.loads(result.stdout)
    except ValueError as exc:
        raise RuntimeError("lsjson failed: invalid JSON") from exc
    parsed: list[RemoteObject] = [
        RemoteObject(path=str(entry.get("Path", "")), size=int(entry.get("Size", 0))) for entry in entries
    ]
    return parsed


def _print_plan(candidates: Sequence[CleanupCandidate], kept: Sequence[tuple[str, str]]) -> None:
    rules: list[CleanupRule] = [
        "l0_superseded_by_l1",
        "l0_quarantine_duplicate",
        "manifest_duplicate",
        "work_transient",
    ]
    for rule in rules:
        matched = [item for item in candidates if item.rule == rule]
        total = sum(item.bytes for item in matched)
        print(f"rule={rule} count={len(matched)} bytes={total}")  # noqa: T201 - workstation review output
    print(f"kept={len(kept)}")  # noqa: T201 - workstation review output


def main(argv: Sequence[str] | None = None) -> int:
    """``python -m src.cli.gdrive_cleanup [--apply]``: list once (lsjson -R --fast-list --files-only), plan, print per-rule counts/bytes; with --apply re-list, re-plan, then purge l0 dirs / deletefile others and rmdirs --leave-root on l0, manifest, work."""
    parser = argparse.ArgumentParser(description="One-time Drive cleanup for superseded krx-alpha objects.")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    remote = _remote_data_root()
    try:
        objects = _list_remote_objects(remote)
    except RuntimeError as exc:
        logger.error("[DATA] stage=gdrive_cleanup status=FAIL error=%s", str(exc))
        return 1
    candidates, kept = build_cleanup_plan(objects)
    _print_plan(candidates, kept)
    if not args.apply:
        return 0
    try:
        fresh = _list_remote_objects(remote)
    except RuntimeError as exc:
        logger.error("[DATA] stage=gdrive_cleanup status=FAIL error=%s", str(exc))
        return 1
    candidates, kept = build_cleanup_plan(fresh)
    _print_plan(candidates, kept)
    failures = 0
    for item in candidates:
        if not (
            item.target.startswith("l0/")
            or item.target.startswith("manifest/")
            or item.target.startswith("work/")
        ):
            continue
        dest = f"{remote}/{item.target}"
        if item.rule == "l0_superseded_by_l1":
            result = subprocess.run(  # noqa: S603 - fixed rclone argv without shell
                ["rclone", "purge", dest],  # noqa: S607
                capture_output=True,
                text=True,
                check=False,
            )
        else:
            result = subprocess.run(  # noqa: S603 - fixed rclone argv without shell
                ["rclone", "deletefile", dest],  # noqa: S607
                capture_output=True,
                text=True,
                check=False,
            )
        if result.returncode != 0:
            logger.error("[DATA] stage=gdrive_cleanup status=FAIL target=%s", item.target)
            failures += 1
    for base in ("l0", "manifest", "work"):
        subprocess.run(  # noqa: S603 - fixed rclone argv without shell
            ["rclone", "rmdirs", f"{remote}/{base}", "--leave-root"],  # noqa: S607
            capture_output=True,
            text=True,
            check=False,
        )
    return 1 if failures else 0


if __name__ == "__main__":  # pragma: no cover - interpreter entry point only
    raise SystemExit(main())
