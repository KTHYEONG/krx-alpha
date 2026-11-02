"""Read-only catalog of the shared Drive lake (`quant-lake`): datasets, date ranges, sizes and duplicate names."""

from __future__ import annotations

import argparse
import json
import logging
import re
import subprocess
import sys
import tempfile
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_ROOT = "gdrive:quant-lake"
CATALOG_REPO_PATH = "_catalog/index.json"
SMALL_OBJECT_BYTES = 64 * 1024
"""Objects below this size dominate Drive creation-rate and listing cost; reported per dataset."""

_KEY_PARTITION = re.compile(r"^(?P<key>dt|date|day|session|year|month)=(?P<val>[0-9A-Za-z_-]+)(?P<ext>\..+)?$")
_DATE_LIKE = re.compile(r"^\d{4}-\d{2}(?:-\d{2})?$")
# Ordered token rules: (pattern, placeholder, date normalizer or None). Dates are captured for first/last.
_TOKEN_RULES: tuple[tuple[re.Pattern[str], str, Callable[[re.Match[str]], str] | None], ...] = (
    (re.compile(r"(?<!\d)(\d{4})-(\d{2})-(\d{2})(?!\d)"), "*", lambda m: f"{m[1]}-{m[2]}-{m[3]}"),
    (re.compile(r"(?<!\d)((?:19|20)\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(?!\d)"), "*", lambda m: f"{m[1]}-{m[2]}-{m[3]}"),
    (re.compile(r"(?<![\d-])(\d{4})-(0[1-9]|1[0-2])(?![\d-])"), "*", lambda m: f"{m[1]}-{m[2]}"),
    (re.compile(r"^((?:19|20)\d{2})(0[1-9]|1[0-2])(?=\.)"), "*", lambda m: f"{m[1]}-{m[2]}"),
    (re.compile(r"(?<![A-Za-z0-9])[A-Z0-9]{2,}USDT(?![A-Za-z0-9])"), "{sym}", None),
    (re.compile(r"(?<![A-Za-z0-9])\d{5}[0-9A-Z](?![A-Za-z0-9])"), "{sym}", None),
    (re.compile(r"(?<![0-9a-z])(?=[0-9a-f]*[a-f])[0-9a-f]{6,}(?![0-9a-z])"), "*", None),
    (re.compile(r"^\d{2}(?=(?:\.s\d+)?\.)"), "{hh}", None),
)


def _normalize_segment(segment: str) -> tuple[str, str | None]:
    key = _KEY_PARTITION.match(segment)
    if key:
        value = key.group("val")
        return f"{key.group('key')}=*{key.group('ext') or ''}", value if _DATE_LIKE.match(value) else None
    date: str | None = None
    for pattern, placeholder, as_date in _TOKEN_RULES:
        def _sub(match: re.Match[str], _ph: str = placeholder, _as: Callable[[re.Match[str]], str] | None = as_date) -> str:
            nonlocal date
            if _as is not None and date is None:
                date = _as(match)
            return _ph

        segment = pattern.sub(_sub, segment)
    return segment, date


@dataclass
class DatasetStats:
    """Aggregate of one dataset path template (partition and run tokens replaced by placeholders)."""

    files: int = 0
    bytes: int = 0
    small_files: int = 0
    first: str | None = None
    last: str | None = None
    latest_mtime: str | None = None

    def add(self, size: int, date: str | None, mtime: str) -> None:
        self.files += 1
        self.bytes += size
        self.small_files += size < SMALL_OBJECT_BYTES
        if date is not None:
            self.first = date if self.first is None or date < self.first else self.first
            self.last = date if self.last is None or date > self.last else self.last
        if self.latest_mtime is None or mtime > self.latest_mtime:
            self.latest_mtime = mtime


@dataclass
class Catalog:
    """Catalog document; ``to_json`` is the published contract."""

    generated_at: str
    root: str
    projects: dict[str, dict[str, DatasetStats]] = field(default_factory=dict)
    duplicate_identical: list[str] = field(default_factory=list)
    duplicate_conflicting: list[str] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        projects: dict[str, Any] = {}
        for project, datasets in sorted(self.projects.items()):
            projects[project] = {
                "files": sum(d.files for d in datasets.values()),
                "bytes": sum(d.bytes for d in datasets.values()),
                "datasets": [
                    {"template": template, **vars(stats)} for template, stats in sorted(datasets.items())
                ],
            }
        return {
            "schema": "quant-lake-catalog-v1",
            "generated_at": self.generated_at,
            "root": self.root,
            "files": sum(p["files"] for p in projects.values()),
            "bytes": sum(p["bytes"] for p in projects.values()),
            "duplicates": {"identical": self.duplicate_identical, "conflicting": self.duplicate_conflicting},
            "projects": projects,
        }


def project_of(path: str) -> str:
    """`live/<p>/…`, `research/<p>/…`, `projects/<p>/…` → `<tier>/<p>`; anything else → its first segment."""
    parts = path.split("/")
    if len(parts) >= 3 and parts[0] in {"live", "research", "projects", "_backup"}:
        return f"{parts[0]}/{parts[1]}"
    return parts[0]


def template_of(path: str) -> tuple[str, str | None]:
    """Dataset template of a project-relative path (dates, symbols, hours and hashes → placeholders) and the
    first date it carries (YYYY-MM[-DD]) if any."""
    date: str | None = None
    out: list[str] = []
    for segment in path.split("/"):
        normalized, found = _normalize_segment(segment)
        date = date or found
        out.append(normalized)
    return "/".join(out), date


def build_catalog(entries: Iterable[Mapping[str, Any]], *, root: str, generated_at: datetime) -> Catalog:
    """Aggregate an `rclone lsjson -R --hash` listing into a catalog; directories are ignored."""
    catalog = Catalog(generated_at=generated_at.astimezone(UTC).isoformat(timespec="seconds"), root=root)
    seen: dict[str, set[tuple[int, str | None]]] = defaultdict(set)
    counts: dict[str, int] = defaultdict(int)
    for entry in entries:
        if entry.get("IsDir", False):
            continue
        path = str(entry["Path"])
        size = int(entry.get("Size", 0))
        hashes = entry.get("Hashes")
        md5 = str(hashes["md5"]) if isinstance(hashes, Mapping) and hashes.get("md5") else None
        counts[path] += 1
        seen[path].add((size, md5))
        project = project_of(path)
        relative = path[len(project) + 1 :] if path.startswith(f"{project}/") else path
        template, date = template_of(relative)
        catalog.projects.setdefault(project, {}).setdefault(template, DatasetStats()).add(
            size, date, str(entry.get("ModTime", ""))
        )
    for path, count in sorted(counts.items()):
        if count < 2:
            continue
        variants = seen[path]
        identical = len(variants) == 1 and next(iter(variants))[1] is not None
        (catalog.duplicate_identical if identical else catalog.duplicate_conflicting).append(path)
    return catalog


def list_remote(root: str, runner: Callable[..., Any] = subprocess.run) -> list[dict[str, Any]]:
    """Recursive listing with md5 of ``root``.

    Raises:
        RuntimeError: rclone failed or returned invalid JSON.
    """
    cmd = ["rclone", "lsjson", root, "--recursive", "--fast-list", "--files-only", "--hash", "--hash-type", "md5"]
    result = runner(cmd, capture_output=True, text=True, check=False, timeout=1800)
    if result.returncode != 0:
        raise RuntimeError(f"lsjson failed rc={result.returncode}: {(result.stderr or '').strip()[-300:]}")
    try:
        entries: list[dict[str, Any]] = json.loads(result.stdout)
    except ValueError as exc:
        raise RuntimeError("lsjson returned invalid JSON") from exc
    return entries


def publish(document: Mapping[str, Any], root: str, work: Path, runner: Callable[..., Any] = subprocess.run) -> None:
    """Upload the catalog to ``<root>/_catalog/index.json``; the previous copy moves to ``_catalog/_versions``.

    Raises:
        RuntimeError: the upload failed.
    """
    work.mkdir(parents=True, exist_ok=True)
    local = work / "index.json"
    local.write_text(json.dumps(document, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    stamp = str(document["generated_at"])[:10]
    cmd = ["rclone", "copyto", str(local), f"{root}/{CATALOG_REPO_PATH}", "--backup-dir", f"{root}/_catalog/_versions/{stamp}"]
    result = runner(cmd, capture_output=True, text=True, check=False, timeout=600)
    if result.returncode != 0:
        raise RuntimeError(f"catalog publish failed rc={result.returncode}: {(result.stderr or '').strip()[-300:]}")


def _summary(document: Mapping[str, Any]) -> str:
    lines = [f"[DATA] stage=lake_catalog files={document['files']} bytes={document['bytes']}"]
    for project, body in document["projects"].items():
        small = sum(d["small_files"] for d in body["datasets"])
        lines.append(
            f"[DATA] stage=lake_catalog project={project} files={body['files']} bytes={body['bytes']} "
            f"datasets={len(body['datasets'])} small_files={small}"
        )
    dup = document["duplicates"]
    lines.append(f"[DATA] stage=lake_catalog duplicates_identical={len(dup['identical'])} duplicates_conflicting={len(dup['conflicting'])}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None, *, runner: Callable[..., Any] = subprocess.run) -> int:
    """Build the catalog; exit 2 when conflicting duplicate names exist (a restore would be ambiguous)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=DEFAULT_ROOT)
    parser.add_argument("--from-json", type=Path, help="use a saved `rclone lsjson -R --hash` listing instead of Drive")
    parser.add_argument("--out", type=Path, help="write the catalog JSON here")
    parser.add_argument("--publish", action="store_true", help=f"upload to <root>/{CATALOG_REPO_PATH}")
    parser.add_argument("--work", type=Path, help="staging dir for --publish (default: a private temp dir)")
    args = parser.parse_args(argv)
    entries = json.loads(args.from_json.read_text(encoding="utf-8")) if args.from_json else list_remote(args.root, runner)
    document = build_catalog(entries, root=args.root, generated_at=datetime.now(UTC)).to_json()
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(document, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    if args.publish:
        if args.work is not None:
            publish(document, args.root, args.work, runner)
        else:
            with tempfile.TemporaryDirectory(prefix="lake-catalog-") as work:
                publish(document, args.root, Path(work), runner)
    sys.stdout.write(_summary(document) + "\n")
    return 2 if document["duplicates"]["conflicting"] else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
