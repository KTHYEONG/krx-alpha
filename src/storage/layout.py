"""Single source of truth for L0 journal and L1 archive partition paths.

Why: the L0 deletion gate compares an L1 repo path computed here with the set of
remotely verified objects. Any drift between writer, normalizer, offload and
retention layout rules silently disables L0 deletion. All functions are pure
path algebra: no filesystem I/O, no clock access.
"""

from __future__ import annotations

import datetime as dt
import pathlib
import re
from dataclasses import dataclass
from typing import Final

from src.realtime.contracts import MarketSession, MarketVenue

L1_REPO_PREFIX: Final[str] = "l1/"
L0_REPO_PREFIX: Final[str] = "l0/"
QUARANTINE_REPO_PREFIX: Final[str] = "quarantine/"
L0_JOURNAL_GLOB: Final[str] = "*.jsonl.zst"

_L1_DT_RE = re.compile(r"^dt=(\d{4}-\d{2}-\d{2})\.parquet$")


@dataclass(frozen=True, slots=True)
class L0PartitionKey:
    """Typed identity of one routed L0 journal partition (one stream, one KST day)."""

    vendor: str
    venue: MarketVenue
    session: MarketSession
    stream: str
    day: dt.date


@dataclass(frozen=True, slots=True)
class L0PartitionRoute:
    """Route defaults inferred from an L0 partition directory for null-filling legacy records.

    `venue` stays a raw directory string because the pre-refactor normalizer
    never validated it; coercing it would change L1 output for hand-made trees.
    """

    venue: str
    session: MarketSession
    stream: str


def l0_partition_key(
    vendor: str, venue: str, session: str, stream: str, day: dt.date
) -> L0PartitionKey | None:
    """Build a key from untyped route strings, failing closed.

    Returns:
        None when `venue` or `session` is not an enum value. Callers treat
        that as "no journal exists" and never raise.
    """
    try:
        return L0PartitionKey(
            vendor=vendor,
            venue=MarketVenue(venue),
            session=MarketSession(session),
            stream=stream,
            day=day,
        )
    except ValueError:
        return None


def l0_partition_relpath(key: L0PartitionKey) -> pathlib.PurePosixPath:
    """Return `vendor/venue/session/stream/dt=YYYY-MM-DD` relative to the journal root.

    An empty `stream` collapses its segment, matching the historical pathlib join
    byte for byte (layout L0-EMPTY).
    """
    return pathlib.PurePosixPath(
        key.vendor,
        key.venue.value,
        key.session.value,
        key.stream,
        f"dt={key.day.isoformat()}",
    )


def l0_journal_file_name(hour: int, file_tag: str | None) -> str:
    """Hourly append-only journal file name: `HH.jsonl.zst` or `HH.{file_tag}.jsonl.zst`.

    The shard tag identifies the owning process, so restart gaps can be measured
    per shard inside a partition that several shards share.
    """
    if file_tag:
        return f"{hour:02d}.{file_tag}.jsonl.zst"
    return f"{hour:02d}.jsonl.zst"


def l0_journal_file_glob(file_tag: str | None) -> str:
    """Glob matching only files written under `file_tag` (untagged excludes every tagged file)."""
    if file_tag:
        return f"[0-9][0-9].{file_tag}.jsonl.zst"
    return "[0-9][0-9].jsonl.zst"


def route_for_l0_partition(part_dir: pathlib.PurePath) -> L0PartitionRoute:
    """Infer route defaults from the trailing components of an L0 partition dir.

    A dir is routed iff its grandparent name is a `MarketSession` value. Otherwise
    it is legacy (L0-LEG / L0-EMPTY) and defaults to `krx` / `regular`.
    `stream` is always the parent name. Accepts absolute or relative paths because
    the normalizer does not know the journal root.
    """
    part = pathlib.PurePath(part_dir)
    stream = part.parent.name
    session_name = part.parent.parent.name
    try:
        session = MarketSession(session_name)
    except ValueError:
        return L0PartitionRoute(venue="krx", session=MarketSession.REGULAR, stream=stream)
    return L0PartitionRoute(venue=part.parent.parent.parent.name, session=session, stream=stream)


def l1_relpath_for_l0_partition(l0_relpath: pathlib.PurePath) -> pathlib.PurePosixPath:
    """Map any L0 partition relpath to its L1 parquet relpath (`<parent>/<dt=D>.parquet`).

    Shape-agnostic by contract: the retention gate must give the same answer for
    current, legacy and unknown shapes alike (layout L0-ANY).
    """
    rel = pathlib.PurePosixPath(l0_relpath)
    return rel.parent / f"{rel.name}.parquet"


def l1_repo_path(l1_relpath: pathlib.PurePath) -> str:
    """Remote-relative key `l1/<posix relpath>` used both for upload and for deletion verification."""
    return L1_REPO_PREFIX + pathlib.PurePosixPath(l1_relpath).as_posix()


def l0_day_journal_glob(vendor: str, day: dt.date) -> str:
    """Recursive glob (relative to the journal root) matching every journal file of `vendor` on `day`.

    Matches routed, legacy and empty-stream shapes alike because `**` spans the venue/session/stream
    segments; liveness probes must not miss any shape.
    """
    return f"{vendor}/**/dt={day.isoformat()}/{L0_JOURNAL_GLOB}"


def quarantine_repo_path_for_l0(repo_path: str) -> str:
    """Map a remote L0 object path to its remote quarantine counterpart (`l0/<rel>` -> `quarantine/<rel>`).

    Raises:
        ValueError: `repo_path` does not start with `L0_REPO_PREFIX`.
    """
    if not repo_path.startswith(L0_REPO_PREFIX):
        raise ValueError(f"repo_path does not start with {L0_REPO_PREFIX!r}: {repo_path!r}")
    return QUARANTINE_REPO_PREFIX + repo_path[len(L0_REPO_PREFIX) :]


def l0_partition_for_l1(repo_path: str) -> str | None:
    """Map a remote L1 object path to the remote L0 partition directory it supersedes.

    Why: L1 parquet retains every raw L0 frame losslessly, so once an L1 object is
    verified remotely its L0 journal partition is redundant offsite.

    Args:
        repo_path: Remote-relative posix path, e.g. "l1/ls/krx/regular/H0STASP0/dt=2026-09-18.parquet".

    Returns:
        "l0/<parent>/dt=YYYY-MM-DD" for journal-backed L1 objects; None for snapshot
        datasets ("l1/snapshot/..."), non-"l1/" paths, or names not matching "dt=YYYY-MM-DD.parquet".
    """
    if not repo_path.startswith(L1_REPO_PREFIX):
        return None
    rest = repo_path[len(L1_REPO_PREFIX) :]
    if rest == "snapshot" or rest.startswith("snapshot/"):
        return None
    if "/" not in rest:
        return None
    parent, _, name = rest.rpartition("/")
    if not parent:
        return None
    match = _L1_DT_RE.match(name)
    if match is None:
        return None
    try:
        dt.date.fromisoformat(match.group(1))
    except ValueError:
        return None
    return f"{L0_REPO_PREFIX}{parent}/{name[: -len('.parquet')]}"
