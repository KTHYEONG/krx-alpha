"""Research membership ledger recording when each symbol entered collection, at which tier and feed."""

from __future__ import annotations

import dataclasses
import fcntl
import json
import os
import pathlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final, Literal

from src.core.errors import KrxAlphaError

_TIER_RANK: Final[Mapping[str, int]] = {"C": 0, "B": 1, "A": 2}
_ACK_SOURCE: Final[str] = "ack"


@dataclass(frozen=True)
class MembershipRow:
    symbol: str
    tier: Literal["A", "B", "C"]
    source: str
    decided_at_ns: int
    direction: str | None
    warning_level: str | None
    ls_shard: int | None
    kis_slot: str | None
    trade_value_krw: int | None
    reason: str | None


_FIELD_NAMES: Final[frozenset[str]] = frozenset(f.name for f in dataclasses.fields(MembershipRow))


class MembershipFileError(KrxAlphaError):
    """Membership ledger unreadable or malformed."""


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_str(value: object) -> bool:
    return isinstance(value, str)


def _is_optional_str(value: object) -> bool:
    return value is None or isinstance(value, str)


def _is_optional_int(value: object) -> bool:
    return value is None or _is_int(value)


def _is_tier(value: object) -> bool:
    return isinstance(value, str) and value in _TIER_RANK


_FIELD_VALIDATORS: Final[Mapping[str, Callable[[object], bool]]] = {
    "symbol": _is_str,
    "tier": _is_tier,
    "source": _is_str,
    "decided_at_ns": _is_int,
    "direction": _is_optional_str,
    "warning_level": _is_optional_str,
    "ls_shard": _is_optional_int,
    "kis_slot": _is_optional_str,
    "trade_value_krw": _is_optional_int,
    "reason": _is_optional_str,
}


def _parse_row(obj: object) -> MembershipRow:
    if not isinstance(obj, dict) or set(obj) != _FIELD_NAMES:
        raise ValueError(f"record must be a JSON object with exactly fields {sorted(_FIELD_NAMES)}")
    record: dict[str, Any] = obj
    for name, valid in _FIELD_VALIDATORS.items():
        if not valid(record[name]):
            raise ValueError(f"field {name!r} has invalid value {record[name]!r}")
    return MembershipRow(**record)


def _encode_row(path: pathlib.Path, row: MembershipRow) -> bytes:
    obj = dataclasses.asdict(row)
    try:
        _parse_row(obj)
    except ValueError as exc:
        raise MembershipFileError(f"{path}: rejected row for symbol {row.symbol!r}: {exc}") from exc
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"


def _write_all(fd: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        view = view[os.write(fd, view) :]


def append_membership(path: pathlib.Path, rows: Sequence[MembershipRow]) -> None:
    """Append rows as JSONL under an exclusive flock; raises MembershipFileError on an invalid row before writing."""
    if not rows:
        return
    payload = b"".join(_encode_row(path, row) for row in rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            _write_all(fd, payload)
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def read_membership(path: pathlib.Path) -> tuple[MembershipRow, ...]:
    """Read all rows in file order; a missing file yields (), a malformed line raises MembershipFileError with its line number."""
    try:
        fd = os.open(path, os.O_RDONLY)
    except FileNotFoundError:
        return ()
    with os.fdopen(fd, "rb") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_SH)
        raw = handle.read()
    lines = raw.split(b"\n")
    if lines[-1] == b"":
        lines.pop()
    rows: list[MembershipRow] = []
    for line_no, line in enumerate(lines, start=1):
        try:
            rows.append(_parse_row(json.loads(line.decode("utf-8"))))
        except ValueError as exc:
            raise MembershipFileError(f"{path}: line {line_no}: {exc}") from exc
    return tuple(rows)


def effective_tiers(rows: Sequence[MembershipRow]) -> Mapping[str, MembershipRow]:
    """Map each symbol to its latest row at the highest tier seen; ack rows are ignored and a lower tier never replaces."""
    current: dict[str, MembershipRow] = {}
    for row in rows:
        if row.source == _ACK_SOURCE:
            continue
        existing = current.get(row.symbol)
        if existing is None or _TIER_RANK[row.tier] >= _TIER_RANK[existing.tier]:
            current[row.symbol] = row
    return current
