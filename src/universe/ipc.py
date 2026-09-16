"""승격 candidates 원자적 파일 IPC + 스캐너 발행 브릿지 (병합)."""

from __future__ import annotations

import datetime as dt
import json
import os
import pathlib
from dataclasses import dataclass
from typing import Any

from src.core.errors import KrxAlphaError


class CandidateFileError(KrxAlphaError):
    """깨진 candidates 파일 (부분 쓰기 등)."""


@dataclass(frozen=True)
class CandidateSnapshot:
    """세션 격리 후보 snapshot (aftermarket/regular 공용 스키마)."""

    schema_version: int
    rev: int
    session_date: dt.date
    session: str
    generated_at: dt.datetime
    source_asof: dt.datetime
    effective_from: dt.datetime
    policy_version: str
    capacity: int
    eligible_count: int
    selected_count: int
    candidates: tuple[dict[str, object], ...]


def _is_kst_timestamp(value: dt.datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() == dt.timedelta(hours=9)


def write_candidate_snapshot(path: pathlib.Path, snapshot: CandidateSnapshot) -> None:
    target = pathlib.Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": snapshot.schema_version,
        "rev": snapshot.rev,
        "session_date": snapshot.session_date.isoformat(),
        "session": snapshot.session,
        "generated_at": snapshot.generated_at.isoformat(),
        "source_asof": snapshot.source_asof.isoformat(),
        "effective_from": snapshot.effective_from.isoformat(),
        "policy_version": snapshot.policy_version,
        "capacity": snapshot.capacity,
        "eligible_count": snapshot.eligible_count,
        "selected_count": snapshot.selected_count,
        "candidates": list(snapshot.candidates),
    }
    tmp = target.parent / f".{target.name}.{os.getpid()}.tmp"
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, target)


def read_candidate_snapshot(
    path: pathlib.Path,
    *,
    expected_session_date: dt.date,
    expected_session: str,
    max_candidates: int,
) -> CandidateSnapshot:
    try:
        data: dict[str, Any] = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
        schema_version = data["schema_version"]
        rev = data["rev"]
        session_date = dt.date.fromisoformat(str(data["session_date"]))
        session = str(data["session"])
        generated_at = dt.datetime.fromisoformat(str(data["generated_at"]))
        source_asof = dt.datetime.fromisoformat(str(data["source_asof"]))
        effective_from = dt.datetime.fromisoformat(str(data["effective_from"]))
        policy_version = str(data["policy_version"])
        capacity = int(data["capacity"])
        eligible_count = int(data["eligible_count"])
        selected_count = int(data["selected_count"])
        raw_candidates = data["candidates"]
        rows = list(raw_candidates)
        symbols = [str(row["symbol"]) for row in rows]
        ranks = [int(row["rank"]) for row in rows]
        if (
            schema_version != 1
            or session_date != expected_session_date
            or session != expected_session
            or rev != int(session_date.strftime("%Y%m%d"))
            or not all(_is_kst_timestamp(value) for value in (source_asof, generated_at, effective_from))
            or not (source_asof <= generated_at <= effective_from)
            or capacity < 1
            or selected_count != len(rows)
            or eligible_count < selected_count
            or max_candidates < 1
            or len(rows) > max_candidates
            or len(set(symbols)) != len(symbols)
            or any(not (symbol.isdigit() and len(symbol) == 6) for symbol in symbols)
            or sorted(ranks) != list(range(1, len(rows) + 1))
        ):
            raise ValueError(f"invalid candidate snapshot: {path}")
        return CandidateSnapshot(schema_version=int(schema_version), rev=int(rev), session_date=session_date, session=session, generated_at=generated_at, source_asof=source_asof, effective_from=effective_from, policy_version=policy_version, capacity=capacity, eligible_count=eligible_count, selected_count=selected_count, candidates=tuple(rows))
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        raise CandidateFileError(f"corrupt candidate snapshot: {path}") from exc


def write_candidates(path: pathlib.Path, candidates: list[dict[str, object]], *, rev: int) -> None:
    target = pathlib.Path(path)
    payload = {"rev": rev, "candidates": candidates}
    tmp = target.parent / f".{target.name}.{os.getpid()}.tmp"
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, target)


def read_candidates(path: pathlib.Path) -> dict[str, object] | None:
    target = pathlib.Path(path)
    try:
        text = target.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    try:
        data: dict[str, object] = json.loads(text)
    except json.JSONDecodeError as exc:
        raise CandidateFileError(f"corrupt candidates file: {target}") from exc
    return data


def emit_candidates(path: pathlib.Path, rows: list[dict[str, Any]], *, rev: int) -> int:
    """스캐너 selection 행을 candidates IPC 로 발행한다."""
    mapped: list[dict[str, object]] = [
        {"symbol": str(row["symbol"]), "selection_reasons": list(row["selection_reasons"])} for row in rows
    ]
    write_candidates(pathlib.Path(path), mapped, rev=rev)
    return len(mapped)
