"""승격 candidates 원자적 파일 IPC."""

from __future__ import annotations

import json
import os
import pathlib


class CandidateFileError(RuntimeError):
    """깨진 candidates 파일 (부분 쓰기 등)."""


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
