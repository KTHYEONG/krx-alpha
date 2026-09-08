"""스캐너->스트리머 승격 브릿지 (selection 행을 candidates IPC 로 발행)."""

from __future__ import annotations

import pathlib
from typing import Any

from src.collector.ipc import write_candidates


def emit_candidates(path: pathlib.Path, rows: list[dict[str, Any]], *, rev: int) -> int:
    mapped: list[dict[str, object]] = [
        {"symbol": str(row["symbol"]), "selection_reasons": list(row["selection_reasons"])} for row in rows
    ]
    write_candidates(pathlib.Path(path), mapped, rev=rev)
    return len(mapped)
