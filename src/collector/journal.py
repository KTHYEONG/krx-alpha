"""L0 원본 저널 writer (hourly-rotate JSONL.zst, append-only)."""

from __future__ import annotations

import datetime as dt
import json
import logging
import pathlib
from typing import Any
from zoneinfo import ZoneInfo

import zstandard as zstd

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")


class JournalWriteError(RuntimeError):
    """저널 쓰기 실패 (디스크 full 등). 수집 중단 신호로 표면화."""


class L0JournalWriter:
    """수집 원문을 파티션 파일에 append 하는 writer."""

    def __init__(self, root: pathlib.Path, vendor: str, stream: str, *, compress_level: int = 3) -> None:
        self._root = pathlib.Path(root)
        self._vendor = vendor
        self._stream = stream
        self._compress_level = compress_level
        self._buffer: list[dict[str, Any]] = []

    def partition_path(self, recv_wall_ns: int) -> pathlib.Path:
        ts = dt.datetime.fromtimestamp(recv_wall_ns / 1_000_000_000, tz=_KST)
        return self._root / self._vendor / self._stream / f"dt={ts.date().isoformat()}" / f"{ts.hour:02d}.jsonl.zst"

    def append(self, *, raw: str, recv_mono_ns: int, recv_wall_ns: int, conn_id: str, conn_seq: int) -> None:
        self._buffer.append({
            "raw": raw,
            "recv_mono_ns": recv_mono_ns,
            "recv_wall_ns": recv_wall_ns,
            "conn_id": conn_id,
            "conn_seq": conn_seq,
            "vendor": self._vendor,
            "tr_id": self._stream,
        })

    def flush(self) -> int:
        if not self._buffer:
            return 0
        groups: dict[pathlib.Path, list[dict[str, Any]]] = {}
        for rec in self._buffer:
            part = self.partition_path(int(rec["recv_wall_ns"]))
            groups.setdefault(part, []).append(rec)
        count = len(self._buffer)
        try:
            for part, recs in groups.items():
                part.parent.mkdir(parents=True, exist_ok=True)
                payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in recs) + "\n"
                frame = zstd.ZstdCompressor(level=self._compress_level).compress(payload.encode("utf-8"))
                with open(part, "ab") as fh:
                    fh.write(frame)
        except OSError as exc:
            logger.critical("[DATA] stage=journal_flush status=FAIL reason=%s", str(exc))
            raise JournalWriteError(str(exc)) from exc
        self._buffer.clear()
        return count
