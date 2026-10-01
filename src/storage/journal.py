"""L0 원본 저널 writer (hourly-rotate JSONL.zst, append-only)."""

from __future__ import annotations

import datetime as dt
import json
import logging
import pathlib
from typing import Any
from zoneinfo import ZoneInfo

import zstandard as zstd

from src.core.errors import KrxAlphaError
from src.realtime.contracts import MarketSession, MarketVenue
from src.storage.layout import (
    L0PartitionKey,
    l0_journal_file_glob,
    l0_journal_file_name,
    l0_partition_relpath,
)

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")


class JournalWriteError(KrxAlphaError):
    """저널 쓰기 실패 (디스크 full 등). 수집 중단 신호로 표면화."""


class L0JournalWriter:
    """수집 원문을 파티션 파일에 append 하는 writer."""

    def __init__(
        self,
        root: pathlib.Path,
        vendor: str,
        venue: MarketVenue | str = MarketVenue.KRX,
        session: MarketSession | str = MarketSession.REGULAR,
        stream: str = "",
        *,
        compress_level: int = 3,
        file_tag: str | None = None,
    ) -> None:
        self._root = pathlib.Path(root)
        self._vendor = vendor
        self._venue = MarketVenue(venue)
        self._session = MarketSession(session)
        self._stream = stream
        self._compress_level = compress_level
        # 같은 파티션을 여러 shard 프로세스가 쓰면 파일 이름으로 소유자를 구분해야 재시작 공백을 shard 별로 잴 수 있다.
        self._file_tag = file_tag
        self._buffer: list[dict[str, Any]] = []

    def partition_path(self, recv_wall_ns: int) -> pathlib.Path:
        ts = dt.datetime.fromtimestamp(recv_wall_ns / 1_000_000_000, tz=_KST)
        return (
            self._root
            / l0_partition_relpath(
                L0PartitionKey(self._vendor, self._venue, self._session, self._stream, ts.date())
            )
            / l0_journal_file_name(ts.hour, self._file_tag)
        )

    def owned_files(self, at_ns: int) -> list[pathlib.Path]:
        """Journal files this writer (not sibling shards) wrote for the date of ``at_ns``."""
        return sorted(self.partition_path(at_ns).parent.glob(l0_journal_file_glob(self._file_tag)))

    def append(
        self,
        *,
        raw: str,
        recv_mono_ns: int,
        recv_wall_ns: int,
        conn_id: str,
        conn_seq: int,
        exchange_event_time: str = "",
        symbol: str = "",
    ) -> None:
        self._buffer.append({
            "raw": raw,
            "recv_mono_ns": recv_mono_ns,
            "recv_wall_ns": recv_wall_ns,
            "conn_id": conn_id,
            "conn_seq": conn_seq,
            "vendor": self._vendor,
            "tr_id": self._stream,
            "venue": self._venue.value,
            "session": self._session.value,
            "stream": self._stream,
            "symbol": symbol,
            "exchange_event_time": exchange_event_time,
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
