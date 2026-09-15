"""세션 manifest (ACK·gap 기록 + 클럭 게이트 + 원자적 저장)."""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import pathlib
from dataclasses import dataclass, field

from src.realtime.clock import ClockUnsyncedError

logger = logging.getLogger(__name__)


@dataclass
class SessionManifest:
    session_date: dt.date
    clock_offset_ns: int
    started_at_ns: int
    subscription_acks: list[dict[str, object]] = field(default_factory=list)
    gaps: list[dict[str, object]] = field(default_factory=list)
    candidates_rev: int | None = None
    degraded_reason: str | None = None
    clock_status: str = "measured"
    boots: list[dict[str, object]] = field(default_factory=list)
    venue: str = "krx"
    session: str = "regular"
    expected_close_ns: int = 0
    writer_closed_at_ns: int | None = None
    planned_pairs: list[dict[str, str]] = field(default_factory=list)
    shard_index: int | None = None
    credential_key_id: str | None = None

    def record_ack(self, *, vendor: str, tr_id: str, symbol: str, rt_cd: str, accepted: bool) -> None:
        self.subscription_acks.append({"vendor": vendor, "tr_id": tr_id, "symbol": symbol, "rt_cd": rt_cd, "accepted": accepted})

    def record_gap(self, *, symbol: str, gap_start_ns: int, gap_end_ns: int, reason: str) -> None:
        self.gaps.append({"symbol": symbol, "gap_start_ns": gap_start_ns, "gap_end_ns": gap_end_ns, "reason": reason})

    def accepted_symbols(self) -> set[str]:
        return {str(a["symbol"]) for a in self.subscription_acks if a.get("accepted") is True}

    def assert_clock_within(self, *, max_offset_ns: int) -> None:
        if abs(self.clock_offset_ns) > max_offset_ns:
            raise ClockUnsyncedError(f"clock offset {self.clock_offset_ns}ns exceeds {max_offset_ns}ns")

    def save(self, path: pathlib.Path) -> None:
        target = pathlib.Path(path)
        payload = {
            "session_date": self.session_date.isoformat(),
            "clock_offset_ns": self.clock_offset_ns,
            "started_at_ns": self.started_at_ns,
            "subscription_acks": self.subscription_acks,
            "gaps": self.gaps,
            "candidates_rev": self.candidates_rev,
            "degraded_reason": self.degraded_reason,
            "clock_status": self.clock_status,
            "boots": self.boots,
            "venue": self.venue,
            "session": self.session,
            "expected_close_ns": self.expected_close_ns,
            "writer_closed_at_ns": self.writer_closed_at_ns,
            "planned_pairs": self.planned_pairs,
            "shard_index": self.shard_index,
            "credential_key_id": self.credential_key_id,
        }
        tmp = target.parent / f".{target.name}.{os.getpid()}.tmp"
        if str(target.parent) not in ("", "."):
            target.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, target)
        logger.debug("[DATA] stage=manifest_save path=%s status=OK", str(target))

    @classmethod
    def load(cls, path: pathlib.Path) -> SessionManifest:
        raw = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
        rev_raw = raw.get("candidates_rev")
        return cls(
            session_date=dt.date.fromisoformat(str(raw["session_date"])),
            clock_offset_ns=int(raw["clock_offset_ns"]),
            started_at_ns=int(raw["started_at_ns"]),
            subscription_acks=list(raw.get("subscription_acks", [])),
            gaps=list(raw.get("gaps", [])),
            candidates_rev=int(rev_raw) if rev_raw is not None else None,
            degraded_reason=raw.get("degraded_reason"),
            clock_status=str(raw.get("clock_status", "measured")),
            boots=list(raw.get("boots", [])),
            venue=str(raw.get("venue", "krx")),
            session=str(raw.get("session", "regular")),
            expected_close_ns=int(raw.get("expected_close_ns", 0)),
            writer_closed_at_ns=int(raw["writer_closed_at_ns"]) if raw.get("writer_closed_at_ns") is not None else None,
            planned_pairs=[dict(pair) for pair in raw.get("planned_pairs", [])],
            shard_index=int(raw["shard_index"]) if raw.get("shard_index") is not None else None,
            credential_key_id=str(raw["credential_key_id"]) if raw.get("credential_key_id") is not None else None,
        )
