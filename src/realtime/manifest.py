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
        }
        tmp = target.parent / f".{target.name}.{os.getpid()}.tmp"
        if str(target.parent) not in ("", "."):
            target.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, target)
        logger.info("[DATA] stage=manifest_save path=%s status=OK", str(target))

    @classmethod
    def load(cls, path: pathlib.Path) -> SessionManifest:
        raw = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
        return cls(
            session_date=dt.date.fromisoformat(str(raw["session_date"])),
            clock_offset_ns=int(raw["clock_offset_ns"]),
            started_at_ns=int(raw["started_at_ns"]),
            subscription_acks=list(raw.get("subscription_acks", [])),
            gaps=list(raw.get("gaps", [])),
        )
