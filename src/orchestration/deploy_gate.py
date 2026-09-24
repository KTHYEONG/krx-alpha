"""Deploy session gate: defer collector recreate during the KST collection session."""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal
from zoneinfo import ZoneInfo

from src.core.calendar import SessionSchedule

EXIT_PROCEED: int = 0
EXIT_DEFER: int = 10
# 스트리머 기동(08:20) 직전 재기동이 세션 시작을 밀지 않도록 앞당겨 막는다.
DEPLOY_BUSY_LEAD: dt.timedelta = dt.timedelta(minutes=10)
# 애프터마켓 EOD(20:30) + 오프로드/퍼지 여유 뒤, 호스트 백업(23:30) 전. 타이머 OnCalendar 와 같아야 한다.
DEFERRED_RECREATE_KST: dt.time = dt.time(22, 0)

_KST = ZoneInfo("Asia/Seoul")


@dataclass(frozen=True, slots=True)
class DeployDecision:
    action: Literal["proceed", "defer"]
    reason: str


def deploy_decision(now: dt.datetime, schedule: SessionSchedule | None = None) -> DeployDecision:
    """Whether a push at ``now`` may recreate the collector immediately.

    Busy window (defer): Monday-Friday in Asia/Seoul, from ``schedule.streamer_start - DEPLOY_BUSY_LEAD``
    (inclusive) to ``DEFERRED_RECREATE_KST`` (exclusive). Everything else proceeds.

    Raises:
        ValueError: when ``now`` is naive.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    sched = schedule if schedule is not None else SessionSchedule()
    kst = now.astimezone(_KST)
    if kst.weekday() in (5, 6):
        return DeployDecision(action="proceed", reason="weekend")
    start = dt.datetime.combine(kst.date(), sched.streamer_start, tzinfo=_KST) - DEPLOY_BUSY_LEAD
    end = dt.datetime.combine(kst.date(), DEFERRED_RECREATE_KST, tzinfo=_KST)
    if start <= kst < end:
        return DeployDecision(action="defer", reason="session_busy")
    return DeployDecision(action="proceed", reason="outside_session")


def _parse_now(raw: str | None) -> dt.datetime:
    if raw is None:
        return dt.datetime.now(_KST)
    text = raw.strip().removesuffix("Z") + ("+00:00" if raw.strip().endswith("Z") else "")
    return dt.datetime.fromisoformat(text)


def main(argv: Sequence[str] | None = None) -> int:
    """``python3 -m src.orchestration.deploy_gate [--now ISO8601]``; prints ``action=<..> reason=<..>``
    and returns ``EXIT_PROCEED`` or ``EXIT_DEFER``."""
    parser = argparse.ArgumentParser(description="Decide whether a push may recreate krx-collector now.")
    parser.add_argument("--now", default=None, help="ISO8601 timestamp (default: current KST time)")
    args = parser.parse_args(list(argv) if argv is not None else None)
    decision = deploy_decision(_parse_now(args.now))
    print(f"action={decision.action} reason={decision.reason}")  # noqa: T201 - CI parses stdout
    return EXIT_PROCEED if decision.action == "proceed" else EXIT_DEFER


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
