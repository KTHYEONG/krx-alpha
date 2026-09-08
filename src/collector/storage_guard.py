"""수집기 디스크 워터마크 및 저널 보존 가드."""

from __future__ import annotations

import datetime as dt
import pathlib
import re
import shutil
from zoneinfo import ZoneInfo

_KST = ZoneInfo("Asia/Seoul")
_DT_RE = re.compile(r"dt=(\d{4})-(\d{2})-(\d{2})")


class StorageExhaustedError(RuntimeError):
    """여유 디스크가 워터마크 미만일 때 발생하는 Fail-Closed 신호."""


def check_disk_watermark(path: pathlib.Path, *, min_free_gb: float = 3.0) -> bool:
    usage = shutil.disk_usage(path)
    return usage.free >= min_free_gb * (1024**3)


def prune_old_journals(
    root: pathlib.Path, *, retain_days: int = 3, reference_date: dt.date | None = None
) -> int:
    ref = reference_date or dt.datetime.now(_KST).date()
    cutoff = ref - dt.timedelta(days=retain_days)
    deleted = 0
    for part in [p for p in pathlib.Path(root).rglob("dt=*") if p.is_dir()]:
        m = _DT_RE.fullmatch(part.name)
        part_date = dt.date.fromisoformat(m.group(0)[3:]) if m else None
        if part_date is None or part_date >= cutoff:
            continue
        deleted += sum(1 for f in part.rglob("*") if f.is_file())
        shutil.rmtree(part)
    return deleted
