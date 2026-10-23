"""Scale invariance for tick-tape backfill: peak RSS must not grow with tick count."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.slow

_CHILD = r"""
import datetime as dt
import pathlib
import random
import resource
import sys

import polars as pl

from src.marketdata.kiwoom_ticks import TapeTick, TickPage, walk_symbol_tape
from src.marketdata.tick_tape_backfill import _write_day_chunk

num_days = int(sys.argv[1])
per_day = int(sys.argv[2])
tmp = pathlib.Path(sys.argv[3])
base = dt.date(2026, 9, 1)
days = [base + dt.timedelta(days=i) for i in range(num_days)]
wanted = frozenset(days)
ordered = sorted(days, reverse=True)
per_page = 10_000
state = {"day_idx": 0, "remaining": per_day}
rng = random.Random(20261023)
staged_rows = 0

def fetch_page(key):
    while state["day_idx"] < len(ordered):
        if state["remaining"] <= 0:
            state["day_idx"] += 1
            state["remaining"] = per_day
            continue
        n = min(per_page, state["remaining"])
        day = ordered[state["day_idx"]]
        ticks = tuple(TapeTick(day=day, ts_hms=120000, price=rng.randrange(1000, 100000), volume=rng.randrange(1, 1000)) for _ in range(n))
        state["remaining"] -= n
        nxt = f"k{state['day_idx']}-{state['remaining']}"
        return TickPage(ticks=ticks, next_key=nxt)
    return TickPage(ticks=(TapeTick(base - dt.timedelta(days=1), 120000, 1000, 1),), next_key=None)

def on_done(day, ticks):
    global staged_rows
    _write_day_chunk(tmp / day.isoformat() / "A.parquet", "A", day, ticks, "scale")
    staged_rows += len(ticks)

walk = walk_symbol_tape("000001", wanted_days=wanted, fetch_page=fetch_page, max_pages=100000, on_day_complete=on_done, max_ticks_per_day=None)
assert walk.emitted == wanted
assert staged_rows == num_days * per_day
assert pl.scan_parquet(str(tmp / "*" / "A.parquet")).select(pl.len()).collect().item() == staged_rows
peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
print(peak_kb)
print(f"pages={walk.pages} emitted={len(walk.emitted)}")
"""


def _peak(num_days: int, per_day: int, tmp_path) -> int:
    out = tmp_path / f"scale-{num_days}"
    out.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(  # noqa: S603 - test spawns fixed local child, no untrusted input
        [sys.executable, "-c", _CHILD, str(num_days), str(per_day), str(out)],
        capture_output=True, text=True, timeout=60, cwd=Path(__file__).resolve().parents[3],
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    return int(proc.stdout.strip().splitlines()[0])


def test_peak_memory_does_not_grow_with_tick_count(tmp_path):
    small_kb = _peak(10, 100_000, tmp_path)
    large_kb = _peak(40, 100_000, tmp_path)
    assert large_kb - small_kb < 64 * 1024, f"growth {(large_kb - small_kb) / 1024:.1f} MiB"
    assert large_kb < 400 * 1024, f"peak {large_kb / 1024:.1f} MiB"
