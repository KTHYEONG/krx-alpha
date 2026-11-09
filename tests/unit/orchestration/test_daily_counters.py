from __future__ import annotations

import datetime as dt
import json
import logging
import os

import pytest

import src.orchestration.daily_counters as dc_mod
from src.orchestration.daily_counters import DailyCounter, DailyCounters

D = dt.date(2026, 11, 9)
WARN_LOGGER = dc_mod.logger.name


def _zero() -> dict[DailyCounter, int]:
    return dict.fromkeys(DailyCounter, 0)


def test_bump_counts_and_persists(tmp_path) -> None:
    path = tmp_path / "work" / "daily_counters.json"
    counters = DailyCounters(path)
    assert counters.bump(DailyCounter.AFTERMARKET_RESTARTS, D) == 1
    assert counters.bump(DailyCounter.AFTERMARKET_RESTARTS, D) == 2
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["day"] == D.isoformat()
    assert payload["counts"]["aftermarket_restarts"] == 2
    assert not (path.parent / f".{path.name}.tmp").exists()


def test_survives_new_instance(tmp_path) -> None:
    path = tmp_path / "daily_counters.json"
    writer = DailyCounters(path)
    writer.bump(DailyCounter.STREAMER_RESTARTS, D)
    writer.bump(DailyCounter.SHARD_STALE_RESTARTS, D)
    expected = _zero() | {DailyCounter.STREAMER_RESTARTS: 1, DailyCounter.SHARD_STALE_RESTARTS: 1}
    assert DailyCounters(path).snapshot(D) == expected


def test_new_day_resets(tmp_path) -> None:
    path = tmp_path / "daily_counters.json"
    counters = DailyCounters(path)
    for _ in range(3):
        counters.bump(DailyCounter.AFTERMARKET_RESTARTS, D)
    assert counters.bump(DailyCounter.AFTERMARKET_RESTARTS, D + dt.timedelta(days=1)) == 1
    assert counters.snapshot(D) == _zero()


def test_snapshot_other_day_is_zero_and_does_not_write(tmp_path) -> None:
    path = tmp_path / "daily_counters.json"
    DailyCounters(path).bump(DailyCounter.AFTERMARKET_RESTARTS, D)
    before = path.read_bytes()
    counters = DailyCounters(path)
    assert counters.snapshot(D + dt.timedelta(days=1)) == _zero()
    assert path.read_bytes() == before


@pytest.mark.parametrize(
    "content",
    [
        "{not json",
        json.dumps({"version": 2, "day": D.isoformat(), "counts": {"streamer_restarts": 1}}),
        json.dumps({"version": 1, "day": D.isoformat(), "counts": {"streamer_restarts": -1}}),
        json.dumps({"version": 1, "day": D.isoformat(), "counts": {"unknown_counter": 1}}),
        json.dumps({"version": 1, "day": D.isoformat(), "counts": []}),
    ],
)
def test_corrupt_file_is_empty_and_logged_once(tmp_path, caplog, content) -> None:
    path = tmp_path / "daily_counters.json"
    path.write_text(content, encoding="utf-8")
    counters = DailyCounters(path)
    with caplog.at_level(logging.WARNING, logger=WARN_LOGGER):
        assert counters.snapshot(D) == _zero()
        assert counters.bump(DailyCounter.STREAMER_RESTARTS, D) == 1
        assert counters.snapshot(D)[DailyCounter.STREAMER_RESTARTS] == 1
    warnings = [r for r in caplog.records if "stage=daily_counters status=READ_FAIL" in r.getMessage()]
    assert len(warnings) == 1
    assert warnings[0].exc_info is not None


def test_undecodable_file_is_empty_and_logged_once(tmp_path, caplog) -> None:
    path = tmp_path / "daily_counters.json"
    path.write_bytes(b"\xff\xfe\x00garbage")
    counters = DailyCounters(path)
    with caplog.at_level(logging.WARNING, logger=WARN_LOGGER):
        assert counters.snapshot(D) == _zero()
        assert counters.bump(DailyCounter.STREAMER_RESTARTS, D) == 1
    warnings = [r for r in caplog.records if "stage=daily_counters status=READ_FAIL" in r.getMessage()]
    assert len(warnings) == 1


def test_unwritable_directory_does_not_raise(tmp_path, caplog) -> None:
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory", encoding="utf-8")
    counters = DailyCounters(blocker / "daily_counters.json")
    with caplog.at_level(logging.WARNING, logger=WARN_LOGGER):
        assert counters.bump(DailyCounter.AFTERMARKET_RESTARTS, D) == 1
        assert counters.bump(DailyCounter.AFTERMARKET_RESTARTS, D) == 2
    warnings = [r for r in caplog.records if "status=WRITE_FAIL" in r.getMessage()]
    assert len(warnings) == 1


def test_bounded_size(tmp_path) -> None:
    path = tmp_path / "daily_counters.json"
    counters = DailyCounters(path)
    for i in range(10_000):
        counters.bump(DailyCounter(("streamer_restarts", "aftermarket_restarts", "shard_stale_restarts")[i % 3]), D)
    assert path.stat().st_size < 512
    assert set(json.loads(path.read_text(encoding="utf-8"))["counts"]) == {c.value for c in DailyCounter}


def test_atomic_replace_keeps_previous_content(tmp_path, monkeypatch) -> None:
    path = tmp_path / "daily_counters.json"
    counters = DailyCounters(path)
    counters.bump(DailyCounter.STREAMER_RESTARTS, D)
    before = path.read_bytes()

    def _fail(src, dst):
        raise OSError("replace refused")

    monkeypatch.setattr(os, "replace", _fail)
    assert counters.bump(DailyCounter.STREAMER_RESTARTS, D) == 2
    assert path.read_bytes() == before
    assert not (tmp_path / f".{path.name}.tmp").exists()


def test_unknown_counter_name_is_rejected(tmp_path) -> None:
    with pytest.raises(ValueError, match="made_up"):
        DailyCounters(tmp_path / "daily_counters.json").bump("made_up", D)  # type: ignore[arg-type]
