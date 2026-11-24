"""Invariant guards for the NXT regular-session symbol pool (pure selection + atomic file)."""

from __future__ import annotations

import datetime as dt
import json
import zoneinfo

import pytest

from src.marketdata.kiwoom_ranking import KiwoomRankingRow
from src.universe import regular_nxt as pool
from src.universe.ipc import read_candidate_snapshot

KST = zoneinfo.ZoneInfo("Asia/Seoul")
TARGET = dt.date(2026, 11, 17)
GEN = dt.datetime(2026, 11, 16, 20, 10, tzinfo=KST)
EFF = dt.datetime(2026, 11, 17, 8, 58, tzinfo=KST)


def _row(rank: int, symbol: str, value: int) -> KiwoomRankingRow:
    return KiwoomRankingRow(rank=rank, symbol=symbol, last_price=10_000, change_pct=1.0, trade_value_krw=value)


def _build(rows, **overrides):
    kwargs = {
        "target_date": TARGET,
        "generated_at": GEN,
        "effective_from": EFF,
        "rows": rows,
        "capacity": 40,
    }
    kwargs.update(overrides)
    return pool.build_regular_nxt_snapshot(**kwargs)


def test_ranks_by_value_then_symbol() -> None:
    # Given: equal values in vendor order that disagrees with symbol order
    rows = [_row(1, "000660", 500), _row(2, "005930", 900), _row(3, "035420", 500)]

    # When
    snapshot = _build(rows)

    # Then: value desc, symbol asc on ties, ranks 1..n
    assert [(c["symbol"], c["rank"]) for c in snapshot.candidates] == [("005930", 1), ("000660", 2), ("035420", 3)]
    assert snapshot.session == "regular_nxt"
    assert snapshot.rev == 20261117
    assert snapshot.selected_count == 3 == snapshot.eligible_count


def test_excluded_and_duplicates() -> None:
    # Given: an excluded symbol and a duplicate code whose later row carries the higher value
    rows = [_row(1, "005930", 100), _row(2, "000660", 300), _row(3, "005930", 200), _row(4, "035420", 50)]

    # When
    snapshot = _build(rows, excluded_symbols=frozenset({"035420"}))

    # Then: excluded absent, duplicate kept once at its higher value, eligible counts post-filter rows
    assert [(c["symbol"], c["trade_value_krw"]) for c in snapshot.candidates] == [("000660", 300), ("005930", 200)]
    assert snapshot.eligible_count == 2


def test_capacity_cut() -> None:
    # Given: 300 rows with distinct values
    rows = [_row(i, f"{i:06d}", 1000 - i) for i in range(1, 301)]

    # When
    snapshot = _build(rows, capacity=40)

    # Then: top 40 by value
    assert snapshot.selected_count == 40
    assert snapshot.eligible_count == 300
    assert snapshot.candidates[0]["symbol"] == "000001"
    assert snapshot.candidates[-1]["rank"] == 40


def test_empty_after_filter_fails() -> None:
    # Given: every row excluded
    rows = [_row(1, "005930", 100)]

    # Then
    with pytest.raises(pool.RegularNxtUniverseError):
        _build(rows, excluded_symbols=frozenset({"005930"}))


@pytest.mark.parametrize(
    "overrides",
    [
        {"generated_at": dt.datetime(2026, 11, 17, 7, 0, tzinfo=KST)},
        {"generated_at": dt.datetime(2026, 11, 16, 20, 10)},
        {"generated_at": dt.datetime(2026, 11, 16, 20, 10, tzinfo=dt.UTC)},
        {"effective_from": dt.datetime(2026, 11, 18, 8, 58, tzinfo=KST)},
        {"effective_from": dt.datetime(2026, 11, 17, 8, 58)},
        {"effective_from": dt.datetime(2026, 11, 16, 19, 0, tzinfo=KST)},
        {"capacity": 0},
    ],
)
def test_point_in_time_guard(overrides) -> None:
    # Given: a timestamp or capacity that violates the pool contract
    rows = [_row(1, "005930", 100)]

    # Then
    with pytest.raises(pool.RegularNxtUniverseError):
        _build(rows, **overrides)


def test_generated_on_target_date_fails() -> None:
    # Given: data stamped on the target day itself (not observable before the target)
    rows = [_row(1, "005930", 100)]

    # Then
    with pytest.raises(pool.RegularNxtUniverseError):
        _build(rows, generated_at=dt.datetime(2026, 11, 17, 7, 0, tzinfo=KST), effective_from=EFF)


def test_roundtrip_via_reader(tmp_path) -> None:
    # Given: a fetch returning unsorted vendor rows
    path = tmp_path / "universe" / "regular_nxt" / "2026-11-17.json"
    rows = [_row(1, "000660", 300), _row(2, "005930", 900)]

    # When
    written = pool.refresh_regular_nxt_pool(
        target_date=TARGET,
        generated_at=GEN,
        effective_from=EFF,
        fetch=lambda: rows,
        out_path=path,
        capacity=40,
    )
    read = read_candidate_snapshot(path, expected_session_date=TARGET, expected_session="regular_nxt", max_candidates=40)

    # Then: the file reads back with the same symbols and ranks, and the pool is ready for the target only
    assert [(c["symbol"], c["rank"]) for c in read.candidates] == [("005930", 1), ("000660", 2)]
    assert read.candidates == written.candidates
    assert pool.regular_nxt_pool_ready(path, target_date=TARGET, max_candidates=40) is True
    assert pool.regular_nxt_pool_ready(path, target_date=dt.date(2026, 11, 18), max_candidates=40) is False


def test_corrupt_file_is_not_ready(tmp_path) -> None:
    # Given: a truncated file on disk
    path = tmp_path / "2026-11-17.json"
    path.write_text(json.dumps({"schema_version": 1}), encoding="utf-8")

    # Then
    assert pool.regular_nxt_pool_ready(path, target_date=TARGET, max_candidates=40) is False


def test_fetch_failure_writes_nothing(tmp_path) -> None:
    # Given: a fetch that fails
    path = tmp_path / "2026-11-17.json"

    def _failing() -> list[KiwoomRankingRow]:
        raise OSError("vendor down")

    # Then: the exception propagates and no file is written
    with pytest.raises(OSError, match="vendor down"):
        pool.refresh_regular_nxt_pool(
            target_date=TARGET,
            generated_at=GEN,
            effective_from=EFF,
            fetch=_failing,
            out_path=path,
            capacity=40,
        )
    assert not path.exists()


def test_pool_not_ready_when_capacity_smaller(tmp_path) -> None:
    # Given: a valid day file built at capacity 40, read by a daemon now configured for 60
    small = tmp_path / "small.json"
    pool.refresh_regular_nxt_pool(
        target_date=TARGET, generated_at=GEN, effective_from=EFF,
        fetch=lambda: [_row(i, f"{i:06d}", 1000 - i) for i in range(1, 41)], out_path=small, capacity=40,
    )

    # Then: it is rebuilt at the current capacity
    assert pool.regular_nxt_pool_ready(small, target_date=TARGET, max_candidates=60) is False

    # Given: a thin ranking built at capacity 60 that selected only 40 names
    thin = tmp_path / "thin.json"
    pool.refresh_regular_nxt_pool(
        target_date=TARGET, generated_at=GEN, effective_from=EFF,
        fetch=lambda: [_row(i, f"{i:06d}", 1000 - i) for i in range(1, 41)], out_path=thin, capacity=60,
    )

    # Then: a short ranking at full capacity is still ready (no rebuild loop)
    assert pool.regular_nxt_pool_ready(thin, target_date=TARGET, max_candidates=60) is True
