"""Month-partitioned store invariant guards."""

from __future__ import annotations

import datetime as dt

import polars as pl

_KEYS = ("date", "symbol")
_SORT = ("symbol", "date")


def _frame(rows: list[tuple[dt.date, str, float]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": [row[0] for row in rows],
            "symbol": [row[1] for row in rows],
            "close": [row[2] for row in rows],
        },
        schema={"date": pl.Date, "symbol": pl.String, "close": pl.Float64},
    )


def test_upsert_writes_one_file_per_month(tmp_path) -> None:
    from src.marketdata.partitioned_store import upsert_month_partitions

    root = tmp_path / "store"
    added = upsert_month_partitions(
        root,
        _frame([(dt.date(2026, 8, 31), "005930", 1.0), (dt.date(2026, 9, 1), "005930", 2.0)]),
        key_columns=_KEYS,
        sort_columns=_SORT,
    )

    assert added == 2
    aug = pl.read_parquet(root / "2026-08.parquet")
    sep = pl.read_parquet(root / "2026-09.parquet")
    assert aug["date"].to_list() == [dt.date(2026, 8, 31)]
    assert sep["date"].to_list() == [dt.date(2026, 9, 1)]


def test_upsert_touches_only_affected_months(tmp_path) -> None:
    from src.marketdata.partitioned_store import upsert_month_partitions

    root = tmp_path / "store"
    upsert_month_partitions(
        root,
        _frame([(dt.date(2026, 8, 31), "005930", 1.0), (dt.date(2026, 9, 1), "005930", 2.0)]),
        key_columns=_KEYS,
        sort_columns=_SORT,
    )
    aug_path = root / "2026-08.parquet"
    before_mtime = aug_path.stat().st_mtime_ns
    before_bytes = aug_path.read_bytes()

    added = upsert_month_partitions(
        root,
        _frame([(dt.date(2026, 9, 2), "005930", 3.0)]),
        key_columns=_KEYS,
        sort_columns=_SORT,
    )

    assert added == 1
    assert aug_path.stat().st_mtime_ns == before_mtime
    assert aug_path.read_bytes() == before_bytes
    assert pl.read_parquet(root / "2026-09.parquet").height == 2


def test_upsert_incoming_row_wins_on_key_conflict(tmp_path) -> None:
    from src.marketdata.partitioned_store import upsert_month_partitions

    root = tmp_path / "store"
    day = dt.date(2026, 9, 1)
    assert upsert_month_partitions(root, _frame([(day, "005930", 1.0)]), key_columns=_KEYS, sort_columns=_SORT) == 1

    added = upsert_month_partitions(root, _frame([(day, "005930", 2.0)]), key_columns=_KEYS, sort_columns=_SORT)

    assert added == 0
    saved = pl.read_parquet(root / "2026-09.parquet")
    assert saved.height == 1
    assert saved["close"].to_list() == [2.0]


def test_upsert_is_idempotent(tmp_path) -> None:
    from src.marketdata.partitioned_store import upsert_month_partitions

    root = tmp_path / "store"
    frame = _frame([
        (dt.date(2026, 8, 31), "005930", 1.0),
        (dt.date(2026, 9, 1), "005930", 2.0),
        (dt.date(2026, 9, 1), "000660", 3.0),
    ])
    assert upsert_month_partitions(root, frame, key_columns=_KEYS, sort_columns=_SORT) == 3
    before = {path.name: path.read_bytes() for path in sorted(root.glob("*.parquet"))}

    assert upsert_month_partitions(root, frame, key_columns=_KEYS, sort_columns=_SORT) == 0

    after = {path.name: path.read_bytes() for path in sorted(root.glob("*.parquet"))}
    assert after == before
    saved = pl.read_parquet(root / "2026-09.parquet")
    assert saved["symbol"].to_list() == ["000660", "005930"]
    assert saved["date"].to_list() == [dt.date(2026, 9, 1)] * 2


def test_scan_never_opens_out_of_range_months(tmp_path) -> None:
    from src.marketdata.partitioned_store import scan_month_partitions, upsert_month_partitions

    root = tmp_path / "store"
    upsert_month_partitions(
        root,
        _frame([
            (dt.date(2026, 8, 15), "005930", 1.0),
            (dt.date(2026, 9, 15), "005930", 2.0),
            (dt.date(2026, 10, 15), "005930", 3.0),
        ]),
        key_columns=_KEYS,
        sort_columns=_SORT,
    )
    (root / "2026-07.parquet").write_bytes(b"not-a-parquet")
    (root / "notes.txt").write_text("stray file", encoding="utf-8")

    out = scan_month_partitions(root, min_date=dt.date(2026, 8, 1), max_date=dt.date(2026, 9, 30)).collect()

    assert sorted(out["date"].to_list()) == [dt.date(2026, 8, 15), dt.date(2026, 9, 15)]


def test_scan_empty_store_is_missing(tmp_path) -> None:
    import pytest

    from src.marketdata.partitioned_store import PartitionedStoreError, scan_month_partitions

    root = tmp_path / "store"
    root.mkdir(parents=True)

    with pytest.raises(PartitionedStoreError):
        scan_month_partitions(root)


def test_migrate_single_file_store_splits_and_preserves_rows(tmp_path, caplog) -> None:
    import logging

    from src.marketdata.partitioned_store import migrate_single_file_store

    legacy = tmp_path / "daily.parquet"
    rows = [
        *[(dt.date(2026, 7, 31), "005930", 1.0)],
        *[(dt.date(2026, 8, 15), "005930", 2.0), (dt.date(2026, 8, 16), "000660", 3.0)],
        *[(dt.date(2026, 9, 1), "005930", 4.0)],
    ]
    _frame(rows).write_parquet(legacy)
    root = tmp_path / "daily"

    with caplog.at_level(logging.INFO):
        result = migrate_single_file_store(legacy, root, key_columns=_KEYS, sort_columns=_SORT)

    assert result.migrated is True
    assert result.rows == 4
    assert result.partitions == 3
    assert sorted(path.name for path in root.glob("*.parquet")) == [
        "2026-07.parquet",
        "2026-08.parquet",
        "2026-09.parquet",
    ]
    total = sum(pl.read_parquet(path).height for path in root.glob("*.parquet"))
    assert total == 4
    migrated_keys = {
        tuple(row)
        for path in root.glob("*.parquet")
        for row in pl.read_parquet(path).select(["date", "symbol"]).iter_rows()
    }
    assert migrated_keys == {(day, symbol) for day, symbol, _ in rows}
    assert not legacy.exists()
    assert (tmp_path / "daily.parquet.migrated").exists()
    assert "stage=store_migration status=OK rows=4 partitions=3" in caplog.text


def test_migrate_single_file_store_refuses_ambiguous_state(tmp_path) -> None:
    import pytest

    from src.marketdata.partitioned_store import PartitionedStoreError, migrate_single_file_store

    legacy = tmp_path / "daily.parquet"
    _frame([(dt.date(2026, 9, 1), "005930", 1.0)]).write_parquet(legacy)
    root = tmp_path / "daily"
    root.mkdir(parents=True)
    part = root / "2026-09.parquet"
    part.write_bytes(b"existing-partition")
    before = part.read_bytes()

    with pytest.raises(PartitionedStoreError):
        migrate_single_file_store(legacy, root, key_columns=_KEYS, sort_columns=_SORT)

    assert part.read_bytes() == before
    assert legacy.exists()


def test_migrate_single_file_store_noop_without_legacy(tmp_path) -> None:
    from src.marketdata.partitioned_store import migrate_single_file_store

    result = migrate_single_file_store(tmp_path / "daily.parquet", tmp_path / "daily", key_columns=_KEYS, sort_columns=_SORT)

    assert result.migrated is False
    assert result.rows == 0
    assert result.partitions == 0
    assert not (tmp_path / "daily").exists()


def test_month_partition_path_names_file_by_month(tmp_path) -> None:
    import datetime as dt

    from src.marketdata.partitioned_store import month_partition_path

    assert month_partition_path(tmp_path, dt.date(2026, 9, 1)) == tmp_path / "2026-09.parquet"


def test_upsert_rejects_frame_without_date_column(tmp_path) -> None:
    import polars as pl
    import pytest

    from src.marketdata.partitioned_store import PartitionedStoreError, upsert_month_partitions

    with pytest.raises(PartitionedStoreError, match="date column"):
        upsert_month_partitions(
            tmp_path / "store",
            pl.DataFrame({"symbol": ["005930"]}),
            key_columns=_KEYS,
            sort_columns=_SORT,
        )


def test_upsert_rejects_unreadable_partition(tmp_path) -> None:
    import pytest

    from src.marketdata.partitioned_store import PartitionedStoreError, upsert_month_partitions

    root = tmp_path / "store"
    root.mkdir(parents=True)
    (root / "2026-09.parquet").write_bytes(b"not-a-parquet")

    with pytest.raises(PartitionedStoreError, match="unreadable"):
        upsert_month_partitions(
            root,
            _frame([(dt.date(2026, 9, 1), "005930", 1.0)]),
            key_columns=_KEYS,
            sort_columns=_SORT,
        )
    assert (root / "2026-09.parquet").read_bytes() == b"not-a-parquet"


def test_upsert_rejects_schema_mismatch(tmp_path) -> None:
    import polars as pl
    import pytest

    from src.marketdata.partitioned_store import PartitionedStoreError, upsert_month_partitions

    root = tmp_path / "store"
    root.mkdir(parents=True)
    pl.DataFrame({"date": [dt.date(2026, 9, 1)], "symbol": ["005930"]}).write_parquet(root / "2026-09.parquet")

    with pytest.raises(PartitionedStoreError, match="schema"):
        upsert_month_partitions(
            root,
            _frame([(dt.date(2026, 9, 1), "005930", 1.0)]),
            key_columns=_KEYS,
            sort_columns=_SORT,
        )


def test_scan_rejects_range_without_partitions(tmp_path) -> None:
    import pytest

    from src.marketdata.partitioned_store import PartitionedStoreError, scan_month_partitions, upsert_month_partitions

    root = tmp_path / "store"
    upsert_month_partitions(
        root,
        _frame([(dt.date(2026, 9, 1), "005930", 1.0)]),
        key_columns=_KEYS,
        sort_columns=_SORT,
    )

    with pytest.raises(PartitionedStoreError, match="range"):
        scan_month_partitions(root, min_date=dt.date(2026, 1, 1), max_date=dt.date(2026, 1, 31))


def test_migrate_rejects_unreadable_legacy(tmp_path) -> None:
    import pytest

    from src.marketdata.partitioned_store import PartitionedStoreError, migrate_single_file_store

    legacy = tmp_path / "daily.parquet"
    legacy.write_bytes(b"not-a-parquet")

    with pytest.raises(PartitionedStoreError, match="unreadable"):
        migrate_single_file_store(legacy, tmp_path / "daily", key_columns=_KEYS, sort_columns=_SORT)
    assert legacy.exists()
    assert not (tmp_path / "daily").exists()


def test_migrate_rejects_legacy_without_date_column(tmp_path) -> None:
    import polars as pl
    import pytest

    from src.marketdata.partitioned_store import PartitionedStoreError, migrate_single_file_store

    legacy = tmp_path / "daily.parquet"
    pl.DataFrame({"symbol": ["005930"]}).write_parquet(legacy)

    with pytest.raises(PartitionedStoreError, match="date column"):
        migrate_single_file_store(legacy, tmp_path / "daily", key_columns=_KEYS, sort_columns=_SORT)
    assert legacy.exists()


def test_migrate_cleans_stale_tmp_from_crashed_run(tmp_path) -> None:
    from src.marketdata.partitioned_store import migrate_single_file_store

    legacy = tmp_path / "daily.parquet"
    _frame([(dt.date(2026, 9, 1), "005930", 1.0)]).write_parquet(legacy)
    stale = tmp_path / ".daily.migrate-tmp"
    stale.mkdir(parents=True)
    (stale / "junk.parquet").write_bytes(b"stale")

    result = migrate_single_file_store(legacy, tmp_path / "daily", key_columns=_KEYS, sort_columns=_SORT)

    assert result.migrated is True
    assert sorted(path.name for path in (tmp_path / "daily").glob("*.parquet")) == ["2026-09.parquet"]
    assert not stale.exists()


def test_migrate_fails_closed_on_row_count_mismatch(tmp_path, monkeypatch) -> None:
    import polars as pl
    import pytest

    from src.marketdata.partitioned_store import PartitionedStoreError, migrate_single_file_store

    legacy = tmp_path / "daily.parquet"
    _frame([(dt.date(2026, 9, 1), "005930", 1.0), (dt.date(2026, 9, 2), "005930", 2.0)]).write_parquet(legacy)
    real_concat = pl.concat
    monkeypatch.setattr(pl, "concat", lambda frames: real_concat(frames).head(1))

    with pytest.raises(PartitionedStoreError, match="row count"):
        migrate_single_file_store(legacy, tmp_path / "daily", key_columns=_KEYS, sort_columns=_SORT)
    assert legacy.exists()
    assert not (tmp_path / "daily").exists()


def test_migrate_fails_closed_on_key_set_mismatch(tmp_path, monkeypatch) -> None:
    import polars as pl
    import pytest

    from src.marketdata.partitioned_store import PartitionedStoreError, migrate_single_file_store

    legacy = tmp_path / "daily.parquet"
    _frame([(dt.date(2026, 9, 1), "005930", 1.0)]).write_parquet(legacy)
    real_concat = pl.concat
    monkeypatch.setattr(
        pl, "concat", lambda frames: real_concat(frames).with_columns(pl.lit("000660").alias("symbol"))
    )

    with pytest.raises(PartitionedStoreError, match="key set"):
        migrate_single_file_store(legacy, tmp_path / "daily", key_columns=_KEYS, sort_columns=_SORT)
    assert legacy.exists()
    assert not (tmp_path / "daily").exists()


def test_migrate_wraps_filesystem_error_as_store_error(tmp_path, monkeypatch) -> None:
    import os

    import pytest

    from src.marketdata import partitioned_store
    from src.marketdata.partitioned_store import PartitionedStoreError, migrate_single_file_store

    legacy = tmp_path / "daily.parquet"
    _frame([(dt.date(2026, 9, 1), "005930", 1.0)]).write_parquet(legacy)

    def _enospc(src, dst):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(partitioned_store.os, "rename", _enospc)

    # 디스크 부족도 데몬을 죽이는 OSError 가 아니라 fail-closed 경로의 PartitionedStoreError 로 나온다.
    with pytest.raises(PartitionedStoreError, match="filesystem error"):
        migrate_single_file_store(legacy, tmp_path / "daily", key_columns=_KEYS, sort_columns=_SORT)
    assert legacy.exists()
    assert os.path.exists(legacy)


def test_migrate_clears_leftover_upsert_tmp_in_empty_store(tmp_path) -> None:
    from src.marketdata.partitioned_store import migrate_single_file_store

    legacy = tmp_path / "daily.parquet"
    _frame([(dt.date(2026, 9, 1), "005930", 1.0)]).write_parquet(legacy)
    store = tmp_path / "daily"
    store.mkdir()
    (store / ".2026-09.parquet.tmp").write_bytes(b"partial")

    result = migrate_single_file_store(legacy, store, key_columns=_KEYS, sort_columns=_SORT)

    assert result.migrated is True
    assert sorted(path.name for path in store.iterdir()) == ["2026-09.parquet"]


def test_migrate_refuses_unexpected_content_in_store(tmp_path) -> None:
    import pytest

    from src.marketdata.partitioned_store import PartitionedStoreError, migrate_single_file_store

    legacy = tmp_path / "daily.parquet"
    _frame([(dt.date(2026, 9, 1), "005930", 1.0)]).write_parquet(legacy)
    store = tmp_path / "daily"
    (store / "notes").mkdir(parents=True)

    with pytest.raises(PartitionedStoreError, match="unexpected content"):
        migrate_single_file_store(legacy, store, key_columns=_KEYS, sort_columns=_SORT)
    assert legacy.exists()
    assert (store / "notes").exists()
