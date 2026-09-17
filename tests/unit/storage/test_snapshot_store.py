import datetime as dt
import pathlib

import polars as pl
import pytest

from src.core.config import DataPaths
from src.marketdata.snapshot_contracts import SNAPSHOT_SCHEMAS, SnapshotDataset
from src.storage.snapshot_store import SnapshotStore, SnapshotStoreError

_SESSION_DATE = dt.date(2026, 9, 17)


def _ranking_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "session_date": _SESSION_DATE,
        "observed_at_ns": 1_000,
        "source_tr": "ranking_tr",
        "market_div_code": "J",
        "list_kind": "change_rate",
        "rank": 1,
        "symbol": "005930",
        "change_pct": 1.5,
        "trade_value_krw": 100,
    }
    row.update(overrides)
    return row


def _news_row(news_id: str, title: str) -> dict[str, object]:
    return {
        "session_date": _SESSION_DATE,
        "observed_at_ns": 2_000,
        "source_tr": "news_tr",
        "market_div_code": "J",
        "news_id": news_id,
        "published_at_ns": 1_500,
        "provider": "provider",
        "provider_code": "P1",
        "category_code": "C1",
        "title": title,
        "symbols": ["005930"],
    }


def _store(tmp_path: pathlib.Path) -> SnapshotStore:
    return SnapshotStore(paths=DataPaths(tmp_path), session_date=_SESSION_DATE)


def test_append_persists_atomic_partition_and_reloads_after_restart(tmp_path: pathlib.Path) -> None:
    store = _store(tmp_path)
    rows = [_ranking_row(rank=1), _ranking_row(rank=2, observed_at_ns=2_000)]

    assert store.append(SnapshotDataset.RANKING, rows) == 2

    part = tmp_path / "l1" / "snapshot" / "ranking" / "dt=2026-09-17.parquet"
    assert part.exists()
    assert not list(part.parent.glob("*.tmp"))
    reloaded = SnapshotStore(paths=DataPaths(tmp_path), session_date=_SESSION_DATE)
    frame = reloaded.frame(SnapshotDataset.RANKING)
    assert frame.height == 2
    assert frame.columns == list(SNAPSHOT_SCHEMAS[SnapshotDataset.RANKING])
    frame2 = reloaded.frame(SnapshotDataset.RANKING)
    assert frame2.equals(frame)
    assert frame2 is not frame


def test_append_deduplicates_keeping_first_committed_row(tmp_path: pathlib.Path) -> None:
    store = _store(tmp_path)

    assert store.append(SnapshotDataset.NEWS_TITLE, [_news_row("n1", "first")]) == 1
    assert store.append(SnapshotDataset.NEWS_TITLE, [_news_row("n1", "second")]) == 0

    frame = store.frame(SnapshotDataset.NEWS_TITLE)
    assert frame.height == 1
    assert frame["title"].to_list() == ["first"]


def test_append_rejects_missing_or_extra_columns(tmp_path: pathlib.Path) -> None:
    store = _store(tmp_path)
    missing = _ranking_row()
    del missing["rank"]
    extra = _ranking_row(rank=9)
    extra["bogus"] = 1

    with pytest.raises(SnapshotStoreError):
        store.append(SnapshotDataset.RANKING, [missing])
    with pytest.raises(SnapshotStoreError):
        store.append(SnapshotDataset.RANKING, [extra])

    assert not (tmp_path / "l1" / "snapshot" / "ranking" / "dt=2026-09-17.parquet").exists()


def test_append_rejects_bad_cast_and_foreign_session_date(tmp_path: pathlib.Path) -> None:
    store = _store(tmp_path)

    with pytest.raises(SnapshotStoreError):
        store.append(SnapshotDataset.RANKING, [_ranking_row(rank="not-an-int")])
    with pytest.raises(SnapshotStoreError):
        store.append(
            SnapshotDataset.RANKING, [_ranking_row(session_date=dt.date(2026, 9, 16))]
        )


def test_append_empty_batch_writes_nothing(tmp_path: pathlib.Path) -> None:
    store = _store(tmp_path)

    assert store.append(SnapshotDataset.RANKING, []) == 0

    assert not (tmp_path / "l1" / "snapshot" / "ranking" / "dt=2026-09-17.parquet").exists()
    frame = store.frame(SnapshotDataset.RANKING)
    assert frame.height == 0
    assert frame.columns == list(SNAPSHOT_SCHEMAS[SnapshotDataset.RANKING])


def test_corrupt_existing_partition_fails_closed_without_overwrite(tmp_path: pathlib.Path) -> None:
    part = tmp_path / "l1" / "snapshot" / "ranking" / "dt=2026-09-17.parquet"
    part.parent.mkdir(parents=True, exist_ok=True)
    part.write_bytes(b"\x00\x01broken-parquet")
    before = part.read_bytes()

    with pytest.raises(SnapshotStoreError):
        _store(tmp_path).append(SnapshotDataset.RANKING, [_ranking_row()])

    assert part.read_bytes() == before


def test_mismatched_existing_schema_fails_closed(tmp_path: pathlib.Path) -> None:
    part = tmp_path / "l1" / "snapshot" / "ranking" / "dt=2026-09-17.parquet"
    part.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"only_one": [1]}).write_parquet(part, compression="zstd")

    with pytest.raises(SnapshotStoreError):
        _store(tmp_path).append(SnapshotDataset.RANKING, [_ranking_row()])


def test_uncastable_existing_partition_fails_closed(tmp_path: pathlib.Path) -> None:
    part = tmp_path / "l1" / "snapshot" / "ranking" / "dt=2026-09-17.parquet"
    part.parent.mkdir(parents=True, exist_ok=True)
    poisoned = _ranking_row(rank="not-an-int")
    pl.DataFrame([poisoned]).write_parquet(part, compression="zstd")

    with pytest.raises(SnapshotStoreError):
        _store(tmp_path).append(SnapshotDataset.RANKING, [_ranking_row(rank=2, observed_at_ns=2_000)])
