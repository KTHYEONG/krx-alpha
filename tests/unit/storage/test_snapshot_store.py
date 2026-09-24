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


def _program_trade_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "session_date": _SESSION_DATE,
        "observed_at_ns": 1_000,
        "source_tr": "program_tr",
        "market_div_code": "J",
        "symbol": "005930",
        "trade_time": "090000",
        "cum_volume": 1000,
        "sell_qty": 100,
        "buy_qty": 150,
        "net_qty": 50,
        "sell_value_krw": 1000,
        "buy_value_krw": 1500,
        "net_value_krw": 500,
    }
    row.update(overrides)
    return row


def test_append_drops_invalid_rows_and_persists_valid_ones(tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture) -> None:
    import logging

    store = _store(tmp_path)
    broken = _program_trade_row(observed_at_ns=2_000, net_qty=999)
    with caplog.at_level(logging.WARNING):
        added = store.append(SnapshotDataset.PROGRAM_TRADE, [_program_trade_row(), broken])

    assert added == 1
    frame = store.frame(SnapshotDataset.PROGRAM_TRADE)
    assert frame.height == 1
    assert frame["observed_at_ns"].to_list() == [1_000]
    assert "stage=snapshot_dq" in caplog.text


def test_append_rejected_key_does_not_block_later_valid_observation(tmp_path: pathlib.Path) -> None:
    store = _store(tmp_path)
    broken = _program_trade_row(net_qty=999)

    assert store.append(SnapshotDataset.PROGRAM_TRADE, [broken]) == 0
    assert store.append(SnapshotDataset.PROGRAM_TRADE, [_program_trade_row()]) == 1
    assert store.frame(SnapshotDataset.PROGRAM_TRADE).height == 1


def test_append_fully_rejected_batch_writes_nothing_and_alerts(tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture) -> None:
    import logging

    store = _store(tmp_path)
    broken = _program_trade_row(net_qty=999)
    with caplog.at_level(logging.CRITICAL):
        added = store.append(SnapshotDataset.PROGRAM_TRADE, [broken])

    assert added == 0
    assert not (tmp_path / "l1" / "snapshot" / "program_trade" / "dt=2026-09-17.parquet").exists()
    assert any(r.levelno == logging.CRITICAL and "status=FAIL" in r.getMessage() for r in caplog.records)


def _security_status_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "session_date": _SESSION_DATE,
        "observed_at_ns": 1_000,
        "source_tr": "FHKST01010100",
        "market_div_code": "J",
        "symbol": "005930",
        "status_code": "00",
        "managed": False,
        "market_warning_code": "00",
        "short_overheated": False,
        "investment_caution": False,
        "liquidation_trading": False,
        "trading_halted": False,
        "vi_code": "00",
        "ovtm_vi_cls_code": "01",
        "credit_available": True,
        "last_price": 70000,
        "base_price": 69500,
        "upper_limit": 90350,
        "lower_limit": 48650,
    }
    row.update(overrides)
    return row


def test_legacy_partition_loads_and_appends_with_current_column(tmp_path: pathlib.Path) -> None:
    part = tmp_path / "l1" / "snapshot" / "security_status" / "dt=2026-09-17.parquet"
    part.parent.mkdir(parents=True, exist_ok=True)
    legacy = _security_status_row()
    legacy["overtime_vi_code"] = legacy.pop("ovtm_vi_cls_code")
    pl.DataFrame([legacy]).write_parquet(part, compression="zstd")

    store = _store(tmp_path)
    assert store.append(SnapshotDataset.SECURITY_STATUS, [_security_status_row(observed_at_ns=2_000)]) == 1

    frame = store.frame(SnapshotDataset.SECURITY_STATUS)
    assert frame.columns == list(SNAPSHOT_SCHEMAS[SnapshotDataset.SECURITY_STATUS])
    assert frame.sort("observed_at_ns")["ovtm_vi_cls_code"].to_list() == ["01", "01"]
    assert pl.read_parquet(part).columns == list(SNAPSHOT_SCHEMAS[SnapshotDataset.SECURITY_STATUS])


def test_conflicting_legacy_and_current_columns_fail_closed(tmp_path: pathlib.Path) -> None:
    import pytest

    part = tmp_path / "l1" / "snapshot" / "security_status" / "dt=2026-09-17.parquet"
    part.parent.mkdir(parents=True, exist_ok=True)
    row = _security_status_row()
    row["overtime_vi_code"] = "01"
    pl.DataFrame([row]).write_parquet(part, compression="zstd")

    with pytest.raises(SnapshotStoreError):
        _store(tmp_path).append(SnapshotDataset.SECURITY_STATUS, [_security_status_row(observed_at_ns=2_000)])
