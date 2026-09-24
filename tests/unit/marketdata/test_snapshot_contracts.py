import datetime as dt
from zoneinfo import ZoneInfo

import pytest

from src.core.config import SnapshotSettings
from src.marketdata.snapshot_contracts import (
    COMMON_SNAPSHOT_COLUMNS,
    SNAPSHOT_DEDUP_KEYS,
    SNAPSHOT_SCHEMAS,
    SnapshotDataset,
    SnapshotJobKind,
    build_session_jobs,
    partition_due_jobs,
)

_KST = ZoneInfo("Asia/Seoul")
_SESSION_DATE = dt.date(2026, 9, 17)


def _settings() -> SnapshotSettings:
    return SnapshotSettings()


def test_schemas_share_common_columns_and_cover_dedup_keys() -> None:
    common = list(COMMON_SNAPSHOT_COLUMNS)
    for dataset in SnapshotDataset:
        schema = SNAPSHOT_SCHEMAS[dataset]
        assert list(schema)[:4] == common
        for key in SNAPSHOT_DEDUP_KEYS[dataset]:
            assert key in schema


def test_build_session_jobs_expands_counts_per_kind() -> None:
    from collections import Counter

    jobs = build_session_jobs(_settings(), _SESSION_DATE)

    counts = Counter(j.kind for j in jobs)
    assert counts[SnapshotJobKind.RANKING] == 390
    assert counts[SnapshotJobKind.INDEX_SNAPSHOT] == 78
    assert counts[SnapshotJobKind.INDEX_MINUTE_BAR] == 5
    assert counts[SnapshotJobKind.AUCTION_OPEN] == 5
    assert counts[SnapshotJobKind.AUCTION_CLOSE] == 4
    assert counts[SnapshotJobKind.INVESTOR_ESTIMATE] == 6
    assert counts[SnapshotJobKind.PROGRAM_TRADE] == 14
    assert counts[SnapshotJobKind.EOD_MINUTE_BARS] == 1
    assert counts[SnapshotJobKind.NEWS_TITLE] == 918


def test_build_session_jobs_not_after_is_next_same_kind_or_deadline() -> None:
    settings = _settings()
    jobs = build_session_jobs(settings, _SESSION_DATE)
    by_id = {j.job_id: j for j in jobs}

    assert by_id["auction_open@085930"].not_after == dt.datetime(2026, 9, 17, 9, 0, tzinfo=_KST)
    assert by_id["ranking@090000"].not_after == dt.datetime(2026, 9, 17, 9, 1, tzinfo=_KST)
    news_jobs = [j for j in jobs if j.kind is SnapshotJobKind.NEWS_TITLE]
    assert news_jobs[-1].not_after == dt.datetime(2026, 9, 17, 15, 39, tzinfo=_KST)
    for job in jobs:
        assert job.due_at < job.not_after
        assert job.due_at.tzinfo is not None


def test_build_session_jobs_orders_same_instant_by_kind_priority() -> None:
    jobs = build_session_jobs(_settings(), _SESSION_DATE)

    at_eod = [j for j in jobs if j.due_at == dt.datetime(2026, 9, 17, 15, 35, tzinfo=_KST)]
    kinds = [j.kind for j in at_eod]

    assert kinds.index(SnapshotJobKind.INVESTOR_ESTIMATE) < kinds.index(SnapshotJobKind.PROGRAM_TRADE)
    assert kinds.index(SnapshotJobKind.PROGRAM_TRADE) < kinds.index(SnapshotJobKind.EOD_MINUTE_BARS)
    assert kinds == sorted(kinds, key=lambda k: list(SnapshotJobKind).index(k))
    assert jobs == build_session_jobs(_settings(), _SESSION_DATE)


def test_build_session_jobs_rejects_collapsed_or_duplicate_windows() -> None:
    settings = _settings()

    with pytest.raises(ValueError, match="due_at < not_after"):
        build_session_jobs(
            settings.model_copy(update={"investor_estimate_times": (dt.time(9, 35), settings.eod_collect_time)}),
            _SESSION_DATE,
        )
    with pytest.raises(ValueError, match="duplicate job_id"):
        build_session_jobs(
            settings.model_copy(
                update={
                    "investor_estimate_times": (
                        dt.time(9, 35, 0, 1),
                        dt.time(9, 35, 0, 2),
                        dt.time(10, 5),
                        dt.time(11, 25),
                        dt.time(13, 25),
                        dt.time(14, 35),
                    )
                }
            ),
            _SESSION_DATE,
        )


def test_partition_due_jobs_separates_expired_from_due() -> None:
    jobs = build_session_jobs(_settings(), _SESSION_DATE)
    now = dt.datetime(2026, 9, 17, 10, 0, 10, tzinfo=_KST)

    due, expired = partition_due_jobs(jobs, completed=set(), now=now)

    due_ids = {j.job_id for j in due}
    assert {"ranking@100000", "index_snapshot@100000", "news_title@100000"} <= due_ids
    expired_ids = {j.job_id for j in expired}
    assert "ranking@095900" in expired_ids
    future_ids = {j.job_id for j in jobs if j.due_at > now}
    assert not (set(due_ids) & future_ids)
    assert not (set(expired_ids) & future_ids)
    assert [j.job_id for j in due] == [j.job_id for j in jobs if j.job_id in due_ids]
    completed_due, _ = partition_due_jobs(jobs, completed={"ranking@100000"}, now=now)
    assert "ranking@100000" not in {j.job_id for j in completed_due}


def test_partition_due_jobs_rejects_naive_now() -> None:
    jobs = build_session_jobs(_settings(), _SESSION_DATE)

    with pytest.raises(ValueError, match="tz-aware"):
        partition_due_jobs(jobs, completed=set(), now=dt.datetime(2026, 9, 17, 10, 0, 10))


def _aware_at(hour: int, minute: int) -> dt.datetime:
    return dt.datetime(2026, 9, 17, hour, minute, tzinfo=_KST)


def test_coverage_series_always_ends_exactly_at_boundary() -> None:
    from src.marketdata.snapshot_contracts import _coverage_series

    result = _coverage_series(_aware_at(9, 0), 4800, _aware_at(15, 30))

    assert result == [
        _aware_at(10, 20),
        _aware_at(11, 40),
        _aware_at(13, 0),
        _aware_at(14, 20),
        _aware_at(15, 30),
    ]
    assert result[-1] == _aware_at(15, 30)


def test_coverage_series_does_not_duplicate_when_interval_divides_evenly() -> None:
    from src.marketdata.snapshot_contracts import _coverage_series

    result = _coverage_series(_aware_at(9, 0), 23400, _aware_at(15, 30))

    assert result == [_aware_at(15, 30)]


def test_index_minute_bar_not_after_defaults_to_run_end_deadline() -> None:
    settings = _settings()
    jobs = build_session_jobs(settings, _SESSION_DATE)
    by_id = {j.job_id: j for j in jobs}

    assert by_id["index_minute_bar@153000"].not_after == dt.datetime(2026, 9, 17, 15, 39, tzinfo=_KST)
    assert by_id["index_minute_bar@102000"].not_after == by_id["index_minute_bar@114000"].due_at


def _common(**overrides):
    row = {
        "session_date": _SESSION_DATE,
        "observed_at_ns": 1_000,
        "source_tr": "tr",
        "market_div_code": "J",
    }
    row.update(overrides)
    return row


def _frame(dataset, rows) -> object:
    import polars as pl

    from src.marketdata.snapshot_contracts import SNAPSHOT_SCHEMAS

    return pl.DataFrame(rows, schema=SNAPSHOT_SCHEMAS[dataset])


def test_snapshot_row_violations_accepts_clean_security_status_row() -> None:
    from src.marketdata.snapshot_contracts import SnapshotDataset, snapshot_row_violations

    row = _common(
        symbol="005930", status_code="00", managed=False, market_warning_code="00",
        short_overheated=False, investment_caution=False, liquidation_trading=False,
        trading_halted=False, vi_code="00", ovtm_vi_cls_code="00", credit_available=True,
        last_price=70000, base_price=69300, upper_limit=80000, lower_limit=60000,
    )
    mask = snapshot_row_violations(SnapshotDataset.SECURITY_STATUS, _frame(SnapshotDataset.SECURITY_STATUS, [row]))
    assert mask.to_list() == [False]


def test_snapshot_row_violations_flags_price_outside_limit_band() -> None:
    from src.marketdata.snapshot_contracts import SnapshotDataset, snapshot_row_violations

    row = _common(
        symbol="005930", status_code="00", managed=False, market_warning_code="00",
        short_overheated=False, investment_caution=False, liquidation_trading=False,
        trading_halted=False, vi_code="00", ovtm_vi_cls_code="00", credit_available=True,
        last_price=90000, base_price=69300, upper_limit=80000, lower_limit=60000,
    )
    mask = snapshot_row_violations(SnapshotDataset.SECURITY_STATUS, _frame(SnapshotDataset.SECURITY_STATUS, [row]))
    assert mask.to_list() == [True]


def test_snapshot_row_violations_flags_program_trade_arithmetic() -> None:
    from src.marketdata.snapshot_contracts import SnapshotDataset, snapshot_row_violations

    row = _common(
        symbol="005930", trade_time="090000", cum_volume=1000, sell_qty=100, buy_qty=150,
        net_qty=999, sell_value_krw=1000, buy_value_krw=1500, net_value_krw=500,
    )
    mask = snapshot_row_violations(SnapshotDataset.PROGRAM_TRADE, _frame(SnapshotDataset.PROGRAM_TRADE, [row]))
    assert mask.to_list() == [True]


def test_snapshot_row_violations_flags_inverted_minute_bar() -> None:
    from src.marketdata.snapshot_contracts import SnapshotDataset, snapshot_row_violations

    row = _common(symbol="005930", bar_time="090100", open=70000, high=70000, low=69500, close=70200, volume=100)
    mask = snapshot_row_violations(SnapshotDataset.STOCK_MINUTE_BAR, _frame(SnapshotDataset.STOCK_MINUTE_BAR, [row]))
    assert mask.to_list() == [True]


def test_snapshot_row_violations_ignores_null_ranking_trade_value() -> None:
    from src.marketdata.snapshot_contracts import SnapshotDataset, snapshot_row_violations

    row = _common(list_kind="fluctuation", rank=1, symbol="005930", change_pct=1.5, trade_value_krw=None)
    mask = snapshot_row_violations(SnapshotDataset.RANKING, _frame(SnapshotDataset.RANKING, [row]))
    assert mask.to_list() == [False]


def test_snapshot_row_violations_never_flags_signed_investor_flow() -> None:
    from src.marketdata.snapshot_contracts import SnapshotDataset, snapshot_row_violations

    rows = [
        _common(symbol="005930", bucket=1, foreign_net_qty=-100, institution_net_qty=-200, total_net_qty=-300),
        _common(symbol="000660", bucket=2, foreign_net_qty=-5, institution_net_qty=10, total_net_qty=5),
    ]
    mask = snapshot_row_violations(SnapshotDataset.INVESTOR_ESTIMATE, _frame(SnapshotDataset.INVESTOR_ESTIMATE, rows))
    assert mask.to_list() == [False, False]


def test_snapshot_row_violations_returns_empty_mask_for_empty_frame() -> None:
    import polars as pl

    from src.marketdata.snapshot_contracts import SNAPSHOT_SCHEMAS, SnapshotDataset, snapshot_row_violations

    frame = pl.DataFrame(schema=SNAPSHOT_SCHEMAS[SnapshotDataset.RANKING], strict=True)
    mask = snapshot_row_violations(SnapshotDataset.RANKING, frame)
    assert len(mask) == 0


def _security_status_frame(columns: list[str], values: list[object]):
    import polars as pl

    return pl.DataFrame([values], schema=columns, orient="row")


def test_normalize_legacy_snapshot_columns_renames_legacy_column() -> None:
    from src.marketdata.snapshot_contracts import (
        SNAPSHOT_SCHEMAS,
        SnapshotDataset,
        normalize_legacy_snapshot_columns,
    )

    current = list(SNAPSHOT_SCHEMAS[SnapshotDataset.SECURITY_STATUS])
    legacy = [c if c != "ovtm_vi_cls_code" else "overtime_vi_code" for c in current]
    idx = legacy.index("overtime_vi_code")
    values: list[object] = [f"v{i}" for i in range(len(legacy))]
    values[idx] = "01"
    frame = _security_status_frame(legacy, values)

    out = normalize_legacy_snapshot_columns(SnapshotDataset.SECURITY_STATUS, frame)

    assert out.columns == current
    assert out["ovtm_vi_cls_code"].to_list() == ["01"]
    assert "overtime_vi_code" not in out.columns


def test_normalize_legacy_snapshot_columns_leaves_current_frame_untouched() -> None:
    from src.marketdata.snapshot_contracts import (
        SNAPSHOT_SCHEMAS,
        SnapshotDataset,
        normalize_legacy_snapshot_columns,
    )

    columns = list(SNAPSHOT_SCHEMAS[SnapshotDataset.SECURITY_STATUS])
    frame = _security_status_frame(columns, [f"v{i}" for i in range(len(columns))])

    out = normalize_legacy_snapshot_columns(SnapshotDataset.SECURITY_STATUS, frame)

    assert out.equals(frame)


def test_normalize_legacy_snapshot_columns_rejects_coexisting_names() -> None:
    import pytest

    from src.marketdata.snapshot_contracts import SnapshotDataset, normalize_legacy_snapshot_columns

    frame = _security_status_frame(["overtime_vi_code", "ovtm_vi_cls_code"], ["a", "b"])

    with pytest.raises(ValueError, match="coexist"):
        normalize_legacy_snapshot_columns(SnapshotDataset.SECURITY_STATUS, frame)


def test_normalize_legacy_snapshot_columns_passes_through_dataset_without_aliases() -> None:
    from src.marketdata.snapshot_contracts import SnapshotDataset, normalize_legacy_snapshot_columns

    frame = _security_status_frame(["list_kind", "rank"], ["fluctuation", 1])

    out = normalize_legacy_snapshot_columns(SnapshotDataset.RANKING, frame)

    assert out.equals(frame)
