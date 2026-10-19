"""Nightly sync CLI exit-code mapping (staleness tolerance vs fatal errors)."""

from __future__ import annotations

import datetime as dt


def _seed(store, entries: dict[str, list[dt.date]]) -> None:
    from src.marketdata.toss_program_trades import append_program_trades

    rows = [
        {
            "symbol": symbol,
            "date": day,
            "arbitrage_buy_volume": 10,
            "arbitrage_sell_volume": 4,
            "arbitrage_net_volume": 6,
            "non_arbitrage_buy_volume": 20,
            "non_arbitrage_sell_volume": 5,
            "non_arbitrage_net_volume": 15,
        }
        for symbol, days in entries.items()
        for day in days
    ]
    append_program_trades(store, rows)


def _sync_env(monkeypatch, tmp_path) -> object:
    import pathlib

    from src.core.config import CollectorSettings

    monkeypatch.setenv("TOSS_APP_KEY", "k")
    monkeypatch.setenv("TOSS_APP_SECRET", "s")
    monkeypatch.setenv("KRX_ALPHA_DATA_ROOT", str(tmp_path / "data"))
    return CollectorSettings(data_root=pathlib.Path(tmp_path) / "data").paths


def _seed_bars(bars_dir, entries: dict[str, list[dt.date]]) -> None:
    import polars as pl

    from src.marketdata.partitioned_store import upsert_month_partitions

    rows = [(symbol, day) for symbol, days in entries.items() for day in days]
    frame = pl.DataFrame(
        {
            "date": [row[1] for row in rows],
            "symbol": [row[0] for row in rows],
            "close": [1000.0] * len(rows),
            "volume": [1000] * len(rows),
            "trade_value_100m": [100.0] * len(rows),
            "daily_change_pct": [1.0] * len(rows),
        },
        schema={
            "date": pl.Date,
            "symbol": pl.String,
            "close": pl.Float64,
            "volume": pl.Int64,
            "trade_value_100m": pl.Float64,
            "daily_change_pct": pl.Float64,
        },
    )
    upsert_month_partitions(bars_dir, frame, key_columns=("date", "symbol"), sort_columns=("date", "symbol"))


def _gap_result(*, status, missing=None, window=(), stale=(), tracked=("000001",), gap_targets=()):
    from src.marketdata.program_trade_service import ProgramTradesSyncResult
    from src.marketdata.toss_program_trades import ProgramTradeGapReport

    report = ProgramTradeGapReport(status=status, window=tuple(window), missing={} if missing is None else missing)
    return ProgramTradesSyncResult(
        forward_targets=(),
        candidate_targets=(),
        symbols_ok=0,
        symbols_failed=0,
        appended_rows=0,
        stale_symbols=tuple(stale),
        tracked=tuple(tracked),
        gap_targets=tuple(gap_targets),
        gap_report=report,
    )


def test_excess_staleness_exits_2_with_critical(tmp_path, monkeypatch, caplog) -> None:
    # Given: 벤더가 신규 행 없이 빈 페이지만 반환하고 절반이 뒤처진 스토어
    import logging

    from src.cli import toss_program_trades_sync as cli
    from src.marketdata import program_trade_service as service

    paths = _sync_env(monkeypatch, tmp_path)
    _seed(
        paths.program_trades_dir,
        {"000001": [dt.date(2026, 9, 24)], "000002": [dt.date(2026, 9, 24)], "000003": [dt.date(2026, 9, 10)], "000004": [dt.date(2026, 9, 10)]},
    )
    _seed_bars(
        paths.bars_daily_dir,
        {
            "000001": [dt.date(2026, 9, 24)],
            "000002": [dt.date(2026, 9, 24)],
            "000003": [dt.date(2026, 9, 10)],
            "000004": [dt.date(2026, 9, 10)],
        },
    )
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: "tok")
    monkeypatch.setattr(service, "backfill_program_trades_history", lambda symbol, **kwargs: ())

    # When
    with caplog.at_level(logging.CRITICAL):
        rc = cli.main(["--session-date", "2026-09-25", "--complete-through", "2026-09-24"])

    # Then: 절반 stale은 허용치를 초과해 exit 2 + CRITICAL
    assert rc == 2
    assert "status=STALE" in caplog.text
    assert sum(r.levelno == logging.CRITICAL and "status=STALE" in r.getMessage() for r in caplog.records) == 1


def test_sync_within_tolerance_exits_0(tmp_path, monkeypatch, caplog) -> None:
    # Given: 전 종목이 complete-through에 도달한 스토어
    import logging

    from src.cli import toss_program_trades_sync as cli
    from src.marketdata import program_trade_service as service

    paths = _sync_env(monkeypatch, tmp_path)
    _seed(paths.program_trades_dir, {"000001": [dt.date(2026, 9, 24)]})
    _seed_bars(paths.bars_daily_dir, {"000001": [dt.date(2026, 9, 24)]})
    monkeypatch.setattr(service, "issue_access_token", lambda **kwargs: "tok")
    monkeypatch.setattr(service, "backfill_program_trades_history", lambda symbol, **kwargs: ())

    # When
    with caplog.at_level(logging.INFO):
        rc = cli.main(["--session-date", "2026-09-25", "--complete-through", "2026-09-24"])

    # Then
    assert rc == 0
    assert "status=OK" in caplog.text


def test_missing_credentials_exit_1(monkeypatch, tmp_path) -> None:
    # Given: TOSS env 부재
    from src.cli import toss_program_trades_sync as cli

    monkeypatch.delenv("TOSS_APP_KEY", raising=False)
    monkeypatch.delenv("TOSS_APP_SECRET", raising=False)
    monkeypatch.setenv("KRX_ALPHA_DATA_ROOT", str(tmp_path / "data"))

    # When / Then
    assert cli.main(["--session-date", "2026-09-25", "--complete-through", "2026-09-24"]) == 1


def test_token_failure_exits_1(tmp_path, monkeypatch) -> None:
    # Given: 토큰 발급 실패를 위장한 벤더
    from src.cli import toss_program_trades_sync as cli
    from src.marketdata import program_trade_service as service
    from src.marketdata.toss_calendar import TossCalendarError

    paths = _sync_env(monkeypatch, tmp_path)
    _seed(paths.program_trades_dir, {"000001": [dt.date(2026, 9, 24)]})

    def _fail_token(**kwargs):
        raise TossCalendarError("auth down")

    monkeypatch.setattr(service, "issue_access_token", _fail_token)

    # When / Then: fail-closed exit 1
    assert cli.main(["--session-date", "2026-09-25", "--complete-through", "2026-09-24"]) == 1


def test_active_symbols_from_market_map_and_fallbacks(tmp_path, caplog) -> None:
    import json
    import logging

    from src.cli.toss_program_trades_sync import _active_symbols

    good = tmp_path / "market_map.json"
    good.write_text(json.dumps({"005930": "KOSPI", "000660": "KOSPI"}), encoding="utf-8")
    empty = tmp_path / "empty.json"
    empty.write_text("{}", encoding="utf-8")

    with caplog.at_level(logging.WARNING):
        assert _active_symbols(good) == frozenset({"005930", "000660"})
        # 기준을 알 수 없으면 None: 전 종목 추적으로 되돌아가 경보를 끄지 않는다.
        assert _active_symbols(tmp_path / "missing.json") is None
        assert _active_symbols(empty) is None

    assert "reason=market_map_unreadable" in caplog.text
    assert "reason=market_map_empty" in caplog.text


def test_residual_gaps_exit_2_with_critical(tmp_path, monkeypatch, caplog) -> None:
    # Given: 사후 갭 보고서가 잔여 갭을 담고 있다
    import logging

    from src.cli import toss_program_trades_sync as cli
    from src.marketdata.toss_program_trades import ProgramTradeGapStatus

    _sync_env(monkeypatch, tmp_path)
    monkeypatch.setattr(
        cli,
        "sync_program_trades_forward",
        lambda **kwargs: _gap_result(
            status=ProgramTradeGapStatus.GAPS,
            missing={"000001": (dt.date(2026, 9, 2),)},
            window=(dt.date(2026, 9, 1), dt.date(2026, 9, 2), dt.date(2026, 9, 3)),
            gap_targets=("000001",),
        ),
    )

    # When
    with caplog.at_level(logging.CRITICAL):
        rc = cli.main(["--session-date", "2026-09-25", "--complete-through", "2026-09-24"])

    # Then: 잔여 갭은 CRITICAL + exit 2
    assert rc == 2
    crits = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.CRITICAL and "stage=program_trades_gap status=GAPS" in record.getMessage()
    ]
    assert len(crits) == 1
    assert "sample=000001:2026-09-02" in crits[0]


def test_unverified_calendar_exits_2(tmp_path, monkeypatch, caplog) -> None:
    # Given: 캘린더 미인증 보고서
    import logging

    from src.cli import toss_program_trades_sync as cli
    from src.marketdata.toss_program_trades import ProgramTradeGapStatus

    _sync_env(monkeypatch, tmp_path)
    monkeypatch.setattr(
        cli,
        "sync_program_trades_forward",
        lambda **kwargs: _gap_result(status=ProgramTradeGapStatus.CALENDAR_UNAVAILABLE),
    )

    # When
    with caplog.at_level(logging.CRITICAL):
        rc = cli.main(["--session-date", "2026-09-25", "--complete-through", "2026-09-24"])

    # Then
    assert rc == 2
    assert sum("status=UNVERIFIED" in record.getMessage() for record in caplog.records) == 1


def test_stale_and_gaps_both_reported(tmp_path, monkeypatch, caplog) -> None:
    # Given: stale 비율 초과와 잔여 갭이 동시에 존재
    import logging

    from src.cli import toss_program_trades_sync as cli
    from src.marketdata.toss_program_trades import ProgramTradeGapStatus

    _sync_env(monkeypatch, tmp_path)
    monkeypatch.setattr(
        cli,
        "sync_program_trades_forward",
        lambda **kwargs: _gap_result(
            status=ProgramTradeGapStatus.GAPS,
            missing={"000001": (dt.date(2026, 9, 2),)},
            window=(dt.date(2026, 9, 1), dt.date(2026, 9, 2), dt.date(2026, 9, 3)),
            stale=("000001",),
            tracked=("000001", "000002"),
            gap_targets=("000001",),
        ),
    )

    # When
    with caplog.at_level(logging.CRITICAL):
        rc = cli.main(["--session-date", "2026-09-25", "--complete-through", "2026-09-24"])

    # Then: 두 진단이 독립적으로 기록되고 exit 2
    assert rc == 2
    assert sum("status=STALE" in record.getMessage() for record in caplog.records) == 1
    assert sum("stage=program_trades_gap status=GAPS" in record.getMessage() for record in caplog.records) == 1


def test_complete_run_exits_0_with_gap_targets(tmp_path, monkeypatch, caplog) -> None:
    # Given: 갭 없이 완료된 실행
    import logging

    from src.cli import toss_program_trades_sync as cli
    from src.marketdata.toss_program_trades import ProgramTradeGapStatus

    _sync_env(monkeypatch, tmp_path)
    monkeypatch.setattr(
        cli,
        "sync_program_trades_forward",
        lambda **kwargs: _gap_result(
            status=ProgramTradeGapStatus.COMPLETE, window=(dt.date(2026, 9, 24),), tracked=("000001",)
        ),
    )

    # When
    with caplog.at_level(logging.INFO):
        rc = cli.main(["--session-date", "2026-09-25", "--complete-through", "2026-09-24"])

    # Then
    assert rc == 0
    assert "status=OK" in caplog.text
    assert "gap_targets=" in caplog.text


def test_cli_passes_configured_gap_window(tmp_path, monkeypatch) -> None:
    # Given: 갭 점검 창을 7영업일로 지정
    from src.cli import toss_program_trades_sync as cli
    from src.marketdata.toss_program_trades import ProgramTradeGapStatus

    paths = _sync_env(monkeypatch, tmp_path)
    monkeypatch.setenv("KRX_ALPHA_TOSS_PROGRAM_GAP_CHECK_BUSINESS_DAYS", "7")
    captured: dict[str, object] = {}

    def _fake(**kwargs):
        captured.update(kwargs)
        return _gap_result(status=ProgramTradeGapStatus.COMPLETE, window=(dt.date(2026, 9, 24),))

    monkeypatch.setattr(cli, "sync_program_trades_forward", _fake)

    # When
    rc = cli.main(["--session-date", "2026-09-25", "--complete-through", "2026-09-24"])

    # Then: 설정값과 일봉 경로가 서비스로 전달된다
    assert rc == 0
    assert captured["gap_window_business_days"] == 7
    assert captured["gap_vendor_empty_max_day_ratio"] == 0.02
    assert captured["bars_daily_dir"] == paths.bars_daily_dir


def test_gap_log_sample_is_sorted_and_limited_to_ten_pairs(tmp_path, monkeypatch, caplog) -> None:
    import logging

    from src.cli import toss_program_trades_sync as cli
    from src.marketdata.toss_program_trades import ProgramTradeGapStatus

    _sync_env(monkeypatch, tmp_path)
    day = dt.date(2026, 9, 1)
    days = tuple(day + dt.timedelta(days=index) for index in range(12))
    monkeypatch.setattr(
        cli, "sync_program_trades_forward",
        lambda **kwargs: _gap_result(
            status=ProgramTradeGapStatus.GAPS,
            missing={"000002": (day,), "000001": days}, window=days,
        ),
    )
    with caplog.at_level(logging.CRITICAL):
        assert cli.main(["--session-date", "2026-09-25", "--complete-through", "2026-09-24"]) == 2
    message = next(record.getMessage() for record in caplog.records if "status=GAPS" in record.getMessage())
    assert message.split(" sample=", 1)[1].split(",") == [f"000001:{value}" for value in days[:10]]
