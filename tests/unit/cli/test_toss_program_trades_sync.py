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
