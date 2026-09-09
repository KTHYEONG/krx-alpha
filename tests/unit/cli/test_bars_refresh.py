def test_bars_refresh_cli_delegates_to_service_and_maps_exit_code(tmp_path, monkeypatch) -> None:
    # Given: service 를 대체한 CLI 호출
    import argparse
    import datetime as dt

    from src.cli import bars_refresh
    from src.marketdata.krx_bars import KrxBarsError

    monkeypatch.setenv("KRX_OPENAPI_KEY", "k")
    seen: dict[str, object] = {}

    def _ok(**kwargs):
        seen.update(kwargs)
        return bars_refresh.BarsRefreshResult(trading_day=dt.date(2026, 9, 9), appended_rows=2, backfilled_days=0)

    monkeypatch.setattr(bars_refresh, "refresh_bars", _ok)
    args = argparse.Namespace(
        store_path=str(tmp_path / "bars.parquet"),
        market_map_path=str(tmp_path / "market_map.json"),
        ref_date="2026-09-10",
        window_days=90,
    )

    # When / Then: 정상 경로 rc=0
    assert bars_refresh.run(args) == 0
    assert seen["ref_date"] == dt.date(2026, 9, 10)
    assert seen["window_days"] == 90

    # When / Then: 도메인 실패는 rc=4 로 매핑된다
    def _fail(**kwargs):
        raise KrxBarsError("down")

    monkeypatch.setattr(bars_refresh, "refresh_bars", _fail)
    assert bars_refresh.run(args) == 4


def test_bars_refresh_cli_returns_rc4_when_credentials_missing(tmp_path, monkeypatch, caplog) -> None:
    # Given: 자격증명 env 부재
    import argparse
    import logging

    from src.cli import bars_refresh

    monkeypatch.delenv("KRX_OPENAPI_KEY", raising=False)
    market_map = tmp_path / "market_map.json"
    market_map.write_text('{"005930": "KOSPI"}', encoding="utf-8")
    args = argparse.Namespace(
        store_path=str(tmp_path / "bars.parquet"),
        market_map_path=str(market_map),
        ref_date="2026-09-10",
        window_days=90,
    )

    # When
    with caplog.at_level(logging.ERROR):
        rc = bars_refresh.run(args)

    # Then: 기존 산출물 보존 + 구조화 로그
    assert rc == 4
    assert market_map.read_text(encoding="utf-8") == '{"005930": "KOSPI"}'
    assert any("missing_credentials" in record.message for record in caplog.records)
