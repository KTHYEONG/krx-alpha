def test_run_delegates_parsed_args_and_maps_exit_codes(tmp_path, monkeypatch) -> None:
    # Given: 서비스 가짜와 자격증명 env
    import argparse
    import datetime as dt

    from src.cli import toss_program_trades_backfill
    from src.marketdata.toss_program_trades import TossProgramTradesError

    monkeypatch.setenv("TOSS_APP_KEY", "k")
    monkeypatch.setenv("TOSS_APP_SECRET", "s")
    seen: dict[str, object] = {}

    def _ok(**kwargs):
        seen.update(kwargs)
        return toss_program_trades_backfill.ProgramTradesBackfillResult(symbols_ok=2, symbols_failed=0, appended_rows=5)

    monkeypatch.setattr(toss_program_trades_backfill, "backfill_program_trades", _ok)
    args = argparse.Namespace(
        store_path=str(tmp_path / "program_trades.parquet"),
        symbols="005930,000660",
        min_date="2024-04-01",
    )

    # When / Then: 정상 경로 rc=0, 인자 분해 전달
    assert toss_program_trades_backfill.run(args) == 0
    assert seen["symbols"] == ("005930", "000660")
    assert seen["min_date"] == dt.date(2024, 4, 1)

    # When / Then: 도메인 실패는 rc=4
    def _fail(**kwargs):
        raise TossProgramTradesError("down")

    monkeypatch.setattr(toss_program_trades_backfill, "backfill_program_trades", _fail)
    assert toss_program_trades_backfill.run(args) == 4


def test_run_maps_missing_credentials_to_exit_code_4(tmp_path, monkeypatch) -> None:
    # Given: TOSS 자격증명 env 부재
    import argparse

    from src.cli import toss_program_trades_backfill

    monkeypatch.delenv("TOSS_APP_KEY", raising=False)
    monkeypatch.delenv("TOSS_APP_SECRET", raising=False)
    called: list[str] = []
    monkeypatch.setattr(
        toss_program_trades_backfill, "backfill_program_trades", lambda **kwargs: called.append("service") or 0
    )
    args = argparse.Namespace(
        store_path=str(tmp_path / "program_trades.parquet"),
        symbols="005930",
        min_date="2024-04-01",
    )

    # When / Then
    assert toss_program_trades_backfill.run(args) == 4
    assert called == []
