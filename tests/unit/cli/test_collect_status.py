"""collect-status CLI unit tests."""

from __future__ import annotations


def test_collect_status_run_summarizes_manifest(tmp_path, caplog):
    # Given: 저장된 세션 manifest
    import argparse
    import datetime as dt
    import logging

    from src.cli.collect_status import run
    from src.realtime.manifest import SessionManifest

    m = SessionManifest(session_date=dt.date(2026, 9, 8), clock_offset_ns=1_089_000_000, started_at_ns=1)
    m.record_ack(vendor="kis", tr_id="H0STCNT0", symbol="005930", rt_cd="0", accepted=True)
    m.record_gap(symbol="005930", gap_start_ns=1, gap_end_ns=2, reason="ws_reconnect")
    mpath = tmp_path / "session.json"
    m.save(mpath)

    args = argparse.Namespace(manifest_path=str(mpath))

    # When
    with caplog.at_level(logging.INFO):
        rc = run(args)

    # Then: 종료코드 0 과 요약 로그(수용 심볼 수 / gap 수)
    assert rc == 0
    joined = " ".join(r.message for r in caplog.records)
    assert "accepted=1" in joined
    assert "gaps=1" in joined


def test_collect_status_run_missing_manifest_returns_2(tmp_path):
    # Given: 없는 manifest 경로
    import argparse

    from src.cli.collect_status import run

    args = argparse.Namespace(manifest_path=str(tmp_path / "absent.json"))

    # When / Then: 예외 대신 비정상 종료코드 2
    assert run(args) == 2
