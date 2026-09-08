"""collect-init CLI unit tests."""

from __future__ import annotations


def test_collect_init_run_bootstraps_and_reports(tmp_path, caplog, monkeypatch):
    # Given: candidates.json + NTP 를 가짜로 대체
    import argparse
    import logging

    import src.collector.clock as clock_mod
    from src.cli.collect_init import run
    from src.collector.ipc import write_candidates

    def _fake_offset(host, *, samples=5, timeout_s=3.0, client=None):
        assert host
        return 500_000_000

    monkeypatch.setattr(clock_mod, "measure_ntp_offset_ns", _fake_offset)
    monkeypatch.setattr("src.collector.session.measure_ntp_offset_ns", _fake_offset)

    cp = tmp_path / "candidates.json"
    write_candidates(cp, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=1)
    args = argparse.Namespace(
        session_date="2026-09-08", journal_root=str(tmp_path / "l0"),
        manifest_path=str(tmp_path / "session.json"), candidates_path=str(cp),
        ntp_host="pool.ntp.org", slot_budget=41, max_clock_offset_ns=2_000_000_000,
        streams="H0STCNT0,H0STASP0", vendor="kis",
    )

    # When
    with caplog.at_level(logging.INFO):
        rc = run(args)

    # Then: 0 반환 + manifest 생성 + 요약 로그
    assert rc == 0
    assert (tmp_path / "session.json").exists()
    joined = " ".join(r.message for r in caplog.records)
    assert "pairs=2" in joined


def test_collect_init_run_rejects_unsynced_clock_returns_3(tmp_path, caplog, monkeypatch):
    # Given: NTP offset 3s (한계 2s 초과) 를 반환하도록 패치
    import argparse
    import logging

    from src.cli.collect_init import run
    from src.collector.ipc import write_candidates

    def _big_offset(host, *, samples=5, timeout_s=3.0, client=None):
        assert host
        return 3_000_000_000

    monkeypatch.setattr("src.collector.session.measure_ntp_offset_ns", _big_offset)

    cp = tmp_path / "candidates.json"
    write_candidates(cp, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=1)
    args = argparse.Namespace(
        session_date="2026-09-08", journal_root=str(tmp_path / "l0"),
        manifest_path=str(tmp_path / "session.json"), candidates_path=str(cp),
        ntp_host="pool.ntp.org", slot_budget=41, max_clock_offset_ns=2_000_000_000,
        streams="H0STCNT0", vendor="kis",
    )

    # When
    with caplog.at_level(logging.ERROR):
        rc = run(args)

    # Then: 종료코드 3 + manifest 미생성 + FAIL 로그
    assert rc == 3
    assert not (tmp_path / "session.json").exists()
    assert any("status=FAIL" in r.message for r in caplog.records)


def test_collect_init_accepts_archive_root_arg(tmp_path, caplog, monkeypatch) -> None:
    import argparse
    import logging
    import src.collector.clock as clock_mod
    from src.cli.collect_init import run
    from src.collector.ipc import write_candidates

    def _fake_offset(host, *, samples=5, timeout_s=3.0, client=None):
        assert host
        return 500_000_000

    monkeypatch.setattr(clock_mod, 'measure_ntp_offset_ns', _fake_offset)
    monkeypatch.setattr('src.collector.session.measure_ntp_offset_ns', _fake_offset)

    cp = tmp_path / 'candidates.json'
    write_candidates(cp, [{'symbol': '005930', 'selection_reasons': ['limit_up']}], rev=1)
    args = argparse.Namespace(
        session_date='2026-09-08', journal_root=str(tmp_path / 'l0'),
        manifest_path=str(tmp_path / 'session.json'), candidates_path=str(cp),
        ntp_host='pool.ntp.org', slot_budget=41, max_clock_offset_ns=2_000_000_000,
        streams='H0STCNT0', vendor='kis', archive_root=str(tmp_path / 'l1'),
    )

    with caplog.at_level(logging.INFO):
        rc = run(args)

    assert rc == 0
    assert (tmp_path / 'session.json').exists()


def test_collect_init_parser_registers_archive_root() -> None:
    import argparse
    from src.cli.collect_init import add_parser

    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers()
    add_parser(sub)
    ns = parser.parse_args([
        'collect-init', '--session-date', '2026-09-08', '--journal-root', 'l0',
        '--manifest-path', 's.json', '--candidates-path', 'c.json',
    ])
    assert hasattr(ns, 'archive_root')
    assert ns.archive_root is None
