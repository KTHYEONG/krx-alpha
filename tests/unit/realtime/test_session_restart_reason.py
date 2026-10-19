"""Invariant guards for the restart-gap reason (`restart` vs `stale_child`)."""

from __future__ import annotations

import datetime as dt
import logging
import os


def _kst_ns(year, month, day, hour, minute=0):
    from zoneinfo import ZoneInfo

    return int(dt.datetime(year, month, day, hour, minute, tzinfo=ZoneInfo("Asia/Seoul")).timestamp() * 1e9)


def _cfg(*, day, journal_root, manifest_path, candidates_path, restart_reason=None):
    import src.realtime.session as session_mod

    return session_mod.SessionConfig(
        session_date=day,
        journal_root=journal_root,
        manifest_path=manifest_path,
        candidates_path=candidates_path,
        ntp_host="ntp",
        slot_budget=2,
        max_clock_offset_ns=2_000_000_000,
        desired_streams=("H0STCNT0",),
        vendor="ls",
        restart_reason=restart_reason,
    )


def _write_candidates(path) -> None:
    from src.universe.ipc import write_candidates

    write_candidates(path, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=20260911)


def _boot_twice(tmp_path, monkeypatch, caplog, *, restart_reason):
    import src.realtime.session as session_mod
    from src.realtime.session import bootstrap_session

    monkeypatch.setattr(session_mod, "measure_ntp_offset_ns", lambda host, *, client=None: 0)
    day = dt.date(2026, 9, 14)
    journal_root = tmp_path / "l0"
    manifest_path = tmp_path / "s.json"
    candidates_path = tmp_path / "c.json"
    _write_candidates(candidates_path)
    bootstrap_session(
        _cfg(day=day, journal_root=journal_root, manifest_path=manifest_path, candidates_path=candidates_path),
        now_ns=_kst_ns(2026, 9, 14, 9, 0),
    )
    journal_file = journal_root / "ls" / "krx" / "regular" / "H0STCNT0" / "dt=2026-09-14" / "09.jsonl.zst"
    journal_file.parent.mkdir(parents=True, exist_ok=True)
    journal_file.write_bytes(b"x")
    last_write_ns = _kst_ns(2026, 9, 14, 10, 0)
    os.utime(journal_file, ns=(last_write_ns, last_write_ns))
    second_ns = last_write_ns + 120_000_000_000
    with caplog.at_level(logging.WARNING):
        session = bootstrap_session(
            _cfg(
                day=day, journal_root=journal_root, manifest_path=manifest_path,
                candidates_path=candidates_path, restart_reason=restart_reason,
            ),
            now_ns=second_ns,
        )
    return session, last_write_ns, second_ns


def test_stale_reason_recorded(tmp_path, monkeypatch, caplog) -> None:
    session, last_write_ns, second_ns = _boot_twice(tmp_path, monkeypatch, caplog, restart_reason="stale_child")
    assert len(session.manifest.gaps) == 1
    gap = session.manifest.gaps[0]
    assert gap["reason"] == "stale_child"
    assert gap["symbol"] == "*"
    assert gap["gap_start_ns"] == last_write_ns
    assert gap["gap_end_ns"] == second_ns
    assert "status=RESTART_GAP gap_s=120.0" in caplog.text
    assert "reason=stale_child" in caplog.text


def test_default_reason_still_restart(tmp_path, monkeypatch, caplog) -> None:
    session, _, _ = _boot_twice(tmp_path, monkeypatch, caplog, restart_reason=None)
    assert session.manifest.gaps[-1]["reason"] == "restart"
    assert "reason=restart" in caplog.text


def test_first_boot_ignores_reason(tmp_path, monkeypatch) -> None:
    import src.realtime.session as session_mod
    from src.realtime.session import bootstrap_session

    monkeypatch.setattr(session_mod, "measure_ntp_offset_ns", lambda host, *, client=None: 0)
    day = dt.date(2026, 9, 14)
    candidates_path = tmp_path / "c.json"
    _write_candidates(candidates_path)
    session = bootstrap_session(
        _cfg(
            day=day, journal_root=tmp_path / "l0", manifest_path=tmp_path / "s.json",
            candidates_path=candidates_path, restart_reason="stale_child",
        ),
        now_ns=_kst_ns(2026, 9, 14, 9, 0),
    )
    assert session.manifest.gaps == []


def test_invalid_reason_rejected_early(tmp_path, monkeypatch) -> None:
    import pytest

    import src.realtime.session as session_mod
    from src.realtime.session import bootstrap_session

    monkeypatch.setattr(session_mod, "measure_ntp_offset_ns", lambda host, *, client=None: 0)
    calls = []
    orig_replace = os.replace
    monkeypatch.setattr(os, "replace", lambda *a, **k: (calls.append(a), orig_replace(*a, **k)))
    with pytest.raises(ValueError, match="restart_reason"):
        bootstrap_session(
            _cfg(
                day=dt.date(2026, 9, 14), journal_root=tmp_path / "l0",
                manifest_path=tmp_path / "s.json", candidates_path=tmp_path / "c.json",
                restart_reason="oops",
            ),
            now_ns=_kst_ns(2026, 9, 14, 9, 0),
        )
