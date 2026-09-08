"""Session manifest unit tests."""

from __future__ import annotations


def test_manifest_roundtrip_preserves_acks_and_gaps(tmp_path):
    # Given: ACK 2건 + gap 1건을 기록한 manifest
    import datetime as dt

    from src.collector.manifest import SessionManifest

    m = SessionManifest(session_date=dt.date(2026, 9, 8), clock_offset_ns=1_089_000_000, started_at_ns=42)
    m.record_ack(vendor="kis", tr_id="H0STCNT0", symbol="005930", rt_cd="0", accepted=True)
    m.record_ack(vendor="kis", tr_id="H0STANC0", symbol="000660", rt_cd="9", accepted=False)
    m.record_gap(symbol="005930", gap_start_ns=100, gap_end_ns=250, reason="ws_reconnect")

    # When: 저장 후 재로드
    path = tmp_path / "session.json"
    m.save(path)
    loaded = SessionManifest.load(path)

    # Then: 필드가 보존되고 accepted 심볼만 집계된다
    assert loaded.clock_offset_ns == 1_089_000_000
    assert loaded.accepted_symbols() == {"005930"}
    assert loaded.gaps[0]["reason"] == "ws_reconnect"
    assert loaded.gaps[0]["gap_end_ns"] == 250


def test_manifest_assert_clock_within_raises_when_exceeded():
    # Given: 3초(3e9 ns) 오프셋 manifest, 허용 한계 2초
    import datetime as dt

    import pytest

    from src.collector.clock import ClockUnsyncedError
    from src.collector.manifest import SessionManifest

    m = SessionManifest(session_date=dt.date(2026, 9, 8), clock_offset_ns=3_000_000_000, started_at_ns=0)

    # When / Then: 한계 초과 시 ClockUnsyncedError
    with pytest.raises(ClockUnsyncedError):
        m.assert_clock_within(max_offset_ns=2_000_000_000)

    # 한계 이내면 통과
    m2 = SessionManifest(session_date=dt.date(2026, 9, 8), clock_offset_ns=1_089_000_000, started_at_ns=0)
    m2.assert_clock_within(max_offset_ns=2_000_000_000)


def test_manifest_save_is_atomic_no_partial_target(tmp_path, monkeypatch):
    # Given: os.replace 직전까지는 최종 경로에 파일이 없어야 한다
    import datetime as dt
    import os as _os

    from src.collector.manifest import SessionManifest

    m = SessionManifest(session_date=dt.date(2026, 9, 8), clock_offset_ns=0, started_at_ns=0)
    target = tmp_path / "session.json"
    seen = {}

    real_replace = _os.replace

    def _spy_replace(src, dst):
        seen["target_exists_before_replace"] = (tmp_path / "session.json").exists()
        seen["src_is_tmp"] = str(src) != str(dst)
        return real_replace(src, dst)

    monkeypatch.setattr(_os, "replace", _spy_replace)

    # When
    m.save(target)

    # Then: 임시 파일 -> os.replace 경로를 통과했고 최종 파일이 존재한다
    assert seen["src_is_tmp"] is True
    assert seen["target_exists_before_replace"] is False
    assert target.exists()
