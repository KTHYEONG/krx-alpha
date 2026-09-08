"""수집 세션 composition root 유닛 테스트."""

from __future__ import annotations


def test_bootstrap_session_wires_primitives_and_persists_manifest(tmp_path):
    # Given: 후보 2종목이 담긴 candidates.json + 가짜 NTP 클라이언트(offset 1.089s)
    from src.collector.ipc import write_candidates
    from src.collector.session import SessionConfig, bootstrap_session

    class _FakeStat:
        offset = 1.089

    class _FakeClient:
        def request(self, host, version=3, timeout=5):
            return _FakeStat()

    import datetime as dt

    cand_path = tmp_path / "candidates.json"
    write_candidates(cand_path, [{"symbol": "005930", "selection_reasons": ["limit_up"]},
                                 {"symbol": "000660", "selection_reasons": ["surge10"]}], rev=1)
    cfg = SessionConfig(
        session_date=dt.date(2026, 9, 8),
        journal_root=tmp_path / "l0",
        manifest_path=tmp_path / "session.json",
        candidates_path=cand_path,
        ntp_host="pool.ntp.org",
        slot_budget=41,
        max_clock_offset_ns=2_000_000_000,
        desired_streams=("H0STCNT0", "H0STASP0"),
        vendor="kis",
    )

    # When
    session = bootstrap_session(cfg, ntp_client=_FakeClient(), now_ns=999)

    # Then: manifest 저장 + 구독 레지스트리에 2종목*2스트림 = 4페어 반영
    assert cfg.manifest_path.exists()
    assert session.manifest.clock_offset_ns == 1_089_000_000
    assert len(session.replay_pairs()) == 4
    assert ("005930", "H0STCNT0") in session.replay_pairs()


def test_bootstrap_session_rejects_unsynced_clock(tmp_path):
    # Given: NTP offset 3s (한계 2s 초과)
    import datetime as dt

    import pytest

    from src.collector.clock import ClockUnsyncedError
    from src.collector.ipc import write_candidates
    from src.collector.session import SessionConfig, bootstrap_session

    class _FakeStat:
        offset = 3.0

    class _FakeClient:
        def request(self, host, version=3, timeout=5):
            return _FakeStat()

    cand_path = tmp_path / "candidates.json"
    write_candidates(cand_path, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=1)
    cfg = SessionConfig(
        session_date=dt.date(2026, 9, 8), journal_root=tmp_path / "l0",
        manifest_path=tmp_path / "session.json", candidates_path=cand_path,
        ntp_host="pool.ntp.org", slot_budget=41, max_clock_offset_ns=2_000_000_000,
        desired_streams=("H0STCNT0",), vendor="kis",
    )

    # When / Then: 수집 시작 거부
    with pytest.raises(ClockUnsyncedError):
        bootstrap_session(cfg, ntp_client=_FakeClient(), now_ns=1)


def test_collector_session_record_frame_routes_and_flushes(tmp_path):
    # Given: 부트스트랩된 세션
    import datetime as dt

    from src.collector.ipc import write_candidates
    from src.collector.session import SessionConfig, bootstrap_session

    class _FC:
        def request(self, host, version=3, timeout=5):
            class _S:
                offset = 0.0

            return _S()

    cp = tmp_path / "c.json"
    write_candidates(cp, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=1)
    cfg = SessionConfig(session_date=dt.date(2026, 9, 8), journal_root=tmp_path / "l0",
                        manifest_path=tmp_path / "s.json", candidates_path=cp, ntp_host="h",
                        slot_budget=41, max_clock_offset_ns=2_000_000_000,
                        desired_streams=("H0STCNT0",), vendor="kis")
    session = bootstrap_session(cfg, ntp_client=_FC(), now_ns=1)

    # When: 프레임 기록 후 flush
    session.record_frame(vendor="kis", stream="H0STCNT0", raw="0|H0STCNT0|001|a^b",
                         recv_mono_ns=10, recv_wall_ns=1_735_954_200_000_000_000, conn_id="c1", conn_seq=1)
    written = session.flush_journals()

    # Then: 1건 기록 + 파티션 파일 존재
    assert written == 1
    assert any((tmp_path / "l0").rglob("*.jsonl.zst"))


def test_collector_session_record_frame_unknown_stream_raises_keyerror(tmp_path):
    # Given: H0STCNT0 만 등록된 세션
    import datetime as dt

    import pytest

    from src.collector.ipc import write_candidates
    from src.collector.session import SessionConfig, bootstrap_session

    class _FC:
        def request(self, host, version=3, timeout=5):
            class _S:
                offset = 0.0

            return _S()

    cp = tmp_path / "c.json"
    write_candidates(cp, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=1)
    cfg = SessionConfig(session_date=dt.date(2026, 9, 8), journal_root=tmp_path / "l0",
                        manifest_path=tmp_path / "s.json", candidates_path=cp, ntp_host="h",
                        slot_budget=41, max_clock_offset_ns=2_000_000_000,
                        desired_streams=("H0STCNT0",), vendor="kis")
    session = bootstrap_session(cfg, ntp_client=_FC(), now_ns=1)

    # When / Then: 미등록 스트림은 조용히 버리지 않고 KeyError
    with pytest.raises(KeyError, match="H0STASP0"):
        session.record_frame(vendor="kis", stream="H0STASP0", raw="x", recv_mono_ns=1,
                             recv_wall_ns=1_735_954_200_000_000_000, conn_id="c", conn_seq=1)


def test_collector_session_note_ack_and_gap_reach_manifest(tmp_path):
    # Given: 부트스트랩된 세션
    import datetime as dt

    from src.collector.ipc import write_candidates
    from src.collector.manifest import SessionManifest
    from src.collector.session import SessionConfig, bootstrap_session

    class _FC:
        def request(self, host, version=3, timeout=5):
            class _S:
                offset = 0.0

            return _S()

    cp = tmp_path / "c.json"
    write_candidates(cp, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=1)
    cfg = SessionConfig(session_date=dt.date(2026, 9, 8), journal_root=tmp_path / "l0",
                        manifest_path=tmp_path / "s.json", candidates_path=cp, ntp_host="h",
                        slot_budget=41, max_clock_offset_ns=2_000_000_000,
                        desired_streams=("H0STCNT0",), vendor="kis")
    session = bootstrap_session(cfg, ntp_client=_FC(), now_ns=1)

    # When: ACK + gap 기록 후 재저장
    session.note_ack(vendor="kis", tr_id="H0STCNT0", symbol="005930", rt_cd="0", accepted=True)
    session.note_gap(symbol="005930", gap_start_ns=5, gap_end_ns=9, reason="ws_reconnect")
    session.persist()

    # Then: manifest 파일에 반영
    reloaded = SessionManifest.load(cfg.manifest_path)
    assert reloaded.accepted_symbols() == {"005930"}
    assert reloaded.gaps[0]["reason"] == "ws_reconnect"
