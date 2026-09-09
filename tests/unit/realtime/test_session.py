def test_session_current_state_follows_injected_schedule(tmp_path) -> None:
    # Given: 서로 다른 스케줄을 주입한 두 세션
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.core.calendar import SessionSchedule, SessionState
    from src.realtime.manifest import SessionManifest
    from src.realtime.session import CollectorSession
    from src.realtime.subscription import SubscriptionRegistry

    kst = ZoneInfo("Asia/Seoul")
    at_0830 = dt.datetime(2026, 9, 10, 8, 30, 0, tzinfo=kst)

    def _make(schedule):
        return CollectorSession(
            manifest=SessionManifest(session_date=dt.date(2026, 9, 10), clock_offset_ns=0, started_at_ns=0),
            registry=SubscriptionRegistry(slot_budget=10),
            journals={},
            manifest_path=tmp_path / "manifest.json",
            schedule=schedule,
        )

    default_session = _make(SessionSchedule())
    shifted_session = _make(
        SessionSchedule(
            streamer_start=dt.time(9, 30),
            scanner_start=dt.time(10, 0),
            market_close=dt.time(15, 40),
            eod_done=dt.time(16, 0),
        )
    )

    # When / Then: schedule 필드가 상태전이에 실제로 반영된다 (dead configuration 회귀 방지)
    assert default_session.current_state(at_0830) == SessionState.STREAMER_ACTIVE
    assert shifted_session.current_state(at_0830) == SessionState.PRE_MARKET_SLEEP
    assert shifted_session.seconds_to_streamer(at_0830) == 3600.0
"""수집 세션 composition root 유닛 테스트."""



def test_bootstrap_session_wires_primitives_and_persists_manifest(tmp_path):
    # Given: 후보 2종목이 담긴 candidates.json + 가짜 NTP 클라이언트(offset 1.089s)
    from src.universe.ipc import write_candidates
    from src.realtime.session import SessionConfig, bootstrap_session

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

    from src.realtime.clock import ClockUnsyncedError
    from src.universe.ipc import write_candidates
    from src.realtime.session import SessionConfig, bootstrap_session

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

    from src.universe.ipc import write_candidates
    from src.realtime.session import SessionConfig, bootstrap_session

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

    from src.universe.ipc import write_candidates
    from src.realtime.session import SessionConfig, bootstrap_session

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

    from src.universe.ipc import write_candidates
    from src.realtime.manifest import SessionManifest
    from src.realtime.session import SessionConfig, bootstrap_session

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


def test_bootstrap_session_runs_eod_maintenance(tmp_path) -> None:
    import datetime as dt
    import json
    import zstandard as zstd
    from src.universe.ipc import write_candidates
    from src.realtime.session import SessionConfig, bootstrap_session

    class _FC:
        def request(self, host, version=3, timeout=5):
            class _S:
                offset = 0.0
            return _S()

    root = tmp_path / 'l0'
    old_part = root / 'kis' / 'H0STCNT0' / 'dt=2026-09-01'
    old_part.mkdir(parents=True, exist_ok=True)
    rec = {'raw': 'a', 'recv_mono_ns': 1, 'recv_wall_ns': 2, 'conn_id': 'c1', 'conn_seq': 1, 'vendor': 'kis', 'tr_id': 'H0STCNT0'}
    (old_part / '09.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress((json.dumps(rec) + '\n').encode('utf-8')))

    cand_path = tmp_path / 'candidates.json'
    write_candidates(cand_path, [{'symbol': '005930', 'selection_reasons': ['limit_up']}], rev=1)
    cfg = SessionConfig(
        session_date=dt.date(2026, 9, 8),
        journal_root=root,
        manifest_path=tmp_path / 'session.json',
        candidates_path=cand_path,
        ntp_host='pool.ntp.org',
        slot_budget=41,
        max_clock_offset_ns=2_000_000_000,
        desired_streams=('H0STCNT0',),
        vendor='kis',
        archive_root=tmp_path / 'l1',
    )

    bootstrap_session(cfg, ntp_client=_FC(), now_ns=999)

    assert not old_part.exists()
    assert (tmp_path / 'l1' / 'kis' / 'H0STCNT0' / 'dt=2026-09-01.parquet').exists()


def test_bootstrap_session_without_archive_root_retains_journals(tmp_path) -> None:
    import datetime as dt
    from src.universe.ipc import write_candidates
    from src.realtime.session import SessionConfig, bootstrap_session

    class _FC:
        def request(self, host, version=3, timeout=5):
            class _S:
                offset = 0.0
            return _S()

    root = tmp_path / 'l0'
    old_part = root / 'kis' / 'H0STCNT0' / 'dt=2026-09-01'
    old_part.mkdir(parents=True, exist_ok=True)
    (old_part / '09.jsonl.zst').write_text('dummy')

    cand_path = tmp_path / 'candidates.json'
    write_candidates(cand_path, [{'symbol': '005930', 'selection_reasons': ['limit_up']}], rev=1)
    cfg = SessionConfig(
        session_date=dt.date(2026, 9, 8),
        journal_root=root,
        manifest_path=tmp_path / 'session.json',
        candidates_path=cand_path,
        ntp_host='pool.ntp.org',
        slot_budget=41,
        max_clock_offset_ns=2_000_000_000,
        desired_streams=('H0STCNT0',),
        vendor='kis',
    )

    bootstrap_session(cfg, ntp_client=_FC(), now_ns=999)

    # Then: archive_root 없음 -> 만료 파티션 보존
    assert old_part.exists()
def test_collector_session_record_frame_raises_storage_exhausted(tmp_path) -> None:
    import datetime as dt
    import pytest
    from unittest.mock import patch
    import shutil
    from src.universe.ipc import write_candidates
    from src.realtime.session import SessionConfig, bootstrap_session
    from src.storage.retention import StorageExhaustedError

    class _FC:
        def request(self, host, version=3, timeout=5):
            class _S:
                offset = 0.0
            return _S()

    cp = tmp_path / 'c.json'
    write_candidates(cp, [{'symbol': '005930', 'selection_reasons': ['limit_up']}], rev=1)
    cfg = SessionConfig(session_date=dt.date(2026, 9, 8), journal_root=tmp_path / 'l0',
                        manifest_path=tmp_path / 's.json', candidates_path=cp, ntp_host='h',
                        slot_budget=41, max_clock_offset_ns=2_000_000_000,
                        desired_streams=('H0STCNT0',), vendor='kis')
    session = bootstrap_session(cfg, ntp_client=_FC(), now_ns=1)

    # Simulate depleted disk (1GB free < 3GB watermark)
    depleted_usage = shutil._ntuple_diskusage(50 * (1024**3), 49 * (1024**3), 1 * (1024**3))
    with patch('shutil.disk_usage', return_value=depleted_usage):  # noqa: SIM117 - spec skeleton keeps nested with
        with pytest.raises(StorageExhaustedError, match='watermark'):
            session.record_frame(vendor='kis', stream='H0STCNT0', raw='dummy',
                                 recv_mono_ns=1, recv_wall_ns=1_735_954_200_000_000_000, conn_id='c', conn_seq=1)
