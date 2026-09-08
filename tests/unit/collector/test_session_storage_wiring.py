def test_collector_session_record_frame_raises_storage_exhausted(tmp_path) -> None:
    import datetime as dt
    import pytest
    from unittest.mock import patch
    import shutil
    from src.collector.ipc import write_candidates
    from src.collector.session import SessionConfig, bootstrap_session
    from src.collector.storage_guard import StorageExhaustedError

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
