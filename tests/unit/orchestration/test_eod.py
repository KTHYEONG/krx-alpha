def test_run_eod_maintenance_threads_archive_root(tmp_path) -> None:
    import datetime as dt
    import json
    import zstandard as zstd
    from src.orchestration.eod import run_eod_maintenance

    # Given: 만료 파티션에 유효 프레임
    part = tmp_path / 'l0' / 'kis' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True, exist_ok=True)
    rec = {'raw': 'a', 'recv_mono_ns': 1, 'recv_wall_ns': 2, 'conn_id': 'c1', 'conn_seq': 1, 'vendor': 'kis', 'tr_id': 'H0STCNT0'}
    (part / '09.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress((json.dumps(rec) + '\n').encode('utf-8')))
    archive_root = tmp_path / 'l1'

    # When
    deleted = run_eod_maintenance(tmp_path / 'l0', retain_days=3, today=dt.date(2026, 9, 8), archive_root=archive_root)

    # Then
    assert deleted == 1
    assert not part.exists()
    assert (archive_root / 'kis' / 'H0STCNT0' / 'dt=2026-09-01.parquet').exists()


def test_run_eod_maintenance_invokes_prune(tmp_path) -> None:
    import datetime as dt
    from src.orchestration.eod import run_eod_maintenance

    # Given: 만료 + 최근 파티션, archive_root 미지정
    root = tmp_path / 'l0' / 'kis' / 'H0STCNT0'
    old_part = root / 'dt=2026-09-01'
    recent_part = root / 'dt=2026-09-07'
    old_part.mkdir(parents=True, exist_ok=True)
    recent_part.mkdir(parents=True, exist_ok=True)
    (old_part / '09.jsonl.zst').write_text('dummy')
    (recent_part / '09.jsonl.zst').write_text('dummy')

    # When
    deleted = run_eod_maintenance(tmp_path / 'l0', retain_days=3, today=dt.date(2026, 9, 8))

    # Then: offload 대상 없음 -> 삭제 0, 원본 보존
    assert deleted == 0
    assert old_part.exists()
    assert recent_part.exists()


def test_run_eod_offload_syncs_and_prunes_with_injected_archiver(tmp_path) -> None:
    import datetime as dt
    from src.orchestration.eod import run_eod_offload

    root = tmp_path / 'l1' / 'kis' / 'H0STCNT0'
    root.mkdir(parents=True)
    old_pq = root / 'dt=2026-07-01.parquet'
    old_pq.write_bytes(b'x')

    class _Arc:
        def sync_l1_tree(self, archive_root):
            return {'uploaded': 1, 'skipped': 0, 'failed': 0}
        def remote_files(self, prefix):
            return {'l1/kis/H0STCNT0/dt=2026-07-01.parquet'}

    stats = run_eod_offload(tmp_path / 'l1', archiver=_Arc(), reference_date=dt.date(2026, 9, 8))

    assert stats['uploaded'] == 1
    assert stats['purged'] == 1
    assert not old_pq.exists()


def test_run_eod_offload_returns_zeros_when_archiver_missing(tmp_path, caplog, monkeypatch) -> None:
    import logging
    from src.orchestration.eod import run_eod_offload

    root = tmp_path / 'l1' / 'kis' / 'H0STCNT0'
    root.mkdir(parents=True)
    old_pq = root / 'dt=2026-07-01.parquet'
    old_pq.write_bytes(b'x')

    monkeypatch.setattr('src.storage.remote.HfDatasetArchiver.try_from_env', staticmethod(lambda: None))

    with caplog.at_level(logging.CRITICAL):
        stats = run_eod_offload(tmp_path / 'l1')

    assert stats == {'uploaded': 0, 'skipped': 0, 'failed': 0, 'purged': 0}
    assert old_pq.exists()
    assert any(r.levelno == logging.CRITICAL for r in caplog.records)
