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

    monkeypatch.setattr('src.storage.remote.RcloneArchiver.try_from_env', staticmethod(lambda: None))

    with caplog.at_level(logging.CRITICAL):
        stats = run_eod_offload(tmp_path / 'l1')

    assert stats == {'uploaded': 0, 'skipped': 0, 'failed': 0, 'purged': 0}
    assert old_pq.exists()
    assert any(r.levelno == logging.CRITICAL for r in caplog.records)


def test_run_eod_maintenance_forwards_quarantine_root(tmp_path, monkeypatch) -> None:
    # Given: prune 호출 인자를 포착하는 스텁
    import datetime as dt
    import pathlib

    import src.orchestration.eod as eod_mod
    from src.orchestration.eod import run_eod_maintenance

    seen: dict[str, object] = {}

    def _fake_prune(root, archive_root=None, *, retain_days=3, reference_date=None, quarantine_root=None):
        seen.update({
            'root': root, 'archive_root': archive_root, 'retain_days': retain_days,
            'reference_date': reference_date, 'quarantine_root': quarantine_root,
        })
        return 7

    monkeypatch.setattr(eod_mod, 'prune_old_journals', _fake_prune)

    # When
    deleted = run_eod_maintenance(
        pathlib.Path(tmp_path) / 'l0',
        retain_days=3,
        today=dt.date(2026, 9, 30),
        archive_root=pathlib.Path(tmp_path) / 'l1',
        quarantine_root=pathlib.Path(tmp_path) / 'quarantine',
    )

    # Then: 격리 경로가 보존 계층까지 전달된다
    assert deleted == 7
    assert seen['quarantine_root'] == pathlib.Path(tmp_path) / 'quarantine'
    assert seen['reference_date'] == dt.date(2026, 9, 30)


def test_run_eod_offload_uses_rclone_archiver_by_default(tmp_path, caplog, monkeypatch) -> None:
    import logging

    from src.orchestration.eod import run_eod_offload

    # 이 환경(개발머신)에는 실제 rclone 바이너리+자격증명이 있을 수 있으므로 반드시 hermetic 하게 차단한다
    # (실제 gdrive 원격에 쓰기 시도가 절대 발생하면 안 된다).
    monkeypatch.setattr("src.storage.remote.shutil.which", lambda name: None)

    root = tmp_path / "l1" / "kis" / "H0STCNT0"
    root.mkdir(parents=True)
    old_pq = root / "dt=2026-07-01.parquet"
    old_pq.write_bytes(b"x")

    with caplog.at_level(logging.CRITICAL):
        stats = run_eod_offload(tmp_path / "l1")

    assert stats == {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}
    assert old_pq.exists()
    assert any(r.levelno == logging.CRITICAL for r in caplog.records)


def test_check_session_reconciliation_true_when_bars_store_missing(tmp_path) -> None:
    import datetime as dt

    from src.orchestration.eod import check_session_reconciliation

    ok = check_session_reconciliation(
        bars_store=tmp_path / "bars" / "daily.parquet",
        manifest_path=tmp_path / "manifest" / "2026-09-10.json",
        date=dt.date(2026, 9, 10),
    )

    assert ok is True


def test_check_session_reconciliation_true_when_no_bars_rows_for_date(tmp_path) -> None:
    import datetime as dt

    import polars as pl

    from src.orchestration.eod import check_session_reconciliation

    store = tmp_path / "bars" / "daily.parquet"
    store.parent.mkdir(parents=True)
    pl.DataFrame({
        "date": [dt.date(2026, 9, 9)], "symbol": ["000001"], "close": [1000.0],
        "volume": [1000], "trade_value_100m": [1.0], "daily_change_pct": [0.0],
    }).write_parquet(store)

    ok = check_session_reconciliation(
        bars_store=store, manifest_path=tmp_path / "manifest" / "2026-09-10.json", date=dt.date(2026, 9, 10)
    )

    assert ok is True


def test_check_session_reconciliation_false_when_bars_exist_without_manifest(tmp_path) -> None:
    import datetime as dt

    import polars as pl

    from src.orchestration.eod import check_session_reconciliation

    store = tmp_path / "bars" / "daily.parquet"
    store.parent.mkdir(parents=True)
    pl.DataFrame({
        "date": [dt.date(2026, 9, 10)], "symbol": ["000001"], "close": [1000.0],
        "volume": [1000], "trade_value_100m": [1.0], "daily_change_pct": [0.0],
    }).write_parquet(store)

    ok = check_session_reconciliation(
        bars_store=store, manifest_path=tmp_path / "manifest" / "2026-09-10.json", date=dt.date(2026, 9, 10)
    )

    assert ok is False


def test_check_session_reconciliation_true_when_manifest_present(tmp_path) -> None:
    import datetime as dt

    import polars as pl

    from src.orchestration.eod import check_session_reconciliation

    store = tmp_path / "bars" / "daily.parquet"
    store.parent.mkdir(parents=True)
    pl.DataFrame({
        "date": [dt.date(2026, 9, 10)], "symbol": ["000001"], "close": [1000.0],
        "volume": [1000], "trade_value_100m": [1.0], "daily_change_pct": [0.0],
    }).write_parquet(store)
    manifest_path = tmp_path / "manifest" / "2026-09-10.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text("{}", encoding="utf-8")

    ok = check_session_reconciliation(bars_store=store, manifest_path=manifest_path, date=dt.date(2026, 9, 10))

    assert ok is True
