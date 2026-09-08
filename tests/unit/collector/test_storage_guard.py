def test_check_disk_watermark_returns_true_when_abundant(tmp_path) -> None:
    from unittest.mock import patch
    import shutil
    from src.collector.storage_guard import check_disk_watermark

    mock_usage = shutil._ntuple_diskusage(100 * (1024**3), 20 * (1024**3), 80 * (1024**3))
    with patch('shutil.disk_usage', return_value=mock_usage):
        assert check_disk_watermark(tmp_path, min_free_gb=3.0) is True


def test_check_disk_watermark_returns_false_when_depleted(tmp_path) -> None:
    from unittest.mock import patch
    import shutil
    from src.collector.storage_guard import check_disk_watermark

    # 2.5GB free < 3.0GB threshold
    mock_usage = shutil._ntuple_diskusage(50 * (1024**3), 475 * (1024**2), int(2.5 * (1024**3)))
    with patch('shutil.disk_usage', return_value=mock_usage):
        assert check_disk_watermark(tmp_path, min_free_gb=3.0) is False


def test_prune_old_journals_removes_expired_partitions(tmp_path) -> None:
    import datetime as dt
    from src.collector.storage_guard import prune_old_journals

    # Setup: dt=2026-09-01 (old, > 3 days) and dt=2026-09-07 (recent)
    root = tmp_path / 'l0' / 'kis' / 'H0STCNT0'
    old_part = root / 'dt=2026-09-01'
    recent_part = root / 'dt=2026-09-07'
    old_part.mkdir(parents=True, exist_ok=True)
    recent_part.mkdir(parents=True, exist_ok=True)
    (old_part / '09.jsonl.zst').write_text('dummy')
    (recent_part / '09.jsonl.zst').write_text('dummy')

    ref_date = dt.date(2026, 9, 8)
    deleted_count = prune_old_journals(tmp_path / 'l0', retain_days=3, reference_date=ref_date)

    assert deleted_count == 1
    assert not old_part.exists()
    assert recent_part.exists()
