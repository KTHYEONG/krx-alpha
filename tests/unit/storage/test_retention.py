def test_check_disk_watermark_returns_true_when_abundant(tmp_path) -> None:
    from unittest.mock import patch
    import shutil
    from src.storage.retention import check_disk_watermark

    mock_usage = shutil._ntuple_diskusage(100 * (1024**3), 20 * (1024**3), 80 * (1024**3))
    with patch('shutil.disk_usage', return_value=mock_usage):
        assert check_disk_watermark(tmp_path, min_free_gb=3.0) is True


def test_check_disk_watermark_returns_false_when_depleted(tmp_path) -> None:
    from unittest.mock import patch
    import shutil
    from src.storage.retention import check_disk_watermark

    # 2.5GB free < 3.0GB threshold
    mock_usage = shutil._ntuple_diskusage(50 * (1024**3), 475 * (1024**2), int(2.5 * (1024**3)))
    with patch('shutil.disk_usage', return_value=mock_usage):
        assert check_disk_watermark(tmp_path, min_free_gb=3.0) is False


def test_normalize_l0_partition_writes_dedup_sorted_parquet(tmp_path) -> None:
    import json
    import polars as pl
    import zstandard as zstd
    from src.storage.retention import normalize_l0_partition

    # Given: 같은 시간별 파일에 append 된 2개 zstd 프레임 (프레임2에 프레임1 레코드 중복 포함)
    part = tmp_path / 'l0' / 'kis' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True, exist_ok=True)
    rec_a = {'raw': 'a', 'recv_mono_ns': 10, 'recv_wall_ns': 100, 'conn_id': 'c1', 'conn_seq': 1, 'vendor': 'kis', 'tr_id': 'H0STCNT0'}
    rec_b = {'raw': 'b', 'recv_mono_ns': 20, 'recv_wall_ns': 90, 'conn_id': 'c1', 'conn_seq': 2, 'vendor': 'kis', 'tr_id': 'H0STCNT0'}

    def _frame(recs: list[dict]) -> bytes:
        payload = '\n'.join(json.dumps(r) for r in recs) + '\n'
        return zstd.ZstdCompressor(level=3).compress(payload.encode('utf-8'))

    with open(part / '09.jsonl.zst', 'ab') as fh:
        fh.write(_frame([rec_a]))
    with open(part / '09.jsonl.zst', 'ab') as fh:
        fh.write(_frame([rec_b, rec_a]))
    out_path = tmp_path / 'l1' / 'kis' / 'H0STCNT0' / 'dt=2026-09-01.parquet'

    # When
    rows = normalize_l0_partition(part, out_path)

    # Then: 중복 1건 제거되어 2행, recv_wall_ns 오름차순 정렬
    assert rows == 2
    assert out_path.exists()
    df = pl.read_parquet(out_path)
    assert df.height == 2
    assert df['recv_wall_ns'].to_list() == [90, 100]
    assert set(df.columns) == {'raw', 'recv_mono_ns', 'recv_wall_ns', 'conn_id', 'conn_seq', 'vendor', 'tr_id'}


def test_normalize_l0_partition_raises_on_empty_partition(tmp_path) -> None:
    import pytest
    from src.storage.retention import L1NormalizationError, normalize_l0_partition

    # Given: 빈 파티션 디렉터리
    part = tmp_path / 'l0' / 'kis' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True, exist_ok=True)
    out_path = tmp_path / 'l1' / 'kis' / 'H0STCNT0' / 'dt=2026-09-01.parquet'

    # When / Then
    with pytest.raises(L1NormalizationError):
        normalize_l0_partition(part, out_path)
    assert not out_path.exists()
    assert list(out_path.parent.glob('*.tmp')) == []


def test_normalize_l0_partition_raises_on_corrupt_frame(tmp_path) -> None:
    import pytest
    from src.storage.retention import L1NormalizationError, normalize_l0_partition

    # Given: zstd 매직바이트가 아닌 쓰레기 바이트
    part = tmp_path / 'l0' / 'kis' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True, exist_ok=True)
    (part / '09.jsonl.zst').write_bytes(b'not-a-zstd-frame')
    out_path = tmp_path / 'l1' / 'kis' / 'H0STCNT0' / 'dt=2026-09-01.parquet'

    # When / Then
    with pytest.raises(L1NormalizationError):
        normalize_l0_partition(part, out_path)
    assert not out_path.exists()


def test_prune_old_journals_archives_then_deletes_expired(tmp_path) -> None:
    import datetime as dt
    import json
    import zstandard as zstd
    from src.storage.retention import prune_old_journals

    # Given: 만료(2026-09-01) 파티션에 유효 프레임 1개
    part = tmp_path / 'l0' / 'kis' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True, exist_ok=True)
    rec = {'raw': 'a', 'recv_mono_ns': 1, 'recv_wall_ns': 2, 'conn_id': 'c1', 'conn_seq': 1, 'vendor': 'kis', 'tr_id': 'H0STCNT0'}
    payload = (json.dumps(rec) + '\n').encode('utf-8')
    (part / '09.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress(payload))
    archive_root = tmp_path / 'l1'

    # When
    deleted = prune_old_journals(tmp_path / 'l0', retain_days=3, reference_date=dt.date(2026, 9, 8), archive_root=archive_root)

    # Then: 파티션 삭제 + L1 Parquet 생성
    assert deleted == 1
    assert not part.exists()
    assert (archive_root / 'kis' / 'H0STCNT0' / 'dt=2026-09-01.parquet').exists()


def test_prune_old_journals_without_archive_root_deletes_nothing(tmp_path) -> None:
    import datetime as dt
    from src.storage.retention import prune_old_journals

    # Given: 만료 파티션이지만 archive_root 미지정
    part = tmp_path / 'l0' / 'kis' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True, exist_ok=True)
    (part / '09.jsonl.zst').write_text('dummy')

    # When
    deleted = prune_old_journals(tmp_path / 'l0', retain_days=3, reference_date=dt.date(2026, 9, 8))

    # Then: 원본 보존, 삭제 0
    assert deleted == 0
    assert part.exists()


def test_prune_old_journals_retains_partition_when_normalization_fails(tmp_path, caplog) -> None:
    import datetime as dt
    import logging
    from src.storage.retention import prune_old_journals

    # Given: 만료 파티션에 복원 불가한 손상 .zst
    part = tmp_path / 'l0' / 'kis' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True, exist_ok=True)
    (part / '09.jsonl.zst').write_bytes(b'corrupt')
    archive_root = tmp_path / 'l1'

    # When
    with caplog.at_level(logging.CRITICAL):
        deleted = prune_old_journals(tmp_path / 'l0', retain_days=3, reference_date=dt.date(2026, 9, 8), archive_root=archive_root)

    # Then: 파티션 보존, 삭제 0, CRITICAL 로그
    assert deleted == 0
    assert part.exists()
    assert any(r.levelno == logging.CRITICAL for r in caplog.records)


def test_prune_old_journals_keeps_recent_partition(tmp_path) -> None:
    import datetime as dt
    from src.storage.retention import prune_old_journals

    # Given: 최근(2026-09-07) 파티션
    part = tmp_path / 'l0' / 'kis' / 'H0STCNT0' / 'dt=2026-09-07'
    part.mkdir(parents=True, exist_ok=True)
    (part / '09.jsonl.zst').write_text('dummy')
    archive_root = tmp_path / 'l1'

    # When
    deleted = prune_old_journals(tmp_path / 'l0', retain_days=3, reference_date=dt.date(2026, 9, 8), archive_root=archive_root)

    # Then: 보존, L1 미생성
    assert deleted == 0
    assert part.exists()
    assert not (archive_root / 'kis' / 'H0STCNT0' / 'dt=2026-09-07.parquet').exists()


def test_normalize_l0_partition_raises_on_zero_rows(tmp_path, monkeypatch) -> None:
    import json
    import polars as pl
    import pytest
    import zstandard as zstd
    from src.storage.retention import L1NormalizationError, normalize_l0_partition

    # Given: 유효 프레임 1개이나 dedup 후 0행인 빈 프레임으로 판독되는 경우
    part = tmp_path / 'l0' / 'kis' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True, exist_ok=True)
    rec = {'raw': 'a', 'recv_mono_ns': 1, 'recv_wall_ns': 2, 'conn_id': 'c1', 'conn_seq': 1, 'vendor': 'kis', 'tr_id': 'H0STCNT0'}
    (part / '09.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress((json.dumps(rec) + '\n').encode('utf-8')))
    out_path = tmp_path / 'l1' / 'kis' / 'H0STCNT0' / 'dt=2026-09-01.parquet'
    empty = pl.DataFrame({
        'raw': pl.Series([], dtype=pl.String),
        'recv_mono_ns': pl.Series([], dtype=pl.Int64),
        'recv_wall_ns': pl.Series([], dtype=pl.Int64),
        'conn_id': pl.Series([], dtype=pl.String),
        'conn_seq': pl.Series([], dtype=pl.Int64),
        'vendor': pl.Series([], dtype=pl.String),
        'tr_id': pl.Series([], dtype=pl.String),
    })
    monkeypatch.setattr(pl, 'read_ndjson', lambda *a, **k: empty)

    # When / Then: 0행 fail-closed, tmp 미잔존
    with pytest.raises(L1NormalizationError):
        normalize_l0_partition(part, out_path)
    assert not out_path.exists()
    assert list(out_path.parent.glob('*.tmp')) == []


def test_prune_old_journals_retains_when_archive_unverified(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import json
    import logging
    import zstandard as zstd
    import src.storage.retention as sg
    from src.storage.retention import prune_old_journals

    # Given: 만료 파티션이나 정규화가 rows=0 미검증으로 끝나는 경우
    part = tmp_path / 'l0' / 'kis' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True, exist_ok=True)
    rec = {'raw': 'a', 'recv_mono_ns': 1, 'recv_wall_ns': 2, 'conn_id': 'c1', 'conn_seq': 1, 'vendor': 'kis', 'tr_id': 'H0STCNT0'}
    (part / '09.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress((json.dumps(rec) + '\n').encode('utf-8')))
    monkeypatch.setattr(sg, 'normalize_l0_partition', lambda p, o: 0)

    # When
    with caplog.at_level(logging.CRITICAL):
        deleted = prune_old_journals(tmp_path / 'l0', retain_days=3, reference_date=dt.date(2026, 9, 8), archive_root=tmp_path / 'l1')

    # Then: 파티션 보존, 삭제 0, CRITICAL 로그
    assert deleted == 0
    assert part.exists()
    assert any(r.levelno == logging.CRITICAL for r in caplog.records)


def test_prune_local_l1_deletes_old_confirmed_parquet(tmp_path) -> None:
    import datetime as dt
    from src.storage.retention import prune_local_l1

    root = tmp_path / 'l1' / 'kis' / 'H0STCNT0'
    root.mkdir(parents=True)
    old_pq = root / 'dt=2026-07-01.parquet'
    old_pq.write_bytes(b'x')
    confirmed = {'l1/kis/H0STCNT0/dt=2026-07-01.parquet'}

    purged = prune_local_l1(tmp_path / 'l1', retain_days=30, reference_date=dt.date(2026, 9, 8), confirmed_remote=confirmed)

    assert purged == 1
    assert not old_pq.exists()


def test_prune_local_l1_keeps_unconfirmed_parquet(tmp_path) -> None:
    import datetime as dt
    from src.storage.retention import prune_local_l1

    root = tmp_path / 'l1' / 'kis' / 'H0STCNT0'
    root.mkdir(parents=True)
    old_pq = root / 'dt=2026-07-01.parquet'
    old_pq.write_bytes(b'x')

    purged = prune_local_l1(tmp_path / 'l1', retain_days=30, reference_date=dt.date(2026, 9, 8), confirmed_remote=set())

    assert purged == 0
    assert old_pq.exists()


def test_prune_local_l1_without_confirmed_remote_is_noop(tmp_path) -> None:
    import datetime as dt
    from src.storage.retention import prune_local_l1

    root = tmp_path / 'l1' / 'kis' / 'H0STCNT0'
    root.mkdir(parents=True)
    old_pq = root / 'dt=2026-07-01.parquet'
    old_pq.write_bytes(b'x')

    purged = prune_local_l1(tmp_path / 'l1', retain_days=30, reference_date=dt.date(2026, 9, 8))

    assert purged == 0
    assert old_pq.exists()


def test_prune_local_l1_keeps_recent_parquet(tmp_path) -> None:
    import datetime as dt
    from src.storage.retention import prune_local_l1

    root = tmp_path / 'l1' / 'kis' / 'H0STCNT0'
    root.mkdir(parents=True)
    recent_pq = root / 'dt=2026-09-05.parquet'
    recent_pq.write_bytes(b'x')
    confirmed = {'l1/kis/H0STCNT0/dt=2026-09-05.parquet'}

    purged = prune_local_l1(tmp_path / 'l1', retain_days=30, reference_date=dt.date(2026, 9, 8), confirmed_remote=confirmed)

    assert purged == 0
    assert recent_pq.exists()


def test_normalize_l0_partition_logs_tick_quality_summary(tmp_path, caplog) -> None:
    import json
    import logging
    import polars as pl
    import zstandard as zstd
    from src.storage.retention import normalize_l0_partition

    # Given: 체결(H0STCNT0) 파티션에 정상 틱 1건
    part = tmp_path / 'l0' / 'kis' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True, exist_ok=True)
    body = {'shcode': '005930', 'price': '70000', 'cvolume': '10', 'volume': '100', 'change': '0', 'sign': '3'}
    raw = json.dumps({'header': {'tr_cd': 'S3_', 'tr_key': '005930'}, 'body': body}, ensure_ascii=False)
    rec = {'raw': raw, 'recv_mono_ns': 1, 'recv_wall_ns': 100, 'conn_id': 'c1', 'conn_seq': 1, 'vendor': 'kis', 'tr_id': 'H0STCNT0'}
    payload = (json.dumps(rec) + '\n').encode('utf-8')
    (part / '09.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress(payload))
    out_path = tmp_path / 'l1' / 'kis' / 'H0STCNT0' / 'dt=2026-09-01.parquet'

    # When
    with caplog.at_level(logging.INFO):
        rows = normalize_l0_partition(part, out_path)

    # Then: 정상 처리 + 기존 컬럼 스키마 불변 + [DATA] quality 요약 로그
    assert rows == 1
    df = pl.read_parquet(out_path)
    assert set(df.columns) == {'raw', 'recv_mono_ns', 'recv_wall_ns', 'conn_id', 'conn_seq', 'vendor', 'tr_id'}
    assert 'stage=quality' in caplog.text
    assert 'decode_fail=0' in caplog.text
    assert 'status=OK' in caplog.text


def test_normalize_l0_partition_logs_quote_quality_summary(tmp_path, caplog) -> None:
    import json
    import logging
    import polars as pl
    import zstandard as zstd
    from src.storage.retention import normalize_l0_partition

    def _quote_body(hotime, offerho, bidho):
        b = {'shcode': '005930', 'hotime': str(hotime).zfill(6)}
        for k in range(10):
            b[f'offerho{k + 1}'] = str(offerho[k])
            b[f'bidho{k + 1}'] = str(bidho[k])
            b[f'offerrem{k + 1}'] = '100'
            b[f'bidrem{k + 1}'] = '100'
        b['totofferrem'] = '1000'
        b['totbidrem'] = '1000'
        return b

    # Given: 호가(H0STASP0) 파티션에 정상 프레임 1건
    part = tmp_path / 'l0' / 'kis' / 'H0STASP0' / 'dt=2026-09-01'
    part.mkdir(parents=True, exist_ok=True)
    clean_offer = [70100 + k * 100 for k in range(10)]
    clean_bid = [69900 - k * 100 for k in range(10)]
    body = _quote_body(101500, clean_offer, clean_bid)
    raw = json.dumps({'header': {'tr_cd': 'H1_', 'tr_key': '005930'}, 'body': body}, ensure_ascii=False)
    rec = {'raw': raw, 'recv_mono_ns': 1, 'recv_wall_ns': 100, 'conn_id': 'c1', 'conn_seq': 1, 'vendor': 'kis', 'tr_id': 'H0STASP0'}
    payload = (json.dumps(rec) + '\n').encode('utf-8')
    (part / '10.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress(payload))
    out_path = tmp_path / 'l1' / 'kis' / 'H0STASP0' / 'dt=2026-09-01.parquet'

    # When
    with caplog.at_level(logging.INFO):
        rows = normalize_l0_partition(part, out_path)

    # Then: 정상 처리 + 기존 컬럼 스키마 불변 + 호가 전용 [DATA] quality 요약 로그
    assert rows == 1
    df = pl.read_parquet(out_path)
    assert set(df.columns) == {'raw', 'recv_mono_ns', 'recv_wall_ns', 'conn_id', 'conn_seq', 'vendor', 'tr_id'}
    assert 'stage=quality' in caplog.text
    assert 'tr_id=H0STASP0' in caplog.text
    assert 'ladder_disorder=0' in caplog.text
    assert 'status=OK' in caplog.text


def test_normalize_l0_partition_logs_extended_tick_quality_fields(tmp_path, caplog) -> None:
    import json
    import logging
    import zstandard as zstd
    from src.storage.retention import normalize_l0_partition

    # Given: 체결(H0STCNT0) 파티션에 정상 틱 1건 (drate/mdchecnt/mschecnt 포함)
    part = tmp_path / 'l0' / 'kis' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True, exist_ok=True)
    body = {
        'shcode': '005930', 'price': '70000', 'cvolume': '10', 'volume': '100', 'change': '0',
        'sign': '3', 'drate': '0.00', 'mdchecnt': '10', 'mschecnt': '5',
    }
    raw = json.dumps({'header': {'tr_cd': 'S3_', 'tr_key': '005930'}, 'body': body}, ensure_ascii=False)
    rec = {'raw': raw, 'recv_mono_ns': 1, 'recv_wall_ns': 100, 'conn_id': 'c1', 'conn_seq': 1, 'vendor': 'kis', 'tr_id': 'H0STCNT0'}
    payload = (json.dumps(rec) + '\n').encode('utf-8')
    (part / '09.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress(payload))
    out_path = tmp_path / 'l1' / 'kis' / 'H0STCNT0' / 'dt=2026-09-01.parquet'

    # When
    with caplog.at_level(logging.INFO):
        rows = normalize_l0_partition(part, out_path)

    # Then: 기존 필드에 더해 확장 필드도 로그에 포함
    assert rows == 1
    assert 'decode_fail=0' in caplog.text
    assert 'schema_disagree=0' in caplog.text
    assert 'tick_loss=0' in caplog.text
    assert 'lost_volume=0' in caplog.text
    assert 'status=OK' in caplog.text
