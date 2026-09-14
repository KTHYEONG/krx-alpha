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


def test_normalize_l0_partition_raises_on_conn_seq_collision(tmp_path) -> None:
    # Given: 동일 (conn_id, conn_seq) 인데 raw 내용이 다른 레코드 (접속 식별 붕괴 신호)
    import json

    import pytest
    import zstandard as zstd

    from src.storage.retention import L1NormalizationError, normalize_l0_partition

    part = tmp_path / 'l0' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True, exist_ok=True)
    recs = [
        {'raw': '{"v":1}', 'recv_mono_ns': 1, 'recv_wall_ns': 100, 'conn_id': 'ls', 'conn_seq': 1, 'vendor': 'ls', 'tr_id': 'H0STCNT0'},
        {'raw': '{"v":2}', 'recv_mono_ns': 2, 'recv_wall_ns': 200, 'conn_id': 'ls', 'conn_seq': 1, 'vendor': 'ls', 'tr_id': 'H0STCNT0'},
    ]
    payload = ('\n'.join(json.dumps(r) for r in recs) + '\n').encode('utf-8')
    (part / '09.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress(payload))
    out_path = tmp_path / 'l1' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01.parquet'

    # When / Then: 조용한 행 삭제 대신 fail-closed 하고 산출물을 쓰지 않는다
    with pytest.raises(L1NormalizationError, match='conn_seq'):
        normalize_l0_partition(part, out_path)
    assert out_path.exists() is False


def test_normalize_l0_partition_allows_identical_duplicate_resend(tmp_path) -> None:
    # Given: 동일 (conn_id, conn_seq) + 동일 raw 인 정상 벤더 재전송
    import json

    import polars as pl
    import zstandard as zstd

    from src.storage.retention import normalize_l0_partition

    part = tmp_path / 'l0' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True, exist_ok=True)
    rec = {'raw': '{"v":1}', 'recv_mono_ns': 1, 'recv_wall_ns': 100, 'conn_id': 'ls-1', 'conn_seq': 1, 'vendor': 'ls', 'tr_id': 'H0STCNT0'}
    payload = ((json.dumps(rec) + '\n') * 2).encode('utf-8')
    (part / '09.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress(payload))
    out_path = tmp_path / 'l1' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01.parquet'

    # When
    rows = normalize_l0_partition(part, out_path)

    # Then: 값 기반이 아닌 신원 기반 dedup 이므로 정상 재전송은 1행으로 수렴하고 통과한다
    assert rows == 1
    assert pl.read_parquet(out_path).height == 1


def test_normalize_l0_partition_logs_reconciliation_counters(tmp_path, caplog) -> None:
    # Given: 원시 3건 중 1건이 동일 신원 중복인 파티션
    import json
    import logging

    import zstandard as zstd

    from src.storage.retention import normalize_l0_partition

    part = tmp_path / 'l0' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True, exist_ok=True)
    rec_a = {'raw': '{"v":1}', 'recv_mono_ns': 1, 'recv_wall_ns': 100, 'conn_id': 'ls-1', 'conn_seq': 1, 'vendor': 'ls', 'tr_id': 'H0STCNT0'}
    rec_b = {'raw': '{"v":2}', 'recv_mono_ns': 2, 'recv_wall_ns': 200, 'conn_id': 'ls-1', 'conn_seq': 2, 'vendor': 'ls', 'tr_id': 'H0STCNT0'}
    payload = ('\n'.join(json.dumps(r) for r in (rec_a, rec_b, rec_a)) + '\n').encode('utf-8')
    (part / '09.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress(payload))
    out_path = tmp_path / 'l1' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01.parquet'

    # When
    with caplog.at_level(logging.INFO):
        rows = normalize_l0_partition(part, out_path)

    # Then: L0 대비 L1 대조 카운터가 구조화 로그로 남는다
    assert rows == 2
    assert 'stage=normalize' in caplog.text
    assert 'raw_records=3' in caplog.text
    assert 'l1_rows=2' in caplog.text
    assert 'dedup_dropped=1' in caplog.text
    assert 'conn_seq_conflict=0' in caplog.text


def test_prune_old_journals_quarantines_failed_partition(tmp_path, caplog) -> None:
    # Given: 정규화가 실패하는 만료 파티션 + 격리 경로 지정
    import datetime as dt
    import logging

    import src.storage.retention as retention_mod
    from src.storage.retention import L1NormalizationError, prune_old_journals

    part = tmp_path / 'l0' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True, exist_ok=True)
    (part / '09.jsonl.zst').write_bytes(b'broken')

    def _fail(part_dir, out_path):
        raise L1NormalizationError('conn_seq collision')

    original = retention_mod.normalize_l0_partition
    retention_mod.normalize_l0_partition = _fail
    try:
        quarantine = tmp_path / 'quarantine'
        # When
        with caplog.at_level(logging.CRITICAL):
            deleted = prune_old_journals(
                tmp_path / 'l0', archive_root=tmp_path / 'l1', retain_days=3,
                reference_date=dt.date(2026, 9, 30), quarantine_root=quarantine,
            )
    finally:
        retention_mod.normalize_l0_partition = original

    # Then: 삭제 대신 격리 경로로 이동하고 원본이 보존된다
    assert deleted == 0
    assert part.exists() is False
    moved = quarantine / 'ls' / 'H0STCNT0' / 'dt=2026-09-01'
    assert moved.exists()
    assert (moved / '09.jsonl.zst').read_bytes() == b'broken'
    assert 'stage=quarantine' in caplog.text


def test_prune_old_journals_keeps_partition_when_quarantine_destination_exists(tmp_path, caplog) -> None:
    # Given: 격리 목적지에 동일 이름 파티션이 이미 존재
    import datetime as dt
    import logging

    import src.storage.retention as retention_mod
    from src.storage.retention import L1NormalizationError, prune_old_journals

    part = tmp_path / 'l0' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True, exist_ok=True)
    (part / '09.jsonl.zst').write_bytes(b'new')
    existing = tmp_path / 'quarantine' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01'
    existing.mkdir(parents=True, exist_ok=True)
    (existing / '09.jsonl.zst').write_bytes(b'old')

    def _fail(part_dir, out_path):
        raise L1NormalizationError('conn_seq collision')

    original = retention_mod.normalize_l0_partition
    retention_mod.normalize_l0_partition = _fail
    try:
        # When
        with caplog.at_level(logging.CRITICAL):
            deleted = prune_old_journals(
                tmp_path / 'l0', archive_root=tmp_path / 'l1', retain_days=3,
                reference_date=dt.date(2026, 9, 30), quarantine_root=tmp_path / 'quarantine',
            )
    finally:
        retention_mod.normalize_l0_partition = original

    # Then: 기존 격리 증거를 덮어쓰지 않고 원본을 제자리에 보존한다
    assert deleted == 0
    assert part.exists()
    assert (existing / '09.jsonl.zst').read_bytes() == b'old'
    assert 'quarantine_exists' in caplog.text


def test_normalize_l0_partition_uses_chunked_tick_quality_without_output_change(tmp_path, monkeypatch) -> None:
    import json

    import polars as pl
    import zstandard as zstd
    import src.storage.retention as retention_mod

    part = tmp_path / "l0" / "kis" / "H0STCNT0" / "dt=2026-09-01"
    part.mkdir(parents=True)
    records = []
    for seq, wall in ((1, 300), (2, 100), (3, 200)):
        body = {
            "shcode": "005930", "price": "70000", "cvolume": "10",
            "volume": str(seq * 100), "change": "0", "sign": "3",
            "drate": "0.00", "mdchecnt": str(seq), "mschecnt": "0",
        }
        records.append({
            "raw": json.dumps({"header": {"tr_cd": "S3_", "tr_key": "005930"}, "body": body}),
            "recv_mono_ns": wall + 1, "recv_wall_ns": wall, "conn_id": "c1",
            "conn_seq": seq, "vendor": "kis", "tr_id": "H0STCNT0",
        })
    payload = ("\n".join(json.dumps(row) for row in records) + "\n").encode()
    (part / "09.jsonl.zst").write_bytes(zstd.ZstdCompressor(level=3).compress(payload))
    out = tmp_path / "l1" / "kis" / "H0STCNT0" / "dt=2026-09-01.parquet"
    calls = []
    real_decode = retention_mod.decode_tick_raw_fields

    def observed(frame):
        calls.append(frame.height)
        return real_decode(frame)

    monkeypatch.setattr(retention_mod, "decode_tick_raw_fields", observed)
    monkeypatch.setattr(retention_mod, "_GATHER_ROWS", 2)

    row_count = retention_mod.normalize_l0_partition(part, out)

    persisted = pl.read_parquet(out)
    assert calls == [2, 1]
    assert row_count == 3
    assert persisted["recv_wall_ns"].to_list() == [100, 200, 300]
    expected_raw_by_wall = {row["recv_wall_ns"]: row["raw"] for row in records}
    assert persisted["raw"].to_list() == [expected_raw_by_wall[100], expected_raw_by_wall[200], expected_raw_by_wall[300]]
    assert not (out.parent / (out.name + ".tmp")).exists()


def test_normalize_l0_partition_bounded_matches_reference_semantics_across_batches(tmp_path, monkeypatch, caplog) -> None:
    # Given: 시간 파일 2개 + 극소 배치/청크로 spill 다중 파일과 gather 청크 경계를 강제
    import json
    import logging

    import polars as pl
    import zstandard as zstd

    import src.storage.retention as retention_mod

    monkeypatch.setattr(retention_mod, '_BATCH_BYTES', 64)
    monkeypatch.setattr(retention_mod, '_GATHER_ROWS', 2)
    part = tmp_path / 'l0' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True)

    def rec(raw, wall, seq):
        return {'raw': raw, 'recv_mono_ns': wall + 1, 'recv_wall_ns': wall, 'conn_id': 'ls-1', 'conn_seq': seq, 'vendor': 'ls', 'tr_id': 'H0STCNT0'}

    file09 = [rec('r-seq1', 300, 1), rec('r-seq2', 100, 2), rec('r-seq1', 300, 1)]
    file10 = [rec('r-seq3', 100, 3), rec('r-seq4', 50, 4)]
    for name, recs in (('09.jsonl.zst', file09), ('10.jsonl.zst', file10)):
        payload = ('\n'.join(json.dumps(r) for r in recs) + '\n').encode('utf-8')
        (part / name).write_bytes(zstd.ZstdCompressor(level=3).compress(payload))
    out = tmp_path / 'l1' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01.parquet'

    # When
    with caplog.at_level(logging.INFO):
        rows = retention_mod.normalize_l0_partition(part, out, work_root=tmp_path / 'work')

    # Then: 첫 행 보존 dedup, recv_wall_ns 오름차순, 동률은 L0 읽기 순서(seq2 가 seq3 보다 먼저)
    frame = pl.read_parquet(out)
    assert rows == 4
    assert frame['raw'].to_list() == ['r-seq4', 'r-seq2', 'r-seq3', 'r-seq1']
    assert dict(frame.schema) == {
        'raw': pl.String, 'recv_mono_ns': pl.Int64, 'recv_wall_ns': pl.Int64, 'conn_id': pl.String,
        'conn_seq': pl.Int64, 'vendor': pl.String, 'tr_id': pl.String,
    }
    assert 'raw_records=5' in caplog.text
    assert 'l1_rows=4' in caplog.text
    assert 'dedup_dropped=1' in caplog.text
    assert 'conn_seq_conflict=0' in caplog.text
    assert not (out.parent / (out.name + '.tmp')).exists()

def test_normalize_l0_partition_raises_on_null_required_field(tmp_path) -> None:
    # Given: conn_seq 키가 누락된 레코드 (명시 스키마에서는 null 로 조용히 읽힘)
    import json

    import pytest
    import zstandard as zstd

    from src.storage.retention import L1NormalizationError, normalize_l0_partition

    part = tmp_path / 'l0' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True)
    rec = {'raw': 'a', 'recv_mono_ns': 1, 'recv_wall_ns': 2, 'conn_id': 'ls-1', 'vendor': 'ls', 'tr_id': 'H0STCNT0'}
    (part / '09.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress((json.dumps(rec) + '\n').encode('utf-8')))
    out = tmp_path / 'l1' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01.parquet'

    # When / Then: dedup/정렬 키가 null 이면 fail-closed
    with pytest.raises(L1NormalizationError, match='null'):
        normalize_l0_partition(part, out, work_root=tmp_path / 'work')
    assert out.exists() is False
    assert not (out.parent / (out.name + '.tmp')).exists()

def test_normalize_l0_partition_raises_when_hash_equal_duplicate_payload_differs(tmp_path, monkeypatch) -> None:
    # Given: 해시 충돌을 시뮬레이션하도록 _dedup_sort_order 가 서로 다른 raw 를 중복으로 지명
    import json

    import numpy as np
    import pytest
    import zstandard as zstd

    import src.storage.retention as retention_mod
    from src.storage.retention import L1NormalizationError

    part = tmp_path / 'l0' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True)
    recs = [
        {'raw': '{"v":1}', 'recv_mono_ns': 1, 'recv_wall_ns': 100, 'conn_id': 'ls-1', 'conn_seq': 1, 'vendor': 'ls', 'tr_id': 'H0STCNT0'},
        {'raw': '{"v":2}', 'recv_mono_ns': 2, 'recv_wall_ns': 200, 'conn_id': 'ls-1', 'conn_seq': 1, 'vendor': 'ls', 'tr_id': 'H0STCNT0'},
    ]
    payload = ('\n'.join(json.dumps(r) for r in recs) + '\n').encode('utf-8')
    (part / '09.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress(payload))
    out = tmp_path / 'l1' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01.parquet'

    def _forged(keys):
        return np.array([0], dtype=np.int64), 0, np.array([1], dtype=np.int64), np.array([0], dtype=np.int64)

    monkeypatch.setattr(retention_mod, '_dedup_sort_order', _forged)

    # When / Then: 해시만 믿고 다른 raw 를 버리지 않는다
    with pytest.raises(L1NormalizationError, match='conn_seq'):
        retention_mod.normalize_l0_partition(part, out, work_root=tmp_path / 'work')
    assert out.exists() is False

def test_normalize_l0_partition_removes_work_dir_and_tmp_on_failure(tmp_path) -> None:
    # Given: conn_seq 충돌 파티션 + 명시 work_root
    import json

    import pytest
    import zstandard as zstd

    from src.storage.retention import L1NormalizationError, normalize_l0_partition

    part = tmp_path / 'l0' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True)
    recs = [
        {'raw': '{"v":1}', 'recv_mono_ns': 1, 'recv_wall_ns': 100, 'conn_id': 'ls', 'conn_seq': 1, 'vendor': 'ls', 'tr_id': 'H0STCNT0'},
        {'raw': '{"v":2}', 'recv_mono_ns': 2, 'recv_wall_ns': 200, 'conn_id': 'ls', 'conn_seq': 1, 'vendor': 'ls', 'tr_id': 'H0STCNT0'},
    ]
    payload = ('\n'.join(json.dumps(r) for r in recs) + '\n').encode('utf-8')
    (part / '09.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress(payload))
    out = tmp_path / 'l1' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01.parquet'
    work_root = tmp_path / 'work'

    # When
    with pytest.raises(L1NormalizationError, match='conn_seq'):
        normalize_l0_partition(part, out, work_root=work_root)

    # Then: spill 작업 디렉터리와 tmp 산출물이 남지 않는다
    assert not (work_root / 'ls.H0STCNT0.dt=2026-09-01').exists()
    assert out.exists() is False
    assert not (out.parent / (out.name + '.tmp')).exists()

def test_normalize_l0_partition_sweeps_stale_work_dir_and_keeps_spill_outside_archive(tmp_path) -> None:
    # Given: 이전 SIGKILL 시도가 남긴 stale spill 파일
    import json

    import polars as pl
    import zstandard as zstd

    from src.storage.retention import normalize_l0_partition

    part = tmp_path / 'l0' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True)
    recs = [
        {'raw': 'a', 'recv_mono_ns': 1, 'recv_wall_ns': 20, 'conn_id': 'ls-1', 'conn_seq': 1, 'vendor': 'ls', 'tr_id': 'H0STCNT0'},
        {'raw': 'b', 'recv_mono_ns': 2, 'recv_wall_ns': 10, 'conn_id': 'ls-1', 'conn_seq': 2, 'vendor': 'ls', 'tr_id': 'H0STCNT0'},
    ]
    payload = ('\n'.join(json.dumps(r) for r in recs) + '\n').encode('utf-8')
    (part / '09.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress(payload))
    work_root = tmp_path / 'work'
    stale = work_root / 'ls.H0STCNT0.dt=2026-09-01'
    (stale / 'in').mkdir(parents=True)
    (stale / 'in' / '000000.parquet').write_bytes(b'stale')
    out = tmp_path / 'l1' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01.parquet'

    # When
    rows = normalize_l0_partition(part, out, work_root=work_root)

    # Then: stale 무시 + 작업 디렉터리 정리 + archive 트리에는 최종 L1 만 존재
    assert rows == 2
    assert pl.read_parquet(out)['raw'].to_list() == ['b', 'a']
    assert not stale.exists()
    archived = sorted(p.relative_to(tmp_path / 'l1').as_posix() for p in (tmp_path / 'l1').rglob('*.parquet'))
    assert archived == ['ls/H0STCNT0/dt=2026-09-01.parquet']

def test_normalize_l0_partition_default_work_dir_is_temporary_and_removed(tmp_path, monkeypatch) -> None:
    # Given: work_root 미지정 + mkdtemp 를 관측 가능한 경로로 치환
    import json

    import zstandard as zstd

    import src.storage.retention as retention_mod

    created = tmp_path / 'sys-tmp'
    prefixes = []

    def _mkdtemp(prefix):
        prefixes.append(prefix)
        created.mkdir()
        return str(created)

    monkeypatch.setattr(retention_mod.tempfile, 'mkdtemp', _mkdtemp)
    part = tmp_path / 'l0' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True)
    rec = {'raw': 'a', 'recv_mono_ns': 1, 'recv_wall_ns': 2, 'conn_id': 'ls-1', 'conn_seq': 1, 'vendor': 'ls', 'tr_id': 'H0STCNT0'}
    (part / '09.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress((json.dumps(rec) + '\n').encode('utf-8')))
    out = tmp_path / 'l1' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01.parquet'

    # When
    rows = retention_mod.normalize_l0_partition(part, out)

    # Then
    assert rows == 1
    assert prefixes == ['krx-l1-normalize-']
    assert created.exists() is False

def test_iter_line_batches_splits_only_at_newline_and_yields_trailing_line(tmp_path) -> None:
    # Given: 다중 zstd 프레임 + 마지막 줄 개행 없음
    import zstandard as zstd

    from src.storage.retention import _iter_line_batches

    lines = [b'{"a":"' + b'x' * n + b'"}' for n in (3, 40, 7)]
    payload = b'\n'.join(lines)
    path = tmp_path / '09.jsonl.zst'
    cctx = zstd.ZstdCompressor(level=3)
    path.write_bytes(cctx.compress(payload[:20]) + cctx.compress(payload[20:]))

    # When
    batches = list(_iter_line_batches(path, 8))

    # Then: 줄 경계에서만 분할되고 원문이 보존된다
    assert b''.join(batches) == payload
    assert all(batch.endswith(b'\n') for batch in batches[:-1])
    assert batches[-1] == lines[-1]

def test_dedup_sort_order_keeps_first_occurrence_and_breaks_wall_ties_by_read_order() -> None:
    # Given: (b,1) 동일 해시 중복 1건 + recv_wall_ns 동률(행 1, 3)
    import polars as pl

    from src.storage.retention import _dedup_sort_order

    keys = pl.DataFrame({
        'conn_id': ['b', 'a', 'b', 'a'],
        'conn_seq': [1, 1, 1, 2],
        'recv_wall_ns': [5, 3, 5, 3],
        'h1': pl.Series([7, 8, 7, 9], dtype=pl.UInt64),
        'h2': pl.Series([70, 80, 70, 90], dtype=pl.UInt64),
    })

    # When
    order, conflict, dropped, kept = _dedup_sort_order(keys)

    # Then
    assert order.tolist() == [1, 3, 0]
    assert conflict == 0
    assert dropped.tolist() == [2]
    assert kept.tolist() == [0]

def test_dedup_sort_order_counts_groups_with_distinct_payload_hashes() -> None:
    # Given: (a,1) 그룹이 h2 만 다른 두 행
    import polars as pl

    from src.storage.retention import _dedup_sort_order

    keys = pl.DataFrame({
        'conn_id': ['a', 'a', 'a', 'c'],
        'conn_seq': [1, 1, 2, 2],
        'recv_wall_ns': [1, 2, 3, 4],
        'h1': pl.Series([5, 5, 6, 6], dtype=pl.UInt64),
        'h2': pl.Series([1, 2, 3, 3], dtype=pl.UInt64),
    })

    # When
    order, conflict, dropped, kept = _dedup_sort_order(keys)

    # Then
    assert conflict == 1
    assert order.tolist() == [0, 2, 3]

def test_prune_old_journals_retains_partition_without_quarantine_on_worker_crash(tmp_path, caplog) -> None:
    # Given: OOM 등 인프라 원인으로 워커가 죽는 정규화기
    import datetime as dt
    import logging

    from src.storage.retention import L1WorkerCrashError, prune_old_journals

    part = tmp_path / 'l0' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True)
    (part / '09.jsonl.zst').write_bytes(b'kept')
    quarantine = tmp_path / 'quarantine'

    def _crash(part_dir, out_path):
        raise L1WorkerCrashError('normalize worker crashed: returncode=-9')

    # When
    with caplog.at_level(logging.CRITICAL):
        deleted = prune_old_journals(
            tmp_path / 'l0', archive_root=tmp_path / 'l1', retain_days=3,
            reference_date=dt.date(2026, 9, 30), quarantine_root=quarantine, normalizer=_crash,
        )

    # Then: 데이터 결함이 아니므로 격리하지 않고 원위치 보존
    assert deleted == 0
    assert (part / '09.jsonl.zst').read_bytes() == b'kept'
    assert quarantine.exists() is False
    assert 'reason=worker_crash' in caplog.text

def test_prune_old_journals_uses_injected_normalizer(tmp_path) -> None:
    # Given: 산출물을 직접 쓰는 주입 정규화기
    import datetime as dt

    from src.storage.retention import prune_old_journals

    part = tmp_path / 'l0' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True)
    (part / '09.jsonl.zst').write_bytes(b'x')
    seen = []

    def _normalizer(part_dir, out_path):
        seen.append((part_dir, out_path))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b'parquet')
        return 3

    # When
    deleted = prune_old_journals(
        tmp_path / 'l0', archive_root=tmp_path / 'l1', retain_days=3,
        reference_date=dt.date(2026, 9, 30), normalizer=_normalizer,
    )

    # Then
    assert deleted == 1
    assert seen == [(part, tmp_path / 'l1' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01.parquet')]
    assert part.exists() is False

def test_normalize_l0_partition_closes_writer_and_removes_tmp_when_failing_after_writer_open(tmp_path, monkeypatch) -> None:
    # Given: 첫 gather 청크를 쓴 뒤(= writer 열림) 두 번째 청크 DQ 에서 ValueError
    import json

    import pytest
    import zstandard as zstd

    import src.storage.retention as retention_mod
    from src.storage.retention import L1NormalizationError

    monkeypatch.setattr(retention_mod, '_GATHER_ROWS', 1)
    part = tmp_path / 'l0' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True)
    recs = [
        {'raw': 'a', 'recv_mono_ns': 1, 'recv_wall_ns': 10, 'conn_id': 'ls-1', 'conn_seq': 1, 'vendor': 'ls', 'tr_id': 'H0STCNT0'},
        {'raw': 'b', 'recv_mono_ns': 2, 'recv_wall_ns': 20, 'conn_id': 'ls-1', 'conn_seq': 2, 'vendor': 'ls', 'tr_id': 'H0STCNT0'},
    ]
    payload = ('\n'.join(json.dumps(r) for r in recs) + '\n').encode('utf-8')
    (part / '09.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress(payload))
    out = tmp_path / 'l1' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01.parquet'
    work_root = tmp_path / 'work'
    calls = []
    real_quotes = retention_mod.decode_and_flag_quotes

    def _fail_on_second_chunk(chunk):
        calls.append(chunk.height)
        if len(calls) == 2:
            raise ValueError('injected failure after writer open')
        return real_quotes(chunk)

    monkeypatch.setattr(retention_mod, 'decode_and_flag_quotes', _fail_on_second_chunk)

    # When
    with pytest.raises(L1NormalizationError, match='normalize failed'):
        retention_mod.normalize_l0_partition(part, out, work_root=work_root)

    # Then: 열린 writer 정리 + tmp/작업 디렉터리 제거 + 산출물 없음
    assert calls == [1, 1]
    assert out.exists() is False
    assert not (out.parent / (out.name + '.tmp')).exists()
    assert not (work_root / 'ls.H0STCNT0.dt=2026-09-01').exists()
