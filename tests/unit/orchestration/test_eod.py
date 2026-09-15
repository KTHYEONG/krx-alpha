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

    # Then: 검증 게이트 없이 정규화만 수행하고 L0를 보존한다
    assert deleted == 0
    assert part.exists()
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
            from src.storage.remote import SyncStats

            return SyncStats(uploaded=1, skipped_verified=0, failed_verification=0)
        def sync_manifest_tree(self, manifest_root):
            from src.storage.remote import SyncStats

            return SyncStats(uploaded=0, skipped_verified=0, failed_verification=0)
        def remote_files(self, prefix):
            return {'l1/kis/H0STCNT0/dt=2026-07-01.parquet'}
        def remote_file_sizes(self, prefix):
            if prefix == "l1/":
                return {'l1/kis/H0STCNT0/dt=2026-07-01.parquet': 1}
            return {}

    stats = run_eod_offload(tmp_path / 'l1', archiver=_Arc(), reference_date=dt.date(2026, 9, 8))

    assert stats.l1.uploaded == 1
    assert stats.purged == 1
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

    assert stats.l1.uploaded == 0
    assert stats.purged == 0
    assert stats.l1.failed_verification == 0
    assert old_pq.exists()
    assert any(r.levelno == logging.CRITICAL for r in caplog.records)


def test_run_eod_maintenance_forwards_quarantine_root(tmp_path, monkeypatch) -> None:
    # Given: prune 호출 인자를 포착하는 스텁
    import datetime as dt
    import pathlib

    import src.orchestration.eod as eod_mod
    from src.orchestration.eod import run_eod_maintenance
    from src.storage.normalize_worker import run_isolated_normalize

    seen: dict[str, object] = {}

    def _fake_prune(root, archive_root=None, *, retain_days=3, reference_date=None, quarantine_root=None, normalizer=None, verified_remote_l1=None):
        seen.update({
            'root': root, 'archive_root': archive_root, 'retain_days': retain_days,
            'reference_date': reference_date, 'quarantine_root': quarantine_root, 'normalizer': normalizer,
            'verified_remote_l1': verified_remote_l1,
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
        work_root=pathlib.Path(tmp_path) / 'work',
    )

    # Then: 격리 경로 + 자식 프로세스 정규화기가 보존 계층까지 전달된다
    assert deleted == 7
    assert seen['quarantine_root'] == pathlib.Path(tmp_path) / 'quarantine'
    assert seen['reference_date'] == dt.date(2026, 9, 30)
    assert seen['normalizer'].func is run_isolated_normalize
    assert seen['normalizer'].keywords == {'work_root': pathlib.Path(tmp_path) / 'work'}


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

    assert stats.l1.uploaded == 0
    assert stats.purged == 0
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

def test_run_eod_maintenance_retains_partition_when_isolated_worker_is_killed(tmp_path, monkeypatch, caplog) -> None:
    # Given: 자식 정규화 프로세스가 cgroup OOM 으로 SIGKILL 되는 상황
    import datetime as dt
    import logging
    import subprocess

    from src.orchestration.eod import run_eod_maintenance

    part = tmp_path / 'l0' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True)
    (part / '09.jsonl.zst').write_bytes(b'raw')
    calls = []

    def _killed(cmd, **kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, -9, stdout='', stderr=None)

    monkeypatch.setattr(subprocess, 'run', _killed)

    # When
    with caplog.at_level(logging.CRITICAL):
        deleted = run_eod_maintenance(
            tmp_path / 'l0', retain_days=3, today=dt.date(2026, 9, 30),
            archive_root=tmp_path / 'l1', quarantine_root=tmp_path / 'quarantine', work_root=tmp_path / 'work',
        )

    # Then: 부모는 살아서 기록하고 원본을 격리하지 않는다
    assert deleted == 0
    assert len(calls) == 1
    assert (part / '09.jsonl.zst').read_bytes() == b'raw'
    assert (tmp_path / 'quarantine').exists() is False
    assert 'reason=worker_crash' in caplog.text



def test_check_session_reconciliation_flags_gap_union_over_limit_in_regular_session(tmp_path, caplog) -> None:

    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo

    import polars as pl

    from src.orchestration.eod import check_session_reconciliation
    from src.realtime.manifest import SessionManifest

    kst = ZoneInfo("Asia/Seoul")
    day = dt.date(2026, 9, 14)
    store = tmp_path / "bars" / "daily.parquet"
    store.parent.mkdir(parents=True)
    pl.DataFrame({"date": [day], "symbol": ["000001"], "close": [1000.0], "volume": [1], "trade_value_100m": [1.0], "daily_change_pct": [0.0]}).write_parquet(store)
    journal_root = tmp_path / "l0"

    def ns(h, m):
        return int(dt.datetime(2026, 9, 14, h, m, tzinfo=kst).timestamp()) * 1_000_000_000

    def journal(stream):
        part = journal_root / "ls" / stream / "dt=2026-09-14"
        part.mkdir(parents=True, exist_ok=True)
        (part / "10.jsonl.zst").write_bytes(b"x")

    def manifest_with(gaps):
        path = tmp_path / "manifest" / "2026-09-14.json"
        m = SessionManifest(session_date=day, clock_offset_ns=0, started_at_ns=0)
        for symbol, start, end in gaps:
            m.record_gap(symbol=symbol, gap_start_ns=start, gap_end_ns=end, reason="disconnect")
        m.save(path)
        return path

    journal("H0STCNT0")
    journal("H0STASP0")
    path = manifest_with([("005930", ns(9, 10), ns(9, 25)), ("000660", ns(9, 10), ns(9, 25))])

    with caplog.at_level(logging.CRITICAL):
        ok = check_session_reconciliation(bars_store=store, manifest_path=path, date=day, journal_root=journal_root,
                                          streams=("H0STCNT0", "H0STASP0"), vendor="ls")

    assert ok is False
    assert "[DATA] stage=session_reconciliation status=FAIL date=2026-09-14 reasons=gap_exceeded:900s" in caplog.text

def test_check_session_reconciliation_clips_gaps_outside_regular_session(tmp_path) -> None:

    import datetime as dt
    from zoneinfo import ZoneInfo

    import polars as pl

    from src.orchestration.eod import check_session_reconciliation
    from src.realtime.manifest import SessionManifest

    kst = ZoneInfo("Asia/Seoul")
    day = dt.date(2026, 9, 14)
    store = tmp_path / "bars" / "daily.parquet"
    store.parent.mkdir(parents=True)
    pl.DataFrame({"date": [day], "symbol": ["000001"], "close": [1000.0], "volume": [1], "trade_value_100m": [1.0], "daily_change_pct": [0.0]}).write_parquet(store)
    journal_root = tmp_path / "l0"

    def ns(h, m):
        return int(dt.datetime(2026, 9, 14, h, m, tzinfo=kst).timestamp()) * 1_000_000_000

    def journal(stream):
        part = journal_root / "ls" / stream / "dt=2026-09-14"
        part.mkdir(parents=True, exist_ok=True)
        (part / "10.jsonl.zst").write_bytes(b"x")

    def manifest_with(gaps):
        path = tmp_path / "manifest" / "2026-09-14.json"
        m = SessionManifest(session_date=day, clock_offset_ns=0, started_at_ns=0)
        for symbol, start, end in gaps:
            m.record_gap(symbol=symbol, gap_start_ns=start, gap_end_ns=end, reason="disconnect")
        m.save(path)
        return path

    journal("H0STCNT0")
    journal("H0STASP0")
    path = manifest_with([("005930", ns(8, 20), ns(8, 59)), ("005930", ns(15, 31), ns(15, 40)), ("005930", ns(9, 0), ns(9, 5))])

    ok = check_session_reconciliation(bars_store=store, manifest_path=path, date=day, journal_root=journal_root,
                                      streams=("H0STCNT0", "H0STASP0"), vendor="ls")

    assert ok is True

def test_check_session_reconciliation_flags_missing_stream_journal(tmp_path, caplog) -> None:

    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo

    import polars as pl

    from src.orchestration.eod import check_session_reconciliation
    from src.realtime.manifest import SessionManifest

    kst = ZoneInfo("Asia/Seoul")
    day = dt.date(2026, 9, 14)
    store = tmp_path / "bars" / "daily.parquet"
    store.parent.mkdir(parents=True)
    pl.DataFrame({"date": [day], "symbol": ["000001"], "close": [1000.0], "volume": [1], "trade_value_100m": [1.0], "daily_change_pct": [0.0]}).write_parquet(store)
    journal_root = tmp_path / "l0"

    def ns(h, m):
        return int(dt.datetime(2026, 9, 14, h, m, tzinfo=kst).timestamp()) * 1_000_000_000

    def journal(stream):
        part = journal_root / "ls" / stream / "dt=2026-09-14"
        part.mkdir(parents=True, exist_ok=True)
        (part / "10.jsonl.zst").write_bytes(b"x")

    def manifest_with(gaps):
        path = tmp_path / "manifest" / "2026-09-14.json"
        m = SessionManifest(session_date=day, clock_offset_ns=0, started_at_ns=0)
        for symbol, start, end in gaps:
            m.record_gap(symbol=symbol, gap_start_ns=start, gap_end_ns=end, reason="disconnect")
        m.save(path)
        return path

    journal("H0STCNT0")
    path = manifest_with([])

    with caplog.at_level(logging.CRITICAL):
        ok = check_session_reconciliation(bars_store=store, manifest_path=path, date=day, journal_root=journal_root,
                                          streams=("H0STCNT0", "H0STASP0"), vendor="ls")

    assert ok is False
    assert "reasons=journal_missing:H0STASP0" in caplog.text

def test_check_session_reconciliation_flags_unreadable_manifest_in_substantive_mode(tmp_path, caplog) -> None:

    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo

    import polars as pl

    from src.orchestration.eod import check_session_reconciliation
    from src.realtime.manifest import SessionManifest

    kst = ZoneInfo("Asia/Seoul")
    day = dt.date(2026, 9, 14)
    store = tmp_path / "bars" / "daily.parquet"
    store.parent.mkdir(parents=True)
    pl.DataFrame({"date": [day], "symbol": ["000001"], "close": [1000.0], "volume": [1], "trade_value_100m": [1.0], "daily_change_pct": [0.0]}).write_parquet(store)
    journal_root = tmp_path / "l0"

    def ns(h, m):
        return int(dt.datetime(2026, 9, 14, h, m, tzinfo=kst).timestamp()) * 1_000_000_000

    def journal(stream):
        part = journal_root / "ls" / stream / "dt=2026-09-14"
        part.mkdir(parents=True, exist_ok=True)
        (part / "10.jsonl.zst").write_bytes(b"x")

    def manifest_with(gaps):
        path = tmp_path / "manifest" / "2026-09-14.json"
        m = SessionManifest(session_date=day, clock_offset_ns=0, started_at_ns=0)
        for symbol, start, end in gaps:
            m.record_gap(symbol=symbol, gap_start_ns=start, gap_end_ns=end, reason="disconnect")
        m.save(path)
        return path

    journal("H0STCNT0")
    path = tmp_path / "manifest" / "2026-09-14.json"
    path.parent.mkdir(parents=True)
    path.write_text("{}", encoding="utf-8")

    with caplog.at_level(logging.CRITICAL):
        substantive = check_session_reconciliation(bars_store=store, manifest_path=path, date=day, journal_root=journal_root,
                                                   streams=("H0STCNT0",), vendor="ls")
    legacy = check_session_reconciliation(bars_store=store, manifest_path=path, date=day)

    assert substantive is False
    assert "reasons=manifest_unreadable" in caplog.text
    assert legacy is True

def test_classify_remote_failure_detects_auth_expiry() -> None:
    from src.orchestration.eod import classify_remote_failure

    measured = "lsjson failed: l1/ Failed to create file system: couldn't find root directory ID: couldn't fetch token: invalid_grant: maybe token expired?"

    assert classify_remote_failure(measured) == "auth_expired"
    assert classify_remote_failure("lsjson failed: l1/ oauth2: couldn't fetch token") == "auth_expired"
    assert classify_remote_failure("lsjson failed: l1/ dial tcp: i/o timeout") == "remote_error"

def test_check_backup_freshness_lists_local_manifests_missing_remotely(tmp_path) -> None:
    import datetime as dt

    from src.orchestration.eod import check_backup_freshness

    manifest_dir = tmp_path / "manifest"
    manifest_dir.mkdir()
    for name in ("2026-09-11.json", "2026-09-14.json", "2026-09-15.json", "notes.json", "2026-09-14.json.corrupt"):
        (manifest_dir / name).write_text("{}", encoding="utf-8")
    prefixes: list[str] = []

    class _Archiver:
        def remote_files(self, prefix):
            prefixes.append(prefix)
            return {"manifests/2026-09-11.json", "l1/ls/H0STCNT0/dt=2026-09-11.parquet"}

    missing = check_backup_freshness(manifest_dir=manifest_dir, today=dt.date(2026, 9, 15), archiver=_Archiver())

    assert missing == ["2026-09-14.json"]
    assert prefixes == ["manifests/"]

def test_check_backup_freshness_returns_empty_without_rclone(tmp_path, monkeypatch) -> None:
    import datetime as dt

    from src.orchestration.eod import check_backup_freshness

    monkeypatch.setattr("src.storage.remote.RcloneArchiver.try_from_env", staticmethod(lambda: None))
    manifest_dir = tmp_path / "manifest"
    manifest_dir.mkdir()
    (manifest_dir / "2026-09-11.json").write_text("{}", encoding="utf-8")

    assert check_backup_freshness(manifest_dir=manifest_dir, today=dt.date(2026, 9, 15)) == []


def _write_due_journal(root):
    import json
    from pathlib import Path

    import zstandard as zstd

    root = Path(root)
    part = root / "l0" / "kis" / "H0STCNT0" / "dt=2026-09-01"
    part.mkdir(parents=True, exist_ok=True)
    rec = {'raw': 'a', 'recv_mono_ns': 1, 'recv_wall_ns': 2, 'conn_id': 'c1', 'conn_seq': 1, 'vendor': 'kis', 'tr_id': 'H0STCNT0'}
    payload = (json.dumps(rec) + '\n').encode('utf-8')
    (part / '09.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress(payload))
    return part / '09.jsonl.zst'


import pytest


@pytest.fixture
def failing_archiver():
    from src.storage.remote import SyncStats

    class _Failing:
        def sync_l1_tree(self, local_root):
            return SyncStats(uploaded=0, skipped_verified=0, failed_verification=1)

        def sync_manifest_tree(self, manifest_root):
            return SyncStats(uploaded=0, skipped_verified=0, failed_verification=1)

        def remote_file_sizes(self, prefix):
            return {}

    return _Failing()


def _run_normalize_offload_prune(root, archiver):
    import datetime as dt
    from pathlib import Path

    from src.orchestration.eod import run_eod_maintenance, run_eod_offload

    root = Path(root)
    # Step 1: pre-offload normalization without deletion gate.
    run_eod_maintenance(
        root / "l0",
        retain_days=3,
        today=dt.date(2026, 9, 8),
        archive_root=root / "l1",
        verified_remote_l1=None,
    )
    # Step 2: offload with verification (raises on failure; prune never reached).
    run_eod_offload(
        archive_root=root / "l1",
        manifest_root=root / "manifest",
        remote=archiver,
    )
    # Step 3 (unreached on failure): post-offload prune would run here.


def test_eod_offload_verification_failure_retains_l0(tmp_path, failing_archiver):
    import pytest

    from src.storage.remote import RemoteArchiveError

    journal = _write_due_journal(tmp_path)
    with pytest.raises(RemoteArchiveError):
        _run_normalize_offload_prune(tmp_path, failing_archiver)
    assert journal.exists()


def test_aftermarket_eod_ready_requires_both_closed_manifests(tmp_path):
    import datetime as dt
    from zoneinfo import ZoneInfo
    from src.orchestration.eod import aftermarket_eod_ready
    from src.realtime.manifest import SessionManifest
    paths = []
    for venue, session, closed in [('nxt','nxt_after', 1), ('krx','krx_after', None)]:
        path = tmp_path / f'{venue}.json'
        manifest = SessionManifest(session_date=dt.date(2026,9,15), clock_offset_ns=0, started_at_ns=1, venue=venue, session=session, expected_close_ns=1, writer_closed_at_ns=closed)
        manifest.save(path); paths.append(path)  # noqa: E702 - verbatim contract skeleton
    now = dt.datetime(2026,9,15,20,1,tzinfo=ZoneInfo('Asia/Seoul'))
    assert aftermarket_eod_ready(manifests=paths, date=dt.date(2026,9,15), now=now) is False


def test_aftermarket_eod_ready_rejects_empty_accepted_streams(tmp_path):
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.orchestration.eod import aftermarket_eod_ready
    from src.realtime.manifest import SessionManifest

    paths = []
    for venue, session in [('nxt', 'nxt_after'), ('krx', 'krx_after')]:
        path = tmp_path / f'{venue}.json'
        SessionManifest(session_date=dt.date(2026, 9, 15), clock_offset_ns=0, started_at_ns=1, venue=venue, session=session, expected_close_ns=1, writer_closed_at_ns=2).save(path)
        paths.append(path)
    assert aftermarket_eod_ready(manifests=paths, date=dt.date(2026, 9, 15), now=dt.datetime(2026, 9, 15, 20, 1, tzinfo=ZoneInfo('Asia/Seoul'))) is False


def test_aftermarket_eod_ready_rejects_unreadable_manifest(tmp_path, caplog):
    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo

    from src.orchestration.eod import aftermarket_eod_ready

    bad = tmp_path / 'bad.json'
    bad.write_text('{')
    with caplog.at_level(logging.CRITICAL):
        ready = aftermarket_eod_ready(manifests=[bad], date=dt.date(2026, 9, 15), now=dt.datetime(2026, 9, 15, 20, 1, tzinfo=ZoneInfo('Asia/Seoul')))
    assert ready is False
    assert 'eod_readiness' in caplog.text


def test_backup_freshness_recognizes_nested_aftermarket_manifests(tmp_path):
    import datetime as dt
    from src.orchestration.eod import check_backup_freshness
    class Archive:
        def remote_files(self, prefix): return {'manifests/2026-09-14.json'}
    (tmp_path/'aftermarket').mkdir()
    (tmp_path/'2026-09-14.json').write_text('{}')
    (tmp_path/'aftermarket'/'2026-09-14.nxt.json').write_text('{}')
    missing = check_backup_freshness(manifest_dir=tmp_path, today=dt.date(2026,9,15), archiver=Archive())
    assert missing == ['aftermarket/2026-09-14.nxt.json']
