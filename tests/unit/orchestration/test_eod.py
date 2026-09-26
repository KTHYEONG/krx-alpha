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
        def sync_l1_tree(self, archive_root, *, progress=None):
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

    def _fake_prune(root, archive_root=None, *, retain_days=3, reference_date=None, quarantine_root=None, normalizer=None, verified_remote_l1=None, progress=None, normalize=True):
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


def _write_routed_journal(journal_root, *, vendor="ls", venue="krx", session="regular", stream, date) -> None:
    part = journal_root / vendor / venue / session / stream / f"dt={date.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    (part / "10.jsonl.zst").write_bytes(b"x")


def _write_session_manifest(path, day, gaps=()) -> None:
    from src.realtime.manifest import SessionManifest

    path.parent.mkdir(parents=True, exist_ok=True)
    manifest = SessionManifest(session_date=day, clock_offset_ns=0, started_at_ns=0)
    for symbol, start, end in gaps:
        manifest.record_gap(symbol=symbol, gap_start_ns=start, gap_end_ns=end, reason="disconnect")
    manifest.save(path)


def test_check_session_reconciliation_passes_for_complete_routed_session(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.orchestration.eod import check_session_reconciliation

    day = dt.date(2026, 9, 14)
    journal_root = tmp_path / "l0"
    _write_routed_journal(journal_root, stream="H0STCNT0", date=day)
    _write_routed_journal(journal_root, stream="H0STASP0", date=day)
    manifest_path = tmp_path / "manifest" / "2026-09-14.json"
    _write_session_manifest(manifest_path, day)

    with caplog.at_level(logging.CRITICAL):
        ok = check_session_reconciliation(
            manifest_path=manifest_path,
            date=day,
            journal_root=journal_root,
            streams=("H0STCNT0", "H0STASP0"),
            vendor="ls",
            venue="krx",
            session="regular",
            regular_open=dt.time(9, 0),
            regular_close=dt.time(15, 30),
        )

    assert ok is True
    assert not [r for r in caplog.records if r.levelno == logging.CRITICAL]


def test_check_session_reconciliation_fails_for_missing_stream_journal(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.orchestration.eod import check_session_reconciliation

    day = dt.date(2026, 9, 14)
    journal_root = tmp_path / "l0"
    _write_routed_journal(journal_root, stream="H0STCNT0", date=day)
    manifest_path = tmp_path / "manifest" / "2026-09-14.json"
    _write_session_manifest(manifest_path, day)

    with caplog.at_level(logging.CRITICAL):
        ok = check_session_reconciliation(
            manifest_path=manifest_path,
            date=day,
            journal_root=journal_root,
            streams=("H0STCNT0", "H0STASP0"),
            vendor="ls",
            venue="krx",
            session="regular",
            regular_open=dt.time(9, 0),
            regular_close=dt.time(15, 30),
        )

    assert ok is False
    assert "reasons=journal_missing:H0STASP0" in caplog.text


def test_check_session_reconciliation_rejects_legacy_layout(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.orchestration.eod import check_session_reconciliation

    day = dt.date(2026, 9, 14)
    journal_root = tmp_path / "l0"
    for stream in ("H0STCNT0", "H0STASP0"):
        part = journal_root / "ls" / stream / "dt=2026-09-14"
        part.mkdir(parents=True, exist_ok=True)
        (part / "10.jsonl.zst").write_bytes(b"x")
    manifest_path = tmp_path / "manifest" / "2026-09-14.json"
    _write_session_manifest(manifest_path, day)

    with caplog.at_level(logging.CRITICAL):
        ok = check_session_reconciliation(
            manifest_path=manifest_path,
            date=day,
            journal_root=journal_root,
            streams=("H0STCNT0", "H0STASP0"),
            vendor="ls",
            venue="krx",
            session="regular",
            regular_open=dt.time(9, 0),
            regular_close=dt.time(15, 30),
        )

    assert ok is False
    assert "journal_missing:H0STCNT0" in caplog.text


def test_check_session_reconciliation_fails_for_missing_manifest(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.orchestration.eod import check_session_reconciliation

    day = dt.date(2026, 9, 14)
    journal_root = tmp_path / "l0"
    _write_routed_journal(journal_root, stream="H0STCNT0", date=day)
    _write_routed_journal(journal_root, stream="H0STASP0", date=day)

    with caplog.at_level(logging.CRITICAL):
        ok = check_session_reconciliation(
            manifest_path=tmp_path / "manifest" / "2026-09-14.json",
            date=day,
            journal_root=journal_root,
            streams=("H0STCNT0", "H0STASP0"),
            vendor="ls",
            venue="krx",
            session="regular",
            regular_open=dt.time(9, 0),
            regular_close=dt.time(15, 30),
        )

    assert ok is False
    assert "reasons=manifest_missing" in caplog.text


def test_check_session_reconciliation_fails_for_gap_over_limit(tmp_path, caplog) -> None:
    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo

    from src.orchestration.eod import check_session_reconciliation

    kst = ZoneInfo("Asia/Seoul")
    day = dt.date(2026, 9, 14)
    journal_root = tmp_path / "l0"
    _write_routed_journal(journal_root, stream="H0STCNT0", date=day)
    _write_routed_journal(journal_root, stream="H0STASP0", date=day)
    start_ns = int(dt.datetime(2026, 9, 14, 9, 10, tzinfo=kst).timestamp()) * 1_000_000_000
    manifest_path = tmp_path / "manifest" / "2026-09-14.json"
    _write_session_manifest(manifest_path, day, gaps=[("005930", start_ns, start_ns + 700_000_000_000)])

    with caplog.at_level(logging.CRITICAL):
        ok = check_session_reconciliation(
            manifest_path=manifest_path,
            date=day,
            journal_root=journal_root,
            streams=("H0STCNT0", "H0STASP0"),
            vendor="ls",
            venue="krx",
            session="regular",
            regular_open=dt.time(9, 0),
            regular_close=dt.time(15, 30),
        )

    assert ok is False
    assert "reasons=gap_exceeded:700s" in caplog.text


def test_check_session_reconciliation_ignores_bars_store(tmp_path) -> None:
    import datetime as dt

    from src.orchestration.eod import check_session_reconciliation

    day = dt.date(2026, 9, 14)
    journal_root = tmp_path / "l0"
    _write_routed_journal(journal_root, stream="H0STCNT0", date=day)
    _write_routed_journal(journal_root, stream="H0STASP0", date=day)
    manifest_path = tmp_path / "manifest" / "2026-09-14.json"
    _write_session_manifest(manifest_path, day)

    ok = check_session_reconciliation(
        manifest_path=manifest_path,
        date=day,
        journal_root=journal_root,
        streams=("H0STCNT0", "H0STASP0"),
        vendor="ls",
        venue="krx",
        session="regular",
        regular_open=dt.time(9, 0),
        regular_close=dt.time(15, 30),
    )

    assert (tmp_path / "bars" / "daily.parquet").exists() is False
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

    from src.orchestration.eod import check_session_reconciliation
    from src.realtime.manifest import SessionManifest

    kst = ZoneInfo("Asia/Seoul")
    day = dt.date(2026, 9, 14)
    journal_root = tmp_path / "l0"

    def ns(h, m):
        return int(dt.datetime(2026, 9, 14, h, m, tzinfo=kst).timestamp()) * 1_000_000_000

    def journal(stream):
        part = journal_root / "ls" / "krx" / "regular" / stream / "dt=2026-09-14"
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
        ok = check_session_reconciliation(manifest_path=path, date=day, journal_root=journal_root,
                                          streams=("H0STCNT0", "H0STASP0"), vendor="ls",
                                          venue="krx", session="regular", regular_open=dt.time(9, 0), regular_close=dt.time(15, 30))

    assert ok is False
    assert "[DATA] stage=session_reconciliation status=FAIL date=2026-09-14 reasons=gap_exceeded:900s" in caplog.text

def test_check_session_reconciliation_clips_gaps_outside_regular_session(tmp_path) -> None:

    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.orchestration.eod import check_session_reconciliation
    from src.realtime.manifest import SessionManifest

    kst = ZoneInfo("Asia/Seoul")
    day = dt.date(2026, 9, 14)
    journal_root = tmp_path / "l0"

    def ns(h, m):
        return int(dt.datetime(2026, 9, 14, h, m, tzinfo=kst).timestamp()) * 1_000_000_000

    def journal(stream):
        part = journal_root / "ls" / "krx" / "regular" / stream / "dt=2026-09-14"
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

    ok = check_session_reconciliation(manifest_path=path, date=day, journal_root=journal_root,
                                      streams=("H0STCNT0", "H0STASP0"), vendor="ls",
                                      venue="krx", session="regular", regular_open=dt.time(9, 0), regular_close=dt.time(15, 30))

    assert ok is True

def test_check_session_reconciliation_flags_missing_stream_journal(tmp_path, caplog) -> None:

    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo

    from src.orchestration.eod import check_session_reconciliation
    from src.realtime.manifest import SessionManifest

    kst = ZoneInfo("Asia/Seoul")
    day = dt.date(2026, 9, 14)
    journal_root = tmp_path / "l0"

    def journal(stream):
        part = journal_root / "ls" / "krx" / "regular" / stream / "dt=2026-09-14"
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
        ok = check_session_reconciliation(manifest_path=path, date=day, journal_root=journal_root,
                                          streams=("H0STCNT0", "H0STASP0"), vendor="ls",
                                          venue="krx", session="regular", regular_open=dt.time(9, 0), regular_close=dt.time(15, 30))

    assert ok is False
    assert "reasons=journal_missing:H0STASP0" in caplog.text

def test_check_session_reconciliation_flags_unreadable_manifest_in_substantive_mode(tmp_path, caplog) -> None:

    import datetime as dt
    import logging

    from src.orchestration.eod import check_session_reconciliation

    day = dt.date(2026, 9, 14)
    journal_root = tmp_path / "l0"

    def journal(stream):
        part = journal_root / "ls" / "krx" / "regular" / stream / "dt=2026-09-14"
        part.mkdir(parents=True, exist_ok=True)
        (part / "10.jsonl.zst").write_bytes(b"x")

    journal("H0STCNT0")
    path = tmp_path / "manifest" / "2026-09-14.json"
    path.parent.mkdir(parents=True)
    path.write_text("{}", encoding="utf-8")

    with caplog.at_level(logging.CRITICAL):
        ok = check_session_reconciliation(manifest_path=path, date=day, journal_root=journal_root,
                                          streams=("H0STCNT0",), vendor="ls",
                                          venue="krx", session="regular", regular_open=dt.time(9, 0), regular_close=dt.time(15, 30))

    assert ok is False
    assert "reasons=manifest_unreadable" in caplog.text

def test_check_session_reconciliation_clips_gap_outside_shifted_window(tmp_path) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.orchestration.eod import check_session_reconciliation
    from src.realtime.manifest import SessionManifest

    kst = ZoneInfo("Asia/Seoul")
    day = dt.date(2026, 11, 19)
    journal_root = tmp_path / "l0"

    def journal(stream):
        part = journal_root / "ls" / "krx" / "regular" / stream / "dt=2026-11-19"
        part.mkdir(parents=True, exist_ok=True)
        (part / "10.jsonl.zst").write_bytes(b"x")

    def ns(h, m):
        return int(dt.datetime(2026, 11, 19, h, m, tzinfo=kst).timestamp()) * 1_000_000_000

    journal("H0STCNT0")
    journal("H0STASP0")
    path = tmp_path / "manifest" / "2026-11-19.json"
    manifest = SessionManifest(session_date=day, clock_offset_ns=0, started_at_ns=0)
    manifest.record_gap(symbol="005930", gap_start_ns=ns(9, 0), gap_end_ns=ns(10, 0), reason="disconnect")
    manifest.save(path)

    ok = check_session_reconciliation(manifest_path=path, date=day, journal_root=journal_root,
                                      streams=("H0STCNT0", "H0STASP0"), vendor="ls",
                                      venue="krx", session="regular",
                                      regular_open=dt.time(10, 0), regular_close=dt.time(16, 30))

    assert ok is True


def test_check_session_reconciliation_counts_gap_inside_shifted_window(tmp_path, caplog) -> None:
    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo

    from src.orchestration.eod import check_session_reconciliation
    from src.realtime.manifest import SessionManifest

    kst = ZoneInfo("Asia/Seoul")
    day = dt.date(2026, 11, 19)
    journal_root = tmp_path / "l0"

    def journal(stream):
        part = journal_root / "ls" / "krx" / "regular" / stream / "dt=2026-11-19"
        part.mkdir(parents=True, exist_ok=True)
        (part / "10.jsonl.zst").write_bytes(b"x")

    def ns(h, m):
        return int(dt.datetime(2026, 11, 19, h, m, tzinfo=kst).timestamp()) * 1_000_000_000

    journal("H0STCNT0")
    journal("H0STASP0")
    path = tmp_path / "manifest" / "2026-11-19.json"
    manifest = SessionManifest(session_date=day, clock_offset_ns=0, started_at_ns=0)
    manifest.record_gap(symbol="005930", gap_start_ns=ns(16, 0), gap_end_ns=ns(16, 15), reason="disconnect")
    manifest.save(path)

    with caplog.at_level(logging.CRITICAL):
        ok = check_session_reconciliation(manifest_path=path, date=day, journal_root=journal_root,
                                          streams=("H0STCNT0", "H0STASP0"), vendor="ls",
                                          venue="krx", session="regular",
                                          regular_open=dt.time(10, 0), regular_close=dt.time(16, 30),
                                          max_gap_s=600)

    assert ok is False
    assert "reasons=gap_exceeded:900s" in caplog.text


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
        def sync_l1_tree(self, local_root, *, progress=None):
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


def test_aftermarket_eod_ready_requires_all_shards_and_pairs(tmp_path) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo
    from src.orchestration.eod import aftermarket_eod_ready
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard
    expected=(AftermarketShard(MarketVenue.NXT,0,('005930',),('H0NXCNT0','H0NXASP0'),'1','id1'),)
    assert aftermarket_eod_ready(manifests=[],date=dt.date(2026,9,15),now=dt.datetime(2026,9,15,20,1,tzinfo=ZoneInfo('Asia/Seoul')),expected_shards=expected) is False


def test_aftermarket_eod_ready_verifies_expected_shards(tmp_path) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo
    from src.orchestration.eod import aftermarket_eod_ready
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard
    from src.realtime.manifest import SessionManifest
    day = dt.date(2026, 9, 15)
    now = dt.datetime(2026, 9, 15, 20, 1, tzinfo=ZoneInfo("Asia/Seoul"))
    expected = (AftermarketShard(MarketVenue.NXT, 0, ("005930",), ("H0NXCNT0", "H0NXASP0"), "1", "id1"),)
    counter = 0

    def write_manifest(shard, *, key_id=None, symbols=None, acked=True, closed=True, day_override=None):
        nonlocal counter
        manifest = SessionManifest(
            session_date=day_override or day, clock_offset_ns=0, started_at_ns=1,
            venue=shard.venue.value, session="nxt_after", expected_close_ns=1,
            writer_closed_at_ns=2 if closed else None,
            planned_pairs=[{"symbol": s, "tr_id": t} for s in (symbols if symbols is not None else shard.symbols) for t in shard.streams],
            shard_index=shard.shard_index, credential_key_id=key_id or shard.credential_key_id,
        )
        for symbol in shard.symbols:
            for stream in shard.streams:
                manifest.record_ack(vendor="kis", tr_id=stream, symbol=symbol, rt_cd="0", accepted=acked)
        counter += 1
        path = tmp_path / f"{shard.venue.value}-{shard.shard_index}-{counter}.json"
        manifest.save(path)
        return path

    good = write_manifest(expected[0])
    assert aftermarket_eod_ready(manifests=[good], date=day, now=now, expected_shards=expected) is True
    assert aftermarket_eod_ready(manifests=[write_manifest(expected[0], key_id="other")], date=day, now=now, expected_shards=expected) is False
    assert aftermarket_eod_ready(manifests=[write_manifest(expected[0], day_override=dt.date(2026, 9, 14))], date=day, now=now, expected_shards=expected) is False
    assert aftermarket_eod_ready(manifests=[write_manifest(expected[0], symbols=("000660",))], date=day, now=now, expected_shards=expected) is False
    assert aftermarket_eod_ready(manifests=[write_manifest(expected[0], acked=False)], date=day, now=now, expected_shards=expected) is False
    assert aftermarket_eod_ready(manifests=[write_manifest(expected[0], closed=False)], date=day, now=now, expected_shards=expected) is False
    assert aftermarket_eod_ready(manifests=[], date=day, now=now, expected_shards=expected) is False
    extra = AftermarketShard(MarketVenue.KRX, 0, ("005930",), ("H0STCNT0", "H0STASP0"), "2", "id2")
    assert aftermarket_eod_ready(manifests=[good], date=day, now=now, expected_shards=(*expected, extra)) is False


def test_aftermarket_eod_ready_rejects_wrong_aftermarket_session(tmp_path) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.orchestration.eod import aftermarket_eod_ready
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard
    from src.realtime.manifest import SessionManifest

    shard = AftermarketShard(MarketVenue.NXT, 0, ("005930",), ("H0NXCNT0", "H0NXASP0"), "1", "id1")
    manifest = SessionManifest(session_date=dt.date(2026, 9, 15), clock_offset_ns=0, started_at_ns=1, venue="nxt", session="regular", expected_close_ns=1, writer_closed_at_ns=2, shard_index=0, credential_key_id="id1", planned_pairs=[{"symbol": "005930", "tr_id": stream} for stream in shard.streams])
    for stream in shard.streams:
        manifest.record_ack(vendor="kis", tr_id=stream, symbol="005930", rt_cd="0", accepted=True)
    path = tmp_path / "wrong-session.json"
    manifest.save(path)
    assert aftermarket_eod_ready(manifests=[path], date=dt.date(2026, 9, 15), now=dt.datetime(2026, 9, 15, 20, 1, tzinfo=ZoneInfo("Asia/Seoul")), expected_shards=(shard,)) is False


def test_run_eod_remote_l0_purge_delegates_with_journal_root(tmp_path) -> None:
    import pathlib

    from src.orchestration.eod import run_eod_remote_l0_purge
    from src.storage.remote import PurgeStats

    seen: dict[str, object] = {}

    class _Fake:
        def purge_superseded_l0(self, verified, journal_root, *, progress=None):
            seen["verified"] = set(verified)
            seen["root"] = journal_root
            return PurgeStats(purged=2, skipped_local_present=1, skipped_absent=0, failed=0)

    verified = {"l1/ls/H0STASP0/dt=2026-09-18.parquet"}
    out = run_eod_remote_l0_purge(pathlib.Path(tmp_path) / "l0", verified, archiver=_Fake())

    assert seen["verified"] == verified
    assert seen["root"] == pathlib.Path(tmp_path) / "l0"
    assert out.purged == 2


def test_run_eod_remote_l0_purge_returns_zero_stats_without_archiver(tmp_path, caplog, monkeypatch) -> None:
    import logging
    import pathlib

    from src.orchestration.eod import run_eod_remote_l0_purge
    from src.storage.remote import GDriveArchiver

    monkeypatch.setattr(GDriveArchiver, "try_from_env", staticmethod(lambda: None))

    with caplog.at_level(logging.CRITICAL):
        out = run_eod_remote_l0_purge(pathlib.Path(tmp_path) / "l0", set())

    assert out.purged == 0
    assert out.skipped_local_present == 0
    assert out.skipped_absent == 0
    assert out.failed == 0
    assert "rclone_settings_missing" in caplog.text


def _write_host_status(path, last_ok_at) -> None:
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"last_ok_at": last_ok_at}), encoding="utf-8")


def test_check_host_backup_freshness_passes_when_fresh(tmp_path) -> None:
    import datetime as dt

    from src.orchestration.eod import check_host_backup_freshness

    now = dt.datetime(2026, 9, 24, 20, 0, tzinfo=dt.UTC)
    status = tmp_path / "host_backup_status.json"
    _write_host_status(status, (now - dt.timedelta(hours=20)).isoformat())
    assert (
        check_host_backup_freshness(status_path=status, now=now, max_age=dt.timedelta(hours=36)) is None
    )


def test_check_host_backup_freshness_reports_stale_with_age(tmp_path) -> None:
    import datetime as dt

    from src.orchestration.eod import check_host_backup_freshness

    now = dt.datetime(2026, 9, 24, 20, 0, tzinfo=dt.UTC)
    status = tmp_path / "host_backup_status.json"
    _write_host_status(status, (now - dt.timedelta(hours=40)).isoformat())

    assert check_host_backup_freshness(status_path=status, now=now, max_age=dt.timedelta(hours=36)) == "stale:40.0h"


def test_check_host_backup_freshness_reports_missing(tmp_path) -> None:
    import datetime as dt

    from src.orchestration.eod import check_host_backup_freshness

    now = dt.datetime(2026, 9, 24, 20, 0, tzinfo=dt.UTC)

    assert (
        check_host_backup_freshness(
            status_path=tmp_path / "host_backup_status.json", now=now, max_age=dt.timedelta(hours=36)
        )
        == "missing"
    )


def test_check_host_backup_freshness_reports_unreadable(tmp_path) -> None:
    import datetime as dt

    from src.orchestration.eod import check_host_backup_freshness

    now = dt.datetime(2026, 9, 24, 20, 0, tzinfo=dt.UTC)
    status = tmp_path / "host_backup_status.json"
    status.write_text("not-json{", encoding="utf-8")

    assert check_host_backup_freshness(status_path=status, now=now, max_age=dt.timedelta(hours=36)) == "unreadable"

    _write_host_status(status, "tomorrow-ish")
    assert check_host_backup_freshness(status_path=status, now=now, max_age=dt.timedelta(hours=36)) == "unreadable"

    _write_host_status(status, 12345)
    assert check_host_backup_freshness(status_path=status, now=now, max_age=dt.timedelta(hours=36)) == "unreadable"

    _write_host_status(status, "2026-09-24T20:00:00")
    assert check_host_backup_freshness(status_path=status, now=now, max_age=dt.timedelta(hours=36)) == "unreadable"


def test_check_host_backup_freshness_reports_never_succeeded(tmp_path) -> None:
    import datetime as dt

    from src.orchestration.eod import check_host_backup_freshness

    now = dt.datetime(2026, 9, 24, 20, 0, tzinfo=dt.UTC)
    status = tmp_path / "host_backup_status.json"
    _write_host_status(status, None)

    assert (
        check_host_backup_freshness(status_path=status, now=now, max_age=dt.timedelta(hours=36))
        == "never_succeeded"
    )


def test_check_host_backup_freshness_rejects_naive_now(tmp_path) -> None:
    import datetime as dt

    import pytest

    from src.orchestration.eod import check_host_backup_freshness

    with pytest.raises(ValueError, match="timezone-aware"):
        check_host_backup_freshness(
            status_path=tmp_path / "host_backup_status.json",
            now=dt.datetime(2026, 9, 24, 20, 0),
            max_age=dt.timedelta(hours=36),
        )


def test_check_host_backup_freshness_treats_future_last_ok_as_fresh(tmp_path) -> None:
    import datetime as dt

    from src.orchestration.eod import check_host_backup_freshness

    now = dt.datetime(2026, 9, 24, 20, 0, tzinfo=dt.UTC)
    status = tmp_path / "host_backup_status.json"
    _write_host_status(status, (now + dt.timedelta(hours=1)).isoformat())

    assert check_host_backup_freshness(status_path=status, now=now, max_age=dt.timedelta(hours=36)) is None


def test_run_eod_offload_rclone_timeout_follows_failure_path(tmp_path) -> None:
    import datetime as dt
    import subprocess

    import pytest

    from src.orchestration.eod import run_eod_offload
    from src.storage.remote import GDriveArchiver, RemoteArchiveError

    root = tmp_path / "l1" / "ls" / "H0STCNT0"
    root.mkdir(parents=True)
    (root / "dt=2026-09-01.parquet").write_bytes(b"0123456789")

    def _behavior(mode):
        def _runner(cmd, **kwargs):
            if "copyto" in cmd:
                if mode == "timeout":
                    raise subprocess.TimeoutExpired(cmd, 600)
                return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="boom")
            return subprocess.CompletedProcess(cmd, 0, stdout="[]", stderr="")

        return _runner

    def _archiver(mode):
        return GDriveArchiver(remote_name="gdrive", remote_path="q", runner=_behavior(mode))

    for mode in ("timeout", "rc"):
        with pytest.raises(RemoteArchiveError, match="offload verification failed"):
            run_eod_offload(tmp_path / "l1", archiver=_archiver(mode), reference_date=dt.date(2026, 9, 8))

    timeout_stats = _archiver("timeout").sync_l1_tree(root)
    rc_stats = _archiver("rc").sync_l1_tree(root)
    assert (timeout_stats.uploaded, timeout_stats.failed_verification) == (0, 1)
    assert (rc_stats.uploaded, rc_stats.failed_verification) == (0, 1)


def test_check_session_reconciliation_fails_for_accumulated_restart_gaps(tmp_path, caplog) -> None:
    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo

    from src.orchestration.eod import check_session_reconciliation

    kst = ZoneInfo("Asia/Seoul")
    day = dt.date(2026, 9, 14)
    journal_root = tmp_path / "l0"
    _write_routed_journal(journal_root, stream="H0STCNT0", date=day)
    _write_routed_journal(journal_root, stream="H0STASP0", date=day)
    base_ns = int(dt.datetime(2026, 9, 14, 9, 10, tzinfo=kst).timestamp()) * 1_000_000_000
    gaps = [("*", base_ns + i * 1_000_000_000_000, base_ns + i * 1_000_000_000_000 + 90_000_000_000) for i in range(8)]
    manifest_path = tmp_path / "manifest" / "2026-09-14.json"
    _write_session_manifest(manifest_path, day, gaps=gaps)

    with caplog.at_level(logging.CRITICAL):
        ok = check_session_reconciliation(
            manifest_path=manifest_path,
            date=day,
            journal_root=journal_root,
            streams=("H0STCNT0", "H0STASP0"),
            vendor="ls",
            venue="krx",
            session="regular",
            regular_open=dt.time(9, 0),
            regular_close=dt.time(15, 30),
        )

    assert ok is False
    assert "gap_exceeded:720s" in caplog.text


def test_offload_and_purge_report_progress_for_every_rclone_call(tmp_path) -> None:
    import datetime as dt
    import pathlib
    from types import SimpleNamespace

    from src.orchestration.eod import run_eod_offload, run_eod_remote_l0_purge
    from src.storage.remote import RcloneArchiver, RemoteArchiveError

    # Given: 업로드 대상 L1 1개, manifest 1개, 원격 검증된 L0 1개
    l1 = pathlib.Path(tmp_path) / "l1" / "ls" / "H0STCNT0" / "dt=2026-09-01.parquet"
    l1.parent.mkdir(parents=True)
    l1.write_bytes(b"x" * 10)
    manifest = pathlib.Path(tmp_path) / "manifest" / "2026-09-01.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text("{}", encoding="utf-8")
    calls: list[list[str]] = []

    def _runner(args, **kwargs):
        calls.append(args)
        return SimpleNamespace(returncode=0, stdout="[]", stderr="")

    arc = RcloneArchiver(remote_name="gdrive", remote_path="p", runner=_runner)
    progress: list[int] = []

    # When: EOD 오프로드와 원격 L0 정리를 진행 콜백과 함께 실행
    # 가짜 원격은 빈 목록을 돌려주므로 검증 실패로 끝난다; 관심사는 실패 경로를 포함한 호출별 진행 신호다.
    with pytest.raises(RemoteArchiveError, match="verif"):
        run_eod_offload(
            pathlib.Path(tmp_path) / "l1",
            pathlib.Path(tmp_path) / "manifest",
            archiver=arc,
            reference_date=dt.date(2026, 9, 8),
            progress=lambda: progress.append(1),
        )
    offload_calls = len(calls)
    run_eod_remote_l0_purge(
        pathlib.Path(tmp_path) / "l0",
        {"l1/ls/H0STCNT0/dt=2026-09-01.parquet"},
        archiver=arc,
        progress=lambda: progress.append(1),
    )

    # Then: 오프로드·정리의 모든 rclone 호출이 진행 신호를 남겨 EOD 무핑 구간이 호출 1회로 제한된다
    assert offload_calls > 0
    assert len(calls) > offload_calls
    assert len(progress) >= len(calls)


def test_aftermarket_eod_ready_waits_for_day_after_market_end(tmp_path):
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.orchestration.eod import aftermarket_eod_ready
    from src.realtime.manifest import SessionManifest

    paths = []
    for venue, session, streams in [
        ("nxt", "nxt_after", ("H0NXCNT0", "H0NXASP0")),
        ("krx", "krx_after", ("H0STCNT0", "H0STASP0")),
    ]:
        manifest = SessionManifest(
            session_date=dt.date(2026, 9, 15), clock_offset_ns=0, started_at_ns=1, venue=venue,
            session=session, expected_close_ns=1, writer_closed_at_ns=2,
        )
        for stream in streams:
            manifest.record_ack(vendor="kis", tr_id=stream, symbol="005930", rt_cd="0", accepted=True)
        path = tmp_path / f"{venue}.json"
        manifest.save(path)
        paths.append(path)
    now = dt.datetime(2026, 9, 15, 19, 5, tzinfo=ZoneInfo("Asia/Seoul"))

    # 20:00 하드코딩이 아니라 그날 앵커의 애프터마켓 종료를 기준으로 판정한다.
    assert aftermarket_eod_ready(manifests=paths, date=dt.date(2026, 9, 15), now=now) is False
    assert aftermarket_eod_ready(
        manifests=paths, date=dt.date(2026, 9, 15), now=now, after_market_end=dt.time(19, 0)
    ) is True
