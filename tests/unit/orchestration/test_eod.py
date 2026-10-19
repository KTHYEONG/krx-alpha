def test_run_eod_maintenance_threads_archive_root(tmp_path) -> None:
    import datetime as dt
    import json
    import zstandard as zstd
    from src.orchestration.eod import run_eod_maintenance

    # Given: 만료 파티션에 유효 프레임
    part = tmp_path / "l0" / "kis" / "H0STCNT0" / "dt=2026-09-01"
    part.mkdir(parents=True, exist_ok=True)
    rec = {
        "raw": "a",
        "recv_mono_ns": 1,
        "recv_wall_ns": 2,
        "conn_id": "c1",
        "conn_seq": 1,
        "vendor": "kis",
        "tr_id": "H0STCNT0",
    }
    (part / "09.jsonl.zst").write_bytes(zstd.ZstdCompressor(level=3).compress((json.dumps(rec) + "\n").encode("utf-8")))
    archive_root = tmp_path / "l1"

    # When
    deleted = run_eod_maintenance(tmp_path / "l0", retain_days=3, today=dt.date(2026, 9, 8), archive_root=archive_root)

    # Then: 검증 게이트 없이 정규화만 수행하고 L0를 보존한다
    assert deleted == 0
    assert part.exists()
    assert (archive_root / "kis" / "H0STCNT0" / "dt=2026-09-01.parquet").exists()


def test_run_eod_maintenance_invokes_prune(tmp_path) -> None:
    import datetime as dt
    from src.orchestration.eod import run_eod_maintenance

    # Given: 만료 + 최근 파티션, archive_root 미지정
    root = tmp_path / "l0" / "kis" / "H0STCNT0"
    old_part = root / "dt=2026-09-01"
    recent_part = root / "dt=2026-09-07"
    old_part.mkdir(parents=True, exist_ok=True)
    recent_part.mkdir(parents=True, exist_ok=True)
    (old_part / "09.jsonl.zst").write_text("dummy")
    (recent_part / "09.jsonl.zst").write_text("dummy")

    # When
    deleted = run_eod_maintenance(tmp_path / "l0", retain_days=3, today=dt.date(2026, 9, 8))

    # Then: offload 대상 없음 -> 삭제 0, 원본 보존
    assert deleted == 0
    assert old_part.exists()
    assert recent_part.exists()


def test_run_eod_offload_syncs_and_prunes_with_injected_archiver(tmp_path) -> None:
    import datetime as dt
    from src.orchestration.eod import run_eod_offload

    root = tmp_path / "l1" / "kis" / "H0STCNT0"
    root.mkdir(parents=True)
    old_pq = root / "dt=2026-07-01.parquet"
    old_pq.write_bytes(b"x")

    class _Arc:
        def sync_l1_tree(self, archive_root, *, progress=None):
            from src.storage.remote import SyncStats

            return SyncStats(uploaded=1, skipped_verified=0, failed_verification=0)

        def sync_manifest_tree(self, manifest_root):
            from src.storage.remote import SyncStats

            return SyncStats(uploaded=0, skipped_verified=0, failed_verification=0)

        def remote_files(self, prefix):
            return {"l1/kis/H0STCNT0/dt=2026-07-01.parquet"}

        def remote_file_sizes(self, prefix):
            if prefix == "l1/":
                return {"l1/kis/H0STCNT0/dt=2026-07-01.parquet": 1}
            return {}

    stats = run_eod_offload(tmp_path / "l1", archiver=_Arc(), reference_date=dt.date(2026, 9, 8))

    assert stats.l1.uploaded == 1
    assert stats.purged == 1
    assert not old_pq.exists()


def test_run_eod_offload_returns_zeros_when_archiver_missing(tmp_path, caplog, monkeypatch) -> None:
    import logging
    from src.orchestration.eod import run_eod_offload

    root = tmp_path / "l1" / "kis" / "H0STCNT0"
    root.mkdir(parents=True)
    old_pq = root / "dt=2026-07-01.parquet"
    old_pq.write_bytes(b"x")

    monkeypatch.setattr("src.storage.remote.RcloneArchiver.try_from_env", staticmethod(lambda: None))

    with caplog.at_level(logging.CRITICAL):
        stats = run_eod_offload(tmp_path / "l1")

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

    def _fake_prune(
        root,
        archive_root=None,
        *,
        retain_days=3,
        reference_date=None,
        quarantine_root=None,
        normalizer=None,
        verified_remote_l1=None,
        progress=None,
        normalize=True,
        reuse_fresh_l1=False,
    ):
        seen.update(
            {
                "root": root,
                "archive_root": archive_root,
                "retain_days": retain_days,
                "reference_date": reference_date,
                "quarantine_root": quarantine_root,
                "normalizer": normalizer,
                "verified_remote_l1": verified_remote_l1,
                "reuse_fresh_l1": reuse_fresh_l1,
            }
        )
        return 7

    monkeypatch.setattr(eod_mod, "prune_old_journals", _fake_prune)

    # When
    deleted = run_eod_maintenance(
        pathlib.Path(tmp_path) / "l0",
        retain_days=3,
        today=dt.date(2026, 9, 30),
        archive_root=pathlib.Path(tmp_path) / "l1",
        quarantine_root=pathlib.Path(tmp_path) / "quarantine",
        work_root=pathlib.Path(tmp_path) / "work",
        reuse_fresh_l1=True,
    )

    # Then: 격리 경로 + 자식 프로세스 정규화기가 보존 계층까지 전달된다
    assert deleted == 7
    assert seen["quarantine_root"] == pathlib.Path(tmp_path) / "quarantine"
    assert seen["reference_date"] == dt.date(2026, 9, 30)
    assert seen["reuse_fresh_l1"] is True
    assert seen["normalizer"].func is run_isolated_normalize
    assert seen["normalizer"].keywords == {"work_root": pathlib.Path(tmp_path) / "work"}


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

    part = tmp_path / "l0" / "ls" / "H0STCNT0" / "dt=2026-09-01"
    part.mkdir(parents=True)
    (part / "09.jsonl.zst").write_bytes(b"raw")
    calls = []

    def _killed(cmd, **kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, -9, stdout="", stderr=None)

    monkeypatch.setattr(subprocess, "run", _killed)

    # When
    with caplog.at_level(logging.CRITICAL):
        deleted = run_eod_maintenance(
            tmp_path / "l0",
            retain_days=3,
            today=dt.date(2026, 9, 30),
            archive_root=tmp_path / "l1",
            quarantine_root=tmp_path / "quarantine",
            work_root=tmp_path / "work",
        )

    # Then: 부모는 살아서 기록하고 원본을 격리하지 않는다
    assert deleted == 0
    assert len(calls) == 1
    assert (part / "09.jsonl.zst").read_bytes() == b"raw"
    assert (tmp_path / "quarantine").exists() is False
    assert "reason=worker_crash" in caplog.text


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
        ok = check_session_reconciliation(
            manifest_path=path,
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
    path = manifest_with(
        [("005930", ns(8, 20), ns(8, 59)), ("005930", ns(15, 31), ns(15, 40)), ("005930", ns(9, 0), ns(9, 5))]
    )

    ok = check_session_reconciliation(
        manifest_path=path,
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
        ok = check_session_reconciliation(
            manifest_path=path,
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
        ok = check_session_reconciliation(
            manifest_path=path,
            date=day,
            journal_root=journal_root,
            streams=("H0STCNT0",),
            vendor="ls",
            venue="krx",
            session="regular",
            regular_open=dt.time(9, 0),
            regular_close=dt.time(15, 30),
        )

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

    ok = check_session_reconciliation(
        manifest_path=path,
        date=day,
        journal_root=journal_root,
        streams=("H0STCNT0", "H0STASP0"),
        vendor="ls",
        venue="krx",
        session="regular",
        regular_open=dt.time(10, 0),
        regular_close=dt.time(16, 30),
    )

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
        ok = check_session_reconciliation(
            manifest_path=path,
            date=day,
            journal_root=journal_root,
            streams=("H0STCNT0", "H0STASP0"),
            vendor="ls",
            venue="krx",
            session="regular",
            regular_open=dt.time(10, 0),
            regular_close=dt.time(16, 30),
            max_gap_s=600,
        )

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
    rec = {
        "raw": "a",
        "recv_mono_ns": 1,
        "recv_wall_ns": 2,
        "conn_id": "c1",
        "conn_seq": 1,
        "vendor": "kis",
        "tr_id": "H0STCNT0",
    }
    payload = (json.dumps(rec) + "\n").encode("utf-8")
    (part / "09.jsonl.zst").write_bytes(zstd.ZstdCompressor(level=3).compress(payload))
    return part / "09.jsonl.zst"


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
    for venue, session, closed in [("nxt", "nxt_after", 1), ("krx", "krx_after", None)]:
        path = tmp_path / f"{venue}.json"
        manifest = SessionManifest(
            session_date=dt.date(2026, 9, 15),
            clock_offset_ns=0,
            started_at_ns=1,
            venue=venue,
            session=session,
            expected_close_ns=1,
            writer_closed_at_ns=closed,
        )
        manifest.save(path)
        paths.append(path)  # noqa: E702 - verbatim contract skeleton
    now = dt.datetime(2026, 9, 15, 20, 1, tzinfo=ZoneInfo("Asia/Seoul"))
    assert aftermarket_eod_ready(manifests=paths, date=dt.date(2026, 9, 15), now=now) is False


def test_aftermarket_eod_ready_rejects_empty_accepted_streams(tmp_path):
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.orchestration.eod import aftermarket_eod_ready
    from src.realtime.manifest import SessionManifest

    paths = []
    for venue, session in [("nxt", "nxt_after"), ("krx", "krx_after")]:
        path = tmp_path / f"{venue}.json"
        SessionManifest(
            session_date=dt.date(2026, 9, 15),
            clock_offset_ns=0,
            started_at_ns=1,
            venue=venue,
            session=session,
            expected_close_ns=1,
            writer_closed_at_ns=2,
        ).save(path)
        paths.append(path)
    assert (
        aftermarket_eod_ready(
            manifests=paths,
            date=dt.date(2026, 9, 15),
            now=dt.datetime(2026, 9, 15, 20, 1, tzinfo=ZoneInfo("Asia/Seoul")),
        )
        is False
    )


def test_aftermarket_eod_ready_rejects_unreadable_manifest(tmp_path, caplog):
    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo

    from src.orchestration.eod import aftermarket_eod_ready

    bad = tmp_path / "bad.json"
    bad.write_text("{")
    with caplog.at_level(logging.CRITICAL):
        ready = aftermarket_eod_ready(
            manifests=[bad],
            date=dt.date(2026, 9, 15),
            now=dt.datetime(2026, 9, 15, 20, 1, tzinfo=ZoneInfo("Asia/Seoul")),
        )
    assert ready is False
    assert "eod_readiness" in caplog.text


def test_backup_freshness_recognizes_nested_aftermarket_manifests(tmp_path):
    import datetime as dt
    from src.orchestration.eod import check_backup_freshness

    class Archive:
        def remote_files(self, prefix):
            return {"manifests/2026-09-14.json"}

    (tmp_path / "aftermarket").mkdir()
    (tmp_path / "2026-09-14.json").write_text("{}")
    (tmp_path / "aftermarket" / "2026-09-14.nxt.json").write_text("{}")
    missing = check_backup_freshness(manifest_dir=tmp_path, today=dt.date(2026, 9, 15), archiver=Archive())
    assert missing == ["aftermarket/2026-09-14.nxt.json"]


def test_aftermarket_eod_ready_requires_all_shards_and_pairs(tmp_path) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo
    from src.orchestration.eod import aftermarket_eod_ready
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard

    expected = (AftermarketShard(MarketVenue.NXT, 0, ("005930",), ("H0NXCNT0", "H0NXASP0"), "1", "id1"),)
    assert (
        aftermarket_eod_ready(
            manifests=[],
            date=dt.date(2026, 9, 15),
            now=dt.datetime(2026, 9, 15, 20, 1, tzinfo=ZoneInfo("Asia/Seoul")),
            expected_shards=expected,
        )
        is False
    )


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
            session_date=day_override or day,
            clock_offset_ns=0,
            started_at_ns=1,
            venue=shard.venue.value,
            session="nxt_after",
            expected_close_ns=1,
            writer_closed_at_ns=2 if closed else None,
            planned_pairs=[
                {"symbol": s, "tr_id": t}
                for s in (symbols if symbols is not None else shard.symbols)
                for t in shard.streams
            ],
            shard_index=shard.shard_index,
            credential_key_id=key_id or shard.credential_key_id,
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
    assert (
        aftermarket_eod_ready(
            manifests=[write_manifest(expected[0], key_id="other")], date=day, now=now, expected_shards=expected
        )
        is False
    )
    assert (
        aftermarket_eod_ready(
            manifests=[write_manifest(expected[0], day_override=dt.date(2026, 9, 14))],
            date=day,
            now=now,
            expected_shards=expected,
        )
        is False
    )
    assert (
        aftermarket_eod_ready(
            manifests=[write_manifest(expected[0], symbols=("000660",))], date=day, now=now, expected_shards=expected
        )
        is False
    )
    assert (
        aftermarket_eod_ready(
            manifests=[write_manifest(expected[0], acked=False)], date=day, now=now, expected_shards=expected
        )
        is False
    )
    assert (
        aftermarket_eod_ready(
            manifests=[write_manifest(expected[0], closed=False)], date=day, now=now, expected_shards=expected
        )
        is False
    )
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
    manifest = SessionManifest(
        session_date=dt.date(2026, 9, 15),
        clock_offset_ns=0,
        started_at_ns=1,
        venue="nxt",
        session="regular",
        expected_close_ns=1,
        writer_closed_at_ns=2,
        shard_index=0,
        credential_key_id="id1",
        planned_pairs=[{"symbol": "005930", "tr_id": stream} for stream in shard.streams],
    )
    for stream in shard.streams:
        manifest.record_ack(vendor="kis", tr_id=stream, symbol="005930", rt_cd="0", accepted=True)
    path = tmp_path / "wrong-session.json"
    manifest.save(path)
    assert (
        aftermarket_eod_ready(
            manifests=[path],
            date=dt.date(2026, 9, 15),
            now=dt.datetime(2026, 9, 15, 20, 1, tzinfo=ZoneInfo("Asia/Seoul")),
            expected_shards=(shard,),
        )
        is False
    )


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
    assert check_host_backup_freshness(status_path=status, now=now, max_age=dt.timedelta(hours=36)) is None


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

    assert check_host_backup_freshness(status_path=status, now=now, max_age=dt.timedelta(hours=36)) == "never_succeeded"


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
            session_date=dt.date(2026, 9, 15),
            clock_offset_ns=0,
            started_at_ns=1,
            venue=venue,
            session=session,
            expected_close_ns=1,
            writer_closed_at_ns=2,
        )
        for stream in streams:
            manifest.record_ack(vendor="kis", tr_id=stream, symbol="005930", rt_cd="0", accepted=True)
        path = tmp_path / f"{venue}.json"
        manifest.save(path)
        paths.append(path)
    now = dt.datetime(2026, 9, 15, 19, 5, tzinfo=ZoneInfo("Asia/Seoul"))

    # 20:00 하드코딩이 아니라 그날 앵커의 애프터마켓 종료를 기준으로 판정한다.
    assert aftermarket_eod_ready(manifests=paths, date=dt.date(2026, 9, 15), now=now) is False
    assert (
        aftermarket_eod_ready(manifests=paths, date=dt.date(2026, 9, 15), now=now, after_market_end=dt.time(19, 0))
        is True
    )


@pytest.mark.parametrize("payload", ["[]", "null", '{"session_date":"2026-10-18","clock_offset_ns":0,"started_at_ns":Infinity}'])
def test_prearm_invalid_manifest_shape_never_aborts_other_shards(tmp_path, payload) -> None:
    import datetime as dt

    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.eod import check_aftermarket_prearm

    day = dt.date(2026, 10, 18)
    path = tmp_path / "bad.json"
    path.write_text(payload, encoding="utf-8")
    shard = _prearm_shard()
    findings = check_aftermarket_prearm(
        expected=((shard, path), (shard, tmp_path / "missing.json")),
        date=day, anchors=standard_session_anchors(day),
    )
    assert [f.reason for f in findings] == ["manifest_unreadable", "manifest_missing"]
    assert all(f.verdict == "UNVERIFIABLE" for f in findings)


@pytest.mark.parametrize("bad_ack", [None, [], {"accepted": True, "symbol": "005930", "tr_id": "H0NXCNT0", "acked_at_ns": "bad"}, {"accepted": True, "symbol": "005930", "tr_id": "H0NXCNT0", "acked_at_ns": True}])
def test_prearm_malformed_ack_is_unverifiable_and_read_only(tmp_path, bad_ack, caplog) -> None:
    import datetime as dt
    import json
    import logging

    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.eod import check_aftermarket_prearm

    day = dt.date(2026, 10, 18)
    shard = _prearm_shard()
    path = _write_prearm_manifest(tmp_path / "bad.json", day, shard, started_ns=_prearm_ns(day, 15, 39), acks=[])
    raw = json.loads(path.read_text())
    raw["subscription_acks"] = [bad_ack]
    path.write_text(json.dumps(raw))
    before = path.read_bytes()
    with caplog.at_level(logging.INFO):
        (finding,) = check_aftermarket_prearm(expected=((shard, path),), date=day, anchors=standard_session_anchors(day))
    assert finding.verdict == "UNVERIFIABLE"
    assert finding.reason == "manifest_unreadable"
    assert path.read_bytes() == before
    assert not caplog.records


@pytest.mark.parametrize(("offset_ns", "verdict"), [(-1, "ARMED"), (0, "LATE_ACK"), (1, "LATE_ACK")])
def test_prearm_ack_boundary_uses_last_pairs_first_accepted_time(tmp_path, offset_ns, verdict) -> None:
    import datetime as dt

    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.eod import check_aftermarket_prearm

    day = dt.date(2026, 10, 18)
    shard = _prearm_shard(symbols=("005930", "000660"))
    open_ns = _prearm_ns(day, 15, 40)
    acks = [(s, t, True, open_ns - 10) for s in shard.symbols for t in shard.streams]
    acks[-1] = (shard.symbols[-1], shard.streams[-1], True, open_ns + offset_ns)
    acks += [(s, t, True, open_ns + 100) for s in shard.symbols for t in shard.streams]
    acks += [(shard.symbols[0], shard.streams[0], False, None), ("unplanned", shard.streams[0], True, None)]
    path = _write_prearm_manifest(tmp_path / "m.json", day, shard, started_ns=open_ns - 100, acks=list(reversed(acks)))
    (finding,) = check_aftermarket_prearm(expected=((shard, path),), date=day, anchors=standard_session_anchors(day))
    assert finding.verdict == verdict
    assert finding.initial_ack_ns == open_ns + offset_ns


def _prearm_ns(day, h, m, s=0):
    import datetime as dt
    from zoneinfo import ZoneInfo

    return int(
        dt.datetime(day.year, day.month, day.day, h, m, s, tzinfo=ZoneInfo("Asia/Seoul")).timestamp() * 1_000_000_000
    )


def _prearm_shard(venue="nxt", index=0, symbols=("005930",), streams=("H0NXCNT0", "H0NXASP0")):
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard

    return AftermarketShard(MarketVenue(venue), index, symbols, streams, "0", f"id{index}{venue}")


def _write_prearm_manifest(path, day, shard, *, started_ns, acks, session=None, venue=None, date_override=None):
    import pathlib
    from src.realtime.manifest import SessionManifest

    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    manifest = SessionManifest(
        session_date=date_override or day,
        clock_offset_ns=0,
        started_at_ns=started_ns,
        venue=venue or shard.venue.value,
        session=session or ("nxt_after" if shard.venue.value == "nxt" else "krx_after"),
        expected_close_ns=1,
        writer_closed_at_ns=started_ns + 1,
        planned_pairs=[{"symbol": s, "tr_id": t} for s in shard.symbols for t in shard.streams],
        shard_index=shard.shard_index,
        credential_key_id=shard.credential_key_id,
    )
    for symbol, tr_id, accepted, at_ns in acks:
        manifest.record_ack(vendor="kis", tr_id=tr_id, symbol=symbol, rt_cd="0", accepted=accepted, acked_at_ns=at_ns)
    manifest.save(path)
    return path


def test_prearm_armed_when_start_and_acks_precede_open(tmp_path) -> None:
    import datetime as dt
    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.eod import check_aftermarket_prearm

    day = dt.date(2026, 10, 18)
    anchors = standard_session_anchors(day)
    shard = _prearm_shard()
    started = _prearm_ns(day, 15, 39, 2)
    acks = [(s, t, True, _prearm_ns(day, 15, 39, 6)) for s in shard.symbols for t in shard.streams]
    path = _write_prearm_manifest(tmp_path / "nxt.json", day, shard, started_ns=started, acks=acks)
    (finding,) = check_aftermarket_prearm(expected=((shard, path),), date=day, anchors=anchors)
    assert finding.verdict == "ARMED"


def test_prearm_late_start_reproduces_observed_launch(tmp_path) -> None:
    import datetime as dt
    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.eod import check_aftermarket_prearm

    day = dt.date(2026, 10, 18)
    anchors = standard_session_anchors(day)
    shard = _prearm_shard()
    started = _prearm_ns(day, 15, 40, 7)
    acks = [(s, t, True, _prearm_ns(day, 15, 40, 8)) for s in shard.symbols for t in shard.streams]
    path = _write_prearm_manifest(tmp_path / "nxt.json", day, shard, started_ns=started, acks=acks)
    (finding,) = check_aftermarket_prearm(expected=((shard, path),), date=day, anchors=anchors)
    assert finding.verdict == "LATE_START"


def test_prearm_start_exactly_at_open_is_late(tmp_path) -> None:
    import datetime as dt
    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.eod import check_aftermarket_prearm

    day = dt.date(2026, 10, 18)
    anchors = standard_session_anchors(day)
    shard = _prearm_shard(venue="krx", streams=("H0STCNT0", "H0STASP0"))
    started = _prearm_ns(day, 16, 0, 0)
    acks = [(s, t, True, _prearm_ns(day, 15, 59, 0)) for s in shard.symbols for t in shard.streams]
    path = _write_prearm_manifest(tmp_path / "krx.json", day, shard, started_ns=started, acks=acks)
    (finding,) = check_aftermarket_prearm(expected=((shard, path),), date=day, anchors=anchors)
    assert finding.verdict == "LATE_START"


def test_prearm_late_ack(tmp_path) -> None:
    import datetime as dt
    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.eod import check_aftermarket_prearm

    day = dt.date(2026, 10, 18)
    anchors = standard_session_anchors(day)
    shard = _prearm_shard()
    started = _prearm_ns(day, 15, 39, 58)
    late = _prearm_ns(day, 15, 40, 1)
    early = _prearm_ns(day, 15, 39, 5)
    acks = [(shard.symbols[0], shard.streams[0], True, late), (shard.symbols[0], shard.streams[1], True, early)]
    path = _write_prearm_manifest(tmp_path / "nxt.json", day, shard, started_ns=started, acks=acks)
    (finding,) = check_aftermarket_prearm(expected=((shard, path),), date=day, anchors=anchors)
    assert finding.verdict == "LATE_ACK"
    assert finding.initial_ack_ns == late


def test_prearm_reconnect_reacks_do_not_taint(tmp_path) -> None:
    import datetime as dt
    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.eod import check_aftermarket_prearm

    day = dt.date(2026, 10, 18)
    anchors = standard_session_anchors(day)
    shard = _prearm_shard()
    started = _prearm_ns(day, 15, 39, 2)
    acks = [(s, t, True, _prearm_ns(day, 15, 39, 6)) for s in shard.symbols for t in shard.streams]
    acks += [(s, t, True, _prearm_ns(day, 17, 12, 0)) for s in shard.symbols for t in shard.streams]
    path = _write_prearm_manifest(tmp_path / "nxt.json", day, shard, started_ns=started, acks=acks)
    (finding,) = check_aftermarket_prearm(expected=((shard, path),), date=day, anchors=anchors)
    assert finding.verdict == "ARMED"
    assert finding.initial_ack_ns == _prearm_ns(day, 15, 39, 6)


def test_prearm_same_day_restart_judged_by_first_boot(tmp_path) -> None:
    import datetime as dt
    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.eod import check_aftermarket_prearm
    from src.realtime.manifest import SessionManifest

    day = dt.date(2026, 10, 18)
    anchors = standard_session_anchors(day)
    shard = _prearm_shard()
    started = _prearm_ns(day, 15, 39, 2)
    acks = [(s, t, True, _prearm_ns(day, 15, 39, 6)) for s in shard.symbols for t in shard.streams]
    path = _write_prearm_manifest(tmp_path / "nxt.json", day, shard, started_ns=started, acks=acks)
    manifest = SessionManifest.load(path)
    manifest.boots.append(
        {"started_at_ns": _prearm_ns(day, 16, 30, 0), "clock_offset_ns": 0, "clock_status": "measured"}
    )
    manifest.save(path)
    (finding,) = check_aftermarket_prearm(expected=((shard, path),), date=day, anchors=anchors)
    assert finding.verdict == "ARMED"


def test_prearm_shifted_day_uses_vendor_anchors(tmp_path) -> None:
    import datetime as dt
    from src.core.session_anchors import AnchorSource, SessionAnchors
    from src.orchestration.eod import check_aftermarket_prearm

    day = dt.date(2026, 11, 19)
    anchors = SessionAnchors(
        date=day,
        regular_open=dt.time(10, 0),
        closing_auction_start=dt.time(16, 20),
        regular_close=dt.time(16, 30),
        after_market_end=dt.time(20, 0),
        source=AnchorSource.VENDOR,
    )
    shard = _prearm_shard()
    acks_armed = [(s, t, True, _prearm_ns(day, 16, 39, 0)) for s in shard.symbols for t in shard.streams]
    ok = _write_prearm_manifest(
        tmp_path / "ok.json", day, shard, started_ns=_prearm_ns(day, 16, 39, 0), acks=acks_armed
    )
    (finding,) = check_aftermarket_prearm(expected=((shard, ok),), date=day, anchors=anchors)
    assert finding.verdict == "ARMED"
    late = _write_prearm_manifest(
        tmp_path / "late.json", day, shard, started_ns=_prearm_ns(day, 16, 40, 30), acks=acks_armed
    )
    (late_finding,) = check_aftermarket_prearm(expected=((shard, late),), date=day, anchors=anchors)
    assert late_finding.verdict == "LATE_START"


def test_prearm_missing_or_corrupt_is_unverifiable(tmp_path) -> None:
    import datetime as dt
    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.eod import check_aftermarket_prearm

    day = dt.date(2026, 10, 18)
    anchors = standard_session_anchors(day)
    shard = _prearm_shard()
    bad = tmp_path / "bad.json"
    bad.write_text("{", encoding="utf-8")
    findings = check_aftermarket_prearm(
        expected=((shard, tmp_path / "absent.json"), (shard, bad)), date=day, anchors=anchors
    )
    assert [f.verdict for f in findings] == ["UNVERIFIABLE", "UNVERIFIABLE"]
    assert [f.reason for f in findings] == ["manifest_missing", "manifest_unreadable"]


def test_prearm_legacy_acks_without_time_are_unverifiable(tmp_path) -> None:
    import datetime as dt
    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.eod import check_aftermarket_prearm
    from src.realtime.manifest import SessionManifest

    day = dt.date(2026, 10, 18)
    anchors = standard_session_anchors(day)
    shard = _prearm_shard()
    manifest = SessionManifest(
        session_date=day,
        clock_offset_ns=0,
        started_at_ns=_prearm_ns(day, 15, 39, 0),
        venue="nxt",
        session="nxt_after",
        expected_close_ns=1,
        writer_closed_at_ns=2,
        planned_pairs=[{"symbol": s, "tr_id": t} for s in shard.symbols for t in shard.streams],
        shard_index=0,
        credential_key_id=shard.credential_key_id,
    )
    for s in shard.symbols:
        for t in shard.streams:
            manifest.subscription_acks.append(
                {"vendor": "kis", "tr_id": t, "symbol": s, "rt_cd": "0", "accepted": True}
            )
    path = tmp_path / "legacy.json"
    manifest.save(path)
    (finding,) = check_aftermarket_prearm(expected=((shard, path),), date=day, anchors=anchors)
    assert finding.verdict == "UNVERIFIABLE"
    assert finding.reason == "ack_time_missing"


def test_prearm_unacked_planned_pair_is_unverifiable(tmp_path) -> None:
    import datetime as dt
    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.eod import check_aftermarket_prearm

    day = dt.date(2026, 10, 18)
    anchors = standard_session_anchors(day)
    shard = _prearm_shard()
    started = _prearm_ns(day, 15, 39, 0)
    acks = [(shard.symbols[0], shard.streams[0], True, _prearm_ns(day, 15, 39, 5))]
    path = _write_prearm_manifest(tmp_path / "nxt.json", day, shard, started_ns=started, acks=acks)
    (finding,) = check_aftermarket_prearm(expected=((shard, path),), date=day, anchors=anchors)
    assert finding.verdict == "UNVERIFIABLE"
    assert finding.reason == "pair_unacked"


def test_prearm_date_and_venue_mismatch_are_unverifiable(tmp_path) -> None:
    import datetime as dt
    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.eod import check_aftermarket_prearm

    day = dt.date(2026, 10, 18)
    anchors = standard_session_anchors(day)
    shard = _prearm_shard()
    started = _prearm_ns(day, 15, 39, 0)
    acks = [(s, t, True, _prearm_ns(day, 15, 39, 5)) for s in shard.symbols for t in shard.streams]
    wrong_date = _write_prearm_manifest(
        tmp_path / "date.json", day, shard, started_ns=started, acks=acks, date_override=dt.date(2026, 10, 17)
    )
    (date_finding,) = check_aftermarket_prearm(expected=((shard, wrong_date),), date=day, anchors=anchors)
    assert date_finding.verdict == "UNVERIFIABLE"
    assert date_finding.reason == "date_mismatch"
    wrong_venue = _write_prearm_manifest(
        tmp_path / "venue.json", day, shard, started_ns=started, acks=acks, venue="krx"
    )
    (venue_finding,) = check_aftermarket_prearm(expected=((shard, wrong_venue),), date=day, anchors=anchors)
    assert venue_finding.verdict == "UNVERIFIABLE"
    assert venue_finding.reason == "venue_mismatch"


def _coverage_shard(symbols=("A", "B", "C")):
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard

    return AftermarketShard(MarketVenue.NXT, 0, tuple(symbols), ("H0NXCNT0", "H0NXASP0"), "0", "id0")


def _write_coverage_manifest(path, day, shard, *, counts, accepted=None, date_override=None):
    from src.realtime.manifest import SessionManifest
    import pathlib

    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    manifest = SessionManifest(
        session_date=date_override or day,
        clock_offset_ns=0,
        started_at_ns=1,
        venue=shard.venue.value,
        session="nxt_after",
        expected_close_ns=1,
        writer_closed_at_ns=2,
        planned_pairs=[{"symbol": s, "tr_id": t} for s in shard.symbols for t in shard.streams],
        shard_index=shard.shard_index,
        credential_key_id=shard.credential_key_id,
    )
    accept = set(accepted) if accepted is not None else set(shard.symbols)
    for s in shard.symbols:
        for t in shard.streams:
            manifest.record_ack(
                vendor="kis", tr_id=t, symbol=s, rt_cd="0", accepted=s in accept, acked_at_ns=1
            )
    for symbol, per_stream in counts.items():
        for stream, n in per_stream.items():
            for _ in range(n):
                manifest.record_frame_count(symbol, stream)
    manifest.save(path)
    return path


def test_coverage_accepted_silent_symbol_is_reported(tmp_path) -> None:
    import datetime as dt
    from src.orchestration.eod import check_aftermarket_symbol_coverage

    day = dt.date(2026, 10, 18)
    shard = _coverage_shard()
    path = _write_coverage_manifest(
        tmp_path / "m.json", day, shard, counts={"A": {"H0NXCNT0": 10}, "C": {"H0NXCNT0": 4}}
    )
    (finding,) = check_aftermarket_symbol_coverage(expected=((shard, path),), date=day)
    assert finding.zero_frame_symbols == ("B",)
    assert finding.accepted_symbols == 3


def test_coverage_quote_only_symbol_is_not_silent(tmp_path) -> None:
    import datetime as dt
    from src.orchestration.eod import check_aftermarket_symbol_coverage

    day = dt.date(2026, 10, 18)
    shard = _coverage_shard(("A", "B"))
    path = _write_coverage_manifest(
        tmp_path / "m.json", day, shard, counts={"A": {"H0NXCNT0": 1}, "B": {"H0NXASP0": 2}}
    )
    (finding,) = check_aftermarket_symbol_coverage(expected=((shard, path),), date=day)
    assert finding.zero_frame_symbols == ()


def test_coverage_unaccepted_symbol_is_not_reported(tmp_path) -> None:
    import datetime as dt
    from src.orchestration.eod import check_aftermarket_symbol_coverage

    day = dt.date(2026, 10, 18)
    shard = _coverage_shard(("A", "B", "C"))
    path = _write_coverage_manifest(
        tmp_path / "m.json",
        day,
        shard,
        counts={"A": {"H0NXCNT0": 1}, "C": {"H0NXCNT0": 1}},
        accepted={"A", "C"},
    )
    (finding,) = check_aftermarket_symbol_coverage(expected=((shard, path),), date=day)
    assert finding.zero_frame_symbols == ()
    assert finding.accepted_symbols == 2


def test_coverage_rank_order_and_probe_shape(tmp_path) -> None:
    import datetime as dt
    from src.orchestration.eod import check_aftermarket_symbol_coverage

    day = dt.date(2026, 10, 18)
    symbols = tuple(f"S{i:02d}" for i in range(20))
    shard = _coverage_shard(symbols)
    silent = set(symbols[1:18:2])
    counts = {s: {"H0NXCNT0": 1} for s in symbols if s not in silent}
    expected_zero = tuple(s for s in symbols if s in silent)
    assert len(expected_zero) == 9
    path = _write_coverage_manifest(tmp_path / "m.json", day, shard, counts=counts)
    (finding,) = check_aftermarket_symbol_coverage(expected=((shard, path),), date=day)
    assert finding.zero_frame_symbols == expected_zero


def test_coverage_untracked_missing_unreadable_and_date_mismatch(tmp_path) -> None:
    import datetime as dt
    import json
    from src.orchestration.eod import check_aftermarket_symbol_coverage

    day = dt.date(2026, 10, 18)
    shard = _coverage_shard(("A",))
    legacy = tmp_path / "legacy.json"
    legacy.write_text(
        json.dumps({"session_date": "2026-10-18", "clock_offset_ns": 0, "started_at_ns": 0}),
        encoding="utf-8",
    )
    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("not json", encoding="utf-8")
    wrong_date = _write_coverage_manifest(
        tmp_path / "wrong.json", day, shard, counts={}, date_override=dt.date(2026, 10, 17)
    )
    missing = tmp_path / "absent.json"
    findings = check_aftermarket_symbol_coverage(
        expected=(
            (shard, legacy),
            (shard, missing),
            (shard, corrupt),
            (shard, wrong_date),
        ),
        date=day,
    )
    assert [f.zero_frame_symbols for f in findings] == [None, None, None, None]
    assert [f.reason for f in findings] == ["untracked", "manifest_missing", "manifest_unreadable", "date_mismatch"]


@pytest.mark.parametrize(
    "change",
    [
        {"subscription_acks": [None]},
        {"subscription_acks": ["invalid"]},
        {"frame_counts": []},
        {"frame_counts": {"A": []}},
        {"frame_counts": {"A": {"H0NXCNT0": -1}}},
        {"frame_counts": {"A": {"H0NXCNT0": True}}},
        {"frame_counts": {"A": {"H0NXCNT0": 0.5}}},
        {"frame_counts": {"A": {"H0NXCNT0": "1"}}},
    ],
)
def test_coverage_bad_manifest_is_isolated_read_only(tmp_path, caplog, change) -> None:
    import datetime as dt
    import json
    import logging

    from src.orchestration.eod import check_aftermarket_symbol_coverage

    day = dt.date(2026, 10, 18)
    shard = _coverage_shard(("A",))
    bad = _write_coverage_manifest(tmp_path / "bad.json", day, shard, counts={})
    payload = json.loads(bad.read_text(encoding="utf-8"))
    payload.update(change)
    bad.write_text(json.dumps(payload), encoding="utf-8")
    before = bad.read_bytes()
    good = _write_coverage_manifest(tmp_path / "good.json", day, shard, counts={})
    with caplog.at_level(logging.DEBUG):
        invalid, valid = check_aftermarket_symbol_coverage(expected=((shard, bad), (shard, good)), date=day)
    assert invalid.zero_frame_symbols is None
    assert invalid.reason == "manifest_unreadable"
    assert valid.zero_frame_symbols == ("A",)
    assert bad.read_bytes() == before
    assert all(record.levelno <= logging.DEBUG for record in caplog.records)


def test_coverage_counts_any_accepted_ack_even_when_untracked(tmp_path) -> None:
    import datetime as dt

    from src.orchestration.eod import check_aftermarket_symbol_coverage
    from src.realtime.manifest import SessionManifest

    day = dt.date(2026, 10, 18)
    shard = _coverage_shard(("B", "A", "C"))
    path = _write_coverage_manifest(tmp_path / "m.json", day, shard, counts={}, accepted=())
    manifest = SessionManifest.load(path)
    manifest.record_ack(vendor="kis", tr_id="H0NXASP0", symbol="B", rt_cd="0", accepted=True)
    manifest.record_ack(vendor="kis", tr_id="H0NXASP0", symbol="B", rt_cd="9", accepted=False)
    manifest.record_ack(vendor="kis", tr_id="H0NXASP0", symbol="outside", rt_cd="0", accepted=True)
    manifest.save(path)
    (tracked,) = check_aftermarket_symbol_coverage(expected=((shard, path),), date=day)
    assert tracked.accepted_symbols == 1
    assert tracked.zero_frame_symbols == ("B",)
    manifest.frame_counts = None
    manifest.save(path)
    (untracked,) = check_aftermarket_symbol_coverage(expected=((shard, path),), date=day)
    assert untracked.accepted_symbols == 1
    assert untracked.zero_frame_symbols is None
    assert untracked.reason == "untracked"


def _ledger_ns(day, hour, minute, second=0):
    import datetime as dt
    from zoneinfo import ZoneInfo

    return int(
        dt.datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=ZoneInfo("Asia/Seoul")).timestamp()
    ) * 1_000_000_000


def _ledger_gap(day, hour, minute, duration_s, symbol="005930"):
    start = _ledger_ns(day, hour, minute)
    return (symbol, start, start + int(duration_s * 1_000_000_000))


def _ledger_session(tmp_path, day, gaps=()):
    journal_root = tmp_path / "l0"
    _write_routed_journal(journal_root, stream="H0STCNT0", date=day)
    _write_routed_journal(journal_root, stream="H0STASP0", date=day)
    manifest_path = tmp_path / "manifest" / f"{day.isoformat()}.json"
    _write_session_manifest(manifest_path, day, gaps=gaps)
    return journal_root, manifest_path


def _ledger_symbol(symbol="005930", *, trades=1_600_000, missing=0, count_only=0, status="OK", intervals=()):
    from src.storage.l0_audit import SymbolLedger

    return SymbolLedger(
        symbol=symbol,
        status=status,
        frames=max(trades, 1),
        trades_observed=trades,
        confirmed_missing=missing,
        count_only_missing=count_only,
        net_missing=0,
        volume_deficit=0,
        duplicates=0,
        regressions=0,
        resets=0,
        decode_fail=0,
        intervals=tuple(intervals),
        intervals_truncated=0,
        manifest_frames=None,
    )


def _ledger_audit(day, *, status="OK", reason="ok", symbols=(), tail_silence_s=0.0, latency=()):
    from src.storage.l0_audit import AuditStatus, LedgerTotals, SessionAudit

    syms = tuple(symbols)
    ok = [s for s in syms if s.status == "OK"]
    trades = sum(s.trades_observed for s in ok)
    confirmed = sum(s.confirmed_missing for s in ok)
    totals = LedgerTotals(
        symbols=len(ok),
        symbols_with_loss=sum(1 for s in ok if s.confirmed_missing + s.count_only_missing > 0),
        symbols_without_counter=sum(1 for s in syms if s.status != "OK"),
        trades_observed=trades,
        confirmed_missing=confirmed,
        count_only_missing=sum(s.count_only_missing for s in ok),
        net_missing_positive=0,
        volume_deficit=0,
        loss_ratio=(confirmed / trades) if trades else 0.0,
    )
    return SessionAudit(
        schema_version=1,
        session_date=day,
        vendor="ls",
        venue="krx",
        session="regular",
        status=AuditStatus(status),
        reason=reason,
        generated_at_ns=1,
        sources=(),
        latency=tuple(latency),
        symbols=syms,
        totals=totals,
        tail_silence_s=tail_silence_s,
        last_regular_tick_recv_ns=1,
    )


def _ledger_latency(stream, peaks):
    from src.storage.l0_audit import AuditStatus, LatencyMinute, StreamLatency

    return StreamLatency(
        stream=stream,
        status=AuditStatus.OK,
        reason="ok",
        rows=60 * len(peaks),
        event_time_missing=0,
        minutes=tuple(
            LatencyMinute(minute_kst=m, count=60, p50_s=p, max_s=x, min_s=0.5) for m, x, p in peaks
        ),
    )


def _ledger_interval(symbol, recv_prev_ns, recv_next_ns, phase="regular"):
    from src.storage.l0_audit import LossInterval

    return LossInterval(
        symbol=symbol,
        phase=phase,
        kind="confirmed",
        missing_trades=10,
        volume_deficit=5,
        cum_prev=1,
        cum_next=12,
        chetime_prev="090100",
        chetime_next="090101",
        recv_prev_ns=recv_prev_ns,
        recv_next_ns=recv_next_ns,
        conn_changed=False,
    )


def _enforce_settings(**overrides):
    from src.core.config import AuditMode, AuditSettings

    return AuditSettings(mode=AuditMode.ENFORCE, **overrides)


def test_check_session_reconciliation_shadow_keeps_legacy_in_charge(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.core.config import AuditMode, AuditSettings
    from src.orchestration.eod import check_session_reconciliation

    # Given: 700초 수신 공백 + 결손 제로 감사, shadow 모드
    day = dt.date(2026, 10, 15)
    journal_root, manifest_path = _ledger_session(tmp_path, day, gaps=[_ledger_gap(day, 9, 10, 700)])
    audit = _ledger_audit(day, symbols=(_ledger_symbol(missing=0),))

    # When
    with caplog.at_level(logging.INFO):
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
            audit=audit,
            audit_settings=AuditSettings(mode=AuditMode.SHADOW),
        )

    # Then: legacy가 실패를 결정하고 장부 판정은 would로만 기록된다
    assert ok is False
    shadow = [r for r in caplog.records if "mode=shadow" in r.getMessage()]
    assert len(shadow) == 1
    assert "would=PASS legacy=FAIL" in shadow[0].getMessage()
    assert shadow[0].levelno == logging.INFO
    critical = [r for r in caplog.records if r.levelno == logging.CRITICAL]
    assert len(critical) == 1
    assert "gap_exceeded:700s" in critical[0].getMessage()


def test_check_session_reconciliation_shadow_reports_would_fail(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.core.config import AuditMode, AuditSettings
    from src.orchestration.eod import check_session_reconciliation

    # Given: legacy도 장부도 실패하는 shadow 감사
    day = dt.date(2026, 10, 15)
    journal_root, manifest_path = _ledger_session(tmp_path, day, gaps=[_ledger_gap(day, 9, 10, 700)])
    audit = _ledger_audit(day, symbols=(_ledger_symbol(missing=10198),))

    # When
    with caplog.at_level(logging.INFO):
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
            audit=audit,
            audit_settings=AuditSettings(mode=AuditMode.SHADOW),
        )

    # Then: legacy 실패 + would=FAIL 경고
    assert ok is False
    shadow = [r for r in caplog.records if "mode=shadow" in r.getMessage()]
    assert len(shadow) == 1
    assert "would=FAIL legacy=FAIL" in shadow[0].getMessage()
    assert "trade_loss_exceeded:10198/1600000" in shadow[0].getMessage()
    assert shadow[0].levelno == logging.WARNING


def test_check_session_reconciliation_shadow_passes_clean_session(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.core.config import AuditMode, AuditSettings
    from src.orchestration.eod import check_session_reconciliation

    # Given: 100초 공백 + 깨끗한 감사, shadow 모드
    day = dt.date(2026, 10, 15)
    journal_root, manifest_path = _ledger_session(tmp_path, day, gaps=[_ledger_gap(day, 9, 10, 100)])
    audit = _ledger_audit(day, symbols=(_ledger_symbol(missing=0),))

    # When
    with caplog.at_level(logging.INFO):
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
            audit=audit,
            audit_settings=AuditSettings(mode=AuditMode.SHADOW),
        )

    # Then: 통과 + would=PASS legacy=PASS 정보줄
    assert ok is True
    assert "mode=shadow date=2026-10-15 would=PASS legacy=PASS reasons=-" in caplog.text
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_check_session_reconciliation_cb_halt_costs_nothing(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.orchestration.eod import check_session_reconciliation

    # Given: 902초 수신 공백 + 결손 제로 OK 감사, enforce 모드
    day = dt.date(2026, 10, 15)
    journal_root, manifest_path = _ledger_session(tmp_path, day, gaps=[_ledger_gap(day, 9, 0, 902)])
    audit = _ledger_audit(day, symbols=(_ledger_symbol(trades=800_000, missing=0),), tail_silence_s=1.2)

    # When
    with caplog.at_level(logging.INFO):
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
            audit=audit,
            audit_settings=_enforce_settings(),
        )

    # Then: 통과, CRITICAL 없음, 공백은 silent로만 보고된다
    assert ok is True
    assert not [r for r in caplog.records if r.levelno == logging.CRITICAL]
    assert "silent_span_s=902.0" in caplog.text
    assert "loss_span_s=0.0" in caplog.text


def test_check_session_reconciliation_real_open_loss_fails(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.orchestration.eod import check_session_reconciliation

    # Given: 09:01 1.4초 공백 + 0.64% 확정 결손, 겹치는 수신 구간 존재
    day = dt.date(2026, 10, 2)
    symbol, start, end = _ledger_gap(day, 9, 1, 1.4)
    journal_root, manifest_path = _ledger_session(tmp_path, day, gaps=[(symbol, start, end)])
    interval = _ledger_interval(symbol, start, start + 2_000_000_000)
    audit = _ledger_audit(day, symbols=(_ledger_symbol(missing=10198, intervals=(interval,)),))

    # When
    with caplog.at_level(logging.INFO):
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
            audit=audit,
            audit_settings=_enforce_settings(loss_fail_ratio=5e-3),
        )

    # Then: CRITICAL 실패 + 결손 구간으로 분류
    assert ok is False
    fail = [r for r in caplog.records if "stage=session_reconciliation status=FAIL" in r.getMessage()]
    assert len(fail) == 1
    assert fail[0].levelno == logging.CRITICAL
    assert "trade_loss_exceeded:10198/1600000" in fail[0].getMessage()
    assert "loss_span_s=1.4" in caplog.text


def test_check_session_reconciliation_warn_band_passes_loudly(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.orchestration.eod import check_session_reconciliation

    # Given: 경고 구간 비율(0.1%)의 결손
    day = dt.date(2026, 10, 13)
    journal_root, manifest_path = _ledger_session(tmp_path, day)
    audit = _ledger_audit(day, symbols=(_ledger_symbol(missing=1600),), tail_silence_s=None)

    # When
    with caplog.at_level(logging.INFO):
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
            audit=audit,
            audit_settings=_enforce_settings(),
        )

    # Then: 통과 + 정확히 하나의 WARN 줄
    assert ok is True
    warns = [r for r in caplog.records if "status=WARN" in r.getMessage()]
    assert len(warns) == 1
    assert "trade_loss_warn:1600/1600000" in warns[0].getMessage()
    assert not [r for r in caplog.records if "status=FAIL" in r.getMessage()]


def test_check_session_reconciliation_threshold_equality_passes(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.orchestration.eod import check_session_reconciliation

    # Given: 실패 임계와 정확히 같은 비율
    day = dt.date(2026, 10, 13)
    journal_root, manifest_path = _ledger_session(tmp_path, day)
    settings = _enforce_settings(loss_fail_ratio=0.005)

    def _check(missing):
        audit = _ledger_audit(day, symbols=(_ledger_symbol(missing=missing),))
        return check_session_reconciliation(
            manifest_path=manifest_path,
            date=day,
            journal_root=journal_root,
            streams=("H0STCNT0", "H0STASP0"),
            vendor="ls",
            venue="krx",
            session="regular",
            regular_open=dt.time(9, 0),
            regular_close=dt.time(15, 30),
            audit=audit,
            audit_settings=settings,
        )

    # When/Then: 같은 값은 통과, 한 건 추가는 실패
    with caplog.at_level(logging.INFO):
        assert _check(8000) is True
    with caplog.at_level(logging.INFO):
        assert _check(8001) is False
    assert "trade_loss_exceeded:8001/1600000" in caplog.text


def test_check_session_reconciliation_symbol_level_breach(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.orchestration.eod import check_session_reconciliation

    # Given: 전체는 미미하지만 한 종목이 20% 결손
    day = dt.date(2026, 10, 13)
    journal_root, manifest_path = _ledger_session(tmp_path, day)
    big = _ledger_symbol("005930", trades=1_599_000, missing=0)
    bad = _ledger_symbol("000660", trades=1000, missing=200)
    audit = _ledger_audit(day, symbols=(big, bad))

    # When
    with caplog.at_level(logging.INFO):
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
            audit=audit,
            audit_settings=_enforce_settings(),
        )

    # Then: 종목 한도로 실패
    assert ok is False
    assert "symbol_loss_exceeded:000660:200/1000" in caplog.text
    assert "trade_loss_exceeded" not in caplog.text


def test_check_session_reconciliation_symbol_below_min_trades_passes(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.orchestration.eod import check_session_reconciliation

    # Given: 같은 20% 결손이지만 관측 건수가 하한 미만
    day = dt.date(2026, 10, 13)
    journal_root, manifest_path = _ledger_session(tmp_path, day)
    big = _ledger_symbol("005930", trades=1_599_000, missing=0)
    small = _ledger_symbol("000660", trades=50, missing=10)
    audit = _ledger_audit(day, symbols=(big, small))

    # When
    with caplog.at_level(logging.INFO):
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
            audit=audit,
            audit_settings=_enforce_settings(),
        )

    # Then: 종목 한도 미적용으로 통과
    assert ok is True
    assert "symbol_loss_exceeded" not in caplog.text


def test_check_session_reconciliation_no_counter_symbols_excluded(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.orchestration.eod import check_session_reconciliation

    # Given: 카운터 없는 종목의 큰 명목 결손 + 깨끗한 관측 종목
    day = dt.date(2026, 10, 16)
    journal_root, manifest_path = _ledger_session(tmp_path, day)
    blind = _ledger_symbol("000660", trades=1_000_000, missing=500_000, status="NO_COUNTER")
    clean = _ledger_symbol("005930", trades=1000, missing=0)
    empty = _ledger_symbol("000270", trades=0, missing=0)
    audit = _ledger_audit(day, symbols=(blind, clean, empty))

    # When
    with caplog.at_level(logging.INFO):
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
            audit=audit,
            audit_settings=_enforce_settings(),
        )

    # Then: 카운터 없는 종목은 분모·분자에서 제외되어 통과
    assert ok is True
    assert audit.totals.trades_observed == 1000
    assert audit.totals.confirmed_missing == 0
    assert audit.totals.symbols_without_counter == 1


def test_check_session_reconciliation_all_no_counter_falls_back(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.orchestration.eod import check_session_reconciliation

    # Given: 모든 종목 카운터 없음 + 700초 공백
    day = dt.date(2026, 10, 15)
    journal_root, manifest_path = _ledger_session(tmp_path, day, gaps=[_ledger_gap(day, 9, 10, 700)])
    audit = _ledger_audit(
        day,
        symbols=(
            _ledger_symbol("005930", trades=0, missing=0, status="NO_COUNTER"),
            _ledger_symbol("000660", trades=0, missing=0, status="NO_COUNTER"),
        ),
    )

    # When
    with caplog.at_level(logging.INFO):
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
            audit=audit,
            audit_settings=_enforce_settings(),
        )

    # Then: 0除 없이 legacy 기준으로 실패
    assert ok is False
    assert "gap_exceeded:700s" in caplog.text
    assert "basis=legacy" in caplog.text


def test_check_session_reconciliation_tail_silence_fails_closed(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.orchestration.eod import check_session_reconciliation

    # Given: 결손 제로 + 3000초 꼬리 침묵
    day = dt.date(2026, 10, 15)
    journal_root, manifest_path = _ledger_session(tmp_path, day)
    settings = _enforce_settings()

    def _check(tail):
        audit = _ledger_audit(day, symbols=(_ledger_symbol(missing=0),), tail_silence_s=tail)
        return check_session_reconciliation(
            manifest_path=manifest_path,
            date=day,
            journal_root=journal_root,
            streams=("H0STCNT0", "H0STASP0"),
            vendor="ls",
            venue="krx",
            session="regular",
            regular_open=dt.time(9, 0),
            regular_close=dt.time(15, 30),
            audit=audit,
            audit_settings=settings,
        )

    # When/Then: 초과는 실패, 정확히는 통과
    with caplog.at_level(logging.INFO):
        assert _check(3000.0) is False
    assert "tail_silence_exceeded:3000s" in caplog.text
    with caplog.at_level(logging.INFO):
        assert _check(600.0) is True


def test_check_session_reconciliation_partial_needs_both_bases(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.orchestration.eod import check_session_reconciliation

    # Given: PARTIAL 감사 3종 (장부깨끗+legacy초과 / 장부실패+legacy깨끗 / 둘다깨끗)
    day = dt.date(2026, 10, 15)
    _, clean_manifest = _ledger_session(tmp_path, day)
    gapped_root, gapped_manifest = _ledger_session(tmp_path / "gapped", day, gaps=[_ledger_gap(day, 9, 10, 700)])
    settings = _enforce_settings()

    def _check(manifest_path, journal_root, missing):
        audit = _ledger_audit(day, status="PARTIAL", reason="truncated", symbols=(_ledger_symbol(missing=missing),))
        return check_session_reconciliation(
            manifest_path=manifest_path,
            date=day,
            journal_root=journal_root,
            streams=("H0STCNT0", "H0STASP0"),
            vendor="ls",
            venue="krx",
            session="regular",
            regular_open=dt.time(9, 0),
            regular_close=dt.time(15, 30),
            audit=audit,
            audit_settings=settings,
        )

    # When/Then
    with caplog.at_level(logging.INFO):
        assert _check(gapped_manifest, gapped_root, 0) is False
    assert "gap_exceeded:700s" in caplog.text
    with caplog.at_level(logging.INFO):
        assert _check(clean_manifest, tmp_path / "l0", 10198) is False
    assert "trade_loss_exceeded:10198/1600000" in caplog.text
    with caplog.at_level(logging.INFO):
        assert _check(clean_manifest, tmp_path / "l0", 0) is True
    assert "partial=1" in caplog.text


def test_check_session_reconciliation_unavailable_falls_back(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.orchestration.eod import check_session_reconciliation

    # Given: UNAVAILABLE 감사 + 100초 공백
    day = dt.date(2026, 10, 15)
    journal_root, manifest_path = _ledger_session(tmp_path, day, gaps=[_ledger_gap(day, 9, 10, 100)])
    _, wide_manifest = _ledger_session(tmp_path / "wide", day, gaps=[_ledger_gap(day, 9, 10, 700)])
    audit = _ledger_audit(day, status="UNAVAILABLE", reason="time_budget")
    settings = _enforce_settings()

    def _check(manifest_path):
        return check_session_reconciliation(
            manifest_path=manifest_path,
            date=day,
            journal_root=journal_root,
            streams=("H0STCNT0", "H0STASP0"),
            vendor="ls",
            venue="krx",
            session="regular",
            regular_open=dt.time(9, 0),
            regular_close=dt.time(15, 30),
            audit=audit,
            audit_settings=settings,
        )

    # When/Then: 작은 공백은 통과 + 폴백 경고, 큰 공백은 legacy 실패
    with caplog.at_level(logging.INFO):
        assert _check(manifest_path) is True
    assert "status=UNAVAILABLE reason=time_budget basis=legacy" in caplog.text
    with caplog.at_level(logging.INFO):
        assert _check(wide_manifest) is False
    assert "gap_exceeded:700s" in caplog.text


def test_check_session_reconciliation_date_mismatch_is_unavailable(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.orchestration.eod import check_session_reconciliation

    # Given: 다른 날짜의 감사
    day = dt.date(2026, 10, 15)
    journal_root, manifest_path = _ledger_session(tmp_path, day, gaps=[_ledger_gap(day, 9, 10, 100)])
    audit = _ledger_audit(day + dt.timedelta(days=1), symbols=(_ledger_symbol(missing=0),))

    # When
    with caplog.at_level(logging.INFO):
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
            audit=audit,
            audit_settings=_enforce_settings(),
        )

    # Then: 폴백 경고 후 legacy로 통과
    assert ok is True
    assert "status=UNAVAILABLE reason=date_mismatch basis=legacy" in caplog.text


def test_check_session_reconciliation_mode_off_is_silent(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.core.config import AuditMode, AuditSettings
    from src.orchestration.eod import check_session_reconciliation, log_session_audit

    # Given: OFF 모드 + 700초 공백 + 감사
    day = dt.date(2026, 10, 15)
    journal_root, manifest_path = _ledger_session(tmp_path, day, gaps=[_ledger_gap(day, 9, 10, 700)])
    audit = _ledger_audit(day, symbols=(_ledger_symbol(missing=0),))
    settings = AuditSettings(mode=AuditMode.OFF)

    # When
    with caplog.at_level(logging.INFO):
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
            audit=audit,
            audit_settings=settings,
        )
        log_session_audit(audit, settings=settings, anchors=None, date=day)
        from src.core.session_anchors import standard_session_anchors

        log_session_audit(None, settings=_enforce_settings(), anchors=standard_session_anchors(day), date=day)

    # Then: legacy 동작만, 감사·지연 줄 없음
    assert ok is False
    assert "gap_exceeded:700s" in caplog.text
    assert "session_audit" not in caplog.text
    assert "ingest_latency" not in caplog.text


def test_check_session_reconciliation_never_masks_structural_issues(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.orchestration.eod import check_session_reconciliation

    # Given: 한 스트림 저널 누락 + 깨끗한 OK 감사
    day = dt.date(2026, 10, 15)
    journal_root = tmp_path / "l0"
    _write_routed_journal(journal_root, stream="H0STCNT0", date=day)
    manifest_path = tmp_path / "manifest" / f"{day.isoformat()}.json"
    _write_session_manifest(manifest_path, day)
    audit = _ledger_audit(day, symbols=(_ledger_symbol(missing=0),))

    # When
    with caplog.at_level(logging.INFO):
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
            audit=audit,
            audit_settings=_enforce_settings(),
        )

    # Then: 구조적 결함이 그대로 실패를 결정
    assert ok is False
    assert "reasons=journal_missing:H0STASP0" in caplog.text


def test_check_session_reconciliation_never_masks_unreadable_manifest(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.orchestration.eod import check_session_reconciliation

    # Given: 깨진 매니페스트 + 깨끗한 OK 감사
    day = dt.date(2026, 10, 15)
    journal_root = tmp_path / "l0"
    _write_routed_journal(journal_root, stream="H0STCNT0", date=day)
    _write_routed_journal(journal_root, stream="H0STASP0", date=day)
    manifest_path = tmp_path / "manifest" / f"{day.isoformat()}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_bytes(b"{broken")
    audit = _ledger_audit(day, symbols=(_ledger_symbol(missing=0),))

    # When
    with caplog.at_level(logging.INFO):
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
            audit=audit,
            audit_settings=_enforce_settings(),
        )

    # Then
    assert ok is False
    assert "manifest_unreadable" in caplog.text


def test_evaluate_session_gap_budget_classification_is_reporting_only() -> None:
    import datetime as dt

    from src.orchestration.eod import evaluate_session_gap_budget

    # Given: 겹치는 두 공백(09:10-09:20, 09:15-09:25) + 윈도우 밖 공백
    day = dt.date(2026, 10, 15)
    gaps = [
        {"symbol": "005930", "gap_start_ns": _ledger_ns(day, 9, 10), "gap_end_ns": _ledger_ns(day, 9, 20)},
        {"symbol": "000660", "gap_start_ns": _ledger_ns(day, 9, 15), "gap_end_ns": _ledger_ns(day, 9, 25)},
        {"symbol": "005930", "gap_start_ns": _ledger_ns(day, 8, 0), "gap_end_ns": _ledger_ns(day, 8, 10)},
    ]
    window = {"regular_open": dt.time(9, 0), "regular_close": dt.time(15, 30)}
    stored = _ledger_interval("005930", _ledger_ns(day, 9, 12), _ledger_ns(day, 9, 13))
    off_window = _ledger_interval("005930", _ledger_ns(day, 8, 1), _ledger_ns(day, 8, 2), phase="premarket")
    loss_audit = _ledger_audit(day, symbols=(_ledger_symbol(trades=100_000, intervals=(stored, off_window)),))
    clean_audit = _ledger_audit(day, symbols=(_ledger_symbol(trades=100_000),))
    settings = _enforce_settings()

    # When
    loss = evaluate_session_gap_budget(gaps=gaps, date=day, audit=loss_audit, settings=settings, **window)
    clean = evaluate_session_gap_budget(gaps=gaps, date=day, audit=clean_audit, settings=settings, **window)

    # Then: 합집합은 한 번만 계산되고 판정은 동일, 분류만 다르다
    assert loss.legacy_gap_s == clean.legacy_gap_s == 900.0
    assert loss.loss_span_s == 900.0
    assert loss.silent_span_s == 0.0
    assert clean.silent_span_s == 900.0
    assert clean.loss_span_s == 0.0
    assert clean.silent_spans == 1
    assert loss.status is clean.status
    assert loss.basis == clean.basis == "ledger"


def test_evaluate_session_gap_budget_honors_shifted_window() -> None:
    import datetime as dt

    from src.orchestration.eod import evaluate_session_gap_budget

    # Given: 시프트된 개장(09:10) 이전의 공백
    day = dt.date(2026, 10, 15)
    gaps = [
        {"symbol": "005930", "gap_start_ns": _ledger_ns(day, 9, 0), "gap_end_ns": _ledger_ns(day, 9, 5)},
    ]
    audit = _ledger_audit(day, symbols=(_ledger_symbol(trades=100_000),))

    # When
    verdict = evaluate_session_gap_budget(
        gaps=gaps,
        date=day,
        regular_open=dt.time(9, 10),
        regular_close=dt.time(15, 30),
        audit=audit,
        settings=_enforce_settings(),
    )

    # Then: 잘려나가 silent/loss 모두 0
    assert verdict.legacy_gap_s == 0.0
    assert verdict.silent_span_s == 0.0
    assert verdict.loss_span_s == 0.0
    assert verdict.status.value == "PASS"


def test_evaluate_session_gap_budget_is_pure_and_deterministic(caplog) -> None:
    import datetime as dt

    from src.orchestration.eod import evaluate_session_gap_budget

    # Given: 동일한 입력
    day = dt.date(2026, 10, 15)
    gaps = [{"symbol": "005930", "gap_start_ns": _ledger_ns(day, 9, 1), "gap_end_ns": _ledger_ns(day, 9, 1) + 1_400_000_000}]
    audit = _ledger_audit(day, symbols=(_ledger_symbol(missing=10198),))
    settings = _enforce_settings()

    # When
    first = evaluate_session_gap_budget(
        gaps=gaps, date=day, regular_open=dt.time(9, 0), regular_close=dt.time(15, 30),
        audit=audit, settings=settings,
    )
    second = evaluate_session_gap_budget(
        gaps=gaps, date=day, regular_open=dt.time(9, 0), regular_close=dt.time(15, 30),
        audit=audit, settings=settings,
    )

    # Then: 같은 판정, 로그 없음
    assert first == second
    assert caplog.records == []


def test_evaluate_session_gap_budget_shadow_and_off_use_legacy() -> None:
    import datetime as dt

    from src.core.config import AuditMode, AuditSettings
    from src.orchestration.eod import evaluate_session_gap_budget

    # Given: 700초 공백 + 깨끗한 감사
    day = dt.date(2026, 10, 15)
    gaps = [{"symbol": "005930", "gap_start_ns": _ledger_ns(day, 9, 10), "gap_end_ns": _ledger_ns(day, 9, 10) + 700_000_000_000}]
    audit = _ledger_audit(day, symbols=(_ledger_symbol(missing=0),))

    # When/Then: shadow·off·None 모두 legacy 기준
    for settings in (AuditSettings(mode=AuditMode.SHADOW), AuditSettings(mode=AuditMode.OFF), None):
        verdict = evaluate_session_gap_budget(
            gaps=gaps, date=day, regular_open=dt.time(9, 0), regular_close=dt.time(15, 30),
            audit=audit, settings=settings,
        )
        assert verdict.basis == "legacy"
        assert verdict.status.value == "FAIL"
        assert verdict.reasons == ("gap_exceeded:700s",)


def test_evaluate_session_gap_budget_reports_worst_symbol_plus_rest() -> None:
    import datetime as dt

    from src.orchestration.eod import evaluate_session_gap_budget

    # Given: 두 종목이 각각 한도를 초과하고 전체 비율은 미미
    day = dt.date(2026, 10, 13)
    audit = _ledger_audit(
        day,
        symbols=(
            _ledger_symbol("000660", trades=1000, missing=200),
            _ledger_symbol("000270", trades=2000, missing=300),
            _ledger_symbol("005930", trades=10_000_000, missing=0),
        ),
    )

    # When
    verdict = evaluate_session_gap_budget(
        gaps=[], date=day, regular_open=dt.time(9, 0), regular_close=dt.time(15, 30),
        audit=audit, settings=_enforce_settings(),
    )

    # Then: 최악 종목 + 나머지 수
    assert verdict.status.value == "FAIL"
    assert verdict.reasons == ("symbol_loss_exceeded:000660:200/1000+1",)


def test_log_session_audit_emits_summary_latency_and_breach(caplog) -> None:
    import datetime as dt
    import logging

    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.eod import log_session_audit

    # Given: 09:01 32초 지연이 있는 감사
    day = dt.date(2026, 10, 15)
    audit = _ledger_audit(
        day,
        symbols=(_ledger_symbol(missing=10198),),
        tail_silence_s=12.5,
        latency=(
            _ledger_latency("H0STCNT0", (("09:01", 32.0, 20.0), ("09:02", 5.0, 3.0))),
            _ledger_latency("H0STASP0", (("09:01", 4.0, 2.0),)),
        ),
    )

    # When
    with caplog.at_level(logging.INFO):
        log_session_audit(audit, settings=_enforce_settings(), anchors=standard_session_anchors(day), date=day)

    # Then: 요약 1 + 스트림별 INFO 2 + CRITICAL 1(최악 분만)
    summary = [r for r in caplog.records if "stage=session_audit status=OK" in r.getMessage()]
    assert len(summary) == 1
    assert "trades=1600000 confirmed_missing=10198" in summary[0].getMessage()
    assert "loss_ratio=" in summary[0].getMessage()
    assert "tail_silence_s=12.5" in summary[0].getMessage()
    infos = [r for r in caplog.records if "stage=ingest_latency status=OK" in r.getMessage()]
    assert len(infos) == 2
    breach = [r for r in caplog.records if "stage=ingest_latency status=FAIL" in r.getMessage()]
    assert len(breach) == 1
    assert breach[0].levelno == logging.CRITICAL
    assert "reason=latency_exceeded" in breach[0].getMessage()
    assert "stream=H0STCNT0 minute=09:01 max_s=32.0" in breach[0].getMessage()


def test_log_session_audit_counts_breach_minutes_once_per_stream(caplog) -> None:
    import datetime as dt
    import logging

    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.eod import log_session_audit

    # Given: 한 스트림에 초과 분이 둘
    day = dt.date(2026, 10, 15)
    audit = _ledger_audit(
        day,
        symbols=(_ledger_symbol(missing=0),),
        latency=(_ledger_latency("H0STCNT0", (("09:01", 32.0, 20.0), ("09:03", 15.0, 9.0))),),
    )

    # When
    with caplog.at_level(logging.INFO):
        log_session_audit(audit, settings=_enforce_settings(), anchors=standard_session_anchors(day), date=day)

    # Then: CRITICAL은 최악 분 하나, breach_minutes=2
    breach = [r for r in caplog.records if "stage=ingest_latency status=FAIL" in r.getMessage()]
    assert len(breach) == 1
    assert "minute=09:01" in breach[0].getMessage()
    assert "breach_minutes=2" in breach[0].getMessage()


def test_log_session_audit_quiet_when_every_minute_within_threshold(caplog) -> None:
    import datetime as dt
    import logging

    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.eod import log_session_audit

    # Given: 임계 이하 분만 있는 감사
    day = dt.date(2026, 10, 15)
    audit = _ledger_audit(
        day,
        symbols=(_ledger_symbol(missing=0),),
        latency=(
            _ledger_latency("H0STCNT0", (("09:01", 9.0, 4.0),)),
            _ledger_latency("H0STASP0", (("09:02", 6.0, 3.0),)),
        ),
    )

    # When
    with caplog.at_level(logging.INFO):
        log_session_audit(audit, settings=_enforce_settings(), anchors=standard_session_anchors(day), date=day)

    # Then: CRITICAL 없음, INFO 줄은 유지
    assert not [r for r in caplog.records if r.levelno == logging.CRITICAL]
    assert len([r for r in caplog.records if "stage=ingest_latency status=OK" in r.getMessage()]) == 2


def test_log_session_audit_alert_window_excludes_premarket_minute(caplog) -> None:
    import datetime as dt
    import logging

    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.eod import log_session_audit

    # Given: 08:55에만 14초 지연
    day = dt.date(2026, 10, 15)
    audit = _ledger_audit(day, symbols=(_ledger_symbol(missing=0),), latency=(_ledger_latency("H0STCNT0", (("08:55", 14.0, 8.0),)),))

    # When
    with caplog.at_level(logging.INFO):
        log_session_audit(audit, settings=_enforce_settings(), anchors=standard_session_anchors(day), date=day)

    # Then: INFO만, CRITICAL 없음
    assert not [r for r in caplog.records if r.levelno == logging.CRITICAL]
    infos = [r for r in caplog.records if "stage=ingest_latency status=OK" in r.getMessage()]
    assert len(infos) == 2
    assert "stream=H0STCNT0 minutes=1 worst_minute=08:55 max_s=14.0" in caplog.text


def test_log_session_audit_unavailable_summary_without_tail(caplog) -> None:
    import datetime as dt
    import logging

    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.eod import log_session_audit

    # Given: tail 없는 UNAVAILABLE 감사
    day = dt.date(2026, 10, 15)
    audit = _ledger_audit(day, status="UNAVAILABLE", reason="time_budget", tail_silence_s=None)

    # When
    with caplog.at_level(logging.INFO):
        log_session_audit(audit, settings=_enforce_settings(), anchors=standard_session_anchors(day), date=day)

    # Then: 요약 WARNING과 요청된 스트림별 빈 INFO가 유지된다
    summary = [r for r in caplog.records if "stage=session_audit status=UNAVAILABLE" in r.getMessage()]
    assert len(summary) == 1
    assert summary[0].levelno == logging.WARNING
    assert "reason=time_budget" in summary[0].getMessage()
    assert "tail_silence_s=0.0" in summary[0].getMessage()
    assert len([r for r in caplog.records if "stage=ingest_latency status=OK" in r.getMessage()]) == 2


def test_log_session_audit_reports_empty_minute_streams(caplog) -> None:
    import datetime as dt
    import logging

    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.eod import log_session_audit

    # Given: 분이 비어 있는 스트림 하나
    day = dt.date(2026, 10, 15)
    audit = _ledger_audit(
        day,
        symbols=(_ledger_symbol(missing=0),),
        latency=(
            _ledger_latency("H0STCNT0", ()),
            _ledger_latency("H0STASP0", (("09:01", 3.0, 2.0),)),
        ),
    )

    # When
    with caplog.at_level(logging.INFO):
        log_session_audit(audit, settings=_enforce_settings(), anchors=standard_session_anchors(day), date=day)

    # Then: 요약과 빈 스트림을 포함한 스트림별 INFO가 유지된다
    assert [r for r in caplog.records if "stage=session_audit status=OK" in r.getMessage()]
    assert len([r for r in caplog.records if "stage=ingest_latency status=OK" in r.getMessage()]) == 2
    assert "stream=H0STCNT0 minutes=0 worst_minute=-" in caplog.text


def test_log_session_audit_contained_on_malformed_audit(caplog) -> None:
    import dataclasses
    import datetime as dt
    import logging

    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.eod import log_session_audit

    # Given: 깨진 감사(스트림 항목 None / totals None)
    day = dt.date(2026, 10, 15)
    broken_stream = _ledger_audit(day, symbols=(_ledger_symbol(missing=0),), latency=(None,))
    broken_totals = dataclasses.replace(_ledger_audit(day, symbols=(_ledger_symbol(missing=0),)), totals=None)

    # When/Then: 예외 없이 요약(또는 폴백) 줄만 남는다
    with caplog.at_level(logging.DEBUG):
        log_session_audit(broken_stream, settings=_enforce_settings(), anchors=standard_session_anchors(day), date=day)
    assert [r for r in caplog.records if "stage=session_audit status=OK" in r.getMessage()]
    assert not [r for r in caplog.records if r.levelno == logging.CRITICAL]
    caplog.clear()
    with caplog.at_level(logging.DEBUG):
        log_session_audit(broken_totals, settings=_enforce_settings(), anchors=standard_session_anchors(day), date=day)
    assert [r for r in caplog.records if "reason=summary_error" in r.getMessage()]


def test_partial_warn_band_still_enforces_legacy_budget(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    from src.orchestration.eod import check_session_reconciliation, evaluate_session_gap_budget

    day = dt.date(2026, 10, 15)
    audit = _ledger_audit(day, status="PARTIAL", reason="truncated", symbols=(_ledger_symbol(missing=1600),))
    settings = _enforce_settings()
    verdict = evaluate_session_gap_budget(
        gaps=[], date=day, regular_open=dt.time(9), regular_close=dt.time(15, 30),
        audit=audit, settings=settings,
    )
    assert verdict.status.value == "WARN"
    assert verdict.basis == "ledger+legacy"
    journal_root, manifest_path = _ledger_session(tmp_path, day, gaps=[_ledger_gap(day, 9, 10, 700)])
    with caplog.at_level(logging.INFO):
        ok = check_session_reconciliation(
            manifest_path=manifest_path, date=day, journal_root=journal_root,
            streams=("H0STCNT0", "H0STASP0"), vendor="ls", venue="krx", session="regular",
            regular_open=dt.time(9), regular_close=dt.time(15, 30), audit=audit, audit_settings=settings,
        )
    assert ok is False
    assert "reasons=gap_exceeded:700s partial=1" in caplog.text
    assert "status=WARN" not in caplog.text


def test_nonobject_manifest_is_contained_with_audit_only(tmp_path, caplog) -> None:
    import datetime as dt

    import pytest

    from src.orchestration.eod import check_session_reconciliation

    day = dt.date(2026, 10, 15)
    journal_root, manifest_path = _ledger_session(tmp_path, day)
    manifest_path.write_text("[]", encoding="utf-8")
    args = {
        "manifest_path": manifest_path, "date": day, "journal_root": journal_root,
        "streams": ("H0STCNT0", "H0STASP0"), "vendor": "ls", "venue": "krx", "session": "regular",
        "regular_open": dt.time(9), "regular_close": dt.time(15, 30),
    }
    with pytest.raises(AttributeError):
        check_session_reconciliation(**args)
    assert check_session_reconciliation(
        **args, audit=_ledger_audit(day, symbols=(_ledger_symbol(),)), audit_settings=_enforce_settings(),
    ) is False
    assert "reasons=manifest_unreadable" in caplog.text
