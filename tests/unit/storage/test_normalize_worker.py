def test_run_isolated_normalize_returns_rows_and_sets_child_allocator_env(tmp_path) -> None:
    import subprocess
    import sys

    from src.storage.normalize_worker import run_isolated_normalize

    seen = {}

    def _runner(cmd, **kwargs):
        seen['cmd'] = cmd
        seen['kwargs'] = kwargs
        return subprocess.CompletedProcess(cmd, 0, stdout='[DATA] noise\n{"rows": 5}\n', stderr=None)

    part = tmp_path / 'l0' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01'
    out = tmp_path / 'l1' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01.parquet'

    # When
    rows = run_isolated_normalize(part, out, work_root=tmp_path / 'work', runner=_runner)

    # Then
    assert rows == 5
    assert seen['cmd'] == [
        sys.executable, '-m', 'src.storage.normalize_worker',
        '--part', str(part), '--out', str(out), '--work-root', str(tmp_path / 'work'),
    ]
    assert seen['kwargs']['env']['_RJEM_MALLOC_CONF'] == 'dirty_decay_ms:0,muzzy_decay_ms:0'
    assert seen['kwargs']['stdout'] == subprocess.PIPE
    assert seen['kwargs']['text'] is True
    assert seen['kwargs']['check'] is False

def test_run_isolated_normalize_omits_work_root_flag_when_not_given(tmp_path) -> None:
    import subprocess
    import sys

    from src.storage.normalize_worker import run_isolated_normalize

    seen = {}

    def _runner(cmd, **kwargs):
        seen['cmd'] = cmd
        return subprocess.CompletedProcess(cmd, 0, stdout='{"rows": 1}\n', stderr=None)

    part = tmp_path / 'p'
    out = tmp_path / 'o.parquet'

    assert run_isolated_normalize(part, out, runner=_runner) == 1
    assert seen['cmd'] == [sys.executable, '-m', 'src.storage.normalize_worker', '--part', str(part), '--out', str(out)]

def test_run_isolated_normalize_raises_normalization_error_on_data_fault_exit(tmp_path) -> None:
    import subprocess

    import pytest

    from src.storage.normalize_worker import EXIT_DATA_FAULT, run_isolated_normalize
    from src.storage.retention import L1NormalizationError

    def _runner(cmd, **kwargs):
        return subprocess.CompletedProcess(cmd, EXIT_DATA_FAULT, stdout='{"error": "conn_seq collision in p: 2 groups"}\n', stderr=None)

    assert EXIT_DATA_FAULT == 3
    with pytest.raises(L1NormalizationError, match='conn_seq collision'):
        run_isolated_normalize(tmp_path / 'p', tmp_path / 'o.parquet', runner=_runner)

def test_run_isolated_normalize_raises_worker_crash_on_signal_exit(tmp_path) -> None:
    import subprocess

    import pytest

    from src.storage.normalize_worker import run_isolated_normalize
    from src.storage.retention import L1NormalizationError, L1WorkerCrashError

    def _runner(cmd, **kwargs):
        return subprocess.CompletedProcess(cmd, -9, stdout='', stderr=None)

    with pytest.raises(L1WorkerCrashError, match='returncode=-9') as excinfo:
        run_isolated_normalize(tmp_path / 'p', tmp_path / 'o.parquet', runner=_runner)
    assert not isinstance(excinfo.value, L1NormalizationError)

def test_run_isolated_normalize_raises_worker_crash_when_success_payload_missing(tmp_path) -> None:
    import subprocess

    import pytest

    from src.storage.normalize_worker import run_isolated_normalize
    from src.storage.retention import L1WorkerCrashError

    for stdout in ('garbage\n', '', '{"rows": "3"}\n'):
        def _runner(cmd, _stdout=stdout, **kwargs):
            return subprocess.CompletedProcess(cmd, 0, stdout=_stdout, stderr=None)

        with pytest.raises(L1WorkerCrashError, match='returncode=0'):
            run_isolated_normalize(tmp_path / 'p', tmp_path / 'o.parquet', runner=_runner)

def test_normalize_worker_main_prints_rows_json(tmp_path, capsys) -> None:
    import json

    import zstandard as zstd

    from src.storage.normalize_worker import main

    part = tmp_path / 'l0' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True)
    rec = {'raw': 'a', 'recv_mono_ns': 1, 'recv_wall_ns': 2, 'conn_id': 'ls-1', 'conn_seq': 1, 'vendor': 'ls', 'tr_id': 'H0STCNT0'}
    (part / '09.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress((json.dumps(rec) + '\n').encode('utf-8')))
    out = tmp_path / 'l1' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01.parquet'

    # When
    code = main(['--part', str(part), '--out', str(out), '--work-root', str(tmp_path / 'work')])

    # Then
    assert code == 0
    assert json.loads(capsys.readouterr().out.strip().splitlines()[-1]) == {'rows': 1}
    assert out.exists()

def test_normalize_worker_main_returns_data_fault_exit_on_collision(tmp_path, capsys) -> None:
    import json

    import zstandard as zstd

    from src.storage.normalize_worker import EXIT_DATA_FAULT, main

    part = tmp_path / 'l0' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True)
    recs = [
        {'raw': '{"v":1}', 'recv_mono_ns': 1, 'recv_wall_ns': 100, 'conn_id': 'ls', 'conn_seq': 1, 'vendor': 'ls', 'tr_id': 'H0STCNT0'},
        {'raw': '{"v":2}', 'recv_mono_ns': 2, 'recv_wall_ns': 200, 'conn_id': 'ls', 'conn_seq': 1, 'vendor': 'ls', 'tr_id': 'H0STCNT0'},
    ]
    payload = ('\n'.join(json.dumps(r) for r in recs) + '\n').encode('utf-8')
    (part / '09.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress(payload))
    out = tmp_path / 'l1' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01.parquet'

    # When
    code = main(['--part', str(part), '--out', str(out)])

    # Then
    assert code == EXIT_DATA_FAULT
    assert 'conn_seq' in json.loads(capsys.readouterr().out.strip().splitlines()[-1])['error']
    assert out.exists() is False

def test_run_isolated_normalize_executes_real_child_process(tmp_path) -> None:
    import json

    import polars as pl
    import zstandard as zstd

    from src.storage.normalize_worker import run_isolated_normalize

    part = tmp_path / 'l0' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True)
    recs = [
        {'raw': 'a', 'recv_mono_ns': 1, 'recv_wall_ns': 20, 'conn_id': 'ls-1', 'conn_seq': 1, 'vendor': 'ls', 'tr_id': 'H0STCNT0'},
        {'raw': 'b', 'recv_mono_ns': 2, 'recv_wall_ns': 10, 'conn_id': 'ls-1', 'conn_seq': 2, 'vendor': 'ls', 'tr_id': 'H0STCNT0'},
    ]
    payload = ('\n'.join(json.dumps(r) for r in recs) + '\n').encode('utf-8')
    (part / '09.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress(payload))
    out = tmp_path / 'l1' / 'ls' / 'H0STCNT0' / 'dt=2026-09-01.parquet'

    # When
    rows = run_isolated_normalize(part, out, work_root=tmp_path / 'work')

    # Then
    assert rows == 2
    assert pl.read_parquet(out)['raw'].to_list() == ['b', 'a']



def test_normalize_worker_main_configures_logging_component(tmp_path, monkeypatch) -> None:
    import json

    import zstandard as zstd

    import src.storage.normalize_worker as worker_mod
    from src.core.config import CollectorSettings

    calls: list[tuple[str, object]] = []
    monkeypatch.setattr(worker_mod, "configure_logging", lambda component, *, log_dir=None: calls.append((component, log_dir)) or "r")
    part = tmp_path / "l0" / "ls" / "H0STCNT0" / "dt=2026-09-01"
    part.mkdir(parents=True)
    rec = {"raw": "a", "recv_mono_ns": 1, "recv_wall_ns": 2, "conn_id": "ls-1", "conn_seq": 1, "vendor": "ls", "tr_id": "H0STCNT0"}
    (part / "09.jsonl.zst").write_bytes(zstd.ZstdCompressor(level=3).compress((json.dumps(rec) + "\n").encode("utf-8")))

    monkeypatch.delenv("KRX_ALPHA_PERSISTENT_LOGS", raising=False)
    assert worker_mod.main(["--part", str(part), "--out", str(tmp_path / "o1.parquet")]) == 0
    monkeypatch.setenv("KRX_ALPHA_PERSISTENT_LOGS", "true")
    assert worker_mod.main(["--part", str(part), "--out", str(tmp_path / "o2.parquet")]) == 0

    assert calls == [("normalize-worker", None), ("normalize-worker", CollectorSettings().paths.logs_dir)]


def _write_l0_trade(part, *, event_time) -> None:
    import json

    import zstandard as zstd

    part.mkdir(parents=True, exist_ok=True)
    rec = {
        "raw": "x", "recv_mono_ns": 1, "recv_wall_ns": 2, "conn_id": "ls-1", "conn_seq": 1,
        "vendor": "ls", "tr_id": "H0STCNT0", "venue": "krx", "session": "regular",
        "stream": "H0STCNT0", "symbol": "005930", "exchange_event_time": event_time,
    }
    (part / "09.jsonl.zst").write_bytes(zstd.ZstdCompressor(level=3).compress((json.dumps(rec) + "\n").encode("utf-8")))


def _stub_collector_settings(monkeypatch, worker_mod, tmp_path):
    from src.core.paths import DataPaths

    class _Settings:
        def __init__(self, paths):
            self.paths = paths

    monkeypatch.setattr(
        worker_mod, "CollectorSettings", lambda *a, **kw: _Settings(DataPaths(tmp_path / "data"))
    )


def test_normalize_worker_labels_backlog_partition_with_own_date_anchors(tmp_path, monkeypatch) -> None:
    import datetime as dt

    import polars as pl

    import src.storage.normalize_worker as worker_mod
    from src.core.session_anchors import AnchorSource, SessionAnchors, save_session_anchors

    _stub_collector_settings(monkeypatch, worker_mod, tmp_path)
    save_session_anchors(
        tmp_path / "data" / "calendar",
        SessionAnchors(
            date=dt.date(2026, 11, 19),
            regular_open=dt.time(10, 0),
            closing_auction_start=dt.time(16, 20),
            regular_close=dt.time(16, 30),
            after_market_end=dt.time(20, 0),
            source=AnchorSource.VENDOR,
        ),
    )
    part = tmp_path / "l0" / "ls" / "H0STCNT0" / "dt=2026-11-19"
    _write_l0_trade(part, event_time="162500")
    out = tmp_path / "l1.parquet"

    assert worker_mod.main(["--part", str(part), "--out", str(out)]) == 0
    assert pl.read_parquet(out)["market_phase"].to_list() == ["closing_auction"]


def test_normalize_worker_falls_back_to_standard_windows_for_unparsable_partition(tmp_path, monkeypatch, caplog) -> None:
    import logging

    import polars as pl

    import src.storage.normalize_worker as worker_mod

    _stub_collector_settings(monkeypatch, worker_mod, tmp_path)
    part = tmp_path / "l0" / "ls" / "H0STCNT0" / "dt=not-a-date"
    _write_l0_trade(part, event_time="100000")
    out = tmp_path / "l1.parquet"

    with caplog.at_level(logging.WARNING):
        assert worker_mod.main(["--part", str(part), "--out", str(out)]) == 0

    assert pl.read_parquet(out)["market_phase"].to_list() == ["regular"]
    assert "reason=unparsable_partition" in caplog.text


def test_run_isolated_normalize_maps_io_exit_code(tmp_path) -> None:
    import subprocess

    import pytest

    from src.storage.normalize_worker import EXIT_IO_FAULT, run_isolated_normalize
    from src.storage.normalization import L1StorageIOError

    def _runner(cmd, **kwargs):
        return subprocess.CompletedProcess(cmd, EXIT_IO_FAULT, stdout='{"error": "disk full", "kind": "io"}\n', stderr=None)

    assert EXIT_IO_FAULT == 4
    with pytest.raises(L1StorageIOError, match="disk full"):
        run_isolated_normalize(tmp_path / "p", tmp_path / "o.parquet", runner=_runner)


def test_normalize_worker_main_returns_io_fault_exit_on_storage_error(tmp_path, monkeypatch, capsys) -> None:
    import json

    import src.storage.normalize_worker as worker_mod
    from src.storage.normalize_worker import EXIT_IO_FAULT, main
    from src.storage.normalization import L1StorageIOError

    def _io_error(part_dir, out_path, **kwargs):
        raise L1StorageIOError("normalize failed: no space")

    monkeypatch.setattr(worker_mod, "normalize_l0_partition", _io_error)
    part = tmp_path / "l0" / "ls" / "H0STCNT0" / "dt=2026-09-01"
    part.mkdir(parents=True)
    (part / "09.jsonl.zst").write_bytes(b"x")

    code = main(["--part", str(part), "--out", str(tmp_path / "o.parquet")])

    assert code == EXIT_IO_FAULT
    assert json.loads(capsys.readouterr().out.strip().splitlines()[-1]) == {"error": "normalize failed: no space", "kind": "io"}
