"""HF Dataset 원격 오프로드 유닛 테스트."""

from __future__ import annotations


def test_upload_and_verify_returns_true_on_size_match(tmp_path) -> None:
    from types import SimpleNamespace
    from src.collector.remote_archive import HfDatasetArchiver

    pq = tmp_path / 'dt=2026-09-01.parquet'
    pq.write_bytes(b'x' * 100)

    class _Api:
        def __init__(self) -> None:
            self.uploaded: list[str] = []
        def upload_file(self, *, path_or_fileobj, path_in_repo, repo_id, repo_type, commit_message, **kw):
            self.uploaded.append(path_in_repo)
        def get_paths_info(self, repo_id, paths, *, repo_type, **kw):
            return [SimpleNamespace(path=p, size=100) for p in paths]

    api = _Api()
    arc = HfDatasetArchiver(token='t', repo_id='u/r', api=api)

    ok = arc.upload_and_verify(pq, 'l1/kis/H0STCNT0/dt=2026-09-01.parquet')

    assert ok is True
    assert api.uploaded == ['l1/kis/H0STCNT0/dt=2026-09-01.parquet']


def test_upload_and_verify_returns_false_on_size_mismatch(tmp_path) -> None:
    from types import SimpleNamespace
    from src.collector.remote_archive import HfDatasetArchiver

    pq = tmp_path / 'dt=2026-09-01.parquet'
    pq.write_bytes(b'x' * 100)

    class _Api:
        def __init__(self) -> None:
            self.calls: list[str] = []
        def upload_file(self, *, path_or_fileobj, path_in_repo, repo_id, repo_type, commit_message, **kw):
            self.calls.append(path_in_repo)
        def get_paths_info(self, repo_id, paths, *, repo_type, **kw):
            return [SimpleNamespace(path=p, size=7) for p in paths]

    arc = HfDatasetArchiver(token='t', repo_id='u/r', api=_Api())

    ok = arc.upload_and_verify(pq, 'l1/kis/H0STCNT0/dt=2026-09-01.parquet')

    assert ok is False


def test_upload_and_verify_raises_on_http_error(tmp_path) -> None:
    import pytest
    import requests
    from huggingface_hub.errors import HfHubHTTPError
    from src.collector.remote_archive import HfDatasetArchiver, RemoteArchiveError

    pq = tmp_path / 'dt=2026-09-01.parquet'
    pq.write_bytes(b'x' * 10)

    _resp = requests.Response()
    _resp.status_code = 500

    class _Api:
        def upload_file(self, *, path_or_fileobj, path_in_repo, repo_id, repo_type, commit_message, **kw):
            raise HfHubHTTPError('boom', response=_resp)

    arc = HfDatasetArchiver(token='t', repo_id='u/r', api=_Api())

    with pytest.raises(RemoteArchiveError, match='upload'):
        arc.upload_and_verify(pq, 'l1/kis/H0STCNT0/dt=2026-09-01.parquet')


def test_repo_path_for_builds_l1_prefixed_posix_path(tmp_path) -> None:
    from src.collector.remote_archive import HfDatasetArchiver

    arc = HfDatasetArchiver(token='t', repo_id='u/r', api=object())
    archive_root = tmp_path / 'l1'
    local = archive_root / 'kis' / 'H0STCNT0' / 'dt=2026-09-01.parquet'

    assert arc.repo_path_for(archive_root, local) == 'l1/kis/H0STCNT0/dt=2026-09-01.parquet'


def test_sync_l1_tree_uploads_new_skips_existing(tmp_path) -> None:
    from types import SimpleNamespace
    from src.collector.remote_archive import HfDatasetArchiver

    root = tmp_path / 'l1' / 'kis' / 'H0STCNT0'
    root.mkdir(parents=True)
    (root / 'dt=2026-09-01.parquet').write_bytes(b'a' * 50)
    (root / 'dt=2026-09-02.parquet').write_bytes(b'b' * 60)

    class _Api:
        def __init__(self) -> None:
            self.uploaded: list[str] = []
        def list_repo_files(self, repo_id, *, repo_type, **kw):
            return ['l1/kis/H0STCNT0/dt=2026-09-01.parquet']
        def upload_file(self, *, path_or_fileobj, path_in_repo, repo_id, repo_type, commit_message, **kw):
            self.uploaded.append(path_in_repo)
        def get_paths_info(self, repo_id, paths, *, repo_type, **kw):
            sizes = {'l1/kis/H0STCNT0/dt=2026-09-02.parquet': 60}
            return [SimpleNamespace(path=p, size=sizes[p]) for p in paths]

    api = _Api()
    arc = HfDatasetArchiver(token='t', repo_id='u/r', api=api)

    stats = arc.sync_l1_tree(tmp_path / 'l1')

    assert stats == {'uploaded': 1, 'skipped': 1, 'failed': 0}
    assert api.uploaded == ['l1/kis/H0STCNT0/dt=2026-09-02.parquet']


def test_try_from_env_returns_none_without_token(monkeypatch) -> None:
    from src.collector.remote_archive import HfDatasetArchiver

    monkeypatch.delenv('HF_TOKEN', raising=False)
    monkeypatch.delenv('HF_DATASET_REPO', raising=False)

    assert HfDatasetArchiver.try_from_env() is None


def test_try_from_env_builds_archiver_from_env(monkeypatch) -> None:
    from src.collector.remote_archive import HfDatasetArchiver

    monkeypatch.setenv('HF_TOKEN', 'tok')
    monkeypatch.setenv('HF_DATASET_REPO', 'user/krx-l1')

    arc = HfDatasetArchiver.try_from_env()

    assert isinstance(arc, HfDatasetArchiver)


def test_remote_files_filters_by_prefix() -> None:
    from src.collector.remote_archive import HfDatasetArchiver

    class _Api:
        def list_repo_files(self, repo_id, *, repo_type, **kw):
            return ['README.md', 'l1/kis/H0STCNT0/dt=2026-09-01.parquet', '.gitattributes']

    arc = HfDatasetArchiver(token='t', repo_id='u/r', api=_Api())

    assert arc.remote_files('l1/') == {'l1/kis/H0STCNT0/dt=2026-09-01.parquet'}


def test_remote_files_raises_on_http_error() -> None:
    import pytest
    import requests
    from huggingface_hub.errors import HfHubHTTPError
    from src.collector.remote_archive import HfDatasetArchiver, RemoteArchiveError

    _resp = requests.Response()
    _resp.status_code = 500

    class _Api:
        def list_repo_files(self, repo_id, *, repo_type, **kw):
            raise HfHubHTTPError('down', response=_resp)

    arc = HfDatasetArchiver(token='t', repo_id='u/r', api=_Api())

    with pytest.raises(RemoteArchiveError, match='list_repo_files'):
        arc.remote_files('l1/')


def test_sync_l1_tree_counts_failed_upload(tmp_path) -> None:
    import requests
    from huggingface_hub.errors import HfHubHTTPError
    from src.collector.remote_archive import HfDatasetArchiver

    root = tmp_path / 'l1' / 'kis' / 'H0STCNT0'
    root.mkdir(parents=True)
    (root / 'dt=2026-09-01.parquet').write_bytes(b'a' * 50)

    _resp = requests.Response()
    _resp.status_code = 503

    class _Api:
        def list_repo_files(self, repo_id, *, repo_type, **kw):
            return []
        def upload_file(self, *, path_or_fileobj, path_in_repo, repo_id, repo_type, commit_message, **kw):
            raise HfHubHTTPError('boom', response=_resp)

    arc = HfDatasetArchiver(token='t', repo_id='u/r', api=_Api())

    stats = arc.sync_l1_tree(tmp_path / 'l1')

    assert stats == {'uploaded': 0, 'skipped': 0, 'failed': 1}


def test_sync_l1_tree_counts_size_mismatch(tmp_path) -> None:
    from types import SimpleNamespace
    from src.collector.remote_archive import HfDatasetArchiver

    root = tmp_path / 'l1' / 'kis' / 'H0STCNT0'
    root.mkdir(parents=True)
    (root / 'dt=2026-09-01.parquet').write_bytes(b'a' * 50)

    class _Api:
        def list_repo_files(self, repo_id, *, repo_type, **kw):
            return []
        def upload_file(self, *, path_or_fileobj, path_in_repo, repo_id, repo_type, commit_message, **kw):
            return None
        def get_paths_info(self, repo_id, paths, *, repo_type, **kw):
            return [SimpleNamespace(path=p, size=1) for p in paths]

    arc = HfDatasetArchiver(token='t', repo_id='u/r', api=_Api())

    stats = arc.sync_l1_tree(tmp_path / 'l1')

    assert stats == {'uploaded': 0, 'skipped': 0, 'failed': 1}
