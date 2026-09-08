"""Candidates IPC unit tests."""

from __future__ import annotations


def test_ipc_write_then_read_candidates_roundtrip(tmp_path):
    # Given: 승격 후보 2건
    from src.collector.ipc import read_candidates, write_candidates

    path = tmp_path / "candidates.json"
    cands = [
        {"symbol": "005930", "selection_reasons": ["limit_up"], "priority": 0},
        {"symbol": "000660", "selection_reasons": ["surge10", "volsurge"], "priority": 1},
    ]

    # When
    write_candidates(path, cands, rev=7)
    got = read_candidates(path)

    # Then
    assert got["rev"] == 7
    assert [c["symbol"] for c in got["candidates"]] == ["005930", "000660"]


def test_ipc_read_missing_file_returns_none(tmp_path):
    # Given: 존재하지 않는 경로
    from src.collector.ipc import read_candidates

    # When / Then: 예외가 아니라 None (스트리머 최초 기동 시 정상 상태)
    assert read_candidates(tmp_path / "nope.json") is None


def test_ipc_read_corrupt_json_raises_candidate_file_error(tmp_path):
    # Given: 깨진 JSON 파일 (부분 쓰기 시뮬레이션)
    import pytest

    from src.collector.ipc import CandidateFileError, read_candidates

    path = tmp_path / "candidates.json"
    path.write_text('{"rev": 3, "candidates": [')

    # When / Then: 조용히 무시하지 않고 CandidateFileError
    with pytest.raises(CandidateFileError):
        read_candidates(path)


def test_ipc_write_candidates_is_atomic(tmp_path, monkeypatch):
    # Given: os.replace 호출을 감시
    import os as _os

    from src.collector.ipc import write_candidates

    path = tmp_path / "candidates.json"
    calls = {}
    real_replace = _os.replace

    def _spy(src, dst):
        calls["src_ne_dst"] = str(src) != str(dst)
        calls["dst"] = str(dst)
        return real_replace(src, dst)

    monkeypatch.setattr(_os, "replace", _spy)

    # When
    write_candidates(path, [{"symbol": "005930"}], rev=1)

    # Then: tmp -> os.replace(최종경로) 경유
    assert calls["src_ne_dst"] is True
    assert calls["dst"] == str(path)
    assert path.exists()
