"""Scan bridge unit tests."""

from __future__ import annotations


def test_emit_candidates_maps_selection_rows_and_writes_atomically(tmp_path):
    # Given: select_universe 결과와 동형인 행 목록
    from src.collector.ipc import read_candidates
    from src.collector.scan_bridge import emit_candidates

    rows = [
        {"symbol": "005930", "selection_reasons": ["limit_up", "surge10"]},
        {"symbol": "000660", "selection_reasons": ["volsurge"]},
    ]
    path = tmp_path / "candidates.json"

    # When
    n = emit_candidates(path, rows, rev=20260908)

    # Then: 2건 발행 + read_candidates 로 복원, symbol 은 문자열, reasons 는 리스트
    assert n == 2
    got = read_candidates(path)
    assert got["rev"] == 20260908
    assert got["candidates"][0]["symbol"] == "005930"
    assert got["candidates"][0]["selection_reasons"] == ["limit_up", "surge10"]
