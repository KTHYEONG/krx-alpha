"""L0 저널 유닛 테스트."""

from __future__ import annotations


def test_journal_flush_writes_readable_zstd_records(tmp_path):
    # Given: KST 2026-01-05 10:30 에 해당하는 벽시계 ns 로 두 레코드를 버퍼링
    import io
    import json as _json

    import zstandard as zstd

    from src.collector.journal import L0JournalWriter

    writer = L0JournalWriter(root=tmp_path, vendor="kis", stream="H0STCNT0")
    wall_ns = 1_735_954_200_000_000_000  # 2025-01-04T10:30:00+09:00 근처 (임의 고정 ns)
    writer.append(raw="0|H0STCNT0|001|a^b^c", recv_mono_ns=111, recv_wall_ns=wall_ns, conn_id="c1", conn_seq=1)
    writer.append(raw="0|H0STCNT0|001|d^e^f", recv_mono_ns=222, recv_wall_ns=wall_ns + 5, conn_id="c1", conn_seq=2)

    # When
    written = writer.flush()

    # Then: 파티션 파일이 생기고 zstd 해제 시 원문 2줄이 JSON 라인으로 복원된다
    assert written == 2
    part = writer.partition_path(wall_ns)
    assert part.exists()
    dctx = zstd.ZstdDecompressor()
    with open(part, "rb") as fh:
        text = dctx.stream_reader(io.BytesIO(fh.read()), read_across_frames=True).read().decode()
    lines = [ln for ln in text.splitlines() if ln]
    assert len(lines) == 2
    rec0 = _json.loads(lines[0])
    assert rec0["raw"] == "0|H0STCNT0|001|a^b^c"
    assert rec0["conn_seq"] == 1
    assert rec0["recv_mono_ns"] == 111
    assert rec0["vendor"] == "kis"
    assert rec0["tr_id"] == "H0STCNT0"


def test_journal_rotates_partition_by_hour(tmp_path):
    # Given: 서로 다른 시(hour)에 속하는 두 벽시계 ns
    from src.collector.journal import L0JournalWriter

    writer = L0JournalWriter(root=tmp_path, vendor="kiwoom", stream="0B")
    h10 = 1_735_954_200_000_000_000
    h11 = h10 + 3_600_000_000_000

    # When: 두 레코드를 각각 append 후 flush
    writer.append(raw="x", recv_mono_ns=1, recv_wall_ns=h10, conn_id="c", conn_seq=1)
    writer.append(raw="y", recv_mono_ns=2, recv_wall_ns=h11, conn_id="c", conn_seq=2)
    writer.flush()

    # Then: 시간대별로 별도 파티션 파일이 생성된다
    p10 = writer.partition_path(h10)
    p11 = writer.partition_path(h11)
    assert p10 != p11
    assert p10.exists()
    assert p11.exists()


def test_journal_flush_empty_buffer_returns_zero(tmp_path):
    # Given: 아무것도 append 하지 않은 writer
    from src.collector.journal import L0JournalWriter

    writer = L0JournalWriter(root=tmp_path, vendor="kis", stream="H0STASP0")

    # When / Then: flush 는 0 을 반환하고 파일을 만들지 않는다
    assert writer.flush() == 0
    assert not any(tmp_path.rglob("*.jsonl.zst"))


def test_journal_write_failure_raises_journal_write_error(tmp_path, monkeypatch):
    # Given: 디스크 쓰기가 OSError 를 던지도록 강제
    import builtins

    from src.collector.journal import JournalWriteError, L0JournalWriter

    writer = L0JournalWriter(root=tmp_path, vendor="kis", stream="H0STCNT0")
    writer.append(raw="z", recv_mono_ns=1, recv_wall_ns=1_735_954_200_000_000_000, conn_id="c", conn_seq=1)

    real_open = builtins.open

    def _boom(path, mode="r", *a, **k):
        if "b" in mode and str(path).endswith(".jsonl.zst"):
            raise OSError("No space left on device")
        return real_open(path, mode, *a, **k)

    monkeypatch.setattr(builtins, "open", _boom)

    # When / Then: 조용히 삼키지 않고 JournalWriteError 로 표면화
    import pytest

    with pytest.raises(JournalWriteError, match="space"):
        writer.flush()
