"""L0 저널 유닛 테스트."""

from __future__ import annotations


def test_journal_flush_writes_readable_zstd_records(tmp_path):
    # Given: KST 2026-01-05 10:30 에 해당하는 벽시계 ns 로 두 레코드를 버퍼링
    import io
    import json as _json

    import zstandard as zstd

    from src.storage.journal import L0JournalWriter

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
    from src.storage.journal import L0JournalWriter

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
    from src.storage.journal import L0JournalWriter

    writer = L0JournalWriter(root=tmp_path, vendor="kis", stream="H0STASP0")

    # When / Then: flush 는 0 을 반환하고 파일을 만들지 않는다
    assert writer.flush() == 0
    assert not any(tmp_path.rglob("*.jsonl.zst"))


def test_journal_write_failure_raises_journal_write_error(tmp_path, monkeypatch):
    # Given: 디스크 쓰기가 OSError 를 던지도록 강제
    import builtins

    from src.storage.journal import JournalWriteError, L0JournalWriter

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


def test_journal_partitions_aftermarket_route_and_metadata(tmp_path):
    import json
    import zstandard as zstd
    from src.realtime.contracts import MarketSession, MarketVenue
    from src.storage.journal import L0JournalWriter
    writer = L0JournalWriter(tmp_path, 'kis', MarketVenue.NXT, MarketSession.NXT_AFTER, 'H0NXCNT0')
    writer.append(raw='005930^154001', exchange_event_time='154001', recv_mono_ns=1, recv_wall_ns=1757922001000000000, conn_id='c1', conn_seq=1)
    assert writer.flush() == 1
    path = next(tmp_path.rglob('*.jsonl.zst'))
    payload = zstd.ZstdDecompressor().decompress(path.read_bytes()).decode()
    row = json.loads(payload)
    assert ('/nxt/nxt_after/H0NXCNT0/' in path.as_posix(), row['venue'], row['session'], row['exchange_event_time']) == (True, 'nxt', 'nxt_after', '154001')


def test_journal_flush_stamps_mtime_with_newest_recv_not_write_time(tmp_path):
    # 재시작 공백·수신 나이 소비자는 mtime 을 "마지막 내구 수신" 으로 읽으므로 데이터 시각이어야 한다.
    from src.storage.journal import L0JournalWriter

    writer = L0JournalWriter(root=tmp_path, vendor="kis", stream="H0STCNT0")
    t1 = 1_735_954_200_000_000_000
    t2 = t1 + 5
    writer.append(raw="a", recv_mono_ns=1, recv_wall_ns=t1, conn_id="c", conn_seq=1)
    writer.append(raw="b", recv_mono_ns=2, recv_wall_ns=t2, conn_id="c", conn_seq=2)

    writer.flush()

    part = writer.partition_path(t1)
    assert part.stat().st_mtime_ns == t2


def test_journal_flush_stamps_each_partition_with_its_own_newest_recv(tmp_path):
    from src.storage.journal import L0JournalWriter

    writer = L0JournalWriter(root=tmp_path, vendor="kis", stream="H0STCNT0")
    h10 = 1_735_954_200_000_000_000
    h11 = h10 + 3_600_000_000_000
    writer.append(raw="a", recv_mono_ns=1, recv_wall_ns=h10, conn_id="c", conn_seq=1)
    writer.append(raw="b", recv_mono_ns=2, recv_wall_ns=h10 + 10, conn_id="c", conn_seq=2)
    writer.append(raw="c", recv_mono_ns=3, recv_wall_ns=h11, conn_id="c", conn_seq=3)

    writer.flush()

    assert writer.partition_path(h10).stat().st_mtime_ns == h10 + 10
    assert writer.partition_path(h11).stat().st_mtime_ns == h11


def test_journal_empty_flush_leaves_partition_mtime_untouched(tmp_path):
    import os

    from src.storage.journal import L0JournalWriter

    writer = L0JournalWriter(root=tmp_path, vendor="kis", stream="H0STCNT0")
    wall_ns = 1_735_954_200_000_000_000
    writer.append(raw="a", recv_mono_ns=1, recv_wall_ns=wall_ns, conn_id="c", conn_seq=1)
    writer.flush()
    part = writer.partition_path(wall_ns)
    frozen_ns = wall_ns + 999
    os.utime(part, ns=(frozen_ns, frozen_ns))

    assert writer.flush() == 0
    assert part.stat().st_mtime_ns == frozen_ns


def test_journal_flush_survives_mtime_stamp_failure_without_duplicating_records(tmp_path, monkeypatch, caplog):
    # 레코드는 이미 기록됐으므로 mtime 보정 실패가 flush 를 실패시키면 재시도가 같은 레코드를 중복 append 한다.
    import json
    import logging
    import os

    import zstandard as zstd

    from src.storage.journal import L0JournalWriter

    writer = L0JournalWriter(root=tmp_path, vendor="kis", stream="H0STCNT0")
    t1 = 1_735_954_200_000_000_000
    writer.append(raw="a", recv_mono_ns=1, recv_wall_ns=t1, conn_id="c", conn_seq=1)

    def _boom(*args, **kwargs):
        raise OSError("utime denied")

    monkeypatch.setattr(os, "utime", _boom)
    with caplog.at_level(logging.WARNING):
        assert writer.flush() == 1
    assert writer.flush() == 0

    with open(writer.partition_path(t1), "rb") as fh:
        payload = zstd.ZstdDecompressor().stream_reader(fh).read().decode()
    assert [json.loads(line)["raw"] for line in payload.splitlines()] == ["a"]
    assert any("MTIME_STAMP_FAIL" in r.getMessage() for r in caplog.records)


def test_journal_flush_never_moves_partition_mtime_backwards(tmp_path):
    from src.storage.journal import L0JournalWriter

    writer = L0JournalWriter(root=tmp_path, vendor="kis", stream="H0STCNT0")
    t_late = 1_735_954_200_000_000_000
    t_early = t_late - 1_000_000_000
    writer.append(raw="a", recv_mono_ns=1, recv_wall_ns=t_late, conn_id="c", conn_seq=1)
    writer.flush()
    writer.append(raw="b", recv_mono_ns=2, recv_wall_ns=t_early, conn_id="c", conn_seq=2)

    writer.flush()

    assert writer.partition_path(t_late).stat().st_mtime_ns == t_late
