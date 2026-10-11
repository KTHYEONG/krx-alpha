
def test_journal_appends_redacted_jsonl_per_day(tmp_path) -> None:
    # Given
    import json

    from src.core.config import ExecutionMode
    from src.execution.journal import OrderJournal
    from tests.unit.execution.fakes import T0, FixedClock, make_quote

    clock = FixedClock(T0)
    journal = OrderJournal(root=tmp_path / "journal", mode=ExecutionMode.PAPER, now=clock)

    # When
    rec = journal.append(
        "paper_would_send",
        body={"CANO": "12345678", "PDNO": "005930"},
        headers={"authorization": "Bearer secret-token", "AppKey": "k"},
        quote=make_quote(),
    )
    journal.append("fill", client_id="paper-20260911-000001", delta_qty=3)
    clock.advance(86_400)
    journal.append("session_start", holdings_count=0)

    # Then: 일자별 파일 분리
    day1 = (tmp_path / "journal" / "2026-09-11.jsonl").read_text(encoding="utf-8").splitlines()
    day2 = (tmp_path / "journal" / "2026-09-12.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(day1) == 2
    assert len(day2) == 1

    # Then: 구조화 필드 + 민감정보 마스킹
    first = json.loads(day1[0])
    assert first["event"] == "paper_would_send"
    assert first["mode"] == "paper"
    assert first["ts"].startswith("2026-09-11T09:00:00")
    assert first["body"] == {"CANO": "***", "PDNO": "005930"}
    assert first["headers"] == {"authorization": "***", "AppKey": "***"}
    assert first["quote"]["asks"] == [[10_010, 5], [10_020, 5]]
    assert rec["body"]["CANO"] == "***"
    joined = "\n".join(day1)
    assert "12345678" not in joined
    assert "secret-token" not in joined

def test_read_day_returns_only_own_mode_records_in_order(tmp_path) -> None:
    # Given: paper/mock/live 기록이 뒤섞인 하루 파일
    import datetime as dt

    from src.core.config import ExecutionMode
    from src.execution.journal import OrderJournal
    from tests.unit.execution.fakes import T0, FixedClock

    root = tmp_path / "journal"
    clock = FixedClock(T0)
    paper = OrderJournal(root=root, mode=ExecutionMode.PAPER, now=clock)
    mock = OrderJournal(root=root, mode=ExecutionMode.MOCK, now=clock)
    live = OrderJournal(root=root, mode=ExecutionMode.LIVE, now=clock)
    paper.append("paper_would_send", client_id="p1")
    mock.append("intent", client_id="m1")
    live.append("live_send", client_id="l1")
    mock.append("submit_result", client_id="m1")

    # When
    records = mock.read_day(dt.date(2026, 9, 11))

    # Then
    assert [r["event"] for r in records] == ["intent", "submit_result"]
    assert all(r["mode"] == "mock" for r in records)

def test_read_day_missing_day_is_empty(tmp_path) -> None:
    # Given / When
    import datetime as dt

    from src.core.config import ExecutionMode
    from src.execution.journal import OrderJournal
    from tests.unit.execution.fakes import T0, FixedClock

    journal = OrderJournal(root=tmp_path / "journal", mode=ExecutionMode.MOCK, now=FixedClock(T0))

    # Then
    assert journal.read_day(dt.date(2026, 9, 11)) == []

def test_read_day_torn_tail_skipped_mid_file_corruption_fails_closed(tmp_path, caplog) -> None:
    # Given
    import datetime as dt
    import json
    import logging

    import pytest

    from src.core.config import ExecutionMode
    from src.execution.contracts import ExecutionError
    from src.execution.journal import OrderJournal
    from tests.unit.execution.fakes import T0, FixedClock

    journal = OrderJournal(root=tmp_path / "journal", mode=ExecutionMode.MOCK, now=FixedClock(T0))
    day = dt.date(2026, 9, 11)
    path = tmp_path / "journal" / "2026-09-11.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    rec = {"ts": "2026-09-11T09:00:00+09:00", "mode": "mock", "event": "intent", "client_id": "m1"}
    path.write_text(json.dumps(rec) + "\n" + '{"ts": "2026-09-11T09:00:01+09:00", "mode": "mock", "even', encoding="utf-8")

    # When: 마지막 줄 찢김은 건너뛴다
    with caplog.at_level(logging.WARNING):
        records = journal.read_day(day)

    # Then
    assert [r["client_id"] for r in records] == ["m1"]
    assert any(record.levelno == logging.WARNING for record in caplog.records)

    # Given: 중간 줄 파손
    path.write_text(
        json.dumps(rec) + "\n" + "{broken\n" + json.dumps({**rec, "client_id": "m2"}) + "\n",
        encoding="utf-8",
    )

    # When / Then: fail-closed
    before = path.read_text(encoding="utf-8")
    with pytest.raises(ExecutionError):
        journal.read_day(day)
    assert path.read_text(encoding="utf-8") == before


def test_utf8_torn_tail_cannot_poison_later_appends(tmp_path, caplog) -> None:
    import json
    import logging

    import pytest

    from src.core.config import ExecutionMode
    from src.execution.contracts import ExecutionError
    from src.execution.journal import OrderJournal
    from tests.unit.execution.fakes import T0

    journal = OrderJournal(root=tmp_path, mode=ExecutionMode.MOCK, now=lambda: T0)
    first = journal.append("session_start", holdings=[])
    path = tmp_path / f"{T0.date().isoformat()}.jsonl"
    intact = path.read_bytes()
    for tail in (b'{"event":"', b'{"event":"\xed\x95'):
        path.write_bytes(intact + tail)
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            assert journal.read_day(T0.date()) == [first]
        assert len(caplog.records) == 1
        before = path.read_bytes()
        with pytest.raises(ExecutionError, match="cannot append"):
            journal.append("intent", client_id="mock-20260911-000001")
        assert path.read_bytes() == before

    path.write_bytes(b"\xff\n" + json.dumps(first).encode() + b"\n")
    with pytest.raises(ExecutionError, match="corrupt journal"):
        journal.read_day(T0.date())

def test_read_day_skips_blank_lines_and_rejects_non_dict_records(tmp_path) -> None:
    # Given
    import datetime as dt
    import json

    import pytest

    from src.core.config import ExecutionMode
    from src.execution.contracts import ExecutionError
    from src.execution.journal import OrderJournal
    from tests.unit.execution.fakes import T0, FixedClock

    journal = OrderJournal(root=tmp_path / "journal", mode=ExecutionMode.MOCK, now=FixedClock(T0))
    day = dt.date(2026, 9, 11)
    path = tmp_path / "journal" / "2026-09-11.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    rec = {"ts": "2026-09-11T09:00:00+09:00", "mode": "mock", "event": "intent", "client_id": "m1"}

    path.write_text(json.dumps(rec) + "\n\n   \n", encoding="utf-8")
    records = journal.read_day(day)

    # Then
    assert [r["client_id"] for r in records] == ["m1"]
    path.write_text(json.dumps(rec) + "\n" + json.dumps([1, 2]) + "\n", encoding="utf-8")
    with pytest.raises(ExecutionError):
        journal.read_day(day)

    # When / Then: 중간 비딕셔너리 줄은 fail-closed
    path.write_text(json.dumps(rec) + "\n" + json.dumps([1, 2]) + "\n" + json.dumps(rec) + "\n", encoding="utf-8")
    with pytest.raises(ExecutionError):
        journal.read_day(day)
