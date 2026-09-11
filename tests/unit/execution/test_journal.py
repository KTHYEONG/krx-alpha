
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
