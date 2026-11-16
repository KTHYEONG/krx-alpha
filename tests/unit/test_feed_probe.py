"""Invariant tests for tools/feed_probe.py (read-only feed probe); every vendor boundary is faked."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime as dt
import importlib.util
import json
import pathlib
import sys
from collections.abc import Sequence
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from src.core.session_anchors import standard_session_anchors
from src.realtime.contracts import VendorAuthRejected, VendorDisconnected
from src.realtime.ws_lease import LeaseRole, LeaseUnavailable

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
MODULE_NAME = "feed_probe_under_test"
KST = ZoneInfo("Asia/Seoul")
MONDAY_10 = dt.datetime(2026, 11, 16, 10, 0, tzinfo=KST)
SYMBOLS = [f"{n:06d}" for n in range(1, 101)]
TOKEN = "tok-SECRET-0123456789"


def _load() -> Any:
    if MODULE_NAME in sys.modules:
        return sys.modules[MODULE_NAME]
    spec = importlib.util.spec_from_file_location(MODULE_NAME, REPO_ROOT / "tools" / "feed_probe.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


fp = _load()


def _at(when: dt.datetime) -> float:
    return when.timestamp()


@pytest.fixture(autouse=True)
def _standard_anchors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fp, "_session_anchors", lambda day: standard_session_anchors(day))


class FakeIO:
    """Accepts pairs until `reject_after` have been accepted, then rejects; frames never arrive."""

    def __init__(self, capacity: int = 200, reject_after: int | None = None) -> None:
        self.capacity = capacity
        self.reject_after = reject_after
        self.accepted = 0
        self.added: list[int] = []
        self.closed = False

    @property
    def secrets(self) -> tuple[str, ...]:
        return (TOKEN,)

    async def connect(self) -> None:
        return None

    async def add(self, pairs: list[Any]) -> list[Any]:
        self.added.append(len(pairs))
        out: list[Any] = []
        for pair in pairs:
            ok = self.reject_after is None or self.accepted < self.reject_after
            if ok:
                self.accepted += 1
            out.append((pair, ok, "00000" if ok else "rejected"))
        return out

    async def next_frame(self) -> Any:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    def renew(self) -> bool:
        return True

    async def close(self) -> None:
        self.closed = True


def _run(vendor: str, out: pathlib.Path, journal: pathlib.Path, ramp: Sequence[int], when: dt.datetime, **kw: Any) -> Any:
    return asyncio.run(
        fp.run_probe(
            vendor,
            symbols=SYMBOLS,
            streams=("trade", "book"),
            ramp=ramp,
            hold_s=0.0,
            out_dir=out,
            journal_root=journal,
            now=lambda: _at(when),
            **kw,
        )
    )


def test_toss_never_issues_token(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    monkeypatch.setenv("TOSS_APP_KEY", "test-app-key-0001")
    monkeypatch.setenv("KRX_ALPHA_KIS_TOKEN_CACHE_DIR", str(tmp_path / "cache"))

    def no_http(*_: object, **__: object) -> None:
        raise AssertionError("no HTTP session may be created when the token is missing")

    monkeypatch.setattr(fp.aiohttp, "ClientSession", no_http)
    summary = _run("toss", tmp_path / "out", tmp_path / "l0", [10], MONDAY_10)
    assert summary.exit_code == fp.EXIT_NO_TOKEN
    assert not (tmp_path / "out").exists()


def test_toss_without_app_key_refuses_with_no_token(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    monkeypatch.delenv("TOSS_APP_KEY", raising=False)
    summary = _run("toss", tmp_path / "out", tmp_path / "l0", [10], MONDAY_10)
    assert summary.exit_code == fp.EXIT_NO_TOKEN


def test_toss_subscribe_types_whitelist() -> None:
    assert fp.toss_declaration([("trade:kr", ["005930"]), ("orderbook:kr", ["005930"])])
    with pytest.raises(ValueError, match="not permitted"):
        fp.toss_declaration([("personal:order", ["1"])])
    with pytest.raises(ValueError, match="not permitted"):
        fp.toss_declaration([("trade:us", ["AAPL"])])


class _FakeToss:
    def __init__(self) -> None:
        self.sent: list[float] = []

    async def send_str(self, _: str) -> None:
        self.sent.append(clock.now)


class _Clock:
    now = 0.0


clock = _Clock()


async def _fake_sleep(seconds: float) -> None:
    clock.now += seconds


def test_toss_declaration_rate_limited() -> None:
    io = fp._TossIO("t", clock=lambda: clock.now, sleep=_fake_sleep)
    io._ws = _FakeToss()
    io._declared = [(s, "trade") for s in SYMBOLS[:4]]

    async def run() -> None:
        for _ in range(40):
            await io._declare()

    asyncio.run(run())
    sent = io._ws.sent
    assert len(sent) == 40
    for start in sent:
        assert sum(1 for t in sent if start <= t < start + 1.0) <= 5


def test_redaction_removes_token_and_app_key() -> None:
    app_key = "appkey-ABCDEFGH-123"
    raw = f'{{"header":{{"token":"{TOKEN}","tr_type":"3"}},"body":{{"app_key":"{app_key}","acnt_no":"12345678901"}}}}'
    text = fp.redact(f"Authorization: Bearer {TOKEN}\n{raw}", (TOKEN, app_key))
    assert TOKEN not in text
    assert app_key not in text
    assert "12345678901" not in text
    assert "<redacted>" in text


def test_out_inside_journal_root_refused(tmp_path: pathlib.Path) -> None:
    journal = tmp_path / "l0"
    summary = _run("db", journal / "probe", journal, [10], MONDAY_10)
    assert summary.exit_code == fp.EXIT_USAGE


def test_ramp_stops_at_first_rejection_and_reports_cap(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    fake = FakeIO(capacity=200, reject_after=50)

    async def open_fake(vendor: str) -> FakeIO:
        return fake

    monkeypatch.setattr(fp, "_open_io", open_fake)
    summary = _run("db", tmp_path / "out", tmp_path / "l0", [10, 25, 50, 100, 200], MONDAY_10)
    assert summary.cap_pairs == 50
    assert summary.stop_reason == "rejected"
    assert [step.total_pairs for step in summary.steps] == [10, 25, 50, 100]
    assert summary.steps[-1].rejected == {"rejected": 50}
    assert fake.closed
    assert (tmp_path / "out" / "capture.db.100.jsonl").exists()
    assert json.loads((tmp_path / "out" / "summary.db.json").read_text())["cap_pairs"] == 50


def test_db_local_capacity_stops_without_second_session(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    fake = FakeIO(capacity=50)

    async def open_fake(vendor: str) -> FakeIO:
        return fake

    monkeypatch.setattr(fp, "_open_io", open_fake)
    summary = _run("db", tmp_path / "out", tmp_path / "l0", [10, 50, 100], MONDAY_10)
    assert summary.stop_reason == "local_capacity"
    assert summary.cap_pairs == 50
    assert "second session not tried" in (summary.detail or "")
    assert fake.added == [10, 40]


def test_forbidden_windows_refuse() -> None:
    std = standard_session_anchors(dt.date(2026, 11, 16))

    def at(hour: int, minute: int, weekday_date: dt.date = dt.date(2026, 11, 16)) -> dt.datetime:
        return dt.datetime.combine(weekday_date, dt.time(hour, minute), tzinfo=KST)

    assert fp.forbidden_reason("ls-nxt", at(9, 0), std)
    assert fp.forbidden_reason("ls-nxt", at(15, 20), std)
    assert fp.forbidden_reason("ls-nxt", at(9, 6), std) is None
    assert fp.forbidden_reason("db", at(9, 0), std)
    assert fp.forbidden_reason("db", at(9, 10), std) is None
    assert fp.forbidden_reason("db", at(15, 20), std)
    assert fp.forbidden_reason("db", at(15, 50), std) is None
    assert fp.forbidden_reason("toss", at(9, 20), std)
    assert fp.forbidden_reason("toss", at(10, 0), std) is None
    assert fp.forbidden_reason("db", at(10, 0, dt.date(2026, 11, 15)), std)


def test_forbidden_windows_follow_shifted_anchors() -> None:
    base = standard_session_anchors(dt.date(2026, 11, 16))
    shifted = dataclasses.replace(
        base,
        regular_open=dt.time(9, 10),
        closing_auction_start=dt.time(15, 20),
        regular_close=dt.time(15, 40),
    )
    at_0915 = dt.datetime(2026, 11, 16, 9, 15, tzinfo=KST)
    at_0920 = dt.datetime(2026, 11, 16, 9, 20, tzinfo=KST)
    assert fp.forbidden_reason("db", at_0915, shifted)
    assert fp.forbidden_reason("db", at_0920, shifted) is None


def test_clock_unsynced_fails_closed(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    monkeypatch.setattr(fp, "measure_ntp_offset_ns", lambda host: 3_000_000_000)
    out = tmp_path / "out"
    code = fp.main(["--vendor", "db", "--symbols", "005930", "--out", str(out)])
    assert code == fp.EXIT_CLOCK
    assert not out.exists()


def test_lease_acquired_before_connect_and_released_on_close() -> None:
    events: list[str] = []

    class Ledger:
        def acquire(self, owner: str, role: LeaseRole) -> object:
            events.append(f"acquire:{role.value}")
            return object()

        def renew(self, lease: object) -> bool:
            return True

        def release(self, lease: object) -> None:
            events.append("release")

    class Adapter:
        capacity_pairs = 100

        async def connect(self) -> None:
            events.append("connect")

        async def aclose(self) -> None:
            events.append("aclose")

    class Http:
        async def close(self) -> None:
            events.append("http_close")

    io = fp._LsNxtIO(Adapter(), Http(), (), Ledger(), owner="probe-test")

    async def run() -> None:
        await io.connect()
        await io.close()

    asyncio.run(run())
    assert events[:2] == ["acquire:standby", "connect"]
    assert events[-1] == "release"


def test_lease_released_when_connect_raises() -> None:
    events: list[str] = []

    class Ledger:
        def acquire(self, owner: str, role: LeaseRole) -> object:
            return object()

        def renew(self, lease: object) -> bool:
            return True

        def release(self, lease: object) -> None:
            events.append("release")

    class Adapter:
        capacity_pairs = 100

        async def connect(self) -> None:
            raise VendorDisconnected("token_store_failed:OSError")

    class Http:
        async def close(self) -> None:
            return None

    io = fp._LsNxtIO(Adapter(), Http(), (), Ledger(), owner="probe-test")
    with pytest.raises(VendorDisconnected):
        asyncio.run(io.connect())
    assert events == ["release"]


def test_lease_denied_maps_to_refused() -> None:
    class Ledger:
        def acquire(self, owner: str, role: LeaseRole) -> object:
            raise LeaseUnavailable("slots full")

    io = fp._LsNxtIO(object(), object(), (), Ledger(), owner="probe-test")  # type: ignore[arg-type]
    with pytest.raises(fp._RefusedError) as info:
        asyncio.run(io.connect())
    assert info.value.exit_code == fp.EXIT_REFUSED


def test_preempted_lease_ends_hold() -> None:
    class PreemptedIO(FakeIO):
        def renew(self) -> bool:
            return False

    recorder = fp._Recorder(pathlib.Path("/nonexistent-unused"), "ls-nxt", lambda: ())
    reason = asyncio.run(fp._hold(PreemptedIO(), recorder, 1.0))
    assert reason == "lease_preempted"


def test_nxt_subscribe_key_and_ack_guard() -> None:
    adapter = fp._LsNxtAdapter.__new__(fp._LsNxtAdapter)
    assert adapter._subscribe_body("005930", "H0STCNT0") == {"tr_cd": "NS3", "tr_key": "N005930   "}
    assert len("N005930   ") == 10
    with pytest.raises(ValueError, match="6 digits"):
        adapter._subscribe_body("ABC", "H0STCNT0")
    assert adapter._data_frame({}, {"rsp_cd": "00000", "tr_cd": "NS3", "tr_key": "N005930   "}, "{}") is None


def test_parse_ramp_and_pairs_validation() -> None:
    assert fp._parse_ramp("10,25,50") == [10, 25, 50]
    with pytest.raises(fp.ProbeUsageError):
        fp.validate_ramp([10, 10], 100)
    with pytest.raises(fp.ProbeUsageError):
        fp.validate_ramp([10, 200], 100)
    with pytest.raises(fp.ProbeUsageError):
        fp.build_pairs(["005930"], ["trade", "fill"])


def test_disconnect_at_connect_is_rejected_exit(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    class Dead(FakeIO):
        async def connect(self) -> None:
            raise VendorAuthRejected("auth_rejected:401")

    async def open_fake(vendor: str) -> Dead:
        return Dead()

    monkeypatch.setattr(fp, "_open_io", open_fake)
    summary = _run("toss", tmp_path / "out", tmp_path / "l0", [10], MONDAY_10)
    assert summary.exit_code == fp.EXIT_REJECTED
    assert dataclasses.is_dataclass(summary)
