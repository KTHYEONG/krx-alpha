import argparse
import json

import aiohttp


def _args(**kw):
    base = {"symbols": "005930", "streams": "H0STCNT0,H0STASP0", "seconds": 0.2, "out": "o.jsonl"}
    base.update(kw)
    return argparse.Namespace(**base)


class _FakeHttp:
    async def close(self):
        return None


class _Msg:
    def __init__(self, t, d=None):
        self.type = t
        self.data = d


def _install(monkeypatch, ws, acks, frames, token="SECRET-TOKEN"):
    from src.cli import probe_db_stream
    from src.realtime.contracts import VendorDisconnected

    class _WS:
        """Mirrors aiohttp: receive_str() delegates to self.receive(timeout) positionally."""

        def __init__(self):
            self._q = [_Msg(aiohttp.WSMsgType.TEXT, a) for a in ws.get("acks", [])] + list(ws.get("msgs", []))

        async def receive_str(self, **_kw):
            msg = await self.receive(None)
            if msg.type is not aiohttp.WSMsgType.TEXT:
                raise TypeError(f"not text: {msg.type}")
            return msg.data

        async def receive(self, *_a):
            return self._q.pop(0)

    fake_ws = _WS()

    class _FakeAdapter:
        def __init__(self, **kw):
            self._token = token
            self._ws = fake_ws
            self.unparsable_frames = 0
            self._frames = list(frames)
            self._acks = list(acks)

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            for _ in pairs:
                await self._ws.receive_str()
            return list(self._acks)

        async def recv(self):
            if self._frames:
                return self._frames.pop(0)
            raise VendorDisconnected("closed")

        async def aclose(self):
            return None

    monkeypatch.setenv("DB_APP_KEY", "k")
    monkeypatch.setenv("DB_APP_SECRET", "s")
    monkeypatch.setattr(probe_db_stream, "DbRealtimeAdapter", _FakeAdapter)
    monkeypatch.setattr(aiohttp, "ClientSession", lambda *a, **k: _FakeHttp())


def test_probe_writes_raw_tap_and_redacts_token(tmp_path, monkeypatch, capsys) -> None:
    import time

    from src.cli import probe_db_stream
    from src.realtime.contracts import L0Frame, VendorAck

    ack = json.dumps({"header": {"tr_cd": "S00", "rsp_cd": "00000"}, "body": None, "tok": "SECRET-TOKEN"})
    ack2 = json.dumps({"header": {"tr_cd": "S01", "rsp_cd": "10017"}, "body": None})
    out = tmp_path / "probe.jsonl"
    frame = L0Frame("db", "H0STCNT0", "005930", '{"header":{}}', time.monotonic_ns(), time.time_ns(), 1, "db-1")
    _install(
        monkeypatch,
        {"acks": [ack, ack2]},
        [VendorAck("005930", "H0STCNT0", True, "00000"), VendorAck("005930", "H0STASP0", False, "10017")],
        [frame],
    )
    rc = probe_db_stream.run(_args(out=str(out)))
    assert rc == 0
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    assert lines
    assert all("SECRET-TOKEN" not in line for line in lines)
    assert any("<token>" in line for line in lines)


def test_probe_summary_lists_keys_not_values(tmp_path, monkeypatch, capsys) -> None:
    import time

    from src.cli import probe_db_stream
    from src.realtime.contracts import L0Frame, VendorAck

    ack = json.dumps({"header": {"tr_cd": "S01", "rsp_cd": "00000"}, "body": None})
    raw = json.dumps({"header": {"tr_cd": "S01", "tr_key": "J 005930"}, "body": {"ShrnIscd": "005930", "Askp1": 70500}})
    out = tmp_path / "probe.jsonl"
    frame = L0Frame("db", "H0STASP0", "005930", raw, time.monotonic_ns(), time.time_ns(), 1, "db-1")
    _install(monkeypatch, {"acks": [ack]}, [VendorAck("005930", "H0STASP0", True, "00000")], [frame])
    rc = probe_db_stream.run(_args(symbols="005930", streams="H0STASP0", out=str(out)))
    assert rc == 0
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert "ShrnIscd" in summary["first_body_keys"]
    assert "Askp1" in summary["first_body_keys"]
    assert "70500" not in json.dumps(summary)


def test_probe_fails_gate_when_accepted_pair_silent(tmp_path, monkeypatch, capsys) -> None:
    import time

    from src.cli import probe_db_stream
    from src.realtime.contracts import L0Frame, VendorAck

    acks_raw = [
        json.dumps({"header": {"tr_cd": "S00", "rsp_cd": "00000"}, "body": None}),
        json.dumps({"header": {"tr_cd": "S00", "rsp_cd": "00000"}, "body": None}),
    ]
    out = tmp_path / "probe.jsonl"
    frame = L0Frame("db", "H0STCNT0", "005930", '{"header":{}}', time.monotonic_ns(), time.time_ns(), 1, "db-1")
    _install(
        monkeypatch,
        {"acks": acks_raw},
        [VendorAck("005930", "H0STCNT0", True, "00000"), VendorAck("000660", "H0STCNT0", True, "00000")],
        [frame],
    )
    rc = probe_db_stream.run(_args(symbols="005930,000660", streams="H0STCNT0", out=str(out)))
    assert rc != 0


def test_probe_rejects_out_path_inside_l0(tmp_path, monkeypatch, capsys) -> None:
    from src.cli import probe_db_stream

    monkeypatch.setenv("DB_APP_KEY", "k")
    monkeypatch.setenv("DB_APP_SECRET", "s")
    called = []
    monkeypatch.setattr(probe_db_stream, "DbRealtimeAdapter", lambda **kw: called.append(True) or 1 / 0)
    rc = probe_db_stream.run(_args(out="data/l0/x.jsonl"))
    assert rc != 0
    assert called == []


def test_probe_rejects_invalid_args_without_connecting(tmp_path, monkeypatch) -> None:
    from src.cli import probe_db_stream

    called = []
    monkeypatch.setattr(probe_db_stream, "DbRealtimeAdapter", lambda **kw: called.append(True))
    bad = [
        {"symbols": ""},
        {"symbols": ",".join(["000001"] * 26)},
        {"streams": "BOGUS"},
        {"seconds": 0},
        {"seconds": 901},
    ]
    for kw in bad:
        rc = probe_db_stream.run(_args(out=str(tmp_path / "o.jsonl"), **kw))
        assert rc == 2
    assert called == []


def test_probe_taps_ws_receive_texts_and_counts_ping(tmp_path, monkeypatch, capsys) -> None:
    import time

    import aiohttp

    from src.cli import probe_db_stream
    from src.realtime.contracts import L0Frame, VendorAck, VendorDisconnected

    ack = json.dumps({"header": {"tr_cd": "S00", "rsp_cd": "00000"}, "body": None})
    raw = json.dumps({"header": {"tr_cd": "S00", "tr_key": "J 005930"}, "body": {"ShrnIscd": "005930"}})
    out = tmp_path / "probe.jsonl"

    class _WS:
        def __init__(self):
            self._acks = [ack]
            self._msgs = [_Msg(aiohttp.WSMsgType.PING), _Msg(aiohttp.WSMsgType.TEXT, raw)]

        async def receive_str(self, **_kw):
            return (await self.receive(None)).data

        async def receive(self, *_a):
            if self._acks:
                return _Msg(aiohttp.WSMsgType.TEXT, self._acks.pop(0))
            try:
                return self._msgs.pop(0)
            except IndexError:
                raise VendorDisconnected("closed") from None

    fake_ws = _WS()

    class _RecvAdapter:
        def __init__(self, **kw):
            self._token = ""
            self._ws = fake_ws
            self.unparsable_frames = 0

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            for _ in pairs:
                await self._ws.receive_str()
            return [VendorAck("005930", "H0STCNT0", True, "00000")]

        async def recv(self):
            while True:
                msg = await self._ws.receive()
                if msg.type is aiohttp.WSMsgType.TEXT:
                    return L0Frame(
                        "db", "H0STCNT0", "005930", msg.data, time.monotonic_ns(), time.time_ns(), 1, "db-1"
                    )

        async def aclose(self):
            return None

    monkeypatch.setenv("DB_APP_KEY", "k")
    monkeypatch.setenv("DB_APP_SECRET", "s")
    monkeypatch.setattr(probe_db_stream, "DbRealtimeAdapter", _RecvAdapter)
    monkeypatch.setattr(aiohttp, "ClientSession", lambda *a, **k: _FakeHttp())
    rc = probe_db_stream.run(_args(symbols="005930", streams="H0STCNT0", out=str(out)))
    assert rc == 0
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["ping_count"] == 1
    assert summary["frame_counts"] == {"005930|H0STCNT0": 1}
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2


def test_probe_subscribe_failure_flushes_tap_and_fails(tmp_path, monkeypatch) -> None:
    from src.cli import probe_db_stream
    from src.realtime.contracts import VendorDisconnected

    ack = json.dumps({"header": {"tr_cd": "S00", "rsp_cd": "00000"}, "body": None})
    out = tmp_path / "probe.jsonl"

    class _WS:
        def __init__(self):
            self._acks = [ack]

        async def receive_str(self, **_kw):
            return (await self.receive(None)).data

        async def receive(self, *_a):
            if self._acks:
                return _Msg(aiohttp.WSMsgType.TEXT, self._acks.pop(0))
            raise AssertionError("recv must not run after subscribe failure")

    fake_ws = _WS()

    class _FailAdapter:
        def __init__(self, **kw):
            self._token = ""
            self._ws = fake_ws
            self.unparsable_frames = 0

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            await self._ws.receive_str()
            raise VendorDisconnected("boom")

        async def recv(self):
            raise AssertionError("unreachable")

        async def aclose(self):
            return None

    monkeypatch.setenv("DB_APP_KEY", "k")
    monkeypatch.setenv("DB_APP_SECRET", "s")
    monkeypatch.setattr(probe_db_stream, "DbRealtimeAdapter", _FailAdapter)
    monkeypatch.setattr(aiohttp, "ClientSession", lambda *a, **k: _FakeHttp())
    rc = probe_db_stream.run(_args(symbols="005930", streams="H0STCNT0", out=str(out)))
    assert rc == 1
    assert len(out.read_text(encoding="utf-8").strip().splitlines()) == 1


def test_probe_recv_timeout_breaks_loop(tmp_path, monkeypatch) -> None:
    import asyncio

    from src.cli import probe_db_stream
    from src.realtime.contracts import VendorAck

    ack = json.dumps({"header": {"tr_cd": "S00", "rsp_cd": "00000"}, "body": None})
    out = tmp_path / "probe.jsonl"

    class _WS:
        def __init__(self):
            self._acks = [ack]

        async def receive_str(self):
            return self._acks.pop(0)

        async def receive(self):
            raise AssertionError("tapped out")

    fake_ws = _WS()

    class _SlowAdapter:
        def __init__(self, **kw):
            self._token = ""
            self._ws = fake_ws
            self.unparsable_frames = 0

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            for _ in pairs:
                await self._ws.receive_str()
            return [VendorAck("005930", "H0STCNT0", True, "00000")]

        async def recv(self):
            await asyncio.sleep(5)

        async def aclose(self):
            return None

    monkeypatch.setenv("DB_APP_KEY", "k")
    monkeypatch.setenv("DB_APP_SECRET", "s")
    monkeypatch.setattr(probe_db_stream, "DbRealtimeAdapter", _SlowAdapter)
    monkeypatch.setattr(aiohttp, "ClientSession", lambda *a, **k: _FakeHttp())
    rc = probe_db_stream.run(_args(symbols="005930", streams="H0STCNT0", seconds=0.05, out=str(out)))
    assert rc == 1


def test_probe_all_rejected_has_no_accepted_pair(tmp_path, monkeypatch) -> None:
    from src.cli import probe_db_stream
    from src.realtime.contracts import VendorAck

    ack = json.dumps({"header": {"tr_cd": "S00", "rsp_cd": "10017"}, "body": None})
    out = tmp_path / "probe.jsonl"
    _install(
        monkeypatch,
        {"acks": [ack]},
        [VendorAck("005930", "H0STCNT0", False, "10017")],
        [],
    )
    rc = probe_db_stream.run(_args(symbols="005930", streams="H0STCNT0", out=str(out)))
    assert rc == 1


def test_probe_deadline_expiry_breaks_busy_loop_and_skips_garbage_frame(
    tmp_path, monkeypatch, capsys
) -> None:
    import time

    from src.cli import probe_db_stream
    from src.realtime.contracts import L0Frame, VendorAck

    ack = json.dumps({"header": {"tr_cd": "S00", "rsp_cd": "00000"}, "body": None})
    out = tmp_path / "probe.jsonl"
    good = L0Frame("db", "H0STCNT0", "005930", '{"header":{}}', time.monotonic_ns(), time.time_ns(), 1, "db-1")
    bad = L0Frame("db", "H0STCNT0", "005930", "garbage{{{", time.monotonic_ns(), time.time_ns(), 2, "db-1")

    class _WS:
        def __init__(self):
            self._acks = [ack]

        async def receive_str(self):
            return self._acks.pop(0)

        async def receive(self):
            raise AssertionError("tapped out")

    fake_ws = _WS()

    class _BusyAdapter:
        def __init__(self, **kw):
            self._token = ""
            self._ws = fake_ws
            self.unparsable_frames = 0

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            for _ in pairs:
                await self._ws.receive_str()
            return [VendorAck("005930", "H0STCNT0", True, "00000")]

        async def recv(self):
            return good if time.monotonic_ns() % 2 else bad

        async def aclose(self):
            return None

    monkeypatch.setenv("DB_APP_KEY", "k")
    monkeypatch.setenv("DB_APP_SECRET", "s")
    monkeypatch.setattr(probe_db_stream, "DbRealtimeAdapter", _BusyAdapter)
    monkeypatch.setattr(aiohttp, "ClientSession", lambda *a, **k: _FakeHttp())
    rc = probe_db_stream.run(_args(symbols="005930", streams="H0STCNT0", seconds=0.01, out=str(out)))
    assert rc == 0


def test_probe_connect_failure_reports_error_and_fails(tmp_path, monkeypatch, capsys) -> None:
    from src.cli import probe_db_stream
    from src.realtime.contracts import VendorDisconnected

    closed: list[bool] = []

    class _FailingAdapter:
        def __init__(self, **kw):
            self.unparsable_frames = 0

        async def connect(self):
            raise VendorDisconnected("token_throttled")

        async def aclose(self):
            closed.append(True)

    _install(monkeypatch, {}, [], [])
    monkeypatch.setattr(probe_db_stream, "DbRealtimeAdapter", _FailingAdapter)
    rc = probe_db_stream.run(_args(out=str(tmp_path / "probe.jsonl")))
    assert rc == 1
    assert json.loads(capsys.readouterr().out.strip())["error"] == "token_throttled"
    assert closed == [True]


def test_probe_parser_registers_defaults() -> None:
    from src.cli import probe_db_stream

    parser = argparse.ArgumentParser()
    probe_db_stream.add_parser(parser.add_subparsers())
    ns = parser.parse_args(["probe-db-stream", "--symbols", "005930", "--out", "scratch/x.jsonl"])
    assert ns.streams == "H0STCNT0,H0STASP0"
    assert ns.seconds == 120.0
    assert ns.handler is probe_db_stream.run
