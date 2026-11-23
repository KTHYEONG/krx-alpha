"""LS NXT adapter: NS3/NH1 subscription bodies, logical NXT frames and decoder compatibility."""

from __future__ import annotations

import json
import pathlib

import polars as pl

from src.realtime.adapters.ls import LsNxtRealtimeAdapter
from src.realtime.contracts import MarketSession, MarketVenue
from src.storage.quality_ls import decode_ls_quotes, decode_ls_ticks

_NS3 = (
    '{"header":{"tr_cd":"NS3","tr_key":"N005930   "},"body":{"mdchecnt":"41833","sign":"5","mschecnt":"60417",'
    '"mdvolume":"1891924","cvolume":"10","price":"193000","change":"3800","shcode":"005930","chetime":"102704",'
    '"ex_shcode":"N005930","volume":"3609743","drate":"-1.93","exchname":"NXT"}}'
)


def _nh1() -> str:
    body: dict[str, str] = {"shcode": "005930", "hotime": "102704", "totofferrem": "5338", "totbidrem": "68975"}
    for level in range(1, 11):
        body[f"offerho{level}"] = str(193000 + level * 100)
        body[f"bidho{level}"] = str(193000 - level * 100)
        body[f"offerrem{level}"] = str(100 + level)
        body[f"bidrem{level}"] = str(200 + level)
    return json.dumps({"header": {"tr_cd": "NH1", "tr_key": "N005930   "}, "body": body})


def _adapter() -> LsNxtRealtimeAdapter:
    return LsNxtRealtimeAdapter(app_key="k", app_secret="s", http=object(), token_store=object())  # type: ignore[arg-type]


def test_ls_nxt_adapter_subscribes_ns3_nh1_with_n_prefix() -> None:
    adapter = _adapter()
    assert adapter._subscribe_body("005930", "H0NXCNT0") == {"tr_cd": "NS3", "tr_key": "N005930   "}
    assert adapter._subscribe_body("005930", "H0NXASP0") == {"tr_cd": "NH1", "tr_key": "N005930   "}
    # Malformed keys are refused before sending (LS closes the socket on one bad key); letter codes are KRX-valid.
    for bad in ("5930", "0059300", "a05930", "N005930", "00593 "):
        assert adapter._invalid_key(bad, "H0NXCNT0") == "invalid_tr_key"
    assert adapter._invalid_key("0088M0", "H0NXASP0") is None


def test_ls_nxt_frames_logical_ids_and_venue() -> None:
    adapter = _adapter()
    for raw, stream in ((_NS3, "H0NXCNT0"), (_nh1(), "H0NXASP0")):
        o = json.loads(raw)
        frame = adapter._data_frame(o, o["header"], raw)
        assert frame is not None
        assert (frame.vendor, frame.stream, frame.symbol) == ("ls", stream, "005930")
        assert (frame.venue, frame.session) == (MarketVenue.NXT, MarketSession.REGULAR)
    krx = {"header": {"tr_cd": "S3_", "tr_key": "005930"}, "body": {}}
    ack = {"header": {"tr_cd": "NS3", "tr_key": "N005930   ", "rsp_cd": "00000"}}
    assert adapter._data_frame(krx, krx["header"], json.dumps(krx)) is None
    assert adapter._data_frame(ack, ack["header"], json.dumps(ack)) is None


def test_ls_nxt_frames_decode_with_ls_decoders() -> None:
    ticks = decode_ls_ticks(pl.DataFrame({"raw": [_NS3], "recv_wall_ns": [1_794_878_824_988_697_365]}))
    quotes = decode_ls_quotes(pl.DataFrame({"raw": [_nh1()], "recv_wall_ns": [1_794_878_824_988_697_365]}))
    assert ticks.height == 1
    assert quotes.height == 1
    assert ticks["shcode"].to_list() == ["005930"]
    assert quotes["shcode"].to_list() == ["005930"]


def test_ls_nxt_l1_normalization(tmp_path: pathlib.Path) -> None:
    import zstandard as zstd

    from src.storage.normalization import normalize_l0_partition

    for stream, raw in (("H0NXCNT0", _NS3), ("H0NXASP0", _nh1())):
        part = tmp_path / "l0" / "ls" / "nxt" / "regular" / stream / "dt=2026-11-17"
        part.mkdir(parents=True)
        record = {
            "raw": raw, "recv_mono_ns": 1, "recv_wall_ns": 1_794_878_824_988_697_365, "conn_id": "ls-1",
            "conn_seq": 1, "vendor": "ls", "tr_id": stream, "stream": stream, "venue": "nxt", "session": "regular",
            "symbol": "005930",
        }
        (part / "10.jsonl.zst").write_bytes(zstd.ZstdCompressor(level=3).compress((json.dumps(record) + "\n").encode()))
        out = tmp_path / "l1" / f"{stream}.parquet"
        assert normalize_l0_partition(part, out) == 1
        assert out.exists()
