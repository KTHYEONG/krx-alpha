import polars as pl
import pytest

from src.storage.quality_kis import decode_kis_ticks


@pytest.mark.parametrize("short", [False, True])
def test_tick_decoder_field_boundary(short):
    fields = ["005930", "090001", "70000", "3", "0", "0.00", "", "", "", "", "", "",
              "10", "1000", "", "99", "1"]
    if short:
        fields.pop()
    decoded = decode_kis_ticks(pl.DataFrame({"raw": ["^".join(fields)], "recv_wall_ns": [42]}))
    row = decoded.row(0, named=True)
    assert row == {
        "shcode": None if short else "005930",
        "price_raw": None if short else "70000",
        "cvolume_raw": None if short else "10",
        "volume_raw": None if short else "1000",
        "change_raw": None if short else "0",
        "sign": None if short else "3",
        "drate_raw": None if short else "0.00",
        "mdchecnt_raw": None if short else "99",
        "mschecnt_raw": None if short else "1",
        "recv_wall_ns": 42,
    }
