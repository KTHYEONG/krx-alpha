"""L1 parquet writer option invariants."""

from __future__ import annotations


def test_writer_kwargs_derived_from_schema() -> None:
    import pyarrow as pa

    from src.storage.parquet_options import L1_DATA_PAGE_BYTES, L1_ZSTD_LEVEL, l1_encoding_metadata, l1_parquet_writer_kwargs

    schema = pa.schema(
        [
            pa.field("price", pa.int64()),
            pa.field("recv_wall_ns", pa.int64()),
            pa.field("symbol", pa.string()),
            pa.field("raw", pa.string()),
            pa.field("exchange_event_time", pa.string()),
        ]
    )
    kwargs = l1_parquet_writer_kwargs(schema)
    assert kwargs["compression"] == "zstd"
    assert kwargs["compression_level"] == L1_ZSTD_LEVEL
    assert kwargs["data_page_size"] == L1_DATA_PAGE_BYTES
    assert kwargs["use_dictionary"] == ["symbol"]
    assert kwargs["column_encoding"] == {"price": "DELTA_BINARY_PACKED", "recv_wall_ns": "DELTA_BINARY_PACKED"}
    assert l1_encoding_metadata() == {"krx_l1_encoding": "v2"}
