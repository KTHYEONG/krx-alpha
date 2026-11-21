"""Parquet writer options shared by every L1-family output file."""

from __future__ import annotations

from typing import Final

import pyarrow as pa

L1_ZSTD_LEVEL: Final[int] = 9
L1_DATA_PAGE_BYTES: Final[int] = 8 * 1024 * 1024
L1_ENCODING_METADATA_KEY: Final[str] = "krx_l1_encoding"
L1_ENCODING_VERSION: Final[str] = "v2"
L1_DICTIONARY_EXCLUDED_COLUMNS: Final[frozenset[str]] = frozenset({"raw", "exchange_event_time"})


def l1_parquet_writer_kwargs(schema: pa.Schema) -> dict[str, object]:
    """Return pyarrow ParquetWriter kwargs for an L1-family file with the given schema."""
    dictionary_columns = [
        field.name
        for field in schema
        if (pa.types.is_string(field.type) or pa.types.is_binary(field.type))
        and field.name not in L1_DICTIONARY_EXCLUDED_COLUMNS
    ]
    column_encoding = {field.name: "DELTA_BINARY_PACKED" for field in schema if pa.types.is_integer(field.type)}
    return {
        "compression": "zstd",
        "compression_level": L1_ZSTD_LEVEL,
        "data_page_size": L1_DATA_PAGE_BYTES,
        "use_dictionary": dictionary_columns,
        "column_encoding": column_encoding,
    }


def l1_encoding_metadata() -> dict[str, str]:
    return {L1_ENCODING_METADATA_KEY: L1_ENCODING_VERSION}
