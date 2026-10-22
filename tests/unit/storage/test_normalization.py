"""L0 normalization storage-I/O vs data-corruption classification."""

from __future__ import annotations


def _write_l0_partition(part, *, raw="a"):
    import json

    import zstandard as zstd

    part.mkdir(parents=True, exist_ok=True)
    rec = {
        "raw": raw, "recv_mono_ns": 1, "recv_wall_ns": 100, "conn_id": "c1",
        "conn_seq": 1, "vendor": "kis", "tr_id": "H0STCNT0",
    }
    (part / "09.jsonl.zst").write_bytes(zstd.ZstdCompressor(level=3).compress((json.dumps(rec) + "\n").encode("utf-8")))
    return part


def test_normalize_l0_partition_disk_full_is_storage_io_not_corruption(tmp_path, monkeypatch) -> None:
    import errno

    import polars as pl
    import pytest

    from src.storage.normalization import L1StorageIOError, normalize_l0_partition

    part = _write_l0_partition(tmp_path / "l0" / "kis" / "H0STCNT0" / "dt=2026-09-01")
    out_path = tmp_path / "l1" / "kis" / "H0STCNT0" / "dt=2026-09-01.parquet"

    def _no_space(self, *args, **kwargs):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", _no_space)

    with pytest.raises(L1StorageIOError):
        normalize_l0_partition(part, out_path)
    assert not out_path.exists()


def test_normalize_l0_partition_corrupt_zstd_stays_data_fault(tmp_path) -> None:
    import json

    import pytest
    import zstandard as zstd

    from src.storage.normalization import L1NormalizationError, normalize_l0_partition

    part = tmp_path / "l0" / "kis" / "H0STCNT0" / "dt=2026-09-01"
    part.mkdir(parents=True, exist_ok=True)
    rec = {
        "raw": "a", "recv_mono_ns": 1, "recv_wall_ns": 100, "conn_id": "c1",
        "conn_seq": 1, "vendor": "kis", "tr_id": "H0STCNT0",
    }
    valid = zstd.ZstdCompressor(level=3).compress((json.dumps(rec) + "\n").encode("utf-8"))
    (part / "09.jsonl.zst").write_bytes(valid[: len(valid) // 2])
    out_path = tmp_path / "l1" / "kis" / "H0STCNT0" / "dt=2026-09-01.parquet"

    with pytest.raises(L1NormalizationError):
        normalize_l0_partition(part, out_path)


def _write_records(part, records, name="09.jsonl.zst"):
    import json

    import zstandard as zstd

    part.mkdir(parents=True, exist_ok=True)
    buf = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in records).encode("utf-8")
    (part / name).write_bytes(zstd.ZstdCompressor(level=3).compress(buf))
    return part


def _rec(raw, wall, cid, seq):
    return {
        "raw": raw, "recv_mono_ns": 1, "recv_wall_ns": wall, "conn_id": cid,
        "conn_seq": seq, "vendor": "kis", "tr_id": "H0STCNT0",
    }


def _oracle_dedup(conn_ids, seq, wall, h1, h2):
    import numpy as np
    import polars as pl

    keys = pl.DataFrame({
        "conn_id": conn_ids, "conn_seq": seq, "recv_wall_ns": wall, "h1": h1, "h2": h2,
    })
    conn = keys["conn_id"].rank("dense").to_numpy().astype(np.int64)
    s = keys["conn_seq"].to_numpy()
    w = keys["recv_wall_ns"].to_numpy()
    a = keys["h1"].to_numpy()
    b = keys["h2"].to_numpy()
    perm = np.lexsort((b, a, s, conn))
    c = conn[perm]
    sp = s[perm]
    ap = a[perm]
    bp = b[perm]
    new_key = np.empty(s.size, dtype=bool)
    new_key[0] = True
    new_key[1:] = (c[1:] != c[:-1]) | (sp[1:] != sp[:-1])
    new_raw = new_key.copy()
    new_raw[1:] |= (ap[1:] != ap[:-1]) | (bp[1:] != bp[:-1])
    key_starts = np.flatnonzero(new_key)
    conflict = int((np.add.reduceat(new_raw.astype(np.int64), key_starts) > 1).sum())
    keep = np.minimum.reduceat(perm, key_starts)
    kept_for = keep[np.cumsum(new_key) - 1]
    mask = perm != kept_for
    dropped = perm[mask]
    kept = kept_for[mask]
    order = keep[np.lexsort((keep, w[keep]))]
    return (order.astype(np.int64), conflict, dropped.astype(np.int64), kept.astype(np.int64))


def test_dedup_contract_equivalence_with_oracle() -> None:
    import numpy as np

    from src.storage.normalization import _KeyColumns, _dedup_sort_order

    rng = np.random.default_rng(7)
    n = 5_000
    pools = [f"c{i}" for i in range(7)]
    conn_ids = [pools[int(x)] for x in rng.integers(0, 7, n)]
    seq = rng.integers(0, 4_500, n).astype(np.int64)
    wall = (1_000_000 + rng.permutation(n)).astype(np.int64)
    h1 = rng.integers(0, 2**63 - 1, n).astype(np.uint64)
    h2 = rng.integers(0, 2**63 - 1, n).astype(np.uint64)
    for i in range(0, 500):
        j = int(rng.integers(0, n))
        conn_ids[i] = conn_ids[j]
        seq[i] = seq[j]
        h1[i] = h1[j]
        h2[i] = h2[j]
    for i in range(500, 510):
        j = int(rng.integers(0, n))
        conn_ids[i] = conn_ids[j]
        seq[i] = seq[j]
        h1[i] = h1[j] ^ np.uint64(1)
    codes = {cid: i for i, cid in enumerate(sorted(set(conn_ids)))}
    code = np.array([codes[cid] for cid in conn_ids], dtype=np.int32)
    keys = _KeyColumns(conn_code=code, conn_seq=seq, recv_wall_ns=wall, h1=h1, h2=h2)
    order, conflict, dropped, kept = _dedup_sort_order(keys)
    e_order, e_conflict, e_dropped, e_kept = _oracle_dedup(conn_ids, seq, wall, h1, h2)
    assert conflict == e_conflict
    np.testing.assert_array_equal(order, e_order, strict=True)
    np.testing.assert_array_equal(dropped, e_dropped, strict=True)
    np.testing.assert_array_equal(kept, e_kept, strict=True)
    first_codes = {cid: i for i, cid in enumerate(dict.fromkeys(conn_ids))}
    first_code = np.array([first_codes[cid] for cid in conn_ids], dtype=np.int32)
    assert not np.array_equal(first_code, code)
    expected_pairs = np.column_stack((e_dropped, e_kept))[np.argsort(e_dropped)]
    for remapped_code in (first_code, (6 - code).astype(np.int32), rng.permutation(7).astype(np.int32)[code]):
        remapped = _KeyColumns(
            conn_code=remapped_code, conn_seq=seq, recv_wall_ns=wall, h1=h1, h2=h2,
        )
        r_order, r_conflict, r_dropped, r_kept = _dedup_sort_order(remapped)
        np.testing.assert_array_equal(r_order, e_order, strict=True)
        assert r_conflict == e_conflict
        r_pairs = np.column_stack((r_dropped, r_kept))[np.argsort(r_dropped)]
        np.testing.assert_array_equal(r_pairs, expected_pairs, strict=True)
        repeated = _dedup_sort_order(remapped)
        np.testing.assert_array_equal(repeated[0], r_order, strict=True)
        assert repeated[1] == r_conflict
        np.testing.assert_array_equal(repeated[2], r_dropped, strict=True)
        np.testing.assert_array_equal(repeated[3], r_kept, strict=True)


def test_conn_code_equality_only(tmp_path) -> None:
    import polars as pl

    from src.storage.normalization import normalize_l0_partition

    rows_a = [
        _rec("dup", 300, "zz", 1),
        _rec("dup", 100, "zz", 1),
        _rec("other", 200, "aa", 1),
    ]
    rows_b = [
        _rec("dup", 300, "aa", 1),
        _rec("dup", 100, "aa", 1),
        _rec("other", 200, "zz", 1),
    ]
    pa = tmp_path / "a" / "l0" / "kis" / "H0STCNT0" / "dt=2026-09-01"
    pb = tmp_path / "b" / "l0" / "kis" / "H0STCNT0" / "dt=2026-09-01"
    _write_records(pa, rows_a)
    _write_records(pb, rows_b)
    oa = tmp_path / "a.parquet"
    ob = tmp_path / "b.parquet"
    assert normalize_l0_partition(pa, oa) == 2
    assert normalize_l0_partition(pb, ob) == 2
    import pyarrow.parquet as pq

    fa = pl.read_parquet(oa)
    fb = pl.read_parquet(ob)
    assert fa.drop("conn_id").equals(fb.drop("conn_id"))
    assert pq.read_metadata(oa).metadata == pq.read_metadata(ob).metadata
    assert fa["raw"].to_list() == fb["raw"].to_list() == ["other", "dup"]
    assert fa["recv_wall_ns"].to_list() == fb["recv_wall_ns"].to_list()


def test_cross_file_duplicate_dedup(tmp_path, caplog) -> None:
    import logging

    import polars as pl

    from src.storage.normalization import normalize_l0_partition

    part = tmp_path / "l0" / "kis" / "H0STCNT0" / "dt=2026-09-01"
    rec = _rec("a", 100, "c1", 1)
    _write_records(part, [rec], name="09.jsonl.zst")
    _write_records(part, [rec], name="10.jsonl.zst")
    out = tmp_path / "l1.parquet"
    with caplog.at_level(logging.INFO, logger="src.storage.normalization"):
        assert normalize_l0_partition(part, out) == 1
    assert pl.read_parquet(out).height == 1
    assert "dedup_dropped=1" in caplog.text


def test_hash_equal_payload_mismatch_still_fails(tmp_path, monkeypatch) -> None:
    import polars as pl
    import pytest

    from src.storage.normalization import L1NormalizationError, normalize_l0_partition

    def constant_hash(self, *args, **kwargs):
        return pl.Series([0] * len(self), dtype=pl.UInt64)

    monkeypatch.setattr(pl.Series, "hash", constant_hash)
    part = tmp_path / "l0" / "kis" / "H0STCNT0" / "dt=2026-09-01"
    _write_records(part, [_rec("a", 100, "c1", 1), _rec("b", 101, "c1", 1)])
    out = tmp_path / "l1.parquet"
    with pytest.raises(L1NormalizationError, match=r"conn_seq collision.*hash-equal payload mismatch"):
        normalize_l0_partition(part, out)
    assert not out.exists()
    assert not (out.parent / (out.name + ".tmp")).exists()


def test_empty_and_single_row_partitions(tmp_path) -> None:
    import pytest
    import zstandard as zstd

    from src.storage.normalization import L1NormalizationError, normalize_l0_partition

    empty = tmp_path / "empty" / "l0" / "kis" / "H0STCNT0" / "dt=2026-09-01"
    empty.mkdir(parents=True, exist_ok=True)
    (empty / "09.jsonl.zst").write_bytes(zstd.ZstdCompressor(level=3).compress(b""))
    with pytest.raises(L1NormalizationError, match="zero rows"):
        normalize_l0_partition(empty, tmp_path / "empty.parquet")
    single = tmp_path / "single" / "l0" / "kis" / "H0STCNT0" / "dt=2026-09-01"
    _write_records(single, [_rec("a", 100, "c1", 1)])
    out = tmp_path / "single.parquet"
    assert normalize_l0_partition(single, out) == 1


def test_dedup_empty_keys() -> None:
    import numpy as np

    from src.storage.normalization import _KeyColumns, _dedup_sort_order

    z32 = np.empty(0, dtype=np.int32)
    z64 = np.empty(0, dtype=np.int64)
    zu64 = np.empty(0, dtype=np.uint64)
    keys = _KeyColumns(conn_code=z32, conn_seq=z64, recv_wall_ns=z64, h1=zu64, h2=zu64)
    order, conflict, dropped, kept = _dedup_sort_order(keys)
    assert conflict == 0
    assert order.size == 0
    assert dropped.size == 0
    assert kept.size == 0


def test_conn_code_overflow_raises(monkeypatch, tmp_path) -> None:
    import pytest

    import src.storage.normalization as norm

    monkeypatch.setattr(norm, '_CONN_CODE_MAX', 1)
    part = tmp_path / 'l0' / 'kis' / 'H0STCNT0' / 'dt=2026-09-01'
    _write_records(part, [_rec('a', 100, 'c1', 1), _rec('a', 101, 'c2', 2)])
    with pytest.raises(norm.L1NormalizationError, match='too many conn_id'):
        norm.normalize_l0_partition(part, tmp_path / 'o.parquet')


def test_multibatch_output_matches_legacy_oracle(tmp_path, monkeypatch) -> None:
    import numpy as np
    import polars as pl
    import pyarrow.parquet as pq

    import src.storage.normalization as norm

    monkeypatch.setattr(norm, "_KEY_BUF_INIT", 2)
    monkeypatch.setattr(norm, "_BATCH_BYTES", 300)
    monkeypatch.setattr(norm, "_GATHER_ROWS", 3)
    part = tmp_path / "l0" / "kis" / "H0STCNT0" / "dt=2026-09-01"
    originals = [_rec("a", i % 3, "zz" if i % 2 else "aa", i) for i in range(15)]
    rows = originals + list(reversed(originals))
    _write_records(part, rows[:15], name="09.jsonl.zst")
    _write_records(part, rows[15:], name="10.jsonl.zst")
    dedup = norm._dedup_sort_order

    def bounded_dedup(keys):
        for column, dtype in (
            (keys.conn_code, np.int32), (keys.conn_seq, np.int64),
            (keys.recv_wall_ns, np.int64), (keys.h1, np.uint64), (keys.h2, np.uint64),
        ):
            assert column.flags.owndata
            assert column.size == len(rows)
            assert column.nbytes == len(rows) * np.dtype(dtype).itemsize
        return dedup(keys)

    monkeypatch.setattr(norm, "_dedup_sort_order", bounded_dedup)
    out = tmp_path / "new.parquet"
    assert norm.normalize_l0_partition(part, out) == 15

    def legacy_dedup(keys):
        return _oracle_dedup(
            [row["conn_id"] for row in rows], keys.conn_seq, keys.recv_wall_ns, keys.h1, keys.h2,
        )

    monkeypatch.setattr(norm, "_dedup_sort_order", legacy_dedup)
    reference = tmp_path / "legacy.parquet"
    assert norm.normalize_l0_partition(part, reference) == 15
    assert pl.read_parquet(out).equals(pl.read_parquet(reference))
    assert pq.read_metadata(out).metadata == pq.read_metadata(reference).metadata
