#!/usr/bin/env bash
# Krx window probe (weekly). Read-only; prints verdict lines only. Run from $HOME/krx-alpha.
export TZ=Asia/Seoul LC_ALL=C
cd "$HOME/krx-alpha/data" || { echo "CTX UNKNOWN reason=no_data_root"; exit 0; }
R=$HOME/.local/bin/rclone; RR=gdrive:quant-lake/live/krx-alpha/data
echo "CTX now=$(date '+%F %T %a') rev=$(docker inspect krx-collector --format '{{slice (index .Config.Labels "org.opencontainers.image.revision") 0 7}}')"
# B7 Drive duplicate folders (break rclone check / verification ambiguity)
dup=$(nice -n 19 $R lsf -R --dirs-only "$RR/l1" 2>/dev/null | sort | uniq -d); n=$(printf '%s' "$dup" | grep -c .)
echo "B7 $([ "$n" -eq 0 ] && echo PASS || echo FAIL) remote_l1_duplicate_dirs=$n sample=$(printf '%s' "$dup" | head -2 | tr '\n' ',')"
# C9 Kiwoom dormant; DB artifacts follow secondary_feeds
env_n=$(docker exec krx-collector sh -c 'env | grep -cE "KRX_ALPHA_KIWOOM_CROSSCHECK_ENABLED=(1|true|True)"' 2>/dev/null)
art=$(ls -d l0/kiwoom manifest/*.kiwoom.json 2>/dev/null | wc -l)
dbcfg=$(docker exec krx-collector sh -c 'printf %s "$KRX_ALPHA_FEED_CONTROL_SECONDARY_FEEDS"' 2>/dev/null | grep -c 'db:')
dbart=$(ls -d l0/db manifest/*.db.json manifest/*.db.1.json 2>/dev/null | wc -l)
dbok=$([ "$dbcfg" -gt 0 ] && [ "$dbart" -gt 0 ] && echo 1 || { [ "$dbcfg" -eq 0 ] && [ "$dbart" -eq 0 ] && echo 1 || echo 0; })
echo "C9 $([ "${env_n:-0}" -eq 0 ] && [ "$art" -eq 0 ] && [ "$dbok" = 1 ] && echo PASS || echo FAIL) crosscheck_env_enabled=${env_n:-?} artifacts=$art db_configured=$dbcfg db_artifacts=$dbart"
# H6 static runtime config
st=$(docker inspect krx-collector --format '{{.Config.StopTimeout}}|{{.HostConfig.Memory}}|{{.HostConfig.MemorySwap}}')
IFS='|' read -r sto mem swp <<<"$st"
echo "H6 $([ "$sto" = 95 ] && [ "$mem" = 3221225472 ] && [ "$swp" = 3221225472 ] && echo PASS || echo FAIL) stop_timeout=$sto mem=$mem memswap=$swp"
# H7 disk trend
# unbounded-by-design sets (no local retention): kca offsite ledger (~11 MB/trading day), krx l1/history (permanent), kse state; everything else is retention-bounded (krx L1 30 d, kca raw 30 d / ticks 40 d, docker GC, journald default cap)
echo "H7 INFO $(df -BG --output=used,avail / | tail -1 | awk '{print "used_gb="$1" avail_gb="$2}') $(nice -n 19 du -s --block-size=1M l0 l1 bars manifest gapfill 2>/dev/null | awk '{printf "%s_mb=%s ", $2, $1}') unbounded_mb: $(nice -n 19 du -s --block-size=1M l1/history "$HOME/k-closing-alpha/data/history/capture/offsite/ledger" "$HOME/k-stock-engine/data/state/kr_swing_2019_v1/quota" 2>/dev/null | awk '{n=$2; sub(".*/data/","",n); sub(".*/","",n); printf "%s=%s ", n, $1}')"
PQ=$(cat <<'EOF'
import glob, os
import polars as pl
from src.storage.quality import decode_tick_raw_fields
# Q6 independent L1 content of the newest partition per stream
for s in sorted({f.rsplit("/", 1)[0] for f in glob.glob("/app/data/l1/*/*/*/H0*/dt=*.parquet")}):
    f = sorted(glob.glob(f"{s}/dt=*.parquet"))[-1]; D = f[-18:-8]
    d = pl.read_parquet(f, columns=["conn_id", "conn_seq", "recv_wall_ns", "exchange_event_time", "market_phase"])
    day = pl.from_epoch(pl.col("recv_wall_ns"), time_unit="ns").dt.convert_time_zone("Asia/Seoul").dt.date().cast(pl.String); e = pl.col("exchange_event_time")
    r = d.select(pl.len().alias("rows"), (pl.len() - pl.struct("conn_id", "conn_seq").n_unique()).alias("dup_seq"), (day != D).sum().alias("wrong_day"),
                 (e.is_null() | (e == "")).sum().alias("evt_empty"), (pl.col("recv_wall_ns").diff().over("conn_id") < 0).sum().alias("recv_regress"),
                 (pl.col("market_phase") == "unclassified").sum().alias("unclassified")).row(0, named=True)
    v = "FAIL" if r["dup_seq"] or r["wrong_day"] or r["recv_regress"] else ("WARN" if r["unclassified"] > 0.01 * r["rows"] else "PASS")
    print(f"Q6 {v} part={f.split('/l1/')[1]} " + " ".join(f"{k}={x}" for k, x in r.items()))
# Q7 tick vs daily-bar volume on the newest LS regular L1 with bars
for f in sorted(glob.glob("/app/data/l1/ls/krx/regular/H0STCNT0/dt=*.parquet"))[-1:]:
    # Fixed-size batches keep memory flat as the universe grows (row-group decode OOM'd under -m 1g after the 11-05 LS 2-shard rollout).
    # Daily bars include off-hours block trades, so a low ratio is a loss only when the lossless session audit's volume_deficit explains >=2% of the bar volume.
    import json, pyarrow.parquet as pq
    D = f[-18:-8]; parts = []
    for batch in pq.ParquetFile(f).iter_batches(batch_size=100_000, columns=["raw", "tr_id", "recv_wall_ns"]):
        t = decode_tick_raw_fields(pl.from_arrow(batch))
        if t is not None: parts.append(t.select("shcode", v=pl.col("volume_raw").cast(pl.Int64, strict=False)).group_by("shcode").agg(pl.col("v").max()))
    g = pl.concat(parts).group_by("shcode").agg(pl.col("v").max().alias("tick_vol"))
    b = pl.read_parquet(f"/app/data/bars/daily/{D[:7]}.parquet", columns=["date", "symbol", "volume"]).filter(pl.col("date").cast(pl.String) == D).select(pl.col("symbol").alias("shcode"), "volume")
    j = g.join(b, on="shcode", how="left").with_columns(ratio=pl.col("tick_vol") / pl.col("volume")); lo = j["ratio"].min()
    low = sorted(j.filter(pl.col("ratio") < 0.98)["shcode"].to_list())
    try: deficit = {x["symbol"]: int(x.get("volume_deficit") or 0) for x in json.load(open(f"/app/data/manifest/audit/{D}.ls.regular.audit.json"))["symbols"]}
    except (OSError, ValueError, KeyError): deficit = None
    vol = dict(zip(j["shcode"].to_list(), j["volume"].to_list()))
    lost = [x for x in low if deficit is None or deficit.get(x, 0) >= 0.02 * (vol.get(x) or 0)]
    v = "WARN" if j["volume"].null_count() or lost else ("INFO" if low else "PASS")
    print(f"Q7 {v} day={D} symbols={j.height} unmatched={j['volume'].null_count()} exact={int((j['ratio'] == 1.0).sum())} min_ratio={lo} low={len(low)} low_with_audit_loss={lost[:5] if deficit is not None else 'audit_unreadable'} offhours_suspected={[x for x in low if x not in lost][:5]}")
EOF
)
[ "${PQRUN:-1}" = 1 ] && nice -n 19 docker run --rm -i -m 2560m --memory-swap 2560m --network none -e POLARS_MAX_THREADS=2 -v "$HOME/krx-alpha/data:/app/data:ro" --entrypoint /app/.venv/bin/python ghcr.io/kthyeong/krx-collector:latest -c "$PQ" 2>&1 | tail -n 40
