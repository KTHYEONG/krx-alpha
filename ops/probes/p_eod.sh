#!/usr/bin/env bash
# Krx window probe (EOD). Read-only; prints verdict lines only. Run from $HOME/krx-alpha.
export TZ=Asia/Seoul LC_ALL=C
cd "$HOME/krx-alpha/data" || { echo "CTX UNKNOWN reason=no_data_root"; exit 1; }
D=${D:-$(date +%F)}; L0C=${L0C:-1}; PQRUN=${PQRUN:-1}; export D L0C
PY=$(cat <<'EOF'
import collections, datetime as dt, glob, json, os, re, statistics, subprocess, sys
Q1_MAX_BITMAP_PAGES: int = 65_536

class _SequenceBitmapLimitError(RuntimeError):
    pass

class _SequenceBitmap:
    def __init__(self, max_pages: int) -> None:
        if max_pages <= 0: raise ValueError("max_pages must be positive")
        self.max_pages = max_pages
        self.pages: dict[tuple[str, int], bytearray] = {}

    def test_and_set(self, conn_id: str, conn_seq: int) -> bool:
        page_index, offset = divmod(conn_seq, 4096)
        key = (conn_id, page_index)
        page = self.pages.get(key)
        if page is None:
            if len(self.pages) >= self.max_pages: raise _SequenceBitmapLimitError()
            page = self.pages[key] = bytearray(512)
        index, bit = divmod(offset, 8)
        mask = 1 << bit
        duplicate = bool(page[index] & mask)
        page[index] |= mask
        return duplicate

K = dt.timezone(dt.timedelta(hours=9)); now = dt.datetime.now(K); D = os.environ["D"]; Dd = dt.date.fromisoformat(D)
R = os.path.expanduser("~/.local/bin/rclone"); RR = "gdrive:quant-lake/live/krx-alpha/data"
cal = sorted(f[-15:-5] for f in glob.glob("calendar/dt=*.json")); biz = D in cal
N = min([d for d in cal if d > D], default="?"); P = max([d for d in cal if d < D], default="?")
L = sys.stdin.read().splitlines()
f = lambda l, k: (re.search(rf"\b{k}=(\S+)", l) or [None, "-"])[1]
def ts(l):
    try: return dt.datetime.fromisoformat(l[:29])
    except ValueError: return None
def jl(p):
    try: return json.load(open(p))
    except (OSError, ValueError): return None
cover = any(l[:10] == D for l in L)
print(f"CTX now={now:%F %T %a} logs_cover_D={cover} D={D} kind={'BUSINESS' if biz else ('WEEKEND' if Dd.weekday() >= 5 else 'HOLIDAY')} P={P} N={N}")
if Dd.weekday() >= 5: print("R4 SKIP(weekend_no_eod)"); sys.exit(0)
# R4 EOD done + duration vs trailing median of the previous 5 EOD finish times
ev = []
for l in open("logs/events-daemon.jsonl", errors="replace"):
    try: r = json.loads(l)
    except ValueError: continue
    if r["fields"].get("stage") == "eod_maintenance" and "deleted_partitions" in r["fields"]: ev.append(r)
mins = lambda t: int(t[11:13]) * 60 + int(t[14:16])
fin = {r["ts"][:10]: r for r in ev}; cur = fin.get(D); prev = [mins(fin[d]["ts"]) for d in sorted(fin) if d < D][-5:]
med = statistics.median(prev) if prev else 21 * 60
if cur is None:
    v = "FAIL" if now.strftime("%H%M") >= "2150" else "UNKNOWN"; print(f"R4 {v} eod=not_logged median_finish={med // 60:02.0f}:{med % 60:02.0f}")
else:
    m = mins(cur["ts"]); st = cur["fields"].get("status")
    v = "FAIL" if st not in ("OK", "HOLIDAY", "DEGRADED") or m > med + 30 or m >= 21 * 60 + 45 else ("WARN" if st == "DEGRADED" or m > med + 15 else "PASS")
    dg = {}
    try: dg = dict(x.split("=", 1) for x in open(f"logs/eod_digests/{D}.txt").read().split("\n")[2:] if "=" in x)
    except OSError: pass
    print(f"R4 {v} status={st} finish={cur['ts'][11:16]} median5={med // 60:02.0f}:{med % 60:02.0f} deleted={cur['fields'].get('deleted_partitions')} uploaded={cur['fields'].get('uploaded')} purged={cur['fields'].get('purged')} degraded={dg.get('degraded_reasons', '-')} reconciled={dg.get('reconciled', '-')} loss={dg.get('regular_trade_loss', '-')} open_lat_s={dg.get('open_max_latency_s', '-')}")
    sent = any("stage=digest status=SENT" in l for l in L)
    print(f"A3 {'PASS' if os.path.exists(f'logs/eod_digests/{D}.txt') and (st != 'DEGRADED' or sent) else ('SKIP(holiday)' if st == 'HOLIDAY' else 'WARN')} digest_file={os.path.exists(f'logs/eod_digests/{D}.txt')} mailed={sent}")
eodt = dt.datetime.fromisoformat(cur["ts"]) if cur else now
crit = [l for l in L if "level=CRITICAL" in l and (t := ts(l)) and t.date() == Dd]
keys = collections.Counter(f"{f(l, 'stage')}:{f(l, 'status')}:{f(l, 'reason')}" for l in crit)
print(f"A2 {'PASS' if not crit else 'WARN'} critical_today={len(crit)} keys={dict(keys.most_common(8))}")
# R6 repair gate, R7 program sync, R8 next premarket pool
exp = [l for l in crit if "stage=repair_hold status=EXPIRED" in l]; ovd = [f(l, "days") for l in crit if "stage=repair_overdue" in l]
print(f"R6 {'FAIL' if exp else ('WARN' if ovd else 'PASS')} hold_expired={[f(l, 'day') for l in exp]} overdue={ovd or '-'}")
ps = [l for l in L if "stage=program_trades_sync" in l and (t := ts(l)) and t.date() == Dd]
pr = ps[-1] if ps else ""
v = "SKIP(holiday)" if not biz else "UNKNOWN" if not cover else ("PASS" if "status=OK exit_code=0" in pr else ("WARN" if "STARTED" in pr else "FAIL"))
print(f"R7 {v} last={f(pr, 'status')} gaps={sum('program_trades_gap status=GAPS' in l for l in L)} stale={sum('program_trades_sync status=STALE' in l for l in L)}")
u = jl(f"universe/premarket/{N}.json") if biz else None
shifted_d = bool(biz and (jl(f"calendar/dt={D}.json") or {}).get("regular_open", "09:00:00") != "09:00:00")  # NXT premarket/regular are not collected on KRX-shifted days: C2/C3/C4 rows for them are SKIP(shifted)
shifted_n = bool(biz and ((jl(f"calendar/dt={N}.json") or {}).get("regular_open", "09:00:00") != "09:00:00"))  # NXT premarket/regular pools are skipped on KRX-shifted days (logs: `premarket_pool status=SKIP reason=shifted_next_day`)
print("R8 " + ("SKIP(holiday)" if not biz else f"SKIP(shifted_next_day) pool={N}" if shifted_n and not u else (f"{'PASS' if u and len(u['candidates']) == u['selected_count'] == min(u['capacity'], u['eligible_count']) else 'FAIL'} pool={N} n={len(u['candidates']) if u else 'MISSING'} generated={u['generated_at'][:16] if u else '-'}")))
if not biz: sys.exit(0)
# C3 manifests and C4 universe consistency
def acked(m): return {(a["symbol"], a["tr_id"]) for a in m["subscription_acks"] if a.get("accepted")}
cand = jl("candidates.json") or {"candidates": [], "rev": None}; cs = {c["symbol"] for c in cand["candidates"]}
groups = {"ls/krx/regular": [f"manifest/{D}.json"] + sorted(glob.glob(f"manifest/ls/{D}.krx.shard-*.json")), "kis/krx/regular": sorted(glob.glob(f"manifest/regular_kis/{D}.krx.shard-*.json")),
          "kis/krx/krx_after": sorted(glob.glob(f"manifest/aftermarket/{D}.krx.shard-*.json")), "kis/nxt/nxt_after": sorted(glob.glob(f"manifest/aftermarket/{D}.nxt.shard-*.json")),
          "kis/nxt/nxt_pre": [f"manifest/premarket/{D}.nxt.shard-00.json"], "kis/nxt/regular": sorted(glob.glob(f"manifest/regular_kis/{D}.nxt.shard-*.json"))}
syms = {}
for g, fs in groups.items():
    ms = [(p, jl(p)) for p in fs]; ms = [(p, m) for p, m in ms if m]
    if not ms and shifted_d and g in ("kis/nxt/nxt_pre", "kis/nxt/regular"): print(f"C3 SKIP(shifted) group={g}"); syms[g] = set(); continue
    if not ms: print(f"C3 FAIL group={g} manifest=MISSING"); syms[g] = set(); continue
    bad = []; un = 0; gapmax = 0.0; off = 0.0; boots = 0; late = 0.0
    for p, m in ms:
        a = acked(m); pl = {(x["symbol"], x["tr_id"]) for x in m.get("planned_pairs") or []}
        un += len(pl - a); boots = max(boots, len(m["boots"])); off = max(off, abs(m["clock_offset_ns"]) / 1e6)
        gapmax = max([gapmax] + [(x["gap_end_ns"] - x["gap_start_ns"]) / 1e9 for x in m["gaps"]])
        if m.get("degraded_reason"): bad.append(f"{os.path.basename(p)}:degraded={m['degraded_reason']}")
        if m.get("expected_close_ns") and m.get("writer_closed_at_ns"): late = max(late, (m["writer_closed_at_ns"] - m["expected_close_ns"]) / 1e9)
        elif g != "ls/krx/regular" and not m.get("writer_closed_at_ns"): bad.append(f"{os.path.basename(p)}:not_closed")
    syms[g] = {s for _, m in ms for s, _ in acked(m)}
    if g == "ls/krx/regular" and ms[0][1].get("candidates_rev") == cand.get("rev") and syms[g] != cs: bad.append(f"acked_syms!=candidates({len(syms[g])}/{len(cs)})")
    v = "FAIL" if bad or un or off >= 5 else ("WARN" if boots > 1 or gapmax > 30 or late > (130 if g in ("kis/krx/regular", "kis/nxt/regular") else 10) else "PASS")
    print(f"C3 {v} group={g} files={len(ms)} acked_syms={len(syms[g])} unacked={un} boots={boots} max_gap_s={gapmax:.1f} clock_ms={off:.3f} close_late_s={late:.1f} issues={bad or '-'}")
for venue, suf in (("krx", ""), ("nxt", ".nxt")):
    u = jl(f"universe/aftermarket/{D}{suf}.json"); g = f"kis/{venue}/{'krx_after' if venue == 'krx' else 'nxt_after'}"
    if not u: print(f"C4 FAIL universe=aftermarket{suf or '.krx'} MISSING"); continue
    s = [c["symbol"] for c in u["candidates"]]; plan = {x["symbol"] for p in groups[g] for x in (jl(p) or {}).get("planned_pairs", [])}
    ok = len(s) == len(set(s)) == u["selected_count"] == min(u["capacity"], u["eligible_count"]) and plan == set(s) and syms.get(g) == plan
    print(f"C4 {'PASS' if ok else 'FAIL'} universe=aftermarket{suf or '.krx'} n={len(s)} want={min(u['capacity'], u['eligible_count'])} planned_eq={plan == set(s)} acked_eq={syms.get(g) == plan} generated={u['generated_at'][11:16]}")
pm = jl(f"universe/premarket/{D}.json")
print("C4 SKIP(shifted) universe=premarket" if shifted_d and not pm else f"C4 {'PASS' if pm and {c['symbol'] for c in pm['candidates']} == syms['kis/nxt/nxt_pre'] else 'FAIL'} universe=premarket n={len(pm['candidates']) if pm else 'MISSING'} acked={len(syms['kis/nxt/nxt_pre'])}")
# C2 L0 hour coverage and size vs trailing local days (per acked symbol)
H = {"ls/krx/regular": range(9, 20), "kis/krx/regular": range(9, 16), "kis/krx/krx_after": range(16, 20), "kis/nxt/nxt_after": range(16, 20), "kis/nxt/nxt_pre": [8], "kis/nxt/regular": range(9, 15)}
def nsym(g, d):
    fs = [x.replace(D, d) for x in groups[g]] if g in ("ls/krx/regular", "kis/nxt/nxt_pre") else glob.glob(groups[g][0].rsplit(".shard", 1)[0].replace(D, d) + ".shard-*.json") if groups.get(g) else []
    return len({s for p in fs if (m := jl(p)) for s, _ in acked(m)}) or None
for sdir in sorted(glob.glob("l0/*/*/*/*")):
    g = "/".join(sdir.split("/")[1:4]); fs = glob.glob(f"{sdir}/dt={D}/*.zst")
    hrs = {int(os.path.basename(x)[:2]) for x in fs}; miss = sorted(set(H.get(g, [])) - hrs); zero = sum(os.path.getsize(x) == 0 for x in fs)
    mb = sum(map(os.path.getsize, fs)) / 1e6
    v = "SKIP(shifted)" if not fs and shifted_d and g in ("kis/nxt/nxt_pre", "kis/nxt/regular") else "FAIL" if not fs or miss or zero else "PASS"
    print(f"C2 {v} stream={sdir[3:]} files={len(fs)} missing_hours={miss or '-'} zero={zero} mb={mb:.1f} acked={nsym(g, D) or 0}")
# C5 snapshot presence + run summary (row thresholds in Q container part)
kinds = sorted(os.path.basename(k) for k in glob.glob("l1/snapshot/*")); miss = [k for k in kinds if not os.path.exists(f"l1/snapshot/{k}/dt={D}.parquet")]
sm = [l for l in L if "stage=snapshot_summary" in l and (t := ts(l)) and t.date() == Dd]; ex = [l for l in L if "stage=snapshot_expired" in l and (t := ts(l)) and t.date() == Dd]
late_exp = [l for l in ex if ts(l).strftime("%H%M") > "0830"]; s = sm[-1] if sm else ""
v = "UNKNOWN" if not cover else "FAIL" if miss or not sm or f(s, "failed_jobs") != "0" or late_exp else ("WARN" if int(f(s, "expired") if f(s, "expired") != "-" else 0) > 45 else "PASS")
print(f"C5 {v} kinds={len(kinds) - len(miss)}/{len(kinds)} missing={miss or '-'} executed={f(s, 'executed')} expired={f(s, 'expired')} failed_jobs={f(s, 'failed_jobs')} expired_after_0830={len(late_exp)} no_success={sum('status=NO_SUCCESS' in l for l in L)} store_fail={sum('STORE_FAIL' in l for l in L)} dq_fail={sum('snapshot_dq' in l and 'status=FAIL' in l for l in L)}")
# B6 retention + Q2 L1 lag rule
cut = (Dd - dt.timedelta(days=3)).isoformat(); dg = {}
try: dg = dict(x.split("=", 1) for x in open(f"logs/eod_digests/{D}.txt").read().split("\n")[2:] if "=" in x)
except OSError: pass
held = {x.split(":")[0] for x in dg.get("repair_days", "none").split(",") if ":" in x}
bad = []; ret = {}
for sdir in sorted(glob.glob("l0/*/*/*/*")):
    ds = sorted(x.split("dt=")[1] for x in glob.glob(f"{sdir}/dt=*")); old = [d for d in ds if d < cut]
    ok_hold = sdir.endswith("/krx/regular/H0STCNT0") and sdir.split("/")[1] in ("ls", "kis")
    for d in old:
        if not (ok_hold and d in held and d >= (Dd - dt.timedelta(days=10)).isoformat()): bad.append(f"{sdir[3:]}@{d}")
    ret[sdir[3:]] = len([d for d in ds if cut <= d <= D])
q = len([x for x in glob.glob("quarantine/**", recursive=True) if os.path.isfile(x)])
l1old = [x for x in glob.glob("l1/**/*.parquet", recursive=True) if not x.startswith("l1/history/") and (m := re.search(r"dt=(\d{4}-\d\d-\d\d)", x)) and m.group(1) < (Dd - dt.timedelta(days=30)).isoformat()]
v = "FAIL" if bad or q else ("WARN" if max(ret.values(), default=0) > 4 or l1old else "PASS")
print(f"B6 {v} l0_dates_ge_cutoff_max={max(ret.values(), default=0)} cutoff={cut} unexpected_old_l0={bad[:4] or '-'} held_days={sorted(held) or '-'} quarantine_files={q} l1_older_30d={len(l1old)}")
miss = []
for l1dir in sorted(glob.glob("l1/*/*/*/H0*")):
    have = sorted(x[-18:-8] for x in glob.glob(f"{l1dir}/dt=*.parquet"))
    if not have: continue
    l0dir = "l0/" + l1dir[3:]; exp = [d for d in cal if have[0] <= d < cut and d < D]
    miss += [f"{l1dir[3:]}@{d}" for d in exp if d not in have and not os.path.isdir(f"{l0dir}/dt={d}")]
print(f"Q2 {'PASS' if not miss else 'FAIL'} rule=L1_exists_for_every_business_day_lt_D-3 missing={miss[:6] or '-'} (held L0 days are exempt until merged)")
# B4 offload parity of l1/ and manifest/ trees; B5 remote L0 vs local L0/L1
def check(src, dst, extra=()):
    p = subprocess.run(["nice", "-n", "19", R, "check", src, dst, "--one-way", "--size-only", "--combined", "-", *extra], capture_output=True, text=True)
    c = collections.Counter(l[:1] for l in p.stdout.splitlines()); st = [l[2:] for l in p.stdout.splitlines() if l[:1] in "+*" and os.path.getmtime(f"{src}/{l[2:]}") < eodt.timestamp()]
    return c, st, p.returncode
for src, dst, ex in (("l1", f"{RR}/l1", ("--exclude", "/history/minute/kis/**")), ("manifest", f"{RR}/manifests", ("--include", "*.json", "--include", "*.jsonl"))):
    c, st, rc = check(src, dst, ex)
    v = "UNKNOWN" if c["!"] or rc not in (0, 1) else ("FAIL" if st else "PASS")
    print(f"B4 {v} tree={src} match={c['=']} local_only={c['+']} size_diff={c['*']} err={c['!']} stale_before_eod={st[:4] or '-'}")
rl0 = subprocess.run([R, "lsf", "-R", "--dirs-only", "--max-depth", "5", f"{RR}/l0"], stdout=subprocess.PIPE, text=True, check=True).stdout.split()
rl1 = set(subprocess.run([R, "lsf", "-R", "--files-only", f"{RR}/l1"], stdout=subprocess.PIPE, text=True, check=True).stdout.split())
loss = []; pend = []
for x in rl0:
    if "/dt=" not in x: continue
    part = x.rstrip("/"); s, d = part.rsplit("/dt=", 1); l1 = f"{s}/dt={d}.parquet"
    if os.path.isdir(f"l0/{part}") or os.path.isdir(f"quarantine/{part}"): continue
    (pend if (l1 in rl1 or os.path.exists(f"l1/{l1}")) else loss).append(part)
print(f"B5 {'FAIL' if loss else ('WARN' if pend else 'PASS')} remote_l0_without_local_l0_and_any_l1={loss[:4] or '-'} purge_pending={pend[:4] or '-'} remote_l0_dirs={sum('/dt=' in x for x in rl0)}")
# Q1 + C6 L0 content of D (heavy, nice; skip with L0C=0)
if os.environ.get("L0C") != "1": print("Q1 SKIP(L0C=0)"); sys.exit(0)
WIN = {"regular": ("083000", "200059"), "krx_after": ("154000", "200059"), "nxt_after": ("153000", "200059"), "nxt_pre": ("075800", "085059")}
for sdir in sorted(glob.glob(f"l0/*/*/*/*/dt={D}")):
    vendor, venue, session, stream = sdir.split("/")[1:5]
    if vendor == "db": print(f"Q1 SKIP(db_format) stream={sdir[3:-14]}"); continue
    quote = stream.endswith("ASP0"); lo, hi = WIN[session]; stream_label = sdir[3:-14]
    n = bad = dup = reg = hmis = emp = ow = px0 = cr = size = 0; sym = collections.Counter(); last = {}; seen = _SequenceBitmap(Q1_MAX_BITMAP_PAGES)
    for fz in sorted(glob.glob(f"{sdir}/*.zst")):
        size += os.path.getsize(fz); fh = os.path.basename(fz)[:2]
        with subprocess.Popen(["nice", "-n", "19", "zstdcat", fz], stdout=subprocess.PIPE) as process:
            for line in process.stdout:
                n += 1
                try:
                    r = json.loads(line)
                    if vendor == "ls":
                        j = json.loads(r["raw"]); b = j["body"]; s = j["header"]["tr_key"]; t = b["hotime" if quote else "chetime"]
                        a1, b1, px = (int(b["offerho1"]), int(b["bidho1"]), 1) if quote else (0, 0, int(b["price"]))
                    else:
                        p = r["raw"].split("^"); s, t = p[0], p[1]; a1, b1, px = (int(p[3]), int(p[13]), 1) if quote else (0, 0, int(p[2]))
                except (ValueError, KeyError, IndexError, TypeError): bad += 1; continue
                conn_id, conn_seq = r.get("conn_id"), r.get("conn_seq")
                if not isinstance(conn_id, str) or not conn_id or type(conn_seq) is not int:
                    process.stdout.close(); process.terminate()
                    print(f"Q1 FAIL stream={stream_label} reason=invalid_sequence_key")
                    sys.exit(1)
                try: dup += seen.test_and_set(conn_id, conn_seq)
                except _SequenceBitmapLimitError:
                    process.stdout.close(); process.terminate()
                    print(f"Q1 FAIL stream={stream_label} reason=bitmap_page_limit limit_pages={Q1_MAX_BITMAP_PAGES}")
                    sys.exit(1)
                sym[s] += 1
                reg += conn_seq < last.get(conn_id, -1); last[conn_id] = conn_seq
                hmis += dt.datetime.fromtimestamp(r["recv_wall_ns"] / 1e9, K).strftime("%H") != fh
                if not t: emp += 1
                else: ow += not lo <= t <= hi
                px0 += px <= 0; cr += a1 > 0 and b1 > 0 and b1 >= a1
            rc = process.wait()
            if rc:
                print(f"Q1 FAIL stream={stream_label} reason=zstd_exit rc={rc}")
                sys.exit(1)
    g = f"{vendor}/{venue}/{session}"; pl = syms.get(g, set())
    v = "FAIL" if dup or reg else ("WARN" if bad or hmis or ow or px0 or cr or (emp and not (vendor == "ls" and quote and emp <= n * 1e-4)) else "PASS")
    print(f"Q1 {v} stream={sdir[3:-14]} rec={n} B_per_rec={size // max(n, 1)} parse_fail={bad} dup_seq={dup} seq_regress={reg} hour_mismatch={hmis} evt_empty={emp} out_win={ow} px_le0={px0} crossed={cr}")
    print(f"C6 INFO stream={sdir[3:-14]} syms={len(sym)} planned={len(pl)} no_data={len(pl - set(sym))} unplanned={len(set(sym) - pl)} thin_lt100={sum(1 for x in pl if 0 < sym[x] < 100)}")
EOF
)
PQ=$(cat <<'EOF'
import datetime as dt, glob, json, os, statistics, time
import polars as pl, pyarrow.parquet as pq
D = os.environ["D"]; t0 = time.time() - 26 * 3600; c = pl.col
def footer(f):
    md = pq.read_metadata(f); return md.num_rows, json.loads((md.metadata or {}).get(b"krx_alpha.dq", b"{}"))
# Q3 conservation: L1 regular H0STCNT0 rows == session-audit trades_received (independent L0 pass at the session's EOD)
for f in sorted(glob.glob("/app/data/l1/*/krx/regular/H0STCNT0/dt=*.parquet")):
    if os.path.getmtime(f) < t0: continue
    v, d = f.split("/l1/")[1].split("/")[0], f[-18:-8]; n, _ = footer(f)
    try: rec = json.load(open(f"/app/data/manifest/audit/{d}.{v}.regular.audit.json"))["totals"]["trades_received"]
    except (OSError, KeyError, ValueError): rec = None
    print(f"Q3 {'UNKNOWN' if rec is None else ('PASS' if n == rec else 'FAIL')} part={v}/{d} l1_rows={n} audit_trades_received={rec}")
# Q4 DQ footer of partitions written in the last 26 h
for f in sorted(glob.glob("/app/data/l1/*/*/*/H0*/dt=*.parquet")):
    if os.path.getmtime(f) < t0: continue
    n, dq = footer(f); t = dq.get("tick") or dq.get("quote") or {}
    print(f"Q4 {dq.get('status', 'NONE') if dq.get('status') in ('PASS', 'WARN', 'FAIL') else 'UNKNOWN'} part={f.split('/l1/')[1]} rows={n} footer_rows={t.get('rows')} nonzero={ {k: x for k, x in t.items() if x and k != 'rows'} } reasons={dq.get('reasons') or '-'}")
# C5 snapshot rows vs expectation for D
PER = {"security_status": (1, 1.0), "auction_book": (9, 1.0), "program_trade": (14, 0.99), "investor_estimate": (20, 0.95)}
FIX = {"stock_minute_bar": (23460, 1.0), "index_minute_bar": (1173, 1.0), "index_snapshot": (234, 0.99), "ranking": (23400, 0.96)}
plan = set()
for mf in [f"/app/data/manifest/{D}.json", *sorted(glob.glob(f"/app/data/manifest/ls/{D}.krx.shard-*.json"))]:
    try: plan |= {a["symbol"] for a in json.load(open(mf))["subscription_acks"] if a.get("accepted")}
    except OSError: pass
for k in sorted(glob.glob("/app/data/l1/snapshot/*")):
    kind = os.path.basename(k); f = f"{k}/dt={D}.parquet"
    if not os.path.exists(f): print(f"C5 FAIL kind={kind} MISSING"); continue
    df = pl.read_parquet(f, columns=[x for x in ("symbol", "session_date", "observed_at_ns") if x in pq.read_schema(f).names]); n = df.height
    if kind in PER: exp, lo = PER[kind][0] * len(plan), PER[kind][1]
    elif kind in FIX: exp, lo = FIX[kind]
    else:
        prev = sorted(x for x in glob.glob(f"{k}/dt=*.parquet") if x < f)[-5:]; exp = statistics.median([pq.read_metadata(x).num_rows for x in prev]) if prev else n; lo = 0.90
    if kind == "investor_estimate" and "observed_at_ns" in df.columns:  # rows are cumulative per poll (5 buckets x symbols at the last poll); total rows depend on the poll schedule, so judge the last poll only
        last = int(df["observed_at_ns"].max()); n = int((df["observed_at_ns"] >= last - 180 * 10**9).sum()); exp, lo = 5 * len(plan), 0.95
    if kind == "ranking" and "observed_at_ns" in df.columns:  # a gainers list on a broad down day returns < 30 names per poll (11-23: 21-29 rows, min change 0.0%); judge polls (2 lists x 390), not rows
        n = int(df["observed_at_ns"].n_unique()); exp, lo = FIX[kind][0] / 30, 0.96
    sd = int((df["session_date"].cast(pl.String) != D).sum()) if "session_date" in df.columns else 0
    nul = sum(int(df[x].null_count()) for x in df.columns); symd = len(set(df["symbol"].unique().to_list()) ^ plan) if kind in PER and PER[kind][1] == 1.0 else 0
    r = n / exp if exp else 0; ok = lo <= r <= (1.10 if kind == "news_title" else 1.0 + 1e-9 if kind in FIX and FIX[kind][1] == 1.0 else 10) and not sd and not nul and not symd
    print(f"C5 {'PASS' if ok else 'FAIL'} kind={kind} rows={n} expected={exp:.0f} ratio={r:.3f} bad_date={sd} nulls={nul} symdiff={symd}")
# Q5 snapshot content invariants (newest partitions)
def last(kind): return sorted(glob.glob(f"/app/data/l1/snapshot/{kind}/dt=*.parquet"))[-1]
def cnt(df, e): return int(df.select(e.sum()).item())
d = pl.read_parquet(last("stock_minute_bar"), columns=["symbol", "bar_time", "open", "high", "low", "close"])
b1 = cnt(d, (c("high") < c("low")) | (c("open") > c("high")) | (c("open") < c("low")) | (c("close") > c("high")) | (c("close") < c("low"))); d1 = d.height - d.select("symbol", "bar_time").unique().height
d = pl.read_parquet(last("news_title"), columns=["news_id", "title"]); d2 = d.height - d["news_id"].n_unique(); n2 = int(d["title"].null_count())
d = pl.read_parquet(last("program_trade"), columns=["net_qty", "buy_qty", "sell_qty"]); m3 = cnt(d, (c("net_qty") - (c("buy_qty") - c("sell_qty"))).abs() > 0)
print(f"Q5 {'PASS' if not (b1 or d1 or d2 or n2 or m3) else 'FAIL'} minute_bad_ohlc={b1} minute_dup={d1} news_dup_id={d2} news_null_title={n2} program_net_mismatch={m3}")
# C7 daily bars and program-trade stores
# Only the two newest monthly files: they hold the last 6 dates and every row EOD can have rewritten (full-history scan peaked ~800 MB > 384m, OOM 2026-11-02).
bf = sorted(glob.glob("/app/data/bars/daily/*.parquet"))[-2:]; bd = pl.scan_parquet(bf).select("date", "symbol", "high", "low", "close")
b = bd.group_by("date").agg(pl.len().alias("n"), (c("high") < c("low")).sum().alias("hl"), (c("close") <= 0).sum().alias("c0")).sort("date").tail(6).collect()
p = pl.scan_parquet(sorted(glob.glob("/app/data/bars/program_trades/*.parquet"))[-2:]).group_by("date").len().sort("date").tail(6).collect()
nb = b["n"].to_list(); med = statistics.median(nb[:-1]); dup = bd.group_by(["date", "symbol"]).len().filter(c("len") > 1).select(pl.len()).collect().item()
print(f"C7 {'PASS' if abs(nb[-1] / med - 1) <= 0.005 and not b['hl'].sum() and not b['c0'].sum() and not dup else 'FAIL'} bars_latest={b['date'][-1]} n={nb[-1]} median={med} dup={dup} program_latest={p['date'][-1]} program_n={p['len'][-1]}")
EOF
)
nice -n 19 docker logs --since 16h krx-collector 2>&1 | grep -E 'level=CRITICAL|stage=(digest|program_trades_sync|program_trades_gap|snapshot_summary|snapshot_expired|snapshot_dq|snapshot_job)' | python3 -u -c "$PY"
statuses=("${PIPESTATUS[@]}")
result=0
if [ "${statuses[0]}" -ne 0 ]; then result="${statuses[0]}"
elif [ "${statuses[1]}" -gt 1 ]; then result="${statuses[1]}"
elif [ "${statuses[2]}" -ne 0 ]; then result="${statuses[2]}"
fi
echo "A4 INFO $(docker exec krx-collector cat /app/data/work/alert_ledger.json 2>/dev/null | python3 -c 'import json,sys; d=json.load(sys.stdin); print("ledger_day=%s sent_today=%s keys=%d cap_reached=%s" % (d["day"], d["sent_today"], len(d["keys"]), d["sent_today"] >= 20))' 2>/dev/null || echo ledger_unreadable)"
if [ "${PQRUN:-1}" = 1 ]; then
  nice -n 19 docker run --rm -i -m 384m --memory-swap 384m --network none -e D -e POLARS_MAX_THREADS=2 -v "$HOME/krx-alpha/data:/app/data:ro" --entrypoint /app/.venv/bin/python ghcr.io/kthyeong/krx-collector:latest -u -c "$PQ" 2>&1
  rc=$?
  [ "$result" -eq 0 ] && result="$rc"
fi
exit "$result"
