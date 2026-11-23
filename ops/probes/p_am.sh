#!/usr/bin/env bash
# Krx window probe (AM). Read-only; prints verdict lines only. Run from $HOME/krx-alpha.
export TZ=Asia/Seoul LC_ALL=C
cd "$HOME/krx-alpha/data" || { echo "CTX UNKNOWN reason=no_data_root"; exit 0; }
PY=$(cat <<'EOF'
import datetime as dt, glob, json, os, re, subprocess, sys
K = dt.timezone(dt.timedelta(hours=9)); now = dt.datetime.now(K); today = now.date().isoformat()
R = os.path.expanduser("~/.local/bin/rclone"); RR = "gdrive:quant-lake/live/krx-alpha/data"
def sh(*a): return subprocess.run(list(a), capture_output=True, text=True).stdout.strip()
def ts(line):
    try: return dt.datetime.fromisoformat(line[:29])
    except ValueError: return None
cal = sorted(f[-15:-5] for f in glob.glob("calendar/dt=*.json"))
P = max([d for d in cal if d < today], default="?"); N = min([d for d in cal if d > today], default="?")
wd = now.weekday(); kind = "WEEKEND" if wd >= 5 else ("BUSINESS" if today in cal else "HOLIDAY")
ins = sh("docker", "inspect", "krx-collector", "--format", "{{.State.Status}}|{{.State.StartedAt}}|{{.RestartCount}}|{{.State.OOMKilled}}|{{index .Config.Labels \"org.opencontainers.image.revision\"}}").split("|")
latest = sh("docker", "image", "inspect", "--format", "{{index .Config.Labels \"org.opencontainers.image.revision\"}}", "ghcr.io/kthyeong/krx-collector:latest")
print(f"CTX now={now:%F %T %a} kind={kind} D={today if kind == 'BUSINESS' else P} P={P} N={N} rev={ins[-1][:7] if len(ins) > 4 else '?'} latest={latest[:7]}")
# H1 container
if len(ins) < 5: print("H1 UNKNOWN reason=inspect_failed")
else:
    st, started, rc, oom, rev = ins; s0 = dt.datetime.fromisoformat(started[:19] + "+00:00").astimezone(K)
    infreeze = s0.date().isoformat() in cal and dt.time(8, 10) <= s0.time() < dt.time(22, 0)
    v = "FAIL" if st != "running" or oom == "true" or rc != "0" or infreeze else ("WARN" if rev != latest else "PASS")
    print(f"H1 {v} status={st} started_kst={s0:%m-%d %H:%M} restarts={rc} oom={oom} rev_eq_latest={rev == latest} start_in_freeze={infreeze}")
# H3 timers
en = {u: sh("systemctl", "--user", "is-enabled", u) for u in ("krx-host-backup.timer", "krx-deferred-recreate.timer", "vps-image-gc.timer")}
print(f"H3 {'PASS' if all(x == 'enabled' for x in en.values()) else 'FAIL'} " + " ".join(f"{k.split('.')[0]}={x}" for k, x in en.items()))
# H4 deferred recreate (last 26 h)
j = sh("journalctl", "--user", "-u", "krx-deferred-recreate", "--since", "26 hours ago", "--no-pager")
res = sh("systemctl", "--user", "show", "krx-deferred-recreate.service", "-p", "Result", "--value")
ran = "deferred_verify status=start" in j; ok = "deferred_verify status=ok" in j
print(f"H4 {'SKIP(not_run_26h)' if not ran and res == 'success' else ('PASS' if res == 'success' and (ok or not ran) else 'FAIL')} ran={ran} verify_ok={ok} result={res} mismatch={'running_revision_mismatch' in j}")
# B1 host backup status (audit 26 h, daemon alarm 36 h)
try:
    b = json.load(open(os.path.expanduser("~/.local/state/krx-alpha/host_backup_status.json")))
    age = (now - dt.datetime.fromisoformat(b["last_ok_at"]).astimezone(K)).total_seconds() / 3600 if b.get("last_ok_at") else 1e9
    v = "PASS" if b["rc"] == 0 and age < 26 else ("WARN" if age <= 36 else "FAIL")
    print(f"B1 {v} rc={b['rc']} data_rc={b['data_rc']} prune_rc={b['prune_rc']} last_ok_age_h={age:.1f} lock_wait_s={b['lock_wait_s']} holders={len(b.get('lock_holders') or [])}")
except Exception as e: print(f"B1 UNKNOWN reason={type(e).__name__}")
# B2 raw L0 offsite parity for every local date < today; same-day exposure
dates = sorted({p.split("dt=")[1] for p in glob.glob("l0/*/*/*/*/dt=*")})
old = [d for d in dates if d < today]
expo = sum(os.path.getsize(f) for f in glob.glob(f"l0/*/*/*/*/dt={today}/*"))
if old:
    cmd = ["nice", "-n", "19", R, "check", "l0", f"{RR}/l0", "--one-way", "--size-only", "--combined", "-"] + sum((["--include", f"/*/*/*/*/dt={d}/**"] for d in old), [])
    p = subprocess.run(cmd, capture_output=True, text=True); c = {k: 0 for k in "=+*!-"}
    for l in p.stdout.splitlines():
        if l[:1] in c: c[l[:1]] += 1
    bad = [l for l in p.stdout.splitlines() if l[:1] in "+*!"][:3]
    v = "PASS" if c["+"] == c["*"] == c["!"] == 0 and p.returncode in (0,) else ("UNKNOWN" if c["!"] or p.returncode not in (0, 1) else "FAIL")
    print(f"B2 {v} dates={old[0]}..{old[-1]}({len(old)}) match={c['=']} local_only={c['+']} size_diff={c['*']} err={c['!']} rc={p.returncode} today_raw_local_only_mb={expo / 1e6:.0f} sample={bad}")
else: print(f"B2 SKIP(no_local_l0_before_today) today_raw_local_only_mb={expo / 1e6:.0f}")
# log pass (stdin = single union-regex docker logs pass)
L = sys.stdin.read().splitlines()
night0 = dt.datetime.combine(now.date() - dt.timedelta(days=1 if now.time() < dt.time(23, 30) else 0), dt.time(23, 30), K)
nl = [l for l in L if (t := ts(l)) and t >= night0]
def last(pat, src=nl):
    m = [l for l in src if re.search(pat, l)]; return m[-1] if m else ""
start = [l for l in L if "stage=start status=ONLINE" in l]
cover = len(ins) > 4 and dt.datetime.fromisoformat(ins[1][:19] + "+00:00") <= night0
# R5 nightly chain tick_tape -> repair -> minute_bars
tt, rp, mb = (last(rf"stage=nightly_{j} status=(OK|NOOP|FAIL)") for j in ("tick_tape", "repair", "minute_bars"))
tsum, rsum = last(r"stage=tick_tape_backfill status="), last(r"stage=repair_ticks status=(OK|DEGRADED|FAIL|SKIP) run_id")
mism = [re.search(r"day=(\S+)", l).group(1) for l in nl if "stage=repair_ticks status=MISMATCH" in l]
f = lambda l, k: (re.search(rf"\b{k}=(\S+)", l) or [None, "-"])[1]
v = "PASS"
if not cover: v = "UNKNOWN"
elif "status=FAIL" in tt or "status=FAIL" in rp or f(tsum, "failed") not in ("0", "-") or f(tsum, "oversize") not in ("0", "-") or "status=FAIL" in rsum: v = "FAIL"
elif not tt or not rp: v = "FAIL"
elif "DEGRADED" in rsum or mism or f(tsum, "edge_partial") not in ("0", "-"): v = "WARN"
print(f"R5 {v} logs_cover_night={cover} tick_tape={f(tt, 'status')} tape_symbol_days={f(tsum, 'symbol_days')} tape_failed={f(tsum, 'failed')} oversize={f(tsum, 'oversize')} repair={f(rp, 'status')} repair_summary={f(rsum, 'status')} merged={f(rsum, 'merged')} failed={f(rsum, 'failed')} mismatch_days={','.join(sorted(set(mism))) or '-'} minute_bars={f(mb, 'status') if mb else 'disabled'}")
# events (persistent) for repair gate and alert/criticals
ev = []
for fn in sorted(glob.glob("logs/events-*.jsonl")):
    for l in open(fn, errors="replace"):
        try: r = json.loads(l)
        except ValueError: continue
        if r["ts"] >= (now - dt.timedelta(hours=26)).isoformat(): ev.append(r)
exp = [r for r in ev if r["fields"].get("stage") == "repair_hold" and r["fields"].get("status") == "EXPIRED"]
ovd = [r["fields"].get("days") for r in ev if r["fields"].get("stage") == "repair_overdue" and r["fields"].get("status") == "FAIL"]
dg = {}
try: dg = dict(x.split("=", 1) for x in open(f"logs/eod_digests/{P}.txt").read().split("\n")[2:] if "=" in x)
except OSError: pass
print(f"R6 {'FAIL' if exp else ('WARN' if ovd or dg.get('repair_held', '0') != '0' else 'PASS')} hold_expired={len(exp)} overdue={';'.join(x for x in ovd if x) or '-'} repair_days_P={dg.get('repair_days', '?')} repair_held_P={dg.get('repair_held', '?')}")
# R4 previous business day's EOD digest
if not dg: print(f"R4 FAIL day={P} digest=MISSING")
else:
    v = "PASS" if dg.get("status") == "OK" else "WARN"
    if dg.get("backup_missing", "0") != "0" or dg.get("host_backup") != "ok" or "offload" in dg.get("degraded_reasons", "") or "maintenance" in dg.get("degraded_reasons", ""): v = "FAIL"
    print(f"R4 {v} day={P} status={dg.get('status')} degraded={dg.get('degraded_reasons')} reconciled={dg.get('reconciled')} loss={dg.get('regular_trade_loss')} uploaded={dg.get('uploaded')} deleted={dg.get('deleted_partitions')} backup_missing={dg.get('backup_missing')} host_backup={dg.get('host_backup')}")
# R1 calendar / R8 premarket pool for today
cc = json.load(open("calendar_cache.json"))
if kind == "BUSINESS":
    a = json.load(open(f"calendar/dt={today}.json")); u = f"universe/premarket/{today}.json"
    print(f"R1 {'PASS' if a.get('source') == 'vendor' else 'WARN'} anchors_source={a.get('source')} open={a.get('regular_open')} close={a.get('regular_close')} cache_date={cc.get('date')}")
    if os.path.exists(u):
        q = json.load(open(u)); n = len(q["candidates"]); want = min(q["capacity"], q["eligible_count"])
        print(f"R8 {'PASS' if n == q['selected_count'] == want and q['effective_from'][11:16] == '07:58' else 'FAIL'} pool={today} n={n} selected={q['selected_count']} want={want} generated={q['generated_at'][:16]}")
    else: print(f"R8 FAIL pool={today} MISSING")
else: print(f"R1 SKIP({kind}) cache_date={cc.get('date')}"); print(f"R8 SKIP({kind})")
# A1 alert path armed since last start
al = [l for l in L if "stage=alert status=" in l]
hcl = [l for l in L if "stage=healthcheck status=" in l and (t := ts(l)) and t >= now - dt.timedelta(hours=24)]
hf = sum("status=FAIL" in l for l in hcl); open_fail = bool(hcl) and "status=FAIL" in hcl[-1]
v = "UNKNOWN" if not al else ("FAIL" if "ENABLED" not in al[-1] or open_fail else ("WARN" if hf > 5 else "PASS"))
print(f"A1 {v} alert={f(al[-1], 'status') if al else '-'} healthcheck_fail_24h={hf} unrecovered={open_fail}")
EOF
)
nice -n 19 docker logs krx-collector 2>&1 | grep -E 'stage=(start|alert|healthcheck|nightly_|tick_tape_backfill|repair_ticks|repair_overdue|repair_hold)' | python3 -c "$PY"
