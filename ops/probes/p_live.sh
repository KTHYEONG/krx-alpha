#!/usr/bin/env bash
# Krx window probe (live). Read-only; prints verdict lines only. Run from $HOME/krx-alpha.
export TZ=Asia/Seoul LC_ALL=C
cd "$HOME/krx-alpha/data" || { echo "CTX UNKNOWN reason=no_data_root"; exit 0; }
PY=$(cat <<'EOF'
import datetime as dt, glob, json, os, re, subprocess, sys
K = dt.timezone(dt.timedelta(hours=9)); now = dt.datetime.now(K); D = now.date().isoformat(); hm = now.strftime("%H%M")
cal = sorted(f[-15:-5] for f in glob.glob("calendar/dt=*.json")); biz = D in cal
def sh(*a): return subprocess.run(list(a), capture_output=True, text=True).stdout.strip()
ins = sh("docker", "inspect", "krx-collector", "--format", "{{.State.Status}}|{{.RestartCount}}|{{.State.OOMKilled}}|{{.State.StartedAt}}").split("|")
mem = sh("docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", "krx-collector").split(" /")[0]
print(f"CTX now={now:%F %T %a} kind={'BUSINESS' if biz else 'CLOSED'} D={D}")
if not biz: print("C1 SKIP(closed_day)"); sys.exit(0)
L = sys.stdin.read().splitlines()
def ts(l):
    try: return dt.datetime.fromisoformat(l[:29])
    except ValueError: return None
def since(m): return [l for l in L if (t := ts(l)) and t >= now - dt.timedelta(minutes=m)]
f = lambda l, k: (re.search(rf"\b{k}=(\S+)", l) or [None, "-"])[1]
mib = float(re.sub(r"[^0-9.]", "", mem) or 0) * (1024 if "GiB" in mem else 1)
print(f"H2 {'PASS' if mib < 2458 and ins[2] == 'false' else ('FAIL' if ins[2] == 'true' else 'WARN')} mem_mib={mib:.0f} restarts={ins[1]} oom={ins[2]}")
hb = [l for l in since(16) if "stage=heartbeat" in l]
st = f(hb[-1], "state") if hb else "-"; alive = f(hb[-1], "streamer_alive") if hb else "-"
need = st in ("FULL_ACTIVE", "AFTER_MARKET_ACTIVE")
v = "FAIL" if not hb or (need and alive != "True") else ("WARN" if f(hb[-1], "streamer_restarts") not in ("0", "-") else "PASS")
rs = [l for l in L if "stage=session_resume" in l]
rsv = "-" if not rs else ("OK" if "status=OK" in rs[-1] else f"REFUSED({f(rs[-1], 'reason')})")
print(f"R2 {v} state={st} streamer_alive={alive} streamer_restarts={f(hb[-1], 'streamer_restarts') if hb else '-'} circuit_open={sum('circuit_open' in l for l in L)} no_first_frame={sum('NO_FIRST_FRAME' in l for l in L)} resume={rsv}")
if hm >= "0821":
    o = [l for l in L if "stage=orchestration status=" in l]; c = json.load(open("candidates.json")); cs = [x["symbol"] for x in c["candidates"]]
    P = max(d for d in cal if d < D).replace("-", "")
    v = "PASS" if o and "status=OK" in o[-1] and "ready=True" in o[-1] and str(c["rev"]) == P and 0 < len(cs) == len(set(cs)) <= 200 else ("WARN" if hm < "0850" else "FAIL")
    pf = [l for l in L if "kis_token_preflight" in l and "result=fail" in l]
    print(f"R3 {v} orchestration={f(o[-1], 'status') if o else '-'} cand_rev={c['rev']} expect_rev={P} n={len(cs)} unique={len(set(cs))} preflight_fail={len(pf)}")
    sub = [l for l in L if "stage=ls_subscriptions" in l]; cov = [l for l in L if "stage=regular_feed_coverage" in l]
    lim = [l for l in L if re.search(r"stage=ls_limits.*VIOLATION|reason=(ls_limit|session_cap)", l)]
    nsh = 1 + len(glob.glob(f"manifest/ls/{D}.krx.shard-*.json")); last = sub[-nsh:]
    acks = set()  # unique accepted (symbol, tr_id) over every LS shard manifest: a reconnect re-subscribes and appends duplicate acks, and summing the last N log lines double counted it
    for mf in [f"manifest/{D}.json", *sorted(glob.glob(f"manifest/ls/{D}.krx.shard-*.json"))]:
        try: acks |= {(a["symbol"], a["tr_id"]) for a in json.load(open(mf))["subscription_acks"] if a.get("accepted")}
        except (OSError, ValueError, KeyError): pass
    sa = len(acks); sp = sa + sum(int(f(l, "rejected_pairs")) for l in last if f(l, "rejected_pairs").isdigit())
    ok = sub and sa == sp == 2 * len(cs) and (hm < "0829" or (cov and f(cov[-1], "uncovered") == "0")) and not lim
    print(f"C4 {'PASS' if ok else 'FAIL'} shards={nsh} pairs={sp} accepted={sa} want={2 * len(cs)} uncovered={f(cov[-1], 'uncovered') if cov else '-'} kis_planned={f(cov[-1], 'kis_planned') if cov else '-'} ls_limit_hits={len(lim)}")
W = {"ls/krx/regular": [("0905", "1520", 180), ("1605", "2000", 180)], "kis/krx/regular": [("0905", "1520", 120)], "kis/nxt/regular": [("0905", "1520", 120)],
     "kis/krx/krx_after": [("1605", "2000", 120)], "kis/nxt/nxt_after": [("1545", "2000", 120)], "kis/nxt/nxt_pre": [("0802", "0850", 120)]}
_cal = json.load(open(f"calendar/dt={D}.json")); shifted = _cal.get("regular_open", "09:00:00") != "09:00:00"  # NXT premarket/regular are not collected on KRX-shifted days
_m = lambda t: int(t[:2]) * 60 + int(t[2:4])
_open_sh = _m(_cal.get("regular_open", "09:00:00").replace(":", "")) - 540; _close_sh = _m(_cal.get("regular_close", "15:30:00").replace(":", "")) - 930  # shifted-day window offsets (11-19: +60/+60); only the after-market starts and the regular edges move, the 20:00 end is fixed
_adj = lambda t, d: f"{(_m(t) + d) // 60:02d}{(_m(t) + d) % 60:02d}"
W = {k: [(_adj(a, _open_sh) if a == "0905" else _adj(a, _close_sh) if a in ("1605", "1545") else a, _adj(b, _close_sh) if b == "1520" else b, x) for a, b, x in v] if k != "kis/nxt/nxt_pre" else v for k, v in W.items()}
for s, wins in W.items():
    lim = None if shifted and s in ("kis/nxt/regular", "kis/nxt/nxt_pre") else next((x for a, b, x in wins if a <= hm < b), None)
    fs = glob.glob(f"l0/{s}/*/dt={D}/*.zst"); age = max(0, now.timestamp() - max((os.path.getmtime(x) for x in fs), default=0)) if fs else float("inf")
    print(f"C1 {'SKIP(outside_window)' if lim is None else ('PASS' if age <= lim else 'FAIL')} stream={s} newest_age_s={age:.0f} limit_s={lim} files={len(fs)} zero_size={sum(os.path.getsize(x) == 0 for x in fs)}")
crit = [l for l in since(30) if re.search(r"level=(CRITICAL|ERROR)|Traceback", l)]
print(f"A2 {'PASS' if not crit else 'WARN'} crit_err_30m={len(crit)} keys={sorted({f(l, 'stage') + ':' + f(l, 'reason') for l in crit})[:5]}")
EOF
)
nice -n 19 docker logs --since 13h krx-collector 2>&1 | grep -E 'stage=(heartbeat|orchestration|kis_token_preflight|ls_subscriptions|ls_limits|regular_feed_coverage|ingest_watchdog)|circuit_open|NO_FIRST_FRAME|reason=(ls_limit|session_cap)|level=(CRITICAL|ERROR)|Traceback' | python3 -c "$PY"
