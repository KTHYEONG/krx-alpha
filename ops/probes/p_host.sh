#!/usr/bin/env bash
# Krx window probe (host). Read-only; prints verdict lines only. Run from $HOME/krx-alpha.
export TZ=Asia/Seoul LC_ALL=C
v() { echo "$*"; }
echo "CTX-HOST now=$(date '+%F %T %a') host=$(hostname)"
# X1 clock
o=$(chronyc tracking 2>/dev/null | awk '/^System time/{print ($5=="slow"?-1:1)*$4}'); l=$(chronyc tracking 2>/dev/null | awk -F': ' '/^Leap status/{print $2}')
if [ -z "$o" ]; then v "X1 UNKNOWN reason=chronyc_unavailable"; else
  a=$(awk -v x="$o" 'BEGIN{x=x<0?-x:x; print (x<0.5)?"PASS":(x<=2.0?"WARN":"FAIL")}'); [ "$l" != "Normal" ] && a=FAIL; v "X1 $a offset_s=$o leap=$l"; fi
# X2 disk
read -r pct avail <<<"$(df -BG --output=pcent,avail / | tail -1 | tr -d '%G')"
s=PASS; [ "$pct" -ge 70 ] && s=WARN; [ "$pct" -gt 85 ] || [ "$avail" -lt 5 ] && s=FAIL; v "X2 $s used_pct=$pct free_gb=$avail"
# X3 failed units + alert template
fu=$(systemctl --user --failed --no-legend --plain 2>/dev/null | awk '{print $1}' | tr '\n' ','); fs=$(systemctl --failed --no-legend --plain 2>/dev/null | awk '{print $1}' | tr '\n' ',')
tpl=$(systemctl --user cat kca-alert@.service >/dev/null 2>&1 && echo ok || echo MISSING)
s=PASS; [ -n "$fu$fs" ] && s=WARN; echo "$fu" | grep -qE 'krx-|vps-image-gc' && s=FAIL; [ "$tpl" != ok ] && s=FAIL
v "X3 $s user_failed=${fu:--} system_failed=${fs:--} alert_template=$tpl"
# X4 broker dir contract v1 (names only, never contents)
d=$HOME/.cache/kis
mk=$([ -f "$d/.host-admission" ] && echo present || echo MISSING)
un=$(ls -A "$d" 2>/dev/null | grep -vE '^(\.host-admission|token_(ls|toss|dbsec|kiwoom)_[0-9a-f]{12}\.json(\.lock)?|token_issue_(ls|toss|dbsec|kiwoom)_[0-9a-f]{12}\.state(\.lock)?|admission_(ls|toss|dbsec|kiwoom)_[0-9a-f]{12}(_[A-Za-z0-9_-]+)?\.state|token_[0-9a-f]{12}\.json(\.lock)?|tps_[0-9a-f]{12}\.state|wslease_(ls|kis|db|toss)_[0-9a-f]{12}\.(json|lock))$' | sed -E 's/[0-9a-f]{12}/<sha>/g' | tr '\n' ',')
k1=$(docker exec krx-collector sha256sum /app/deploy/shared/host_broker_contract.py 2>/dev/null | cut -c1-12)
k2=$(sha256sum "$HOME/k-closing-alpha/src/api/host_broker_contract.py" 2>/dev/null | cut -c1-12)
k3=$(docker run --rm --network none --entrypoint sha256sum ghcr.io/kthyeong/k-stock-engine:latest /app/src/integrations/host_broker_contract.py 2>/dev/null | cut -c1-12)
ce=$(journalctl --user --since '24 hours ago' --no-pager 2>/dev/null | grep -cE 'IGW00201|EGW00201|TokenIssueThrottled|HostBrokerDirNotShared|SCHEMA_INVALID')
s=PASS; [ -n "$un" ] && s=WARN; [ "$ce" -gt 0 ] && s=WARN; { [ -z "$k1" ] || [ "$k1" != "$k2" ] || [ "$k1" != "$k3" ]; } && s=FAIL; [ "$mk" != present ] && s=FAIL
v "X4 $s marker=$mk unexpected=${un:--} sha_krx=${k1:-?} sha_kca=${k2:-?} sha_kse=${k3:-?} unit_contract_errors_24h=$ce"
# X5 Drive lock: current holders and backup schedule
lt=$(readlink -f "/run/user/$(id -u)/quant-gdrive.lock" 2>/dev/null)
h=$(for f in /proc/[0-9]*/fd/*; do [ "$(readlink "$f" 2>/dev/null)" = "$lt" ] && p=${f#/proc/} && echo "${p%%/*}:$(tr '\0' ' ' </proc/${p%%/*}/cmdline 2>/dev/null | cut -c1-60 | tr ' ' '_')"; done | sort -u | tr '\n' ',')
sch=$(systemctl --user list-timers --all --no-pager --no-legend 2>/dev/null | grep -E '(krx-host|kca|kse|crypto-pilot)-backup\.timer' | awk '{for(i=1;i<=NF;i++) if($i ~ /\.timer$/) u=$i; print u"@"$3}' | tr '\n' ',')
v "X5 PASS lock_holders_now=${h:-none} backup_next_kst=$sch"
# X6 vps-deploy-lib.sh parity (canonical krx copy)
c=$(sha256sum "$HOME/krx-alpha/deploy/host/vps-deploy-lib.sh" 2>/dev/null | cut -c1-12); bad=""
for f in "$HOME/k-closing-alpha/deploy/vps-deploy-lib.sh" "$HOME/crypto-pilot/deploy/vps-deploy-lib.sh"; do x=$(sha256sum "$f" 2>/dev/null | cut -c1-12); [ "$x" != "$c" ] && bad="$bad${f#$HOME/}:${x:-missing},"; done
v "X6 $([ -z "$bad" ] && [ -n "$c" ] && echo PASS || echo FAIL) canonical=${c:-missing} mismatch=${bad:--}"
# X7 unattended upgrades outside 08:10-22:00 KST
oc=$(systemctl cat apt-daily-upgrade.timer 2>/dev/null | grep -E '^OnCalendar=.+' | tail -1)
last=$(systemctl show apt-daily-upgrade.timer -p LastTriggerUSec --value 2>/dev/null); lh=$(date -d "$last" +%H%M 2>/dev/null || echo ?)
s=PASS; echo "$oc" | grep -q '19:00:00 UTC' || s=FAIL; [ "$lh" != "?" ] && [ "$lh" -ge 0810 ] && [ "$lh" -lt 2200 ] && s=FAIL
v "X7 $s oncalendar='${oc#OnCalendar=}' last_kst=$lh"
# X8 image gc
g=$(journalctl --user -u vps-image-gc --since '26 hours ago' --no-pager 2>/dev/null | grep -o 'stage=image_gc status=[A-Z]*.*' | tail -1)
s=FAIL; echo "$g" | grep -q 'status=OK' && s=PASS; echo "$g" | grep -qE 'status=(DEGRADED|SKIPPED)' && s=WARN
v "X8 $s ${g:-no_run_26h}"
# X9 key/secret rotation applied (env_file is read only when the container is created)
st=$(docker inspect krx-collector --format '{{.State.StartedAt}}' 2>/dev/null); se=$(date -d "$st" +%s 2>/dev/null || echo 0); late=""
for f in krx-alpha.env kis-data.env; do m=$(stat -c %Y "$HOME/quant-secrets/$f" 2>/dev/null || echo 0); [ "$m" -gt "$se" ] && late="$late$f@$(date -d "@$m" +%m-%d_%H:%M),"; done
s=PASS; [ -n "$late" ] && s=WARN; [ "$se" = 0 ] && s=UNKNOWN
v "X9 $s env_newer_than_container=${late:--} container_started=$(date -d "$st" +%m-%d_%H:%M 2>/dev/null || echo ?)"
# X10 loose ad-hoc files left by operators/agents (names, count, size only; never deletes)
lf=$(find /tmp -maxdepth 1 -user "$(id -un)" -mmin +1440 \( -name '*.py' -o -name '*.sh' -o -name '*.log' -o -name '*.parquet' -o -name '*.json' -o -name 'kca_*' -o -name 'krx_*' -o -name 'kse_*' -o -name 'p0*_*' \) 2>/dev/null)
n=$(printf '%s' "$lf" | grep -c . ); mb=$(printf '%s' "$lf" | xargs -r du -sm 2>/dev/null | awk '{s+=$1} END{print s+0}')
s=PASS; [ "$n" -gt 0 ] && s=WARN; v "X10 $s loose_tmp_files=$n size_mb=$mb sample=$(printf '%s' "$lf" | head -3 | tr '\n' ',')"
