#!/usr/bin/env bash
# Waits (bounded) for a nightly repair job to release its flock before the
# collector is recreated. Always exits 0: every path proceeds with the recreate.
set -Eeuo pipefail

: "${KRX_REPAIR_LOCK:=$HOME/krx-alpha/data/work/repair.lock}"
: "${KRX_PROC_LOCKS:=/proc/locks}"
: "${KRX_REPAIR_WAIT_MAX_S:=900}"
: "${KRX_REPAIR_WAIT_POLL_S:=30}"

validate_int() {
  local name="$1" default="$2" value="${!1}"
  if [[ ! "$value" =~ ^[0-9]+$ ]] || (( 10#$value < 1 )); then
    echo "[SYS] stage=repair_lock_wait status=bad_config var=$name using=$default"
    printf -v "$name" '%s' "$default"
  fi
}
validate_int KRX_REPAIR_WAIT_MAX_S 900
validate_int KRX_REPAIR_WAIT_POLL_S 30

idle() {
  echo "[SYS] stage=repair_lock_wait status=idle reason=$1 waited_s=${2:-0}"
  exit 0
}

[[ -e "$KRX_REPAIR_LOCK" ]] || idle no_lock_file
stat_out="$(stat -c '%d %i' "$KRX_REPAIR_LOCK" 2>/dev/null)" || idle lock_unstatable
read -r dev ino <<<"$stat_out"
key="$(printf '%02x:%02x:%d' "$(( (dev >> 8) & 0xfff ))" "$(( (dev & 0xff) | ((dev >> 12) & 0xfff00) ))" "$ino")"

[[ -r "$KRX_PROC_LOCKS" ]] || idle locks_unreadable

holders() {
  awk -v key="$key" '$2 == "FLOCK" && $6 == key { pids = pids (pids == "" ? "" : ",") $5 } END { print pids }' "$KRX_PROC_LOCKS"
}

pids="$(holders || true)"
waited=0
polls=0
while [[ -n "$pids" ]]; do
  if (( waited >= KRX_REPAIR_WAIT_MAX_S )); then
    echo "[SYS] stage=repair_lock_wait status=timeout waited_s=$waited max_s=$KRX_REPAIR_WAIT_MAX_S holder_pids=$pids"
    echo "::warning title=vps-deploy/krx-alpha/repair_lock_wait::reason=repair_lock_wait_timeout waited_s=$waited"
    exit 0
  fi
  if (( polls % 5 == 0 )); then
    echo "[SYS] stage=repair_lock_wait status=waiting waited_s=$waited max_s=$KRX_REPAIR_WAIT_MAX_S holder_pids=$pids"
  fi
  sleep "$KRX_REPAIR_WAIT_POLL_S"
  waited=$(( waited + KRX_REPAIR_WAIT_POLL_S ))
  polls=$(( polls + 1 ))
  pids="$(holders || true)"
done
idle released "$waited"
