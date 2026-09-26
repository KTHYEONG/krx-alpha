#!/usr/bin/env bash
set -uo pipefail

KRX_ROOT="${KRX_ROOT:-$HOME/krx-alpha}"
RCLONE_BIN="${RCLONE_BIN:-$(command -v rclone 2>/dev/null || echo "$HOME/.local/bin/rclone")}"
REMOTE_ROOT="${REMOTE_ROOT:-gdrive:quant-lake/live/krx-alpha}"
QUANT_GDRIVE_LOCK="${QUANT_GDRIVE_LOCK:-/run/user/$(id -u)/quant-gdrive.lock}"
LOCK_WAIT_SEC="${LOCK_WAIT_SEC:-7200}"
VERSION_RETENTION_DAYS="${VERSION_RETENTION_DAYS:-30}"
BACKUP_TODAY_UTC="${BACKUP_TODAY_UTC:-$(date -u +%F)}"
LOG_DIR="${LOG_DIR:-$HOME/logs}"
LOG_FILE="$LOG_DIR/krx-host-backup-$BACKUP_TODAY_UTC.log"
KRX_HOST_STATE_DIR="${KRX_HOST_STATE_DIR:-$HOME/.local/state/krx-alpha}"
STATUS_FILE="$KRX_HOST_STATE_DIR/host_backup_status.json"
HOLDERS_TMP="$KRX_HOST_STATE_DIR/.host_backup_holders.tmp"
ATTEMPT_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%S+00:00)"

mkdir -p "$LOG_DIR"
mkdir -p "$(dirname "$QUANT_GDRIVE_LOCK")"

log() {
  printf '%s\n' "$*" | tee -a "$LOG_FILE"
}

HC_URL="${KRX_HOST_BACKUP_HEALTHCHECK_URL:-}"
ATTEMPT_RID="${ATTEMPT_RID:-$(cat /proc/sys/kernel/random/uuid 2>/dev/null || echo "no-uuid")}"
HC_DISABLED_LOGGED=0

hc_ping() {
  local suffix="$1"
  local body="${2:-}"
  if [ -z "$HC_URL" ]; then
    if [ "$HC_DISABLED_LOGGED" -eq 0 ]; then
      HC_DISABLED_LOGGED=1
      log "[SYS] stage=gdrive_backup project=krx-alpha step=healthcheck status=disabled"
    fi
    return 0
  fi
  local ping_url="$HC_URL/$suffix?rid=$ATTEMPT_RID"
  if [ -n "$body" ]; then
    if ! curl -fsS -m 10 --retry 2 -o /dev/null --data-raw "$body" "$ping_url" >/dev/null 2>&1; then
      log "[SYS] stage=gdrive_backup project=krx-alpha step=healthcheck status=failed"
    fi
  else
    if ! curl -fsS -m 10 --retry 2 -o /dev/null "$ping_url" >/dev/null 2>&1; then
      log "[SYS] stage=gdrive_backup project=krx-alpha step=healthcheck status=failed"
    fi
  fi
  return 0
}

collect_lock_holders() {
  LOCK_HOLDERS=()
  local lock_target="$1" fd rest pid cmdline cmd
  for fd in /proc/[0-9]*/fd/*; do
    cmdline=""
    if ! cmdline="$(readlink "$fd" 2>/dev/null)"; then
      continue
    fi
    [ "$cmdline" = "$lock_target" ] || continue
    rest="${fd#/proc/}"
    pid="${rest%%/*}"
    [ "$pid" = "$$" ] && continue
    if cmd="$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null)"; then
      cmd="${cmd% }"
      LOCK_HOLDERS+=("$pid ${cmd:0:200}")
    fi
  done
}

write_status() {
  local rc="$1" data_rc="$2" prune_rc="$3" lock_wait_s="$4"
  RC="$rc" DATA_RC="$data_rc" PRUNE_RC="$prune_rc" LOCK_WAIT_S="$lock_wait_s" \
    STARTED_AT="$ATTEMPT_STARTED_AT" FINISHED_AT="$(date -u +%Y-%m-%dT%H:%M:%S+00:00)" \
    STATUS_FILE="$STATUS_FILE" HOLDERS_FILE="$HOLDERS_TMP" \
    python3 - <<'PYEOF'
import json
import os

status_file = os.environ["STATUS_FILE"]
rc = int(os.environ["RC"])
data_rc = None if os.environ["DATA_RC"] == "null" else int(os.environ["DATA_RC"])
prune_rc = None if os.environ["PRUNE_RC"] == "null" else int(os.environ["PRUNE_RC"])
lock_wait_s = int(os.environ["LOCK_WAIT_S"])
started_at = os.environ["STARTED_AT"]
finished_at = os.environ["FINISHED_AT"]
holders_file = os.environ.get("HOLDERS_FILE") or ""

holders = []
if holders_file and os.path.exists(holders_file):
    with open(holders_file, encoding="utf-8") as handle:
        holders = [line.rstrip("\n") for line in handle if line.strip() != ""]

prev_last_ok = None
try:
    with open(status_file, encoding="utf-8") as handle:
        prev = json.load(handle)
    cand = prev.get("last_ok_at") if isinstance(prev, dict) else None
    if cand is None or isinstance(cand, str):
        prev_last_ok = cand
except (OSError, ValueError):
    prev_last_ok = None

doc = {
    "schema_version": 1,
    "attempt_started_at": started_at,
    "attempt_finished_at": finished_at,
    "rc": rc,
    "data_rc": data_rc,
    "prune_rc": prune_rc,
    "lock_wait_s": lock_wait_s,
    "lock_holders": holders,
    "last_ok_at": finished_at if rc == 0 else prev_last_ok,
}
tmp_path = status_file + ".tmp"
with open(tmp_path, "w", encoding="utf-8") as handle:
    json.dump(doc, handle)
    handle.write("\n")
os.replace(tmp_path, status_file)
PYEOF
}

if ! mkdir -p "$KRX_HOST_STATE_DIR"; then
  log "[SYS] stage=gdrive_backup project=krx-alpha step=status status=failed"
  hc_ping "74"
  exit 74
fi

exec 9>"$QUANT_GDRIVE_LOCK"
SECONDS=0
if ! flock -w "$LOCK_WAIT_SEC" 9; then
  hc_ping "start"
  lock_wait_s="$SECONDS"
  lock_target="$(readlink -f "$QUANT_GDRIVE_LOCK" 2>/dev/null || printf '%s' "$QUANT_GDRIVE_LOCK")"
  collect_lock_holders "$lock_target"
  log "[SYS] stage=gdrive_backup project=krx-alpha step=lock status=failed rc=75 holders=${#LOCK_HOLDERS[@]}"
  for holder in "${LOCK_HOLDERS[@]}"; do
    log "[SYS] stage=gdrive_backup project=krx-alpha step=lock holder=$holder"
  done
  rm -f "$HOLDERS_TMP"
  for holder in "${LOCK_HOLDERS[@]}"; do
    printf '%s\n' "$holder" >> "$HOLDERS_TMP"
  done
  if ! write_status 75 null null "$lock_wait_s"; then
    log "[SYS] stage=gdrive_backup project=krx-alpha step=status status=failed"
  fi
  rm -f "$HOLDERS_TMP"
  hc_ping "75"
  exit 75
fi
lock_wait_s="$SECONDS"
hc_ping "start"

overall_rc=0

data_rc=0
"$RCLONE_BIN" copy "$KRX_ROOT/data" "$REMOTE_ROOT/data" \
  --filter-from "$KRX_ROOT/deploy/host/krx-alpha.rclone-filter" \
  --backup-dir "$REMOTE_ROOT/_versions/$BACKUP_TODAY_UTC/data" \
  --exclude ".env*" --exclude "*.key" --exclude "*_key.txt" \
  --fast-list --transfers 4 -v || data_rc=$?
if [ "$data_rc" -eq 0 ]; then
  log "[SYS] stage=gdrive_backup project=krx-alpha step=data status=ok rc=0"
else
  log "[SYS] stage=gdrive_backup project=krx-alpha step=data status=failed rc=$data_rc"
  overall_rc=1
fi

prune_rc=0
lsf_out=""
lsf_rc=0
lsf_out=$("$RCLONE_BIN" lsf --dirs-only "$REMOTE_ROOT/_versions" 2>/dev/null) || lsf_rc=$?
if [ "$lsf_rc" -ne 0 ]; then
  if [ "$lsf_rc" -eq 3 ]; then
    prune_rc=0
  else
    prune_rc="$lsf_rc"
  fi
else
  cutoff=$(date -u -d "$BACKUP_TODAY_UTC - $VERSION_RETENTION_DAYS days" +%F)
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    name=${line%/}
    case "$name" in
      ????-??-??)
        if [[ "$name" < "$cutoff" ]]; then
          if ! "$RCLONE_BIN" purge "$REMOTE_ROOT/_versions/$name" >/dev/null 2>&1; then
            prune_rc=1
          fi
        fi
        ;;
    esac
  done <<< "$lsf_out"
fi
if [ "$prune_rc" -eq 0 ]; then
  log "[SYS] stage=gdrive_backup project=krx-alpha step=prune status=ok rc=0"
else
  log "[SYS] stage=gdrive_backup project=krx-alpha step=prune status=failed rc=$prune_rc"
  overall_rc=1
fi

rm -f "$HOLDERS_TMP"
if ! write_status "$overall_rc" "$data_rc" "$prune_rc" "$lock_wait_s"; then
  log "[SYS] stage=gdrive_backup project=krx-alpha step=status status=failed"
  if [ "$overall_rc" -eq 0 ]; then
    overall_rc=74
  fi
fi
rm -f "$HOLDERS_TMP"

hc_ping "$overall_rc"
exit "$overall_rc"
