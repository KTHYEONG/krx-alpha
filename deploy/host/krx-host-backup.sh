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

mkdir -p "$LOG_DIR"
mkdir -p "$(dirname "$QUANT_GDRIVE_LOCK")"

log() {
  printf '%s\n' "$*" | tee -a "$LOG_FILE"
}

exec 9>"$QUANT_GDRIVE_LOCK"
if ! flock -w "$LOCK_WAIT_SEC" 9; then
  log "[SYS] stage=gdrive_backup project=krx-alpha step=lock status=failed rc=75"
  exit 75
fi

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

exit "$overall_rc"
