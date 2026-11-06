#!/usr/bin/env bash
# Encrypted off-host escrow of the runtime env files. The VPS holds only age public keys.
set -Eeuo pipefail
umask 077
shopt -s nullglob

SRC_DIR="${ESCROW_SRC_DIR:-$HOME/quant-secrets}"
RECIPIENTS="${ESCROW_RECIPIENTS:-$HOME/.config/krx-secrets-escrow/recipients.txt}"
STATE="${ESCROW_STATE:-$HOME/.local/state/krx-alpha/secrets_escrow.json}"
REMOTE="${ESCROW_REMOTE:-gdrive:quant-escrow/or-vps}"
KEEP="${ESCROW_KEEP:-12}"
MAX_AGE_DAYS="${ESCROW_MAX_AGE_DAYS:-30}"
# Same resolution as krx-host-backup.sh: or-vps ships rclone in ~/.local/bin, which is not on the unit PATH.
RCLONE_BIN="${RCLONE_BIN:-$(command -v rclone 2>/dev/null || echo "$HOME/.local/bin/rclone")}"
AGE_BIN="${AGE_BIN:-age}"

STATUS=failed
OBJECT=-
KEPT=-
KEPT_COUNT=0
WORK=""

emit() {
  printf '[SYS] stage=secrets_escrow status=%s object=%s kept=%s\n' "$STATUS" "$OBJECT" "$KEPT"
}

cleanup() {
  local rc=$?
  if [ -n "$WORK" ]; then
    rm -rf -- "$WORK"
  fi
  emit
  exit "$rc"
}
trap cleanup EXIT

fail() {
  local rc="$1"
  shift
  printf 'secrets_escrow: %s\n' "$*" >&2
  exit "$rc"
}

if [[ ! "$KEEP" =~ ^[1-9][0-9]*$ ]]; then
  fail 2 "ESCROW_KEEP must be a positive integer"
fi
if [[ ! "$MAX_AGE_DAYS" =~ ^[0-9]+$ ]]; then
  fail 2 "ESCROW_MAX_AGE_DAYS must be a non-negative integer"
fi

if [ ! -f "$RECIPIENTS" ] || ! grep -q '[^[:space:]]' "$RECIPIENTS"; then
  STATUS=not_configured
  exit 0
fi

if ! command -v "$AGE_BIN" >/dev/null 2>&1; then
  fail 69 "age binary not found on PATH"
fi

SRC_PARENT="$(dirname -- "$SRC_DIR")"
SRC_BASE="$(basename -- "$SRC_DIR")"
ENV_FILES=("$SRC_DIR"/*.env)
if (( ${#ENV_FILES[@]} == 0 )); then
  fail 1 "no env files in source directory"
fi
MEMBERS=()
for f in "${ENV_FILES[@]}"; do
  MEMBERS+=("$SRC_BASE/${f##*/}")
done

archive() {
  tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
    -C "$SRC_PARENT" -cf - "${MEMBERS[@]}"
}

IFS= read -r -d '' DECIDE_PY <<'PY' || true
import datetime as dt
import json
import sys

path, fingerprint, max_days = sys.argv[1], sys.argv[2], int(sys.argv[3])
try:
    with open(path, encoding="utf-8") as fh:
        state = json.load(fh)
    uploaded_at = dt.datetime.fromisoformat(state["uploaded_at"])
    same = state["fingerprint"] == fingerprint
    fresh = dt.datetime.now(dt.timezone.utc) - uploaded_at < dt.timedelta(days=max_days)
except (OSError, ValueError, KeyError, TypeError, AttributeError):
    same = fresh = False
print("unchanged" if same and fresh else "upload")
PY

IFS= read -r -d '' WRITE_PY <<'PY' || true
import json
import os
import sys

path, fingerprint, uploaded_at, obj = sys.argv[1:5]
tmp = f"{path}.tmp"
with open(tmp, "w", encoding="utf-8") as fh:
    json.dump({"fingerprint": fingerprint, "uploaded_at": uploaded_at, "object": obj}, fh)
    fh.write("\n")
os.replace(tmp, path)
PY

prune_objects() {
  local listing name sorted_list i total
  local -a objects=() sorted=()
  listing="$("$RCLONE_BIN" lsf "$REMOTE" --files-only)" || return 1
  while IFS= read -r name; do
    if [[ "$name" == secrets-*.tar.age ]]; then
      objects+=("$name")
    fi
  done <<< "$listing"
  KEPT_COUNT=0
  if (( ${#objects[@]} == 0 )); then
    return 0
  fi
  sorted_list="$(printf '%s\n' "${objects[@]}" | sort)"
  mapfile -t sorted <<< "$sorted_list"
  total=${#sorted[@]}
  KEPT_COUNT=$total
  for (( i = 0; i < total - KEEP; i++ )); do
    "$RCLONE_BIN" deletefile "$REMOTE/${sorted[i]}" || return 1
    KEPT_COUNT=$(( KEPT_COUNT - 1 ))
  done
  return 0
}

FINGERPRINT="$(archive | sha256sum)"
FINGERPRINT="${FINGERPRINT%% *}"

DECISION="$(python3 -I -c "$DECIDE_PY" "$STATE" "$FINGERPRINT" "$MAX_AGE_DAYS")"
if [ "$DECISION" = unchanged ]; then
  STATUS=unchanged
  exit 0
fi

WORK="$(mktemp -d)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OBJECT_NAME="secrets-$STAMP.tar.age"
ARCHIVE_PATH="$WORK/$OBJECT_NAME"
archive | "$AGE_BIN" -R "$RECIPIENTS" > "$ARCHIVE_PATH"

"$RCLONE_BIN" copyto "$ARCHIVE_PATH" "$REMOTE/$OBJECT_NAME"
OBJECT="$OBJECT_NAME"

UPLOADED_AT="$(date -u +%Y-%m-%dT%H:%M:%S+00:00)"
mkdir -p -- "$(dirname -- "$STATE")"
python3 -I -c "$WRITE_PY" "$STATE" "$FINGERPRINT" "$UPLOADED_AT" "$OBJECT"

if ! prune_objects; then
  fail 1 "prune failed after upload"
fi
KEPT="$KEPT_COUNT"
STATUS=uploaded
