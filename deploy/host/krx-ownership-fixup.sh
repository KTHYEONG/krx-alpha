#!/usr/bin/env bash
# Re-owns collector state to the invoking host user before the collector (user: 1001:1001) starts.
set -Eeuo pipefail

ROOT="${KRX_FIXUP_ROOT:-$HOME/krx-alpha}"
TOKEN_CACHE="${KRX_FIXUP_TOKEN_CACHE:-$HOME/.cache/kis}"
IMAGE="${KRX_COLLECTOR_IMAGE:-ghcr.io/kthyeong/krx-collector:latest}"
CONTAINER="${KRX_COLLECTOR_CONTAINER:-krx-collector}"
STOP_GRACE_S=95

TARGET_UID="$(id -u)"
TARGET_GID="$(id -g)"
if [ "$TARGET_UID" -eq 0 ]; then
  echo "krx-ownership-fixup: refusing to run as root" >&2
  exit 2
fi

PATHS=("$ROOT/data" "$ROOT/logs" "$TOKEN_CACHE")

# Succeeds when any existing path holds an entry not owned by the target uid:gid.
has_foreign() {
  local p hit
  for p in "${PATHS[@]}"; do
    [ -e "$p" ] || continue
    hit="$(find "$p" \( ! -user "$TARGET_UID" -o ! -group "$TARGET_GID" \) -print -quit)" || {
      echo "krx-ownership-fixup: find failed under $p" >&2
      exit 1
    }
    [ -n "$hit" ] && return 0
  done
  return 1
}

if ! has_foreign; then
  printf 'krx-ownership-fixup status=clean stopped=0\n'
  exit 0
fi

stopped=0
if out="$(docker stop -t "$STOP_GRACE_S" "$CONTAINER" 2>&1)"; then
  stopped=1
elif ! grep -qi 'no such container' <<<"$out"; then
  echo "krx-ownership-fixup: docker stop failed: $out" >&2
  exit 1
fi

for p in "${PATHS[@]}"; do
  [ -e "$p" ] || continue
  docker run --rm --pull never --network none --user 0:0 --entrypoint /usr/bin/find \
    -v "$p:/fix" "$IMAGE" /fix '(' ! -user "$TARGET_UID" -o ! -group "$TARGET_GID" ')' \
    -exec chown -h "$TARGET_UID:$TARGET_GID" '{}' +
done

if has_foreign; then
  echo "krx-ownership-fixup: foreign-owned entries remain after re-own" >&2
  exit 1
fi
printf 'krx-ownership-fixup status=fixed stopped=%s\n' "$stopped"
