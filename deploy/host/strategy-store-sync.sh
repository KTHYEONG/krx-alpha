#!/usr/bin/env bash
# Pull-based sync of the private strategy store. Read-only deploy key; this host never pushes.
set -Eeuo pipefail
umask 077

STORE_HOME="${STORE_HOME:-$HOME/quant-strategy-store}"
STORE_REMOTE="${STORE_REMOTE:-git@github-strategy-store:KTHYEONG/quant-strategy-store.git}"
STORE_BRANCH="${STORE_BRANCH:-main}"
STORE_KEEP="${STORE_KEEP:-10}"
STORE_KEY="${STORE_KEY:-$HOME/.ssh/strategy_store_ro}"
STORE_STATUS="${STORE_STATUS:-$HOME/.local/state/krx-alpha/strategy_store_status.json}"

REPO="$STORE_HOME/repo"
RELEASES="$STORE_HOME/releases"
CURRENT="$STORE_HOME/current"

if [[ ! "$STORE_KEEP" =~ ^[1-9][0-9]*$ ]]; then
  echo "strategy_store_sync: STORE_KEEP must be a positive integer" >&2
  exit 2
fi

write_status() {
  local status="$1" revision="${2:-}" short tmp
  short="${revision:0:12}"
  mkdir -p -- "$(dirname -- "$STORE_STATUS")"
  tmp="$STORE_STATUS.tmp"
  printf '{"status":"%s","revision":"%s","checked_at":"%s"}\n' \
    "$status" "$revision" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$tmp"
  mv -f -- "$tmp" "$STORE_STATUS"
  printf '[SYS] stage=strategy_store_sync status=%s revision=%s\n' "$status" "${short:--}"
}

if [ ! -f "$STORE_KEY" ]; then
  write_status not_configured
  exit 0
fi

mkdir -p -- "$STORE_HOME"

if [ ! -d "$REPO/.git" ]; then
  if ! git clone --quiet --no-checkout -- "$STORE_REMOTE" "$REPO"; then
    write_status fetch_failed
    exit 75
  fi
fi

if ! git -C "$REPO" fetch --quiet --prune origin "+refs/heads/$STORE_BRANCH:refs/remotes/origin/$STORE_BRANCH"; then
  write_status fetch_failed
  exit 75
fi
if ! SHA="$(git -C "$REPO" rev-parse --verify --quiet "refs/remotes/origin/$STORE_BRANCH^{commit}")"; then
  write_status fetch_failed
  exit 75
fi

if [ -L "$CURRENT" ] && [ "$(readlink -- "$CURRENT")" = "releases/$SHA" ] && [ -d "$RELEASES/$SHA" ]; then
  write_status unchanged "$SHA"
  exit 0
fi

TMP_REL="$RELEASES/$SHA.tmp"
FINAL_REL="$RELEASES/$SHA"
rm -rf -- "$TMP_REL"
mkdir -p -- "$TMP_REL"
if ! git -C "$REPO" archive --format=tar "$SHA" | tar -x -C "$TMP_REL" \
  || ! printf '%s\n' "$SHA" > "$TMP_REL/STORE_REVISION" \
  || ! python3 -I "$TMP_REL/tools/validate_store.py" "$TMP_REL"; then
  rm -rf -- "$TMP_REL"
  write_status invalid "$SHA"
  exit 65
fi

touch -- "$TMP_REL"
rm -rf -- "$FINAL_REL"
mv -- "$TMP_REL" "$FINAL_REL"
rm -f -- "$CURRENT.new"
ln -s "releases/$SHA" "$CURRENT.new"
mv -T -- "$CURRENT.new" "$CURRENT"

i=0
while IFS= read -r name; do
  i=$((i + 1))
  if (( i <= STORE_KEEP )) || [ "$name" = "$SHA" ]; then
    continue
  fi
  rm -rf -- "$RELEASES/$name"
done < <(find "$RELEASES" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %f\n' | sort -rn | cut -d' ' -f2-)

write_status updated "$SHA"
