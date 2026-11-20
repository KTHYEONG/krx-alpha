#!/usr/bin/env bash
# Read-only preview of the docker image retention policy; prints candidate commands, executes none.
set -euo pipefail

readonly KEEP_SHA_TAGS=3
readonly BACKUP_REPO="kse-backup-memory"
readonly BACKUP_MAX_AGE_DAYS=14
readonly MANAGED_REPOS=(
  "ghcr.io/kthyeong/krx-collector"
  "ghcr.io/kthyeong/k-closing-alpha"
  "ghcr.io/kthyeong/k-stock-engine"
)
readonly SHA_TAG_RE='^(sha-)?[0-9a-f]{7,64}$'

is_managed() {
  local repo
  for repo in "${MANAGED_REPOS[@]}"; do
    [[ "$1" == "$repo" ]] && return 0
  done
  return 1
}

in_use() {
  local ref="$1" id="$2" used
  while IFS= read -r used; do
    [[ -z "$used" ]] && continue
    [[ "$used" == "$ref" || "$id" == "$used"* || "$used" == "$id"* ]] && return 0
  done <<<"$used_images"
  return 1
}

age_days() {
  local created_epoch now_epoch
  # GNU date rejects docker's "+0000 UTC" double zone suffix; the numeric offset alone is unambiguous.
  created_epoch=$(date -d "${1% UTC}" +%s 2>/dev/null) || { echo 0; return; }
  now_epoch=$(date +%s)
  echo $(((now_epoch - created_epoch) / 86400))
}

used_images=$(docker ps -a --format '{{.Image}}')
declare -A sha_seen=()
candidates=0

# `docker images` lists newest first, so the first KEEP_SHA_TAGS sha tags per repository are the rollback set.
while IFS=$'\t' read -r repo tag id created size; do
  [[ -z "$repo" ]] && continue
  ref="$repo:$tag"
  if is_managed "$repo"; then
    if in_use "$ref" "$id"; then
      printf 'KEEP(in_use) %s %s\n' "$ref" "$size"
    elif [[ "$tag" == "latest" ]]; then
      printf 'KEEP(latest) %s %s\n' "$ref" "$size"
    elif [[ "$tag" =~ $SHA_TAG_RE ]]; then
      sha_seen[$repo]=$((${sha_seen[$repo]:-0} + 1))
      if ((sha_seen[$repo] <= KEEP_SHA_TAGS)); then
        printf 'KEEP(rollback) %s %s\n' "$ref" "$size"
      else
        printf 'CANDIDATE(old_sha) %s %s\n' "$ref" "$size"
        printf 'docker rmi %s\n' "$ref"
        candidates=$((candidates + 1))
      fi
    else
      printf 'KEEP(unclassified) %s %s\n' "$ref" "$size"
    fi
  elif [[ "$repo" == "$BACKUP_REPO" ]]; then
    age=$(age_days "$created")
    if in_use "$ref" "$id"; then
      printf 'KEEP(in_use) %s %s\n' "$ref" "$size"
    elif ((age > BACKUP_MAX_AGE_DAYS)); then
      printf 'CANDIDATE(stale_backup_%sd) %s %s\n' "$age" "$ref" "$size"
      printf 'docker rmi %s\n' "$ref"
      candidates=$((candidates + 1))
    else
      printf 'KEEP(recent_backup) %s %s\n' "$ref" "$size"
    fi
  else
    printf 'SKIP(other_tenant) %s %s\n' "$ref" "$size"
  fi
done < <(docker images --format '{{.Repository}}\t{{.Tag}}\t{{.ID}}\t{{.CreatedAt}}\t{{.Size}}')

printf 'preview_only candidates=%s (nothing was executed; run the printed commands manually after owner approval)\n' "$candidates"
