#!/usr/bin/env bash
# usage: krx-validate-runtime-env.sh <env_file> <key_manifest>
# env: EXPECTED_OWNER (default "ubuntu:ubuntu")
# Fails closed on any deviation and prints only fixed text and key names, never
# credential values. The manifest is generated from the same commit by
# `python3 -m src.core.runtime_env_provisioning emit-key-manifest`.
set -euo pipefail

env_file="$1"
manifest="$2"
expected_owner="${EXPECTED_OWNER:-ubuntu:ubuntu}"

if [ ! -s "$env_file" ]; then
  echo "Missing runtime env" >&2
  echo "Hint: run uv run python -m src.cli.provision_kis_keypool --host or-vps from a trusted workstation" >&2
  exit 1
fi

mode="$(stat -c %a "$env_file")"
if [ "$mode" != "600" ]; then
  echo "Runtime env has unexpected mode" >&2
  exit 1
fi

owner="$(stat -c %U:%G "$env_file")"
if [ "$owner" != "$expected_owner" ]; then
  echo "Runtime env has unexpected owner" >&2
  exit 1
fi

if [ ! -s "$manifest" ]; then
  echo "Runtime env key manifest is invalid" >&2
  exit 1
fi
if ! awk '{ if ($0 !~ /^(required|optional) [A-Z][A-Z0-9_]*$/) exit 1 }' "$manifest"; then
  echo "Runtime env key manifest is invalid" >&2
  exit 1
fi

unexpected_key="$(awk '
  NR==FNR { split($0, m, " "); allowed[m[2]]=1; next }
  /^[[:space:]]*(#|$)/ { next }
  { split($0, kv, "="); if (!(kv[1] in allowed)) { print kv[1]; exit } }
' "$manifest" "$env_file")"
if [ -n "$unexpected_key" ]; then
  echo "Runtime env contains an unexpected key: $unexpected_key" >&2
  exit 1
fi

required_count=0
while IFS=' ' read -r kind key || [ -n "$kind" ]; do
  [ "$kind" = "required" ] || continue
  required_count=$((required_count + 1))
  credential_value="$(awk -F= -v key="$key" '$1 == key { if (index($0, "=") > 0) print substr($0, index($0, "=") + 1); exit }' "$env_file")"
  if [ -z "$credential_value" ]; then
    echo "Missing required runtime env key: $key" >&2
    echo "Hint: run uv run python -m src.cli.provision_kis_keypool --host or-vps from a trusted workstation" >&2
    exit 1
  fi
done < "$manifest"

echo "Runtime env validated: $required_count keys"
