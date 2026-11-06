#!/usr/bin/env bash
# Forced command for the CI "ops" SSH key (authorized_keys `command=`).
# Scheduled CI jobs that run without a deploy approval (kca watchdog, kse KIND
# relay) hold this key instead of the deploy key, so a leaked ops key can run
# only the operations allowlisted below, never a shell. Canonical in krx-alpha;
# installed to ~/bin/ci-ops-gate by the krx deploy.
set -Eeuo pipefail

readonly KIND_INBOX="$HOME/k-stock-engine/data/state/kr_swing_2019_v1/kind_relay/inbox"
readonly KIND_MAX_BYTES=$((64 * 1024 * 1024))

case "${SSH_ORIGINAL_COMMAND:-}" in
  kca-watchdog-probe)
    cd "$HOME/k-closing-alpha"
    exec "$HOME/.local/bin/uv" run --no-sync python -m src.tools.watchdog_probe
    ;;
  kse-kind-relay-put)
    mkdir -p "$KIND_INBOX"
    incoming="$KIND_INBOX/bundle.json.incoming"
    head -c "$((KIND_MAX_BYTES + 1))" > "$incoming"
    size="$(stat -c %s "$incoming")"
    if [ "$size" -eq 0 ] || [ "$size" -gt "$KIND_MAX_BYTES" ]; then
      rm -f "$incoming"
      echo "ci-ops-gate: rejected bundle size=$size" >&2
      exit 65
    fi
    mv -f "$incoming" "$KIND_INBOX/bundle.json"
    echo "ci-ops-gate: bundle delivered size=$size"
    ;;
  *)
    echo "ci-ops-gate: command not allowed" >&2
    exit 126
    ;;
esac
