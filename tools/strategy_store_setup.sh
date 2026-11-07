#!/usr/bin/env bash
# One-time setup: install a read-only deploy key on or-vps for the private strategy store, then start the first sync.
# Run by the human from the workstation. The private key never leaves or-vps and is never printed.
set -Eeuo pipefail
umask 077

HOST="or-vps"
STORE_REPO="KTHYEONG/quant-strategy-store"
KEY_TITLE="or-vps-ro"

PUBLIC_KEY="$(ssh "$HOST" 'bash -s' <<'REMOTE'
set -Eeuo pipefail
umask 077
KEY="$HOME/.ssh/strategy_store_ro"
CONFIG="$HOME/.ssh/config"
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"
if [ ! -f "$KEY" ]; then
  ssh-keygen -q -t ed25519 -N '' -C 'or-vps-strategy-store-ro' -f "$KEY"
fi
chmod 600 "$KEY"
touch "$CONFIG"
chmod 600 "$CONFIG"
if ! grep -q '^Host github-strategy-store$' "$CONFIG"; then
  cat >> "$CONFIG" <<'CFG'

Host github-strategy-store
  HostName github.com
  User git
  IdentityFile ~/.ssh/strategy_store_ro
  IdentitiesOnly yes
CFG
fi
cat "$KEY.pub"
REMOTE
)"

printf '%s\n' "$PUBLIC_KEY" | gh repo deploy-key add - --repo "$STORE_REPO" --title "$KEY_TITLE"

# GitHub answers a deploy-key probe with exit 1 even on success; the greeting is the evidence.
ssh "$HOST" 'ssh -o StrictHostKeyChecking=accept-new -T git@github-strategy-store 2>&1 || true'

ssh "$HOST" 'systemctl --user start strategy-store-sync.service'
ssh "$HOST" 'cat "$HOME/.local/state/krx-alpha/strategy_store_status.json"'
