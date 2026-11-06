#!/usr/bin/env bash
# One-time workstation setup: generate the escrow age identity and install its public key on the VPS.
set -Eeuo pipefail
umask 077

if [ "$#" -ne 1 ]; then
  echo "usage: secrets_escrow_setup.sh <identity_path>" >&2
  exit 2
fi
IDENTITY="$1"
if [ -e "$IDENTITY" ]; then
  echo "refusing to overwrite existing identity: $IDENTITY" >&2
  exit 1
fi

age-keygen -o "$IDENTITY" 2>/dev/null
chmod 600 "$IDENTITY"
PUBLIC_KEY="$(age-keygen -y "$IDENTITY")"

printf '%s\n' "$PUBLIC_KEY" | ssh or-vps \
  'mkdir -p ~/.config/krx-secrets-escrow && cat > ~/.config/krx-secrets-escrow/recipients.txt && chmod 600 ~/.config/krx-secrets-escrow/recipients.txt'

echo "recipients installed on or-vps: $PUBLIC_KEY"
echo "restore: age -d -i <identity_path> secrets-<UTC>.tar.age | tar -x"
echo "store a second copy of the identity file in a password manager; the private key never leaves this workstation and is never printed."
