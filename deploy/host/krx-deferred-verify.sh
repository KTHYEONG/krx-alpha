#!/usr/bin/env bash
# Verifies the 22:00 KST deferred recreate, which runs without a CI job to
# watch it: the running collector must carry the revision of the promoted
# :latest image and stay stable, otherwise the unit fails and OnFailure alerts.
set -Eeuo pipefail

LIB="${VPS_DEPLOY_LIB:-$HOME/krx-alpha/deploy/host/vps-deploy-lib.sh}"
IMAGE="${KRX_COLLECTOR_IMAGE:-ghcr.io/kthyeong/krx-collector:latest}"
CONTAINER="${KRX_COLLECTOR_CONTAINER:-krx-collector}"
STABLE_WINDOW_S="${KRX_DEFERRED_STABLE_WINDOW_S:-45}"
REVISION_FORMAT='{{ index .Config.Labels "org.opencontainers.image.revision" }}'

# shellcheck source=deploy/host/vps-deploy-lib.sh
source "$LIB"

want="$(docker image inspect --format "$REVISION_FORMAT" "$IMAGE")"
vps_deploy_init krx-alpha "$want"
vps_stage deferred_verify
got="$(docker inspect --format "$REVISION_FORMAT" "$CONTAINER")"
if [ -z "$want" ] || [ "$got" != "$want" ]; then
  vps_fail running_revision_mismatch
fi
vps_verify_stable "$STABLE_WINDOW_S" "$CONTAINER"
