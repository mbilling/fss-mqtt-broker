#!/usr/bin/env bash
# Prove the one-provisioning size A/B (482-per-node-knee.sh) end to end on a
# small, shared-vCPU shape before the full campaign pays for 25 servers:
# provision 3, re-form the same hosts as 1, back to 3, then destroy — every
# resize, bootstrap-on-a-used-host and trapped teardown the real run relies on.
# This WILL provision unless PREFLIGHT_ONLY=1.
set -euo pipefail
if [ "${CLOUD:-hcloud}" != hcloud ]; then
	echo '482-knee-smoke.sh is a Hetzner recipe, not an UpCloud plan' >&2
	exit 2
fi
# The knee env first, for the per-site shape; then the smoke overrides the fleet
# and the ladder. The binary pin (MQTTD_VERSION / MQTTD_URL / MQTTD_SHA256 /
# BENCH_GIT_REF) is inherited from the caller, so the smoke proves THAT binary.
set -a
# shellcheck disable=SC1091
. "$(dirname "$0")/482-per-node-knee.env"
set +a
export DRIVER_COUNT=1 BROKER_TYPE=cpx32 DRIVER_TYPE=cpx42
export LANE_E_SITE_RATE=1000 LANE_E_PUBS_PER_SITE=100 LANE_E_PUB_CONTAINERS_PER_SITE=1
export KNEE_ARMS="3:1:1; 1:1:1; 3:1:1"
export OBSERVE=0
unset RUN_DIR KEEP_INFRA
exec bash "$(dirname "$0")/482-per-node-knee.sh"
