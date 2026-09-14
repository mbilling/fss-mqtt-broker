#!/usr/bin/env bash
# Prove Option B's binary/key apply->destroy independently of its full shape.
# This WILL provision unless PREFLIGHT_ONLY=1; normal run.sh teardown applies.
set -euo pipefail
if [ "${CLOUD:-hcloud}" != hcloud ]; then
	echo '482-smoke.sh is the Hetzner Option B recipe, not an UpCloud plan' >&2
	exit 2
fi
export CLOUD=hcloud DRIVER_COUNT=1 BROKER_TYPE=cpx32 DRIVER_TYPE=cpx42
export LANES=E LANE_E_SITES_OVERRIDE=1 LANE_E_SITE_RATE=1000
export LANE_E_PUBS_PER_SITE=100 LANE_E_SUBS_PER_SITE=7
export LANE_E_PUB_CONTAINERS_PER_SITE=1 LANE_E_SUB_CONTAINERS_PER_SITE=1
export LANE_E_PIN_SITES=0 LANE_E_QOS=0 LANE_E_SUB_QOS=0 LANE_E_PAYLOAD=200
export LANE_E_CONTROL=1 LANE_E_CALIBRATE=1 OBSERVE=0 KEEP_INFRA=0
# Never resume the full pair's RUN_DIR for this separate teardown proof.
unset RUN_DIR
exec bash "$(dirname "$0")/run.sh" smoke
