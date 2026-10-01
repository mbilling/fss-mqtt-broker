#!/usr/bin/env bash
# A 3-node mqttd cluster with the admin API (ADR 0081), in Docker, to try the admin
# commands by hand, check every one end to end, and run orchestrated failure scenarios.
#
#   scripts/admin-e2e.sh up      build the image and host binary, mint a PKI, start the
#                                cluster, and wait until all three nodes are ready
#   scripts/admin-e2e.sh env     print the exports that point `mqttd --admin` at it
#   scripts/admin-e2e.sh test    run every admin command against it (CLI and curl, real
#                                MQTT clients); exit 1 on any failure
#   scripts/admin-e2e.sh down    stop and remove the cluster and its state
#   scripts/admin-e2e.sh all     up, test, down
#   scripts/admin-e2e.sh scenarios [name...]
#                                the scenario suite: each scenario on a FRESH cluster of
#                                its own (kill, partition, scale, decommission, durability,
#                                policy, drills), asserted through the admin API
#   scripts/admin-e2e.sh list    the scenario names
#
# The cluster: nodes mqttd-1..3 with cluster mTLS (each node's cluster certificate also
# serves its admin listener), SWIM, an ACL (everything allowed except publishing under
# secret/) and a config file per node. Published on the host as N1883 (MQTT, anonymous,
# plaintext — a test rig), N9443 (admin API) and N8080 (health), for N = 1, 2, 3.
# Admin identities: oncall (viewer), root (operator), stranger (in no role list).
#
# State (PKI, per-node config, logs) lives in $ADMIN_E2E_DIR (default target/admin-e2e),
# which git ignores. Needs docker (with compose), openssl, curl, python3 and
# mosquitto_pub/mosquitto_sub; `test` and `scenarios` also need cargo for the host binary.
#
# `scenarios` never touches the `up` cluster: it uses its own compose project
# (mqttd-admin-scenarios), host ports (42010 and up) and state (target/admin-scenarios),
# all overridable with the ADMIN_E2E_* variables described in scripts/admin-e2e/lib.sh.
#
# Most of a run is quiet: the first image build takes minutes, and waits print only on a
# timeout. VERBOSE=1 shows the image and binary builds and docker compose output, and
# reports progress every 10 s while waiting. `bash -x scripts/admin-e2e.sh ...` traces
# every command.
set -euo pipefail

KIT="$(cd "$(dirname "$0")/admin-e2e" && pwd)"

build() {
  local quiet_hint=""
  [ "${VERBOSE:-0}" = 1 ] || quiet_hint=" (VERBOSE=1 shows its output)"
  if [ "${SKIP_BUILD:-0}" != 1 ]; then
    echo "building the image $IMAGE (demo/Dockerfile) — the first build takes several minutes${quiet_hint}"
    if [ "${VERBOSE:-0}" = 1 ]; then
      docker build -f "$ROOT/demo/Dockerfile" -t "$IMAGE" "$ROOT"
    else
      docker build -q -f "$ROOT/demo/Dockerfile" -t "$IMAGE" "$ROOT" >/dev/null
    fi
    echo "building the host binary $BIN${quiet_hint}"
    # Without -q, cargo also says when it is blocked on another build's lock on target/.
    if [ "${VERBOSE:-0}" = 1 ]; then
      (cd "$ROOT" && cargo build --release --bin mqttd)
    else
      (cd "$ROOT" && cargo build -q --release --bin mqttd)
    fi
  fi
}

load_lib() {
  # shellcheck source=admin-e2e/lib.sh
  . "$KIT/lib.sh"
}

up() {
  load_lib
  build
  prepare_state
  echo -n "waiting for three ready nodes"
  local ready=0
  if [ "$VERBOSE" = 1 ]; then cluster_up && ready=1; else cluster_up >/dev/null && ready=1; fi
  if [ "$ready" = 1 ]; then
    echo " — ready"
    env_hint
  else
    echo
    echo "the cluster did not become ready; logs: docker compose -p $PROJECT -f $STATE/compose.yml logs"
    exit 1
  fi
}

env_hint() {
  load_lib
  cat <<EOF

Point the admin CLI at node 1 (use 2/3 in the URL port and server name for the others):

  export MQTTD_ADMIN_URL=https://127.0.0.1:$(admin_port 1)
  export MQTTD_ADMIN_SERVER_NAME=mqttd-1
  export MQTTD_ADMIN_CA=$STATE/pki/cluster-ca.pem
  export MQTTD_ADMIN_CLIENT_CERT=$STATE/pki/root.pem     # operator; oncall.pem = viewer
  export MQTTD_ADMIN_CLIENT_KEY=$STATE/pki/root.key
  alias mqttd=$BIN

  mqttd --admin help
  mqttd --admin cluster
  mosquitto_sub -h 127.0.0.1 -p $(mqtt_port 1) -i me -t 'demo/#' &   # a client to look at
  mqttd --admin clients

Or from inside a node (no host binary needed):

  docker exec $(container 1) mqttd --admin cluster --url https://mqttd-1:9443 \\
    --ca /e2e/pki/cluster-ca.pem --cert /e2e/pki/root.pem --key /e2e/pki/root.key
EOF
}

test_() {
  load_lib
  if [ ! -x "$BIN" ]; then build; fi
  BIN="$BIN" bash "$KIT/checks.sh"
}

down() {
  load_lib
  cluster_down
  rm -rf "$STATE"
  echo "cluster and state removed ($STATE)"
}

scenario_names() {
  local f
  for f in "$KIT"/scenarios/*.sh; do basename "$f" .sh; done
}

# Each scenario runs in its own bash on a fresh cluster; results land in $RESULTS.
scenarios() {
  export ADMIN_E2E_PROJECT="${ADMIN_E2E_PROJECT:-mqttd-admin-scenarios}"
  export ADMIN_E2E_PORT_BASE="${ADMIN_E2E_PORT_BASE:-42000}"
  export ADMIN_E2E_DIR="${ADMIN_E2E_DIR:-$(cd "$KIT/../.." && pwd)/target/admin-scenarios}"
  load_lib
  if [ "$PROJECT" = mqttd-admin-e2e ]; then
    echo "refusing to run scenarios on the mqttd-admin-e2e project (the \`up\` cluster)"; exit 2
  fi
  if [ ! -x "$BIN" ] || [ "${SKIP_BUILD:-0}" != 1 ]; then build; fi
  local results="$STATE-results"
  rm -rf "$results"; mkdir -p "$results"
  local wanted=("$@") name file status total_pass=0 total_fail=0 failed_scenarios=()
  [ ${#wanted[@]} = 0 ] && wanted=($(scenario_names))  # names have no spaces; bash 3.2 has no mapfile
  for name in "${wanted[@]}"; do
    file=$(ls "$KIT"/scenarios/*"$name"*.sh 2>/dev/null | head -1)
    if [ -z "$file" ]; then echo "no scenario matches '$name'"; exit 2; fi
    name=$(basename "$file" .sh)
    echo
    echo "=== scenario $name"
    status=0
    ADMIN_E2E_DIR="$STATE/$name" RESULT="$results/$name" BIN="$BIN" \
      bash "$KIT/run-scenario.sh" "$file" || status=$?
    read -r p f < "$results/$name" || { p=0; f=1; }
    total_pass=$((total_pass + p)); total_fail=$((total_fail + f))
    if [ "$status" != 0 ] || [ "$f" != 0 ]; then failed_scenarios+=("$name"); fi
  done
  echo
  echo "SCENARIOS: ${#wanted[@]} run, $(( ${#wanted[@]} - ${#failed_scenarios[@]} )) passed; checks: $total_pass passed, $total_fail failed"
  local s
  for s in "${failed_scenarios[@]:-}"; do [ -n "$s" ] && echo "  failed: $s (logs: $STATE/$s/logs/)"; done
  [ ${#failed_scenarios[@]} = 0 ]
}

case "${1:-}" in
  up) up ;;
  env) env_hint ;;
  test) test_ ;;
  down) down ;;
  all)
    up
    status=0
    test_ || status=$?
    down
    exit "$status"
    ;;
  scenarios) shift; scenarios "$@" ;;
  list) scenario_names ;;
  *)
    sed -n '2,31p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
