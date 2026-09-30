#!/usr/bin/env bash
# A 3-node mqttd cluster with the admin API (ADR 0081), in Docker, to try the admin
# commands by hand and to check every one end to end.
#
#   scripts/admin-e2e.sh up      build the image and host binary, mint a PKI, start the
#                                cluster, and wait until all three nodes are ready
#   scripts/admin-e2e.sh env     print the exports that point `mqttd --admin` at it
#   scripts/admin-e2e.sh test    run every admin command against it (CLI and curl, real
#                                MQTT clients); exit 1 on any failure
#   scripts/admin-e2e.sh down    stop and remove the cluster and its state
#   scripts/admin-e2e.sh all     up, test, down
#
# The cluster: nodes mqttd-1..3 with cluster mTLS (each node's cluster certificate also
# serves its admin listener), SWIM, an ACL (everything allowed except publishing under
# secret/) and a config file per node. Published on the host as N1883 (MQTT, anonymous,
# plaintext — a test rig), N9443 (admin API) and N8080 (health), for N = 1, 2, 3.
# Admin identities: oncall (viewer), root (operator), stranger (in no role list).
#
# State (PKI, per-node config, logs) lives in $ADMIN_E2E_DIR (default target/admin-e2e),
# which git ignores. Needs docker (with compose), openssl, curl, python3 and
# mosquitto_pub/mosquitto_sub; `test` also needs cargo to build the host binary.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
KIT="$ROOT/scripts/admin-e2e"
STATE="${ADMIN_E2E_DIR:-$ROOT/target/admin-e2e}"
IMAGE="${IMAGE:-mqttd-admin-e2e:latest}"
BIN="${BIN:-$ROOT/target/release/mqttd}"
export IMAGE

compose() { docker compose -f "$STATE/compose.yml" "$@"; }

build() {
  if [ "${SKIP_BUILD:-0}" != 1 ]; then
    echo "building the image $IMAGE (demo/Dockerfile) — the first build takes several minutes"
    docker build -q -f "$ROOT/demo/Dockerfile" -t "$IMAGE" "$ROOT" >/dev/null
    echo "building the host binary $BIN"
    (cd "$ROOT" && cargo build -q --release --bin mqttd)
  fi
}

up() {
  build
  mkdir -p "$STATE"
  cp "$KIT/compose.yml" "$KIT/acl.toml" "$STATE/"
  bash "$KIT/pki.sh" "$STATE/pki"
  for n in 1 2 3; do
    mkdir -p "$STATE/cfg$n"
    cp "$KIT/mqttd.toml" "$STATE/cfg$n/mqttd.toml"
  done
  chmod -R a+rX "$STATE"
  compose down -v >/dev/null 2>&1 || true
  compose up -d >/dev/null 2>&1
  echo -n "waiting for three ready nodes"
  for _ in $(seq 1 90); do
    ready=0
    for n in 1 2 3; do
      if curl -s "localhost:${n}8080/readyz" | grep -q '"ready":true'; then ready=$((ready + 1)); fi
    done
    if [ "$ready" = 3 ]; then echo " — ready"; env_hint; return 0; fi
    echo -n "."
    sleep 1
  done
  echo
  echo "the cluster did not become ready in 90 s; logs: docker compose -f $STATE/compose.yml logs"
  exit 1
}

env_hint() {
  cat <<EOF

Point the admin CLI at node 1 (use 2/3 in the URL port and server name for the others):

  export MQTTD_ADMIN_URL=https://127.0.0.1:19443
  export MQTTD_ADMIN_SERVER_NAME=mqttd-1
  export MQTTD_ADMIN_CA=$STATE/pki/cluster-ca.pem
  export MQTTD_ADMIN_CLIENT_CERT=$STATE/pki/root.pem     # operator; oncall.pem = viewer
  export MQTTD_ADMIN_CLIENT_KEY=$STATE/pki/root.key
  alias mqttd=$BIN

  mqttd --admin help
  mqttd --admin cluster
  mosquitto_sub -h 127.0.0.1 -p 11883 -i me -t 'demo/#' &   # a client to look at
  mqttd --admin clients

Or from inside a node (no host binary needed):

  docker exec mqttd-admin-e2e-mqttd-1-1 mqttd --admin cluster --url https://mqttd-1:9443 \\
    --ca /e2e/pki/cluster-ca.pem --cert /e2e/pki/root.pem --key /e2e/pki/root.key
EOF
}

test_() {
  if [ ! -x "$BIN" ]; then build; fi
  BIN="$BIN" bash "$KIT/checks.sh" "$STATE"
}

down() {
  if [ -f "$STATE/compose.yml" ]; then compose down -v >/dev/null 2>&1 || true; fi
  rm -rf "$STATE"
  echo "cluster and state removed ($STATE)"
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
  *)
    sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
