# Shared helpers for scripts/admin-e2e.sh: the cluster (rendered compose file, per-node
# config and data, PKI), the admin CLI/API wrappers, assertions, bounded waits, and the
# Docker orchestration the scenarios drive. Sourced, never executed.
#
# Settings (environment):
#   ADMIN_E2E_PROJECT    compose project name (default mqttd-admin-e2e)
#   ADMIN_E2E_DIR        state directory: PKI, configs, data, logs (default target/admin-e2e)
#   ADMIN_E2E_PORT_BASE  host port base; empty = the classic N1883 / N9443 / N8080 layout,
#                        otherwise node N gets BASE+10N (MQTT), +1 (admin), +2 (health)
#   ADMIN_E2E_NODES      how many nodes the compose file defines (default 3)
#   ADMIN_E2E_DATA       1 = each node keeps its state in a data dir (durable across
#                        restarts); 0 = ephemeral durability (default 0)
#   IMAGE, BIN           the broker image and the host mqttd binary

KIT="${KIT:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
ROOT="${ROOT:-$(cd "$KIT/../.." && pwd)}"
PROJECT="${ADMIN_E2E_PROJECT:-mqttd-admin-e2e}"
STATE="${ADMIN_E2E_DIR:-$ROOT/target/admin-e2e}"
PORT_BASE="${ADMIN_E2E_PORT_BASE:-}"
NODES="${ADMIN_E2E_NODES:-3}"
DATA="${ADMIN_E2E_DATA:-0}"
IMAGE="${IMAGE:-mqttd-admin-e2e:latest}"
BIN="${BIN:-$ROOT/target/release/mqttd}"
P="$STATE/pki"
export IMAGE

# --- ports and names ------------------------------------------------------------------

mqtt_port()   { if [ -z "$PORT_BASE" ]; then echo "${1}1883"; else echo $((PORT_BASE + 10 * $1)); fi; }
admin_port()  { if [ -z "$PORT_BASE" ]; then echo "${1}9443"; else echo $((PORT_BASE + 10 * $1 + 1)); fi; }
health_port() { if [ -z "$PORT_BASE" ]; then echo "${1}8080"; else echo $((PORT_BASE + 10 * $1 + 2)); fi; }
container()   { echo "$PROJECT-mqttd-$1-1"; }
network()     { echo "${PROJECT}_default"; }

compose() { docker compose -p "$PROJECT" -f "$STATE/compose.yml" "$@"; }

# --- state and cluster ------------------------------------------------------------------

render_compose() {
  {
    echo "# Rendered by scripts/admin-e2e/lib.sh — $NODES nodes, project $PROJECT."
    echo "name: $PROJECT"
    echo "services:"
    local n seeds
    for n in $(seq 1 "$NODES"); do
      seeds=""
      [ "$n" -gt 1 ] && seeds="      MQTTD_SWIM_SEEDS: mqttd-1:7946"
      cat <<EOF
  mqttd-$n:
    image: \${IMAGE:-mqttd-admin-e2e:latest}
    command: ["--config", "/cfg/mqttd.toml"]
    hostname: mqttd-$n
    environment:
      MQTTD_NODE_ID: mqttd-$n
      MQTTD_PEER_BIND: mqttd-$n:7001
      MQTTD_SWIM_BIND: mqttd-$n:7946
$seeds
      MQTTD_PEER_TLS_CERT: /e2e/pki/mqttd-$n.pem
      MQTTD_PEER_TLS_KEY: /e2e/pki/mqttd-$n.key
      MQTTD_ADMIN_CERT: /e2e/pki/mqttd-$n.pem
      MQTTD_ADMIN_KEY: /e2e/pki/mqttd-$n.key
EOF
      [ "$DATA" = 1 ] && echo "      MQTTD_DATA_DIR: /data"
      cat <<EOF
    volumes:
      - ./pki:/e2e/pki:ro
      - ./acl.toml:/e2e/acl.toml:ro
      - ./passwd:/e2e/passwd:ro
      - ./cfg$n:/cfg
EOF
      [ "$DATA" = 1 ] && echo "      - ./data$n:/data"
      echo "    ports: [\"$(mqtt_port "$n"):1883\", \"$(admin_port "$n"):9443\", \"$(health_port "$n"):8080\"]"
    done
  } | grep -v '^$' > "$STATE/compose.yml"
}

# Fresh state: PKI for $NODES nodes, the ACL, an (empty) password file, per-node config
# and data dirs, the compose file. Removes any previous cluster of THIS project first.
prepare_state() {
  if [ -f "$STATE/compose.yml" ]; then compose down -v >/dev/null 2>&1 || true; fi
  rm -rf "$STATE"
  mkdir -p "$STATE"
  cp "$KIT/acl.toml" "$STATE/acl.toml"
  : > "$STATE/passwd"
  bash "$KIT/pki.sh" "$STATE/pki" "$NODES"
  local n
  for n in $(seq 1 "$NODES"); do
    mkdir -p "$STATE/cfg$n"
    cp "$KIT/mqttd.toml" "$STATE/cfg$n/mqttd.toml"
    if [ "$DATA" = 1 ]; then mkdir -p "$STATE/data$n"; fi
  done
  render_compose
  chmod -R a+rwX "$STATE"
}

# Start the listed nodes (default: all) and wait until each reports ready.
cluster_up() {
  local nodes=("$@")
  [ ${#nodes[@]} = 0 ] && nodes=($(seq 1 "$NODES"))
  local services=()
  local n
  for n in "${nodes[@]}"; do services+=("mqttd-$n"); done
  compose up -d "${services[@]}" >/dev/null 2>&1
  wait_ready "${nodes[@]}"
}

cluster_down() {
  if [ -f "$STATE/compose.yml" ]; then compose down -v >/dev/null 2>&1 || true; fi
}

# Wait (at most 120 s) until every listed node's /readyz says ready.
wait_ready() {
  local n
  for n in "$@"; do
    wait_until "mqttd-$n ready" 120 is_ready "$n" || return 1
  done
}

is_ready() { curl -s --max-time 2 "localhost:$(health_port "$1")/readyz" | grep -q '"ready":true'; }

# Save every node's log to $STATE/logs/ (for a failed scenario).
collect_logs() {
  mkdir -p "$STATE/logs"
  local n
  for n in $(seq 1 "$NODES"); do
    docker logs "$(container "$n")" > "$STATE/logs/mqttd-$n.log" 2>&1 || true
  done
}

# --- node orchestration ---------------------------------------------------------------

node_kill()      { docker kill "$(container "$1")" >/dev/null; }
node_start()     { docker start "$(container "$1")" >/dev/null; }
node_restart()   { docker restart "$(container "$1")" >/dev/null; }
net_disconnect() { docker network disconnect "$(network)" "$(container "$1")"; }
net_connect()    { docker network connect --alias "mqttd-$1" "$(network)" "$(container "$1")"; }
node_running()   { [ "$(docker inspect -f '{{.State.Running}}' "$(container "$1")" 2>/dev/null)" = true ]; }

# Edit node N's config file (append TOML) or the shared ACL / password file, then give a
# Docker Desktop bind mount a moment to show the host's write inside the container.
cfg_append()  { printf '%s\n' "$2" >> "$STATE/cfg$1/mqttd.toml"; sync_pause; }
sync_pause()  { sleep "${ADMIN_E2E_SYNC_PAUSE:-2}"; }

# --- admin access ---------------------------------------------------------------------

# cli <who> <node> <verb...>: the host CLI as <who> (root / oncall / stranger / mqttd-N).
cli() {
  local who=$1 n=$2; shift 2
  "$BIN" --admin "$@" --url "https://127.0.0.1:$(admin_port "$n")" --ca "$P/cluster-ca.pem" \
    --cert "$P/$who.pem" --key "$P/$who.key" --server-name "mqttd-$n" 2>&1
  echo "[exit=$?]"
}

# api <who> <node> <METHOD> <path>: raw HTTPS; the body, then [http=NNN].
api() {
  local who=$1 n=$2 m=$3 path=$4 port
  port=$(admin_port "$n")
  curl -sS -X "$m" --max-time 30 --cacert "$P/cluster-ca.pem" --cert "$P/$who.pem" \
    --key "$P/$who.key" --resolve "mqttd-$n:$port:127.0.0.1" -w '\n[http=%{http_code}]' \
    "https://mqttd-$n:$port$path" 2>&1
}

# api_json <who> <node> <METHOD> <path>: the body only.
api_json() { api "$@" | sed '$d'; }

# The admin CLI from inside node N's container, against its own loopback — works even
# when the node is cut off the network.
cli_inside() {
  local n=$1; shift
  docker exec "$(container "$n")" mqttd --admin "$@" --url https://127.0.0.1:9443 \
    --server-name "mqttd-$n" --ca /e2e/pki/cluster-ca.pem --cert /e2e/pki/root.pem \
    --key /e2e/pki/root.key 2>&1
}

reload_node() { api root "$1" POST /admin/v1/reload; }

# jqv <python expr over d>: evaluate against JSON on stdin; empty on bad JSON.
jqv() { python3 -c "import json,sys
try: d=json.load(sys.stdin)
except Exception: sys.exit(0)
try: print(eval(sys.argv[1]))
except Exception: pass" "$1"; }

# --- assertions -------------------------------------------------------------------------

PASS=${PASS:-0}; FAIL=${FAIL:-0}; FAILED=()
ok()  { PASS=$((PASS + 1)); echo "PASS  $1"; }
bad() { FAIL=$((FAIL + 1)); FAILED+=("$1"); echo "FAIL  $1"; [ -n "${2:-}" ] && echo "      got: $(echo "$2" | head -c 600)"; return 0; }
# expect <name> <substring> <output>
expect()     { if grep -qF -- "$2" <<<"$3"; then ok "$1"; else bad "$1" "$3"; fi; }
expect_not() { if grep -qF -- "$2" <<<"$3"; then bad "$1" "$3"; else ok "$1"; fi; }
# check <name> <command...>: PASS when the command succeeds.
check() { local name=$1; shift; if "$@" >/dev/null 2>&1; then ok "$name"; else bad "$name" "$("$@" 2>&1)"; fi; }

# wait_until <description> <timeout seconds> <command...>: poll every 0.5 s until the
# command succeeds; on timeout, say what never happened and return 1.
wait_until() {
  local desc=$1 timeout=$2; shift 2
  local deadline=$((SECONDS + timeout))
  while ! "$@" >/dev/null 2>&1; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "      timed out after ${timeout}s waiting for: $desc"
      return 1
    fi
    sleep 0.5
  done
}

# eventually <name> <timeout> <command...>: a check that may take a while to become true.
eventually() {
  local name=$1 t=$2; shift 2
  LAST_VIEW=""
  if wait_until "$name" "$t" "$@"; then ok "$name"; else bad "$name" "${LAST_VIEW:+last (replied, same_membership, same_cluster_id): $LAST_VIEW}"; fi
}

# --- cluster-view predicates (for wait_until / eventually) ------------------------------

# view_field <node> <python expr>: evaluate against node N's /admin/v1/cluster.
view_field() { api_json oncall "$1" GET /admin/v1/cluster | jqv "$2"; }
# view_is <node> <python expr> <expected>
view_is() { [ "$(view_field "$1" "$2")" = "$3" ]; }
# row <node> <member> <field>: one field of one member's row in node N's cluster view.
row_field() { view_field "$1" "[r for r in d['nodes'] if r['node_id']=='mqttd-$2'][0]['$3']"; }
row_is() { [ "$(row_field "$1" "$2" "$3")" = "$4" ]; }

# --- MQTT clients -----------------------------------------------------------------------

# raw_start <name> <node> <client-id> [raw_v5.py options]: a never-reconnecting MQTT 5
# client in the background, output in $STATE/<name>.out; waits for a successful CONNACK.
raw_start() {
  local name=$1 n=$2; shift 2
  raw_try "$name" "$n" "$@"
  wait_until "$name connected" 15 raw_has "$name" "CONNACK reason=0x00"
}
# raw_try: the same, without requiring success (waits for any CONNACK or a close).
raw_try() {
  local name=$1 n=$2; shift 2
  : > "$STATE/$name.out"
  python3 -u "$KIT/raw_v5.py" "$(mqtt_port "$n")" "$@" > "$STATE/$name.out" 2>&1 &
  echo $! > "$STATE/$name.pid"
  wait_until "$name answered" 15 raw_has_any "$name" "CONNACK" "closed"
}
raw_has()     { grep -qF -- "$2" "$STATE/$1.out"; }
raw_has_any() { grep -qE -- "$2|$3" "$STATE/$1.out"; }
raw_out()     { cat "$STATE/$1.out"; }
raw_closed()  { raw_has_any "$1" "server closed" "DISCONNECT"; }
raw_stop()    { kill "$(cat "$STATE/$1.pid")" 2>/dev/null || true; }

# pub <node> <mosquitto_pub args...>
pub() { local n=$1; shift; mosquitto_pub -h 127.0.0.1 -p "$(mqtt_port "$n")" "$@"; }

# session_node <client>: the node whose admin API holds this client's session (or none).
session_node() {
  local n
  for n in $(seq 1 "$NODES"); do
    node_running "$n" || continue
    if api oncall "$n" GET "/admin/v1/session?client=$1" | grep -q '\[http=200\]'; then
      echo "$n"; return 0
    fi
  done
  return 1
}

# Edit a file IN PLACE (same inode, so a container's bind mount of it sees the change).
# file_replace <path> <old> <new>: the first occurrence. file_write <path> <content>.
file_replace() { python3 "$KIT/edit.py" replace "$1" "$2" "$3"; }
file_write()   { python3 "$KIT/edit.py" write "$1" "$2"; }

# reload_all: reload every running node; succeeds when all answer 200.
reload_all() {
  local n out ok_all=1
  for n in $(seq 1 "$NODES"); do
    node_running "$n" || continue
    out=$(reload_node "$n")
    grep -q '\[http=200\]' <<<"$out" || { ok_all=0; echo "reload of mqttd-$n: $out"; }
  done
  [ "$ok_all" = 1 ]
}

# formed <node> [members]: node N sees every expected member reply and agree.
formed() {
  LAST_VIEW=$(view_field "$1" '(d["summary"]["replied"], d["summary"]["same_membership"], d["summary"]["same_cluster_id"])')
  [ "$LAST_VIEW" = "(${2:-3}, True, True)" ]
}
