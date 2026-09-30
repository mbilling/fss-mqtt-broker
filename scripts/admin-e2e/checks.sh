#!/usr/bin/env bash
# The checks behind `scripts/admin-e2e.sh test`: the ADR 0081 admin API on the running
# 3-node cluster — every CLI verb from the host, every endpoint with curl, real MQTT
# clients, and the offline commands inside a container. PASS/FAIL per check, a summary,
# and exit 1 on any failure. It edits node 1's config file (and restores it) and cordons
# node 3 (and uncordons it), so run it on the kit's own cluster, not a shared one.
# Usage: BIN=<host mqttd> checks.sh <state dir>
set -u
E="${1:?usage: checks.sh <state dir>}"
KIT="$(cd "$(dirname "$0")" && pwd)"
BIN="${BIN:?set BIN to the host mqttd binary}"
P="$E/pki"
PASS=0; FAIL=0; FAILED=()

ok()   { PASS=$((PASS+1)); echo "PASS  $1"; }
bad()  { FAIL=$((FAIL+1)); FAILED+=("$1"); echo "FAIL  $1"; echo "      got: $(echo "$2" | head -c 600)"; }
# expect <name> <substring> <output>
expect() { if grep -qF -- "$2" <<<"$3"; then ok "$1"; else bad "$1" "$3"; fi; }
expect_not() { if grep -qF -- "$2" <<<"$3"; then bad "$1" "$3"; else ok "$1"; fi; }

# CLI as <who> against node <n>: cli <who> <n> <verb...>
cli() {
  local who=$1 n=$2; shift 2
  "$BIN" --admin "$@" --url "https://127.0.0.1:${n}9443" --ca "$P/cluster-ca.pem" \
    --cert "$P/$who.pem" --key "$P/$who.key" --server-name "mqttd-$n" 2>&1
  echo "[exit=$?]"
}
# curl as <who> against node <n>: api <who> <n> <METHOD> <path>
api() {
  local who=$1 n=$2 m=$3 path=$4
  curl -sS -X "$m" --cacert "$P/cluster-ca.pem" --cert "$P/$who.pem" --key "$P/$who.key" \
    --resolve "mqttd-$n:${n}9443:127.0.0.1" -w '\n[http=%{http_code}]' \
    "https://mqttd-$n:${n}9443$path" 2>&1
}
jqv() { python3 -c "import json,sys; d=json.load(sys.stdin); print(eval(sys.argv[1]))" "$1"; }

echo "=== day 0: offline commands inside the container"
out=$(docker exec mqttd-admin-e2e-mqttd-1-1 mqttd --check-config --config /cfg/mqttd.toml 2>&1; echo "[exit=$?]")
expect "--check-config" "[exit=0]" "$out"
out=$(docker exec mqttd-admin-e2e-mqttd-1-1 mqttd --print-config --config /cfg/mqttd.toml 2>&1; echo "[exit=$?]")
expect "--print-config shows [admin]" "[admin]" "$out"
expect "--print-config exit 0" "[exit=0]" "$out"
out=$(docker exec mqttd-admin-e2e-mqttd-1-1 mqttd --check-tls --config /cfg/mqttd.toml 2>&1; echo "[exit=$?]")
expect "--check-tls exit 0" "[exit=0]" "$out"
out=$(docker exec mqttd-admin-e2e-mqttd-1-1 mqttd --probe /readyz --config /cfg/mqttd.toml 2>&1; echo "[exit=$?]")
expect "--probe /readyz" "200" "$out"
out=$(docker exec mqttd-admin-e2e-mqttd-1-1 mqttd --admin help 2>&1)
expect "--admin help in container" "log-override" "$out"

echo "=== roles"
out=$(cli oncall 1 whoami);    expect "whoami viewer" "viewer" "$out"
out=$(cli root 1 whoami);      expect "whoami operator" "operator" "$out"
out=$(cli stranger 1 whoami);  expect "unlisted subject refused" "403 forbidden" "$out"
out=$(api stranger 1 GET /admin/v1/whoami); expect "api: unlisted 403" "[http=403]" "$out"
out=$(curl -sS --cacert "$P/cluster-ca.pem" --resolve "mqttd-1:19443:127.0.0.1" https://mqttd-1:19443/admin/v1/node 2>&1; echo "[exit=$?]")
expect_not "no client cert: no answer" "node_id" "$out"

echo "=== day 1: node, cluster, placement (CLI and API on every node)"
for n in 1 2 3; do
  out=$(cli oncall $n node);  expect "node on mqttd-$n" "mqttd-$n" "$out"
  out=$(api oncall $n GET /admin/v1/cluster)
  expect "api cluster on mqttd-$n: 200" "[http=200]" "$out"
  body=$(sed '$d' <<<"$out")
  expect "cluster from mqttd-$n: 3 nodes replied" "3/3/True/True" "$(jqv 'f"{d["summary"]["nodes"]}/{d["summary"]["replied"]}/{d["summary"]["same_cluster_id"]}/{d["summary"]["same_membership"]}"' <<<"$body")"
done
out=$(cli oncall 2 cluster);   expect "cluster CLI table" "NODE_ID" "$out"
out=$(cli oncall 3 placement); expect "placement CLI" "views:" "$out"
out=$(api oncall 1 GET /admin/v1/placement); expect "api placement" ""views"" "$out"

echo "=== MQTT traffic"
mosquitto_sub -h 127.0.0.1 -p 11883 -i keeper -c -q 1 -t 'q/#' -W 2 >/dev/null 2>&1
mosquitto_pub -h 127.0.0.1 -p 11883 -i pub1 -q 1 -t q/1 -m queued-for-keeper
mosquitto_pub -h 127.0.0.1 -p 11883 -i pub2 -q 1 -r -t r/1 -m hello
mosquitto_pub -h 127.0.0.1 -p 11883 -i pub3 -q 1 -r -t r/2 -m world
mosquitto_pub -h 127.0.0.1 -p 11883 -i pub4 -q 1 -r -t other/1 -m x
mosquitto_sub -h 127.0.0.1 -p 21883 -i watcher -V mqttv5 -q 1 -t 'a/+/temp' >/dev/null 2>&1 &
SUB_PID=$!
sleep 2

echo "=== day 2: clients, session, subscribers, backlog, retained"
# Sessions live on their placement owner: ask every node, one of them holds each.
found_keeper=""; found_watcher=""
for n in 1 2 3; do
  out=$(cli oncall $n clients --prefix keeper)
  grep -q "keeper" <<<"$out" && found_keeper=$n
  out=$(cli oncall $n clients --prefix watcher)
  grep -q "watcher" <<<"$out" && found_watcher=$n
done
[ -n "$found_keeper" ] && ok "clients: keeper found on mqttd-$found_keeper" || bad "clients: keeper found" "none"
[ -n "$found_watcher" ] && ok "clients: watcher found on mqttd-$found_watcher" || bad "clients: watcher found" "none"
if [ -n "$found_keeper" ]; then
  out=$(cli oncall "$found_keeper" session keeper)
  expect "session keeper: persistent" "persistent" "$out"
  expect "session keeper: q/# subscription" "q/#" "$out"
  out=$(api oncall "$found_keeper" GET "/admin/v1/session?client=keeper")
  body=$(sed '$d' <<<"$out")
  expect "api session: queued 1" "1" "$(jqv 'd.get("queued")' <<<"$body")"
fi
if [ -n "$found_watcher" ]; then
  out=$(cli oncall "$found_watcher" subscribers a/b/temp)
  expect "subscribers a/b/temp: watcher" "watcher" "$out"
  out=$(api oncall "$found_watcher" GET "/admin/v1/subscribers?topic=a/%2B/temp")
  expect "api subscribers: wildcard refused" "[http=400]" "$out"
fi
out=$(cli oncall 1 backlog --top 5); expect "backlog verb" "[exit=0]" "$out"
retained_ok=""
for n in 1 2 3; do
  out=$(api oncall $n GET "/admin/v1/retained?prefix=r/")
  body=$(sed '$d' <<<"$out")
  [ "$(jqv 'd["count"]' <<<"$body" 2>/dev/null)" = "2" ] && retained_ok="$retained_ok $n"
done
[ -n "$retained_ok" ] && ok "retained r/: count 2 on mqttd-$retained_ok" || bad "retained r/ count 2" "$out"
out=$(cli oncall 1 retained --prefix r/ --limit 1); expect "retained CLI paging" "next_cursor" "$out"

echo "=== authz dry run"
out=$(cli oncall 1 authz anonymous publish a/b); expect "authz allow" "True" "$(sed -n '/allowed/p' <<<"$out" | tr 'a-z' 'A-Z' | sed 's/TRUE/True/')"
out=$(api oncall 1 GET "/admin/v1/authz?user=anonymous&action=publish&target=secret/x")
body=$(sed '$d' <<<"$out")
expect "authz deny names rule 1" "False/1/deny" "$(jqv 'f"{d["allowed"]}/{d["rule"]["index"]}/{d["rule"]["effect"]}"' <<<"$body")"
out=$(api oncall 1 GET "/admin/v1/authz?user=x&action=subscribe&target=a/%23/b"); expect "authz invalid filter 400" "[http=400]" "$out"

echo "=== config and reload"
out=$(cli oncall 1 config --json); expect "config has admin section" ""admin"" "$out"
expect "config file checksum" "file_checksum" "$out"
out=$(cli oncall 1 reload); expect "viewer cannot reload" "403 forbidden" "$out"
printf '\n[limits]\nmax_sessions = 100000\n' >> "$E/cfg1/mqttd.toml"; sleep 2
out=$(cli root 1 reload); expect "reload applied, limits changed" "limits" "$out"; expect "reload exit 0" "[exit=0]" "$out"
cp "$E/cfg1/mqttd.toml" "$E/cfg1/good.toml"; echo "[limits" >> "$E/cfg1/mqttd.toml"; sleep 2
out=$(cli root 1 reload); expect "broken file: 409" "409 reload-rejected" "$out"; expect "broken reload exit 1" "[exit=1]" "$out"
out=$(api root 1 POST /admin/v1/reload); expect "api reload 409 carries outcome" ""outcome"" "$out"
cp "$E/cfg1/good.toml" "$E/cfg1/mqttd.toml"; sleep 2
out=$(cli root 1 reload); expect "restored reload applied" "[exit=0]" "$out"

echo "=== kick and purge (forwarded to the owner from another node)"
python3 -u "$KIT/raw_v5.py" 31883 kickme > "$E/kickme.out" 2>&1 &
KICK_PID=$!
sleep 2
out=$(cli oncall 1 kick kickme); expect "viewer cannot kick" "403 forbidden" "$out"
out=$(cli root 1 kick kickme --json)
expect "kick via mqttd-1: disconnected" '"disconnected": true' "$out"
expect "kick acted on mqttd-3, where the client is" '"node": "mqttd-3"' "$out"
expect "kick forwarded from mqttd-1" '"forwarded_to": "mqttd-3"' "$out"
sleep 1
if kill -0 $KICK_PID 2>/dev/null; then bad "kicked client's connection closed" "still running"; kill $KICK_PID; else ok "kicked client's connection closed"; fi
expect "client received DISCONNECT 0x98" "DISCONNECT reason=0x98" "$(cat "$E/kickme.out")"
if [ -n "$found_keeper" ]; then
  other=$(( found_keeper % 3 + 1 ))
  out=$(cli root $other purge keeper --json)
  expect "purge keeper from mqttd-$other" '"session_found": true' "$out"
  out=$(cli oncall "$found_keeper" session keeper); expect "purged session gone" "404 not-found" "$out"
fi

echo "=== cordon"
out=$(cli root 3 cordon); expect "cordon" "cordoned" "$out"
out=$(docker exec mqttd-admin-e2e-mqttd-3-1 mqttd --probe /readyz --config /cfg/mqttd.toml 2>&1; echo "[exit=$?]"); expect "cordoned /readyz 503" "503" "$out"
out=$(mosquitto_pub -h 127.0.0.1 -p 31883 -i refused -t x -m y 2>&1; echo "[exit=$?]"); expect_not "cordoned node refuses a new connection" "[exit=0]" "$out"
out=$(api oncall 1 GET /admin/v1/cluster); body=$(sed '$d' <<<"$out")
expect "cluster shows mqttd-3 not ready" "False" "$(jqv '[n for n in d["nodes"] if n["node_id"]=="mqttd-3"][0]["ready"]' <<<"$body")"
out=$(api root 3 POST /admin/v1/uncordon); expect "api uncordon" '"cordoned":false' "$out"
out=$(mosquitto_pub -h 127.0.0.1 -p 31883 -i accepted -t x -m y 2>&1; echo "[exit=$?]"); expect "uncordoned node accepts" "[exit=0]" "$out"

echo "=== log filter"
out=$(cli oncall 2 log-level); expect "log-level base" "base" "$out"
out=$(cli root 2 log-override 'audit=off'); expect "audit filter refused" "400" "$out"
out=$(cli root 2 log-override 'mqttd::hub=debug' --ttl 120); expect "override set" "audit=info" "$out"
out=$(api oncall 2 GET /admin/v1/node); expect "statusz shows override" "log_filter" "$out"
out=$(api root 2 POST /admin/v1/log-level/reset); expect "api reset" "[http=200]" "$out"

echo "=== audit"
logs=$(docker logs mqttd-admin-e2e-mqttd-1-1 2>&1)
expect "audit records admin requests" "admin.request" "$logs"
expect "audit names the operator" "CN=root" "$logs"

kill $SUB_PID 2>/dev/null
echo
echo "SUMMARY: $PASS passed, $FAIL failed"
for f in "${FAILED[@]:-}"; do [ -n "$f" ] && echo "  failed: $f"; done
[ "$FAIL" = 0 ]
