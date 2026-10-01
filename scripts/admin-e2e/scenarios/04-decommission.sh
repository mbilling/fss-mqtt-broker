# Decommission: `mqttd --decommission` on node 3 drains its durable data to the others and
# stops it; the drain is visible on its health endpoint while it runs, the process exits,
# and the remaining nodes' views settle on two members.

decommission_seen() { curl -s --max-time 1 "localhost:$(health_port 3)/readyz" | grep -q '"decommission"'; }

run() {
  eventually "the cluster forms" 90 formed 1
  pub 1 -q 1 -r -t keep/me -m before-the-drain
  docker exec "$(container 3)" mqttd --decommission --timeout 120 >/dev/null 2>&1 &
  eventually "the drain is visible on mqttd-3's /readyz" 30 decommission_seen
  wait_until "mqttd-3 to exit" 150 bash -c "! docker inspect -f '{{.State.Running}}' $(container 3) | grep -q true" \
    && ok "mqttd-3 exited after the drain" || bad "mqttd-3 exited after the drain"
  check "it exited cleanly (status 0)" bash -c "[ \"\$(docker inspect -f '{{.State.ExitCode}}' $(container 3))\" = 0 ]"
  eventually "mqttd-1's view settles on 2 members, both replying" 120 view_is 1 '(d["summary"]["nodes"], d["summary"]["replied"])' "(2, 2)"
  eventually "mqttd-2 agrees" 60 view_is 2 '(d["summary"]["nodes"], d["summary"]["replied"], d["summary"]["same_membership"])' "(2, 2, True)"
  check "a retained message survives the departure" bash -c \
    "curl -s --max-time 10 --cacert $P/cluster-ca.pem --cert $P/oncall.pem --key $P/oncall.key --resolve mqttd-1:$(admin_port 1):127.0.0.1 'https://mqttd-1:$(admin_port 1)/admin/v1/retained?prefix=keep/' | grep -q '\"count\":1'"
}
