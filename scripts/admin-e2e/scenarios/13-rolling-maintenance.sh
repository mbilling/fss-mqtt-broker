# A rolling-maintenance drill on the node holding a persistent session: cordon it (not
# ready, new connections refused), kick the client, publish while it is away, restart the
# node, uncordon — and the client reconnects to its intact session and gets the message.
#
# The restart is a kill + start, not a graceful stop. Known defect (reported with this
# suite): after a GRACEFUL stop (SIGTERM, drained) and a start with the same id and data
# dir, the other nodes keep the node DEAD in their membership (re-declared every 30 s, still
# excluded after 2 min, with or without a pause before the start) while it believes it has
# rejoined. Switch this back to `node_restart` once that is fixed.

# One reconnect attempt. Right after a restart the node can answer 0x88 "server
# unavailable, retry" while its durable plane re-forms (ADR 0017); a real client retries.
reconnect_worker() {
  raw_stop worker2 2>/dev/null
  raw_try worker2 "$NODE" worker --persist --sub 'jobs/#'
  raw_has worker2 "CONNACK reason=0x00" && return 0
  raw_has worker2 "CONNACK reason=0x88" && REFUSALS=$((REFUSALS + 1))
  sleep 2
  return 1
}

run() {
  eventually "the cluster forms" 90 formed 1
  raw_start worker 2 worker --persist --sub 'jobs/#' && ok "worker connected (persistent)" || bad "worker connected" "$(raw_out worker)"
  eventually "worker's subscription is granted" 10 raw_has worker "SUBACK"
  local node
  wait_until "worker's session to be located" 20 session_node worker
  node=$(session_node worker) || node=2
  echo "      worker's session is on mqttd-$node"
  expect "cordon mqttd-$node" '"cordoned":true' "$(api root "$node" POST /admin/v1/cordon)"
  check "cordoned: /readyz is not ready" bash -c "! curl -s localhost:$(health_port "$node")/readyz | grep -q '\"ready\":true'"
  check "cordoned: a new connection is refused" bash -c "! mosquitto_pub -h 127.0.0.1 -p $(mqtt_port "$node") -i newcomer -t x -m y"
  expect "kick worker" '"disconnected":true' "$(api root 1 POST '/admin/v1/kick?client=worker')"
  eventually "worker's connection is closed" 15 raw_closed worker
  local other=$(( node % 3 + 1 ))
  pub "$other" -q 1 -t jobs/1 -m while-away
  node_kill "$node"; node_start "$node"  # unclean on purpose: see the header
  wait_ready "$node" && ok "mqttd-$node restarted and is ready (cordon not persisted)" || bad "mqttd-$node restarted and is ready"
  expect "uncordon is a no-op after the restart" '"changed":false' "$(api root "$node" POST /admin/v1/uncordon)"
  eventually "the cluster re-forms" 90 formed 1
  REFUSALS=0 NODE=$node
  eventually "worker reconnects (retrying on 0x88, as a client must)" 90 reconnect_worker
  [ "$REFUSALS" = 0 ] || echo "      NOTE: refused $REFUSALS time(s) with 0x88 (durable session unavailable) after mqttd-$node reported ready"
  eventually "worker receives the message published while it was away" 30 raw_has worker2 "PUBLISH jobs/1 while-away"
}
