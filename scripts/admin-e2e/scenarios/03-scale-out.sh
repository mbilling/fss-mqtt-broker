# Scale out: a 4th node joins a running 3-node cluster; every node's view shows four
# members, all replying and agreeing.
SCENARIO_NODES=4
SCENARIO_START="1 2 3"

run() {
  eventually "the 3-node cluster forms" 90 formed 1 3
  cluster_up 4 && ok "mqttd-4 started and is ready" || bad "mqttd-4 started and is ready"
  local n
  for n in 1 2 3 4; do
    eventually "mqttd-$n sees 4 members reply and agree" 120 formed "$n" 4
  done
  check "mqttd-4 is in mqttd-1's placement view" view_is 1 'len(d["nodes"])' 4
}
