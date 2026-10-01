# Retained replication: a retained message published on node 1 is served by nodes 2 and 3,
# and clearing it on node 3 clears it everywhere.

retained_count_is() { [ "$(api_json oncall "$1" GET '/admin/v1/retained?prefix=plant/' | jqv 'd["count"]')" = "$2" ]; }

run() {
  eventually "the cluster forms" 90 formed 1
  pub 1 -q 1 -r -t plant/7/status -m online
  local n
  for n in 1 2 3; do
    eventually "mqttd-$n serves the retained message" 30 retained_count_is "$n" 1
  done
  pub 3 -q 1 -r -t plant/7/status -n
  for n in 1 2 3; do
    eventually "the clear on mqttd-3 reaches mqttd-$n" 30 retained_count_is "$n" 0
  done
}
