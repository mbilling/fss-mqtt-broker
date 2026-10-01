# Split-brain detection: the founder restarts WITHOUT its state (ephemeral durability), so
# it founds a second cluster beside the live one. Hearing the other cluster's gossip, it
# quarantines itself (the re-found guard), and the cluster view flags the split:
# `same_cluster_id: false` and the node marked quarantined.
SCENARIO_DATA=0

node1_live() { curl -s --max-time 2 "localhost:$(health_port 1)/livez" | grep -q '"live":true'; }
node1_cluster_id() { api_json oncall 1 GET /admin/v1/node | jqv 'd.get("cluster_id") or ""'; }
node1_refounded() { local id; id=$(node1_cluster_id); [ -n "$id" ] && [ "$id" != "$ORIGINAL" ]; }
node1_quarantined() { api_json oncall 1 GET /admin/v1/node | grep -q 'refounded-beside-live-cluster'; }
node1_not_ready() { ! curl -s --max-time 2 "localhost:$(health_port 1)/readyz" | grep -q '"ready":true'; }

run() {
  eventually "the cluster forms" 90 formed 2
  ORIGINAL=$(row_field 2 2 cluster_id)
  node_kill 1
  node_start 1
  eventually "mqttd-1 answers again" 60 node1_live
  eventually "mqttd-1 founded a new cluster identity" 60 node1_refounded
  eventually "mqttd-1 quarantines itself (re-founded beside a live cluster)" 90 node1_quarantined
  eventually "mqttd-2's cluster view flags the split: same_cluster_id false" 60 view_is 2 'd["summary"]["same_cluster_id"]' False
  check "mqttd-2's view marks mqttd-1 quarantined" row_is 2 1 quarantined True
  check "the quarantined node is not ready (out of rotation)" node1_not_ready
}
