# Session takeover across nodes: a persistent client id connected on node 1 connects again
# on node 2 (persistent sessions live on their placement owner, ADR 0005; clean-start ones
# are served where they land, so only persistent sessions can be taken over across nodes);
# the first connection is closed, the second stays, and the cluster holds one connected
# session for that id.

connected_count() {
  local n total=0 c
  for n in 1 2 3; do
    c=$(api_json oncall "$n" GET '/admin/v1/clients?prefix=dup' | jqv 'sum(1 for s in d["sessions"] if s["connected"])')
    total=$((total + ${c:-0}))
  done
  echo "$total"
}
connected_is() { [ "$(connected_count)" = "$1" ]; }

run() {
  eventually "the cluster forms" 90 formed 1
  raw_start first 1 dup --persist && ok "dup connected on mqttd-1" || bad "dup connected on mqttd-1" "$(raw_out first)"
  eventually "one connected session for dup" 15 connected_is 1
  raw_start second 2 dup --persist && ok "dup connected again on mqttd-2" || bad "dup connected again on mqttd-2" "$(raw_out second)"
  eventually "the first connection is closed" 15 raw_closed first
  sleep 2  # SETTLE: the second connection must STAY open; an absence has no event to poll
  if raw_closed second; then bad "the second connection stays open" "$(raw_out second)"; else ok "the second connection stays open"; fi
  eventually "still exactly one connected session for dup" 15 connected_is 1
}
