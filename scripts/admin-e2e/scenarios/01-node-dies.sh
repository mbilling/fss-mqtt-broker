# A node dies: the survivors' cluster view shows it as not replying (with the reason),
# never silently dropped while it is still a member; restarted, it rejoins with the same
# cluster identity and membership. Node 3 (not the founder) is killed; data dirs keep its
# identity across the restart.

run() {
  eventually "the cluster forms: mqttd-1 sees 3 nodes reply and agree" 90 formed 1
  node_kill 3
  eventually "mqttd-1's view shows mqttd-3 not replying" 30 row_is 1 3 replied False
  local err
  err=$(row_field 1 3 error)
  [ -n "$err" ] && ok "the row says why: $err" || bad "the row says why" "$(view_field 1 'd')"
  eventually "mqttd-2's view shows mqttd-3 not replying" 30 row_is 2 3 replied False
  check "the summary counts 2 replies of 3 members" view_is 2 '(d["summary"]["nodes"], d["summary"]["replied"])' "(3, 2)"
  node_start 3
  wait_ready 3 && ok "mqttd-3 restarted and is ready" || bad "mqttd-3 restarted and is ready"
  eventually "mqttd-3 rejoined: every node replies, same cluster id and membership" 90 formed 1
  eventually "mqttd-3's own view agrees" 30 formed 3
}
