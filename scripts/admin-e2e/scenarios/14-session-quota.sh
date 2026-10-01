# Quotas through reload: node 1 reloads with max_sessions = 1; with one client connected the
# next is refused (MQTT 5 CONNACK 0x97 Quota exceeded); raised again, a client connects.

run() {
  eventually "the cluster forms" 90 formed 1
  cp "$STATE/cfg1/mqttd.toml" "$STATE/cfg1.orig"
  cfg_append 1 "
[limits]
max_sessions = 1"
  expect "mqttd-1 reloads with the quota" '"applied":true' "$(api_json root 1 POST /admin/v1/reload)"
  raw_start q1 1 q-one && ok "the first client connects" || bad "the first client connects" "$(raw_out q1)"
  raw_try q2 1 q-two
  check "the second is refused with 0x97 Quota exceeded" raw_has q2 "CONNACK reason=0x97"
  cp "$STATE/cfg1.orig" "$STATE/cfg1/mqttd.toml"  # cp rewrites in place: the bind mount sees it
  sync_pause
  expect "mqttd-1 reloads without the quota" '"applied":true' "$(api_json root 1 POST /admin/v1/reload)"
  raw_start q3 1 q-three && ok "a client connects again" || bad "a client connects again" "$(raw_out q3)"
}
