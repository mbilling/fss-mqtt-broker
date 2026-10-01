# Config drift: one node's config file changes and only that node reloads; the cluster view
# flags `same_config: false`; restoring the file and reloading brings it back to true.

checksum_differs() { [ "$(row_field 1 2 config_checksum)" != "$(row_field 1 1 config_checksum)" ]; }

run() {
  eventually "the cluster forms with one config" 90 view_is 1 'd["summary"]["same_config"]' True
  cp "$STATE/cfg2/mqttd.toml" "$STATE/cfg2.orig"
  cfg_append 2 "
[limits]
max_sessions = 5000"
  local out
  out=$(api_json root 2 POST /admin/v1/reload)
  expect "mqttd-2's reload reports the limits section changed" "limits" "$out"
  eventually "the cluster view flags the drift (same_config false)" 30 view_is 1 'd["summary"]["same_config"]' False
  check "mqttd-2's checksum is the odd one out" checksum_differs
  cp "$STATE/cfg2.orig" "$STATE/cfg2/mqttd.toml"  # cp rewrites in place: the bind mount sees it
  sync_pause
  expect "mqttd-2 reloads the restored file" '"applied":true' "$(api_json root 2 POST /admin/v1/reload)"
  eventually "the drift is gone (same_config true)" 30 view_is 1 'd["summary"]["same_config"]' True
}
