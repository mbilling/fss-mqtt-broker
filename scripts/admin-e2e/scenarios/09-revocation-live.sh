# Revocation reaches a live session: a client subscribed to feed/# receives; the ACL is
# tightened to deny subscribing to feed/# and every node reloads; the dry run reports the
# deny, and the already-connected subscriber stops receiving.

run() {
  eventually "the cluster forms" 90 formed 1
  raw_start watcher 2 watcher --sub 'feed/#' && ok "watcher subscribed on mqttd-2" || bad "watcher subscribed" "$(raw_out watcher)"
  eventually "watcher's subscription is granted" 10 raw_has watcher "SUBACK reasons=0x01"
  pub 1 -q 1 -t feed/1 -m before
  eventually "watcher receives feed/1 before the change" 15 raw_has watcher "PUBLISH feed/1 before"
  printf '\n[[rules]]\nactions = ["subscribe"]\neffect = "deny"\ntopics = ["feed/#"]\n' >> "$STATE/acl.toml"
  sync_pause
  check "every node reloads the tightened ACL" reload_all
  local out
  out=$(api_json oncall 1 GET "/admin/v1/authz?user=anonymous&action=subscribe&target=feed/%23")
  expect "the dry run now denies subscribing to feed/#" '"allowed": false' "$(jqv 'json.dumps(d, indent=1)' <<<"$out" 2>/dev/null || echo "$out")"
  pub 1 -q 1 -t feed/2 -m after
  sleep 3  # SETTLE: proving feed/2 is NOT delivered; an absence has no event to poll
  if raw_has watcher "feed/2"; then bad "the live subscriber no longer receives feed/#" "$(raw_out watcher)"; else ok "the live subscriber no longer receives feed/#"; fi
}
