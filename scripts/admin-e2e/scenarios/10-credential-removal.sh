# Removing a user evicts their live connection: password auth (no anonymous access), two
# users connected; alice is removed from the password file and every node reloads; alice's
# connection is closed, bob's stays, and alice can no longer connect.

setup() {
  local alice bob n
  alice=$(printf %s alice-secret | "$BIN" --hash-password alice)
  bob=$(printf %s bob-secret | "$BIN" --hash-password bob)
  printf '%s\n%s\n' "$alice" "$bob" > "$STATE/passwd"
  for n in 1 2 3; do
    file_replace "$STATE/cfg$n/mqttd.toml" "allow_anonymous = true" \
      "allow_anonymous = false
password_file = \"/e2e/passwd\""
  done
  BOB_LINE=$bob
}

run() {
  eventually "the cluster forms" 90 formed 1
  raw_start alice 2 alice-dev --user alice --password alice-secret && ok "alice connected" || bad "alice connected" "$(raw_out alice)"
  raw_start bob 2 bob-dev --user bob --password bob-secret && ok "bob connected" || bad "bob connected" "$(raw_out bob)"
  raw_try nobody 2 anon-dev
  check "an anonymous client is refused" raw_has_any nobody "CONNACK reason=0x8" "closed"
  file_write "$STATE/passwd" "$BOB_LINE
"
  sync_pause
  check "every node reloads the password file" reload_all
  eventually "alice's live connection is closed" 20 raw_closed alice
  sleep 2  # SETTLE: bob must STAY connected; an absence has no event to poll
  if raw_closed bob; then bad "bob stays connected" "$(raw_out bob)"; else ok "bob stays connected"; fi
  raw_try again 2 alice-dev2 --user alice --password alice-secret
  check "alice can no longer connect" raw_has_any again "CONNACK reason=0x8" "closed"
}
