# Durability through the admin API: a persistent session with three queued QoS 1 messages;
# its owner node is killed; the session — with the messages — shows up on a surviving node,
# and the client reconnecting there receives all three, exactly once each.
#
# The kill comes within seconds of the enqueue, inside the takeover window, which is what
# used to duplicate them (#784): the survivor re-delivered the held publishes into the
# inherited session, which already held the dead owner's copies — queued 6, each delivered
# twice. A replay now skips a session that already holds the publish, so the survivor must
# hold exactly 3 for the whole window and the client must receive exactly 3.

queued_on() { api_json oncall "$1" GET "/admin/v1/session?client=keeper" | jqv 'd.get("queued")'; }

queued_three_on_owner() {
  local n
  n=$(session_node keeper) || return 1
  [ "$(queued_on "$n")" = 3 ]
}

# Exactly 3 queued on the survivor at every read for 15 s: the takeover window (and its
# settle-pass replays) runs for several sweep ticks after the session first appears.
stays_three_on_survivor() {
  local i q
  for i in $(seq 1 15); do
    q=$(queued_on "$SURVIVOR")
    [ "$q" = 3 ] || { echo "mqttd-$SURVIVOR holds $q queued, 3 were published"; return 1; }
    sleep 1  # SETTLE: proving the count does NOT grow through the window; an absence has no event to poll
  done
}

queued_on_survivor() {
  local n q
  for n in 1 2 3; do
    [ "$n" = "$OWNER" ] && continue
    q=$(queued_on "$n")
    if [ -n "$q" ] && [ "$q" != None ] && [ "$q" -ge 3 ] 2>/dev/null; then
      SURVIVOR=$n; SURVIVOR_QUEUED=$q; return 0
    fi
  done
  return 1
}

run() {
  eventually "the cluster forms" 90 formed 1
  mosquitto_sub -h 127.0.0.1 -p "$(mqtt_port 1)" -i keeper -c -q 1 -t 'q/#' -W 2 >/dev/null 2>&1
  local i
  for i in 1 2 3; do pub 2 -q 1 -t "q/$i" -m "msg-$i"; done
  eventually "the owner holds keeper's session with 3 queued messages" 30 queued_three_on_owner
  OWNER=$(session_node keeper)
  echo "      keeper's session is on mqttd-$OWNER; killing it"
  node_kill "$OWNER"
  SURVIVOR="" SURVIVOR_QUEUED=""
  eventually "a surviving node holds keeper's session with the messages queued" 120 queued_on_survivor
  check "the survivor holds exactly 3 queued through the takeover window, not duplicated (#784)" \
    stays_three_on_survivor
  local got
  got=$(mosquitto_sub -h 127.0.0.1 -p "$(mqtt_port "${SURVIVOR:-$(( OWNER % 3 + 1 ))}")" -i keeper -c -q 1 -t 'q/#' -v -W 10 2>&1)
  expect "the reconnecting client receives msg-1" "q/1 msg-1" "$got"
  expect "… msg-2" "q/2 msg-2" "$got"
  expect "… and msg-3" "q/3 msg-3" "$got"
  local n
  n=$(grep -c '^q/' <<<"$got")
  expect "… exactly 3 messages, none twice (#784)" "received=3" "received=$n"
}
