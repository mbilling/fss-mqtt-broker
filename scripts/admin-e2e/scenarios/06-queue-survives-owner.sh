# Durability through the admin API: a persistent session with three queued QoS 1 messages;
# its owner node is killed; the session — with the messages — shows up on a surviving node,
# and the client reconnecting there receives all three.
#
# Known defect this surfaces (reported, not asserted): when the owner dies within seconds
# of the enqueue, the promoted copy holds every message twice (queued 6, each delivered
# twice). QoS 1 allows redelivery, so survival is what passes or fails here; the count is
# printed as a NOTE so a fix shows up in the nightly log.

queued_on() { api_json oncall "$1" GET "/admin/v1/session?client=keeper" | jqv 'd.get("queued")'; }

queued_three_on_owner() {
  local n
  n=$(session_node keeper) || return 1
  [ "$(queued_on "$n")" = 3 ]
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
  [ "$SURVIVOR_QUEUED" = 3 ] || echo "      NOTE: mqttd-${SURVIVOR:-?} holds $SURVIVOR_QUEUED queued (3 were published): duplicated on promotion"
  local got
  got=$(mosquitto_sub -h 127.0.0.1 -p "$(mqtt_port "${SURVIVOR:-$(( OWNER % 3 + 1 ))}")" -i keeper -c -q 1 -t 'q/#' -v -W 10 2>&1)
  expect "the reconnecting client receives msg-1" "q/1 msg-1" "$got"
  expect "… msg-2" "q/2 msg-2" "$got"
  expect "… and msg-3" "q/3 msg-3" "$got"
  local n
  n=$(grep -c '^q/' <<<"$got")
  [ "$n" = 3 ] || echo "      NOTE: the client received $n messages for 3 published"
}
