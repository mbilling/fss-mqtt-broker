# The temporary log filter expires on its own: a 3 s override is shown on the node and in
# log-level, then gone without anyone resetting it.

override_gone() { [ "$(api_json oncall 1 GET /admin/v1/log-level | jqv 'd["override_filter"]')" = None ]; }

run() {
  eventually "the cluster forms" 90 formed 1
  local out
  out=$(api_json root 1 POST '/admin/v1/log-level?filter=mqttd%3A%3Ahub%3Ddebug&ttl=3')
  expect "the override is set (with audit kept)" "mqttd::hub=debug,audit=info" "$out"
  expect "the node status shows it" "log_filter" "$(api_json oncall 1 GET /admin/v1/node)"
  eventually "the override expires on its own" 15 override_gone
  expect_not "and the node status no longer shows it" "log_filter" "$(api_json oncall 1 GET /admin/v1/node)"
}
