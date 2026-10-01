# Role boundaries: a node's cluster certificate (the peer role) may read a node's own
# state and nothing else; it may not act unless it is forwarding; an unlisted certificate
# gets nothing; a viewer may not act.

run() {
  eventually "the cluster forms" 90 formed 1
  expect "peer may read node state" "[http=200]" "$(api mqttd-1 2 GET /admin/v1/node)"
  expect "peer may not list clients" "[http=403]" "$(api mqttd-1 2 GET /admin/v1/clients)"
  expect "peer may not fan out (cluster)" "[http=403]" "$(api mqttd-1 2 GET /admin/v1/cluster)"
  expect "peer may not kick without the forwarding marker" "[http=403]" "$(api mqttd-1 2 POST '/admin/v1/kick?client=nobody')"
  expect "peer may kick as a forward (acts locally, nothing there: 404)" "[http=404]" \
    "$(api mqttd-1 2 POST '/admin/v1/kick?client=nobody&forwarded_for=CN%3Droot')"
  expect "peer may not cordon" "[http=403]" "$(api mqttd-1 2 POST /admin/v1/cordon)"
  expect "unlisted certificate: whoami refused" "[http=403]" "$(api stranger 2 GET /admin/v1/whoami)"
  expect "viewer may read" "[http=200]" "$(api oncall 2 GET /admin/v1/clients)"
  expect "viewer may not cordon" "[http=403]" "$(api oncall 2 POST /admin/v1/cordon)"
  expect "viewer may not reload" "[http=403]" "$(api oncall 2 POST /admin/v1/reload)"
  expect "operator may act" "[http=200]" "$(api root 2 POST /admin/v1/uncordon)"
}
