# A network partition: node 3 is cut off the cluster network. The majority side shows it
# as not replying; node 3's own view (asked from inside its container, since the host
# cannot reach it either) shows it cannot reach its peers. Healed, the views converge.

inside_replied() { cli_inside 3 cluster --json | jqv 'd["summary"]["replied"]'; }
inside_replied_is() { [ "$(inside_replied)" = "$1" ]; }

run() {
  eventually "the cluster forms" 90 formed 1
  net_disconnect 3 && ok "mqttd-3 cut off the network" || bad "mqttd-3 cut off the network"
  eventually "mqttd-1 sees mqttd-3 not replying" 30 row_is 1 3 replied False
  eventually "mqttd-2 sees mqttd-3 not replying" 30 row_is 2 3 replied False
  eventually "mqttd-3, isolated, gets no peer replies (only itself)" 30 inside_replied_is 1
  net_connect 3 && ok "the partition healed" || bad "the partition healed"
  eventually "mqttd-1: all three reply and agree again" 120 formed 1
  eventually "mqttd-3: all three reply and agree again" 60 formed 3
}
