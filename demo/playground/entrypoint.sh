#!/bin/sh
# Playground gateway entrypoint: start the cluster-side relay, then mosquitto.
#
# The relay runs ON the cluster (connects to mqttd-1): it republishes every
# play/up/<topic> the browser publishes to play/down/<topic>, so each message makes a
# full round-trip through the mqttd cluster before fanning out to the browser
# subscribers (which subscribe to play/down/#). Disjoint up/down topics mean no echo.
set -eu

relay() {
  # Wait for the cluster to accept connections.
  until mosquitto_pub -h mqttd-1 -p 1883 -t play/_relay/ready -m 1 2>/dev/null; do
    sleep 1
  done
  echo "playground relay: cluster reachable, relaying play/up/# -> play/down/#"
  # `-F '%t %p'`: one line per message, "<topic> <payload>". read -r keeps payload spaces.
  mosquitto_sub -h mqttd-1 -p 1883 -t 'play/up/#' -F '%t %p' | while read -r topic payload; do
    mosquitto_pub -h mqttd-1 -p 1883 -t "play/down/${topic#play/up/}" -m "$payload"
  done
}

relay &
exec /usr/sbin/mosquitto -c /mosquitto/config/mosquitto.conf
