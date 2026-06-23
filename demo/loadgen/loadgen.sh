#!/bin/sh
# Drive cross-node MQTT traffic so the dashboard is populated:
#  - a persistent QoS-1 subscriber on node-2 (=> sessions, subscriptions, cross-node
#    interest gossip, publish_delivered on node-2),
#  - a steady stream of QoS-1 publishes on node-1 (=> publish_received, peer forward,
#    inflight) plus a retained message (=> retained_messages),
#  - a few short-lived connections on node-3 (=> connection churn / accepts).
set -e

echo "loadgen: waiting for brokers to accept connections..."
for host in mqttd-1 mqttd-2 mqttd-3; do
	until mosquitto_pub -h "$host" -p 1883 -t demo/_ready -m ping -q 0 2>/dev/null; do
		sleep 2
	done
done
echo "loadgen: brokers up — generating traffic"

# Persistent cross-node subscriber (clean session off so it shows as a retained session).
mosquitto_sub -h mqttd-2 -p 1883 -t 'demo/#' -q 1 -i demo-sub -c -d >/dev/null 2>&1 &

i=0
while true; do
	mosquitto_pub -h mqttd-1 -p 1883 -t demo/topic   -q 1 -m "msg $i"       -i demo-pub
	mosquitto_pub -h mqttd-1 -p 1883 -t demo/retained -q 1 -r -m "retained $i" -i demo-pub
	# A short-lived connection on node-3 every few iterations (connection churn).
	if [ $((i % 5)) -eq 0 ]; then
		mosquitto_pub -h mqttd-3 -p 1883 -t demo/ping -q 0 -m hi -i "demo-ping-$i"
	fi
	i=$((i + 1))
	sleep 1
done
