#!/usr/bin/env bash
# Run the parity comparison in BOTH founder-guard states (ADR 0055 T9): the default
# (bootstrap-capable) render, and the armed render where ordinal 0 seeds to its peers.
# A drift in either is a drift.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"$HERE/render-parity-one.sh" "default (founder guard off)" "" ""
"$HERE/render-parity-one.sh" "founder guard ARMED" "--set clusterEstablished=true" "--established"
# Issue #262: with the cluster bus ON, both paths must derive the per-pod
# MQTTD_PEER_TLS_{CA,CERT,KEY} and MQTTD_SWIM_KEY_FILE paths from the secret names —
# the wiring that stops a mounted peer-bus Secret from sitting unread. The two
# secret-less passes above compare none of it, which is precisely how the chart and
# the operator both came to mount cluster-bus material no broker ever opened.
"$HERE/render-parity-one.sh" "cluster bus ON (peer TLS + gossip key)" \
  "--set secrets.peerTls.secretName=mqttd-peer-tls --set secrets.gossipKey.secretName=mqttd-gossip" \
  "--peer-tls"
echo
echo "RENDER PARITY OK IN BOTH FOUNDER-GUARD STATES, AND WITH THE CLUSTER BUS ON"
