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
# ADR 0081 T15: the admin API on — the MQTTD_ADMIN_* env (served with each pod's own
# cluster-bus leaf), the client-CA mount and the admin container/headless-service ports.
"$HERE/render-parity-one.sh" "admin API ON (cluster bus + admin)" \
  "--set secrets.peerTls.secretName=mqttd-peer-tls --set secrets.gossipKey.secretName=mqttd-gossip --set admin.enabled=true --set admin.clientCa.secretName=mqttd-admin-ca --set admin.viewers[0]=CN=oncall --set admin.operators[0]=CN=sre-lead" \
  "--admin"
# Issue #778: the NetworkPolicy on, with the admin API, so every ingress rule renders.
"$HERE/render-parity-one.sh" "NetworkPolicy ON (admin API + every peer list)" \
  "--set secrets.peerTls.secretName=mqttd-peer-tls --set secrets.gossipKey.secretName=mqttd-gossip --set admin.enabled=true --set admin.clientCa.secretName=mqttd-admin-ca --set admin.viewers[0]=CN=oncall --set admin.operators[0]=CN=sre-lead -f $HERE/../../deploy/helm/mqttd/ci/values-parity-netpol.yaml" \
  "--admin --netpol"
echo
echo "RENDER PARITY OK IN BOTH FOUNDER-GUARD STATES, WITH THE CLUSTER BUS ON, WITH THE ADMIN API ON, AND WITH THE NETWORKPOLICY ON"
