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
# ADR 0088 D1: per-pod cluster-bus certificates from cert-manager's csi-driver, with the
# lifetime and the renewal lead stated — the csi volume and its attributes, the tls.crt /
# tls.key paths (no $(POD_NAME) indirection) and the admin API served from the same volume.
"$HERE/render-parity-one.sh" "cert-manager per-pod certificates (lifetime + renewal lead stated)" \
  "--set secrets.peerTls.certManager.issuerName=mqttd-cluster-ca --set secrets.peerTls.certManager.duration=24h --set secrets.peerTls.certManager.renewBefore=10h --set secrets.gossipKey.secretName=mqttd-gossip" \
  "--peer-tls-cert-manager"
"$HERE/render-parity-one.sh" "cert-manager per-pod certificates + admin API" \
  "--set secrets.peerTls.certManager.issuerName=mqttd-cluster-ca --set secrets.peerTls.certManager.duration=24h --set secrets.peerTls.certManager.renewBefore=10h --set secrets.gossipKey.secretName=mqttd-gossip --set admin.enabled=true --set admin.clientCa.secretName=mqttd-admin-ca --set admin.viewers[0]=CN=oncall --set admin.operators[0]=CN=sre-lead" \
  "--peer-tls-cert-manager --admin"
# ... and the chart refuses to render that volume without them: a lifetime nobody stated is
# cert-manager's 90 days.
for missing in duration renewBefore; do
  other=$([ "$missing" = duration ] && echo renewBefore || echo duration)
  if out="$(helm template mqttd "$HERE/../../deploy/helm/mqttd" \
       --set secrets.peerTls.certManager.issuerName=mqttd-cluster-ca \
       --set "secrets.peerTls.certManager.$other=10h" 2>&1)"; then
    echo "FAIL: the chart rendered a cert-manager csi volume with no $missing stated"; exit 1
  fi
  grep -q "certManager.$missing is required" <<<"$out" \
    || { echo "$out"; echo "FAIL: the chart's refusal does not name certManager.$missing"; exit 1; }
done
echo "ok — the chart refuses secrets.peerTls.certManager without a stated duration and renewBefore"
echo
echo "RENDER PARITY OK IN BOTH FOUNDER-GUARD STATES, WITH THE CLUSTER BUS ON, WITH THE ADMIN API ON, WITH THE NETWORKPOLICY ON, AND WITH CERT-MANAGER PER-POD CERTIFICATES"
