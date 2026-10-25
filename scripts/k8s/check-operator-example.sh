#!/usr/bin/env bash
# The operator's example MqttdCluster names only Secrets and ConfigMaps that the chart's
# bootstrap.sh creates. The README, docs/KUBERNETES.md and the operator README all say
# "run bootstrap.sh, then apply the example"; until 2026-10-25 the example named
# mqttd-server-tls and mqttd-gossip-key, which bootstrap.sh never made, so that path
# could not start a pod.
#
# Usage: scripts/k8s/check-operator-example.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
EXAMPLE="deploy/helm/mqttd-operator/example-mqttdcluster.yaml"
BOOTSTRAP="deploy/helm/mqttd/bootstrap.sh"

# What bootstrap.sh creates: `kubectl create secret <type> <name>` and `kubectl create configmap <name>`.
made="$(grep -oE 'kubectl create (secret (generic|tls)|configmap) [a-z0-9-]+' "$BOOTSTRAP" | awk '{print $NF}' | sort -u)"
# What the example's spec.secrets names (uncommented `key: name` lines inside that block).
named="$(awk '/^  secrets:/{f=1;next} f&&/^  [^ ]/{f=0} f&&/^    [a-zA-Z]+: [a-z0-9-]+ *$/{print $2}' "$EXAMPLE" | sort -u)"

[ -n "$named" ] || { echo "FATAL: found no spec.secrets names in $EXAMPLE"; exit 1; }
missing="$(comm -23 <(echo "$named") <(echo "$made"))"
if [ -n "$missing" ]; then
  echo "FATAL: $EXAMPLE names objects $BOOTSTRAP does not create:"
  echo "$missing" | sed 's/^/  /'
  echo "bootstrap.sh creates: $(echo "$made" | tr '\n' ' ')"
  exit 1
fi
echo "operator example OK: $(echo "$named" | tr '\n' ' ')all created by bootstrap.sh"
