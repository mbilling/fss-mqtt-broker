#!/usr/bin/env bash
# The chart's alert rules load in Prometheus and fire when they should.
#
# Renders the chart's PrometheusRule (metrics.prometheusRule.enabled=true), takes the rule
# groups out of it, and runs promtool over them twice: `check rules` (every expression
# parses, every rule is well formed) and `test rules` against each unit test in
# deploy/helm/mqttd/ci/alerts/ — series a broker would export, and the alerts that must
# and must not fire from them at given times. No cluster and no Prometheus server.
#
# Needs helm and promtool (part of the Prometheus release; PROMTOOL names another binary).
#
# Usage: scripts/k8s/check-alert-rules.sh
set -euo pipefail

# From a checkout or from the unpacked example bundle: the chart is two levels up either way.
cd "$(dirname "$0")/../.."
CHART="deploy/helm/mqttd"
TESTS="$CHART/ci/alerts"
PROMTOOL="${PROMTOOL:-promtool}"

for tool in helm "$PROMTOOL"; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: $tool not found"; exit 2; }
done

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# A PrometheusRule's `spec` is a Prometheus rule file, two spaces in.
helm template mqttd "$CHART" --set metrics.prometheusRule.enabled=true \
    --show-only templates/prometheusrule.yaml \
  | awk '/^spec:/{f=1;next} f&&/^[^ ]/{f=0} f{sub(/^  /,"");print}' > "$WORK/rules.yaml"
grep -q '^groups:' "$WORK/rules.yaml" \
  || { echo "FATAL: the rendered PrometheusRule has no spec.groups"; exit 1; }

"$PROMTOOL" check rules "$WORK/rules.yaml"

count=0
for t in "$TESTS"/*.yaml; do
  [ -e "$t" ] || { echo "FATAL: no alert tests in $TESTS"; exit 1; }
  cp "$t" "$WORK/"
  "$PROMTOOL" test rules "$WORK/$(basename "$t")"
  count=$((count + 1))
done
echo "alert rules OK: $(grep -c '^ *- alert:' "$WORK/rules.yaml") rules load, $count test file(s) pass"
