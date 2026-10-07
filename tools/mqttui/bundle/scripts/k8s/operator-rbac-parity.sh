#!/usr/bin/env bash
# The operator's Role must grant the SAME rules on both install paths: the chart
# (deploy/helm/mqttd-operator/templates/rbac.yaml) and the dev/e2e manifest
# (deploy/operator/operator.yaml). Both say "keep in lockstep", and nothing checked
# it: the NetworkPolicy (#778) gave the chart `networkpolicies` and the operator a
# delete on them, the manifest got neither, and the nightly operator e2e (which
# installs the manifest) went red on a 403 for four nights running.
#
# Compares the Role's `rules:` block of each, comments and blank lines dropped and
# whitespace normalised, so a rule present in one and not the other (or a verb that
# differs) fails here on every PR instead of in a kind cluster at night.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CHART="$ROOT/deploy/helm/mqttd-operator/templates/rbac.yaml"
MANIFEST="$ROOT/deploy/operator/operator.yaml"

role_rules() { # role_rules <file> — the Role document's rules, one normalised line each
	awk '
		/^---/ { in_role = 0; in_rules = 0; next }
		/^kind: Role$/ { in_role = 1; next }
		in_role && /^rules:/ { in_rules = 1; next }
		in_rules && /^[^ ]/ { in_rules = 0 }
		in_rules {
			sub(/#.*/, "")
			gsub(/[ \t]+/, " ")
			sub(/^ /, ""); sub(/ $/, "")
			if ($0 != "") print
		}
	' "$1"
}

chart_rules=$(role_rules "$CHART")
manifest_rules=$(role_rules "$MANIFEST")
[ -n "$chart_rules" ] || { echo "FAIL: no Role rules found in $CHART" >&2; exit 1; }
[ -n "$manifest_rules" ] || { echo "FAIL: no Role rules found in $MANIFEST" >&2; exit 1; }
if [ "$chart_rules" != "$manifest_rules" ]; then
	echo "FAIL: the operator Role differs between the chart and the dev/e2e manifest:" >&2
	diff <(printf '%s\n' "$chart_rules") <(printf '%s\n' "$manifest_rules") \
		--label "chart: ${CHART#"$ROOT"/}" --label "manifest: ${MANIFEST#"$ROOT"/}" >&2 || true
	exit 1
fi
echo "operator Role: chart and dev/e2e manifest grant the same $(printf '%s\n' "$chart_rules" | grep -c 'verbs:') rules"
