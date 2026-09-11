# Demo Grafana provisioning

Dashboards are **not** stored here. The Compose stack mounts
[`deploy/observability/grafana/`](../../deploy/observability/grafana/) as
`/var/lib/grafana/dashboards` (ADR 0070 T4 — production is the source).

This directory keeps Grafana *provisioning* (datasources, dashboard provider).
Regenerate the bridge JSON with `python3 scripts/gen-bridge-dashboard.py`.
