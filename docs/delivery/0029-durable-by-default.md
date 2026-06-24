---
adr: "0029"
title: Durable sessions by default
adr_status: Accepted
tasks:
  - id: 0029-T1
    title: Flip MQTTD_DURABLE_SESSIONS to default-on (opt-out via 0/false/off/no); update startup docs/logging
    status: done
    date: 2026-06-24
    evidence: "start_hub uses durable_enabled(MQTTD_DURABLE_SESSIONS): unset -> on, 0/false/off/no (case-insensitive) -> off. Unit test durable_is_the_default_and_opts_out_explicitly. main.rs module + start_hub docs updated."
  - id: 0029-T2
    title: Make the demo durable by default (fold durable.yml into docker-compose.yml; drop the opt-in overlay)
    status: done
    date: 2026-06-24
    evidence: "docker-compose.yml carries MQTTD_DATA_DIR=/data + per-node d1/d2/d3 volumes in the base (durable via the new broker default, no explicit flag); durable.yml deleted. docker compose config validates."
  - id: 0029-T3
    title: Update README (env var table default, demo instructions, durable framing)
    status: done
    date: 2026-06-24
    evidence: "README: env table shows durable on-by-default + opt-out + a MQTTD_DATA_DIR row; feature bullet and demo section reframed as durable-by-default; removed the durable.yml overlay instructions."
---

# Delivery — ADR 0029: Durable sessions by default

Decision: [docs/adr/0029-durable-by-default.md](../adr/0029-durable-by-default.md).

Durable is stable (ADR 0026/0027/0028), so the robust replicated store becomes the default
rather than an opt-in. `MQTTD_DURABLE_SESSIONS` becomes an opt-out; on-disk persistence stays
governed orthogonally by `MQTTD_DATA_DIR`.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0029-T1** Broker | `start_hub` defaults to durable when `MQTTD_DURABLE_SESSIONS` is unset; `0/false/off/no` opts out to the in-memory store; the effective mode is logged. Module docs updated. |
| **0029-T2** Demo | `docker compose up` runs the durable cluster (durable env + per-node volumes folded into `docker-compose.yml`); the `durable.yml` overlay is removed. |
| **0029-T3** Docs | README env var table shows durable as the default with the opt-out and the single-node/data-dir guidance; demo instructions updated. |

## Progress

<!-- status-table:0029 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0029-T1 | ✅ done | 2026-06-24 | "start_hub uses durable_enabled(MQTTD_DURABLE_SESSIONS): unset -> on, 0/false/off/no (case-insensitive) -> off. Unit test durable_is_the_default_and_opts_out_explicitly. main.rs module + start_hub docs updated." |
| 0029-T2 | ✅ done | 2026-06-24 | "docker-compose.yml carries MQTTD_DATA_DIR=/data + per-node d1/d2/d3 volumes in the base (durable via the new broker default, no explicit flag); durable.yml deleted. docker compose config validates." |
| 0029-T3 | ✅ done | 2026-06-24 | "README: env table shows durable on-by-default + opt-out + a MQTTD_DATA_DIR row; feature bullet and demo section reframed as durable-by-default; removed the durable.yml overlay instructions." |
<!-- /status-table:0029 -->

## Changelog

- **2026-06-24** — ADR accepted: with formation churn fixed (ADR 0028) and steady state proven,
  durable becomes the default for the broker and the demo.
