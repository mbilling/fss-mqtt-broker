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
  - id: 0029-T4
    title: Refuse durable-on with no data dir unless explicitly opted in (issue #240) — MQTTD_ALLOW_EPHEMERAL_DURABILITY / [durable] allow_ephemeral, enforced at startup, --check-config, and reload; every in-repo runnable path updated
    status: done
    date: 2026-08-14
    evidence: "Config::validate() refuses durable-on with no data_dir unless durable.allow_ephemeral (MQTTD_ALLOW_EPHEMERAL_DURABILITY, presence = on) — so startup, --check-config and the reload gate reject it by construction, plus a belt-and-braces duplicate in runtime_precheck via the same helper. Red-first: bare_defaults_are_refused_naming_both_remedies (check_config.rs), durable_on_with_no_data_dir_refuses_to_start + the_ephemeral_opt_in_boots_and_still_warns (binary_smoke.rs), a_reload_into_ephemeral_durability_is_rejected_and_the_running_config_kept (reload.rs), durable_on_without_a_data_dir_refuses_naming_both_remedies + each_remedy_individually_unblocks_validation (mqtt-config). ENV_VARS 73->74. Every in-repo runnable path swept: README blocks (two-minute run gains the data-dir volume), quickstart-smoke/interop/migrate/oidc scripts, CI paho boot, test spawns. ADR gains the As-delivered note reversing the data-dir-not-required alternative."
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
| **0029-T4** Refusal (issue #240) | Durable-on with no data dir is a hard error at startup, `--check-config`, and reload acceptance, naming both remedies (MQTTD_DATA_DIR, or MQTTD_ALLOW_EPHEMERAL_DURABILITY for dev/tests — still loudly WARNed); `MQTTD_DURABLE_SESSIONS=0` needs no flag; no in-repo runnable path trips the refusal by accident. |

## Progress

<!-- status-table:0029 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0029-T1 | ✅ done | 2026-06-24 | "start_hub uses durable_enabled(MQTTD_DURABLE_SESSIONS): unset -> on, 0/false/off/no (case-insensitive) -> off. Unit test durable_is_the_default_and_opts_out_explicitly. main.rs module + start_hub docs updated." |
| 0029-T2 | ✅ done | 2026-06-24 | "docker-compose.yml carries MQTTD_DATA_DIR=/data + per-node d1/d2/d3 volumes in the base (durable via the new broker default, no explicit flag); durable.yml deleted. docker compose config validates." |
| 0029-T3 | ✅ done | 2026-06-24 | "README: env table shows durable on-by-default + opt-out + a MQTTD_DATA_DIR row; feature bullet and demo section reframed as durable-by-default; removed the durable.yml overlay instructions." |
| 0029-T4 | ✅ done | 2026-08-14 | "Config::validate() refuses durable-on with no data_dir unless durable.allow_ephemeral (MQTTD_ALLOW_EPHEMERAL_DURABILITY, presence = on) — so startup, --check-config and the reload gate reject it by construction, plus a belt-and-braces duplicate in runtime_precheck via the same helper. Red-first: bare_defaults_are_refused_naming_both_remedies (check_config.rs), durable_on_with_no_data_dir_refuses_to_start + the_ephemeral_opt_in_boots_and_still_warns (binary_smoke.rs), a_reload_into_ephemeral_durability_is_rejected_and_the_running_config_kept (reload.rs), durable_on_without_a_data_dir_refuses_naming_both_remedies + each_remedy_individually_unblocks_validation (mqtt-config). ENV_VARS 73->74. Every in-repo runnable path swept: README blocks (two-minute run gains the data-dir volume), quickstart-smoke/interop/migrate/oidc scripts, CI paho boot, test spawns. ADR gains the As-delivered note reversing the data-dir-not-required alternative." |
<!-- /status-table:0029 -->

## Changelog

- **2026-06-24** — ADR accepted: with formation churn fixed (ADR 0028) and steady state proven,
  durable becomes the default for the broker and the demo.
- **2026-08-14** — 0029-T4 (issue #240): the ADR's "sane zero-config cluster default" —
  durable-on with RAM-only replicated state — is no longer bootable by accident. The
  refusal lives in `Config::validate()` (one testable home, all three gates by
  construction; belt-and-braces duplicate in `runtime_precheck` via the same helper),
  names both remedies, and the new `[durable] allow_ephemeral` /
  `MQTTD_ALLOW_EPHEMERAL_DURABILITY` opt-in (presence = on, mirroring the bridge's
  ADR 0060 T4 `allow_ephemeral_spool`) keeps dev/test one env var away — still loudly
  warned on every start. The README two-minute `docker run` now mounts a named volume
  and sets `MQTTD_DATA_DIR`, so the first documented experience is genuinely durable;
  every other in-repo runnable path (quickstart/interop/migrate/oidc scripts, CI paho
  boot, test spawns) carries the explicit dev opt-in.
