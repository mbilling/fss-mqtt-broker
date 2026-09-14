# #482 constant-driver A/B — Option B (unpinned, crossing ≈ 0%)

Campaign card for a same-day N=5 vs N=7 Lane E pair with the **driver fleet held
constant**. Diagnostic only: do not copy one-off numbers into
`docs/benchmarks/SCALE-CURVE.md`. Env: [`482-constant-driver-N5-N7-optionB.env`](482-constant-driver-N5-N7-optionB.env).
Issue: [#482](https://github.com/mbilling/fss-mqtt-broker/issues/482).

## Goal

Hold D=5 CCX33 drivers, CCX23 brokers, site rate 30 000, sites `1 2 4 8 10`, and
ask whether **per-node delivered work still falls with N** when:

- the load-generator fleet does not shrink as brokers are added (the confound
  that voided the v1.0.16 5/7/10 ladders);
- crossing is predicted ≈ 0% on **both** arms, so a remaining gap is not
  “more forwards at N=7”.

Until both sizes complete under one pin, with the crossing gate passing, this
does **not** settle the membership-cost claim.

## Option B rationale

`LANE_E_PIN_SITES=0` (unpinned): every container spans all brokers.
`LANE_E_SUBS_PER_SITE=7` ≥ max(N)=7. Cluster default since #511 is
**prefer-local**, not global `$share` round-robin. With C ≥ N every publishing
node has a local shared member, so predicted crossing is
`(N − min(C,N))/N` ≈ 0% at N=5 and N=7.

The shape text used to claim round-robin ⇒ `(N-1)/N` remote whenever sites were
unpinned. That is stale. The **metric** is authoritative:
`mqttd_publish_forwarded_total / mqttd_publish_received_total`. Lazily absent
forwarded samples mean zero only after all expected broker scrapes are complete,
valid, and declare/support this counter. Missing files, unsupported metrics and
per-broker counter resets make crossing **UNKNOWN/INVALID**, not zero.

Do **not** set `MQTTD_SHARED_PREFER_LOCAL=0`. That would be a different experiment.

## Quota

15 servers / 100 dedicated vCPUs: 7×CCX23 + 5×CCX33 = 7×4 + 5×8 = 68 vCPU at
the larger size (N=5 is 5×4 + 5×8 = 60). Fits the 100-vCPU cap without shrinking
the driver fleet. Confirm the dedicated Hetzner project is empty before start;
audit to 0 after.

## Missing before we run again

The 2026-09-14 operator pass completed **N=7 only**. Destroy then failed; N=5
never ran. Do not spend another fleet hour until this list is green.

### Ops / harness

- [ ] **Fix/prove destroy with `SSH_KEY`.** OpenTofu evaluates
      `file(pathexpand(var.ssh_public_key_path))` on destroy. Apply already
      passed `${SSH_KEY}.pub` when set; destroy (between sizes, EXIT trap,
      `teardown.sh`) used to omit it and fall back to `~/.ssh/id_ed25519.pub`.
      If that file is missing, destroy fails and servers keep billing. Prove
      teardown **before** chaining 7+5: `SSH_KEY` set, `.pub` present, one
      apply→destroy (smoke is enough). Then confirm the project is empty.
- [ ] **Observe on the Mac/laptop**, not a remote orchestrator without Docker.
      `observe.sh attach` starts the compose stack on *this* machine
      (Grafana :3000 + Alloy). The 2026-09-14 operator host had no Docker; the
      laptop already had the stack. Attach fails and the run continues
      unobserved. Set `OBSERVE=1` only where Grafana/Alloy live; `OBSERVE=0`
      only for a purity publish.
- [ ] Confirm Hetzner project empty before start; label-audit to 0 after
      (`./teardown.sh`, `--force` only in the dedicated project). A missing public
      key must not prevent Hetzner's explicitly requested label recovery after
      state-backed destroy fails; provisioning still refuses a missing `.pub`.

### Binary / Option B pin

- [ ] Prefer a **pinned current main** candidate (`MQTTD_URL` + `MQTTD_SHA256`,
      `BENCH_GIT_REF` one commit for both arms) including at least
      #598 / #612 / #526 / #540 — not only `v1.0.16`. The env file still
      defaults to 1.0.16 so a rerun is explicit about the override.
- [ ] Same SHA + `DRIVER_COUNT=5` + this Option B env on **both** arms.

### Validity gates (abort the compare if any fail)

- [ ] **Crossing ≈ 0%** both arms via `mqttd_publish_forwarded_total` /
      `mqttd_publish_received_total`, with complete validated per-broker snapshots
      and no detected resets. The reported absent N=7 forwarded samples alone do
      **not** establish this gate; revalidate the original raw captures.
- [ ] Calibration + settled + drained; drivers not pinned at the top rung.
- [ ] Trust **broker/drain totals**, not summed emqtt-bench `pub … rate=` log
      lines across containers (that over-reports). `extract-lane-e.py` uses
      broker snapshots; `before`/`after` totals include ramp/drain and are for
      delivery accounting only.
- [x] **Aligned windows before a capacity claim.** Each rung now scrapes every
      broker's `/metrics` and every consumer's histogram in ONE parallel batch at
      both edges of the steady window (`metrics-window-{open,close}-broker*.prom`,
      `sub-*-base.prom` / `sub-*.prom`), and each host stamps its own scrape into
      `window.tsv`. The CPU samplers are bounded by that window (`cpu.sh`), not
      by a SETTLE+SECS timer, and the extractor keeps only each host's mpstat rows
      between its own stamps. `rung.txt` carries `window=aligned` and
      `cpu_window=aligned|incomplete|missing`. A rung without these files reports
      `UNALIGNED`; a partial window is INVALID, never a lifetime fallback. This
      still does not certify concurrent publisher/consumer headroom.

### Diagnosis extracts we still need on both arms

- [ ] `mqttd_hub_dispatch_seconds_{sum,count}` **by `command`**
      (`publish` / `cluster` / `sweep` / …) per rung — mean µs =
      Δsum/Δcount.
- [ ] Peer in-flight peaks / drop reasons / sessions after drain.
- [ ] Side-by-side N=5 vs N=7 table at matched site rungs (offered, received,
      delivered, crossing %, per-node delivered, broker idle, hub publish µs,
      driver idle) **before** claiming membership cost.

## Offline preflight

No token, no OpenTofu:

```sh
cd bench/scale
set -a && . ./482-constant-driver-N5-N7-optionB.env && set +a
PREFLIGHT_ONLY=1 ./run.sh full 7 5
# and the shape tables:
jq -n '{brokers: [range(7) | {vcpus:4}], drivers: [range(5) | {vcpus:8}]}' > /tmp/inv7.json
LANE_E_PIN_SITES=0 LANE_E_SUBS_PER_SITE=7 LANE_E_SITES_OVERRIDE="1 2 4 8 10" \
  LANES=E SHAPE_ONLY=1 ./run-curve.sh /tmp/shape-e7 /tmp/inv7.json
```

Refuse any 12/17-site ladder with D=5: 3 containers/site on CCX33 ⇒
`ceil(sites/5)*3 ≤ 8` ⇒ sites ≤ 10.

## Run order

Prefer **prove destroy before chaining 7+5**. A failed between-size destroy is
how 12 servers billed after N=7.

```sh
export HCLOUD_TOKEN=...          # shell only, never this file
export SSH_KEY=~/.ssh/hetzner    # if not the default id_ed25519; .pub must exist
cd bench/scale
set -a && . ./482-constant-driver-N5-N7-optionB.env && set +a
# Prefer MQTTD_URL+SHA256 for current main; pin BENCH_GIT_REF to one commit.

# 1. Prove apply→destroy with this SSH_KEY and binary pin on a SMALL shape.
#    This helper overrides the sourced fleet/ladder: one CPX32 + one CPX42,
#    one site at 1,000/s. It uses a fresh run dir, OBSERVE=0, KEEP_INFRA=0.
#    Check current prices/budget; then confirm the dedicated project is empty.
bash ./482-smoke.sh

# 2. Real pair. N=7 first (already path-proven 2026-09-14) then N=5.
#    KEEP_INFRA is unset/0 — tear down between sizes.
./run.sh full 7 5
```

If destroy still fails: `CLOUD=hcloud SSH_KEY=… ./teardown.sh` (same `.pub`),
then `./teardown.sh --force` only inside the dedicated project.

## Metrics checklist

Per size, per rung (`results/nodes=$N/laneE/sites-*`):

| check | source |
|---|---|
| offered | `rung.txt` (`offered=`) |
| broker received | Δ `mqttd_publish_received_total` window-open→window-close (rate); before→after for lifetime delivery accounting |
| drain recv | subscriber `.drain` totals; must match broker received within the usual slack |
| crossing | Δ forwarded / Δ received over the aligned window, only with complete supported snapshots and no detected resets; otherwise INVALID |
| hub dispatch | Δ `mqttd_hub_dispatch_seconds_{sum,count}` **by `command`** over the aligned window |
| peer in-flight / drops | `mqttd_peer_forwards_in_flight` at drain/after; `mqttd_publish_dropped_total` by reason |
| sessions after drain | `mqttd_sessions` / `mqttd_connections_active` at drain/after |
| broker / driver idle | `cpu/cpu-{broker,driver}*.txt` (`mpstat` `%idle` on `all`), rows inside that host's `window.tsv` stamps |
| settled + drained | `rung.txt` |

```sh
python3 extract-lane-e.py .runs/<stamp>/results
```

## How to read outcomes

- **Crossing gate fail** (measured forwards not ≈ 0%): stop. The arm is not
  Option B. Do not interpret capacity vs N.
- **Drivers pinned / offer not met / publishers late**: the rung measures the
  harness. Same as every other lane E validity rule.
- **N=7 climbs, N=5 missing**: path-prove only. That is the 2026-09-14 state.
  Incomplete A/B; no membership-cost claim.
- **Both arms sustain the same total offer within the latency/validity gates**:
  this is a matched-total-load regression comparison, not a capacity estimate.
  At 300k/s, average per-node delivered work is necessarily 60k/s at N=5 and
  about 42.9k/s at N=7. That fall is arithmetic, not evidence of membership cost.
- **Both pass the top rung without a knee**: report lower bounds only. Neither
  closing the membership-cost claim nor asserting a capacity ratio is justified.
- **Seven nodes receives less than five at the same valid total offer**: a real
  regression candidate; investigate placement, hot-core/driver load, forwarding,
  latency and recovery before attributing it to membership.
- **Membership-cost/capacity investigation**: additionally compare equal per-node
  useful work and measure sustainable SLO-compliant knees with independently
  adequate endpoints, aligned windows, controls and repeats. The same finite
  total-offer ladder does not by itself perform either experiment.

Do not sum emqtt-bench `pub … rate=` lines across containers and treat the
sum as offered or sent.

## Out of scope

- Publishing this ladder into `SCALE-CURVE.md`.
- 12/17-site rungs at D=5 (container budget).
- N=10 at D=5 (15 servers: 10 brokers + 5 drivers; check the project server
  cap — default 10 servers is too small; vCPU 10×4+5×8=80 would fit 100 if
  the server limit is raised).
- `LANE_E_PIN_SITES=1` (Option A / pinned). Different experiment.
- Forcing `MQTTD_SHARED_PREFER_LOCAL=0`.
- Hub sharding / reopening #447 from a path-prove.
- mqtt-bridge live-queue work.

## Afterward → #482

Post the side-by-side table (or the reason the compare aborted) on #482. Do
not close the issue from this card. The 2026-09-14 pre-rerun checklist is
already on the issue; keep this file in sync if the checklist moves.

## 2026-09-14 path-prove (N=7 only) — not a curve point

Operator machine was **not** the Grafana laptop; `SSH_KEY` pointed at a
non-default key.

- MQTTD **v1.0.16**, Option B env as above, D=5, sites 1 2 4 8 10.
- N=7 Lane E **completed cleanly**: every rung settled+drained through 300k
  msg/s offered; broker `publish_received` matched drain recv; hub dispatch
  mean ~5–8 µs; drivers still ~65–70% idle at the top; no knee.
- `mqttd_publish_forwarded_*` was reported absent while `publish_received`
  was large. The original zero-crossing interpretation is **not certified**:
  complete raw scrape coverage and counter support must be revalidated.
- Destroy between sizes **failed** (`ssh_public_key_path` defaulted to a
  missing `~/.ssh/id_ed25519.pub`). Left 12 servers billing until manual
  recover. **N=5 never ran.** A/B incomplete.
- Observe attach failed on the operator host (no local Docker).

That is a clean climb at N=7 under Option B on v1.0.16, and nothing more.
