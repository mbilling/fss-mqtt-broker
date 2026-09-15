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
`mqttd_publish_forwarded_total / mqttd_publish_received_total`, per broker and
in aggregate. But mqttd omits a labelled family with no children entirely — no
sample, no `# TYPE` line — so on a healthy prefer-local arm the forwarded family
may never appear, and "absent" cannot be told from "not exported" or "lost to a
restart". A zero crossing counts only under a certificate (see the crossing gate
below). Missing files, truncated scrapes, per-broker counter resets and
uncertified rungs make crossing **UNKNOWN/INVALID**, not zero.

Do **not** set `MQTTD_SHARED_PREFER_LOCAL=0`. That would be a different experiment.

## Quota

15 servers / 100 dedicated vCPUs: 7×CCX23 + 5×CCX33 = 7×4 + 5×8 = 68 vCPU at
the larger size (N=5 is 5×4 + 5×8 = 60). Fits the 100-vCPU cap without shrinking
the driver fleet. Confirm the dedicated Hetzner project is empty before start;
audit to 0 after.

## Missing before we run again

The 2026-09-14 operator pass completed **N=7 only**. Destroy then failed; N=5
never ran. The 2026-09-15 pair (below) ran with this list green except where an
item says otherwise.

### Ops / harness

- [x] **Fix/prove destroy with `SSH_KEY`.** OpenTofu evaluates
      `file(pathexpand(var.ssh_public_key_path))` on destroy. Apply already
      passed `${SSH_KEY}.pub` when set; destroy (between sizes, EXIT trap,
      `teardown.sh`) used to omit it and fall back to `~/.ssh/id_ed25519.pub`.
      If that file is missing, destroy fails and servers keep billing. Prove
      teardown **before** chaining 7+5: `SSH_KEY` set, `.pub` present, one
      apply→destroy (smoke is enough). Then confirm the project is empty.
      *2026-09-15: proved end to end with the default key (N=1 smoke, the
      between-size destroy and the final destroy, each audited to zero); the
      non-default `SSH_KEY` path is covered by `test-cloud.py` only.*
- [x] **Observe on the Mac/laptop**, not a remote orchestrator without Docker.
      `observe.sh attach` starts the compose stack on *this* machine
      (Grafana :3000 + Alloy). The 2026-09-14 operator host had no Docker; the
      laptop already had the stack. Attach fails and the run continues
      unobserved. Set `OBSERVE=1` only where Grafana/Alloy live; `OBSERVE=0`
      only for a purity publish. *2026-09-15: a fresh laptop also lacked the
      compose stack's external `observe_prom-data` volume, so N=7 attached late
      by hand; `observe.sh attach` now creates it.*
- [x] Confirm Hetzner project empty before start; label-audit to 0 after
      (`./teardown.sh`, `--force` only in the dedicated project). A missing public
      key must not prevent Hetzner's explicitly requested label recovery after
      state-backed destroy fails; provisioning still refuses a missing `.pub`.

### Binary / Option B pin

- [x] Prefer a **pinned current main** candidate (`MQTTD_URL` + `MQTTD_SHA256`,
      `BENCH_GIT_REF` one commit for both arms) including at least
      #598 / #612 / #526 / #540 — not only `v1.0.16`. The env file still
      defaults to 1.0.16 so a rerun is explicit about the override.
- [x] Same SHA + `DRIVER_COUNT=5` + this Option B env on **both** arms.

### Validity gates (abort the compare if any fail)

- [x] **Crossing gate, both arms.** Pass means BOTH of:
      1. `results/nodes=$N/laneE/forward-canary.txt` starts `status=pass nodes=$N` (the
         forwarding positive control `run-curve.sh` runs before calibration and
         any rung; a failed control stops the size with its evidence kept);
      2. `python3 extract-lane-e.py --crossing-gate 0.5 .runs/<stamp>/results`
         exits 0 — one `GATE nodes=N PASS` line per size.

      The gate re-derives the control through `forward-canary.py`'s own ledger
      (never the status line alone) and fails a size unless every rung its
      `shape.txt` declares, the `-rep2` control included, was run and is valid,
      aligned and certified; every broker
      holds `mqttd_peer_links` = N−1 at both window edges; no broker's window
      received is zero; and **every broker's own** crossing is ≤ 0.5% — an
      aggregate near zero can hide one broker forwarding a real share.
      A rung's certificate is **structural** at N=1 (every snapshot shows
      `mqttd_peer_links 0`: nowhere to forward) or **canary** at N≥2: the size's
      control passed, every broker snapshot (before, window-open, window-close,
      drain, after) still carries at least that broker's canary
      `forwarded{reason="shared-remote"}` and received totals, and the MainPID in
      `window.tsv` is the process that passed the control (a `window.tsv` without
      its `main_pid` column certifies nothing at N≥2, and a rung with no
      `window.tsv` at all reads `cert=canary-unbound`, which fails the gate).
      `run-curve.sh` refuses to start calibration when it cannot read every
      broker's MainPID after the control, and refuses a rung whose broker's MainPID
      changed since. Anything else is INVALID, never zero. `LANE_E_FORWARD_CANARY=0` means no N≥2 arm can pass.
      **The 2026-09-14 N=7 captures cannot be certified**: they predate the
      control and carry no forwarded family, so they establish nothing about
      crossing. (That day's N=1 smoke is certifiable only structurally, which
      says nothing about an N≥2 arm.)
- [ ] Calibration + settled + drained; drivers not pinned at the top rung.
      *2026-09-15: calibration, settle and drain held on every rung and no driver
      was CPU-pinned, but two rungs flag PUBLISHERS LATE (see below), so this
      item is not green for those rungs.*
- [x] Trust **broker/drain totals**, not summed emqtt-bench `pub … rate=` log
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

- [x] `mqttd_hub_dispatch_seconds_{sum,count}` **by `command`**
      (`publish` / `cluster` / `sweep` / …) per rung — mean µs =
      Δsum/Δcount.
- [x] Peer in-flight peaks / drop reasons / sessions after drain.
- [x] Side-by-side N=5 vs N=7 table at matched site rungs (offered, received,
      delivered, crossing %, per-node delivered, broker idle, hub publish µs,
      driver idle) **before** claiming membership cost.

## Offline preflight

No token, no OpenTofu:

```sh
cd bench/scale
set -a && . ./482-constant-driver-N5-N7-optionB.env && set +a
PREFLIGHT_ONLY=1 ./run.sh full 7 5   # also runs forward-canary.py and extract-lane-e.py --self-test
# the positive control against the pinned binary on this machine, at both arms' sizes:
python3 forward-canary.py local-proof --mqttd <candidate mqttd, sha256 checked> --nodes 7
python3 forward-canary.py local-proof --mqttd <candidate mqttd, sha256 checked> --nodes 5
# and the shape tables:
jq -n '{brokers: [range(7) | {vcpus:4}], drivers: [range(5) | {vcpus:8}]}' > /tmp/inv7.json
LANE_E_PIN_SITES=0 LANE_E_SUBS_PER_SITE=7 LANE_E_SITES_OVERRIDE="1 2 4 8 10" \
  LANES=E SHAPE_ONLY=1 ./run-curve.sh /tmp/shape-e7 /tmp/inv7.json
```

`local-proof` must print `local-proof: PASS`: the canary's ledger passes on N real
local processes, a zero-crossing rung written in `run-curve.sh`'s layout gets
`GATE nodes=N PASS … cert=canary` from `extract-lane-e.py --crossing-gate 0.5`,
and after a SIGKILL restart of one broker the same rung is INVALID. The
`shape.txt` for each size must show `forward canary: ON`.

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
| broker received | Δ `mqttd_publish_received_total` window-open→window-close (`win_recv`, rate); before→drain (`life_recv`) for lifetime delivery accounting |
| drain recv | subscriber `.drain` totals; must match broker `life_recv` / `life_deliv` within the usual slack |
| crossing | Δ forwarded (all reasons) / Δ received over the aligned window, aggregate and `max_broker`; `cert` must be `structural` (N=1) or `canary` (N≥2), else INVALID |
| hub dispatch | Δ `mqttd_hub_dispatch_seconds_{sum,count}` **by `command`** over the aligned window |
| peer in-flight / drops | `mqttd_peer_forwards_in_flight` at drain/after; `mqttd_publish_dropped_total` by reason, `win_drops` and `life_drops` |
| sessions after drain | `mqttd_sessions` / `mqttd_connections_active` at drain/after |
| broker / driver idle | `cpu/cpu-{broker,driver}*.txt` (`mpstat` `%idle` on `all`), rows inside that host's `window.tsv` stamps: mean / lowest host / lowest 1 s row; `[n/m hosts]` when a host has no usable window row or samples; `*` when `cpu_window` is not `aligned` |
| window quality | `bracket_ms` (widest scrape; `!` past 2% of the window), `rep` / `control` |
| settled + drained | `rung.txt` |

```sh
python3 extract-lane-e.py --crossing-gate 0.5 .runs/<stamp>/results
```

## How to read outcomes

- **Crossing gate fail** (a broker's forwards above 0.5%, or crossing not
  certified): stop. The arm is not demonstrably Option B. Do not interpret
  capacity vs N.
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

## 2026-09-15 pair (N=7 then N=5) — diagnostic, not a curve point

Unreleased candidate: `main` 5761b1e built for x86_64 musl (zig cc as the C
compiler, so not byte-identical to a CI build), sha256
`0f517c94758cff41e820c72c34f221f0033097acfe10aa4eafda3d2c604e96de`, fetched from
the `bench-candidate-5761b1e` prerelease. That prerelease was removed once v1.0.17
shipped the same source signed; the hash above is what identifies the binary, and
rebuilding it means `scripts/release/build-repro.sh x86_64-unknown-linux-musl mqttd`
at 5761b1e with `CC_MUSL` pointing at `zig cc -target x86_64-linux-musl
-fno-sanitize=all` (zig 0.16.0). Harness 5a1ad0f as recorded in the run's
`provenance.txt`, clean tree, both arms; that commit landed on `main` as 8424d22
after a rebase, with an identical `bench/scale` tree
(`4b571f82ff40607e0d7052156b2fa3f6d5867e9c`). Option B env as above, D=5 CCX33, CCX23 brokers, `fsn1`, OBSERVE=1 from the
operator laptop. Raw results: `.runs/20260914T233336Z` (untracked).

**Gates.** The forwarding control passed on both arms (`status=pass`: 600
shared-remote forwards per broker at N=7, 400 at N=5, exact; residue gone in 2 s).
`extract-lane-e.py --crossing-gate 0.5` exits 0: `GATE nodes=7 PASS 6 rungs,
cert=canary` and `GATE nodes=5 PASS 6 rungs, cert=canary`, max broker crossing
**0.00%** on every rung of both arms. Every rung settled, drained, dropped
nothing and left no peer in-flight frames; broker delivered equals broker
received over every rung's lifetime, and the consumers' post-drain receipts match
it exactly except 6 messages at N=5 10 sites (the summarizer's dup column). Calibration met 15 000/s with no late publishes on
both arms.

| N | sites | offered/s | window recv/s | per node/s | crossing | p99 | broker idle mean / busiest host | driver idle mean / busiest host | hub publish µs | hub cluster µs | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 7 | 1 | 30 000 | 29 999 | 4 286 | 0.00% | ≤1 ms | 93 / 82% | 95 / 74% | 8.3 | 13.2 | pass |
| 5 | 1 | 30 000 | 30 000 | 6 000 | 0.00% | ≤1 ms | 88 / 79% | 97 / 85% | 6.8 | 14.5 | pass |
| 7 | 2 | 60 000 | 60 000 | 8 571 | 0.00% | ≤1 ms | 88 / 75% | 93 / 75% | 7.9 | 15.9 | pass |
| 5 | 2 | 60 000 | 59 999 | 12 000 | 0.00% | ≤1 ms | 84 / 65% | 96 / 88% | 8.0 | 14.2 | pass |
| 7 | 4 | 120 000 | 120 000 | 17 143 | 0.00% | ≤1 ms | n/a¹ | n/a¹ | 5.8 | 20.9 | pass |
| 5 | 4 | 120 000 | 119 999 | 24 000 | 0.00% | ≤1 ms | 74 / 70% | 89 / 82% | 5.3 | 15.5 | pass |
| 7 | 8 | 240 000 | 240 080 | 34 297 | 0.00% | ≤25 ms | 67 / 56% | 76 / 62% | 5.1 | 44.9 | PUBLISHERS LATE (6%) |
| 5 | 8 | 240 000 | 240 004 | 48 001 | 0.00% | ≤50 ms | 53 / 42% | 72 / 58% | 6.7 | 29.2 | pass |
| 7 | 10 | 300 000 | 300 065 | 42 866 | 0.00% | ≤25 ms | 60 / 56% | 70 / 60% | 5.4 | 63.9 | pass |
| 5 | 10 | 300 000 | 299 719 | 59 944 | 0.00% | ≤500 ms | 44 / 27% | 67 / 58% | 7.7 | 52.5 | PUBLISHERS LATE (8%) |
| 7 | 1 (control) | 30 000 | 30 000 | 4 286 | 0.00% | ≤1 ms | 93 / 84% | 95 / 74% | 7.6 | 12.7 | pass |
| 5 | 1 (control) | 30 000 | 30 000 | 6 000 | 0.00% | ≤1 ms | 94 / 85% | 95 / 76% | 7.3 | 12.0 | pass |

¹ The N=7 4-site rung lost its CPU samplers: two of twelve sampler ssh
connections from the laptop got `Network is unreachable` at window open
(`cpu_window=missing`); broker, consumer and crossing evidence is unaffected.

**Reading, under "How to read outcomes".**

- Crossing gate: passed. This is a certified Option B pair, which the
  2026-09-14 run was not.
- Seven nodes do **not** receive less than five at any total offer where both
  rungs are valid (1, 2, 4 sites, and both controls): both deliver the offer.
  No regression candidate.
- The matched-total-load per-node fall (6 000 → 4 286 at 1 site, 59 944 →
  42 866 at 10) is the arithmetic the card warns about, not membership cost.
- Neither arm has a valid top-rung pair. N=5 at 300 000/s is flagged
  PUBLISHERS LATE with p99 ≤500 ms while its busiest broker was 27% idle (22%
  lowest second) and its busiest driver 58% idle; N=7 at 240 000/s is flagged
  with p99 ≤25 ms and a later, heavier rung that passed. emqtt-bench's late
  counter cannot separate driver scheduling from TCP backpressure, so neither
  flag is attributed to the brokers. The N=5 latency rise at 10 sites is
  consistent with approaching a per-node limit near 60 000/s on CCX23, and that
  is a hypothesis, not a measurement.
- N=7 at 300 000/s passes with ≥56% idle on every broker: a lower bound, not a
  capacity.
- The mean `cluster` hub dispatch grows with load on both arms and is higher at
  N=7 at the top two rungs (44.9 / 63.9 µs vs 29.2 / 52.5 µs); `publish`
  stays at 5–8 µs on both. That is an input for #613, not a membership-cost
  result.

The membership-cost claim stays open. Settling it still needs equal per-node
useful work, SLO knees with independently adequate endpoints, and repeats.

## 2026-09-14 path-prove (N=7 only) — not a curve point

Operator machine was **not** the Grafana laptop; `SSH_KEY` pointed at a
non-default key.

- MQTTD **v1.0.16**, Option B env as above, D=5, sites 1 2 4 8 10.
- N=7 Lane E **completed cleanly**: every rung settled+drained through 300k
  msg/s offered; broker `publish_received` matched drain recv; hub dispatch
  mean ~5–8 µs; drivers still ~65–70% idle at the top; no knee.
- `mqttd_publish_forwarded_*` was reported absent while `publish_received`
  was large. That zero crossing **cannot be certified**: the run predates the
  forwarding positive control, and an absent family is exactly what a broken or
  unexported counter looks like. Only a rerun under the crossing gate settles it.
- Destroy between sizes **failed** (`ssh_public_key_path` defaulted to a
  missing `~/.ssh/id_ed25519.pub`). Left 12 servers billing until manual
  recover. **N=5 never ran.** A/B incomplete.
- Observe attach failed on the operator host (no local Docker).

That is a clean climb at N=7 under Option B on v1.0.16, and nothing more.
