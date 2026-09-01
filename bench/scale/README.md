# bench/scale — the multi-host scaling curve rig (ADR 0048 T3)

Provisions real Hetzner Cloud clusters of 1, 3 and 5 broker hosts (one broker,
one local NVMe disk per host — ADR 0048 §2's hardware rule), drives the same
workload against each size from separate load-generator hosts, and collects the
raw results that become `docs/benchmarks/SCALE-CURVE.md`. Runs from a laptop,
**never from CI** — no cloud credential exists in this repository or its
workflows.

## Prerequisites (once)

1. **A dedicated Hetzner Cloud project** (e.g. `mqttd-bench`). Dedicated so the
   label-scoped leak sweeper (`teardown.sh --force`) can never touch anything
   that is not this rig's. The default 10-server limit fits (max 5 brokers + 3
   drivers).
2. **A Read & Write API token** for that project → `export HCLOUD_TOKEN=...` in
   the shell that runs the rig. Never committed, never a CI secret.
3. **An SSH keypair.** The default is `~/.ssh/id_ed25519`; for any other key
   (say `~/.ssh/hetzner`), `export SSH_KEY=~/.ssh/hetzner` — the rig injects
   `${SSH_KEY}.pub` into every host via cloud-init and dials with that identity.
   The key is never registered with the hcloud API, so it may freely also exist
   in the project's console inventory. (Because no project key is attached to
   the servers, Hetzner emails a root password per server — ignore it; the
   cloud-init key is already in place.)
4. Laptop tools: `terraform` ≥ 1.7 (or OpenTofu), `jq`, and — on macOS —
   `openssl@3` (`brew install openssl@3`; the system LibreSSL cannot mint the
   cluster PKI and `deploy/systemd/gen-certs.sh` refuses it loudly). The
   `hcloud` CLI is optional but lets `teardown.sh` audit by label.
5. Sanity-check current CCX23/CCX33 pricing and availability in `fsn1`
   (fallbacks: `nbg1`, `hel1` via the `location` variable).
6. **Dedicated-core limit.** A fresh project's default cap (~16 dedicated
   cores) fits `smoke` (1×CCX23 + 1×CCX33 = 12) but NOT the full curve —
   5×CCX23 + 2×CCX33 = **36 cores** at the 5-node point. Before `full`,
   request a limit increase in the console (Limits → dedicated vCPUs, ≥40);
   a run that trips the cap fails at `terraform apply` with
   `dedicated core limit exceeded` and tears itself down, costing cents.

## Cost

| run | servers | ≈ wall time | ≈ cost |
|---|---|---|---|
| `smoke` | 1×CCX23 + 1×CCX33 | 20–30 min | <€0.50 |
| `standard` (10 only) | 10×CCX23 + 4×CCX33, one pass | 25–35 min | €1.50–2 |
| `full` (1+3+5) | up to 5×CCX23 + 2×CCX33, one size at a time | 4–5 h | €1.50–2 |
| `full` (1+3+5+7+10) | up to 10×CCX23 + 6×CCX33, one size at a time | 12–14 h | €33–40 |
| forgotten 5-node stack | — | per day | ≈€8.50 |

**Which profile.** `standard` is the release **regression gate**: one 10-node
cluster — the top of our range, where a regression shows first — measured
once. Lane A sat + lat at 1 rep, lane B at 50k / 150k / 300k in the plain
posture, and nothing else: no lane C (50k idle connections cost ~4 minutes to
establish and answer a question no durable-path change moves), no ADR 0072
tier arms, no mTLS reference rung, no past-the-voter-cap variant. `LANES=A
./run.sh standard` narrows it further to the durable path alone.

What a standard run measures is identical to what `full` measures — same code,
same shapes, same machine types. What it gives up is **confidence** (one rep:
a point estimate, no median, no spread) and **coverage** (one size, one
posture, no ladder tail). That makes it a gate, not a publishable curve point:
numbers that go into `docs/benchmarks/SCALE-CURVE.md` come from `full`, which
is also what a release touching the durable path should run.

Budget **€10** and several standard runs plus the worst case are covered. The last line is why teardown is
trapped on EXIT/INT/TERM, why `teardown.sh` exists separately, and why step 1
says *dedicated project*: after any run, `hcloud server list` (or the console)
must show zero servers.

## Running

```sh
cd bench/scale
export HCLOUD_TOKEN=...
export MQTTD_VERSION=1.0.6   # the release under test — required, recorded in the run
./run.sh smoke     # proves token → apply → PKI → bring-up → lanes → destroy
./run.sh standard  # the release gate: one 10-node cluster, ~30 min, ~€2
./run.sh full      # the curve; or ./run.sh full 3 5 for a subset
python3 summarize-curve.py .runs/<stamp>/results   # markdown for the doc
```

Each size is applied **fresh and destroyed** before the next — a grown cluster
keeps replica groups tracked under an earlier membership and never re-greens,
so growing 1→3→5 would measure a known-degraded configuration. `RESUME`: a
re-run with `RUN_DIR=.runs/<stamp>` skips sizes already marked done.
`KEEP_INFRA=1` skips teardown for debugging and prints a red reminder that the
meter is running.

**Live dashboards are ON.** Each size attaches Grafana on `localhost:3000`, fed by
an Alloy scraper on driver-1 that reads every broker's `/metrics` over the private
network every 2s and reverse-tunnels it to the laptop (`observe.sh`). Nothing runs
on the broker hosts, and the scrape is a plain HTTP GET: the expensive gauges (the
per-session sums for subscriptions, inflight and backlog bytes) are recomputed on
the hub's own sweep tick, not per scrape. If the local stack cannot start the run
continues unobserved with a warning. `OBSERVE=0` opts out — worth doing for a run
whose numbers are published, if you want the measured path provably untouched. Ctrl-C is safe (trapped); `kill -9` is not — after one, run
`./teardown.sh`.

**Checking a shape without paying.** Lane B's populations must split exactly
over their containers (`drivers × LANE_B_PUB_CONTAINERS`, `drivers ×
LANE_B_SUB_CONTAINERS`), each container's clients exactly over the brokers it
spans, and every rung must need an integer `-I` of at least `LANE_B_MIN_INTERVAL` ms — `run-curve.sh` refuses
anything else before its first ssh, so a wrong shape costs the provisioning
minutes rather than lane A's hour (a floored split used to run fewer clients
than the doc says; a floored timer used to offer a rate other than the label).
Lane D is held to the same discipline and refused in the same place: its
sessions must split exactly over their containers *and* over the brokers, its
offered rate must need an integer `-I`, its session expiry must outlast the
whole offline+drain cycle, and QoS 0 is rejected outright (an offline session
queues nothing at QoS 0, so the lane would measure a guaranteed zero).

`SHAPE_ONLY=1` runs only that check, so a knob set can be tried offline against
a synthesized inventory:

```sh
jq -n '{brokers: [range(10) | {}], drivers: [range(5) | {}]}' > /tmp/inv.json
LANE_B_PUB_CONTAINERS=6 SHAPE_ONLY=1 ./run-curve.sh /tmp/shape-check /tmp/inv.json
```

The tables it prints are what a real run keeps as `laneB/shape.txt` and
`laneD/shape.txt`.

**The rung can be a tenant, not a rate.** Lanes A–D all vary one dimension
against a fixed population. Lane E's rung is a **site**: it scales publishers,
offered rate and consumers together, each site landing in its own `$share`
group on `site/<n>/#`, which is how an estate actually grows. Its ladder is
checked *per rung* rather than once, because a shape can be valid at 1 site and
impossible at 8 — and the binding constraint is usually the harness: sites are
dealt round-robin to drivers at three one-vCPU containers each, so 16 sites
needs 12 containers on the busiest of 5 drivers and is **refused**. Reaching 16
needs `DRIVER_COUNT=8`, which `driver_count` permits (it caps at 8, and
`quota.tf` refuses any broker/driver combination that would exceed the project's
vCPU quota part way through an apply). That bound belongs to the load
generators, not the broker, and the shape check prints it rather than letting a
driver shortfall read as a broker limit.

**Fan-out percentage.** `LANE_B_FANOUT` (0–100) is the share of subscriber containers that receive the *full* publish stream on a plain `bench/#` wildcard; the rest stay in one `$share/g1` group and split one copy per publish. `0` (default) is today's `$share` fan-in (delivered ≈ offered); `100` gives every subscriber every publish (delivered ≈ offered × `LANE_B_SUBS`); `50` makes half the containers wildcard. Egress fan-out is the shape that stresses the per-connection write path (issue #443). Above 0 the rung is the *publish* rate, so keep it modest — `shape.txt` prints the resulting `delivered ≈ offered × N`.

## Anatomy of one size (what `full` executes, in order)

| step | what | knobs (full profile) |
|---|---|---|
| provision | terraform apply, cloud-init (checksum-verified release binary), private-net mesh gate, founder-first bring-up + arm, full-membership gate | one clean+reboot retry per host |
| barrier probes | `device_barrier_floor` + `store_append_floor` on EVERY broker's data-dir volume — gates Curve 1 | 150 ops |
| lane A `sat` | durable QoS1 closed loop, spread ownership — the headline row (+ qos2 and clean-session arms inside) | 48 pubs × window 8, 48 subs, 3 reps × 60 s |
| lane A `lat` | uncontended ack RTT | N pubs × window 1, 3 reps |
| lane A `tier-local` / `tier-relaxed` | same shape per ADR 0072 tier (`MQTTD_ALLOW_RELAXED_PUBLISH=1`) | sat + lat each |
| lane A voters variant (N>5 only) | wipe + fresh formation with `MQTTD_LEASE_VOTERS=N` — prices the committee against the ADR 0073 default | sat + lat |
| re-form (clean mode) | durable plane OFF for the routing-bound lanes | — |
| lane B | non-durable `$share` fan-out ladder (+ one mTLS rung) | 12 000 pubs / 300 subs in 5 + 3 one-vCPU containers per driver, 300k…20k offered (top rung first), 60 s/rung |
| lane C | idle-connection scaling, plaintext + mTLS | 50k conns, ramp 2.5k/s, 120 s hold |
| lane D — **opt-in, not in the default `full`** (`LANES` defaults to `ABC`; the `logistics` workload selects it) | store-and-forward: persistent sessions attach, detach, are published to while offline, then resume and drain | 1920 sessions, 4000 msg/s for 40 s offline, 180 s drain budget |
| lane E — **opt-in** (`LANES` defaults to `ABC`; the `scada-sites` workload selects it) | scale-out by tenant: each rung adds whole SITES — publishers, rate and consumers together — each with its own `$share` group | ladder 1·2·4·8 sites × 30 000 msg/s, 1200 pubs + 6 consumers per site |
| teardown | destroy before the next size (fresh clusters only), host logs collected on failure | — |

`smoke` runs the same pipeline with every knob shrunk (1 rep, 15 s, 2k conns) —
it proves the rig end to end for cents and its numbers are never published.

## What runs where

- **This machine:** Terraform; PKI minting (`deploy/systemd/gen-certs.sh` — the
  CA keys never leave); orchestration over SSH; result collection.
- **Broker hosts (CCX23):** the pinned, signed release binary, checksum-verified
  by cloud-init, under the shipped `deploy/systemd/mqttd.service` plus a
  disclosed drop-in (memory ceiling, fd limits); kernel tuning in
  `terraform/files/sysctl-broker.conf`. The founder-first start + arm sequence
  is `scripts/deploy-smoke.sh`'s, executed by `bootstrap-cluster.sh`.
- **Driver hosts (CCX33):** emqtt-bench 0.6.3 in docker (lanes B/C); driver-1
  builds `crates/mqttd/tests/durable_bench.rs` from a pinned ref for lane A and
  serves the binary to the brokers for the per-host fsync **barrier probes**
  that gate Curve 1.

**Measuring a binary that has not shipped.** By default the brokers run the
published, signed release for `MQTTD_VERSION` — that is what makes a published
number attributable. It also meant a performance change had to be *released in
order to be measured*, which inverts the normal order: a change should earn its
release, not be released to earn evidence. (#445's +29% was measured v1.0.8 →
v1.0.9 for exactly this reason.)

`MQTTD_URL` overrides the source:

```sh
MQTTD_VERSION=1.0.13 \
MQTTD_URL=https://…/mqttd-candidate-x86_64-unknown-linux-musl \
MQTTD_SHA256=$(sha256sum mqttd-candidate | cut -d' ' -f1) \
  ./run.sh standard
```

Two things are enforced, not advisory:

- **`MQTTD_SHA256` is mandatory.** The release path can fall back to the
  `.sha256` published beside the artifact; an arbitrary URL has no companion, so
  without a hash there would be no verification at all. `run.sh` refuses the
  combination before anything is provisioned.
- **The run is stamped.** `UNRELEASED-BINARY.txt` is written into the run
  directory recording the URL, the hash and the nominal version. A results
  directory read a week later cannot otherwise tell you which binary produced
  it, and a number from an unreleased build is **not** a published-curve point.

**The bench driver is pinned to the release under test.** Lane A's `durable_bench`
is built from `BENCH_GIT_REF`, which now defaults to the tag matching
`MQTTD_VERSION` — it used to default to `main`, and `main` moves. Two lane A runs
days apart therefore measured different *harnesses* as well as different brokers:
on 2026-08-28 the same broker (v1.0.9), same size and same shape gave 20 394 vs
25 292 msg/s depending only on which `main` the driver came from, non-overlapping
across three reps each. That 24% is larger than most effects this rig is used to
detect.

For a **cross-version comparison, pin `BENCH_GIT_REF` explicitly** to one commit
for every arm. Otherwise the harness varies with the broker and any difference is
unattributable.

Lane definitions, the ladder, and every validity rule live in `run-curve.sh`
and are documented in `docs/benchmarks/SCALE-CURVE.md` — the method is fixed
before the first paid run.

## Honesty notes

- Raw results under `.runs/` are untracked scratch; the published record is
  `docs/benchmarks/SCALE-CURVE.md`, and it may cite only tracked paths
  (`scripts/check-readme-facts.py` enforces this).
- A smoke run's numbers prove the pipeline, never the broker.
- Known blocker for the durable curve at N>1: issue #358 (spread-ownership
  durable acks stall), found by this rig's harness before any money was spent.
