# Gated membership A/B — did wave B remove an O(peers) per-message term?

A paired before/after of the gated `shared_membership` matrix across the #613
wave A/B change set. **Local mechanism probe on one shared workstation, not a
broker capacity result.** It answers one narrow question — does per-message hub
cost still grow with peer count — and deliberately answers nothing else.

## Answer

**Yes, the term was real, and it is gone.** On the arms where the mechanism
exists, per-message cost grew **+15.2% from 2 to 9 peers** before the change and
**−0.4%** after, replicated across three seeds with no overlap between the two
populations.

**And it is far too small to explain the 5→7 scaling symptom.** +15.2% over
seven added peers is 2.17% per peer, so going from 5 nodes (4 peers) to 7 nodes
(6 peers) removes about **4.3%** of per-message gated hub cost. That is a real
improvement and it is not a cliff. Whatever bends the 5→7 curve, this was not it.

## Method

- `crates/mqttd/benches/shared_membership.rs`, `MEMBERSHIP_GATED=1` — the QoS 1
  gated arm added by item 1.6. Items 1.1 and 1.2 live only on the gated path
  (`routing_unsettled()` is reached from `register_pending`), so the pre-existing
  QoS 0 arms are structurally blind to them.
- **Baseline** = `5761b1e` in a `git worktree` with only the three fixture files
  copied in, so the broker stayed pre-change. Verified genuine: `recompute_mesh`,
  `ack_awaits_settle`, `interest_scratch`, `admit_pending` all absent, and
  `peers_all` still building its `Vec<NodeId>` per call. **Head** = `1e189c5`.
- **Versions interleaved per seed** (head/613, base/613, head/614, …) so machine
  drift over the 36-minute campaign hit both arms equally.
- Seeds 613/614/615 reshuffle arm order, averaging out time-position bias.
- Hub pinned to CPU 2, two drain workers to CPUs 3,4 — distinct physical cores on
  an i7-9750H, siblings NOT isolated. `powersave` governor. Criterion
  `--warm-up-time 1 --measurement-time 2 --sample-size 20 --noplot`; means read
  from `new/estimates.json["mean"]`, never the console regression slope.

## Why a SLOPE and not a time

The absolute numbers on this host are unusable. Three measurements of the
identical arm within one process (`opening`, `A-miss-unlinked/0`, `closing`)
spread 8.3–19.5%, with opening→closing drift up to −16.3% against a predeclared
5% gate. Criterion's within-arm CIs are ±1% and badly understate that.

A slope is a within-run ratio, so the drift that ruins absolutes largely divides
out. The evidence that this works is the base population's own tightness: sd 1.02
across six observations spanning 36 minutes of a live desktop.

## Result — 2→9 slope, linked families only

`C-miss-linked` and `D-hit-linked` × 3 seeds = 6 observations per version.

| version | observations (%) | mean | sd |
|---|---|---|---|
| base (`5761b1e`) | +15.3 +13.6 +16.3 +16.2 +14.5 +15.2 | **+15.20%** | 1.02 |
| head (`1e189c5`) | −1.8 +2.3 −3.4 +0.2 +1.0 −0.5 | **−0.37%** | 2.03 |

Difference 15.56 pp, 16.8× the standard error of the difference. Worst base
observation +13.6% vs best head observation +2.3% — **no overlap**. The routed
twin agrees: +12.32% → −1.25%, 5.1×, also no overlap.

**The internal control matters as much as the result.** `A-miss-unlinked` and
`B-hit-unlinked` show no slope on *either* version (base +0.5% / −1.9%). That is
structurally correct rather than lucky: the rig observes placement members only
under `if shape.connected`, so the unlinked shapes run with an EMPTY placement
whatever `peers` says, and `peers_all` has nothing to walk. Had the effect
appeared there too it would have been an artifact of the harness.

## Contrasts at P=9, unrouted, averaged over seeds

| contrast | base | head |
|---|---|---|
| C−A, connected peer links | +19.8% | **+1.4%** |
| D−A, both | +37.3% | +16.3% |
| B−A, matching remote interest | +13.6% | **+16.5%** |

C−A is items 1.1/1.2: the per-publish `Placement::members()` walk and its N
`String` clones, now a cached read. B−A is **unchanged and is the honest
residual** — matching remote shared interest still costs ~15%, wave B never
targeted it, and it is the `plan_shared` remote-index walk. It is FLAT in peer
count on both versions, so it is a fixed cost of having a matching remote group,
not a scale-out cost.

## What this does not say

- Nothing about throughput, capacity, or node count on real hardware. One host,
  one process, channel transport, no network.
- Nothing about item 1.3. On gated arms the `gated && !retain_broadcasts` return
  fires before 1.3's early return, so 1.3 is never reached here. It remains a
  QoS 0 claim, carried only by its unit test's zero-touch assertion.
- Nothing about wave A. The settle-gate and cap changes are proven by unit tests,
  not by this.
- The QoS 0 control run taken alongside this campaign is discarded: +37.5% drift.

## The fixture defect this campaign found

The gated arm could not run past its first linked shape until `1e189c5`.
`setup_with` asserted every connected peer had received `PeerMessage::Interest`,
but `peer_connected` sends it only under `if self.interest_authoritative`, which a
clustered hub holds false until a complete scan lands over a whole mesh — seconds
after the assertion fired. It survived review because the A/B shapes are
`connected: false`, so the assertion loop never executes, and the QoS 0 arms run
with `placement: None` and settle in milliseconds. Nothing had run the matrix.

## Reproducing

```sh
MEMBERSHIP_SEED=613 MEMBERSHIP_TRANSPORT=channel MEMBERSHIP_GATED=1 \
MEMBERSHIP_DRAIN_THREADS=2 MEMBERSHIP_HUB_CPUS=2 MEMBERSHIP_DRAIN_CPUS=3,4 \
CRITERION_HOME=<dir> cargo bench -p mqttd --bench shared_membership -- \
  --warm-up-time 1 --measurement-time 2 --sample-size 20 --noplot
```

For the baseline arm, `git worktree add --detach <dir> 5761b1e`, copy
`crates/mqttd/benches/shared_membership.rs` and
`crates/mqttd/benches/shared_membership/rig.rs` (plus its `paced.rs`,
`perf_control.rs` and `tcp.rs` siblings) in, add the `[[bench]]` stanza to
`crates/mqttd/Cargo.toml`, and give it its own `CARGO_TARGET_DIR`. Interleave the
versions.

Raw criterion samples, per-run logs and the run manifest were kept outside the
tree; they are not a hosted evidence bundle.

## Related

- `MEMBERSHIP-PROBE.md` — the QoS 0 channel probe and its two preserved pilots.
- `QOS0-SCALING-PLAN.md` — the investigation this measures one hypothesis of.
- `SCALE-CURVE.md` — the published curve, which has no 7-node point.
