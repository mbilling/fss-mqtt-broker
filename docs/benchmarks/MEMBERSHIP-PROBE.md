# QoS 0 membership probe — #613

Follow-up: [profiles, real TCP client receipts and aligned fixed-offer probes](MEMBERSHIP-TCP-PROBE.md).
The historical channel results and their limitations below are unchanged.

## What this slice adds

A local mechanism probe after #612, **not a broker capacity result**:

- `crates/mqttd/benches/shared_membership.rs` runs the A/B/C/D matrix from
  [the investigation plan](QOS0-SCALING-PLAN.md).
- `crates/mqttd/benches/shared_membership/rig.rs` provides exact burst/sequence receipt checking and
  the metered drainers; `crates/mqttd/tests/shared_membership.rs` tests the oracle
  and exercises all 16 arms. No production routing code changed.
- Peer count is 0/2/4/9; each peer advertises six groups with one member each.
  Four local subscribers share one exact group/filter. Exactly one local delivery
  is required for each publication. All messages are non-retained MQTT 3.1.1
  QoS 0, 200-byte payloads; prefer-local is explicitly on.

| Arm | Advertised shared filters | Connected peer channels |
|---|---|---|
| A | All miss | No |
| B | One matches the exact local group/filter; five miss | No |
| C | All miss | Yes |
| D | Same as B | Yes |

No ordinary remote interest is installed. Unlike `shared_plan`'s original peer
arms, C/D actually register `PeerConnected`. Setup barriers plus receipt of each
peer's initial interest snapshot verify registration. A/B are synthetic states,
not proposed deployment topologies. Four P=0 arms are equivalent controls.

One current-thread runtime drives the Hub and publisher. A separate two-worker
runtime drains local and peer channels. A burst has 2,000 uniquely identified
messages and must yield exactly 500 receipts per local member, all IDs present
once, zero remaining outbound packets/bytes, and no peer data forwarding.
The publication payload carries a monotonically increasing burst ID plus sequence;
every new rig has fresh channels, so a stale prior burst cannot satisfy receipt.

A Hub Ping establishes that all publishes have been dispatched. Only **then** are
local drain checkpoints requested, consuming any remaining packets before answering.
The benchmark waits for these receipts and validates their identities. No missing
receipts, panic, timeout, unexpected packet or wrong member count becomes a timing
result. Setup, burst and peer-check deadlines are each bounded at ten seconds.
Task aborts on rig drop prevent old arms from continuing as load sources.

Peer checkpoints run **outside** timed bursts: otherwise the diagnostic would add
its own O(peer count) round trips. Both ordinary and shared data frames are forbidden;
only the fixture's known interest/digest control frames are allowed. Local receipts
alone would miss an incorrectly duplicated remote forward.

The direct-bypass control sends the same sequence-labelled packets straight into
the metered local drainers. It prices construction/channel/oracle overhead, **not**
independent TCP generator or consumer headroom. The fixture does not attach production
metrics and times construction, Hub dispatch, receipts and validation together.
It does not report Hub-only service time or enqueue-to-dequeue latency.

## Reproduce without provisioning

Run the correctness gates first:

```sh
cargo test -p mqttd --test shared_membership
cargo clippy -p mqttd --all-targets -- -D warnings
```

A short pilot (CPU IDs are examples: choose disjoint physical cores on your host):

```sh
set -o pipefail
out=$(mktemp -d /tmp/membership-613.XXXXXX)
git rev-parse HEAD > "$out/revision.txt"
git diff > "$out/diff.patch"
rustc -Vv > "$out/rustc.txt"
lscpu > "$out/lscpu.txt"
printf '%s\n' 'seed=613 hub=2 drains=3,4 warmup=1s measurement=2s samples=20' \
  > "$out/config.txt"
CRITERION_HOME="$out/criterion" MEMBERSHIP_SEED=613 \
  MEMBERSHIP_HUB_CPUS=2 MEMBERSHIP_DRAIN_CPUS=3,4 \
  cargo bench -p mqttd --bench shared_membership -- \
  --warm-up-time 1 --measurement-time 2 --sample-size 20 --noplot \
  2>&1 | tee "$out/benchmark.log"
status=$?
printf '%s\n' "$status" > "$out/exit-status.txt"
```

Preserve the output directory and exact source/binary when reporting it, including
untracked fixture files (a git diff alone omits those). `CRITERION_HOME` prevents
new runs from overwriting previous raw `sample.json`/`estimates.json`. For repeated
runs, create a new directory and record a different `MEMBERSHIP_SEED`. The seed
randomizes arm order, not traffic. Opening/closing and bypass controls retain their
positions. Affinity variables are optional; when provided, Linux `/proc/thread-self`
and `taskset` are required and a failed pin is fatal, not a silent unpinned run.

The ten-second receipt deadline is a failure bound, not a latency SLO. Criterion's
short automatic warm-up/iteration estimates do not implement the plan's paced,
60-second equal-work experiment. No CPU utilization should be derived by dividing
this benchmark's wall time by message count.

## Two preserved pilots, 2026-09-14

Production source: merged main `5c5cbae` (includes #612), with the new fixture.
Rustc 1.97.0, release profile, Linux x86_64, Intel i7-9750H shared workstation.
Hub/publisher pinned to logical CPU 2; two drain workers restricted to CPUs 3/4.
These are different physical cores on this host, but **not isolated from other
processes or their SMT siblings**. One-second warm-up, two-second requested
measurement, 20 Criterion samples per arm; some automatic sample collections
ran longer. No background build/tests were launched during the pilots.

Artifacts remain locally under `.scratch/membership-613/`:

- `20260914T083044Z-seed613/`
- `20260914T083538Z-seed614/`

Each contains the command/environment/revision/toolchain manifest, binary SHA-256,
exact fixture sources, diff, full log, raw Criterion samples and estimates, and
exit status. `cpu.jsonl` holds lifetime one-second process/thread `/proc` and system
CPU samples with monotonic and UTC timestamps, including affinity in thread status.
Those samples cover the **whole benchmark**, not precisely aligned arm windows;
no per-arm CPU claim is made. No CPU/off-CPU profile was collected in these runs.
Artifacts are local, not a hosted evidence bundle.

Mean milliseconds per 2,000 complete channel receipts, with Criterion's within-run
95% mean confidence intervals (these are **not** cross-run uncertainty):

| Arm | Seed 613 | Seed 614 |
|---|---|---|
| Opening P=0 | 5.6044 [5.5255, 5.6950] | 5.2286 [5.1618, 5.3073] |
| Direct bypass | 1.7178 [1.6950, 1.7393] | 1.6829 [1.6595, 1.7046] |
| A, P=9 | 6.2998 [6.1615, 6.4322] | 5.8518 [5.7112, 6.0855] |
| B, P=9 | 8.5518 [8.4422, 8.6961] | 8.0043 [7.9724, 8.0351] |
| C, P=9 | 9.0347 [8.8703, 9.2068] | 6.2452 [6.1684, 6.3221] |
| D, P=9 | 11.7588 [11.4414, 12.0297] | 8.3583 [8.2864, 8.4295] |
| Closing P=0 | 5.9616 [5.7234, 6.2853] | 5.2377 [5.2068, 5.2717] |

Use `new/estimates.json["mean"]` for this table. Criterion's console headline can
instead show its regression slope; do not mix those estimators for control checks.

Both runs completed every receipt/zero-forwarding check. **Seed 613 fails the
predeclared 5% opening/closing mean-time stability gate (+6.37%)** and cannot support
a causal cost estimate. Keep it, including the much larger linked-peer timings.
Seed 614 passes that gate (+0.17%), but passing one control is not full validation.
At P=9 in that run, B−A is +36.8% and C−A is +6.7%. Matching interest therefore
provides a useful next profiling target. It does **not** prove the matching-location
loop limits TCP throughput or that the effect scales linearly with P. Intermediate
points are noisy/non-monotonic and between-run linked-peer costs differ materially.

Bypass time was about one third of opening Hub time in both runs. This is reassuring
about gross fixture overhead, not a proof that generators/consumers never limit a
burst or that the receipt oracle is non-perturbing. No paced offer, independently
certified concurrent endpoint pool, production-metric overhead control, aligned
per-arm CPU, or five-block confidence calculation has yet been measured.

## Validation and next action

The matrix test runs 24 bursts per arm, exceeding the outbound packet cap per
worker cumulatively, then checks the direct control. Separate tests reject missing,
duplicate, stale and out-of-range IDs, and peer data even when local receipt could
succeed. A temporary omission of `OutboundMeter::drained` was actually exercised:
the matrix failed on its first burst with depth 500 instead of zero. The mutation
was restored; its log is in `.scratch/membership-613/validation/`.

Final local checks: `cargo test -p mqttd` completed with **711 passed, 0 failed,
9 existing ignored**; the new fixture's three tests all passed. All-target mqttd
clippy with `-D warnings`, workspace fmt, test hygiene/inventory (1,603 tests in
74 binaries), README facts, generated dashboard and diff whitespace checks passed.

**Next:** profile the matched-versus-missed P=9 pair with stable controls and measured
fixture overhead; add paced, aligned stage timing if the profile cannot separate
Hub work from drain scheduling. Then reproduce the mechanism over TCP while testing
broker N=1/2/4 with fixed per-node work and independently verified endpoint headroom.
The broker/driver/subscriber window repair remains outstanding for lane E. No
production traversal rewrite, sharding, bridge prerequisite, or cloud expenditure
is justified by these pilots alone.
