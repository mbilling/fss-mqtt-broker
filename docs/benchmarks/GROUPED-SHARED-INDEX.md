# Remove per-peer work for an already-selected shared group (#613)

## Scope of this change

Production baseline: `29e4385` (same broker code as `5c5cbae`; #612 included).
This branch groups the derived remote shared-filter index by **exact group name**
before listing that group's peer locations. It removes a measured membership cost,
not the previously fixed all-filter scan, and does **not** claim proportional
broker/worker scaling or a 20% capacity improvement.

The [earlier TCP timing controls](MEMBERSHIP-TCP-PROBE.md) remain invalid for their
original timing claims. This follow-up measures a narrower quantity—retired
userspace instructions on the hot runtime thread per exact TCP data receipt—to
separate executed work from CPU-frequency/scheduling variability. It does not
replace the previous offer, latency or control gates with easier capacity criteria.

## Evidence that selected the change

The channel profile had identified `plan_shared`'s matching-location callback and
hashing as candidates. New `perf stat` measurements isolate a single paced TCP
window, with counters **disabled during setup/warm-up**, enabled with an acknowledged
control command, and disabled/acknowledged after the window. `--no-inherit` measures
only the hot thread, not the endpoint runtime or setup child processes.

Fixed workload per arm:

- 20,000 data publications/s, 200-byte payload, MQTT 3.1.1 QoS 0, one publisher,
  four online local shared subscribers, prefer-local explicitly enabled.
- 2-second paced warm-up, then 10-second measurement: **200,000 unique data
  receipts**, plus the existing four per-burst FIFO fences, not counted as data.
- Same single-thread broker runtime pinned to CPU 2; two endpoint workers on
  CPUs 3/4 of the shared i7-9750H workstation. No governor changes or cloud use.
- Each peer advertises six groups. C: all miss the published topic; D: one matches
  the same local group/filter, five miss. Peer channels remain synthetic.
- Three before and three after blocks, each bracketed by standalone controls,
  alternating C/D order. Additional 2/4-peer probes and an old-binary recheck follow.

All data receipt/fence/zero-forward checks passed. Every arm met the 1% actual-offer
gate. Both hardware events were available and reported 100% running coverage;
missing, zero, unsupported, duplicate or <99%-running counters are rejected rather
than interpreted as zero cost. Instruction-control drift was at most 0.30%.
**Cycle and wall-time controls still varied**, so neither is used for a capacity
or latency-speedup claim. Instruction stability is evidence for this work-count
comparison, not independent consumer/generator capacity certification.

Mean retired hot-thread userspace instructions per data receipt; brackets are the
**observed range of three runs**, not a confidence interval:

| Arm | Before | Group-first index |
|---|---|---|
| Standalone opening | 19,221 [19,217–19,223] | 19,213 [19,208–19,217] |
| C / non-matching, 9 peers | 21,535 [21,522–21,547] | 21,525 [21,513–21,534] |
| D / matching, 9 peers | 31,263 [31,254–31,277] | 25,089 [25,082–25,096] |
| Standalone closing | 19,222 [19,210–19,229] | 19,235 [19,209–19,266] |

D's total decreases **19.75%** while C and standalone remain essentially unchanged.
The matched-versus-missed increment isolates the extra matching work more closely:

| Peers | Before: D−C instructions/receipt | After: D−C instructions/receipt |
|---|---|---|
| 2 | 4,487 | 3,543 |
| 4 | 6,004 | 3,562 |
| 9 | 9,728 | 3,563 |

The 2/4-peer points are single probes; nine peers uses the three-run means above.
The residual matching increment is approximately flat across this range; it is not
zero. A final recheck with the preserved **old binary** again measured D≈31,281 and
C≈21,507 instructions/receipt. The reduction therefore did not merely track the
chronological before/after transition. These data justify removing the repeated
location/group work, not other speculative routing or scheduling changes.

## Production implementation and preserved invariants

`crates/mqttd/src/hub/mod.rs` now derives:

```text
filter -> group name -> [(node, index in that node's remote group vector), ...]
```

`remote_shared` remains authoritative. The index is fully rebuilt on the same
membership updates/loss events as before. Nodes are visited in sorted order;
locations within a node retain source-vector order, including repeated entries
for the same group. Grouping is paid at rebuild time, not per publication.

In `crates/mqttd/src/hub/delivery.rs::plan_shared`, the `decided` lookup now precedes
**all** per-peer lookups for that exact group/filter. A local winner skips the group
once, not once per announcing peer. Other groups on the same filter, and other
matching filters with the same group name, still receive their own decisions.

The cold `shared_candidates` path and #612's `qos0_shared_alternative` use the same
grouped locations, maintaining the old cursor positions and exact identity. Local
admission, byte/count bounds, link checks, ownership fencing, QoS 1/2 obligations,
retained behavior and retry policy are unchanged. No wire/config/schema change.
The connected-peer iteration in ordinary forwarding is **not** changed here.

**Trade-off:** the derived index now owns a group-name key and BTreeMap metadata per
filter/group structure, in addition to the existing location lists. It does not
create per-message state or unbounded message buffering, but membership rebuild
CPU and resident index memory have not been benchmarked at large subscription
populations. Do not claim a memory/churn improvement or free cache construction.

## Reproduce the counters

Build and test first:

```sh
cargo test -p mqttd --lib remote_group_index
cargo test -p mqttd --lib shared_capacity
cargo test -p mqttd --test shared_membership
python3 bench/scale/test-membership-counters.py
cargo bench -p mqttd --bench shared_membership --no-run
```

Use the executable path printed by cargo, an available perf binary, and unused
artifact directories. Example for a single matching arm:

```sh
python3 bench/scale/run-membership-counters.py \
  --binary target/release/deps/shared_membership-<build-hash> \
  --perf /path/to/perf --out /tmp/membership-D-new-run \
  --case D-hit --peers 9 --rate 20000 --seconds 10 --warmup 2 \
  --hub-cpus 2 --endpoint-cpus 3,4
```

The runner does not provision hosts, change perf permissions, or select a power
policy. It saves the manifest, exact binary SHA, current worktree diff/benchmark
sources, benchmark log, raw perf JSON, perf exit status and validated summary.
When using an older binary, its SHA and original source manifest—not the current
worktree snapshot—identify its production source. A perf exit of zero alone is
insufficient: failed counter parsing or under-offer exits the runner unsuccessfully.

`MEMBERSHIP_CASE` selects one paced arm; `MEMBERSHIP_PEERS` changes the C/D peer
population. The optional `MEMBERSHIP_PERF_CONTROL` / `MEMBERSHIP_PERF_ACK` FIFOs
require one selected arm so counter totals cannot accidentally combine windows.
The handshake is nonblocking with a five-second failure bound outside timed work.
The first real smoke run exposed perf's NUL-terminated `ack\n\0` protocol: reading
only the newline left a byte that desynchronized the next acknowledgement. That
failed run is preserved and the parser/test now consume the complete frame.

Counts include small handshake/snapshot/oracle overhead around the workload and
the hot thread's server connection tasks; they are **not Hub-only instructions**.
They exclude kernel instructions, endpoints and idle time. No throughput multiplier
may be obtained by simply inverting this instruction reduction.

Local raw artifacts under `.scratch/membership-613/`:

- `counters-smoke/` (failed acknowledgement), `counters-smoke-fixed/`.
- `counters-before/rep-*/` and `baseline-binary`.
- `counters-after/rep-*/`.
- `counters-peer-count/` (2/4 peers and the final old-binary recheck).
- `validation/group-index-*` (build, regression, mutation and suite logs).

Artifacts remain local; this document is not a hosted evidence bundle. All prior
failed/invalid timing runs are retained and their interpretation is unchanged.

## Correctness and remaining acceptance

`crates/mqttd/src/hub/tests/remote_group_index.rs` pins:

- Exact group/filter identity with overlapping wildcards and unrelated remote-only
  groups, at all three granted QoS levels.
- Stable candidate/cursor ordering across reverse peer arrival, interspersed groups,
  repeated same-group source entries, and online/offline updates; hot and cold
  selection agree on client, node and QoS.
- Full replacement invalidation, stale versus current connection loss, and peer death.

Two actual mutations were caught and restored: suppressing by filter alone lost an
obligation; reversing node order changed the expected cursor recipients. #612's
seven admission/pressure regressions also pass. Hardware-counter parser tests reject
bad coverage/data, and the acknowledgement test handles fragmentation, interruption,
consecutive acknowledgements, malformed frames and EOF.

Final local validation: **1,548 workspace tests passed, 0 failed, 9 existing ignored**.
The updated platform-coverage guard also passed with `CI=true`. The two Python
counter-validity tests, mqttd all-target clippy with `-D warnings`, formatting,
test hygiene/inventory (1,609 tests across 74 binaries), README facts, generated
dashboard and diff whitespace checks passed.

**Next:** validate sustainable delivered throughput/latency and overload recovery
with real broker peers and controlled per-node workload/placement; measure the
index's memory/rebuild trade-off at the intended subscription population. The
instruction-work reduction is a supporting fix under #613/#482, not closure of
the proportional QoS 0 delivery objective. Bridge scaling remains a separate axis.
