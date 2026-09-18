# QoS 1 scaling qualification

The paid authorization for this change is ONE three-node provisioning. The
confirmation run executes a 30/60/90/120/90/60/30/120k load sweep, a five-minute
120k hold on its last rung, and a closing 30k control. It cannot establish a
cross-size scaling curve or long-term reliability.

## Instrument and confirmation

Build `../qos1-driver/build.sh /absolute/build-dir`, export
`QOS1_DRIVER_ARCHIVE=/absolute/build-dir/driver.tar.gz`, then execute
`./run-confirm.sh`. Use PREFLIGHT_ONLY=1 with ../run.sh to check shape without
provisioning. The wrapper always requests `full 3`; destruction remains trapped
by run.sh. Raw evidence is written outside the worktree from the start.

Predeclared acceptance: actual emission and per-site receipt at least 97% of
offer, late publications at most 5%, p99 histogram upper bound at most 1000ms,
complete endpoints and stable processes, settled population, steady per-site
receive rates before opening, successful pause/PUBACK completion, no drops or
QoS downgrade, and an exact group-wide sent/acked/received identity ledger.
Duplicates are measured separately and do not masquerade as new deliveries.
Any invalid evidence prevents a pass. A quiet drain alone is not delivery proof.

Payload is 200 application bytes plus 16 timestamp/sequence bytes (216 bytes).
This differs from legacy 208-byte timestamp-only runs. Instrumentation adds
bitmap ledger operations and PUBACK histograms; compare only runs using the
same image and disclose this overhead. Each site's topics identify publishers;
a fresh container ledger is used per rung. A bitmap per receiving process is
merged across every member of the shared group. This permits out-of-order and
duplicate receipt without an invalid per-subscriber sequence assertion.

## Subsequent stages (prepared, not authorized/executed by confirmation)

1. Repeat the three-node sweep at least three times. Use 10–15% load increments
   around the first valid failing load, including downward steps. Invalid or
   under-offered points do not bracket a broker capacity boundary.
2. Calibrate the actual co-resident publishers AND subscribers. Compare the
   same cluster/load with more publisher containers spread across hosts; record
   per-core CPU, Erlang runtime metrics, PUBACK latency, and network counters.
   Raising DRIVER_COUNT alone does not redistribute a site. Until such a
   comparison succeeds, do not attribute a shortfall to the broker or drivers.
3. Measure fresh 3/5/7/10-node clusters with the SAME binary, driver image,
   per-publisher pacing, subscriber policy, payload, session semantics, and
   routing proportions. Increase independent driver capacity with workload.
   Provision at least one driver per site for the current site placement.
   Check every shape offline first. Run each load three times and include a
   closing control. Capture both increasing and decreasing load.
4. Hold a load 10–20% below each measured boundary for 30 minutes. Inspect
   periodic per-site receive/PUBACK rates and broker backlog/inflight gauges:
   no persistent upward queue trend, rising latency, reconnects, or errors.
   The five-minute confirmation is a smoke check for this stage, not a soak.
5. Run a SEPARATE persistent-session arm if claiming durability; clean-session
   results do not establish disk-backed QoS 1 capacity. Likewise, cross-node
   forwarding requires a separate workload with declared placement; the
   positive forwarding canary certifies counters, not sustained forwarding.

Predeclare predictable scaling as efficiency within 0.85–1.15 of C(3)*N/3,
repeat range within 10% of median, and the same latency budget. Publish the
actual efficiency and uncertainty even when this hypothesis fails. These are
experimental acceptance targets, not MQTT requirements. Finite steps establish
behavior only over the tested range, not mathematical continuity.

Use `report.py RUN... --output DIR` for a fail-closed per-rung report and
repetition qualification. It refuses a cross-size scaling claim when sizes or
repetitions are missing. The JSON retains exact ledgers and rates; a qualified
operating point is still a lower bound unless a valid higher load fails.

The September 18 confirmation result is recorded in
[the harness review](../../../astra/20260918T162211Z-qos1-harness-confirmation.md).
It did not qualify a cloud capacity point. The revised local integration can
exercise three FSS processes and a cross-node shared group with
`../qos1-driver/test-local.py --mqttd /path/to/pinned/mqttd`.
`ledger-report.py RUN` independently reconciles completed lifetime ledgers even
when timing evidence prevents a throughput claim. The final audited steady gate
uses per-endpoint remote timestamp bounds; historical manifests retain their
original polling semantics and are not silently upgraded.
