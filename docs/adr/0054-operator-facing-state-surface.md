# ADR 0054 — Operator-facing state surface: `/statusz` + state gauges

- **Status:** Accepted
- **Date:** 2026-08-05
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0054-operator-facing-state-surface.md](../delivery/0054-operator-facing-state-surface.md) — plan, progress, and changelog
- **Related:** [ADR 0020](0020-metrics-and-observability.md) (the one-ops-listener and
  cardinality rules this extends), [ADR 0047](0047-kubernetes-deployment.md) (whose
  2026-08-04 amendment's operator triggers this work engages),
  [ADR 0041](0041-resource-governance.md) (the brownout state this surfaces),
  [ADR 0043](0043-elastic-cluster-resize.md) (the drain and caught-up watermark this
  surfaces), [ADR 0049](0049-voter-eligible-durable-ownership.md) (the durable-
  serviceability detail this generalizes)

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0054-operator-facing-state-surface.md).

## Context

The maintainer has decided to pursue a Kubernetes operator (reopening ADR 0047's
deferral through its recorded triggers): split-brain, brownout, and drain handling
should eventually be reconciled automatically. An operator can only be as good as the
state it observes, and the 2026-08-05 observability inventory found the broker's
externally visible state falls short of what a reconciler needs:

- **Split-brain is undetectable in principle** — no cluster identity exists; the
  seedless-founder boolean is the sole bootstrap guard, with no post-hoc detector.
- **Brownout is a state with no state signal** — only rejection counters; an *idle*
  browned-out broker is externally silent.
- **A decommission drain has no metric** — visible only to a direct `/readyz` probe.
- **Membership is counts-only** — *who* a node sees (the asymmetric-view signal) is
  exposed nowhere; the lease voter set and leader identity likewise.
- Key-rotation posture, config convergence, and the peer protocol range are invisible.

## Decision

**One structured, read-only `GET /statusz` on the existing health listener, plus
bounded state gauges — split by a hard rule: unbounded identity detail (member lists,
voter ids) goes in the JSON body; only bounded values become metrics.**

1. **`/statusz`** (this ADR's tranche): node identity + founder flag + version, the
   placement membership view (id/addr/failure-domain per member), lease detail
   (leader flag, epoch, voter identities, group-ready, quorum-ack age, replica
   catch-up summary), decommission progress **including the previously unsurfaced
   `active` flag**, brownout state with onset timestamp, per-store bytes + the
   configured watermark, and the peer-bus protocol range. Later tranches add the
   cluster identity (split-brain detection) and rotation/convergence blocks.
   `/statusz` always answers 200 — only `/readyz` carries readiness in its status
   code (the ADR 0049 rule, kept). Same trust model as the probes (ADR 0020 §2:
   unauthenticated ops network) — therefore **never any secret material**; where a
   secret must be identified (rotation posture), a fingerprint stands in for it.
2. **State gauges** (Prometheus + OTLP, ADR 0020 cardinality rules): `brownout{axis}`
   (the condition, not the symptoms), `store_max_bytes` (utilization becomes
   computable), `decommission_state`/`decommission_pending` (a scrape-only observer
   sees a drain), `voters` (previously body-only), and
   `replica_groups_current`/`replica_groups_tracked` (tracked − current = this
   node's replication lag in groups — the takeover-safety signal).
3. **The split rule is the contract**: metrics stay bounded-label (the existing
   `no_unbounded_label_keys` test keeps enforcing it); anything naming nodes,
   members, or per-group detail lives only in the `/statusz` body. The health
   module's "exposes no broker state beyond…" docs are updated to name `/statusz`
   as the sanctioned wider surface.

## Consequences

- An operator (or an alert rule, or a human with `curl`) can now see brownout as a
  condition, watch a drain from outside, compare membership views across nodes, and
  verify voter sets — the acting-signals for the ADR 0047 amendment's reconciliations.
- The health listener's response surface grows; its trust model does not. Everything
  in `/statusz` is state an ops-network reader could already infer from logs and
  probes — consolidated, structured, and cheap to poll.
- The gauges are refreshed on existing paths (hub gauge tick, store watcher poll,
  brownout transitions, a small drain-poller task) — no new periodic machinery.
- Follow-on tranches (same delivery doc): **cluster identity** minted at founding,
  persisted, gossip-propagated, and guarded (`cluster-mismatch` gossip rejections) —
  turning split-brain from undetectable to detected *and* contained; then key-
  rotation fingerprints and config checksum/generation for convergence checks.

## Amendment (2026-08-06): the re-founder quarantines itself

T2 made a second, separately-founded cluster **detectable** (compare `cluster_id` across
`/statusz`) and **contained at the gossip layer** (foreign datagrams are dropped and
counted). Containment was scoped to membership, and that scope turned out to be too
narrow: the divergent node's own MQTT listener was untouched. A pod-0 whose volume is
lost mints a new identity, its single-voter lease group elects itself, `members = 1`
satisfies the founder's `ready_min_members = 1`, and `/readyz` returns 200 — so it joins
the client Service endpoints and serves a share of connections from an empty session and
retained store, beside the real cluster. Detection told an operator to "act promptly"; it
did not stop the node serving in the meantime.

**A node now refuses to serve when, with the guard armed (cluster networking configured
and not disabled), both hold:** it has observed at least one `cluster-mismatch` gossip
rejection, **and** its membership view holds only itself. Surfaced as `/statusz`'s
`quarantine` block and the `mqttd_refound_quarantine` gauge.

Those two identify the divergent node and only the divergent node. It rejects every
foreign datagram, so it never learns a peer and its view stays at one; the healthy
majority sees exactly the same foreign gossip — from the divergent node — but has each
other, so it keeps serving. A genuine first bootstrap is alone too, but nothing is alive
to send it foreign gossip, so the rule **cannot** fire there — which matters, because
`podManagementPolicy: OrderedReady` means pod-0 must reach Ready alone or no other pod is
ever created. The evidence lands ~1 s after the gossip socket binds, well inside the ~5 s
the startup probe takes, so in practice the divergent node never reaches the Service.

The first design keyed the rule on `ClusterIdentity::minted` — "this process minted an
identity and then heard another cluster" — and the kind e2e falsified it. `minted` is
per-PROCESS while the divergence lives on disk: the re-founder mints only in its first
life, and once it has written the new `cluster-id`, every later life *loads* it and looks
innocent. The operator's fence, by deleting the pod, is exactly what ends that first life,
so the guard would have been inert in the case it was built for. Membership survives a
restart; provenance does not.

The verdict is evaluated live rather than latched, so a node that legitimately rejoins
serves again with no operator action — and the divergent node cannot exploit that, since
it rejects the very gossip that would grow its view.

The latch fires regardless of gossip authentication. On an unauthenticated mesh a spoofed
datagram could hold a founder NotReady — but that same port already accepts injected
`Dead` claims that tear down routing, which is strictly worse, and the broker warns about
it at startup. `[cluster] refound_guard = false` (env `MQTTD_REFOUND_GUARD`, so it is
reachable mid-incident without a config roll) is the escape for a deliberate re-bootstrap
beside a cluster being abandoned.

This is containment, not prevention: the node still re-founds, and still needs the
documented wipe-and-rejoin. Making a wiped pod-0 *join* instead of found is ADR 0055 T9.
