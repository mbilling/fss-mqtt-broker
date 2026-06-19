# ADR 0019 — Graceful shutdown and connection draining

- **Status:** Proposed (awaiting ratification)
- **Date:** 2026-06-19
- **Deciders:** project maintainers
- **Related:** [ADR 0005](0005-session-affinity.md) (placement/relocation),
  [ADR 0006](0006-consensus-and-replication.md) (lease group),
  [ADR 0016](0016-swim-membership-stability.md) (membership),
  [ADR 0017](0017-durable-attach-readiness.md) (attach lifecycle),
  [ADR 0018](0018-on-disk-persistence.md) (durable writes to flush)

## Context

`mqttd` has **no shutdown handling**. `main.rs` spawns each listener into a `Vec` of
join handles and then blocks forever:

```rust
for l in listeners { let _ = l.await; }
```

There is no `SIGTERM`/`SIGINT` handler, no stop signal to the accept loops or the hub,
and no draining. On `SIGTERM` (the normal Kubernetes/systemd stop signal) the kernel
terminates the process immediately. The consequences:

- **In-flight QoS 1/2 messages are lost** — unacked PUBLISH/PUBREL state in the hub never
  completes, and (until ADR 0018) was never persisted.
- **Wills misfire** — an abrupt process death is not a clean client DISCONNECT, so every
  connected client's will is *not* fired by us in an orderly way; on restart, peers' will
  handling and session takeover see a hard drop rather than a graceful leave.
- **The lease group and SWIM see a crash, not a leave** — peers must wait out failure
  detection (suspicion → dead) and a lease re-election, rather than being told "I'm
  leaving" so they can re-own immediately. This lengthens the unavailability window on
  every routine restart/upgrade.
- **Durable writes (ADR 0018) are not flushed/checkpointed** before exit.

For a broker that aims at rolling upgrades and orchestrated deployments, orderly
shutdown is table stakes.

## Decision

Implement cooperative, bounded graceful shutdown driven by a single cancellation signal.

### 1. Signal handling

`main` installs handlers for `SIGTERM` and `SIGINT` (and treats channel closure the
same). The first signal begins graceful shutdown; a **second** signal escalates to
immediate exit (operator override for a hung drain).

### 2. A single shutdown signal threaded through the runtime

Introduce one `tokio_util::sync::CancellationToken` (or a `watch` channel) created in
`main` and passed to: the accept loops, `conn::handle` per connection, the hub, the SWIM
driver, and the cluster tasks. Shutdown proceeds in ordered stages:

1. **Stop accepting.** Accept loops select on the token and stop taking new connections
   immediately; the TLS/health listeners close. New connects are refused fast (so a load
   balancer drains us).
2. **Leave the cluster cleanly.** Announce departure on SWIM (a graceful "leaving"
   so peers mark us gone without waiting out suspicion) and **relinquish owned leases**
   so a survivor re-owns those groups promptly (ADR 0005/0006). Persistent-session owners
   hand off rather than being failure-detected.
3. **Drain client connections.** Each live connection is asked to finish: complete
   in-flight QoS handshakes where possible, then send a v5 **Server DISCONNECT** with
   reason `0x8B Server shutting down` (v3.1.1 clients are simply closed after their
   in-flight settles). Bounded by a **grace deadline** (`MQTTD_SHUTDOWN_GRACE`, default
   30s, aligned with a typical k8s `terminationGracePeriodSeconds`).
4. **Flush durable state.** Once connections are drained (or the deadline hits), flush /
   checkpoint the persistent stores (ADR 0018) and stop the hub.
5. **Exit.** Clean exit code on success; log loudly if the grace deadline forced an
   ungraceful drain (named connections still in flight).

### 3. Bounded, observable

The grace deadline guarantees termination even if a client never settles. The shutdown
path emits structured logs at each stage and (with ADR 0020) a `mqttd_shutdown_*` metric
so operators can see drain duration and whether it timed out.

### 4. Readiness flips first

On receiving the signal, `/readyz` immediately reports **not ready** (before stage 1
completes) so orchestrators stop routing new traffic to us while existing connections
drain — the standard "fail readiness, keep liveness during drain" pattern.

## Implementation notes (for the workstream)

- Add `tokio-util` (pure-Rust, already common) for `CancellationToken`; thread a clone
  into `serve_plaintext_clients` / `serve_tls_clients` (select! on `token.cancelled()` vs
  `listener.accept()`), into `conn::handle`/`run_framed` (select against the read loop so
  a draining connection can be told to finish and DISCONNECT), and into `Hub::run`.
- New `HubCommand::Drain { reply }` (or reuse a shutdown token in the hub loop): on drain,
  stop accepting new attaches, optionally fire wills for clients that won't reconnect, and
  flush. The hub already serializes state, so a drain step is a natural final command.
- `health.rs`: a shared `AtomicBool`/watch `draining` flag that `/readyz` consults.
- Cluster leave: a SWIM `Leave`/`Dead(self)` gossip plus a lease `relinquish` call on
  owned groups; peers already handle membership changes (ADR 0016) and lease re-election
  (ADR 0006).
- Config: `MQTTD_SHUTDOWN_GRACE` (seconds, default 30).
- Testing: integration test that connects clients (incl. an in-flight QoS 2), sends the
  shutdown signal to an in-process node, and asserts: readiness flips, new connects are
  refused, existing clients get a Server DISCONNECT, in-flight settles or is persisted
  (ADR 0018), and the process returns within the grace bound. A cluster test that a
  graceful leave triggers immediate lease re-ownership (no suspicion wait).

## Consequences

- **Good:** rolling upgrades and orchestrated restarts stop dropping in-flight messages
  and stop incurring a failure-detection-latency outage on every restart; wills and
  session handoff are orderly; durable writes are flushed. Aligns with k8s lifecycle
  (preStop + terminationGracePeriod) and load-balancer draining.
- **Cost:** a shutdown signal must be plumbed through several task boundaries (modest,
  mechanical). A misbehaving client cannot delay shutdown beyond the grace bound by
  design.
- **Risk:** low. The main subtlety is ordering (stop-accept → leave-cluster → drain →
  flush → exit) and ensuring the grace deadline always wins; covered by the integration
  test. Graceful cluster-leave interacts with membership/lease code, so it is gated behind
  the same care as ADR 0016 (test-first on the leave/relinquish path).

## Alternatives considered

- **Rely on the OS / orchestrator to just kill us.** This is today's behaviour; it loses
  in-flight work and pays a failure-detection outage on every restart. Rejected.
- **Drain without a deadline.** A single stuck client would hang termination; orchestrators
  would then `SIGKILL` anyway, losing the orderly flush. The bounded grace is required.
- **Persist-and-die (lean on ADR 0018, skip draining).** Persistence protects committed
  state but does not give connected clients a clean DISCONNECT, does not settle in-flight
  QoS that *could* complete in the grace window, and does not hand off leases promptly.
  Draining and persistence are complementary, not substitutes.
