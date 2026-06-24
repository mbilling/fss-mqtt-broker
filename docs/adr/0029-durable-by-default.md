# ADR 0029 — Durable sessions by default

- **Status:** Accepted
- **Date:** 2026-06-24
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0029-durable-by-default.md](../delivery/0029-durable-by-default.md) — plan, progress, and changelog
- **Related:** [ADR 0006](0006-consensus-and-replication.md)/[0007](0007-durable-store-integration.md)
  (the durable store this turns on), [ADR 0018](0018-on-disk-persistence.md) (on-disk
  persistence via `MQTTD_DATA_DIR`), [ADR 0026](0026-lease-timing-durable-storage.md) /
  [0027](0027-replica-group-commit.md) / [0028](0028-link-gated-voter-admission.md) (the three
  that made durable stable at rest, under load, and through formation — the precondition for
  this), [ADR 0004](0004-authentication.md) (secure-by-default posture this extends)

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0029-durable-by-default.md).

## Context

Durable, consensus-backed sessions are the reason this broker exists, but they shipped as an
opt-in (`MQTTD_DURABLE_SESSIONS=1`) because the lease group churned. That is now resolved: ADR
0026 (raft timing for fsync latency), ADR 0027 (replica group-commit) and ADR 0028 (link-gated
voter admission) together took a 3-node durable cluster from "churns to epoch 218" to forming
in ~90s and holding a flat term for 20+ minutes under load. The robust path is now the stable
path, so it should be the **default** rather than something an operator has to know to switch
on. Leaving the durable, replicated store opt-in means the out-of-the-box experience silently
loses sessions on failover — the opposite of this broker's posture.

The cost to weigh is the single-node / no-disk case (see Consequences): durable is a *cluster*
durability mechanism, and a lone broker gets the machinery without the replication benefit.

## Decision

**Durable sessions are the default.** `MQTTD_DURABLE_SESSIONS` is now an opt-**out**:

- **unset (the default) → durable on** — the consensus-backed replicated store (ADR 0006/0007).
- `MQTTD_DURABLE_SESSIONS=0|false|off|no` → durable off — the bounded in-memory store, for
  operators who explicitly want the lightweight non-replicated backend.
- `1|true|...` → durable on (unchanged).

On-disk persistence is still governed by `MQTTD_DATA_DIR` (ADR 0018), orthogonally:

- **durable + `MQTTD_DATA_DIR`** → consensus-replicated **and** on-disk: survives node failover
  *and* full-cluster restart. **The recommended production configuration.**
- **durable, no data dir** → consensus-replicated, **in-memory**: survives a node failover
  (R≥2), rebuilds from peers, but a full-cluster restart starts empty. A sane zero-config
  cluster default.

Startup logs the effective mode loudly (durable yes/no, persistent yes/no), and `/readyz`
continues to gate on lease-group readiness when durable — so an orchestrator still waits for
the group to form before routing traffic.

## Consequences

- **Good:** the secure, robust path is what you get by default — a cluster replicates persistent
  sessions out of the box and survives a node loss without configuration. Aligns the default
  with the broker's reason to exist and its secure-by-default posture (ADR 0004).
- **Cost — single node / no peers:** a lone broker forms a one-voter lease group (quorum=1,
  R=1): it runs the raft/lease machinery but gains no replication (there is no follower to
  replicate to). Without `MQTTD_DATA_DIR` it is also not restart-durable. So for a genuinely
  single-node deployment the default adds overhead for little benefit — such operators should
  set `MQTTD_DURABLE_SESSIONS=0` (lightweight) or, better, `MQTTD_DATA_DIR` (on-disk
  single-node persistence, ADR 0018 phase 1). This is documented at the env var and logged at
  startup; it is not silent.
- **Cost — behaviour change:** deployments that relied on the implicit in-memory default now
  run durable. The opt-out is one env var, the change is loudly logged, and no on-disk state is
  written unless `MQTTD_DATA_DIR` is set, so the change is observable and reversible.
- **Risk:** durable is a more complex code path to make the default. It is gated behind the
  three stability ADRs above, each soak-validated; `/readyz` prevents premature traffic; and
  the opt-out is a clean escape hatch.

## Alternatives considered

- **Keep it opt-in.** Rejected: now that durable is stable, opt-in means the default experience
  silently drops sessions on failover — exactly what this broker is built to avoid.
- **Default durable only when clustered (seeds present).** A "smart" default keyed on whether
  the node has SWIM seeds. Rejected: a founder starts with no seeds yet *is* the start of a
  cluster, so the signal is unreliable, and a default that changes based on inferred topology is
  surprising. A single, predictable default plus a documented opt-out is clearer.
- **Require `MQTTD_DATA_DIR` for the durable default.** Rejected: it would break zero-config
  startup. Persistence stays an orthogonal, recommended-but-optional axis.
