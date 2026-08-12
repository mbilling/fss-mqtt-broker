# Post-mortem: CI acked-facts flake — retained anti-entropy repair refused forever after an owner restart

**Date:** 2026-08-12 · **Environment:** CI (`spawned_process_schedules_hold_acked_facts`, ADR 0044 P1 out-of-process tier, seed 0) · **Status:** root cause confirmed and FIXED (snapshot exports carry the durable authority's committed token; regression tests added at hub level)

> Related decisions: [ADR 0037](../adr/0037-durable-retained-messages.md) (durable retained keyspace,
> P5 token convergence), [ADR 0044](../adr/0044-release-readiness-assurance.md) (the out-of-process
> tier that caught it), [ADR 0014](../adr/0014-cluster-retained-replication.md) §3 (snapshot back-fill).
> Tracked as issue **#214**; the anti-entropy digest this rode on is issue **#87**.
> Sibling incident: [2026-07-20](2026-07-20-kube-retained-ownership-split.md) — same subsystem
> (durable retained), a *different* root cause (gossip/lease ownership split).

## Summary

`spawned_process_schedules_hold_acked_facts` failed twice in one day (PRs #194, #213 — both unrelated
to broker code), both times **identically**: seed 0, topic `rt/0/1`, the retained-convergence oracle
reporting `proc0-a=r-0-1, proc0-b=r-0-2, proc0-c=r-0-1` after its full poll window. Both re-runs went
green, so it was filed as a probable timing flake — with the explicit instruction (issue #214) that
nobody widen the deadline until a diagnostic could distinguish "deadline too tight on a loaded runner"
from "the oracle genuinely lost a fact".

The attempt-1 CI logs decided it. The divergence was **stable for the entire 20s window** and was the
third possibility nobody named: the fact was durably held but **permanently unservable from 2 of 3
nodes**, while every anti-entropy round logged that it had converged.

## Impact

- A retained value acked by the topic's group owner, followed by that owner crashing before its
  commit fan-out left the process, was served **only by the restarted owner, forever**. Subscribers
  attaching through any other node read the previous retained value indefinitely. The committed record
  existed at quorum the whole time; nothing on the other nodes ever read it.
- The convergence `warn!` claimed `converged to the higher-token committed value` on every anti-entropy
  round while applying nothing — the log actively concealed the failure it was built to surface.
- Observed in CI only (pre-release). Production exposure was real: any owner crash inside the
  ~post-ack, pre-fan-out window, followed by a restart, reproduces it.

## What the evidence shows

Both failures carried the same trace: two acked retained sets to `rt/0/1` via `proc0-b`
(→ tokens `(E,1)`, `(E,2)`), `proc0-b` SIGKILLed ~83ms into the next schedule step, restarted later,
cluster quiesced on `/readyz`, acked-payload oracle passing — then 40 poll rounds of
`a=r-0-1 b=r-0-2 c=r-0-1`. One truncated line survived at the top of proc0-c's log tail:

```
… to the higher-token committed value (ADR 0037 P5) node=proc0-b topics=1
```

c had run the divergence-resolving snapshot exchange against b — and still served the old value.

## Root cause

Retained convergence tokens (`retained_tokens`, hub.rs) are **in-memory by design**; the durable
keyspace is the authority. The snapshot sender attached tokens from the in-memory map with
`unwrap_or((0, 0))`. After the owner's restart:

1. the persistent cache reopened with the committed `r-0-2` — the node *serves* the right value;
2. the in-memory token map reopened **empty** — the node *exports* the value untokened `(0, 0)`;
3. every peer had applied the previous fan-out (`r-0-1` at `(E,1)`) and held that token in memory, so
   the untokened repair was refused as stale: `(0,0) <= (E,1)`;
4. the refusal repeated on every ~30s anti-entropy round, each time logging "converged".

The receiver-side durable fence added for issue #87 item 4 was correct but read-only on the wrong side:
the *sender* lost its tokens with the process and never re-read the authority it had them from.

## Fix

- **Sender** (`send_retained_snapshot`): a cache topic with no in-memory token re-reads the durable
  authority and exports the committed record under its committed token — and re-adopts it locally,
  which also repairs the harsher variant where the crash landed between the commit and the owner's own
  cache apply (cache older than the authority).
- **Receiver** (`apply_retained_snapshot`): refusing an entry as stale against the durable authority
  now also re-adopts the authority's record into the local cache, so a fresh process whose fence is
  ahead of its cache converges on receipt instead of re-detecting the divergence forever. The
  documented untokened rule ("gap-fill only, never overwrite") is enforced from the values rather than
  from a fence that dies with the process.
- **Honest logging**: the divergence warn reports `applied` vs `kept` counts instead of claiming
  convergence unconditionally.
- **Oracle** (`cluster_proc.rs`): the failure is now self-diagnosing per issue #214 — what was awaited,
  a change-compressed observation timeline, elapsed/rounds, per-node `/readyz` and process state, and a
  verdict (stable divergence vs still-moving) anchored to the anti-entropy period. The deadline moved
  20s → 45s with its justification written down: 20s sat *inside* the ~30s repair cadence it was
  supposed to allow for.

## What made it hard to see

- The CI annotation dropped the panic message (fixed separately, PR #215), so the reports read as a
  bare `proc_common/mod.rs:490`.
- The oracle's own probe clients wrote ~6 log lines/second/node, flushing the explanatory divergence
  warns out of the 4KB log tails the failure printed. The harness now also prints warn/error/link-event
  extracts (`log_notables`), and the one warn that mattered now tells the truth.
- Two clean re-runs. A permanently divergent cluster still passes a *fresh* schedule: the next run's
  retained sets commit new tokens through a live owner, fan out normally, and converge — the failure
  needed the crash to land in the post-ack pre-fan-out window, which only shows under runner load.

## Follow-ups

- **Residual (deliberate)**: a restarted node whose retained state is *tombstone-only* still goes
  digest-silent (its cache is empty and the token map that would advertise the tombstones died with the
  process), so a peer holding a stale value it missed the clear for is not repaired until the next
  committed write to that topic. The receiver-side durable fence keeps the restarted node itself from
  resurrecting the value. Needs a keyspace scan (or persisted tokens) to close; tracked as issue **#216**.
- The in-process tier (ADR 0042) cannot see this class — its "restart" rebuilds hubs whose caches and
  token maps die together. The out-of-process tier caught it precisely because `retained.redb` outlives
  the process while `retained_tokens` does not. That asymmetry is now a named thing to think about when
  a struct splits its state across the two.
