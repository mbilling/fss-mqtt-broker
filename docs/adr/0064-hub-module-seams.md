# ADR 0064 — The hub's module seams

- **Status:** Accepted
- **Date:** 2026-08-18
- **Delivery:** [docs/delivery/0064-hub-module-seams.md](../delivery/0064-hub-module-seams.md)
- **Related:** [ADR 0037](0037-retained-convergence.md), [ADR 0041](0041-resource-governance.md),
  [ADR 0061](0061-hub-append-lanes.md) — the invariants the extracted modules own

> This record states the decision only. Progress lives in the delivery doc.

## Context

`hub.rs` reached 19,893 lines (~9,700 production) — a god-actor holding every
correctness-critical decision in one review surface (issue #258; the 2026-08-13 panel's
reviewer 4: the complexity "will outrun its excellent comments"). With bus factor 1,
reviewability is the mitigation, and one file defeats focused review.

## Decision

The hub is a directory module. Extraction is **move-only**: each slice relocates items
verbatim (visibility widened to `pub(super)` and paths requalified — never logic), lands
green against the untouched test suite, and the new module's doc comment states the ONE
invariant it owns. The seams, and the rule for future work — code lands in the module
whose invariant it serves:

| Module | Owns |
|---|---|
| `hub/retained.rs` | ADR 0037: a retained value is one cluster-wide fact — authority tokens, digests/snapshots, tombstones, windowed replay |
| `hub/policy.rs` | ADR 0041/#238: a refusal is an on-loop, effect-free policy decision; brownout axes gate growth |
| `hub/lanes.rs` | ADR 0061: nothing the store must answer runs on the loop — frozen jobs, per-session FIFO, owned workers |
| `hub/delivery.rs` | One plan per answerable publish; ack-after-durable; the send chain stays `fn` so the compiler forbids on-loop store awaits |
| `hub/forwarding.rs` | An `Accepted` releases only against recorded evidence — obligations, verdicts (first-terminal-wins, unknown withholds), retransmission |
| `hub/mod.rs` | The actor itself: state, dispatch, session lifecycle, sweep, and the mesh/settle honesty gates beside the sweep that arms them |

## Consequences

Review attention can land on one invariant at a time; a diff's file names now say which
contract it touches. The struct's fields remain in `mod.rs` (one item), so the split is
of METHODS and their types — field ownership is documented per module, not enforced by
the compiler. Tests stay in `mod.rs`'s `mod tests` unchanged; moving them is deliberate
non-scope (their location proves the no-behaviour-change claim).
