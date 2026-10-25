# ADR 0016 — SWIM membership stability (dead-node fencing + false-positive resistance)

- **Status:** Accepted
- **Date:** 2026-06-18
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0016-swim-membership-stability.md](../delivery/0016-swim-membership-stability.md) — phases, progress, and changelog
- **Related:** [ADR 0003](0003-gossip-authentication.md) (SWIM datagram auth),
  [ADR 0005](0005-session-affinity.md) (placement owns relocation),
  [ADR 0006](0006-consensus-and-replication.md) / [ADR 0007](0007-durable-store-integration.md)
  (durable sessions whose recovery depends on a correct replica set),
  [ADR 0017](0017-durable-attach-readiness.md) (the attach-path half of the failover gap),
  [ADR 0021](0021-bounded-lease-voters.md) (the bounded voter set §5 spreads across
  failure domains), [ADR 0022](0022-signed-gossip.md) (the authenticated gossip that
  carries §5's domain labels)

> This record states the decision only. The phased rollout and how far along it is live
> in the [delivery doc](../delivery/0016-swim-membership-stability.md).

## Context

Placement (and therefore the durable session store's replica sets) is derived directly
from SWIM membership. A diagnosed failure (the durable client-observable-failover gap)
traced to **membership instability**: after a node is killed, a survivor's `members()`
flaps to a *wrong* set that **still lists the killed node** (resurrected) and has **dropped
a live survivor** (falsely evicted). The resulting replica set has no live quorum, so
session recovery reads the dead node, times out, and fails — stalling the first client
reconnect ~10s with `session_present=false`.

The SWIM implementation already had the core mechanisms (`swim.rs`): per-node
**incarnation** numbers, **suspicion** (a probe failure marks `Suspect`, not `Dead`), and
**self-refutation** (a node hearing itself suspected bumps its incarnation and re-asserts
`Alive`). Two specific gaps remained — exactly the two halves of the diagnosed bug:

1. **`Dead` is not terminal.** A `Dead` member stayed in the map and was *resurrected* by
   any later higher-incarnation update. A node that refuted to a high incarnation just
   before dying left that `Alive(high)` gossip in flight; it arrived after the `Dead`
   declaration and revived the corpse. No tombstone, no pruning.
2. **Suspicion → `Dead` is single-source with fixed timeouts.** One prober's timeout was
   enough to drive a peer to `Dead`. A CPU-starved prober declares healthy peers dead, and
   a starved victim cannot refute within the fixed window. Nothing adapted timeouts to
   local health or required independent confirmation.

These are well-understood problems with well-understood fixes (the SWIM paper's terminal
`Dead`/tombstone, and the **Lifeguard** extensions). This ADR adopts them, scoped to what
closes the gap.

## Decision

### 1. `Dead` is a tombstoned terminal state (fixes resurrection)

When a member reaches `Dead` it becomes a **tombstone**: kept with a `tombstone_deadline =
now + DEAD_TTL`, and during that window **no gossiped update can revive it** —
`Alive`/`Suspect` updates *about a tombstoned node* are dropped regardless of incarnation.
After `DEAD_TTL` the tombstone is **pruned**.

`DEAD_TTL` is set comfortably above the gossip drain time, so a stale pre-death refutation
cannot outlive the tombstone — the resurrection is impossible. A node that genuinely
restarts rejoins **after** the tombstone is pruned, or under a fresh id; it does not need
to out-race stale gossip. (Self-refutation for the *local* node is unaffected — a node
always defends its own liveness; the tombstone governs only third-party claims about
*others*.) This mirrors the dead-node interval in mature gossip stacks (memberlist/Serf).

### 2. Lifeguard local-health awareness (reduces false positives)

Adopt Lifeguard's **self-awareness multiplier**: the local node tracks a small `awareness`
score that rises when its *own* probes go unanswered or it has to refute itself (signals
that *it* is the slow one) and decays on healthy rounds. Both `ack_timeout` and
`suspicion_timeout` scale by `(1 + awareness)`, so a locally-degraded node waits **longer**
before suspecting peers, and as a victim benefits from peers' longer suspicion windows.
This attacks false-eviction without weakening detection on a healthy node (awareness = 0 ⇒
unchanged timeouts).

### 3. Independent-suspicion confirmation before `Dead`

Also from Lifeguard: a node's **suspicion timeout shrinks as independent suspicions
accumulate** (and starts long). A single prober's suspicion holds the full window; only
when multiple distinct nodes independently suspect the same peer does it fast-track to
`Dead`. One contended prober can no longer unilaterally kill a healthy peer. (Refutation
still overrides a false suspicion outright.)

### 4. Keep the existing incarnation/refutation core

Incarnation precedence and self-refutation are correct and stay. The changes are
*additive*: a terminal/tombstoned `Dead`, awareness-scaled timeouts, and
confirmation-scaled suspicion.

### 5. Failure-domain-aware voter placement (T4/T5, added after acceptance)

[ADR 0021](0021-bounded-lease-voters.md)'s bounded voter set introduced a concentration
risk the full-membership model never had: a topology-blind (lowest-id) fill can put a
quorum of the N voters in one rack/zone, so that domain's loss halts lease consensus for
the whole cluster. Two additive tasks close this:

- **Selection (T4):** voter selection is **sticky first, balanced second**. A live,
  eligible voter is never demoted to improve the spread (voter changes are Raft membership
  changes — churn there costs more than an imperfect spread); only *free* decisions —
  filling a vacancy, or the one-time shrink when adopting the cap — pick from the
  **least-represented failure domain**, tie-breaking by lowest id so every successive
  leader computes the same target deterministically. An unlabelled node is its own
  singleton domain; with no labels at all the selection is bit-for-bit the prior
  id-ordered fill.
- **Topology source (T5):** each node advertises **only its own** `MQTTD_FAILURE_DOMAIN`
  label inside the authenticated SWIM gossip payload (HMAC + per-node signature +
  anti-replay, ADR 0003/0022/0023), learned non-erasingly like the routing address. The
  lease driver reads the assembled map live each reconcile tick, overlaid on the optional
  static `MQTTD_FAILURE_DOMAINS` seed (gossip wins) — the topology self-assembles and
  tracks membership. The selection algorithm is source-agnostic (`decide_with_domains`
  takes any map), which is the seam that let T5 swap the source without touching T4.

This mirrors proven operational designs — Consul autopilot's redundancy zones (Serf-tag
driven zone-aware voters) and Kubernetes zone-spread — rather than inventing new theory.

**Honest limits of §5** (stated so operators do not assume stronger properties than are
delivered):

1. **The spread is eventual, not an invariant.** Stickiness means an already-concentrated
   voter set (e.g. 3-of-5 voters in one rack because those nodes joined first) *stays*
   concentrated until natural churn opens vacancies. The system converges toward balance;
   it never forces it. Do not read "voters spread across domains" as a guarantee that
   holds from the moment labels are configured.
2. **Arithmetic beats topology at fewer than 3 domains.** Spreading 5 voters over 2 racks
   still leaves ≥3 in one rack, so that rack's loss still takes quorum. Domain-loss
   tolerance requires **≥ 3 failure domains** (just as Raft needs ≥ 3 voters for one node
   loss). The mechanism spreads voters; it cannot beat the majority arithmetic.
3. **T5 trades "identical map by construction" for eventual consistency.** T4's static
   cluster-uniform map guaranteed every leader computed the same target from the same
   input. Gossip-learned labels can transiently differ between nodes, so a leadership
   change mid-propagation may briefly produce a different target. Accepted because the
   reconciler is leader-only, debounced, vacancy-driven and sticky — divergence yields at
   worst a transient extra membership proposal, never a safety violation (Raft joint
   consensus owns safety) — and the views converge as gossip settles.
4. **Domain labels were self-asserted — hardened by T6 (CA attestation).** ADR 0022
   signing authenticates *which node* made a claim, not that the claim is *true*: a
   compromised (but validly-certified) node could claim any domain — a unique fake rack to
   make itself the balancing algorithm's most attractive voter pick, or a victim domain's
   label to dilute its representation. (Impact bounded to voter-placement skew /
   availability, since consensus safety still needs a quorum.) **T6 closes this for
   attested clusters:** a cluster-bus certificate carrying
   `URI:urn:fss:failure-domain:<label>` makes the **CA** the authority for the label — a
   self-claim contradicting the certificate drops the datagram, and in the signed posture
   a member's label is accepted **only from the member itself** (third-party claims in
   relayed gossip are stripped, closing relay-forged topology as well; labels then
   converge via SWIM's direct probes). A certificate without the URN keeps the
   self-assertion residual trust — adopt incrementally by reissuing certs with the label;
   relabeling is a reissue(+revoke, ADR 0022 T7) ceremony, the accepted cost of coupling
   PKI to topology. The alternatives considered (authoritative controller labels, platform
   attestation, corroboration heuristics, damage-limiting only) and their costs are
   recorded in the delivery doc's T6 entry.
5. **A gossiped label overriding the static seed is loud (T6).** When both sources are
   set and disagree, gossip wins and the lease driver `warn!`s once per label — one of
   the two configs is stale, and silent divergence would let an operator trust a map that
   is not the one being enforced.

## Consequences

- **Good:** closes the membership half of the durable failover gap (recovery sees a live
  quorum); placement/replica-set correctness no longer depends on perfectly-timed gossip;
  fewer spurious ownership churns; grounded in published, battle-tested algorithms.
- **Cost / limits:** a node that is **genuinely** restarted (same id) must wait out
  `DEAD_TTL` before rejoining, or use a fresh id. A node *falsely* declared `Dead` is
  tombstoned for `DEAD_TTL` too; §3 makes that rare but not impossible, so `DEAD_TTL` is a
  bounded, tuned value. Awareness adds a little per-node state and slows detection on a
  degraded node (by design).
- **Risk:** this is correctness-critical membership code feeding durability. It must be
  developed **test-first against the pure `Swim` state machine** (no network), reproducing
  resurrection and false-eviction as deterministic unit tests *before* any wiring. A wrong
  change here can silently corrupt the durability guarantees, so it warrants its own focused
  workstream — not a reactive edit.

## Alternatives considered

- **Just loosen the test's SWIM timings.** Reduces the *false-eviction* rate but does
  nothing about *resurrection*, and papers over a real production fragility. Rejected.
- **Prune `Dead` immediately (no tombstone).** A re-learned `Alive` still re-inserts the
  node. The tombstone is what makes `Dead` stick.
- **Full Lifeguard (buddy-system probing, NACK-aware, all knobs).** More than this gap
  needs; §2–§3 take the parts that address the observed failures and leave the rest as a
  later option.
- **Make recovery tolerate a bad replica set (retry / shrink quorum).** Retrying against a
  set with no live quorum cannot succeed, and shrinking the quorum breaks the safety
  (intersection) property. Membership must be correct; recovery must not paper over it.

## Amendment (2026-08-05): claims are scoped to a process GENERATION (issue #92)

§1 assumed one sentence too many: *"A node that genuinely restarts rejoins after the
tombstone is pruned … it does not need to out-race stale gossip."* It does, and on
Kubernetes it loses that race every time.

A `Dead` claim never drains, because **applying one re-gossips it** — each node that
accepts the claim re-arms its own dissemination of it. Meanwhile a restarted process
has no memory of its incarnation, so it re-enters at the bottom of the range, where
`Dead` outranks `Alive` at equal incarnation. The tombstone is pruned after
`DEAD_TTL`, the node rejoins, the still-circulating claim kills it again, and the
cycle repeats at exactly the `DEAD_TTL` period — indefinitely. Downstream, every
`Dead` transition drops the peer link and its routing state, so the durable lease
group never elects a leader and the affected pod stays NotReady for good. A rolling
restart, the most routine operation there is, silently left a three-node cluster
running on two. Found by the ADR 0055 T7 operator e2e; the existing kind smoke
missed it because it asserts immediately after the roll and the flap needs ~30 s.

**Membership claims now carry the subject's process `generation`** — a monotonic
per-start token (the broker uses wall-clock ms at start; the state machine takes it
as a parameter and stays clock-free). Ordering is `generation`, then incarnation,
then state precedence:

- a claim at a **lower** generation is about a previous life and is discarded
  outright — it can no longer resurrect, re-kill, or downgrade the current process;
- a claim at a **higher** generation supersedes everything held about the older
  life, **including its tombstone** — a restart is not a resurrection, and fencing
  one out is precisely what left a rolled pod dead;
- at the **same** generation, every rule in §1 applies unchanged.

So the anti-resurrection guarantee is kept exactly where it was aimed: a dead
process's own in-flight refutation carries that same generation and is still fenced.
What it no longer does is fence out the node's *next life*.

Two adjacent defects, found in the same investigation and fixed with it: an address
change that did not alter state raised no membership event (so a dialer kept the
address it was spawned with for the life of the process), and a peer first seen
through relayed gossip without a routing address was dropped permanently rather than
picked up when the address arrived.

Wire: `Update.generation` and `Message.from_generation` are appended fields — a
pre-1.0 reshape under ADR 0039, same clean-break rules as ADR 0052 and the
`cluster_id` field before them, so `BASELINE_REF` moves with it.

## Amendment (2026-10-12): a falsely evicted member is re-admitted in bounded time (issue #971)

The formation storm of issue #971 was a certificate-distribution fault and is settled in
[ADR 0022](0022-signed-gossip.md)'s amendment of the same date. What it exposed here is
what happens *after* a false `Dead`, whatever caused it: on 1.1.0 nothing guaranteed the
member ever came back.

**Why it stayed out.** Membership is disseminated only on change — updates piggyback on
probe traffic for a bounded number of retransmissions, and a `Sync` answers a `Join` and
nothing else; there is no periodic exchange of views. A node that declares a peer `Dead`
fences it for `DEAD_TTL` (§1) and then prunes it, after which it holds nothing about the
peer and never addresses it again. Whether the peer returns then depends on the peer:

- if it still holds the evictor alive it keeps probing it, and its datagram re-admits
  it first-hand — unless the evictor cannot open that datagram (ADR 0022: a missing
  certificate), in which case the probe fails, helpers rescue it, and the pair stays
  one-sided. The issue-#383 re-greet was meant for exactly this and never fired: it
  counted only probes that helpers could **not** rescue, and an indirect ack reset the
  count;
- if the two nodes declared **each other** dead — which a short outage of one of them
  produces routinely, see below — neither probes the other. A third party's gossip is
  the only way back, and in a quiet cluster there is none. The first-hand rule of #383
  cannot help: no first-hand datagram is ever sent.

In the simulation a member cut off for 8 s in a formed 5-node cluster was still evicted
by some node five minutes later in 13 of 16 seeds on 1.1.0; at 14 and 30 nodes the views
took up to 88 s and 116 s to become whole, healed only by gossip that happened to pass.

### Decision

1. **A probe that only helpers rescued counts toward the re-greet.** `unanswered_probes`
   is now "probes this member did not answer *directly*": it rises when the direct ack
   deadline lapses and is cleared by the member's own ack, not by an `IndirectAck`. An
   indirect ack says the member is alive; it does not say the member hears *us*. After
   `reprime_after` (3) such probes the next one is accompanied by the certificate-carrying
   `Join`, as #383 intended. (Between two current nodes ADR 0022's request heals the pair
   on the first probe; this rule is what heals it when the evictor still runs 1.1.0, or
   the request was not sent.)
2. **A pruned member gets one parting `Join`.** At the moment a tombstone is pruned the
   node greets the member it is about to forget. A live member answers with a `Sync` and
   is re-admitted first-hand, whichever of the two had stopped probing; a dead one costs
   one datagram to a closed port per prune. A node that is itself leaving greets nobody.

Neither weakens §1 or the generation rules. The fence still stands for its full
`DEAD_TTL`; a crashed process answers no greeting; a restarted one was already admitted
by its generation. Nothing is accepted that was not accepted before — the greeting only
makes a live member speak.

With both, the same 8 s cut heals in every seed: no node holds a healthy member out for
longer than `DEAD_TTL` plus a round trip (30.1 s measured, at every size), and every view
is whole within 59 s at worst (14 nodes; 41 s at 30).

Both rules are `Config` switches (`reprime_on_rescued_probe`, `greet_pruned`), on by
default; off, they are 1.1.0's behaviour, which is how the formation simulation runs a
1.1.0 node beside a current one. They are not operator configuration.

### What this does not fix: a short outage can still end in a false `Dead`

The suspicion window is 4 s for one suspecter and shrinks to 1.5 s with three (§3). It
does not grow with the cluster, and a refutation travels only as fast as piggybacked
gossip: the suspicion has to reach the suspect and the refutation has to return, each a
few probe periods' worth of hops. With every certificate known, cutting one member of a
formed cluster off for **1.5 s** ends in some node declaring it dead in the median seed
at 14 and at 30 nodes (none at 5), on 1.1.0 and now alike. What has changed is the cost:
the eviction now ends at `DEAD_TTL` (30.1 s), where it ran to 46 s and 63 s at worst.

SWIM's own answer is a window that scales with log N, and Lifeguard's is to tell the
suspect directly; either is a change to failure-detection timing that this amendment
does not make; it is left for a change of its own.

## Amendment (2026-10-13): the residual is measured, and two candidates are in the tree, off (issues #973, #975)

The paragraph above is now a number. In the formation simulation, cutting one member of a
formed cluster off for 1.5 s ends with some node declaring it `Dead` in 3 of 32 seeds at
5 nodes, 7 at 10, 16 at 14 and 25 at 30; the table for 0.5 s to 5 s, four candidate
detectors and what each costs when the member really died is in
[ADR 0087](0087-delivery-across-a-false-dead-verdict.md) §7 and issue #975.

Two `Config` switches were added to measure the candidates. **Both are off by default and
neither is operator configuration**; with both off the state machine is what it was.
(The first became the default the same day: the next amendment.)

- `confirm_before_dead` — Lifeguard's direct refutation path in its smallest form: when a
  suspect's window lapses it is sent one `Ping` carrying the suspicion and given one ack
  timeout, and only a silent one is declared `Dead`. A member that answers is kept
  whatever its `Ack` carries. Usually the `Ack` carries the refutation, first-hand (a
  node told it is suspected puts its refutation at the head of its gossip). When it does
  not — the member had already refuted an earlier claim and that refutation's retransmit
  budget is spent, so our claim is stale to it; or it is an older build — the `Ack` to
  that probe is itself proof of life: the window restarts and the member is sent one
  `Join`, whose `Sync` states its incarnation. No new wire element, and nothing asked of
  the suspect that a v1.1.0 node does not already do. In the simulation no member cut
  off for up to 1.5 s is declared dead at any size, and a real death is detected
  0.1–0.5 s later in the median.
- `suspicion_scales_with_log_n` — the SWIM paper's rule: both ends of the window times
  `max(1, log10 n)`.

Whether either becomes the default is #975's decision, as a change of its own, as this
record asks of any change to failure detection.

## Amendment (2026-10-13): a suspect is asked once more, directly, before it is declared `Dead` (issue #975, ADR 0087 T10)

Decided by the maintainer on 2026-10-13: the last direct probe of the amendment above
is the default. This is the change of its own that this record asks for, with its
evidence.

### Decision

`Config::confirm_before_dead` defaults to `true`. When a suspect's window lapses
(§3: 4 s for one suspecter, 1.5 s with three, times the local-health multiplier of §2)
the node does not declare it `Dead`. It sends the suspect one `Ping` that carries the
suspicion and waits one ack timeout (250 ms, times the same multiplier):

- a suspect that stays silent is declared `Dead`, one ack timeout later than before;
- a suspect whose `Ack` carries its refutation — the usual case: a node told it is
  suspected bumps its incarnation and puts the refutation at the head of its gossip —
  is `Alive` at once, first-hand;
- a suspect whose `Ack` carries none (the claim was stale to it, or it is a v1.1.0
  node, which queues its refutation behind other gossip) is kept on the `Ack` alone:
  the window restarts at its full length and the member is sent one `Join`, whose
  `Sync` states its incarnation. If that `Sync` is lost the member stays `Suspect`
  and is asked again when the new window lapses: one `Ping` and one `Join` per
  window, on top of the ordinary round-robin probes a suspect keeps receiving, for
  as long as it answers, and `Dead` the first time it does not
  (`a_suspect_that_answers_without_ever_refuting_is_asked_once_per_window`).

Only the suspect's own `Ack` to that very probe counts, so the probe spares a member
only on a datagram round trip between the two nodes concerned. §1 (the tombstone), §2,
§3 and the generation rules are unchanged; so is everything a node does with a `Dead`
it is *told* about. Nothing is added to the wire: a `Ping`, an `Ack`, a `Join` and a
`Sync`, as every release has sent them.

**It is not operator configuration, and there is no switch.** None of the detector's
settings is: the protocol period, the ack timeout, the window, the tombstone's lifetime
and the rules of the 2026-10-12 amendment are constants of the release, and
`[cluster.swim]` configures addresses, seeds, keys and the signing and anti-replay
postures only. A key for this one rule would be the first of its kind, for a rule
whose benefit depends on every node running it (a node without it declares, and
spreads, the verdicts the others would have spared), and whose off position was not
better in any measured cell below. A release that turns out to need the old behaviour
back gets it from the previous release; the field stays on `Config` so that the
formation simulation can run a v1.1.0 node, which lacks the rule, beside a current one.

### Evidence

All of it from `crates/mqtt-cluster/tests/swim_formation_sim.rs` — the driver's real
gossip endpoint (signing, certificate cache, state machine) over a virtual clock, with
the broker's default timings — re-run on the tree that makes the change. A fault is
put on one member of a formed cluster a minute after formation. Unless stated, a cell
is **the number of runs, of 32 seeds, in which some node turned a healthy member
`Dead`**, during the fault or after it.

```
cargo test -p mqtt-cluster --release --test swim_formation_sim false_dead_report  -- --ignored --nocapture
cargo test -p mqtt-cluster --release --test swim_formation_sim fault_shapes_report -- --ignored --nocapture
cargo test -p mqtt-cluster --release --test swim_formation_sim formation_report    -- --ignored --nocapture
```

**A clean cut: nothing in, nothing out.** With the probe / without it.

| nodes | cut 0.5 s | 1 s | 1.5 s | 2 s | 3 s | 5 s |
|---|---|---|---|---|---|---|
| 3 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 3 | 32 / 32 |
| 5 | 0 / 0 | 0 / 0 | 0 / 3 | 0 / 7 | 6 / 18 | 29 / 32 |
| 10 | 0 / 4 | 0 / 4 | 0 / 7 | 0 / 13 | 3 / 25 | 31 / 32 |
| 14 | 0 / 6 | 0 / 10 | 0 / 16 | 0 / 21 | 8 / 25 | 30 / 32 |
| 30 | 0 / 6 | 0 / 20 | 0 / 25 | 0 / 28 | 0 / 31 | 28 / 32 |

A cut of up to 2 s ends in no verdict at any size (by arithmetic, with about 0.1 s to
spare: the amendment for #1002 below derives it and adds the 2.5 s column, which is not
zero). The members are still *suspected*
(a median of 20 `Suspect` transitions across a 14-node cluster after a 1.5 s cut, the
same with and without the probe); what changes is how the suspicion ends. At 3 s the
probe no longer holds: three suspecters shorten the window to 1.5 s, the member really
is silent when it is asked, and the `Dead` declared during the cut is adopted from
gossip by the rest after it. A 5 s cut is longer than the window and is a death to
every detector.

**The price: a member that really died.** Milliseconds from the death until the last
survivor holds it `Dead`, median / worst of 16 seeds.

| | 3 nodes | 5 | 10 | 14 | 30 |
|---|---|---|---|---|---|
| with the probe | 4089 / 4996 | 4716 / 6982 | 4997 / 6011 | 5613 / 8337 | 7005 / 8347 |
| without | 4010 / 4696 | 4787 / 6682 | 4729 / 5404 | 5313 / 8037 | 6500 / 8347 |

About 0.3 s later in the median (between 0.07 s sooner and 0.5 s later, by size), and
up to 0.6 s later at worst: one ack timeout per node that has to reach the verdict for
itself, less where the verdict arrives by gossip first.

**Formation.** The rig's bring-up, 16 seeds per size, with the probe / without it:
milliseconds until every view is whole for good after the last start, median and
worst. No run of either raised a `Suspect` or a `Dead`, so the probe was never sent
and the two columns are the same runs.

| | 3 nodes | 5 | 10 | 12 | 14 | 20 | 30 |
|---|---|---|---|---|---|---|---|
| with the probe | 56 / 98 | 43 / 100 | 4532 / 8040 | 6638 / 9317 | 7490 / 11933 | 15853 / 19089 | 26298 / 32065 |
| without | 56 / 98 | 43 / 100 | 4532 / 8040 | 6638 / 9317 | 7490 / 11933 | 15853 / 19089 | 26298 / 32065 |

A formation part-way through the upgrade is not as clean, either way. With one node
left on v1.1.0 the two rows are again the same up to 20 nodes (at 30: a median of
23.9 s with the probe and 24.6 s without, 33.7 s at worst for both). With half the
nodes on v1.1.0 the formation is still the storm of the 2026-10-12 amendment among
those nodes, and the probe changes its course without clearly improving or worsening
it: fewer false `Dead` per node at every size (0.1 against 0.9 at 12 nodes, 0.7 against
1.6 at 14, 4.8 against 5.5 at 20, 15.4 against 17.0 at 30), and a settling time that is
sooner at 12 nodes (a median of 27 s against 48 s) and later at 14, 20 and 30 (88 s
against 53 s, 148 s against 131 s, 269 s against 257 s), with 1, 3, 1 and 10 runs of
16 still unsettled after five minutes against 0, 1, 3 and 6. A cluster is cold-started
half upgraded only by accident: the roll itself restarts one node at a time into a
formed cluster, which is the next table.

`a_signed_cluster_forms_with_no_false_suspicion_at_any_size` asserts it at 3 to 30
nodes with the new default, and `swim_formation_proc` forms twelve real brokers.

**A cluster part-way through the upgrade from v1.1.0** (14 nodes). A v1.1.0 node
answers the probe and the `Join` as it answers any; it does not send them, and a
`Dead` it declares is adopted by the upgraded nodes like any other.

| | 0.5 s | 1 s | 1.5 s | 2 s | 3 s | 5 s |
|---|---|---|---|---|---|---|
| every node upgraded | 0 | 0 | 0 | 0 | 8 | 30 |
| one node left on v1.1.0 | 1 | 4 | 5 | 4 | 12 | 31 |
| half the nodes on v1.1.0 | 4 | 8 | 11 | 14 | 21 | 31 |
| one node upgraded | 5 | 9 | 15 | 20 | 24 | 32 |
| every node on v1.1.0 | 6 | 10 | 16 | 21 | 25 | 32 |

The benefit arrives as the roll proceeds, and no row is worse than the one below it.

**Packet loss.** Every datagram lost with the given probability, from 30 s after
formation. The probe and its answer are two datagrams and either can be lost; then
the member is declared `Dead` as it would have been. With the probe / without it.
(These two tables are superseded: the two arms of each cell were not run on the same
network, and the cells with a cut did not count what the loss did before it. The
amendment for #1002 below measures them again.)

| nodes | nobody at fault, 75 s: 5 % | 10 % | 20 % | a 1.5 s cut as well: 5 % | 10 % | 20 % |
|---|---|---|---|---|---|---|
| 3 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 1 |
| 5 | 0 / 0 | 0 / 0 | 2 / 2 | 0 / 1 | 0 / 1 | 2 / 6 |
| 10 | 0 / 0 | 0 / 2 | 8 / 28 | 1 / 7 | 1 / 6 | 14 / 32 |
| 14 | 0 / 1 | 1 / 9 | 31 / 32 | 1 / 11 | 5 / 19 | 31 / 32 |
| 30 | 0 / 5 | 10 / 28 | 32 / 32 | 2 / 22 | 17 / 31 | 32 / 32 |

Fewer in every cell, and not none: at 30 nodes a network losing one datagram in ten
still produces a false verdict in a third of the runs within 75 s. That is the window
not growing with the cluster, which this amendment does not change.

A member that really died is buried by every survivor in every run at every loss rate
(ms, median / worst of 16; with the probe, then without):

| loss | 3 nodes | 5 | 10 | 14 | 30 |
|---|---|---|---|---|---|
| 5 % | 4135 / 5019 | 4614 / 5246 | 5557 / 7602 | 5719 / 7254 | 6662 / 8498 |
| | 4135 / 5019 | 4359 / 4946 | 5471 / 6914 | 5294 / 7254 | 6441 / 7799 |
| 10 % | 4271 / 5098 | 4409 / 6364 | 5487 / 7465 | 5125 / 7292 | 6250 / 9316 |
| | 4060 / 5010 | 4073 / 6362 | 5410 / 7165 | 4689 / 9800 | 5880 / 7471 |
| 20 % | 4135 / 13071 | 4655 / 5347 | 4953 / 6167 | 5988 / 9463 | 9550 / 28956 |
| | 4089 / 12697 | 4203 / 5346 | 5029 / 6704 | 5694 / 9149 | 16305 / 33968 |

**A member reachable one way only.** *Deaf*: nothing reaches it, what it sends
arrives (its own probes, and acks to nothing). *Mute*: it hears everything and nothing
it sends arrives. Neither can complete the round trip, so neither is kept: for good,
every survivor declares it `Dead` in every run (ms, median / worst of 16).

| | 3 nodes | 5 | 10 | 14 | 30 |
|---|---|---|---|---|---|
| deaf, with the probe | 4958 / 5098 | 4852 / 6361 | 5014 / 6467 | 5714 / 9200 | 6747 / 8094 |
| deaf, without | 4658 / 5078 | 4610 / 6361 | 4596 / 6467 | 5313 / 8349 | 6500 / 7762 |
| mute, with the probe | 4090 / 4996 | 4717 / 6982 | 4997 / 6009 | 5613 / 8337 | 7004 / 8346 |
| mute, without | 4009 / 4696 | 4788 / 6682 | 4730 / 5405 | 5313 / 8037 | 6489 / 8347 |

For a while, it behaves as the clean cut does (runs of 32 with a false `Dead`; with
the probe / without it):

| nodes | deaf 1.5 s | 2 s | 3 s | mute 1.5 s | 2 s | 3 s |
|---|---|---|---|---|---|---|
| 3 | 0 / 0 | 0 / 0 | 0 / 4 | 0 / 0 | 0 / 0 | 0 / 3 |
| 5 | 0 / 1 | 0 / 5 | 4 / 16 | 0 / 4 | 0 / 8 | 6 / 22 |
| 10 | 0 / 5 | 0 / 12 | 7 / 23 | 0 / 7 | 0 / 12 | 2 / 30 |
| 14 | 0 / 14 | 0 / 21 | 6 / 28 | 0 / 14 | 0 / 21 | 8 / 28 |
| 30 | 0 / 26 | 0 / 27 | 1 / 31 | 0 / 25 | 0 / 28 | 0 / 31 |

**One link lost one way.** What one node, A, sends one member, B, never arrives;
everything else flows, B's own datagrams to A included. For a minute and for good, at
every size: no `Dead` and no `Suspect` in any run, with the probe or without it. A's
probes of B are rescued by its helpers every round, as they have been since this
record was accepted, so A never suspects B and there is no suspicion for a last probe
to confirm. The probe enters only if some other node's suspicion of B reaches A; then
A's probe goes unanswered — B cannot hear it — and A declares B `Dead` exactly as it
did before, unless B's refutation arrives first, exactly as before. In the mirrored
case (B hears A, A does not hear B) the `Ack` is what is lost, with the same result.
So the probe never keeps, in a node's view, a member that node cannot exchange a
datagram with in both directions.

What this means for [ADR 0087](0087-delivery-across-a-false-dead-verdict.md): a pair
whose membership says `Alive` while the peer link between them (TCP, both directions)
is down is that record's "member without a link" — what A owes B's subscribers is
held, A keeps dialing, and the hold has ADR 0087's bound (D1: eight sweep ticks without
a link until D2 is built; the pending table's bound throughout). That state is as old
as indirect probes and the last probe neither creates nor prolongs it. What the probe
changes for ADR 0087 is how often its machinery is needed at all.

**A member whose datagrams arrive late**, for 20 s (runs of 32 with a `Dead`, nearly
most of them declared while the member was late; with the probe / without it):

| nodes | 200 ms late | 300 ms | 600 ms | 1 s | 2 s |
|---|---|---|---|---|---|
| 3 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 1 |
| 5 | 0 / 0 | 0 / 0 | 0 / 1 | 4 / 10 | 26 / 30 |
| 10 | 0 / 0 | 0 / 0 | 5 / 17 | 25 / 30 | 32 / 32 |
| 14 | 0 / 0 | 0 / 0 | 19 / 30 | 28 / 32 | 32 / 32 |
| 30 | 0 / 0 | 0 / 0 | 32 / 32 | 32 / 32 | 32 / 32 |

Up to 300 ms late it is not suspected at all. From 600 ms it answers neither the
direct probe nor the indirect one in time (250 ms each) and is suspected by every
node, round after round; its answer to the last probe is late too. The probe helps
where the late `Ack`'s refutation still arrives before the verdict, and does not make
a cell worse.

**Real processes.** Twelve brokers with signed gossip on loopback, one stopped
(`SIGSTOP`) for 2 s and resumed, repeatedly
(`a_broker_stopped_for_two_seconds_is_declared_dead_by_no_one`, nightly tier: a
verdict is a matter of deadlines, and a loaded per-PR runner misses them for reasons
of its own). On a development machine: 68 stops over ten runs (three of four stops
each, then seven of eight), the stopped broker suspected by a peer in 50 of them and
declared `Dead` in none. With the probe
switched off, four runs of four stops each produced one `Dead`. That is fewer than the
simulation's cut would suggest, because a stopped process is not a cut network: the
kernel queues what arrives, and the broker answers all of it the moment it resumes.

### Who can answer the probe, and what it can be made to send

The probe is an ordinary `Ping` answered by an ordinary `Ack`, accepted on the terms
every `Ack` has been accepted on since [ADR 0003](0003-gossip-authentication.md) and
[ADR 0022](0022-signed-gossip.md): the rule adds a use of that check, not a check.

- **Signed gossip.** An `Ack` is the suspect's when the datagram is signed with the
  suspect's certificate and names the suspect as its sender. A holder of the cluster
  key without that key cannot produce one (dropped as `auth`); one with a certificate
  of its own cannot claim to be the suspect (dropped as `identity`); a member in good
  standing can send the probe's sequence number only as itself, which is not the
  suspect's answer. Tested at the driver:
  `only_a_datagram_signed_by_the_suspect_answers_its_last_probe`. So a member that is
  really dead cannot be kept by anyone who does not hold its private key.
- **Replay.** An `Ack` counts only if it carries the sequence number of this probe,
  drawn from the prober's own counter when the probe is sent. An attacker who can
  record the suspect's datagrams and inject datagrams holds, once the suspect is
  dead, only `Ack`s it sent before: to this node (to earlier, lower numbers — or to
  the same number, if this node has restarted since and its counter began again) and
  to other nodes (a datagram is not bound to its destination). Anti-replay
  ([ADR 0023](0023-gossip-anti-replay.md)) drops what this node has already received
  and anything older than its window; it does not drop a recent datagram that was
  addressed elsewhere. Each replayed `Ack` with the right number buys one more
  window, from a finite stock. This is the exposure of the ordinary probe, by which
  the same attacker could keep the member from being suspected at all, and not a new
  one. (From the code; not tested.)
- **The shared-key posture** (no signatures) trusts every key-holder with every
  statement, this one included: a key-holder could already answer the ordinary probe
  or forge the refutation.
- **What it can be made to send.** One `Ping` per suspect per lapsed window, of the
  usual bounded size (`the_last_probe_is_one_datagram_per_suspect_per_window`), to the
  address the member is known at; one `Ack` in return; and, where that `Ack` carries
  no refutation, one `Join` and its `Sync`. A node holding every other member suspect
  therefore sends at most N−1 probes per window (1.5 s at the shortest), against the
  one probe a second it sends anyway. Making a node hold members suspect takes a
  datagram it accepts — in the signed posture, a member's — and such a member can
  already do worse: without the probe the same lie ends with those members declared
  `Dead`. A datagram from a key-holder whose certificate is not cached reaches no
  rule here (the 2026-10-12 amendment's bounded certificate request is all it draws).

### What this does not change

- **A `Dead` a node is told about is adopted unchecked**, including about a member it
  can reach. That is where the 3 s cells and the mixed-version rows come from.
- **The window does not grow with the cluster.** Under packet loss that still shows
  from about 14 nodes up.
- A v1.1.0 node in a mixed cluster does not probe.

The first two stay with #975 (0087-T10b).

### Tests

- `a_member_cut_off_for_up_to_two_seconds_is_not_declared_dead` — the bound: at 5, 14
  and 30 nodes a clean cut of up to 2 s ends in no `Dead` and a whole view, over the
  suite's seeds (the 32-seed table above is zero from 3 to 30 nodes at 0.5 s to 2 s),
  and a real death is still buried. It replaces
  `a_short_cut_still_ends_in_a_false_dead_on_the_default_window`.
- `without_the_last_probe_a_short_cut_ends_in_a_false_dead` — the old behaviour is
  what `confirm_before_dead = false` gives, on this tree and in the v1.1.0 model.
- `a_v1_1_node_still_declares_what_the_last_probe_would_have_spared`,
  `a_member_that_cannot_complete_a_round_trip_is_declared_dead_as_before`,
  `one_link_lost_one_way_raises_no_verdict`,
  `packet_loss_neither_keeps_a_dead_member_nor_adds_false_verdicts`.
- The simulations (`swim_sim.rs`) and the real-socket cluster suites run the default.

## Amendment (2026-10-13): the last probe when the prober itself was stalled; whose `Ack` counts; the bound, derived (issue #1002, ADR 0087 T10a)

An independent read-through of the amendment above found no regression and seven
things to harden. This is those seven: what was wrong, what changed, and the tables
measured again. Nothing is added to the wire and a v1.1.0 node interoperates as before.

### A node that was itself stalled

**What was wrong.** The driver's loop waits on a tick, every 100 ms, and on a datagram,
and `select!` takes one of the ready branches at random. A node that is not scheduled
for longer than the 250 ms ack timeout after sending a last probe wakes with both
ready: the tick that is overdue, and the suspect's `Ack` in the socket. Half the time
it took the tick, found the probe's deadline past, and declared the suspect `Dead`
with the answer unread. A saturated broker is the case the probe exists for — the
README records a busy broker held dead for about 30 s near saturation — and there the
probers are busy too. Neither test tier could see it: the simulation's nodes never
stalled, and the nightly test stopped the suspect.

**Membership runs on the data path's runtime.** `main` builds one multi-threaded tokio
runtime and the SWIM driver is a task spawned on it, beside every connection, the hub
and the peer links. Nothing reserves a thread for it, so a broker with all its workers
busy does keep the driver off the CPU.

**Decision.** Four properties, each with its test.

- **P1. A node never declares `Dead` while the suspect's `Ack` is in its socket.**
  A tick first reads what the socket holds, then advances the state machine
  (`swim_driver::run_gossip`). Deadlines are acted on only by ticks, so an `Ack` that
  arrived is never beaten by the tick that would have given up on it, however late
  that tick is. The read asks the kernel, on a second handle on the socket, and not
  the runtime: tokio reads a socket only once its reactor has seen it become
  readable, and a process resumed from a stop finds its reactor's poll interrupted,
  with no events, and its timers due — the tick then runs with a socket full of
  datagrams the runtime believes empty. (Found by the nightly scenario below, once the
  stall clock that had been hiding it was gone: six runs of ten, the stopped prober
  woke, read "nothing" through the runtime, and declared dead a suspect whose `Ack`
  was in the socket.) The read is bounded so that a flood cannot keep the tick
  from running: 64 datagrams are read whatever the time (the time budget is wall
  time, and a loop preempted in the middle of its read must not find the budget
  spent one datagram short of the `Ack`), then up to 1024 within a quarter of the
  tick. Missed ticks are not made up (`MissedTickBehavior::Delay`).
- **P2. The state machine's clock is real time.** It is the monotonic clock, late
  wake-up or not. A late node acts on each deadline at its first wake-up at or after
  it, so each of the five steps to a verdict — the probe round begins, the ack
  timeout, the indirect timeout, the suspicion window, the last probe — comes at
  most one gap between wake-ups after it was due, and each step's deadline is set
  from when the step before was taken: **a node whose wake-ups are *g* apart (the
  tick plus its lateness) holds a member that really died `Dead` at most 5 × *g*
  later than a node running on time**, counted from the round in which it probes
  that member, and no lag builds up beyond those five gaps. Waking 370 ms late
  (*g* = 470 ms, lined up with no timeout and not with the period) that is 2.35 s on
  a nominal 5.9 s; measured at the driver, 7.05 s. The rounds *before* the member's
  turn need their own sentence. The probe period used to be counted from the
  moment a round started, so a late node started each round one lateness later
  than the last and a member whose turn was several rounds off waited that many
  latenesses — in every release, not from this change. The period is now kept
  from when a round was due (a node a whole period or more behind starts again
  from now and makes up no rounds), so those rounds are each at most one gap late
  and their lateness does not add up. The certificate-request rate limit and the
  tombstone are on the same clock and are not stretched: a tombstone is pruned
  `dead_ttl` after the verdict give or take one gap, which is what the link
  manager's `held_dead_dial_window` (twice `dead_ttl`, on that clock) assumes.
- **P3. A stall does not touch awareness.** The local-health score of §2 rises when a
  node has to refute a suspicion of itself and falls with each probe a peer answers.
  It is not raised by a stall of the node's own driver. A node whose every probe
  target is dead has no clean round to lower it by, and must still reach `Dead`
  inside P2's bound.
- **P4. None of this keeps, or costs, a verdict.** A prober stalled on its last probe
  gives up on no suspect that answered; a suspect that stayed silent is declared
  `Dead` on the stalled prober's first wake-up.

A stall — a wake-up more than two ticks after the last (`StallWatch`) — is counted,
with its length, and changes nothing else.

**What was tried first, and withdrawn.** The first version of this amendment also left
stalled time out of the state machine's clock (a late wake-up counted as one tick,
however late) and raised awareness by one on every stall. An independent review found,
and the simulation then measured, that the two together make a persistently late node
blind: its clock ran at a fraction of real time and the lag was never made up, and
its awareness climbed to the cap with no clean probe round to bring it down, each rise
lengthening the window under the last probe that was out. With every wake-up 300 to
500 ms late, a lone survivor took 64 s to see its only peer's death, against 5.7 s, and
14 and 30 survivors saw it only when the lateness ended (the table below). The
tombstone stretched with everything else, out from under the dial window that assumes
it. Neither mechanism was needed for the finding: once the socket is read ahead of the
tick, an `Ack` that arrived is seen first whatever the clock says, and a node's own
lateness cannot make it misjudge a peer that answered. (The stall clock had been
doing one thing nobody had asked of it: covering for a read that went through the
runtime and so, after a stopped process resumed, read nothing. With the clock gone the
nightly scenario failed, and the read was made to ask the kernel — P1.) Lifeguard's local-health score
exists for implementations that time a probe out without looking at what has arrived;
here the read is the remedy and it costs no time.

What a stall of the prober did to each timer, and does now:

| Timer | The loop before | Now |
|---|---|---|
| ack timeout of an ordinary probe (250 ms) | tick first: its helpers were asked although the `Ack` was in the socket | the `Ack` is read first |
| indirect timeout (250 ms) | tick first: the member was suspected although its rescue was in the socket | the rescue is read first |
| suspicion window (4 s to 1.5 s) | tick first: on v1.1.0's rule, `Dead` with the refutation in the socket | the refutation is read first; if there is none, the last probe is sent |
| ack timeout of the last probe (250 ms) | tick first: `Dead` with the `Ack` unread — the finding | the `Ack` is read first; if there is none, `Dead` |
| tombstone, protocol period, rate limits | real time | real time |
| catch-up ticks | one per missed tick, back to back, each a fresh toss against the next datagram | none |

One step of a probe is taken per tick, and each sets its deadline from the moment it
is taken, so a stall cannot walk a member from `Alive` to `Dead`: waking after any
length of time, a node asks helpers, or suspects, or asks the last time — and then
waits the full timeout for that, awake.

**What the bound on the read gives an attacker.** Without the gossip key every
datagram fails authentication before a byte is decoded, so a sender can only spend the
budget: a flood that keeps more than 1024 datagrams, or (past the first 64) 25 ms of
HMAC work, queued at a tick leaves datagrams unread behind it at that tick — the order
every tick had before this change — and it cannot stop the tick or forge an `Ack`.
That flood already displaces real gossip in the kernel's buffer; it is the port's
exposure to a network that admits strangers, not a new one. With the key but not the
suspect's signing key (the signed posture) the same holds at a higher price per
datagram. In the shared-key posture a key-holder can forge the `Ack` itself, as before.

**What the read does not cover.** An `Ack` the kernel dropped because the node was
stopped for so long that its receive buffer filled (tens of seconds of gossip) is not
there to be read, and the node gives up on that probe on waking. A node away that long
has been declared dead by everyone else, and what it holds about anyone is stale; no
allowance is made for it.

A narrower case is left open as well. After a stall of several seconds in a large
cluster with signed gossip, more than 64 datagrams can be waiting, each costing a
signature check to read; the `Ack` can then sit past the 64-datagram floor while the
25 ms budget is spent on verification, and the tick runs with it unread. That is a
much narrower false `Dead` than the one this amendment closes — it needs the stall,
the backlog and the `Ack` behind it at once — and it is not zero.

Where the second handle on the socket cannot be had (the process is out of file
descriptors when the driver starts), the read goes through the runtime and P1 does
not hold after a stop; the driver says so once, at WARN, when it starts.

**Should membership have a thread of its own?** P1 stops a late node from blaming a
peer that answered. Nothing here makes a late node answer: a broker whose runtime
does not schedule the SWIM task for a second answers no probe for a second, and
beyond two seconds its peers may rightly conclude what the README records. A dedicated
thread with its own small runtime would keep the acks going while the workers are
busy, at a price that needs deciding rather than assuming: membership would then say
`Alive` about a broker that is not moving data, which is ADR 0087's member without a
link and has its own bound there. That is not built here. It is written up for
0087-T10b (#975) with the measure to decide it by: `mqttd_swim_stalls_total` and
`mqttd_swim_stalled_milliseconds_total` on the scale rig at the rungs where the flap
was seen.

### The `Ack` must come from the life that is suspected

The last probe's `Ack` was matched on the sender's id and the probe's sequence number,
not on the generation. A node restarted in place with a *lower* generation — its
host's wall clock stepped back across the restart; the generation is the wall clock at
start (the 2026-08-05 amendment) — acked the probe of its previous life and was spared, but nothing
it said about itself could clear the suspicion, since every claim of a lower
generation is discarded as a previous life's. It stayed `Suspect` for good: one `Ping`
and one `Join` per window, never `Dead`, never pruned, its address never updated;
before the last probe existed it went `Dead`, was pruned, and was learned again about
35 s later.

The `Ack` now has to carry the generation held for the member. One from an earlier
life does not answer the probe: the held life is declared `Dead` when the probe runs
out, the tombstone is pruned, and the process that is actually there is admitted at
the generation it runs (`an_ack_from_an_earlier_life_does_not_answer_the_last_probe`).

**What a duplicate node id looks like.** Two processes configured with one id, on
different hosts, are two lives of one member, and the cluster holds the member at the
higher generation. That one's claims are believed. The other's datagrams are accepted
as coming from a member — gossip it relays about third parties is merged — but what
it says about itself is dropped, the routing address stays the higher life's, and the
member's SWIM address follows whichever of the two spoke last. An ordinary probe is
answered by whichever process is at that address, so the member stays `Alive` while
either runs. What this amendment changes is the end of it: once the member is
suspected, only the higher life's `Ack` keeps it, so when that process is gone the
member is declared `Dead` and, a tombstone later, the cluster learns the process that
is left as what it is. Ordinary acks and the address refresh remain blind to the
generation; that is the 2026-08-05 amendment's residual and not changed here.

### Five details of the state machine

- **One datagram per window is now true.** A relayed `Suspect` at a higher incarnation
  dropped the probe that was out while the window kept running: the `Ack` in flight
  was ignored and a second probe went out on the next tick. The probe now stands until
  the member stops being suspected or starts a new life
  (`a_suspicion_at_a_higher_incarnation_leaves_the_last_probe_standing`).
- **A probe overtaken by a longer window is sent afresh.** If the window grows while a
  probe is out — the node's awareness rose, or the suspecters were reset — the verdict
  is no longer due. The probe used to stay on record, and when the longer window
  lapsed the member was declared `Dead` on the silence of a probe sent a window
  earlier. It is now dropped when it runs out, and the lapse sends a new one
  (`a_probe_overtaken_by_a_longer_window_is_sent_afresh_when_it_lapses`).
- **A member spared on a stale claim is cleared at once.** With the probe left
  standing, its `Ack` can spare a member whose suspicion has since moved to a higher
  incarnation; the member, already there, ignores the probe's old claim and says
  nothing about itself. The `Join` it is then sent used to carry only the asker's own
  claim, the `Sync` answered `Alive` at the incarnation already suspected, which
  supersedes nothing, and the member sat `Suspect` for another whole window. The
  `Join` now carries the suspicion as it is held at that moment; the member refutes
  it and its `Sync` clears it
  (`a_suspect_spared_on_a_stale_claim_is_asked_with_the_claim_held_now`).
- **The probe period is kept from when a round was due.** It was counted from when the
  round started, so a node that woke late pushed every later round back by its
  lateness (P2). Late by less than a period, the next round is now due one period
  after the last was; a whole period or more behind, the node starts again from now.
  A node on time is unaffected. It is a rule a v1.1.0 node lacks, so like the others
  it is a `Config` field (`period_from_schedule`, on; not operator configuration)
  that the simulation's v1.1.0 model switches off
  (`the_probe_period_runs_from_when_a_round_was_due_not_from_a_late_tick`).
- **The last probe spends no gossip.** It was built like any datagram and so drew up
  to six updates from the gossip queue, each of which has a bounded number of
  retransmissions; a node whose windows for many suspects lapsed on one tick spent
  them on members that might not be there, its own refutation first. The probe now
  carries the suspicion it is about and nothing else
  (`the_last_probe_is_one_datagram_per_suspect_per_window`).

### The bound, derived

"Up to two seconds" is arithmetic. A node declares a member `Dead` on its own account
only when a last probe runs out unanswered, so if every last probe is sent after the
cut has ended the member answers each, and there is no `Dead` for anyone to adopt.
With the cut beginning at *c* and a datagram taking up to *L* each way:

1. The earliest probe the cut leaves unanswered started at *c* − 2*L* (its `Ping`
   arrived just before the cut and its `Ack` just after).
2. No deadline is acted on before it falls due. So the helpers are asked no sooner
   than one ack timeout after the probe started, and the member is suspected no
   sooner than one more: start + 500 ms.
3. The window is never shorter than its floor of 1500 ms, and local health only
   lengthens it. No last probe is sent before start + 2000 ms.

**In a broker, a member cut off for up to 2 s less twice the network's latency is
declared `Dead` by no node** — whatever the phase of each node's ticks and however
late they come. That is what is promised, and it is just under the two seconds the
documents used to state without the qualification.

The simulation's zeros reach further, to 2098 ms, and the reason is its grid, not the
detector: its ticks land exactly 100 ms apart, so a 250 ms deadline is acted on at
300 ms, twice, and with *L* = 1 ms the earliest last probe is at *c* + 2098 ms. A
broker's ticks are on no grid — the timer fires late, the loop is scheduled late, and a
late tick shifts every one after it — so a tick 50 ms late acts on that deadline at
250 ms and the margin is gone. Both figures are pinned as arithmetic
(`a_member_cut_off_for_up_to_two_seconds_is_not_declared_dead` for the grid's 2098 ms;
`with_ticks_on_no_grid_a_cut_of_two_seconds_less_twice_the_latency_ends_in_no_dead`
for 1998 ms with every tick 0 to 99 ms late), each computed from the broker's own
timeouts and failing with the derivation when one changes. Between the two a verdict
needs a schedule in which a prober's ticks land on its deadlines and two more nodes
suspect in step; the table further down sweeps for it.

The simulation's tick and the broker's were two constants that happened to agree. They
are now one (`swim_driver::TICK`).

### Counters

- `mqttd_swim_last_probes_total{outcome}` counts the last probes a node has `sent`,
  those `spared` (the suspect answered: its `Ack` to that probe, or its own refutation
  arriving first-hand while the probe was out) and those `silent` (the member was
  declared `Dead`). A member, or a path to one, that goes quiet for seconds at a time
  and comes back shows as `spared` climbing; the `suspect` gauge shows it only if it
  is read at the right moment. No member is named — the membership metrics are not
  labelled by peer either — so the series stay three.
- `mqttd_swim_stalls_total` counts the stalls the driver observed in itself and
  `mqttd_swim_stalled_milliseconds_total` their length: the rate of the second over
  1000 is the fraction of time the membership task was not running, and the second
  over the first is the length of a stall — 201 ms and 20 s are different findings.
- `mqttd_swim_awareness` is the local-health score of §2: 0 for a healthy node, up to
  8, and the node's ack timeouts and windows are (1 + it) times the release's.

### Evidence, measured again

From the same three reports, on the tree that makes these changes.

**How the packet-loss cells are measured now.** Two things were wrong with them.
Which datagrams were lost was the next draw from the generator that also supplied
latency, so its position depended on how many datagrams had been sent: the two arms
of a comparison diverged at the first extra datagram and from then on were run on
different networks. On a lossy network both the loss of a datagram and its delay are
now functions of the seed, the directed link, the millisecond and the datagram's
place among those offered on that link in that millisecond, and of nothing a node
does: two detectors on one seed are on one network. (On a lossless network the delay
is still the next draw from the run's generator, as every lossless table on record
was measured; the two arms of such a cell are the same run only until one of them
sends a datagram the other does not, and are not presented as more.) And the "loss
with a cut" cells started the loss 30 s after formation, the cut at 60 s and the
counting at the cut's end, so what the loss did in between was in no column and the
member cut off could already be a tombstone. Loss now begins when the fault does, a
minute after formation, with or without a fault.

**A clean cut**, with the 2.5 s column. With the probe / without it.

| nodes | cut 0.5 s | 1 s | 1.5 s | 2 s | 2.5 s | 3 s | 5 s |
|---|---|---|---|---|---|---|---|
| 3 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 3 | 32 / 32 |
| 5 | 0 / 0 | 0 / 0 | 0 / 3 | 0 / 7 | 0 / 14 | 6 / 18 | 29 / 32 |
| 10 | 0 / 4 | 0 / 4 | 0 / 7 | 0 / 13 | 1 / 24 | 3 / 25 | 31 / 32 |
| 14 | 0 / 6 | 0 / 10 | 0 / 16 | 0 / 21 | 1 / 23 | 8 / 25 | 30 / 32 |
| 30 | 0 / 6 | 0 / 20 | 0 / 25 | 0 / 28 | 0 / 30 | 0 / 31 | 26 / 32 |

The columns of the earlier table are what they were, to the run, except one cell (30
nodes at 5 s: 26, from 28). The "without" side is identical in every cell, which is the
check that the simulation's changes did not move a lossless run. At 2.5 s the bound is
past, and it shows: one run of 32 at 10 and at 14 nodes.

How often the last probe is what decides it — probes sent / answered by the suspect /
run out in silence, over the 32 runs of each cell above:

| nodes | cut 0.5 s | 1 s | 1.5 s | 2 s | 2.5 s | 3 s | 5 s |
|---|---|---|---|---|---|---|---|
| 3 | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 | 3 / 3 / 0 | 63 / 4 / 47 |
| 5 | 0 / 0 / 0 | 0 / 0 / 0 | 5 / 5 / 0 | 11 / 11 / 0 | 18 / 17 / 0 | 32 / 22 / 10 | 118 / 41 / 70 |
| 10 | 5 / 5 / 0 | 6 / 6 / 0 | 12 / 12 / 0 | 34 / 34 / 0 | 63 / 62 / 1 | 71 / 65 / 4 | 235 / 93 / 114 |
| 14 | 12 / 12 / 0 | 27 / 27 / 0 | 35 / 32 / 0 | 58 / 57 / 0 | 76 / 75 / 1 | 94 / 80 / 12 | 308 / 146 / 126 |
| 30 | 15 / 15 / 0 | 61 / 58 / 0 | 121 / 115 / 0 | 186 / 176 / 0 | 211 / 204 / 0 | 266 / 255 / 0 | 540 / 375 / 126 |

(Sent less answered less silent is the probes overtaken while out: a refutation or a
`Dead` arrived by gossip first.) Up to 2 s no probe is silent, at any size.

**The price** is what it was: the milliseconds until the last survivor holds a member
that really died `Dead` are the earlier table's, to the millisecond.

**A cluster part-way through the upgrade from v1.1.0** (14 nodes).

| | 0.5 s | 1 s | 1.5 s | 2 s | 2.5 s | 3 s | 5 s |
|---|---|---|---|---|---|---|---|
| every node upgraded | 0 | 0 | 0 | 0 | 1 | 8 | 30 |
| one node left on v1.1.0 | 1 | 4 | 5 | 4 | 10 | 12 | 31 |
| half the nodes on v1.1.0 | 4 | 8 | 11 | 14 | 17 | 21 | 31 |
| one node upgraded | 5 | 10 | 15 | 21 | 23 | 24 | 32 |
| every node on v1.1.0 | 6 | 10 | 16 | 21 | 23 | 25 | 32 |

Two cells of the fourth row moved by one run (9 and 20 before). No row is worse than
the one below it.

**Packet loss, both detectors on one network.** Every datagram lost with the given
probability from a minute after formation, watched for 75 s; in the right half one
member is also cut off for 1.5 s at that moment, and the 75 s run from the cut's end.
Runs of 32 with a false `Dead`, with the probe / without it.

| nodes | nobody at fault: 5 % | 10 % | 20 % | a 1.5 s cut as well: 5 % | 10 % | 20 % |
|---|---|---|---|---|---|---|
| 3 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| 5 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 1 | 0 / 1 | 1 / 1 |
| 10 | 0 / 0 | 0 / 3 | 10 / 25 | 0 / 3 | 0 / 7 | 10 / 30 |
| 14 | 0 / 0 | 2 / 10 | 30 / 32 | 1 / 10 | 4 / 15 | 25 / 32 |
| 30 | 1 / 6 | 11 / 27 | 32 / 32 | 1 / 22 | 10 / 29 | 32 / 32 |

The reading of the earlier table stands on firmer ground: never more with the probe
than without it, in any cell, and not none — at 30 nodes one datagram in ten lost still
ends in a false verdict in a third of the runs. What the pairing shows that the earlier
table could not: the suspicions raised are the same in both arms (a median of 71 and 71
`Suspect` transitions at 14 nodes and 10 %, 321 and 332 at 10 nodes and 20 %), so the
difference in verdicts is the probe's and not a different network's. And it is the
probe's member by member: in the test's sixteen runs at 10 nodes and 20 %, every
`Dead` a node declared was a last probe of its own that ran out, and every `Dead` a
node adopted was about a member some node had so declared.

A member that really died under that loss is buried by every survivor in every run
(ms, median / worst of 16; with the probe, then without). The earlier table's worst
cases of 29 s and 34 s at 20 % were the accounting, not the detector: the loss had
been running for 30 s before the member died.

| loss | 3 nodes | 5 | 10 | 14 | 30 |
|---|---|---|---|---|---|
| 5 % | 4090 / 4996 | 4753 / 6059 | 4932 / 8183 | 5734 / 8021 | 6817 / 10035 |
| | 4071 / 4696 | 4638 / 5917 | 4867 / 7883 | 5566 / 7449 | 6459 / 10035 |
| 10 % | 4090 / 8145 | 4753 / 6368 | 5008 / 7990 | 5937 / 7521 | 6701 / 10193 |
| | 4071 / 8145 | 4599 / 6164 | 4620 / 7883 | 5453 / 7449 | 6580 / 9814 |
| 20 % | 4098 / 8145 | 5123 / 6060 | 4978 / 7931 | 5313 / 7558 | 7300 / 12936 |
| | 4090 / 8145 | 4863 / 6060 | 4790 / 7931 | 5294 / 7560 | 6580 / 12414 |

**One-way reachability, one link lost one way, a late member.** The earlier tables
stand: the deaf and mute rows are identical (for good: within a millisecond), one link
lost one way still raises nothing, and of the late member's table two cells moved by
one run each (10 nodes: 4 with the probe at 600 ms late, from 5; 29 without it at 1 s
late, from 30).

**A node that stops and resumes** — a stopped process, a suspended VM: its ticks stop
and what is sent to it queues on its socket. It is a silent member and, on waking, a
prober whose deadlines all passed. Runs of 32 with a `Dead`, with the probe / without.

| nodes | stopped 1 s | 2 s | 3 s | 5 s | 10 s |
|---|---|---|---|---|---|
| 3 | 0 / 0 | 0 / 0 | 0 / 0 | 32 / 32 | 32 / 32 |
| 5 | 0 / 0 | 0 / 0 | 1 / 7 | 25 / 26 | 32 / 32 |
| 10 | 0 / 1 | 0 / 6 | 2 / 8 | 28 / 30 | 32 / 32 |
| 14 | 0 / 5 | 0 / 8 | 6 / 16 | 24 / 28 | 32 / 32 |
| 30 | 0 / 6 | 0 / 12 | 0 / 15 | 17 / 29 | 32 / 32 |

Up to 2 s nobody is declared dead, as for a cut, and at 3 s fewer are than for a cut:
what was sent to a stopped node waits for it and is answered when it wakes. From 5 s
it is dead to its peers while it is stopped, rightly: by 10 s every node has buried it
during the stop, with the probe or without. At 5 s the probe still shows, in fewer
runs with a verdict from 10 nodes up and in fewer verdicts reached after the node was
back (10 of 32 runs at 30 nodes, against 28). The same state machine on the loop as it
was gives this table again: a node stopped with nobody suspected has no last probe
out, and its ordinary probe walks one timeout on waking, not two.

**A prober stopped on its last probe** is the case built to hit the finding (P4). A
member is cut off for 2 s; the first node to send a last probe stops, from the instant
it has sent it, for 0.3 s, 1 s or 2 s. The three stall lengths give the same counts,
and so did the first version of this change.

| nodes | runs of 32 in which a prober was so stopped | of those, it gave up on waking: this tree | the loop before |
|---|---|---|---|
| 3 | 0 | – | – |
| 5 | 7 | 0 | 7 |
| 10 | 12 | 0 | 12 |
| 14 | 17 | 0 | 17 |
| 30 | 28 | 0 | 28 |

On the loop as it was, every one of those runs ends with the prober's suspect `Dead` —
the member that had been cut off (7, 12, 14 and 17 runs) or a third node it had come to
suspect from that member's gossip, which was never at fault at all. On this tree the
prober gives up in none and the member cut off is `Dead` in none. One run of the 64
with a 2 s stall does end in a `Dead`, about the *stopped node*: the member that had
been cut off still held it suspect from its own outage, on a window begun then, and
asked it the last time while it was stopped. That is two faults whose silences add up
past the bound, and it is the suspect's side, not the prober's.

**A real death, seen by survivors that are all late** (P2, P3). A member dies, and
from that moment every survivor's driver wakes 300 to 500 ms late, every time, for
30 s, and does not run in between. Milliseconds until the last survivor holds the
member `Dead`, median / worst of 16. A cluster of two is the hard case: the one
survivor has nobody to probe but the corpse.

| | 2 nodes | 3 | 14 | 30 |
|---|---|---|---|---|
| every survivor on time | 4934 / 4994 | 4089 / 4996 | 5613 / 8337 | 7005 / 8347 |
| late, the loop before this change (#1000) | 5716 / 6055 | 5407 / 5853 | 6455 / 8554 | 7229 / 8930 |
| late, the first version (stall clock, awareness on a stall) | 63805 / 64003 | 47193 / 49097 | 38167 / 40504 | 38421 / 39357 |
| late, as built but with the period counted from each late start | 5716 / 6055 | 5685 / 6088 | 7482 / 10498 | 8787 / 11176 |
| late, as built | 5716 / 6055 | 5681 / 6088 | 6993 / 8859 | 8024 / 9556 |

The first version saw nothing until the lateness was over. As built, the lone survivor
is 0.8 s late in the median and 1.1 s at worst, inside P2's five gaps. At 14 and 30
nodes two more things add to it: the rounds a survivor spends on other members before
the dead one's turn, and gossip handed from one late node to another. The first was
compounding — the fourth row is the same code with the probe period counted from each
late start, as every release has counted it — and keeping the period to its schedule
takes 0.5 s and 0.8 s off the median and 1.6 s off the worst case. What is left is
1.4 s and 1.0 s in the median and 0.5 s and 1.2 s at worst over a cluster on time: a
quarter more at most. The loop before this change is still quicker there (with the
same period rule it gives 6185 / 7778 at 14 nodes and 7025 / 8274 at 30), and for a
bad reason: reading its socket after the tick, it raised ten times the suspicions
over those runs (247 `Suspect` transitions a run at 14 nodes against 25) — of healthy
peers that were merely late — and the third more datagrams that cost carried the news.

**Where the bound ends off the grid.** One member cut off, 32 seeds a cell, with every
node's ticks 0 to 99 ms late each time for the whole run, and again with every node's
ticks on a 50 ms grid (every instant a tick up to 50 ms late can land on, the 250 ms
deadlines included): no run with a `Dead` at 1.9 s, 1.998 s, 2 s, 2.05 s, 2.098 s or
2.2 s, at any size from 3 to 30, either way; at 2.5 s, one or two runs in 32 at 10
nodes and up, as on the grid. The sweep does not find the worst schedule — it needs a prober's ticks on
its deadlines and two more suspecters in step — so what is promised is the arithmetic,
2 s less twice the latency, and not the sweep.

**Formation.** Re-run. The upgraded rows are the earlier table's to the millisecond,
with and without the probe, at 3 to 30 nodes: no `Suspect`, no `Dead`, the same
settling times (a formation raises no suspicion, so no probe is sent, and no node
stalls). One node left on v1.1.0: the same. A cold start half upgraded — the storm
among the v1.1.0 nodes — moved inside its own noise at 20 and 30 nodes (a median of
156 s and 263 s to settle, from 148 s and 269 s; 5 and 8 runs of 16 unsettled, from 1
and 10; 4.8 and 14.5 false `Dead` per node, from 4.8 and 15.4).

**Real processes.** Two nightly scenarios, ten runs of both on a development machine,
on the tree as built. The suspect stopped (twelve brokers, one stopped for 2 s, eight
times a run): 80 stops, suspected by a peer in 62, declared `Dead` in none. The prober
stopped (two brokers; the one whose last probe is out is stopped for 1.5 s while the
other answers, four times a run): the prober was stopped inside its probe's 250 ms in
40 episodes of 40, and gave up in none. That scenario fails in its first episode
against a broker built with the loop as it was, and failed in six runs of ten while
the read ahead of the tick still went through the runtime (P1).

### What this does not change

The three items of the amendment above stand, and T10b keeps the first two. Added to
it by this amendment:

- whether membership should run on a thread of its own;
- awareness earned the old way has the weakness the withdrawn stall rule made plain.
  A node that had to refute a suspicion of itself is slower to accuse for as long as
  its score stands, and the score falls only when a peer answers one of its probes.
  A node left with nobody but dead members to probe keeps it — up to 9× on its
  windows — until someone answers. That is §2 as accepted and as every release has
  run it; `mqttd_swim_awareness` now shows it, and whether the score should also
  decay with time is a question for T10b.

### Tests

- **P1**, at the driver, on a paused clock over a real socket:
  `a_prober_stalled_past_the_ack_timeout_reads_the_ack_before_it_gives_up` (a stall of
  twelve ack timeouts with the `Ack` queued: not `Dead`, and the stall is counted) and
  `an_ack_already_in_the_socket_beats_the_tick_that_would_give_up_on_it` (the race at
  its narrowest, no stall: with the read taken out this fails);
  `the_read_ahead_of_a_tick_is_bounded` (the minimum read whatever the time, and the
  maximum). Those two run on a paused clock, where the runtime's reactor has always
  turned before the tick, so they pin the ORDER (read, then tick) and would pass with
  the read going through the runtime. That the read goes past the runtime is pinned
  by `the_read_ahead_of_a_tick_finds_a_datagram_the_runtime_has_not_noticed`: a
  datagram in the kernel's buffer, no turn of the reactor since, tokio's own read
  says "nothing" and the driver's returns it — switched back to the runtime's read
  it fails — and, with a kernel and a real stop in between, by the nightly scenario.
- **P2 and P3**, at the driver:
  `a_persistently_late_node_declares_a_dead_member_within_five_wake_ups_of_on_time`
  — a node whose only peer is dead and whose every wake-up is 370 ms late declares it
  within the bound (7.05 s against 5.9 s on time), and prunes the tombstone `dead_ttl`
  later give or take one gap; `the_stall_watch_reports_a_wake_up_more_than_two_ticks_late`.
  In the simulation: `a_real_death_is_seen_on_time_by_survivors_that_are_all_late`, at
  2 and 14 nodes.
- **P4**: `a_suspect_that_stays_silent_is_declared_dead_when_the_stalled_prober_wakes`
  at the driver; in the simulation, which can now stall a node (its ticks stop,
  datagrams queue, it resumes) and can run the loop as it was,
  `a_prober_stalled_on_its_last_probe_declares_nobody_dead` — and, on the old loop,
  the prober gives up in every run in which it was so stalled — and
  `a_node_stopped_for_two_seconds_neither_dies_nor_declares`.
- The bound: `a_member_cut_off_for_up_to_two_seconds_is_not_declared_dead` (the
  simulation's grid, 2098 ms) and
  `with_ticks_on_no_grid_a_cut_of_two_seconds_less_twice_the_latency_ends_in_no_dead`
  (what a broker is promised, with ticks randomly late and on a 50 ms grid).
- In the state machine: the five tests named above.
- On real processes, nightly:
  `a_prober_stopped_with_its_last_probe_out_does_not_declare_the_suspect_dead` — two
  brokers; the one whose last probe is out is stopped for six ack timeouts while the
  other answers. Each step waits on something a broker reports. An episode in which
  the test was too slow to stop the prober inside the probe's 250 ms is recognised
  from the prober's own counters and the test's clock — it was not seen stopped
  within an ack timeout of the last scrape that showed no probe, and the probe was
  not spared — and set aside. Against the loop as it was it fails in the first
  episode. The job now keeps the brokers' logs when it fails.
- The three tests that counted seeds
  (`without_the_last_probe_a_short_cut_ends_in_a_false_dead`,
  `a_v1_1_node_still_declares_what_the_last_probe_would_have_spared`,
  `packet_loss_neither_keeps_a_dead_member_nor_adds_false_verdicts`) sweep enough seeds
  that a change to SWIM's traffic elsewhere does not decide them, say what the odds
  are, and where a property could be asserted instead it is: under loss, every `Dead`
  a node declares is a last probe of its own that ran out, and every `Dead` a node
  adopts is about a member some node had so declared.

## Amendment (2026-10-25): a relayed `Dead` is checked before it is adopted, and the window's floor is 2.5 s (issue #975, ADR 0087 T10b)

What the two amendments above left of #975: a member cut off for 2.5 s or 3 s — shorter
than the 4 s window — was still turned `Dead` by some node once it was reachable again
(1 to 8 runs of 32 at 10, 14 and 16 nodes), and a `Dead` a node was told about was
adopted unchecked. Pinned by `a_member_cut_off_for_less_than_the_suspicion_window_is_not_dead_once_reachable`,
which failed on 1.2.0 and now runs in every tier.

### What was wrong, traced run by run

Two mechanisms, in every failing run of that test:

1. **A relayed `Dead` was adopted unchecked**, also about a member the node could reach.
   With three suspecters the window is at its floor, so a node asked the member the last
   time during the cut, heard nothing (correctly: it was cut off) and declared it `Dead`;
   the other nodes adopted that verdict from gossip after the cut had ended, each one a
   further `Dead` about a member that would have answered them at once. In the failing
   3 s runs at 16 nodes this was 1 to 11 adoptions after the cut, per run.
2. **A last probe sent during the cut that ran out just after it** was declared. This one
   no rule about the timeout can remove: a probe sent while the member is cut off is
   lost, so its silence just after the cut ends says nothing different from its silence
   just before. No check of finite length tells a member that is back from one that is
   not when its question was put before the member was back. What can be decided is
   *when* the first question is put.

### Decision

- **`Config::check_relayed_dead`, on.** A `Dead` that arrives second-hand about a member
  this node holds `Alive` or `Suspect`, in the life it holds, is not adopted and not
  passed on. The member is sent one `Ping` carrying the claim — the last probe of the
  amendment of 2026-10-13, on the same terms: only the member's own `Ack` to that probe,
  from the life held, answers it. Silent for one ack timeout, it is declared `Dead` by
  this node, on its own evidence, and the verdict is gossiped from there; answering, it is
  kept, and its `Ack` carries its refutation (a node told it is `Dead` bumps its
  incarnation and leads with the refutation), or, if it does not, the member is sent one
  `Join` carrying the claim and its `Sync` states its incarnation. A last probe already
  out for the member is not sent twice: it now checks the claim too, and its silence is
  a verdict whatever the window says. A refutation arriving by gossip while the check is
  out ends it. Unchanged: a member's own `Dead` (a graceful leave, first-hand), a `Dead`
  about a member this node does not hold, and everything about tombstones.
- **`suspicion_min_timeout_ms` = 2500** (from 1500). §3's fast track stays — three
  independent suspecters still shorten the 4 s window — but not below 2.5 s. With two
  ack timeouts before anyone suspects, no node asks a cut-off member the last time sooner
  than **3 s less twice the network's latency** into the cut, so a cut shorter than that
  has ended before any question is put and nothing is declared at all (the derivation of
  the amendment for #1002, with 2500 for 1500: 2998 ms in a broker, 3098 ms on the
  simulation's grid; `a_member_cut_off_for_up_to_three_seconds_is_not_declared_dead` and
  `with_ticks_on_no_grid_a_cut_of_three_seconds_less_twice_the_latency_ends_in_no_dead`
  pin it as arithmetic).

Each answers one mechanism; the floor alone clears the cells of the test, and the check
is what bounds the damage when a verdict is reached anyway — past the bound, by a node
still on 1.2.0 or 1.1.0 during a roll, under loss, or by an isolated member about the
nodes it could not reach (below). Neither is operator configuration.

### Why it is safe

- **A real death is still declared everywhere.** The check is a fresh probe, not a
  memory of having heard from the member: there is no "recently seen" state that could
  keep a dead member `Alive`. Every node that holds the member gets the verdict at most
  one ack timeout (health-scaled) after it would have adopted it before; and every node
  also runs its own window, which ends in its own last probe. Deaf and mute members are
  declared as before (the probe needs a round trip). Measured below: every survivor holds
  a dead member `Dead` in every run, at every size, also under 10 % loss.
- **A member that one node can reach and the others cannot** is kept by that node and
  declared by the others: each node's verdict rests on its own round trip. That is what
  this record already did a moment later — #383's first-hand refutation pierces the
  tombstone the moment the member's own datagram arrives — so the views were split
  before as well; the node now simply does not flip to `Dead` and back. The others
  converge among themselves, each on its own probe. What the reachable node does with a
  member the others hold `Dead` is ADR 0087 D1's member without a link, seen from the
  other side.
- **The tombstone, refutation and generations** are untouched: the check runs only
  while the member is not `Dead` here, in the life held; a `Dead` at a higher
  incarnation is put to the member and, if it is silent, declared at that incarnation; a
  refutation, relayed or first-hand, ends the check.
- **Signed gossip and replay.** The answer is the last probe's: an `Ack` signed by the
  member, naming its own sequence number. The exposure is the one the 2026-10-13
  amendment states for the last probe and the ordinary probe: an attacker who can
  replay a recent `Ack` of the member with the right number buys one more ack timeout.
- **The wire and the roll from 1.2.0.** No new element: a `Ping` carrying a claim about
  its receiver, an `Ack`, a `Join` and a `Sync`, as every release sends. A 1.2.0 node
  answers the check as it answers the last probe. In a mixed cluster the 1.2.0 nodes
  still declare on the shorter floor and adopt unchecked; the upgraded nodes check their
  verdicts, so what they cost is the 1.2.0 nodes' own (the table below).

### Evidence

All from the formation simulation (`swim_formation_sim.rs`): 32 seeds a cell, one member
of a formed cluster cut off, the cell counting the runs in which some node turned a
member `Dead` (`false_dead_report`, `fault_shapes_report`). The v1.2.0 rows are the
default with both rules off, which is that release's state machine.

**A clean cut.** Runs of 32 with a `Dead`: the default / v1.2.0.

| nodes | 0.5 s | 1 s | 1.5 s | 2 s | 2.5 s | 3 s | 3.5 s | 5 s |
|---|---|---|---|---|---|---|---|---|
| 3 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 6 | 32 / 32 |
| 5 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 6 | 2 / 14 | 28 / 29 |
| 10 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 1 | 0 / 3 | 3 / 16 | 28 / 31 |
| 14 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 1 | 0 / 8 | 2 / 12 | 27 / 30 |
| 16 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 3 | 0 / 5 | 5 / 11 | 26 / 28 |
| 30 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 3 | 23 / 26 |

Each rule alone (runs with a `Dead`, 2.5 s / 3 s / 3.5 s): the check alone, on 1.2.0's
floor, leaves the cells as they were (10 nodes 1 / 3 / 16, 14 nodes 1 / 8 / 12, 16 nodes
3 / 5 / 11) — the declarations there are mechanism 2 — and takes out the adoptions
(after-the-cut runs at 3 s: 3, 8, 5 become 2, 8, 4; `Dead` transitions per run, worst,
at 16 nodes: 14 become 4). The floor alone gives the default's counts in these columns.
Where verdicts are reached anyway, the check keeps them few: at 5 s the worst run has 14
`Dead` transitions at 14 nodes, 16 at 16 and 20 at 30 — with the floor alone 26, 30 and
29, on v1.2.0 26, 30 and 53.

**The price**: ms until the last survivor holds a member that really died `Dead`, median /
worst of 16 seeds.

| detector | 3 nodes | 5 | 10 | 14 | 16 | 30 |
|---|---|---|---|---|---|---|
| **default** | 4910 / 4996 | 5119 / 7482 | 5640 / 6789 | 6099 / 8337 | 6363 / 8221 | 7441 / 8956 |
| v1.2.0 | 4089 / 4996 | 4716 / 6982 | 4997 / 6011 | 5613 / 8337 | 5687 / 7484 | 7005 / 8347 |
| v1.2.0 + the check | 4309 / 4996 | 5019 / 6982 | 5136 / 6589 | 5968 / 8337 | 5981 / 7574 | 7304 / 8646 |
| v1.2.0 + the 2.5 s floor | 4773 / 4996 | 5078 / 7312 | 5571 / 6403 | 5720 / 8337 | 5982 / 7958 | 7063 / 8746 |
| default × `log10 N` | 4910 / 4996 | 5119 / 7482 | 5640 / 6789 | 6417 / 8794 | 7171 / 8704 | 8738 / 10407 |
| memberlist's defaults, no last probe | 15077 / 16079 | 9019 / 17918 | 6838 / 16195 | 7775 / 20079 | 8357 / 20259 | 10869 / 23494 |

0.4 to 0.8 s in the median and up to 0.8 s at worst over v1.2.0; the two rules add up
to it (the check 0.1 to 0.4 s, the floor 0.1 to 0.7 s). (Passing a relayed `Dead` on while it is checked, instead of after, was measured
and saves 0.1 to 0.2 s at 14 to 30 nodes; it was not taken, because during a roll it
hands an unchecked verdict to the nodes that adopt unchecked.)

**A cluster part-way through the upgrade** (14 nodes; runs of 32 with a `Dead`).

| | 1.5 s | 2 s | 2.5 s | 3 s | 3.5 s | 5 s |
|---|---|---|---|---|---|---|
| every node upgraded | 0 | 0 | 0 | 0 | 2 | 27 |
| one node left on v1.2.0 | 0 | 0 | 0 | 3 | 5 | 27 |
| half the nodes on v1.2.0 | 0 | 0 | 0 | 7 | 9 | 30 |
| every node on v1.2.0 | 0 | 0 | 1 | 8 | 12 | 30 |
| one node left on v1.1.0 | 5 | 4 | 10 | 10 | 16 | 30 |
| every node on v1.1.0 | 16 | 21 | 23 | 25 | 28 | 32 |

With one node left on v1.2.0 the 3 s verdicts are that node's own (0 to 2 transitions a
run, against 0 to 8 with every node on v1.2.0).

**Packet loss**, on one network for every detector (the amendment for #1002). Runs of 32
with a false `Dead` within 75 s, the default / v1.2.0; in the right half one member is
also cut off for 1.5 s.

| nodes | nobody at fault: 5 % | 10 % | 20 % | a 1.5 s cut as well: 5 % | 10 % | 20 % |
|---|---|---|---|---|---|---|
| 10 | 0 / 0 | 0 / 0 | 7 / 10 | 0 / 0 | 0 / 0 | 9 / 10 |
| 14 | 0 / 0 | 2 / 2 | 27 / 30 | 1 / 1 | 3 / 4 | 26 / 25 |
| 30 | 1 / 1 | 13 / 11 | 32 / 32 | 1 / 1 | 9 / 10 | 32 / 32 |

Much the same number of runs, and far fewer verdicts in each: at 30 nodes and 20 % loss
the worst run has 192 `Dead` transitions against 361, at 14 nodes 9 against 22, and at 30
nodes and 10 % loss 4 against 31. Two cells are a run or two worse (30 nodes at 10 %, 13
against 11; 14 nodes with the cut at 20 %, 26 against 25). Under 10 % loss every survivor
still holds a member that really died `Dead` in every run, 0.4 to 0.9 s later in the
median than on v1.2.0. Loss is where the window's scaling with the cluster, not either
rule here, would have to act.

**Other fault shapes** (`fault_shapes_report`, runs of 32 with a `Dead`, the default /
v1.2.0). A member that hears nothing, or is not heard, for 3 s: none at 5 to 30 nodes,
against 4, 7, 6, 1 (deaf) and 6, 2, 8, 0 (mute) at 5, 10, 14, 30; for good, it is
declared by every survivor, 0.1 to 1.0 s later in the median. A node stopped for 3 s:
none, against up to 6 at 14 nodes. One link lost one way: none, as before. A member
whose datagrams arrive 0.6 s to 2 s late for 20 s: fewer runs with a verdict in every
cell (at 14 nodes 15 / 19, 20 / 28 and 31 / 32 for 0.6, 1 and 2 s), and no more runs
with a verdict after the fault in any cell (at 30 nodes and 0.6 s, 5 against 15). The bound
with ticks off the grid: with every tick 0 to 99 ms late no `Dead` up to 3.2 s at any
size; on a 50 ms grid one run of 32 at 5 nodes from 3050 ms — past 2998, as the
arithmetic allows. Survivors that are all 300 to 500 ms late for 30 s hold a member that
died `Dead` within 7962 / 9331 ms at 14 nodes (6099 / 8337 on time; the test's bound is
twice on time's worst).

**Not built, measured.** The SWIM paper's `log10 N` scaling on top of the default clears
the 3.5 s column from 14 nodes up and halves the 5 s cells at 14 and 16 (17 runs of 32)
and at 30 (4), for 0.3 s (14 nodes) to 1.3 s (30) more in the median; it is
`Config::suspicion_scales_with_log_n`, still off, and the scaling question stays with
T10b together with the packet-loss cells. A rule that a last probe running out after
the member has answered something else of this node's since it was sent is not
declared (#975's candidate (b)) was built and measured: it changed no cell, because a
member back from a cut seldom says anything to the prober inside one ack timeout, and
it would have let any recent `Ack` of the member, to anything, stand in for the answer
to the probe. Withdrawn.

### What this does not change

- **What lies past the bound.** A cut of 3.5 s still ends in a verdict in a few runs of
  32 (above), and one of 5 s in most.
- **Packet loss** at 14 nodes and more (above).
- **Where membership runs**, and whether awareness decays with time: T10b's other two
  questions, open.

### Tests

- `a_member_cut_off_for_less_than_the_suspicion_window_is_not_dead_once_reachable` —
  2.5 s and 3 s at 10, 14 and 16 nodes, 32 seeds: no `Dead`, during the cut or after.
- `a_member_cut_off_for_up_to_three_seconds_is_not_declared_dead` and
  `with_ticks_on_no_grid_a_cut_of_three_seconds_less_twice_the_latency_ends_in_no_dead`
  — the bound, 3098 ms on the grid and 2998 ms with ticks on no grid, as arithmetic
  (renamed from their two-second forms).
- `a_member_falsely_declared_dead_is_readmitted_within_the_tombstone_ttl` counted the
  isolated member's verdicts about the others being adopted after an 8 s cut as its
  "false deaths"; with the check those are gone (each is put to the member declared,
  which answers), and the test now asks for verdicts reached during the cut that
  outlast it — the mutual evictions it is about.
- In the state machine: `a_relayed_dead_about_a_member_that_answers_is_not_adopted`,
  `a_member_that_acks_a_relayed_dead_without_refuting_is_kept_and_asked`,
  `a_relayed_dead_about_a_silent_member_is_adopted_when_its_probe_runs_out`,
  `a_relayed_dead_at_a_higher_incarnation_is_checked_and_declared_at_it`,
  `a_relayed_dead_refuted_while_it_is_checked_is_dropped`,
  `a_relayed_dead_about_a_suspect_is_checked_by_one_probe`,
  `a_leave_an_unknown_member_and_the_1_2_rule_adopt_a_dead_at_once`,
  `by_default_a_relayed_dead_is_checked_and_the_floor_is_two_and_a_half_seconds`.
- The formation simulation can run a v1.2.0 node (its driver is this tree's; its state
  machine has both rules off) beside a current one; the reports have its rows.
