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
it took the tick, read a wall clock on which the probe's deadline had passed, and
declared the suspect `Dead` with the answer unread. A saturated broker is the case the
probe exists for — the README records a busy broker held dead for about 30 s near
saturation — and there the probers are busy too. Neither test tier could see it: the
simulation's nodes never stalled, and the nightly test stopped the suspect.

**Membership runs on the data path's runtime.** `main` builds one multi-threaded tokio
runtime and the SWIM driver is a task spawned on it, beside every connection, the hub
and the peer links. Nothing reserves a thread for it, so a broker with all its workers
busy does keep the driver off the CPU.

**Decision.** Three rules in the driver (`swim_driver::run_gossip`), which until now
was a clock and a socket:

1. **Read before expiring.** A tick first reads everything the socket held when the
   tick came due, then advances the state machine. Deadlines are acted on only by
   ticks, so an `Ack` that arrived in time is never beaten by the tick that would
   have given up on it. The read is bounded, so that a flood cannot keep the tick
   from running: 1024 datagrams (more than Linux's default receive buffer holds of
   gossip-sized ones) and a quarter of the tick, 25 ms.
2. **Stalled time is not time.** The driver feeds the state machine a clock that
   leaves out the time its loop did not run (`StallClock`). The loop wakes at least
   once a tick; a wake-up more than two ticks after the last is a stall and counts as
   one tick, however long it was. A node's clock therefore advances by at most 200 ms
   per wake-up, less than one ack timeout: no deadline can be set and found expired
   without the loop having run, and read its socket, in between. Slow is safe here and
   fast is not — every timer, the tombstone's included, only runs longer in wall time
   than configured.
3. **A stall is a finding about this node.** It raises the local-health awareness of
   §2 by one (`Swim::local_stall`), as having to refute a suspicion does: for the next
   rounds the node waits longer for acks and holds suspects longer, and each clean
   probe round lowers it again. This was not already the case. Awareness rose only
   when a peer had suspected the node and the claim had come back to it, which takes a
   stall of about 0.6 s that some peer happened to probe into, and arrives after the
   harm; a stall the node observes on its own clock is the same evidence, first-hand
   and at once. (Lifeguard raises its score on missed acks and on refutations. An
   unanswered probe is deliberately not a signal here, §2; the clock is.)

Missed ticks are not made up (`MissedTickBehavior::Delay`): after a stall the loop
ticks once and again a full interval later.

What a stall of the prober did to each timer, and does now:

| Timer | The loop before | Now |
|---|---|---|
| ack timeout of an ordinary probe (250 ms) | tick first: its helpers were asked although the `Ack` was in the socket | the `Ack` is read first |
| indirect timeout (250 ms) | tick first: the member was suspected although its rescue was in the socket | the rescue is read first |
| suspicion window (4 s to 1.5 s) | lapsed while the node was away, with the refutation unread: on v1.1.0's rule, `Dead` at once | runs on the node's own clock; the refutation is read first |
| ack timeout of the last probe (250 ms) | tick first: `Dead` with the `Ack` unread — the finding | the `Ack` is read first, and the deadline has not passed |
| tombstone (30 s) | — | longer by the stall, never shorter |
| catch-up ticks | one per missed tick, back to back, each a fresh toss against the next datagram | none |

**What the bound on the read gives an attacker.** Without the gossip key every
datagram fails authentication before a byte is decoded, so a sender can only spend the
budget: a flood that keeps more than 1024 datagrams, or 25 ms of HMAC work, queued at
a tick leaves datagrams unread behind it at that tick — the order every tick had
before this change — and it cannot stop the tick, forge an `Ack`, or make the node
believe it was stalled. That flood already displaces real gossip in the kernel's
buffer; it is the port's exposure to a network that admits strangers, not a new one.
With the key but not the suspect's signing key (the signed posture) the same holds at
a higher price per datagram. In the shared-key posture a key-holder can forge the
`Ack` itself, as before.

**What it costs.** A node that is stalled detects more slowly: its timers pause with
it, and for a few rounds afterwards they are doubled or more. The other nodes are not
slowed, and a `Dead` they reach is adopted as before.

**Should membership have a thread of its own?** These rules stop a stalled node from
blaming its peers. They do not make it answer: a broker whose runtime does not
schedule the SWIM task for a second answers no probe for a second, and beyond two
seconds its peers may rightly conclude what the README records. A dedicated thread
with its own small runtime would keep the acks going while the workers are busy, at a
price that needs deciding rather than assuming: membership would then say `Alive`
about a broker that is not moving data, which is ADR 0087's member without a link
and has its own bound there. That is not built here. It is written up for 0087-T10b
(#975) with the measure to decide it by: `mqttd_swim_stalls_total` on the scale rig at
the rungs where the flap was seen.

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

### Three details of the state machine

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
- **The last probe spends no gossip.** It was built like any datagram and so drew up
  to six updates from the gossip queue, each of which has a bounded number of
  retransmissions; a node whose windows for many suspects lapsed on one tick spent
  them on members that might not be there, its own refutation first. The probe now
  carries the suspicion it is about and nothing else
  (`the_last_probe_is_one_datagram_per_suspect_per_window`).

### The bound, derived

"Up to two seconds" is arithmetic, and its margin is the driver's tick. A node declares
a member `Dead` on its own account only when a last probe runs out unanswered, so if
every last probe is sent after the cut has ended the member answers each, and there is
no `Dead` for anyone to adopt. With the cut beginning at *c*:

1. The earliest probe the cut leaves unanswered started at *c* − 2 ms (its `Ping`
   arrived just before the cut and its `Ack` just after; the simulated network takes up
   to 1 ms each way).
2. A deadline is acted on by the first tick at or after it. The ack timeout runs out
   and the helpers are asked at the first tick 250 ms on, which is 300 ms; the
   indirect timeout, the same again. No node suspects the member before
   start + 2 × ⌈250⌉₁₀₀ = start + 600 ms.
3. The window is never shorter than its floor of 1500 ms, and local health only
   lengthens it. No last probe is sent before start + 2100 ms, which is
   ***c* + 2098 ms**.

A cut of up to 2098 ms has ended before the first last probe is sent; a 2000 ms cut
has 98 ms to spare, and all of it is the tick. With a 50 ms tick the same lines give
2 × 250 + 1500 − 2 = 1998 ms, and a 2 s cut could swallow the probe. So the bound is
stated as **two seconds, ending at about 2.1 s, for the release's tick of 100 ms**,
and the table below gains a 2.5 s column, which is not zero.

The simulation's tick and the broker's were two constants that happened to agree. They
are now one (`swim_driver::TICK`), and the bound's test computes the figure from the
broker's own timeouts, requires it to cover two seconds and to be 2098, runs a cut of
exactly that length, and prints this derivation when any of it fails.

### Counters

`mqttd_swim_last_probes_total{outcome}` counts the last probes a node has `sent`, those
`spared` (the suspect answered: its `Ack` to that probe, or its own refutation
arriving first-hand while the probe was out) and those `silent` (the member was
declared `Dead`). A member, or a path to one, that goes quiet for seconds at a time
and comes back shows as `spared` climbing; the `suspect` gauge shows it only if it is
read at the right moment. No member is named — the membership metrics are not
labelled by peer either — so the series stay three. `mqttd_swim_stalls_total` counts
the stalls the driver observed in itself.

### Evidence, measured again

From the same three reports, on the tree that makes these changes.

**How the packet-loss cells are measured now.** Two things were wrong with them.
Which datagrams were lost was the next draw from the generator that also supplied
latency, so its position depended on how many datagrams had been sent: the two arms
of a comparison diverged at the first extra datagram and from then on were run on
different networks. Loss is now a function of the seed, the directed link, the
millisecond and the datagram's place among those offered on that link in that
millisecond, and of nothing a node does: two detectors on one seed lose the same
datagrams wherever they send the same ones. And the "loss with a cut" cells started
the loss 30 s after formation, the cut at 60 s and the counting at the cut's end, so
what the loss did in between was in no column and the member cut off could already be
a tombstone. Loss now begins when the fault does — a minute after formation, with or
without a fault, so a cell with a cut and the cell beside it without are the same
network and differ by the cut alone.

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
| 5 | 0 / 0 | 0 / 0 | 0 / 1 | 0 / 1 | 1 / 1 | 2 / 7 |
| 10 | 0 / 0 | 0 / 2 | 8 / 27 | 1 / 4 | 2 / 4 | 15 / 28 |
| 14 | 1 / 2 | 1 / 10 | 27 / 32 | 1 / 9 | 2 / 14 | 27 / 32 |
| 30 | 1 / 6 | 11 / 24 | 32 / 32 | 1 / 15 | 11 / 27 | 32 / 32 |

The reading of the earlier table stands on firmer ground: never more with the probe
than without it, in any cell, and not none — at 30 nodes one datagram in ten lost still
ends in a false verdict in a third of the runs. What the pairing shows that the earlier
table could not: the suspicions raised are the same in both arms (a median of 87 and 81
`Suspect` transitions at 14 nodes and 10 %, 347 and 332 at 10 nodes and 20 %), so the
difference in verdicts is the probe's and not a different network's; and with the cut
counted on the same network, no cell with a cut is below the cell without one.

A member that really died under that loss is buried by every survivor in every run
(ms, median / worst of 16; with the probe, then without). The earlier table's worst
cases of 29 s and 34 s at 20 % were the accounting, not the detector: the loss had
been running for 30 s before the member died.

| loss | 3 nodes | 5 | 10 | 14 | 30 |
|---|---|---|---|---|---|
| 5 % | 4089 / 4996 | 4938 / 5955 | 4997 / 5829 | 5238 / 7737 | 6153 / 8347 |
| | 4010 / 4696 | 4647 / 5926 | 4492 / 5628 | 5056 / 7737 | 5964 / 8347 |
| 10 % | 4069 / 5086 | 4619 / 6217 | 4492 / 6729 | 5189 / 7979 | 6494 / 8107 |
| | 4054 / 5010 | 4416 / 5917 | 4373 / 6429 | 4889 / 7979 | 6138 / 7656 |
| 20 % | 4097 / 6079 | 4709 / 6319 | 4552 / 6202 | 5088 / 6980 | 6524 / 8346 |
| | 4089 / 6079 | 4356 / 6319 | 4333 / 6202 | 4808 / 6694 | 6305 / 7762 |

**One-way reachability, one link lost one way, a late member.** The earlier tables
stand: the deaf and mute rows are identical (for good: within a millisecond), one link
lost one way still raises nothing, and of the late member's table one cell moved by
one run (10 nodes, 600 ms late: 4, from 5).

**A node that stops and resumes** — a stopped process, a suspended VM: its ticks stop
and what is sent to it queues on its socket. It is a silent member and, on waking, a
prober whose deadlines all passed. Runs of 32 with a `Dead`, with the probe / without.

| nodes | stopped 1 s | 2 s | 3 s | 5 s | 10 s |
|---|---|---|---|---|---|
| 3 | 0 / 0 | 0 / 0 | 0 / 0 | 32 / 32 | 32 / 32 |
| 5 | 0 / 0 | 0 / 0 | 1 / 7 | 25 / 26 | 32 / 32 |
| 10 | 0 / 3 | 0 / 6 | 2 / 10 | 28 / 30 | 32 / 32 |
| 14 | 0 / 4 | 0 / 9 | 6 / 16 | 24 / 28 | 32 / 32 |
| 30 | 0 / 4 | 0 / 11 | 0 / 18 | 17 / 28 | 32 / 32 |

Up to 2 s nobody is declared dead, as for a cut, and at 3 s fewer are than for a cut:
what was sent to a stopped node waits for it and is answered when it wakes. From 5 s
it is dead to its peers while it is stopped, rightly: by 10 s every node has buried it
during the stop, with the probe or without. At 5 s the probe still shows, in fewer
runs with a verdict from 10 nodes up and in fewer verdicts reached after the node was
back (10 of 32 runs at 30 nodes, against 27). The same state machine on the loop as it
was gives this table again: a node stopped with nobody suspected has no last probe
out, and its ordinary probe walks one timeout on waking, not two.

**A prober stopped on its last probe** is the case built to hit the finding. A member
is cut off for 2 s; the first node to send a last probe stops, from the instant it has
sent it, for 0.3 s, 1 s or 2 s. The three stall lengths give the same counts.

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

**Formation.** Re-run. The upgraded rows are the earlier table's to the millisecond,
with and without the probe, at 3 to 30 nodes: no `Suspect`, no `Dead`, the same
settling times (a formation raises no suspicion, so no probe is sent, and no node
stalls). One node left on v1.1.0: the same. A cold start half upgraded — the storm
among the v1.1.0 nodes — moved inside its own noise at 20 and 30 nodes (a median of
156 s and 263 s to settle, from 148 s and 269 s; 5 and 8 runs of 16 unsettled, from 1
and 10; 4.8 and 14.5 false `Dead` per node, from 4.8 and 15.4).

**Real processes.** Two nightly scenarios, ten runs of both on a development machine.
The suspect stopped (twelve brokers, one stopped for 2 s, eight times a run): 80 stops,
suspected by a peer in 58, declared `Dead` in none. The prober stopped (two brokers;
the one whose last probe is out is stopped for 1.5 s while the other answers, four
times a run): the prober was stopped inside its probe's 250 ms in 40 episodes of 40,
and declared `Dead` in none. Against a broker built with the loop as it was, that
scenario fails in its first episode.

### What this does not change

The three items of the amendment above stand, and T10b keeps the first two. Added to
it by this amendment: whether membership should run on a thread of its own.

### Tests

- At the driver, on a paused clock over a real socket:
  `a_prober_stalled_past_the_ack_timeout_reads_the_ack_before_it_gives_up` (a stall of
  twelve ack timeouts with the `Ack` queued: not `Dead`, and the stall is counted),
  `an_ack_already_in_the_socket_beats_the_tick_that_would_give_up_on_it` (the same race
  with no stall for the clock to catch: the read alone decides it; with the read taken
  out this fails), `a_suspect_that_stays_silent_is_declared_dead_after_the_stall`,
  `the_stall_clock_leaves_out_the_time_the_loop_did_not_run`,
  `the_read_ahead_of_a_tick_is_bounded`.
- In the simulation, which can now stall a node (its ticks stop, datagrams queue, it
  resumes) and can run the loop as it was:
  `a_prober_stalled_on_its_last_probe_declares_nobody_dead` — and, on the old loop,
  declares the member `Dead` in every run in which a prober was so stalled —
  and `a_node_stopped_for_two_seconds_neither_dies_nor_declares`.
- In the state machine: the four tests named above and
  `a_stall_of_this_node_raises_its_awareness`.
- On real processes, nightly:
  `a_prober_stopped_with_its_last_probe_out_does_not_declare_the_suspect_dead` — two
  brokers; the one whose last probe is out is stopped for six ack timeouts while the
  other answers. Each step waits on something a broker reports; an episode in which
  the test was too slow to stop the prober inside the probe's 250 ms is recognised
  (the verdict is in the prober's log before the stop) and set aside. Against the loop
  as it was it fails in the first episode. The job now keeps the brokers' logs when
  it fails.
- The three tests that counted seeds
  (`without_the_last_probe_a_short_cut_ends_in_a_false_dead`,
  `a_v1_1_node_still_declares_what_the_last_probe_would_have_spared`,
  `packet_loss_neither_keeps_a_dead_member_nor_adds_false_verdicts`) sweep enough seeds
  that a change to SWIM's traffic elsewhere does not decide them, say what the odds
  are, and where a property of each run could be asserted instead it is: under loss, no
  run turns a member `Dead` without a last probe having run out in silence.
