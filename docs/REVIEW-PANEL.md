# The review panel

A repeatable way to find out what this project looks like to someone who did not
build it.

Five independent reviewers, each given a concrete identity and a real decision to
make, run against the repository in parallel with no shared context. The output is
five verdicts that can be compared across runs — the point is not a single audit
but a **before/after** measurement.

Run it before a release, and again after acting on it. The panel earns its keep
when a verdict changes.

## Why it works

The first run (2026-08-09) produced findings the maintainers could not have
produced, for a specific reason: **reviewers read the documentation as truth and
report what it tells them.** Two of them stated, as a finding, that no tagged
release existed — citing `README.md` — hours after `v0.9.0` was published. Two
others quoted an image size of ~27 MB in a footprint comparison; the real figure
was 13.8 MB.

Neither was a reviewer error. Both were stale claims in our own documents, and the
panel is the fastest known way to surface that class of defect: a stale number in
a comparison table does not stay in the table, it becomes somebody's evaluation.

The same run also found a genuine correctness defect (QoS-0 fan-out bypassing
`MAX_BACKLOG`) and raised a credible question about the breadth of the durability
claim — neither of which was visible from inside the project.

## The rules every reviewer gets

These are load-bearing. Dropping any one of them measurably degrades the output.

1. **Ignore `docs/adr/` and `docs/delivery/` entirely.** They are internal
   engineering records. A real evaluator never reads them, and a reviewer allowed
   to read them will credit the project for intentions rather than for what ships.
   This is the single most important constraint.
2. **Cite file paths and line numbers for every claim.** Vague impressions are
   useless and cannot be acted on.
3. **Verify `COMPARISON.md`'s claims against the repository rather than trusting
   them.** Our own comparison document is evidence to be checked, not a source.
4. **Be harsh.** Reviewers are told they are risking a production platform, or
   their own name, on the decision.
5. **A word limit** (~1200–1400). Forces prioritisation; without it the reports
   pad.

## The five reviewers

Each gets a *specific* production context — cluster size, client count, what they
currently depend on — because a generic "evaluate this broker" prompt produces
generic output. The specificity is what makes the feature-gap analysis real.

| # | Identity | The decision they are making |
|---|---|---|
| 1 | Platform engineer, **EMQX 5.x**, 3 nodes, ~50k clients, durable sessions + dashboard + rule engine | Cost-out: EMQX went BSL 1.1 and clustering is now commercial |
| 2 | IoT engineer, **Mosquitto**, two standalone instances, ~8k devices, TLS client certs, ACL files | Wants HA; failover currently loses queued messages. No licence cost today |
| 3 | Enterprise architect, **HiveMQ Enterprise**, 4 nodes, ~500k vehicles, Enterprise Security + Kafka extensions | Procurement-mandated cost-out. Accountable if it fails |
| 4 | Senior infrastructure engineer, deep Kafka/etcd/Postgres background | Assess engineering **quality**; sceptical of new brokers claiming durability |
| 5 | Startup engineer who has **never run an MQTT broker** | Choosing a first broker, 6 weeks to production |

### Sections each report must produce

**1–3 (the migrators)** — cost-out case (what spend disappears *and* what new cost
appears); migration blockers; feature gaps vs their current broker; operational
readiness; trust signals; verdict with top-5 fixes in priority order.

**4 (quality)** — does the durability claim hold up under code reading; test
*quality* not count, including any tests that cannot fail; operational failure
modes at 3am; code-quality signals; what they would refuse to run; a
trustworthiness score out of 10 with the top-5 engineering concerns.

**5 (newcomer)** — first 10 minutes on the README; time to first message; getting
to production; concepts assumed but never taught; danger zones for a novice;
verdict with the top-5 documentation/UX fixes.

## Running it

**Setup: give every reviewer the full repository metadata.** The 2026-08-13 run
produced a false finding reproduced by three of five reviewers — "the release does
not verify; the only tag is `v0.9.0-rc`" — because the review container's clone
fetched branch heads only, so `git tag` was missing the release. Before a run:
`git fetch --tags` in the review clone (or hand the reviewers the GitHub release
list as input). And as a standing instruction to every reviewer: **release claims
are verified against the GitHub release list, never against local `git tag`** — a
sandbox clone's tag list is an artifact of how the clone was made, not evidence
about the project.

Launch all five **in one batch so they run concurrently**, each with no knowledge
of the others. Independence is the point: findings that three reviewers reach
separately are the ones to act on first. In the first run, all three migrators
independently ranked *missing migration tooling* their top blocker, and all three
independently ran `git log` and weighted the two-contributor history heavily.

Two practical notes from the first run:

- **A reviewer can stop early without producing its report** — one returned a
  status line because it had farmed work out and returned before that resolved.
  Resume it and tell it to answer from what it has already read, without waiting
  on or spawning anything further.
- **Reviewers repeat our stale claims back as fact.** Treat every such repetition
  as a defect report against our documentation, not as a reviewer mistake.

## Reading the results

Sort findings into three buckets, because conflating them wastes the run:

- **Factually wrong in our docs** — fix immediately, same day. These are free.
- **Real gaps already tracked** — the value is the *priority signal*. Three
  independent evaluators agreeing on the top blocker outranks internal ordering.
- **Not in our control** — project age, contributor count, absence of a
  production track record. These are accurate readings of real evidence. Do not
  argue with them and do not paper over them; only time changes them.

Then verify anything that touches a correctness claim **yourself** before acting.
The first run's durability finding was explicitly inferred by the reviewer from
the absence of a counter-test, and it was right to say so — one of its adjacent
claims (that outbound queues are unbounded generally) turned out to need
correcting, because the QoS 1/2 backlog *is* bounded. The unbounded path was
QoS 0 specifically, which is both narrower and more actionable.

The same discipline applies to repository **metadata**, and the hazard there is
environmental rather than inferential: any finding that rests on tags, remotes,
release artifacts, or CI history describes *the reviewer's sandbox* until the
session running the panel re-verifies it against the real repository (a normal
clone, the GitHub release list, the Actions history). The 2026-08-13 run's
three-reviewer "release does not verify" finding was exactly this — true in the
container, false about the project — and the synthesis had to spend a paragraph
refuting our own panel. Correctness claims get re-verified against the code;
metadata claims get re-verified against the hosting platform. Neither is a
defect until it survives that.

## Runs

- **2026-08-09 — first run.** Established the method and this doc's rules. Key
  outputs: the stale-claims class (release status, image size), the QoS 0
  `MAX_BACKLOG` bypass, the durability question that became the acked-facts work,
  and reviewer 5's starting verdict: *"I would not choose this as my first MQTT
  broker."*
- **2026-08-13 — second run** (main @ `bf2084f`). Full distilled report:
  [review-panels/2026-08-13.md](review-panels/2026-08-13.md); every technical
  attack filed with citations under tracking issue #261. Verdicts: reviewers 1–3
  (the migrators) all *not-yet* with re-evaluation windows of 6–18 months, on
  scale evidence, 1.0 policy, and integration gaps — not on correctness; reviewer
  4 scored **7.5/10** trustworthiness; reviewer 5 moved from *"would not choose"*
  to **"cautious yes — single secured node"** (cluster still no; #255/#256 hold
  back the full yes). The run also produced one **false finding** (the tags
  artifact recorded under "Running it" above) — reproduced by three reviewers,
  which is a reminder that independent agreement measures shared *environment* as
  well as shared truth.

**The standing baseline from run 2:** reviewer 4 traced the acked-durability
claim end to end for the first time — `conn.rs` ack-withholding → `hub.rs`
append-before-wire → redb `Durability::Immediate` → self-ack-counts-only-when-
durable in `cluster_log.rs` — and largely upheld it (*"mostly survives code
reading — unusual for the genre"*). The next run should hand reviewer 4 that
traced chain and ask it to **falsify** it (find the arm where an ack escapes
before the durable copy exists), not re-derive it from scratch; re-derivation
spends the reviewer's budget re-earning what is now baseline.

## Done criterion

Re-run the panel and compare verdicts against the previous run. Reviewer 5's
verdict is the sharpest single signal for onboarding: it started at *"I would not
choose this as my first MQTT broker"* (2026-08-09) and stands at *"cautious yes —
single secured node"* (2026-08-13); the next milestone is a yes that includes the
cluster.
