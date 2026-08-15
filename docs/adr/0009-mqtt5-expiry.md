# ADR 0009 — MQTT 5.0 session & message expiry

- **Status:** Accepted
- **Date:** 2026-06-17
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0009-mqtt5-expiry.md](../delivery/0009-mqtt5-expiry.md) — plan, progress, and changelog
- **Related:** [ADR 0001](0001-session-durability.md) (session lifecycle/storage),
  [ADR 0005](0005-session-affinity.md) (the owner serves a session),
  [ADR 0008](0008-mqtt-5-codec.md) (the v5 wire that carries these properties)

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0009-mqtt5-expiry.md).

## Context

The v5 wire codec (ADR 0008) is complete; the broker now negotiates v5 but ignores
the v5 *semantics*. The first two are **session expiry** and **message expiry** — the
lifetimes of, respectively, a disconnected client's session state and an undelivered
queued message.

MQTT 3.1.1 has only `clean_session`: `1` discards session state at disconnect, `0`
keeps it **forever**. MQTT 5.0 splits this into two independent controls:

- **Clean Start** (the same CONNECT flag bit) — whether to *resume* an existing
  session at connect (`0`) or start fresh, discarding any prior one (`1`).
- **Session Expiry Interval** (CONNECT/DISCONNECT property `0x11`, seconds) — how long
  to *retain* the session after disconnect: `0` = discard at disconnect,
  `0xFFFFFFFF` = never expire, otherwise a deadline.

**Message Expiry Interval** (PUBLISH property `0x02`, seconds) — a queued message's
lifetime; if still undelivered when it elapses, drop it, and forward the remaining
interval on delivery.

The questions this fixes: how the two v5 controls map onto the broker's existing
`clean_session` lifecycle, where expiry is enforced (especially in a cluster), and how
a message carries its deadline through the store.

## Decision

### 1. Normalize both versions to (clean_start, session_expiry) at the connection edge

The hub speaks only `(clean_start: bool, session_expiry: u32)`; the connection layer
translates each protocol version into that pair:

- **v3.1.1:** `clean_start = clean_session`, and
  `session_expiry = if clean_session { 0 } else { 0xFFFFFFFF }` — exactly reproducing
  "discard now" vs "keep forever".
- **v5:** `clean_start` is the CONNECT clean-start bit; `session_expiry` is the
  `Session Expiry Interval` property (absent = `0`, per spec).

So the hub's lifecycle logic is single, version-agnostic, and the existing v3.1.1
behaviour falls out as the `{0, 0xFFFFFFFF}` special cases — no separate code path.

### 2. The hub owns session lifecycle; expiry is a periodic sweep on the owner

Session lifecycle already lives in the hub (`attach`/`detach`, the durable
`SessionStore`). Expiry extends it:

- **Attach.** `clean_start` discards any existing session first. The session's
  `session_expiry` is recorded, and any pending expiry deadline is cancelled (the
  client is back).
- **Detach.** `session_expiry == 0` discards immediately (the old `clean_session=1`
  path); `0xFFFFFFFF` keeps it indefinitely (the old `clean_session=0` path);
  otherwise the session is kept with a deadline `now + session_expiry`.
- **Sweep.** A periodic tick in the hub actor loop discards every session whose
  deadline has passed (drop subscriptions, in-flight, and `store.remove`).

Discarding is the same operation everywhere (a `discard_session` helper), so the
durable backend's `remove` (which quorum-replicates the deletion) and the in-memory
backend are both covered with one implementation.

**Cluster.** A persistent session is relocated to its placement owner (ADR 0005), so
the **owner's** hub holds it and runs its expiry — no cross-node coordination. *Carried
limitation:* the expiry deadline is in-memory on the owner. If the owner dies and a
replica takes over, the session data survives (it is in the replicated
log) but the deadline is lost — the clock effectively restarts. Persisting the
disconnect time in the session's durable meta snapshot closes this and is a follow-up.

**As delivered — §3's persisted deadline, and the one case it cannot reach
(2026-08-14, issue #284).** The follow-up above was delivered: `detach` persists the
**absolute** deadline through the (group-routed) session store, so a new owner expires an
inherited session at the right wall-clock time. It cannot land, however, when the
detaching node does **not** hold the session group's lease: the write is refused
`NotOwner` by construction. That is now the routine case, because a rehome close (ADR
0005's as-delivered note) happens precisely on a non-owning node. As delivered the write
is therefore **skipped deliberately**, warned, and counted
(`mqttd_session_expiry_unpersisted_total{reason="not-owner"}`) rather than attempted with
its error discarded.

*The residual, stated:* the new owner then holds a session record with no deadline, so a
client that never comes back leaves a persistent session and its queue behind instead of
expiring at its stated interval. It self-heals the moment the client reconnects anywhere
(its CONNECT carries the interval; that owner's next detach persists the deadline) —
~0.1 s in the measured rehome. It cannot be closed inside this seam: only the absolute
deadline is persisted, never the *interval*, so no owner can re-derive it, and no peer
frame carries a session's deadline. The same hole is pre-existing for every takeover of an
**online** session (the deadline is cleared while the client is connected, so a dead
owner's successor inherits none). The follow-up that closes both at once is to persist the
interval alongside the deadline in the durable session record.

**As delivered — the Will Delay Interval and its second bound (2026-08-15, issue #299).**
The Will Delay Interval (`0x18`, §3.1.3.2.2) belongs to this ADR because §3.1.2.11.2 makes
the Session Expiry Interval its other bound. Until #299 the property round-tripped in the
codec with **no reader anywhere outside it**: the will fired the instant the connection
ended, so every client using the property got a spurious "offline" on every brief network
blip — the exact case MQTT 5 added the property to fix. Found by the Eclipse
`paho.mqtt.testing` suite, i.e. by an *independent* oracle: every test this project wrote
itself was green.

*The arithmetic, computed once at the moment the will would have fired:*

```
effective = match session_expiry {           // the hub's own map: non-zero values only
    None | Some(0) => 0,                     // the session ends now → the will fires now
    Some(u32::MAX) => will_delay,            // only the delay bounds it
    Some(secs)     => min(will_delay, secs), // §3.1.2.11.2, whichever comes first
}
if effective == 0 { publish now, on this dispatch }   // pre-#299 behaviour, byte for byte
else              { arm for `effective` seconds }
```

Every edge, stated: an **absent** delay is 0 (the spec's default), so every v3.1.1 client
and every v5 client that does not ask keeps today's behaviour exactly. **Session Expiry
Interval 0** — including a v5 CONNECT with the property *absent*, which §1 above normalizes
to 0 — clamps the delay to 0 and publishes immediately; that is spec-correct and the single
most surprising edge for users, so it is worth saying plainly: **a client that wants a Will
Delay must also ask for a session that outlives it.** `delay > expiry` means the expiry
wins; `delay < expiry` means the session outlives its own will and the record's pending-will
block is cleared while the session record stays.

*What checks the deadline:* a **deadline-driven wakeup on the hub loop** — a third
`select!` branch on `sleep_until(next_will_due)`, where the hint is a cached
`Option<Instant>` that may be too early but never too late. A spawned timer per disconnect
was rejected (ADR 0061: it must be `spawn_owned`, can read no hub state, needs a `conn_id`
fence and a `JoinHandle` per client, and a resize that rehome-closes 1 700 sessions would
spawn 1 700 sleeping tasks). The 1 s sweep *alone* was rejected too: its granularity is
observable, and a will that may land a second late makes the Eclipse oracle's timing bound
flaky. The sweep keeps the two jobs it is genuinely better at — the session-expiry coupling
(the expiry pass publishes a pending will **before** `discard_session`, so a wall-clock jump
can never expire a session out from under its own will) and re-arming wills read back from
the store, both epoch-grained by nature.

*Cancellation, and what counts as a resume:* an **accepted, registered** CONNECT
[MQTT-3.1.3-9] — the cancel sits beside `online.insert`, so a CONNECT refused for
auth/quota/`Unavailable`/owner-mismatch does **not** cancel a will (the session was never
resumed).

**A resume is a resume, whatever the resuming CONNECT asks for.** [MQTT-3.1.3-9]'s sentence
says nothing about the new connection's Session Expiry Interval, so the decision must not
read it. A **takeover** is a resume too, and this is where getting it wrong is worst: the
first implementation re-derived `min(delay, expiry)` from the *new* connection, so a
half-open old socket plus a client reconnecting without the Session Expiry property (the
v5 default of 0 — paho's default, and every client that does not set it) yielded
`effective == 0` and published the replaced connection's delayed will. That is a death
announced for a device connected right then, on the most ordinary reconnect shape there is,
and the outcome flipped purely on whether the broker had already reaped the old socket —
timing no client can see. Session Expiry bounds a will only while the session has **no**
connection; a takeover means it never lost one.

**A CLEAN START CONNECT for the same client id DELETES the will — it does not publish it.**
This reverses what an earlier round of this ADR said, so here are the two clauses in full,
for a reader who wants to check the reasoning rather than trust it.

§3.1.2.5 (Will Flag) carries the obligation to publish:

> "The Will Message MUST be published after the Network Connection is subsequently closed
> and either the Will Delay Interval has elapsed or the Session ends, unless the Will
> Message has been deleted by the Server on receipt of a DISCONNECT packet with Reason Code
> 0x00 (Normal disconnection) or a new Network Connection for the ClientID is opened before
> the Will Delay Interval has elapsed [MQTT-3.1.2-8]."

§3.1.3.2.2 (Will Delay Interval) carries the delay and the resume rule:

> "The Server delays publishing the Client's Will Message until the Will Delay Interval has
> passed or the Session ends, whichever happens first. If a new Network Connection to this
> Session is made before the Will Delay Interval has passed, the Server MUST NOT send the
> Will Message [MQTT-3.1.3-9]."

Read together, three things follow. First, [MQTT-3.1.2-8]'s `unless` is an exception to the
whole obligation — "either the Will Delay Interval has elapsed **or the Session ends**" — so
it overrides *both* triggers, the session-end one included. Second, its second exception is
keyed on **the ClientID**, not on the Session: "a new Network Connection **for the ClientID**
is opened before the Will Delay Interval has elapsed". A Clean Start CONNECT for the same
client id inside the window is exactly that, whatever it does to the Session. Third,
[MQTT-3.1.3-9] is the narrower session-keyed rule and covers only the resume, and
§3.1.3.2.2's first sentence describes when the *delay* ends — it cannot create an obligation
that [MQTT-3.1.2-8] explicitly excepts.

The earlier reading here — [MQTT-3.1.2-4] discards the Session, therefore "or the Session
ends" fires the will — reached its conclusion without weighing the one normative sentence
that governs the case, and it was wrong in the direction that costs users the feature:
`clean_start = 1` with a non-zero Session Expiry Interval is an ordinary client shape (it is
what this project's own paho oracle sends, and what paho's examples do), and for every such
client the Will Delay Interval suppressed nothing at all — each blip still produced the
spurious device-offline announcement #299 exists to remove.

**Both halves give the same answer**, which is the point, and they have now disagreed in
both directions across two rounds — so the pairing is asserted rather than assumed. Offline:
`discard_session_local` takes `SessionEnd::ReplacedByNewConnection` from the clean-start
attach and deletes the will (`mqttd_wills_cancelled_total{reason="clean-start"}`). Online:
`resolve_replaced_will` does the same for a still-registered connection. `delay == 0` still
publishes on the dispatch in both, and that is not an inconsistency but the same clause:
[MQTT-3.1.2-8]'s exception needs a window to open inside, and a zero interval has already
elapsed when the connection closes. The Eclipse oracle now covers the shape
(`scripts/interop/paho_conformance.py` case (c)), which is what an interpretation this
contested should have had from the start.

*A resume clears the DURABLE block, not only this node's memory.* The obligation can
outlive the node that armed it — that is what persisting it is for — so a record carrying a
pending will this node never armed (armed on a peer, or armed before this node restarted)
must be cleared by the resume. It was not, and the failure was the worst shape available:
the inherited-session scan re-armed the stale block with an already-past deadline and
announced the client dead *after* it had resumed and then disconnected cleanly. The attach's
own off-loop recovery now reads the block (one metadata read on a path that already does an
authoritative claim plus two reads), so the clearing write happens only when there is
something to clear.

*A lease move is not a session end.* Releasing a moved session's local routing
(`release_moved_sessions`, ADR 0043 P2) goes through the same in-memory teardown as a
session end, and the first implementation therefore published the pending will on the spot —
up to a whole delay early, labelled `session-ended` for a session that was still alive, and
then a second time at the real deadline from the record it had left behind. The teardown now
takes an explicit reason: a routing release **hands the will over** when the record provably
holds it (`mqttd_wills_handed_off_total`; the new owner fires it at the original absolute
deadline, `reason="inherited"`), and otherwise keeps it armed here, because a memory-only
will has no other holder and a duplicate is recoverable where a silence is not.

*Node death — answered deliberately: the will and its ABSOLUTE deadline are PERSISTED,*
beside this ADR's own expiry deadline in the same `SessionMeta` record, for the reason §3
gives for absolute deadlines generally. Before #299 an ungraceful detach handed the will to
the delivery path synchronously, so no node-death window existed on that path; a delay
*creates* one, and accepting the loss inside it would mean the broker silently drops the
announcement of an unexpected death in the very window where a death is suspected — that is
not a residual, it is the feature inverted. The existing inherited-session scan re-arms from
the record under its existing owned-and-offline filter. A deadline already past fires on that
scan's tick: late, but delivered. The record's block is strictly trailing and EOF-defaulted
(no ADR 0058 schema stamp, no `BASELINE_REF` bump); an older binary ignores it and forgets
pending wills for the duration of a downgrade.

**What survives what, per store shape — because the answer is not the same in all three,
and a claim that holds in one shape and not another is worse than no claim.** The store
shape is a deployment choice (`MQTTD_DURABLE_SESSIONS`, `MQTTD_DATA_DIR` — ADR 0006 §3,
ADR 0018):

| Shape | A pending will survives… | …and does not survive |
| --- | --- | --- |
| **Clustered durable** (`MQTTD_DURABLE_SESSIONS=1` + `MQTTD_DATA_DIR`) | **node death** — proven end to end on a real 3-node cluster over real sockets, owner killed inside the window, the survivor that takes the group over announcing it from the replicated record (`durable_sessions::a_delayed_will_survives_the_death_of_the_node_that_armed_it`). A lease move, a takeover and a restart ride the same record and the same scan, and are covered by hub-level tests only | nothing above; the residuals below are about *cancellation*, not durability |
| **Clustered durable, no data dir** (announced as EPHEMERAL durability at boot) | node death and a lease move: peers hold the replicated record | a whole-cluster restart — the replicated state was only ever in RAM, which is what that boot warning says |
| **Single-node on-disk** (`MQTTD_DATA_DIR` only, ADR 0018 phase 1) | a crash restart and a graceful restart over the same data dir: the will and its absolute deadline are in `sessions.redb`, and the boot scan re-arms them | losing the data dir; there is no second copy anywhere (this is the mode's whole documented trade) |
| **In-memory** (neither variable) | nothing | the process ending, in either direction — the same thing `MemorySessionStore` already says about every session it holds, and residual R4 below |

That table is a *correction*, not just documentation: until issue #299's remediation round
the single-node on-disk row was FALSE. The will was written to `sessions.redb` correctly and
then never read back, because `ReplicatedSessionStore::all_sessions` enumerates through
`ReplicatedLog::keys()` and `PersistentLog` had no `keys()` — it took the trait's
`Ok(Vec::new())` default, so a restarted node's inherited-session scan saw *no sessions at
all*. The same hole silently disabled this ADR's own §3 machinery in that mode: persisted
session-expiry deadlines never fired across a restart either. `PersistentLog::keys()` now
range-scans the entry table (skipping key-to-key, so a session with a thousand queued
messages costs one step, not a thousand), pinned by
`persistent_log::keys_enumerates_every_logical_key_and_survives_a_reopen` and by an
end-to-end restart test over the real broker
(`persistence::a_delayed_will_survives_a_node_restart_over_the_same_data_dir`). The lesson
worth keeping: a trait default that returns "nothing" is indistinguishable from a correct
empty answer, and this one sat behind two features' inherit paths.

> **⚠️ That fix is an UPGRADE BEHAVIOUR CHANGE, and a data-visible one.** Implementing
> `keys()` switches **this ADR's own §3 session expiry ON** for every ADR 0018 phase-1
> deployment, because it was the enumeration §3's inherit path reads through. So the FIRST
> restart onto this build discards every offline session whose persisted deadline has
> already passed — **together with its offline queue, acked messages included** — where
> before those survived indefinitely. Verified on the real binary: a session with
> `SessionExpiryInterval=5` plus one PUBACKed `QoS` 1 message, node down 8 s → after the
> restart `session_present=0` and the message is never replayed; with the pre-fix `keys()`
> the identical run gives `session_present=1` and replays it. This is the spec-correct
> behaviour finally taking effect and it is still a one-way deletion, so the boot scan
> **says what it is about to do before doing it**: one `WARN` naming the count, the worst
> overdue, and up to 20 client ids —
> `DISCARDING offline sessions past their persisted Session Expiry deadline` — built by
> `summarize_overdue_discard` and pinned by
> `the_overdue_discard_summary_counts_every_session_and_names_the_worst_first`. Operator
> text: [OPERATIONS](../OPERATIONS.md#rolling-upgrades) (with the pre-flight: start a node
> over a *copy* of the data dir first) and [TROUBLESHOOTING](../TROUBLESHOOTING.md).
> Clustered durable is unaffected (expiry already worked there); the in-memory default keeps
> no sessions across a restart at all.

*Precision promised, in operator terms:* **never early — and, on the inherited path, only
after being wrong about it once.** The local deadline is a monotonic
`tokio::time::Instant`, not a whole-second epoch value: epoch truncation would fire a 4 s
delay after 3.1 s of wall time, a false death announcement *early*, which is the defect
class this feature exists to remove, and an NTP step cannot move it at all.

That was true locally and **false across the record**, which is exactly the path the
persistence exists for. The arm stored `floor(now) + effective` and the re-arm computed
`deadline − floor(now)`, so the elapsed delay came out as
`effective − frac(arm) + frac(re-arm)` — uniform in `(effective − 1 s, effective + 1 s)`.
Measured on the real broker across a restart: armed at 09:28:54.7408 with an 8 s delay,
published at 09:29:02.0524 — **7.312 s, 0.69 s early**; and on a real 3-node durable
cluster, a 12 s delay announced 11.20 s after the owner was killed. The fix is arithmetic,
not machinery: the persisted deadline is **rounded UP** to the next whole second from a
millisecond clock reading (`Hub::will_deadline_epoch`), and the remaining time is computed
from it **in milliseconds** — so every rounding error in the round trip lands LATE, by
strictly less than one second. A will is a death announcement: late is a slow announcement,
early is a false one, and only one of the two can be un-published. Pinned by
`a_will_inherited_across_a_restart_never_fires_before_its_deadline` (direction asserted,
margin printed) and, on the real binary over six repeated restarts, by
`persistence::a_will_inherited_across_a_restart_fires_no_earlier_than_its_deadline_exactly_once`.

Then, per path: **locally**, within one hub-loop dispatch of the deadline — and that
dispatch is sub-millisecond only where the store is in memory. Where it persists, the fire
includes an fsync'd durable clear and `mqttd_hub_dispatch_seconds_sum{command="will"}`
measured **8.9 ms mean over 486 fires** (12.2 ms for a single fire on an idle node), which
is the honest number; an earlier draft of this paragraph promised "sub-millisecond" flatly
and was off by ~10× in the shapes that persist. **When session expiry is the binding
bound**, within the 1 s sweep tick after it, never before (second-grained, as this ADR
already promises for expiry itself). **After a takeover, restart or node death**,
whole-second wall clock, late by the round-up (< 1 s) plus the inherited-scan cadence — ~1 s
inside a takeover window, otherwise up to `EXPIRY_RECONCILE_EVERY` = 30 s past the deadline
— plus inter-node clock skew (the trade this ADR's Consequences already record for absolute
deadlines).

*Exactly once, and the fence that makes it so.* The inherited-session scan reads
`all_sessions()` **off the hub loop** and its result is applied later, so anything that
happens to a will in between is invisible to it. Applying it blindly announced the same
death **twice in 5 of 6 restart runs**: the sweep spawns the scan before `fire_due_wills`,
the scan's read raced the fire's durable clear, and the stale block was re-armed with zero
remaining and published again ~20 ms later — both copies labelled `reason="inherited"`, so
an operator could not tell one duplicate from two deaths. The same staleness re-armed a will
a **resume** had already cancelled, and published it at once because its deadline had passed
([MQTT-3.1.3-9] and [MQTT-3.14.4-3] violated together, with no connection event to correlate
against). One mechanism closes both, and it is the one this hub already uses for
`SessionRecovered`'s stale results: a per-client fence
(`Hub::note_will_decided` / `wills_decided_since_scan`) recording every will published,
cancelled or handed off since the in-flight scan started, which `inherit_sessions` refuses
to re-arm. **Re-ordering the sweep's own steps is not a fix and is deliberately not relied
on** — the read is off-loop, so any ordering can still be raced by a slow read.

*Residuals, named:*

- **R1 — the unpersistable arm.** After a rehome close (issue #284) the closing node is by
  construction *not* the owner, so the group-routed write cannot land and that pending will
  is memory-only: a node death inside the window loses it. Same seam, same counter shape and
  same stated cause as §2's expiry residual above
  (`mqttd_pending_will_unpersisted_total{reason="not-owner"}`).
- **R2 — the wider, pre-existing hole this does NOT close.** A will lives only in the
  hub's `Online` entry while its client is *connected*, so a node that dies with live
  clients loses all of their wills — before and after #299. This work closes the window it
  would otherwise have opened (detached-and-waiting); it does not make wills survive node
  death in general. Persisting the will at *attach* is a separate feature with its own
  liveness question.
- **R3 — cross-node cancellation is not closed, so a rehome close is silent *at the close*
  but the will still fires one delay later.** A rehome close now arms rather than publishes,
  and the routing release that follows leaves the pending will alone — so the burst no longer
  lands at the close, which is the visible payoff for issue #284 and is pinned by
  `a_rehome_close_is_silent_for_a_delay_using_client`. What is NOT closed is the
  cancellation: the pending will sits in the closing node's memory, the client relocates, and
  its CONNECT is served by the *owner* (ADR 0005 proxies the stream, so even a reconnect to
  the same address never reaches the closing node's hub). Nothing cancels it and it fires one
  delay later. Operationally that moves the problem rather than removing it: the false-offline
  burst now arrives **after** `mqttd_session_rehomes_total` stops climbing, i.e. outside the
  suppression window OPERATIONS previously told operators to use — hence the doc change to
  "the roll plus your fleet's largest will delay", and `mqttd_pending_wills` to see the burst
  before it lands. Two alternatives were considered and rejected: suppressing the will
  entirely on a rehome close (issue #265's "document the suppression" exit was rejected for
  the same reason, and a client that never comes back would lose a real death
  announcement), and keeping the rehome will *immediate* so the false event lands inside the
  documented window (more operable, but a deliberate [MQTT-3.1.3-9] violation on the one path
  where the broker knows a reconnect is imminent). The real fix is 0009-P5. Note what the
  durable clear at attach *does* buy here: where the resuming node is the group's owner it
  clears the record, so the new owner does not fire a second copy — the surviving fire is the
  old node's memory copy, one announcement, not two.
- **R5 — retired, by getting the spec reading right.** This slot used to name "a Clean
  Start CONNECT drops a record-only pending will instead of publishing it" as a *loss*.
  Under [MQTT-3.1.2-8], read above, dropping it is the correct outcome: a new Network
  Connection for the ClientID inside the window deletes the will, whether or not this node
  holds an in-memory twin, and whether or not the record is the only copy. The residual and
  the durable read that would have "fixed" it both disappear — kept here as a numbered slot
  so the R-numbers in OPERATIONS, TROUBLESHOOTING and the delivery record keep meaning what
  they meant.
- **R6 — the arm's durable write is awaited ON the hub loop.** `arm_or_publish_will` →
  `persist_pending_will` → `set_pending_will` is a read-modify-write of the session record
  (`load_meta` + append + truncate, fsync'd), awaited inside `detach` beside the pre-existing
  `persist_detach_deadline` await — so a mass ungraceful disconnect of delay-using sessions
  roughly **doubles** an already-multi-second stall rather than adding a new class of one.
  Measured against the real binary, single-node on-disk, with a probe client timing hub
  round-trips: 500 sessions killed at once with a 6 s delay → worst round-trip **6.49 s**,
  loop degraded until **+11.0 s**, versus a delay-0 control (today's fleet) of 4.98 s /
  +6.84 s; at 200 sessions, 2.37 s / +4.21 s versus 1.13 s / +2.05 s. Not fixed here:
  moving it off-loop means arming optimistically and correcting `persisted` from a
  spawned reply — a new command, a new fence, and a window in which R1's counter lies — and
  that is an ADR 0017-shaped motion, not a closing-round edit. Named with its numbers
  instead, in OPERATIONS where an operator sizing a roll will meet it.
- **R4 — graceful shutdown does not fire pending wills early.** Deliberate: firing them at
  SIGTERM would make every rolling restart emit an early LWT storm, and "no earlier than the
  deadline" is the one timing promise worth keeping. The durable path re-arms from the record
  on restart; with `MemorySessionStore` the pending will dies with the process, which is what
  that store already says about every session it holds.

*A duplicate is possible where before there was exactly one, and the hand-off rule is what
keeps it rare.* A lease move between arm and fire could leave the old holder's memory and the
new owner's record both holding a will. The rule: a node that provably persisted the will and
no longer owns the session **hands it over** (counted, `mqttd_wills_handed_off_total`) and
holds nothing; a node whose arm never persisted keeps holding it, because nothing else has it.
Where the two cannot be distinguished the choice is deliberate — **prefer a duplicate over a
silence**: a second "offline" announcement is recoverable, a missing one is not, and
#238/#265 already settled that a will is never suppressed to be tidy. Which copy fired is
readable from the `reason` label (`inherited` = from a record, `delay-elapsed` = from the
memory of the node that armed it).

### 3. Message expiry rides in the stored queue entry; the deadline is absolute

A queued message carries an **absolute expiry deadline** (not the original interval),
stored alongside it in the `SessionStore`. On enqueue, `deadline = now + interval`
(none if the property is absent). On replay/delivery: drop entries past their deadline,
and set the outbound `Message Expiry Interval` to the **remaining** seconds
(`deadline - now`), as the spec requires. An absolute deadline (rather than re-deriving
elapsed time) is what survives a broker restart or a takeover correctly.

This needs the stored message to gain an optional deadline; that is a storage-format
change.

### 4. Typed property accessors, not raw `Vec` scans at every use

Per ADR 0008, the broker reads v5 properties through thin typed accessors on
`Properties` (e.g. `session_expiry_interval() -> Option<u32>`), added as each is needed
— keeping the generic wire model while giving the broker ergonomic, single-scan reads.

**As delivered (issue #299):** `will_delay_interval() -> Option<u32>` joins them. Its
absence for six months is the cautionary tale for this convention — a `Property` variant
that decodes and encodes correctly with no accessor and no reader looks *complete* from
inside the codec's own tests. When adding a property, the accessor and its one reader belong
in the same change as the variant, or a conformance oracle finds the gap instead.

## Consequences

- The v3.1.1 `clean_session` behaviour is now a degenerate case of the v5 model;
  existing tests must continue to pass unchanged (they pin the `{0, 0xFFFFFFFF}` cases).
- A new periodic tick enters the hub actor loop — cheap (a map scan), bounded by the
  number of disconnected-but-retained sessions on the node.
- Absolute deadlines make expiry correct across restarts and (once §2/§3 land) takeover,
  at the cost of trusting wall-clock skew between nodes — acceptable for second-grained
  expiry intervals.
