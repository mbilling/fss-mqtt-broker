# ADR 0062 — Online backup and restore: a per-node export with a stated window

- **Status:** Accepted
- **Date:** 2026-08-15
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0062-online-backup-and-restore.md](../delivery/0062-online-backup-and-restore.md) — plan, progress, and changelog
- **Related:** [ADR 0018](0018-on-disk-persistence.md) (the four redb stores and the
  fsync-before-ack contract this inherits), [ADR 0037](0037-durable-retained-messages.md) (the
  `(epoch, offset)` token the export stamps, and the retained commit path the import
  writes through), [ADR 0031](0031-session-identity-binding.md) (the owner binding a
  restore must reproduce), [ADR 0057](0057-durable-outbound-inflight.md) and issue #238
  (the two QoS-2 windows and the acked bit), [ADR 0058](0058-one-dot-zero-stability-contract.md)
  (an export format is a new compatibility surface), [ADR 0061](0061-off-loop-durable-appends.md)
  / issue #242 (a task that outlives the node keeps the data dir locked), issue #249
  (the review finding), issue #248 (the per-pod drain cost that makes
  stop-and-snapshot expensive).

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0062-online-backup-and-restore.md).

## Context

Disaster-recovery guidance was "snapshot the PVs of a **stopped** node"
(`docs/OPERATIONS.md`), and there was no export tool and no import path. Quorum is the
primary durability story, and it is a good one — but it is a *durability* story, not a
*backup* story. It does not protect against operator error, a bad migration, or
correlated corruption, and "stop a node to get a consistent snapshot" is not available
to a 24/7 fleet: it costs a full drain per node (the per-pod cost measured in #248) and
it is exactly the motion an operator is least willing to make during an incident.

## The question that had to be answered first

**What IS a consistent cut here, and can one be taken online?** Established from the
code, not from hope:

1. **Four stores are four snapshot domains.** `store_watch::STORE_FILES` is the
   authoritative list: `sessions.redb`, `retained.redb`, `replicas.redb`, `lease.redb`,
   each a separate `redb::Database` opened in a different module. redb gives a read
   transaction snapshot isolation over **one** database; redb 2.6.3 has no
   cross-database transaction, and three independent writers (the hub, the replica
   writer applying peers' committed appends, openraft) are never simultaneously parked —
   `store_watch`'s own module docs record that a follower applies peer appends
   unconditionally. **A cross-store atomic cut is not available.**
2. **A second process cannot read a running node's stores.** redb 2.6.3's unix file
   backend takes `flock(fd, LOCK_EX | LOCK_NB)` and answers a conflict with
   `DatabaseAlreadyOpen`; `flock` conflicts across processes *and* across separate opens
   in one process. So a `scripts/` script is impossible, and so is `mqttd --export` as a
   new process — the flag would be a second process hitting the same lock. **The
   exporter must run inside the running broker.**
3. **What is available**: per-store reads through the store traits. Retained is one
   genuinely atomic whole-store snapshot, taken **on the hub's actor loop**: values, their
   `(epoch, offset)` convergence tokens and the live tombstones are read in one command, so
   no mutation can interleave between the three. A session is a per-key cut across **two**
   independent log keys — `q/{client}` (the queue) and `m/{client}` (the metadata snapshot).
   A retained snapshot that fails to read **fails the whole run**: an export shipping a
   valid-looking file with every retained topic silently missing is the worst outcome
   available.

## Decision

**Ship an online, per-node export taken from the live process through the store traits,
plus an import that rebuilds a fresh cluster's data, and state the consistency claim as a
WINDOW rather than an instant.** The claim, which OPERATIONS leads with in these words:

> Every fact durably committed before `started_unix_ms` is present; facts committed
> inside `[started_unix_ms, finished_unix_ms]` may or may not be; facts committed after
> `finished_unix_ms` are not.

### 1. The skew direction is chosen, not conceded

A session's two keys are two reads, so skew is unavoidable. `export_session` reads the
**queue first and the metadata second**, always, so the metadata is never older than the
queue. Every interleaving then lands on a spec-legal outcome:

- a message acked and truncated between the reads is exported and redelivered on restore
  — legal at QoS 1, and suppressed at QoS 2 by the *newer* dedup window;
- `last_packet_id` is read after the queue, so it is ≥ any id those messages could have
  been delivered under: **a restore can never reuse a packet id that was live at export
  time.**

The opposite order produces exactly those two corruptions. It is a decision no type can
enforce, so it is written into the trait's contract and pinned by
`the_queue_is_read_before_the_metadata_that_covers_it` (a recording log asserts `q/`
precedes `m/`).

### 2. Each record carries the `(epoch, offset)` token

Per session the export stamps the lease epoch a write to its queue would commit under
and the highest live queue offset seen — the same token pair ADR 0037 uses to order
retained convergence. It makes one session's cut citable, and it is how the import
resolves a client id that appears in two nodes' exports (highest token wins, so file
order cannot change the result).

### 3. Scope: a per-node export; a cluster-scoped restore into a FRESH cluster

In cluster mode a node enumerates the whole cluster's session key set but can only read
the slice it owns (a foreign key answers `NotOwner`). So:

- the unit of export is **one node's readable state**, and the file name carries the node
  id;
- **a cluster backup is the set of every node's export**, and the docs say it in those
  words;
- the incompleteness of a union is **checked, not narrated**. Each trailer records the
  `not_owned` client ids the node skipped, and each header records the placement members
  it could see. The import refuses a set that is missing a named member's export, or that
  leaves a `not_owned` client id present in no file, naming what is absent.

**The coverage check has exactly one override, and it states its own cost.** All-or-nothing
was wrong in one case: the disaster that takes a node's volume can take the copy of its
export with it, and then a refusal holds the *surviving* nodes' backups hostage to a file
that no longer exists — the exact disaster the tool is for. So
`backup.restore_partial_accept_data_loss` (`MQTTD_RESTORE_PARTIAL_ACCEPT_DATA_LOSS`)
proceeds with an incomplete set and **forfeits** every session the missing nodes owned.
Three properties make it safe to ship: it is off by default; only `1`/`true`/`on`/`yes`
turns it on, unlike the presence-flips-on flags, because a data-forfeiting switch must not
be armed by a stray value; and the loss is named where it will still be readable a year
later — at startup, in the log naming every forfeited node and client id, in `/statusz`'s
`restore.detail`, and permanently in the on-disk `restored-from` stamp
(`"partial": true`).

A restore rebuilds **data**, not identity or consensus. Each node imports the slice the
*current* ring gives it and skips the rest with `NotOwner` — every node importing the
same files means every session lands exactly once, on its owner. Two consequences that
are decisions, not accidents:

- **`NotOwner` is retried in rounds.** A freshly assembled cluster's lease assignment
  converges over seconds, and while it does two nodes can each believe the other owns a
  group (the transient half of the 2026-07-20 ring/lease split). A single pass would drop
  that session with nobody importing it. The not-owned set is re-attempted until it stops
  shrinking for three consecutive rounds; each attempt is a placement-lock read on a
  foreign key, so retrying is cheap.
- **A session already present is never re-imported.** `claim_session` returning
  `present = true` means another node's restore claimed it first (or ownership moved
  mid-restore); importing again would append the queue twice and deliver every message
  twice.

The operator must tell the node when the cluster is assembled: `ready_min_members` is
that statement, and a restore waits for lease-group readiness **and** that member count
before importing. With the default of 1, a node can import against a single-member ring
and own keys the assembled cluster places elsewhere.

### 4. Where the tool lives, and how it is triggered

An in-process task, spawned on the existing `connections` tracker with the shutdown
token, holding `Arc` clones of the live stores and **never opening a redb handle of its
own** (ADR 0061 / #242: a handle held past the work keeps the data dir locked and the next
start fails with "Database already open"). Triggers: `[backup] every_secs` (default 0 =
off) and **`SIGUSR2`** on demand — SIGUSR1 is already the decommission trigger, so the
precedent and the CLI shape existed. `mqttd --backup [--pid n] [--timeout s]` is the
front end: a *new* process signalling the old one, then waiting for a file. It is never a
second reader of the stores, and the export is deliberately **not** exposed on the
health/metrics listener, which is unauthenticated — serving every retained payload and
queued message there would be an unauthenticated data-plane exfiltration endpoint.

**The signal handler is installed unconditionally, at startup, before anything can send
one** — not inside the backup task. Installing the stream is what overrides the signal's
*default disposition*, and `SIGUSR2`'s default is **terminate**. Installed only when
`[backup] dir` was set, it left every default-configured node one `kill -USR2` away from a
crash-semantics death: no drain, no readiness fail-first, in-flight publishes lost — the
failure mode being a monitoring or cron rollout that lands before the config does. With no
destination the handle goes to a task that answers the signal with the missing setting and
keeps serving. `SIGUSR1` was always installed unconditionally; this now matches it.

### 5. The format, and what a version mismatch does

`mqttd-backup` NDJSON **v2**: a header line, one JSON record per line, a trailer line;
payloads and correlation data base64. NDJSON rather than the storage codec because an
export format is a new compatibility surface (ADR 0058) and pinning it to `postcard`
would weld it to the store layouts — the very coupling the trait-level export was chosen
to avoid. It is also greppable by an operator whose restore just got refused. No new
crate: `serde_json` is already a dependency and sha-256 comes from `aws-lc-rs`.

**Why v2 and not v1.** The version was bumped before anything shipped, for two additions
the resolution rules below cannot work without, plus one correctness fix:

| Added in v2 | Why |
|---|---|
| `retained.token: {epoch, offset}` (optional) | A retained record carried **no ordering evidence at all**, so a union across files had to fall back on iteration order. This is ADR 0037's convergence token, the cluster's own order |
| `retained.tombstone: bool` | A retained *clear* exists in no cache, so a union of several nodes' files could resurrect a value another node had already deleted |
| `header.created_at` is RFC 3339 | It was `2026-08-15T055527Z` — no colons, which no RFC 3339 reader parses. It is now `2026-08-15T07:19:31Z`. The **file name** keeps the colon-less stamp, deliberately: a colon is a filename hazard |

**What an older reader does with a v2 file: it refuses**, naming the version it found, the
version it reads, and the `binary_version` that wrote the file — "restore it with that
build (or newer)". That is the same gate v2 applies to a v1 file, in the other direction
("no migration path exists pre-1.0"). v1 never shipped, so this is a stated contract rather
than a live migration burden; ADR 0058's posture is refuse-loudly, and no pre-1.0 migration
path is faked.

Refusal semantics, all of them importing nothing:

| Condition | Behaviour |
|---|---|
| `format_version` newer than the build | Refuse, naming found, expected, **and the `binary_version` that wrote the file** — "restore with that build" is the actionable instruction (the store gate's "wipe it" makes no sense for a backup) |
| `format_version` older | Refuse: "no migration path exists pre-1.0" — the established wording. Wired like the empty `MigrationStep` registries, and pinned by a test that forges both directions |
| Unknown record `kind` | Refuse. A silently skipped kind is data loss at the one moment an operator cannot afford it |
| Unknown *fields* in a known kind | Ignore — additive-field discipline, the same EOF-defaulting contract the session-meta codec spells out |
| Missing/malformed trailer, or a sha-256 that does not match | Refuse. Two independent guards against a truncated export |
| A `.partial` file | Invisible to an import, and never counted by retention |
| `complete = false` in the trailer | Refuse: the export itself says it is missing sessions |
| Two exports of one node id with the same `created_unix_ms` | Refuse, naming both. Recency is then undecidable, and guessing between two generations of one node is the failure the generation rule exists to stop |
| Two different `cluster_id`s in one set | Refuse, naming both ids and an example file for each |

The header's per-store schema stamps are **provenance only and gate nothing** — the
import writes through the logical API, so the source layout is irrelevant. This is stated
here so nobody later assumes it is a check.

### 5a. The two resolution rules, and why neither is file order

A restore directory is not a curated set: `backup.keep` defaults to 7, several nodes write
into one directory, and retained state is replicated, so the same topic appears in every
node's file. Two questions therefore have to be *decided*, and the original answers were
both accidents of iteration order.

**Generation — which FILES form the set.** Exactly one export per node id: the newest by
the header's `created_unix_ms`. Older generations are ignored **as sets**, never merged
record by record, and are reported as `superseded_files` in the restore log. They are deliberately NOT in the
stamp: the stamp records what WAS restored, and a list of files the restore did not read
would grow without bound as a backup directory accumulates generations. What the stamp
does carry about freshness is `skew_ms` with the oldest and newest export named — the
number that actually bounds how stale the recovered cluster is.
Selection reads only each file's first line, so a discarded generation costs no parse.
The rule this replaced resolved a duplicate client id across *all* files by
`(epoch, high_offset)` — and `high_offset` is `0` for a fully-drained queue, so an **older**
generation won whenever the newer one's queue was empty: stale subscriptions restored and
already-acked messages redelivered. Recency is a property of the file, so it is decided per
file, before a record is read. Sessions still resolve *across nodes* by the highest
`(epoch, high_offset)`: within one generation per node, a client id in two files means it
migrated during the backup, and the higher lease epoch is the later owner.

**Retained — which VALUE survives per topic**, in order:

1. a record carrying an `(epoch, offset)` token beats one carrying none — a value enters
   the token map when it is applied, so an untokened cached value predates that node's
   restart;
2. two tokens compare directly, which is the cluster's own convergence order (ADR 0037 P2);
   equal tokens are the same committed record seen twice, so the incumbent stands;
3. neither has a token (durable retained off, ADR 0014's best-effort mode, where no
   cluster-wide order exists) → the greater `created_unix_ms`, then the greater `node_id`
   purely for determinism, never because a node id means anything.

A winning **tombstone** removes the topic from the plan instead of restoring a value. The
rule this replaced was last-writer-wins by `BTreeMap` insertion over a lexicographic file
sort — i.e. **the highest-sorting node id won**, which could roll a retained topic *back*
while the newer value sat in the same directory. File order now decides nothing, anywhere.

**Rollback**: nothing on disk changes, so a downgrade needs no data migration — but
`[backup]` is a new config *section*, which the previous release's default
`config_unknown_keys = "refuse"` rejects. It is an unknown KEY, not a type mismatch, so
`warn` rescues it (ADR 0058 §E), and configuring backups through `MQTTD_BACKUP_*` avoids
the question entirely. OPERATIONS says so beside the write-floor rollback note.

**`BASELINE_REF` is deliberately not bumped.** The rolling-upgrade oracle's rule binds a
change that reshapes a persisted layout or a peer frame; this change does neither. No
store `SCHEMA_VERSION` moves, no redb table is added or reshaped, no peer frame changes —
because the export reads through the traits rather than the bytes. Recorded here so the
next reader can check the reasoning instead of trusting it.

### 5b. A restore reproduces the exported state and NOTHING ELSE

The governing rule, and the one the first implementation broke: a restored retained value
is written **as retained state**, never as a publish.

A retained mutation must commit through its topic's group lease owner or it will not
converge (ADR 0037), and that path lives in the hub — so the import goes through the hub.
But it goes through a dedicated `RestoreRetained` command, not
`Publish { retain: true }`, because a publish also **fans out**: it appends to every
durable *offline* session whose restored subscription matches the topic. No client listener
need be bound for that to happen. Run on every node, after the session import, it gave
every restored session queued messages that were in no backup — the restore inventing data
at the one moment an operator most needs it not to.

The committed-apply path has a second injection point that the same rule closes: the
window-scoped back-fill (issue #219) also reaches an offline durable session's queue. So a
restore-flagged retained commit delivers to **nobody**; the only outward traffic is the
token-carrying fan-out to peer caches, which is what makes the value converge. The oracle
in the cluster test is the backup file itself: the delivered multiset must *equal* the
exported queue, read back through the loader.

### 6. RPO and RTO as formulas whose terms are measured

**RPO = now − `started_unix_ms` of the newest successful export ≤ `every_secs` + W**,
where W = the export's own window width: `finished_unix_ms − started_unix_ms`, in every
trailer and on `/statusz` as `backup.window_ms`. `mqttd_backup_duration_ms` is the whole
run's wall clock — the reads *plus* the serialisation, write, fsync and rename — so it is a
safe **upper bound** on W rather than W itself, which is what makes it the right single
series to alert on. `mqttd_backup_last_success_timestamp_seconds` carries the age, so the
RPO is *observed on every run* — and it is only real if the operator alerts on it,
which OPERATIONS ships as a rule (with the `> 0` guard clause the watermark rules taught:
an unconfigured backup exports a literal 0).

**RTO = fresh-cluster start + records / durable-write rate**, and the second term
dominates: the import writes through the ordinary durable path, every durable write is one
fsync (`Durability::Immediate` on every mutating transaction in both stores), and there is
no batch-append API. In cluster mode each write is additionally a quorum round-trip.
Numbers: [docs/benchmarks/BACKUP-RESTORE.md](../benchmarks/BACKUP-RESTORE.md).

This is deliberately **not** fixed by adding a batched-durability import path. That would
open a second durability seam beside ADR 0018's fsync-before-ack contract, and a batch
that crashes mid-way leaves a partially-imported store — the one state this design
refuses to permit. The mitigations that cost no contract: the restore is a cold-start
operation (nothing is serving, so the wall clock is not downtime *plus* degradation),
`/readyz` reports NotReady with reason `restore-in-progress` and no client listener is
bound until the import completes, and an interrupted restore is never resumed — the data
dir must be fresh, so the operator re-runs from scratch and a half-imported store can
never serve a subset.

### 7. The precondition is filesystem freshness — and the stamp is a licence to reboot

A restore is permitted only into a node whose data dir holds none of the four store files,
checked **before any store is opened**. This is deliberate: an "is the store empty" query
through the cluster store is racy by construction — in a 3-node restore each node imports
its own slice, so a peer's already-imported keys would fail the next node's emptiness check
— while "this node has never opened a store here" is local, race-free, and exactly what an
operator means by *a fresh cluster*.

**The `restored-from` stamp is the exception, and it is the whole point of the stamp.**
`backup.restore_from` lives in a pod spec or a unit file; it does not disappear when the
restore succeeds. Treated as evidence of non-freshness, it made a successfully restored
node **unbootable**: the first ordinary reschedule — an OOM kill, a rolling upgrade, a node
drain — exited non-zero, and the remedy printed in the error was "delete the volume's
contents", which destroys the data just restored. A restore must therefore be
*idempotent-or-inert* on a second boot. So the stamp became a JSON record (source, instant,
files, set digest, forfeits, counts) and a start reads it first:

| Data dir state | This boot |
|---|---|
| Fresh | Import |
| A completed stamp naming the **same** source | The setting is **inert**; start normally. `mqttd_restore_state` reads 2 and `/statusz` says "this boot imported nothing" |
| A stamp naming a **different** source | Refuse — restoring a second set into a node that already holds one is a merge by another name |
| A stamp that will not parse | Inert, and said out loud: a stamp exists only once an import finished |
| Store files, no stamp | Refuse — state with nothing to explain it |

Identity is the **source path string**, deliberately not the file list: a backup directory
keeps receiving exports, so comparing file lists would fail on the next boot and recreate
the crash loop.

## Consequences

### What ships

An export of every session this node can read (owner binding, subscriptions, absolute
expiry deadline, packet-id high-water, the inbound QoS-2 dedup window **with its acked
bit**, the outbound QoS-2 in-flight window with `pubrec_seen`, and the offline queue with
each message's application properties and expiry), plus every retained value with its
properties, expiry and `(epoch, offset)` convergence token — and every live tombstone —
plus identity/provenance. An import that reproduces all of it through the ordinary store
API, and through the hub for retained values so they commit at their group's lease owner
and converge **without delivering to anyone** (§5b). `/statusz` blocks, four metrics
(`mqttd_backup_runs_total{outcome}`, `mqttd_backup_last_success_timestamp_seconds`,
`mqttd_backup_duration_ms`, `mqttd_restore_state`), a `--check-config`-gated `[backup]`
config section with six `MQTTD_*` variables, and an operator CLI.

### What is documented as a GAP, not covered by 1.0

Each is named in OPERATIONS' "Not covered by 1.0" list, with its reason:

1. **`lease.redb` / Raft state is never exported or restored.** It holds the persisted
   vote, log, membership and snapshot; re-injecting a stale vote or log is a
   consensus-safety violation, not a recovery. Lease-group recovery stays what it is:
   rejoin from survivors, or found a fresh cluster and import.
2. **Cluster identity and node identity are provenance only.** Writing them back would
   manufacture a second cluster carrying a live cluster's id — precisely what the
   founder/refound guard exists to catch. The `cluster_id` is read **fresh on every export
   run**, like `members`: a joiner adopts it over gossip *after* its process starts, so a
   value snapshotted at construction was `None` on every node that had not restarted since
   first boot — the provenance field the docs told an incident responder to trust was empty
   exactly on a freshly deployed cluster, and the cross-cluster refusal had nothing to
   compare. What it gates is *mixing*: a set naming two cluster ids is refused, but the
   target cluster is never checked against the backup's id, so a complete set from the
   wrong cluster restores that cluster.
3. **`replicas.redb` bytes are not exported.** A replica copy of a session this node does
   not own is unreadable without a lease, so it is a `not_owned` skip, not data.
4. **No cluster-wide consistent instant.** Per-node windows make the skew between nodes
   visible rather than asserting it away.
5. **A session that changed owner between two nodes' exports may be in neither.** The
   coverage check catches it and the restore REFUSES rather than losing it silently; the
   mitigation is to run the exports close together, and the guarantee is that you find
   out. It can be forfeited knowingly under the partial opt-in, never silently.
6. **A partial restore is lossy by definition and reconciles with nothing.** It exists for
   one disaster (a node's data *and* its export both gone); what it drops is named
   everywhere and recorded forever, but nothing later notices or repairs it.
7. **A restore into a live or non-fresh node is refused.** No merge, no selective
   per-session restore, no point-in-time. The one thing that is *not* a merge — the same
   node rebooting on the set it already imported — is inert, not refused (§7).
8. **No incremental or differential backup.** Every export is full; the RPO floor is
   bounded by the export's own duration.
9. **The bridge spool is out of scope.** `mqttd-bridge` is a separate binary with its own
   redb spool holding acked-but-unforwarded messages — a different process's durable
   state.
10. **Non-durable state** (QoS 0 queues, live connection state, topic aliases, pending
   wills) is not exported, by construction.
11. **Config, ACLs, PKI and passwords are not exported** — that is GitOps' job, and
    `--check-config` is its gate.
12. **A node with no durable store refuses to export**, loudly: `export_sessions` has no
    lossy default. A file that looks like a backup of nothing is worse than a refusal.
13. **The byte-level stop-and-snapshot path stays documented** as the complement, for the
    cases this deliberately does not cover (lease/Raft state, an exact image of one node),
    with its downtime cost and its #248 interaction stated.

### Residuals and the traps that follow

- **The export materialises the readable session set in memory** before writing (the
  trait's scan returns it whole). At the documented queue caps that is tens of MB; a node
  holding a million large queued messages would need a key-enumeration streaming seam,
  which is a follow-up, not a silent property.
- **No long-lived redb read transaction is taken**, which is what keeps the store file
  from growing during a backup. Anyone "improving" this later by holding one read
  transaction per store to gain per-store atomicity buys it with pinned pages: the file
  grows for the transaction's lifetime, and that growth is counted against
  `MQTTD_STORE_MAX_BYTES`, so a big backup could brown out the node it is backing up.
- **SIGUSR2 is the second signal overloaded for operations** (SIGUSR1 is decommission). A
  third would be a smell; the next such trigger should argue for an authenticated admin
  surface instead. The near-miss it produced is recorded in §4: a signal handler that is
  installed *conditionally* does not merely fail to work, it leaves the signal's default
  disposition in place, and for `SIGUSR2` that default is death.
- **Neither shipped deployment surface mounts a backup directory**, and this ADR does not
  change that. The Helm chart's `readOnlyRootFilesystem: true` is not the obstacle (a
  mounted volume stays writable); the obstacle is that there is no backup volume, mount or
  `MQTTD_BACKUP_*` plumbing in the chart, and `deploy/systemd/mqttd.service` is
  `ProtectSystem=strict` with `ReadWritePaths=/var/lib/mqttd` only. Both are documented
  opt-ins in OPERATIONS (chart extension points; a `systemctl edit` drop-in), verified with
  `helm template`. Turning them into first-class chart values is a follow-up, and until then
  the runbook is explicit that a default install has nowhere to write.
- **`--check-config` validates the backup SETTING, not the volume.** A nonexistent or
  mode-500 `backup.dir` passes; the failure surfaces at the first run as an error run with
  the path and the OS error. Probing the directory from `--check-config` would mean creating
  or writing a file from a validation-only command, which the flag deliberately does not do.
- **The restore verifies the SET, never the target.** Two cluster ids in one directory are
  refused, but a complete set from the wrong cluster is indistinguishable from the right
  one: nothing declares which cluster this node expects to be. An expected-`cluster_id`
  setting would close it and is a follow-up, not a claim made here.
- **The export file is plaintext data-plane content** — every retained payload, every
  queued message, every client id and owner subject. Files are created `0600` and
  OPERATIONS says the artifact is as sensitive as the broker's data, but at-rest
  encryption and transport are the operator's, and a shared backup volume is a
  lateral-movement path.
- **Restoring a session resurrects it**, including one whose client had cleanly ended it
  between the export and the disaster. That is inherent to any point-in-time restore; the
  import applies the exported absolute `session_expiry_at`, so the expiry sweeper collects
  a stale session promptly instead of serving it forever.
- **Two facts here were read from a vendored dependency, not our own code**: redb
  **2.6.3**'s flock-based exclusive open, and read-transaction snapshot isolation being
  per-database. A redb major upgrade must re-check both rather than inherit this argument.
- **A latent defect this work surfaced**: `PersistentLog` never implemented
  `ReplicatedLog::keys`, so the trait default (empty) applied to the single-node
  persistent store and everything built on key enumeration — the ADR 0009 expiry sweep,
  ADR 0042 T9 takeover materialisation, and (had it not been fixed) this export — read
  "this node holds no sessions". Implemented and tested here.
