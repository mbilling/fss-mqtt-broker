//! Online backup + restore of the durable state
//! ([ADR 0062](../../../docs/adr/0062-online-backup-and-restore.md), issue #249).
//!
//! Quorum is a *durability* story, not a *backup* story: it protects against a lost node,
//! not against operator error, a bad migration, or correlated corruption. Before this
//! module the only documented disaster-recovery path was to stop a node and snapshot its
//! volumes, which a 24/7 fleet cannot afford (and which pays the per-pod drain cost
//! measured in issue #248 every time). This is the online path: an export taken from a
//! LIVE process, and an import that rebuilds the data into a fresh cluster.
//!
//! ## What a consistent cut IS here — and what it is not
//!
//! **There is no cross-store atomic cut, and there is no cluster-wide instant.** Four
//! independent `redb` databases (`sessions.redb`, `retained.redb`, `replicas.redb`,
//! `lease.redb` — `store_watch::STORE_FILES` is the authoritative list) mean four snapshot
//! domains: `redb` gives a read transaction snapshot isolation over ONE database, there is
//! no cross-database transaction in redb 2.6.3, and three independent writers (the hub,
//! the replica writer applying peers' committed appends, and openraft) are never
//! simultaneously parked. So the export does not claim an instant. It claims a **window**,
//! written into every file's trailer:
//!
//! > every fact durably committed before `started_unix_ms` is present; facts committed
//! > inside the window may or may not be; facts committed after `finished_unix_ms` are not.
//!
//! Within that window two stronger properties hold and are worth stating precisely:
//!
//! - **Retained is one atomic whole-store snapshot, values AND convergence tokens.** The cut
//!   is taken inside one hub dispatch ([`crate::hub::HubCommand::RetainedExportSnapshot`]):
//!   `PersistentRetainedStore::all()` clones the in-memory map under a single mutex
//!   acquisition, and the `(epoch, offset)` token of each topic is read from the hub's own
//!   token map with no await in between. Since every retained mutation lands on that same
//!   single-threaded loop, nothing can interleave — so the pair is genuinely instantaneous,
//!   with no long-lived redb read transaction pinning pages. The token matters because a
//!   restore has to decide which of two nodes' exports of a topic is the later value; live
//!   TOMBSTONES ride along as empty-payload records, so a topic cleared after another node's
//!   export is not resurrected by the union.
//! - **Each session is a per-key cut with the skew direction CHOSEN.** See
//!   [`mqtt_storage::SessionStore::export_session`]: the queue is read before the metadata
//!   that describes it, so worst-case skew is a spec-legal redelivery and never a reused
//!   packet id. Each record carries the `(epoch, offset)` token pair (ADR 0037) as its
//!   audit position.
//!
//! ## Why the exporter lives INSIDE the broker
//!
//! It has to. `redb`'s unix file backend takes `flock(LOCK_EX | LOCK_NB)` on open and
//! answers a conflict with `DatabaseAlreadyOpen`, and `flock` conflicts across processes
//! *and* across separate opens in one process. So a `scripts/` script cannot read a
//! running node's stores, and neither can a second `mqttd` process — the flag would be a
//! different process hitting the same lock. The exporter therefore borrows the running
//! node's own store handles through the [`mqtt_storage`] traits and **never opens a redb
//! handle of its own** (ADR 0061 / issue #242: a handle held past the work keeps the data
//! dir locked and the next start fails with "Database already open"). Reading through the
//! traits also decouples the backup from the on-disk layouts, which is why no store
//! `SCHEMA_VERSION` moves and `BASELINE_REF` is not bumped by this change.
//!
//! The operator front-end is a *new* process signalling the old one: `mqttd --backup`
//! sends `SIGUSR2` (SIGUSR1 is already the decommission trigger) and waits for a file.
//!
//! ## Scope: a per-node export, a cluster-scoped restore into a FRESH cluster
//!
//! One file is one node's readable durable state. A cluster backup is **the set of every
//! node's export** — in cluster mode a node enumerates the whole cluster's session key set
//! but can only read the slice it owns, so the ids it skipped are recorded in the trailer
//! as `not_owned` and the import REFUSES a union that does not cover them. What a restore
//! rebuilds is DATA, never identity or consensus: `lease.redb` (the persisted vote, log
//! and membership) is never exported, and `cluster-id` / `node-id` travel as provenance
//! only. See OPERATIONS' "Not covered by 1.0" list.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mqtt_core::{AppProperties, ClientId, Message, QoS, Subscription};
use mqtt_observability::metrics::Metrics;
use mqtt_storage::{RetainedStore, SessionClaim, SessionExport, SessionStore, StorageError};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// The format name stamped in every file's header line.
pub const FORMAT: &str = "mqttd-backup";

/// The export format version (ADR 0058: an export format is a new compatibility surface,
/// so it is version-stamped and a mismatch REFUSES).
///
/// **v2** carries two things v1 could not express, both of them decisions a restore has to
/// make: a retained record's `(epoch, offset)` convergence token — without which two nodes'
/// exports of one topic can only be ordered by *file name*, i.e. by node id — and the
/// `tombstone` bit, without which a value cleared after an older node's export is
/// resurrected by the union. `created_at` also became a real RFC 3339 instant.
///
/// An older (v1) reader meeting a v2 file refuses with "`format_version` 2 is NEWER than
/// this build reads (1) … restore it with that build (or newer)"; this build meeting a v1 file
/// refuses with "no migration path exists pre-1.0" (ADR 0058 — there is no pre-1.0
/// migration path, and a backup is the last place to invent one). Neither imports anything.
pub const FORMAT_VERSION: u32 = 2;

/// The suffix of a completed export.
const SUFFIX: &str = ".ndjson";

/// The suffix of an export still being written. A `.partial` file is never read by an
/// import and never counted by retention: it is either renamed on success or left as
/// evidence of a run that died.
const PARTIAL_SUFFIX: &str = ".ndjson.partial";

/// The stamp written into the data dir once a restore completes, so the node's provenance
/// is legible on disk — and so the node's OWN next boot, with its own unchanged
/// environment, knows the import already happened and starts normally instead of refusing
/// (see [`restore_disposition`]).
pub const RESTORED_STAMP: &str = "restored-from";

// ---------------------------------------------------------------------------
// The file format: a header line, one record per line, a trailer line.
// ---------------------------------------------------------------------------

/// Line 1 of an export: identity and provenance, known before the scan begins.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Header {
    /// Always `"header"`.
    pub kind: String,
    /// Always [`FORMAT`].
    pub format: String,
    /// The format version this file is written in.
    pub format_version: u32,
    /// The broker build that wrote it — the actionable half of a version refusal.
    pub binary_version: String,
    /// Human-readable UTC instant the export started.
    pub created_at: String,
    /// The same instant in Unix milliseconds.
    pub created_unix_ms: u64,
    /// The node whose readable state this is.
    pub node_id: String,
    /// The cluster identity it belonged to (ADR 0054 T2), if known. **Provenance only** —
    /// a restore never writes it back.
    pub cluster_id: Option<String>,
    /// Whether durable sessions were on.
    pub durable: bool,
    /// The on-disk schema stamp of each store, by file name. **Provenance only, gating
    /// nothing**: the import writes through the logical store API, so the source layout is
    /// irrelevant. Stated here so nobody later mistakes it for a check.
    pub store_schema: BTreeMap<String, u32>,
    /// The placement members this node saw at export time. The import's coverage check
    /// reads it: every member named here must have supplied an export.
    pub members: Vec<String>,
}

/// One exported session (`kind = "session"`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRecord {
    /// Always `"session"`.
    pub kind: String,
    /// The client id.
    pub client: String,
    /// The identity bound to the session (ADR 0031). Restored through `claim_session`, so
    /// a foreign principal cannot adopt it.
    pub owner: Option<String>,
    /// The persisted subscription set.
    pub subscriptions: Vec<SubscriptionRecord>,
    /// Absolute session-expiry deadline, Unix epoch seconds (ADR 0009 §3).
    pub session_expiry_at: Option<u64>,
    /// The outbound packet-id high-water (ADR 0007 T9).
    pub last_packet_id: u16,
    /// The inbound QoS-2 dedup window, with the acknowledged bit (issue #238).
    pub received_qos2: Vec<InboundRecord>,
    /// The outbound QoS-2 in-flight window (ADR 0057).
    pub outbound_qos2: Vec<OutboundRecord>,
    /// The `(epoch, offset)` audit token (ADR 0037) — the position this record was read at,
    /// and how a duplicate client id across two files is resolved.
    pub token: Token,
    /// The offline queue, in offset order.
    pub queue: Vec<QueuedRecord>,
}

/// The `(epoch, high_offset)` audit token of a session record.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Token {
    /// The lease epoch a write to this session's queue would have committed under.
    pub epoch: u64,
    /// The highest live queue offset the read saw (0 = empty queue).
    pub high_offset: u64,
}

/// One subscription in a session record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionRecord {
    /// The topic filter.
    pub filter: String,
    /// The granted maximum `QoS` (0/1/2).
    pub max_qos: u8,
    /// MQTT 5 No Local.
    pub no_local: bool,
}

/// One inbound QoS-2 dedup entry: the packet id and whether its PUBREC was released.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct InboundRecord {
    /// The held packet id.
    pub packet_id: u16,
    /// Whether the success PUBREC was released (`false` = held-unacked, issue #238).
    pub acked: bool,
}

/// One outbound QoS-2 in-flight entry (ADR 0057).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct OutboundRecord {
    /// The packet id the subscriber knows the message by.
    pub packet_id: u16,
    /// The queue offset of the message, as exported (re-mapped on restore).
    pub offset: u64,
    /// Whether the PUBREC was seen (decides PUBREL vs PUBLISH+DUP on resume).
    pub pubrec_seen: bool,
}

/// One queued message in a session record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedRecord {
    /// The offset it was exported at (used to re-map outbound in-flight entries).
    pub offset: u64,
    /// Destination topic.
    pub topic: String,
    /// Payload, base64.
    pub payload_b64: String,
    /// Delivery `QoS` (0/1/2).
    pub qos: u8,
    /// Absolute message-expiry deadline, Unix epoch seconds (issue #227).
    pub expiry_at: Option<u64>,
    /// The publisher's forwardable application properties (ADR 0030).
    pub props: PropsRecord,
}

/// One retained message (`kind = "retained"`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetainedRecord {
    /// Always `"retained"`.
    pub kind: String,
    /// The topic.
    pub topic: String,
    /// Payload, base64.
    pub payload_b64: String,
    /// `QoS` the value was published at.
    pub qos: u8,
    /// Absolute expiry deadline, Unix epoch seconds (issue #227).
    pub expires_at: Option<u64>,
    /// The publisher's forwardable application properties (ADR 0030).
    pub props: PropsRecord,
    /// The `(epoch, offset)` **convergence token** of the committed retained record this
    /// value was applied from (ADR 0037 P2) — the same token the cluster itself orders
    /// retained writes by, and therefore the only sound way to decide which of two nodes'
    /// exports of one topic is the later value (v2; `None` under durable-off, where
    /// retained is ADR 0014 best-effort and no cluster-wide order exists, and for a value
    /// this node had cached but not yet attributed to a committed record).
    #[serde(default)]
    pub token: Option<RetainedToken>,
    /// Whether this record is a **clear** rather than a value (a versioned tombstone,
    /// ADR 0037 P2 / MQTT-3.3.1-10). Exported so that a topic cleared after an older
    /// node's export is not RESURRECTED by the union: the clear carries a token like any
    /// value and wins or loses by it (v2).
    #[serde(default)]
    pub tombstone: bool,
}

/// The `(epoch, offset)` convergence token of a committed retained record (ADR 0037 P2).
///
/// Epochs are consensus-issued and globally monotonic; offsets strictly increase per key.
/// So a higher token is a strictly later committed write — clock-free, and the same
/// comparison every cache/back-fill decision in the broker already reduces to.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RetainedToken {
    /// The lease epoch the retained write committed under.
    pub epoch: u64,
    /// The committed log offset of the record, strictly increasing per topic.
    pub offset: u64,
}

/// The forwardable MQTT 5 application properties of a message (ADR 0030).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PropsRecord {
    /// `0x01` Payload Format Indicator.
    #[serde(default)]
    pub payload_format: Option<u8>,
    /// `0x03` Content Type.
    #[serde(default)]
    pub content_type: Option<String>,
    /// `0x08` Response Topic.
    #[serde(default)]
    pub response_topic: Option<String>,
    /// `0x09` Correlation Data, base64.
    #[serde(default)]
    pub correlation_data_b64: Option<String>,
    /// User Properties, in wire order.
    #[serde(default)]
    pub user_properties: Vec<(String, String)>,
}

/// The last line of an export: the scan's own verdict on itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trailer {
    /// Always `"trailer"`.
    pub kind: String,
    /// Whether every enumerated session key was read or cleanly foreign. An export is
    /// only ever renamed into place when this is `true`.
    pub complete: bool,
    /// Session records written.
    pub sessions: u64,
    /// Queued messages written, across all sessions.
    pub queued: u64,
    /// Retained records written.
    pub retained: u64,
    /// Client ids this node enumerated but could not read because another node owns them.
    /// The import's coverage check refuses a union that does not cover these.
    pub not_owned: Vec<String>,
    /// When the cut started, Unix milliseconds.
    pub started_unix_ms: u64,
    /// When it finished, Unix milliseconds. `[started, finished]` is the window the
    /// consistency claim is stated over.
    pub finished_unix_ms: u64,
    /// SHA-256, hex, over every byte of the file before this line.
    pub sha256: String,
}

// ---------------------------------------------------------------------------
// Exporting.
// ---------------------------------------------------------------------------

/// One retained topic as the exporter sees it: the value (or a clear), and the
/// `(epoch, offset)` convergence token it was applied from, when there is one.
#[derive(Debug, Clone)]
pub struct RetainedSnapshotEntry {
    /// The retained message. An EMPTY payload is a clear (a tombstone), the same
    /// convention `RetainedStore::set` and the durable keyspace already use.
    pub message: Message,
    /// The committed record's `(epoch, offset)` token, or `None` under durable-off.
    pub token: Option<(u64, u64)>,
}

/// Where the exporter reads retained state from.
///
/// Not `RetainedStore` directly, because the value alone is not enough: two nodes' exports
/// of one topic can only be ordered by their `(epoch, offset)` convergence tokens, and the
/// token of a *cleared* topic exists nowhere in the local cache (the cache drops the key).
/// Both live beside each other in the hub, which is also the only place they can be read
/// **together atomically** — the hub is a single-threaded actor, so a snapshot taken inside
/// one command dispatch cannot interleave with a retained mutation, and the export keeps
/// the "one atomic whole-store cut" property it claims instead of pairing a value from one
/// instant with a token from another.
#[async_trait::async_trait]
pub trait RetainedSource: Send + Sync {
    /// Every retained topic this node holds — values and live tombstones — with tokens.
    ///
    /// # Errors
    /// A message naming why the snapshot could not be taken.
    async fn snapshot(&self) -> Result<Vec<RetainedSnapshotEntry>, String>;
}

/// A [`RetainedSource`] over a bare [`RetainedStore`]: values only, no tokens, no
/// tombstones — the shape available with durable retained OFF (ADR 0014 best-effort), and
/// what the store-level tests use.
#[derive(Debug)]
pub struct StoreRetainedSource(pub Arc<dyn RetainedStore>);

#[async_trait::async_trait]
impl RetainedSource for StoreRetainedSource {
    async fn snapshot(&self) -> Result<Vec<RetainedSnapshotEntry>, String> {
        let all = self
            .0
            .all()
            .await
            .map_err(|e| format!("backup: retained read failed: {e}"))?;
        Ok(all
            .into_iter()
            .map(|message| RetainedSnapshotEntry {
                message,
                token: None,
            })
            .collect())
    }
}

/// What the exporter needs to know about the node it is backing up.
#[derive(Debug, Clone)]
pub struct ExportContext {
    /// Destination directory (never inside the data dir — `Config::validate` refuses that).
    pub dir: PathBuf,
    /// Exports kept per node id.
    pub keep: u32,
    /// This node's id — in the header and in the file name.
    pub node_id: String,
    /// The cluster identity, if known (provenance).
    pub cluster_id: Option<String>,
    /// Whether durable sessions are on.
    pub durable: bool,
    /// The placement members this node sees (the coverage check's input).
    pub members: Vec<String>,
}

/// What one export run produced — also what `/statusz` reports.
#[derive(Debug, Clone)]
pub struct ExportReport {
    /// The file written.
    pub path: PathBuf,
    /// Its size in bytes.
    pub bytes: u64,
    /// Sessions written.
    pub sessions: u64,
    /// Queued messages written.
    pub queued: u64,
    /// Retained topics written.
    pub retained: u64,
    /// Client ids skipped because another node owns them.
    pub not_owned: Vec<String>,
    /// The cut's start, Unix milliseconds.
    pub started_unix_ms: u64,
    /// The cut's end, Unix milliseconds.
    pub finished_unix_ms: u64,
}

impl ExportReport {
    /// The window width in milliseconds — the `W` term of the RPO formula, measured by
    /// every run rather than asserted once.
    #[must_use]
    pub fn window_ms(&self) -> u64 {
        self.finished_unix_ms.saturating_sub(self.started_unix_ms)
    }
}

/// The store schema stamps this build reads and writes, for the header's provenance block.
fn store_schema() -> BTreeMap<String, u32> {
    BTreeMap::from([
        (
            "sessions.redb".to_string(),
            mqtt_storage::persistent_log::SCHEMA_VERSION,
        ),
        (
            "retained.redb".to_string(),
            mqtt_storage::persistent_retained::SCHEMA_VERSION,
        ),
        (
            "replicas.redb".to_string(),
            mqtt_cluster::cluster_log::R_SCHEMA_VERSION,
        ),
        (
            "lease.redb".to_string(),
            mqtt_cluster::lease_store::LEASE_SCHEMA_VERSION,
        ),
    ])
}

/// Take one online export of `sessions` + `retained` into `ctx.dir`.
///
/// Writes `<dir>/mqttd-backup-<node>-<UTC>.ndjson.partial`, fsyncs it, then renames it to
/// the final name — so a reader can never see a torn file and an interrupted run leaves
/// evidence rather than a plausible-looking backup. An INCOMPLETE session scan fails the
/// run: nothing is renamed, so the last-success timestamp does not advance and the RPO
/// alert fires, which is strictly better than an operator trusting a file that is missing
/// sessions.
///
/// # Errors
/// A message naming what failed: the store refusing to export (a non-durable node), an
/// incomplete scan, or an I/O failure.
pub async fn export(
    ctx: &ExportContext,
    sessions: &Arc<dyn SessionStore>,
    retained: &Arc<dyn RetainedSource>,
) -> Result<ExportReport, String> {
    let started_ms = unix_millis();
    std::fs::create_dir_all(&ctx.dir)
        .map_err(|e| format!("backup: cannot create {}: {e}", ctx.dir.display()))?;
    clean_stale_partials(&ctx.dir, &ctx.node_id);

    // Read the two sources through the LIVE handles (never a second redb open, ADR 0061).
    // Sessions first: it is the long half, and reading retained second keeps the retained
    // cut as close to `finished_unix_ms` as possible.
    let scan = sessions
        .export_sessions()
        .await
        .map_err(|e| format!("backup: session export refused: {e}"))?;
    if !scan.complete {
        // A transient per-key failure (no quorum mid-recovery) means a session this scan
        // SHOULD have seen was not read. Publishing that file would be the half-true
        // backup this feature exists not to be.
        return Err(
            "backup: the session scan was INCOMPLETE (at least one group could not be read \
             — no quorum / unavailable); nothing was written. Retry once the durable plane \
             reports ready"
                .to_string(),
        );
    }
    let retained_all = retained.snapshot().await?;

    // The stamp carries MILLISECONDS: two exports inside one second (an operator hitting
    // `--backup` twice, a schedule racing a signal) must be two files, not one silently
    // overwriting the other. Still lexicographically sortable, which is what retention and
    // "the newest export" both rely on.
    let stem = format!(
        "{FORMAT}-{}-{}-{:03}",
        sanitize(&ctx.node_id),
        utc_stamp(started_ms / 1000),
        started_ms % 1000
    );
    let partial = ctx.dir.join(format!("{stem}{PARTIAL_SUFFIX}"));
    let final_path = ctx.dir.join(format!("{stem}{SUFFIX}"));

    let header = Header {
        kind: "header".to_string(),
        format: FORMAT.to_string(),
        format_version: FORMAT_VERSION,
        binary_version: env!("CARGO_PKG_VERSION").to_string(),
        created_at: rfc3339(started_ms / 1000),
        created_unix_ms: started_ms,
        node_id: ctx.node_id.clone(),
        cluster_id: ctx.cluster_id.clone(),
        durable: ctx.durable,
        store_schema: store_schema(),
        members: ctx.members.clone(),
    };

    let mut body = Vec::new();
    push_line(&mut body, &header)?;
    let mut queued = 0u64;
    for s in &scan.sessions {
        queued += s.queue.len() as u64;
        push_line(&mut body, &session_record(s))?;
    }
    for m in &retained_all {
        push_line(&mut body, &retained_record(m))?;
    }
    let finished_ms = unix_millis();
    let trailer = Trailer {
        kind: "trailer".to_string(),
        complete: true,
        sessions: scan.sessions.len() as u64,
        queued,
        retained: retained_all.len() as u64,
        not_owned: scan.not_owned.iter().map(|c| c.0.clone()).collect(),
        started_unix_ms: started_ms,
        finished_unix_ms: finished_ms,
        sha256: sha256_hex(&body),
    };
    push_line(&mut body, &trailer)?;

    write_private_fsynced(&partial, &body)?;
    std::fs::rename(&partial, &final_path).map_err(|e| {
        format!(
            "backup: cannot rename {} to {}: {e}",
            partial.display(),
            final_path.display()
        )
    })?;
    prune(&ctx.dir, &ctx.node_id, ctx.keep);

    Ok(ExportReport {
        path: final_path,
        bytes: body.len() as u64,
        sessions: trailer.sessions,
        queued,
        retained: trailer.retained,
        not_owned: trailer.not_owned,
        started_unix_ms: started_ms,
        finished_unix_ms: finished_ms,
    })
}

fn session_record(s: &SessionExport) -> SessionRecord {
    SessionRecord {
        kind: "session".to_string(),
        client: s.client.0.clone(),
        owner: s.owner.clone(),
        subscriptions: s
            .subscriptions
            .iter()
            .map(|sub| SubscriptionRecord {
                filter: sub.filter.clone(),
                max_qos: sub.max_qos as u8,
                no_local: sub.no_local,
            })
            .collect(),
        session_expiry_at: s.session_expiry_at,
        last_packet_id: s.last_packet_id,
        received_qos2: s
            .received_qos2
            .iter()
            .map(|(packet_id, acked)| InboundRecord {
                packet_id: *packet_id,
                acked: *acked,
            })
            .collect(),
        outbound_qos2: s
            .outbound_qos2
            .iter()
            .map(|o| OutboundRecord {
                packet_id: o.packet_id,
                offset: o.offset,
                pubrec_seen: o.pubrec_seen,
            })
            .collect(),
        token: Token {
            epoch: s.epoch,
            high_offset: s.high_offset,
        },
        queue: s
            .queue
            .iter()
            .map(|q| QueuedRecord {
                offset: q.offset,
                topic: q.message.topic.clone(),
                payload_b64: b64_encode(&q.message.payload),
                qos: q.message.qos as u8,
                expiry_at: q.expiry_at.or(q.message.expires_at),
                props: props_record(&q.message.app),
            })
            .collect(),
    }
}

fn retained_record(e: &RetainedSnapshotEntry) -> RetainedRecord {
    let m = &e.message;
    RetainedRecord {
        kind: "retained".to_string(),
        topic: m.topic.clone(),
        payload_b64: b64_encode(&m.payload),
        qos: m.qos as u8,
        expires_at: m.expires_at,
        props: props_record(&m.app),
        token: e
            .token
            .map(|(epoch, offset)| RetainedToken { epoch, offset }),
        // The same convention as every other retained path in the broker: an empty
        // payload IS the clear (MQTT-3.3.1-10).
        tombstone: m.payload.is_empty(),
    }
}

fn props_record(app: &AppProperties) -> PropsRecord {
    PropsRecord {
        payload_format: app.payload_format,
        content_type: app.content_type.clone(),
        response_topic: app.response_topic.clone(),
        correlation_data_b64: app.correlation_data.as_ref().map(|c| b64_encode(c)),
        user_properties: app.user_properties.clone(),
    }
}

fn push_line<T: Serialize>(out: &mut Vec<u8>, value: &T) -> Result<(), String> {
    let mut line =
        serde_json::to_vec(value).map_err(|e| format!("backup: cannot encode record: {e}"))?;
    line.push(b'\n');
    out.extend_from_slice(&line);
    Ok(())
}

/// Write `bytes` to `path` with mode 0600 and fsync before returning: an export is
/// data-plane content (every retained payload, every queued message, every client id), and
/// a file that is not fsynced is a file a power cut can turn into a truncated backup.
fn write_private_fsynced(path: &Path, bytes: &[u8]) -> Result<(), String> {
    use std::io::Write;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts
        .open(path)
        .map_err(|e| format!("backup: cannot write {}: {e}", path.display()))?;
    f.write_all(bytes)
        .map_err(|e| format!("backup: cannot write {}: {e}", path.display()))?;
    f.sync_all()
        .map_err(|e| format!("backup: cannot fsync {}: {e}", path.display()))?;
    Ok(())
}

/// The export files belonging to `node_id`, oldest first.
fn exports_of(dir: &Path, node_id: &str) -> Vec<PathBuf> {
    let prefix = format!("{FORMAT}-{}-", sanitize(node_id));
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(&prefix) && n.ends_with(SUFFIX))
        })
        .collect();
    // The name embeds a sortable UTC stamp, so lexicographic order is chronological —
    // and unlike mtime it survives a copy.
    files.sort();
    files
}

/// Keep the newest `keep` exports **per node id**, so a directory shared by several nodes
/// cannot have one node's rotation delete another's backups.
fn prune(dir: &Path, node_id: &str, keep: u32) {
    let files = exports_of(dir, node_id);
    let keep = keep.max(1) as usize;
    if files.len() <= keep {
        return;
    }
    for old in &files[..files.len() - keep] {
        if let Err(e) = std::fs::remove_file(old) {
            warn!(path = %old.display(), error = %e, "backup retention: cannot delete an old export");
        }
    }
}

/// Delete `.partial` files this node left behind: an abandoned run's evidence is useful
/// exactly once, and never worth accumulating.
fn clean_stale_partials(dir: &Path, node_id: &str) {
    let prefix = format!("{FORMAT}-{}-", sanitize(node_id));
    for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.starts_with(&prefix) && name.ends_with(PARTIAL_SUFFIX) {
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// A node id reduced to a file-name-safe token (ids are operator-supplied).
fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Importing.
// ---------------------------------------------------------------------------

/// A parsed, verified, coverage-checked set of export files, ready to apply.
#[derive(Debug, Default)]
pub struct RestorePlan {
    /// The files it was built from: exactly one generation per node id (the newest).
    pub files: Vec<PathBuf>,
    /// Older generations of a node found beside the selected ones and deliberately NOT
    /// read. Reported so "which backup did I actually restore?" is answerable.
    pub superseded: Vec<PathBuf>,
    /// The SKEW of the composed set: the span between the oldest and newest selected
    /// export's `created_unix_ms`, in milliseconds.
    ///
    /// A cluster backup is one export per node, and those exports were taken at DIFFERENT
    /// moments — so the set as a whole is only as fresh as its oldest member. A node whose
    /// export is a day stale contributes a day-old view of its sessions: deleted sessions
    /// reappear, and messages already acked and drained are re-queued. Nothing about the
    /// per-node consistency guarantee bounds this, so it is measured, logged, and kept in
    /// the stamp rather than left for an operator to notice from file names.
    pub skew_ms: u64,
    /// The oldest and newest selected exports, as `(file name, RFC 3339 instant)`, so the
    /// skew above can be attributed to a node without re-reading the set.
    pub oldest_export: Option<(String, String)>,
    pub newest_export: Option<(String, String)>,
    /// One session per client id — the highest-token copy when a client migrated between
    /// two nodes' exports.
    pub sessions: Vec<SessionRecord>,
    /// One retained value per topic.
    pub retained: Vec<RetainedRecord>,
    /// Topics whose winning record is a CLEAR: deliberately absent from `retained`, so a
    /// value another node still cached is not resurrected by the union.
    pub cleared_topics: Vec<String>,
    /// Cluster members with no export in the set, accepted only under
    /// [`Coverage::PartialAcceptDataLoss`]. Their sessions are forfeited.
    pub forfeited_nodes: Vec<String>,
    /// Client ids the set names as owned-elsewhere and covers nowhere, accepted only under
    /// [`Coverage::PartialAcceptDataLoss`]. These sessions are forfeited.
    pub forfeited_clients: Vec<String>,
    /// The set's identity: sha-256 over the selected files' own trailer digests, in sorted
    /// order. Path- and name-independent, so it identifies the DATA that was restored.
    pub set_sha256: String,
}

impl RestorePlan {
    /// Whether this plan knowingly forfeits data (a partial restore).
    #[must_use]
    pub fn is_partial(&self) -> bool {
        !self.forfeited_nodes.is_empty() || !self.forfeited_clients.is_empty()
    }
}

/// How a load treats a set that does not cover the cluster it came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Coverage {
    /// REFUSE a set missing a member's export, or missing a session some export named as
    /// owned elsewhere. The default, and the right answer almost always: a restore that
    /// silently drops a third of the cluster's sessions is the failure this feature exists
    /// to prevent.
    #[default]
    Complete,
    /// PROCEED with an incomplete set, FORFEITING the missing nodes' data.
    ///
    /// This exists because the disaster the tool is for can take a node's data *and* its
    /// export together, and an all-or-nothing check then makes the surviving nodes'
    /// backups unrestorable too — the whole cluster held hostage by the one file that is
    /// gone. It is an explicit operator decision (`backup.restore_partial_accept_data_loss`
    /// / `MQTTD_RESTORE_PARTIAL_ACCEPT_DATA_LOSS`), never a fallback, and every forfeited
    /// node and session id is named in the log, in `/statusz` and in the on-disk
    /// `restored-from` stamp, permanently.
    PartialAcceptDataLoss,
}

/// Load and verify every export under `path` (a file or a directory), refusing anything
/// that does not cover the cluster it came from.
///
/// # Errors
/// A message naming the file and the reason. See [`load_with`].
pub fn load(path: &Path) -> Result<RestorePlan, String> {
    load_with(path, Coverage::Complete)
}

/// Load and verify every export under `path`, with an explicit coverage posture.
///
/// **The two resolution rules, stated once and implemented here:**
///
/// 1. **Generation — one export per node id, the NEWEST by `created_unix_ms`.** A restore
///    directory legitimately holds several generations per node (`backup.keep` defaults to
///    7 in the very directory an operator copies off the volume and points
///    `MQTTD_RESTORE_FROM` at), so the older ones are IGNORED as a set, not merged
///    record-by-record. Merging is what produced state time-travel: the previous rule
///    resolved a duplicate client by `(epoch, high_offset)`, and `high_offset` is 0 for a
///    fully-drained queue, so an older generation won whenever the newer one's queue was
///    empty — restoring stale subscriptions and redelivering acked messages. Recency is a
///    property of the FILE, so it is decided per file, before a record is read. Two files
///    of one node with the SAME `created_unix_ms` are refused, naming both: recency is then
///    undecidable, and guessing is exactly what this rule exists to stop.
/// 2. **Retained — by `(epoch, offset)` convergence token, then by file recency.** A record
///    carrying a token beats one carrying none (no token means the exporting node had not
///    attributed the cached value to a committed record, i.e. the value predates that
///    node's restart); two tokens compare directly, which is the cluster's own order; two
///    untokened records (durable-off, where no cluster-wide order exists) fall back to the
///    newer export, then to the higher node id so the outcome is deterministic. **File
///    order never decides** — the previous rule was last-writer-by-`BTreeMap`-insertion
///    over a lexicographic file sort, i.e. the highest-sorting NODE ID won, which could
///    roll a retained topic back while the newer value sat in the same set. A winning
///    TOMBSTONE removes the topic instead of restoring it.
///
/// Sessions still resolve across NODES by the highest `(epoch, high_offset)` token: within
/// one generation per node, a client id in two files means it migrated during the backup,
/// and the higher lease epoch is the later owner.
///
/// Refusals, all of them loud and all of them importing nothing:
/// - a `format_version` newer than this build (naming found, expected, and the
///   `binary_version` that wrote the file, because "restore with that build" is the
///   actionable instruction);
/// - a `format_version` older ("no migration path exists pre-1.0" — the established
///   wording);
/// - two exports of one node id that share a `created_unix_ms`;
/// - exports from **two different clusters** (`cluster_id`), naming both ids;
/// - an unknown record `kind` (a silently skipped kind is data loss at the one moment an
///   operator cannot afford it);
/// - a missing/malformed trailer, or a sha-256 that does not match the bytes;
/// - a trailer that says the export was incomplete;
/// - under [`Coverage::Complete`], a set that is missing a node named in some file's
///   `members`, or that does not cover every `not_owned` client id — naming exactly what is
///   absent, and naming the opt-in that accepts the loss knowingly.
///
/// Unknown *fields* inside a known kind are ignored: additive-field discipline, the same
/// EOF-defaulting contract the session-meta codec spells out.
///
/// # Errors
/// A message naming the file and the reason.
// One linear pass with one refusal per paragraph: long by the number of things it owes the
// operator an answer about, not by branching complexity.
#[allow(clippy::too_many_lines)]
pub fn load_with(path: &Path, coverage: Coverage) -> Result<RestorePlan, String> {
    let found = collect_files(path)?;
    if found.is_empty() {
        return Err(format!(
            "restore: no {FORMAT}*{SUFFIX} files found at {} (a `.partial` file is never \
             read — it is an interrupted export, not a backup)",
            path.display()
        ));
    }
    // THE CLUSTER CHECK RUNS OVER EVERY FILE FOUND, BEFORE GENERATION SELECTION — and the
    // order is the whole point. `select_generations` reduces the directory to one file per
    // NODE ID, so another cluster's export of the same node id (the staging-vs-prod shape,
    // where both name their nodes `mqttd-0..2`) looks exactly like an older GENERATION of
    // that node and is silently discarded. Round 2 proved it on two real unrelated clusters:
    // the refusal below existed, ran only over the survivors, and never fired while the
    // wrong cluster's data was imported. A refusal that the step before it has already
    // hidden the evidence from is not a refusal.
    let mut all_clusters: BTreeMap<String, PathBuf> = BTreeMap::new();
    for file in &found {
        // Unreadable heads are skipped here too: a file that cannot be read tells us no
        // cluster id AND will be skipped by generation selection, so it contributes nothing
        // to the restore either way.
        if let Ok(head) = read_head(file) {
            if let Some(id) = head.cluster_id {
                all_clusters.entry(id).or_insert_with(|| file.clone());
            }
        }
    }
    refuse_mixed_clusters(&all_clusters)?;
    let (files, superseded) = select_generations(&found)?;
    // The composed set's freshness is its OLDEST member's, not its newest — see `skew_ms`.
    let mut stamps: Vec<(u64, String)> = Vec::new();
    for file in &files {
        let head = read_head(file)?;
        stamps.push((
            head.created_unix_ms,
            file.file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned(),
        ));
    }
    stamps.sort_unstable();
    let describe = |(ms, name): &(u64, String)| (name.clone(), rfc3339_from_unix_ms(*ms));
    let oldest_export = stamps.first().map(describe);
    let newest_export = stamps.last().map(describe);
    let skew_ms = match (stamps.first(), stamps.last()) {
        (Some((lo, _)), Some((hi, _))) => hi.saturating_sub(*lo),
        _ => 0,
    };
    if skew_ms > 0 {
        warn!(
            skew_ms,
            oldest = ?oldest_export,
            newest = ?newest_export,
            "restore: the selected exports were taken at DIFFERENT moments — this set is only \
             as fresh as its OLDEST member, so that node's sessions are restored as they were \
             then (a session deleted since will reappear; messages already acked and drained \
             will be re-queued). Take a fresh export on every node before a planned restore"
        );
    }
    let mut plan = RestorePlan {
        files: files.clone(),
        superseded,
        skew_ms,
        oldest_export,
        newest_export,
        ..RestorePlan::default()
    };
    // client id -> (token, record): the highest (epoch, high_offset) wins, so file order
    // cannot change the outcome.
    let mut sessions: BTreeMap<String, (Token, SessionRecord)> = BTreeMap::new();
    // topic -> the winning candidate so far (see `retained_supersedes`).
    let mut retained: BTreeMap<String, RetainedCandidate> = BTreeMap::new();
    let mut not_owned: BTreeSet<String> = BTreeSet::new();
    let mut members: BTreeSet<String> = BTreeSet::new();
    let mut node_ids: BTreeSet<String> = BTreeSet::new();
    // cluster id -> the first file that named it, for the mixed-cluster refusal.
    let mut clusters: BTreeMap<String, PathBuf> = BTreeMap::new();
    let mut digests: Vec<String> = Vec::new();

    for file in &files {
        let (header, trailer, file_sessions, file_retained) = parse_file(file)?;
        node_ids.insert(header.node_id.clone());
        members.extend(header.members.iter().cloned());
        not_owned.extend(trailer.not_owned.iter().cloned());
        digests.push(trailer.sha256.clone());
        if let Some(id) = &header.cluster_id {
            clusters.entry(id.clone()).or_insert_with(|| file.clone());
        }
        for record in file_sessions {
            let token = record.token;
            match sessions.get(&record.client) {
                Some((held, _)) if *held >= token => {}
                _ => {
                    sessions.insert(record.client.clone(), (token, record));
                }
            }
        }
        for record in file_retained {
            let candidate = RetainedCandidate {
                created_unix_ms: header.created_unix_ms,
                node_id: header.node_id.clone(),
                record,
            };
            match retained.get(&candidate.record.topic) {
                Some(held) if !retained_supersedes(&candidate, held) => {}
                _ => {
                    retained.insert(candidate.record.topic.clone(), candidate);
                }
            }
        }
    }

    // Defence in depth only: the authoritative check ran over EVERY file found, before
    // generation selection could hide one cluster behind another (see `load_with`'s hoist).
    // This one can only fire if the selected set disagrees with the full set, which would
    // mean a bug above rather than an operator slip.
    refuse_mixed_clusters(&clusters)?;

    // Coverage — the check that turns "run the export on every node" from an instruction
    // into a verified precondition. Two independent halves:
    //   (1) every node the exporting nodes could SEE must have supplied a file;
    //   (2) every client id skipped as `not_owned` must be present, owned, in some file.
    let missing_nodes: Vec<String> = members.difference(&node_ids).cloned().collect();
    if !missing_nodes.is_empty() {
        if coverage == Coverage::Complete {
            return Err(format!(
                "restore: REFUSED — the supplied exports name cluster members with no export \
                 of their own: {missing_nodes:?}. A per-node export is not a cluster \
                 snapshot; supply every node's file. If a node's data AND its export are \
                 permanently lost, restore the rest KNOWINGLY: set \
                 backup.restore_partial_accept_data_loss = true \
                 (MQTTD_RESTORE_PARTIAL_ACCEPT_DATA_LOSS=1), which imports the surviving \
                 nodes' data and FORFEITS every session those nodes owned. Files read: {}",
                files.len()
            ));
        }
        plan.forfeited_nodes = missing_nodes;
    }
    let missing_clients: Vec<String> = not_owned
        .iter()
        .filter(|c| !sessions.contains_key(*c))
        .cloned()
        .collect();
    if !missing_clients.is_empty() {
        if coverage == Coverage::Complete {
            return Err(format!(
                "restore: REFUSED — {} session(s) were skipped as owned-elsewhere by the \
                 exports supplied and appear in none of them: {missing_clients:?}. Their \
                 owner's export is missing, or the session changed owner between two exports \
                 — restoring now would silently lose them. To accept that loss knowingly, set \
                 backup.restore_partial_accept_data_loss = true \
                 (MQTTD_RESTORE_PARTIAL_ACCEPT_DATA_LOSS=1)",
                missing_clients.len()
            ));
        }
        plan.forfeited_clients = missing_clients;
    }
    if plan.is_partial() {
        warn!(
            forfeited_nodes = ?plan.forfeited_nodes,
            forfeited_sessions = plan.forfeited_clients.len(),
            forfeited_clients = ?plan.forfeited_clients,
            files = plan.files.len(),
            "restore: PARTIAL restore accepted by explicit opt-in \
             (backup.restore_partial_accept_data_loss): the restored cluster will NOT contain \
             those nodes' sessions, their queued messages, or any retained value only they \
             held. This is recorded in the restored-from stamp permanently"
        );
    }

    plan.sessions = sessions.into_values().map(|(_, r)| r).collect();
    for candidate in retained.into_values() {
        if candidate.record.tombstone {
            plan.cleared_topics.push(candidate.record.topic);
        } else {
            plan.retained.push(candidate.record);
        }
    }
    // The set's identity, order-independent: the files' own trailer digests, sorted.
    digests.sort();
    plan.set_sha256 = sha256_hex(digests.join("\n").as_bytes());
    Ok(plan)
}

/// One file's copy of a retained topic, with the two facts that order it.
struct RetainedCandidate {
    created_unix_ms: u64,
    node_id: String,
    record: RetainedRecord,
}

/// Whether `new` is a strictly later retained value for its topic than `held` — rule 2 of
/// [`load_with`], and the only place the answer is decided.
fn retained_supersedes(new: &RetainedCandidate, held: &RetainedCandidate) -> bool {
    match (new.record.token, held.record.token) {
        // The cluster's own order (ADR 0037 P2). Equal tokens are the SAME committed
        // record seen by two nodes, so the incumbent stands and the answer is stable.
        (Some(a), Some(b)) => a > b,
        // A committed token beats an unattributed cache value: a value reaches the token
        // map at the moment it is applied, so a cached value with no token is one this node
        // held from before its last restart — older by construction.
        (Some(_), None) => true,
        (None, Some(_)) => false,
        // Durable retained off: there is no cluster-wide order to appeal to, so the newest
        // export wins, and the node id breaks a tie only to make the result deterministic
        // (never because a node id means anything).
        (None, None) => {
            (new.created_unix_ms, new.node_id.as_str())
                > (held.created_unix_ms, held.node_id.as_str())
        }
    }
}

/// Group the files by node id and keep the NEWEST generation of each (rule 1 of
/// [`load_with`]), returning `(selected, superseded)`.
///
/// # Errors
/// A message naming both files when one node's two exports share a `created_unix_ms`.
fn select_generations(found: &[PathBuf]) -> Result<(Vec<PathBuf>, Vec<PathBuf>), String> {
    // node id -> (created_unix_ms, path), newest kept.
    let mut newest: BTreeMap<String, (u64, PathBuf)> = BTreeMap::new();
    let mut superseded: Vec<PathBuf> = Vec::new();
    for path in found {
        // An unreadable or wrong-version head is SKIPPED, not fatal — this function's whole
        // point is that selecting a generation does not parse the ones it discards, and a
        // corrupt file among six superseded generations must not block a disaster recovery.
        // Safe because the consequence is caught downstream: if the skipped file was a node's
        // ONLY export, that node has no export in the set and the coverage check refuses (or
        // forfeits it under the explicit opt-in) by name.
        let header = match parse_head_only(path) {
            Ok(h) => h,
            Err(e) => {
                warn!(
                    file = %path.display(),
                    error = %e,
                    "restore: ignoring an unreadable export while selecting generations; if \
                     this was a node's only export, the coverage check below will name that \
                     node as missing"
                );
                continue;
            }
        };
        // Parsed, so the format/version verdict is authoritative and FATAL either way — a
        // file written by a newer build is refused loudly (ADR 0058), superseded or not.
        check_format(&header, path)?;
        let incumbent = newest.get(&header.node_id).cloned();
        match incumbent {
            Some((held_ms, held_path)) if held_ms == header.created_unix_ms => {
                return Err(format!(
                    "restore: REFUSED — two exports of node {:?} carry the SAME \
                     created_unix_ms ({}): {} and {}. Which one is newer is not decidable, \
                     and a restore must never guess between two generations of one node — \
                     keep the one you mean and move the other out of the directory. Nothing \
                     was imported",
                    header.node_id,
                    header.created_unix_ms,
                    held_path.display(),
                    path.display()
                ));
            }
            Some((held_ms, _)) if held_ms > header.created_unix_ms => {
                superseded.push(path.clone());
            }
            Some((_, held_path)) => {
                superseded.push(held_path);
                newest.insert(header.node_id, (header.created_unix_ms, path.clone()));
            }
            None => {
                newest.insert(header.node_id, (header.created_unix_ms, path.clone()));
            }
        }
    }
    let mut selected: Vec<PathBuf> = newest.into_values().map(|(_, p)| p).collect();
    selected.sort();
    superseded.sort();
    if !superseded.is_empty() {
        info!(
            selected = selected.len(),
            superseded = superseded.len(),
            superseded_files = ?superseded.iter().map(|p| p.file_name().unwrap_or_default().to_string_lossy().into_owned()).collect::<Vec<_>>(),
            "restore: several generations per node are present; reading only the NEWEST \
             export of each node and IGNORING the older ones (ADR 0062: a set is one \
             generation per node, never a record-by-record merge of two moments)"
        );
    }
    Ok((selected, superseded))
}

/// The header fields needed to place a file in the set, read from its FIRST LINE only —
/// so selecting a generation never costs a full parse of the ones it discards, and a
/// corrupt older generation cannot fail a restore that does not read it.
///
/// Every field defaults, so a header from a future build still parses far enough for the
/// version gate below to produce the actionable refusal instead of a serde error.
#[derive(Deserialize, Default)]
struct HeadOnly {
    #[serde(default)]
    format: String,
    #[serde(default)]
    format_version: u32,
    #[serde(default)]
    binary_version: String,
    #[serde(default)]
    node_id: String,
    #[serde(default)]
    created_unix_ms: u64,
    /// Needed here, not only on the full header: the mixed-cluster refusal must run over
    /// every file BEFORE generation selection keys on `node_id` and hides one cluster behind
    /// another, and selection deliberately reads only this first line.
    #[serde(default)]
    cluster_id: Option<String>,
}

/// Refuse a restore set drawn from more than one cluster, naming both ids and an example
/// file for each.
///
/// Called over EVERY discovered file before generation selection, because selection keys on
/// node id: two clusters that name their nodes identically (`mqttd-0..2` in staging and in
/// prod) present as several generations of one node, and the loser is discarded in silence.
/// Merging two clusters' sessions and retained state into one cluster is not a recovery, and
/// the header already holds the evidence — the coverage check cannot see it, `cluster_id`
/// can.
fn refuse_mixed_clusters(clusters: &BTreeMap<String, PathBuf>) -> Result<(), String> {
    if clusters.len() <= 1 {
        return Ok(());
    }
    let named: Vec<String> = clusters
        .iter()
        .map(|(id, file)| {
            format!(
                "{id} (e.g. {})",
                file.file_name().unwrap_or_default().to_string_lossy()
            )
        })
        .collect();
    Err(format!(
        "restore: REFUSED — the supplied exports come from {} DIFFERENT clusters: {}. \
         Two clusters' backups in one directory (staging beside prod, or a stale bundle left \
         behind) name their nodes identically, so neither the coverage check nor the \
         newest-generation-per-node selection can see it; the cluster_id in each header can. \
         Restore ONE cluster's set: move the other files out of the way. Nothing was imported",
        clusters.len(),
        named.join(", ")
    ))
}

fn read_head(path: &Path) -> Result<HeadOnly, String> {
    let head = parse_head_only(path)?;
    check_format(&head, path)?;
    Ok(head)
}

/// The header line, parsed but NOT format-checked.
///
/// The two failure kinds are deliberately separate. A head that cannot be read or parsed is
/// UNCLASSIFIABLE — it names no node and no cluster — so generation selection skips it loudly
/// rather than failing a restore that would never have read it (and the coverage check catches
/// the case where it was a node's only export). A head that parses but declares a format or
/// version this build does not accept is a different animal: ADR 0058's posture is to refuse
/// LOUDLY with no pre-1.0 migration path, and that refusal must not be downgraded to a skip
/// just because the file happened to be a superseded generation.
fn parse_head_only(path: &Path) -> Result<HeadOnly, String> {
    use std::io::BufRead;
    let file = std::fs::File::open(path)
        .map_err(|e| format!("restore: cannot read {}: {e}", path.display()))?;
    let mut line = String::new();
    std::io::BufReader::new(file)
        .read_line(&mut line)
        .map_err(|e| format!("restore: cannot read {}: {e}", path.display()))?;
    serde_json::from_str(line.trim_end()).map_err(|e| {
        format!(
            "restore: {}: cannot parse the header line: {e}",
            path.display()
        )
    })
}

/// The format/version gate, applied to every file in the directory — including a
/// generation that would then be discarded, because a file this build cannot read is
/// evidence about the set and must not pass silently (ADR 0058).
fn check_format(head: &HeadOnly, path: &Path) -> Result<(), String> {
    let name = path.display();
    if head.format != FORMAT {
        return Err(format!(
            "restore: {name}: not an {FORMAT} file (format {:?})",
            head.format
        ));
    }
    if head.format_version > FORMAT_VERSION {
        return Err(format!(
            "restore: {name}: format_version {} is NEWER than this build reads ({}); it was \
             written by mqttd {} — restore it with that build (or newer). Nothing was imported",
            head.format_version, FORMAT_VERSION, head.binary_version
        ));
    }
    if head.format_version < FORMAT_VERSION {
        return Err(format!(
            "restore: {name}: format_version {} is older than this build reads ({}); no \
             migration path exists pre-1.0. Nothing was imported",
            head.format_version, FORMAT_VERSION
        ));
    }
    Ok(())
}

fn collect_files(path: &Path) -> Result<Vec<PathBuf>, String> {
    let meta = std::fs::metadata(path)
        .map_err(|e| format!("restore: cannot read {}: {e}", path.display()))?;
    if meta.is_file() {
        return Ok(vec![path.to_path_buf()]);
    }
    let mut files: Vec<PathBuf> = std::fs::read_dir(path)
        .map_err(|e| format!("restore: cannot read {}: {e}", path.display()))?
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(FORMAT) && n.ends_with(SUFFIX))
        })
        .collect();
    files.sort();
    Ok(files)
}

type ParsedFile = (Header, Trailer, Vec<SessionRecord>, Vec<RetainedRecord>);

/// Just the `kind` discriminator of a record line, read before the record itself so an
/// unknown kind is a named refusal rather than a parse error.
#[derive(Deserialize)]
struct KindOnly {
    kind: String,
}

// One linear parse: header gate, trailer gate, digest, then a record per line. Long by
// the number of refusals it owes the operator, not by branching complexity.
#[allow(clippy::too_many_lines)]
fn parse_file(path: &Path) -> Result<ParsedFile, String> {
    let bytes =
        std::fs::read(path).map_err(|e| format!("restore: cannot read {}: {e}", path.display()))?;
    let name = path.display();
    let mut lines = bytes.split_inclusive(|b| *b == b'\n');
    let header_line = lines
        .next()
        .ok_or_else(|| format!("restore: {name} is empty"))?;
    let header: Header = serde_json::from_slice(header_line)
        .map_err(|e| format!("restore: {name}: cannot parse the header line: {e}"))?;
    if header.format != FORMAT {
        return Err(format!(
            "restore: {name}: not an {FORMAT} file (format {:?})",
            header.format
        ));
    }
    if header.format_version > FORMAT_VERSION {
        return Err(format!(
            "restore: {name}: format_version {} is NEWER than this build reads ({}); it was \
             written by mqttd {} — restore it with that build (or newer). Nothing was imported",
            header.format_version, FORMAT_VERSION, header.binary_version
        ));
    }
    if header.format_version < FORMAT_VERSION {
        return Err(format!(
            "restore: {name}: format_version {} is older than this build reads ({}); no \
             migration path exists pre-1.0. Nothing was imported",
            header.format_version, FORMAT_VERSION
        ));
    }

    // Everything up to (not including) the last line is covered by the trailer's digest.
    let rest: Vec<&[u8]> = lines.collect();
    let Some((trailer_line, records)) = rest.split_last() else {
        return Err(format!(
            "restore: {name}: the file has a header and nothing else — no trailer, so it is a \
             truncated export, not a backup"
        ));
    };
    let trailer: Trailer = serde_json::from_slice(trailer_line).map_err(|e| {
        format!(
            "restore: {name}: the last line is not a trailer ({e}) — the export was \
             interrupted (a truncated file is never imported)"
        )
    })?;
    if trailer.kind != "trailer" {
        return Err(format!(
            "restore: {name}: the last line is a {:?} record, not a trailer — the export was \
             interrupted",
            trailer.kind
        ));
    }
    if !trailer.complete {
        return Err(format!(
            "restore: {name}: the export itself reports an INCOMPLETE session scan; it is \
             missing sessions and must not be restored from"
        ));
    }
    let mut digest_input = Vec::with_capacity(bytes.len());
    digest_input.extend_from_slice(header_line);
    for r in records {
        digest_input.extend_from_slice(r);
    }
    let found = sha256_hex(&digest_input);
    if found != trailer.sha256 {
        return Err(format!(
            "restore: {name}: sha256 mismatch (file says {}, bytes hash to {found}) — the \
             export is truncated or altered",
            trailer.sha256
        ));
    }

    let mut sessions = Vec::new();
    let mut retained = Vec::new();
    for (i, line) in records.iter().enumerate() {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let kind: KindOnly = serde_json::from_slice(line)
            .map_err(|e| format!("restore: {name}: line {} is not a record: {e}", i + 2))?;
        match kind.kind.as_str() {
            "session" => sessions.push(
                serde_json::from_slice(line)
                    .map_err(|e| format!("restore: {name}: line {}: {e}", i + 2))?,
            ),
            "retained" => retained.push(
                serde_json::from_slice(line)
                    .map_err(|e| format!("restore: {name}: line {}: {e}", i + 2))?,
            ),
            other => {
                return Err(format!(
                    "restore: {name}: line {} has unknown record kind {other:?}. Refusing \
                     rather than skipping it: a record this build cannot read is data an \
                     operator asked for, and silently dropping it is exactly the failure a \
                     restore must not have. Use a build that understands it",
                    i + 2
                ))
            }
        }
    }
    Ok((header, trailer, sessions, retained))
}

/// What an applied restore did.
#[derive(Debug, Clone, Default)]
pub struct RestoreReport {
    /// Sessions written by this node.
    pub sessions: u64,
    /// Queued messages written.
    pub queued: u64,
    /// Sessions another node owns (this node skipped them; that node's restore writes
    /// them — every node imports the same set of files).
    pub skipped_not_owner: u64,
    /// Retained topics published.
    pub retained: u64,
    /// Retained topics dropped because their absolute expiry deadline had already passed.
    pub retained_expired: u64,
    /// Sessions already present in the target when this node got to them — another node's
    /// restore of the same file set claimed them first (or an ownership hand-off happened
    /// mid-restore). Counted, never re-written: a second import would duplicate a queue.
    pub already_present: u64,
    /// The client ids this node imported (bounded to the first 32 for the log line).
    pub imported_clients: Vec<String>,
    /// The client ids it skipped as owned elsewhere (bounded likewise). Named because
    /// "some sessions went to other nodes" is unverifiable and "these did" is not.
    pub skipped_clients: Vec<String>,
}

/// How the caller applies a restored retained value.
///
/// The importer does not touch the retained store directly: a retained mutation must commit
/// through the topic's group lease-owner (ADR 0037) or it would not converge cluster-wide,
/// and that path lives in the hub.
///
/// **It must write retained state AS retained state, and must not fan the value out to
/// subscribers.** A restore that re-publishes each retained value as an ordinary publish
/// does not reproduce the backup: the publish reaches every durable OFFLINE session whose
/// restored subscription matches the topic — no client listener needs to be bound, because
/// an offline session's queue is the hub's to append to — so each such session gains one
/// spurious queued message per matching retained topic PER NODE, none of which were in the
/// export. At `QoS` 2 that is an exactly-once violation introduced by the recovery tool
/// itself. See [`crate::hub::HubCommand::RestoreRetained`], which is this seam's real
/// implementation: it commits through the authority and fans out only to PEER CACHES.
#[async_trait::async_trait]
pub trait RetainedSink: Send + Sync {
    /// Apply one retained value, returning an error message if it was refused.
    async fn publish(
        &self,
        record: &RetainedRecord,
        message_expiry: Option<u32>,
    ) -> Result<(), String>;
}

/// Consecutive rounds with no progress after which a `NotOwner` is taken as final (see
/// [`apply`]): 3 s of a settled ring, which is well past lease-assignment convergence and
/// short enough that a restore is not padded by it.
const SETTLE_ROUNDS: u32 = 3;

/// Apply `plan` to this node.
///
/// Sessions are written through the ORDINARY store API — `claim_session` (so the owner
/// binding of ADR 0031 is reproduced and a foreign principal cannot adopt a restored
/// session), `set_subscriptions`, `set_session_expiry`, `enqueue_with_expiry`,
/// `record_received`/`ack_received` (reproducing held-unacked vs held-acked, issue #238),
/// `record_outbound`/`advance_outbound` (ADR 0057's `pubrec_seen`), and
/// `reserve_packet_ids` for the id high-water. So restored state is quorum-replicated and
/// placed by the CURRENT ring, exactly as if the clients had produced it.
///
/// In cluster mode a node may only write the keys it owns, so a session owned elsewhere is
/// skipped with `NotOwner` — every node imports the same files, so each session lands
/// exactly once, on its owner. A *transient* failure is retried and then fails the
/// restore: a half-imported store must never serve.
///
/// # Errors
/// A message naming the client id or topic that could not be written.
pub async fn apply(
    plan: &RestorePlan,
    sessions: &Arc<dyn SessionStore>,
    retained: &dyn RetainedSink,
) -> Result<RestoreReport, String> {
    let mut report = RestoreReport::default();
    // Sessions in ROUNDS. A `NotOwner` is not final on a cluster that has just assembled:
    // lease assignment converges over seconds, and while it does two nodes can each believe
    // the other owns a group (the transient half of the 2026-07-20 ring/lease split). A
    // single pass would then drop that session on the floor with nobody importing it —
    // silently, which is the one outcome a restore must never have. So the not-owned set is
    // re-attempted until it stops shrinking for `SETTLE_ROUNDS` consecutive rounds; each
    // attempt is a placement-lock read on a foreign key, so retrying is cheap.
    let mut pending: Vec<&SessionRecord> = plan.sessions.iter().collect();
    let mut quiet = 0u32;
    while !pending.is_empty() {
        let mut still = Vec::new();
        let mut progressed = false;
        for record in pending {
            match apply_session(record, sessions).await? {
                SessionOutcome::Imported(queued) => {
                    progressed = true;
                    report.sessions += 1;
                    report.queued += queued;
                    if report.imported_clients.len() < 32 {
                        report.imported_clients.push(record.client.clone());
                    }
                }
                SessionOutcome::AlreadyPresent => {
                    progressed = true;
                    report.already_present += 1;
                }
                SessionOutcome::NotOwner => still.push(record),
            }
        }
        pending = still;
        quiet = if progressed { 0 } else { quiet + 1 };
        if pending.is_empty() || quiet >= SETTLE_ROUNDS {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    for record in pending {
        report.skipped_not_owner += 1;
        if report.skipped_clients.len() < 32 {
            report.skipped_clients.push(record.client.clone());
        }
    }
    let now = unix_millis() / 1000;
    for record in &plan.retained {
        let expiry = match record.expires_at {
            None => None,
            Some(deadline) if deadline > now => {
                Some(u32::try_from(deadline - now).unwrap_or(u32::MAX))
            }
            Some(_) => {
                // Already past its deadline: restoring it would publish a value the broker
                // must immediately delete. Counted, not hidden.
                report.retained_expired += 1;
                continue;
            }
        };
        retained.publish(record, expiry).await?;
        report.retained += 1;
    }
    Ok(report)
}

/// What happened to one session record on this node.
enum SessionOutcome {
    /// Written here, with this many queued messages.
    Imported(u64),
    /// Another node owns the key: its own restore of the same files writes it.
    NotOwner,
    /// Already in the target — claimed by another node's restore first.
    AlreadyPresent,
}

/// Apply one session.
async fn apply_session(
    record: &SessionRecord,
    store: &Arc<dyn SessionStore>,
) -> Result<SessionOutcome, String> {
    let client = ClientId(record.client.clone());
    // Retry a transient refusal (a quorum momentarily unreachable) before giving up: a
    // restore runs at cold start, where those are expected and self-healing. `NotOwner` is
    // NOT retried here — it is retried in ROUNDS by the caller, because a fresh cluster's
    // lease assignment can still be in flux and a node that answers `NotOwner` this second
    // may own the key the next (2026-07-20's ring/lease split, transient form).
    let mut attempt = 0;
    loop {
        match import_session_once(record, &client, store).await {
            Ok(v) => return Ok(v),
            Err(StorageError::NotOwner) => return Ok(SessionOutcome::NotOwner),
            Err(e) if e.is_transient() && attempt < 30 => {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(e) => {
                return Err(format!(
                    "restore: cannot write session {:?}: {e}. Nothing further was imported — a \
                     half-imported store must never serve",
                    record.client
                ))
            }
        }
    }
}

async fn import_session_once(
    record: &SessionRecord,
    client: &ClientId,
    store: &Arc<dyn SessionStore>,
) -> Result<SessionOutcome, StorageError> {
    // The owner binding first, and through `claim_session`: a record written with no owner
    // is adopted by its next claimant (SessionClaim::Granted), which would hand a restored
    // session to whoever connects first.
    // The claim is also the CLAIM CHECK: `present = true` means the record is already in
    // the target, so another node's restore of the same files got there first (or ownership
    // moved mid-restore). Importing again would append the queue a second time and deliver
    // every message twice, so this returns instead.
    match &record.owner {
        Some(owner) => match store.claim_session(client, owner).await? {
            SessionClaim::Granted { present: false } => {}
            SessionClaim::Granted { present: true } => return Ok(SessionOutcome::AlreadyPresent),
            SessionClaim::Denied { owner } => {
                return Err(StorageError::Backend(format!(
                    "the target already binds this client id to {owner:?}"
                )))
            }
        },
        // A legacy record that never carried an owner restores as it was — unbound.
        None => {
            if store.ensure_session(client).await? {
                return Ok(SessionOutcome::AlreadyPresent);
            }
        }
    }
    let subs: Vec<Subscription> = record
        .subscriptions
        .iter()
        .map(|s| Subscription {
            filter: s.filter.clone(),
            max_qos: qos_from_u8(s.max_qos),
            no_local: s.no_local,
        })
        .collect();
    store.set_subscriptions(client, &subs).await?;
    store
        .set_session_expiry(client, record.session_expiry_at)
        .await?;

    // The queue, in offset order. Offsets are reassigned by the target log, so keep the
    // old→new map for the outbound in-flight entries that reference them.
    let mut offsets = BTreeMap::new();
    let mut queued = 0u64;
    for q in &record.queue {
        let message = Message {
            topic: q.topic.clone(),
            payload: bytes::Bytes::from(b64_decode(&q.payload_b64).map_err(StorageError::Backend)?),
            qos: qos_from_u8(q.qos),
            retain: false,
            app: app_props(&q.props).map_err(StorageError::Backend)?,
            expires_at: q.expiry_at,
        };
        if let mqtt_storage::Enqueued::Stored { offset, .. } = store
            .enqueue_with_expiry(client, &message, q.expiry_at)
            .await?
        {
            offsets.insert(q.offset, offset);
            queued += 1;
        }
    }
    // The inbound QoS-2 dedup window, reproducing BOTH states (issue #238): a record is
    // written held-unacked, and only an entry whose PUBREC was released is acked.
    for entry in &record.received_qos2 {
        store.record_received(client, entry.packet_id).await?;
        if entry.acked {
            store.ack_received(client, entry.packet_id).await?;
        }
    }
    // The outbound QoS-2 in-flight window (ADR 0057), against the RE-MAPPED offset.
    for entry in &record.outbound_qos2 {
        let offset = offsets.get(&entry.offset).copied().unwrap_or(entry.offset);
        store
            .record_outbound(client, entry.packet_id, offset)
            .await?;
        if entry.pubrec_seen {
            store.advance_outbound(client, entry.packet_id).await?;
        }
    }
    // The packet-id high-water LAST: every write above is a read-modify-write of the same
    // metadata snapshot, and this is the one that must survive.
    if record.last_packet_id > 0 {
        store
            .reserve_packet_ids(client, record.last_packet_id)
            .await?;
    }
    Ok(SessionOutcome::Imported(queued))
}

fn app_props(p: &PropsRecord) -> Result<AppProperties, String> {
    Ok(AppProperties {
        payload_format: p.payload_format,
        content_type: p.content_type.clone(),
        response_topic: p.response_topic.clone(),
        correlation_data: match &p.correlation_data_b64 {
            Some(v) => Some(bytes::Bytes::from(b64_decode(v)?)),
            None => None,
        },
        user_properties: p.user_properties.clone(),
    })
}

/// Rebuild a message from a QUEUED record — the same decode the importer does, exposed so a
/// test (or an operator's own tooling) can compare a restored queue against the exported
/// one message by message rather than by count.
///
/// # Errors
/// A message naming the base64 field that would not decode.
pub fn queued_message(record: &QueuedRecord) -> Result<Message, String> {
    Ok(Message {
        topic: record.topic.clone(),
        payload: bytes::Bytes::from(b64_decode(&record.payload_b64)?),
        qos: qos_from_u8(record.qos),
        retain: false,
        app: app_props(&record.props)?,
        expires_at: record.expiry_at,
    })
}

/// Rebuild a message from a retained record (for the caller's publish path).
///
/// # Errors
/// A message naming the base64 field that would not decode.
pub fn retained_message(record: &RetainedRecord) -> Result<Message, String> {
    Ok(Message {
        topic: record.topic.clone(),
        payload: bytes::Bytes::from(b64_decode(&record.payload_b64)?),
        qos: qos_from_u8(record.qos),
        retain: true,
        app: app_props(&record.props)?,
        expires_at: record.expires_at,
    })
}

fn qos_from_u8(v: u8) -> QoS {
    match v {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    }
}

/// The on-disk record of a COMPLETED restore, written into the data dir.
///
/// It is provenance an incident responder can read, and it is also the reason a restored
/// node can be rebooted: the `restore_from` setting lives in a pod spec or a unit file and
/// does not disappear after the restore, so the node's own next start meets it again. This
/// stamp is how that start knows the import already happened (see [`restore_disposition`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreStamp {
    /// The stamp layout version.
    pub stamp_version: u32,
    /// The `backup.restore_from` value the restore was driven by, verbatim — the key the
    /// next boot compares against, because a directory that keeps receiving exports has a
    /// stable PATH and a changing file list.
    pub restored_from: String,
    /// When the restore completed (RFC 3339, UTC).
    pub restored_at: String,
    /// The same instant in Unix milliseconds.
    pub restored_unix_ms: u64,
    /// The file names actually imported — one generation per node.
    pub files: Vec<String>,
    /// The set's identity: sha-256 over the files' own trailer digests, sorted.
    pub set_sha256: String,
    /// Whether this was a PARTIAL restore that knowingly forfeited data.
    pub partial: bool,
    /// The span between the oldest and newest export in the restored set, in milliseconds,
    /// with both named. Kept forever because it bounds how stale the recovered cluster is,
    /// and the log line that reported it will not survive the incident.
    pub skew_ms: u64,
    pub oldest_export: Option<(String, String)>,
    pub newest_export: Option<(String, String)>,
    /// Cluster members whose export was absent, and whose sessions were therefore
    /// forfeited. Kept forever: "what is missing from this cluster" must outlive the log.
    pub forfeited_nodes: Vec<String>,
    /// Client ids forfeited with them.
    pub forfeited_clients: Vec<String>,
    /// Sessions this node imported.
    pub sessions: u64,
    /// Queued messages this node imported.
    pub queued: u64,
    /// Retained topics this node applied.
    pub retained: u64,
}

/// What a start should do about `backup.restore_from` for this data dir.
#[derive(Debug)]
pub enum RestoreDisposition {
    /// A fresh data dir: run the import.
    Proceed,
    /// This data dir already holds a COMPLETED restore of the same set: the setting is
    /// inert and the node starts normally.
    AlreadyRestored(Box<RestoreStamp>),
    /// A stamp is present but not parseable (hand-edited, or written by another build).
    /// Still inert — a stamp means an import completed here — but worth saying out loud.
    AlreadyRestoredUnreadable(String),
}

/// Decide what `backup.restore_from` means for `dir` this boot.
///
/// Three outcomes, and the middle one is the whole point:
///
/// - **fresh dir** → [`RestoreDisposition::Proceed`].
/// - **a completed restore stamp naming the same source** → [`RestoreDisposition::AlreadyRestored`]:
///   the import already happened, so the node starts normally. Without this a successful
///   restore made the node unbootable — the setting is part of the pod spec, the data dir
///   now holds stores, and the next ordinary reschedule (OOM kill, rolling upgrade, node
///   drain) exited non-zero with "delete the volume's contents", which would destroy the
///   data just restored. A restore must be idempotent-or-inert on a second boot.
/// - **anything else** → a refusal: state with no stamp (a restore never merges into
///   existing data), or a stamp naming a DIFFERENT source (restoring a second backup set
///   into a node that already holds one is a merge by another name).
///
/// The check is deliberately *pre-open*, on the filesystem: emptiness read through the
/// cluster store is racy by construction — in a 3-node restore each node imports the slice
/// it owns, so a peer's already-imported keys would make the next node's emptiness check
/// fail — while "this node has never opened a store here" is local and race-free.
///
/// # Errors
/// A message naming what was found and the way forward.
pub fn restore_disposition(dir: &Path, requested_from: &str) -> Result<RestoreDisposition, String> {
    let stamp_path = dir.join(RESTORED_STAMP);
    if let Ok(raw) = std::fs::read_to_string(&stamp_path) {
        return match serde_json::from_str::<RestoreStamp>(&raw) {
            Ok(stamp) if stamp.restored_from == requested_from => {
                Ok(RestoreDisposition::AlreadyRestored(Box::new(stamp)))
            }
            Ok(stamp) => Err(format!(
                "restore: REFUSED — {} was already restored from {:?} at {} ({} file(s), set \
                 sha256 {}), and {:?} is a DIFFERENT source. A restore never merges two \
                 backup sets into one node. To restore {:?}, point node.data_dir at a fresh \
                 volume (or delete this one's contents deliberately). To start this node on \
                 the data it already holds, set backup.restore_from back to {:?} or remove it",
                dir.display(),
                stamp.restored_from,
                stamp.restored_at,
                stamp.files.len(),
                stamp.set_sha256,
                requested_from,
                requested_from,
                stamp.restored_from
            )),
            Err(e) => Ok(RestoreDisposition::AlreadyRestoredUnreadable(format!(
                "{}: {e}",
                stamp_path.display()
            ))),
        };
    }
    require_fresh_data_dir(dir)?;
    Ok(RestoreDisposition::Proceed)
}

/// Refuse a restore into a data dir that already holds store files (with no restore stamp
/// to explain them).
///
/// # Errors
/// A message naming the files it found and the two ways forward.
pub fn require_fresh_data_dir(dir: &Path) -> Result<(), String> {
    let mut found: Vec<String> = crate::store_watch::STORE_FILES
        .iter()
        .map(|(_, file)| *file)
        .filter(|f| dir.join(f).exists())
        .map(str::to_string)
        .collect();
    found.sort();
    if found.is_empty() {
        return Ok(());
    }
    Err(format!(
        "restore: REFUSED — {} already holds {found:?} and no {RESTORED_STAMP} stamp, so this \
         node is neither fresh nor a node this backup was already restored into. A restore \
         never merges into existing state (it would resurrect expired sessions beside current \
         ones) and an interrupted restore is never resumed. Restore into an empty data dir: \
         delete the volume's contents deliberately, or point node.data_dir at a new one",
        dir.display()
    ))
}

/// Record that this node was restored, from what, and what it forfeited.
///
/// # Errors
/// A message if the stamp cannot be written — the restore has already succeeded at that
/// point, so the caller logs rather than fails.
pub fn write_restored_stamp(
    dir: &Path,
    requested_from: &str,
    plan: &RestorePlan,
    report: &RestoreReport,
) -> Result<(), String> {
    let now_ms = unix_millis();
    let stamp = RestoreStamp {
        stamp_version: 1,
        restored_from: requested_from.to_string(),
        restored_at: rfc3339(now_ms / 1000),
        restored_unix_ms: now_ms,
        files: plan
            .files
            .iter()
            .map(|p| {
                p.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect(),
        set_sha256: plan.set_sha256.clone(),
        partial: plan.is_partial(),
        skew_ms: plan.skew_ms,
        oldest_export: plan.oldest_export.clone(),
        newest_export: plan.newest_export.clone(),
        forfeited_nodes: plan.forfeited_nodes.clone(),
        forfeited_clients: plan.forfeited_clients.clone(),
        sessions: report.sessions,
        queued: report.queued,
        retained: report.retained,
    };
    let body = serde_json::to_string_pretty(&stamp)
        .map_err(|e| format!("restore: cannot encode the {RESTORED_STAMP} stamp: {e}"))?;
    // WRITE, FSYNC, RENAME, then fsync the DIRECTORY — the same discipline the export itself
    // uses, and for a sharper reason: this stamp is the only thing that lets a restored node
    // reboot (`restore_disposition`), while the data it licenses was already fsynced by redb.
    // A bare `fs::write` leaves a window in which a power loss yields a complete restore with
    // no stamp, and the next boot then meets `require_fresh_data_dir` on a full data dir —
    // whose printed remedy is to delete the very data just recovered.
    let final_path = dir.join(RESTORED_STAMP);
    let tmp_path = dir.join(format!("{RESTORED_STAMP}.partial"));
    {
        use std::io::Write as _;
        let mut f = std::fs::File::create(&tmp_path)
            .map_err(|e| format!("restore: cannot write the {RESTORED_STAMP} stamp: {e}"))?;
        f.write_all(format!("{body}\n").as_bytes())
            .map_err(|e| format!("restore: cannot write the {RESTORED_STAMP} stamp: {e}"))?;
        f.sync_all()
            .map_err(|e| format!("restore: cannot fsync the {RESTORED_STAMP} stamp: {e}"))?;
    }
    std::fs::rename(&tmp_path, &final_path)
        .map_err(|e| format!("restore: cannot install the {RESTORED_STAMP} stamp: {e}"))?;
    // The rename itself must be durable, or the stamp can still vanish with the directory
    // entry unflushed.
    if let Ok(d) = std::fs::File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The in-process task: a schedule + SIGUSR2, and the state /statusz reports.
// ---------------------------------------------------------------------------

/// The backup/restore state shared with `/statusz` and `/readyz` (ADR 0054's shape: the
/// hub and watchers keep a snapshot, health reads it).
#[derive(Debug, Default)]
pub struct BackupStatus {
    /// Unix seconds the last SUCCESSFUL export started (0 = none yet).
    last_ok_unix: AtomicU64,
    /// The last run's wall clock, milliseconds.
    last_duration_ms: AtomicU64,
    /// The last successful export's window width, milliseconds.
    last_window_ms: AtomicU64,
    /// Records in the last successful export.
    last_records: AtomicU64,
    /// The last error, if the most recent run failed.
    last_error: Mutex<Option<String>>,
    /// Restore state: 0 none, 1 in progress, 2 completed, 3 failed.
    restore_state: AtomicI64,
    /// A short description of the restore outcome.
    restore_detail: Mutex<Option<String>>,
    /// Whether a schedule is configured (so `/statusz` can distinguish "off" from "never
    /// succeeded", which the `== 0` guard clause in the alert rule also depends on).
    scheduled: AtomicBool,
}

impl BackupStatus {
    /// Note that a schedule is configured.
    pub fn set_scheduled(&self, on: bool) {
        self.scheduled.store(on, Ordering::Release);
    }

    /// Record a successful export.
    pub fn record_ok(&self, report: &ExportReport, duration_ms: u64) {
        self.last_ok_unix
            .store(report.started_unix_ms / 1000, Ordering::Release);
        self.last_duration_ms.store(duration_ms, Ordering::Release);
        self.last_window_ms
            .store(report.window_ms(), Ordering::Release);
        self.last_records.store(
            report.sessions + report.queued + report.retained,
            Ordering::Release,
        );
        *self.error_slot() = None;
    }

    /// Record a failed export run.
    pub fn record_error(&self, error: &str, duration_ms: u64) {
        self.last_duration_ms.store(duration_ms, Ordering::Release);
        *self.error_slot() = Some(error.to_string());
    }

    /// Publish the restore state: 0 none, 1 in progress, 2 completed, 3 failed.
    pub fn set_restore(&self, state: i64, detail: Option<String>) {
        self.restore_state.store(state, Ordering::Release);
        *self
            .restore_slot()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = detail;
    }

    /// Whether a restore is running right now (`/readyz` must report `NotReady`: the client
    /// listeners are not bound yet, so Ready would send an orchestrator's traffic to a
    /// closed port).
    #[must_use]
    pub fn restore_in_progress(&self) -> bool {
        self.restore_state.load(Ordering::Acquire) == 1
    }

    /// The `/statusz` JSON fragment: `"backup":{…},"restore":{…}`, or `None` when neither
    /// has ever been configured or run.
    #[must_use]
    pub fn statusz_fragment(&self) -> Option<String> {
        use std::fmt::Write;
        let last_ok = self.last_ok_unix.load(Ordering::Acquire);
        let restore = self.restore_state.load(Ordering::Acquire);
        let scheduled = self.scheduled.load(Ordering::Acquire);
        let error = self
            .error_slot_read()
            .clone()
            .map(|e| json_string_escape(&e));
        if last_ok == 0 && restore == 0 && !scheduled && error.is_none() {
            return None;
        }
        let mut s = format!(
            ",\"backup\":{{\"scheduled\":{scheduled},\"last_ok_unix\":{last_ok},\
             \"duration_ms\":{},\"window_ms\":{},\"records\":{}",
            self.last_duration_ms.load(Ordering::Acquire),
            self.last_window_ms.load(Ordering::Acquire),
            self.last_records.load(Ordering::Acquire),
        );
        if let Some(e) = error {
            let _ = write!(s, ",\"last_error\":\"{e}\"");
        }
        s.push('}');
        let state = match restore {
            1 => "in-progress",
            2 => "completed",
            3 => "failed",
            _ => "none",
        };
        let _ = write!(s, ",\"restore\":{{\"state\":\"{state}\"");
        if let Some(detail) = self.restore_detail_read() {
            let _ = write!(s, ",\"detail\":\"{}\"", json_string_escape(&detail));
        }
        s.push('}');
        Some(s)
    }

    fn error_slot(&self) -> std::sync::MutexGuard<'_, Option<String>> {
        self.last_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn error_slot_read(&self) -> std::sync::MutexGuard<'_, Option<String>> {
        self.error_slot()
    }

    fn restore_slot(
        &self,
    ) -> Result<
        std::sync::MutexGuard<'_, Option<String>>,
        std::sync::PoisonError<std::sync::MutexGuard<'_, Option<String>>>,
    > {
        self.restore_detail.lock()
    }

    fn restore_detail_read(&self) -> Option<String> {
        self.restore_slot()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

/// Minimal JSON string-body escaping for values that reach `/statusz`.
fn json_string_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => out.push(' '),
            c => out.push(c),
        }
    }
    out
}

/// Everything the backup task holds: the live store handles (never a redb handle of its
/// own), the destination, and the places a run is reported.
pub struct BackupTask {
    /// The export destination and identity.
    pub ctx: ExportContext,
    /// Seconds between scheduled exports; 0 = on demand only.
    pub every_secs: u64,
    /// The live session store.
    pub sessions: Arc<dyn SessionStore>,
    /// Where retained values and their convergence tokens are read from.
    pub retained: Arc<dyn RetainedSource>,
    /// Where a run is reported for `/statusz`.
    pub status: Arc<BackupStatus>,
    /// Where a run is counted for the RPO alert.
    pub metrics: Option<Arc<Metrics>>,
    /// The placement members to stamp in the header, read FRESH per run: the coverage
    /// check's precondition is "every member this node could see supplied an export", so a
    /// stale membership would weaken it. A closure keeps this module free of cluster types.
    pub members: Arc<dyn Fn() -> Vec<String> + Send + Sync>,
    /// The cluster identity to stamp in the header, also read FRESH per run — for the same
    /// reason and one more. A JOINER adopts the cluster id over gossip *after* its process
    /// starts, so a value snapshotted at construction is `None` on every node that has not
    /// restarted since its first boot: the provenance field the docs tell an incident
    /// responder to trust was empty exactly on a freshly deployed cluster, and the
    /// cross-cluster refusal in [`load_with`] had nothing to compare.
    pub cluster_id: Arc<dyn Fn() -> Option<String> + Send + Sync>,
}

impl std::fmt::Debug for BackupTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackupTask")
            .field("dir", &self.ctx.dir)
            .field("every_secs", &self.every_secs)
            .finish_non_exhaustive()
    }
}

impl BackupTask {
    /// Run one export and report it everywhere an operator might look.
    pub async fn run_once(&self, trigger: &str) {
        let started = std::time::Instant::now();
        let ctx = ExportContext {
            members: (self.members)(),
            cluster_id: (self.cluster_id)(),
            ..self.ctx.clone()
        };
        match export(&ctx, &self.sessions, &self.retained).await {
            Ok(report) => {
                let ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                info!(
                    trigger,
                    path = %report.path.display(),
                    bytes = report.bytes,
                    sessions = report.sessions,
                    queued = report.queued,
                    retained = report.retained,
                    not_owned = report.not_owned.len(),
                    window_ms = report.window_ms(),
                    "online backup written (ADR 0062): this node's readable state, not a \
                     cluster snapshot"
                );
                self.status.record_ok(&report, ms);
                if let Some(m) = &self.metrics {
                    m.backup_run("ok", ms, Some(report.started_unix_ms / 1000));
                }
            }
            Err(e) => {
                let ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                warn!(trigger, error = %e, "online backup FAILED; nothing was written");
                self.status.record_error(&e, ms);
                if let Some(m) = &self.metrics {
                    m.backup_run("error", ms, None);
                }
            }
        }
    }

    /// The task loop: an optional schedule, plus `SIGUSR2` on demand, stopping on the
    /// shutdown token.
    ///
    /// Cancellation matters: an export in flight when the process is asked to stop must
    /// end before the stores drop, or the data dir stays locked past exit and the next
    /// start fails with "Database already open" — the #242 failure mode, which would
    /// surface as a flaky restart under load rather than as a backup bug.
    pub async fn run(self, shutdown: tokio_util::sync::CancellationToken, mut demand: OnDemand) {
        self.status.set_scheduled(self.every_secs > 0);
        // Only with a schedule: a `0` here would be a 1-tick-per-instant loop, i.e. a
        // continuous export of the node it is meant to protect.
        let mut ticker = (self.every_secs > 0).then(|| {
            let mut t = tokio::time::interval(Duration::from_secs(self.every_secs));
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            t
        });
        if self.every_secs > 0 {
            info!(
                dir = %self.ctx.dir.display(),
                every_secs = self.every_secs,
                keep = self.ctx.keep,
                "scheduled online backups active (ADR 0062); alert on the age of \
                 mqttd_backup_last_success_timestamp_seconds"
            );
        } else {
            info!(
                dir = %self.ctx.dir.display(),
                "online backup available on demand (`mqttd --backup` / SIGUSR2); no schedule \
                 configured (backup.every_secs = 0)"
            );
        }
        loop {
            let tick = async {
                match ticker.as_mut() {
                    Some(t) => {
                        t.tick().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tick => self.run_once("schedule").await,
                () = demand.next() => self.run_once("signal").await,
            }
        }
    }
}

/// The on-demand trigger: `SIGUSR2`. SIGUSR1 is already the decommission signal
/// (ADR 0043 P3), and a signal is what a distroless image can be reached by.
pub struct OnDemand {
    #[cfg(unix)]
    signal: Option<tokio::signal::unix::Signal>,
}

impl std::fmt::Debug for OnDemand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OnDemand").finish_non_exhaustive()
    }
}

impl OnDemand {
    /// Wait for the next `SIGUSR2` (never resolves if the handler could not be installed).
    pub async fn next(&mut self) {
        #[cfg(unix)]
        if let Some(s) = self.signal.as_mut() {
            s.recv().await;
            return;
        }
        std::future::pending::<()>().await;
    }
}

/// Install the `SIGUSR2` handler — **unconditionally, at startup, before anything can send
/// one**, and regardless of whether `[backup] dir` is configured.
///
/// This is not a detail of the backup task. Installing the stream is what OVERRIDES the
/// signal's default disposition, and `SIGUSR2`'s default is *terminate*: while it was
/// installed inside the backup task (spawned only when a dir was configured), a node with
/// no `[backup] dir` — the default — was KILLED by the very signal the docs advertise as
/// "take a backup", with crash semantics: no drain, no readiness fail-first, in-flight
/// publishes lost. A monitoring or cron rollout that lands before the config does would
/// take down a serving broker on a node where the intended action was a no-op. The
/// decommission handler (`SIGUSR1`) has always been installed unconditionally; this matches
/// it, and [`log_no_backup_dir`] answers the signal honestly when there is nowhere to write.
#[must_use]
pub fn install_on_demand_signal() -> OnDemand {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::user_defined2()) {
            Ok(s) => OnDemand { signal: Some(s) },
            Err(e) => {
                // Worth a WARN either way, but note what it costs: with no stream the
                // default disposition stands, so a SIGUSR2 would terminate the process.
                warn!(
                    error = %e,
                    "cannot install the SIGUSR2 handler; on-demand backup is unavailable AND \
                     the signal keeps its default disposition (terminate) — do not send it"
                );
                OnDemand { signal: None }
            }
        }
    }
    #[cfg(not(unix))]
    {
        OnDemand {}
    }
}

/// Answer `SIGUSR2` on a node with no `[backup] dir`: say so, and keep serving.
///
/// The point of this task is the signal handler it holds (see [`install_on_demand_signal`]).
/// It also turns the trap into a diagnosis: an operator who signalled the wrong node, or
/// signalled before the config landed, gets a log line naming the missing setting instead of
/// a dead broker.
pub async fn log_no_backup_dir(
    mut demand: OnDemand,
    shutdown: tokio_util::sync::CancellationToken,
) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = demand.next() => warn!(
                "SIGUSR2 received (the online-backup trigger, ADR 0062) but no [backup] dir is \
                 configured (MQTTD_BACKUP_DIR): NOTHING was exported and the broker keeps \
                 serving. Configure a destination on a volume outside node.data_dir, then \
                 retry with `mqttd --backup`"
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// Small self-contained helpers: base64, sha-256 hex, UTC stamps.
// ---------------------------------------------------------------------------

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

#[allow(clippy::cast_possible_truncation)] // the 6/8-bit repacking IS the algorithm
/// Standard base64 with padding. Hand-rolled rather than pulled in: payloads are the one
/// thing an export must not mangle, and this is 20 lines with a round-trip test.
fn b64_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(B64[(n >> 18) as usize & 63] as char);
        out.push(B64[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            B64[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            B64[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

fn b64_value(c: u8) -> Option<u32> {
    match c {
        b'A'..=b'Z' => Some(u32::from(c - b'A')),
        b'a'..=b'z' => Some(u32::from(c - b'a') + 26),
        b'0'..=b'9' => Some(u32::from(c - b'0') + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

#[allow(clippy::cast_possible_truncation, clippy::naive_bytecount)]
fn b64_decode(s: &str) -> Result<Vec<u8>, String> {
    let raw: Vec<u8> = s.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    if !raw.len().is_multiple_of(4) {
        return Err(format!(
            "base64: length {} is not a multiple of 4",
            raw.len()
        ));
    }
    let mut out = Vec::with_capacity(raw.len() / 4 * 3);
    for chunk in raw.chunks(4) {
        let pad = chunk.iter().filter(|c| **c == b'=').count();
        let mut n = 0u32;
        for (i, c) in chunk.iter().enumerate() {
            let v = if *c == b'=' {
                0
            } else {
                b64_value(*c).ok_or_else(|| format!("base64: invalid byte {c:#04x}"))?
            };
            n |= v << (18 - 6 * i);
        }
        out.push((n >> 16) as u8);
        if pad < 2 {
            out.push((n >> 8) as u8);
        }
        if pad < 1 {
            out.push(n as u8);
        }
    }
    Ok(out)
}

/// SHA-256 of `bytes`, lower-case hex (aws-lc-rs — already compiled in via rustls).
fn sha256_hex(bytes: &[u8]) -> String {
    let d = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes);
    d.as_ref().iter().fold(String::new(), |mut s, b| {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
        s
    })
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// `YYYY-MM-DDTHH:MM:SSZ` — a real RFC 3339 instant, for the header's human-facing time.
///
/// Separate from [`utc_stamp`] on purpose: that one names FILES, where a colon is a hazard
/// (Windows/SMB targets, shell quoting), so it cannot simply grow colons. The two are the
/// same instant rendered for two audiences, and both are pinned by tests — a `…T…Z` string
/// that no RFC 3339 reader accepts is worse than no timestamp, because tooling trusts it.
/// [`rfc3339`] from a millisecond stamp — the export header's own unit.
fn rfc3339_from_unix_ms(unix_ms: u64) -> String {
    rfc3339(unix_ms / 1000)
}

fn rfc3339(unix_secs: u64) -> String {
    let days = i64::try_from(unix_secs / 86_400).unwrap_or(0);
    let secs_of_day = unix_secs % 86_400;
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60
    )
}

/// `YYYY-MM-DD_HHMMSS` in UTC — sortable and filename-safe. Hand-rolled because the broker
/// carries no date crate; the civil-from-days arithmetic is Howard Hinnant's, with a
/// round-trip test against known instants.
fn utc_stamp(unix_secs: u64) -> String {
    let days = i64::try_from(unix_secs / 86_400).unwrap_or(0);
    let secs_of_day = unix_secs % 86_400;
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}_{:02}{:02}{:02}",
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60
    )
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = u32::try_from(doy - (153 * mp + 2) / 5 + 1).unwrap_or(1);
    let m = u32::try_from(if mp < 10 { mp + 3 } else { mp - 9 }).unwrap_or(1);
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header_line(version: u32, binary: &str) -> String {
        serde_json::to_string(&header(version, binary)).unwrap()
    }

    fn header(version: u32, binary: &str) -> Header {
        Header {
            kind: "header".to_string(),
            format: FORMAT.to_string(),
            format_version: version,
            binary_version: binary.to_string(),
            created_at: "2026-08-15T00:00:00Z".to_string(),
            created_unix_ms: 1,
            node_id: "a".to_string(),
            cluster_id: Some("cid".to_string()),
            durable: true,
            store_schema: store_schema(),
            members: vec!["a".to_string()],
        }
    }

    /// A header for `node`, seeing `members`, taken at `created_unix_ms`, belonging to
    /// `cluster` — the four facts every cross-file rule in `load_with` reads.
    fn header_for(node: &str, members: &[&str], created_unix_ms: u64, cluster: &str) -> String {
        let mut h = header(FORMAT_VERSION, "0.9.0");
        h.node_id = node.to_string();
        h.members = members.iter().map(|m| (*m).to_string()).collect();
        h.created_unix_ms = created_unix_ms;
        h.created_at = rfc3339(created_unix_ms / 1000);
        h.cluster_id = Some(cluster.to_string());
        serde_json::to_string(&h).unwrap()
    }

    /// A retained record line: a value (or a clear, when `payload` is empty) with an
    /// optional convergence token.
    fn retained_line(topic: &str, payload: &[u8], token: Option<(u64, u64)>) -> String {
        let r = RetainedRecord {
            kind: "retained".to_string(),
            topic: topic.to_string(),
            payload_b64: b64_encode(payload),
            qos: 0,
            expires_at: None,
            props: PropsRecord::default(),
            token: token.map(|(epoch, offset)| RetainedToken { epoch, offset }),
            tombstone: payload.is_empty(),
        };
        serde_json::to_string(&r).unwrap()
    }

    /// A session record line carrying a queue and subscriptions — the two facts a
    /// mixed-generation restore gets WRONG (a stale queue, stale filters).
    fn session_line_full(
        client: &str,
        epoch: u64,
        high_offset: u64,
        filter: &str,
        queue: &[&str],
    ) -> String {
        let r = SessionRecord {
            kind: "session".to_string(),
            client: client.to_string(),
            owner: Some("anonymous".to_string()),
            subscriptions: vec![SubscriptionRecord {
                filter: filter.to_string(),
                max_qos: 1,
                no_local: false,
            }],
            session_expiry_at: None,
            last_packet_id: 0,
            received_qos2: Vec::new(),
            outbound_qos2: Vec::new(),
            token: Token { epoch, high_offset },
            queue: queue
                .iter()
                .enumerate()
                .map(|(i, p)| QueuedRecord {
                    offset: i as u64 + 1,
                    topic: "t/1".to_string(),
                    payload_b64: b64_encode(p.as_bytes()),
                    qos: 1,
                    expiry_at: None,
                    props: PropsRecord::default(),
                })
                .collect(),
        };
        serde_json::to_string(&r).unwrap()
    }

    /// Write one export file into `dir`, named the way the exporter names them.
    fn write_export(dir: &Path, name: &str, header: &str, records: &[String]) -> PathBuf {
        let path = dir.join(format!("{FORMAT}-{name}{SUFFIX}"));
        std::fs::write(&path, file_with(header, records, &[])).unwrap();
        path
    }

    /// Build a valid file from a header line and record lines, computing the trailer's
    /// digest the way the exporter does.
    fn file_with(header: &str, records: &[String], not_owned: &[&str]) -> String {
        let mut body = format!("{header}\n");
        for r in records {
            body.push_str(r);
            body.push('\n');
        }
        let trailer = Trailer {
            kind: "trailer".to_string(),
            complete: true,
            sessions: records
                .iter()
                .filter(|r| r.contains("\"kind\":\"session\""))
                .count() as u64,
            queued: 0,
            retained: 0,
            not_owned: not_owned.iter().map(|s| (*s).to_string()).collect(),
            started_unix_ms: 1,
            finished_unix_ms: 2,
            sha256: sha256_hex(body.as_bytes()),
        };
        format!("{body}{}\n", serde_json::to_string(&trailer).unwrap())
    }

    fn session_line(client: &str, epoch: u64, high_offset: u64) -> String {
        let r = SessionRecord {
            kind: "session".to_string(),
            client: client.to_string(),
            owner: Some("anonymous".to_string()),
            subscriptions: Vec::new(),
            session_expiry_at: None,
            last_packet_id: 0,
            received_qos2: Vec::new(),
            outbound_qos2: Vec::new(),
            token: Token { epoch, high_offset },
            queue: Vec::new(),
        };
        serde_json::to_string(&r).unwrap()
    }

    #[test]
    fn base64_round_trips_every_byte_and_length() {
        for len in 0..=32usize {
            let bytes: Vec<u8> = (0..len)
                .map(|i| u8::try_from(i * 7 % 256).unwrap())
                .collect();
            let encoded = b64_encode(&bytes);
            assert_eq!(b64_decode(&encoded).unwrap(), bytes, "len {len}");
        }
        let all: Vec<u8> = (0..=255u8).collect();
        assert_eq!(b64_decode(&b64_encode(&all)).unwrap(), all);
        assert!(b64_decode("!!!!").is_err());
        assert!(b64_decode("abc").is_err());
    }

    #[test]
    fn the_utc_stamp_is_sortable_and_correct() {
        assert_eq!(utc_stamp(0), "1970-01-01_000000");
        // A leap day, and the last second of a year — the two dates a hand-rolled civil
        // calendar gets wrong.
        assert_eq!(utc_stamp(1_709_164_800), "2024-02-29_000000");
        assert_eq!(utc_stamp(1_735_689_599), "2024-12-31_235959");
        assert_eq!(utc_stamp(1_770_000_000), "2026-02-02_024000");
        // Sortable: a later instant is lexicographically greater.
        assert!(utc_stamp(1_770_000_001) > utc_stamp(1_770_000_000));
    }

    /// ADR 0058 clause 3 applied to a NEW compatibility surface: a stamp this build does
    /// not read refuses loudly and imports nothing, and the message carries the one fact
    /// that is actionable — the build that wrote the file.
    #[test]
    fn a_newer_format_version_is_refused_naming_both_versions_and_the_writer_build() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.ndjson");

        std::fs::write(
            &path,
            file_with(&header_line(FORMAT_VERSION + 1, "9.9.9"), &[], &[]),
        )
        .unwrap();
        let err = load(&path).expect_err("a newer format version must refuse");
        assert!(
            err.contains(&format!("format_version {}", FORMAT_VERSION + 1)),
            "{err}"
        );
        assert!(
            err.contains("9.9.9"),
            "the writer build must be named: {err}"
        );
        assert!(err.contains("Nothing was imported"), "{err}");

        std::fs::write(&path, file_with(&header_line(0, "0.0.1"), &[], &[])).unwrap();
        let err = load(&path).expect_err("an older format version must refuse");
        assert!(
            err.contains("no migration path exists pre-1.0"),
            "the established wording: {err}"
        );

        // An unknown KIND refuses (a silently skipped record is data loss).
        std::fs::write(
            &path,
            file_with(
                &header_line(FORMAT_VERSION, "0.9.0"),
                &[r#"{"kind":"quantum-session","client":"c1"}"#.to_string()],
                &[],
            ),
        )
        .unwrap();
        let err = load(&path).expect_err("an unknown kind must refuse");
        assert!(err.contains("quantum-session"), "{err}");

        // An unknown FIELD inside a known kind is ignored, and the record still imports.
        let mut line = session_line("c1", 1, 1);
        line.truncate(line.len() - 1);
        line.push_str(r#","future_field":{"nested":[1,2,3]}}"#);
        std::fs::write(
            &path,
            file_with(&header_line(FORMAT_VERSION, "0.9.0"), &[line], &[]),
        )
        .unwrap();
        let plan = load(&path).expect("additive fields must be ignored, not fatal");
        assert_eq!(plan.sessions.len(), 1);
        assert_eq!(plan.sessions[0].client, "c1");
    }

    /// Two independent guards against a truncated export, because a backup an operator
    /// cannot trust is worse than no backup.
    #[test]
    fn an_export_without_a_valid_trailer_or_still_partial_is_never_imported() {
        let dir = tempfile::tempdir().unwrap();

        // No trailer at all (killed mid-record).
        let truncated = dir.path().join("mqttd-backup-a-t1.ndjson");
        std::fs::write(
            &truncated,
            format!(
                "{}\n{}\n",
                header_line(FORMAT_VERSION, "0.9.0"),
                session_line("c1", 1, 1)
            ),
        )
        .unwrap();
        let err = load(&truncated).expect_err("a file with no trailer must refuse");
        assert!(err.contains("trailer"), "{err}");

        // A trailer whose digest does not match the bytes.
        let altered = dir.path().join("mqttd-backup-a-t2.ndjson");
        let good = file_with(
            &header_line(FORMAT_VERSION, "0.9.0"),
            &[session_line("c1", 1, 1)],
            &[],
        );
        std::fs::write(&altered, good.replace("\"c1\"", "\"cX\"")).unwrap();
        let err = load(&altered).expect_err("an altered file must refuse");
        assert!(err.contains("sha256 mismatch"), "{err}");

        // A `.partial` file is invisible even as the only file in the directory.
        let only_partial = tempfile::tempdir().unwrap();
        std::fs::write(
            only_partial.path().join("mqttd-backup-a-t3.ndjson.partial"),
            &good,
        )
        .unwrap();
        let err = load(only_partial.path()).expect_err("a .partial file is not a backup");
        assert!(err.contains("no mqttd-backup"), "{err}");
    }

    /// The coverage check: a multi-node union is VERIFIED, not narrated.
    #[test]
    fn a_restore_missing_a_peers_export_is_refused_naming_the_client_ids() {
        let dir = tempfile::tempdir().unwrap();
        // Node a's export: it owns c1 and skipped b1 (owned by node b).
        let header_a = {
            let mut h: Header =
                serde_json::from_str(&header_line(FORMAT_VERSION, "0.9.0")).unwrap();
            h.node_id = "a".to_string();
            h.members = vec!["a".to_string(), "b".to_string()];
            serde_json::to_string(&h).unwrap()
        };
        let file_a = dir.path().join("mqttd-backup-a-1.ndjson");
        std::fs::write(
            &file_a,
            file_with(&header_a, &[session_line("c1", 1, 5)], &["b1"]),
        )
        .unwrap();

        // Node a alone: refused, naming node b AND (once b is present) nothing.
        let err = load(&file_a).expect_err("a one-node union of a two-node cluster must refuse");
        assert!(
            err.contains("\"b\""),
            "the missing node must be named: {err}"
        );

        // Node b's export: it owns b1 and skipped c1.
        let header_b = {
            let mut h: Header =
                serde_json::from_str(&header_line(FORMAT_VERSION, "0.9.0")).unwrap();
            h.node_id = "b".to_string();
            h.members = vec!["a".to_string(), "b".to_string()];
            serde_json::to_string(&h).unwrap()
        };
        std::fs::write(
            dir.path().join("mqttd-backup-b-1.ndjson"),
            file_with(&header_b, &[session_line("b1", 1, 2)], &["c1"]),
        )
        .unwrap();
        let plan = load(dir.path()).expect("both nodes' exports cover the cluster");
        let clients: Vec<&str> = plan.sessions.iter().map(|s| s.client.as_str()).collect();
        assert_eq!(clients, vec!["b1", "c1"]);

        // A not_owned id present in NO file is named.
        let orphan = tempfile::tempdir().unwrap();
        let header_solo = {
            let mut h: Header =
                serde_json::from_str(&header_line(FORMAT_VERSION, "0.9.0")).unwrap();
            h.node_id = "a".to_string();
            h.members = vec!["a".to_string()];
            serde_json::to_string(&h).unwrap()
        };
        std::fs::write(
            orphan.path().join("mqttd-backup-a-1.ndjson"),
            file_with(&header_solo, &[session_line("c1", 1, 5)], &["ghost"]),
        )
        .unwrap();
        let err = load(orphan.path()).expect_err("an uncovered client id must refuse");
        assert!(err.contains("ghost"), "{err}");
    }

    /// A client id in two exports (it migrated during the backup) resolves by the higher
    /// `(epoch, high_offset)` token — and file order cannot change the answer.
    #[test]
    fn a_duplicate_client_resolves_to_the_highest_token_whatever_the_file_order() {
        for (first, second) in [((1u64, 9u64), (2u64, 1u64)), ((2, 1), (1, 9))] {
            let dir = tempfile::tempdir().unwrap();
            for (i, (epoch, high)) in [first, second].into_iter().enumerate() {
                let mut h: Header =
                    serde_json::from_str(&header_line(FORMAT_VERSION, "0.9.0")).unwrap();
                h.node_id = format!("n{i}");
                h.members = vec!["n0".to_string(), "n1".to_string()];
                std::fs::write(
                    dir.path().join(format!("mqttd-backup-n{i}-1.ndjson")),
                    file_with(
                        &serde_json::to_string(&h).unwrap(),
                        &[session_line("c1", epoch, high)],
                        &[],
                    ),
                )
                .unwrap();
            }
            let plan = load(dir.path()).unwrap();
            assert_eq!(plan.sessions.len(), 1);
            assert_eq!(
                plan.sessions[0].token,
                Token {
                    epoch: 2,
                    high_offset: 1
                },
                "the higher lease epoch wins regardless of offsets or file order"
            );
        }
    }

    /// The pre-open freshness precondition, which is what "a FRESH cluster" means
    /// mechanically. It names the files it found, because at 03:00 that is the difference
    /// between a two-minute fix and an hour.
    #[test]
    fn a_restore_into_a_data_dir_that_already_holds_state_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        require_fresh_data_dir(dir.path()).expect("an empty dir is fresh");
        // The node-id stamp alone does not make it non-fresh: guard_data_dir writes it on
        // first use, and a restore is a first use.
        std::fs::write(dir.path().join("node-id"), "a").unwrap();
        require_fresh_data_dir(dir.path()).expect("the node-id stamp is not store state");

        std::fs::write(dir.path().join("sessions.redb"), b"x").unwrap();
        let err = require_fresh_data_dir(dir.path()).expect_err("existing state must refuse");
        assert!(err.contains("sessions.redb"), "{err}");
        assert!(err.contains("never merges"), "{err}");
    }

    /// The evidence the whole placement decision rests on: a SECOND `redb` open of a store
    /// this process already holds is refused by the OS (`flock(LOCK_EX | LOCK_NB)` →
    /// `DatabaseAlreadyOpen`), while the shipped exporter — which borrows the live handle —
    /// produces a valid file over the very same data dir.
    ///
    /// Together those two facts rule out every other design: no `scripts/` script can read a
    /// running node's stores, no separate-process subcommand can either, and the exporter
    /// must therefore live inside the broker and hold no handle of its own (ADR 0061 /
    /// issue #242 — a handle outliving the work keeps the dir locked and the next start
    /// fails with "Database already open").
    #[tokio::test]
    async fn the_exporter_borrows_the_running_handle_and_never_opens_its_own() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.redb");
        // The "running broker": one live handle on the store.
        let live = mqtt_storage::persistent_log::PersistentLog::open(&path)
            .expect("the first handle opens");
        let sessions: Arc<dyn SessionStore> =
            Arc::new(mqtt_storage::logged::ReplicatedSessionStore::new(live));
        sessions
            .claim_session(&ClientId("c1".to_string()), "alice")
            .await
            .unwrap();

        // A second opener — a script, or `mqttd --export` as a new process — cannot read it.
        let second = mqtt_storage::persistent_log::PersistentLog::open(&path);
        let err = second
            .expect_err("redb must refuse a second handle on an open store")
            .to_string();
        assert!(
            err.to_lowercase().contains("already open"),
            "expected redb's DatabaseAlreadyOpen, got: {err}"
        );

        // The shipped exporter, borrowing the live handle, works over the same dir.
        let out = tempfile::tempdir().unwrap();
        let report = export(&ctx(out.path()), &sessions, &memory_retained_source())
            .await
            .expect("the borrowed handle exports while the store is locked by this process");
        assert_eq!(report.sessions, 1);
        load(out.path()).expect("and the file it wrote is valid");
    }

    /// A store double whose scan reports a TRANSIENT gap — the `NoQuorum`/`Unavailable`
    /// case `export_sessions` distinguishes from a clean `NotOwner` skip.
    #[derive(Debug)]
    struct IncompleteStore;

    #[async_trait::async_trait]
    impl SessionStore for IncompleteStore {
        async fn ensure_session(&self, _c: &ClientId) -> Result<bool, StorageError> {
            Ok(false)
        }
        async fn set_subscriptions(
            &self,
            _c: &ClientId,
            _s: &[Subscription],
        ) -> Result<(), StorageError> {
            Ok(())
        }
        async fn subscriptions(&self, _c: &ClientId) -> Result<Vec<Subscription>, StorageError> {
            Ok(Vec::new())
        }
        async fn enqueue_with_expiry(
            &self,
            _c: &ClientId,
            _m: &Message,
            _e: Option<u64>,
        ) -> Result<mqtt_storage::Enqueued, StorageError> {
            Ok(mqtt_storage::Enqueued::Rejected)
        }
        async fn pending(
            &self,
            _c: &ClientId,
            _a: u64,
            _l: usize,
        ) -> Result<Vec<mqtt_storage::QueuedMessage>, StorageError> {
            Ok(Vec::new())
        }
        async fn ack(&self, _c: &ClientId, _u: u64) -> Result<(), StorageError> {
            Ok(())
        }
        async fn record_received(
            &self,
            _c: &ClientId,
            _p: u16,
        ) -> Result<mqtt_storage::InboundSighting, StorageError> {
            Ok(mqtt_storage::InboundSighting::Fresh)
        }
        async fn ack_received(&self, _c: &ClientId, _p: u16) -> Result<(), StorageError> {
            Ok(())
        }
        async fn clear_received(&self, _c: &ClientId, _p: u16) -> Result<(), StorageError> {
            Ok(())
        }
        async fn received(&self, _c: &ClientId) -> Result<Vec<u16>, StorageError> {
            Ok(Vec::new())
        }
        async fn record_outbound(
            &self,
            _c: &ClientId,
            _p: u16,
            _o: u64,
        ) -> Result<(), StorageError> {
            Ok(())
        }
        async fn advance_outbound(&self, _c: &ClientId, _p: u16) -> Result<(), StorageError> {
            Ok(())
        }
        async fn clear_outbound(&self, _c: &ClientId, _p: u16) -> Result<(), StorageError> {
            Ok(())
        }
        async fn outbound(
            &self,
            _c: &ClientId,
        ) -> Result<Vec<mqtt_storage::OutboundInflight>, StorageError> {
            Ok(Vec::new())
        }
        async fn next_packet_id(&self, _c: &ClientId) -> Result<u16, StorageError> {
            Ok(1)
        }
        async fn remove(&self, _c: &ClientId) -> Result<(), StorageError> {
            Ok(())
        }
        async fn export_sessions(&self) -> Result<mqtt_storage::SessionExportScan, StorageError> {
            Ok(mqtt_storage::SessionExportScan {
                sessions: Vec::new(),
                not_owned: Vec::new(),
                complete: false,
            })
        }
    }

    /// An empty retained source (durable-off shape: values only, no tokens).
    fn memory_retained_source() -> Arc<dyn RetainedSource> {
        Arc::new(StoreRetainedSource(Arc::new(
            mqtt_storage::MemoryRetainedStore::new(),
        )))
    }

    fn ctx(dir: &Path) -> ExportContext {
        ExportContext {
            dir: dir.to_path_buf(),
            keep: 7,
            node_id: "node-a".to_string(),
            cluster_id: Some("cid".to_string()),
            durable: true,
            members: vec!["node-a".to_string()],
        }
    }

    /// A round trip through the real exporter: what it writes is what `load` accepts, and
    /// the payload bytes and application properties survive verbatim.
    #[tokio::test]
    async fn an_export_of_a_live_store_round_trips_through_the_importer() {
        let dir = tempfile::tempdir().unwrap();
        let sessions: Arc<dyn SessionStore> =
            Arc::new(mqtt_storage::logged::ReplicatedSessionStore::new(
                mqtt_storage::repl::InMemoryReplicatedLog::new(),
            ));
        let client = ClientId("c1".to_string());
        assert!(matches!(
            sessions.claim_session(&client, "alice").await.unwrap(),
            SessionClaim::Granted { present: false }
        ));
        let mut message = Message::new(
            "t/1".to_string(),
            bytes::Bytes::from_static(&[0u8, 1, 2, 255, 254]),
            QoS::AtLeastOnce,
            false,
        );
        message.app.user_properties = vec![("k".to_string(), "v".to_string())];
        message.app.correlation_data = Some(bytes::Bytes::from_static(&[9u8, 8, 7]));
        sessions.enqueue(&client, &message).await.unwrap();
        sessions.record_received(&client, 7).await.unwrap();
        sessions.ack_received(&client, 7).await.unwrap();
        sessions.record_received(&client, 8).await.unwrap();
        let retained: Arc<dyn RetainedStore> = Arc::new(mqtt_storage::MemoryRetainedStore::new());
        retained
            .set(&Message::new(
                "r/1".to_string(),
                bytes::Bytes::from_static(b"retained"),
                QoS::AtMostOnce,
                true,
            ))
            .await
            .unwrap();

        let source: Arc<dyn RetainedSource> = Arc::new(StoreRetainedSource(retained.clone()));
        let report = export(&ctx(dir.path()), &sessions, &source).await.unwrap();
        assert_eq!((report.sessions, report.queued, report.retained), (1, 1, 1));
        assert!(report.path.to_string_lossy().contains("node-a"));

        let plan = load(dir.path()).expect("the exporter's own output must import");
        assert_eq!(plan.sessions.len(), 1);
        let s = &plan.sessions[0];
        assert_eq!(s.owner.as_deref(), Some("alice"));
        assert_eq!(s.queue[0].topic, "t/1");
        assert_eq!(
            b64_decode(&s.queue[0].payload_b64).unwrap(),
            vec![0u8, 1, 2, 255, 254],
            "payload bytes survive base64 verbatim"
        );
        assert_eq!(
            s.queue[0].props.user_properties,
            vec![("k".to_string(), "v".to_string())]
        );
        let mut window: Vec<(u16, bool)> = s
            .received_qos2
            .iter()
            .map(|r| (r.packet_id, r.acked))
            .collect();
        window.sort_unstable();
        assert_eq!(
            window,
            vec![(7, true), (8, false)],
            "held-ACKED and held-UNACKED are different facts (issue #238) and both survive"
        );
        assert_eq!(plan.retained.len(), 1);
        assert_eq!(plan.retained[0].topic, "r/1");
    }

    /// A retained source that cannot answer — the hub too busy, gone, or its store erroring.
    #[derive(Debug)]
    struct FailingRetainedSource;

    #[async_trait::async_trait]
    impl RetainedSource for FailingRetainedSource {
        async fn snapshot(&self) -> Result<Vec<RetainedSnapshotEntry>, String> {
            Err("backup: the retained store could not be read: no quorum".to_string())
        }
    }

    /// A retained snapshot that FAILS fails the whole run, exactly like an incomplete session
    /// scan. The alternative — treating "cannot read" as "there is nothing" — would write a
    /// perfectly valid-looking export with every retained topic missing, and a restore from it
    /// would wipe the deployment's desired state without a single error anywhere.
    #[tokio::test]
    async fn a_retained_snapshot_that_fails_fails_the_run_and_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let sessions: Arc<dyn SessionStore> =
            Arc::new(mqtt_storage::logged::ReplicatedSessionStore::new(
                mqtt_storage::repl::InMemoryReplicatedLog::new(),
            ));
        let source: Arc<dyn RetainedSource> = Arc::new(FailingRetainedSource);
        let err = export(&ctx(dir.path()), &sessions, &source)
            .await
            .expect_err("an unreadable retained set must fail the export");
        assert!(err.contains("retained store could not be read"), "{err}");
        let files: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            files.is_empty(),
            "nothing may be written when the retained set could not be read; found {files:?}"
        );
    }

    /// An incomplete scan fails the run: nothing is renamed into place, the error is
    /// counted, and the last-success timestamp does NOT advance — so the RPO alert fires
    /// instead of an operator trusting a file that is missing sessions.
    #[tokio::test]
    async fn an_incomplete_session_scan_fails_the_run_and_advances_no_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let task = BackupTask {
            ctx: ctx(dir.path()),
            every_secs: 0,
            sessions: Arc::new(IncompleteStore),
            retained: memory_retained_source(),
            status: Arc::new(BackupStatus::default()),
            metrics: Some(Arc::new(Metrics::new("test"))),
            members: Arc::new(Vec::new),
            cluster_id: Arc::new(|| Some("cid".to_string())),
        };
        task.run_once("test").await;

        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            files.is_empty(),
            "an incomplete scan must write NOTHING; found {files:?}"
        );
        let rendered = task.metrics.as_ref().unwrap().render();
        assert!(
            rendered.contains("mqttd_backup_runs_total{outcome=\"error\"} 1"),
            "the failed run must be counted: {rendered}"
        );
        assert!(
            rendered.contains("mqttd_backup_last_success_timestamp_seconds 0"),
            "a failed run must NOT advance the RPO series: {rendered}"
        );
        // Every series OPERATIONS cites must exist under exactly this rendered name — three
        // invented metric references were caught in this campaign.
        for series in [
            "mqttd_backup_duration_ms",
            "mqttd_backup_last_success_timestamp_seconds",
            "mqttd_restore_state",
            "mqttd_backup_runs_total",
        ] {
            assert!(
                rendered.contains(series),
                "{series} is not exported: {rendered}"
            );
        }
        let status = task.status.statusz_fragment().expect("statusz reports it");
        assert!(status.contains("INCOMPLETE"), "{status}");
    }

    /// **Rule 1 — one generation per node, the newest.** The default retention keeps
    /// `keep = 7` exports per node in the very directory an operator copies off the volume
    /// and points `MQTTD_RESTORE_FROM` at, so a set with several generations is the ORDINARY
    /// case, not an abuse. The old rule merged them record-by-record and resolved a duplicate
    /// client by `(epoch, high_offset)` — which is not recency, and `high_offset` is 0 for a
    /// fully-drained queue, so the OLDER generation won exactly when the newer one had
    /// nothing left to deliver: stale subscriptions restored, acked messages redelivered.
    ///
    /// The assertions are equalities over the whole surviving record, because "a session was
    /// restored" is true in both the right and the wrong outcome.
    #[test]
    fn a_restore_reads_the_newest_generation_of_each_node_and_never_merges_two() {
        let dir = tempfile::tempdir().unwrap();
        // Generation 1: a queue of two, subscribed to `old/#`, retained value OLD.
        write_export(
            dir.path(),
            "a-2026-08-15_030000-000",
            &header_for("a", &["a"], 1_000, "cid"),
            &[
                session_line_full("psub", 245, 7, "old/#", &["g1-1", "g1-2"]),
                retained_line("cfg/x", b"OLD", Some((1, 1))),
            ],
        );
        // Generation 2, later: the client drained and ACKED everything (`high_offset` 0),
        // resubscribed to `new/#`, and the retained value was overwritten.
        write_export(
            dir.path(),
            "a-2026-08-15_040000-000",
            &header_for("a", &["a"], 2_000, "cid"),
            &[
                session_line_full("psub", 245, 0, "new/#", &[]),
                retained_line("cfg/x", b"NEW", Some((1, 2))),
            ],
        );

        let plan = load(dir.path()).expect("several generations of one node is not an error");
        assert_eq!(plan.files.len(), 1, "exactly one generation is read");
        assert!(
            plan.files[0].to_string_lossy().contains("040000"),
            "the NEWEST generation must be the one read, got {:?}",
            plan.files
        );
        assert_eq!(
            plan.superseded.len(),
            1,
            "the older one is named, not merged"
        );
        assert_eq!(plan.sessions.len(), 1);
        let s = &plan.sessions[0];
        assert_eq!(
            (
                s.client.as_str(),
                s.queue.len(),
                s.subscriptions
                    .iter()
                    .map(|f| f.filter.as_str())
                    .collect::<Vec<_>>(),
                s.token
            ),
            (
                "psub",
                0,
                vec!["new/#"],
                Token {
                    epoch: 245,
                    high_offset: 0
                }
            ),
            "the restored session must equal the NEWEST export's record exactly — an empty \
             queue and the CURRENT subscriptions, never the older generation's"
        );
        assert_eq!(plan.retained.len(), 1);
        assert_eq!(
            b64_decode(&plan.retained[0].payload_b64).unwrap(),
            b"NEW".to_vec(),
            "and the retained value comes from the same generation, not a mix of two moments"
        );

        // Two files of one node at the SAME instant: recency is undecidable, so it refuses
        // and names both rather than guessing.
        let tie = tempfile::tempdir().unwrap();
        for name in ["a-1", "a-2"] {
            write_export(
                tie.path(),
                name,
                &header_for("a", &["a"], 5_000, "cid"),
                &[session_line("psub", 1, 1)],
            );
        }
        let err = load(tie.path()).expect_err("a tie on created_unix_ms must refuse");
        assert!(err.contains("SAME created_unix_ms"), "{err}");
        assert!(
            err.contains("a-1") && err.contains("a-2"),
            "both colliding files must be named: {err}"
        );
    }

    /// **Rule 2 — retained recency is the `(epoch, offset)` convergence token, and file
    /// order never decides.** The old rule was `BTreeMap::insert` in file-iteration order
    /// over a lexicographic sort, i.e. the highest-sorting NODE ID won: a restore could roll
    /// a retained topic back to an earlier value while the newer value sat in the same set,
    /// decided by an accident of node naming. Every branch of the replacement is pinned
    /// here, including the one that resurrected deleted values.
    #[test]
    fn retained_recency_is_the_convergence_token_then_file_time_never_the_node_name() {
        // (a) The token decides even against the newer FILE: node `b`'s export is later but
        // its cache was behind, and the token says so.
        let dir = tempfile::tempdir().unwrap();
        write_export(
            dir.path(),
            "a-1",
            &header_for("a", &["a", "b"], 1_000, "cid"),
            &[retained_line("cfg/x", b"NEW", Some((7, 9)))],
        );
        write_export(
            dir.path(),
            "b-1",
            &header_for("b", &["a", "b"], 9_000, "cid"),
            &[retained_line("cfg/x", b"OLD", Some((7, 8)))],
        );
        let plan = load(dir.path()).unwrap();
        assert_eq!(
            payloads(&plan.retained),
            vec![("cfg/x".to_string(), b"NEW".to_vec())],
            "the higher (epoch, offset) is the later committed write, whatever the file times"
        );

        // (b) Durable retained OFF (no tokens anywhere): the newest EXPORT wins — the case
        // the verifier proved, where node `b` held the old value and merely sorted later.
        let untokened = tempfile::tempdir().unwrap();
        write_export(
            untokened.path(),
            "a-1",
            &header_for("a", &["a", "b"], 9_000, "cid"),
            &[retained_line("cfg/x", b"NEW", None)],
        );
        write_export(
            untokened.path(),
            "b-1",
            &header_for("b", &["a", "b"], 1_000, "cid"),
            &[retained_line("cfg/x", b"OLD", None)],
        );
        assert_eq!(
            payloads(&load(untokened.path()).unwrap().retained),
            vec![("cfg/x".to_string(), b"NEW".to_vec())],
            "with no token to appeal to, the newer export wins — never the higher node id"
        );

        // (c) A tokened record beats an untokened one: a value reaches the token map when it
        // is applied, so a cached value with no token predates that node's restart.
        let mixed = tempfile::tempdir().unwrap();
        write_export(
            mixed.path(),
            "a-1",
            &header_for("a", &["a", "b"], 1_000, "cid"),
            &[retained_line("cfg/x", b"COMMITTED", Some((1, 1)))],
        );
        write_export(
            mixed.path(),
            "b-1",
            &header_for("b", &["a", "b"], 9_000, "cid"),
            &[retained_line("cfg/x", b"UNATTRIBUTED", None)],
        );
        assert_eq!(
            payloads(&load(mixed.path()).unwrap().retained),
            vec![("cfg/x".to_string(), b"COMMITTED".to_vec())]
        );

        // (d) A CLEAR is versioned like a value: a topic deleted after an older node's
        // export stays deleted instead of being resurrected by the union.
        let cleared = tempfile::tempdir().unwrap();
        write_export(
            cleared.path(),
            "a-1",
            &header_for("a", &["a", "b"], 1_000, "cid"),
            &[retained_line("cfg/x", b"", Some((7, 9)))],
        );
        write_export(
            cleared.path(),
            "b-1",
            &header_for("b", &["a", "b"], 9_000, "cid"),
            &[retained_line("cfg/x", b"RESURRECTED", Some((7, 8)))],
        );
        let plan = load(cleared.path()).unwrap();
        assert!(
            plan.retained.is_empty(),
            "a topic whose newest record is a clear must NOT be restored: {:?}",
            payloads(&plan.retained)
        );
        assert_eq!(plan.cleared_topics, vec!["cfg/x".to_string()]);

        // (e) …and the reverse token order restores the value, so (d) is the rule and not an
        // accident of which record is a tombstone.
        let revalued = tempfile::tempdir().unwrap();
        write_export(
            revalued.path(),
            "a-1",
            &header_for("a", &["a", "b"], 1_000, "cid"),
            &[retained_line("cfg/x", b"", Some((7, 8)))],
        );
        write_export(
            revalued.path(),
            "b-1",
            &header_for("b", &["a", "b"], 9_000, "cid"),
            &[retained_line("cfg/x", b"CURRENT", Some((7, 9)))],
        );
        assert_eq!(
            payloads(&load(revalued.path()).unwrap().retained),
            vec![("cfg/x".to_string(), b"CURRENT".to_vec())]
        );
    }

    /// `(topic, payload)` pairs of a plan's retained set — the shape an equality reads on.
    fn payloads(records: &[RetainedRecord]) -> Vec<(String, Vec<u8>)> {
        let mut out: Vec<(String, Vec<u8>)> = records
            .iter()
            .map(|r| (r.topic.clone(), b64_decode(&r.payload_b64).unwrap()))
            .collect();
        out.sort();
        out
    }

    /// Two clusters' exports in one directory are REFUSED, naming both ids.
    ///
    /// The realistic form is two environments whose node ids are identical (`mqttd-0/1/2`
    /// from two `StatefulSets` — staging beside prod), or a stale bundle left behind. Their
    /// `members` lists then match the node ids perfectly, so the coverage check passes and
    /// one cluster's sessions and retained payloads quietly replace the other's. The
    /// `cluster_id` in every header is the fact that can see it.
    #[test]
    fn exports_from_two_different_clusters_are_refused_naming_both_ids() {
        let dir = tempfile::tempdir().unwrap();
        write_export(
            dir.path(),
            "mqttd-0-1",
            &header_for("mqttd-0", &["mqttd-0", "mqttd-1"], 1_000, "CLUSTER-PROD"),
            &[session_line("c-prod", 1, 1)],
        );
        write_export(
            dir.path(),
            "mqttd-1-1",
            &header_for("mqttd-1", &["mqttd-0", "mqttd-1"], 1_100, "CLUSTER-STAGING"),
            &[session_line("c-staging", 1, 1)],
        );
        let err = load(dir.path()).expect_err("two clusters in one set must refuse");
        assert!(err.contains("CLUSTER-PROD"), "{err}");
        assert!(err.contains("CLUSTER-STAGING"), "{err}");
        assert!(err.contains("2 DIFFERENT clusters"), "{err}");
        assert!(err.contains("Nothing was imported"), "{err}");

        // THE CASE THAT ESCAPED: the two clusters share a NODE ID. Above, `mqttd-0` and
        // `mqttd-1` are distinct, so both files survive generation selection and the refusal
        // sees both cluster ids — which is exactly why the original test passed while the
        // real slip did not refuse. When both exports name the SAME node (one StatefulSet
        // name reused across environments, or a rebuilt cluster), selection keys on node id,
        // treats the other cluster's export as an older GENERATION, and discards it in
        // silence — so the refusal never ran and the wrong cluster's data was imported.
        // Proven on two real unrelated clusters in review; the check now runs over every
        // file found, before selection.
        let same_node = tempfile::tempdir().unwrap();
        write_export(
            same_node.path(),
            "n1-older",
            &header_for("n1", &["n1"], 1_000, "CLUSTER-STAGING"),
            &[session_line("c-staging", 1, 1)],
        );
        write_export(
            same_node.path(),
            "n1-newer",
            &header_for("n1", &["n1"], 2_000, "CLUSTER-PROD"),
            &[session_line("c-prod", 1, 1)],
        );
        let err = load(same_node.path())
            .expect_err("two clusters sharing a node id must refuse, not silently pick one");
        assert!(err.contains("CLUSTER-PROD"), "{err}");
        assert!(err.contains("CLUSTER-STAGING"), "{err}");
        assert!(
            err.contains("2 DIFFERENT clusters"),
            "the refusal must name the mix rather than report a generation choice: {err}"
        );

        // One cluster's set loads, so the refusal is about the mix and not about the shape.
        let single = tempfile::tempdir().unwrap();
        write_export(
            single.path(),
            "mqttd-0-1",
            &header_for("mqttd-0", &["mqttd-0"], 1_000, "CLUSTER-PROD"),
            &[session_line("c-prod", 1, 1)],
        );
        let plan = load(single.path()).expect("one cluster's set is fine");
        assert_eq!(plan.sessions.len(), 1);
    }

    /// A node that was restored successfully must be able to RESTART with its own unchanged
    /// environment — and a request to restore a DIFFERENT set into it must still refuse.
    ///
    /// The setting lives in a pod spec or a unit file and does not disappear when the restore
    /// finishes, so the node's own next boot meets it again. Refusing there (because the data
    /// dir now holds stores) made every ordinary reschedule of a recovered cluster a
    /// `CrashLoopBackOff` whose printed remedy — "delete the volume's contents" — destroys the
    /// data just restored.
    #[test]
    fn a_restored_node_restarts_inertly_and_a_different_set_is_still_refused() {
        let dir = tempfile::tempdir().unwrap();
        let plan = RestorePlan {
            files: vec![dir.path().join("mqttd-backup-a-1.ndjson")],
            set_sha256: "deadbeef".to_string(),
            ..RestorePlan::default()
        };
        // Fresh: proceed.
        assert!(matches!(
            restore_disposition(dir.path(), "/restore").unwrap(),
            RestoreDisposition::Proceed
        ));

        // The restore ran: stores exist, and the stamp explains them.
        std::fs::write(dir.path().join("sessions.redb"), b"x").unwrap();
        write_restored_stamp(dir.path(), "/restore", &plan, &RestoreReport::default()).unwrap();

        // The ordinary restart: same environment, same source — INERT, not a refusal.
        match restore_disposition(dir.path(), "/restore").unwrap() {
            RestoreDisposition::AlreadyRestored(stamp) => {
                assert_eq!(stamp.restored_from, "/restore");
                assert_eq!(stamp.set_sha256, "deadbeef");
                assert!(
                    stamp.restored_at.contains('T') && stamp.restored_at.ends_with('Z'),
                    "the stamp records when: {}",
                    stamp.restored_at
                );
            }
            other => panic!("a restored node must restart inertly, got {other:?}"),
        }

        // A DIFFERENT source into the same node is a merge by another name: refused, naming
        // both, with a way forward that is not "delete the data you just restored".
        let err = restore_disposition(dir.path(), "/other-backup")
            .expect_err("restoring a second set into a restored node must refuse");
        assert!(
            err.contains("/restore") && err.contains("/other-backup"),
            "{err}"
        );
        assert!(err.contains("never merges"), "{err}");
        assert!(err.contains("fresh volume"), "{err}");

        // State with NO stamp is still refused — that is an operator pointing a restore at a
        // live node's volume, and nothing explains the files.
        let stateful = tempfile::tempdir().unwrap();
        std::fs::write(stateful.path().join("sessions.redb"), b"x").unwrap();
        let err = restore_disposition(stateful.path(), "/restore")
            .expect_err("existing state with no stamp must refuse");
        assert!(err.contains("sessions.redb"), "{err}");

        // An unreadable stamp is still evidence a restore completed here: inert, and loud.
        let odd = tempfile::tempdir().unwrap();
        std::fs::write(odd.path().join(RESTORED_STAMP), "hand-edited\n").unwrap();
        assert!(matches!(
            restore_disposition(odd.path(), "/restore").unwrap(),
            RestoreDisposition::AlreadyRestoredUnreadable(_)
        ));
    }

    /// A set missing a node's export refuses by default — and names the opt-in that accepts
    /// the loss knowingly, because "there is no override" describes no action an operator can
    /// take when the disaster took a node's data AND its export together.
    #[test]
    fn a_partial_restore_needs_the_explicit_opt_in_and_names_what_it_forfeits() {
        let dir = tempfile::tempdir().unwrap();
        // n1 and n2 exported; n3's data and export are both gone. n1 skipped `lost-1`
        // because n3 owned it.
        std::fs::write(
            dir.path().join(format!("{FORMAT}-n1-1{SUFFIX}")),
            file_with(
                &header_for("n1", &["n1", "n2", "n3"], 1_000, "cid"),
                &[session_line("kept-1", 3, 4)],
                &["lost-1"],
            ),
        )
        .unwrap();
        std::fs::write(
            dir.path().join(format!("{FORMAT}-n2-1{SUFFIX}")),
            file_with(
                &header_for("n2", &["n1", "n2", "n3"], 1_100, "cid"),
                &[session_line("kept-2", 3, 5)],
                &[],
            ),
        )
        .unwrap();

        let err = load(dir.path()).expect_err("an incomplete set refuses by default");
        assert!(err.contains("\"n3\""), "the missing node is named: {err}");
        assert!(
            err.contains("restore_partial_accept_data_loss"),
            "the refusal must name the way forward: {err}"
        );
        assert!(
            !err.contains("there is no override"),
            "the old text described no action an operator could take: {err}"
        );

        let plan = load_with(dir.path(), Coverage::PartialAcceptDataLoss)
            .expect("the opt-in imports what survived");
        assert!(plan.is_partial());
        assert_eq!(plan.forfeited_nodes, vec!["n3".to_string()]);
        assert_eq!(plan.forfeited_clients, vec!["lost-1".to_string()]);
        let mut kept: Vec<&str> = plan.sessions.iter().map(|s| s.client.as_str()).collect();
        kept.sort_unstable();
        assert_eq!(
            kept,
            vec!["kept-1", "kept-2"],
            "the surviving nodes' own sessions are restored — the whole point of the opt-in"
        );

        // And what was forfeited is recorded on disk, permanently, not only in a log line.
        let data = tempfile::tempdir().unwrap();
        write_restored_stamp(data.path(), "/restore", &plan, &RestoreReport::default()).unwrap();
        let stamp: RestoreStamp = serde_json::from_str(
            &std::fs::read_to_string(data.path().join(RESTORED_STAMP)).unwrap(),
        )
        .unwrap();
        assert!(stamp.partial);
        assert_eq!(stamp.forfeited_nodes, vec!["n3".to_string()]);
        assert_eq!(stamp.forfeited_clients, vec!["lost-1".to_string()]);
    }

    /// The header's `created_at` is a real RFC 3339 instant, and it agrees with
    /// `created_unix_ms`.
    ///
    /// It read `2026-08-15T055527Z` — a `…T…Z` string with no colons, which every RFC 3339
    /// reader rejects while looking exactly like one an operator or a tool may parse. On a
    /// NEW 1.0 compatibility surface that shape is the thing a v2 would have to keep.
    #[tokio::test]
    async fn the_headers_created_at_is_a_parseable_rfc_3339_instant() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339(1_755_237_327), "2025-08-15T05:55:27Z");
        assert_eq!(rfc3339(1_709_164_800), "2024-02-29T00:00:00Z");

        let dir = tempfile::tempdir().unwrap();
        let sessions: Arc<dyn SessionStore> =
            Arc::new(mqtt_storage::logged::ReplicatedSessionStore::new(
                mqtt_storage::repl::InMemoryReplicatedLog::new(),
            ));
        export(&ctx(dir.path()), &sessions, &memory_retained_source())
            .await
            .unwrap();
        let file = std::fs::read_to_string(&exports_of(dir.path(), "node-a")[0]).unwrap();
        let header: Header = serde_json::from_str(file.lines().next().unwrap()).unwrap();
        let at = header.created_at;
        assert_eq!(at.len(), 20, "YYYY-MM-DDTHH:MM:SSZ is 20 characters: {at}");
        assert_eq!(
            at.matches(':').count(),
            2,
            "the time needs its COLONS — this is the whole defect: {at}"
        );
        assert_eq!(
            at,
            rfc3339(header.created_unix_ms / 1000),
            "the human-readable instant must be the machine-readable one"
        );
        let (date, time) = at[..at.len() - 1].split_once('T').expect("a T separator");
        let ymd: Vec<&str> = date.split('-').collect();
        let hms: Vec<&str> = time.split(':').collect();
        assert_eq!((ymd.len(), hms.len()), (3, 3), "{at}");
        for part in ymd.iter().chain(hms.iter()) {
            assert!(
                part.chars().all(|c| c.is_ascii_digit()),
                "every component is numeric: {at}"
            );
        }
    }

    /// The `cluster_id` in the header is read FRESH for every export.
    ///
    /// A joiner adopts the cluster identity over gossip AFTER its process starts, so a value
    /// snapshotted once at task construction is `None` on every node that has not restarted
    /// since first boot — two files in three on a freshly deployed cluster, carrying no
    /// provenance at all, and nothing for the cross-cluster refusal to compare.
    #[tokio::test]
    async fn the_cluster_id_is_read_fresh_for_every_export_not_snapshotted_at_startup() {
        let dir = tempfile::tempdir().unwrap();
        let adopted = Arc::new(Mutex::new(None::<String>));
        let task = BackupTask {
            ctx: ctx(dir.path()),
            every_secs: 0,
            sessions: Arc::new(mqtt_storage::logged::ReplicatedSessionStore::new(
                mqtt_storage::repl::InMemoryReplicatedLog::new(),
            )),
            retained: memory_retained_source(),
            status: Arc::new(BackupStatus::default()),
            metrics: None,
            members: Arc::new(Vec::new),
            cluster_id: {
                let adopted = adopted.clone();
                Arc::new(move || {
                    adopted
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .clone()
                })
            },
        };
        // Before gossip: the identity is genuinely unknown, and the file says so.
        task.run_once("before").await;
        // The identity arrives over gossip, long after the task was built.
        *adopted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some("cid-from-gossip".into());
        task.run_once("after").await;

        let files = exports_of(dir.path(), "node-a");
        assert_eq!(files.len(), 2, "two runs, two files: {files:?}");
        let ids: Vec<Option<String>> = files
            .iter()
            .map(|f| {
                let text = std::fs::read_to_string(f).unwrap();
                let header: Header = serde_json::from_str(text.lines().next().unwrap()).unwrap();
                header.cluster_id
            })
            .collect();
        assert_eq!(
            ids,
            vec![None, Some("cid-from-gossip".to_string())],
            "the export taken AFTER adoption must carry the id — a snapshot taken at \
             construction would leave both files empty"
        );
    }
}
