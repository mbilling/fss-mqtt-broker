//! The instrument behind ADR 0062's RPO/RTO numbers: how long an online export takes, how
//! wide its window is, and how long an import of it takes.
//!
//! `#[ignore]` in the per-PR profile and run in the nightly tier, exactly like
//! `durable_bench.rs`: it writes tens of thousands of fsync'd records, so its cost is
//! measured in minutes, and it reports numbers rather than a pass/fail verdict.
//!
//! ```text
//! cargo test -p mqttd --test backup_bench -- --ignored --nocapture
//! MQTTD_BACKUP_BENCH_SESSIONS=1000 MQTTD_BACKUP_BENCH_QUEUED=10 \
//!   MQTTD_BACKUP_BENCH_RETAINED=10000 cargo test -p mqttd --test backup_bench -- --ignored --nocapture
//! ```
//!
//! **What it measures and what it does not.** Both halves run against the single-node
//! persistent stores (`PersistentLog` + `PersistentRetainedStore`), in one process. That
//! isolates the term that dominates both numbers — one fsync per durable write, since
//! `Durability::Immediate` is set on every mutating transaction and neither store has a
//! batch-append API — and deliberately excludes the cluster's per-write quorum round-trip.
//! A cluster restore is therefore SLOWER than what this reports, never faster, and the
//! published record says so.
//!
//! Its only assertion is that the restored fixture verifies (every session and every
//! retained topic present, with its queue): a fast number that came from an export or an
//! import which skipped work would be worse than no number at all.

mod common;

use std::sync::Arc;
use std::time::Instant;

use mqtt_core::{ClientId, Message, QoS, Subscription};
use mqtt_storage::{RetainedStore, SessionStore};
use mqttd::backup::{ExportContext, RetainedSink};

/// A fixture knob, from the environment with the ADR's stated default.
fn knob(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Write restored retained values straight into a retained store. The production import
/// goes through the hub (so a retained mutation commits through its topic's group
/// lease-owner, ADR 0037); this measures the STORE cost, which is the fsync-bound term.
struct StoreSink(Arc<dyn RetainedStore>);

#[async_trait::async_trait]
impl RetainedSink for StoreSink {
    async fn publish(
        &self,
        record: &mqttd::backup::RetainedRecord,
        _message_expiry: Option<u32>,
    ) -> Result<(), String> {
        let message = mqttd::backup::retained_message(record)?;
        self.0.set(&message).await.map_err(|e| e.to_string())
    }
}

fn persistent(dir: &std::path::Path) -> (Arc<dyn SessionStore>, Arc<dyn RetainedStore>) {
    let log = mqtt_storage::persistent_log::PersistentLog::open(dir.join("sessions.redb"))
        .expect("open sessions.redb");
    let sessions: Arc<dyn SessionStore> =
        Arc::new(mqtt_storage::logged::ReplicatedSessionStore::new(log));
    let retained: Arc<dyn RetainedStore> = Arc::new(
        mqtt_storage::persistent_retained::PersistentRetainedStore::open(dir.join("retained.redb"))
            .expect("open retained.redb"),
    );
    (sessions, retained)
}

// A measurement, not a branchy function: build the fixture, export, restore, verify, report.
// The casts are all instrument arithmetic over fixture sizes an operator chose.
#[allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement: tens of thousands of fsync'd writes; run it by hand with \
              `cargo test -p mqttd --release --test backup_bench -- --ignored`. NO CI tier runs \
              this: nightly drives --ignored for cluster_upgrade and cluster_soak only, and \
              saying otherwise here made a number look defended that nothing re-measures."]
async fn export_and_restore_wall_clock_at_stated_fixtures() {
    let n_sessions = knob("MQTTD_BACKUP_BENCH_SESSIONS", 1000);
    let n_queued = knob("MQTTD_BACKUP_BENCH_QUEUED", 10);
    let n_retained = knob("MQTTD_BACKUP_BENCH_RETAINED", 10_000);
    let payload = vec![b'x'; 256];

    let source_dir = tempfile::tempdir().expect("source dir");
    let (sessions, retained) = persistent(source_dir.path());

    // ---- fixture ----
    let built = Instant::now();
    for i in 0..n_sessions {
        let client = ClientId(format!("bench-{i}"));
        sessions
            .claim_session(&client, "bench-principal")
            .await
            .unwrap();
        sessions
            .set_subscriptions(
                &client,
                &[Subscription {
                    filter: format!("bench/{i}/#"),
                    max_qos: QoS::AtLeastOnce,
                    no_local: false,
                }],
            )
            .await
            .unwrap();
        for q in 0..n_queued {
            let message = Message::new(
                format!("bench/{i}/{q}"),
                bytes::Bytes::from(payload.clone()),
                QoS::AtLeastOnce,
                false,
            );
            sessions.enqueue(&client, &message).await.unwrap();
        }
    }
    for t in 0..n_retained {
        retained
            .set(&Message::new(
                format!("bench/retained/{t}"),
                bytes::Bytes::from(payload.clone()),
                QoS::AtMostOnce,
                true,
            ))
            .await
            .unwrap();
    }
    let fixture_secs = built.elapsed().as_secs_f64();
    let fixture_writes = (n_sessions * (2 + n_queued) + n_retained) as f64;

    // ---- export ----
    let out = tempfile::tempdir().expect("backup dir");
    let ctx = ExportContext {
        dir: out.path().to_path_buf(),
        keep: 2,
        node_id: "bench".to_string(),
        cluster_id: None,
        durable: false,
        members: vec!["bench".to_string()],
    };
    let started = Instant::now();
    // The single-node measurement reads retained straight off the store handle (no hub in
    // this fixture), so the numbers exclude the token lookup a clustered node pairs with it —
    // an in-memory map read per topic, and stated here so the exclusion is not silent.
    let source: Arc<dyn mqttd::backup::RetainedSource> =
        Arc::new(mqttd::backup::StoreRetainedSource(retained.clone()));
    let report = mqttd::backup::export(&ctx, &sessions, &source)
        .await
        .expect("export");
    let export_secs = started.elapsed().as_secs_f64();

    // ---- restore into a fresh store ----
    let target_dir = tempfile::tempdir().expect("target dir");
    let (t_sessions, t_retained) = persistent(target_dir.path());
    let plan = mqttd::backup::load(out.path()).expect("the export imports");
    let sink = StoreSink(t_retained.clone());
    let started = Instant::now();
    let restore_report = mqttd::backup::apply(&plan, &t_sessions, &sink)
        .await
        .expect("restore");
    let restore_secs = started.elapsed().as_secs_f64();

    // ---- the only assertion: the numbers came from real work ----
    assert_eq!(restore_report.sessions as usize, n_sessions);
    assert_eq!(restore_report.retained as usize, n_retained);
    assert_eq!(restore_report.queued as usize, n_sessions * n_queued);
    let probe = ClientId(format!("bench-{}", n_sessions / 2));
    let pending = t_sessions.pending(&probe, 0, usize::MAX).await.unwrap();
    assert_eq!(
        pending.len(),
        n_queued,
        "a probed session's queue is intact"
    );
    assert_eq!(pending[0].message.payload.len(), payload.len());
    assert_eq!(t_retained.count().await.unwrap(), n_retained);

    let records = report.sessions + report.queued + report.retained;
    println!(
        "\n=== ADR 0062 backup/restore measurement ===\n\
         fixture:           {n_sessions} sessions x {n_queued} queued x {} B, {n_retained} retained x {} B\n\
         fixture build:     {fixture_secs:.1} s for {fixture_writes:.0} durable writes \
         ({:.0} writes/s — the same fsync-bound path a restore uses)\n\
         export:            {export_secs:.2} s, {} bytes, window W = {} ms, {records} records\n\
         restore:           {restore_secs:.1} s for {} records ({:.0} records/s)\n\
         note:              single-node persistent stores, one process, one volume. A CLUSTER\n\
         \x20                  restore adds a quorum round-trip per write and is slower.\n",
        payload.len(),
        payload.len(),
        fixture_writes / fixture_secs.max(0.001),
        report.bytes,
        report.window_ms(),
        restore_report.sessions + restore_report.queued + restore_report.retained,
        (restore_report.sessions + restore_report.queued + restore_report.retained) as f64
            / restore_secs.max(0.001),
    );
}
