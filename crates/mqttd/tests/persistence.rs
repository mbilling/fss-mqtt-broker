//! On-disk persistence restart test (ADR 0018 phase 1, the phase-5 node-level proof).
//!
//! The in-memory broker (`start_broker`) proves a *retained* session is replayed while
//! the process stays up. This proves the stronger, on-disk claim: a persistent session
//! and a message queued for it while it was offline both survive a **full node restart**
//! from the same data directory — observed end to end over real TCP by a client.
//!
//! It is also the proof that graceful shutdown (ADR 0019) releases the redb file lock:
//! the restarting node reopens `sessions.redb` from the same directory, which fails
//! ("Database already open") if the previous node leaked a database handle.

mod common;

use common::{start_persistent_node, Client, TempDir};
use mqtt_codec::{Packet, QoS};
use std::time::Duration;

#[tokio::test]
async fn persistent_session_and_offline_queue_survive_a_node_restart() {
    let dir = TempDir::new();

    // --- node lifetime #1: establish a persistent session and queue a message for it ---
    let node = start_persistent_node(dir.path()).await;
    let addr = node.client_addr;

    // A persistent subscriber registers its session + subscription, then detaches.
    let (mut sub, present) = Client::connect_v311(addr, "durable", false).await;
    assert!(
        !present,
        "a brand-new persistent session has no prior state"
    );
    sub.subscribe(1, "offline/topic", QoS::AtMostOnce).await;
    sub.disconnect().await; // session retained on disk, subscriber now offline

    // Publish QoS 1 while the subscriber is away; the PUBACK proves the message is
    // durably enqueued (fsync'd) before we restart.
    let mut pubr = Client::connect(addr, "pub-offline").await;
    pubr.publish(
        "offline/topic",
        b"queued-while-away",
        QoS::AtLeastOnce,
        Some(1),
        vec![],
    )
    .await;
    assert_eq!(pubr.recv().await, mqtt_codec::Packet::PubAck(1.into()));
    pubr.disconnect().await;

    // Restart: a clean shutdown must release the redb lock so the directory reopens.
    node.shutdown().await;

    // --- node lifetime #2: a fresh node over the SAME directory recovers the state ---
    let node = start_persistent_node(dir.path()).await;
    let addr = node.client_addr;

    // Same client id, clean_session=false: the session is present (recovered from disk)
    // and the message queued before the restart is replayed.
    let (mut sub, present) = Client::connect_v311(addr, "durable", false).await;
    assert!(
        present,
        "the persistent session must survive a node restart"
    );
    let p = sub.expect_publish().await;
    assert_eq!(p.topic, "offline/topic");
    assert_eq!(&p.payload[..], b"queued-while-away");

    sub.disconnect().await;
    node.shutdown().await;
}

/// Issue #299, the node-death claim tested in the durable shape operators actually run on
/// ONE node (ADR 0018 phase 1: `MQTTD_DATA_DIR`, no cluster): **a delayed Will armed before
/// a restart is still announced after it.**
///
/// This is the shape where the claim was false. The will and its absolute deadline were
/// written to `sessions.redb` correctly — and then never read back, because
/// `ReplicatedSessionStore::all_sessions` enumerates through `ReplicatedLog::keys()` and
/// `PersistentLog` had no `keys()`, taking the trait's empty default. The restarted node's
/// inherited-session scan therefore saw *no sessions at all*, so the will was silently lost
/// (and, the same way, ADR 0009 §3's persisted expiry deadlines never fired here either).
/// Before #299 a will could not be lost this way at all: an ungraceful disconnect published
/// it synchronously. So the delay had to bring its own durability with it, in every durable
/// shape, or it is a regression rather than a feature.
///
/// The will is RETAINED here on purpose: it is published by the boot scan within a second of
/// the restart, and the assertion should be "it was published", not a race against how fast
/// a test client can subscribe.
#[tokio::test]
async fn a_delayed_will_survives_a_node_restart_over_the_same_data_dir() {
    use mqtt_codec::packet::{Connect, LastWill};
    use mqtt_codec::{Properties, Property};

    let dir = TempDir::new();
    let node = start_persistent_node(dir.path()).await;
    let addr = node.client_addr;

    // A v5 client that asks for 3 s of grace and a session that outlives its connection.
    let mut victim = Client::open(addr, common::V5).await;
    victim
        .send(&Packet::Connect(Connect {
            properties: Properties(vec![Property::SessionExpiryInterval(300)]),
            protocol: common::V5,
            clean_session: true,
            keep_alive: 0,
            client_id: "restart-will".to_string(),
            last_will: Some(LastWill {
                properties: Properties(vec![Property::WillDelayInterval(3)]),
                topic: "wills/restart".into(),
                payload: bytes::Bytes::from_static(b"gone"),
                qos: QoS::AtMostOnce,
                retain: true,
            }),
            username: None,
            password: None,
        }))
        .await;
    match victim.recv().await {
        Packet::ConnAck(a) => assert_eq!(a.code, 0, "v5 CONNACK should be success"),
        other => panic!("expected CONNACK, got {other:?}"),
    }

    // A bystander watching the will topic: nothing may arrive while the delay runs.
    let mut watcher = Client::connect(addr, "restart-watch").await;
    watcher.subscribe(1, "wills/restart", QoS::AtMostOnce).await;

    // The socket dies without a DISCONNECT — the canonical will-firing end.
    drop(victim);
    assert!(
        matches!(
            watcher.recv_bounded(Duration::from_millis(800)).await,
            common::Recv::Quiet
        ),
        "a 3 s Will Delay Interval must not be announced within the first second"
    );

    // …and the node goes down INSIDE the window, taking the hub's memory with it. Only the
    // record is left.
    watcher.disconnect().await;
    node.shutdown().await;

    let node = start_persistent_node(dir.path()).await;
    let addr = node.client_addr;
    let mut watcher = Client::connect(addr, "restart-watch-2").await;
    // The boot inherited-session scan reads the record back and fires the will: its
    // deadline is absolute, so the restart does not restart the clock — it has already
    // passed, and a will that is late is still delivered.
    let mut got = None;
    for _ in 0..40 {
        watcher.subscribe(1, "wills/restart", QoS::AtMostOnce).await;
        if let common::Recv::Packet(Packet::Publish(p)) =
            watcher.recv_bounded(Duration::from_millis(250)).await
        {
            got = Some(p);
            break;
        }
    }
    let p = got.expect(
        "a delayed will armed before the restart must still be announced after it — the \
         record held it, and this is the one case the Will exists for",
    );
    assert_eq!(p.topic, "wills/restart");
    assert_eq!(&p.payload[..], b"gone");

    watcher.disconnect().await;
    node.shutdown().await;
}

/// Issue #299, the two PROPERTIES the inherited path is judged by, on the real broker over
/// real sockets, **repeated** — because a single green run proves nothing about a race:
///
/// 1. **A will inherited across a restart fires at or after its deadline, never before.** It
///    fired up to ~1 s early: the arm stored `floor(now) + delay` and the re-arm computed
///    `deadline − floor(now)`, so the elapsed delay was `delay − frac(arm) + frac(re-arm)`.
///    Measured 7.312 s for an 8 s delay. ADR 0009 promises "never early" and TROUBLESHOOTING
///    calls an early will a defect rather than a tuning question, so this asserts the
///    DIRECTION and prints the margin.
/// 2. **It is published exactly once.** It was published twice in 5 of 6 restart runs: the
///    sweep spawns the off-loop session scan before `fire_due_wills`, so the scan's read of
///    the record raced the fire's durable clear, and the stale block was re-armed with zero
///    remaining and announced again ~20 ms later — both copies labelled `reason="inherited"`,
///    so no operator could tell a duplicate from two deaths.
///
/// The disconnect is deliberately aligned to just BEFORE a whole wall-clock second. That is
/// not decoration: the old error was `frac(re-arm) − frac(arm)`, so an arm at `x.05` hid it
/// (the error lands late) and an arm at `x.92` exposes it in ~92 % of runs. Aligned, and
/// repeated, the old arithmetic fails essentially every time instead of one run in two.
#[tokio::test]
async fn a_will_inherited_across_a_restart_fires_no_earlier_than_its_deadline_exactly_once() {
    /// Long enough that a restart (redb reopen, boot scan, a client connecting and
    /// subscribing) always finishes inside the window with room to spare.
    const DELAY_S: u64 = 6;
    /// Enough to catch the duplicate, which arrived ~20 ms after the first copy.
    const QUIET_AFTER: Duration = Duration::from_secs(2);
    const RUNS: usize = 6;

    for run in 0..RUNS {
        one_restart_round(run, DELAY_S, QUIET_AFTER).await;
    }
}

/// One repetition of [`a_will_inherited_across_a_restart_fires_no_earlier_than_its_deadline_exactly_once`]:
/// arm, kill the node inside the window, restart, and judge the single announcement.
/// The first half of one repetition: arm a delayed will on a node and kill that node inside
/// the window, leaving only the durable record. Returns the data dir and a LOWER BOUND on the
/// arm instant — read before the socket closes, so the deadline is at or after
/// `dropped_at + delay_s` and the caller's "never early" assertion can never be a false alarm.
async fn arm_a_will_and_kill_the_node(run: usize, delay_s: u64) -> (TempDir, std::time::Instant) {
    use mqtt_codec::packet::{Connect, LastWill};
    use mqtt_codec::{Properties, Property};
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    let dir = TempDir::new();
    let node = start_persistent_node(dir.path()).await;
    let addr = node.client_addr;

    let mut victim = Client::open(addr, common::V5).await;
    victim
        .send(&Packet::Connect(Connect {
            properties: Properties(vec![Property::SessionExpiryInterval(300)]),
            protocol: common::V5,
            clean_session: true,
            keep_alive: 0,
            client_id: "early-probe".to_string(),
            last_will: Some(LastWill {
                properties: Properties(vec![Property::WillDelayInterval(
                    u32::try_from(delay_s).unwrap(),
                )]),
                topic: "wills/early".into(),
                payload: bytes::Bytes::from_static(b"gone"),
                qos: QoS::AtMostOnce,
                // NOT retained: a retained copy would be redelivered on subscribe and could
                // not be told apart from a second announcement.
                retain: false,
            }),
            username: None,
            password: None,
        }))
        .await;
    match victim.recv().await {
        Packet::ConnAck(a) => assert_eq!(a.code, 0, "v5 CONNACK should be success"),
        other => panic!("expected CONNACK, got {other:?}"),
    }

    // A bystander on this node, for two reasons: nothing may be announced at the disconnect
    // itself, and waiting for that answer is what proves the broker has processed the detach —
    // armed and fsync'd the record — before the node is stopped.
    let mut before = Client::connect(addr, "early-watch-1").await;
    before.subscribe(1, "wills/early", QoS::AtMostOnce).await;

    // Align the disconnect to just before a whole second of wall clock: the direction in which
    // truncating the stored deadline fires EARLY.
    loop {
        let frac = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_millis();
        if (900..980).contains(&frac) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(3)).await;
    }

    let dropped_at = Instant::now();
    drop(victim);
    assert!(
        matches!(
            before.recv_bounded(Duration::from_millis(400)).await,
            common::Recv::Quiet
        ),
        "run {run}: a {delay_s} s Will Delay Interval must not be announced at the disconnect"
    );
    before.disconnect().await;
    node.shutdown().await;
    (dir, dropped_at)
}

async fn one_restart_round(run: usize, delay_s: u64, quiet_after: Duration) {
    use std::time::Instant;

    let (dir, dropped_at) = arm_a_will_and_kill_the_node(run, delay_s).await;
    {
        let node = start_persistent_node(dir.path()).await;
        let addr = node.client_addr;
        let mut watcher = Client::connect(addr, "early-watch").await;
        watcher.subscribe(1, "wills/early", QoS::AtMostOnce).await;
        let deadline = dropped_at + Duration::from_secs(delay_s);
        assert!(
            Instant::now() < deadline,
            "run {run}: the restart must finish inside the delay window, or this test \
             cannot observe when the will fires"
        );

        let mut landed = None;
        while Instant::now() < deadline + Duration::from_secs(delay_s) {
            if let common::Recv::Packet(Packet::Publish(p)) =
                watcher.recv_bounded(Duration::from_millis(20)).await
            {
                assert_eq!(p.topic, "wills/early");
                assert_eq!(&p.payload[..], b"gone");
                landed = Some(Instant::now());
                break;
            }
        }
        let landed = landed.expect(
            "a delayed will armed before the restart must still be announced after it — \
             the record held it, and this is the one case the Will exists for",
        );

        // PROPERTY 1, as a direction with the margin reported.
        if landed >= deadline {
            println!(
                "run {run}: inherited will landed {} ms LATE (the only permitted direction)",
                (landed - deadline).as_millis()
            );
        } else {
            panic!(
                "run {run}: an inherited will fired {} ms EARLY (a {delay_s} s delay \
                 elapsed in {} ms). A will is a death announcement: late is a slow \
                 announcement, early is a false one",
                (deadline - landed).as_millis(),
                (landed - dropped_at).as_millis()
            );
        }

        // PROPERTY 2: nothing follows it.
        let mut duplicates = 0;
        let quiet_until = Instant::now() + quiet_after;
        while Instant::now() < quiet_until {
            if let common::Recv::Packet(Packet::Publish(p)) =
                watcher.recv_bounded(Duration::from_millis(50)).await
            {
                assert_eq!(p.topic, "wills/early");
                duplicates += 1;
            }
        }
        assert_eq!(
            duplicates, 0,
            "run {run}: one death, one announcement. A duplicate labelled the same way as \
             the first copy is indistinguishable from a second death"
        );

        watcher.disconnect().await;
        node.shutdown().await;
    }
}

#[tokio::test]
async fn a_clean_session_does_not_survive_a_node_restart() {
    let dir = TempDir::new();

    let node = start_persistent_node(dir.path()).await;
    let addr = node.client_addr;

    // A clean (clean_session=true) session must not be persisted.
    let (mut c, present) = Client::connect_v311(addr, "ephemeral", true).await;
    assert!(!present);
    c.subscribe(1, "x", QoS::AtMostOnce).await;
    c.disconnect().await;
    node.shutdown().await;

    // Reopen: reconnecting clean shows no prior session, and a fresh persistent connect
    // under the same id is likewise absent (nothing was ever written).
    let node = start_persistent_node(dir.path()).await;
    let addr = node.client_addr;
    let (mut c, present) = Client::connect_v311(addr, "ephemeral", false).await;
    assert!(!present, "a clean session must not survive a restart");

    c.disconnect().await;
    node.shutdown().await;
}
