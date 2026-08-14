//! Brownout refuses the publisher's ack, end to end (issue #238, ADR 0041 T5/T11).
//!
//! The hub tests prove the outcome the ack-gate carries and the `conn` tests prove
//! the per-version mapping. Neither proves the two are wired together over a real
//! socket with the real codec: that a v5 publisher actually *receives* `0x97` on the
//! wire, that a v3.1.1 publisher actually sees its connection close with no PUBACK,
//! and that nothing was quietly stored during the refusal window.
//!
//! The broker is built inline (rather than via `common::start_broker`) because the
//! test needs `hub_tx` to drive `SetBrownout` — and brownout must be toggled AFTER
//! the clients exist, since a browned-out broker refuses NEW sessions (T5 growth).

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{Client, Recv};
use mqtt_cluster::NodeId;
use mqtt_codec::{Packet, QoS};
use mqtt_storage::MemorySessionStore;
use mqttd::hub::{BrownoutAxis, Hub, HubCommand};
use tokio::net::TcpListener;

/// MQTT 5.0 reason code 0x97 — the honest answer to "I cannot durably take this".
const QUOTA_EXCEEDED: u8 = 0x97;

struct Broker {
    addr: std::net::SocketAddr,
    hub_tx: tokio::sync::mpsc::UnboundedSender<HubCommand>,
}

impl Broker {
    fn brownout(&self, on: bool) {
        self.hub_tx
            .send(HubCommand::SetBrownout {
                axis: BrownoutAxis::Disk,
                on,
            })
            .unwrap();
    }
}

/// A permissive in-process broker whose hub channel the test keeps.
///
/// The connection policy is built explicitly rather than via `mqttd::conn::handle`
/// because that convenience path hardcodes `store: None` — so every connection would run
/// on the per-connection `QoS` 2 dedup fallback and no test could observe the DURABLE
/// window that issue #238's `QoS` 2 half turns on. Here the same store the hub owns backs
/// it, which is the production wiring.
async fn start_broker() -> Broker {
    let store = Arc::new(MemorySessionStore::new());
    let (hub, hub_tx) = Hub::with_config(NodeId("brownout-ack".into()), store.clone());
    tokio::spawn(hub.run());

    let policy = Arc::new(mqttd::conn::ConnPolicy {
        auth: mqttd::conn::auth_handle(Arc::new(mqtt_auth::basic::BasicAuthenticator {
            allow_anonymous: true,
        })),
        authz: mqttd::conn::authz_handle(Arc::new(mqtt_auth::AllowAll)),
        identity_source: mqtt_auth::mtls::IdentitySource::default(),
        audit: Arc::new(mqtt_observability::AuditLog::new()),
        proxy: None,
        node: None,
        store: Some(store as Arc<dyn mqtt_storage::SessionStore>),
        connect_timeout: Duration::from_secs(10),
        enhanced: None,
        shutdown: None,
        metrics: None,
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accept_tx = hub_tx.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let peer = stream.peer_addr().ok();
            let policy = policy.clone();
            let tx = accept_tx.clone();
            tokio::spawn(async move {
                mqttd::conn::handle_stream(stream, peer, None, policy, tx).await;
            });
        }
    });
    Broker { addr, hub_tx }
}

/// A persistent v5 subscriber on `bo/t` at `QoS` 1 that then goes offline — the
/// recipient whose durable queue is the only place a publish could land.
///
/// The explicit Session Expiry Interval is load-bearing: in MQTT 5 an ABSENT interval
/// means zero, so `clean_start = false` alone would still discard the session at
/// disconnect and leave nothing for a publish to be owed.
async fn sleeping_subscriber(addr: std::net::SocketAddr, id: &str) {
    let (mut sub, ack) = Client::connect_v5(
        addr,
        id,
        false,
        vec![mqtt_codec::Property::SessionExpiryInterval(u32::MAX)],
    )
    .await;
    assert_eq!(ack.code, 0);
    let suback = sub.subscribe(1, "bo/t", QoS::AtLeastOnce).await;
    assert_eq!(suback.return_codes, vec![1]);
    sub.disconnect().await;
}

/// Issue #238 — a v5 publisher is told `0x97` for a `QoS` 1 message brownout will
/// not durably enqueue, and nothing is stored during the refusal window.
#[tokio::test]
async fn a_v5_publisher_is_told_0x97_when_brownout_refuses_the_durable_enqueue() {
    let broker = start_broker().await;
    sleeping_subscriber(broker.addr, "bo-sub-v5").await;
    // The publisher connects BEFORE the watermark is crossed: a browned-out broker
    // refuses NEW sessions (T5 growth), which would refuse the CONNECT instead of
    // the publish and prove nothing about the ack.
    let mut pubr = Client::connect_v5_ok(broker.addr, "bo-pub-v5").await;
    broker.brownout(true);

    pubr.publish("bo/t", b"refused", QoS::AtLeastOnce, Some(1), vec![])
        .await;
    match pubr.recv_bounded(Duration::from_secs(5)).await {
        Recv::Packet(Packet::PubAck(a)) => {
            assert_eq!(a.pkid, 1);
            assert_eq!(
                a.reason, QUOTA_EXCEEDED,
                "a QoS 1 publish brownout will not enqueue must be refused, not acked"
            );
        }
        other => panic!("expected PUBACK 0x97, got {other:?}"),
    }
    // The connection survives the refusal: it is a per-publish delivery error the
    // application can retry, not an outage of the session.
    pubr.publish("bo/t", b"refused-again", QoS::AtLeastOnce, Some(2), vec![])
        .await;
    match pubr.recv_bounded(Duration::from_secs(5)).await {
        Recv::Packet(Packet::PubAck(a)) => assert_eq!(a.reason, QUOTA_EXCEEDED),
        other => panic!("the publisher's connection must stay open, got {other:?}"),
    }

    // And nothing was stored: the sleeper resumes to an empty queue.
    let (mut sub, ack) = Client::connect_v5(
        broker.addr,
        "bo-sub-v5",
        false,
        vec![mqtt_codec::Property::SessionExpiryInterval(u32::MAX)],
    )
    .await;
    assert_eq!(ack.code, 0);
    assert!(ack.session_present, "the persistent session must resume");
    sub.expect_silence().await;
}

/// Issue #238 — v3.1.1 has no PUBACK reason byte, so the refusal is said the only
/// way the protocol allows: no ack, and a close. The publisher resends on reconnect
/// per [MQTT-4.4.0-1]. The control in the same test proves this is a refusal WINDOW,
/// not an outage: after recovery the same publish is acked and does replay.
#[tokio::test]
async fn a_v311_publisher_is_closed_without_a_puback_when_brownout_refuses() {
    let broker = start_broker().await;
    sleeping_subscriber(broker.addr, "bo-sub-v4").await;
    // Connected before the watermark, as above: it is the PUBLISH that must be
    // refused here, not the CONNECT.
    let (mut pubr, _) = Client::connect_v311(broker.addr, "bo-pub-v4", true).await;
    broker.brownout(true);

    pubr.publish("bo/t", b"refused", QoS::AtLeastOnce, Some(1), vec![])
        .await;
    pubr.expect_closed().await;

    // Recovery below the mark restores the growth write.
    broker.brownout(false);
    let (mut pubr, _) = Client::connect_v311(broker.addr, "bo-pub-v4b", true).await;
    pubr.publish("bo/t", b"kept", QoS::AtLeastOnce, Some(1), vec![])
        .await;
    assert_eq!(
        pubr.recv().await,
        Packet::PubAck(1.into()),
        "below the watermark the publish is acked again"
    );

    // The sleeper replays exactly the acked payload — and only it.
    let (mut sub, present) = Client::connect_v311(broker.addr, "bo-sub-v4", false).await;
    assert!(present, "the persistent session must resume");
    let replayed = sub.expect_publish().await;
    assert_eq!(replayed.payload.as_ref(), b"kept");
    if let Some(pkid) = replayed.pkid {
        sub.puback(pkid).await;
    }
    sub.expect_silence().await;
}

/// Issue #238, the `QoS` 2 half, against a REAL browned-out broker (the wiring gap: the
/// per-version `QoS` 2 mapping was proved only against a stub whose refusal is hard-coded).
#[tokio::test]
async fn a_v311_qos2_publisher_is_closed_without_a_pubrec_by_a_real_browned_out_broker() {
    let broker = start_broker().await;
    sleeping_subscriber(broker.addr, "bo2-sub").await;
    // Both publishers connect BEFORE the watermark: a browned-out broker refuses NEW
    // sessions (T5 growth), which would refuse the CONNECT and prove nothing about the
    // PUBREC.
    let (mut pubr, _) = Client::connect_v311(broker.addr, "bo2-pub", true).await;
    let mut v5 = Client::connect_v5_ok(broker.addr, "bo2-pub-v5").await;
    broker.brownout(true);

    pubr.publish("bo/t", b"refused", QoS::ExactlyOnce, Some(1), vec![])
        .await;
    pubr.expect_closed().await;

    // v5 gets the reason byte instead, and its connection survives.
    v5.publish("bo/t", b"refused", QoS::ExactlyOnce, Some(1), vec![])
        .await;
    match v5.recv_bounded(Duration::from_secs(5)).await {
        Recv::Packet(Packet::PubRec(a)) => {
            assert_eq!(a.pkid, 1);
            assert_eq!(
                a.reason, QUOTA_EXCEEDED,
                "a QoS 2 publish brownout will not enqueue is refused, not PUBRECed"
            );
        }
        other => panic!("expected PUBREC 0x97, got {other:?}"),
    }

    // Nothing was stored in either case.
    let (mut sub, ack) = Client::connect_v5(
        broker.addr,
        "bo2-sub",
        false,
        vec![mqtt_codec::Property::SessionExpiryInterval(u32::MAX)],
    )
    .await;
    assert_eq!(ack.code, 0);
    assert!(ack.session_present);
    sub.expect_silence().await;
}

/// Issue #238 — THE NO-SECOND-FAN-OUT TEST. The client's resend of a refused `QoS` 2
/// packet id [MQTT-4.4.0-1] must be re-decided, and the whole exchange must fan out
/// EXACTLY ONCE across all three attempts.
///
/// The witness is Z: online, CLEAN session, `QoS` 1. It owes no durability, so before the
/// refusal hoist it received a live copy on every refused attempt — three copies for one
/// message. And the persistent subscriber S must end up with exactly one queued entry,
/// which is what fails if the record is kept without the acked bit (the third attempt
/// would be answered from the window with nothing stored).
#[tokio::test]
async fn a_refused_qos2_publish_fans_out_exactly_once_across_the_clients_mandatory_resend() {
    let broker = start_broker().await;

    // S: persistent QoS 2 subscriber, offline through the whole refusal window.
    let (mut s, ack) = Client::connect_v5(
        broker.addr,
        "q2-sleeper",
        false,
        vec![mqtt_codec::Property::SessionExpiryInterval(u32::MAX)],
    )
    .await;
    assert_eq!(ack.code, 0);
    assert_eq!(
        s.subscribe(1, "bo/t", QoS::ExactlyOnce).await.return_codes,
        vec![2]
    );
    s.disconnect().await;

    // Z: online clean-session QoS 1 witness.
    let mut z = Client::connect_v5_ok(broker.addr, "q2-witness").await;
    assert_eq!(
        z.subscribe(1, "bo/t", QoS::AtLeastOnce).await.return_codes,
        vec![1]
    );

    let mut pubr = Client::connect_v311(broker.addr, "q2-pub", false).await.0;
    broker.brownout(true);

    // Attempt 1: refused, no PUBREC, closed.
    pubr.publish("bo/t", b"once", QoS::ExactlyOnce, Some(1), vec![])
        .await;
    pubr.expect_closed().await;

    // Attempt 2: the mandatory resend of the SAME id, on a resumed session. Refused again
    // — and it must reach routing rather than earn a fabricated success PUBREC.
    let mut pubr = Client::connect_v311(broker.addr, "q2-pub", false).await.0;
    pubr.publish("bo/t", b"once", QoS::ExactlyOnce, Some(1), vec![])
        .await;
    pubr.expect_closed().await;

    // Attempt 3, below the watermark: accepted, and the flow completes.
    broker.brownout(false);
    let mut pubr = Client::connect_v311(broker.addr, "q2-pub", false).await.0;
    pubr.publish("bo/t", b"once", QoS::ExactlyOnce, Some(1), vec![])
        .await;
    assert_eq!(pubr.recv().await, Packet::PubRec(1.into()));
    pubr.pubrel(1).await;
    assert_eq!(pubr.recv().await, Packet::PubComp(1.into()));

    // Z received EXACTLY ONE copy in total: the two refused attempts were effect-free.
    let first = z.expect_publish().await;
    assert_eq!(first.payload.as_ref(), b"once");
    if let Some(pkid) = first.pkid {
        z.puback(pkid).await;
    }
    z.expect_silence().await;

    // And S's durable queue holds exactly one entry.
    let (mut s, ack) = Client::connect_v5(
        broker.addr,
        "q2-sleeper",
        false,
        vec![mqtt_codec::Property::SessionExpiryInterval(u32::MAX)],
    )
    .await;
    assert!(ack.session_present);
    let replayed = s.expect_publish().await;
    assert_eq!(replayed.payload.as_ref(), b"once");
    if let Some(pkid) = replayed.pkid {
        s.pubrec(pkid).await;
        assert_eq!(s.recv().await, Packet::PubRel(pkid.into()));
        s.pubcomp(pkid).await;
    }
    s.expect_silence().await;
}

/// Issue #238 (R1) — the Will of a v3.1.1 publisher closed BY THE BROKER's refusal must
/// still be published. The close is not a client DISCONNECT [MQTT-3.14.4-3], and this is
/// the path brownout makes routine: every v3.1.1 `QoS` ≥ 1 publisher above the watermark
/// is hung up on.
#[tokio::test]
async fn a_v311_publisher_closed_by_a_brownout_refusal_still_fires_its_will() {
    let broker = start_broker().await;

    // The watcher is CLEAN-session at QoS 1: it owes no durable record, so the will's
    // ungated publish is not itself refused for it, and the assertion is about the will
    // being published at all rather than about the will's durability.
    let mut watcher = Client::connect_v5_ok(broker.addr, "will-watcher").await;
    assert_eq!(
        watcher
            .subscribe(1, "dev/42/status", QoS::AtLeastOnce)
            .await
            .return_codes,
        vec![1]
    );
    // A sleeping subscriber on the PUBLISH topic, so the publish below owes an append.
    sleeping_subscriber(broker.addr, "will-sleeper").await;

    let mut dev = Client::open(broker.addr, mqtt_codec::ProtocolVersion::V311).await;
    dev.connect_with_will("dev42", "dev/42/status", b"offline")
        .await;
    broker.brownout(true);

    dev.publish("bo/t", b"telemetry", QoS::AtLeastOnce, Some(1), vec![])
        .await;
    dev.expect_closed().await;

    let will = watcher.expect_publish().await;
    assert_eq!(
        (will.topic.as_str(), will.payload.as_ref()),
        ("dev/42/status", b"offline".as_slice()),
        "a broker-initiated close is NOT a client DISCONNECT: the will must fire"
    );
}
