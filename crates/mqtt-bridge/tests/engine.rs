//! Engine integration (0025-T3/T4/T5, and the central 0025-T10 security property): run the
//! bridge between two **real** in-process brokers (a "local" and an "upstream" `mqttd`) and
//! verify that a one-way `out` rule forwards local→upstream with a remap and a stamped hop
//! count — and **never** leaks the reverse direction.
#![allow(clippy::similar_names)] // pub/sub-style test client names are intentionally paired

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use mqtt_bridge::client::{ConnectOptions, Event, MqttClient, Transport};
use mqtt_bridge::config::BridgeConfig;
use mqtt_bridge::engine::Bridge;
use mqtt_codec::properties::{Properties, Property};
use mqtt_codec::{ProtocolVersion, QoS};
use mqttd::Hub;
use tokio::net::TcpListener;

async fn start_broker() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    serve_broker(listener);
    addr
}

/// Start a broker on an already-bound listener (so a test can reserve a port, keep an
/// upstream "down", then bring it up on the same address).
fn serve_broker(listener: TcpListener) {
    let (hub, hub_tx) = Hub::new();
    tokio::spawn(hub.run());
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(mqttd::conn::handle(stream, hub_tx.clone()));
        }
    });
}

async fn client(addr: SocketAddr, id: &str) -> MqttClient {
    MqttClient::connect(&ConnectOptions {
        addr: addr.to_string(),
        transport: Transport::Plain,
        version: ProtocolVersion::V5,
        client_id: id.to_string(),
        username: None,
        password: None,
        keep_alive: 30,
        clean_start: true,
    })
    .await
    .unwrap()
}

/// Wait for a subscriber's SUBACK.
async fn subscribe(c: &mut MqttClient, filter: &str) {
    c.subscribe(1, filter, QoS::AtMostOnce).await.unwrap();
    match c.next_event().await.unwrap() {
        Event::SubAck { .. } => {}
        other => panic!("expected SubAck, got {other:?}"),
    }
}

fn hop_count(p: &mqtt_codec::packet::Publish) -> Option<String> {
    p.properties.0.iter().find_map(|prop| match prop {
        Property::UserProperty(k, v) if k == "fss-bridge-hop-count" => Some(v.clone()),
        _ => None,
    })
}

#[tokio::test]
async fn a_one_way_out_rule_forwards_to_the_upstream_and_never_leaks_back() {
    let local = start_broker().await;
    let upstream = start_broker().await;

    let cfg = BridgeConfig::parse_toml(&format!(
        r#"
        [local]
        url = "{local}"

        [[upstreams]]
        name = "partner"
        url = "{upstream}"

        [[upstreams.rules]]
        direction = "out"
        filter = "telemetry/#"
        remap = {{ strip_prefix = "telemetry/", prefix = "org/telemetry/" }}
        "#,
    ))
    .unwrap();
    let bridge = Bridge::start(cfg);

    // A subscriber on the UPSTREAM for the remapped topic.
    let mut up_sub = client(upstream, "up-sub").await;
    subscribe(&mut up_sub, "org/telemetry/#").await;

    // A subscriber on the LOCAL side that must NEVER receive an upstream-origin message for
    // this one-way `out` rule (the reverse path is closed).
    let mut local_sub = client(local, "local-sub").await;
    subscribe(&mut local_sub, "telemetry/#").await;

    // Publish on LOCAL; the bridge subscribes to telemetry/# locally and forwards to the
    // upstream as org/telemetry/.... Retry until the bridge's local subscription is live.
    let mut local_pub = client(local, "local-pub").await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let forwarded = loop {
        local_pub
            .publish(
                "telemetry/room/temp",
                Bytes::from_static(b"21C"),
                QoS::AtMostOnce,
                false,
                None,
                Properties::new(),
            )
            .await
            .unwrap();
        match tokio::time::timeout(Duration::from_millis(300), up_sub.next_event()).await {
            Ok(Ok(Event::Publish(p))) => break p,
            _ => assert!(
                tokio::time::Instant::now() < deadline,
                "the upstream never received the forwarded message"
            ),
        }
    };
    assert_eq!(forwarded.topic, "org/telemetry/room/temp", "remap applied");
    assert_eq!(&forwarded.payload[..], b"21C");
    assert_eq!(
        hop_count(&forwarded).as_deref(),
        Some("1"),
        "the first bridge hop stamps hop-count=1"
    );

    // Reverse direction: publish an upstream-origin message that MATCHES the out rule's
    // filter; the local subscriber must NEVER receive *that* message back (a one-way `out`
    // rule never opens the reverse path — the bridge never subscribed on the upstream for
    // it). We tag the probe with a unique payload so the legitimate local-origin "21C"
    // deliveries that `local_sub` also sees (normal same-broker delivery, not a bridge hop)
    // are not mistaken for a leak.
    let mut up_pub = client(upstream, "up-pub").await;
    for _ in 0..6 {
        up_pub
            .publish(
                "telemetry/leak/probe",
                Bytes::from_static(b"LEAK-PROBE"),
                QoS::AtMostOnce,
                false,
                None,
                Properties::new(),
            )
            .await
            .unwrap();
        // Drain whatever the local subscriber sees; only a probe-tagged message is a leak.
        while let Ok(Ok(Event::Publish(p))) =
            tokio::time::timeout(Duration::from_millis(150), local_sub.next_event()).await
        {
            assert_ne!(
                &p.payload[..],
                b"LEAK-PROBE",
                "one-way out rule leaked an upstream message back to local"
            );
        }
    }

    // Observability (T9): the out forward was counted; nothing was forwarded inbound.
    let m = bridge.metrics();
    assert!(m.forwarded_out_count() >= 1, "the out forward was counted");
    assert_eq!(m.forwarded_in_count(), 0, "nothing was forwarded inbound");

    bridge.shutdown();
}

/// #189 (bridge audit #1/#2): retained state must cross the boundary. A value that is already
/// retained on the local side (published before the bridge connects) must be forwarded **with**
/// the retain flag and land in the **upstream** broker's retained store — a *fresh* subscriber
/// on the upstream then replays it with RETAIN set. Before the fix the bridge forwarded with
/// `retain` hardcoded false, so the value crossed (if at all) as an ordinary message and never
/// entered the upstream's retained store.
///
/// The bridge receives this existing retained value as a **retained replay at subscribe time**,
/// which always carries retain=1 (independent of Retain As Published). The *live* case — a
/// value published while the bridge is already subscribed — is covered by
/// [`a_live_retained_publish_crosses_the_boundary_retained`], and needs the broker to honour
/// RAP (#198, now implemented).
///
/// `share_group` is cleared here: a `$share` subscription receives **no** retained messages
/// (MQTT-3.8.4), so retained state can only cross over a plain subscription. Under the default
/// shared-subscription HA the retained replay never arrives — the convergence with the HA
/// redesign in ADR 0059, whose partitioned *plain* subscriptions restore retained crossing.
#[tokio::test]
async fn a_retained_message_crosses_the_boundary_and_lands_retained() {
    let local = start_broker().await;
    let upstream = start_broker().await;

    let cfg = BridgeConfig::parse_toml(&format!(
        r#"
        share_group = ""

        [local]
        url = "{local}"

        [[upstreams]]
        name = "partner"
        url = "{upstream}"

        [[upstreams.rules]]
        direction = "out"
        filter = "sensors/#"
        "#,
    ))
    .unwrap();

    // Existing retained state on LOCAL, published BEFORE the bridge connects.
    let mut local_pub = client(local, "ret-pub").await;
    local_pub
        .publish(
            "sensors/room/temp",
            Bytes::from_static(b"21C"),
            QoS::AtMostOnce,
            true, // retained
            None,
            Properties::new(),
        )
        .await
        .unwrap();

    // Let the local broker store the retained value before the bridge subscribes.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // A live upstream subscriber, connected before the bridge, to confirm the value crosses
    // at all (separate from whether it lands retained).
    let mut up_live = client(upstream, "ret-live").await;
    subscribe(&mut up_live, "sensors/#").await;

    // The bridge subscribes to sensors/# on local, receives the retained value as a retained
    // replay (retain=1), and must forward it to the upstream WITH the retain flag so it enters
    // the upstream's retained store.
    let bridge = Bridge::start(cfg);

    // 1. It must cross at all.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        match tokio::time::timeout(Duration::from_millis(500), up_live.next_event()).await {
            Ok(Ok(Event::Publish(p))) if p.topic == "sensors/room/temp" => {
                assert_eq!(&p.payload[..], b"21C");
                break;
            }
            Ok(Ok(_)) => {}
            _ => assert!(
                tokio::time::Instant::now() < deadline,
                "the retained value never crossed to the upstream at all"
            ),
        }
    }

    // 2. A FRESH upstream subscriber replays only what the upstream stored retained.
    let mut n = 0;
    let replayed = loop {
        n += 1;
        let mut fresh = client(upstream, &format!("ret-fresh-{n}")).await;
        fresh
            .subscribe(1, "sensors/#", QoS::AtMostOnce)
            .await
            .unwrap();
        let mut found = None;
        let inner = tokio::time::Instant::now() + Duration::from_millis(500);
        while tokio::time::Instant::now() < inner {
            match tokio::time::timeout(Duration::from_millis(150), fresh.next_event()).await {
                Ok(Ok(Event::Publish(p))) if p.topic == "sensors/room/temp" => {
                    found = Some(p);
                    break;
                }
                _ => {}
            }
        }
        if let Some(p) = found {
            break p;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the retained value never reached the upstream's retained store"
        );
    };
    assert!(
        replayed.retain,
        "the crossed value must be stored RETAINED on the upstream (retain flag set on replay)"
    );
    assert_eq!(&replayed.payload[..], b"21C");

    bridge.shutdown();
}

/// #189 residual, closed by #198: a retained value published **while the bridge is already
/// subscribed** (live traffic, not a subscribe-time replay) must also cross retained.
///
/// This is the case Retain As Published exists for. A plain subscription has RETAIN cleared on
/// an established-subscription delivery [MQTT-3.3.1-9], so before the broker honoured RAP the
/// bridge saw `retain=0` for live traffic and could not re-publish it as retained — only
/// *existing* retained state crossed (via the replay at subscribe). With RAP set on the
/// bridge's subscriptions (#191) and honoured by the broker (#198), the live value crosses too.
#[tokio::test]
async fn a_live_retained_publish_crosses_the_boundary_retained() {
    let local = start_broker().await;
    let upstream = start_broker().await;

    let cfg = BridgeConfig::parse_toml(&format!(
        r#"
        share_group = ""
        [local]
        url = "{local}"

        [[upstreams]]
        name = "partner"
        url = "{upstream}"

        [[upstreams.rules]]
        direction = "out"
        filter = "live/#"
        "#,
    ))
    .unwrap();
    let bridge = Bridge::start(cfg);

    // A live upstream subscriber, used to detect when the bridge's local subscription is up —
    // so the retained publish below is genuinely LIVE traffic, not a subscribe-time replay.
    let mut up_live = client(upstream, "live-ret-probe").await;
    subscribe(&mut up_live, "live/#").await;

    let mut local_pub = client(local, "live-ret-pub").await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        local_pub
            .publish(
                "live/warmup",
                Bytes::from_static(b"w"),
                QoS::AtMostOnce,
                false,
                None,
                Properties::new(),
            )
            .await
            .unwrap();
        match tokio::time::timeout(Duration::from_millis(200), up_live.next_event()).await {
            Ok(Ok(Event::Publish(p))) if p.topic == "live/warmup" => break,
            _ => assert!(
                tokio::time::Instant::now() < deadline,
                "the bridge's local subscription never went live"
            ),
        }
    }

    // Now publish a RETAINED value as live traffic and assert it lands retained upstream.
    let mut n = 0;
    let replayed = loop {
        n += 1;
        local_pub
            .publish(
                "live/room/temp",
                Bytes::from_static(b"22C"),
                QoS::AtMostOnce,
                true, // retained, published while the bridge is already subscribed
                None,
                Properties::new(),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        // A FRESH upstream subscriber replays only what the upstream stored retained.
        let mut fresh = client(upstream, &format!("live-fresh-{n}")).await;
        fresh.subscribe(1, "live/#", QoS::AtMostOnce).await.unwrap();
        let mut found = None;
        let inner = tokio::time::Instant::now() + Duration::from_millis(500);
        while tokio::time::Instant::now() < inner {
            match tokio::time::timeout(Duration::from_millis(150), fresh.next_event()).await {
                Ok(Ok(Event::Publish(p))) if p.topic == "live/room/temp" => {
                    found = Some(p);
                    break;
                }
                _ => {}
            }
        }
        if let Some(p) = found {
            break p;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "a LIVE retained publish never reached the upstream's retained store (RAP path)"
        );
    };
    assert!(
        replayed.retain,
        "the live-crossed value must be stored RETAINED on the upstream"
    );
    assert_eq!(&replayed.payload[..], b"22C");

    bridge.shutdown();
}

/// ADR 0025-T6: two bridge instances sharing a cluster-side group must **not** duplicate
/// forwarding — the shared subscription load-balances, so each local message is forwarded
/// to the upstream at most once.
#[tokio::test]
async fn two_bridge_instances_do_not_duplicate_forwarding() {
    let local = start_broker().await;
    let upstream = start_broker().await;

    let cfg = |client_id: &str| {
        BridgeConfig::parse_toml(&format!(
            r#"
            ha = "shared"
            share_group = "ha"
            [local]
            url = "{local}"
            client_id = "{client_id}"
            [[upstreams]]
            name = "partner"
            url = "{upstream}"
            [[upstreams.rules]]
            direction = "out"
            filter = "telemetry/#"
            "#,
        ))
        .unwrap()
    };
    let b1 = Bridge::start(cfg("bridge-a"));
    let b2 = Bridge::start(cfg("bridge-b"));

    // A subscriber on the upstream collects forwarded messages.
    let mut up_sub = client(upstream, "ha-up-sub").await;
    subscribe(&mut up_sub, "telemetry/#").await;

    // Let both instances connect and register their shared subscription.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Publish a batch of uniquely-payloaded messages on the local side.
    let mut local_pub = client(local, "ha-local-pub").await;
    for n in 0..20u32 {
        local_pub
            .publish(
                "telemetry/x",
                Bytes::from(format!("m{n}")),
                QoS::AtMostOnce,
                false,
                None,
                Properties::new(),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Collect for a window; every payload must appear at most once (no duplicate forward).
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    while let Ok(Ok(Event::Publish(p))) =
        tokio::time::timeout(Duration::from_millis(400), up_sub.next_event()).await
    {
        assert!(
            seen.insert(p.payload.to_vec()),
            "duplicate forward of {:?}: two instances both forwarded it",
            String::from_utf8_lossy(&p.payload)
        );
    }
    assert!(
        !seen.is_empty(),
        "the HA pair forwarded nothing — the shared subscription never delivered"
    );

    b1.shutdown();
    b2.shutdown();
}

/// #187 (ADR 0059): two PARTITIONED instances must deliver each **inbound** message exactly
/// once — the case the `$share` HA model cannot cover (the subscription lives on the foreign
/// broker, which has no `$share` for the bridge to share on). Each instance subscribes the
/// full upstream filter and forwards only the topics it owns, so a local subscriber sees every
/// message once — not twice (double-deliver, the bug) and not zero (a partition gap).
#[tokio::test]
async fn two_partitioned_instances_deliver_each_inbound_message_exactly_once() {
    let local = start_broker().await;
    let upstream = start_broker().await;

    let cfg = |client_id: &str, instance: usize| {
        BridgeConfig::parse_toml(&format!(
            r#"
            total = 2
            instance = {instance}
            [local]
            url = "{local}"
            client_id = "{client_id}"
            [[upstreams]]
            name = "partner"
            url = "{upstream}"
            # Distinct per instance — in production each pod's hostname differs; in one test
            # process both would derive the same default and collide (MQTT takeover), leaving
            # one instance's upstream subscription dead.
            client_id = "up-{client_id}"
            [[upstreams.rules]]
            direction = "in"
            filter = "cmd/#"
            "#,
        ))
        .unwrap()
    };
    let b1 = Bridge::start(cfg("pbridge-a", 0));
    let b2 = Bridge::start(cfg("pbridge-b", 1));

    // A subscriber on LOCAL collects inbound-forwarded messages.
    let mut local_sub = client(local, "part-local-sub").await;
    subscribe(&mut local_sub, "cmd/#").await;

    // Let both instances connect and subscribe on the upstream.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Publish on many DIFFERENT topics so ownership spreads across both instances.
    let mut up_pub = client(upstream, "part-up-pub").await;
    for n in 0..30u32 {
        let topic = format!("cmd/{n}");
        up_pub
            .publish(
                &topic,
                Bytes::from(format!("c{n}")),
                QoS::AtMostOnce,
                false,
                None,
                Properties::new(),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Every payload must arrive EXACTLY once — not twice (double-deliver) nor zero (a gap).
    let mut seen: std::collections::HashMap<Vec<u8>, usize> = std::collections::HashMap::new();
    while let Ok(Ok(Event::Publish(p))) =
        tokio::time::timeout(Duration::from_millis(500), local_sub.next_event()).await
    {
        *seen.entry(p.payload.to_vec()).or_default() += 1;
    }
    for (payload, count) in &seen {
        assert_eq!(
            *count,
            1,
            "payload {:?} delivered {count} times — partitioning failed",
            String::from_utf8_lossy(payload)
        );
    }
    assert_eq!(
        seen.len(),
        30,
        "every one of the 30 messages must arrive exactly once (got {} distinct)",
        seen.len()
    );

    b1.shutdown();
    b2.shutdown();
}

/// ADR 0025-T7: messages destined for a **down** upstream are spooled (not lost) and
/// replayed when it comes back.
#[tokio::test]
async fn a_full_spool_must_not_shed_a_message_the_bridge_already_accepted() {
    // ADR 0060 T1/T2 + T5: a spooled message was already acknowledged to the source, so the
    // source has dropped its copy. Shedding it to make room for a NEWER message loses a message
    // the source believes was delivered — the newer one must be refused instead (and left
    // unacked, so the source keeps it). Before the fix, drop-oldest quietly evicted the older,
    // already-acked message.
    let local = start_broker().await;
    let upstream_addr = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();

    // A QoS-1 rule (so the spool refuses at the cap) with room for exactly ONE message.
    let cfg = BridgeConfig::parse_toml(&format!(
        r#"
        share_group = ""
        [local]
        url = "{local}"
        [spool]
        max_messages = 1
        allow_ephemeral_spool = true
        [[upstreams]]
        name = "down"
        url = "{upstream_addr}"
        [[upstreams.rules]]
        direction = "out"
        filter = "t/#"
        qos = 1
        "#,
    ))
    .unwrap();
    let bridge = Bridge::start(cfg);
    tokio::time::sleep(Duration::from_secs(1)).await; // local connects; upstream stays down

    // FIRST is accepted into the one-slot spool; SECOND arrives with the spool full.
    let mut local_pub = client(local, "shed-pub").await;
    for (n, payload) in [(1u16, &b"FIRST"[..]), (2, &b"SECOND"[..])] {
        local_pub
            .publish(
                "t/x",
                Bytes::copy_from_slice(payload),
                QoS::AtLeastOnce,
                false,
                Some(n),
                Properties::new(),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Bring the upstream up; the spool replays on reconnect.
    let listener = TcpListener::bind(upstream_addr).await.unwrap();
    serve_broker(listener);
    let mut up_sub = client(upstream_addr, "shed-up-sub").await;
    subscribe(&mut up_sub, "t/#").await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
    let replayed = loop {
        if let Ok(Ok(Event::Publish(p))) =
            tokio::time::timeout(Duration::from_millis(300), up_sub.next_event()).await
        {
            if let Some(id) = p.pkid {
                up_sub.puback(id).await.unwrap();
            }
            break p.payload.to_vec();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "nothing replayed after the upstream came back — the accepted message was lost"
        );
    };
    assert_eq!(
        replayed,
        b"FIRST".to_vec(),
        "the spool shed a message it had already accepted (and acked) to make room for a newer \
         one; the newer message must be refused instead"
    );

    bridge.shutdown();
}

#[tokio::test]
async fn messages_spooled_while_an_upstream_is_down_replay_on_reconnect() {
    let local = start_broker().await;
    // Reserve an upstream address, then free it so the bridge's connects are refused (the
    // upstream is "down").
    let upstream_addr = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();

    let cfg = BridgeConfig::parse_toml(&format!(
        r#"
        share_group = ""
        [local]
        url = "{local}"
        [spool]
        allow_ephemeral_spool = true   # this test uses the in-memory spool deliberately
        [[upstreams]]
        name = "down"
        url = "{upstream_addr}"
        [[upstreams.rules]]
        direction = "out"
        filter = "t/#"
        qos = 1
        "#,
    ))
    .unwrap();
    let bridge = Bridge::start(cfg);

    // Let the local side connect; the upstream keeps failing (down).
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Publish while the upstream is down → the router spools these for it.
    let mut local_pub = client(local, "spool-pub").await;
    for n in 1..=5u16 {
        local_pub
            .publish(
                "t/x",
                Bytes::from(format!("s{n}")),
                QoS::AtLeastOnce,
                false,
                Some(n),
                Properties::new(),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Bring the upstream up on the reserved address, and subscribe before the bridge's
    // backoff fires its reconnect (which replays the spool).
    let listener = TcpListener::bind(upstream_addr).await.unwrap();
    serve_broker(listener);
    let mut up_sub = client(upstream_addr, "spool-up-sub").await;
    subscribe(&mut up_sub, "t/#").await;

    // The spooled messages replay to the upstream once the bridge reconnects.
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
    while seen.is_empty() {
        if let Ok(Ok(Event::Publish(p))) =
            tokio::time::timeout(Duration::from_millis(300), up_sub.next_event()).await
        {
            assert!(
                p.payload.starts_with(b"s"),
                "unexpected payload {:?}",
                p.payload
            );
            seen.insert(p.payload.to_vec());
            if let Some(id) = p.pkid {
                up_sub.puback(id).await.unwrap();
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "spooled messages never replayed after the upstream came back"
        );
    }

    bridge.shutdown();
}

/// ADR 0025-T10 (loop bounding): a `both` rule with no remap echoes between local and
/// upstream — the classic loop. The hop-count limit must **terminate** it (a copy reaches
/// the limit and is dropped) rather than amplify forever.
#[tokio::test]
async fn a_no_remap_both_rule_loop_is_bounded_by_the_hop_limit() {
    let local = start_broker().await;
    let upstream = start_broker().await;

    let cfg = BridgeConfig::parse_toml(&format!(
        r#"
        hop_count_limit = 3
        share_group = ""
        [local]
        url = "{local}"
        [[upstreams]]
        name = "a"
        url = "{upstream}"
        [[upstreams.rules]]
        direction = "both"
        filter = "loop/#"
        "#,
    ))
    .unwrap();
    let bridge = Bridge::start(cfg);
    let metrics = bridge.metrics();

    // Let both sides connect and subscribe (both directions of the `both` rule).
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // A message that has ALREADY traversed the hop limit must be dropped at the crossing —
    // the backstop for a multi-broker cycle (A→B→C→A) that No Local cannot catch. The trivial
    // single-broker self-echo loop is now prevented at the source by No Local (#198: the broker
    // does not echo the bridge's own forwards back to it), so the hop counter is exercised
    // directly with a pre-stamped message rather than by spinning a real loop. Republish until
    // the bridge's local subscription is live and the drop registers.
    let mut local_pub = client(local, "loop-pub").await;
    let at_limit = || {
        Properties(vec![Property::UserProperty(
            "fss-bridge-hop-count".into(),
            "3".into(), // == hop_count_limit
        )])
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while metrics.dropped_hop_limit_count() == 0 {
        local_pub
            .publish(
                "loop/x",
                Bytes::from_static(b"echo"),
                QoS::AtMostOnce,
                false,
                None,
                at_limit(),
            )
            .await
            .unwrap();
        assert!(
            tokio::time::Instant::now() < deadline,
            "a message already at the hop limit was not dropped at the crossing"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    bridge.shutdown();
}

/// ADR 0025-T10 (multi-upstream): a local message matching an `out` rule on two upstreams is
/// forwarded to both.
#[tokio::test]
async fn a_local_message_fans_out_to_multiple_upstreams() {
    let local = start_broker().await;
    let up1 = start_broker().await;
    let up2 = start_broker().await;

    let cfg = BridgeConfig::parse_toml(&format!(
        r#"
        share_group = ""
        [local]
        url = "{local}"
        [[upstreams]]
        name = "one"
        url = "{up1}"
        [[upstreams.rules]]
        direction = "out"
        filter = "fan/#"
        [[upstreams]]
        name = "two"
        url = "{up2}"
        [[upstreams.rules]]
        direction = "out"
        filter = "fan/#"
        "#,
    ))
    .unwrap();
    let bridge = Bridge::start(cfg);

    let mut sub1 = client(up1, "fan-sub-1").await;
    subscribe(&mut sub1, "fan/#").await;
    let mut sub2 = client(up2, "fan-sub-2").await;
    subscribe(&mut sub2, "fan/#").await;

    let mut local_pub = client(local, "fan-pub").await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let (mut got1, mut got2) = (false, false);
    while !(got1 && got2) {
        local_pub
            .publish(
                "fan/x",
                Bytes::from_static(b"fanout"),
                QoS::AtMostOnce,
                false,
                None,
                Properties::new(),
            )
            .await
            .unwrap();
        if let Ok(Ok(Event::Publish(_))) =
            tokio::time::timeout(Duration::from_millis(150), sub1.next_event()).await
        {
            got1 = true;
        }
        if let Ok(Ok(Event::Publish(_))) =
            tokio::time::timeout(Duration::from_millis(150), sub2.next_event()).await
        {
            got2 = true;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "a local message did not reach both upstreams (got1={got1}, got2={got2})"
        );
    }

    bridge.shutdown();
}

// ---------------------------------------------------------------------------
// Loop prevention across REAL topologies (BR-030..038).
//
// Three defences are active at once and they catch different shapes, which is
// why the tests below are written as a set rather than one "loops are prevented"
// case:
//
//   1. No Local — every bridge subscription sets it (`bridge_subscription_options`).
//      Unforgeable, but the broker only suppresses when the publisher is the SAME
//      client, so it catches the single-broker echo and nothing else.
//   2. Hop count (`fss-bridge-hop-count`) — the ONLY defence against a cycle
//      through several brokers, where each hop is a different client.
//   3. Direction — `plan_forwards` never routes upstream→upstream.
//
// The pre-existing hop-limit test pre-stamps a message at the limit and checks
// the counter moved; it never spins a cycle. So layer 2's actual job had never
// been exercised end to end.
// ---------------------------------------------------------------------------

/// Start a bridge whose local side is `local` and whose single upstream is `up`,
/// with one `both` rule on `filter` and an explicit id on BOTH endpoints.
///
/// The ids are not decoration. `default_client_id` derives from the hostname, so
/// several bridge instances in one test process would all pick the same id, take
/// each other over, and leave the ring silently broken rather than failing.
fn ring_bridge(tag: &str, local: SocketAddr, up: SocketAddr, filter: &str, limit: u32) -> Bridge {
    let cfg = BridgeConfig::parse_toml(&format!(
        r#"
        hop_count_limit = {limit}
        [local]
        url = "{local}"
        client_id = "{tag}-local"
        [[upstreams]]
        name = "next"
        url = "{up}"
        client_id = "{tag}-up"
        [[upstreams.rules]]
        direction = "both"
        filter = "{filter}"
        "#,
    ))
    .unwrap();
    Bridge::start(cfg)
}

/// Drain everything a subscriber sees until it goes quiet for `quiet`, returning
/// the payloads in arrival order.
///
/// Quiescence is the termination proof: a cycle that had NOT been cut would keep
/// feeding this loop, so "the socket went quiet" is the observable that says the
/// ring stopped, and the count says how far it got before it did.
async fn drain_until_quiet(c: &mut MqttClient, quiet: Duration) -> Vec<Vec<u8>> {
    let mut seen = Vec::new();
    while let Ok(Ok(Event::Publish(p))) = tokio::time::timeout(quiet, c.next_event()).await {
        seen.push(p.payload.to_vec());
    }
    seen
}

/// BR-031: a **cycle** A→B→C→A is terminated by the hop counter.
///
/// This is the topology No Local cannot see: three bridges are three different
/// clients, so every hop looks like a fresh publisher to the broker it lands on.
/// Nothing but the hop count stops the message going round forever.
#[tokio::test]
async fn a_three_broker_ring_is_terminated_by_the_hop_counter() {
    let (a, b, c) = (
        start_broker().await,
        start_broker().await,
        start_broker().await,
    );

    // A→B, B→C, C→A: a closed ring on the same topic space, no remap, so every
    // hop re-matches the rule that would forward it onward.
    let ab = ring_bridge("ab", a, b, "ring/#", 3);
    let bc = ring_bridge("bc", b, c, "ring/#", 3);
    let ca = ring_bridge("ca", c, a, "ring/#", 3);

    let mut watch = client(a, "ring-watch").await;
    subscribe(&mut watch, "ring/#").await;

    // Inject ONE message, retrying until the ring is actually wired (each bridge
    // must have its local subscription live) — then stop injecting.
    let mut pubc = client(a, "ring-pub").await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        pubc.publish(
            "ring/x",
            Bytes::from_static(b"go"),
            QoS::AtMostOnce,
            false,
            None,
            Properties::new(),
        )
        .await
        .unwrap();
        if ab.metrics().forwarded_out_count() > 0
            && bc.metrics().forwarded_out_count() > 0
            && ca.metrics().forwarded_out_count() > 0
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the ring never came up: ab={} bc={} ca={}",
            ab.metrics().forwarded_out_count(),
            bc.metrics().forwarded_out_count(),
            ca.metrics().forwarded_out_count()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // THE ASSERTION: the ring goes quiet. An uncut cycle would feed this drain
    // forever; reaching quiescence at all is the termination proof.
    let seen = drain_until_quiet(&mut watch, Duration::from_secs(2)).await;

    let dropped = ab.metrics().dropped_hop_limit_count()
        + bc.metrics().dropped_hop_limit_count()
        + ca.metrics().dropped_hop_limit_count();
    assert!(
        dropped > 0,
        "the ring stopped, but not because of the hop limit — no bridge counted a \
         hop-limit drop, so something else (or nothing) cut it: {} deliveries seen",
        seen.len()
    );

    // Bounded, not merely finite: with a limit of 3 and one injected message per
    // warm-up round, a runaway would be orders of magnitude larger than this.
    assert!(
        seen.len() < 200,
        "the ring amplified before it was cut: {} deliveries",
        seen.len()
    );

    ab.shutdown();
    bc.shutdown();
    ca.shutdown();
}

/// BR-032: hop count **increments** along a chain A→B→C→D.
///
/// The stamp was only ever asserted at hop 1 (one bridge) and as a pure unit test
/// of `set_hop_count`. If the counter were re-stamped rather than incremented at
/// each crossing, every single-bridge test would still pass and the ring test
/// above would never terminate — so the increment is the property that makes the
/// backstop work, and this pins it end to end.
#[tokio::test]
async fn hop_count_increments_along_a_chain() {
    // Four brokers named for their position in the chain, not a-b-c-d soup.
    let (first, second, third, last) = (
        start_broker().await,
        start_broker().await,
        start_broker().await,
        start_broker().await,
    );

    let one_way = |tag: &str, local: SocketAddr, up: SocketAddr| {
        Bridge::start(
            BridgeConfig::parse_toml(&format!(
                r#"
                [local]
                url = "{local}"
                client_id = "{tag}-local"
                [[upstreams]]
                name = "next"
                url = "{up}"
                client_id = "{tag}-up"
                [[upstreams.rules]]
                direction = "out"
                filter = "chain/#"
                "#,
            ))
            .unwrap(),
        )
    };
    let ab = one_way("ab", first, second);
    let bc = one_way("bc", second, third);
    let cd = one_way("cd", third, last);

    let mut end = client(last, "chain-sub").await;
    subscribe(&mut end, "chain/#").await;

    // Republish until the whole chain is wired; the far end tells us when.
    let mut pubc = client(first, "chain-pub").await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let arrived = loop {
        pubc.publish(
            "chain/x",
            Bytes::from_static(b"walk"),
            QoS::AtMostOnce,
            false,
            None,
            Properties::new(),
        )
        .await
        .unwrap();
        match tokio::time::timeout(Duration::from_millis(300), end.next_event()).await {
            Ok(Ok(Event::Publish(p))) => break p,
            _ => assert!(
                tokio::time::Instant::now() < deadline,
                "the message never traversed all three hops"
            ),
        }
    };

    assert_eq!(
        hop_count(&arrived).as_deref(),
        Some("3"),
        "three crossings must stamp 3, not a re-stamped 1"
    );

    ab.shutdown();
    bc.shutdown();
    cd.shutdown();
}

/// BR-030: a bidirectional pair delivers **exactly one** copy per side.
///
/// The `both` rule is the shape most likely to double-deliver: the same filter is
/// subscribed on both sides, so a naive implementation echoes the message back to
/// the broker it came from and the origin-side subscriber sees it twice.
#[tokio::test]
async fn a_bidirectional_pair_delivers_exactly_one_copy_per_side() {
    let (a, b) = (start_broker().await, start_broker().await);
    let bridge = ring_bridge("pair", a, b, "pair/#", 4);

    let mut a_sub = client(a, "pair-a-sub").await;
    subscribe(&mut a_sub, "pair/#").await;
    let mut b_sub = client(b, "pair-b-sub").await;
    subscribe(&mut b_sub, "pair/#").await;

    // Warm up on a topic the assertions ignore, so the retried warm-up publishes
    // cannot be mistaken for duplicates of the message under test.
    let mut a_pub = client(a, "pair-a-pub").await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        a_pub
            .publish(
                "pair/warmup",
                Bytes::from_static(b"w"),
                QoS::AtMostOnce,
                false,
                None,
                Properties::new(),
            )
            .await
            .unwrap();
        if bridge.metrics().forwarded_out_count() > 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the bridge never forwarded the warm-up"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = drain_until_quiet(&mut a_sub, Duration::from_millis(400)).await;
    let _ = drain_until_quiet(&mut b_sub, Duration::from_millis(400)).await;

    a_pub
        .publish(
            "pair/once",
            Bytes::from_static(b"ONCE"),
            QoS::AtMostOnce,
            false,
            None,
            Properties::new(),
        )
        .await
        .unwrap();

    let a_seen = drain_until_quiet(&mut a_sub, Duration::from_millis(600)).await;
    let b_seen = drain_until_quiet(&mut b_sub, Duration::from_millis(600)).await;
    let count = |v: &[Vec<u8>]| v.iter().filter(|p| p.as_slice() == b"ONCE").count();

    assert_eq!(
        count(&a_seen),
        1,
        "the origin-side subscriber must see the message once (its own broker's \
         delivery), not a second time echoed back across the bridge"
    );
    assert_eq!(count(&b_seen), 1, "the far side must see exactly one copy");

    bridge.shutdown();
}

/// BR-037: a **retained** value in a `both` rule does not ping-pong.
///
/// Retained deserves its own case: bridge subscriptions use `retain_handling: 0`,
/// so the bridge receives a retained replay on **every reconnect**, and a replay
/// carries `retain = 1`. A retained value crossing a bidirectional rule can
/// therefore be re-injected on each reconnect rather than only once, with the hop
/// counter as the only bound.
#[tokio::test]
async fn a_retained_value_does_not_ping_pong_across_a_both_rule() {
    let (a, b) = (start_broker().await, start_broker().await);
    let bridge = ring_bridge("ret", a, b, "ret/#", 3);

    let mut a_pub = client(a, "ret-pub").await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        a_pub
            .publish(
                "ret/v",
                Bytes::from_static(b"RETAINED"),
                QoS::AtMostOnce,
                true, // retain
                None,
                Properties::new(),
            )
            .await
            .unwrap();
        if bridge.metrics().forwarded_out_count() > 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the retained value never crossed the bridge"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // A late subscriber on the far side gets the retained value — it crossed and
    // landed retained, which is the behaviour a loop test must not accidentally
    // "prove" by the value never having arrived at all.
    let mut b_late = client(b, "ret-late").await;
    subscribe(&mut b_late, "ret/#").await;
    let got = tokio::time::timeout(Duration::from_secs(5), b_late.next_event()).await;
    assert!(
        matches!(got, Ok(Ok(Event::Publish(ref p))) if &p.payload[..] == b"RETAINED"),
        "the retained value must be served on the far side, got {got:?}"
    );

    // And it settles: no endless re-injection.
    let tail = drain_until_quiet(&mut b_late, Duration::from_secs(2)).await;
    assert!(
        tail.len() < 50,
        "the retained value re-amplified across the bridge: {} extra deliveries",
        tail.len()
    );

    bridge.shutdown();
}

/// BR-036: a remap ping-pong that PASSES config validation is stopped by No Local.
///
/// Validation rejects `both` + remap (#192), but the same cycle can be written as
/// two separate, individually legal one-way rules — `out ping/# → pong/` paired
/// with `in pong/# → ping/`. Config cannot see it. This asserts the runtime
/// defence that does: the bridge publishes to its local broker on the same client
/// that subscribes there, so the broker's No Local suppression breaks the cycle at
/// the source.
///
/// It is the complement to the ring test above: together they show which layer
/// catches which shape — No Local the single-broker echo, the hop counter the
/// multi-broker cycle it cannot see.
#[tokio::test]
async fn a_remap_ping_pong_within_one_bridge_is_stopped_by_no_local() {
    let (local, up) = (start_broker().await, start_broker().await);
    let cfg = BridgeConfig::parse_toml(&format!(
        r#"
        hop_count_limit = 4
        [local]
        url = "{local}"
        client_id = "pp-local"
        [[upstreams]]
        name = "partner"
        url = "{up}"
        client_id = "pp-up"
        [[upstreams.rules]]
        direction = "out"
        filter = "ping/#"
        remap = {{ strip_prefix = "ping/", prefix = "pong/" }}
        [[upstreams.rules]]
        direction = "in"
        filter = "pong/#"
        remap = {{ strip_prefix = "pong/", prefix = "ping/" }}
        "#,
    ))
    .unwrap();
    let bridge = Bridge::start(cfg);

    let mut watch = client(local, "pp-watch").await;
    subscribe(&mut watch, "ping/#").await;

    let mut pubc = client(local, "pp-pub").await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        pubc.publish(
            "ping/x",
            Bytes::from_static(b"PP"),
            QoS::AtMostOnce,
            false,
            None,
            Properties::new(),
        )
        .await
        .unwrap();
        if bridge.metrics().forwarded_out_count() > 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the out leg never forwarded"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let seen = drain_until_quiet(&mut watch, Duration::from_secs(2)).await;
    assert!(
        seen.len() < 100,
        "a two-rule remap ping-pong amplified: {} deliveries",
        seen.len()
    );

    bridge.shutdown();
}
