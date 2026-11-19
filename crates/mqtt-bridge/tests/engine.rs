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
use mqtt_bridge::spool::{Spool, SpooledMessage};
use mqtt_codec::packet::{Ack, ConnAck, Packet, Publish, SubAck};
use mqtt_codec::properties::{Properties, Property};
use mqtt_codec::{ProtocolVersion, QoS};
use mqtt_net::{FrameReader, FrameWriter};
use mqttd::Hub;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};

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
    serve_on(listener, hub_tx);
}

/// Accept connections on `listener` into an existing broker.
fn serve_on(listener: TcpListener, hub_tx: mqttd::hub::shards::HubTx) {
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(mqttd::conn::handle(stream, hub_tx.clone()));
        }
    });
}

/// A broker that is about to come up at `addr`, with a subscriber already on `filter`.
///
/// The subscriber connects through a side door (a second listener on the same broker)
/// BEFORE `addr` accepts anything, so the bridge, which keeps retrying `addr`, can never
/// connect and forward ahead of the subscriber. Returns the subscriber; the broker serves
/// `addr` from `listener` once this returns.
async fn broker_with_subscriber(listener: TcpListener, id: &str, filter: &str) -> MqttClient {
    let (hub, hub_tx) = Hub::new();
    tokio::spawn(hub.run());
    let side_door = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let side_addr = side_door.local_addr().unwrap();
    serve_on(side_door, hub_tx.clone());
    let mut sub = client(side_addr, id).await;
    subscribe_qos1(&mut sub, filter).await;
    serve_on(listener, hub_tx);
    sub
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
        session_expiry: 0,
        receive_maximum: None,
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

/// How long a bounded poll in this file will wait before failing. Generous, because the
/// number is not the test's subject: a slow machine must not fail, and a broken bridge must
/// not pass. That is the whole trade a bounded poll makes over a fixed sleep — it costs
/// nothing when the state arrives in 20 ms and it still fails, by name, when it never does.
const POLL_LIMIT: Duration = Duration::from_secs(20);

/// Poll the bridge's OWN metric surface until `pred` accepts the render.
///
/// `fss_bridge_connected`, `fss_bridge_spool_depth` and `fss_bridge_spool_capacity` make
/// "the local side is up" and "five messages are spooled" observable facts, so this file
/// no longer has to guess at a duration for them. On failure the render at the deadline is
/// printed: the point of a bounded poll over a sleep is that the failure says what never
/// happened, and the gauges are exactly that answer.
async fn await_metrics(bridge: &Bridge, what: &str, pred: impl Fn(&str) -> bool) {
    let deadline = tokio::time::Instant::now() + POLL_LIMIT;
    loop {
        let render = bridge.metrics().render();
        if pred(&render) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what} (waited {POLL_LIMIT:?}). The bridge's own metrics at the deadline:\n{render}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Wait until `side` reports `fss_bridge_connected 1`.
async fn await_connected(bridge: &Bridge, side: &str) {
    let needle = format!("fss_bridge_connected{{side=\"{side}\"}} 1");
    await_metrics(
        bridge,
        &format!("bridge side `{side}` never reported connected"),
        |r| r.contains(&needle),
    )
    .await;
}

fn spool_depth(render: &str, side: &str) -> Option<u64> {
    let prefix = format!("fss_bridge_spool_depth{{side=\"{side}\"}} ");
    render
        .lines()
        .find_map(|l| l.strip_prefix(prefix.as_str())?.trim().parse().ok())
}

/// Wait until `side`'s spool holds at least `n` messages.
async fn await_spool_depth(bridge: &Bridge, side: &str, n: u64) {
    await_metrics(
        bridge,
        &format!("side `{side}`'s spool never reached {n} message(s)"),
        |r| spool_depth(r, side).is_some_and(|d| d >= n),
    )
    .await;
}

/// Read everything already queued for `c` and throw it away, so a later collection window
/// sees only what was published after this returns. Bounded by `POLL_LIMIT` overall and by
/// a quiet window per read — "the stream has gone quiet" is the observable, and it is one
/// the receiving client can answer directly.
async fn drain(c: &mut MqttClient) {
    let deadline = tokio::time::Instant::now() + POLL_LIMIT;
    while (tokio::time::timeout(Duration::from_millis(200), c.next_event()).await).is_ok() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the subscriber never went quiet, so no collection window can be trusted"
        );
    }
}

/// Wait until `topic` is in the broker's retained store at `addr`.
///
/// "Stored" is observable rather than a duration to guess at: a SUBSCRIBE gets the value back
/// as a retained replay with `retain=1`, and each fresh SUBSCRIBE triggers a fresh replay — so
/// re-subscribing in a loop polls the store itself.
async fn await_retained(addr: SocketAddr, filter: &str, topic: &str) {
    let mut probe = client(addr, "retained-probe").await;
    let deadline = tokio::time::Instant::now() + POLL_LIMIT;
    loop {
        probe.subscribe(1, filter, QoS::AtMostOnce).await.unwrap();
        let mut replayed = false;
        while let Ok(Ok(ev)) =
            tokio::time::timeout(Duration::from_millis(150), probe.next_event()).await
        {
            if let Event::Publish(p) = ev {
                if p.retain && p.topic == topic {
                    replayed = true;
                    break;
                }
            }
        }
        if replayed {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the broker never replayed {topic} as retained, so there was nothing in its store"
        );
    }
    probe.disconnect().await;
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

    await_retained(local, "sensors/#", "sensors/room/temp").await;

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

    // BOTH instances must be forwarding before the batch, and the 1.5 s sleep this replaces
    // only hoped for that. It mattered: if just one instance's shared subscription had come
    // up, "no duplicate forward" would hold vacuously — a single forwarder cannot duplicate,
    // so the test would pass while proving nothing. Publish warm-up messages until each
    // instance's OWN forward counter is non-zero. Round-robin over the `$share` group means
    // two accepted messages suffice, and the observable is per-instance, so this is strictly
    // stronger than any duration.
    let mut local_pub = client(local, "ha-local-pub").await;
    let deadline = tokio::time::Instant::now() + POLL_LIMIT;
    let mut warm = 0u32;
    while b1.metrics().forwarded_out_count() == 0 || b2.metrics().forwarded_out_count() == 0 {
        local_pub
            .publish(
                "telemetry/warmup",
                Bytes::from(format!("w{warm}")),
                QoS::AtMostOnce,
                false,
                None,
                Properties::new(),
            )
            .await
            .unwrap();
        warm += 1;
        assert!(
            tokio::time::Instant::now() < deadline,
            "after {warm} warm-up publishes only one instance had ever forwarded \
             (a={}, b={}): the HA pair never both joined the shared subscription, and a \
             duplicate-forward test with a single forwarder cannot fail",
            b1.metrics().forwarded_out_count(),
            b2.metrics().forwarded_out_count()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The warm-up traffic must not land in the collection window below.
    drain(&mut up_sub).await;

    // Publish a batch of uniquely-payloaded messages on the local side.
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

    // BOTH instances must be forwarding before the batch. If only one were live, every
    // message would still arrive exactly once — via the surviving instance — and the test
    // would pass while the partitioning it exists to check had never been exercised. So warm
    // up over varying topics (ownership is by topic, so one topic only wakes one owner) until
    // each instance's OWN inbound forward counter is non-zero.
    let mut up_pub = client(upstream, "part-up-pub").await;
    let deadline = tokio::time::Instant::now() + POLL_LIMIT;
    let mut warm = 0u32;
    while b1.metrics().forwarded_in_count() == 0 || b2.metrics().forwarded_in_count() == 0 {
        up_pub
            .publish(
                &format!("cmd/warmup/{warm}"),
                Bytes::from(format!("w{warm}")),
                QoS::AtMostOnce,
                false,
                None,
                Properties::new(),
            )
            .await
            .unwrap();
        warm += 1;
        assert!(
            tokio::time::Instant::now() < deadline,
            "after {warm} warm-up publishes across distinct topics only one instance had ever \
             forwarded inbound (a={}, b={}): both partitions were never live, so an \
             exactly-once result would say nothing about partitioning",
            b1.metrics().forwarded_in_count(),
            b2.metrics().forwarded_in_count()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The warm-up traffic must not land in the collection window below.
    drain(&mut local_sub).await;

    // Publish on many DIFFERENT topics so ownership spreads across both instances.
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
    // Count only the batch's payloads: a warm-up message that slipped past the drain would
    // otherwise inflate this, while the exactly-once assertion above still holds for it
    // (partitioning owes exactly-once for every topic, warm-ups included).
    let batch = seen.keys().filter(|p| p.starts_with(b"c")).count();
    assert_eq!(
        batch, 30,
        "every one of the 30 messages must arrive exactly once (got {batch} distinct)"
    );

    b1.shutdown();
    b2.shutdown();
}

/// ADR 0060 T1/T2 + T5, on the path that still spools (Amendment 1 §7): messages from an
/// upstream while the local link is down.
#[tokio::test]
async fn a_full_spool_must_not_shed_a_message_the_bridge_already_accepted() {
    // A spooled message was already acknowledged to the upstream, so the upstream has
    // dropped its copy. Shedding it to make room for a NEWER message loses a message the
    // upstream believes was delivered — the newer one must be refused instead, and held
    // unacknowledged. Before T5, drop-oldest quietly evicted the older, already-acked message.
    let upstream = start_broker().await;
    let local_addr = reserve_addr().await;

    // A QoS-1 rule (so the spool refuses at the cap) with room for exactly ONE message.
    let cfg = BridgeConfig::parse_toml(&format!(
        r#"
        share_group = ""
        [local]
        url = "{local_addr}"
        [spool]
        max_messages = 1
        allow_ephemeral_spool = true
        [[upstreams]]
        name = "partner"
        url = "{upstream}"
        [[upstreams.rules]]
        direction = "in"
        filter = "t/#"
        qos = 1
        "#,
    ))
    .unwrap();
    let bridge = Bridge::start(cfg);
    await_connected(&bridge, "partner").await;

    // FIRST is accepted into the one-slot spool; SECOND arrives with the spool full. The
    // ORDER is the whole subject, so "FIRST is in the spool" is asserted rather than waited
    // out: the spool-depth gauge answers it directly.
    publish_acked(upstream, "shed-pub-1", "t/x", "FIRST", 1).await;
    await_spool_depth(&bridge, "local", 1).await;
    publish_acked(upstream, "shed-pub-2", "t/x", "SECOND", 1).await;
    // SETTLE(bridge-spool-refusal): SECOND's *refusal* is deliberately not counted anywhere —
    // spool.rs documents that an `Overflow::Refuse` rejection is not a loss and so must not
    // land in `fss_bridge_dropped_total`. That leaves no positive observable for "SECOND has
    // been offered to a full spool", so the depth gauge is sampled over a bounded window
    // instead. Every sample asserts, so a spool that sheds FIRST or accepts both fails here
    // immediately; on a slow machine the window can only make the check vacuous, and the
    // delivery assertion below still pins the ordering (issue #260 asks for a counter).
    for _ in 0..50 {
        let depth = spool_depth(&bridge.metrics().render(), "local");
        assert_eq!(
            depth,
            Some(1),
            "the one-slot spool moved off its cap after SECOND arrived: it either shed the \
             already-acked FIRST or accepted both"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The local cluster comes up: the spool replays FIRST, then SECOND follows from memory.
    let listener = TcpListener::bind(local_addr).await.unwrap();
    let mut local_sub = broker_with_subscriber(listener, "shed-local-sub", "t/#").await;
    let got = collect_n(&mut local_sub, "", 2).await;
    assert_eq!(
        got.first().map(String::as_str),
        Some("FIRST1"),
        "the spool shed a message it had already accepted (and acked) to make room for a newer \
         one; the newer message must be refused instead"
    );
    assert_eq!(
        distinct_in_order(&got),
        ["FIRST1", "SECOND1"],
        "the refused message must not be lost"
    );

    bridge.shutdown();
}

/// ADR 0060 Amendment 1 §1: messages for an unreachable upstream wait in the bridge's local
/// session, not in the bridge. While the upstream is down the link stays away from the local
/// cluster, and everything published meanwhile crosses, in order, once it is back.
#[tokio::test]
async fn messages_published_while_an_upstream_is_down_wait_in_the_local_session() {
    let local = start_broker().await;
    let upstream_addr = reserve_addr().await;
    let bridge = Bridge::start(out_rule_config(local, upstream_addr, ""));
    await_local_session(&bridge, "local").await;

    publish_acked(local, "wait-pub", "t/x", "s", 5).await;
    // The bridge holds none of it: it is not even connected to the local cluster.
    assert!(
        !render_connected(&bridge, "local"),
        "the local side must stay disconnected while its upstream is down"
    );

    let listener = TcpListener::bind(upstream_addr).await.unwrap();
    let mut up_sub = broker_with_subscriber(listener, "wait-up-sub", "t/#").await;
    let got = collect_n(&mut up_sub, "s", 5).await;
    assert_eq!(distinct_in_order(&got), ["s1", "s2", "s3", "s4", "s5"]);

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

    // No pre-wait for the sides to connect: the republish loop below IS the readiness poll —
    // it keeps publishing until the drop registers and fails with a name if it never does, so
    // a fixed 1.5 s head start was pure duplication of a condition already being polled.

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

/// Issue #1100, the real mechanism: a message the router forwards toward a side **while
/// that side is connecting** could be stranded in the side's spool for as long as the
/// connection then stays up. The supervisor connects, drains the spool (`replay_spool`),
/// and only then sets the side's `connected` flag; the router reads that flag and, on
/// `false`, pushes into the same spool. A router that read `false` before the flag was set
/// but reached the spool after the drain leaves its message there, and nothing drains a
/// spool except the next reconnect. The source was already acknowledged (the spool is
/// "durably accepted", ADR 0060 T2), so the source will never retry: the message is
/// delivered only if the side happens to drop and reconnect, and lost if the bridge is
/// replaced or its spool discarded.
///
/// This is what `scripts/migrate/dual-run-smoke.sh` step 3 caught in CI. The bridge's
/// upstream side subscribed and received the incumbent's retained value in the same
/// millisecond its local side connected to mqttd: the bridge logged it "forwarded across
/// the boundary", mqttd never received a PUBLISH on that topic, and the bridge's own
/// `fss_bridge_spool_depth{side="local"}` read 1 with `fss_bridge_connected{side="local"}
/// 1` thirty seconds later. mqttd is not involved: the issue's first reading (a retained
/// write lost between `/readyz` and the first ownership commit) does not reproduce
/// against mqttd alone, at any offset from ready, on either store.
///
/// The CI race needs one message to land in a window a few microseconds to a few
/// milliseconds wide (the drain holds the spool's lock across its fsync, and a router
/// push that read `false` waits behind it). This test makes the window certain to be hit
/// rather than lucky: the upstream streams messages the whole time the local side is
/// down and coming up, so the router is pushing into the local spool at the instant the
/// supervisor drains it. A multi-thread runtime is required — on a current-thread
/// runtime the supervisor's drain-then-flag sequence has no await point and the router
/// cannot interleave with it at all.
///
/// The assertion is the invariant a fix must restore, whatever its shape: once the side
/// is connected and the stream has stopped, its spool empties. A message accepted for a
/// connected side is either sent or still sent later; it never waits for a reconnect
/// that may not come.
///
/// The first fix put the live-or-spool decision and the connect handoff under one lock. Since
/// ADR 0060 Amendment 1 a link spools only while its local connection is down, and once it is
/// up it replays the spool before anything newer, deleting each message on its PUBACK, so a
/// message can no longer slip into a spool that nothing will drain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_message_spooled_while_its_side_connects_is_not_stranded_in_the_spool() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let upstream = start_broker().await;
    // Reserve the local address and free it, so the local side starts DOWN and its
    // connects are refused until the test serves a broker there.
    let local_addr = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let spool_dir = tempfile::tempdir().unwrap();
    // The smoke's shape: an inbound QoS 1 rule over a disk spool, sharing off.
    let cfg = BridgeConfig::parse_toml(&format!(
        r#"
        share_group = ""
        [local]
        url = "{local_addr}"
        [spool]
        dir = "{dir}"
        max_messages = 100000
        [[upstreams]]
        name = "incumbent"
        url = "{upstream}"
        [[upstreams.rules]]
        direction = "in"
        filter = "t/#"
        qos = 1
        "#,
        dir = spool_dir.path().display(),
    ))
    .unwrap();
    let bridge = Bridge::start(cfg);
    await_connected(&bridge, "incumbent").await;

    // The stream: one message every millisecond until told to stop (or a hard bound), so
    // the router is forwarding toward the local side at every instant of its connect.
    let stop = Arc::new(AtomicBool::new(false));
    let publisher = {
        let stop = stop.clone();
        let mut up_pub = client(upstream, "stream-pub").await;
        tokio::spawn(async move {
            let mut n = 0u64;
            while !stop.load(Ordering::SeqCst) && n < 20_000 {
                up_pub
                    .publish(
                        &format!("t/{n}"),
                        Bytes::from(n.to_string()),
                        QoS::AtMostOnce,
                        false,
                        None,
                        Properties::new(),
                    )
                    .await
                    .unwrap();
                n += 1;
                // SETTLE(bridge-connect-stream): the pause is the stream's RATE, not a wait
                // for state — it spreads the messages across the local side's connect so one
                // is in flight in the router whenever the supervisor drains the spool.
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            n
        })
    };
    // Some of it is spooled while the local side is down — the ordinary path, replayed at
    // connect — and the stream is still running when the side comes up.
    await_spool_depth(&bridge, "local", 20).await;
    serve_broker(TcpListener::bind(local_addr).await.unwrap());
    await_connected(&bridge, "local").await;
    // Keep streaming past the connect — until a hundred more messages have crossed inbound
    // — then stop: everything after the flag goes live.
    let crossed_at_connect = bridge.metrics().forwarded_in_count();
    await_metrics(
        &bridge,
        "the stream never kept crossing after the local side connected",
        |_| bridge.metrics().forwarded_in_count() >= crossed_at_connect + 100,
    )
    .await;
    stop.store(true, Ordering::SeqCst);
    let published = publisher.await.unwrap();

    // The local side is connected and nothing more is arriving: its spool must empty.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let render = bridge.metrics().render();
        let depth = spool_depth(&render, "local");
        if depth == Some(0) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{depth:?} message(s) of the {published} streamed sit in the LOCAL spool while the \
             local side is connected: the router spooled them after the supervisor's \
             connect-time drain and before it set `connected`, and nothing drains a spool \
             but the next reconnect (issue #1100). They were already acknowledged to the \
             source, which will not resend them. The bridge's metrics:\n{render}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        render_connected(&bridge, "local"),
        "the local side must still be connected: a reconnect would have drained the spool \
         and hidden the defect"
    );

    bridge.shutdown();
}

/// Whether `side` currently reports `fss_bridge_connected 1`.
fn render_connected(bridge: &Bridge, side: &str) -> bool {
    bridge
        .metrics()
        .render()
        .contains(&format!("fss_bridge_connected{{side=\"{side}\"}} 1"))
}

/// A free address: bound, then released, so a broker can come up on it later.
async fn reserve_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
}

/// One upstream with an `out` `QoS` 1 rule on `t/#`, ephemeral spool; `extra` lands in the
/// upstream table.
fn out_rule_config(local: SocketAddr, upstream: SocketAddr, extra: &str) -> BridgeConfig {
    BridgeConfig::parse_toml(&format!(
        r#"
        share_group = ""
        [local]
        url = "{local}"
        [spool]
        allow_ephemeral_spool = true
        [[upstreams]]
        name = "up"
        url = "{upstream}"
        {extra}
        [[upstreams.rules]]
        direction = "out"
        filter = "t/#"
        qos = 1
        "#
    ))
    .unwrap()
}

/// The `fss_bridge_reconnects_total` reading for `side`.
fn reconnects(render: &str, side: &str) -> u64 {
    metric(
        render,
        &format!("fss_bridge_reconnects_total{{side=\"{side}\"}}"),
    )
    .unwrap_or(0)
}

/// A series' value in a render, by its full name with labels.
fn metric(render: &str, series: &str) -> Option<u64> {
    let prefix = format!("{series} ");
    render
        .lines()
        .find_map(|l| l.strip_prefix(prefix.as_str())?.trim().parse().ok())
}

/// Wait until the link has set up (or found) its local session: that first, short connect
/// counts as the local side's first connection (ADR 0060 Amendment 1 §1).
async fn await_local_session(bridge: &Bridge, side: &str) {
    await_metrics(
        bridge,
        &format!("the bridge never set up the local session of `{side}`"),
        |r| reconnects(r, side) >= 1,
    )
    .await;
}

/// Subscribe at `QoS` 1 and wait for the SUBACK.
async fn subscribe_qos1(c: &mut MqttClient, filter: &str) {
    c.subscribe(1, filter, QoS::AtLeastOnce).await.unwrap();
    loop {
        if let Event::SubAck { .. } = c.next_event().await.unwrap() {
            return;
        }
    }
}

/// Publish `n` `QoS` 1 messages `<prefix>1..=<prefix>n` on `topic` and wait for the broker's
/// PUBACKs, so they are surely in it.
async fn publish_acked(addr: SocketAddr, id: &str, topic: &str, prefix: &str, n: u16) {
    let mut p = client(addr, id).await;
    for i in 1..=n {
        p.publish(
            topic,
            Bytes::from(format!("{prefix}{i}")),
            QoS::AtLeastOnce,
            false,
            Some(i),
            Properties::new(),
        )
        .await
        .unwrap();
    }
    let mut acks = 0;
    while acks < n {
        if let Event::PubAck(_) = p.next_event().await.unwrap() {
            acks += 1;
        }
    }
}

/// Receive until `n` distinct payloads starting with `prefix` have arrived (acknowledging
/// each), or fail after `POLL_LIMIT`. Returns every matching payload in arrival order,
/// duplicates included — at-least-once allows them.
async fn collect_n(c: &mut MqttClient, prefix: &str, n: usize) -> Vec<String> {
    let deadline = tokio::time::Instant::now() + POLL_LIMIT;
    let mut got: Vec<String> = Vec::new();
    while distinct_in_order(&got).len() < n {
        if let Ok(Ok(Event::Publish(p))) =
            tokio::time::timeout(Duration::from_millis(300), c.next_event()).await
        {
            if let Some(id) = p.pkid {
                c.puback(id).await.unwrap();
            }
            let payload = String::from_utf8_lossy(&p.payload).into_owned();
            if payload.starts_with(prefix) {
                got.push(payload);
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "only {got:?} of {n} messages starting with {prefix:?} arrived"
        );
    }
    got
}

/// The payloads in first-arrival order, each once.
fn distinct_in_order(got: &[String]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    got.iter().filter(|g| seen.insert(*g)).cloned().collect()
}

type FakeReader = FrameReader<ReadHalf<TcpStream>>;
type FakeWriter = FrameWriter<WriteHalf<TcpStream>>;

/// Play a broker by hand: accept one connection on `listener` and answer its CONNECT.
async fn accept_fake(listener: &TcpListener) -> (FakeReader, FakeWriter) {
    let (stream, _) = listener.accept().await.unwrap();
    let (rh, wh) = tokio::io::split(stream);
    let mut r = FrameReader::new(rh, ProtocolVersion::V5);
    let mut w = FrameWriter::new(wh, ProtocolVersion::V5);
    match r.next_packet().await {
        Ok(Some(Packet::Connect(_))) => {}
        other => panic!("expected CONNECT, got {other:?}"),
    }
    w.send(&Packet::ConnAck(ConnAck {
        session_present: false,
        code: 0,
        properties: Properties::new(),
    }))
    .await
    .unwrap();
    (r, w)
}

/// The next PUBLISH the bridge sends a fake broker. SUBSCRIBEs are granted; everything else
/// (PINGREQ among it) goes unanswered.
async fn fake_next_publish(r: &mut FakeReader, w: &mut FakeWriter) -> Publish {
    loop {
        match r.next_packet().await {
            Ok(Some(Packet::Publish(p))) => return p,
            Ok(Some(Packet::Subscribe(s))) => {
                w.send(&Packet::SubAck(SubAck {
                    pkid: s.pkid,
                    return_codes: vec![1; s.filters.len()],
                    properties: Properties::new(),
                }))
                .await
                .unwrap();
            }
            Ok(Some(_)) => {}
            other => panic!("the fake broker's connection ended: {other:?}"),
        }
    }
}

/// #1103 finding 1: a `QoS` 1 forward written to the upstream but not yet acknowledged when the
/// link drops is not lost. The bridge acknowledges the local cluster only on the upstream's
/// PUBACK (Amendment 1 §1), so the local session resends it on the next connect.
#[tokio::test]
async fn a_forward_on_the_wire_when_the_upstream_drops_is_delivered_after_the_reconnect() {
    let local = start_broker().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let up_addr = listener.local_addr().unwrap();
    let bridge = Bridge::start(out_rule_config(local, up_addr, ""));

    // The first upstream connection is a fake that takes the forwards and never answers them.
    let (mut r, mut w) = accept_fake(&listener).await;
    await_connected(&bridge, "local").await;
    publish_acked(local, "wire-pub", "t/x", "f", 5).await;
    for _ in 0..5 {
        fake_next_publish(&mut r, &mut w).await;
    }

    // A real upstream takes over the address; then the fake link drops with all 5 unanswered.
    let mut up_sub = broker_with_subscriber(listener, "wire-up-sub", "t/#").await;
    drop(r);
    drop(w);
    let got = collect_n(&mut up_sub, "f", 5).await;
    assert_eq!(distinct_in_order(&got), ["f1", "f2", "f3", "f4", "f5"]);

    bridge.shutdown();
}

/// The local session outlives a bridge restart (Amendment 1 §1): the bridge now sends a
/// Session Expiry Interval, so what is published while it is down waits for it. Before, MQTT 5
/// read the missing interval as 0 and every message published during a restart was lost.
#[tokio::test]
async fn messages_published_while_the_bridge_restarts_cross_after_it() {
    let local = start_broker().await;
    let upstream = start_broker().await;
    let mut up_sub = client(upstream, "restart-up-sub").await;
    subscribe_qos1(&mut up_sub, "t/#").await;

    let bridge = Bridge::start(out_rule_config(local, upstream, ""));
    await_connected(&bridge, "local").await;
    publish_acked(local, "restart-warm", "t/x", "w", 3).await;
    collect_n(&mut up_sub, "w", 3).await;

    bridge.shutdown(); // a restart: both connections close
                       // SETTLE(bridge-restart-gap): the gap is the subject — the broker must see the bridge's
                       // local session disconnect before the publishes, so they land in the stored session.
    tokio::time::sleep(Duration::from_millis(300)).await;
    publish_acked(local, "restart-gap", "t/x", "g", 5).await;

    let bridge = Bridge::start(out_rule_config(local, upstream, ""));
    let got = collect_n(&mut up_sub, "g", 5).await;
    assert_eq!(distinct_in_order(&got), ["g1", "g2", "g3", "g4", "g5"]);

    bridge.shutdown();
}

/// Amendment 1 §4: an upstream that vanishes without a reset is noticed by the keepalive, and
/// what was on the wire to it is resent. Before, PINGRESPs were never checked, so such a link
/// counted as up until TCP gave up, about 15 minutes with Linux defaults.
#[tokio::test]
async fn a_half_open_upstream_is_caught_by_the_keepalive() {
    let local = start_broker().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let up_addr = listener.local_addr().unwrap();
    let bridge = Bridge::start(out_rule_config(local, up_addr, "keep_alive_secs = 2"));

    // The fake answers the CONNECT, takes three forwards, then goes silent with its socket
    // still open: no PUBACK, no PINGRESP, no close.
    let (mut r, mut w) = accept_fake(&listener).await;
    await_connected(&bridge, "local").await;
    publish_acked(local, "half-open-pub", "t/x", "h", 3).await;
    for _ in 0..3 {
        fake_next_publish(&mut r, &mut w).await;
    }
    let silent_since = tokio::time::Instant::now();

    let mut up_sub = broker_with_subscriber(listener, "half-open-up-sub", "t/#").await;
    let got = collect_n(&mut up_sub, "h", 3).await;
    assert_eq!(distinct_in_order(&got), ["h1", "h2", "h3"]);
    assert!(
        silent_since.elapsed() < Duration::from_secs(15),
        "a 2-second keepalive must notice the silent link within seconds, took {:?}",
        silent_since.elapsed()
    );

    drop((r, w));
    bridge.shutdown();
}

/// Amendment 1 §2: each upstream has its own local session, so one that is down does not
/// stall another; its messages wait in its own session.
#[tokio::test]
async fn an_unreachable_upstream_does_not_stall_another() {
    let local = start_broker().await;
    let b = start_broker().await;
    let mut b_sub = client(b, "iso-b-sub").await;
    subscribe_qos1(&mut b_sub, "t/#").await;
    let a_addr = reserve_addr().await;
    let cfg = BridgeConfig::parse_toml(&format!(
        r#"
        share_group = ""
        [local]
        url = "{local}"
        [[upstreams]]
        name = "a"
        url = "{a_addr}"
        [[upstreams.rules]]
        direction = "out"
        filter = "t/#"
        qos = 1
        [[upstreams]]
        name = "b"
        url = "{b}"
        [[upstreams.rules]]
        direction = "out"
        filter = "t/#"
        qos = 1
        "#
    ))
    .unwrap();
    let bridge = Bridge::start(cfg);
    await_local_session(&bridge, "local:a").await;
    await_connected(&bridge, "local:b").await;

    publish_acked(local, "iso-pub", "t/x", "i", 3).await;
    let got = collect_n(&mut b_sub, "i", 3).await;
    assert_eq!(
        distinct_in_order(&got),
        ["i1", "i2", "i3"],
        "b must not wait for a"
    );
    assert!(!render_connected(&bridge, "local:a"));

    let listener = TcpListener::bind(a_addr).await.unwrap();
    let mut a_sub = broker_with_subscriber(listener, "iso-a-sub", "t/#").await;
    let got = collect_n(&mut a_sub, "i", 3).await;
    assert_eq!(
        distinct_in_order(&got),
        ["i1", "i2", "i3"],
        "a's messages waited in its session"
    );

    bridge.shutdown();
}

/// Amendment 1 §5: a permanent refusal (0x87, not authorized) is acknowledged at the source,
/// audited and counted, so it cannot block the link; a temporary one (0x97, quota exceeded)
/// is published again. Both end acknowledged at the local cluster: when the link reconnects,
/// its session gives back nothing.
#[tokio::test]
async fn refusals_are_dropped_or_retried_and_both_are_acknowledged_at_the_source() {
    let local = start_broker().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let up_addr = listener.local_addr().unwrap();
    let bridge = Bridge::start(out_rule_config(local, up_addr, ""));
    let (mut r, mut w) = accept_fake(&listener).await;
    await_connected(&bridge, "local").await;

    publish_acked(local, "refuse-pub-1", "t/denied", "d", 1).await;
    publish_acked(local, "refuse-pub-2", "t/busy", "b", 1).await;
    publish_acked(local, "refuse-pub-3", "t/ok", "o", 1).await;
    let mut busy_seen = 0;
    let mut answered = 0;
    while answered < 4 {
        let p = fake_next_publish(&mut r, &mut w).await;
        let reason = match p.topic.as_str() {
            "t/denied" => 0x87,
            "t/busy" => {
                busy_seen += 1;
                if busy_seen == 1 {
                    0x97
                } else {
                    0
                }
            }
            _ => 0,
        };
        w.send(&Packet::PubAck(Ack {
            pkid: p.pkid.unwrap(),
            reason,
            properties: Properties::new(),
        }))
        .await
        .unwrap();
        answered += 1;
    }
    assert_eq!(
        busy_seen, 2,
        "the temporarily refused message is published again"
    );
    await_metrics(&bridge, "the refusal was never counted", |r| {
        metric(r, r#"fss_bridge_dropped_total{reason="refused",side="up"}"#) == Some(1)
    })
    .await;

    // Everything was acknowledged at the local cluster. The upstream drops, so the link leaves
    // the local cluster (acknowledgements first, then the disconnect, on the one connection);
    // when the upstream is back the link resumes the session, which has nothing to resend.
    drop((r, w));
    await_metrics(&bridge, "the link never left the local cluster", |r| {
        metric(r, r#"fss_bridge_connected{side="local"}"#) == Some(0)
    })
    .await;
    let (mut r2, mut w2) = accept_fake(&listener).await;
    await_connected(&bridge, "local").await;
    let leftover =
        tokio::time::timeout(Duration::from_secs(2), fake_next_publish(&mut r2, &mut w2)).await;
    assert!(
        leftover.is_err(),
        "a message came back from the local session, so it was never acknowledged: {leftover:?}"
    );

    bridge.shutdown();
}

/// Amendment 1 §7: messages from an upstream that are on their way to the local cluster when
/// the local link drops go to the spool, and replay once it is back. Before, they were written,
/// acknowledged to the upstream at dispatch, and lost.
#[tokio::test]
async fn inbound_messages_in_flight_when_the_local_link_drops_are_spooled_and_replayed() {
    let upstream = start_broker().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    let cfg = BridgeConfig::parse_toml(&format!(
        r#"
        share_group = ""
        [local]
        url = "{local_addr}"
        [spool]
        allow_ephemeral_spool = true
        [[upstreams]]
        name = "partner"
        url = "{upstream}"
        [[upstreams.rules]]
        direction = "in"
        filter = "t/#"
        qos = 1
        "#
    ))
    .unwrap();
    let bridge = Bridge::start(cfg);
    // The first local connection is a fake that takes the forwards and never answers them.
    let (mut r, mut w) = accept_fake(&listener).await;
    await_connected(&bridge, "partner").await;
    publish_acked(upstream, "in-wire-pub", "t/x", "n", 3).await;
    for _ in 0..3 {
        fake_next_publish(&mut r, &mut w).await;
    }

    let mut local_sub = broker_with_subscriber(listener, "in-wire-sub", "t/#").await;
    drop(r);
    drop(w);
    let got = collect_n(&mut local_sub, "n", 3).await;
    assert_eq!(distinct_in_order(&got), ["n1", "n2", "n3"]);

    bridge.shutdown();
}

/// #1103 / Amendment 1 §7: a replay deletes a spooled message only once the local cluster
/// acknowledges it. A bridge that stops in the middle of a replay loses nothing; before, the
/// drain removed everything from disk first.
#[tokio::test]
async fn a_replay_cut_short_by_a_restart_loses_nothing() {
    let upstream = start_broker().await;
    let local_addr = reserve_addr().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = || {
        BridgeConfig::parse_toml(&format!(
            r#"
            share_group = ""
            [local]
            url = "{local_addr}"
            [spool]
            dir = "{dir}"
            [[upstreams]]
            name = "partner"
            url = "{upstream}"
            [[upstreams.rules]]
            direction = "in"
            filter = "t/#"
            qos = 1
            "#,
            dir = dir.path().display(),
        ))
        .unwrap()
    };
    let bridge = Bridge::start(cfg());
    await_connected(&bridge, "partner").await;
    publish_acked(upstream, "cut-pub", "t/x", "r", 3).await;
    await_spool_depth(&bridge, "local", 3).await;

    // The local cluster comes back as a fake that takes the replay and never acknowledges it;
    // then the bridge stops.
    let listener = TcpListener::bind(local_addr).await.unwrap();
    let (mut r, mut w) = accept_fake(&listener).await;
    for _ in 0..3 {
        fake_next_publish(&mut r, &mut w).await;
    }
    bridge.shutdown();
    drop((r, w));

    let mut local_sub = broker_with_subscriber(listener, "cut-sub", "t/#").await;
    let bridge = Bridge::start(cfg());
    let got = collect_n(&mut local_sub, "r", 3).await;
    assert_eq!(distinct_in_order(&got), ["r1", "r2", "r3"]);

    bridge.shutdown();
}

/// Amendment 1 §8: spools an older bridge left behind are sent, not dropped — its spool for an
/// upstream to that upstream, its shared spool for the local cluster to the local cluster.
#[tokio::test]
async fn spools_an_older_bridge_left_behind_are_drained() {
    let dir = tempfile::tempdir().unwrap();
    let message = |topic: &str, payload: &str| SpooledMessage {
        topic: topic.into(),
        payload: payload.as_bytes().to_vec(),
        qos: 1,
        retain: false,
        user_properties: vec![("fss-bridge-hop-count".into(), "1".into())],
    };
    {
        let out = Spool::on_disk(&dir.path().join("spool-upstream-0.redb"), 100).unwrap();
        out.push(&message("o/legacy", "L-OUT")).unwrap();
        let shared = Spool::on_disk(&dir.path().join("spool-local.redb"), 100).unwrap();
        shared.push(&message("i/legacy", "L-IN")).unwrap();
    }

    let local = start_broker().await;
    let mut local_sub = client(local, "legacy-local-sub").await;
    subscribe_qos1(&mut local_sub, "i/#").await;
    let upstream = start_broker().await;
    let mut up_sub = client(upstream, "legacy-up-sub").await;
    subscribe_qos1(&mut up_sub, "o/#").await;

    let cfg = BridgeConfig::parse_toml(&format!(
        r#"
        share_group = ""
        [local]
        url = "{local}"
        [spool]
        dir = "{dir}"
        [[upstreams]]
        name = "partner"
        url = "{upstream}"
        [[upstreams.rules]]
        direction = "out"
        filter = "o/#"
        qos = 1
        [[upstreams.rules]]
        direction = "in"
        filter = "i/#"
        qos = 1
        "#,
        dir = dir.path().display(),
    ))
    .unwrap();
    let bridge = Bridge::start(cfg);
    collect_n(&mut up_sub, "L-OUT", 1).await;
    collect_n(&mut local_sub, "L-IN", 1).await;
    // The upstream's old spool is empty once acknowledged, and the link went on to connect
    // its local session.
    await_connected(&bridge, "local").await;

    bridge.shutdown();
}

/// Amendment 1 §1: a reconnect that finds the local session gone — here it expired while the
/// upstream was down — is counted and audited, not silent.
#[tokio::test]
async fn a_local_session_found_gone_on_reconnect_is_counted() {
    let local = start_broker().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let up_addr = listener.local_addr().unwrap();
    let cfg = BridgeConfig::parse_toml(&format!(
        r#"
        share_group = ""
        [local]
        url = "{local}"
        session_expiry_secs = 1
        [spool]
        allow_ephemeral_spool = true
        [[upstreams]]
        name = "up"
        url = "{up_addr}"
        [[upstreams.rules]]
        direction = "out"
        filter = "t/#"
        qos = 1
        "#
    ))
    .unwrap();
    let bridge = Bridge::start(cfg);
    let (r, w) = accept_fake(&listener).await;
    await_connected(&bridge, "local").await;

    // The upstream drops, so the link leaves the local cluster; the 1-second session expires.
    drop((r, w));
    // SETTLE(bridge-session-expiry): the expiry is the subject. The broker sweeps expired
    // sessions every second, so three seconds is past a 1-second expiry with margin.
    tokio::time::sleep(Duration::from_secs(3)).await;
    serve_broker(listener);
    await_metrics(&bridge, "the lost session was never counted", |r| {
        metric(r, r#"fss_bridge_sessions_lost_total{side="local"}"#) == Some(1)
    })
    .await;

    bridge.shutdown();
}
