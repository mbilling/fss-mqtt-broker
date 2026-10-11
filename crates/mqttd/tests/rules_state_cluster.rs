//! `eventual` rule state across real nodes (ADR 0085 T3): real sockets, the real peer
//! links, each node with its own hub, its own rules and its own store.
//!
//! The store's own tests (`mqttd::rule_state::tests_replica`) prove what converges and
//! what is refused, with the links driven by hand. These prove the broker around it:
//! that rules on different nodes count one counter and fill one set, that every node
//! then reads the same, that a partition heals, and that a node that comes back with
//! nothing is filled by the others. Nothing here waits for time: every wait is for a
//! node to read what it is expected to read, with a bound.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::Client;
use mqtt_cluster::NodeId;
use mqtt_codec::{Packet, QoS};
use mqtt_storage::MemorySessionStore;
use mqttd::hub::{Hub, HubCommand};
use mqttd::rule_state::Store;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// Every node's rules: one rule counts and collects, one reads back what this node
/// holds, to a topic of the reader's own.
const RULES: &str = r#"
[state.shared]
consistency = "eventual"

[rules.count]
state = "shared"
sql = '''SELECT kv_incr('n') AS n, kv_add('seen', clientid) AS seen, kv_set('last:' + clientid, payload) AS last FROM "in/#"'''

[rules.forget]
state = "shared"
sql = '''SELECT kv_del('last:' + payload) AS gone FROM "forget/#"'''

[rules.count_once]
state = "shared"
sql = '''SELECT kv_incr('orders', 1, 3600, payload) AS orders FROM "once/#"'''

[rules.read_orders]
state = "shared"
sql = '''SELECT clientid AS c, kv_get('orders') AS orders FROM "orders/#"'''
actions = [{ function = "republish", args = { topic = "out/${c}", payload = "${orders}" } }]

[rules.read]
state = "shared"
sql = '''SELECT clientid AS c, kv_get('n') AS n, kv_get('seen') AS seen, kv_get('last:' + payload) AS last FROM "read/#"'''
actions = [{ function = "republish", args = { topic = "out/${c}", payload = "${.}" } }]
"#;

/// One node of the cluster.
struct Node {
    id: NodeId,
    addr: SocketAddr,
    peer_addr: SocketAddr,
    hub_tx: tokio::sync::mpsc::UnboundedSender<HubCommand>,
    state: Arc<Store>,
}

/// A node as the broker wires one: one store for its rules, named after the node,
/// carried by every peer link it accepts or dials.
async fn start_node(name: &str) -> Node {
    let (functions, state) =
        mqttd::rules::functions_and_state_from_config(&mqtt_config::Rules::default());
    let state = state.expect("rule state is on by default");
    state.set_node(name);
    let id = NodeId(name.into());
    let sessions = Arc::new(MemorySessionStore::new());
    let (hub, hub_tx) = Hub::with_config(id.clone(), sessions.clone());
    tokio::spawn(hub.run());
    let set = mqtt_rules::RuleSet::parse_with(RULES, &functions)
        .unwrap_or_else(|e| panic!("test rules must load: {e}"))
        .rules;
    let (rules_tx, rx) = tokio::sync::watch::channel(Arc::new(set));
    // The rules never reload in these tests; the sender only has to outlive them.
    std::mem::forget(rules_tx);
    let engine = mqttd::rules::Rules::new(rx, Arc::from(name), None)
        .with_functions(functions)
        .with_state(Some(state.clone()));
    hub_tx
        .send(HubCommand::AttachRules(engine.clone()))
        .unwrap();
    let policy = Arc::new(mqttd::conn::ConnPolicy {
        anonymous: None,
        auth: mqttd::conn::auth_handle(Arc::new(mqtt_auth::basic::BasicAuthenticator {
            allow_anonymous: true,
        })),
        authz: mqttd::conn::authz_handle(Arc::new(mqtt_auth::AllowAll)),
        identity_source: mqtt_auth::mtls::IdentitySource::default(),
        audit: Arc::new(mqtt_observability::AuditLog::new()),
        proxy: None,
        node: None,
        store: Some(sessions as Arc<dyn mqtt_storage::SessionStore>),
        connect_timeout: Duration::from_secs(10),
        enhanced: None,
        shutdown: None,
        metrics: None,
        ingress: None,
        rules: Some(engine),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accept_tx = hub_tx.clone();
    tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            let arrival = mqttd::conn::Arrival {
                sockname: stream.local_addr().ok(),
                transport: mqtt_net::Transport::PlainTcp,
            };
            tokio::spawn(mqttd::conn::handle_stream_watched(
                stream,
                Some(peer),
                arrival,
                None,
                policy.clone(),
                accept_tx.clone(),
                None,
            ));
        }
    });
    let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = peer.local_addr().unwrap();
    tokio::spawn(mqttd::peer::serve_listener_with_ingress(
        peer,
        id.clone(),
        hub_tx.clone(),
        None,
        None,
        None,
        None,
        Some(state.clone()),
    ));
    Node {
        id,
        addr,
        peer_addr,
        hub_tx,
        state,
    }
}

/// A link between two nodes: the one whose id sorts first dials, and keeps it. Abort
/// the task to cut it.
fn link(a: &Node, b: &Node) -> JoinHandle<()> {
    let (from, to) = if a.id.0 < b.id.0 { (a, b) } else { (b, a) };
    tokio::spawn(mqttd::peer::dial_forever_with_ingress(
        to.peer_addr.to_string(),
        from.id.clone(),
        from.hub_tx.clone(),
        None,
        None,
        None,
        Some(from.state.clone()),
    ))
}

async fn publish(c: &mut Client, topic: &str, payload: &str) {
    c.publish(topic, payload.as_bytes(), QoS::AtMostOnce, None, vec![])
        .await;
}

/// A client on one node that asks that node's rules what the node holds.
struct Reader {
    client: Client,
    id: String,
}

impl Reader {
    async fn on(node: &Node, id: &str) -> Self {
        let mut client = Client::connect(node.addr, id).await;
        client
            .subscribe(1, &format!("out/{id}"), QoS::AtMostOnce)
            .await;
        Self {
            client,
            id: id.to_string(),
        }
    }

    /// What the node reads now: the counter, the set, and `last:<of>`.
    async fn read(&mut self, of: &str) -> String {
        publish(&mut self.client, "read/now", of).await;
        match self.client.recv().await {
            Packet::Publish(p) => {
                String::from_utf8_lossy(&p.payload).replace(&format!("\"c\":\"{}\",", self.id), "")
            }
            other => panic!("expected the rule's output, got {other:?}"),
        }
    }

    /// Wait until the node reads `wanted`: replication is on its own time.
    async fn reads(&mut self, of: &str, wanted: &str) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let read = self.read(of).await;
            if read == wanted {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{}: read {read}, never {wanted}",
                self.id
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// T3's cluster acceptance: rules on three nodes increment one counter and add to one
/// set, and after that every node reads the same; a delete on one node reaches the
/// others; no rule's state call went through the hub to get there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rules_on_every_node_count_one_counter_and_every_node_reads_the_same() {
    let nodes = [
        start_node("node-a").await,
        start_node("node-b").await,
        start_node("node-c").await,
    ];
    let _links = [
        link(&nodes[0], &nodes[1]),
        link(&nodes[0], &nodes[2]),
        link(&nodes[1], &nodes[2]),
    ];
    let mut writers = Vec::new();
    for (node, name) in nodes.iter().zip(["w-a", "w-b", "w-c"]) {
        writers.push(Client::connect(node.addr, name).await);
    }
    // Ten messages on each node, interleaved.
    for round in 0..10 {
        for writer in &mut writers {
            publish(writer, "in/x", &round.to_string()).await;
        }
    }
    let wanted = r#"{"n":30,"seen":["w-a","w-b","w-c"],"last":"9"}"#;
    let mut readers = Vec::new();
    for (node, name) in nodes.iter().zip(["r-a", "r-b", "r-c"]) {
        readers.push(Reader::on(node, name).await);
    }
    for (reader, of) in readers.iter_mut().zip(["w-b", "w-c", "w-a"]) {
        reader.reads(of, wanted).await;
    }
    // A delete on one node is a delete on all.
    publish(&mut writers[1], "forget/x", "w-a").await;
    let gone = r#"{"n":30,"seen":["w-a","w-b","w-c"],"last":"undefined"}"#;
    for reader in &mut readers {
        reader.reads("w-a", gone).await;
    }
    // Every node holds the same keys: the counter, the set, three last values, one
    // of them deleted and still counted.
    for node in &nodes {
        let stats = node.state.stats("shared").unwrap();
        assert_eq!(stats.keys, 5, "{}", node.id.0);
        let replication = node.state.replication("shared").unwrap();
        assert_eq!(replication.links, 2, "{}", node.id.0);
        assert_eq!(replication.refused, [0; 6], "{}", node.id.0);
    }
}

/// What `kv_get('orders')` reads on the reader's node, after everything the reader
/// has published there.
async fn orders(reader: &mut Reader) -> String {
    publish(&mut reader.client, "orders/now", "").await;
    match reader.client.recv().await {
        Packet::Publish(p) => String::from_utf8_lossy(&p.payload).into_owned(),
        other => panic!("expected the rule's output, got {other:?}"),
    }
}

/// Wait until `node` remembers the idempotency key `once` and its reader reads the
/// count `wanted`: replication is on its own time, and the entry and the counter are
/// two keys.
async fn heard(node: &Node, reader: &mut Reader, once: &str, wanted: &str) {
    let entry = mqtt_rules::state::Remembered::of(once.as_bytes());
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let remembered = node.state.key("shared", entry.key()).unwrap().is_some();
        let read = orders(reader).await;
        if remembered && read == wanted {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{}: remembers {once}: {remembered}, read {read}, never {wanted}",
            node.id.0
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// T5's cluster acceptance (D8): a message counted on one node is not counted again
/// on another once the entry that remembers its idempotency key has arrived there,
/// and a message that is new counts wherever it arrives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_message_resent_to_another_node_is_counted_once_when_the_entry_has_arrived() {
    let (a, b) = (start_node("node-a").await, start_node("node-b").await);
    let _up = link(&a, &b);
    let mut on_a = Reader::on(&a, "r-a").await;
    let mut on_b = Reader::on(&b, "r-b").await;
    publish(&mut on_a.client, "once/x", "m-1").await;
    assert_eq!(orders(&mut on_a).await, "1");
    heard(&b, &mut on_b, "m-1", "1").await;
    // The device reconnects to the other node and sends it again, twice.
    publish(&mut on_b.client, "once/x", "m-1").await;
    publish(&mut on_b.client, "once/x", "m-1").await;
    // A new message there counts, and node a hears of it.
    publish(&mut on_b.client, "once/x", "m-2").await;
    assert_eq!(orders(&mut on_b).await, "2");
    heard(&a, &mut on_a, "m-2", "2").await;
    publish(&mut on_a.client, "once/x", "m-2").await;
    assert_eq!(orders(&mut on_a).await, "2");
    assert_eq!(orders(&mut on_b).await, "2");
    // The counter and the two remembered keys, on both.
    for node in [&a, &b] {
        assert_eq!(node.state.stats("shared").unwrap().keys, 3, "{}", node.id.0);
    }
}

/// T3: two nodes that update the same keys while they cannot reach each other
/// converge when they can again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partition_heals() {
    let (a, b) = (start_node("node-a").await, start_node("node-b").await);
    let up = link(&a, &b);
    let mut on_a = Client::connect(a.addr, "w-a").await;
    let mut on_b = Client::connect(b.addr, "w-b").await;
    let mut read_a = Reader::on(&a, "r-a").await;
    let mut read_b = Reader::on(&b, "r-b").await;
    publish(&mut on_a, "in/x", "1").await;
    publish(&mut on_b, "in/x", "1").await;
    let together = r#"{"n":2,"seen":["w-a","w-b"],"last":"1"}"#;
    read_a.reads("w-b", together).await;
    read_b.reads("w-a", together).await;

    // Cut: each side counts on, and sees only its own.
    up.abort();
    let deadline = Instant::now() + Duration::from_secs(10);
    while a.state.replication("shared").unwrap().links != 0
        || b.state.replication("shared").unwrap().links != 0
    {
        assert!(Instant::now() < deadline, "the link never went down");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    for _ in 0..5 {
        publish(&mut on_a, "in/x", "2").await;
    }
    for _ in 0..7 {
        publish(&mut on_b, "in/x", "3").await;
    }
    read_a
        .reads("w-b", r#"{"n":7,"seen":["w-a","w-b"],"last":"1"}"#)
        .await;
    read_b
        .reads("w-a", r#"{"n":9,"seen":["w-a","w-b"],"last":"1"}"#)
        .await;

    // Healed: nothing was lost on either side.
    let _up = link(&a, &b);
    read_a
        .reads("w-b", r#"{"n":14,"seen":["w-a","w-b"],"last":"3"}"#)
        .await;
    read_b
        .reads("w-a", r#"{"n":14,"seen":["w-a","w-b"],"last":"2"}"#)
        .await;
}

/// T3: the store is in memory, so a node that restarts comes back with nothing, and
/// the others fill it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_node_is_refilled_from_the_others() {
    let (a, b) = (start_node("node-a").await, start_node("node-b").await);
    let up = link(&a, &b);
    let mut on_a = Client::connect(a.addr, "w-a").await;
    let mut on_b = Client::connect(b.addr, "w-b").await;
    for i in 0..20 {
        publish(&mut on_a, "in/x", &i.to_string()).await;
        publish(&mut on_b, "in/x", &i.to_string()).await;
    }
    let full = r#"{"n":40,"seen":["w-a","w-b"],"last":"19"}"#;
    Reader::on(&a, "r-a").await.reads("w-b", full).await;
    Reader::on(&b, "r-b").await.reads("w-a", full).await;

    // B goes, and a node of the same name comes up with an empty store.
    up.abort();
    drop(on_b);
    let b = start_node("node-b").await;
    assert_eq!(b.state.stats("shared").unwrap().keys, 0);
    let _up = link(&a, &b);
    let mut read_b = Reader::on(&b, "r-b2").await;
    read_b.reads("w-a", full).await;
    read_b.reads("w-b", full).await;
    // What it counts from here on is added to what it had counted before.
    let mut on_b = Client::connect(b.addr, "w-b").await;
    publish(&mut on_b, "in/x", "20").await;
    let more = r#"{"n":41,"seen":["w-a","w-b"],"last":"20"}"#;
    read_b.reads("w-b", more).await;
    Reader::on(&a, "r-a2").await.reads("w-b", more).await;
}
