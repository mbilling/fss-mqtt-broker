//! Shared end-to-end test harness (see `docs/TEST-PLAN.md`).
//!
//! Starts an in-process broker over real TCP loopback and provides a small MQTT
//! client — v3.1.1 and v5 — built on the project codec. Used by the integration
//! suites so each one does not re-implement `start_broker`/`Client`.
//!
//! The self-codec client is intentional: it gives precise control over the wire,
//! including the malformed/adversarial packets the darksky tests need.

#![allow(dead_code)] // each test crate uses only part of the harness

/// The CI-fatal environmental skip (issue #260). `#[macro_export]` puts
/// `skip_locally_or_fail_in_ci!` at each test binary's crate root, so a suite reaches it as
/// `crate::skip_locally_or_fail_in_ci!(…)` with no `#[macro_use]` ordering to get wrong.
pub mod skip;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use mqtt_codec::{
    packet::{Auth, ConnAck, Connect, Publish, SubAck, Subscribe, SubscribeFilter},
    Packet, Properties, Property, ProtocolVersion, QoS,
};
use mqttd::conn::ConnPolicy;
use mqttd::hub::Hub;
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpListener, TcpStream,
};
use tokio::time::timeout;

pub const V4: ProtocolVersion = ProtocolVersion::V311;
pub const V5: ProtocolVersion = ProtocolVersion::V5;

/// How long a `recv`/`expect_*` waits before declaring the broker unresponsive.
/// Generous so a contended CI runner — where off-loop durable-session recovery
/// (redb reopen + offline-queue replay after a node restart) can be momentarily
/// starved of CPU — does not spuriously time out; it bounds only the failure path.
const RECV_TIMEOUT: Duration = Duration::from_secs(10);

/// Spawn a permissive in-process broker (anonymous allowed, open ACL) on an
/// ephemeral port and return its address. The common path for protocol tests.
pub async fn start_broker() -> SocketAddr {
    let (hub, hub_tx) = Hub::new();
    tokio::spawn(hub.run());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(mqttd::conn::handle(stream, hub_tx.clone()));
        }
    });
    addr
}

/// Spawn a permissive broker whose per-session offline queue uses the given limits,
/// for exercising the bounded-queue overflow policy (ADR 0001 §6) end to end.
pub async fn start_broker_with_queue_limits(limits: mqtt_storage::QueueLimits) -> SocketAddr {
    use mqtt_cluster::NodeId;
    use mqtt_storage::MemorySessionStore;

    let store = Arc::new(MemorySessionStore::with_limits(limits));
    let (hub, hub_tx) = Hub::with_config(NodeId("node-local".into()), store);
    tokio::spawn(hub.run());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    spawn_client_loop(listener, hub_tx);
    addr
}

/// A single in-process cluster node: its client + peer listener addresses, plus the
/// handles needed to dial another node (for tests that link nodes on demand, e.g.
/// to exercise a node joining *after* a publish).
pub struct Node {
    /// Address clients connect to.
    pub client_addr: SocketAddr,
    /// Address peers dial to establish the cluster bus link.
    pub peer_addr: SocketAddr,
    id: mqtt_cluster::NodeId,
    tx: tokio::sync::mpsc::UnboundedSender<mqttd::HubCommand>,
}

/// Start a standalone cluster node (hub + client listener + peer listener), not yet
/// linked to anything. Use [`link`] to join it to another node.
pub async fn start_node(name: &str) -> Node {
    use mqtt_cluster::NodeId;
    use mqtt_storage::MemorySessionStore;

    let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = peer.local_addr().unwrap();
    let cli = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_addr = cli.local_addr().unwrap();

    let id = NodeId(name.to_string());
    let (hub, tx) = Hub::with_config(id.clone(), Arc::new(MemorySessionStore::new()));
    tokio::spawn(hub.run());
    spawn_client_loop(cli, tx.clone());
    tokio::spawn(mqttd::peer::serve_listener(
        peer,
        id.clone(),
        tx.clone(),
        None,
        None,
        None,
    ));

    Node {
        client_addr,
        peer_addr,
        id,
        tx,
    }
}

/// A live peer link between two nodes. Dropping it leaves the link up (the dial
/// tasks detach); [`Link::sever`] tears it down (and stops it re-dialing), to
/// simulate a network partition. Re-`link` the nodes to heal.
pub struct Link {
    dials: Vec<tokio::task::JoinHandle<()>>,
}

impl Link {
    /// Sever the link: abort the dial tasks, which cancels the in-flight serve and
    /// drops the TCP connection, and stops any re-dial. The peer that was accepting
    /// sees the EOF and drops its routing.
    pub fn sever(self) {
        for d in self.dials {
            d.abort();
        }
    }
}

/// Link two nodes into a full mesh: each dials the other's peer listener. Returns a
/// handle that can [`sever`](Link::sever) the link.
pub fn link(a: &Node, b: &Node) -> Link {
    let d1 = tokio::spawn(mqttd::peer::dial_forever(
        b.peer_addr.to_string(),
        a.id.clone(),
        a.tx.clone(),
        None,
        None,
    ));
    let d2 = tokio::spawn(mqttd::peer::dial_forever(
        a.peer_addr.to_string(),
        b.id.clone(),
        b.tx.clone(),
        None,
        None,
    ));
    Link {
        dials: vec![d1, d2],
    }
}

/// Bring up a two-node cluster (full peer mesh) on ephemeral ports and return each
/// node's client address. Cross-node routing is eventually consistent (interest is
/// gossiped on subscribe), so cluster tests retry until interest has propagated.
pub async fn start_two_node_cluster() -> (SocketAddr, SocketAddr) {
    use mqtt_cluster::NodeId;
    use mqtt_storage::MemorySessionStore;

    let peer_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let paddr_a = peer_a.local_addr().unwrap();
    let paddr_b = peer_b.local_addr().unwrap();
    let cli_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cli_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let caddr_a = cli_a.local_addr().unwrap();
    let caddr_b = cli_b.local_addr().unwrap();

    let id_a = NodeId("node-a".into());
    let id_b = NodeId("node-b".into());
    let (hub_a, tx_a) = Hub::with_config(id_a.clone(), Arc::new(MemorySessionStore::new()));
    let (hub_b, tx_b) = Hub::with_config(id_b.clone(), Arc::new(MemorySessionStore::new()));
    tokio::spawn(hub_a.run());
    tokio::spawn(hub_b.run());

    spawn_client_loop(cli_a, tx_a.clone());
    spawn_client_loop(cli_b, tx_b.clone());

    tokio::spawn(mqttd::peer::serve_listener(
        peer_a,
        id_a.clone(),
        tx_a.clone(),
        None,
        None,
        None,
    ));
    tokio::spawn(mqttd::peer::serve_listener(
        peer_b,
        id_b.clone(),
        tx_b.clone(),
        None,
        None,
        None,
    ));
    tokio::spawn(mqttd::peer::dial_forever(
        paddr_b.to_string(),
        id_a,
        tx_a,
        None,
        None,
    ));
    tokio::spawn(mqttd::peer::dial_forever(
        paddr_a.to_string(),
        id_b,
        tx_b,
        None,
        None,
    ));

    (caddr_a, caddr_b)
}

/// A self-removing, uniquely-named temporary directory for on-disk persistence
/// tests. Avoids a `tempfile` dependency (kept out to keep `cargo deny` lean); the
/// name is unique per process + monotonic counter so parallel tests never collide.
pub struct TempDir {
    path: std::path::PathBuf,
}

impl TempDir {
    #[must_use]
    pub fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("mqttd-it-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&path).unwrap();
        TempDir { path }
    }

    #[must_use]
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Default for TempDir {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// An in-process broker backed by the **on-disk** persistent session store (ADR 0018
/// phase 1): a `PersistentLog` (redb) under a `ReplicatedSessionStore`. Unlike
/// [`start_broker`], its state lives in a data directory and survives a
/// [`shutdown`](PersistentNode::shutdown) + restart from that same directory — the
/// node-level proof of the headline durability promise.
pub struct PersistentNode {
    /// Address clients connect to.
    pub client_addr: SocketAddr,
    shutdown: tokio_util::sync::CancellationToken,
    accept: tokio::task::JoinHandle<()>,
    hub: tokio::task::JoinHandle<()>,
}

/// Start a persistent node whose `sessions.redb` lives under `data_dir`. Reopening
/// the same directory after [`shutdown`](PersistentNode::shutdown) recovers the
/// sessions, subscriptions, and offline queues persisted there.
///
/// `open` is retried briefly: redb takes an advisory file lock, and on a same-process
/// restart the previous node's lock can take a moment to release after its last
/// `Database` handle drops. A genuine leak still fails (the retry budget is tight).
pub async fn start_persistent_node(data_dir: &std::path::Path) -> PersistentNode {
    use mqtt_cluster::NodeId;
    use mqtt_storage::logged::ReplicatedSessionStore;
    use mqtt_storage::persistent_log::PersistentLog;
    use mqtt_storage::{QueueLimits, SessionStore};

    let path = data_dir.join("sessions.redb");
    let mut attempt = 0;
    let log = loop {
        match PersistentLog::open(&path) {
            Ok(log) => break log,
            Err(e) if attempt < 40 => {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(25)).await;
                let _ = e;
            }
            Err(e) => panic!("open persistent session log at {}: {e}", path.display()),
        }
    };
    let store: Arc<dyn SessionStore> = Arc::new(ReplicatedSessionStore::with_limits(
        log,
        QueueLimits::default(),
    ));
    // The hub holds the only lasting `store` clone, so aborting it on shutdown drops
    // the last redb handle and releases the file lock.
    let (hub, hub_tx) = Hub::with_config(NodeId("node-persist".into()), store);
    let hub = tokio::spawn(hub.run());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_addr = listener.local_addr().unwrap();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let accept = {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => break,
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        tokio::spawn(mqttd::conn::handle(stream, hub_tx.clone()));
                    }
                }
            }
        })
    };
    PersistentNode {
        client_addr,
        shutdown,
        accept,
        hub,
    }
}

impl PersistentNode {
    /// Stop the node and **release the redb file lock** so the same data directory can
    /// be reopened. Stops the accept loop, then aborts the hub (which holds the only
    /// store handle) and awaits it so its `Database` is fully dropped before returning.
    ///
    /// Disconnect any live clients first: connection tasks are not force-closed here, so
    /// a still-attached client would keep its session "online". (Restart durability is
    /// about *retained* sessions, so tests detach cleanly before calling this.)
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        let _ = self.accept.await;
        self.hub.abort();
        let _ = self.hub.await;
        // Let any blocking redb Drop (file-lock release) settle before the caller
        // reopens the same directory.
        tokio::task::yield_now().await;
    }
}

fn spawn_client_loop(
    listener: TcpListener,
    tx: tokio::sync::mpsc::UnboundedSender<mqttd::HubCommand>,
) {
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(mqttd::conn::handle(stream, tx.clone()));
        }
    });
}

/// A permissive (anonymous, open-ACL) [`ConnPolicy`] with an explicit connect
/// deadline — for the half-open / slow-loris darksky tests.
#[must_use]
pub fn permissive_policy(connect_timeout: Duration) -> Arc<ConnPolicy> {
    Arc::new(ConnPolicy {
        auth: mqttd::conn::auth_handle(Arc::new(mqtt_auth::basic::BasicAuthenticator {
            allow_anonymous: true,
        })),
        enhanced: None,
        authz: mqttd::conn::authz_handle(Arc::new(mqtt_auth::AllowAll)),
        identity_source: mqtt_auth::mtls::IdentitySource::default(),
        audit: Arc::new(mqtt_observability::AuditLog::new()),
        proxy: None,
        node: None,
        store: None,
        connect_timeout,
        shutdown: None,
        metrics: None,
    })
}

/// Spawn an in-process broker driven by a caller-supplied [`ConnPolicy`] — for
/// tests that need a specific authenticator, ACL, or enhanced-auth mechanism.
pub async fn start_broker_with_policy(policy: Arc<ConnPolicy>) -> SocketAddr {
    let (hub, hub_tx) = Hub::new();
    tokio::spawn(hub.run());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            tokio::spawn(mqttd::conn::handle_stream(
                stream,
                Some(peer),
                None,
                policy.clone(),
                hub_tx.clone(),
            ));
        }
    });
    addr
}

/// A minimal MQTT client over the project framing + codec.
/// Outcome of a bounded, non-panicking receive ([`Client::recv_bounded`]).
#[derive(Debug)]
pub enum Recv {
    /// A packet arrived.
    Packet(Packet),
    /// Nothing arrived in the window; the connection is still open.
    Quiet,
    /// The connection is over (clean close or transport error).
    Closed,
}

pub struct Client {
    reader: mqtt_net::FrameReader<OwnedReadHalf>,
    writer: mqtt_net::FrameWriter<OwnedWriteHalf>,
}

impl Client {
    /// Open a TCP connection framed at `version` (no CONNECT sent yet).
    pub async fn open(addr: SocketAddr, version: ProtocolVersion) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (rh, wh) = stream.into_split();
        Client {
            reader: mqtt_net::FrameReader::new(rh, version),
            writer: mqtt_net::FrameWriter::new(wh, version),
        }
    }

    /// Connect as a clean v3.1.1 session, asserting a successful CONNACK.
    pub async fn connect(addr: SocketAddr, client_id: &str) -> Self {
        Self::connect_v311(addr, client_id, true).await.0
    }

    /// Connect as v3.1.1 with an explicit clean-session flag; returns the client and
    /// the CONNACK `session_present` flag.
    pub async fn connect_v311(addr: SocketAddr, client_id: &str, clean: bool) -> (Self, bool) {
        let mut c = Self::open(addr, V4).await;
        c.send(&Packet::Connect(Connect {
            properties: Properties::new(),
            protocol: V4,
            clean_session: clean,
            keep_alive: 0, // disabled: harness phases can idle a conn arbitrarily long
            client_id: client_id.to_string(),
            last_will: None,
            username: None,
            password: None,
        }))
        .await;
        let present = match c.recv().await {
            Packet::ConnAck(a) => {
                assert_eq!(a.code, 0, "v3.1.1 CONNACK should be success");
                a.session_present
            }
            other => panic!("expected CONNACK, got {other:?}"),
        };
        (c, present)
    }

    /// Send a v3.1.1 CONNECT carrying a `QoS` 1 Last Will and assert the CONNACK.
    /// `clean_session = false`, so the session (and therefore the will) survives long
    /// enough for a broker-initiated close to publish it (issue #238, R1).
    pub async fn connect_with_will(&mut self, client_id: &str, topic: &str, payload: &[u8]) {
        self.send(&Packet::Connect(Connect {
            properties: Properties::new(),
            protocol: V4,
            clean_session: false,
            keep_alive: 0,
            client_id: client_id.to_string(),
            last_will: Some(mqtt_codec::packet::LastWill {
                topic: topic.to_string(),
                payload: bytes::Bytes::copy_from_slice(payload),
                qos: QoS::AtLeastOnce,
                retain: false,
                properties: Properties::new(),
            }),
            username: None,
            password: None,
        }))
        .await;
        match self.recv().await {
            Packet::ConnAck(a) => assert_eq!(a.code, 0, "CONNECT with a will should succeed"),
            other => panic!("expected CONNACK, got {other:?}"),
        }
    }

    /// Connect as v3.1.1, waiting up to `wait` for the CONNACK instead of the default
    /// 2s recv bound. Returns `None` — dropping the half-open connection so the caller
    /// can retry a fresh connect — if the CONNACK does not arrive in time, the peer
    /// closes, or the broker refuses with a non-success code (e.g. Server-unavailable
    /// while a durable session's lease is still reassigning, ADR 0017). A successful
    /// CONNACK yields the client and its `session_present` flag.
    pub async fn connect_v311_within(
        addr: SocketAddr,
        client_id: &str,
        clean: bool,
        wait: Duration,
    ) -> Option<(Self, bool)> {
        // A refused/unreachable TCP connect is also a `None` (not a panic):
        // the out-of-process harness (ADR 0044) dials brokers that may still
        // be booting — or be SIGKILLED — and its callers retry.
        let Ok(Ok(stream)) = timeout(wait, TcpStream::connect(addr)).await else {
            return None;
        };
        let (rh, wh) = stream.into_split();
        let mut c = Client {
            reader: mqtt_net::FrameReader::new(rh, V4),
            writer: mqtt_net::FrameWriter::new(wh, V4),
        };
        c.send(&Packet::Connect(Connect {
            properties: Properties::new(),
            protocol: V4,
            clean_session: clean,
            keep_alive: 0, // disabled: harness phases can idle a conn arbitrarily long
            client_id: client_id.to_string(),
            last_will: None,
            username: None,
            password: None,
        }))
        .await;
        match timeout(wait, c.reader.next_packet()).await {
            Ok(Ok(Some(Packet::ConnAck(a)))) if a.code == 0 => Some((c, a.session_present)),
            // Refused (transient Server-unavailable), timed out, errored, or closed:
            // the caller retries a fresh connect.
            _ => None,
        }
    }

    /// Connect as v5 with the given CONNECT properties; returns the client and the
    /// full CONNACK (so the caller can assert negotiated properties or a reason code).
    pub async fn connect_v5(
        addr: SocketAddr,
        client_id: &str,
        clean_start: bool,
        properties: Vec<Property>,
    ) -> (Self, ConnAck) {
        let mut c = Self::open(addr, V5).await;
        c.send(&Packet::Connect(Connect {
            properties: Properties(properties),
            protocol: V5,
            clean_session: clean_start,
            keep_alive: 0, // disabled: harness phases can idle a conn arbitrarily long
            client_id: client_id.to_string(),
            last_will: None,
            username: None,
            password: None,
        }))
        .await;
        match c.recv().await {
            Packet::ConnAck(a) => (c, a),
            other => panic!("expected v5 CONNACK, got {other:?}"),
        }
    }

    /// Connect as a clean v5 session, asserting success.
    pub async fn connect_v5_ok(addr: SocketAddr, client_id: &str) -> Self {
        let (c, ack) = Self::connect_v5(addr, client_id, true, vec![]).await;
        assert_eq!(ack.code, 0, "v5 CONNACK should be success");
        c
    }

    pub async fn send(&mut self, packet: &Packet) {
        self.writer.send(packet).await.unwrap();
    }

    /// The next packet, or panic on timeout/close.
    pub async fn recv(&mut self) -> Packet {
        timeout(RECV_TIMEOUT, self.reader.next_packet())
            .await
            .expect("timed out waiting for a packet")
            .expect("transport error")
            .expect("connection closed unexpectedly")
    }

    /// The next event within `window`, distinguishing quiet from a dead
    /// connection **without panicking** — for harnesses whose nodes crash on
    /// purpose (ADR 0042 T3): a killed node resets its sockets, and both a clean
    /// close and a transport error mean the same thing to a stress client.
    pub async fn recv_bounded(&mut self, window: Duration) -> Recv {
        match timeout(window, self.reader.next_packet()).await {
            Err(_) => Recv::Quiet,
            Ok(Ok(Some(p))) => Recv::Packet(p),
            Ok(Ok(None) | Err(_)) => Recv::Closed,
        }
    }

    /// Publish with every knob exposed (retain included) — the stress harness
    /// needs retained `QoS` 1 publishes whose ack it awaits tolerantly itself.
    pub async fn publish_full(
        &mut self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
        retain: bool,
        pkid: Option<u16>,
    ) {
        self.send(&Packet::Publish(Publish {
            properties: Properties::new(),
            dup: false,
            qos,
            retain,
            topic: topic.into(),
            pkid,
            payload: bytes::Bytes::copy_from_slice(payload),
        }))
        .await;
    }

    /// The next packet within the window, or `None` if none arrived (still open).
    pub async fn try_recv(&mut self) -> Option<Packet> {
        match timeout(Duration::from_millis(300), self.reader.next_packet()).await {
            Ok(r) => r.expect("transport error"),
            Err(_) => None,
        }
    }

    /// Assert that no packet arrives in the quiet window (the socket stays open).
    pub async fn expect_silence(&mut self) {
        if let Some(p) = self.try_recv().await {
            panic!("expected silence, got {p:?}");
        }
    }

    /// Assert the broker closed the connection (clean EOF).
    pub async fn expect_closed(&mut self) {
        let pkt = timeout(RECV_TIMEOUT, self.reader.next_packet())
            .await
            .expect("timed out waiting for close")
            .expect("transport error");
        assert!(pkt.is_none(), "expected connection close, got {pkt:?}");
    }

    /// Assert the broker sent a v5 DISCONNECT with reason `reason`, then closed.
    pub async fn expect_disconnect(&mut self, reason: u8) {
        match self.recv().await {
            Packet::Disconnect(d) => assert_eq!(d.reason, reason, "DISCONNECT reason"),
            other => panic!("expected DISCONNECT {reason:#04x}, got {other:?}"),
        }
        self.expect_closed().await;
    }

    /// Subscribe to one filter and return the SUBACK.
    pub async fn subscribe(&mut self, pkid: u16, filter: &str, qos: QoS) -> SubAck {
        self.send(&Packet::Subscribe(Subscribe {
            properties: Properties::new(),
            pkid,
            filters: vec![SubscribeFilter {
                options: mqtt_codec::SubscriptionOptions::default(),
                path: filter.into(),
                qos,
            }],
        }))
        .await;
        match self.recv().await {
            Packet::SubAck(a) => a,
            other => panic!("expected SUBACK, got {other:?}"),
        }
    }

    /// Publish with the given `QoS`, packet id, and (v5) properties. For `QoS` > 0 the
    /// caller supplies the packet id so it can drive the ack handshake.
    pub async fn publish(
        &mut self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
        pkid: Option<u16>,
        properties: Vec<Property>,
    ) {
        self.send(&Packet::Publish(Publish {
            properties: Properties(properties),
            dup: false,
            qos,
            retain: false,
            topic: topic.into(),
            pkid,
            payload: bytes::Bytes::copy_from_slice(payload),
        }))
        .await;
    }

    /// Publish a retained message (`QoS` 0).
    pub async fn publish_retained(&mut self, topic: &str, payload: &[u8]) {
        self.send(&Packet::Publish(Publish {
            properties: Properties::new(),
            dup: false,
            qos: QoS::AtMostOnce,
            retain: true,
            topic: topic.into(),
            pkid: None,
            payload: bytes::Bytes::copy_from_slice(payload),
        }))
        .await;
    }

    /// Publish a retained message at `QoS` 1 and wait for the PUBACK. The ack is sent
    /// only after the connection has forwarded the PUBLISH to the hub, so by the time
    /// it returns the retain-store command sits ahead of any later command in the hub's
    /// FIFO queue. Use this (not the `QoS` 0 variant) when a subscribe that must observe
    /// the retained message follows: it removes the store-vs-subscribe race.
    pub async fn publish_retained_acked(&mut self, topic: &str, payload: &[u8], pkid: u16) {
        self.send(&Packet::Publish(Publish {
            properties: Properties::new(),
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: true,
            topic: topic.into(),
            pkid: Some(pkid),
            payload: bytes::Bytes::copy_from_slice(payload),
        }))
        .await;
        assert_eq!(self.recv().await, Packet::PubAck(pkid.into()));
    }

    pub async fn puback(&mut self, pkid: u16) {
        self.send(&Packet::PubAck(pkid.into())).await;
    }

    pub async fn pubrec(&mut self, pkid: u16) {
        self.send(&Packet::PubRec(pkid.into())).await;
    }

    pub async fn pubrel(&mut self, pkid: u16) {
        self.send(&Packet::PubRel(pkid.into())).await;
    }

    pub async fn pubcomp(&mut self, pkid: u16) {
        self.send(&Packet::PubComp(pkid.into())).await;
    }

    /// Send a clean DISCONNECT and wait for the broker to close the socket. Waiting
    /// for the close guarantees the Detach is processed before the test proceeds.
    pub async fn disconnect(&mut self) {
        self.send(&Packet::Disconnect(
            mqtt_codec::packet::Disconnect::default(),
        ))
        .await;
        self.expect_closed().await;
    }

    /// A clean DISCONNECT carrying properties — the v5 way to change a session's
    /// terms on the way out (§3.14.2.2.2 lets a Session Expiry Interval here
    /// override the one agreed at CONNECT).
    pub async fn disconnect_with(&mut self, properties: Vec<mqtt_codec::Property>) {
        self.send(&Packet::Disconnect(mqtt_codec::packet::Disconnect {
            reason: 0,
            properties: mqtt_codec::Properties(properties),
        }))
        .await;
        self.expect_closed().await;
    }

    /// The next packet expected to be a PUBLISH.
    pub async fn expect_publish(&mut self) -> Publish {
        match self.recv().await {
            Packet::Publish(p) => p,
            other => panic!("expected PUBLISH, got {other:?}"),
        }
    }

    /// The next packet expected to be an AUTH.
    pub async fn expect_auth(&mut self) -> Auth {
        match self.recv().await {
            Packet::Auth(a) => a,
            other => panic!("expected AUTH, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// RawClient — the byte-level client the codec suite needs.
//
// `Client` above speaks in typed `Packet`s, which is what makes it ergonomic and
// also what makes it USELESS for suite WIRE: an encoder will not emit a 5-byte
// remaining-length, an overlong UTF-8 sequence, or a reserved packet type. Those
// frames have to be written as bytes. This client does that, and reads the
// server's answer as bytes too — because "did the server close without sending
// anything" and "did it send DISCONNECT(0x81) first" are different conformance
// outcomes that a decoding reader flattens into the same `Err`.
//
// Everything here is deliberately dumb: no framing, no codec, no retry. The test
// supplies the bytes and states the expected outcome exactly.
// ---------------------------------------------------------------------------

/// What the server did in response to bytes we wrote.
#[derive(Debug, PartialEq, Eq)]
pub enum RawOutcome {
    /// The server sent these bytes (at least one).
    Bytes(Vec<u8>),
    /// The server closed without sending anything — the required behaviour for a
    /// Malformed Packet detected before a session exists (MQTT-3.1.4-1: no CONNACK
    /// may be sent, so the only correct answer is a silent close).
    ClosedSilently,
    /// Nothing arrived and the connection stayed open.
    Quiet,
}

/// A raw byte-level client: writes arbitrary bytes, reads raw bytes back.
pub struct RawClient {
    stream: TcpStream,
}

impl RawClient {
    /// Open a TCP connection to the broker. Nothing is sent.
    pub async fn open(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        Self { stream }
    }

    /// Write bytes in one `write_all`. The kernel may still split them; use
    /// [`send_fragmented`](Self::send_fragmented) when the split itself is the subject.
    pub async fn send_bytes(&mut self, bytes: &[u8]) {
        use tokio::io::AsyncWriteExt;
        self.stream.write_all(bytes).await.unwrap();
        self.stream.flush().await.unwrap();
    }

    /// Like [`send_bytes`](Self::send_bytes), but a broken pipe / connection reset
    /// mid-write is ACCEPTED rather than a panic (issues #292/#306): when the send
    /// itself is what the server refuses — an oversized packet, say — the refusal
    /// can land while these bytes are still going out, and the resulting `EPIPE`
    /// *is the behaviour under test* arriving one syscall early. macOS's smaller
    /// socket buffers surface this deterministically where Linux's usually absorb
    /// the write; a test whose subject is "the server hangs up on me" must not
    /// unwrap its own writes. Any other I/O error still panics.
    pub async fn send_bytes_tolerating_close(&mut self, bytes: &[u8]) {
        use std::io::ErrorKind;
        use tokio::io::AsyncWriteExt;
        let ok_kind =
            |k: ErrorKind| matches!(k, ErrorKind::BrokenPipe | ErrorKind::ConnectionReset);
        if let Err(e) = self.stream.write_all(bytes).await {
            assert!(ok_kind(e.kind()), "unexpected write error: {e:?}");
            return;
        }
        if let Err(e) = self.stream.flush().await {
            assert!(ok_kind(e.kind()), "unexpected flush error: {e:?}");
        }
    }

    /// Write `bytes` in `chunk` -sized pieces, flushing and pausing `gap` between each,
    /// so the broker's decoder genuinely sees a partial frame and must buffer it.
    /// `TCP_NODELAY` is set so a small chunk is not coalesced by Nagle into the next.
    pub async fn send_fragmented(&mut self, bytes: &[u8], chunk: usize, gap: Duration) {
        use tokio::io::AsyncWriteExt;
        self.stream.set_nodelay(true).unwrap();
        for piece in bytes.chunks(chunk.max(1)) {
            self.stream.write_all(piece).await.unwrap();
            self.stream.flush().await.unwrap();
            if !gap.is_zero() {
                // SETTLE(wire-fragment-gap): the caller's `gap` exists to make the peer see a
                // PARTIAL frame, and "the decoder is holding an incomplete packet" is not
                // observable from the wire — it is precisely the state that produces no output.
                // `TCP_NODELAY` is set above so Nagle cannot coalesce the pieces back together.
                // One-sided failure mode: a slower machine makes the fragmentation more certain,
                // so this cannot silently stop testing what it exists for.
                tokio::time::sleep(gap).await;
            }
        }
    }

    /// Write several packets as ONE write, so they arrive coalesced in a single
    /// segment and the decoder must find every boundary itself.
    pub async fn send_coalesced(&mut self, packets: &[&[u8]]) {
        let joined: Vec<u8> = packets.concat();
        self.send_bytes(&joined).await;
    }

    /// Read whatever the server sends within `window`, distinguishing a silent close
    /// from a response from silence-with-the-connection-open. Reads once: enough for
    /// the "what did the server answer" question, which is all suite WIRE asks.
    pub async fn read_outcome(&mut self, window: Duration) -> RawOutcome {
        use tokio::io::AsyncReadExt;
        let mut buf = vec![0u8; 512];
        match timeout(window, self.stream.read(&mut buf)).await {
            Err(_) => RawOutcome::Quiet,
            Ok(Ok(n)) if n > 0 => {
                buf.truncate(n);
                RawOutcome::Bytes(buf)
            }
            // Read of 0 is a clean FIN; an error here is a RST. Both mean "the
            // server ended the connection without answering", which is the
            // outcome under test — they are one case, not two.
            Ok(_) => RawOutcome::ClosedSilently,
        }
    }

    /// Assert the server closed the connection WITHOUT sending a packet — the
    /// required response to a Malformed Packet before a session exists.
    pub async fn expect_closed_silently(&mut self) {
        match self.read_outcome(RECV_TIMEOUT).await {
            RawOutcome::ClosedSilently => {}
            other => panic!("expected a silent close, got {other:?}"),
        }
    }

    /// Assert the server answered with a DISCONNECT carrying `reason`, checked on the
    /// wire bytes: `0xE0`, remaining length, then the reason code. Used after a session
    /// exists, where the spec requires the server to say why before closing.
    pub async fn expect_disconnect_bytes(&mut self, reason: u8) {
        match self.read_outcome(RECV_TIMEOUT).await {
            RawOutcome::Bytes(b) => {
                assert!(b.len() >= 3, "DISCONNECT is at least 3 bytes, got {b:02x?}");
                assert_eq!(b[0], 0xE0, "expected DISCONNECT packet type, got {b:02x?}");
                assert_eq!(
                    b[2], reason,
                    "expected DISCONNECT reason {reason:#04x}, got {:#04x} ({b:02x?})",
                    b[2]
                );
            }
            other => panic!("expected DISCONNECT({reason:#04x}), got {other:?}"),
        }
    }

    /// Assert the server answered with a CONNACK carrying `reason` (byte 3 of
    /// `0x20 len session_present reason`), and that `session_present` is 0 — which
    /// MQTT-3.2.2-6 requires whenever the reason code is a failure.
    pub async fn expect_connack_bytes(&mut self, reason: u8) {
        match self.read_outcome(RECV_TIMEOUT).await {
            RawOutcome::Bytes(b) => {
                assert!(b.len() >= 4, "CONNACK is at least 4 bytes, got {b:02x?}");
                assert_eq!(b[0], 0x20, "expected CONNACK packet type, got {b:02x?}");
                assert_eq!(
                    b[3], reason,
                    "expected CONNACK reason {reason:#04x}, got {:#04x} ({b:02x?})",
                    b[3]
                );
                if reason >= 0x80 {
                    assert_eq!(
                        b[2], 0x00,
                        "a failure CONNACK must carry session_present = 0 [MQTT-3.2.2-6]"
                    );
                }
            }
            other => panic!("expected CONNACK({reason:#04x}), got {other:?}"),
        }
    }

    /// Assert the server REFUSED the packet, and that `reason` is what it gave.
    ///
    /// After CONNACK this broker announces decode failures with `DISCONNECT(reason)`
    /// before closing ([MQTT-4.13.2]: closing is the MUST, saying why the SHOULD), so
    /// the reason assertion is live. The bare-close arm remains accepted because
    /// **before** a success CONNACK a DISCONNECT is forbidden [MQTT-3.14.0-1] and
    /// silence is the only correct answer — see the pair of tests around
    /// `malformed_input_before_connack_is_met_with_silence` in `tests/wire.rs`.
    /// What must NOT happen — and what this asserts against — is the packet being
    /// *accepted*.
    pub async fn expect_refused(&mut self, reason: u8) {
        match self.read_outcome(RECV_TIMEOUT).await {
            RawOutcome::ClosedSilently => {}
            RawOutcome::Bytes(b) if b[0] == 0xE0 => {
                assert_eq!(
                    b[2], reason,
                    "refused, but with reason {:#04x} instead of {reason:#04x}",
                    b[2]
                );
            }
            other => panic!("expected the packet to be refused, got {other:?}"),
        }
    }

    /// Assert nothing arrives within `window` and the connection stays open — for the
    /// under-run case, where the server is legitimately still waiting for more bytes.
    pub async fn expect_quiet(&mut self, window: Duration) {
        match self.read_outcome(window).await {
            RawOutcome::Quiet => {}
            other => panic!("expected silence with the connection open, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Byte-level packet builders. These deliberately do NOT go through the codec:
// a test that builds its malformed frame with the encoder under test proves
// nothing. Each helper is the minimum hand-assembled bytes for a legal packet,
// which individual tests then corrupt in exactly one way.
// ---------------------------------------------------------------------------

/// Encode a Variable Byte Integer per MQTT 1.5.5.
#[must_use]
pub fn vbi(mut value: u32) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            return out;
        }
    }
}

/// A length-prefixed UTF-8 string field (MQTT 1.5.4).
#[must_use]
pub fn mqtt_str(s: &str) -> Vec<u8> {
    let mut out = u16::try_from(s.len()).unwrap().to_be_bytes().to_vec();
    out.extend_from_slice(s.as_bytes());
    out
}

/// A length-prefixed field from raw bytes — for deliberately invalid UTF-8.
#[must_use]
pub fn mqtt_bytes(b: &[u8]) -> Vec<u8> {
    let mut out = u16::try_from(b.len()).unwrap().to_be_bytes().to_vec();
    out.extend_from_slice(b);
    out
}

/// Wrap a variable header + payload in a fixed header with `first_byte`.
#[must_use]
pub fn frame(first_byte: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![first_byte];
    out.extend_from_slice(&vbi(u32::try_from(body.len()).unwrap()));
    out.extend_from_slice(body);
    out
}

/// A minimal, VALID v5 CONNECT for `client_id`: clean start, keep-alive 0, no
/// properties, no will. Tests corrupt one field of this to isolate one rule.
#[must_use]
pub fn connect_v5_bytes(client_id: &str) -> Vec<u8> {
    let mut body = mqtt_str("MQTT");
    body.push(5); // protocol version
    body.push(0x02); // connect flags: clean start
    body.extend_from_slice(&0u16.to_be_bytes()); // keep alive
    body.push(0x00); // property length 0
    body.extend_from_slice(&mqtt_str(client_id));
    frame(0x10, &body)
}

/// A minimal, VALID v5 SUBSCRIBE for one filter at `QoS` 0, packet id 1.
#[must_use]
pub fn subscribe_v5_bytes(filter: &str) -> Vec<u8> {
    let mut body = 1u16.to_be_bytes().to_vec(); // packet id
    body.push(0x00); // property length 0
    body.extend_from_slice(&mqtt_str(filter));
    body.push(0x00); // subscription options: QoS 0
    frame(0x82, &body)
}

/// A minimal, VALID v5 QoS-0 PUBLISH.
#[must_use]
pub fn publish_v5_bytes(topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut body = mqtt_str(topic);
    body.push(0x00); // property length 0
    body.extend_from_slice(payload);
    frame(0x30, &body)
}

/// Helpers for the HMAC-SHA256 enhanced-authentication mechanism (ADR 0013): a
/// broker policy configured with one subject ("alice"), the proof the client
/// returns, and an AUTH-packet builder. Shared by the sunshine and darksky suites.
pub mod enhanced {
    use super::{Arc, ConnPolicy, Packet, Properties, Property};

    pub const METHOD: &str = "HMAC-SHA256";
    pub const SUBJECT: &str = "alice";
    const SECRET: &[u8] = b"alice-secret";

    /// A broker policy whose enhanced authenticator knows `SUBJECT`'s secret.
    #[must_use]
    pub fn policy() -> Arc<ConnPolicy> {
        let mut secrets = std::collections::HashMap::new();
        secrets.insert(SUBJECT.to_string(), SECRET.to_vec());
        Arc::new(ConnPolicy {
            auth: mqttd::conn::auth_handle(Arc::new(mqtt_auth::basic::BasicAuthenticator {
                allow_anonymous: true,
            })),
            enhanced: Some(Arc::new(mqtt_auth::HmacChallengeAuthenticator::new(
                secrets,
            ))),
            authz: mqttd::conn::authz_handle(Arc::new(mqtt_auth::AllowAll)),
            identity_source: mqtt_auth::mtls::IdentitySource::default(),
            audit: Arc::new(mqtt_observability::AuditLog::new()),
            proxy: None,
            node: None,
            store: None,
            connect_timeout: std::time::Duration::from_secs(10),
            shutdown: None,
            metrics: None,
        })
    }

    /// The correct HMAC-SHA256 proof over `nonce` for `SUBJECT`.
    #[must_use]
    pub fn proof(nonce: &[u8]) -> Vec<u8> {
        let key = aws_lc_rs::hmac::Key::new(aws_lc_rs::hmac::HMAC_SHA256, SECRET);
        aws_lc_rs::hmac::sign(&key, nonce).as_ref().to_vec()
    }

    /// An AUTH packet for `METHOD` with the given reason and data.
    #[must_use]
    pub fn auth(reason: u8, data: &[u8]) -> Packet {
        Packet::Auth(mqtt_codec::packet::Auth {
            reason,
            properties: Properties(vec![
                Property::AuthenticationMethod(METHOD.into()),
                Property::AuthenticationData(bytes::Bytes::copy_from_slice(data)),
            ]),
        })
    }

    /// Extract a challenge nonce (Authentication Data) from an AUTH's properties.
    #[must_use]
    pub fn nonce_of(props: &Properties) -> Vec<u8> {
        props
            .0
            .iter()
            .find_map(|p| match p {
                Property::AuthenticationData(b) => Some(b.to_vec()),
                _ => None,
            })
            .expect("an AUTH challenge nonce")
    }
}

// ---------------------------------------------------------------------------
// FlakyStore — the fault-injecting SessionStore, promoted from the hub's unit
// test module to a shared harness fixture (ADR 0042 T4). Wraps ANY store and,
// while `fail_writes` is set, fails every durable WRITE with a terminal
// `Backend` error — the disk-full / write-error shape. Reads keep working
// (a full disk still serves what it has). The broker under test must respond
// by WITHHOLDING the corresponding acknowledgements (fail closed): a PUBACK,
// SUBACK, or CONNACK granted while the write failed would be a durability lie
// (ADR 0041 T5, 0042 T9).
// ---------------------------------------------------------------------------

/// See the module note above. `fail_writes` is shared with the harness, which
/// toggles it mid-schedule (the disk-fault step).
#[derive(Debug)]
pub struct FlakyStore {
    inner: std::sync::Arc<dyn mqtt_storage::SessionStore>,
    pub fail_writes: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl FlakyStore {
    pub fn wrap(inner: std::sync::Arc<dyn mqtt_storage::SessionStore>) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            inner,
            fail_writes: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    fn check_write(&self) -> Result<(), mqtt_storage::StorageError> {
        if self.fail_writes.load(std::sync::atomic::Ordering::SeqCst) {
            Err(mqtt_storage::StorageError::Backend(
                "injected disk fault (ADR 0042 T4)".into(),
            ))
        } else {
            Ok(())
        }
    }
}

#[async_trait::async_trait]
impl mqtt_storage::SessionStore for FlakyStore {
    async fn ensure_session(
        &self,
        client: &mqtt_core::ClientId,
    ) -> Result<bool, mqtt_storage::StorageError> {
        self.check_write()?;
        self.inner.ensure_session(client).await
    }

    async fn claim_session(
        &self,
        client: &mqtt_core::ClientId,
        owner: &str,
    ) -> Result<mqtt_storage::SessionClaim, mqtt_storage::StorageError> {
        self.check_write()?;
        self.inner.claim_session(client, owner).await
    }

    async fn set_subscriptions(
        &self,
        client: &mqtt_core::ClientId,
        subs: &[mqtt_core::Subscription],
    ) -> Result<(), mqtt_storage::StorageError> {
        self.check_write()?;
        self.inner.set_subscriptions(client, subs).await
    }

    async fn subscriptions(
        &self,
        client: &mqtt_core::ClientId,
    ) -> Result<Vec<mqtt_core::Subscription>, mqtt_storage::StorageError> {
        self.inner.subscriptions(client).await
    }

    async fn enqueue_with_expiry(
        &self,
        client: &mqtt_core::ClientId,
        message: &mqtt_core::Message,
        expiry_at: Option<u64>,
    ) -> Result<mqtt_storage::Enqueued, mqtt_storage::StorageError> {
        self.check_write()?;
        self.inner
            .enqueue_with_expiry(client, message, expiry_at)
            .await
    }

    async fn pending(
        &self,
        client: &mqtt_core::ClientId,
        after: mqtt_storage::Offset,
        limit: usize,
    ) -> Result<Vec<mqtt_storage::QueuedMessage>, mqtt_storage::StorageError> {
        self.inner.pending(client, after, limit).await
    }

    async fn ack(
        &self,
        client: &mqtt_core::ClientId,
        up_to: mqtt_storage::Offset,
    ) -> Result<(), mqtt_storage::StorageError> {
        self.check_write()?;
        self.inner.ack(client, up_to).await
    }

    async fn record_received(
        &self,
        client: &mqtt_core::ClientId,
        packet_id: u16,
    ) -> Result<mqtt_storage::InboundSighting, mqtt_storage::StorageError> {
        self.check_write()?;
        self.inner.record_received(client, packet_id).await
    }

    async fn ack_received(
        &self,
        client: &mqtt_core::ClientId,
        packet_id: u16,
    ) -> Result<(), mqtt_storage::StorageError> {
        self.check_write()?;
        self.inner.ack_received(client, packet_id).await
    }

    async fn clear_received(
        &self,
        client: &mqtt_core::ClientId,
        packet_id: u16,
    ) -> Result<(), mqtt_storage::StorageError> {
        self.check_write()?;
        self.inner.clear_received(client, packet_id).await
    }

    async fn received(
        &self,
        client: &mqtt_core::ClientId,
    ) -> Result<Vec<u16>, mqtt_storage::StorageError> {
        self.inner.received(client).await
    }

    async fn record_outbound(
        &self,
        client: &mqtt_core::ClientId,
        packet_id: u16,
        offset: mqtt_storage::Offset,
    ) -> Result<(), mqtt_storage::StorageError> {
        // A WRITE like any other on the durable path (ADR 0057): while writes fail, the
        // PUBLISH it gates must not go out, exactly as a failed enqueue withholds PUBACK.
        self.check_write()?;
        self.inner.record_outbound(client, packet_id, offset).await
    }

    async fn advance_outbound(
        &self,
        client: &mqtt_core::ClientId,
        packet_id: u16,
    ) -> Result<(), mqtt_storage::StorageError> {
        self.check_write()?;
        self.inner.advance_outbound(client, packet_id).await
    }

    async fn clear_outbound(
        &self,
        client: &mqtt_core::ClientId,
        packet_id: u16,
    ) -> Result<(), mqtt_storage::StorageError> {
        self.check_write()?;
        self.inner.clear_outbound(client, packet_id).await
    }

    async fn outbound(
        &self,
        client: &mqtt_core::ClientId,
    ) -> Result<Vec<mqtt_storage::OutboundInflight>, mqtt_storage::StorageError> {
        self.inner.outbound(client).await
    }

    async fn next_packet_id(
        &self,
        client: &mqtt_core::ClientId,
    ) -> Result<u16, mqtt_storage::StorageError> {
        self.check_write()?;
        self.inner.next_packet_id(client).await
    }

    async fn reserve_packet_ids(
        &self,
        client: &mqtt_core::ClientId,
        count: u16,
    ) -> Result<u16, mqtt_storage::StorageError> {
        self.check_write()?;
        self.inner.reserve_packet_ids(client, count).await
    }

    async fn remove(&self, client: &mqtt_core::ClientId) -> Result<(), mqtt_storage::StorageError> {
        self.check_write()?;
        self.inner.remove(client).await
    }

    async fn set_session_expiry(
        &self,
        client: &mqtt_core::ClientId,
        deadline: Option<u64>,
    ) -> Result<(), mqtt_storage::StorageError> {
        self.check_write()?;
        self.inner.set_session_expiry(client, deadline).await
    }

    async fn expiring_sessions(
        &self,
    ) -> Result<Vec<(mqtt_core::ClientId, u64)>, mqtt_storage::StorageError> {
        self.inner.expiring_sessions().await
    }

    async fn all_sessions(&self) -> Result<mqtt_storage::SessionScan, mqtt_storage::StorageError> {
        self.inner.all_sessions().await
    }
}
