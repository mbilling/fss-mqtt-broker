//! TLS material hot-reload (ADR 0032 — T6).
//!
//! The TLS listener reads its acceptor from a [`reload::Reloader`]-owned `watch` channel
//! per accept, so a SIGHUP that renews the cert/key is served on the **next** handshake —
//! while **in-flight** TLS sessions, already past their handshake, keep flowing. The test
//! uses two independent CAs so the served leaf is unambiguously observable: a connector
//! trusting CA-A succeeds only while cert-A is served, and CA-B only after the reload.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use mqtt_auth::{Authenticator, Authorizer};
use mqtt_cluster::NodeId;
use mqtt_codec::{
    packet::{Connect, Publish, Subscribe, SubscribeFilter},
    Packet, ProtocolVersion, QoS,
};
use mqtt_storage::MemorySessionStore;
use mqttd::{reload, Hub};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_rustls::TlsConnector;

const V4: ProtocolVersion = ProtocolVersion::V311;

/// One self-signed CA plus a `127.0.0.1` server leaf, as PEM strings.
struct Pem {
    ca: String,
    cert: String,
    key: String,
}

/// Mint a fresh CA + server leaf for `127.0.0.1`. Each call is an independent trust root.
fn gen_pki() -> Pem {
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let mut leaf_params = rcgen::CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
    leaf_params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
    let leaf_cert = leaf_params.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();

    Pem {
        ca: ca_cert.pem(),
        cert: leaf_cert.pem(),
        key: leaf_key.serialize_pem(),
    }
}

/// A client TLS connector that trusts exactly `ca_pem`.
fn connector_trusting(ca_pem: &str) -> TlsConnector {
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(ca_pem.as_bytes()) {
        roots.add(cert.unwrap()).unwrap();
    }
    // Explicit `ring` provider (matches mqtt-net::tls and the other TLS tests).
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
}

/// A self-removing temp dir holding the live `cert.pem` / `key.pem` the broker reloads.
struct LiveDir {
    dir: PathBuf,
}

impl LiveDir {
    fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("mqttd-tlsreload-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        LiveDir { dir }
    }

    fn cert(&self) -> PathBuf {
        self.dir.join("cert.pem")
    }

    fn key(&self) -> PathBuf {
        self.dir.join("key.pem")
    }

    /// Install `pki`'s leaf cert + key into the live paths.
    fn install(&self, pki: &Pem) {
        std::fs::write(self.cert(), &pki.cert).unwrap();
        std::fs::write(self.key(), &pki.key).unwrap();
    }
}

impl Drop for LiveDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Start a TLS node whose acceptor is reloaded from `cert`/`key`. Returns its address and
/// the reloader (its `reload()` re-reads the PEM files and swaps the served acceptor).
async fn start_reloadable_tls_node(cert: PathBuf, key: PathBuf) -> (SocketAddr, reload::Reloader) {
    // A fixed permissive policy; this test only reloads the TLS material, but `reload()`
    // rebuilds the policy too (validate-before-swap is all-or-nothing), so the closure
    // returns the same permissive pair every time.
    let make_policy = || -> reload::BuildResult {
        Ok((
            Arc::new(mqtt_auth::AllowAll) as Arc<dyn Authorizer>,
            Arc::new(mqtt_auth::basic::BasicAuthenticator {
                allow_anonymous: true,
            }) as Arc<dyn Authenticator>,
        ))
    };
    let audit = Arc::new(mqtt_observability::AuditLog::new());
    let (mut reloader, handles) = reload::Reloader::new(make_policy().unwrap(), audit, make_policy);

    let acceptor = mqtt_net::tls::server_acceptor(&cert, &key, None).expect("initial acceptor");
    let acceptor_rx = reloader.attach_tls(acceptor, move || {
        mqtt_net::tls::server_acceptor(&cert, &key, None).map_err(|e| e.to_string())
    });

    let (hub, hub_tx) = Hub::with_config(
        NodeId("tlsreload-node".into()),
        std::sync::Arc::new(MemorySessionStore::new()),
    );
    tokio::spawn(hub.run());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            // Read the current acceptor per accept — a reload is served on the next handshake.
            let acceptor = acceptor_rx.borrow().clone();
            let auth = mqttd::conn::auth_handle(handles.auth.borrow().clone());
            let authz = mqttd::conn::authz_handle(handles.authz.borrow().clone());
            let hub = hub_tx.clone();
            tokio::spawn(async move {
                if let Ok(tls) = acceptor.accept(stream).await {
                    let policy = Arc::new(mqttd::conn::ConnPolicy {
                        auth,
                        authz,
                        audit: Arc::new(mqtt_observability::AuditLog::new()),
                        proxy: None,
                        store: None,
                        connect_timeout: Duration::from_secs(10),
                        shutdown: None,
                        metrics: None,
                        enhanced: None,
                    });
                    mqttd::conn::handle_stream(tls, Some(peer), None, policy, hub).await;
                }
            });
        }
    });
    (addr, reloader)
}

/// An MQTT client over an established TLS stream.
struct Client<S> {
    reader: mqtt_net::FrameReader<tokio::io::ReadHalf<S>>,
    writer: mqtt_net::FrameWriter<tokio::io::WriteHalf<S>>,
}

impl<S: AsyncRead + AsyncWrite> Client<S> {
    async fn connect(stream: S, id: &str) -> Self {
        let (rh, wh) = tokio::io::split(stream);
        let mut c = Client {
            reader: mqtt_net::FrameReader::new(rh, V4),
            writer: mqtt_net::FrameWriter::new(wh, V4),
        };
        c.writer
            .send(&Packet::Connect(Connect {
                properties: mqtt_codec::Properties::new(),
                protocol: V4,
                clean_session: true,
                keep_alive: 30,
                client_id: id.to_string(),
                last_will: None,
                username: None,
                password: None,
            }))
            .await
            .unwrap();
        assert!(matches!(c.recv().await, Some(Packet::ConnAck(_))));
        c
    }

    async fn subscribe(&mut self, filter: &str) {
        self.writer
            .send(&Packet::Subscribe(Subscribe {
                properties: mqtt_codec::Properties::new(),
                pkid: 1,
                filters: vec![SubscribeFilter {
                    options: mqtt_codec::SubscriptionOptions::default(),
                    path: filter.to_string(),
                    qos: QoS::AtMostOnce,
                }],
            }))
            .await
            .unwrap();
        assert!(matches!(self.recv().await, Some(Packet::SubAck(_))));
    }

    async fn publish(&mut self, topic: &str, payload: &'static [u8]) {
        self.writer
            .send(&Packet::Publish(Publish {
                properties: mqtt_codec::Properties::new(),
                dup: false,
                qos: QoS::AtMostOnce,
                retain: false,
                topic: topic.to_string(),
                pkid: None,
                payload: bytes::Bytes::from_static(payload),
            }))
            .await
            .unwrap();
    }

    async fn recv(&mut self) -> Option<Packet> {
        timeout(Duration::from_millis(300), self.reader.next_packet())
            .await
            .ok()
            .and_then(Result::ok)
            .flatten()
    }
}

/// Try a TLS handshake + MQTT CONNECT; return whether a CONNACK came back (i.e. the served
/// cert validated against the connector's trusted CA and a session formed).
async fn mqtt_connects(addr: SocketAddr, connector: &TlsConnector) -> bool {
    let Ok(tcp) = TcpStream::connect(addr).await else {
        return false;
    };
    let name = ServerName::try_from("127.0.0.1").unwrap();
    let Ok(tls) = connector.connect(name, tcp).await else {
        return false; // server cert not trusted → handshake refused
    };
    let (rh, wh) = tokio::io::split(tls);
    let mut writer = mqtt_net::FrameWriter::new(wh, V4);
    if writer
        .send(&Packet::Connect(Connect {
            properties: mqtt_codec::Properties::new(),
            protocol: V4,
            clean_session: true,
            keep_alive: 30,
            client_id: "probe".into(),
            last_will: None,
            username: None,
            password: None,
        }))
        .await
        .is_err()
    {
        return false;
    }
    let mut reader = mqtt_net::FrameReader::new(rh, V4);
    matches!(
        timeout(Duration::from_millis(500), reader.next_packet()).await,
        Ok(Ok(Some(Packet::ConnAck(_))))
    )
}

/// A renewed cert/key is served on the next handshake; an in-flight TLS session survives.
#[tokio::test]
async fn renewed_cert_is_served_on_the_next_handshake() {
    let a = gen_pki();
    let b = gen_pki();
    let live = LiveDir::new();
    live.install(&a);

    let (addr, reloader) = start_reloadable_tls_node(live.cert(), live.key()).await;
    let trust_a = connector_trusting(&a.ca);
    let trust_b = connector_trusting(&b.ca);

    // Before the reload: cert-A is served. CA-A validates; CA-B does not.
    let mut inflight = Client::connect(
        tls_stream(addr, &trust_a)
            .await
            .expect("CA-A trusts cert-A"),
        "inflight",
    )
    .await;
    inflight.subscribe("t").await;
    assert!(
        !mqtt_connects(addr, &trust_b).await,
        "cert-A must not validate against CA-B before the reload"
    );

    // Renew the served material to cert-B and reload.
    live.install(&b);
    assert!(
        reloader.reload("signal"),
        "a valid renewed cert/key must apply"
    );

    // After the reload: cert-B is served. CA-B now validates; CA-A no longer does.
    assert!(
        mqtt_connects(addr, &trust_b).await,
        "the renewed cert-B must be served on the next handshake"
    );
    assert!(
        !mqtt_connects(addr, &trust_a).await,
        "the rotated-out cert-A must no longer be served"
    );

    // The in-flight session (handshaked under cert-A) is undisturbed: traffic still flows.
    inflight.publish("t", b"still-connected").await;
    match inflight.recv().await {
        Some(Packet::Publish(p)) => assert_eq!(&p.payload[..], b"still-connected"),
        other => panic!("the in-flight TLS session must keep flowing, got {other:?}"),
    }
}

/// A malformed cert/key file is rejected; the running acceptor is kept serving cert-A.
#[tokio::test]
async fn malformed_cert_reload_is_rejected_and_keeps_serving() {
    let a = gen_pki();
    let live = LiveDir::new();
    live.install(&a);

    let (addr, reloader) = start_reloadable_tls_node(live.cert(), live.key()).await;
    let trust_a = connector_trusting(&a.ca);
    assert!(mqtt_connects(addr, &trust_a).await);

    // Corrupt the cert file and reload — the acceptor build must fail.
    std::fs::write(live.cert(), "-----BEGIN CERTIFICATE-----\ngarbage\n").unwrap();
    assert!(
        !reloader.reload("signal"),
        "a malformed cert must be rejected, not applied"
    );

    // The kept acceptor still serves the original cert-A.
    assert!(
        mqtt_connects(addr, &trust_a).await,
        "the kept acceptor must keep serving the original cert"
    );
}

async fn tls_stream(
    addr: SocketAddr,
    connector: &TlsConnector,
) -> std::io::Result<impl AsyncRead + AsyncWrite> {
    let tcp = TcpStream::connect(addr).await?;
    let name = ServerName::try_from("127.0.0.1").unwrap();
    connector.connect(name, tcp).await
}
