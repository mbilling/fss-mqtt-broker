//! A tiny MQTT-over-QUIC demo client (ADR 0036) for the demo stack.
//!
//! Browsers cannot speak MQTT-over-QUIC, so this stand-in shows the *native* QUIC path: it
//! connects to the cluster over QUIC (TLS 1.3 + mTLS, ALPN `mqtt`), then publishes a steady
//! stream of messages **across several QUIC data streams** — multi-stream in action. The
//! messages flow through the mqttd cluster, so a browser (over WebSocket) subscribed to
//! `quic/demo/#` sees them, and Grafana's "accepts by listener" shows the `quic` connection.
//!
//! Env: `QUIC_TARGET` (host:port), `QUIC_SERVERNAME` (cert SAN to verify), `QUIC_CA`,
//! `QUIC_CERT`, `QUIC_KEY` (PEM paths), `QUIC_STREAMS` (data streams, default 3),
//! `QUIC_INTERVAL_MS` (per-publish delay, default 500), `QUIC_MIGRATE_MS` (if >0, rebind the
//! client's UDP socket every N ms to simulate a network path change — connection migration,
//! ADR 0036 §3b; the broker logs the migration and `mqttd_quic_path_migrations_total` ticks while
//! this same feed keeps flowing, with no reconnect).

use std::sync::Arc;
use std::time::Duration;

use mqtt_codec::{
    packet::{Connect, Publish},
    Packet, ProtocolVersion, QoS,
};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

const V4: ProtocolVersion = ProtocolVersion::V311;

fn env(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn pem_certs(path: &str) -> Vec<CertificateDer<'static>> {
    CertificateDer::pem_file_iter(path)
        .expect("read cert PEM")
        .collect::<Result<_, _>>()
        .expect("parse cert PEM")
}

#[tokio::main]
async fn main() {
    let target = env("QUIC_TARGET", "127.0.0.1:1894");
    let server_name = env("QUIC_SERVERNAME", "mqttd-1");
    let ca = env("QUIC_CA", "/certs/ca.pem");
    let cert = env("QUIC_CERT", "/certs/client.pem");
    let key = env("QUIC_KEY", "/certs/client.key");
    let n_streams: usize = env("QUIC_STREAMS", "3").parse().unwrap_or(3);
    let interval = Duration::from_millis(env("QUIC_INTERVAL_MS", "500").parse().unwrap_or(500));
    // 0 disables migration; otherwise rebind the client socket every N ms (a new source port →
    // the broker sees the same connection from a new path = connection migration).
    let migrate_ms: u64 = env("QUIC_MIGRATE_MS", "0").parse().unwrap_or(0);

    // QUIC client config: trust the demo CA, present the client cert (mTLS), ALPN `mqtt`.
    let mut roots = rustls::RootCertStore::empty();
    for c in pem_certs(&ca) {
        roots.add(c).expect("add CA");
    }
    let mut crypto = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_root_certificates(roots)
    .with_client_auth_cert(
        pem_certs(&cert),
        PrivateKeyDer::from_pem_file(&key).expect("key"),
    )
    .expect("client cert");
    crypto.alpn_protocols = vec![b"mqtt".to_vec()];

    let qcc = quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap();
    let mut endpoint =
        quinn::Endpoint::client("0.0.0.0:0".parse().unwrap()).expect("client endpoint");
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(qcc)));

    let addr = loop {
        if let Some(a) = tokio::net::lookup_host(&target)
            .await
            .ok()
            .and_then(|mut it| it.next())
        {
            break a;
        }
        eprintln!("quic-demo: resolving {target}…");
        tokio::time::sleep(Duration::from_secs(1)).await;
    };

    // Reconnect forever (the cluster may not be up yet, or a node may restart).
    loop {
        if let Err(e) = run(
            &endpoint,
            addr,
            &server_name,
            n_streams,
            interval,
            migrate_ms,
        )
        .await
        {
            eprintln!("quic-demo: session ended ({e}); reconnecting in 2s");
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}

/// Rebind the endpoint's UDP socket to a fresh ephemeral port. quinn migrates the live
/// connection to it — the next packet leaves from a new source address, which the broker sees as
/// a path migration on the *same* connection (no reconnect). The same mechanism real clients hit
/// on a Wi-Fi↔cellular handover or a NAT rebind.
fn rebind(endpoint: &quinn::Endpoint) -> std::io::Result<()> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
    endpoint.rebind(socket)
}

async fn run(
    endpoint: &quinn::Endpoint,
    addr: std::net::SocketAddr,
    server_name: &str,
    n_streams: usize,
    interval: Duration,
    migrate_ms: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = endpoint.connect(addr, server_name)?.await?;
    eprintln!("quic-demo: connected to {addr} over QUIC (mTLS)");

    // CONNECT on the control stream.
    let (mut ctrl_send, ctrl_recv) = conn.open_bi().await?;
    let mut connect = Vec::new();
    Packet::Connect(Connect {
        properties: mqtt_codec::Properties::new(),
        protocol: V4,
        clean_session: true,
        keep_alive: 30,
        client_id: "quic-demo".into(),
        last_will: None,
        username: None,
        password: None,
    })
    .encode(&mut connect, V4)?;
    ctrl_send.write_all(&connect).await?;
    // Read the CONNACK (4 bytes for v3.1.1) to confirm the session.
    let mut recv = ctrl_recv;
    let mut connack = [0u8; 4];
    recv.read_exact(&mut connack).await?;
    eprintln!("quic-demo: CONNACK received; publishing across {n_streams} QUIC data streams");

    // Open the data streams up front; round-robin publishes across them (multi-stream).
    let mut streams = Vec::new();
    for _ in 0..n_streams.max(1) {
        let (send, _recv) = conn.open_bi().await?;
        streams.push(send);
    }

    let mut tick: u64 = 0;
    let mut s: usize = 0; // round-robin stream index
    let mut since_migrate = Duration::ZERO;
    loop {
        let topic = format!("quic/demo/stream{s}/tick");
        let payload = format!("tick {tick} via QUIC data stream {s}");
        let mut bytes = Vec::new();
        Packet::Publish(Publish {
            properties: mqtt_codec::Properties::new(),
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic,
            pkid: None,
            payload: bytes::Bytes::from(payload.into_bytes()),
        })
        .encode(&mut bytes, V4)?;
        streams[s].write_all(&bytes).await?;
        s = (s + 1) % streams.len();
        tick += 1;
        tokio::time::sleep(interval).await;

        // Periodically shift the network path under the live connection (migration).
        if migrate_ms > 0 {
            since_migrate += interval;
            if since_migrate >= Duration::from_millis(migrate_ms) {
                since_migrate = Duration::ZERO;
                match rebind(endpoint) {
                    Ok(()) => {
                        let local = endpoint
                            .local_addr()
                            .map_or_else(|_| "?".into(), |a| a.to_string());
                        eprintln!(
                            "quic-demo: rebound to {local} — migrating the live connection (same session)"
                        );
                    }
                    Err(e) => eprintln!("quic-demo: rebind failed: {e}"),
                }
            }
        }
    }
}
