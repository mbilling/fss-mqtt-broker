//! The HTTP authentication hook against a REAL server (ADR 0004 T16).
//!
//! The unit tests in `http_auth` cover the cache and the URL-scheme guard without a
//! network. What they cannot cover is the part that decides whether the feature works: a
//! real broker, a real HTTP endpoint, a real CONNECT — and, most importantly, what happens
//! when that endpoint stops answering.
//!
//! The hook is the one authenticator whose *failure* behaviour is the feature. Anyone can
//! write one that says yes; the question is what it does when the far end is down, returns
//! a 500, or hangs past the timeout. Each of those has a test here, because each of them
//! is a way a broker could be talked into admitting a client that proved nothing.
//!
//! The server is a hand-rolled `tokio` listener speaking just enough HTTP/1.1 — no test
//! HTTP framework enters the dependency tree for this.

mod common;
mod proc_common;

use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mqtt_codec::{packet::Connect, Packet, ProtocolVersion};
use proc_common::free_tcp_port;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};

const V4: ProtocolVersion = ProtocolVersion::V311;
/// MQTT 3.1.1 CONNACK codes (table 3.1).
const ACCEPTED: u8 = 0x00;
const BAD_CREDENTIALS: u8 = 0x04;
const NOT_AUTHORIZED: u8 = 0x05;

/// How a stub hook should answer.
#[derive(Clone, Copy)]
enum Answer {
    /// `200` with a JSON body carrying groups.
    AllowWithGroups,
    /// `401`.
    Deny,
    /// `500` — the hook is up but broken.
    ServerError,
    /// Accept the connection and never reply, so the client's timeout decides.
    Hang,
}

/// A minimal HTTP server that answers every request the same way, counting requests.
/// Returns its address and the counter.
async fn stub_hook(answer: Answer) -> (SocketAddr, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            counter.fetch_add(1, Ordering::Relaxed);
            let answer = answer;
            tokio::spawn(async move {
                // Read the request head; we do not care about its content beyond draining
                // enough that the client is not blocked writing.
                let mut buf = [0u8; 4096];
                let _ = socket.read(&mut buf).await;
                let response: &[u8] = match answer {
                    Answer::AllowWithGroups => {
                        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                          Content-Length: 24\r\n\r\n{\"groups\":[\"ops\",\"eu\"]}"
                    }
                    Answer::Deny => b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n",
                    Answer::ServerError => {
                        b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n"
                    }
                    Answer::Hang => {
                        // Never answer. The broker's own timeout must end this.
                        tokio::time::sleep(Duration::from_secs(60)).await;
                        return;
                    }
                };
                let _ = socket.write_all(response).await;
                let _ = socket.flush().await;
            });
        }
    });
    (addr, hits)
}

struct Broker(Child);

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Boot the real binary with the hook pointed at `hook_url`.
async fn start_broker(hook_url: &str, extra: &[(&str, &str)]) -> (Broker, SocketAddr) {
    let client: SocketAddr = format!("127.0.0.1:{}", free_tcp_port()).parse().unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttd"));
    for (k, _) in std::env::vars() {
        if k.starts_with("MQTTD_") {
            cmd.env_remove(k);
        }
    }
    cmd.env("MQTTD_NODE_ID", "hook-node")
        .env("MQTTD_PLAINTEXT_BIND", client.to_string())
        .env("MQTTD_HTTP_AUTH_URL", hook_url)
        // The stub speaks plaintext; the broker refuses that unless told, which is
        // itself covered by a unit test.
        .env("MQTTD_HTTP_AUTH_ALLOW_HTTP", "1")
        .env("RUST_LOG", "off");
    for (k, v) in extra {
        cmd.env(k, v);
    }
    let child = cmd
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn mqttd");
    let broker = Broker(child);
    for _ in 0..300 {
        if TcpStream::connect(client).await.is_ok() {
            return (broker, client);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("mqttd never listened on {client}");
}

/// CONNECT with credentials; return the CONNACK return code.
async fn connack_code(addr: SocketAddr, client_id: &str, user: &str, pass: &str) -> u8 {
    let stream = TcpStream::connect(addr).await.expect("connect");
    let (rh, wh) = stream.into_split();
    let mut reader = mqtt_net::FrameReader::new(rh, V4);
    let mut writer = mqtt_net::FrameWriter::new(wh, V4);
    writer
        .send(&Packet::Connect(Connect {
            properties: mqtt_codec::Properties::new(),
            protocol: V4,
            clean_session: true,
            keep_alive: 30,
            client_id: client_id.to_string(),
            last_will: None,
            username: Some(user.to_string()),
            password: Some(bytes::Bytes::from(pass.as_bytes().to_vec())),
        }))
        .await
        .expect("send CONNECT");
    match tokio::time::timeout(Duration::from_secs(20), reader.next_packet()).await {
        Ok(Ok(Some(Packet::ConnAck(ack)))) => ack.code,
        other => panic!("expected a CONNACK, got {other:?}"),
    }
}

/// The happy path: a `200` admits the client, and the hook really was consulted.
#[tokio::test]
async fn a_200_admits_the_client_and_the_hook_was_actually_asked() {
    let (hook, hits) = stub_hook(Answer::AllowWithGroups).await;
    let (_broker, addr) = start_broker(&format!("http://{hook}/auth"), &[]).await;

    assert_eq!(
        connack_code(addr, "c1", "alice", "s3cret").await,
        ACCEPTED,
        "a 200 from the hook must admit the client"
    );
    assert!(
        hits.load(Ordering::Relaxed) >= 1,
        "the broker must actually have called the hook — otherwise this test proves \
         only that anonymous access is on"
    );
}

/// A `401` refuses. Without this the test above would pass against a broker that admitted
/// everyone regardless of what the hook said.
#[tokio::test]
async fn a_401_refuses_the_client() {
    let (hook, _hits) = stub_hook(Answer::Deny).await;
    let (_broker, addr) = start_broker(&format!("http://{hook}/auth"), &[]).await;

    let code = connack_code(addr, "c1", "alice", "wrong").await;
    assert!(
        code == BAD_CREDENTIALS || code == NOT_AUTHORIZED,
        "a 401 from the hook must refuse the client; got CONNACK {code:#04x}"
    );
}

/// **The feature is the failure behaviour.** A hook that is up but broken has not
/// authenticated anybody, and a `500` is not an acceptance.
#[tokio::test]
async fn a_500_is_a_denial_not_an_acceptance() {
    let (hook, _hits) = stub_hook(Answer::ServerError).await;
    let (_broker, addr) = start_broker(&format!("http://{hook}/auth"), &[]).await;

    let code = connack_code(addr, "c1", "alice", "s3cret").await;
    assert_ne!(
        code, ACCEPTED,
        "a 5xx must never admit a client — an ambiguous answer is not a yes"
    );
}

/// An unreachable hook denies rather than hanging the CONNECT forever or admitting.
#[tokio::test]
async fn an_unreachable_hook_denies() {
    // A port nothing is listening on: connection refused, immediately.
    let dead = free_tcp_port();
    let (_broker, addr) = start_broker(&format!("http://127.0.0.1:{dead}/auth"), &[]).await;

    let code = connack_code(addr, "c1", "alice", "s3cret").await;
    assert_ne!(
        code, ACCEPTED,
        "an unreachable hook must fail CLOSED — it has not authenticated anybody"
    );
}

/// A hook that accepts the connection and never answers must be cut off by the broker's
/// own timeout, and the CONNECT refused. Without a bound here a single hung endpoint
/// would park connections indefinitely.
#[tokio::test]
async fn a_hanging_hook_is_cut_off_by_the_timeout_and_denies() {
    let (hook, _hits) = stub_hook(Answer::Hang).await;
    let (_broker, addr) = start_broker(
        &format!("http://{hook}/auth"),
        &[("MQTTD_HTTP_AUTH_TIMEOUT", "1")],
    )
    .await;

    let started = std::time::Instant::now();
    let code = connack_code(addr, "c1", "alice", "s3cret").await;
    let elapsed = started.elapsed();

    assert_ne!(code, ACCEPTED, "a hook that never answered must not admit");
    assert!(
        elapsed < Duration::from_secs(15),
        "the 1s hook timeout must bound the CONNECT; it took {elapsed:?}"
    );
}

/// Caching is off by default, so every CONNECT consults the hook — which is what makes a
/// revoked credential stop working immediately.
#[tokio::test]
async fn without_caching_every_connect_asks_the_hook() {
    let (hook, hits) = stub_hook(Answer::AllowWithGroups).await;
    let (_broker, addr) = start_broker(&format!("http://{hook}/auth"), &[]).await;

    for i in 0..3 {
        assert_eq!(
            connack_code(addr, &format!("c{i}"), "alice", "s3cret").await,
            ACCEPTED
        );
    }
    assert!(
        hits.load(Ordering::Relaxed) >= 3,
        "with caching off each CONNECT must reach the hook; saw {} calls",
        hits.load(Ordering::Relaxed)
    );
}

/// With caching on, the same credential is not re-asked — the point of the cache.
#[tokio::test]
async fn caching_spares_the_hook_a_repeat_of_the_same_credential() {
    let (hook, hits) = stub_hook(Answer::AllowWithGroups).await;
    let (_broker, addr) = start_broker(
        &format!("http://{hook}/auth"),
        &[("MQTTD_HTTP_AUTH_CACHE_SECS", "60")],
    )
    .await;

    // Same client id, username and password each time: one cache entry.
    for _ in 0..4 {
        assert_eq!(
            connack_code(addr, "same-client", "alice", "s3cret").await,
            ACCEPTED
        );
    }
    let calls = hits.load(Ordering::Relaxed);
    assert!(
        calls < 4,
        "the cache must spare the hook repeats of an identical credential; saw {calls} \
         calls for 4 connects"
    );
    assert!(calls >= 1, "and the first one must still have been asked");
}
