//! Authenticator hot-reload over **new** connections (ADR 0032 — T5).
//!
//! The broker's authenticator is a [`PasswordAuthenticator`] supplied through a
//! [`reload::Reloader`] whose `build` closure re-reads a password file from disk. Rotating
//! the file and reloading must take effect on the *next* CONNECT: the new credential
//! authenticates and the old one is rejected — no broker restart. The connection reads the
//! current authenticator per CONNECT, so the swap is live.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use argon2::password_hash::SaltString;
use argon2::{Argon2, PasswordHasher};
use mqtt_auth::password::PasswordAuthenticator;
use mqtt_auth::{AllowAll, Authenticator, Authorizer};
use mqtt_cluster::NodeId;
use mqtt_codec::{packet::Connect, Packet, ProtocolVersion};
use mqtt_storage::MemorySessionStore;
use mqttd::{reload, Hub};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

const V4: ProtocolVersion = ProtocolVersion::V311;
const RECV_TIMEOUT: Duration = Duration::from_millis(300);

/// Produce a real Argon2id PHC hash for `password` (fixed salt → deterministic test).
fn hash(password: &str) -> String {
    let salt = SaltString::encode_b64(b"fixed-salt-bytes").expect("valid salt");
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .expect("hash")
        .to_string()
}

/// A self-removing temp file holding the `username:phc-hash` password file.
struct PwFile {
    path: PathBuf,
}

impl PwFile {
    fn new(line: &str) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("mqttd-pwreload-{}-{n}.txt", std::process::id()));
        std::fs::write(&path, line).unwrap();
        PwFile { path }
    }

    fn write(&self, line: &str) {
        std::fs::write(&self.path, line).unwrap();
    }
}

impl Drop for PwFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Start a broker whose authenticator is reloaded from `pw_path`. Returns the address and
/// the reloader that re-reads the file on `reload()`.
async fn start_reloadable_node(pw_path: PathBuf) -> (SocketAddr, reload::Reloader) {
    let build = move || -> reload::BuildResult {
        let text = std::fs::read_to_string(&pw_path).map_err(|e| format!("read pw: {e}"))?;
        let auth = PasswordAuthenticator::from_file_contents(&text)
            .map_err(|e| format!("parse pw: {e}"))?;
        Ok((
            Arc::new(AllowAll) as Arc<dyn Authorizer>,
            Arc::new(auth) as Arc<dyn Authenticator>,
        ))
    };
    let initial = build().expect("the initial password file builds");
    let audit = Arc::new(mqtt_observability::AuditLog::new());
    let (mut reloader, handles) = reload::Reloader::new(initial, audit, build);

    let (hub, hub_tx) = Hub::with_config(
        NodeId("pwreload-node".into()),
        std::sync::Arc::new(MemorySessionStore::new()),
    );
    tokio::spawn(hub.run());
    // Revocation reaches live state (ADR 0040 T2): a successful reload sweeps the
    // online table against the new credential store.
    reloader.attach_identity_sweep(hub_tx.clone(), None);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            let conn_policy = Arc::new(mqttd::conn::ConnPolicy {
                auth: handles.auth.clone(),
                authz: handles.authz.clone(),
                audit: Arc::new(mqtt_observability::AuditLog::new()),
                proxy: None,
                store: None,
                connect_timeout: Duration::from_secs(10),
                shutdown: None,
                metrics: None,
                enhanced: None,
            });
            let hub = hub_tx.clone();
            tokio::spawn(async move {
                mqttd::conn::handle_stream(stream, Some(peer), None, conn_policy, hub).await;
            });
        }
    });
    (addr, reloader)
}

/// CONNECT with `username`/`password`; return the CONNACK return code (or panic on a
/// non-CONNACK / no reply).
async fn connect_code(addr: SocketAddr, username: &str, password: &str) -> u8 {
    let (rh, wh) = TcpStream::connect(addr).await.unwrap().into_split();
    let mut reader = mqtt_net::FrameReader::new(rh, V4);
    let mut writer = mqtt_net::FrameWriter::new(wh, V4);
    writer
        .send(&Packet::Connect(Connect {
            properties: mqtt_codec::Properties::new(),
            protocol: V4,
            clean_session: true,
            keep_alive: 30,
            client_id: "c".to_string(),
            last_will: None,
            username: Some(username.to_string()),
            password: Some(password.as_bytes().to_vec().into()),
        }))
        .await
        .unwrap();
    match timeout(RECV_TIMEOUT, reader.next_packet()).await {
        Ok(Ok(Some(Packet::ConnAck(ack)))) => ack.code,
        other => panic!("expected CONNACK, got {other:?}"),
    }
}

/// MQTT 3.1.1 CONNACK return code 4: bad user name or password.
const BAD_CREDENTIALS: u8 = 0x04;

/// Rotating the password file and reloading takes effect on the next CONNECT: the new
/// password authenticates, the old one is rejected — no restart.
#[tokio::test]
async fn rotated_password_file_reload_authenticates_the_new_credential() {
    let pw = PwFile::new(&format!("alice:{}", hash("secret-one")));
    let (addr, reloader) = start_reloadable_node(pw.path.clone()).await;

    // Before the reload: the original password authenticates; a wrong one is rejected.
    assert_eq!(connect_code(addr, "alice", "secret-one").await, 0x00);
    assert_eq!(
        connect_code(addr, "alice", "secret-two").await,
        BAD_CREDENTIALS
    );

    // Rotate the password and reload.
    pw.write(&format!("alice:{}", hash("secret-two")));
    assert!(
        reloader.reload("signal"),
        "a valid rotated password file must apply"
    );

    // After the reload: the new password authenticates; the old one is now rejected.
    assert_eq!(connect_code(addr, "alice", "secret-two").await, 0x00);
    assert_eq!(
        connect_code(addr, "alice", "secret-one").await,
        BAD_CREDENTIALS,
        "the rotated-out password must no longer authenticate"
    );
}

/// A malformed password file is rejected; the running authenticator is kept, so the
/// existing credential still authenticates (never fail open, never lock everyone out).
#[tokio::test]
async fn malformed_password_file_reload_is_rejected_and_keeps_the_running_authenticator() {
    let pw = PwFile::new(&format!("alice:{}", hash("secret-one")));
    let (addr, reloader) = start_reloadable_node(pw.path.clone()).await;
    assert_eq!(connect_code(addr, "alice", "secret-one").await, 0x00);

    // A line with no ':' separator is a parse error.
    pw.write("this-line-has-no-separator");
    assert!(
        !reloader.reload("signal"),
        "a malformed password file must be rejected, not applied"
    );

    // The running authenticator is unchanged: the original credential still works.
    assert_eq!(
        connect_code(addr, "alice", "secret-one").await,
        0x00,
        "the kept authenticator must still accept the existing credential"
    );
}

/// ADR 0040 T2 — the identity sweep: deleting a user from the password file and
/// reloading evicts that user's **live** session with no client action, while a
/// still-present user's session keeps flowing (its next operation succeeds).
#[tokio::test]
async fn removing_a_password_user_evicts_their_live_session() {
    let pw = PwFile::new(&format!(
        "alice:{}\nbob:{}",
        hash("alice-pw"),
        hash("bob-pw")
    ));
    let (addr, reloader) = start_reloadable_node(pw.path.clone()).await;

    // Both users connect and hold their sessions open.
    let mut bob = connected_session(addr, "bob", "bob-pw").await;
    let mut alice = connected_session(addr, "alice", "alice-pw").await;

    // Remove bob and reload: his live session is evicted, alice's is untouched.
    pw.write(&format!("alice:{}", hash("alice-pw")));
    assert!(reloader.reload("signal"), "the rotated file should reload");

    assert!(
        timeout(RECV_TIMEOUT, bob.reader.next_packet())
            .await
            .ok()
            .and_then(Result::ok)
            .flatten()
            .is_none(),
        "the removed user's live session must be closed by the sweep"
    );

    // Alice still holds a working session: a PINGREQ gets a PINGRESP.
    alice.writer.send(&Packet::PingReq).await.unwrap();
    match timeout(RECV_TIMEOUT, alice.reader.next_packet()).await {
        Ok(Ok(Some(Packet::PingResp))) => {}
        other => panic!("the surviving user must keep flowing, got {other:?}"),
    }
}

/// A held-open authenticated session for the sweep tests.
struct Session {
    reader: mqtt_net::FrameReader<tokio::net::tcp::OwnedReadHalf>,
    writer: mqtt_net::FrameWriter<tokio::net::tcp::OwnedWriteHalf>,
}

/// CONNECT with `username`/`password` and keep the session open (unlike
/// [`connect_code`], which drops it after the CONNACK).
async fn connected_session(addr: SocketAddr, username: &str, password: &str) -> Session {
    let (rh, wh) = TcpStream::connect(addr).await.unwrap().into_split();
    let mut s = Session {
        reader: mqtt_net::FrameReader::new(rh, V4),
        writer: mqtt_net::FrameWriter::new(wh, V4),
    };
    s.writer
        .send(&Packet::Connect(Connect {
            properties: mqtt_codec::Properties::new(),
            protocol: V4,
            clean_session: true,
            keep_alive: 30,
            client_id: format!("c-{username}"),
            last_will: None,
            username: Some(username.to_string()),
            password: Some(password.as_bytes().to_vec().into()),
        }))
        .await
        .unwrap();
    match timeout(RECV_TIMEOUT, s.reader.next_packet()).await {
        Ok(Ok(Some(Packet::ConnAck(ack)))) if ack.code == 0 => s,
        other => panic!("expected CONNACK 0x00, got {other:?}"),
    }
}
