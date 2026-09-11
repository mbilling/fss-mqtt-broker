//! Hold a real startup stage, rather than hoping to win the ready-before-bind race.
use super::*;
use rustix::fs::{mkfifoat, open, Mode, OFlags, CWD};

#[tokio::test]
async fn a_live_process_is_not_ready_while_client_startup_is_held() {
    let root = tempfile::tempdir().unwrap();
    let fifo = root.path().join("credentials");
    mkfifoat(CWD, &fifo, Mode::RUSR | Mode::WUSR).unwrap();
    let path = fifo.to_str().unwrap();
    // Credential loading happens after health is bound but before MQTT is bound.
    // Disable durability here so a lease election cannot mask a missing startup gate.
    let mut node = Standalone::new(
        root.path(),
        "held-startup",
        &[
            ("MQTTD_PASSWORD_FILE", path),
            ("MQTTD_DURABLE_SESSIONS", "0"),
        ],
    );
    node.spawn();
    let writer = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match open(
                &fifo,
                OFlags::WRONLY | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            ) {
                Ok(fd) => break fd,
                Err(error) => {
                    assert_eq!(
                        error,
                        rustix::io::Errno::NXIO,
                        "unexpected FIFO open failure"
                    );
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    })
    .await
    .expect("broker must enter credential loading");
    // A successful nonblocking writer open proves the reader has entered. Keep
    // the writer open without EOF so the broker cannot advance to its MQTT bind.
    let body = http_get(node.health, "/readyz")
        .await
        .expect("health endpoint is live");
    assert!(body.contains("\"live\":true"), "{body}");
    assert!(
        body.contains("\"ready\":false"),
        "startup is held, not ready: {body}"
    );
    assert!(tokio::net::TcpStream::connect(node.client).await.is_err());
    drop(writer); // EOF: an empty credential file is valid; allow startup to finish.
    assert!(
        node.wait_ready(Duration::from_secs(15)).await,
        "{}",
        node.log()
    );
    assert!(tokio::net::TcpStream::connect(node.client).await.is_ok());
}
