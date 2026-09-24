//! Correctness gates for the #613 measurement fixture, not timing thresholds.
#[path = "../benches/shared_membership/rig.rs"]
mod rig;
#[path = "../benches/shared_membership/tcp.rs"]
mod tcp;
// This integration binary exercises the accounting parser; the paced runner is
// invoked manually, not turned into a CI throughput threshold.
#[allow(dead_code)]
#[path = "../benches/shared_membership/paced.rs"]
mod paced;

use mqtt_cluster::peer::PeerMessage;
use rig::{assert_control_only, validate_receipts, Rig, Shape, BURST};

#[test]
fn membership_fixture_delivers_exactly_once_across_all_arms() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let drain = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    for peers in [0, 2, 4, 9] {
        for (matching, connected) in [(false, false), (true, false), (false, true), (true, true)] {
            let shape = Shape {
                peers,
                matching,
                connected,
            };
            eprintln!("{} peers={peers}", shape.arm());
            rt.block_on(async {
                let mut rig = Rig::new(&drain, shape).await;
                // More than MAX_OUTBOUND_QUEUE per worker in total: omitting the
                // meter decrement cannot accidentally pass this fixture.
                for _ in 0..24 {
                    rig.burst(false).await;
                }
                rig.verify_peers().await;
                rig.burst(true).await;
                rig.verify_peers().await;
            });
        }
    }
}

#[test]
fn membership_tcp_fixture_receives_all_data_before_each_socket_fence() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let endpoints = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    for peers in [0, 2, 9] {
        for (matching, connected) in [(false, false), (true, false), (false, true), (true, true)] {
            rt.block_on(async {
                let shape = Shape {
                    peers,
                    matching,
                    connected,
                };
                let mut rig = tcp::TcpRig::new(&endpoints, shape).await;
                for _ in 0..3 {
                    rig.burst().await;
                }
                rig.verify_peers().await;
            });
        }
    }
}

#[test]
fn membership_receipt_oracle_rejects_loss_duplicates_and_stale_bursts() {
    let valid: Vec<_> = (0..BURST).map(|s| (7, u64::try_from(s).unwrap())).collect();
    validate_receipts(7, &valid);
    let mut reordered = valid.clone();
    reordered.reverse();
    validate_receipts(7, &reordered);
    assert!(std::panic::catch_unwind(|| validate_receipts(7, &valid[..BURST - 1])).is_err());
    let mut duplicate = valid.clone();
    duplicate[BURST - 1] = duplicate[0];
    assert!(std::panic::catch_unwind(|| validate_receipts(7, &duplicate)).is_err());
    let mut stale = valid.clone();
    stale[0].0 = 6;
    assert!(std::panic::catch_unwind(|| validate_receipts(7, &stale)).is_err());
    let mut out_of_range = valid;
    out_of_range[0].1 = u64::try_from(BURST).unwrap();
    assert!(std::panic::catch_unwind(|| validate_receipts(7, &out_of_range)).is_err());
}

#[test]
fn membership_cpu_accounting_handles_parentheses_and_excludes_child_time() {
    let fields = [
        "R", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "123", "456", "999", "888",
    ];
    let line = format!("123 (worker (hot) name) {}", fields.join(" "));
    assert_eq!(paced::parse_ticks(&line), 579);
    assert!(std::panic::catch_unwind(|| paced::parse_ticks("bad stat")).is_err());
    assert!(std::panic::catch_unwind(|| paced::parse_ticks("1 (short) R 1")).is_err());
}

#[cfg(target_os = "linux")]
#[test]
fn membership_perf_ack_requires_complete_confirmation() {
    use std::io::{Error, ErrorKind, Read};
    struct Fragmented(u8);
    impl Read for Fragmented {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.0 += 1;
            match self.0 {
                1 => Err(Error::from(ErrorKind::WouldBlock)),
                2 => Err(Error::from(ErrorKind::Interrupted)),
                3..=7 => {
                    buf[0] = b"ack\n\0"[usize::from(self.0 - 3)];
                    Ok(1)
                }
                _ => Ok(0),
            }
        }
    }
    paced::perf_control::expect_ack(&mut Fragmented(0));
    let mut consecutive = &b"ack\n\0ack\n\0"[..];
    paced::perf_control::expect_ack(&mut consecutive);
    paced::perf_control::expect_ack(&mut consecutive);
    assert!(consecutive.is_empty());
    assert!(
        std::panic::catch_unwind(|| paced::perf_control::expect_ack(&mut &b"bad\n\0"[..])).is_err()
    );
    assert!(std::panic::catch_unwind(|| paced::perf_control::expect_ack(&mut &b"ac"[..])).is_err());
}

#[test]
fn membership_peer_oracle_rejects_forwarding_even_with_local_receipts() {
    assert_control_only(&[PeerMessage::Interest { filters: vec![] }]);
    let ordinary = PeerMessage::Publish {
        topic: "t".into(),
        payload: vec![0; 200],
        qos: 0,
        retain: false,
        message_expiry: None,
        app: mqtt_cluster::peer::WireAppProps::default(),
    };
    assert!(std::panic::catch_unwind(|| assert_control_only(&[ordinary])).is_err());
}
