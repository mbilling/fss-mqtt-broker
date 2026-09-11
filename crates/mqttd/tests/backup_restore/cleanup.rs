//! Failed setup must not leave a broker behind (#597).
use super::*;
use std::panic::{catch_unwind, AssertUnwindSafe};

async fn assert_reaped(raw: u32) {
    let pid = rustix::process::Pid::from_raw(i32::try_from(raw).unwrap()).unwrap();
    let reaped = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rustix::process::test_kill_process(pid) {
                Ok(()) => tokio::task::yield_now().await,
                Err(error) => {
                    assert_eq!(error, rustix::io::Errno::SRCH);
                    break;
                }
            }
        }
    })
    .await;
    // A regression in the guard must fail this test without itself leaving a
    // running broker behind (also keeps deliberate mutation checks contained).
    if reaped.is_err() {
        let _ = rustix::process::kill_process(pid, rustix::process::Signal::KILL);
    }
    reaped.expect("failed setup must kill and reap its broker child");
}

#[tokio::test]
async fn standalone_is_reaped_when_setup_panics() {
    let root = tempfile::tempdir().unwrap();
    let mut pid = None;
    let failed = catch_unwind(AssertUnwindSafe(|| {
        let mut node = Standalone::new(root.path(), "unwind", &[]);
        node.spawn();
        pid = Some(node.pid());
        panic!("intentional setup failure");
    }));
    assert!(failed.is_err());
    assert_reaped(pid.unwrap()).await;
}

#[tokio::test]
async fn partially_constructed_cluster_is_reaped_when_setup_panics() {
    let root = tempfile::tempdir().unwrap();
    let mut nodes = build_topology(597, root.path()).await;
    nodes[0].spawn();
    let pid = nodes[0].pid().unwrap();
    let failed = catch_unwind(AssertUnwindSafe(move || {
        let _owned_nodes = nodes;
        panic!("intentional failure before wait_all_ready/proc_over");
    }));
    assert!(failed.is_err());
    assert_reaped(pid).await;
}
