//! A v5 client that asks to resume (`clean_start = false`) with Session Expiry
//! Interval 0 is served by whichever node of a settled cluster it reaches.
//!
//! MQTT 5 §3.1.2.11.2 allows the pair: resume the session if there is one, and end it
//! when this connection closes. On a 3-node durable cluster a node that does not own
//! the client id's placement group answers it CONNACK 0x88 (Server unavailable) after
//! a 5 s wait, every time, for a session that does not even exist. The owner node
//! accepts it at once.
//!
//! The mechanism, in two halves that disagree about what "persistent" means:
//! - `conn.rs` relocates a session to its owner (ADR 0005, `relocation_target`) only
//!   when it outlives the connection — `session_expiry != 0` — so this one stays on
//!   the node it reached;
//! - the hub (`Hub::attach`) runs the durable recovery for every `!clean_start` attach,
//!   and that recovery (`recover_until_ready`: `claim_session`, `subscriptions`,
//!   `pending`) reads groups this node does not own. The reads fail transiently until
//!   `ATTACH_RECOVERY_TIMEOUT` (5 s), and the attach is refused `Unavailable`.
//!
//! The same refusal is what a `clean_start = false` client with a NON-zero expiry gets
//! in the first seconds after cluster start, when `relocation_target` cannot name an
//! owner yet and the session is served locally on a non-owner.

mod common;
mod proc_common;

use std::time::Instant;

use common::Client;
use proc_common::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resume_with_session_expiry_zero_is_served_by_every_node() {
    let seed = 0x0E59;
    let disk = tempfile::tempdir().expect("tempdir");
    let mut nodes = build_topology(seed, disk.path()).await;
    for n in &mut nodes {
        n.spawn();
    }
    wait_all_ready(&mut nodes, seed).await;

    // Several ids, each tried on every node: whichever node owns an id, the other two
    // do not, so every id exercises the non-owner path twice.
    let mut refused = Vec::new();
    let mut tried = 0;
    for k in 0..4 {
        for (i, n) in nodes.iter().enumerate() {
            let id = format!("resume0-{k}");
            let started = Instant::now();
            // No Session Expiry Interval property: the v5 default, 0.
            let (mut c, ack) = Client::connect_v5(n.client_addr, &id, false, vec![]).await;
            tried += 1;
            if ack.code == 0 {
                c.disconnect().await;
            } else {
                refused.push(format!(
                    "{id} on node {i} ({}): CONNACK 0x{:02x} after {:?}",
                    n.id,
                    ack.code,
                    started.elapsed()
                ));
            }
        }
    }
    assert!(
        refused.is_empty(),
        "{} of {tried} clean_start=false, expiry-0 CONNECTs to a ready cluster were \
         refused:\n{}",
        refused.len(),
        refused.join("\n")
    );
}
