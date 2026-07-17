#![no_main]
//! Fuzz target: the SWIM gossip-message decoder must never panic.
//!
//! After a datagram clears the shared-key HMAC (and, in the signed postures,
//! the per-node signature), its payload is `bincode`-decoded into a
//! [`Message`](mqtt_cluster::swim::Message) carrying membership gossip. A
//! leaked cluster key, or a buggy peer, can put arbitrary bytes here — the
//! decode must reject them cleanly, never panic or hang. Fuzzing the decoder
//! directly (past auth) is defence in depth on the deepest gossip parser.

use libfuzzer_sys::fuzz_target;
use mqtt_cluster::swim::Message;

fuzz_target!(|data: &[u8]| {
    let _ = Message::decode(data);
});
