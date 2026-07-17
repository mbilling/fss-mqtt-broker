#![no_main]
//! Fuzz target: opening a gossip datagram must never panic — pre-auth.
//!
//! `SwimAuth::open` is the very first thing a node does with a raw UDP datagram
//! from anyone who can reach the gossip port: length/version checks, the
//! constant-time HMAC gate, and (in the signed postures) certificate-reference
//! parsing. This is the most exposed surface in the broker — no authentication
//! precedes it — so it must reject any bytes with a bounded error, never panic
//! or hang. The fuzzer holds a fixed key; almost every input fails the HMAC
//! (as an attacker's would), exercising the length/version/parse guards that
//! run before and around it.

use libfuzzer_sys::fuzz_target;
use mqtt_cluster::swim_auth::{SwimAuth, KEY_LEN};

fuzz_target!(|data: &[u8]| {
    let auth = SwimAuth::new(&[0x5A; KEY_LEN]);
    let _ = auth.open(data);
});
