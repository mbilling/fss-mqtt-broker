#![no_main]
//! Fuzz target: the peer-bus frame decoder must never panic on arbitrary bytes.
//!
//! `peer::decode` runs on every inbound peer link — a length prefix followed by
//! a `bincode` body an authenticated-but-possibly-buggy-or-hostile peer sent.
//! The only safety invariant here: no input, however malformed, may panic, hang,
//! or read out of bounds. A correct `Err`/`Ok(None)` is success; a panic is a
//! finding. Anything decoded must re-encode without panicking.

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use mqtt_cluster::peer::{decode, encode};

fuzz_target!(|data: &[u8]| {
    let mut buf = BytesMut::from(data);
    // Drain the buffer the way the link pump does, until it stops yielding
    // frames. Must terminate and never panic.
    loop {
        match decode(&mut buf) {
            Ok(Some(msg)) => {
                let mut out = Vec::new();
                let _ = encode(&msg, &mut out);
            }
            Ok(None) | Err(_) => break,
        }
    }
});
