#![no_main]
//! Fuzz target: the decoder must never panic on arbitrary bytes.
//!
//! Decoding attacker-controlled input is the highest-risk operation in the
//! broker. This target asserts the only invariant that matters for safety here:
//! no input, however malformed, may cause a panic, hang, or out-of-bounds access.
//! A correct `Err` is success; a panic is a finding.

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use mqtt_codec::{Packet, ProtocolVersion};

fuzz_target!(|data: &[u8]| {
    // Try both protocol versions; neither may panic.
    for version in [ProtocolVersion::V311, ProtocolVersion::V5] {
        let mut buf = BytesMut::from(data);
        // Drain the buffer the way the network loop would, until it stops
        // yielding packets. Must terminate and never panic.
        loop {
            match Packet::decode(&mut buf, version) {
                Ok(Some(packet)) => {
                    // Anything we decode must re-encode without panicking.
                    let mut out = Vec::new();
                    let _ = packet.encode(&mut out, version);
                }
                Ok(None) | Err(_) => break,
            }
        }
    }
});
