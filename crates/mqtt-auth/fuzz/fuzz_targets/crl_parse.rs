#![no_main]
//! Fuzz target: the certificate-revocation-list decoder must never panic.
//!
//! `RevocationList::from_bytes_unverified` parses a DER CRL from a file an
//! operator points the broker at and hot-reloads (ADR 0022 T7 / ADR 0040) —
//! untrusted input to a byte parser. A malformed or hostile CRL must be
//! rejected with a bounded `Err`, never a panic, hang, or OOB read.

use libfuzzer_sys::fuzz_target;
use mqtt_auth::signed_gossip::RevocationList;

fuzz_target!(|data: &[u8]| {
    let _ = RevocationList::from_bytes_unverified(data);
});
