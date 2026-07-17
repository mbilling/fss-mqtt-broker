#![no_main]
//! Fuzz target: the topic-ACL policy parser must never panic.
//!
//! `AclPolicy::from_toml_str` parses a TOML policy file an operator supplies and
//! hot-reloads (ADR 0032/0033) — untrusted input to the authorization core. A
//! malformed policy must be rejected with a bounded `Err` (validate-before-swap
//! keeps the running policy), never a panic. Non-UTF-8 bytes are simply not a
//! policy string; the parser is only reachable through valid UTF-8.

use libfuzzer_sys::fuzz_target;
use mqtt_auth::acl::AclPolicy;

fuzz_target!(|data: &[u8]| {
    if let Ok(text) = std::str::from_utf8(data) {
        let _ = AclPolicy::from_toml_str(text);
    }
});
