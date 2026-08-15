//! Shared test support for mqttui's integration suites.
//!
//! mqttui is a separate workspace with no lib target (ADR 0056 §1), so its integration tests
//! cannot import the broker's copy of anything. `skip.rs` is therefore a byte-identical
//! duplicate of `crates/mqttd/tests/common/skip.rs`, and `scripts/check-test-hygiene.py`
//! compares them so one copy cannot quietly stop being fatal.

pub mod skip;
