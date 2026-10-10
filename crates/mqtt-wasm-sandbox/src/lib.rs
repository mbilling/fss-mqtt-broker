//! Rule functions as WebAssembly modules, run in an interpreter (ADR 0086, Proposed).
//!
//! A spike: nothing in the broker depends on this crate. It answers whether EMQX's
//! `jq` rule function can be the real jq — the C library, compiled to WebAssembly —
//! run where it can do no harm, and what the same mechanism needs to carry functions
//! an operator supplies.
//!
//! * [`Sandbox`] validates a module, refuses any import outside a fixed list and
//!   makes [`Instance`]s of it. An instance is one linear memory; it reaches nothing
//!   of the host but what [`Grants`] names.
//! * [`Instance::call`] runs one function under [`Limits`]: a deadline (fuel in
//!   slices, the clock read between them), a memory cap, a stack cap, an output cap.
//! * [`jq::Jq`] is the `jq` function on top: EMQX's argument and result rules, its
//!   error tags and texts, its timeout.
//!
//! The interpreter is `wasmi`. This crate has no `unsafe`; the module's code is data
//! to the interpreter, never machine code the processor runs.

pub mod jq;
mod sandbox;
#[cfg(test)]
mod tests;
mod wasi;

pub use sandbox::{Arg, CallError, Instance, Limits, LoadError, Reply, Sandbox, ABI_VERSION};
pub use wasi::Grants;
