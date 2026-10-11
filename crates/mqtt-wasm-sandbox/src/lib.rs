//! Rule functions as WebAssembly modules, run in a sandbox (ADR 0086).
//!
//! Nothing in the broker depends on this crate yet. It is how EMQX's `jq` rule
//! function can be the real jq — the C library, compiled to WebAssembly — run where it
//! can do no harm, and what the same mechanism needs to carry functions an operator
//! supplies.
//!
//! * [`Sandbox`] validates and compiles a module, refuses any import outside a fixed
//!   list and makes [`Instance`]s of it. An instance is one linear memory; it reaches
//!   nothing of the host but what [`Grants`] names.
//! * [`Instance::call`] runs one function under [`Limits`]: a deadline, a memory cap,
//!   a stack cap, an output cap.
//! * [`jq::Jq`] is the `jq` function on top: EMQX's argument and result rules, its
//!   error tags and texts, its timeout. [`jq::Pool`] shares warm instances between
//!   callers.
//!
//! The runtime is `wasmtime`: Cranelift compiles the module to machine code when it
//! is loaded, and the processor runs that code. This crate has no `unsafe` and uses
//! only the runtime's safe API (no precompiled code is ever read back from disk); the
//! sandbox's soundness rests on the runtime's own `unsafe` and on the code its
//! compiler generates. A host that forbids executable memory cannot load a module:
//! [`LoadError::Engine`].

pub mod jq;
mod sandbox;
#[cfg(test)]
mod tests;
mod timer;
mod wasi;

pub use sandbox::{
    Arg, CallError, Execution, Instance, Limits, LoadError, Reply, Sandbox, ABI_VERSION,
};
pub use wasi::Grants;
