//! Rule functions as WebAssembly modules, run in a sandbox (ADR 0086).
//!
//! It is how EMQX's `jq` rule function is the real jq — the C library, compiled to
//! WebAssembly — run where it can do no harm (`mqtt-rules` calls it through
//! [`jq::Pool`]), and what the same mechanism needs to carry functions an operator
//! supplies.
//!
//! * [`Sandbox`] validates and compiles a module, refuses any import outside a fixed
//!   list and makes [`Instance`]s of it. An instance is one linear memory; it reaches
//!   nothing of the host but what [`Grants`] names.
//! * [`Instance::call`] runs one function under [`Limits`]: a deadline, a memory cap,
//!   a stack cap, an output cap.
//! * [`ModulePool`] is what a broker holds of a module an operator supplied: the
//!   compiled module and the warm instances its callers take turns on.
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
mod pool;
mod sandbox;
#[cfg(any(test, feature = "test-module"))]
pub mod test_module;
#[cfg(test)]
mod tests;
mod timer;
mod wasi;

pub use pool::{ModulePool, ModuleStats};
pub use sandbox::{
    Arg, CallError, Execution, HandOff, Instance, Limits, LoadError, Reply, Sandbox, ABI_VERSION,
};
pub use wasi::Grants;
