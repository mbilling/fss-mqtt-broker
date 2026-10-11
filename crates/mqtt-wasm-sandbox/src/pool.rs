//! What a broker holds of one module an operator supplied (ADR 0086 D5, D9): the
//! compiled module and the warm instances its callers take turns on.
//!
//! A caller takes an instance for one call and puts it back. An instance a call
//! stopped (a deadline, the memory or stack limit, a trap) is dropped and the next
//! caller gets a new one; with `fresh_instance` every call gets a new one and none is
//! kept, so no state of the module survives a call.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::sandbox::{Arg, CallError, Instance, Reply, Sandbox};

/// What a [`ModulePool`] holds and has done.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ModuleStats {
    /// Instances the pool holds now: those waiting and those in a call.
    pub instances: u64,
    /// Of those, the ones in a call.
    pub in_call: u64,
    /// Linear memory of the instances, in bytes, as of the end of each one's last
    /// call.
    pub memory_bytes: u64,
    /// Instances made since the module was loaded.
    pub made: u64,
    /// Instances given up because a call stopped them.
    pub replaced: u64,
}

/// A warm instance and the memory the pool has counted for it.
struct Warm {
    instance: Instance,
    accounted: u64,
}

/// One loaded module and its warm instances.
pub struct ModulePool {
    sandbox: Sandbox,
    idle: Mutex<Vec<Warm>>,
    max_idle: usize,
    fresh_instance: bool,
    busy: AtomicU64,
    memory: AtomicU64,
    made: AtomicU64,
    replaced: AtomicU64,
}

impl std::fmt::Debug for ModulePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModulePool")
            .field("max_idle", &self.max_idle)
            .field("fresh_instance", &self.fresh_instance)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

fn bytes(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

impl ModulePool {
    /// A pool over a loaded module, keeping at most `max_idle` warm instances between
    /// calls (a broker's number of worker threads) — or none, with `fresh_instance`.
    #[must_use]
    pub fn new(sandbox: Sandbox, max_idle: usize, fresh_instance: bool) -> Self {
        Self {
            sandbox,
            idle: Mutex::new(Vec::new()),
            max_idle: if fresh_instance { 0 } else { max_idle.max(1) },
            fresh_instance,
            busy: AtomicU64::new(0),
            memory: AtomicU64::new(0),
            made: AtomicU64::new(0),
            replaced: AtomicU64::new(0),
        }
    }

    /// The module.
    #[must_use]
    pub fn sandbox(&self) -> &Sandbox {
        &self.sandbox
    }

    fn idle_instances(&self) -> std::sync::MutexGuard<'_, Vec<Warm>> {
        self.idle.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// What the pool holds and has done so far.
    #[must_use]
    pub fn stats(&self) -> ModuleStats {
        let waiting = bytes(self.idle_instances().len());
        let in_call = self.busy.load(Relaxed);
        ModuleStats {
            instances: waiting + in_call,
            in_call,
            memory_bytes: self.memory.load(Relaxed),
            made: self.made.load(Relaxed),
            replaced: self.replaced.load(Relaxed),
        }
    }

    /// Calls the function `name` with `args` on a warm instance, or on a new one when
    /// every one is in use. `timeout` is how long the call may run; the result may be
    /// at most `max_output` bytes (and never more than the module's own limit).
    pub fn call(
        &self,
        name: &str,
        args: &[Arg<'_>],
        timeout: Duration,
        max_output: u32,
    ) -> Result<Reply, CallError> {
        let taken = self.idle_instances().pop();
        let mut warm = if let Some(warm) = taken {
            warm
        } else {
            let instance = self
                .sandbox
                .instantiate()
                .map_err(|e| CallError::Trap(e.to_string()))?;
            self.made.fetch_add(1, Relaxed);
            Warm {
                instance,
                accounted: 0,
            }
        };
        self.busy.fetch_add(1, Relaxed);
        let deadline = Instant::now().checked_add(timeout);
        let result = warm.instance.call_within(name, args, deadline, max_output);
        self.put_back(warm);
        result
    }

    /// Takes the caller's instance back, or lets it go (stopped, not kept, or the
    /// pool is full), and accounts for what its call did.
    fn put_back(&self, mut warm: Warm) {
        let stopped = warm.instance.is_poisoned();
        if stopped {
            self.replaced.fetch_add(1, Relaxed);
        }
        let memory = bytes(warm.instance.memory_bytes());
        let mut idle = self.idle_instances();
        let kept = !stopped && !self.fresh_instance && idle.len() < self.max_idle;
        // An instance the pool lets go of counts for nothing from here on.
        let now = if kept { memory } else { 0 };
        let before = std::mem::replace(&mut warm.accounted, now);
        if kept {
            idle.push(warm);
        }
        drop(idle);
        self.busy.fetch_sub(1, Relaxed);
        if now >= before {
            self.memory.fetch_add(now - before, Relaxed);
        } else {
            // Saturating: the gauge is never left wrapped.
            let _ = self
                .memory
                .fetch_update(Relaxed, Relaxed, |m| Some(m.saturating_sub(before - now)));
        }
    }
}
