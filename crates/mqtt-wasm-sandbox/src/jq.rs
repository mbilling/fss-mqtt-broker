//! EMQX's `jq` rule function on the sandbox: the jq module (`jq-module/`), called
//! with EMQX's argument rules and answering with its error tags and texts.
//!
//! What EMQX does (`emqx_rule_funcs:jq/2,3` over the `emqx/jq` NIF), and so this:
//!
//! * the input is handed to jq's own JSON parser as text when it is a binary, and
//!   JSON-encoded first when it is any other value ([`Arg`]);
//! * the result is every output of the program, in order, as one list — here the
//!   text of one JSON array, for the rule engine's exact reader;
//! * an error fails the call, and outputs produced before it are gone;
//! * the timeout stops a running program. It does not cover compiling one: EMQX
//!   looks at the cancel flag only between a program's steps. Here compiling runs
//!   under a bound of its own instead ([`COMPILE_TIMEOUT`]). Unlike EMQX, a program
//!   stopped at its timeout costs its instance and the programs compiled in it: the
//!   host cannot set the module's cancel flag during a call (see `at_epoch` in the
//!   sandbox), so the call is ended by a trap.
//!
//! Not EMQX's: a module loaded with [`Limits::max_call`] holds every call to that
//! ceiling, whatever timeout the call names. The call runs under the shorter of the
//! two, a call stopped there fails with the same `timeout` and names the bound it ran
//! under, and the ceiling, unlike the timeout, also covers compiling the program and
//! making the instance.
//!
//! [`Jq`] is the function for one caller, with one warm instance. [`Pool`] is what a
//! broker holds: the module, compiled on first use, and the warm instances its
//! callers take turns on.
//!
//! The module itself is not in the source tree: `jq-module/build.sh` builds it from
//! pinned sources, and a build of this crate that finds the file embeds it
//! ([`embedded`]).

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use crate::sandbox::{
    Arg, CallError, Execution, HandOff, Instance, Limits, LoadError, Reply, Sandbox,
};
use crate::wasi::Grants;

/// The jq module this build carries: the file `jq-module/build.sh` writes (or the one
/// `MQTTD_JQ_WASM` named when this crate was built), which the build script found and
/// compared with the hash the tree records. `None` in a build made without it.
#[must_use]
pub fn embedded() -> Option<&'static [u8]> {
    #[cfg(jq_wasm)]
    {
        Some(include_bytes!(concat!(env!("OUT_DIR"), "/jq.wasm")))
    }
    #[cfg(not(jq_wasm))]
    {
        None
    }
}

/// The sha256 of the module this revision's `jq-module/build.sh` builds, in hex.
#[must_use]
pub fn recorded_sha256() -> &'static str {
    include_str!("../jq-module/jq.wasm.sha256")
        .split_whitespace()
        .next()
        .unwrap_or_default()
}

/// `rule_engine.jq_function_default_timeout`: what `jq/2` runs under.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// The longest compiling one program may take. The longest rule SQL (64 KiB of
/// program text) compiles in a small fraction of it on an idle machine; a loaded or
/// emulated host can take many times longer, and a bound a healthy host can run into
/// fails rules for no fault of theirs. Inside a call the ceiling
/// (`rules.function_max_ms`) ends compiling first.
pub const COMPILE_TIMEOUT: Duration = Duration::from_secs(30);

/// An instance holding more memory than this after a call is replaced: an
/// instance's memory never shrinks, and one large payload should not pin it.
pub const RECYCLE_ABOVE_BYTES: usize = 16 << 20;

/// The module's status for a program it has not compiled yet.
const NOT_COMPILED: i32 = 10;

/// Which kind of failure, as EMQX tags it — or one of this sandbox's own limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tag {
    /// `jq_err_compile`: the program does not compile.
    Compile,
    /// `jq_err_parse`: the input is not JSON text.
    Parse,
    /// `jq_err_process`: the program raised an error.
    Process,
    /// `timeout`: the program ran past the timeout.
    Timeout,
    /// `jq_err_system`: jq could not start, or its instance broke.
    System,
    /// Not in EMQX: the memory, stack, fuel, output or compile-time limit of the
    /// sandbox.
    Limit,
}

impl Tag {
    /// The tag as EMQX writes it in `{jq_exception, {Tag, Message}}`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Compile => "jq_err_compile",
            Self::Parse => "jq_err_parse",
            Self::Process => "jq_err_process",
            Self::Timeout => "timeout",
            Self::System => "jq_err_system",
            Self::Limit => "sandbox_limit",
        }
    }
}

/// A failed `jq` call: EMQX's `{jq_exception, {Tag, Message}}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JqError {
    /// The kind of failure.
    pub tag: Tag,
    /// The message, byte for byte EMQX's for EMQX's tags.
    pub message: Vec<u8>,
    /// What to count the failure under: `compile`, `parse`, `process`, `timeout` and
    /// `system` for EMQX's tags; `memory`, `output`, `stack`, `fuel` and
    /// `compile_timeout` for the sandbox's limits; `trap` for a module that broke.
    pub kind: &'static str,
}

impl JqError {
    fn system(message: impl Into<Vec<u8>>) -> Self {
        Self {
            tag: Tag::System,
            message: message.into(),
            kind: "system",
        }
    }
}

impl fmt::Display for JqError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: {}",
            self.tag.as_str(),
            String::from_utf8_lossy(&self.message).trim_end()
        )
    }
}

impl std::error::Error for JqError {}

/// Time spent in the parts of the last call, for measurements.
#[derive(Debug, Clone, Copy, Default)]
pub struct Timing {
    /// Making a fresh instance (zero when one was reused).
    pub instantiate: Duration,
    /// Compiling the program (zero when the instance had it cached).
    pub compile: Duration,
    /// Parsing the input, running the program and printing the outputs.
    pub run: Duration,
}

/// The `jq` function for one caller: a module and, between calls, one warm instance
/// of it with its compiled programs.
#[derive(Debug)]
pub struct Jq {
    sandbox: Sandbox,
    instance: Option<Instance>,
    /// Instances made so far (the first, and one after every stopped call).
    pub instances: u64,
    /// Instances given up so far: stopped by a call, or grown past
    /// [`RECYCLE_ABOVE_BYTES`].
    pub replaced: u64,
    /// Parts of the last call.
    pub last: Timing,
    /// The memory a [`Pool`] has counted for this caller's instance.
    accounted: u64,
}

impl Jq {
    /// Loads the jq module with the default limits, compiled to machine code.
    /// `grants` is what jq's `now` and `$ENV` see.
    pub fn new(wasm: &[u8], grants: Grants) -> Result<Self, LoadError> {
        Self::new_as(wasm, Execution::Compiled, grants)
    }

    /// [`new`](Self::new), compiled as `execution` says.
    pub fn new_as(wasm: &[u8], execution: Execution, grants: Grants) -> Result<Self, LoadError> {
        Ok(Self::from_sandbox(Sandbox::load_as(
            wasm,
            execution,
            Limits::default(),
            grants,
        )?))
    }

    /// The function on an already loaded module (a [`Sandbox`] is cheap to clone:
    /// every caller shares the compiled code and owns only its instance).
    #[must_use]
    pub fn from_sandbox(sandbox: Sandbox) -> Self {
        Self {
            sandbox,
            instance: None,
            instances: 0,
            replaced: 0,
            last: Timing::default(),
            accounted: 0,
        }
    }

    /// Drops the warm instance: the next call starts from a fresh one.
    pub fn reset(&mut self) {
        self.instance = None;
    }

    /// Fuel burnt by the warm instance so far (zero unless the module was loaded
    /// with a fuel bound).
    #[must_use]
    pub fn fuel_used(&self) -> u64 {
        self.instance.as_ref().map_or(0, Instance::fuel_used)
    }

    /// The module's own counters, `runs that found their program compiled << 32 |
    /// compilations`, of the warm instance.
    pub fn cache_stats(&mut self) -> Option<u64> {
        self.instance
            .as_mut()?
            .call_extra::<u64>("jq_cache_stats")
            .ok()
    }

    /// Linear memory the warm instance holds, in bytes.
    #[must_use]
    pub fn memory_bytes(&self) -> usize {
        self.instance.as_ref().map_or(0, Instance::memory_bytes)
    }

    /// Runs `program` on `input`. `timeout` is `jq/3`'s third argument (`None` is
    /// EMQX's `infinity`). The result is the text of a JSON array of the outputs.
    pub fn eval(
        &mut self,
        program: &[u8],
        input: Arg<'_>,
        timeout: Option<Duration>,
    ) -> Result<Vec<u8>, JqError> {
        self.eval_within(program, input, timeout, u32::MAX)
    }

    /// [`eval`](Self::eval) with this call's own bound on the size of the result (it
    /// is never more than the module's [`Limits::max_output_bytes`]).
    pub fn eval_within(
        &mut self,
        program: &[u8],
        input: Arg<'_>,
        timeout: Option<Duration>,
        max_output: u32,
    ) -> Result<Vec<u8>, JqError> {
        let started = Instant::now();
        // The call runs under its timeout or the module's ceiling, whichever is
        // shorter: one deadline for all of it.
        let most = self.sandbox.limits().max_call;
        let timeout = match (timeout, most) {
            (Some(own), Some(most)) => Some(own.min(most)),
            (own, most) => own.or(most),
        };
        let bounds = Bounds {
            // A timeout no clock can hold (EMQX accepts any integer below 2^64) is none.
            deadline: timeout.and_then(|t| started.checked_add(t)),
            timeout,
            ceiling: most.and_then(|most| Some((started.checked_add(most)?, most))),
        };
        self.last = Timing::default();
        let result = self.eval_on_instance(program, input, bounds, max_output);
        self.let_go_of_a_stopped_instance();
        result
    }

    /// Compiles `program` into the warm instance's cache without running it: what a
    /// broker does with the programs its rules spell out, when the rules are loaded.
    pub fn compile(&mut self, program: &[u8]) -> Result<(), JqError> {
        let result = self
            .warm_instance(None)
            .and_then(|instance| compile(instance, program, Bounds::default()));
        self.let_go_of_a_stopped_instance();
        result.map(|_| ())
    }

    fn let_go_of_a_stopped_instance(&mut self) {
        let stopped = self
            .instance
            .as_ref()
            .is_some_and(|i| i.is_poisoned() || i.memory_bytes() > RECYCLE_ABOVE_BYTES);
        if stopped {
            self.instance = None;
            self.replaced += 1;
        }
    }

    /// The warm instance, made now when there is none (within `ceiling`, when the
    /// call it is made for has one).
    fn warm_instance(&mut self, ceiling: Option<Instant>) -> Result<&mut Instance, JqError> {
        if self.instance.is_none() {
            let at = Instant::now();
            let made = self
                .sandbox
                .instantiate_within(ceiling)
                .map_err(|e| JqError::system(e.to_string()))?;
            self.instance = Some(made);
            self.instances += 1;
            self.last.instantiate = at.elapsed();
        }
        self.instance
            .as_mut()
            .ok_or_else(|| JqError::system("no instance"))
    }

    fn eval_on_instance(
        &mut self,
        program: &[u8],
        input: Arg<'_>,
        bounds: Bounds,
        max_output: u32,
    ) -> Result<Vec<u8>, JqError> {
        let Bounds {
            deadline, timeout, ..
        } = bounds;
        let instance = self.warm_instance(bounds.ceiling.map(|(at, _)| at))?;
        let args = [Arg::Binary(program), input];
        let at = Instant::now();
        let mut reply = instance.call_within("jq", &args, deadline, max_output);
        let mut compiling = Duration::ZERO;
        if matches!(
            &reply,
            Err(CallError::Module {
                status: NOT_COMPILED,
                ..
            })
        ) {
            compiling = compile(instance, program, bounds)?;
            reply = instance.call_within("jq", &args, deadline, max_output);
        }
        self.last.compile = compiling;
        self.last.run = at.elapsed().saturating_sub(compiling);
        match reply {
            Ok(Reply::Json(text)) => Ok(text),
            Ok(Reply::Binary(_)) => Err(JqError::system("the jq module returned a binary")),
            Err(e) => Err(error(e, timeout)),
        }
    }
}

/// What one call runs under.
#[derive(Debug, Clone, Copy, Default)]
struct Bounds {
    /// When the program is stopped: the end of `timeout`.
    deadline: Option<Instant>,
    /// What the call runs under and a `timeout` error names: the caller's timeout, or
    /// the module's ceiling where that is shorter.
    timeout: Option<Duration>,
    /// The end of the module's [`Limits::max_call`] for this call, and its length:
    /// what also stops compiling the program and making the instance.
    ceiling: Option<(Instant, Duration)>,
}

/// Compiles `program` into `instance`'s cache, under [`COMPILE_TIMEOUT`] and the
/// call's ceiling; how long it took.
fn compile(instance: &mut Instance, program: &[u8], bounds: Bounds) -> Result<Duration, JqError> {
    let at = Instant::now();
    let own = at + COMPILE_TIMEOUT;
    // The timeout does not cover compiling (as in EMQX); the ceiling covers the call.
    let (within, ceiling) = match bounds.ceiling {
        Some((ceiling, length)) if ceiling < own => (ceiling, Some(length)),
        _ => (own, None),
    };
    let compiled = instance.call("jq_compile", &[Arg::Binary(program)], Some(within));
    compiled.map_err(|e| match e {
        CallError::Timeout => JqError {
            tag: Tag::Limit,
            message: match ceiling {
                Some(length) => format!(
                    "jq stopped: compiling the program ran into the {} ms one call may take",
                    length.as_millis()
                ),
                None => format!(
                    "jq stopped: compiling the program took longer than {} ms",
                    COMPILE_TIMEOUT.as_millis()
                ),
            }
            .into_bytes(),
            kind: "compile_timeout",
        },
        e => error(e, bounds.timeout),
    })?;
    Ok(at.elapsed())
}

/// A sandbox error as the `jq` function reports it.
fn error(e: CallError, timeout: Option<Duration>) -> JqError {
    let limit = |kind, e: &CallError| (Tag::Limit, kind, format!("jq stopped: {e}").into_bytes());
    let (tag, kind, message) = match e {
        CallError::Module { status, message } => {
            let (tag, kind) = match status {
                // The program saw its cancel flag and ended by itself. The host no
                // longer sets it, but a module's answer is still read as the NIF's.
                7 => return error(CallError::Timeout, timeout),
                4 => (Tag::Compile, "compile"),
                5 => (Tag::Parse, "parse"),
                6 => (Tag::Process, "process"),
                8 => (Tag::Limit, "output"),
                _ => (Tag::System, "system"),
            };
            (tag, kind, message)
        }
        // The NIF's text, completed by `emqx_rule_funcs:jq/3`.
        CallError::Timeout => (
            Tag::Timeout,
            "timeout",
            format!(
                "jq program canceled as it took too long time to execute (timeout set to {} ms)",
                timeout.map_or(0, |t| t.as_millis())
            )
            .into_bytes(),
        ),
        CallError::Memory => limit("memory", &e),
        CallError::Stack => limit("stack", &e),
        CallError::Fuel => limit("fuel", &e),
        CallError::OutputTooLarge => limit("output", &e),
        CallError::Trap(_) | CallError::Poisoned => {
            (Tag::System, "trap", e.to_string().into_bytes())
        }
    };
    JqError { tag, message, kind }
}

/// What a [`Pool`] holds and has done, for a broker's metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolStats {
    /// Instances the pool holds now: those waiting and those in a call.
    pub instances: u64,
    /// Of those, the ones in a call.
    pub in_call: u64,
    /// Linear memory of those instances, in bytes, as of the end of each one's last
    /// call.
    pub memory_bytes: u64,
    /// Instances made since the module was loaded.
    pub made: u64,
    /// Instances given up after a call: stopped at a limit, or grown past
    /// [`RECYCLE_ABOVE_BYTES`].
    pub replaced: u64,
}

/// The `jq` function for a whole broker: the module's bytes, compiled when first
/// needed, and the warm instances callers take turns on.
///
/// Making a pool costs nothing. The first [`load`](Self::load) (or the first
/// [`eval`](Self::eval)) compiles the module to machine code, once: a broker whose
/// rules never call `jq` never pays for it, and one whose rules do pays when the rules
/// are loaded, not when a message arrives.
pub struct Pool {
    wasm: Vec<u8>,
    execution: Execution,
    limits: Limits,
    grants: Grants,
    hand_off: Option<HandOff>,
    /// The module, or why it was not loaded and whether that was the host refusing
    /// the compiler ([`LoadError::Engine`]).
    loaded: OnceLock<Result<Sandbox, (String, bool)>>,
    idle: Mutex<Vec<Jq>>,
    max_idle: usize,
    /// Instances out with a caller.
    busy: AtomicU64,
    /// Memory of every instance the pool holds, waiting or out.
    memory: AtomicU64,
    made: AtomicU64,
    replaced: AtomicU64,
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool")
            .field("loaded", &self.is_loaded())
            .field("max_idle", &self.max_idle)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

fn bytes(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

impl Pool {
    /// A pool over the module `wasm`, keeping at most `max_idle` warm instances
    /// between calls (a broker's number of worker threads). Nothing is compiled yet.
    #[must_use]
    pub fn new(
        wasm: Vec<u8>,
        execution: Execution,
        limits: Limits,
        grants: Grants,
        max_idle: usize,
    ) -> Self {
        Self {
            wasm,
            execution,
            limits,
            grants,
            hand_off: None,
            loaded: OnceLock::new(),
            idle: Mutex::new(Vec::new()),
            max_idle,
            busy: AtomicU64::new(0),
            memory: AtomicU64::new(0),
            made: AtomicU64::new(0),
            replaced: AtomicU64::new(0),
        }
    }

    /// The same pool, whose calls give what is left of them after
    /// [`Limits::first_slice`] to `hand_off` ([`Sandbox::with_hand_off`]). Set before
    /// the first call.
    #[must_use]
    pub fn with_hand_off(mut self, hand_off: HandOff) -> Self {
        self.hand_off = Some(hand_off);
        self
    }

    /// Compiles the module if no call has yet, and says why it cannot be: the reason
    /// a rule calling `jq` is refused when the rules are loaded. A failure is kept;
    /// it is not tried again.
    pub fn load(&self) -> Result<&Sandbox, JqError> {
        self.loaded
            .get_or_init(|| {
                Sandbox::load_as(
                    &self.wasm,
                    self.execution,
                    self.limits.clone(),
                    self.grants.clone(),
                )
                .map(|sandbox| match &self.hand_off {
                    Some(hand_off) => sandbox.with_hand_off(hand_off.clone()),
                    None => sandbox,
                })
                .map_err(|e| (e.to_string(), matches!(e, LoadError::Engine(_))))
            })
            .as_ref()
            .map_err(|(reason, _)| JqError::system(format!("jq is not available: {reason}")))
    }

    /// Whether [`load`](Self::load) failed because this host does not let the
    /// compiler run: no executable memory, or no thread for the timer.
    #[must_use]
    pub fn refused_by_host(&self) -> bool {
        matches!(self.loaded.get(), Some(Err((_, true))))
    }

    /// Whether the module has been compiled (by [`load`](Self::load) or a call).
    #[must_use]
    pub fn is_loaded(&self) -> bool {
        matches!(self.loaded.get(), Some(Ok(_)))
    }

    /// Warm instances waiting for a caller.
    #[must_use]
    pub fn idle(&self) -> usize {
        self.idle_instances().len()
    }

    fn idle_instances(&self) -> std::sync::MutexGuard<'_, Vec<Jq>> {
        self.idle.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// What the pool holds and has done so far.
    #[must_use]
    pub fn stats(&self) -> PoolStats {
        // A caller's place in the pool is kept when its call stopped its instance; the
        // instance itself is made again by the next call, and counts from then.
        let waiting = self
            .idle_instances()
            .iter()
            .filter(|jq| jq.instance.is_some())
            .count();
        let in_call = self.busy.load(Relaxed);
        PoolStats {
            instances: bytes(waiting) + in_call,
            in_call,
            memory_bytes: self.memory.load(Relaxed),
            made: self.made.load(Relaxed),
            replaced: self.replaced.load(Relaxed),
        }
    }

    /// [`Jq::eval`] on a warm instance of the pool, or on a new one when every one is
    /// in use. The instance goes back to the pool afterwards.
    pub fn eval(
        &self,
        program: &[u8],
        input: Arg<'_>,
        timeout: Option<Duration>,
    ) -> Result<Vec<u8>, JqError> {
        self.eval_within(program, input, timeout, u32::MAX)
    }

    /// [`Jq::eval_within`] on an instance of the pool.
    pub fn eval_within(
        &self,
        program: &[u8],
        input: Arg<'_>,
        timeout: Option<Duration>,
        max_output: u32,
    ) -> Result<Vec<u8>, JqError> {
        let mut jq = self.take()?;
        let result = jq.eval_within(program, input, timeout, max_output);
        self.put_back(jq);
        result
    }

    /// Compiles `programs` into every instance the pool keeps warm (making them, up to
    /// the pool's size): the first message that meets one of them does not pay for
    /// it. A program that does not compile is skipped; a call with it says why. The
    /// first error other than that, such as a module that cannot be loaded, ends it.
    pub fn warm(&self, programs: &[&[u8]]) -> Result<(), JqError> {
        if programs.is_empty() {
            return Ok(());
        }
        let mut out = Vec::with_capacity(self.max_idle);
        let mut result = Ok(());
        for _ in 0..self.max_idle {
            match self.take() {
                Ok(jq) => out.push(jq),
                Err(e) => {
                    result = Err(e);
                    break;
                }
            }
        }
        for jq in &mut out {
            for program in programs {
                match jq.compile(program) {
                    Ok(()) => {}
                    Err(e) if e.tag == Tag::Compile => {}
                    Err(e) => result = result.and(Err(e)),
                }
            }
        }
        for jq in out {
            self.put_back(jq);
        }
        result
    }

    /// Compiles one program in one instance of the pool, and says why it does not
    /// compile.
    pub fn compile(&self, program: &[u8]) -> Result<(), JqError> {
        let mut jq = self.take()?;
        let result = jq.compile(program);
        self.put_back(jq);
        result
    }

    /// A warm instance for one caller, or a new one when every one is out.
    fn take(&self) -> Result<Jq, JqError> {
        let taken = self.idle_instances().pop();
        let jq = match taken {
            Some(jq) => jq,
            None => Jq::from_sandbox(self.load()?.clone()),
        };
        self.busy.fetch_add(1, Relaxed);
        Ok(jq)
    }

    /// Takes the caller's instance back, or lets it go when the pool is full, and
    /// accounts for what its call did.
    fn put_back(&self, mut jq: Jq) {
        self.made
            .fetch_add(std::mem::take(&mut jq.instances), Relaxed);
        self.replaced
            .fetch_add(std::mem::take(&mut jq.replaced), Relaxed);
        let memory = bytes(jq.memory_bytes());
        let mut idle = self.idle_instances();
        let kept = idle.len() < self.max_idle;
        // An instance the pool lets go of counts for nothing from here on.
        let now = if kept { memory } else { 0 };
        let before = std::mem::replace(&mut jq.accounted, now);
        if kept {
            idle.push(jq);
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
