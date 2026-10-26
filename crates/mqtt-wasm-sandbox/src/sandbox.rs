//! Loading a module, making instances of it, and calling a function in one under
//! limits: time (an epoch deadline, raised by a timer thread that sleeps until the
//! next deadline), memory, stack and output size.
//!
//! A call that outlasts its first slice ([`Limits::first_slice`]) is suspended once and
//! the rest of it is run through the module's [`HandOff`], when it has one: the place a
//! caller on an async runtime's worker thread hands that thread's other work to another
//! worker before it goes on (ADR 0086 D6).
//!
//! The module is compiled by Cranelift when it is loaded ([`Sandbox::load`]), with the
//! safe API only: nothing is ever read back from disk as code. [`Execution`] chooses
//! what it is compiled to: machine code, or bytecode for the runtime's own
//! interpreter where the host forbids executable memory. A host that cannot map
//! memory executable gets [`LoadError::Engine`] for the first, never a crash.

use std::fmt;
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use wasmtime::{
    Config, Engine, ExternType, Linker, Memory, Module, OptLevel, ResourceLimiter, Store, Strategy,
    Trap, TypedFunc, UpdateDeadline, WasmParams, WasmResults,
};

use crate::timer::Timer;
use crate::wasi::{self, Grants};

/// The ABI version this host speaks (`mqttd_abi_version`).
pub const ABI_VERSION: u32 = 1;

/// The stack a call has beyond [`Limits::max_stack_bytes`]: room for the host's own
/// frames (the imports, the deadline callback) under the module's deepest call.
const HOST_STACK_HEADROOM: usize = 256 * 1024;

/// What a module's code is compiled to when it is loaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Execution {
    /// Machine code for this processor (Cranelift). The fast one, and the default. It
    /// needs memory the process may map executable: under systemd's
    /// `MemoryDenyWriteExecute=yes`, an `SELinux` policy denying `execmem`, or a
    /// seccomp profile refusing `mprotect(PROT_EXEC)`, loading fails with
    /// [`LoadError::Engine`].
    #[default]
    Compiled,
    /// Bytecode for the runtime's portable interpreter (Pulley). No executable memory
    /// and no signal handlers; the same module, limits and answers, at a small
    /// fraction of the speed.
    Interpreted,
}

/// The bounds one instance and one call run under.
#[derive(Debug, Clone)]
pub struct Limits {
    /// Most linear memory an instance may hold, in bytes.
    pub memory_bytes: usize,
    /// Largest result a call may return, in bytes.
    pub max_output_bytes: u32,
    /// Most stack the module's own calls may use, in bytes. A call runs on a stack of
    /// its own of this size (and a little more for the host's frames), not on the
    /// calling thread's.
    pub max_stack_bytes: usize,
    /// How long a call always gets, whatever its deadline: a timeout of zero still
    /// runs what is quick, as in EMQX.
    pub min_run: Duration,
    /// The deadline of instantiation (data segments, constructors) and of the calls
    /// that read a module's description.
    pub init_timeout: Duration,
    /// Most fuel (about one unit per executed instruction) one call may burn,
    /// whatever the clock says: a bound that does not depend on how fast the machine
    /// is. `None` compiles the module without fuel accounting, which is faster, and
    /// leaves the deadline as the only bound on time. Making an instance (its data
    /// segments, a start function) is not a call and is bounded by `init_timeout`
    /// alone.
    pub max_fuel: Option<u64>,
    /// How long a call runs before it is suspended once and the rest of it is given to
    /// the module's [`HandOff`]. Nothing is suspended in a module without one.
    pub first_slice: Duration,
    /// The longest one call of a function may take as a whole, whatever deadline its
    /// caller asks for: every entry the host makes into the module for it (allocate,
    /// call, free), and what the call has to make first when it finds none (an
    /// instance; for `jq`, a compiled program). Held by the two things that are one
    /// call of a rule function, [`ModulePool::call`](crate::ModulePool::call) and
    /// [`jq::Pool::eval_within`](crate::jq::Pool::eval_within); an [`Instance`] on its
    /// own knows only the deadline it is given. `None`: no such ceiling.
    ///
    /// An entry that starts with the ceiling passed still gets [`min_run`](Self::min_run),
    /// one millisecond by default, and a call has a handful of entries left by then, so
    /// a call ends within some tens of milliseconds of its ceiling.
    pub max_call: Option<Duration>,
}

/// What a caller wraps the rest of a long call in ([`Sandbox::with_hand_off`]): it is
/// given the rest of the call as a closure and runs it, once, before it returns. A
/// broker passes `tokio::task::block_in_place` here, so the worker thread a long call
/// occupies gives its other tasks to another worker first.
pub type HandOff = Arc<dyn Fn(&mut dyn FnMut()) + Send + Sync>;

impl Default for Limits {
    fn default() -> Self {
        Self {
            memory_bytes: 64 << 20,
            max_output_bytes: 1 << 20,
            max_stack_bytes: 4 << 20,
            min_run: Duration::from_millis(1),
            init_timeout: Duration::from_secs(5),
            max_fuel: None,
            first_slice: Duration::from_millis(1),
            max_call: None,
        }
    }
}

impl Execution {
    /// Whether this host can run a module compiled this way: compiles a module of one
    /// function that returns 1, and calls it. What a broker asks before it promises
    /// `jq` to a rules file, so the answer is a line in a log and not a failed rule.
    pub fn probe(self) -> Result<(), LoadError> {
        // (module (func (export "f") (result i32) (i32.const 1)))
        const ONE: [u8; 31] = [
            0, 0x61, 0x73, 0x6d, 1, 0, 0, 0, 1, 5, 1, 0x60, 0, 1, 0x7f, 3, 2, 1, 0, 7, 5, 1, 1,
            0x66, 0, 0, 0x0a, 6, 1, 4, 0,
        ];
        let answer = || -> wasmtime::Result<i32> {
            let mut wasm = ONE.to_vec();
            wasm.extend_from_slice(&[0x41, 1, 0x0b]);
            let engine = Engine::new(&config(self, &Limits::default())?)?;
            let module = Module::new(&engine, &wasm)?;
            let mut store = Store::new(&engine, ());
            store.set_epoch_deadline(1);
            let instance = on_fiber(
                wasmtime::Instance::new_async(&mut store, &module, &[]),
                None,
            )?;
            let one = instance.get_typed_func::<(), i32>(&mut store, "f")?;
            on_fiber(one.call_async(&mut store, ()), None)
        };
        match answer() {
            Ok(1) => Ok(()),
            Ok(other) => Err(LoadError::Engine(format!(
                "the probe returned {other}, not 1"
            ))),
            Err(e) => Err(LoadError::Engine(chain(&e))),
        }
    }
}

/// Why a module was not loaded.
#[derive(Debug)]
pub enum LoadError {
    /// The compiler or its timer thread could not start on this host: no
    /// executable memory (a `MemoryDenyWriteExecute`, `SELinux` `execmem` or seccomp
    /// policy), or no thread.
    Engine(String),
    /// Not a valid WebAssembly module (or one using a feature that is off).
    Invalid(String),
    /// It imports something the host does not provide.
    Import {
        /// The import's module name.
        module: String,
        /// The import's field name.
        name: String,
    },
    /// An export of the ABI is missing or has another signature.
    Export(String),
    /// It speaks another ABI version.
    AbiVersion(u32),
    /// Its first instance could not be made or did not describe itself.
    Start(CallError),
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Engine(e) => write!(
                f,
                "the WebAssembly compiler cannot run on this host (it needs memory it may \
                 map executable): {e}"
            ),
            Self::Invalid(e) => write!(f, "not a usable WebAssembly module: {e}"),
            Self::Import { module, name } => {
                write!(
                    f,
                    "the module imports {module}.{name}, which is not provided"
                )
            }
            Self::Export(e) => write!(f, "the module does not export the function ABI: {e}"),
            Self::AbiVersion(v) => {
                write!(
                    f,
                    "the module speaks ABI version {v}, this host {ABI_VERSION}"
                )
            }
            Self::Start(e) => write!(f, "the module did not start: {e}"),
        }
    }
}

impl std::error::Error for LoadError {}

/// Why a call produced no result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallError {
    /// The function itself refused: its status and its message.
    Module {
        /// The module's own status number (never 0).
        status: i32,
        /// The module's message.
        message: Vec<u8>,
    },
    /// The deadline passed.
    Timeout,
    /// [`Limits::max_fuel`] was burnt.
    Fuel,
    /// The instance asked for more memory than [`Limits::memory_bytes`].
    Memory,
    /// The module's calls outgrew [`Limits::max_stack_bytes`].
    Stack,
    /// The result is larger than [`Limits::max_output_bytes`].
    OutputTooLarge,
    /// The module trapped, exited, or broke the ABI.
    Trap(String),
    /// The instance was stopped by an earlier call and must be replaced.
    Poisoned,
}

impl fmt::Display for CallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Module { status, message } => {
                write!(f, "status {status}: {}", String::from_utf8_lossy(message))
            }
            Self::Timeout => f.write_str("the call ran past its deadline"),
            Self::Fuel => f.write_str("the call used all its fuel"),
            Self::Memory => f.write_str("the module reached its memory limit"),
            Self::Stack => f.write_str("the module's stack overflowed"),
            Self::OutputTooLarge => f.write_str("the result is larger than the limit"),
            Self::Trap(e) => write!(f, "the module trapped: {e}"),
            Self::Poisoned => f.write_str("the instance was stopped by an earlier call"),
        }
    }
}

impl std::error::Error for CallError {}

/// One argument crossing into the module.
#[derive(Debug, Clone, Copy)]
pub enum Arg<'a> {
    /// A binary: its bytes, as they are.
    Binary(&'a [u8]),
    /// Any other value, as JSON text.
    Json(&'a str),
}

/// The value a call returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    /// A binary.
    Binary(Vec<u8>),
    /// Any other value, as JSON text.
    Json(Vec<u8>),
}

/// The host's reason for ending a call, carried through the runtime as the error of
/// the import, limiter or deadline callback that raised it.
#[derive(Debug)]
pub(crate) enum Stop {
    /// The deadline.
    Timeout,
    /// The memory cap.
    Memory,
    /// `proc_exit`: the module's `exit` or `abort`.
    Exit(i32),
}

impl fmt::Display for Stop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout => f.write_str("deadline"),
            Self::Memory => f.write_str("memory limit"),
            Self::Exit(code) => write!(f, "the module exited with status {code}"),
        }
    }
}

impl std::error::Error for Stop {}

/// The state the host keeps per instance, reachable from the imports.
pub(crate) struct Host {
    pub(crate) grants: Grants,
    pub(crate) discarded_output: u64,
    limiter: MemoryCap,
    timer: Arc<Timer>,
    /// When the running call is to be stopped. `None`: never.
    deadline: Option<Instant>,
    /// When the running call is to be suspended for its [`HandOff`]. `None`: it has
    /// none, or has been suspended already.
    yield_at: Option<Instant>,
    /// The timer's registration for the running call: of `yield_at` while that is
    /// ahead, of `deadline` after.
    armed: Option<crate::timer::Armed>,
}

/// Refuses linear memory beyond a cap, by a trap at the `memory.grow` that asks for
/// it — rather than by handing the module's allocator a failure it may or may not
/// survive.
#[derive(Debug)]
struct MemoryCap {
    max_bytes: usize,
}

impl ResourceLimiter for MemoryCap {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if desired > self.max_bytes {
            return Err(Stop::Memory.into());
        }
        Ok(maximum.is_none_or(|max| desired <= max))
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(desired <= 65_536 && maximum.is_none_or(|max| desired <= max))
    }

    fn instances(&self) -> usize {
        1
    }

    fn tables(&self) -> usize {
        1
    }

    fn memories(&self) -> usize {
        1
    }
}

/// A validated, compiled module: made once, shared by every instance of it.
#[derive(Clone)]
pub struct Sandbox {
    engine: Engine,
    module: Module,
    linker: Arc<Linker<Host>>,
    timer: Arc<Timer>,
    limits: Limits,
    grants: Grants,
    description: Vec<u8>,
    hand_off: Option<HandOff>,
}

impl fmt::Debug for Sandbox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sandbox")
            .field("limits", &self.limits)
            .field("description", &String::from_utf8_lossy(&self.description))
            .finish_non_exhaustive()
    }
}

/// The runtime's configuration: one compiler, the WebAssembly features a C module
/// built for `wasm32-wasi` uses, and nothing else switched on.
fn config(execution: Execution, limits: &Limits) -> wasmtime::Result<Config> {
    let mut config = Config::new();
    if execution == Execution::Interpreted {
        config.target(if cfg!(target_pointer_width = "32") {
            "pulley32"
        } else {
            "pulley64"
        })?;
        // Bounds are checked by the interpreter; no fault handler is installed.
        config.signals_based_traps(false);
    }
    config
        .strategy(Strategy::Cranelift)
        .cranelift_opt_level(OptLevel::Speed)
        // The same bits for a NaN on every processor: a module's answers do not
        // depend on the machine.
        .cranelift_nan_canonicalization(true)
        .epoch_interruption(true)
        .consume_fuel(limits.max_fuel.is_some())
        .max_wasm_stack(limits.max_stack_bytes)
        .async_stack_size(limits.max_stack_bytes + HOST_STACK_HEADROOM)
        // A trap is reported by its kind; nobody reads a module's stack trace.
        .wasm_backtrace_max_frames(None)
        .wasm_bulk_memory(true)
        .wasm_multi_value(true)
        .wasm_multi_memory(false)
        .wasm_memory64(false)
        .wasm_tail_call(false)
        .wasm_custom_page_sizes(false)
        .wasm_relaxed_simd(false)
        .wasm_simd(false)
        .wasm_wide_arithmetic(false)
        .wasm_extended_const(false);
    Ok(config)
}

impl Sandbox {
    /// [`load_as`](Self::load_as) with [`Execution::Compiled`].
    pub fn load(wasm: &[u8], limits: Limits, grants: Grants) -> Result<Self, LoadError> {
        Self::load_as(wasm, Execution::Compiled, limits, grants)
    }

    /// Validates and compiles `wasm`, checks its imports against the allowed list and
    /// its exports against the ABI, and starts one instance to read its description.
    ///
    /// This is where the module's code is generated: about half a second for the jq
    /// module, once per process. Nothing is compiled later, in a call.
    pub fn load_as(
        wasm: &[u8],
        execution: Execution,
        limits: Limits,
        grants: Grants,
    ) -> Result<Self, LoadError> {
        let engine = config(execution, &limits)
            .and_then(|config| Engine::new(&config))
            .map_err(|e| LoadError::Engine(chain(&e)))?;
        let module = Module::new(&engine, wasm).map_err(|e| {
            // A module that is not valid is the module's fault; anything else at this
            // point is the host refusing the compiler its memory.
            if Module::validate(&engine, wasm).is_err() {
                LoadError::Invalid(chain(&e))
            } else {
                LoadError::Engine(chain(&e))
            }
        })?;
        for import in module.imports() {
            let known = import.module() == wasi::MODULE
                && wasi::ALLOWED.contains(&import.name())
                && matches!(import.ty(), ExternType::Func(_));
            if !known {
                return Err(LoadError::Import {
                    module: import.module().to_owned(),
                    name: import.name().to_owned(),
                });
            }
        }
        if !matches!(module.get_export("memory"), Some(ExternType::Memory(_))) {
            return Err(LoadError::Export("no exported memory".to_owned()));
        }
        let mut linker = Linker::new(&engine);
        wasi::define(&mut linker).map_err(|e| LoadError::Invalid(chain(&e)))?;
        let ticked = engine.clone();
        let timer = Timer::start(move || ticked.increment_epoch())
            .map_err(|e| LoadError::Engine(format!("no timer thread: {e}")))?;
        let mut sandbox = Self {
            engine,
            module,
            linker: Arc::new(linker),
            timer: Arc::new(timer),
            limits,
            grants,
            description: Vec::new(),
            hand_off: None,
        };
        let mut first = sandbox.instantiate()?;
        let within = Some(Instant::now() + first.limits.init_timeout);
        let version = first
            .run(first.abi_version.clone(), (), within)
            .map_err(LoadError::Start)?;
        if version != ABI_VERSION {
            return Err(LoadError::AbiVersion(version));
        }
        sandbox.description = first.describe(within).map_err(LoadError::Start)?;
        Ok(sandbox)
    }

    /// What the module says about itself (`mqttd_describe`): JSON text.
    #[must_use]
    pub fn description(&self) -> &[u8] {
        &self.description
    }

    /// The limits instances of this module run under.
    #[must_use]
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// The same module, whose instances made from here on give the rest of a call
    /// that outlasts [`Limits::first_slice`] to `hand_off`.
    #[must_use]
    pub fn with_hand_off(mut self, hand_off: HandOff) -> Self {
        self.hand_off = Some(hand_off);
        self
    }

    /// A fresh instance: its own memory, nothing shared with any other.
    pub fn instantiate(&self) -> Result<Instance, LoadError> {
        self.instantiate_within(None)
    }

    /// [`instantiate`](Self::instantiate) for a call that has `deadline` as a whole:
    /// the module's start (data segments, a start function, constructors) is stopped
    /// at [`Limits::init_timeout`] or at `deadline`, whichever comes first.
    pub fn instantiate_within(&self, deadline: Option<Instant>) -> Result<Instance, LoadError> {
        let host = Host {
            grants: self.grants.clone(),
            discarded_output: 0,
            limiter: MemoryCap {
                max_bytes: self.limits.memory_bytes,
            },
            timer: Arc::clone(&self.timer),
            deadline: None,
            yield_at: None,
            armed: None,
        };
        let mut store = Store::new(&self.engine, host);
        store.limiter(|host| &mut host.limiter);
        store.epoch_deadline_callback(at_epoch);
        let within = Instant::now() + self.limits.init_timeout;
        let within = deadline.map_or(within, |at| at.min(within));
        let start = |e: wasmtime::Error| LoadError::Start(classify(&e));
        // Data segments and a start function, when the module has one.
        let instance = {
            arm(&mut store, Some(within), &self.limits, false).map_err(start)?;
            // Not a call, and not charged as one: the runtime bills making an
            // instance to fuel as well (measured for a module of one page on an
            // arm64 Mac: 16,385 units compiled, 65,537 interpreted), which made a
            // smaller `max_fuel` refuse every module. `init_timeout` is what bounds
            // a start function.
            if self.limits.max_fuel.is_some() {
                store.set_fuel(u64::MAX).map_err(start)?;
            }
            let made = on_fiber(
                self.linker.instantiate_async(&mut store, &self.module),
                None,
            );
            disarm(&mut store);
            made
        }
        .map_err(start)?;
        let export = |e: wasmtime::Error| LoadError::Export(chain(&e));
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| LoadError::Export("no exported memory".to_owned()))?;
        let abi_version = instance
            .get_typed_func(&mut store, "mqttd_abi_version")
            .map_err(export)?;
        let alloc = instance
            .get_typed_func(&mut store, "mqttd_alloc")
            .map_err(export)?;
        let free = instance
            .get_typed_func(&mut store, "mqttd_free")
            .map_err(export)?;
        let describe = instance
            .get_typed_func(&mut store, "mqttd_describe")
            .map_err(export)?;
        let call = instance
            .get_typed_func(&mut store, "mqttd_call")
            .map_err(export)?;
        // A reactor's constructors (libc start-up), when it has them.
        let initialize = instance
            .get_typed_func::<(), ()>(&mut store, "_initialize")
            .ok();
        let mut made = Instance {
            abi_version,
            alloc,
            free,
            describe,
            call,
            memory,
            module: instance,
            store,
            limits: self.limits.clone(),
            hand_off: self.hand_off.clone(),
            poisoned: false,
            fuel_used: 0,
        };
        if let Some(initialize) = initialize {
            made.run(initialize, (), Some(within))
                .map_err(LoadError::Start)?;
        }
        // `mqttd_cancel_slot`, when the module has it, is not called: the host has no
        // sound way to set the flag it points at during a call (see [`at_epoch`]).
        Ok(made)
    }
}

/// Runs a call into a module to its end. The runtime gives such a call a stack of
/// its own (a fiber, with a guard page below it), so how deep a module may go never
/// depends on how much stack the calling thread happens to have left.
///
/// The host's imports are all synchronous, so the first poll finishes the call unless
/// the deadline callback suspended it: which it does once, when the call has used its
/// first slice and has a [`HandOff`]. The rest of the call is then polled inside the
/// hand-off, on this same thread.
fn on_fiber<F: Future>(call: F, hand_off: Option<&HandOff>) -> F::Output {
    let mut call = pin!(call);
    let mut context = Context::from_waker(Waker::noop());
    if let Poll::Ready(output) = call.as_mut().poll(&mut context) {
        return output;
    }
    let mut output = None;
    {
        let mut rest = || {
            // A hand-off that calls this twice finds the call finished.
            while output.is_none() {
                match call.as_mut().poll(&mut context) {
                    Poll::Ready(done) => output = Some(done),
                    Poll::Pending => std::thread::yield_now(),
                }
            }
        };
        match hand_off {
            Some(hand_off) => hand_off(&mut rest),
            None => rest(),
        }
    }
    loop {
        // Only a hand-off that did not run the rest gets here with nothing.
        if let Some(output) = output.take() {
            return output;
        }
        if let Poll::Ready(done) = call.as_mut().poll(&mut context) {
            return done;
        }
        std::thread::yield_now();
    }
}

/// An error and what caused it, on one line.
fn chain(error: &wasmtime::Error) -> String {
    format!("{error:#}")
}

/// Names the limit behind a failed call, when a limit is behind it.
fn classify(error: &wasmtime::Error) -> CallError {
    match error.downcast_ref::<Stop>() {
        Some(Stop::Timeout) => return CallError::Timeout,
        Some(Stop::Memory) => return CallError::Memory,
        Some(exit @ Stop::Exit(_)) => return CallError::Trap(exit.to_string()),
        None => {}
    }
    match error.downcast_ref::<Trap>() {
        Some(Trap::StackOverflow) => CallError::Stack,
        Some(Trap::OutOfFuel) => CallError::Fuel,
        Some(Trap::Interrupt) => CallError::Timeout,
        _ => CallError::Trap(chain(error)),
    }
}

/// The store's answer to a tick of the timer. A tick that is not this call's (another
/// instance's deadline) is passed over. At this call's deadline the call is ended, by
/// a trap, and its instance is dropped.
///
/// Nothing in the module is asked first. This callback runs between two of the
/// module's instructions, where the runtime lets the host change its own data and
/// nothing the module can see (wasmtime 50 gives it only that, bytecodealliance/wasmtime
/// #14048 and #14386: a write to the module's memory here breaks the module's
/// semantics, and such writes have been behind vulnerabilities). Calling into the
/// module from here is refused as well (RUSTSEC-2026-0362), and while the call is
/// suspended its store is lent to it, so [`on_fiber`] cannot write either.
///
/// Before that, a call with a [`HandOff`] is suspended once, when its first slice is
/// used up: the runtime returns to whoever polls the call, which is [`on_fiber`].
fn at_epoch(mut state: wasmtime::StoreHookState<'_, Host>) -> wasmtime::Result<UpdateDeadline> {
    let host = state.data_mut();
    let now = Instant::now();
    if let Some(at) = host.yield_at {
        if now < at {
            return Ok(UpdateDeadline::Continue(1));
        }
        // From here the timer watches the deadline (the new registration is made
        // before the slice's is dropped: the timer never sees the call unwatched).
        host.yield_at = None;
        host.armed = Some(host.timer.arm(host.deadline));
        if host.deadline.is_none_or(|deadline| now < deadline) {
            return Ok(UpdateDeadline::Yield(1));
        }
    }
    match host.deadline {
        Some(deadline) if now >= deadline => Err(Stop::Timeout.into()),
        _ => Ok(UpdateDeadline::Continue(1)),
    }
}

/// Sets a store up for one call: its deadline with the timer, its fuel, and with `hand_off` the end of its first slice. [`disarm`] takes the call off
/// the timer again.
fn arm(
    store: &mut Store<Host>,
    deadline: Option<Instant>,
    limits: &Limits,
    hand_off: bool,
) -> wasmtime::Result<()> {
    let now = Instant::now();
    let deadline = deadline.map(|at| at.max(now + limits.min_run));
    // A call whose deadline comes first is never suspended: it is over by then.
    let yield_at = hand_off
        .then(|| now + limits.first_slice)
        .filter(|at| deadline.is_none_or(|deadline| *at < deadline));
    // The epoch first, the timer second: a tick between the two is then seen.
    store.set_epoch_deadline(1);
    let host = store.data_mut();
    host.deadline = deadline;
    host.yield_at = yield_at;
    host.armed = Some(host.timer.arm(yield_at.or(deadline)));
    if let Some(fuel) = limits.max_fuel {
        store.set_fuel(fuel)?;
    }
    Ok(())
}

/// Takes the call that just ended off the timer.
fn disarm(store: &mut Store<Host>) {
    let host = store.data_mut();
    host.armed = None;
    host.yield_at = None;
}

/// One instance of a module: one linear memory, used by one caller at a time.
pub struct Instance {
    store: Store<Host>,
    module: wasmtime::Instance,
    memory: Memory,
    abi_version: TypedFunc<(), u32>,
    alloc: TypedFunc<u32, u32>,
    free: TypedFunc<u32, ()>,
    describe: TypedFunc<u32, ()>,
    call: TypedFunc<(u32, u32, u32, u32, u32, u32), i32>,
    limits: Limits,
    hand_off: Option<HandOff>,
    poisoned: bool,
    fuel_used: u64,
}

impl fmt::Debug for Instance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Instance")
            .field("memory_bytes", &self.memory_bytes())
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}

impl Instance {
    /// Linear memory the instance holds now, in bytes. It only grows.
    #[must_use]
    pub fn memory_bytes(&self) -> usize {
        self.memory.data_size(&self.store)
    }

    /// Whether an earlier call stopped this instance; it serves no further call.
    #[must_use]
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Fuel burnt by every call on this instance so far. Zero unless the module was
    /// loaded with [`Limits::max_fuel`].
    #[must_use]
    pub fn fuel_used(&self) -> u64 {
        self.fuel_used
    }

    /// Bytes the module wrote to its standard output and error (all discarded).
    #[must_use]
    pub fn discarded_output(&self) -> u64 {
        self.store.data().discarded_output
    }

    /// Calls an exported function that is not part of the ABI and takes no
    /// arguments (a module's own measurement hooks).
    pub fn call_extra<R: WasmResults + Sync>(&mut self, name: &str) -> Result<R, CallError> {
        let func = self
            .module
            .get_typed_func::<(), R>(&mut self.store, name)
            .map_err(|e| CallError::Trap(chain(&e)))?;
        let within = Some(Instant::now() + self.limits.init_timeout);
        self.run(func, (), within)
    }

    /// Runs `func` until it returns, traps, or is stopped at the deadline. Anything
    /// but a return poisons the instance.
    // `func` is a handle cloned out of a field of `self`, which `&mut self` cannot
    // lend at the same time.
    #[allow(clippy::needless_pass_by_value)]
    fn run<P: WasmParams + Sync, R: WasmResults + Sync>(
        &mut self,
        func: TypedFunc<P, R>,
        params: P,
        deadline: Option<Instant>,
    ) -> Result<R, CallError> {
        if self.poisoned {
            return Err(CallError::Poisoned);
        }
        let result = self.run_armed(&func, params, deadline);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn run_armed<P: WasmParams + Sync, R: WasmResults + Sync>(
        &mut self,
        func: &TypedFunc<P, R>,
        params: P,
        deadline: Option<Instant>,
    ) -> Result<R, CallError> {
        arm(
            &mut self.store,
            deadline,
            &self.limits,
            self.hand_off.is_some(),
        )
        .map_err(|e| CallError::Trap(chain(&e)))?;
        let result = on_fiber(
            func.call_async(&mut self.store, params),
            self.hand_off.as_ref(),
        );
        disarm(&mut self.store);
        if let Some(granted) = self.limits.max_fuel {
            let left = self.store.get_fuel().unwrap_or(0);
            self.fuel_used = self.fuel_used.saturating_add(granted.saturating_sub(left));
        }
        result.map_err(|e| classify(&e))
    }

    fn read(&self, at: u32, len: u32) -> Result<Vec<u8>, CallError> {
        let outside =
            || CallError::Trap("the module returned an address outside its memory".into());
        // Looked at before anything is allocated: a length the memory cannot hold is
        // refused without a buffer of that length being made for it.
        let end = (at as usize)
            .checked_add(len as usize)
            .ok_or_else(outside)?;
        if end > self.memory.data_size(&self.store) {
            return Err(outside());
        }
        let mut bytes = vec![0u8; len as usize];
        self.memory
            .read(&self.store, at as usize, &mut bytes)
            .map_err(|_| outside())?;
        Ok(bytes)
    }

    fn write(&mut self, at: u32, bytes: &[u8]) -> Result<(), CallError> {
        self.memory
            .write(&mut self.store, at as usize, bytes)
            .map_err(|_| CallError::Trap("the module allocated outside its memory".into()))
    }

    /// Reads the (address, length) pair a function left at `ret`, takes the bytes
    /// and frees them in the module.
    fn take_result(
        &mut self,
        ret: u32,
        deadline: Option<Instant>,
        max_output: u32,
    ) -> Result<Vec<u8>, CallError> {
        let pair = self.read(ret, 8)?;
        let at = u32::from_le_bytes([pair[0], pair[1], pair[2], pair[3]]);
        let len = u32::from_le_bytes([pair[4], pair[5], pair[6], pair[7]]);
        if len > max_output.saturating_add(1) {
            return Err(CallError::OutputTooLarge);
        }
        let bytes = if at == 0 {
            Vec::new()
        } else {
            self.read(at, len)?
        };
        if at != 0 {
            self.run(self.free.clone(), at, deadline)?;
        }
        Ok(bytes)
    }

    fn describe(&mut self, deadline: Option<Instant>) -> Result<Vec<u8>, CallError> {
        let ret = self.run(self.alloc.clone(), 8, deadline)?;
        self.run(self.describe.clone(), ret, deadline)?;
        let text = self.take_result(ret, deadline, self.limits.max_output_bytes)?;
        self.run(self.free.clone(), ret, deadline)?;
        Ok(text)
    }

    /// Calls the function `name` with `args`. `deadline` is when the call is stopped
    /// (`None`: never, EMQX's `infinity`). A call that ends in anything but a result
    /// or [`CallError::Module`] leaves the instance poisoned.
    pub fn call(
        &mut self,
        name: &str,
        args: &[Arg<'_>],
        deadline: Option<Instant>,
    ) -> Result<Reply, CallError> {
        self.call_within(name, args, deadline, self.limits.max_output_bytes)
    }

    /// [`call`](Self::call) with this call's own bound on the result, at most
    /// [`Limits::max_output_bytes`]: what a caller with a budget has left of it.
    pub fn call_within(
        &mut self,
        name: &str,
        args: &[Arg<'_>],
        deadline: Option<Instant>,
        max_output: u32,
    ) -> Result<Reply, CallError> {
        let max_output = max_output.min(self.limits.max_output_bytes);
        let result = self.call_inner(name, args, deadline, max_output);
        // Whatever went wrong outside the function's own refusal, the instance's
        // memory is not to be relied on any more.
        if !matches!(result, Ok(_) | Err(CallError::Module { .. })) {
            self.poisoned = true;
        }
        result
    }

    fn call_inner(
        &mut self,
        name: &str,
        args: &[Arg<'_>],
        deadline: Option<Instant>,
        max_output: u32,
    ) -> Result<Reply, CallError> {
        if self.poisoned {
            return Err(CallError::Poisoned);
        }
        let too_large = |_| CallError::Trap("the arguments do not fit a module's memory".into());
        // One buffer in the module: the result pair, the name, the argument frame.
        let mut buffer = vec![0u8; 8];
        buffer.extend_from_slice(name.as_bytes());
        let frame_at = buffer.len();
        let count = u32::try_from(args.len()).map_err(too_large)?;
        buffer.extend_from_slice(&count.to_le_bytes());
        for arg in args {
            let (tag, bytes) = match arg {
                Arg::Binary(b) => (b'b', *b),
                Arg::Json(j) => (b'j', j.as_bytes()),
            };
            let len = u32::try_from(bytes.len()).map_err(too_large)?;
            buffer.push(tag);
            buffer.extend_from_slice(&len.to_le_bytes());
            buffer.extend_from_slice(bytes);
        }
        let total = u32::try_from(buffer.len()).map_err(too_large)?;
        let name_len = u32::try_from(name.len()).map_err(too_large)?;
        let frame_len = total - 8 - name_len;
        debug_assert_eq!(frame_at, 8 + name.len());

        let base = self.run(self.alloc.clone(), total, deadline)?;
        if base == 0 {
            return Err(CallError::Memory);
        }
        self.write(base, &buffer)?;
        // An address near the top of memory wraps; the module then faults on it.
        let name_at = base.wrapping_add(8);
        let status = self.run(
            self.call.clone(),
            (
                name_at,
                name_len,
                name_at.wrapping_add(name_len),
                frame_len,
                max_output,
                base,
            ),
            deadline,
        )?;
        let bytes = self.take_result(base, deadline, max_output)?;
        self.run(self.free.clone(), base, deadline)?;
        if status != 0 {
            return Err(CallError::Module {
                status,
                message: bytes,
            });
        }
        match bytes.split_first() {
            Some((b'j', text)) => Ok(Reply::Json(text.to_vec())),
            Some((b'b', data)) => Ok(Reply::Binary(data.to_vec())),
            _ => Err(CallError::Trap(
                "the module returned a value without a type tag".into(),
            )),
        }
    }
}
