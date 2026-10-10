//! Loading a module, making instances of it, and calling a function in one under
//! limits: time (fuel in slices, the clock read between slices), memory, stack and
//! output size.

use std::fmt;
use std::time::Instant;

use wasmi::{
    CompilationMode, Config, Engine, ExternType, Linker, Memory, Module, ResourceLimiter, Store,
    TrapCode, TypedFunc, TypedResumableCall, WasmParams, WasmResults,
};
use wasmi_core::LimiterError;

use crate::wasi::{self, Grants};

/// The ABI version this host speaks (`mqttd_abi_version`).
pub const ABI_VERSION: u32 = 1;

/// The bounds one instance and one call run under.
#[derive(Debug, Clone)]
pub struct Limits {
    /// Most linear memory an instance may hold, in bytes.
    pub memory_bytes: usize,
    /// Largest result a call may return, in bytes.
    pub max_output_bytes: u32,
    /// Fuel (about one unit per executed instruction) between two looks at the
    /// clock. Smaller is a tighter timeout and more overhead.
    pub fuel_slice: u64,
    /// Most fuel one call may burn, whatever the clock says: a bound that does not
    /// depend on how fast the machine is. `None` leaves the deadline as the only one.
    pub max_fuel: Option<u64>,
    /// Fuel a call may still burn after the deadline when the module can be asked
    /// to stop (`mqttd_cancel`): the time it has to end by itself and stay usable.
    pub cancel_grace_fuel: u64,
    /// Fuel for instantiation (data segments, constructors).
    pub init_fuel: u64,
    /// Deepest chain of calls inside the module.
    pub max_call_depth: usize,
    /// Largest interpreter value stack, in bytes.
    pub max_stack_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            memory_bytes: 64 << 20,
            max_output_bytes: 1 << 20,
            fuel_slice: 200_000,
            max_fuel: None,
            cancel_grace_fuel: 2_000_000,
            init_fuel: 50_000_000,
            max_call_depth: 100_000,
            max_stack_bytes: 8 << 20,
        }
    }
}

/// Why a module was not loaded.
#[derive(Debug)]
pub enum LoadError {
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
    /// The call chain or the value stack outgrew its limit.
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

/// The state the host keeps per instance, reachable from the imports.
#[derive(Debug)]
pub(crate) struct Host {
    pub(crate) grants: Grants,
    pub(crate) discarded_output: u64,
    limiter: MemoryCap,
}

/// Refuses linear memory beyond a cap, by a trap at the `memory.grow` that asks for
/// it — rather than by handing the module's allocator a failure it may or may not
/// survive. (wasmi's own `StoreLimits` can trap too, but then it also turns a
/// `memory.grow` that merely has to wait for its fuel into that trap.)
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
    ) -> Result<bool, LimiterError> {
        if desired > self.max_bytes {
            return Err(LimiterError::ResourceLimiterDeniedAllocation);
        }
        Ok(maximum.is_none_or(|max| desired <= max))
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> Result<bool, LimiterError> {
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

/// A validated, translated module: made once, shared by every instance of it.
#[derive(Clone)]
pub struct Sandbox {
    engine: Engine,
    module: Module,
    linker: Linker<Host>,
    limits: Limits,
    grants: Grants,
    description: Vec<u8>,
}

impl fmt::Debug for Sandbox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sandbox")
            .field("limits", &self.limits)
            .field("description", &String::from_utf8_lossy(&self.description))
            .finish_non_exhaustive()
    }
}

impl Sandbox {
    /// Validates `wasm`, checks its imports against the allowed list and its exports
    /// against the ABI, and starts one instance to read its description.
    pub fn load(wasm: &[u8], limits: Limits, grants: Grants) -> Result<Self, LoadError> {
        let mut config = Config::default();
        config
            .consume_fuel(true)
            // Everything is translated here, once, when the module is loaded: a call
            // never pays for translation, in time or in fuel, so the same call burns
            // the same fuel every time.
            .compilation_mode(CompilationMode::Eager)
            .set_max_recursion_depth(limits.max_call_depth)
            .set_max_stack_height(limits.max_stack_bytes)
            .wasm_multi_memory(false)
            .wasm_tail_call(false)
            .wasm_custom_page_sizes(false);
        let engine = Engine::new(&config);
        let module = Module::new(&engine, wasm).map_err(|e| LoadError::Invalid(e.to_string()))?;
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
        wasi::define(&mut linker).map_err(|e| LoadError::Invalid(e.to_string()))?;
        let mut sandbox = Self {
            engine,
            module,
            linker,
            limits,
            grants,
            description: Vec::new(),
        };
        let mut first = sandbox.instantiate()?;
        let deadline = None;
        let version = first
            .run(first.abi_version, (), deadline)
            .map_err(LoadError::Start)?;
        if version != ABI_VERSION {
            return Err(LoadError::AbiVersion(version));
        }
        sandbox.description = first.describe().map_err(LoadError::Start)?;
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

    /// A fresh instance: its own memory, nothing shared with any other.
    pub fn instantiate(&self) -> Result<Instance, LoadError> {
        let host = Host {
            grants: self.grants.clone(),
            discarded_output: 0,
            limiter: MemoryCap {
                max_bytes: self.limits.memory_bytes,
            },
        };
        let mut store = Store::new(&self.engine, host);
        store.limiter(|host| &mut host.limiter);
        let start = |e: wasmi::Error| LoadError::Start(CallError::Trap(e.to_string()));
        store.set_fuel(self.limits.init_fuel).map_err(start)?;
        let instance = self
            .linker
            .instantiate_and_start(&mut store, &self.module)
            .map_err(start)?;
        let export = |e: wasmi::Error| LoadError::Export(e.to_string());
        let memory = instance
            .get_memory(&store, "memory")
            .ok_or_else(|| LoadError::Export("no exported memory".to_owned()))?;
        let mut made = Instance {
            abi_version: instance
                .get_typed_func(&store, "mqttd_abi_version")
                .map_err(export)?,
            alloc: instance
                .get_typed_func(&store, "mqttd_alloc")
                .map_err(export)?,
            free: instance
                .get_typed_func(&store, "mqttd_free")
                .map_err(export)?,
            describe: instance
                .get_typed_func(&store, "mqttd_describe")
                .map_err(export)?,
            call: instance
                .get_typed_func(&store, "mqttd_call")
                .map_err(export)?,
            cancel: instance.get_typed_func(&store, "mqttd_cancel").ok(),
            memory,
            module: instance,
            store,
            limits: self.limits.clone(),
            poisoned: false,
            fuel_used: 0,
        };
        // A reactor's constructors (libc start-up), when it has them.
        if let Ok(init) = instance.get_typed_func::<(), ()>(&made.store, "_initialize") {
            let budget = made.limits.init_fuel;
            made.run_within(init, (), None, Some(budget))
                .map_err(LoadError::Start)?;
        }
        Ok(made)
    }
}

/// One instance of a module: one linear memory, used by one caller at a time.
pub struct Instance {
    store: Store<Host>,
    module: wasmi::Instance,
    memory: Memory,
    abi_version: TypedFunc<(), u32>,
    alloc: TypedFunc<u32, u32>,
    free: TypedFunc<u32, ()>,
    describe: TypedFunc<u32, ()>,
    call: TypedFunc<(u32, u32, u32, u32, u32, u32), i32>,
    /// `mqttd_cancel`, when the module has it.
    cancel: Option<TypedFunc<(), ()>>,
    limits: Limits,
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

    /// Fuel burnt by every call on this instance so far.
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
    pub fn call_extra<R: WasmResults>(&mut self, name: &str) -> Result<R, CallError> {
        let func = self
            .module
            .get_typed_func::<(), R>(&self.store, name)
            .map_err(|e| CallError::Trap(e.to_string()))?;
        self.run(func, (), None)
    }

    /// Runs `func` in fuel slices until it returns, traps, or the deadline passes.
    fn run<P: WasmParams, R: WasmResults>(
        &mut self,
        func: TypedFunc<P, R>,
        params: P,
        deadline: Option<Instant>,
    ) -> Result<R, CallError> {
        let max_fuel = self.limits.max_fuel;
        self.run_within(func, params, deadline, max_fuel)
    }

    fn run_within<P: WasmParams, R: WasmResults>(
        &mut self,
        func: TypedFunc<P, R>,
        params: P,
        deadline: Option<Instant>,
        max_fuel: Option<u64>,
    ) -> Result<R, CallError> {
        if self.poisoned {
            return Err(CallError::Poisoned);
        }
        let result = self.slices(func, params, deadline, max_fuel);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn slices<P: WasmParams, R: WasmResults>(
        &mut self,
        func: TypedFunc<P, R>,
        params: P,
        deadline: Option<Instant>,
        max_fuel: Option<u64>,
    ) -> Result<R, CallError> {
        let slice = self.limits.fuel_slice.max(1);
        let mut granted = slice;
        let mut burnt = 0u64;
        // Once the module has been asked to stop: the fuel it has left to do so.
        let mut grace: Option<u64> = None;
        self.set_fuel(granted)?;
        let mut state = func.call_resumable(&mut self.store, params);
        loop {
            match state {
                Ok(TypedResumableCall::Finished(results)) => {
                    let left = self.store.get_fuel().unwrap_or(0);
                    self.fuel_used += granted.saturating_sub(left);
                    return Ok(results);
                }
                Ok(TypedResumableCall::OutOfFuel(paused)) => {
                    burnt += granted;
                    self.fuel_used += granted;
                    if max_fuel.is_some_and(|max| burnt >= max) {
                        return Err(CallError::Fuel);
                    }
                    if let Some(left) = grace {
                        if left == 0 {
                            return Err(CallError::Timeout);
                        }
                        grace = Some(left.saturating_sub(granted));
                    } else if deadline.is_some_and(|d| Instant::now() >= d) {
                        // The paused call keeps its own stack; this one runs beside it.
                        let Some(cancel) = self.cancel else {
                            return Err(CallError::Timeout);
                        };
                        self.set_fuel(slice)?;
                        cancel
                            .call(&mut self.store, ())
                            .map_err(|e| Self::classify(&e))?;
                        grace = Some(self.limits.cancel_grace_fuel);
                    }
                    granted = slice.max(paused.required_fuel());
                    self.set_fuel(granted)?;
                    state = paused.resume(&mut self.store);
                }
                Ok(TypedResumableCall::HostTrap(trap)) => {
                    return Err(CallError::Trap(trap.host_error().to_string()));
                }
                Err(error) => return Err(Self::classify(&error)),
            }
        }
    }

    fn set_fuel(&mut self, fuel: u64) -> Result<(), CallError> {
        self.store
            .set_fuel(fuel)
            .map_err(|e| CallError::Trap(e.to_string()))
    }

    /// Names the limit behind a trap, when a limit is behind it.
    fn classify(error: &wasmi::Error) -> CallError {
        match error.as_trap_code() {
            Some(TrapCode::GrowthOperationLimited) => CallError::Memory,
            Some(TrapCode::StackOverflow) => CallError::Stack,
            Some(TrapCode::OutOfFuel) => CallError::Fuel,
            _ => CallError::Trap(error.to_string()),
        }
    }

    fn read(&self, at: u32, len: u32) -> Result<Vec<u8>, CallError> {
        let mut bytes = vec![0u8; len as usize];
        self.memory
            .read(&self.store, at as usize, &mut bytes)
            .map_err(|_| {
                CallError::Trap("the module returned an address outside its memory".into())
            })?;
        Ok(bytes)
    }

    fn write(&mut self, at: u32, bytes: &[u8]) -> Result<(), CallError> {
        self.memory
            .write(&mut self.store, at as usize, bytes)
            .map_err(|_| CallError::Trap("the module allocated outside its memory".into()))
    }

    /// Reads the (address, length) pair a function left at `ret`, takes the bytes
    /// and frees them in the module.
    fn take_result(&mut self, ret: u32, deadline: Option<Instant>) -> Result<Vec<u8>, CallError> {
        let pair = self.read(ret, 8)?;
        let at = u32::from_le_bytes([pair[0], pair[1], pair[2], pair[3]]);
        let len = u32::from_le_bytes([pair[4], pair[5], pair[6], pair[7]]);
        if len > self.limits.max_output_bytes.saturating_add(1) {
            return Err(CallError::OutputTooLarge);
        }
        let bytes = if at == 0 {
            Vec::new()
        } else {
            self.read(at, len)?
        };
        if at != 0 {
            self.run(self.free, at, deadline)?;
        }
        Ok(bytes)
    }

    fn describe(&mut self) -> Result<Vec<u8>, CallError> {
        let ret = self.run(self.alloc, 8, None)?;
        self.run(self.describe, ret, None)?;
        let text = self.take_result(ret, None)?;
        self.run(self.free, ret, None)?;
        Ok(text)
    }

    /// Calls the function `name` with `args`. A call that ends in anything but a
    /// result or [`CallError::Module`] leaves the instance poisoned.
    pub fn call(
        &mut self,
        name: &str,
        args: &[Arg<'_>],
        deadline: Option<Instant>,
    ) -> Result<Reply, CallError> {
        let max_fuel = self.limits.max_fuel;
        self.call_bounded(name, args, deadline, max_fuel)
    }

    /// [`call`](Self::call) with no deadline and `max_fuel` as the bound: the same
    /// work stops at the same point on any machine.
    pub fn call_with_fuel(
        &mut self,
        name: &str,
        args: &[Arg<'_>],
        max_fuel: u64,
    ) -> Result<Reply, CallError> {
        self.call_bounded(name, args, None, Some(max_fuel))
    }

    fn call_bounded(
        &mut self,
        name: &str,
        args: &[Arg<'_>],
        deadline: Option<Instant>,
        max_fuel: Option<u64>,
    ) -> Result<Reply, CallError> {
        let result = self.call_inner(name, args, deadline, max_fuel);
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
        max_fuel: Option<u64>,
    ) -> Result<Reply, CallError> {
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

        let base = self.run(self.alloc, total, deadline)?;
        if base == 0 {
            return Err(CallError::Memory);
        }
        self.write(base, &buffer)?;
        // An address near the top of memory wraps; the module then faults on it.
        let name_at = base.wrapping_add(8);
        let status = self.run_within(
            self.call,
            (
                name_at,
                name_len,
                name_at.wrapping_add(name_len),
                frame_len,
                self.limits.max_output_bytes,
                base,
            ),
            deadline,
            max_fuel,
        )?;
        let bytes = self.take_result(base, deadline)?;
        self.run(self.free, base, deadline)?;
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
