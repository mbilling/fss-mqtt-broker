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
//!   under a fuel bound instead ([`COMPILE_FUEL`]).

use std::fmt;
use std::time::{Duration, Instant};

use crate::sandbox::{Arg, CallError, Instance, Limits, LoadError, Reply, Sandbox};
use crate::wasi::Grants;

/// `rule_engine.jq_function_default_timeout`: what `jq/2` runs under.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// The most fuel compiling one program may burn. The longest rule SQL (64 KiB of
/// program text) compiles in a small fraction of it.
pub const COMPILE_FUEL: u64 = 20_000_000_000;

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
    /// `jq_err_system`: jq could not start.
    System,
    /// Not in EMQX: the memory, stack, fuel or output limit of the sandbox.
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
    /// Parts of the last call.
    pub last: Timing,
}

impl Jq {
    /// Loads the jq module with the default limits. `grants` is what jq's `now` and
    /// `$ENV` see.
    pub fn new(wasm: &[u8], grants: Grants) -> Result<Self, LoadError> {
        Ok(Self::from_sandbox(Sandbox::load(
            wasm,
            Limits::default(),
            grants,
        )?))
    }

    /// The function on an already loaded module (a [`Sandbox`] is cheap to clone:
    /// every caller shares the translated code and owns only its instance).
    #[must_use]
    pub fn from_sandbox(sandbox: Sandbox) -> Self {
        Self {
            sandbox,
            instance: None,
            instances: 0,
            last: Timing::default(),
        }
    }

    /// Drops the warm instance: the next call starts from a fresh one.
    pub fn reset(&mut self) {
        self.instance = None;
    }

    /// Fuel burnt by the warm instance so far.
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
        let started = Instant::now();
        let deadline = timeout.map(|t| started + t);
        self.last = Timing::default();
        let result = self.eval_on_instance(program, input, deadline, timeout);
        let stopped = self
            .instance
            .as_ref()
            .is_some_and(|i| i.is_poisoned() || i.memory_bytes() > RECYCLE_ABOVE_BYTES);
        if stopped {
            self.instance = None;
        }
        result
    }

    fn eval_on_instance(
        &mut self,
        program: &[u8],
        input: Arg<'_>,
        deadline: Option<Instant>,
        timeout: Option<Duration>,
    ) -> Result<Vec<u8>, JqError> {
        if self.instance.is_none() {
            let at = Instant::now();
            let made = self.sandbox.instantiate().map_err(|e| JqError {
                tag: Tag::System,
                message: e.to_string().into_bytes(),
            })?;
            self.instance = Some(made);
            self.instances += 1;
            self.last.instantiate = at.elapsed();
        }
        let Some(instance) = self.instance.as_mut() else {
            return Err(JqError {
                tag: Tag::System,
                message: b"no instance".to_vec(),
            });
        };
        let args = [Arg::Binary(program), input];
        let at = Instant::now();
        let mut reply = instance.call("jq", &args, deadline);
        if matches!(
            &reply,
            Err(CallError::Module {
                status: NOT_COMPILED,
                ..
            })
        ) {
            let compiled = instance.call_with_fuel("jq_compile", &args[..1], COMPILE_FUEL);
            self.last.compile = at.elapsed();
            compiled.map_err(|e| error(e, timeout))?;
            let at = Instant::now();
            reply = instance.call("jq", &args, deadline);
            self.last.run = at.elapsed();
        } else {
            self.last.run = at.elapsed();
        }
        match reply {
            Ok(Reply::Json(text)) => Ok(text),
            Ok(Reply::Binary(_)) => Err(JqError {
                tag: Tag::System,
                message: b"the jq module returned a binary".to_vec(),
            }),
            Err(e) => Err(error(e, timeout)),
        }
    }
}

/// A sandbox error as the `jq` function reports it.
fn error(e: CallError, timeout: Option<Duration>) -> JqError {
    let (tag, message) = match e {
        CallError::Module { status, message } => {
            let tag = match status {
                // The program saw the cancel flag and ended by itself.
                7 => return error(CallError::Timeout, timeout),
                4 => Tag::Compile,
                5 => Tag::Parse,
                6 => Tag::Process,
                8 => Tag::Limit,
                _ => Tag::System,
            };
            (tag, message)
        }
        // The NIF's text, completed by `emqx_rule_funcs:jq/3`.
        CallError::Timeout => (
            Tag::Timeout,
            format!(
                "jq program canceled as it took too long time to execute (timeout set to {} ms)",
                timeout.map_or(0, |t| t.as_millis())
            )
            .into_bytes(),
        ),
        CallError::Memory | CallError::Stack | CallError::Fuel | CallError::OutputTooLarge => {
            (Tag::Limit, format!("jq stopped: {e}").into_bytes())
        }
        CallError::Trap(_) | CallError::Poisoned => (Tag::System, e.to_string().into_bytes()),
    };
    JqError { tag, message }
}
