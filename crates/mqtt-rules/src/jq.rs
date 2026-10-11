//! EMQX's `jq` rule function ([ADR 0086](../../../docs/adr/0086-wasm-rule-functions.md)):
//! the real jq, compiled to WebAssembly, run in a sandbox.
//!
//! `jq` is **off** unless the broker's operator switches it on, and what a rules file
//! is loaded with says so: [`Functions`]. A file that calls `jq` then loads only when
//! [`JqSwitch::On`] holds a [`Jq`] whose module could be compiled on this host; in
//! every other case the load fails and says why.
//!
//! What `jq/2` and `jq/3` do is `emqx_rule_funcs:jq/2,3` (probed on EMQX 6.3.1):
//!
//! * the program must be a string; an input that is a string is handed to jq's own
//!   JSON reader as text, any other value is written as JSON first;
//! * the result is the array of the program's outputs, each read back with the rule
//!   engine's JSON reader; outputs before an error are gone with it;
//! * `jq/3`'s third argument is a timeout in milliseconds, an integer of zero or more
//!   (anything else is `bad_timeout_val`, looked at before the program is); `jq/2`
//!   runs under the configured default, EMQX's `jq_function_default_timeout`;
//! * a failure fails the rule with EMQX's tag and text.
//!
//! Not EMQX's: the sandbox's limits (ADR 0086 D7), with the output of a call charged
//! to the message's build budget, and an environment that is empty unless listed (D8).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::plugin::{Module, ModuleHost, ModuleSettings};
use crate::value::Value;

/// What a caller on an async runtime wraps the rest of a long `jq` call in
/// ([`JqSettings::hand_off`]): it is given the rest of the call as a closure and runs
/// it, once, before returning.
pub type HandOff = Arc<dyn Fn(&mut dyn FnMut()) + Send + Sync>;

/// How `jq` runs once it is switched on.
#[derive(Clone)]
pub struct JqSettings {
    /// What `jq/2` runs under (EMQX's `rule_engine.jq_function_default_timeout`).
    pub default_timeout: Duration,
    /// What `$ENV` and `env` hold inside jq, as (name, value): nothing unless listed.
    pub environment: Vec<(String, String)>,
    /// How many warm instances are kept between calls: a broker's worker threads.
    pub instances: usize,
    /// Given the rest of a call that has run for a millisecond and is not done, so
    /// that a broker's worker thread can hand its other connections to another worker
    /// first (ADR 0086 D6). `None`: a long call simply runs where it was made.
    pub hand_off: Option<HandOff>,
}

impl std::fmt::Debug for JqSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JqSettings")
            .field("default_timeout", &self.default_timeout)
            // Names only: a value may be a secret.
            .field(
                "environment",
                &self.environment.iter().map(|(n, _)| n).collect::<Vec<_>>(),
            )
            .field("instances", &self.instances)
            .field("hand_off", &self.hand_off.is_some())
            .finish()
    }
}

impl Default for JqSettings {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(10),
            environment: Vec::new(),
            instances: 1,
            hand_off: None,
        }
    }
}

/// Whether rules may call `jq`.
#[derive(Debug, Clone, Default)]
pub enum JqSwitch {
    /// Not switched on (the default): a rules file that calls `jq` does not load, and
    /// the error names the switch.
    #[default]
    Off,
    /// Switched on in a build that cannot provide it, and why.
    Unavailable(String),
    /// Switched on.
    On(Arc<Jq>),
}

/// What a rules file may call beyond the built-in functions: what
/// [`RuleSet::parse_with`](crate::RuleSet::parse_with) loads it with.
#[derive(Debug, Clone, Default)]
pub struct Functions {
    /// The `jq` function.
    pub jq: JqSwitch,
    /// What loads the modules a rules file declares (`[functions.<name>]`, ADR 0086
    /// D9). `None`: a file that declares one does not load.
    modules: Option<Arc<ModuleHost>>,
    /// The directory modules are read from under: the rules file's.
    dir: Option<PathBuf>,
    /// The modules a statement on its own may call ([`declaring`](Self::declaring)).
    pub(crate) declared: Vec<Arc<Module>>,
}

impl Functions {
    /// `jq` switched on with `settings`, or [`JqSwitch::Unavailable`] in a build
    /// without it. Nothing is compiled, started or mapped by this: that happens when
    /// a rules file that calls `jq` is first loaded.
    #[must_use]
    pub fn with_jq(settings: JqSettings) -> Self {
        Self {
            jq: match Jq::new(settings) {
                Ok(jq) => JqSwitch::On(Arc::new(jq)),
                Err(why) => JqSwitch::Unavailable(why),
            },
            ..Self::default()
        }
    }

    /// The same, able to load the modules a rules file declares, each with `settings`.
    /// Nothing is read or compiled by this: a module is loaded when a rules file that
    /// declares it is.
    #[must_use]
    pub fn with_modules(mut self, settings: ModuleSettings) -> Self {
        self.modules = Some(Arc::new(ModuleHost::new(settings)));
        self
    }

    /// The same, for rules text that belongs in the file `rules_file`: the modules it
    /// declares are read from under that file's directory, and nowhere else.
    /// [`RuleSet::load_with`](crate::RuleSet::load_with) does this itself.
    #[must_use]
    pub fn beside(&self, rules_file: &Path) -> Self {
        let dir = match rules_file.parent() {
            Some(dir) if !dir.as_os_str().is_empty() => dir.to_path_buf(),
            _ => PathBuf::from("."),
        };
        Self {
            dir: Some(dir),
            ..self.clone()
        }
    }

    /// The same, for a statement on its own (`mqttd --rule-test`): it may call the
    /// functions of the modules the rules file `text` declares, which are loaded now.
    pub fn declaring(&self, text: &str) -> Result<Self, crate::LoadError> {
        let declared = crate::declared_modules(text, self)?;
        Ok(Self {
            declared,
            ..self.clone()
        })
    }

    /// What loads this node's modules, when it can.
    #[must_use]
    pub fn module_host(&self) -> Option<&Arc<ModuleHost>> {
        self.modules.as_ref()
    }

    /// Where modules are read from under.
    pub(crate) fn dir(&self) -> Option<&Path> {
        self.dir.as_deref()
    }

    /// The running `jq`, when it is switched on.
    #[must_use]
    pub fn jq(&self) -> Option<&Arc<Jq>> {
        match &self.jq {
            JqSwitch::On(jq) => Some(jq),
            _ => None,
        }
    }
}

/// What the load error says while the switch is off (ADR 0086 D13).
pub(crate) const SWITCHED_OFF: &str = "jq is switched off: set `rules.jq = true` to use it (see \
     RULES.md for what it needs from the host)";

/// What one instance pool holds and has done ([`Jq::stats`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ModuleStats {
    /// Instances held now, waiting or in a call.
    pub instances: u64,
    /// Of those, the ones in a call now.
    pub in_call: u64,
    /// Their linear memory in bytes, as of each one's last call.
    pub memory_bytes: u64,
    /// Instances given up after a call since the module was loaded: stopped at a limit,
    /// or grown too large to keep.
    pub replaced: u64,
}

/// One call of a sandboxed function, as [`Outcome::Function`](crate::Outcome::Function)
/// reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionCall {
    /// The function.
    pub function: Arc<str>,
    /// The module that provides it: `jq`, or the name of a `[functions.<name>]` table.
    pub module: Arc<str>,
    /// How long the call took.
    pub elapsed: Duration,
    /// The kind of failure, when it failed: for `jq`, `compile`, `parse`, `process`,
    /// `timeout` or `system` (EMQX's tags); `memory`, `output`, `stack` or
    /// `compile_timeout` (a limit of the sandbox); `trap`; or `argument` (a program
    /// that is not a string, a bad timeout) and `decode` (an output the rule engine's
    /// JSON reader refuses). For an operator's module: `module` (the function's own
    /// refusal), `timeout`, `memory`, `output`, `stack`, `trap`, `argument`, `decode`.
    pub error: Option<&'static str>,
    /// The module's instance pool after the call.
    pub stats: ModuleStats,
}

/// Why a `jq` call failed: the text the rule fails with, and the kind it is counted
/// under.
#[derive(Debug)]
pub(crate) struct Failure {
    pub text: String,
    pub kind: &'static str,
}

/// The `jq` function of one broker process: the module, compiled when a rules file
/// first calls `jq`, and the warm instances its callers take turns on.
pub struct Jq {
    /// `jq`: the function's name and its module's.
    name: Arc<str>,
    default_timeout: Duration,
    #[cfg(feature = "jq")]
    pool: mqtt_wasm_sandbox::jq::Pool,
}

impl std::fmt::Debug for Jq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Jq")
            .field("default_timeout", &self.default_timeout)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

// Without the `jq` feature every method is a stub that says so: it keeps its
// signature, and so takes what it does not use.
#[cfg_attr(
    not(feature = "jq"),
    allow(
        clippy::unused_self,
        clippy::needless_pass_by_value,
        clippy::unnecessary_wraps
    )
)]
impl Jq {
    /// Whether this build can run `jq` at all, and why not: it was built without the
    /// `jq` feature (no runtime), or without the module (`jq-module/build.sh`).
    pub fn built_in() -> Result<(), String> {
        #[cfg(feature = "jq")]
        {
            if mqtt_wasm_sandbox::jq::embedded().is_some() {
                Ok(())
            } else {
                Err("this mqttd was built without the jq module \
                     (crates/mqtt-wasm-sandbox/jq-module/build.sh builds it; release builds \
                     carry it)"
                    .to_string())
            }
        }
        #[cfg(not(feature = "jq"))]
        {
            Err("this mqttd was built without jq (the `jq` cargo feature is off)".to_string())
        }
    }

    /// Whether this host lets compiled WebAssembly run: it must allow memory to be
    /// mapped executable, which systemd's `MemoryDenyWriteExecute=true` does not. What
    /// `mqttd --check-config` reports while `jq` is switched on.
    pub fn probe() -> Result<(), String> {
        #[cfg(feature = "jq")]
        {
            mqtt_wasm_sandbox::Execution::Compiled
                .probe()
                .map_err(|e| e.to_string())
        }
        #[cfg(not(feature = "jq"))]
        {
            Self::built_in()
        }
    }

    /// The sha256 of the jq module this build carries, in hex.
    #[must_use]
    pub fn module_sha256() -> Option<&'static str> {
        #[cfg(feature = "jq")]
        {
            mqtt_wasm_sandbox::jq::embedded().map(|_| mqtt_wasm_sandbox::jq::recorded_sha256())
        }
        #[cfg(not(feature = "jq"))]
        {
            None
        }
    }

    /// `jq` with `settings`. Nothing is compiled yet. `Err`: this build has no jq
    /// ([`built_in`](Self::built_in)).
    pub fn new(settings: JqSettings) -> Result<Self, String> {
        Self::built_in()?;
        #[cfg(feature = "jq")]
        {
            use mqtt_wasm_sandbox::{jq, Execution, Grants, Limits};
            let module = jq::embedded().unwrap_or_default().to_vec();
            let grants = Grants {
                // `now`, `localtime`, `mktime` work as in EMQX (ADR 0086 D8).
                clock: true,
                environment: settings
                    .environment
                    .iter()
                    .map(|(name, value)| format!("{name}={value}").into_bytes())
                    .collect(),
            };
            let limits = Limits {
                max_output_bytes: u32::try_from(crate::funcs::MAX_BUILT_BYTES).unwrap_or(u32::MAX),
                ..Limits::default()
            };
            let mut pool = jq::Pool::new(
                module,
                Execution::Compiled,
                limits,
                grants,
                settings.instances.max(1),
            );
            if let Some(hand_off) = settings.hand_off {
                pool = pool.with_hand_off(hand_off);
            }
            Ok(Self {
                name: Arc::from("jq"),
                default_timeout: settings.default_timeout,
                pool,
            })
        }
        #[cfg(not(feature = "jq"))]
        {
            let _ = settings;
            Err(String::new())
        }
    }

    /// `jq`, as the name its calls are reported under.
    pub(crate) fn name(&self) -> &Arc<str> {
        &self.name
    }

    /// What `jq/2` runs under.
    #[must_use]
    pub fn default_timeout(&self) -> Duration {
        self.default_timeout
    }

    /// Whether the module has been compiled: whether any rules file that calls `jq`
    /// has been loaded in this process.
    #[must_use]
    pub fn is_loaded(&self) -> bool {
        #[cfg(feature = "jq")]
        {
            self.pool.is_loaded()
        }
        #[cfg(not(feature = "jq"))]
        {
            false
        }
    }

    /// The instance pool now.
    #[must_use]
    pub fn stats(&self) -> ModuleStats {
        #[cfg(feature = "jq")]
        {
            let stats = self.pool.stats();
            ModuleStats {
                instances: stats.instances,
                in_call: stats.in_call,
                memory_bytes: stats.memory_bytes,
                replaced: stats.replaced,
            }
        }
        #[cfg(not(feature = "jq"))]
        {
            ModuleStats::default()
        }
    }

    /// Compiles the module if no load has yet (about half a second, once per process),
    /// and says why it cannot be: what a rules file that calls `jq` fails to load with.
    pub(crate) fn load(&self) -> Result<(), String> {
        #[cfg(feature = "jq")]
        {
            self.pool.load().map(|_| ()).map_err(|e| {
                let reason = String::from_utf8_lossy(&e.message).into_owned();
                if self.pool.refused_by_host() {
                    // ADR 0086 D13: the way out, where the operator is looking.
                    format!(
                        "{reason}. Compiled code needs memory the process may map executable: \
                         under systemd set `MemoryDenyWriteExecute=false` for the service \
                         (`systemctl edit mqttd`), or lift the seccomp or SELinux rule that \
                         forbids it; see RULES.md"
                    )
                } else {
                    reason
                }
            })
        }
        #[cfg(not(feature = "jq"))]
        {
            Self::built_in()
        }
    }

    /// Compiles one literal program in one warm instance; `Err` is why it does not
    /// compile (jq's own text), which every call with it will fail with.
    pub(crate) fn check_program(&self, program: &[u8]) -> Result<(), String> {
        #[cfg(feature = "jq")]
        {
            self.pool.compile(program).map_err(|e| e.to_string())
        }
        #[cfg(not(feature = "jq"))]
        {
            let _ = program;
            Ok(())
        }
    }

    /// Compiles the programs a rules file spells out into every warm instance, off
    /// the publish path (ADR 0086 D5).
    pub(crate) fn warm(&self, programs: &[&[u8]]) {
        #[cfg(feature = "jq")]
        {
            // A failure here is not the load's: the module is loaded, and a call that
            // meets a program its instance has not compiled compiles it then.
            let _ = self.pool.warm(programs);
        }
        #[cfg(not(feature = "jq"))]
        {
            let _ = programs;
        }
    }

    /// One call: `program` on `input`, under `timeout` (`None`: no deadline), with at
    /// most `max_output` bytes of result. The text of a JSON array of the outputs.
    pub(crate) fn call(
        &self,
        program: &[u8],
        input: &Value,
        timeout: Option<Duration>,
        max_output: usize,
    ) -> Result<Vec<u8>, Failure> {
        #[cfg(feature = "jq")]
        {
            use mqtt_wasm_sandbox::jq::Tag;
            use mqtt_wasm_sandbox::Arg;
            let max_output = u32::try_from(max_output).unwrap_or(u32::MAX);
            let encoded;
            let input = match input.as_bytes() {
                Some(text) if input.is_binary() => Arg::Binary(text),
                _ => {
                    encoded = input.to_json().map_err(|e| Failure {
                        text: e.to_string(),
                        kind: "argument",
                    })?;
                    Arg::Json(&encoded)
                }
            };
            self.pool
                .eval_within(program, input, timeout, max_output)
                .map_err(|e| Failure {
                    text: match e.tag {
                        // The sandbox's own limits say which one.
                        Tag::Limit => format!(
                            "{} ({}): {}",
                            e.tag.as_str(),
                            e.kind,
                            String::from_utf8_lossy(&e.message).trim_end()
                        ),
                        _ => e.to_string(),
                    },
                    kind: e.kind,
                })
        }
        #[cfg(not(feature = "jq"))]
        {
            let _ = (program, input, timeout, max_output);
            Err(Failure {
                text: Self::built_in().err().unwrap_or_default(),
                kind: "system",
            })
        }
    }
}
