//! Functions an operator supplies as WebAssembly modules
//! ([ADR 0086](../../../docs/adr/0086-wasm-rule-functions.md) D9): the `[functions.<name>]`
//! tables of a rules file.
//!
//! ```toml
//! [functions.geo]
//! wasm = "plugins/geo.wasm"        # under the rules file's directory
//! sha256 = "9f2c…"                 # the load fails on any other content
//! functions = ["geo_distance"]     # among those the module describes
//! timeout_ms = 50                  # per call; default 100, at most 10000
//! memory_mib = 16                  # per instance; default 16, at most 64
//! clock = false                    # the wall clock; without it every clock reads 0
//! env = []                         # variables the module sees; each must be listed
//!                                  # in the broker's rules.functions_env
//! fresh_instance = false           # true: no state of the module survives a call
//! ```
//!
//! **This is an mqttd extension**: EMQX has no such table, and a rules file that uses
//! it does not load there (D12). A module's function can never change what an EMQX rule
//! means, because its name may not be a built-in function's or any EMQX function's
//! ([`reserved`]).
//!
//! **What is checked when the rules are loaded**, each a load error with its reason
//! (the running rules stay): the path, the file's sha256 against the pin, that it is a
//! WebAssembly module importing nothing but the allowed list and exporting the function
//! ABI in this host's version, that it describes every function the table names, and
//! the names.
//!
//! **Who may name a file.** A rules file comes from the operator's disk or, where
//! `rules.admin_writers` lists someone, through the admin API; a check or a dry run of
//! rules text needs only the operator role. None of them may make the broker read an
//! arbitrary file, so a module is only ever read from **under the directory of the
//! configured rules file**: a relative path of plain names ending in `.wasm`, which
//! after every symlink is resolved still lies in that directory. Nothing the broker
//! writes there is a module (it writes the rules file, which is TOML), so every file
//! that can be loaded is one the operator placed. The error for a wrong hash does not
//! say what the file's hash is: whoever declares a module has to know its content.
//! The environment a module sees is the same kind of grant: a table may ask only for
//! variables the broker's own configuration lists (`rules.functions_env`).
//!
//! **A live edit** (a reload, an admin API write) loads the new file's modules beside
//! the running ones. A declaration that did not change keeps its loaded module and its
//! warm instances; a changed one (another hash, other limits) is loaded anew, and the
//! old module lives until the last evaluation that started on the old rules is done.

use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use serde::Deserialize;

use crate::jq::{HandOff, ModuleStats};
use crate::value::Value;

/// The most modules one rules file may declare.
pub const MAX_MODULES: usize = 16;

/// The largest module file read.
pub const MAX_MODULE_BYTES: u64 = 16 << 20;

/// The most functions one module's table may name.
pub const MAX_FUNCTIONS_PER_MODULE: usize = 64;

/// What a call runs under when the table does not say (`timeout_ms`).
pub const DEFAULT_TIMEOUT_MS: u64 = 100;

/// The longest `timeout_ms` a table may ask for: what `jq/2` is given by default.
pub const MAX_TIMEOUT_MS: u64 = 10_000;

/// An instance's memory when the table does not say (`memory_mib`).
pub const DEFAULT_MEMORY_MIB: u64 = 16;

/// The broker's ceiling on `memory_mib`: what a `jq` instance may hold.
pub const MAX_MEMORY_MIB: u64 = 64;

/// One `[functions.<name>]` table, as the rules file writes it.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ModuleSchema {
    wasm: String,
    sha256: String,
    functions: Vec<String>,
    timeout_ms: Option<u64>,
    memory_mib: Option<u64>,
    #[serde(default)]
    clock: bool,
    #[serde(default)]
    env: Vec<String>,
    #[serde(default)]
    fresh_instance: bool,
}

/// What a broker gives every module its rules files declare.
#[derive(Clone, Default)]
pub struct ModuleSettings {
    /// The environment variables a table's `env` may name, each with the value it has
    /// (`None`: not set, and so not given): the broker's `rules.functions_env`.
    pub environment: Vec<(String, Option<String>)>,
    /// How many warm instances a module keeps between calls: a broker's worker threads.
    pub instances: usize,
    /// Given the rest of a call that has run for a millisecond and is not done
    /// (ADR 0086 D6), as for `jq`.
    pub hand_off: Option<HandOff>,
}

impl std::fmt::Debug for ModuleSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModuleSettings")
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

/// What loads modules for one broker process: its settings, and the modules now
/// loaded, so that a reload which leaves a declaration as it was keeps the module.
#[derive(Debug, Default)]
pub struct ModuleHost {
    settings: ModuleSettings,
    /// The modules some rule set still holds. Weak: a module goes when the last rule
    /// set that declared it does.
    loaded: Mutex<Vec<Weak<Module>>>,
}

impl ModuleHost {
    /// A host with `settings`. Nothing is read, compiled or started by this.
    #[must_use]
    pub fn new(settings: ModuleSettings) -> Self {
        Self {
            settings,
            loaded: Mutex::new(Vec::new()),
        }
    }

    fn loaded(&self) -> std::sync::MutexGuard<'_, Vec<Weak<Module>>> {
        self.loaded.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// How many modules are loaded now, in any rule set still alive.
    #[must_use]
    pub fn loaded_modules(&self) -> usize {
        self.loaded()
            .iter()
            .filter(|m| m.strong_count() > 0)
            .count()
    }
}

/// One function a module provides, as its table names it and its module describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleFunction {
    /// The name rules call it by.
    pub name: Arc<str>,
    /// The fewest arguments it takes.
    pub min_args: usize,
    /// The most.
    pub max_args: usize,
}

/// What a table said, resolved: what makes two loads the same module. (No `Debug`:
/// the environment's values may be secrets.)
#[derive(Clone, PartialEq, Eq)]
struct Declaration {
    name: String,
    wasm: String,
    sha256: String,
    functions: Vec<String>,
    timeout_ms: u64,
    memory_mib: u64,
    clock: bool,
    /// The granted variables, as (name, value).
    environment: Vec<(String, String)>,
    fresh_instance: bool,
}

/// One loaded module: a `[functions.<name>]` table whose file passed every check.
pub struct Module {
    declaration: Declaration,
    /// What the module calls itself and its version (`mqttd_describe`).
    described: (String, String),
    functions: Vec<ModuleFunction>,
    name: Arc<str>,
    /// Instances the modules loaded under this name before this one had given up when
    /// it was loaded: what its own count goes on from, so the count of a name does not
    /// start again at a reload that changes its module.
    replaced_before: u64,
    #[cfg(feature = "jq")]
    pool: mqtt_wasm_sandbox::ModulePool,
}

impl std::fmt::Debug for Module {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Module")
            .field("name", &self.name)
            .field("sha256", &self.declaration.sha256)
            .field("functions", &self.functions)
            .finish_non_exhaustive()
    }
}

impl Module {
    /// The table's name (`[functions.<name>]`): what metrics call the module.
    #[must_use]
    pub fn name(&self) -> &Arc<str> {
        &self.name
    }

    /// The path the table gives, relative to the rules file's directory.
    #[must_use]
    pub fn wasm(&self) -> &str {
        &self.declaration.wasm
    }

    /// The file's sha256, in lower-case hex: the table's pin.
    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.declaration.sha256
    }

    /// What the module calls itself.
    #[must_use]
    pub fn described_name(&self) -> &str {
        &self.described.0
    }

    /// The version the module gives itself.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.described.1
    }

    /// The functions rules may call.
    #[must_use]
    pub fn functions(&self) -> &[ModuleFunction] {
        &self.functions
    }

    /// How long one call may run.
    #[must_use]
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.declaration.timeout_ms)
    }

    /// The most linear memory one instance may hold, in bytes.
    #[must_use]
    pub fn memory_limit_bytes(&self) -> u64 {
        self.declaration.memory_mib << 20
    }

    /// Whether the module reads the wall clock.
    #[must_use]
    pub fn clock(&self) -> bool {
        self.declaration.clock
    }

    /// The names of the environment variables the module sees.
    pub fn environment(&self) -> impl Iterator<Item = &str> {
        self.declaration.environment.iter().map(|(n, _)| n.as_str())
    }

    /// Whether every call gets an instance of its own.
    #[must_use]
    pub fn fresh_instance(&self) -> bool {
        self.declaration.fresh_instance
    }

    /// The module's instances now, and how many instances the modules of this name
    /// have given up after a call since the process started.
    #[must_use]
    pub fn stats(&self) -> ModuleStats {
        #[cfg(feature = "jq")]
        {
            let stats = self.pool.stats();
            ModuleStats {
                instances: stats.instances,
                in_call: stats.in_call,
                memory_bytes: stats.memory_bytes,
                replaced: self.replaced_before.saturating_add(stats.replaced),
            }
        }
        #[cfg(not(feature = "jq"))]
        {
            ModuleStats {
                replaced: self.replaced_before,
                ..ModuleStats::default()
            }
        }
    }

    /// One call of `function`: the arguments cross as ADR 0086 D3 says (a binary as its
    /// bytes, any other value as the JSON text the rule engine writes), and the result
    /// comes back the same way, at most `max_output` bytes of it.
    #[cfg_attr(
        not(feature = "jq"),
        allow(clippy::unused_self, clippy::needless_pass_by_value)
    )]
    pub(crate) fn call(
        &self,
        function: &str,
        args: &[Value],
        max_output: usize,
    ) -> Result<Reply, Failure> {
        #[cfg(feature = "jq")]
        {
            use mqtt_wasm_sandbox::{Arg, CallError};
            let mut encoded = Vec::with_capacity(args.len());
            for value in args {
                encoded.push(match value.as_bytes() {
                    Some(_) if value.is_binary() => None,
                    _ => Some(value.to_json().map_err(|e| Failure {
                        text: e.to_string(),
                        kind: "argument",
                    })?),
                });
            }
            let frame: Vec<Arg<'_>> = args
                .iter()
                .zip(&encoded)
                .map(|(value, json)| match json {
                    Some(json) => Arg::Json(json),
                    None => Arg::Binary(value.as_bytes().unwrap_or_default()),
                })
                .collect();
            let max_output = u32::try_from(max_output).unwrap_or(u32::MAX);
            let timeout = self.timeout();
            let limit = |kind: &'static str, what: String| Failure {
                text: format!("sandbox_limit ({kind}): {what}"),
                kind,
            };
            match self.pool.call(function, &frame, timeout, max_output) {
                Ok(mqtt_wasm_sandbox::Reply::Binary(bytes)) => Ok(Reply::Binary(bytes)),
                Ok(mqtt_wasm_sandbox::Reply::Json(text)) => Ok(Reply::Json(text)),
                Err(CallError::Module { status, message }) => Err(Failure {
                    text: format!(
                        "status {status}: {}",
                        String::from_utf8_lossy(&message).trim_end()
                    ),
                    kind: "module",
                }),
                Err(CallError::Timeout) => Err(limit(
                    "timeout",
                    format!(
                        "the call ran past its {} ms (timeout_ms of [functions.{}])",
                        timeout.as_millis(),
                        self.name
                    ),
                )),
                Err(e @ CallError::Memory) => Err(limit(
                    "memory",
                    format!(
                        "{e} ({} MiB, memory_mib of [functions.{}])",
                        self.declaration.memory_mib, self.name
                    ),
                )),
                Err(e @ CallError::Stack) => Err(limit("stack", e.to_string())),
                Err(e @ CallError::Fuel) => Err(limit("fuel", e.to_string())),
                Err(CallError::OutputTooLarge) => Err(limit(
                    "output",
                    format!(
                        "the result is larger than what is left of the message's {} bytes \
                         of function output",
                        crate::funcs::MAX_BUILT_BYTES
                    ),
                )),
                Err(e @ (CallError::Trap(_) | CallError::Poisoned)) => Err(Failure {
                    text: e.to_string(),
                    kind: "trap",
                }),
            }
        }
        #[cfg(not(feature = "jq"))]
        {
            let _ = (function, args, max_output);
            Err(Failure {
                text: NO_RUNTIME.to_string(),
                kind: "system",
            })
        }
    }
}

/// What a call returned.
#[cfg_attr(not(feature = "jq"), allow(dead_code))]
pub(crate) enum Reply {
    /// A binary.
    Binary(Vec<u8>),
    /// Any other value, as JSON text.
    Json(Vec<u8>),
}

/// Why a call failed: the text the rule fails with, and the kind it is counted under.
pub(crate) use crate::jq::Failure;

/// One function of one loaded module: what a call in a rule's SQL is resolved to.
#[derive(Debug, Clone)]
pub(crate) struct ModuleCall {
    pub module: Arc<Module>,
    pub function: ModuleFunction,
}

#[cfg(not(feature = "jq"))]
const NO_RUNTIME: &str = "this mqttd was built without the WebAssembly runtime (the `jq` cargo \
     feature is off), so it cannot load a module";

/// Whether `name` can be a module's or a function's name: a lower-case letter, then up
/// to 63 lower-case letters, digits or `_`. Such a name is one rule SQL can call.
fn valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && name.len() <= 64
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// Every function name of EMQX 6.3.1's rule engine that is not a built-in function
/// here: the exports of `emqx_rule_funcs` this engine refuses, and the names it
/// resolves without exporting them (`'$handle_undefined_function'/2`: the schema
/// registry's and the Sparkplug functions). With the built-in functions these are the
/// names a module may not take, so that no module changes what an EMQX rule means.
const EMQX_ONLY: &[&str] = &[
    // Exported by earlier releases, and still in rules written for them.
    "byteszie",
    "str_utf16_le_decode",
    "maptab_lookup",
    "mongo_date",
    "schema_check",
    "schema_decode",
    "schema_encode",
    "sparkplug_decode",
    "sparkplug_encode",
    "term_decode",
    "term_encode",
];

/// The words of rule SQL: a function of that name could not be called.
const KEYWORDS: &[&str] = &[
    "select", "from", "where", "as", "and", "or", "not", "in", "case", "when", "then", "else",
    "end", "foreach", "do", "incase",
];

/// Why a module may not provide a function called `name`; `None` when it may.
#[must_use]
pub fn reserved(name: &str) -> Option<&'static str> {
    if crate::funcs::lookup(name).is_some() {
        Some("a built-in function has that name")
    } else if EMQX_ONLY.contains(&name) {
        Some("EMQX has a function of that name")
    } else if KEYWORDS.contains(&name) {
        Some("it is a word of rule SQL")
    } else {
        None
    }
}

/// The names of [`reserved`] that are EMQX's and not built in here.
#[must_use]
pub fn emqx_only_function_names() -> &'static [&'static str] {
    EMQX_ONLY
}

/// The file a table's `wasm` names, under `dir` — or why it may not be read.
fn resolve(dir: &Path, wasm: &str) -> Result<PathBuf, String> {
    let relative = Path::new(wasm);
    let plain = !wasm.is_empty()
        && relative
            .components()
            .all(|c| matches!(c, Component::Normal(_)));
    if !plain {
        return Err(format!(
            "wasm = {wasm:?}: a module is named by a relative path under the rules file's \
             directory, without `..`"
        ));
    }
    if relative.extension().and_then(|e| e.to_str()) != Some("wasm") {
        return Err(format!(
            "wasm = {wasm:?}: a module file's name ends in .wasm"
        ));
    }
    let base = std::fs::canonicalize(dir)
        .map_err(|e| format!("the rules file's directory {}: {e}", dir.display()))?;
    let file = std::fs::canonicalize(base.join(relative)).map_err(|e| {
        format!(
            "wasm = {wasm:?}: {} ({e})",
            if e.kind() == std::io::ErrorKind::NotFound {
                format!("no such file under {}", dir.display())
            } else {
                format!("cannot be read under {}", dir.display())
            }
        )
    })?;
    if !file.starts_with(&base) {
        return Err(format!(
            "wasm = {wasm:?}: a symlink leads out of the rules file's directory {}; a module \
             is only read from under it",
            dir.display()
        ));
    }
    Ok(file)
}

/// The bytes of the module file at `path`, which must be a regular file of at most
/// [`MAX_MODULE_BYTES`].
fn read(path: &Path, wasm: &str) -> Result<Vec<u8>, String> {
    use std::io::Read as _;
    let unreadable = |e: std::io::Error| format!("wasm = {wasm:?}: {e}");
    let meta = std::fs::metadata(path).map_err(unreadable)?;
    if !meta.is_file() {
        return Err(format!("wasm = {wasm:?}: not a regular file"));
    }
    if meta.len() > MAX_MODULE_BYTES {
        return Err(format!(
            "wasm = {wasm:?}: {} bytes is more than the {MAX_MODULE_BYTES} a module file may \
             have",
            meta.len()
        ));
    }
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(unreadable)?
        .take(MAX_MODULE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(unreadable)?;
    if bytes.len() as u64 > MAX_MODULE_BYTES {
        return Err(format!(
            "wasm = {wasm:?}: more than the {MAX_MODULE_BYTES} bytes a module file may have"
        ));
    }
    Ok(bytes)
}

/// What `mqttd_describe` answers.
#[cfg(feature = "jq")]
#[derive(Deserialize)]
struct Description {
    #[serde(default)]
    module: String,
    #[serde(default)]
    version: String,
    functions: Vec<DescribedFunction>,
}

#[cfg(feature = "jq")]
#[derive(Deserialize)]
struct DescribedFunction {
    name: String,
    min_args: usize,
    max_args: usize,
}

/// A table's settings checked and resolved, without touching its file.
fn declare(name: &str, table: &ModuleSchema, host: &ModuleHost) -> Result<Declaration, String> {
    if !valid_name(name) {
        return Err(
            "a module's name is a lower-case letter followed by up to 63 lower-case letters, \
             digits or `_`"
                .to_string(),
        );
    }
    if name == "jq" {
        return Err("`jq` is the built-in module's name".to_string());
    }
    let sha256 = table.sha256.to_ascii_lowercase();
    if sha256.len() != 64 || !sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("sha256 is the file's SHA-256 as 64 hex digits".to_string());
    }
    if table.functions.is_empty() {
        return Err(
            "functions names no function: list the ones rules may call, among those the \
             module describes"
                .to_string(),
        );
    }
    if table.functions.len() > MAX_FUNCTIONS_PER_MODULE {
        return Err(format!(
            "{} functions is more than the {MAX_FUNCTIONS_PER_MODULE} one module may provide",
            table.functions.len()
        ));
    }
    let mut functions = table.functions.clone();
    functions.sort();
    if let Some(twice) = functions
        .iter()
        .zip(functions.iter().skip(1))
        .find_map(|(a, b)| (a == b).then_some(a))
    {
        return Err(format!("functions lists `{twice}` twice"));
    }
    for function in &functions {
        if !valid_name(function) {
            return Err(format!(
                "function name {function:?}: a lower-case letter followed by up to 63 \
                 lower-case letters, digits or `_`"
            ));
        }
        if let Some(why) = reserved(function) {
            return Err(format!(
                "a module cannot provide `{function}`: {why}, and a module must not change \
                 what a rule written for EMQX means"
            ));
        }
    }
    let timeout_ms = table.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS);
    if timeout_ms == 0 || timeout_ms > MAX_TIMEOUT_MS {
        return Err(format!(
            "timeout_ms = {timeout_ms}: from 1 to {MAX_TIMEOUT_MS}"
        ));
    }
    let memory_mib = table.memory_mib.unwrap_or(DEFAULT_MEMORY_MIB);
    if memory_mib == 0 || memory_mib > MAX_MEMORY_MIB {
        return Err(format!(
            "memory_mib = {memory_mib}: from 1 to {MAX_MEMORY_MIB}"
        ));
    }
    let mut environment = Vec::new();
    for variable in &table.env {
        let Some((_, value)) = host
            .settings
            .environment
            .iter()
            .find(|(allowed, _)| allowed == variable)
        else {
            return Err(format!(
                "env names {variable:?}, which the broker's rules.functions_env \
                 (MQTTD_RULES_FUNCTIONS_ENV) does not list: a module sees only variables the \
                 broker's own configuration allows"
            ));
        };
        // A listed variable that is not set is not given, as for `jq`.
        if let Some(value) = value {
            if !environment.iter().any(|(n, _)| n == variable) {
                environment.push((variable.clone(), value.clone()));
            }
        }
    }
    Ok(Declaration {
        name: name.to_string(),
        wasm: table.wasm.clone(),
        sha256,
        functions,
        timeout_ms,
        memory_mib,
        clock: table.clock,
        environment,
        fresh_instance: table.fresh_instance,
    })
}

/// The bytes of the file a declaration names, which must have the sha256 it pins.
fn read_pinned(declaration: &Declaration, dir: &Path) -> Result<Vec<u8>, String> {
    let path = resolve(dir, &declaration.wasm)?;
    let wasm = read(&path, &declaration.wasm)?;
    let found =
        mqtt_core::hex_lower(aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &wasm).as_ref());
    if found != declaration.sha256 {
        // Not the file's own hash: whoever declares a module has to know its content.
        return Err(format!(
            "wasm = {:?}: the file does not have the sha256 the table pins ({}); `sha256sum` \
             prints the file's",
            declaration.wasm, declaration.sha256
        ));
    }
    Ok(wasm)
}

/// Loads the module a declaration names: reads the file, compares its hash with the
/// pin, compiles it and checks what it describes.
#[cfg_attr(not(feature = "jq"), allow(clippy::needless_pass_by_value))]
fn load(declaration: Declaration, dir: &Path, host: &ModuleHost) -> Result<Module, String> {
    let wasm = read_pinned(&declaration, dir)?;
    #[cfg(feature = "jq")]
    {
        use mqtt_wasm_sandbox::{Grants, Limits, LoadError, ModulePool, Sandbox};
        let limits = Limits {
            memory_bytes: usize::try_from(declaration.memory_mib << 20).unwrap_or(usize::MAX),
            max_output_bytes: u32::try_from(crate::funcs::MAX_BUILT_BYTES).unwrap_or(u32::MAX),
            ..Limits::default()
        };
        let grants = Grants {
            clock: declaration.clock,
            environment: declaration
                .environment
                .iter()
                .map(|(name, value)| format!("{name}={value}").into_bytes())
                .collect(),
        };
        let mut sandbox = Sandbox::load(&wasm, limits, grants).map_err(|e| match e {
            // ADR 0086 D13: the way out, where the operator is looking.
            LoadError::Engine(_) => format!(
                "{e}. Compiled code needs memory the process may map executable: under \
                 systemd set `MemoryDenyWriteExecute=false` for the service (`systemctl edit \
                 mqttd`), or lift the seccomp or SELinux rule that forbids it; see RULES.md"
            ),
            e => e.to_string(),
        })?;
        if let Some(hand_off) = &host.settings.hand_off {
            sandbox = sandbox.with_hand_off(hand_off.clone());
        }
        let described: Description =
            serde_json::from_slice(sandbox.description()).map_err(|e| {
                format!(
                    "the module's description (mqttd_describe) is not the JSON expected \
                     ({{\"module\", \"version\", \"functions\": [{{\"name\", \"min_args\", \
                     \"max_args\"}}]}}): {e}"
                )
            })?;
        let mut functions = Vec::with_capacity(declaration.functions.len());
        for wanted in &declaration.functions {
            let Some(function) = described.functions.iter().find(|f| f.name == *wanted) else {
                let mut provided: Vec<&str> = described
                    .functions
                    .iter()
                    .map(|f| f.name.as_str())
                    .collect();
                provided.sort_unstable();
                return Err(format!(
                    "the module does not provide `{wanted}` (it describes: {})",
                    if provided.is_empty() {
                        "no function".to_string()
                    } else {
                        provided.join(", ")
                    }
                ));
            };
            if function.min_args > function.max_args {
                return Err(format!(
                    "the module describes `{wanted}` as taking {} to {} arguments",
                    function.min_args, function.max_args
                ));
            }
            functions.push(ModuleFunction {
                name: Arc::from(wanted.as_str()),
                min_args: function.min_args,
                max_args: function.max_args,
            });
        }
        // The count of instances given up goes on from where the module now loaded
        // under this name is.
        let replaced_before = host
            .loaded()
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|m| *m.name == *declaration.name)
            .map(|m| m.stats().replaced)
            .max()
            .unwrap_or(0);
        Ok(Module {
            name: Arc::from(declaration.name.as_str()),
            described: (described.module, described.version),
            functions,
            replaced_before,
            pool: ModulePool::new(
                sandbox,
                host.settings.instances.max(1),
                declaration.fresh_instance,
            ),
            declaration,
        })
    }
    #[cfg(not(feature = "jq"))]
    {
        let _ = (wasm, host);
        Err(NO_RUNTIME.to_string())
    }
}

/// Loads every `[functions.<name>]` table of a rules file, in name order: the modules
/// its rules may call. `Err` is the first table that cannot be loaded, as (name, why).
pub(crate) fn load_all(
    tables: &std::collections::BTreeMap<String, ModuleSchema>,
    host: Option<&ModuleHost>,
    dir: Option<&Path>,
) -> Result<Vec<Arc<Module>>, (String, String)> {
    let Some((first, _)) = tables.iter().next() else {
        return Ok(Vec::new());
    };
    let (Some(host), Some(dir)) = (host, dir) else {
        return Err((
            first.clone(),
            "modules are read from under the directory of the broker's rules file \
             (rules.file), and this text is loaded without one"
                .to_string(),
        ));
    };
    if tables.len() > MAX_MODULES {
        return Err((
            first.clone(),
            format!(
                "{} modules is more than the {MAX_MODULES} a rules file may declare",
                tables.len()
            ),
        ));
    }
    let mut modules: Vec<Arc<Module>> = Vec::with_capacity(tables.len());
    for (name, table) in tables {
        let fail = |why: String| (name.clone(), why);
        let declaration = declare(name, table, host).map_err(fail)?;
        for function in &declaration.functions {
            if let Some(other) = modules
                .iter()
                .find(|m| m.functions.iter().any(|f| &*f.name == function.as_str()))
            {
                return Err(fail(format!(
                    "`{function}` is already provided by [functions.{}]",
                    other.name
                )));
            }
        }
        // The file is read and compared with its pin on every load, also when the
        // module it names is already loaded: a file that changed under an unchanged
        // rules file fails the load.
        let running = host
            .loaded()
            .iter()
            .filter_map(Weak::upgrade)
            .find(|m| m.declaration == declaration);
        let module = if let Some(module) = running {
            read_pinned(&declaration, dir).map_err(fail)?;
            module
        } else {
            let module = Arc::new(load(declaration, dir, host).map_err(fail)?);
            let mut loaded = host.loaded();
            loaded.retain(|m| m.strong_count() > 0);
            loaded.push(Arc::downgrade(&module));
            module
        };
        modules.push(module);
    }
    Ok(modules)
}
