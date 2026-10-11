//! State for rules ([ADR 0085](../../../docs/adr/0085-rule-state-store.md)): what a
//! rule remembers between messages.
//!
//! This crate still holds no broker state. It defines the [`StateStore`] trait, the
//! SQL functions that call it, and how a rules file declares namespaces; whoever loads
//! the rules supplies the store, in [`Functions`](crate::Functions), as `jq` and the
//! modules are supplied (ADR 0086). A state call is resolved to its namespace when the
//! file is loaded, so an evaluation does no lookup by name, and a file that calls a
//! state function where there is no store does not load.
//!
//! Three families of functions, which do not share keys:
//!
//! * **EMQX's** `kv_store_put/2`, `kv_store_get/1,2` and `kv_store_del/1`, with what
//!   `emqx_rule_funcs` does (probed on EMQX 6.3.1): one table per node, shared by
//!   every rule, a key and a value of any type, keys compared exactly (`=:=`: `1`,
//!   `1.0` and `'1'` are three keys, `0.0` and `-0.0` two), `undefined` storable,
//!   `put` and `del` always `true`. They live in the reserved namespace
//!   [`KV_STORE_NAMESPACE`].
//! * **EMQX's** `proc_dict_put/2`, `proc_dict_get/1` and `proc_dict_del/1`: the
//!   dictionary of whoever evaluates the rule, which in the broker is the client's
//!   connection task ([`Dictionary`]). In no namespace and in no store: the store
//!   only counts their bytes against its ceiling.
//! * **mqttd's** `kv_*` (the names are in [`FUNCS`], and nowhere else), on the
//!   namespace the rule names with its `state` key, or one of its own.
//!
//! A state call takes effect when evaluation reaches it, in EMQX's order (every
//! `SELECT` field, then the `WHERE`), and nothing undoes it: it is an effect of the
//! expression it stands in, as EMQX's `kv_store_put` is an `ets:insert` there.
//!
//! What a message can make the store hold or do is bounded here and in the store: a
//! key of [`MAX_KEY_BYTES`], a value of [`MAX_VALUE_BYTES`], a namespace's `max_keys`
//! and `max_bytes`, the store's ceiling, and [`MAX_STATE_CALLS_PER_TRIGGER`] calls per
//! original message. A refused call fails the rule; it is never "no result".

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Deserialize;

use crate::funcs::{FnCtx, Func, MAX_BUILT_BYTES};
use crate::value::Value;
use crate::EvalError;

mod dict;

pub use dict::{Dictionary, DICTIONARY_FULL, MAX_DICT_BYTES, MAX_DICT_KEYS};

/// EMQX's table (`kv_store_*`): a reserved namespace name.
pub const KV_STORE_NAMESPACE: &str = "kv_store";

/// The `[state.default]` table: the settings a rule's own namespace gets. Reserved.
pub const DEFAULT_NAMESPACE: &str = "default";

/// The most namespaces a rules file may declare (beside `default` and `kv_store`).
pub const MAX_NAMESPACES: usize = 64;

/// The most state calls, read or write, one original message may make across every
/// rule it matches and every message its rules republish. The next one fails its rule.
pub const MAX_STATE_CALLS_PER_TRIGGER: usize = 1024;

/// The longest key, in bytes (for `kv_store_*`: as the rule engine writes the key).
pub const MAX_KEY_BYTES: usize = 256;

/// The largest value, in bytes as the rule engine writes it ([`written_len`]).
pub const MAX_VALUE_BYTES: usize = 4096;

/// The most elements a set (`kv_add`) holds.
pub const MAX_SET_ELEMENTS: usize = 1024;

/// The largest element of a set, in bytes as written.
pub const MAX_ELEMENT_BYTES: usize = 256;

/// A namespace's `max_keys` when its table does not say.
pub const DEFAULT_MAX_KEYS: usize = 100_000;

/// The most `max_keys` may be.
pub const MAX_MAX_KEYS: usize = 1_000_000;

/// A namespace's `max_bytes` when its table does not say (or the store's ceiling, when
/// that is lower).
pub const DEFAULT_MAX_BYTES: u64 = 64 << 20;

/// A namespace's `default_ttl` when its table does not say: 24 hours.
pub const DEFAULT_TTL_SECS: u64 = 24 * 60 * 60;

/// A namespace's `max_ttl` when its table does not say, and the most it may be: 30 days.
pub const MAX_TTL_SECS: u64 = 30 * 24 * 60 * 60;

/// A namespace's tombstone window when its table does not say: one hour. For that
/// long a deleted key of an `eventual` namespace is remembered as deleted, so that a
/// copy from before the delete cannot bring it back.
pub const DEFAULT_TOMBSTONE_WINDOW_SECS: u64 = 60 * 60;

/// Where a namespace's keys live and how they agree across nodes (D5). `strong` is
/// phase 2, and a file that names it does not load.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Consistency {
    /// This node only.
    Local,
    /// Every member of the cluster holds the namespace: a write is applied on this
    /// node and sent to the others in the background, and their copies converge by
    /// merge (D4, D15).
    Eventual,
}

/// What a namespace does with a new key when it is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Eviction {
    /// The call fails its rule (`namespace_full`, `store_full`).
    Fail,
    /// The least recently used key makes room.
    Lru,
}

/// One namespace's declaration, as the rules file gives it (defaults filled in).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceSettings {
    /// The mode.
    pub consistency: Consistency,
    /// The most keys it holds.
    pub max_keys: usize,
    /// The most bytes its keys and values take, as the store accounts them.
    pub max_bytes: u64,
    /// The expiry a new key gets from a write that names none, in seconds. `None`: no
    /// expiry (EMQX's table has none).
    pub default_ttl_secs: Option<u64>,
    /// The longest TTL a call may name, in seconds.
    pub max_ttl_secs: u64,
    /// What a full namespace does with a new key.
    pub eviction: Eviction,
    /// How long an `eventual` namespace remembers a deleted key as deleted, in
    /// seconds. Nothing in a `local` one.
    pub tombstone_window_secs: u64,
}

impl NamespaceSettings {
    /// The settings of a namespace whose table says nothing, under a store whose
    /// ceiling is `store_max_bytes`.
    #[must_use]
    pub fn defaults(store_max_bytes: u64) -> Self {
        Self {
            consistency: Consistency::Local,
            max_keys: DEFAULT_MAX_KEYS,
            max_bytes: DEFAULT_MAX_BYTES.min(store_max_bytes),
            default_ttl_secs: Some(DEFAULT_TTL_SECS),
            max_ttl_secs: MAX_TTL_SECS,
            eviction: Eviction::Fail,
            tombstone_window_secs: DEFAULT_TOMBSTONE_WINDOW_SECS,
        }
    }

    /// The settings of EMQX's table: a namespace like any other, without expiry.
    #[must_use]
    pub fn kv_store(store_max_bytes: u64) -> Self {
        Self {
            default_ttl_secs: None,
            ..Self::defaults(store_max_bytes)
        }
    }
}

/// A value to store, with its size as the rule engine writes it ([`written_len`]),
/// worked out once.
#[derive(Debug, Clone, Copy)]
pub struct Stored<'a> {
    /// The value.
    pub value: &'a Value,
    /// Its size in bytes.
    pub bytes: usize,
}

/// One state call, with its arguments checked: the key is 1 to [`MAX_KEY_BYTES`]
/// bytes (or a `kv_store_*` key's encoding, [`term_key`]), a value fits
/// [`MAX_VALUE_BYTES`], a TTL is whole seconds within the namespace's `max_ttl`.
///
/// A write with a TTL sets the key's expiry; one without gives a new key the
/// namespace's `default_ttl` and leaves an existing key's expiry as it is.
#[derive(Debug, Clone, Copy)]
pub enum StateOp<'a> {
    /// `kv_get`, `kv_store_get`: the value. A counter reads as its count, a set as its
    /// elements in term order.
    Get {
        /// The key.
        key: &'a [u8],
    },
    /// `kv_set`, `kv_store_put`: store the value (type `lww`).
    Set {
        /// The key.
        key: &'a [u8],
        /// What to store.
        value: Stored<'a>,
        /// The expiry to set, in seconds.
        ttl: Option<u64>,
    },
    /// `kv_set_once`: store the value unless one is held (type `first`). Replies with
    /// the value now held.
    SetOnce {
        /// The key.
        key: &'a [u8],
        /// What to store.
        value: Stored<'a>,
        /// The expiry to set, in seconds.
        ttl: Option<u64>,
    },
    /// `kv_incr`: add `by` to the count (type `sum`). Replies with the count after.
    Incr {
        /// The key.
        key: &'a [u8],
        /// What to add; may be negative.
        by: i64,
        /// The expiry to set, in seconds.
        ttl: Option<u64>,
    },
    /// `kv_max` (`largest`) and `kv_min`: keep the larger or smaller of the number
    /// held and `value` (types `max`, `min`). Replies with the number now held.
    Extreme {
        /// The key.
        key: &'a [u8],
        /// The number offered.
        value: Stored<'a>,
        /// `true`: keep the larger (`kv_max`). `false`: the smaller (`kv_min`).
        largest: bool,
        /// The expiry to set, in seconds.
        ttl: Option<u64>,
    },
    /// `kv_add`: add an element to the set (type `union`). Replies with the number of
    /// elements after.
    Add {
        /// The key.
        key: &'a [u8],
        /// The element, and its identity ([`term_key`]): two elements are one when
        /// they are exactly equal.
        element: Stored<'a>,
        /// The element's [`term_key`].
        identity: &'a [u8],
        /// The expiry to set, in seconds.
        ttl: Option<u64>,
    },
    /// `kv_del`, `kv_store_del`: delete the key, whatever its type. Replies with
    /// whether a value was there.
    Del {
        /// The key.
        key: &'a [u8],
    },
    /// `kv_ttl`: whole seconds until the key expires (rounded up, so a key that is
    /// still held never reads `0`); nothing for no key or no expiry.
    Ttl {
        /// The key.
        key: &'a [u8],
    },
    /// `kv_held`: while `holds`, the whole seconds since it first did (recording now
    /// when the key is new; type `first`); otherwise delete the key and reply nothing.
    Held {
        /// The key.
        key: &'a [u8],
        /// Whether the condition is `true` now.
        holds: bool,
        /// The expiry to set, in seconds.
        ttl: Option<u64>,
    },
    /// `kv_changed`: whether `value` differs exactly from the one held (`true` when
    /// none is), storing it (type `lww`).
    Changed {
        /// The key.
        key: &'a [u8],
        /// What to compare and store.
        value: Stored<'a>,
        /// The expiry to set, in seconds.
        ttl: Option<u64>,
    },
    /// `kv_hll_add`: add a value to the sketch of distinct values (type `hll`). Replies
    /// with the estimate of how many distinct values it has seen.
    HllAdd {
        /// The key.
        key: &'a [u8],
        /// The value's hash ([`stable_hash`](crate::crdt::stable_hash) of its
        /// [`term_key`]): two values are one when they are exactly equal.
        hash: u64,
        /// The expiry to set, in seconds.
        ttl: Option<u64>,
    },
    /// `kv_hll_count`: the sketch's estimate; `0` for no key.
    HllCount {
        /// The key.
        key: &'a [u8],
    },
}

/// What a state call answers.
#[derive(Debug, Clone, Default)]
pub struct StateReply {
    /// The answer; `None` where there is none (no key, no expiry, a `Set`).
    pub value: Option<Value>,
    /// For an answer read out of the store (`Get`, `SetOnce`): its size in bytes, which
    /// the caller charges to the message's build budget.
    pub read_bytes: usize,
}

/// A refused state call: it fails its rule.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{reason}: {message}")]
pub struct StateError {
    /// The kind, which the store counts refusals by: [`NAMESPACE_FULL`],
    /// [`STORE_FULL`], [`BROWNOUT`], [`WRONG_TYPE`], or another of its own.
    pub reason: &'static str,
    /// What happened, for the rule's error.
    pub message: String,
}

/// A new key in a namespace at its `max_keys` or `max_bytes`.
pub const NAMESPACE_FULL: &str = "namespace_full";
/// A new key in a store at its ceiling (`rules.state_max_bytes`).
pub const STORE_FULL: &str = "store_full";
/// A new key while the broker is over its memory watermark.
pub const BROWNOUT: &str = "brownout";
/// An operation of one type on a key another type's operation created.
pub const WRONG_TYPE: &str = "wrong_type";

/// One namespace of a store, as a loaded rule calls it.
pub trait StateNamespace: Send + Sync + std::fmt::Debug {
    /// One call. Never waits: one operation on an in-memory map (D14).
    fn call(&self, op: &StateOp<'_>) -> Result<StateReply, StateError>;
}

/// A keyed store of small values in namespaces, which the broker implements and hands
/// over in [`Functions`](crate::Functions).
pub trait StateStore: Send + Sync + std::fmt::Debug {
    /// The store's ceiling in bytes (`rules.state_max_bytes`): the most a namespace's
    /// `max_bytes` may be.
    fn max_bytes(&self) -> u64;

    /// The namespace `name` with `settings`, for a rules file being loaded. A namespace
    /// that rules already loaded hold keeps its keys, and the settings are this
    /// caller's: a reload's changed bounds apply to the calls of the rules it loaded.
    /// A namespace is dropped, with its keys, when the last rules that hold it are.
    /// `Err`: why the file cannot be loaded.
    fn open(
        &self,
        name: &str,
        settings: &NamespaceSettings,
    ) -> Result<Arc<dyn StateNamespace>, String>;

    /// Count `bytes` more against the ceiling for what a [`Dictionary`] holds
    /// (`proc_dict_*`), outside any namespace. `Err`: there is no room ([`STORE_FULL`],
    /// [`BROWNOUT`]) and nothing was counted; the call fails its rule. A store that
    /// keeps no such count accepts everything.
    fn dictionary_grow(&self, _bytes: u64) -> Result<(), StateError> {
        Ok(())
    }

    /// Give back `bytes` that [`dictionary_grow`](Self::dictionary_grow) counted.
    fn dictionary_shrink(&self, _bytes: u64) {}
}

/// Whether the rules being loaded may keep state.
#[derive(Debug, Clone, Default)]
pub enum StateSwitch {
    /// Loaded without a store (the default): a file that uses state does not load.
    #[default]
    None,
    /// The broker's store is switched off (`rules.state_max_bytes = 0`): a file that
    /// uses state does not load, and the error names the key.
    Off,
    /// The store.
    On(Arc<dyn StateStore>),
}

/// What the load error says while the store is switched off.
const SWITCHED_OFF: &str = "rule state is switched off: `rules.state_max_bytes` is 0 (set it \
     above 0 for rules to keep state; see RULES.md)";

/// What the load error says where the rules are loaded without a store.
const NO_STORE: &str = "rule state is not available here: the rules are loaded without a state \
     store";

impl StateSwitch {
    fn store(&self) -> Result<&Arc<dyn StateStore>, String> {
        match self {
            Self::None => Err(NO_STORE.to_string()),
            Self::Off => Err(SWITCHED_OFF.to_string()),
            Self::On(store) => Ok(store),
        }
    }
}

/// A namespace as a loaded rule holds it: its name, its declaration and the store's
/// handle. One per namespace per rules file, shared by every call that uses it.
#[derive(Debug)]
pub struct StateBinding {
    /// The namespace: a declared name, `kv_store`, or `rule:<id>` for a rule's own.
    pub name: Arc<str>,
    /// Its settings, as this rules file declares them.
    pub settings: NamespaceSettings,
    namespace: Arc<dyn StateNamespace>,
}

/// One `[state.<name>]` table.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct StateSchema {
    consistency: Option<String>,
    max_keys: Option<i64>,
    max_bytes: Option<toml::Value>,
    default_ttl: Option<toml::Value>,
    max_ttl: Option<toml::Value>,
    eviction: Option<String>,
    tombstone_window: Option<toml::Value>,
}

/// Whether `name` can be a namespace's: a lower-case letter, then up to 63 lower-case
/// letters, digits or `_`.
fn valid_namespace(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && name.len() <= 64
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// A size: a whole number of bytes, or text such as `"64MiB"` (`B`, `KiB`, `MiB`,
/// `GiB`).
fn size(v: &toml::Value) -> Result<u64, String> {
    let bad = || format!("{v} is not a size (a number of bytes, or text such as \"64MiB\")");
    match v {
        toml::Value::Integer(n) => u64::try_from(*n).map_err(|_| bad()),
        toml::Value::String(s) => {
            let s = s.trim();
            let digits = s.len() - s.trim_start_matches(|c: char| c.is_ascii_digit()).len();
            let n: u64 = s[..digits].parse().map_err(|_| bad())?;
            let unit: u64 = match s[digits..].trim() {
                "" | "B" => 1,
                "KiB" => 1 << 10,
                "MiB" => 1 << 20,
                "GiB" => 1 << 30,
                _ => return Err(bad()),
            };
            n.checked_mul(unit).ok_or_else(bad)
        }
        _ => Err(bad()),
    }
}

/// Text such as `10m` as whole seconds (`s`, `m`, `h`, `d`).
fn duration_text(s: &str) -> Option<u64> {
    let digits = s.len() - s.trim_start_matches(|c: char| c.is_ascii_digit()).len();
    let n: u64 = s[..digits].parse().ok()?;
    let unit = match &s[digits..] {
        "s" => 1,
        "m" => 60,
        "h" => 60 * 60,
        "d" => 24 * 60 * 60,
        _ => return None,
    };
    n.checked_mul(unit)
}

/// A duration in a `[state.<name>]` table: whole seconds, or text such as `"1h"`.
fn duration(v: &toml::Value) -> Result<u64, String> {
    let bad =
        || format!("{v} is not a duration (a number of seconds, or text such as \"10m\", \"1h\")");
    match v {
        toml::Value::Integer(n) => u64::try_from(*n).map_err(|_| bad()),
        toml::Value::String(s) => duration_text(s.trim()).ok_or_else(bad),
        _ => Err(bad()),
    }
}

impl StateSchema {
    /// The table as settings, over `base` (what it leaves out), under a store whose
    /// ceiling is `ceiling` bytes.
    #[allow(clippy::too_many_lines)] // one check per key of the table
    fn settings(
        &self,
        name: &str,
        base: NamespaceSettings,
        ceiling: u64,
    ) -> Result<NamespaceSettings, String> {
        let mut s = base;
        match self.consistency.as_deref() {
            None | Some("local") => {}
            Some("eventual") if name == KV_STORE_NAMESPACE => {
                return Err(
                    "consistency = \"eventual\": EMQX's kv_store table is one per node; it is \
                     always \"local\""
                        .to_string(),
                );
            }
            Some("eventual") => s.consistency = Consistency::Eventual,
            Some("strong") => {
                return Err(
                    "consistency = \"strong\" is not available: it is a later phase (ADR 0085 \
                     D5); use \"local\" or \"eventual\""
                        .to_string(),
                );
            }
            Some(other) => {
                return Err(format!(
                    "consistency = \"{other}\": expected \"local\" or \"eventual\" \
                     (\"strong\" is not available yet)"
                ));
            }
        }
        if let Some(n) = self.max_keys {
            s.max_keys = usize::try_from(n)
                .ok()
                .filter(|n| (1..=MAX_MAX_KEYS).contains(n))
                .ok_or_else(|| format!("max_keys = {n}: expected 1 to {MAX_MAX_KEYS}"))?;
        }
        if let Some(v) = &self.max_bytes {
            let n = size(v).map_err(|e| format!("max_bytes: {e}"))?;
            if n == 0 || n > ceiling {
                return Err(format!(
                    "max_bytes = {v}: expected 1 byte to the store's ceiling, {ceiling} bytes \
                     (`rules.state_max_bytes`)"
                ));
            }
            s.max_bytes = n;
        }
        if name == KV_STORE_NAMESPACE && (self.default_ttl.is_some() || self.max_ttl.is_some()) {
            return Err(
                "EMQX's kv_store table has no expiry: default_ttl and max_ttl cannot be set"
                    .to_string(),
            );
        }
        if let Some(v) = &self.max_ttl {
            let n = duration(v).map_err(|e| format!("max_ttl: {e}"))?;
            if !(1..=MAX_TTL_SECS).contains(&n) {
                return Err(format!(
                    "max_ttl = {v}: expected 1 second to 30 days ({MAX_TTL_SECS} seconds)"
                ));
            }
            s.max_ttl_secs = n;
        }
        if let Some(v) = &self.default_ttl {
            let n = duration(v).map_err(|e| format!("default_ttl: {e}"))?;
            s.default_ttl_secs = Some(n);
        }
        if let Some(n) = s.default_ttl_secs {
            // Checked whichever of the two the table set: a `max_ttl` below the
            // default `default_ttl` needs its own `default_ttl`.
            if !(1..=s.max_ttl_secs).contains(&n) {
                return Err(format!(
                    "default_ttl is {n} seconds: expected 1 second to max_ttl ({} seconds)",
                    s.max_ttl_secs
                ));
            }
        }
        match self.eviction.as_deref() {
            None => {}
            Some("fail") => s.eviction = Eviction::Fail,
            Some("lru") => s.eviction = Eviction::Lru,
            Some(other) => {
                return Err(format!(
                    "eviction = \"{other}\": expected \"fail\" or \"lru\""
                ));
            }
        }
        if let Some(v) = &self.tombstone_window {
            let n = duration(v).map_err(|e| format!("tombstone_window: {e}"))?;
            if s.consistency != Consistency::Eventual {
                return Err(
                    "tombstone_window is for an \"eventual\" namespace: a \"local\" one keeps \
                     nothing of a deleted key"
                        .to_string(),
                );
            }
            if !(1..=MAX_TTL_SECS).contains(&n) {
                return Err(format!(
                    "tombstone_window = {v}: expected 1 second to 30 days ({MAX_TTL_SECS} seconds)"
                ));
            }
            s.tombstone_window_secs = n;
        }
        if s.consistency == Consistency::Eventual && s.eviction == Eviction::Lru {
            return Err(
                "eviction = \"lru\" is for a \"local\" namespace: a key evicted on one node of \
                 an \"eventual\" one would come back from the others"
                    .to_string(),
            );
        }
        Ok(s)
    }
}

/// Which table a state function works on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Family {
    /// EMQX's `kv_store_*`: the node's one table.
    Emqx,
    /// mqttd's `kv_*`: the rule's namespace.
    Own,
    /// EMQX's `proc_dict_*`: the evaluating task's [`Dictionary`], in no namespace.
    Dict,
}

/// The state of one rules file while it loads: its declarations, and the namespaces
/// its rules have been resolved to.
#[derive(Debug)]
pub(crate) struct StateLoading {
    switch: StateSwitch,
    /// The `[state.<name>]` tables, as the file wrote them.
    tables: BTreeMap<String, StateSchema>,
    /// The namespaces opened so far, by name.
    opened: BTreeMap<String, Arc<StateBinding>>,
    /// The rule being compiled: its id, and the namespace its `state` key names.
    rule: (Arc<str>, Option<String>),
    /// Whether a statement called a function of each family.
    pub called_emqx: bool,
    pub called_own: bool,
}

impl StateLoading {
    /// For rules loaded with `switch` under the tables `tables`. Nothing is checked or
    /// opened yet: a statement on its own meets its namespace when it calls one.
    pub(crate) fn new(switch: &StateSwitch, tables: BTreeMap<String, StateSchema>) -> Self {
        Self {
            switch: switch.clone(),
            tables,
            opened: BTreeMap::new(),
            rule: (Arc::from("test"), None),
            called_emqx: false,
            called_own: false,
        }
    }

    /// For a whole file: its declarations are checked and the declared namespaces
    /// opened, so that a declared namespace keeps its keys across reloads whether or
    /// not a rule uses it at the moment.
    pub(crate) fn declare(&mut self) -> Result<(), String> {
        if self.tables.is_empty() {
            return Ok(());
        }
        let in_table = |name: &str, e: String| format!("[state.{name}]: {e}");
        let names: Vec<String> = self.tables.keys().cloned().collect();
        for name in &names {
            if !valid_namespace(name) {
                return Err(in_table(
                    name,
                    "a namespace's name is a lower-case letter followed by up to 63 lower-case \
                     letters, digits or `_`"
                        .to_string(),
                ));
            }
        }
        let declared = names
            .iter()
            .filter(|n| *n != DEFAULT_NAMESPACE && *n != KV_STORE_NAMESPACE)
            .count();
        if declared > MAX_NAMESPACES {
            return Err(format!(
                "{declared} [state.<name>] tables is more than the {MAX_NAMESPACES} a file may \
                 declare"
            ));
        }
        // A file that declares a namespace uses state, whether or not a rule calls it.
        let ceiling = self
            .switch
            .store()
            .map_err(|e| in_table(&names[0], e))?
            .max_bytes();
        for name in &names {
            if name == DEFAULT_NAMESPACE {
                self.default_settings(ceiling)
                    .map_err(|e| in_table(name, e))?;
            } else {
                self.open(name)?;
            }
        }
        Ok(())
    }

    /// What follows is the rule `id`, whose `state` key is `named`. `Err`: the key
    /// names no declared namespace.
    pub(crate) fn rule(&mut self, id: &str, named: Option<&str>) -> Result<(), String> {
        if let Some(name) = named {
            if name == DEFAULT_NAMESPACE || name == KV_STORE_NAMESPACE {
                return Err(format!(
                    "state = \"{name}\": `{name}` is reserved ([state.default] holds the \
                     settings of a rule's own namespace, and kv_store is EMQX's table, which \
                     kv_store_put, kv_store_get and kv_store_del always use)"
                ));
            }
            if !self.tables.contains_key(name) {
                return Err(format!(
                    "state = \"{name}\": the file declares no [state.{name}]"
                ));
            }
        }
        self.rule = (Arc::from(id), named.map(str::to_string));
        Ok(())
    }

    /// `[state.default]` as settings: what a rule's own namespace gets.
    fn default_settings(&self, ceiling: u64) -> Result<NamespaceSettings, String> {
        let base = NamespaceSettings::defaults(ceiling);
        match self.tables.get(DEFAULT_NAMESPACE) {
            Some(table) => table.settings(DEFAULT_NAMESPACE, base, ceiling),
            None => Ok(base),
        }
    }

    /// The namespace `name`, opened once per file: a declared one, EMQX's table, or a
    /// rule's own (`rule:<id>`).
    fn open(&mut self, name: &str) -> Result<Arc<StateBinding>, String> {
        if let Some(binding) = self.opened.get(name) {
            return Ok(binding.clone());
        }
        let store = self.switch.store()?.clone();
        let ceiling = store.max_bytes();
        let settings = if name.starts_with("rule:") {
            self.default_settings(ceiling)
                .map_err(|e| format!("[state.default]: {e}"))?
        } else {
            let base = if name == KV_STORE_NAMESPACE {
                NamespaceSettings::kv_store(ceiling)
            } else {
                NamespaceSettings::defaults(ceiling)
            };
            match self.tables.get(name) {
                Some(table) => table
                    .settings(name, base, ceiling)
                    .map_err(|e| format!("[state.{name}]: {e}"))?,
                None => base,
            }
        };
        let namespace = store.open(name, &settings)?;
        let binding = Arc::new(StateBinding {
            name: Arc::from(name),
            settings,
            namespace,
        });
        self.opened.insert(name.to_string(), binding.clone());
        Ok(binding)
    }

    /// The namespace a call of a `family` function in the rule being compiled works
    /// on (`None` for `proc_dict_*`, which work on no namespace), or why the file
    /// cannot be loaded.
    pub(crate) fn binding(&mut self, family: Family) -> Result<Option<Arc<StateBinding>>, String> {
        let name = match family {
            Family::Dict => {
                // A dictionary's bytes count against the store's ceiling, so these
                // too load only where there is a store.
                self.switch.store()?;
                return Ok(None);
            }
            Family::Emqx => {
                self.called_emqx = true;
                KV_STORE_NAMESPACE.to_string()
            }
            Family::Own => {
                self.called_own = true;
                match &self.rule.1 {
                    Some(named) => named.clone(),
                    None => format!("rule:{}", self.rule.0),
                }
            }
        };
        self.open(&name).map(Some)
    }

    /// Every namespace the file holds: the declared ones and the ones its rules use.
    pub(crate) fn into_bindings(self) -> Vec<Arc<StateBinding>> {
        self.opened.into_values().collect()
    }
}

/// The size of `v` as the rule engine writes it (ADR 0086 D3's form): a string or a
/// binary as its bytes, anything else as JSON text. What a value's and a key's bounds
/// are measured in, and what the store accounts.
#[must_use]
pub fn written_len(v: &Value) -> usize {
    match v {
        Value::Str(s) => s.len(),
        Value::Bin(b) => b.len(),
        Value::Bool(true) | Value::Null => 4,
        Value::Bool(false) => 5,
        Value::Int(n) => {
            let digits = n.unsigned_abs().checked_ilog10().map_or(1, |d| d + 1);
            usize::try_from(digits).unwrap_or(20) + usize::from(*n < 0)
        }
        other => other.to_json().map_or(usize::MAX, |text| text.len()),
    }
}

/// `v` as bytes that are equal exactly when two values are exactly equal (Erlang's
/// `=:=`, which is how an `ets` `set` matches a key): an integer is never a float,
/// `0.0` is not `-0.0`, a string and a binary of the same bytes are one, and a map is
/// the same whatever order its keys were written in.
#[must_use]
pub fn term_key(v: &Value) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    write_term(v, &mut out);
    out
}

fn write_len(n: usize, out: &mut Vec<u8>) {
    out.extend_from_slice(&u64::try_from(n).unwrap_or(u64::MAX).to_be_bytes());
}

fn write_term(v: &Value, out: &mut Vec<u8>) {
    match v {
        Value::Undefined => out.push(0),
        Value::Null => out.push(1),
        Value::Bool(false) => out.push(2),
        Value::Bool(true) => out.push(3),
        Value::Int(n) => {
            out.push(4);
            out.extend_from_slice(&n.to_be_bytes());
        }
        // Never an integer that fits 64 bits (`Value::Big`'s invariant), so the two
        // integer forms cannot name one number.
        Value::Big(n) => {
            out.push(5);
            let bytes = n.to_signed_bytes_be();
            write_len(bytes.len(), out);
            out.extend_from_slice(&bytes);
        }
        Value::Float(f) => {
            out.push(6);
            out.extend_from_slice(&f.to_bits().to_be_bytes());
        }
        Value::Str(_) | Value::Bin(_) => {
            let bytes = v.as_bytes().unwrap_or_default();
            out.push(7);
            write_len(bytes.len(), out);
            out.extend_from_slice(bytes);
        }
        Value::Array(items) => {
            out.push(8);
            write_len(items.len(), out);
            for item in items.iter() {
                write_term(item, out);
            }
        }
        Value::Map(map) => {
            out.push(9);
            write_len(map.len(), out);
            let mut entries: Vec<(&Arc<str>, &Value)> = map.iter().collect();
            entries.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            for (key, value) in entries {
                write_len(key.len(), out);
                out.extend_from_slice(key.as_bytes());
                write_term(value, out);
            }
        }
    }
}

/// The value a [`term_key`] was made from, for whoever shows a store's keys (a
/// replayed sequence's, ADR 0085 D11): EMQX's table is keyed by any value. A binary
/// that is text reads back as the string. `None` for bytes [`term_key`] never wrote.
#[must_use]
pub fn key_term(key: &[u8]) -> Option<Value> {
    let mut rest = key;
    let value = read_term(&mut rest, 0)?;
    rest.is_empty().then_some(value)
}

fn take<'a>(rest: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
    let (head, tail) = rest.split_at_checked(n)?;
    *rest = tail;
    Some(head)
}

fn read_len(rest: &mut &[u8]) -> Option<usize> {
    let n = u64::from_be_bytes(take(rest, 8)?.try_into().ok()?);
    // Every element takes at least a byte, so a length past what is left is not one.
    usize::try_from(n).ok().filter(|n| *n <= rest.len())
}

fn read_term(rest: &mut &[u8], depth: usize) -> Option<Value> {
    // Deeper than any value the rule engine builds.
    if depth > 128 {
        return None;
    }
    Some(match take(rest, 1)?[0] {
        0 => Value::Undefined,
        1 => Value::Null,
        2 => Value::Bool(false),
        3 => Value::Bool(true),
        4 => Value::Int(i64::from_be_bytes(take(rest, 8)?.try_into().ok()?)),
        5 => {
            let n = read_len(rest)?;
            Value::Big(Arc::new(num_bigint::BigInt::from_signed_bytes_be(take(
                rest, n,
            )?)))
        }
        6 => Value::Float(f64::from_bits(u64::from_be_bytes(
            take(rest, 8)?.try_into().ok()?,
        ))),
        7 => {
            let n = read_len(rest)?;
            Value::from_bytes(&bytes::Bytes::copy_from_slice(take(rest, n)?))
        }
        8 => {
            let n = read_len(rest)?;
            let mut items = Vec::with_capacity(n);
            for _ in 0..n {
                items.push(read_term(rest, depth + 1)?);
            }
            Value::from(items)
        }
        9 => {
            let n = read_len(rest)?;
            let mut map = crate::value::Map::with_capacity(n);
            for _ in 0..n {
                let len = read_len(rest)?;
                let key = std::str::from_utf8(take(rest, len)?).ok()?;
                map.insert(key, read_term(rest, depth + 1)?);
            }
            Value::from(map)
        }
        _ => return None,
    })
}

fn fail(reason: &str, message: impl std::fmt::Display) -> EvalError {
    EvalError::new(format!("{reason}: {message}"))
}

/// A `kv_*` key: a string of 1 to [`MAX_KEY_BYTES`] bytes.
fn key(v: &Value) -> Result<&[u8], EvalError> {
    match v.as_bytes() {
        Some(bytes) if (1..=MAX_KEY_BYTES).contains(&bytes.len()) => Ok(bytes),
        Some(bytes) => Err(fail(
            "bad_key",
            format!("a key is 1 to {MAX_KEY_BYTES} bytes, got {}", bytes.len()),
        )),
        None => Err(fail(
            "bad_key",
            format!("a key is a string, got a {}", v.type_name()),
        )),
    }
}

/// A value to store: anything but `undefined`, of at most `max` bytes as written.
fn stored<'a>(v: &'a Value, max: usize, what: &str) -> Result<Stored<'a>, EvalError> {
    if v.is_undefined() {
        return Err(fail(
            "bad_value",
            format!("{what} cannot be undefined (kv_del deletes a key)"),
        ));
    }
    sized(v, max, what)
}

fn sized<'a>(v: &'a Value, max: usize, what: &str) -> Result<Stored<'a>, EvalError> {
    let bytes = written_len(v);
    if bytes > max {
        return Err(fail(
            "value_too_large",
            format!("{what} is {bytes} bytes as written; at most {max}"),
        ));
    }
    Ok(Stored { value: v, bytes })
}

/// A TTL argument: a positive integer of seconds or a duration string (`'10m'`), at
/// least one second and at most the namespace's `max_ttl`.
fn ttl(v: Option<&Value>, binding: &StateBinding) -> Result<Option<u64>, EvalError> {
    let Some(v) = v else {
        return Ok(None);
    };
    let max = binding.settings.max_ttl_secs;
    let seconds = match v {
        Value::Int(n) => u64::try_from(*n).ok(),
        Value::Str(_) | Value::Bin(_) => v.as_str().and_then(duration_text),
        // Past 64 bits it is past any `max_ttl`.
        Value::Big(n) if n.sign() == num_bigint::Sign::Plus => Some(u64::MAX),
        _ => None,
    };
    match seconds {
        Some(n) if (1..=max).contains(&n) => Ok(Some(n)),
        Some(n) if n > max => Err(fail(
            "bad_ttl",
            format!("a TTL is at most the namespace's max_ttl, {max} seconds"),
        )),
        _ => Err(fail(
            "bad_ttl",
            "a TTL is a positive integer of seconds or a duration such as '10m', at least one \
             second",
        )),
    }
}

/// One call on the namespace the rule was loaded with: counted against the message's
/// [`MAX_STATE_CALLS_PER_TRIGGER`], and what it read charged to the message's build
/// budget.
fn call(cx: &FnCtx, op: &StateOp<'_>) -> Result<StateReply, EvalError> {
    let binding = binding(cx)?;
    let reply = cx
        .ctx
        .state_call(binding, op)?
        .map_err(|e| EvalError::new(e.to_string()))?;
    charge_read(cx, reply.read_bytes)?;
    Ok(reply)
}

/// Charge `bytes` read out of state to the message's build budget.
fn charge_read(cx: &FnCtx, bytes: usize) -> Result<(), EvalError> {
    if bytes > 0 {
        let total = cx
            .ctx
            .built
            .get()
            .checked_add(bytes)
            .filter(|&total| total <= MAX_BUILT_BYTES)
            .ok_or_else(|| {
                EvalError::new(format!(
                    "the values this message's rules read from state would pass \
                     {MAX_BUILT_BYTES} bytes (the budget is per message, shared with every \
                     function call)"
                ))
            })?;
        cx.ctx.built.set(total);
    }
    Ok(())
}

fn binding<'a>(cx: &'a FnCtx) -> Result<&'a StateBinding, EvalError> {
    // A rules file that calls a state function loads only with a store.
    cx.state.ok_or_else(|| EvalError::new(NO_STORE))
}

/// Every call counts, whatever its arguments turn out to be.
fn begin<'a>(cx: &'a FnCtx) -> Result<&'a StateBinding, EvalError> {
    cx.ctx.charge_state_call()?;
    binding(cx)
}

fn answer(reply: StateReply) -> Value {
    reply.value.unwrap_or_default()
}

fn kv_get(a: &[Value], cx: &FnCtx) -> Result<Value, EvalError> {
    begin(cx)?;
    call(cx, &StateOp::Get { key: key(&a[0])? }).map(answer)
}

fn kv_set(a: &[Value], cx: &FnCtx) -> Result<Value, EvalError> {
    let binding = begin(cx)?;
    let op = StateOp::Set {
        key: key(&a[0])?,
        value: stored(&a[1], MAX_VALUE_BYTES, "a value")?,
        ttl: ttl(a.get(2), binding)?,
    };
    call(cx, &op)?;
    Ok(a[1].clone())
}

fn kv_set_once(a: &[Value], cx: &FnCtx) -> Result<Value, EvalError> {
    let binding = begin(cx)?;
    let op = StateOp::SetOnce {
        key: key(&a[0])?,
        value: stored(&a[1], MAX_VALUE_BYTES, "a value")?,
        ttl: ttl(a.get(2), binding)?,
    };
    call(cx, &op).map(answer)
}

fn kv_incr(a: &[Value], cx: &FnCtx) -> Result<Value, EvalError> {
    let binding = begin(cx)?;
    let by = match a.get(1) {
        None => 1,
        Some(Value::Int(n)) => *n,
        Some(other) => {
            return Err(fail(
                "bad_value",
                format!("kv_incr adds a 64-bit integer, got a {}", other.type_name()),
            ))
        }
    };
    let op = StateOp::Incr {
        key: key(&a[0])?,
        by,
        ttl: ttl(a.get(2), binding)?,
    };
    call(cx, &op).map(answer)
}

fn extreme(a: &[Value], cx: &FnCtx, largest: bool) -> Result<Value, EvalError> {
    let binding = begin(cx)?;
    if !a[1].is_number() {
        return Err(fail(
            "bad_value",
            format!("expected a number, got a {}", a[1].type_name()),
        ));
    }
    let op = StateOp::Extreme {
        key: key(&a[0])?,
        value: stored(&a[1], MAX_VALUE_BYTES, "a value")?,
        largest,
        ttl: ttl(a.get(2), binding)?,
    };
    call(cx, &op).map(answer)
}

fn kv_add(a: &[Value], cx: &FnCtx) -> Result<Value, EvalError> {
    let binding = begin(cx)?;
    let element = stored(&a[1], MAX_ELEMENT_BYTES, "an element")?;
    let identity = term_key(&a[1]);
    let op = StateOp::Add {
        key: key(&a[0])?,
        element,
        identity: &identity,
        ttl: ttl(a.get(2), binding)?,
    };
    call(cx, &op).map(answer)
}

fn kv_del(a: &[Value], cx: &FnCtx) -> Result<Value, EvalError> {
    begin(cx)?;
    call(cx, &StateOp::Del { key: key(&a[0])? }).map(answer)
}

fn kv_ttl(a: &[Value], cx: &FnCtx) -> Result<Value, EvalError> {
    begin(cx)?;
    call(cx, &StateOp::Ttl { key: key(&a[0])? }).map(answer)
}

fn kv_held(a: &[Value], cx: &FnCtx) -> Result<Value, EvalError> {
    let binding = begin(cx)?;
    let op = StateOp::Held {
        key: key(&a[0])?,
        // Anything but `true` ends the run: `false`, `undefined`, a number.
        holds: a[1].is_true(),
        ttl: ttl(a.get(2), binding)?,
    };
    call(cx, &op).map(answer)
}

fn kv_changed(a: &[Value], cx: &FnCtx) -> Result<Value, EvalError> {
    let binding = begin(cx)?;
    let op = StateOp::Changed {
        key: key(&a[0])?,
        value: stored(&a[1], MAX_VALUE_BYTES, "a value")?,
        ttl: ttl(a.get(2), binding)?,
    };
    call(cx, &op).map(answer)
}

/// `kv_hll_add(key, value[, ttl])`: Redis's `PFADD`. The sketch keeps no value, only
/// what the value's hash raises in its registers; the hash is of the value's exact
/// identity, so `1` and `1.0` are two values and a string and a binary of the same
/// bytes are one, as in a set.
fn kv_hll_add(a: &[Value], cx: &FnCtx) -> Result<Value, EvalError> {
    let binding = begin(cx)?;
    stored(&a[1], MAX_VALUE_BYTES, "a value")?;
    let op = StateOp::HllAdd {
        key: key(&a[0])?,
        hash: crate::crdt::stable_hash(&term_key(&a[1])),
        ttl: ttl(a.get(2), binding)?,
    };
    call(cx, &op).map(answer)
}

/// `kv_hll_count(key)`: Redis's `PFCOUNT`.
fn kv_hll_count(a: &[Value], cx: &FnCtx) -> Result<Value, EvalError> {
    begin(cx)?;
    call(cx, &StateOp::HllCount { key: key(&a[0])? }).map(answer)
}

/// A `kv_store_*` key: any value, of at most [`MAX_KEY_BYTES`] bytes as written,
/// matched exactly.
fn store_key(v: &Value) -> Result<Vec<u8>, EvalError> {
    let bytes = written_len(v);
    if bytes > MAX_KEY_BYTES {
        return Err(fail(
            "bad_key",
            format!(
                "a key is {bytes} bytes as written; at most {MAX_KEY_BYTES} (EMQX's table has \
                 no bound; mqttd's has)"
            ),
        ));
    }
    Ok(term_key(v))
}

/// `kv_store_put(Key, Value)`: `ets:insert/2`, which answers `true`.
fn kv_store_put(a: &[Value], cx: &FnCtx) -> Result<Value, EvalError> {
    begin(cx)?;
    let key = store_key(&a[0])?;
    let op = StateOp::Set {
        key: &key,
        // `undefined` is a value like any other here.
        value: sized(&a[1], MAX_VALUE_BYTES, "a value")?,
        ttl: None,
    };
    call(cx, &op)?;
    Ok(Value::Bool(true))
}

/// `kv_store_get(Key)` and `kv_store_get(Key, Default)`: the value, or `undefined` or
/// the default when the table has no such key. A stored `undefined` is a value, not a
/// missing key: the default is not used for it.
fn kv_store_get(a: &[Value], cx: &FnCtx) -> Result<Value, EvalError> {
    begin(cx)?;
    let key = store_key(&a[0])?;
    let reply = call(cx, &StateOp::Get { key: &key })?;
    Ok(match reply.value {
        Some(value) => value,
        None => a.get(1).cloned().unwrap_or_default(),
    })
}

/// `kv_store_del(Key)`: `ets:delete/2`, which answers `true` whether or not the key
/// was there.
fn kv_store_del(a: &[Value], cx: &FnCtx) -> Result<Value, EvalError> {
    begin(cx)?;
    let key = store_key(&a[0])?;
    call(cx, &StateOp::Del { key: &key })?;
    Ok(Value::Bool(true))
}

const fn func(
    name: &'static str,
    min: usize,
    max: usize,
    f: fn(&[Value], &FnCtx) -> Result<Value, EvalError>,
) -> Func {
    Func {
        name,
        min,
        max,
        f,
        regex_arg: None,
    }
}

/// The prefixes of EMQX's own state functions: what tells the families apart.
const EMQX_PREFIX: &str = "kv_store_";
const DICT_PREFIX: &str = "proc_dict_";

/// The state functions. **The one place their names are written**: mqttd's own are
/// `kv_*` beside EMQX's `kv_store_*` (ADR 0085, decided by default), and a rename is a
/// change to this table and to the documentation.
pub(crate) static FUNCS: &[Func] = &[
    // -- EMQX's (`emqx_rule_funcs`), with EMQX's arities
    func("kv_store_put", 2, 2, kv_store_put),
    func("kv_store_get", 1, 2, kv_store_get),
    func("kv_store_del", 1, 1, kv_store_del),
    func("proc_dict_put", 2, 2, dict::proc_dict_put),
    func("proc_dict_get", 1, 1, dict::proc_dict_get),
    func("proc_dict_del", 1, 1, dict::proc_dict_del),
    // -- mqttd's
    func("kv_get", 1, 1, kv_get),
    func("kv_set", 2, 3, kv_set),
    func("kv_set_once", 2, 3, kv_set_once),
    // A fourth argument, the idempotency key, is ADR 0085 D8.
    func("kv_incr", 1, 3, kv_incr),
    func("kv_max", 2, 3, |a, cx| extreme(a, cx, true)),
    func("kv_min", 2, 3, |a, cx| extreme(a, cx, false)),
    func("kv_add", 2, 3, kv_add),
    func("kv_del", 1, 1, kv_del),
    func("kv_ttl", 1, 1, kv_ttl),
    func("kv_held", 2, 3, kv_held),
    func("kv_changed", 2, 3, kv_changed),
    func("kv_hll_add", 2, 3, kv_hll_add),
    func("kv_hll_count", 1, 1, kv_hll_count),
];

/// The family of a state function; `None` for any other function.
pub(crate) fn family(func: &Func) -> Option<Family> {
    FUNCS.iter().any(|f| std::ptr::eq(f, func)).then(|| {
        if func.name.starts_with(EMQX_PREFIX) {
            Family::Emqx
        } else if func.name.starts_with(DICT_PREFIX) {
            Family::Dict
        } else {
            Family::Own
        }
    })
}

/// What a dry run evaluates state calls on: a scratch store that is dropped
/// afterwards, never the store the rules were loaded with (D11).
pub(crate) struct Scratch<'a> {
    store: Option<&'a dyn StateStore>,
    opened: std::cell::RefCell<Vec<Opened>>,
}

/// A namespace of the scratch store, by name.
type Opened = (Arc<str>, Arc<dyn StateNamespace>);

impl<'a> Scratch<'a> {
    pub(crate) fn new(store: Option<&'a dyn StateStore>) -> Self {
        Self {
            store,
            opened: std::cell::RefCell::new(Vec::new()),
        }
    }

    /// The scratch store's namespace of the same name and settings.
    pub(crate) fn namespace(
        &self,
        binding: &StateBinding,
    ) -> Result<Arc<dyn StateNamespace>, EvalError> {
        if let Some((_, ns)) = self
            .opened
            .borrow()
            .iter()
            .find(|(name, _)| *name == binding.name)
        {
            return Ok(ns.clone());
        }
        let store = self.store.ok_or_else(|| {
            EvalError::new("a dry run was given no scratch store, so its rules cannot call state")
        })?;
        let ns = store
            .open(&binding.name, &binding.settings)
            .map_err(EvalError::new)?;
        self.opened
            .borrow_mut()
            .push((binding.name.clone(), ns.clone()));
        Ok(ns)
    }
}

impl StateBinding {
    /// The store's namespace the rules were loaded with.
    pub(crate) fn namespace(&self) -> &dyn StateNamespace {
        &*self.namespace
    }
}
