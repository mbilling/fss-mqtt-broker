//! EMQX's `proc_dict_put/2`, `proc_dict_get/1` and `proc_dict_del/1`
//! ([ADR 0085](../../../../docs/adr/0085-rule-state-store.md) D13): the dictionary of
//! whoever evaluates the rule.
//!
//! In EMQX they are `erlang:put/2`, `get/1` and `erase/1` under the key
//! `{'@rule_engine', Key}`, in the process that evaluates the rule. Probed on EMQX
//! 6.3.1, that process is the client's connection process for everything about the
//! client: its publishes and what their rules republish, its events, the deliveries to
//! it, its Will. Two publishes of one connection see each other's values; another
//! client, and the same client after a reconnect, do not.
//!
//! Here the dictionary is a [`Dictionary`] that whoever evaluates owns and hands to
//! the evaluation ([`RuleSet::on_publish_in`](crate::RuleSet::on_publish_in)): the
//! broker keeps one per connection task. It is a plain map behind a `RefCell`, so it
//! is `Send` and not `Sync`: nothing can share one between tasks, and no call takes a
//! lock. An evaluation that is handed none gets an empty one of its own, dropped with
//! it.
//!
//! What EMQX returns, mqttd returns: `put` and `del` the value held before or
//! `undefined`, `get` the value or `undefined`; a key is any value, matched exactly
//! (`=:=`, [`term_key`]). EMQX's dictionary grows until the node is out of memory; a
//! [`Dictionary`] holds [`MAX_DICT_KEYS`] keys and [`MAX_DICT_BYTES`] bytes, counted
//! against the store's ceiling, and a call past either fails its rule.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::sync::Arc;

use super::{charge_read, fail, term_key, written_len, StateError, StateStore};
use crate::funcs::FnCtx;
use crate::value::{Map, Value};
use crate::EvalError;

/// The most keys one dictionary holds.
pub const MAX_DICT_KEYS: usize = 64;

/// The most bytes one dictionary's keys and values take together, each as the rule
/// engine writes it ([`written_len`]).
pub const MAX_DICT_BYTES: usize = 64 * 1024;

/// A new key in a dictionary at [`MAX_DICT_KEYS`], or a value that would take it past
/// [`MAX_DICT_BYTES`].
pub const DICTIONARY_FULL: &str = "dictionary_full";

/// What one key holds.
#[derive(Debug)]
struct Held {
    value: Value,
    /// What the key and the value are accounted as.
    bytes: usize,
}

/// The dictionary `proc_dict_*` work on: one per evaluating task, as EMQX's is one
/// per process.
#[derive(Debug, Default)]
pub struct Dictionary {
    /// By the key's [`term_key`]. A `BTreeMap`: it is at most [`MAX_DICT_KEYS`] long,
    /// and an empty one allocates nothing and needs no hasher, so a connection whose
    /// rules never call `proc_dict_*` pays nothing for having one.
    entries: RefCell<BTreeMap<Box<[u8]>, Held>>,
    bytes: Cell<usize>,
    /// The store whose ceiling this dictionary's bytes count against.
    counted_in: Option<Arc<dyn StateStore>>,
}

impl Dictionary {
    /// An empty dictionary, bounded like any and counted in no store: a dry run's.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty dictionary whose bytes count against `store`'s ceiling
    /// ([`StateStore::dictionary_grow`]) for as long as it holds them.
    #[must_use]
    pub fn counted_in(store: Arc<dyn StateStore>) -> Self {
        let mut dictionary = Self::default();
        dictionary.counted_in = Some(store);
        dictionary
    }

    /// How many keys it holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.borrow().len()
    }

    /// Whether it holds no key.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.borrow().is_empty()
    }

    /// The bytes its keys and values are accounted as.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes.get()
    }

    /// Forget every key, as a process that ends takes its dictionary with it.
    pub fn clear(&self) {
        if self.bytes.get() == 0 && self.is_empty() {
            return;
        }
        self.entries.borrow_mut().clear();
        // Shrinking is never refused.
        let _ = self.account(self.bytes.replace(0), 0);
    }

    /// Tell the store the dictionary went from `before` bytes to `after`. Growth is
    /// refused where the store has no room.
    fn account(&self, before: usize, after: usize) -> Result<(), StateError> {
        let Some(store) = &self.counted_in else {
            return Ok(());
        };
        let wide = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
        if after > before {
            store.dictionary_grow(wide(after - before))
        } else {
            if before > after {
                store.dictionary_shrink(wide(before - after));
            }
            Ok(())
        }
    }

    fn get(&self, key: &[u8]) -> Option<(Value, usize)> {
        self.entries
            .borrow()
            .get(key)
            .map(|held| (held.value.clone(), held.bytes))
    }

    /// Take `key` out; what it held.
    fn del(&self, key: &[u8]) -> Option<(Value, usize)> {
        let held = self.entries.borrow_mut().remove(key)?;
        let before = self.bytes.get();
        let after = before.saturating_sub(held.bytes);
        self.bytes.set(after);
        // Shrinking is never refused.
        let _ = self.account(before, after);
        Some((held.value, held.bytes))
    }

    /// Hold `value` under `key`, which is `key_bytes` as written; what it held before.
    fn put(
        &self,
        key: Vec<u8>,
        key_bytes: usize,
        value: &Value,
    ) -> Result<Option<(Value, usize)>, StateError> {
        let bytes = key_bytes.saturating_add(written_len(value));
        let mut entries = self.entries.borrow_mut();
        let held = entries.get(key.as_slice()).map(|held| held.bytes);
        if held.is_none() && entries.len() >= MAX_DICT_KEYS {
            return Err(StateError {
                reason: DICTIONARY_FULL,
                message: format!(
                    "this connection's dictionary already holds {MAX_DICT_KEYS} keys \
                     (proc_dict_del frees one; EMQX's dictionary has no bound, mqttd's has)"
                ),
            });
        }
        let before = self.bytes.get();
        let after = before
            .saturating_sub(held.unwrap_or(0))
            .saturating_add(bytes);
        if after > MAX_DICT_BYTES {
            return Err(StateError {
                reason: DICTIONARY_FULL,
                message: format!(
                    "this connection's dictionary would hold {after} bytes of keys and values \
                     as written; at most {MAX_DICT_BYTES} (EMQX's dictionary has no bound, \
                     mqttd's has)"
                ),
            });
        }
        self.account(before, after)?;
        self.bytes.set(after);
        let previous = entries.insert(
            key.into_boxed_slice(),
            Held {
                value: owned(value),
                bytes,
            },
        );
        Ok(previous.map(|held| (held.value, held.bytes)))
    }
}

impl Drop for Dictionary {
    fn drop(&mut self) {
        // Its keys go with it; the store's total gives their bytes back.
        let held = self.bytes.get();
        if held > 0 {
            let _ = self.account(held, 0);
        }
    }
}

/// A value as a dictionary keeps it. A binary is copied: one cut out of a payload
/// shares the payload's buffer, and keeping it would keep the whole message for as
/// long as the connection lives.
fn owned(v: &Value) -> Value {
    fn has_binary(v: &Value) -> bool {
        match v {
            Value::Bin(_) => true,
            Value::Array(items) => items.iter().any(has_binary),
            Value::Map(map) => map.iter().any(|(_, v)| has_binary(v)),
            _ => false,
        }
    }
    fn copied(v: &Value) -> Value {
        match v {
            Value::Bin(b) => Value::Bin(bytes::Bytes::copy_from_slice(b)),
            Value::Array(items) => Value::from(items.iter().map(copied).collect::<Vec<_>>()),
            Value::Map(map) => Value::from(
                map.iter()
                    .map(|(k, v)| (k.clone(), copied(v)))
                    .collect::<Map>(),
            ),
            other => other.clone(),
        }
    }
    if has_binary(v) {
        copied(v)
    } else {
        v.clone()
    }
}

fn refused(e: &StateError) -> EvalError {
    EvalError::new(e.to_string())
}

/// A key: any value (`undefined` included), matched exactly, and its size as written.
/// `None` for one that could not fit a dictionary, and so is in none: its identity is
/// not even built.
fn dict_key(v: &Value) -> Option<(Vec<u8>, usize)> {
    let bytes = written_len(v);
    (bytes <= MAX_DICT_BYTES).then(|| (term_key(v), bytes))
}

/// What a call answers: the value that was held, charged to the message's build
/// budget as any value read out of state is, or `undefined`.
fn answer(cx: &FnCtx, held: Option<(Value, usize)>) -> Result<Value, EvalError> {
    match held {
        Some((value, bytes)) => {
            charge_read(cx, bytes)?;
            Ok(value)
        }
        None => Ok(Value::Undefined),
    }
}

/// `proc_dict_put(Key, Value)`: `erlang:put/2`, which answers the value held before,
/// or `undefined`.
pub(super) fn proc_dict_put(a: &[Value], cx: &FnCtx) -> Result<Value, EvalError> {
    cx.ctx.charge_state_call()?;
    let dict = cx.ctx.dictionary();
    let Some((key, key_bytes)) = dict_key(&a[0]) else {
        if a[1].is_undefined() {
            return Ok(Value::Undefined);
        }
        return Err(fail(
            DICTIONARY_FULL,
            format!(
                "a key of more than {MAX_DICT_BYTES} bytes as written does not fit a \
                 dictionary (EMQX's dictionary has no bound, mqttd's has)"
            ),
        ));
    };
    let held = if a[1].is_undefined() {
        // In EMQX a key that holds `undefined` and a key that is not there answer
        // alike to all three functions, so holding it would only use up one of the
        // keys a dictionary has here.
        dict.del(&key)
    } else {
        dict.put(key, key_bytes, &a[1]).map_err(|e| refused(&e))?
    };
    answer(cx, held)
}

/// `proc_dict_get(Key)`: `erlang:get/1`, the value or `undefined`.
pub(super) fn proc_dict_get(a: &[Value], cx: &FnCtx) -> Result<Value, EvalError> {
    cx.ctx.charge_state_call()?;
    let held = dict_key(&a[0]).and_then(|(key, _)| cx.ctx.dictionary().get(&key));
    answer(cx, held)
}

/// `proc_dict_del(Key)`: `erlang:erase/1`, which answers the value held before, or
/// `undefined`.
pub(super) fn proc_dict_del(a: &[Value], cx: &FnCtx) -> Result<Value, EvalError> {
    cx.ctx.charge_state_call()?;
    let held = dict_key(&a[0]).and_then(|(key, _)| cx.ctx.dictionary().del(&key));
    answer(cx, held)
}
