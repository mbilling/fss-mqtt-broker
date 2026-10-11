//! The rule state endpoints ([ADR 0085](../../../../docs/adr/0085-rule-state-store.md)
//! D10), in the shape of the rules endpoints (ADR 0084 D6): each answers for the node it
//! runs on, names it, and takes its parameters in the query string.
//!
//! | Endpoint | Role | Does |
//! |---|---|---|
//! | `GET /admin/v1/rules/state` | viewer | Each namespace's mode, bounds, counts and bytes; the store's total and the connections' dictionaries'. No key, no value. |
//! | `GET /admin/v1/rules/state/keys?ns=&prefix=&expires_within=&no_expiry=&values=&cursor=` | operator | One page of a namespace's keys, each with its type, size and expiry, and its value when asked. |
//! | `GET /admin/v1/rules/state/key?ns=&key=` | operator | One key's value, type, size and expiry. |
//! | `DELETE /admin/v1/rules/state/key?ns=&key=` | operator and rules writer | Deletes one key; audited as `state.delete`. |
//!
//! # Who
//!
//! A key is built from client ids, topic levels and payload fields, and a value is
//! payload data: both are an operator's, as the rules' SQL is. A viewer reads counts. A
//! delete changes what the rules decide next, so it needs what a rules write needs: the
//! operator role and a subject listed in `[rules] admin_writers`.
//!
//! # How a key and a value are written
//!
//! A key is bytes. It is shown as `key` with `key_encoding`, and addressed with the
//! same two as parameters, so what a listing shows is what a read or a delete takes:
//!
//! * `utf8` (the default): the key is that text.
//! * `base64`: the key is those bytes, for one that is not UTF-8.
//! * `term`, in `kv_store` only: EMQX's table takes a key of any type, matched exactly
//!   (`1`, `1.0` and `'1'` are three keys). A string key is shown and addressed like any
//!   other, as `utf8` or `base64`. Any other key is shown as the base64 of the bytes the
//!   store matches it by ([`mqtt_rules::state::term_key`]), with `term`: the key as the
//!   rule engine writes it, to read.
//!
//! A value is shown as the rule engine writes it (ADR 0086 D3's form): a string or a
//! binary as its bytes, anything else as JSON text, so an integer of any size keeps its
//! digits and a map its order. `value_encoding` says how those bytes are carried
//! (`utf8`, or `base64` for bytes that are not), and `value_type` what the value is
//! (`string`, `integer`, `map`, …; `undefined` in `kv_store`, where it can be stored).
//!
//! # Which keys: one selection
//!
//! [`Selection`] is the keys of one namespace a request names, read from its query
//! once: `prefix` (with `prefix_encoding`), `expires_within=<duration>` and
//! `no_expiry=true`. A key is selected when **every** parameter given selects it.
//! `expires_within` and `no_expiry` together select nothing by definition and are
//! refused. The listing takes it now; whatever acts on a selection of keys (a purge, a
//! change of expiry) takes the same one, so that what a listing shows is what the
//! action reaches.
//!
//! # The audit log names no key
//!
//! `state.delete` records the namespace and the key's SHA-256 and length, and the
//! `admin.request` record of these endpoints carries `key`, `prefix` and `cursor` the
//! same way ([`audit_query`]; a cursor holds the last key listed): the audit log is
//! read by more people than may read payload data, and whoever knows a key can still
//! find what was done to it. Which filters a listing used (`expires_within`,
//! `no_expiry`, `values`) is recorded as it came: none of them is of a key.

use super::http::Request;
use super::routes::{error, Answer};
use super::{AdminState, Caller};
use crate::reload::sha256_hex;
use crate::rule_state::report;
use crate::rule_state::{
    ByExpiry, Cursor, KeysError, Limits, Selector, Store, Values, MAX_PAGE_KEYS,
    MAX_PAGE_VALUE_BYTES,
};
use crate::rules_sys::rfc3339_millis;
use mqtt_rules::state::{duration_text, key_term, term_key, KV_STORE_NAMESPACE, MAX_KEY_BYTES};
use mqtt_rules::Value as Term;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

/// The paths whose `key` and `prefix` parameters the audit log does not carry.
const STATE_PATHS: &str = "/admin/v1/rules/state/";

/// Bytes as JSON text can carry them: as text when they are UTF-8, base64 otherwise.
fn text_of(bytes: &[u8]) -> (String, &'static str) {
    match std::str::from_utf8(bytes) {
        Ok(text) => (text.to_string(), "utf8"),
        Err(_) => (crate::backup::b64_encode(bytes), "base64"),
    }
}

/// A term as the rule engine writes it, to read. Its JSON has no `undefined` and no
/// `-0.0`, which are both keys of their own in EMQX's table, so those two are written
/// out; inside an array or a map they read as the JSON has them.
fn term_text(term: &Term) -> String {
    match term {
        Term::Undefined => "undefined".to_string(),
        Term::Float(f) if *f == 0.0 && f.is_sign_negative() => "-0.0".to_string(),
        other => other.to_json().unwrap_or_default(),
    }
}

/// The bytes of a string key in EMQX's table: what follows the tag and the length in
/// its [`term_key`]. `None` for a key of any other type.
fn string_term(key: &[u8]) -> Option<&[u8]> {
    let (head, text) = key.split_at_checked(9)?;
    let len = u64::from_be_bytes(head[1..].try_into().ok()?);
    (head[0] == 7 && usize::try_from(len).ok() == Some(text.len())).then_some(text)
}

/// A stored key as it is shown: `key` and `key_encoding`, and `term` for a `kv_store`
/// key that is not a string. What it shows is what [`addressed`] takes.
fn shown_key(kv_store: bool, key: &[u8]) -> Value {
    if !kv_store {
        let (key, encoding) = text_of(key);
        return json!({"key": key, "key_encoding": encoding});
    }
    if let Some(text) = string_term(key) {
        let (key, encoding) = text_of(text);
        return json!({"key": key, "key_encoding": encoding});
    }
    json!({
        "key": crate::backup::b64_encode(key),
        "key_encoding": "term",
        "term": key_term(key).as_ref().map(term_text),
    })
}

/// The bytes a `key` or `prefix` parameter names under `encoding`, before they are
/// made a namespace's key: what the audit log hashes.
fn named(text: &str, encoding: Option<&str>) -> Result<Vec<u8>, String> {
    match encoding {
        None | Some("utf8") => Ok(text.as_bytes().to_vec()),
        Some("base64" | "term") => {
            crate::backup::b64_decode(text).map_err(|e| format!("not base64: {e}"))
        }
        Some(other) => Err(format!(
            "the encoding is utf8, base64 or term, not {other:?}"
        )),
    }
}

/// The store's key for the `key` parameter `text` under `encoding`, in a namespace
/// that is EMQX's table (`kv_store`) or not.
fn addressed(kv_store: bool, text: &str, encoding: Option<&str>) -> Result<Vec<u8>, String> {
    let bytes = named(text, encoding)?;
    match (encoding, kv_store) {
        (Some("term"), false) => Err(
            "key_encoding=term is for kv_store, whose keys are any term; this namespace's \
             keys are strings (utf8 or base64)"
                .to_string(),
        ),
        (Some("term"), true) => key_term(&bytes)
            .map(|_| bytes)
            .ok_or_else(|| "key is not a term's key as a listing shows it".to_string()),
        // A string and a binary of the same bytes are one key there.
        (_, true) => Ok(term_key(&Term::Bin(bytes.into()))),
        (_, false) if (1..=MAX_KEY_BYTES).contains(&bytes.len()) => Ok(bytes),
        (_, false) => Err(format!(
            "a key is 1 to {MAX_KEY_BYTES} bytes, got {}",
            bytes.len()
        )),
    }
}

/// A value as it is shown: as the rule engine writes it, how those bytes are carried,
/// and what the value is.
fn shown_value(value: &Term) -> (String, &'static str, &'static str) {
    match value.as_bytes() {
        Some(bytes) => {
            let (text, encoding) = text_of(bytes);
            let kind = if encoding == "utf8" {
                "string"
            } else {
                "binary"
            };
            (text, encoding, kind)
        }
        None => (term_text(value), "utf8", value.type_name()),
    }
}

/// What a value takes in an answer: its text as JSON carries it, quotes and escapes
/// included. What [`MAX_PAGE_VALUE_BYTES`] bounds.
fn value_size(value: &Term) -> usize {
    serde_json::to_string(&shown_value(value).0).map_or(0, |text| text.len())
}

/// When a key expires as an answer says it: the time, or `null` for never.
fn expires_at(expires_ms: u64) -> Value {
    json!((expires_ms != 0).then(|| rfc3339_millis(UNIX_EPOCH + Duration::from_millis(expires_ms))))
}

/// A parameter that is `true` or `false` (`false` when it is left out).
fn flag(req: &Request, name: &str) -> Result<bool, String> {
    match req.param(name) {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        Some(other) => Err(format!("{name} is true or false, not {other:?}")),
    }
}

/// A duration as a parameter carries it: as the rules file writes one, a whole number
/// with a unit (`90s`, `10m`, `1h`, `7d`), or a whole number of seconds.
fn duration(text: &str) -> Result<Duration, String> {
    let seconds = || {
        text.bytes()
            .all(|b| b.is_ascii_digit())
            .then(|| text.parse::<u64>().ok())?
    };
    seconds()
        .or_else(|| duration_text(text))
        .map(Duration::from_secs)
        .ok_or_else(|| {
            format!(
                "{text:?} is not a duration: a whole number of seconds, or one with a unit \
                 (s, m, h, d) such as 90s, 10m, 1h or 7d"
            )
        })
}

/// The keys of one namespace that a request names (see the module's documentation).
/// Read from the query once; [`selector`](Self::selector) is what the store walks by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Selection {
    /// The bytes `prefix` names; empty for none.
    prefix: Vec<u8>,
    /// Which keys by their expiry: `expires_within`, `no_expiry`, or neither.
    expiry: ByExpiry,
    /// The namespace is EMQX's table, where a prefix is of a string key's text.
    kv_store: bool,
}

impl Selection {
    /// The selection `req` names in a namespace that is EMQX's table (`kv_store`) or
    /// not.
    ///
    /// # Errors
    /// What is wrong with a parameter, or with two of them together.
    pub(super) fn of(req: &Request, kv_store: bool) -> Result<Self, String> {
        let prefix = match req.param("prefix_encoding") {
            Some("term") => Err("prefix_encoding is utf8 or base64".to_string()),
            encoding => named(req.param("prefix").unwrap_or_default(), encoding),
        }
        .map_err(|e| format!("prefix: {e}"))?;
        let never = flag(req, "no_expiry")?;
        let within = req
            .param("expires_within")
            .map(|text| duration(text).map_err(|e| format!("expires_within: {e}")))
            .transpose()?;
        let expiry = match (within, never) {
            (Some(_), true) => {
                return Err(
                    "expires_within and no_expiry select nothing together: a key that \
                     expires within a time has an expiry. Give one of them"
                        .to_string(),
                )
            }
            (Some(within), false) => ByExpiry::Within(within),
            (None, true) => ByExpiry::Never,
            (None, false) => ByExpiry::Any,
        };
        Ok(Self {
            prefix,
            expiry,
            kv_store,
        })
    }

    /// What the store selects keys by.
    pub(super) fn selector(&self) -> Selector<'_> {
        let by_prefix = if self.kv_store {
            Selector::string_prefix(&self.prefix)
        } else {
            Selector::prefix(&self.prefix)
        };
        by_prefix.expiring(self.expiry)
    }
}

/// The query of a state request as the audit log records it: `key`, `prefix` and
/// `cursor` (which holds the last key listed) by SHA-256 and length, everything else
/// as it came. `None` for any other path.
#[must_use]
pub fn audit_query(request: &Request) -> Option<Vec<(String, String)>> {
    if !request.path.starts_with(STATE_PATHS) {
        return None;
    }
    let encoding = |name: &str| request.param(&format!("{name}_encoding"));
    let mut query = Vec::with_capacity(request.query.len() + 2);
    for (name, value) in &request.query {
        if name == "key" || name == "prefix" || name == "cursor" {
            // Bytes that do not decode are hashed as they were sent.
            let bytes = named(value, encoding(name)).unwrap_or_else(|_| value.clone().into_bytes());
            query.push((format!("{name}_sha256"), sha256_hex(&bytes)));
            query.push((format!("{name}_bytes"), bytes.len().to_string()));
        } else {
            query.push((name.clone(), value.clone()));
        }
    }
    Some(query)
}

/// A refusal that names the node.
fn refuse(node: &str, status: u16, code: &str, message: &str) -> Answer {
    (
        status,
        json!({ "error": { "code": code, "message": message }, "node": node }).to_string(),
    )
}

fn bad_request(node: &str, message: &str) -> Answer {
    refuse(node, 400, "bad-request", message)
}

/// The store the running rules keep their state in, and the rules; or the refusal.
fn store_of(state: &AdminState) -> Result<(crate::rules::Rules, Arc<Store>), Answer> {
    let Some(access) = &state.rules else {
        return Err(error(
            503,
            "unavailable",
            "the rules are not wired on this node",
        ));
    };
    let rules = access.rules().clone();
    match rules.state().cloned() {
        Some(store) => Ok((rules, store)),
        None => Err(refuse(
            &state.node_id,
            409,
            "rule-state-off",
            "rule state is switched off on this node (rules.state_max_bytes = 0)",
        )),
    }
}

/// `?ns=`, or the refusal.
fn namespace<'a>(node: &str, req: &'a Request) -> Result<&'a str, Answer> {
    req.param("ns")
        .filter(|ns| !ns.is_empty())
        .ok_or_else(|| bad_request(node, "ns is required: the namespace"))
}

fn no_namespace(node: &str, ns: &str) -> Answer {
    refuse(
        node,
        404,
        "not-found",
        &format!(
            "the running rules hold no state namespace `{ns}` (GET /admin/v1/rules/state lists \
             them)"
        ),
    )
}

/// `?key=` of namespace `ns` as the store's key, or the refusal.
fn key_of(node: &str, req: &Request, ns: &str) -> Result<Vec<u8>, Answer> {
    let Some(text) = req.param("key") else {
        return Err(bad_request(node, "key is required"));
    };
    addressed(ns == KV_STORE_NAMESPACE, text, req.param("key_encoding"))
        .map_err(|e| bad_request(node, &format!("key: {e}")))
}

/// Run `work`, which takes the store's locks and may walk a namespace, off the async
/// workers.
async fn blocking(work: impl FnOnce() -> Answer + Send + 'static) -> Answer {
    tokio::task::spawn_blocking(work)
        .await
        .unwrap_or_else(|e| error(503, "unavailable", &format!("the state task failed: {e}")))
}

/// `GET /admin/v1/rules/state`: what the store holds, by namespace. Counts only: a
/// viewer's to read.
pub async fn overview(state: &AdminState) -> Answer {
    let node = state.node_id.clone();
    let (rules, store) = match store_of(state) {
        Ok(found) => found,
        // Switched off is an answer, not a refusal: there is nothing held.
        Err((409, _)) => {
            return (
                200,
                json!({"node": node, "enabled": false, "namespaces": []}).to_string(),
            )
        }
        Err(refusal) => return refusal,
    };
    blocking(move || {
        let totals = store.totals();
        let namespaces: Vec<Value> = report::namespaces(&store, &rules.current())
            .iter()
            .map(report::NamespaceReport::json)
            .collect();
        (
            200,
            json!({
                "node": node,
                "enabled": true,
                "store": {
                    "bytes": totals.bytes,
                    "max_bytes": totals.max_bytes,
                    "brownout": totals.brownout,
                },
                "dictionaries": {
                    "bytes": totals.dictionary_bytes,
                    "refused": totals.dictionary_refused,
                },
                "namespaces": namespaces,
            })
            .to_string(),
        )
    })
    .await
}

/// `GET /admin/v1/rules/state/keys?ns=<namespace>[&prefix=<text>[&prefix_encoding=…]]
/// [&expires_within=<duration>|&no_expiry=true][&values=true][&limit=<n>]
/// [&cursor=<next_cursor>]`: one page of the namespace's keys that the [`Selection`]
/// names, 100 by default and at most 1,000. Each key is an object with its `type`,
/// its `bytes` (what the namespace accounts it as) and `expires_at` (`null`: never);
/// with `values=true` also `value`, `value_encoding` and `value_type`, as the read of
/// one key shows them, and the page then ends where its values pass
/// [`MAX_PAGE_VALUE_BYTES`] of the answer. `next_cursor` is `null` on the last page; a
/// page can be short, or empty, and still have one.
pub async fn keys(state: &AdminState, req: &Request) -> Answer {
    let node = state.node_id.clone();
    let (_, store) = match store_of(state) {
        Ok(found) => found,
        Err(refusal) => return refusal,
    };
    let ns = match namespace(&node, req) {
        Ok(ns) => ns.to_string(),
        Err(refusal) => return refusal,
    };
    let kv_store = ns == KV_STORE_NAMESPACE;
    let asked = Selection::of(req, kv_store).and_then(|keys| Ok((keys, flag(req, "values")?)));
    let (selection, values) = match asked {
        Ok(asked) => asked,
        Err(e) => return bad_request(&node, &e),
    };
    // As every list of the API: 100 by default, at most 1,000.
    let limit = match super::sessions::count_param(req, "limit", super::sessions::DEFAULT_LIMIT) {
        Ok(n) => n.min(MAX_PAGE_KEYS),
        Err(refusal) => return refusal,
    };
    let cursor = match req.param("cursor").filter(|c| !c.is_empty()) {
        None => None,
        Some(text) => match Cursor::decode(text) {
            Some(cursor) => Some(cursor),
            None => return bad_request(&node, "cursor is not one a listing gave"),
        },
    };
    blocking(move || {
        let selector = selection.selector();
        let limits = Limits {
            keys: limit,
            values: values.then_some(Values {
                room: MAX_PAGE_VALUE_BYTES,
                size: value_size,
            }),
            ..Limits::default()
        };
        let page = match store.keys(&ns, &selector, cursor.as_ref(), &limits) {
            Ok(page) => page,
            Err(KeysError::NoNamespace) => return no_namespace(&node, &ns),
            Err(KeysError::StaleCursor) => {
                return refuse(
                    &node,
                    409,
                    "cursor-stale",
                    "the namespace this cursor was given by is gone (a reload dropped it and \
                     declared it again): start the listing over",
                )
            }
        };
        let keys: Vec<Value> = page
            .keys
            .iter()
            .map(|listed| {
                let mut key = shown_key(kv_store, &listed.key);
                key["type"] = json!(listed.type_name);
                key["bytes"] = json!(listed.bytes);
                key["expires_at"] = expires_at(listed.expires_ms);
                if let Some(value) = &listed.value {
                    let (value, value_encoding, value_type) = shown_value(value);
                    key["value"] = json!(value);
                    key["value_encoding"] = json!(value_encoding);
                    key["value_type"] = json!(value_type);
                }
                key
            })
            .collect();
        (
            200,
            json!({
                "node": node,
                "namespace": ns,
                "count": keys.len(),
                "keys": keys,
                "next_cursor": page.next.as_ref().map(Cursor::encode),
            })
            .to_string(),
        )
    })
    .await
}

/// `GET /admin/v1/rules/state/key?ns=<namespace>&key=<key>[&key_encoding=…]`: what the
/// key holds, as a rule would read it; its type; what the namespace accounts it as;
/// when it expires.
pub async fn key(state: &AdminState, req: &Request) -> Answer {
    let node = state.node_id.clone();
    let (_, store) = match store_of(state) {
        Ok(found) => found,
        Err(refusal) => return refusal,
    };
    let found = namespace(&node, req)
        .and_then(|ns| key_of(&node, req, ns).map(|key| (ns.to_string(), key)));
    let (ns, key) = match found {
        Ok(found) => found,
        Err(refusal) => return refusal,
    };
    blocking(move || {
        let view = match store.key(&ns, &key) {
            Ok(Some(view)) => view,
            Ok(None) => {
                return refuse(
                    &node,
                    404,
                    "not-found",
                    &format!("namespace `{ns}` holds no such key"),
                )
            }
            Err(_) => return no_namespace(&node, &ns),
        };
        let (value, value_encoding, value_type) = shown_value(&view.value);
        let mut body = shown_key(ns == KV_STORE_NAMESPACE, &key);
        body["node"] = json!(node);
        body["namespace"] = json!(ns);
        body["type"] = json!(view.type_name);
        body["bytes"] = json!(view.bytes);
        body["value"] = json!(value);
        body["value_encoding"] = json!(value_encoding);
        body["value_type"] = json!(value_type);
        body["expires_at"] = expires_at(view.expires_ms);
        (200, body.to_string())
    })
    .await
}

/// Whether `caller` may change the state the rules keep, or the refusal: what a rules
/// write needs (the operator role, which the route asked for, and a subject listed in
/// `[rules] admin_writers`). Asked before anything about the request is answered.
fn may_change(state: &AdminState, caller: &Caller) -> Result<(), Answer> {
    super::rules::writer_refusal(state, caller).map_or(Ok(()), Err)
}

/// Record a change to the state the rules keep. `key` is named by its SHA-256 and its
/// length, never by itself.
fn audit_change(state: &AdminState, caller: &Caller, kind: &str, ns: &str, key: &[u8], fact: &str) {
    state.audit.record(
        kind,
        Some(&caller.subject),
        &format!(
            "ns={ns} key_sha256={} key_bytes={} {fact}",
            sha256_hex(key),
            key.len()
        ),
    );
}

/// `DELETE /admin/v1/rules/state/key?ns=<namespace>&key=<key>[&key_encoding=…]`:
/// delete one key, whatever its type. `deleted` says whether a value was there.
pub async fn delete_key(state: &AdminState, caller: &Caller, req: &Request) -> Answer {
    if let Err(refusal) = may_change(state, caller) {
        return refusal;
    }
    let node = state.node_id.clone();
    let (_, store) = match store_of(state) {
        Ok(found) => found,
        Err(refusal) => return refusal,
    };
    let ns = match namespace(&node, req) {
        Ok(ns) => ns.to_string(),
        Err(refusal) => return refusal,
    };
    let key = match key_of(&node, req, &ns) {
        Ok(key) => key,
        Err(refusal) => return refusal,
    };
    // What the caller named, which is what the request's own audit record hashes.
    let named = named(
        req.param("key").unwrap_or_default(),
        req.param("key_encoding"),
    )
    .unwrap_or_default();
    let (state, caller) = (state.clone(), caller.clone());
    blocking(move || {
        let Ok(deleted) = store.delete_key(&ns, &key) else {
            return no_namespace(&node, &ns);
        };
        audit_change(
            &state,
            &caller,
            "state.delete",
            &ns,
            &named,
            &format!("deleted={deleted}"),
        );
        let mut body = shown_key(ns == KV_STORE_NAMESPACE, &key);
        body["node"] = json!(node);
        body["namespace"] = json!(ns);
        body["deleted"] = json!(deleted);
        (200, body.to_string())
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use mqtt_rules::Map;

    fn request(path: &str, query: &[(&str, &str)]) -> Request {
        Request {
            method: "GET".into(),
            path: path.into(),
            query: query
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
            body: Vec::new(),
        }
    }

    /// What a listing shows of a key is what a read or a delete takes: text, bytes that
    /// are not UTF-8, and in EMQX's table a key of any type, each back to the same
    /// stored bytes.
    #[test]
    fn a_shown_key_addresses_the_key_it_was_shown_for() {
        let round_trip = |kv_store: bool, stored: &[u8]| {
            let shown = shown_key(kv_store, stored);
            let back = addressed(
                kv_store,
                shown["key"].as_str().unwrap(),
                shown["key_encoding"].as_str(),
            )
            .unwrap_or_else(|e| panic!("{shown}: {e}"));
            assert_eq!(back, stored, "{shown}");
            shown
        };
        // A namespace's keys are strings: text, control characters and all, or bytes.
        let shown = round_trip(false, b"hot:device-1");
        assert_eq!(
            shown,
            json!({"key": "hot:device-1", "key_encoding": "utf8"})
        );
        round_trip(false, "k\u{1b}[2J\r\n\u{9b}".as_bytes());
        round_trip(false, " leading and trailing ".as_bytes());
        let shown = round_trip(false, b"\xff\x00\xfe");
        assert_eq!(shown, json!({"key": "/wD+", "key_encoding": "base64"}));
        // EMQX's table: a string key reads like any other; any other term is shown by
        // the bytes it is matched by, with the term to read.
        let shown = round_trip(true, &term_key(&Term::from("device-1")));
        assert_eq!(shown, json!({"key": "device-1", "key_encoding": "utf8"}));
        let shown = round_trip(true, &term_key(&Term::Bin(vec![0xff, 0x01].into())));
        assert_eq!(shown["key_encoding"], "base64");
        let mut map = Map::new();
        map.insert("b", Term::Int(1));
        map.insert("a", Term::Float(1.0));
        for (term, text) in [
            (Term::Int(1), "1"),
            (Term::Float(1.0), "1.0"),
            (Term::Float(-0.0), "-0.0"),
            (Term::Bool(true), "true"),
            (Term::Null, "null"),
            (Term::Undefined, "undefined"),
            (Term::from(vec![Term::Int(1), Term::from("x")]), "[1,\"x\"]"),
            (Term::from(map), "{\"a\":1.0,\"b\":1}"),
        ] {
            let shown = round_trip(true, &term_key(&term));
            assert_eq!(shown["key_encoding"], "term", "{text}");
            assert_eq!(shown["term"], text);
        }
        // `1`, `1.0` and `'1'` are three keys, and are shown as three.
        let keys: std::collections::BTreeSet<String> =
            [Term::Int(1), Term::Float(1.0), Term::from("1")]
                .iter()
                .map(|t| shown_key(true, &term_key(t)).to_string())
                .collect();
        assert_eq!(keys.len(), 3);
    }

    /// A key parameter that names no key is refused, saying why.
    #[test]
    fn a_key_that_names_none_is_refused() {
        for (kv_store, text, encoding, why) in [
            (false, "", None, "1 to 256 bytes"),
            (false, "x", Some("hex"), "utf8, base64 or term"),
            (false, "!!", Some("base64"), "not base64"),
            (false, "BA==", Some("term"), "kv_store"),
            (true, "/w==", Some("term"), "not a term's key"),
        ] {
            let e = addressed(kv_store, text, encoding).unwrap_err();
            assert!(e.contains(why), "{text:?} {encoding:?}: {e}");
        }
        assert!(addressed(false, &"k".repeat(257), None).is_err());
        assert!(addressed(false, &"k".repeat(256), None).is_ok());
    }

    /// A value reads as the rule engine writes it, with what it is: a string is not the
    /// number of the same digits, an integer past 64 bits keeps every digit, and bytes
    /// that are not text are base64.
    #[test]
    fn a_value_is_shown_as_the_rule_engine_writes_it() {
        let big = mqtt_rules::json_decode(b"123456789012345678901234567890").unwrap();
        assert!(matches!(big, Term::Big(_)));
        let mut map = Map::new();
        map.insert("z", Term::Int(1));
        map.insert("a", Term::from("\u{1b}"));
        for (value, shown) in [
            (Term::from("1"), ("1", "utf8", "string")),
            (Term::Int(1), ("1", "utf8", "integer")),
            (Term::Float(1.0), ("1.0", "utf8", "float")),
            (big, ("123456789012345678901234567890", "utf8", "integer")),
            (Term::Bool(false), ("false", "utf8", "boolean")),
            (Term::Null, ("null", "utf8", "null")),
            (Term::Undefined, ("undefined", "utf8", "undefined")),
            (
                Term::Bin(vec![0xff, 0x00].into()),
                ("/wA=", "base64", "binary"),
            ),
            (
                Term::from(map),
                ("{\"z\":1,\"a\":\"\\u001B\"}", "utf8", "map"),
            ),
            (
                Term::from(vec![Term::Int(1), Term::Null]),
                ("[1,null]", "utf8", "array"),
            ),
        ] {
            let (text, encoding, kind) = shown_value(&value);
            assert_eq!((text.as_str(), encoding, kind), shown);
        }
    }

    /// The audit log's record of a state request names `key` and `prefix` by hash and
    /// length, whatever their encoding, and leaves every other request as it was.
    #[test]
    fn the_audit_log_names_a_state_key_by_its_hash() {
        let hash = sha256_hex(b"hot:device-1");
        let plain = request(
            "/admin/v1/rules/state/key",
            &[("ns", "alerts"), ("key", "hot:device-1")],
        );
        let expected = vec![
            ("ns".to_string(), "alerts".to_string()),
            ("key_sha256".to_string(), hash.clone()),
            ("key_bytes".to_string(), "12".to_string()),
        ];
        assert_eq!(audit_query(&plain), Some(expected));
        let encoded = request(
            "/admin/v1/rules/state/key",
            &[
                ("ns", "alerts"),
                ("key", "aG90OmRldmljZS0x"),
                ("key_encoding", "base64"),
            ],
        );
        let query = audit_query(&encoded).unwrap();
        assert_eq!(query[1], ("key_sha256".to_string(), hash));
        assert!(!format!("{query:?}").contains("aG90"));
        let listing = request(
            "/admin/v1/rules/state/keys",
            &[("ns", "alerts"), ("prefix", "hot:")],
        );
        let query = audit_query(&listing).unwrap();
        assert_eq!(query[1], ("prefix_sha256".to_string(), sha256_hex(b"hot:")));
        assert_eq!(query[2], ("prefix_bytes".to_string(), "4".to_string()));
        // Not a state request: as it came.
        assert_eq!(
            audit_query(&request("/admin/v1/kick", &[("client", "c1")])),
            None
        );
        assert_eq!(audit_query(&request("/admin/v1/rules/state", &[])), None);
    }

    /// The record of a listing says which filters it used and whether it asked for
    /// values, as they came, and names no key: not the prefix, and not the cursor,
    /// which holds the last key the page before listed.
    #[test]
    fn the_audit_log_records_a_listings_filters_and_no_key() {
        // A cursor is the last key listed, in hex: here `hot:dev-1`.
        let cursor = Cursor::decode("1-00000000000000aa-3-686f743a6465762d31").unwrap();
        assert!(cursor.encode().ends_with("-686f743a6465762d31"));
        let listing = request(
            "/admin/v1/rules/state/keys",
            &[
                ("ns", "alerts"),
                ("prefix", "hot:"),
                ("expires_within", "10m"),
                ("values", "true"),
                ("limit", "50"),
                ("cursor", &cursor.encode()),
            ],
        );
        let pair = |name: &str, value: &str| (name.to_string(), value.to_string());
        assert_eq!(
            audit_query(&listing).unwrap(),
            [
                pair("ns", "alerts"),
                pair("prefix_sha256", &sha256_hex(b"hot:")),
                pair("prefix_bytes", "4"),
                pair("expires_within", "10m"),
                pair("values", "true"),
                pair("limit", "50"),
                pair("cursor_sha256", &sha256_hex(cursor.encode().as_bytes())),
                pair("cursor_bytes", &cursor.encode().len().to_string()),
            ]
        );
        let never = request(
            "/admin/v1/rules/state/keys",
            &[("ns", "kv_store"), ("no_expiry", "true")],
        );
        assert_eq!(
            audit_query(&never).unwrap(),
            [pair("ns", "kv_store"), pair("no_expiry", "true")]
        );
    }

    /// One selection, read from the query once: a prefix, `expires_within` and
    /// `no_expiry`, each alone or with the prefix. A duration is written as the rules
    /// file writes one, or as a whole number of seconds. The two expiry filters
    /// together are refused, as is anything that is not what its parameter takes.
    #[test]
    fn a_selection_is_read_from_the_query_and_refuses_what_contradicts() {
        let of = |query: &[(&str, &str)], kv_store: bool| {
            Selection::of(&request("/admin/v1/rules/state/keys", query), kv_store)
        };
        let all = of(&[("ns", "alerts")], false).unwrap();
        assert_eq!(
            (all.prefix.as_slice(), all.expiry),
            (&b""[..], ByExpiry::Any)
        );
        for (text, secs) in [
            ("90", 90),
            ("90s", 90),
            ("10m", 600),
            ("1h", 3600),
            ("7d", 7 * 86_400),
            ("0s", 0),
        ] {
            let keys = of(&[("prefix", "hot:"), ("expires_within", text)], false).unwrap();
            assert_eq!(
                (keys.prefix.as_slice(), keys.expiry),
                (&b"hot:"[..], ByExpiry::Within(Duration::from_secs(secs))),
                "{text}"
            );
        }
        let never = of(
            &[
                ("no_expiry", "true"),
                ("prefix", "/wD+"),
                ("prefix_encoding", "base64"),
            ],
            true,
        )
        .unwrap();
        assert_eq!(
            (never.prefix.as_slice(), never.expiry, never.kv_store),
            (&b"\xff\x00\xfe"[..], ByExpiry::Never, true)
        );
        // `false` is the parameter left out.
        let plain = of(&[("no_expiry", "false"), ("expires_within", "1h")], false).unwrap();
        assert_eq!(plain.expiry, ByExpiry::Within(Duration::from_hours(1)));
        for (query, why) in [
            (
                &[("no_expiry", "true"), ("expires_within", "1h")][..],
                "expires_within and no_expiry select nothing together",
            ),
            (
                &[("expires_within", "soon")],
                "expires_within: \"soon\" is not a duration",
            ),
            (&[("expires_within", "")], "is not a duration"),
            (&[("expires_within", "1.5h")], "is not a duration"),
            (&[("expires_within", "-5m")], "is not a duration"),
            (&[("expires_within", "+5")], "is not a duration"),
            (&[("expires_within", "10 m")], "is not a duration"),
            (&[("expires_within", "250ms")], "is not a duration"),
            (
                &[("expires_within", "99999999999999999999d")],
                "is not a duration",
            ),
            (&[("no_expiry", "yes")], "no_expiry is true or false"),
            (&[("no_expiry", "")], "no_expiry is true or false"),
            (
                &[("prefix", "!"), ("prefix_encoding", "base64")],
                "prefix: not base64",
            ),
            (
                &[("prefix", "x"), ("prefix_encoding", "term")],
                "prefix: prefix_encoding is utf8 or base64",
            ),
        ] {
            let e = of(query, false).unwrap_err();
            assert!(e.contains(why), "{query:?}: {e}");
        }
    }

    /// What a value takes of a page is its text as the answer's JSON carries it: the
    /// quotes, and an escape for every character JSON escapes.
    #[test]
    fn a_value_is_measured_as_the_answer_carries_it() {
        assert_eq!(value_size(&Term::from("abc")), 5);
        assert_eq!(value_size(&Term::Int(1234)), 6);
        assert_eq!(value_size(&Term::from("a\"b")), 6);
        assert_eq!(value_size(&Term::from("\u{1b}")), 8);
        // A map is JSON text inside a JSON string: its quotes are escaped.
        let mut map = Map::new();
        map.insert("a", Term::Int(1));
        assert_eq!(value_size(&Term::from(map)), "\"{\\\"a\\\":1}\"".len());
        // Bytes that are not text are carried as base64.
        assert_eq!(value_size(&Term::Bin(vec![0xff, 0x00, 0xfe].into())), 6);
    }
}
