//! The store behind rule SQL (ADR 0085), through the rule engine.
//!
//! `kv_store_*` is EMQX's, and every expectation of it below is what EMQX 6.3.1
//! answered (`emqx/emqx:latest`, 2026-10-11): `emqx_rule_funcs` called directly, and
//! statements run through `emqx_rule_sqltester:test/1`, whose effects were then read
//! back from the node's table. A comment names the probe (`d..` direct, `s..` SQL).
//! EMQX's `{ok, Map}` is the output's JSON here, `{error, nomatch}` no output, and an
//! error a failed rule. The atom `undefined` is written `"undefined"`, as the rule
//! engine writes it everywhere.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use bytes::Bytes;
use mqtt_rules::state::{StateOp, StateStore};
use mqtt_rules::{Effect, Functions, Outcome, PublishInput, RuleSet};

use super::{stored, NamespaceStats, Store, ENTRY_OVERHEAD};

mod once;

/// A store on a clock the test drives, and rules loaded with it.
struct Rig {
    store: Arc<Store>,
    functions: Functions,
    clock: Arc<AtomicU64>,
    /// Rules that hold EMQX's table and a lone statement's own namespace
    /// (`rule:test`), so that statements tried one after another see one state, as
    /// messages on a broker do.
    _held: Arc<RuleSet>,
}

const HOLD: &str = "[rules.test]\nsql = '''SELECT kv_get('held') AS a, kv_store_get('held') AS b \
                    FROM \"hold/#\"'''\n";

fn rig() -> Rig {
    rig_of(256 << 20)
}

fn rig_of(max_bytes: u64) -> Rig {
    let clock = Arc::new(AtomicU64::new(1_800_000_000_000));
    let store = Store::with_manual_clock(max_bytes, clock.clone());
    let functions = Functions::default().with_state(store.clone());
    let held = Arc::new(RuleSet::parse_with(HOLD, &functions).unwrap().rules);
    Rig {
        store,
        functions,
        clock,
        _held: held,
    }
}

/// What one rule did with one message.
#[derive(Debug, PartialEq, Eq)]
enum Did {
    /// Its outputs.
    Out(Vec<String>),
    /// Why it failed.
    Failed(String),
}

impl Rig {
    fn advance_secs(&self, secs: u64) {
        self.clock.fetch_add(secs * 1000, Relaxed);
    }

    /// One statement on one message: EMQX's SQL test, on this rig's store.
    fn sql(&self, sql: &str, payload: &str) -> Result<Vec<String>, String> {
        let payload = Bytes::from(payload.to_string());
        let props = mqtt_core::AppProperties::default();
        let input = PublishInput::new("c1", "t/1", &payload, 1, &props);
        mqtt_rules::test_sql_with(sql, &input, &self.functions)
    }

    /// The one output of `SELECT <fields>` on a `{}` message.
    fn select(&self, fields: &str) -> String {
        self.select_on(fields, "{}")
    }

    fn select_on(&self, fields: &str, payload: &str) -> String {
        let sql = format!("SELECT {fields} FROM \"t/#\"");
        let mut out = self
            .sql(&sql, payload)
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(out.len(), 1, "{sql}: {out:?}");
        out.remove(0)
    }

    fn fails(&self, fields: &str) -> String {
        let sql = format!("SELECT {fields} FROM \"t/#\"");
        self.sql(&sql, "{}").expect_err(&sql)
    }

    /// What EMQX's table holds under `key` (an expression), as `kv_store_get` reads it.
    fn table(&self, key: &str) -> String {
        self.select(&format!("kv_store_get({key}) AS v"))
    }

    fn load(&self, text: &str) -> Arc<RuleSet> {
        Arc::new(
            RuleSet::parse_with(text, &self.functions)
                .unwrap_or_else(|e| panic!("{e}"))
                .rules,
        )
    }

    fn stats(&self, namespace: &str) -> NamespaceStats {
        self.store
            .stats(namespace)
            .unwrap_or_else(|| panic!("no namespace {namespace}"))
    }
}

/// A payload whose `a` is the integers from `lo` to `hi`.
fn ints(lo: u32, hi: u32) -> String {
    let list: Vec<String> = (lo..=hi).map(|i| i.to_string()).collect();
    format!("{{\"a\":[{}]}}", list.join(","))
}

/// A rules file of `(id, state key, sql)` rules, each with a `console` action.
fn rules(tables: &str, rules: &[(&str, Option<&str>, &str)]) -> String {
    let rules: Vec<String> = rules
        .iter()
        .map(|(id, state, sql)| {
            let state = state.map_or_else(String::new, |s| format!("state = \"{s}\"\n"));
            format!(
                "\n[rules.{id}]\n{state}sql = '''{sql}'''\nactions = [{{ function = \"console\" }}]\n"
            )
        })
        .collect();
    format!("{tables}{}", rules.concat())
}

/// One message through `set`, as the broker evaluates it: what each rule did.
fn publish(set: &RuleSet, topic: &str, payload: &str) -> Vec<(String, Did)> {
    let payload = Bytes::from(payload.to_string());
    let props = mqtt_core::AppProperties::default();
    let input = PublishInput::new("c1", topic, &payload, 1, &props);
    let mut failed = Vec::new();
    let mut ran = Vec::new();
    let mut out = Vec::new();
    set.on_publish(
        &input,
        &mut |rule, outcome| match outcome {
            Outcome::Passed | Outcome::NoResult => ran.push(rule.id().to_string()),
            Outcome::Failed(e) => {
                ran.push(rule.id().to_string());
                failed.push((rule.id().to_string(), e.to_string()));
            }
            _ => {}
        },
        &mut out,
    );
    ran.into_iter()
        .map(|id| {
            let did = match failed.iter().find(|(f, _)| *f == id) {
                Some((_, e)) => Did::Failed(e.clone()),
                None => Did::Out(
                    out.iter()
                        .filter(|(rule, _)| **rule == *id)
                        .filter_map(|(_, effect)| match effect {
                            Effect::Console(line) => Some(line.clone()),
                            Effect::Republish(_) => None,
                        })
                        .collect(),
                ),
            };
            (id, did)
        })
        .collect()
}

/// The outputs of the one rule `id` for one message.
fn outputs(set: &RuleSet, id: &str, payload: &str) -> Vec<String> {
    match publish(set, "t/1", payload)
        .into_iter()
        .find(|(rule, _)| rule == id)
    {
        Some((_, Did::Out(out))) => out,
        other => panic!("{id}: {other:?}"),
    }
}

// -- EMQX's kv_store_*: what EMQX 6.3.1 answers -----------------------------------------

/// d01–d08, s01: `put` is `true`, `get` the value or `undefined` or the default, `del`
/// `true` whether or not the key was there.
#[test]
fn kv_store_answers_what_emqx_answers() {
    let r = rig();
    assert_eq!(
        r.select(
            "kv_store_put('a', 1) AS p, kv_store_get('a') AS g, kv_store_get('nope') AS m, \
             kv_store_get('nope', 'dflt') AS d, kv_store_get('a', 'dflt') AS e, \
             kv_store_del('a') AS x, kv_store_del('a') AS y, kv_store_get('a') AS z"
        ),
        r#"{"p":true,"g":1,"m":"undefined","d":"dflt","e":1,"x":true,"y":true,"z":"undefined"}"#
    );
    // d32: a second put replaces the first.
    assert_eq!(
        r.select("kv_store_put('ow', 1) AS a, kv_store_put('ow', 2) AS b, kv_store_get('ow') AS g"),
        r#"{"a":true,"b":true,"g":2}"#
    );
    // s26, s27: both results are `true`, so a bare call passes a WHERE.
    for cond in ["kv_store_del('zz')", "kv_store_put('zz', 1)"] {
        let out = r.sql(&format!("SELECT 1 AS x FROM \"t/#\" WHERE {cond}"), "{}");
        assert_eq!(out.unwrap(), [r#"{"x":1}"#], "{cond}");
    }
}

/// d09–d22, d27–d31, d33–d35, s17, s19, s30, s34–s36: a key is any term, matched
/// exactly (`=:=`), as the `ets` `set` table matches it.
#[test]
fn a_kv_store_key_is_any_term_matched_exactly() {
    let r = rig();
    // s17: 1, 1.0 and '1' are three keys.
    assert_eq!(
        r.select(
            "kv_store_put(1, 'int') AS a, kv_store_put(1.0, 'float') AS b, \
             kv_store_put('1', 'bin') AS c, kv_store_get(1) AS g1, kv_store_get(1.0) AS g2, \
             kv_store_get('1') AS g3"
        ),
        r#"{"a":true,"b":true,"c":true,"g1":"int","g2":"float","g3":"bin"}"#
    );
    // d15–d17: deleting 1.0 leaves 1.
    assert_eq!(
        r.select("kv_store_del(1.0) AS d, kv_store_get(1) AS g1, kv_store_get(1.0) AS g2"),
        r#"{"d":true,"g1":"int","g2":"undefined"}"#
    );
    // s30: a number out of a payload is the same key as the literal.
    assert_eq!(
        r.select_on(
            "kv_store_put(payload.i, 'pi') AS a, kv_store_put(payload.f, 'pf') AS b, \
             kv_store_get(1) AS g1, kv_store_get(1.0) AS g2, kv_store_get(payload.i) AS g3",
            r#"{"i":1,"f":1.0}"#
        ),
        r#"{"a":true,"b":true,"g1":"pi","g2":"pf","g3":"pi"}"#
    );
    // d35: 0 and 0.0 are two keys, and -0.0 a third (no value under it).
    assert_eq!(
        r.select(
            "kv_store_put(0, 'i') AS a, kv_store_put(0.0, 'f') AS b, kv_store_get(0) AS g1, \
             kv_store_get(0.0) AS g2, kv_store_get(-0.0) AS g3"
        ),
        r#"{"a":true,"b":true,"g1":"i","g2":"f","g3":"undefined"}"#
    );
    // s19, s35: a map and a list are keys; a map in another order is the same key, one
    // with a float where the other has an integer is not.
    assert_eq!(
        r.select_on(
            "kv_store_put(payload.a, 'ints') AS a, kv_store_get(payload.b) AS g, \
             kv_store_get(payload.a) AS g2, kv_store_put(payload.m1, 'm') AS b, \
             kv_store_get(payload.m2) AS g3, kv_store_get(payload.m3) AS g4",
            r#"{"a":[1,2],"b":[1.0,2.0],"m1":{"x":1,"y":2},"m2":{"y":2,"x":1},"m3":{"y":2.0,"x":1}}"#
        ),
        r#"{"a":true,"g":"undefined","g2":"ints","b":true,"g3":"m","g4":"undefined"}"#
    );
    // s36, d27–d29, d34: `true` and `null` are not the strings 'true' and 'null'.
    assert_eq!(
        r.select_on(
            "kv_store_put(true, 'atom') AS a, kv_store_get('true') AS g, \
             kv_store_get(true) AS g2, kv_store_put(payload.n, 'nullkey') AS b, \
             kv_store_get('null') AS g3, kv_store_get(payload.n) AS g4, \
             kv_store_get(payload.t) AS g5",
            r#"{"n":null,"t":true}"#
        ),
        r#"{"a":true,"g":"undefined","g2":"atom","b":true,"g3":"undefined","g4":"nullkey","g5":"atom"}"#
    );
    // s20, d30, d31: `undefined` is a key. d33: so is the empty string.
    assert_eq!(
        r.select(
            "kv_store_put(payload.nokey, 'x') AS a, kv_store_get(payload.nokey) AS g, \
             kv_store_put('', 1) AS b, kv_store_get('') AS g2"
        ),
        r#"{"a":true,"g":"x","b":true,"g2":1}"#
    );
    // s34: an integer past 64 bits is a key, and not the float of the same value.
    assert_eq!(
        r.select(
            "kv_store_put(18446744073709551616, 'big') AS a, \
             kv_store_get(18446744073709551616) AS g, \
             kv_store_get(18446744073709551616.0) AS g2"
        ),
        r#"{"a":true,"g":"big","g2":"undefined"}"#
    );
    // s44: a key is its bytes, however the string was built.
    assert_eq!(
        r.select("kv_store_put('ab', 1) AS p, kv_store_get('a' + 'b') AS g, kv_store_get(concat('a', 'b')) AS g2"),
        r#"{"p":true,"g":1,"g2":1}"#
    );
}

/// d23–d26, s18, s20, s34, s37: a value of any type comes back exactly as it went in.
#[test]
fn a_kv_store_value_is_any_term_and_comes_back_as_it_was() {
    let r = rig();
    // s18, s33: the whole payload is stored as its text; a field of it as the map.
    assert_eq!(
        r.select_on(
            "kv_store_put('m', payload) AS a, kv_store_get('m') AS g, \
             kv_store_put('m2', payload.o) AS b, kv_store_get('m2') AS g2",
            r#"{"o":{"l":[1,2.5,"s",true,null]}}"#
        ),
        r#"{"a":true,"g":"{\"o\":{\"l\":[1,2.5,\"s\",true,null]}}","b":true,"g2":{"l":[1,2.5,"s",true,null]}}"#
    );
    // s37: a float stays a float, and `null`, '' and `false` are values a default does
    // not replace.
    assert_eq!(
        r.select_on(
            "kv_store_put('v1', 1.0) AS a, kv_store_get('v1') AS g, \
             kv_store_put('v2', payload.n) AS b, kv_store_get('v2', 'd') AS g2, \
             kv_store_put('v3', '') AS c, kv_store_get('v3', 'd') AS g3, \
             kv_store_put('v4', false) AS d, kv_store_get('v4', 'd') AS g4",
            r#"{"n":null}"#
        ),
        r#"{"a":true,"g":1.0,"b":true,"g2":null,"c":true,"g3":"","d":true,"g4":false}"#
    );
    // s20, d23, d24: `undefined` is stored, and is a value: the default is for a
    // missing key only.
    assert_eq!(
        r.select("kv_store_put('uv', payload.nokey) AS b, kv_store_get('uv', 'd') AS g2"),
        r#"{"b":true,"g2":"undefined"}"#
    );
    // s34: an integer past 64 bits.
    assert_eq!(
        r.select(
            "kv_store_put('bigv', 123456789012345678901234567890) AS b, kv_store_get('bigv') AS g"
        ),
        r#"{"b":true,"g":123456789012345678901234567890}"#
    );
    // s29: a missing key with a default that is itself `undefined`.
    assert_eq!(
        r.select("kv_store_get('nx') AS a, is_null(kv_store_get('nx')) AS n, kv_store_get('nx', payload.nokey) AS c"),
        r#"{"a":"undefined","n":true,"c":"undefined"}"#
    );
}

/// s12: three messages through one statement count 1, 2, 3. s39: two statements share
/// the node's one table. s32: what the statements wrote is in the table afterwards.
#[test]
fn kv_store_keeps_its_values_from_one_message_to_the_next() {
    let r = rig();
    let count = "kv_store_get('n', 0) + 1 AS n, kv_store_put('n', n) AS p";
    assert_eq!(r.select(count), r#"{"n":1,"p":true}"#);
    assert_eq!(r.select(count), r#"{"n":2,"p":true}"#);
    assert_eq!(r.select(count), r#"{"n":3,"p":true}"#);
    assert_eq!(r.table("'n'"), r#"{"v":3}"#);
    assert_eq!(
        r.select("kv_store_put('shared', clientid) AS a"),
        r#"{"a":true}"#
    );
    assert_eq!(
        r.sql("SELECT kv_store_get('shared') AS g FROM \"t/1\"", "{}")
            .unwrap(),
        [r#"{"g":"c1"}"#]
    );
}

/// The table is shared by every rule of the file, whatever its `state` key says, and
/// is not the namespace `kv_*` works on.
#[test]
fn kv_store_is_one_table_for_every_rule_and_apart_from_kv() {
    let r = rig();
    let set = r.load(&rules(
        "[state.alerts]\n",
        &[
            (
                "a",
                Some("alerts"),
                "SELECT kv_store_put('x', 'emqx') AS p, kv_set('x', 'own') AS s FROM \"t/#\"",
            ),
            (
                "b",
                None,
                "SELECT kv_store_get('x') AS e, kv_get('x') AS o FROM \"t/#\"",
            ),
            (
                "c",
                Some("alerts"),
                "SELECT kv_store_get('x') AS e, kv_get('x') AS o FROM \"t/#\"",
            ),
        ],
    ));
    assert_eq!(
        publish(&set, "t/1", "{}"),
        [
            ("a".into(), Did::Out(vec![r#"{"p":true,"s":"own"}"#.into()])),
            (
                "b".into(),
                Did::Out(vec![r#"{"e":"emqx","o":"undefined"}"#.into()])
            ),
            (
                "c".into(),
                Did::Out(vec![r#"{"e":"emqx","o":"own"}"#.into()])
            ),
        ]
    );
    assert_eq!(
        r.store.namespaces(),
        ["alerts", "kv_store", "rule:b", "rule:test"]
    );
}

// -- when a state call runs: EMQX's order, probed with kv_store_put -----------------------

/// s02: a write in a `SELECT` field is in the table although the `WHERE` was false.
/// s10, s11: fields run left to right, and the `WHERE` after them, seeing their writes.
#[test]
fn a_select_write_happens_for_a_message_the_where_rejects() {
    let r = rig();
    let out = r.sql(
        "SELECT kv_store_put('w', 7) AS p FROM \"t/#\" WHERE 1 = 2",
        "{}",
    );
    assert_eq!(out.unwrap(), Vec::<String>::new());
    assert_eq!(r.table("'w'"), r#"{"v":7}"#);
    assert_eq!(
        r.select(
            "kv_store_get('o') AS before, kv_store_put('o', 5) AS p, kv_store_get('o') AS after"
        ),
        r#"{"before":"undefined","p":true,"after":5}"#
    );
    let out = r.sql(
        "SELECT kv_store_put('ws', 5) AS p FROM \"t/#\" WHERE kv_store_get('ws') = 5",
        "{}",
    );
    assert_eq!(out.unwrap(), [r#"{"p":true}"#]);
    // s51: a comparison's operands run left to right.
    let out = r.sql(
        "SELECT kv_store_put('cmp', 1) AS p FROM \"t/#\" \
         WHERE kv_store_get('cmp') = kv_store_put('cmp', true)",
        "{}",
    );
    assert_eq!(out.unwrap(), Vec::<String>::new());
    assert_eq!(r.table("'cmp'"), r#"{"v":true}"#);
    // s53.
    assert_eq!(
        r.select(
            "kv_store_put('ar2', 5) AS p, (kv_store_get('ar2') * 2) - kv_store_get('ar2') AS r"
        ),
        r#"{"p":true,"r":5}"#
    );
    // The same holds for mqttd's own functions: every message counts, not the ones
    // that pass (D3).
    let count = "SELECT kv_incr('n') AS n FROM \"t/#\" WHERE payload.ok = true";
    assert_eq!(
        r.sql(count, r#"{"ok":false}"#).unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(
        r.sql(count, r#"{"ok":false}"#).unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(r.sql(count, r#"{"ok":true}"#).unwrap(), [r#"{"n":3}"#]);
}

/// s03–s06, s57: `AND` and `OR` stop at the operand that decides. s07, s40, s41, s55: a
/// `CASE` runs its conditions until one holds and only that branch. s54: `NOT`.
#[test]
fn a_write_behind_and_or_or_an_untaken_case_branch_does_not_happen() {
    let r = rig();
    let where_ = |cond: &str| {
        r.sql(
            &format!("SELECT 1 AS x FROM \"t/#\" WHERE {cond}"),
            r#"{"x":1}"#,
        )
        .unwrap()
        .len()
    };
    assert_eq!(where_("1 = 2 AND kv_store_put('and', 1) = true"), 0);
    assert_eq!(r.table("'and'"), r#"{"v":"undefined"}"#);
    assert_eq!(where_("1 = 1 OR kv_store_put('or', 1) = true"), 1);
    assert_eq!(r.table("'or'"), r#"{"v":"undefined"}"#);
    assert_eq!(where_("1 = 1 AND kv_store_put('and2', 1) = true"), 1);
    assert_eq!(r.table("'and2'"), r#"{"v":1}"#);
    assert_eq!(where_("1 = 2 OR kv_store_put('or2', 1) = true"), 1);
    assert_eq!(r.table("'or2'"), r#"{"v":1}"#);
    assert_eq!(
        where_(
            "kv_store_put('a3_1', 1) = true AND kv_store_put('a3_2', 1) = false \
             AND kv_store_put('a3_3', 1) = true"
        ),
        0
    );
    assert_eq!(
        r.select("kv_store_get('a3_1') AS a, kv_store_get('a3_2') AS b, kv_store_get('a3_3') AS c"),
        r#"{"a":1,"b":1,"c":"undefined"}"#
    );
    assert_eq!(where_("NOT (kv_store_put('nt', 1) = false)"), 1);
    assert_eq!(r.table("'nt'"), r#"{"v":1}"#);

    assert_eq!(
        r.select_on(
            "CASE WHEN payload.x = 1 THEN kv_store_put('case_t', 1) \
             ELSE kv_store_put('case_e', 1) END AS c",
            r#"{"x":1}"#
        ),
        r#"{"c":true}"#
    );
    assert_eq!(
        r.select("kv_store_get('case_t') AS t, kv_store_get('case_e') AS e"),
        r#"{"t":1,"e":"undefined"}"#
    );
    assert_eq!(
        r.select_on(
            "CASE payload.x WHEN 1 THEN kv_store_put('cw_t', 1) \
             WHEN 2 THEN kv_store_put('cw_2', 1) END AS c",
            r#"{"x":1}"#
        ),
        r#"{"c":true}"#
    );
    assert_eq!(
        r.select("kv_store_get('cw_t') AS t, kv_store_get('cw_2') AS e"),
        r#"{"t":1,"e":"undefined"}"#
    );
    assert_eq!(
        r.select(
            "CASE WHEN kv_store_put('cc1', 1) = false THEN 'a' \
             WHEN kv_store_put('cc2', 1) = true THEN 'b' \
             WHEN kv_store_put('cc3', 1) = true THEN 'c' END AS c"
        ),
        r#"{"c":"b"}"#
    );
    assert_eq!(
        r.select("kv_store_get('cc1') AS a, kv_store_get('cc2') AS b, kv_store_get('cc3') AS c"),
        r#"{"a":1,"b":1,"c":"undefined"}"#
    );
    assert_eq!(
        r.select(
            "CASE kv_store_put('con', 1) WHEN true THEN 'yes' ELSE kv_store_put('cel', 1) END AS c"
        ),
        r#"{"c":"yes"}"#
    );
    assert_eq!(
        r.select("kv_store_get('con') AS a, kv_store_get('cel') AS b"),
        r#"{"a":1,"b":"undefined"}"#
    );
}

/// s45–s47: an `IN` list is evaluated whole, so a write after the match happens and a
/// failing element after it fails the rule. s56: every element of a list literal runs.
/// s38: `kv_store_get`'s default is evaluated whether or not it is used.
#[test]
fn an_in_list_a_list_literal_and_a_default_are_evaluated_whole() {
    let r = rig();
    let out = r.sql(
        "SELECT 1 AS x FROM \"t/#\" WHERE 1 IN (1, kv_store_put('inl', 1))",
        "{}",
    );
    assert_eq!(out.unwrap(), [r#"{"x":1}"#]);
    assert_eq!(r.table("'inl'"), r#"{"v":1}"#);
    let out = r.sql(
        "SELECT 1 AS x FROM \"t/#\" WHERE 5 IN (kv_store_put('in1', 1), 2, kv_store_put('in2', 1))",
        "{}",
    );
    assert_eq!(out.unwrap(), Vec::<String>::new());
    assert_eq!(
        r.select("kv_store_get('in1') AS a, kv_store_get('in2') AS b"),
        r#"{"a":1,"b":1}"#
    );
    assert!(r
        .sql("SELECT 1 AS x FROM \"t/#\" WHERE 1 IN (1, abs('x'))", "{}")
        .is_err());
    assert_eq!(
        r.select("[kv_store_put('l1', 1), kv_store_put('l2', 1)] AS l"),
        r#"{"l":[true,true]}"#
    );
    assert_eq!(
        r.select("kv_store_get('l1') AS a, kv_store_get('l2') AS b"),
        r#"{"a":1,"b":1}"#
    );
    assert_eq!(
        r.select("kv_store_put('e', 1) AS a, kv_store_get('e', kv_store_put('side', 1)) AS g, kv_store_get('side') AS s"),
        r#"{"a":true,"g":1,"s":1}"#
    );
}

/// s08, s42: a write stays when a later field, or the `WHERE`, fails the rule. s09: a
/// field that fails first stops the write after it.
#[test]
fn a_write_stays_when_the_rule_then_fails() {
    let r = rig();
    assert!(r
        .fails("kv_store_put('fail', 1) AS p, abs('x') AS q")
        .starts_with("abs(): "));
    assert_eq!(r.table("'fail'"), r#"{"v":1}"#);
    assert!(r
        .fails("abs('x') AS q, kv_store_put('fail2', 1) AS p")
        .starts_with("abs(): "));
    assert_eq!(r.table("'fail2'"), r#"{"v":"undefined"}"#);
    assert!(r
        .sql(
            "SELECT kv_store_put('wf', 1) AS p FROM \"t/#\" WHERE abs('x') = 1",
            "{}"
        )
        .is_err());
    assert_eq!(r.table("'wf'"), r#"{"v":1}"#);
    // A refused state call fails the rule like any error, after the writes before it.
    assert!(r
        .fails("kv_set('k', 1) AS a, kv_incr('k') AS b")
        .starts_with("kv_incr(): wrong_type: "));
    assert_eq!(r.select("kv_get('k') AS v"), r#"{"v":1}"#);
}

/// s13–s16, s31, s43: in a `FOREACH` the collection's calls run once and before the
/// `WHERE`, the `INCASE` once per element, the `DO` once per element that passed.
#[test]
fn foreach_writes_happen_per_element() {
    let r = rig();
    // s13.
    assert_eq!(
        r.sql(
            "FOREACH payload.a DO kv_store_put(item, 1) AS p, item AS i FROM \"t/#\"",
            r#"{"a":["f1","f2","f3"]}"#
        )
        .unwrap(),
        [
            r#"{"p":true,"i":"f1"}"#,
            r#"{"p":true,"i":"f2"}"#,
            r#"{"p":true,"i":"f3"}"#
        ]
    );
    assert_eq!(
        r.select("kv_store_get('f1') AS a, kv_store_get('f2') AS b, kv_store_get('f3') AS c"),
        r#"{"a":1,"b":1,"c":1}"#
    );
    // s14: the INCASE ran for all three, the DO for the two that passed.
    assert_eq!(
        r.sql(
            "FOREACH payload.a DO kv_store_put(item, 'do') AS p \
             INCASE kv_store_put('in_' + item, 1) = true AND item <> 'g2' FROM \"t/#\"",
            r#"{"a":["g1","g2","g3"]}"#
        )
        .unwrap(),
        [r#"{"p":true}"#, r#"{"p":true}"#]
    );
    assert_eq!(
        r.select(
            "kv_store_get('g1') AS a, kv_store_get('g2') AS b, kv_store_get('g3') AS c, \
             kv_store_get('in_g1') AS d, kv_store_get('in_g2') AS e, kv_store_get('in_g3') AS f"
        ),
        r#"{"a":"do","b":"undefined","c":"do","d":1,"e":1,"f":1}"#
    );
    // s15: a false WHERE comes after the collection's write and before any element's.
    assert_eq!(
        r.sql(
            "FOREACH payload.a, kv_store_put('coll', 1) AS c DO kv_store_put(item, 'do') AS p \
             FROM \"t/#\" WHERE 1 = 2",
            r#"{"a":["h1"]}"#
        )
        .unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(
        r.select("kv_store_get('coll') AS a, kv_store_get('h1') AS b"),
        r#"{"a":1,"b":"undefined"}"#
    );
    // s16: a later DO field, and the next element, see the write before them.
    assert_eq!(
        r.sql(
            "FOREACH kv_store_put('c1', 1) AS c, payload.a \
             DO kv_store_get('cnt', 0) + 1 AS n, kv_store_put('cnt', n) AS p FROM \"t/#\"",
            r#"{"a":[1,2,3]}"#
        )
        .unwrap(),
        [
            r#"{"n":1,"p":true}"#,
            r#"{"n":2,"p":true}"#,
            r#"{"n":3,"p":true}"#
        ]
    );
    assert_eq!(
        r.select("kv_store_get('c1') AS a, kv_store_get('cnt') AS b"),
        r#"{"a":1,"b":3}"#
    );
    // s31: an INCASE without a DO.
    let out = r.sql(
        "FOREACH payload.a INCASE kv_store_put('nd_' + item, 1) = true FROM \"t/#\"",
        r#"{"a":["q1","q2"]}"#,
    );
    assert_eq!(out.unwrap().len(), 2);
    assert_eq!(
        r.select("kv_store_get('nd_q1') AS a, kv_store_get('nd_q2') AS b"),
        r#"{"a":1,"b":1}"#
    );
    // s43: a DO that fails leaves the write its element made before it.
    assert!(r
        .sql(
            "FOREACH payload.a DO kv_store_put('df_' + item, 1) AS p, abs(item) AS q FROM \"t/#\"",
            r#"{"a":["x"]}"#
        )
        .is_err());
    assert_eq!(r.table("'df_x'"), r#"{"v":1}"#);
}

/// D3: a later rule the message matches sees an earlier rule's write (rules run in id
/// order), within one evaluation and without waiting for anything.
#[test]
fn a_later_rule_sees_an_earlier_rules_write() {
    let r = rig();
    let set = r.load(&rules(
        "[state.s]\n",
        &[
            (
                "a",
                Some("s"),
                "SELECT kv_incr('n') AS n, kv_store_put('last', topic) AS p FROM \"t/#\"",
            ),
            (
                "b",
                Some("s"),
                "SELECT kv_get('n') AS n, kv_store_get('last') AS last FROM \"t/#\"",
            ),
            (
                "c",
                Some("s"),
                "SELECT kv_incr('n') AS n FROM \"t/#\" WHERE 1 = 2",
            ),
            ("d", Some("s"), "SELECT kv_get('n') AS n FROM \"t/#\""),
        ],
    ));
    assert_eq!(
        publish(&set, "t/1", "{}"),
        [
            ("a".into(), Did::Out(vec![r#"{"n":1,"p":true}"#.into()])),
            ("b".into(), Did::Out(vec![r#"{"n":1,"last":"t/1"}"#.into()])),
            ("c".into(), Did::Out(vec![])),
            ("d".into(), Did::Out(vec![r#"{"n":2}"#.into()])),
        ]
    );
}

// -- mqttd's kv_* ----------------------------------------------------------------------

#[test]
fn kv_set_get_and_del() {
    let r = rig();
    assert_eq!(
        r.select_on(
            "kv_get('k') AS a, kv_set('k', payload.v) AS s, kv_get('k') AS g, \
             kv_set('k', 'next') AS s2, kv_get('k') AS g2, kv_del('k') AS d, kv_del('k') AS d2, \
             kv_get('k') AS g3",
            r#"{"v":{"a":[1,2.5,"x",null]}}"#
        ),
        r#"{"a":"undefined","s":{"a":[1,2.5,"x",null]},"g":{"a":[1,2.5,"x",null]},"s2":"next","g2":"next","d":true,"d2":false,"g3":"undefined"}"#
    );
    // An integer past 64 bits and a float stay what they were.
    assert_eq!(
        r.select("kv_set('big', 123456789012345678901234567890) AS a, kv_get('big') AS b, kv_set('f', 2.0) AS c, kv_get('f') AS d"),
        r#"{"a":123456789012345678901234567890,"b":123456789012345678901234567890,"c":2.0,"d":2.0}"#
    );
}

#[test]
fn kv_set_once_keeps_the_first_value() {
    let r = rig();
    assert_eq!(
        r.select(
            "kv_set_once('k', 'first') AS a, kv_set_once('k', 'second') AS b, kv_get('k') AS g"
        ),
        r#"{"a":"first","b":"first","g":"first"}"#
    );
    assert_eq!(
        r.select("kv_del('k') AS d, kv_set_once('k', 'third') AS c"),
        r#"{"d":true,"c":"third"}"#
    );
}

#[test]
fn kv_incr_counts_up_and_down() {
    let r = rig();
    assert_eq!(
        r.select("kv_incr('n') AS a, kv_incr('n') AS b, kv_incr('n', 10) AS c, kv_incr('n', -15) AS d, kv_get('n') AS g"),
        r#"{"a":1,"b":2,"c":12,"d":-3,"g":-3}"#
    );
    assert_eq!(r.select("kv_incr('m', -2) AS a"), r#"{"a":-2}"#);
    // A count stays within 64 bits: the call past it fails, and the count is kept.
    assert_eq!(
        r.select("kv_incr('big', 9223372036854775807) AS a"),
        r#"{"a":9223372036854775807}"#
    );
    assert_eq!(
        r.fails("kv_incr('big') AS a"),
        "kv_incr(): refused: the counter would pass 64 bits"
    );
    assert_eq!(
        r.select("kv_get('big') AS a"),
        r#"{"a":9223372036854775807}"#
    );
}

#[test]
fn kv_max_and_kv_min_keep_the_extreme() {
    let r = rig();
    assert_eq!(
        r.select("kv_max('hi', 3) AS a, kv_max('hi', 2) AS b, kv_max('hi', 7.5) AS c, kv_max('hi', 7) AS d, kv_get('hi') AS g"),
        r#"{"a":3,"b":3,"c":7.5,"d":7.5,"g":7.5}"#
    );
    assert_eq!(
        r.select(
            "kv_min('lo', 3) AS a, kv_min('lo', 5) AS b, kv_min('lo', -1) AS c, kv_get('lo') AS g"
        ),
        r#"{"a":3,"b":3,"c":-1,"g":-1}"#
    );
    // Equal by value: the number held stays (an integer is not replaced by its float).
    assert_eq!(
        r.select("kv_max('eq', 2) AS a, kv_max('eq', 2.0) AS b"),
        r#"{"a":2,"b":2}"#
    );
}

#[test]
fn kv_add_keeps_a_set_in_term_order() {
    let r = rig();
    assert_eq!(
        r.select("kv_add('s', 'b') AS a, kv_add('s', 'a') AS b, kv_add('s', 'a') AS c, kv_add('s', 2) AS d, kv_add('s', true) AS e, kv_add('s', 1) AS f, kv_get('s') AS g"),
        r#"{"a":1,"b":2,"c":2,"d":3,"e":4,"f":5,"g":[1,2,true,"a","b"]}"#
    );
    // Elements are one when they are exactly equal: 1 and 1.0 are two.
    assert_eq!(
        r.select(
            "kv_add('n', 1) AS a, kv_add('n', 1.0) AS b, kv_add('n', 1) AS c, kv_get('n') AS g"
        ),
        r#"{"a":1,"b":2,"c":2,"g":[1,1.0]}"#
    );
    // At most 1,024 elements.
    let sql = "FOREACH payload.a INCASE kv_add('full', item) < 0 FROM \"t/#\"";
    assert_eq!(r.sql(sql, &ints(1, 1000)).unwrap(), Vec::<String>::new());
    assert_eq!(r.sql(sql, &ints(1001, 1024)).unwrap(), Vec::<String>::new());
    assert_eq!(r.select("kv_add('full', 5) AS n"), r#"{"n":1024}"#);
    assert_eq!(
        r.fails("kv_add('full', 'one more') AS n"),
        "kv_add(): refused: the set holds 1024 elements, the most a set may"
    );
}

/// D3, D4: a key has the type of the operation that created it until it is deleted;
/// another type's operation fails the rule, and `kv_get`, `kv_ttl` and `kv_del` take any.
#[test]
fn a_key_has_the_type_of_the_operation_that_created_it() {
    let r = rig();
    let created = [
        ("lww", "kv_set('KEY', 1)"),
        ("first", "kv_set_once('KEY', 1)"),
        ("sum", "kv_incr('KEY')"),
        ("max", "kv_max('KEY', 1)"),
        ("min", "kv_min('KEY', 1)"),
        ("union", "kv_add('KEY', 1)"),
    ];
    let operations = [
        ("lww", "kv_set('KEY', 2)"),
        ("lww", "kv_changed('KEY', 2)"),
        ("first", "kv_set_once('KEY', 2)"),
        ("first", "kv_held('KEY', false)"),
        ("sum", "kv_incr('KEY')"),
        ("max", "kv_max('KEY', 2)"),
        ("min", "kv_min('KEY', 2)"),
        ("union", "kv_add('KEY', 2)"),
    ];
    let mut refused = 0;
    for (held, create) in created {
        for (i, (wanted, op)) in operations.iter().enumerate() {
            let key = format!("{held}_{i}");
            r.select(&format!("{} AS v", create.replace("KEY", &key)));
            let call = format!("{} AS v", op.replace("KEY", &key));
            if held == *wanted {
                r.select(&call);
            } else {
                let e = r.fails(&call);
                let name = &op[..op.find('(').unwrap()];
                assert_eq!(
                    e,
                    format!(
                        "{name}(): wrong_type: the key holds a value of type `{held}`, and this \
                         is an operation on type `{wanted}` (kv_del deletes a key of any type)"
                    )
                );
                refused += 1;
            }
            // Reading, its expiry and deleting work on any type.
            r.select(&format!(
                "kv_get('{key}') AS g, kv_ttl('{key}') AS t, kv_del('{key}') AS d"
            ));
        }
    }
    assert_eq!(r.stats("rule:test").refused_wrong_type, refused);
    assert_eq!(r.stats("rule:test").keys, 0);
    // What `kv_set_once` stored is not the time `kv_held` records.
    r.select("kv_set_once('once', 'x') AS v");
    assert!(r
        .fails("kv_held('once', true) AS v")
        .starts_with("kv_held(): wrong_type: "));
}

/// D3: a write without a TTL gives a new key the namespace's `default_ttl` and leaves
/// an existing key's expiry as it is; a write with one sets it. A key past its expiry
/// reads as absent from that moment.
#[test]
fn a_key_expires_and_reads_as_absent_from_that_moment() {
    let r = rig();
    assert_eq!(
        r.select("kv_ttl('k') AS none, kv_set('k', 1) AS s, kv_ttl('k') AS dflt"),
        r#"{"none":"undefined","s":1,"dflt":86400}"#
    );
    assert_eq!(
        r.select("kv_set('k', 2, 600) AS s, kv_ttl('k') AS t"),
        r#"{"s":2,"t":600}"#
    );
    r.advance_secs(100);
    // Rounded up: a key that is still held never reads 0.
    assert_eq!(r.select("kv_ttl('k') AS t"), r#"{"t":500}"#);
    r.clock.fetch_add(1, Relaxed);
    assert_eq!(r.select("kv_ttl('k') AS t"), r#"{"t":500}"#);
    r.clock.fetch_add(999, Relaxed);
    assert_eq!(
        r.select("kv_set('k', 3) AS s, kv_ttl('k') AS t"),
        r#"{"s":3,"t":499}"#
    );
    assert_eq!(
        r.select("kv_set('k', 4, '10m') AS s, kv_ttl('k') AS t"),
        r#"{"s":4,"t":600}"#
    );
    r.clock.fetch_add(599_999, Relaxed);
    assert_eq!(
        r.select("kv_get('k') AS g, kv_ttl('k') AS t"),
        r#"{"g":4,"t":1}"#
    );
    r.clock.fetch_add(1, Relaxed);
    assert_eq!(
        r.select("kv_get('k') AS g, kv_ttl('k') AS t, kv_del('k') AS d"),
        r#"{"g":"undefined","t":"undefined","d":false}"#
    );
    assert_eq!(
        r.stats("rule:test").keys,
        0,
        "the call that found it removed it"
    );
    // An expired key is a new key to the next write: of any type, with a new expiry.
    r.select("kv_set('t', 1, 10) AS s");
    r.advance_secs(10);
    assert_eq!(
        r.select("kv_incr('t') AS n, kv_ttl('t') AS t"),
        r#"{"n":1,"t":86400}"#
    );
    // Every writing function takes a TTL, and reading does not move an expiry.
    for call in [
        "kv_set_once('e1', 1, 30)",
        "kv_incr('e2', 1, 30)",
        "kv_max('e3', 1, 30)",
        "kv_min('e4', 1, 30)",
        "kv_add('e5', 1, 30)",
        "kv_held('e6', true, 30)",
        "kv_changed('e7', 1, 30)",
    ] {
        let key = &call[call.find('\'').unwrap()..call.find(',').unwrap()];
        assert!(
            r.select(&format!(
                "{call} AS v, kv_get({key}) AS g, kv_ttl({key}) AS t"
            ))
            .ends_with(r#""t":30}"#),
            "{call}"
        );
    }
    // EMQX's table has no expiry.
    r.select("kv_store_put('forever', 1) AS p");
    r.advance_secs(40 * 24 * 60 * 60);
    assert_eq!(r.table("'forever'"), r#"{"v":1}"#);
    assert_eq!(r.select("kv_get('e1') AS g"), r#"{"g":"undefined"}"#);
}

/// D3's first example: "above 30 for 5 minutes". `kv_held` runs on every message the
/// `FROM` selects, records the first hot reading on a message the `WHERE` rejects, and
/// resets itself on a cool one.
#[test]
fn kv_held_measures_how_long_a_condition_has_held() {
    let r = rig();
    let set = r.load(&rules(
        "",
        &[(
            "hot",
            None,
            "SELECT clientid, payload.temp AS temp, \
             kv_held('hot:' + clientid, payload.temp > 30, 3600) AS hot_for \
             FROM \"t/#\" WHERE hot_for >= 300",
        )],
    ));
    let reading = |temp: u32| outputs(&set, "hot", &format!("{{\"temp\":{temp}}}"));
    assert_eq!(reading(25), Vec::<String>::new());
    assert_eq!(reading(31), Vec::<String>::new(), "hot for 0 seconds");
    r.advance_secs(120);
    assert_eq!(reading(35), Vec::<String>::new(), "hot for 120 seconds");
    r.advance_secs(180);
    assert_eq!(
        reading(32),
        [r#"{"clientid":"c1","temp":32,"hot_for":300}"#]
    );
    r.advance_secs(59);
    assert_eq!(
        reading(33),
        [r#"{"clientid":"c1","temp":33,"hot_for":359}"#]
    );
    // One cool reading ends the run; the next hot one starts a new one.
    assert_eq!(reading(30), Vec::<String>::new());
    assert_eq!(r.stats("rule:hot").keys, 0);
    r.advance_secs(1);
    assert_eq!(reading(40), Vec::<String>::new());
    r.advance_secs(299);
    assert_eq!(reading(40), Vec::<String>::new(), "299 seconds");
    r.advance_secs(1);
    assert_eq!(
        reading(40),
        [r#"{"clientid":"c1","temp":40,"hot_for":300}"#]
    );
    // Silence past the TTL forgets the run.
    r.advance_secs(3601);
    assert_eq!(reading(40), Vec::<String>::new());
    // The values the function itself answers.
    assert_eq!(
        r.select("kv_held('h', false) AS a, kv_held('h', true) AS b, kv_held('h', true) AS c, kv_held('h', payload.nope) AS d, kv_held('h', 1) AS e"),
        r#"{"a":"undefined","b":0,"c":0,"d":"undefined","e":"undefined"}"#
    );
}

/// D3's second example: "only when the state flips". `kv_changed` is `true` the first
/// time and whenever the value differs exactly from the one stored.
#[test]
fn kv_changed_tells_when_a_value_differs_from_the_last_one() {
    let r = rig();
    let set = r.load(&rules(
        "",
        &[(
            "flip",
            None,
            "SELECT clientid, payload.state AS state FROM \"t/#\" \
             WHERE kv_changed('state:' + clientid, payload.state)",
        )],
    ));
    let status = |state: &str| outputs(&set, "flip", &format!("{{\"state\":{state}}}")).len();
    assert_eq!(status("\"on\""), 1, "the first time");
    assert_eq!(status("\"on\""), 0);
    assert_eq!(status("\"on\""), 0);
    assert_eq!(status("\"off\""), 1);
    assert_eq!(status("\"off\""), 0);
    assert_eq!(status("\"on\""), 1);
    // Exactly: 1 and 1.0 differ, a map in another order does not.
    assert_eq!(status("1"), 1);
    assert_eq!(status("1.0"), 1);
    assert_eq!(status("1.0"), 0);
    assert_eq!(status("{\"a\":1,\"b\":2}"), 1);
    assert_eq!(status("{\"b\":2,\"a\":1}"), 0);
    assert_eq!(
        r.select("kv_changed('c', 1) AS a, kv_changed('c', 1) AS b, kv_changed('c', 2) AS c, kv_get('c') AS g"),
        r#"{"a":true,"b":false,"c":true,"g":2}"#
    );
}

// -- bounds ----------------------------------------------------------------------------

/// D9: a namespace at `max_keys` refuses a key that does not exist yet, counted by
/// reason; writes to existing keys still work, and a deleted key makes room.
#[test]
fn a_full_namespace_refuses_new_keys_and_still_updates_existing_ones() {
    let r = rig();
    let set = r.load(&rules(
        "[state.small]\nmax_keys = 3\n",
        &[(
            "w",
            Some("small"),
            "SELECT kv_set(payload.k, payload.v) AS v FROM \"t/#\"",
        )],
    ));
    let write = |k: &str, v: u32| {
        publish(&set, "t/1", &format!("{{\"k\":\"{k}\",\"v\":{v}}}"))
            .remove(0)
            .1
    };
    for k in ["a", "b", "c"] {
        assert_eq!(write(k, 1), Did::Out(vec![r#"{"v":1}"#.into()]));
    }
    assert_eq!(
        write("d", 1),
        Did::Failed(format!(
            "kv_set(): namespace_full: namespace `small` holds 3 keys and {} bytes (max_keys 3, \
             max_bytes 67108864); a new key does not fit",
            3 * (ENTRY_OVERHEAD + 2)
        ))
    );
    assert_eq!(write("a", 2), Did::Out(vec![r#"{"v":2}"#.into()]));
    assert!(matches!(write("e", 1), Did::Failed(_)));
    let stats = r.stats("small");
    assert_eq!((stats.keys, stats.refused_namespace_full), (3, 2));
    assert_eq!(stats.bytes, r.store.bytes());
    // Every other creating function is refused the same way.
    let other = r.load(&rules(
        "[state.small]\nmax_keys = 3\n",
        &[("o", Some("small"), "SELECT kv_incr('n1') AS v FROM \"a\"")],
    ));
    assert!(
        matches!(publish(&other, "a", "{}").remove(0).1, Did::Failed(e) if e.starts_with("kv_incr(): namespace_full: "))
    );
    // A deleted key makes room for one.
    let del = r.load(&rules(
        "[state.small]\nmax_keys = 3\n",
        &[("d", Some("small"), "SELECT kv_del('a') AS v FROM \"t/#\"")],
    ));
    publish(&del, "t/1", "{}");
    assert_eq!(write("d", 1), Did::Out(vec![r#"{"v":1}"#.into()]));
    assert!(matches!(write("f", 1), Did::Failed(_)));
}

/// D9: `max_bytes` bounds what a namespace's keys and values take, whichever of the
/// two bounds is reached first.
#[test]
fn a_namespace_is_bounded_in_bytes() {
    let r = rig();
    // Room for two keys of one byte with a 1,000-byte value each, not three.
    let per_key = ENTRY_OVERHEAD + 1 + 1000;
    let set = r.load(&rules(
        &format!("[state.b]\nmax_bytes = {}\n", 2 * per_key + 500),
        &[("w", Some("b"), "SELECT kv_set(payload.k, pad('', 1000, 'trailing', 'x')) AS v FROM \"t/#\" WHERE 1 = 2")],
    ));
    let write = |k: &str| {
        publish(&set, "t/1", &format!("{{\"k\":\"{k}\"}}"))
            .remove(0)
            .1
    };
    assert_eq!(write("a"), Did::Out(vec![]));
    assert_eq!(write("b"), Did::Out(vec![]));
    assert!(matches!(write("c"), Did::Failed(e) if e.starts_with("kv_set(): namespace_full: ")));
    let stats = r.stats("b");
    assert_eq!(stats.keys, 2);
    assert_eq!(stats.bytes, 2 * per_key as u64);
    assert_eq!(stats.refused_namespace_full, 1);
    // A refused key reserves nothing.
    assert_eq!(r.store.bytes(), 2 * per_key as u64);
}

/// D9: the store's ceiling (`rules.state_max_bytes`) is across namespaces: a namespace
/// with room of its own is refused a new key when the store has none.
#[test]
fn the_stores_ceiling_refuses_across_namespaces() {
    let per_key = (ENTRY_OVERHEAD + 1 + 1000) as u64;
    let r = rig_of(3 * per_key + 100);
    let set = r.load(&rules(
        "[state.one]\n[state.two]\n",
        &[
            ("a", Some("one"), "SELECT kv_set(payload.k, pad('', 1000, 'trailing', 'x')) AS v FROM \"one\" WHERE 1 = 2"),
            ("b", Some("two"), "SELECT kv_set(payload.k, pad('', 1000, 'trailing', 'x')) AS v FROM \"two\" WHERE 1 = 2"),
        ],
    ));
    let write = |ns: &str, k: &str| publish(&set, ns, &format!("{{\"k\":\"{k}\"}}")).remove(0).1;
    assert_eq!(write("one", "a"), Did::Out(vec![]));
    assert_eq!(write("one", "b"), Did::Out(vec![]));
    assert_eq!(write("two", "a"), Did::Out(vec![]));
    assert_eq!(
        write("two", "b"),
        Did::Failed(format!(
            "kv_set(): store_full: rule state holds {} bytes across its namespaces, and a new \
             key in `two` would pass the store's ceiling of {} (`rules.state_max_bytes`)",
            3 * per_key,
            3 * per_key + 100
        ))
    );
    assert!(matches!(write("one", "c"), Did::Failed(e) if e.starts_with("kv_set(): store_full: ")));
    assert_eq!(r.stats("one").refused_store_full, 1);
    assert_eq!(r.stats("two").refused_store_full, 1);
    assert_eq!(r.stats("two").refused_namespace_full, 0);
    assert_eq!(r.store.bytes(), 3 * per_key);
    // Existing keys still update.
    assert_eq!(write("two", "a"), Did::Out(vec![]));
    // A namespace's `max_bytes` cannot be declared past the ceiling, and its default
    // is capped by it.
    let e = RuleSet::parse_with(
        &format!("[state.big]\nmax_bytes = {}\n", 3 * per_key + 101),
        &r.functions,
    )
    .unwrap_err()
    .to_string();
    assert!(e.contains("expected 1 byte to the store's ceiling"), "{e}");
    let (_, settings) = set.state_namespaces().next().unwrap();
    assert_eq!(settings.max_bytes, 3 * per_key + 100);
}

/// D9, ADR 0041: over the memory watermark a key that does not exist yet is refused
/// (`brownout`); existing keys still update, read and delete.
#[test]
fn a_brownout_refuses_new_keys_and_nothing_else() {
    let r = rig();
    r.select("kv_set('old', 1) AS v, kv_incr('n') AS n, kv_store_put('p', 1) AS p");
    r.store.set_brownout(true);
    assert_eq!(
        r.fails("kv_set('new', 1) AS v"),
        "kv_set(): brownout: the broker is over its memory watermark, so namespace \
         `rule:test` takes no new key (existing keys still update and delete)"
    );
    assert!(r
        .fails("kv_incr('n2') AS v")
        .starts_with("kv_incr(): brownout: "));
    assert!(r
        .fails("kv_held('h', true) AS v")
        .starts_with("kv_held(): brownout: "));
    assert!(r
        .fails("kv_store_put('q', 1) AS v")
        .starts_with("kv_store_put(): brownout: "));
    assert_eq!(
        r.select("kv_set('old', 2) AS v, kv_incr('n') AS n, kv_get('old') AS g, kv_store_put('p', 2) AS p, kv_del('old') AS d"),
        r#"{"v":2,"n":2,"g":2,"p":true,"d":true}"#
    );
    assert_eq!(r.stats("rule:test").refused_brownout, 3);
    assert_eq!(r.stats("kv_store").refused_brownout, 1);
    r.store.set_brownout(false);
    assert_eq!(r.select("kv_set('new', 1) AS v"), r#"{"v":1}"#);
}

/// D9: with `eviction = "lru"` the least recently used key makes room, where reading
/// a key is using it.
#[test]
fn an_lru_namespace_evicts_the_least_recently_used_key() {
    let r = rig();
    let set = r.load(&rules(
        "[state.cache]\nmax_keys = 3\neviction = \"lru\"\n",
        &[
            (
                "w",
                Some("cache"),
                "SELECT kv_set(payload.k, 1) AS v FROM \"w\"",
            ),
            (
                "r",
                Some("cache"),
                "SELECT kv_get(payload.k) AS v FROM \"r\"",
            ),
        ],
    ));
    let write = |k: &str| {
        assert_eq!(
            publish(&set, "w", &format!("{{\"k\":\"{k}\"}}"))
                .remove(0)
                .1,
            Did::Out(vec![r#"{"v":1}"#.into()])
        );
    };
    let read = |k: &str| match publish(&set, "r", &format!("{{\"k\":\"{k}\"}}"))
        .remove(0)
        .1
    {
        Did::Out(out) => out[0] == r#"{"v":1}"#,
        Did::Failed(e) => panic!("{e}"),
    };
    write("a");
    write("b");
    write("c");
    assert!(read("a"), "used: now b is the oldest");
    write("d");
    assert_eq!(r.stats("cache").keys, 3);
    assert_eq!(r.stats("cache").evicted, 1);
    assert!(!read("b"), "the least recently used key went");
    assert!(read("a") && read("c") && read("d"));
    // An update is a use, too.
    write("a");
    write("e");
    assert!(!read("c"));
    assert!(read("a") && read("d") && read("e"));
    assert_eq!(r.stats("cache").refused_namespace_full, 0);
    assert_eq!(r.stats("cache").evicted, 2);
    // Many keys, over every shard: the namespace never holds more than its bound, and
    // the survivors are the last ones written.
    let fill = r.load(&rules(
        "[state.cache]\nmax_keys = 3\neviction = \"lru\"\n",
        &[(
            "f",
            Some("cache"),
            "FOREACH payload.a DO kv_set(str(item), 1) AS v FROM \"f\"",
        )],
    ));
    assert!(
        matches!(publish(&fill, "f", &ints(1, 250)).remove(0).1, Did::Out(out) if out.len() == 250)
    );
    assert_eq!(r.stats("cache").keys, 3);
    assert!(read("248") && read("249") && read("250"));
    assert!(!read("247") && !read("e"));
}

/// An `lru` namespace bounded in bytes evicts as many keys as a large value needs.
#[test]
fn an_lru_namespace_evicts_for_bytes_too() {
    let r = rig();
    let small = ENTRY_OVERHEAD + 2 + 10;
    let set = r.load(&rules(
        &format!("[state.cache]\nmax_bytes = {}\neviction = \"lru\"\n", 10 * small),
        &[
            ("s", Some("cache"), "FOREACH payload.a DO kv_set(str(item), pad('', 10, 'trailing', 'x')) AS v FROM \"s\""),
            ("b", Some("cache"), "SELECT kv_set('bb', pad('', 300, 'trailing', 'x')) AS v FROM \"b\" WHERE 1 = 2"),
        ],
    ));
    assert!(
        matches!(publish(&set, "s", &ints(10, 19)).remove(0).1, Did::Out(out) if out.len() == 10)
    );
    assert_eq!(r.stats("cache").keys, 10);
    assert_eq!(publish(&set, "b", "{}").remove(0).1, Did::Out(vec![]));
    // The large value is 290 bytes more than a small one: four small keys make room.
    let stats = r.stats("cache");
    assert!(stats.bytes <= (10 * small) as u64);
    assert_eq!(stats.evicted, 4, "{stats:?}");
    assert_eq!(stats.keys, 7);
}

/// Expired keys nobody reads again are room when room is needed (the sweep that
/// removes them on time is D7's).
#[test]
fn expired_keys_make_room_when_a_namespace_is_full() {
    let r = rig();
    let set = r.load(&rules(
        "[state.s]\nmax_keys = 200\ndefault_ttl = \"1m\"\n",
        &[
            (
                "fill",
                Some("s"),
                "FOREACH payload.a DO kv_set(str(item), 1) AS v FROM \"fill\"",
            ),
            (
                "more",
                Some("s"),
                "FOREACH payload.a DO kv_set(str(item), 1) AS v FROM \"more\"",
            ),
        ],
    ));
    assert!(
        matches!(publish(&set, "fill", &ints(1, 200)).remove(0).1, Did::Out(out) if out.len() == 200)
    );
    assert!(
        matches!(publish(&set, "more", &ints(1000, 1199)).remove(0).1, Did::Failed(e) if e.contains("namespace_full"))
    );
    r.advance_secs(61);
    assert!(
        matches!(publish(&set, "more", &ints(1000, 1199)).remove(0).1, Did::Out(out) if out.len() == 200)
    );
    assert_eq!(r.stats("s").keys, 200);
}

// -- a namespace's life ----------------------------------------------------------------

/// D1: across a reload a namespace whose declaration did not change keeps its keys;
/// changed bounds apply at once, and lowering `max_keys` evicts nothing; a namespace
/// whose declaration is gone, and the namespace of a rule that is gone or renamed, is
/// dropped with its keys; a rule edited under the same id keeps its keys.
#[test]
#[allow(clippy::too_many_lines)] // one reload after another, on one store
fn across_a_reload_a_namespace_keeps_changes_or_drops_its_keys() {
    let r = rig();
    let write = |id: &'static str, state: Option<&'static str>, topic: &'static str| {
        (
            id,
            state,
            format!("SELECT kv_set(payload.k, 1) AS v, kv_incr('count') AS n FROM \"{topic}\""),
        )
    };
    let file = |tables: &str, list: &[(&'static str, Option<&'static str>, String)]| {
        let list: Vec<(&str, Option<&str>, &str)> = list
            .iter()
            .map(|(id, state, sql)| (*id, *state, &**sql))
            .collect();
        rules(tables, &list)
    };
    let v1 = r.load(&file(
        "[state.kept]\nmax_keys = 10\n[state.gone]\n",
        &[
            write("kept_rule", Some("kept"), "kept"),
            write("gone_ns", Some("gone"), "gone"),
            write("own", None, "own"),
            write("renamed", None, "renamed"),
        ],
    ));
    for topic in ["kept", "gone", "own", "renamed"] {
        for k in ["a", "b", "c"] {
            let did = publish(&v1, topic, &format!("{{\"k\":\"{k}\"}}"))
                .remove(0)
                .1;
            assert!(matches!(did, Did::Out(_)), "{topic}: {did:?}");
        }
    }
    assert_eq!(
        r.store.namespaces(),
        [
            "gone",
            "kept",
            "kv_store",
            "rule:own",
            "rule:renamed",
            "rule:test"
        ]
    );
    let before = r.store.bytes();
    // The reload: `kept` with a lower bound, `gone` no longer declared, `own` edited
    // under its id, `renamed` under a new id.
    let v2 = r.load(&file(
        "[state.kept]\nmax_keys = 2\n",
        &[
            write("kept_rule", Some("kept"), "kept"),
            (
                "own",
                None,
                "SELECT kv_get('count') AS n, kv_get('a') AS a FROM \"own\"".to_string(),
            ),
            write("renamed2", None, "renamed"),
        ],
    ));
    // While both sets are held (the new one is validated before the swap), nothing is
    // dropped.
    assert_eq!(r.stats("gone").keys, 4);
    drop(v1);
    // Once the new set is the only one: what it does not hold is gone, with its bytes.
    assert_eq!(
        r.store.namespaces(),
        ["kept", "kv_store", "rule:own", "rule:renamed2", "rule:test"]
    );
    assert!(r.store.stats("gone").is_none());
    assert!(r.store.stats("rule:renamed").is_none());
    assert!(r.store.bytes() < before);
    assert_eq!(
        r.store.bytes(),
        r.store
            .namespaces()
            .iter()
            .map(|n| r.stats(n).bytes)
            .sum::<u64>()
    );
    // `kept` kept its four keys under the lower bound: nothing was evicted, existing
    // keys update, and a new key is refused until the count is below the bound.
    assert_eq!(r.stats("kept").keys, 4);
    let kept = |k: &str| {
        publish(&v2, "kept", &format!("{{\"k\":\"{k}\"}}"))
            .remove(0)
            .1
    };
    assert_eq!(kept("a"), Did::Out(vec![r#"{"v":1,"n":4}"#.into()]));
    assert!(
        matches!(kept("z"), Did::Failed(e) if e.contains("namespace_full") && e.contains("holds 4 keys") && e.contains("max_keys 2"))
    );
    // The rule edited under its id reads what it wrote before the reload.
    assert_eq!(
        publish(&v2, "own", "{}").remove(0).1,
        Did::Out(vec![r#"{"n":3,"a":1}"#.into()])
    );
    // The renamed rule starts empty.
    assert_eq!(
        publish(&v2, "renamed", r#"{"k":"a"}"#).remove(0).1,
        Did::Out(vec![r#"{"v":1,"n":1}"#.into()])
    );
    // A namespace declared again later is a new one.
    let v3 = r.load(&file(
        "[state.kept]\n[state.gone]\n",
        &[write("gone_ns", Some("gone"), "gone")],
    ));
    drop(v2);
    assert_eq!(r.stats("gone").keys, 0);
    assert_eq!(r.stats("kept").keys, 4, "declared in every set since");
    assert_eq!(
        publish(&v3, "gone", r#"{"k":"a"}"#).remove(0).1,
        Did::Out(vec![r#"{"v":1,"n":1}"#.into()])
    );
}

/// A rules file that fails to load after it opened namespaces leaves nothing behind,
/// and the running rules' keys as they were.
#[test]
fn a_load_that_fails_leaves_the_store_as_it_was() {
    let r = rig();
    let running = r.load(&rules(
        "[state.s]\n",
        &[("a", Some("s"), "SELECT kv_incr('n') AS n FROM \"t/#\"")],
    ));
    publish(&running, "t/1", "{}");
    let broken = rules(
        "[state.s]\nmax_keys = 1\n[state.fresh]\n",
        &[
            ("a", Some("s"), "SELECT kv_incr('n') AS n FROM \"t/#\""),
            ("b", None, "SELECT nope() FROM \"t/#\""),
        ],
    );
    assert!(RuleSet::parse_with(&broken, &r.functions).is_err());
    assert_eq!(r.store.namespaces(), ["kv_store", "rule:test", "s"]);
    assert_eq!(
        publish(&running, "t/1", "{}").remove(0).1,
        Did::Out(vec![r#"{"n":2}"#.into()])
    );
}

/// D11: a scratch store is another store. What a dry run writes there is not in the
/// running one, and what the running one holds is not read.
#[test]
fn a_scratch_store_shares_nothing_with_the_running_one() {
    let r = rig();
    let set = r.load(&rules(
        "[state.s]\nmax_keys = 2\n",
        &[("a", Some("s"), "SELECT kv_incr('n') AS n, kv_store_put('p', 1) AS p, kv_store_get('seen', 'no') AS seen FROM \"t/#\"")],
    ));
    publish(&set, "t/1", "{}");
    r.select("kv_store_put('seen', 'yes') AS p");
    let before = (r.stats("s"), r.stats("kv_store"), r.store.bytes());
    let scratch = Store::scratch(Some(&r.store));
    assert_eq!(scratch.max_bytes(), r.store.max_bytes());
    let payload = Bytes::from_static(b"{}");
    let props = mqtt_core::AppProperties::default();
    let input = PublishInput::new("c1", "t/1", &payload, 1, &props);
    for expected in [
        r#"{"n":1,"p":true,"seen":"no"}"#,
        r#"{"n":2,"p":true,"seen":"no"}"#,
    ] {
        let mut out = Vec::new();
        set.evaluate_one("a", &input, Some(&*scratch), &mut |_, _| {}, &mut out);
        assert!(
            matches!(&out[0].1, Effect::Console(line) if line == expected),
            "{out:?}"
        );
    }
    assert_eq!((r.stats("s"), r.stats("kv_store"), r.store.bytes()), before);
    assert_eq!(
        publish(&set, "t/1", "{}").remove(0).1,
        Did::Out(vec![r#"{"n":2,"p":true,"seen":"yes"}"#.into()])
    );
    // The scratch store holds what the dry run wrote until it is dropped, so each
    // rule of a dry run sees the ones before it.
    assert_eq!(scratch.stats("s").unwrap().keys, 1);
    assert_eq!(scratch.namespaces(), ["kv_store", "s"]);
    // A store of its own for every command that checks or tries rules.
    let (a, b) = (
        crate::rules::functions_from_config(&mqtt_config::Rules::default()),
        crate::rules::functions_from_config(&mqtt_config::Rules::default()),
    );
    let sql = "SELECT kv_incr('n') AS n FROM \"t/#\"";
    assert_eq!(
        mqtt_rules::test_sql_with(sql, &input, &a).unwrap(),
        [r#"{"n":1}"#]
    );
    assert_eq!(
        mqtt_rules::test_sql_with(sql, &input, &b).unwrap(),
        [r#"{"n":1}"#]
    );
    // And none at all where the ceiling is 0: the file is refused, naming the key.
    let off = crate::rules::functions_from_config(&mqtt_config::Rules {
        state_max_bytes: 0,
        ..mqtt_config::Rules::default()
    });
    assert_eq!(
        mqtt_rules::test_sql_with(sql, &input, &off).unwrap_err(),
        "rule state is switched off: `rules.state_max_bytes` is 0 (set it above 0 for rules to \
         keep state; see RULES.md) (line 1, column 8, near `kv_incr('n') AS n FROM \"`)"
    );
}

/// A binary cut out of a payload shares the payload's buffer; the store keeps a copy,
/// so a stored value never holds a whole message in memory.
#[test]
fn a_stored_binary_does_not_keep_its_payload_alive() {
    let store = Store::new(1 << 20);
    let ns = store
        .open(
            "n",
            &mqtt_rules::state::NamespaceSettings::defaults(1 << 20),
        )
        .unwrap();
    let payload = Bytes::from(vec![0xff_u8; 4096]);
    let cut = mqtt_rules::Value::Bin(payload.slice(0..8));
    let nested = mqtt_rules::Value::from(vec![cut.clone(), mqtt_rules::Value::Int(1)]);
    for (key, value) in [(&b"a"[..], &cut), (&b"b"[..], &nested)] {
        ns.call(&StateOp::Set {
            key,
            value: stored(value),
            ttl: None,
        })
        .unwrap();
    }
    drop((cut, nested));
    assert!(
        payload.is_unique(),
        "the store holds no view of the payload"
    );
    let got = ns.call(&StateOp::Get { key: b"a" }).unwrap();
    assert_eq!(got.value.unwrap().as_bytes(), Some(&[0xff_u8; 8][..]));
    assert_eq!(got.read_bytes, 8);
}

// -- many callers at once --------------------------------------------------------------

/// Many connection tasks on several worker threads share one store: counts are exact,
/// and a bound holds exactly, whatever the interleaving.
#[test]
fn concurrent_callers_keep_counts_and_bounds_exact() {
    const THREADS: usize = 8;
    const EACH: usize = 5_000;
    let store = Store::new(64 << 20);
    let settings = mqtt_rules::state::NamespaceSettings {
        max_keys: 1_000,
        ..mqtt_rules::state::NamespaceSettings::defaults(64 << 20)
    };
    let ns = store.open("n", &settings).unwrap();
    std::thread::scope(|scope| {
        for t in 0..THREADS {
            let ns = ns.clone();
            scope.spawn(move || {
                for i in 0..EACH {
                    // One hot key, counted by everyone.
                    ns.call(&StateOp::Incr {
                        key: b"hot",
                        by: 1,
                        ttl: None,
                    })
                    .unwrap();
                    // And keys of its own, more than the namespace may hold together.
                    let key = format!("{t}:{i}");
                    let _ = ns.call(&StateOp::Incr {
                        key: key.as_bytes(),
                        by: 1,
                        ttl: None,
                    });
                }
            });
        }
    });
    let hot = ns
        .call(&StateOp::Get { key: b"hot" })
        .unwrap()
        .value
        .unwrap();
    assert_eq!(hot.to_json().unwrap(), (THREADS * EACH).to_string());
    let stats = store.stats("n").unwrap();
    assert_eq!(stats.keys, 1_000, "full to the bound and not past it");
    assert_eq!(
        stats.refused_namespace_full,
        (THREADS * EACH + 1 - 1_000) as u64
    );
    assert_eq!(stats.bytes, store.bytes());
}

/// The same under eviction: an `lru` namespace hammered from every thread ends at its
/// bound, with its accounting whole.
#[test]
fn concurrent_callers_of_an_lru_namespace_stay_within_its_bound() {
    const THREADS: usize = 8;
    const EACH: usize = 2_000;
    let store = Store::new(64 << 20);
    let settings = mqtt_rules::state::NamespaceSettings {
        max_keys: 100,
        eviction: mqtt_rules::state::Eviction::Lru,
        ..mqtt_rules::state::NamespaceSettings::defaults(64 << 20)
    };
    let ns = store.open("n", &settings).unwrap();
    let one = mqtt_rules::Value::Int(1);
    std::thread::scope(|scope| {
        for t in 0..THREADS {
            let (ns, one, store) = (ns.clone(), &one, &store);
            scope.spawn(move || {
                for i in 0..EACH {
                    let key = format!("{t}:{i}");
                    ns.call(&StateOp::Set {
                        key: key.as_bytes(),
                        value: stored(one),
                        ttl: None,
                    })
                    .unwrap();
                    assert!(store.stats("n").unwrap().keys <= 100);
                }
            });
        }
    });
    let stats = store.stats("n").unwrap();
    assert_eq!(stats.keys, 100);
    assert_eq!(stats.evicted, (THREADS * EACH - 100) as u64);
    assert_eq!(stats.bytes, store.bytes());
    drop(ns);
    assert_eq!(store.bytes(), 0, "a dropped namespace gives its bytes back");
    assert_eq!(store.namespaces(), Vec::<String>::new());
}

// -- sketches, and the types that merge (T2) -------------------------------------------

/// D3: `kv_hll_add` answers the estimate after, `kv_hll_count` the estimate or `0`,
/// and `kv_get` reads a sketch as its estimate. Two values are one when they are
/// exactly equal, as in a set.
#[test]
fn kv_hll_add_and_kv_hll_count_estimate_distinct_values() {
    let rig = rig();
    assert_eq!(rig.select("kv_hll_count('d') AS v"), r#"{"v":0}"#);
    assert_eq!(rig.select("kv_hll_add('d', 1) AS v"), r#"{"v":1}"#);
    assert_eq!(rig.select("kv_hll_add('d', 1) AS v"), r#"{"v":1}"#);
    // `1.0` is not `1`, and `'1'` is neither.
    assert_eq!(rig.select("kv_hll_add('d', 1.0) AS v"), r#"{"v":2}"#);
    assert_eq!(rig.select("kv_hll_add('d', '1') AS v"), r#"{"v":3}"#);
    // A map is the same value whatever order its keys are in.
    assert_eq!(
        rig.select_on(
            "kv_hll_add('d', json_decode(payload)) AS v",
            r#"{"a":1,"b":2}"#
        ),
        r#"{"v":4}"#
    );
    assert_eq!(
        rig.select_on(
            "kv_hll_add('d', json_decode(payload)) AS v",
            r#"{"b":2,"a":1}"#
        ),
        r#"{"v":4}"#
    );
    assert_eq!(
        rig.select("kv_hll_count('d') AS c, kv_get('d') AS g"),
        r#"{"c":4,"g":4}"#
    );
    assert_eq!(rig.select("kv_del('d') AS v"), r#"{"v":true}"#);
    assert_eq!(
        rig.select("kv_hll_count('d') AS c, kv_get('d') AS g"),
        r#"{"c":0,"g":"undefined"}"#
    );
    // `undefined` is no value, here as anywhere.
    assert!(rig
        .fails("kv_hll_add('d', payload.missing) AS v")
        .contains("bad_value"));
}

/// D4, D9: 2^14 registers, a standard error of about 0.8%, and never more than 12 KiB
/// of registers however many values a publisher sends.
#[test]
fn a_sketch_counts_within_its_error_and_within_12_kib() {
    let store = Store::new(1 << 20);
    let ns = store
        .open(
            "n",
            &mqtt_rules::state::NamespaceSettings::defaults(1 << 20),
        )
        .unwrap();
    let mut last_bytes = 0;
    for i in 0..60_000u32 {
        let value = mqtt_rules::Value::from(format!("device-{i}"));
        let hash = mqtt_rules::crdt::stable_hash(&mqtt_rules::state::term_key(&value));
        let reply = ns
            .call(&StateOp::HllAdd {
                key: b"devices",
                hash,
                ttl: None,
            })
            .unwrap();
        if [100, 1_000, 10_000, 60_000].contains(&(i + 1)) {
            let Some(mqtt_rules::Value::Int(estimate)) = reply.value else {
                panic!("an estimate");
            };
            let off = (estimate - i64::from(i + 1)).abs();
            assert!(
                off * 40 <= i64::from(i + 1),
                "{} estimated {estimate}",
                i + 1
            );
        }
        // What it is accounted only grows, and stops at the registers' 12 KiB.
        let bytes = store.stats("n").unwrap().bytes;
        assert!(bytes >= last_bytes);
        last_bytes = bytes;
    }
    let most = ENTRY_OVERHEAD + b"devices".len() + 128 + 12 * 1024;
    assert_eq!(last_bytes, u64::try_from(most).unwrap());
    assert_eq!(store.bytes(), last_bytes);
    // A read is charged as the number it answers, not as the registers.
    let got = ns.call(&StateOp::Get { key: b"devices" }).unwrap();
    assert_eq!(got.read_bytes, 8);
    let counted = ns.call(&StateOp::HllCount { key: b"devices" }).unwrap();
    assert_eq!(counted.read_bytes, 0);
    assert!(got.value.unwrap().exact_eq(&counted.value.unwrap()));
    // Deleted, it gives everything back.
    ns.call(&StateOp::Del { key: b"devices" }).unwrap();
    assert_eq!(store.bytes(), 0);
}

/// D3: a sketch is a type like the others, and expires like them.
#[test]
fn a_sketch_is_a_type_of_its_own_and_expires() {
    let rig = rig();
    rig.select("kv_hll_add('seen', 'a', 10) AS v, kv_incr('n') AS n");
    for call in ["kv_incr('seen')", "kv_set('seen', 1)", "kv_add('seen', 1)"] {
        let e = rig.fails(&format!("{call} AS v"));
        assert!(
            e.contains("wrong_type: the key holds a value of type `hll`"),
            "{e}"
        );
    }
    for call in ["kv_hll_add('n', 'a')", "kv_hll_count('n')"] {
        let e = rig.fails(&format!("{call} AS v"));
        assert!(
            e.contains(
                "wrong_type: the key holds a value of type `sum`, and this is an operation on \
                 type `hll`"
            ),
            "{e}"
        );
    }
    assert_eq!(rig.stats("rule:test").refused_wrong_type, 5);
    assert_eq!(rig.select("kv_ttl('seen') AS v"), r#"{"v":10}"#);
    rig.advance_secs(9);
    // A call without a TTL leaves the expiry; one with a TTL sets it.
    assert_eq!(rig.select("kv_hll_add('seen', 'b') AS v"), r#"{"v":2}"#);
    assert_eq!(rig.select("kv_ttl('seen') AS v"), r#"{"v":1}"#);
    rig.advance_secs(1);
    assert_eq!(rig.select("kv_hll_count('seen') AS v"), r#"{"v":0}"#);
    assert_eq!(rig.select("kv_hll_add('seen', 'c', 5) AS v"), r#"{"v":1}"#);
    assert_eq!(rig.select("kv_ttl('seen') AS v"), r#"{"v":5}"#);
}

/// D4: a set reads in one order whatever order its elements were added in, so that
/// two nodes' copies of it can be the same. Erlang's term order leaves two maps of one
/// size undecided; their identities decide.
#[test]
fn a_set_reads_in_one_order_whatever_order_it_was_built_in() {
    let (forwards, backwards) = (rig(), rig());
    let elements = [
        r#"{"x":1}"#,
        r#"{"y":1}"#,
        r#"{"a":[]}"#,
        r#"{"x":2}"#,
        r#"[{"k":1}]"#,
        r#"[{"j":1}]"#,
    ];
    for element in elements {
        forwards.select_on("kv_add('s', json_decode(payload)) AS n", element);
    }
    for element in elements.iter().rev() {
        backwards.select_on("kv_add('s', json_decode(payload)) AS n", element);
    }
    let read = forwards.select("kv_get('s') AS v");
    assert_eq!(read, backwards.select("kv_get('s') AS v"));
    assert_eq!(read.matches('{').count(), 7, "{read}");
}

/// T1 pinned a count at 64 bits, and it still is: each step is refused where it would
/// pass them, in either direction, though the counter now keeps what it added and what
/// it took away apart.
#[test]
fn a_counter_goes_up_and_down_by_the_largest_step_without_end() {
    let store = Store::new(1 << 20);
    let ns = store
        .open(
            "n",
            &mqtt_rules::state::NamespaceSettings::defaults(1 << 20),
        )
        .unwrap();
    let incr = |by: i64| {
        ns.call(&StateOp::Incr {
            key: b"n",
            by,
            ttl: None,
        })
    };
    for _ in 0..10_000 {
        assert!(incr(i64::MAX)
            .unwrap()
            .value
            .unwrap()
            .exact_eq(&i64::MAX.into()));
        assert!(incr(i64::MIN)
            .unwrap()
            .value
            .unwrap()
            .exact_eq(&(-1).into()));
        assert!(incr(1).unwrap().value.unwrap().exact_eq(&0.into()));
    }
    incr(i64::MIN).unwrap();
    assert_eq!(
        incr(-1).unwrap_err().message,
        "the counter would pass 64 bits"
    );
    incr(i64::MAX).unwrap();
    incr(i64::MAX).unwrap();
    assert_eq!(
        incr(2).unwrap_err().message,
        "the counter would pass 64 bits"
    );
    assert!(incr(1).unwrap().value.unwrap().exact_eq(&i64::MAX.into()));
    assert_eq!(store.stats("n").unwrap().refused_other, 2);
}
