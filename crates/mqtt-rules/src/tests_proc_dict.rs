//! EMQX's `proc_dict_put/2`, `proc_dict_get/1` and `proc_dict_del/1` (ADR 0085 D13).
//!
//! Every expectation of what the three functions answer is what EMQX 6.3.1 answered
//! (`emqx/emqx:latest`, 2026-10-11): `emqx_rule_funcs` called directly, statements run
//! through `emqx_rule_sqltester:test/1` in one process (its dictionary read back
//! afterwards), and the SQL test over the REST API. A comment names the probe (`d..`
//! direct, `s..` SQL, `R..` REST). EMQX's `{ok, Map}` is the output's JSON here,
//! `{error, nomatch}` no output, and an error a failed rule. The atom `undefined` is
//! written `"undefined"`, as the rule engine writes it everywhere.
//!
//! Which process's dictionary a rule works on in EMQX, for each way a rule is
//! triggered, was probed with clients on a running node; those results are pinned
//! where the broker's tasks are (`mqttd`, `tests/rules_proc_dict.rs`). Here the
//! dictionary is whatever the evaluation is handed.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use super::*;
use crate::state::{
    Dictionary, NamespaceSettings, StateError, StateNamespace, StateStore, MAX_DICT_BYTES,
    MAX_DICT_KEYS, STORE_FULL,
};

/// A store with a ceiling and no namespaces: all a dictionary asks of one.
#[derive(Debug)]
struct Ceiling {
    max: u64,
    bytes: AtomicU64,
}

impl StateStore for Ceiling {
    fn max_bytes(&self) -> u64 {
        self.max
    }

    fn open(&self, name: &str, _: &NamespaceSettings) -> Result<Arc<dyn StateNamespace>, String> {
        Err(format!("this test's store has no namespace `{name}`"))
    }

    fn dictionary_grow(&self, bytes: u64) -> Result<(), StateError> {
        self.bytes
            .fetch_update(Relaxed, Relaxed, |held| {
                held.checked_add(bytes).filter(|after| *after <= self.max)
            })
            .map(|_| ())
            .map_err(|_| StateError {
                reason: STORE_FULL,
                message: "the test's ceiling".to_string(),
            })
    }

    fn dictionary_shrink(&self, bytes: u64) {
        self.bytes.fetch_sub(bytes, Relaxed);
    }
}

/// One evaluating task: its dictionary, counted in a store.
struct Task {
    store: Arc<Ceiling>,
    functions: Functions,
    dictionary: Dictionary,
}

fn task() -> Task {
    task_under(1 << 20)
}

fn task_under(max: u64) -> Task {
    let store = Arc::new(Ceiling {
        max,
        bytes: AtomicU64::new(0),
    });
    Task {
        functions: Functions::default().with_state(store.clone()),
        dictionary: Dictionary::counted_in(store.clone()),
        store,
    }
}

fn load(functions: &Functions, sql: &str) -> RuleSet {
    let text =
        format!("[rules.test]\nsql = '''{sql}'''\nactions = [{{ function = \"console\" }}]\n");
    RuleSet::parse_with(&text, functions)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .rules
}

/// One message through `set`: its outputs, or why the rule failed. `dictionary` is
/// the evaluating task's; without one the evaluation has its own.
fn publish(
    set: &RuleSet,
    dictionary: Option<&Dictionary>,
    payload: &str,
) -> Result<Vec<String>, String> {
    let payload = Bytes::from(payload.to_string());
    let props = mqtt_core::AppProperties::default();
    let input = PublishInput::new("c1", "t/1", &payload, 1, &props);
    let budget = Budget::new(payload.len());
    let mut failed = None;
    let mut out = Vec::new();
    let mut report = |_: &Rule, outcome: Outcome<'_>| {
        if let Outcome::Failed(e) = outcome {
            failed = Some(e.to_string());
        }
    };
    match dictionary {
        Some(dictionary) => set.on_publish_in(&input, &budget, dictionary, &mut report, &mut out),
        None => set.on_publish_within(&input, &budget, &mut report, &mut out),
    }
    match failed {
        Some(e) => Err(e),
        None => Ok(out
            .into_iter()
            .filter_map(|(_, effect)| match effect {
                Effect::Console(line) => Some(line),
                Effect::Republish(_) => None,
            })
            .collect()),
    }
}

impl Task {
    /// One statement on one message, evaluated by this task: EMQX's SQL test, run
    /// again and again in one process.
    fn sql(&self, sql: &str, payload: &str) -> Result<Vec<String>, String> {
        publish(&load(&self.functions, sql), Some(&self.dictionary), payload)
    }

    fn select_on(&self, fields: &str, payload: &str) -> String {
        let sql = format!("SELECT {fields} FROM \"t/#\"");
        let mut out = self
            .sql(&sql, payload)
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(out.len(), 1, "{sql}: {out:?}");
        out.remove(0)
    }

    /// The one output of `SELECT <fields>` on a `{}` message.
    fn select(&self, fields: &str) -> String {
        self.select_on(fields, "{}")
    }

    fn fails(&self, fields: &str, payload: &str) -> String {
        let sql = format!("SELECT {fields} FROM \"t/#\"");
        self.sql(&sql, payload).expect_err(&sql)
    }

    /// What the dictionary holds under `key` (an expression), as `proc_dict_get`
    /// reads it.
    fn held(&self, key: &str) -> String {
        self.select(&format!("proc_dict_get({key}) AS v"))
    }
}

const NOT_HELD: &str = r#"{"v":"undefined"}"#;

/// D13: `put` and `del` answer the value held before or `undefined`, `get` the value
/// or `undefined`.
#[test]
fn put_and_del_answer_the_value_held_before_and_get_the_value() {
    let t = task();
    // d01..d08: undefined, 1, 2, undefined, 2, undefined, undefined, undefined.
    assert_eq!(t.select("proc_dict_put('k', 1) AS v"), NOT_HELD);
    assert_eq!(t.select("proc_dict_put('k', 2) AS v"), r#"{"v":1}"#);
    assert_eq!(t.held("'k'"), r#"{"v":2}"#);
    assert_eq!(t.held("'missing'"), NOT_HELD);
    assert_eq!(t.select("proc_dict_del('k') AS v"), r#"{"v":2}"#);
    assert_eq!(t.select("proc_dict_del('k') AS v"), NOT_HELD);
    assert_eq!(t.held("'k'"), NOT_HELD);
    assert_eq!(t.select("proc_dict_del('never') AS v"), NOT_HELD);
    // s01: all of it in one statement, each field seeing the one before.
    assert_eq!(
        t.select(
            "proc_dict_put('a', 1) AS p1, proc_dict_put('a', 2) AS p2, proc_dict_get('a') AS g, \
             proc_dict_del('a') AS d, proc_dict_del('a') AS d2, proc_dict_get('a') AS g2"
        ),
        r#"{"p1":"undefined","p2":1,"g":2,"d":2,"d2":"undefined","g2":"undefined"}"#
    );
    assert!(t.dictionary.is_empty());
    assert_eq!(t.dictionary.bytes(), 0);
}

/// D13: a key is any value, matched exactly, as the process dictionary matches
/// `{'@rule_engine', Key}` (`=:=`).
#[test]
fn a_key_is_the_exact_term() {
    let t = task();
    // d09, d10, s18: 1, 1.0 and '1' are three keys.
    assert_eq!(
        t.select(
            "proc_dict_put(1, 'int') AS a, proc_dict_put(1.0, 'float') AS b, \
             proc_dict_put('1', 'str') AS c, proc_dict_get(1) AS g1, proc_dict_get(1.0) AS g2, \
             proc_dict_get('1') AS g3"
        ),
        r#"{"a":"undefined","b":"undefined","c":"undefined","g1":"int","g2":"float","g3":"str"}"#
    );
    // d11: deleting 1.0 leaves 1.
    assert_eq!(
        t.select("proc_dict_del(1.0) AS d, proc_dict_get(1) AS i, proc_dict_get(1.0) AS f"),
        r#"{"d":"float","i":"int","f":"undefined"}"#
    );
    // d12, s19: a map is the same key whatever order it was written in, and another
    // with 1.0 where it has 1. d13: a list is its elements, in order.
    assert_eq!(
        t.select_on(
            "proc_dict_put(payload.m, 'map') AS a, proc_dict_get(payload.m2) AS same, \
             proc_dict_get(payload.m3) AS other, proc_dict_put(payload.l, 'list') AS b, \
             proc_dict_get(payload.l) AS l, proc_dict_get(payload.l2) AS l2, \
             proc_dict_get(payload.l3) AS l3",
            r#"{"m":{"a":1,"b":2},"m2":{"b":2,"a":1},"m3":{"a":1.0,"b":2},"l":[1,"x"],"l2":[1.0,"x"],"l3":["x",1]}"#
        ),
        r#"{"a":"undefined","same":"map","other":"undefined","b":"undefined","l":"list","l2":"undefined","l3":"undefined"}"#
    );
    // d29: all the way down.
    assert_eq!(
        t.select_on(
            "proc_dict_put(payload.a, 'n') AS p, proc_dict_get(payload.a) AS same, \
             proc_dict_get(payload.b) AS other",
            r#"{"a":{"a":{"b":[1]}},"b":{"a":{"b":[1.0]}}}"#
        ),
        r#"{"p":"undefined","same":"n","other":"undefined"}"#
    );
    // d16, d23: `true` is not the string, and `false` is a key and a value.
    assert_eq!(
        t.select(
            "proc_dict_put(true, 1) AS p, proc_dict_get('true') AS s, proc_dict_get(true) AS b, \
             proc_dict_put(false, false) AS p2, proc_dict_get(false) AS f, \
             proc_dict_del(false) AS d"
        ),
        r#"{"p":"undefined","s":"undefined","b":1,"p2":"undefined","f":false,"d":false}"#
    );
    // d17, s21: `undefined` (a field that is not there) is a key, and not the string.
    assert_eq!(
        t.select(
            "proc_dict_put(payload.nope, 'x') AS p1, proc_dict_get(payload.nope) AS g, \
             proc_dict_get('undefined') AS s, proc_dict_del(payload.nope) AS d, \
             proc_dict_get(payload.nope) AS g2"
        ),
        r#"{"p1":"undefined","g":"x","s":"undefined","d":"x","g2":"undefined"}"#
    );
    // d18: so is `null`. d19: and the empty string.
    assert_eq!(
        t.select_on(
            "proc_dict_put(payload.n, 1) AS p, proc_dict_get(payload.n) AS g, \
             proc_dict_get('null') AS s, proc_dict_put('', 2) AS e, proc_dict_get('') AS ge",
            r#"{"n":null}"#
        ),
        r#"{"p":"undefined","g":1,"s":"undefined","e":"undefined","ge":2}"#
    );
    // d20: 0, 0.0 and -0.0 are three keys (OTP's `=:=` since 27).
    assert_eq!(
        t.select(
            "proc_dict_put(0, 'i') AS a, proc_dict_put(0.0, 'pz') AS b, \
             proc_dict_put(-0.0, 'nz') AS c, proc_dict_get(0) AS g1, proc_dict_get(0.0) AS g2, \
             proc_dict_get(-0.0) AS g3"
        ),
        r#"{"a":"undefined","b":"undefined","c":"undefined","g1":"i","g2":"pz","g3":"nz"}"#
    );
    // d21: an integer past 64 bits is a key, and not the float of the same value.
    assert_eq!(
        t.select(
            "proc_dict_put(18446744073709551616, 'big') AS p, \
             proc_dict_get(18446744073709551616) AS g, \
             proc_dict_get(1.8446744073709552e19) AS f"
        ),
        r#"{"p":"undefined","g":"big","f":"undefined"}"#
    );
}

/// D13: `undefined` can be put. In EMQX the key is then in the dictionary (d15), but
/// nothing a rule can call tells it from a key that is not: all three functions answer
/// `undefined` for both. So here it takes none of the dictionary's 64 keys.
#[test]
fn a_held_undefined_reads_as_no_key_and_takes_none() {
    let t = task();
    // d14: undefined, undefined, undefined, 5, undefined.
    assert_eq!(
        t.select(
            "proc_dict_put('u', payload.nope) AS p1, proc_dict_get('u') AS g, \
             proc_dict_put('u', 5) AS p2, proc_dict_put('u', payload.nope) AS p3, \
             proc_dict_del('u') AS d"
        ),
        r#"{"p1":"undefined","g":"undefined","p2":"undefined","p3":5,"d":"undefined"}"#
    );
    // s20: the same from a statement that puts a value in between.
    assert_eq!(
        t.select(
            "proc_dict_put('uv', payload.nope) AS p1, proc_dict_get('uv') AS g, \
             proc_dict_put('uv', 3) AS p2, proc_dict_put('uv', payload.nope) AS p3, \
             proc_dict_del('uv') AS d"
        ),
        r#"{"p1":"undefined","g":"undefined","p2":"undefined","p3":3,"d":"undefined"}"#
    );
    assert!(t.dictionary.is_empty());
    // Putting `undefined` over a value gives the value's room back.
    t.select("proc_dict_put('k', 'value') AS p");
    assert_eq!((t.dictionary.len(), t.dictionary.bytes()), (1, 6));
    t.select("proc_dict_put('k', payload.nope) AS p");
    assert_eq!((t.dictionary.len(), t.dictionary.bytes()), (0, 0));
}

/// D13: a value is any value, and comes back as it went in.
#[test]
fn a_value_of_any_type_is_held_as_it_is() {
    let t = task();
    // d22: a map with a list of every scalar; the put over it answers it.
    assert_eq!(
        t.select_on(
            "proc_dict_put('mv', payload.v) AS p, proc_dict_get('mv') AS g, \
             proc_dict_put('mv', 0) AS p2",
            r#"{"v":{"a":[1,2.5,"s",true,null]}}"#
        ),
        r#"{"p":"undefined","g":{"a":[1,2.5,"s",true,null]},"p2":{"a":[1,2.5,"s",true,null]}}"#
    );
    // s26: `null` and `true` are values, and `null` is not "no value".
    assert_eq!(
        t.select_on(
            "proc_dict_put('nv', payload.n) AS p1, proc_dict_put('nv', payload.t) AS p2, \
             proc_dict_get('nv') AS g",
            r#"{"n":null,"t":true}"#
        ),
        r#"{"p1":"undefined","p2":null,"g":true}"#
    );
    // A float stays a float and an integer past 64 bits an integer.
    assert_eq!(
        t.select(
            "proc_dict_put('f', 2.0) AS a, proc_dict_get('f') AS f, \
             proc_dict_put('b', 18446744073709551616) AS b, proc_dict_get('b') AS big"
        ),
        r#"{"a":"undefined","f":2.0,"b":"undefined","big":18446744073709551616}"#
    );
}

/// D3: a call takes effect when evaluation reaches it, in EMQX's order (every `SELECT`
/// field, then the `WHERE`), and nothing undoes it: it is `erlang:put/2` at that
/// moment.
#[test]
fn a_call_runs_when_evaluation_reaches_it_and_is_never_undone() {
    let t = task();
    let none: Result<Vec<String>, String> = Ok(Vec::new());
    // s05: a put in a SELECT field is there after a false WHERE.
    let sql = "SELECT proc_dict_put('w', 1) AS p FROM \"t/#\" WHERE 1 = 2";
    assert_eq!(t.sql(sql, "{}"), none);
    assert_eq!(t.held("'w'"), r#"{"v":1}"#);
    // s06: behind a false AND it is not reached.
    let sql = "SELECT 1 AS x FROM \"t/#\" WHERE 1 = 2 AND proc_dict_put('w2', 1) = 1";
    assert_eq!(t.sql(sql, "{}"), none);
    assert_eq!(t.held("'w2'"), NOT_HELD);
    // s07: nor behind a true OR.
    let sql = "SELECT 1 AS x FROM \"t/#\" WHERE 1 = 1 OR proc_dict_put('w3', 1) = 1";
    assert_eq!(t.sql(sql, "{}"), Ok(vec![r#"{"x":1}"#.to_string()]));
    assert_eq!(t.held("'w3'"), NOT_HELD);
    // s08: reached, it puts; and what it answers (`undefined`) is not 1.
    let sql = "SELECT 1 AS x FROM \"t/#\" WHERE 1 = 1 AND proc_dict_put('w4', 1) = 1";
    assert_eq!(t.sql(sql, "{}"), none);
    assert_eq!(t.held("'w4'"), r#"{"v":1}"#);
    // s09: the WHERE runs after the SELECT, and sees what it put.
    let sql = "SELECT proc_dict_put('o', 'select') AS p FROM \"t/#\" \
               WHERE proc_dict_get('o') = 'select'";
    assert_eq!(t.sql(sql, "{}"), Ok(vec![NOT_HELD.replace('v', "p")]));
    // s10, s11: a CASE evaluates the branch it takes.
    assert_eq!(
        t.select("CASE WHEN 1 = 2 THEN proc_dict_put('c', 1) ELSE 'no' END AS c"),
        r#"{"c":"no"}"#
    );
    assert_eq!(t.held("'c'"), NOT_HELD);
    assert_eq!(
        t.select("CASE WHEN 1 = 1 THEN proc_dict_put('c2', 1) ELSE 'no' END AS c"),
        r#"{"c":"undefined"}"#
    );
    assert_eq!(t.held("'c2'"), r#"{"v":1}"#);
    // s12: a put stays when a later field fails the rule.
    t.fails("proc_dict_put('f', 1) AS p, abs('x') AS boom", "{}");
    assert_eq!(t.held("'f'"), r#"{"v":1}"#);
    // s13: a field that fails first stops the put after it.
    t.fails("abs('x') AS boom, proc_dict_put('f2', 1) AS p", "{}");
    assert_eq!(t.held("'f2'"), NOT_HELD);
    // s25: an IN list is evaluated whole.
    let sql = "SELECT 1 AS x FROM \"t/#\" WHERE 1 IN (1, proc_dict_put('in', 1))";
    assert_eq!(t.sql(sql, "{}"), Ok(vec![r#"{"x":1}"#.to_string()]));
    assert_eq!(t.held("'in'"), r#"{"v":1}"#);
}

/// D3: in a `FOREACH` the collection's calls run before the `WHERE`, the `INCASE` per
/// element, the `DO` per element that passed.
#[test]
fn foreach_calls_run_per_element() {
    let t = task();
    let xs = r#"{"xs":[1,2,3]}"#;
    // s15: the DO, for 2 and 3.
    let sql = "FOREACH payload.xs AS x DO proc_dict_put('fe', x) AS prev, x AS x \
               INCASE x > 1 FROM \"t/#\"";
    assert_eq!(
        t.sql(sql, xs).unwrap(),
        [r#"{"prev":"undefined","x":2}"#, r#"{"prev":2,"x":3}"#]
    );
    assert_eq!(t.held("'fe'"), r#"{"v":3}"#);
    // s16: the INCASE, for every element. What it answers is the element before, so
    // the element that passes is the one after 2.
    let sql = "FOREACH payload.xs AS x DO x AS x INCASE proc_dict_put('fi', x) = 2 FROM \"t/#\"";
    assert_eq!(t.sql(sql, xs).unwrap(), [r#"{"x":3}"#]);
    assert_eq!(t.held("'fi'"), r#"{"v":3}"#);
    // s17: a call among the collection expressions runs before a false WHERE.
    let sql = "FOREACH payload.xs AS x, proc_dict_put('fc', 1) AS side DO x AS x FROM \"t/#\" \
               WHERE 1 = 2";
    assert_eq!(t.sql(sql, xs), Ok(Vec::new()));
    assert_eq!(t.held("'fc'"), r#"{"v":1}"#);
}

/// D13: the dictionary is the evaluating task's. What one statement put is there for
/// the next one the same task evaluates (s02..s04, s14), and for no other task (s27).
#[test]
fn a_dictionary_is_one_tasks_from_one_message_to_the_next() {
    let t = task();
    // s02, s03: left behind by one test, read by the next.
    assert_eq!(
        t.select_on("proc_dict_put('left', payload.x) AS p", r#"{"x":7}"#),
        r#"{"p":"undefined"}"#
    );
    assert_eq!(t.held("'left'"), r#"{"v":7}"#);
    // s14: a counter over three messages: (undefined, 1), (1, 2), (2, 3).
    let first = "proc_dict_put('n', 1) AS before, proc_dict_get('n') AS now";
    let next = "proc_dict_put('n', proc_dict_get('n') + 1) AS before, proc_dict_get('n') AS now";
    assert_eq!(t.select(first), r#"{"before":"undefined","now":1}"#);
    assert_eq!(t.select(next), r#"{"before":1,"now":2}"#);
    assert_eq!(t.select(next), r#"{"before":2,"now":3}"#);
    // s27: another process (here: another task's dictionary) sees none of it.
    let other = task();
    assert_eq!(other.held("'left'"), NOT_HELD);
    assert_eq!(other.held("'n'"), NOT_HELD);
    // Two rules one message matches share it, in id order, like any state.
    let text = "[rules.a_put]\nsql = '''SELECT proc_dict_put('seen', topic) AS p FROM \"t/#\"'''\n\
                actions = [{ function = \"console\" }]\n\
                [rules.b_get]\nsql = '''SELECT proc_dict_get('seen') AS g FROM \"t/#\"'''\n\
                actions = [{ function = \"console\" }]\n";
    let set = RuleSet::parse_with(text, &t.functions).unwrap().rules;
    assert_eq!(
        publish(&set, Some(&t.dictionary), "{}").unwrap(),
        [r#"{"p":"undefined"}"#, r#"{"g":"t/1"}"#]
    );
}

/// D13: an evaluation that is handed no dictionary (a Will, an event about a client
/// that has no connection, a statement tried on its own) gets an empty one that is
/// dropped with it.
#[test]
fn an_evaluation_without_a_dictionary_gets_an_empty_one() {
    let t = task();
    let set = load(
        &t.functions,
        "SELECT proc_dict_put('n', 1) AS before, proc_dict_get('n') AS now FROM \"t/#\"",
    );
    for _ in 0..2 {
        assert_eq!(
            publish(&set, None, "{}").unwrap(),
            [r#"{"before":"undefined","now":1}"#]
        );
    }
    // Nothing was counted, and the task's own dictionary was never involved.
    assert_eq!(t.store.bytes.load(Relaxed), 0);
    assert!(t.dictionary.is_empty());
    // `mqttd --rule-test`: R01..R03, EMQX's SQL test over its API, where each request
    // is a process of its own: (undefined, 1), (undefined, 2), undefined.
    let payload = Bytes::from_static(b"{}");
    let props = mqtt_core::AppProperties::default();
    let input = PublishInput::new("c1", "t/1", &payload, 1, &props);
    let test = |sql: &str| test_sql_with(sql, &input, &t.functions).unwrap();
    assert_eq!(
        test("SELECT proc_dict_put('rest', 1) AS prev, proc_dict_get('rest') AS now FROM \"t/#\""),
        [r#"{"prev":"undefined","now":1}"#]
    );
    assert_eq!(
        test("SELECT proc_dict_put('rest', 2) AS prev, proc_dict_get('rest') AS now FROM \"t/#\""),
        [r#"{"prev":"undefined","now":2}"#]
    );
    assert_eq!(
        test("SELECT proc_dict_get('rest') AS now FROM \"t/#\""),
        [r#"{"now":"undefined"}"#]
    );
}

/// D11: a dry run works on a scratch dictionary, never a connection's: one of its
/// own per call, or the one the caller made for several rules.
#[test]
fn a_dry_run_works_on_a_scratch_dictionary() {
    let t = task();
    let text = "[rules.a_put]\nsql = '''SELECT proc_dict_put('n', 1) AS before FROM \"t/#\"'''\n\
                [rules.b_get]\nsql = '''SELECT proc_dict_get('n') AS now FROM \"t/#\"'''\n";
    let set = RuleSet::parse_with(text, &t.functions).unwrap().rules;
    let payload = Bytes::from_static(b"{}");
    let props = mqtt_core::AppProperties::default();
    let input = PublishInput::new("c1", "t/1", &payload, 1, &props);
    let outcome = |id: &str, dictionary: Option<&Dictionary>| {
        let mut passed = false;
        let mut report = |_: &Rule, o: Outcome<'_>| passed |= matches!(o, Outcome::Passed);
        let mut out = Vec::new();
        assert!(match dictionary {
            Some(d) => set.evaluate_one_on(id, &input, None, d, &mut report, &mut out),
            None => set.evaluate_one(id, &input, None, &mut report, &mut out),
        });
        passed
    };
    // Each call on its own: the second rule finds nothing, and its WHERE-less
    // statement still passes with `undefined`.
    assert!(outcome("a_put", None));
    let scratch = Dictionary::new();
    assert!(outcome("b_get", Some(&scratch)));
    assert!(scratch.is_empty());
    // On one scratch dictionary the rules see each other, as on one connection.
    assert!(outcome("a_put", Some(&scratch)));
    assert_eq!(scratch.len(), 1);
    assert!(outcome("b_get", Some(&scratch)));
    // None of it reached the task's dictionary or the store's count.
    assert!(t.dictionary.is_empty());
    assert_eq!(t.store.bytes.load(Relaxed), 0);
}

/// D13, a listed difference: EMQX's dictionary has no bound; one here holds 64 keys.
/// The 65th fails its rule (`dictionary_full`) and changes nothing; a key that is
/// held can still be put, and a deleted one makes room.
#[test]
fn a_dictionary_holds_64_keys() {
    let t = task();
    let keys: Vec<String> = (0..=MAX_DICT_KEYS).map(|i| i.to_string()).collect();
    let payload = format!("{{\"a\":[{}]}}", keys.join(","));
    let sql = "FOREACH payload.a AS k DO proc_dict_put(k, k) AS p FROM \"t/#\"";
    assert_eq!(
        t.sql(sql, &payload).unwrap_err(),
        "proc_dict_put(): dictionary_full: this connection's dictionary already holds 64 keys \
         (proc_dict_del frees one; EMQX's dictionary has no bound, mqttd's has)"
    );
    assert_eq!(t.dictionary.len(), MAX_DICT_KEYS);
    assert_eq!(t.held("64"), NOT_HELD);
    // A key it holds is put again, and read, and a key it does not hold is read and
    // deleted, as before.
    assert_eq!(t.select("proc_dict_put(63, 'again') AS v"), r#"{"v":63}"#);
    assert_eq!(t.select("proc_dict_del('nope') AS v"), NOT_HELD);
    t.fails("proc_dict_put('one more', 1) AS v", "{}");
    // `undefined` takes no key, so putting it where there is no room is no failure.
    assert_eq!(t.select("proc_dict_put('x', payload.nope) AS v"), NOT_HELD);
    assert_eq!(t.select("proc_dict_del(0) AS v"), r#"{"v":0}"#);
    assert_eq!(t.select("proc_dict_put('one more', 1) AS v"), NOT_HELD);
    assert_eq!(t.dictionary.len(), MAX_DICT_KEYS);
}

/// D13, a listed difference: a dictionary's keys and values, as the rule engine
/// writes them, take at most 64 KiB together. A put past that fails its rule
/// (`dictionary_full`) and leaves what was held.
#[test]
fn a_dictionary_holds_64_kib() {
    let t = task();
    // A key of 1 byte and a value of 65,535: exactly the bound.
    let fits = format!("{{\"v\":\"{}\"}}", "x".repeat(MAX_DICT_BYTES - 1));
    assert_eq!(
        t.select_on("proc_dict_put('k', payload.v) AS v", &fits),
        NOT_HELD
    );
    assert_eq!(t.dictionary.bytes(), MAX_DICT_BYTES);
    assert_eq!(
        t.fails("proc_dict_put('b', 1) AS v", "{}"),
        "proc_dict_put(): dictionary_full: this connection's dictionary would hold 65538 bytes \
         of keys and values as written; at most 65536 (EMQX's dictionary has no bound, mqttd's \
         has)"
    );
    // One byte more under the same key does not fit either, and the value stays.
    let over = format!("{{\"v\":\"{}\"}}", "y".repeat(MAX_DICT_BYTES));
    t.fails("proc_dict_put('k', payload.v) AS v", &over);
    assert_eq!(t.dictionary.bytes(), MAX_DICT_BYTES);
    assert_eq!(
        t.select("substr(proc_dict_get('k'), 0, 3) AS v"),
        r#"{"v":"xxx"}"#
    );
    // A smaller value under it does, and the room is back.
    t.select("proc_dict_put('k', 'small') AS v");
    assert_eq!(t.dictionary.bytes(), 6);
    assert_eq!(t.select("proc_dict_put('b', 1) AS v"), NOT_HELD);
    // A key that could not fit is in no dictionary: reading or deleting it answers
    // `undefined`, as EMQX would; putting under it fails.
    let key = format!("{{\"k\":\"{}\"}}", "k".repeat(MAX_DICT_BYTES + 1));
    assert_eq!(
        t.select_on(
            "proc_dict_get(payload.k) AS g, proc_dict_del(payload.k) AS d",
            &key
        ),
        r#"{"g":"undefined","d":"undefined"}"#
    );
    assert_eq!(
        t.fails("proc_dict_put(payload.k, 1) AS v", &key),
        "proc_dict_put(): dictionary_full: a key of more than 65536 bytes as written does not \
         fit a dictionary (EMQX's dictionary has no bound, mqttd's has)"
    );
}

/// D13: what a dictionary holds counts against the store's ceiling for as long as it
/// holds it, and a put the store has no room for fails its rule.
#[test]
fn a_dictionary_counts_against_the_stores_ceiling_and_gives_its_bytes_back() {
    let t = task_under(100);
    let counted = || t.store.bytes.load(Relaxed);
    // A key of 1 byte and a value of 60.
    let sixty = format!("{{\"v\":\"{}\"}}", "x".repeat(60));
    t.select_on("proc_dict_put('a', payload.v) AS v", &sixty);
    assert_eq!((counted(), t.dictionary.bytes()), (61, 61));
    // Another task's dictionary counts in the same store.
    let other = Dictionary::counted_in(t.store.clone());
    let set = load(
        &t.functions,
        "SELECT proc_dict_put('b', payload.v) AS v FROM \"t/#\"",
    );
    let put = |v: &str| publish(&set, Some(&other), &format!("{{\"v\":\"{v}\"}}"));
    assert_eq!(
        put(&"y".repeat(40)).unwrap_err(),
        "proc_dict_put(): store_full: the test's ceiling"
    );
    assert!(other.is_empty());
    assert_eq!(counted(), 61);
    put("fits").unwrap();
    assert_eq!((counted(), other.bytes()), (66, 5));
    // A smaller value, a delete and `clear` give back what they free.
    put("f").unwrap();
    assert_eq!(counted(), 63);
    t.select("proc_dict_del('a') AS v");
    assert_eq!(counted(), 2);
    other.clear();
    assert_eq!((counted(), other.len()), (0, 0));
    // And a dictionary that goes takes its count with it: a connection that ends.
    put("fits").unwrap();
    t.select("proc_dict_put('a', 1) AS v");
    assert_eq!(counted(), 7);
    drop(other);
    assert_eq!(counted(), 2);
    drop(t.dictionary);
    assert_eq!(t.store.bytes.load(Relaxed), 0);
}

/// D9: every call, read or write, is one of the message's 1,024 state calls, and what
/// a call reads out of the dictionary is charged to the message's build budget.
#[test]
fn a_call_counts_against_the_messages_state_calls_and_build_budget() {
    let t = task();
    let ints = |n: usize| {
        let list: Vec<String> = (0..n).map(|i| i.to_string()).collect();
        format!("{{\"a\":[{}]}}", list.join(","))
    };
    let sql = "FOREACH payload.a INCASE proc_dict_get('k') = 'no' FROM \"t/#\"";
    assert_eq!(
        t.sql(sql, &ints(MAX_STATE_CALLS_PER_TRIGGER)),
        Ok(Vec::new())
    );
    let e = t
        .sql(sql, &ints(MAX_STATE_CALLS_PER_TRIGGER + 1))
        .unwrap_err();
    assert!(
        e.starts_with("proc_dict_get(): this message already made 1024 state calls"),
        "{e}"
    );
    // 16 reads of a 64 KiB value are the whole megabyte; the 17th is past it.
    let big = format!("{{\"v\":\"{}\"}}", "x".repeat(MAX_DICT_BYTES - 1));
    t.select_on("proc_dict_put('k', payload.v) AS v", &big);
    assert_eq!(t.sql(sql, &ints(16)), Ok(Vec::new()));
    assert_eq!(
        t.sql(sql, &ints(17)).unwrap_err(),
        "proc_dict_get(): the values this message's rules read from state would pass 1048576 \
         bytes (the budget is per message, shared with every function call)"
    );
}

/// D13: the three names are built in (no module may take one), with EMQX's arities,
/// and a file that calls one loads only where there is a store: a dictionary's bytes
/// count against its ceiling.
#[test]
fn the_functions_are_built_in_and_load_only_with_a_store() {
    for name in ["proc_dict_put", "proc_dict_get", "proc_dict_del"] {
        assert!(function_names().contains(&name), "{name}");
        assert_eq!(
            reserved_function_name(name),
            Some("a built-in function has that name"),
            "{name}"
        );
        assert!(!emqx_only_function_names().contains(&name), "{name}");
    }
    let t = task();
    // d24..d26, s22..s24: EMQX fails every message (`sql_function_not_supported`); a
    // wrong argument count is a load error here, as for every function.
    for (call, takes) in [
        (
            "proc_dict_put('a')",
            "proc_dict_put() takes 2 argument(s), 1 given",
        ),
        (
            "proc_dict_get('a', 1)",
            "proc_dict_get() takes 1 argument(s), 2 given",
        ),
        (
            "proc_dict_del()",
            "proc_dict_del() takes 1 argument(s), 0 given",
        ),
        (
            "proc_dict_del('a', 1)",
            "proc_dict_del() takes 1 argument(s), 2 given",
        ),
    ] {
        let text = format!("[rules.r]\nsql = '''SELECT {call} AS v FROM \"t/#\"'''\n");
        let e = RuleSet::parse_with(&text, &t.functions)
            .expect_err(call)
            .to_string();
        assert!(e.contains(takes), "{call}: {e}");
    }
    for call in [
        "proc_dict_put('k', 1)",
        "proc_dict_get('k')",
        "proc_dict_del('k')",
    ] {
        let text = format!("[rules.r]\nsql = '''SELECT {call} AS v FROM \"t/#\"'''\n");
        let e = RuleSet::parse(&text).expect_err(call).to_string();
        assert!(
            e.starts_with("rule `r`: ")
                && e.contains(
                    "rule state is not available here: the rules are loaded without a state store"
                ),
            "{e}"
        );
        let off = RuleSet::parse_with(&text, &Functions::default().without_state())
            .expect_err(call)
            .to_string();
        assert!(off.contains("`rules.state_max_bytes` is 0"), "{off}");
        // With a store it loads, and holds no namespace: a dictionary is in none.
        let loaded = RuleSet::parse_with(&text, &t.functions).unwrap();
        assert_eq!(loaded.rules.state_namespaces().count(), 0);
        assert!(loaded.warnings.is_empty(), "{:?}", loaded.warnings);
    }
}
