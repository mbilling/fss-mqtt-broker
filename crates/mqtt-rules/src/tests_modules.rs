//! Operator modules (ADR 0086 D9, delivery task 0086-T3): `[functions.<name>]` in a
//! rules file. The modules here are written out by hand
//! (`mqtt_wasm_sandbox::test_module`), so these tests need no Wasm toolchain; the
//! worked examples in `examples/rule-plugins` are built and run by CI through the
//! broker.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use mqtt_wasm_sandbox::test_module::{self, Shape};

use super::*;

/// Writes `wasm` at `relative` under `dir` and returns its sha256.
fn place(dir: &Path, relative: &str, wasm: &[u8]) -> String {
    let path = dir.join(relative);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, wasm).unwrap();
    sha256(wasm)
}

fn sha256(bytes: &[u8]) -> String {
    mqtt_core::hex_lower(aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes).as_ref())
}

/// What a broker whose rules file is `dir/rules.toml` loads rules with.
fn functions_in(dir: &Path) -> Functions {
    host(ModuleSettings {
        instances: 2,
        ..ModuleSettings::default()
    })
    .beside(&dir.join("rules.toml"))
}

fn host(settings: ModuleSettings) -> Functions {
    Functions::default().with_modules(settings)
}

/// A `[functions.<name>]` table.
fn table(name: &str, wasm: &str, sha256: &str, functions: &[&str], more: &str) -> String {
    let functions: Vec<String> = functions.iter().map(|f| format!("\"{f}\"")).collect();
    format!(
        "[functions.{name}]\nwasm = \"{wasm}\"\nsha256 = \"{sha256}\"\nfunctions = [{}]\n{more}\n",
        functions.join(", ")
    )
}

fn rule(id: &str, sql: &str) -> String {
    format!("[rules.{id}]\nsql = '{sql}'\nactions = [{{ function = \"console\" }}]\n")
}

/// What one publish did: each console line, each failure text, each function call.
#[derive(Debug, Default)]
struct Seen {
    lines: Vec<String>,
    failures: Vec<String>,
    calls: Vec<FunctionCall>,
}

fn publish(set: &RuleSet, topic: &str, payload: &[u8]) -> Seen {
    let payload = Bytes::copy_from_slice(payload);
    let props = mqtt_core::AppProperties::default();
    let input = PublishInput::new("c", topic, &payload, 0, &props);
    let mut seen = Seen::default();
    let mut out = Vec::new();
    set.on_publish(
        &input,
        &mut |_, outcome| match outcome {
            Outcome::Failed(e) => seen.failures.push(e.to_string()),
            Outcome::Function(call) => seen.calls.push(call.clone()),
            _ => {}
        },
        &mut out,
    );
    for (_, effect) in out {
        if let Effect::Console(line) = effect {
            seen.lines.push(line);
        }
    }
    seen
}

fn load_error(text: &str, functions: &Functions) -> String {
    RuleSet::parse_with(text, functions)
        .expect_err(text)
        .to_string()
}

/// The acceptance row's first clause: a rules file declares a module by path and
/// sha256, and its rules call the module's functions like any other.
#[test]
fn a_rules_file_declares_a_module_by_path_and_sha256_and_its_rules_call_it() {
    let dir = tempfile::tempdir().unwrap();
    let sha = place(
        dir.path(),
        "plugins/test.wasm",
        &test_module::plugin("1.2.3", b"j41"),
    );
    let text = table(
        "site",
        "plugins/test.wasm",
        &sha.to_uppercase(),
        &["which", "ident"],
        "",
    ) + &rule(
        "r",
        "SELECT which() AS w, ident(payload) AS text, ident(payload.a) AS a, \
         ident(json_decode(payload)) AS all FROM \"t/#\" WHERE which() = 41",
    );
    std::fs::write(dir.path().join("rules.toml"), &text).unwrap();
    let loaded = RuleSet::load_with(&dir.path().join("rules.toml"), &functions_in(dir.path()))
        .expect("the file loads");
    assert!(loaded.warnings.is_empty(), "{:?}", loaded.warnings);
    let set = loaded.rules;
    assert!(set.calls_sandbox(), "a module's call may run long");

    // A binary crosses as its bytes and comes back a string; any other value crosses
    // as the JSON the rule engine writes and is read back by its reader, so a float
    // stays a float and an integer of any size stays exact (ADR 0086 D3).
    let payload = br#"{"a":{"big":123456789012345678901234567890,"f":1.0}}"#;
    let seen = publish(&set, "t/1", payload);
    assert_eq!(seen.failures, Vec::<String>::new());
    assert_eq!(
        seen.lines,
        vec![format!(
            "{{\"w\":41,\"text\":{},\"a\":{{\"big\":123456789012345678901234567890,\"f\":1.0}},\
             \"all\":{}}}",
            Value::from(std::str::from_utf8(payload).unwrap())
                .to_json()
                .unwrap(),
            std::str::from_utf8(payload).unwrap(),
        )]
    );
    // Every call is reported with its function and its module (D10).
    let called: Vec<(&str, &str, Option<&str>)> = seen
        .calls
        .iter()
        .map(|c| (&*c.function, &*c.module, c.error))
        .collect();
    assert_eq!(
        called,
        vec![
            ("which", "site", None),
            ("ident", "site", None),
            ("ident", "site", None),
            ("ident", "site", None),
            ("which", "site", None),
        ]
    );
    assert_eq!(seen.calls[4].stats.instances, 1, "one warm instance");

    // What the admin API lists (D9).
    let [module] = set.modules() else {
        panic!("one module")
    };
    assert_eq!(&**module.name(), "site");
    assert_eq!(module.wasm(), "plugins/test.wasm");
    assert_eq!(module.sha256(), sha);
    assert_eq!(
        (module.described_name(), module.version()),
        ("test", "1.2.3")
    );
    let names: Vec<&str> = module.functions().iter().map(|f| &*f.name).collect();
    assert_eq!(names, ["ident", "which"]);
    assert_eq!(
        (
            module.functions()[0].min_args,
            module.functions()[0].max_args
        ),
        (0, 8)
    );
    assert_eq!(
        module.timeout(),
        Duration::from_millis(DEFAULT_TIMEOUT_MS),
        "the default"
    );
    assert_eq!(module.memory_limit_bytes(), DEFAULT_MEMORY_MIB << 20);
    assert!(!module.clock() && !module.fresh_instance());
    assert_eq!(module.environment().count(), 0);
    let stats = module.stats();
    assert_eq!((stats.instances, stats.in_call, stats.replaced), (1, 0, 0));
    assert_eq!(stats.memory_bytes, 65_536);

    // A function the table does not name is not a function, though the module has it;
    // and the module's arity is checked at load like a built-in's.
    let other = table("site", "plugins/test.wasm", &sha, &["which"], "");
    assert!(load_error(
        &(other.clone() + &rule("r", "SELECT spin() AS s FROM \"t\"")),
        &functions_in(dir.path())
    )
    .contains("unknown function spin()"));
    assert!(load_error(
        &(other + &rule("r", "SELECT which(1,2,3,4,5,6,7,8,9) AS s FROM \"t\"")),
        &functions_in(dir.path())
    )
    .contains("which() takes 0..=8 argument(s), 9 given"));
}

/// The acceptance row's second clause, one case per reason: "a wrong hash, a missing
/// export, another ABI version, a forbidden import, or a name that collides with a
/// built-in or an EMQX function fails the load with the reason".
#[test]
#[allow(clippy::too_many_lines)] // one case per reason
fn a_module_that_fails_a_check_fails_the_load_with_the_reason() {
    let dir = tempfile::tempdir().unwrap();
    let functions = functions_in(dir.path());
    let good = test_module::plugin("1", b"j1");
    let sha = place(dir.path(), "good.wasm", &good);
    let described = |shape: Shape| {
        test_module::module(&Shape {
            description: test_module::description("1"),
            ..shape
        })
    };
    let declared = |file: &str, wasm: &[u8]| {
        let sha = place(dir.path(), file, wasm);
        table("m", file, &sha, &["which"], "")
    };

    // A wrong hash: the file is not the one the rules file pins. The error says which
    // hash was asked for and never the file's own.
    let wrong = "0".repeat(64);
    let error = load_error(&table("m", "good.wasm", &wrong, &["which"], ""), &functions);
    assert_eq!(
        error,
        format!(
            "functions `m`: wasm = \"good.wasm\": the file does not have the sha256 the table \
             pins ({wrong}); `sha256sum` prints the file's"
        )
    );
    assert!(!error.contains(&sha));
    // A missing export.
    let error = load_error(
        &declared(
            "nocall.wasm",
            &described(Shape {
                without_call: true,
                ..Shape::default()
            }),
        ),
        &functions,
    );
    assert!(
        error.starts_with("functions `m`: the module does not export the function ABI:")
            && error.contains("mqttd_call"),
        "{error}"
    );
    // Another ABI version.
    let error = load_error(
        &declared(
            "abi2.wasm",
            &described(Shape {
                version: 2,
                ..Shape::default()
            }),
        ),
        &functions,
    );
    assert_eq!(
        error,
        "functions `m`: the module speaks ABI version 2, this host 1"
    );
    // A forbidden import: anything outside the sixteen functions the sandbox provides.
    for import in [("env", "system"), ("wasi_snapshot_preview1", "sock_send")] {
        let error = load_error(
            &declared(
                "import.wasm",
                &described(Shape {
                    import: Some(import),
                    ..Shape::default()
                }),
            ),
            &functions,
        );
        assert_eq!(
            error,
            format!(
                "functions `m`: the module imports {}.{}, which is not provided",
                import.0, import.1
            )
        );
    }
    // A name that collides with a built-in function, with a function of EMQX's this
    // engine refuses, or with a word of rule SQL.
    for (name, why) in [
        ("upper", "a built-in function has that name"),
        ("jq", "a built-in function has that name"),
        ("kv_store_get", "a built-in function has that name"),
        ("kv_get", "a built-in function has that name"),
        ("proc_dict_get", "a built-in function has that name"),
        ("schema_decode", "EMQX has a function of that name"),
        ("select", "it is a word of rule SQL"),
    ] {
        let error = load_error(&table("m", "good.wasm", &sha, &[name], ""), &functions);
        assert_eq!(
            error,
            format!(
                "functions `m`: a module cannot provide `{name}`: {why}, and a module must not \
                 change what a rule written for EMQX means"
            )
        );
    }

    // And the rest of what a table can get wrong.
    for (text, want) in [
        (
            table("m", "missing.wasm", &sha, &["which"], ""),
            "wasm = \"missing.wasm\": no such file under",
        ),
        (
            declared("text.wasm", b"not a module"),
            "not a usable WebAssembly module",
        ),
        (
            table("m", "good.wasm", &sha, &["absent"], ""),
            "the module does not provide `absent` (it describes: big, err, grow_memory, ident, \
             loop_forever, nargs, recurse, spin, which, xtrap)",
        ),
        (
            declared("plain.wasm", &test_module::module(&Shape::default())),
            "the module's description (mqttd_describe) is not the JSON expected",
        ),
        (
            table("m", "good.wasm", "abc", &["which"], ""),
            "sha256 is the file's SHA-256 as 64 hex digits",
        ),
        (
            table("m", "good.wasm", &sha, &[], ""),
            "functions names no function",
        ),
        (
            table("m", "good.wasm", &sha, &["which", "which"], ""),
            "functions lists `which` twice",
        ),
        (
            table("m", "good.wasm", &sha, &["Which"], ""),
            "function name \"Which\": a lower-case letter",
        ),
        (
            table("M", "good.wasm", &sha, &["which"], ""),
            "a module's name is a lower-case letter",
        ),
        (
            table("jq", "good.wasm", &sha, &["which"], ""),
            "`jq` is the built-in module's name",
        ),
        (
            table("m", "good.wasm", &sha, &["which"], "timeout_ms = 0"),
            "timeout_ms = 0: from 1 to 10000",
        ),
        (
            table("m", "good.wasm", &sha, &["which"], "timeout_ms = 10001"),
            "timeout_ms = 10001: from 1 to 10000",
        ),
        (
            table("m", "good.wasm", &sha, &["which"], "memory_mib = 65"),
            "memory_mib = 65: from 1 to 64",
        ),
        (
            table("m", "good.wasm", &sha, &["which"], "env = [\"HOME\"]"),
            "env names \"HOME\", which the broker's rules.functions_env \
             (MQTTD_RULES_FUNCTIONS_ENV) does not list",
        ),
        (
            table("m", "good.wasm", &sha, &["which"], "")
                + &table("n", "good.wasm", &sha, &["which"], ""),
            "functions `n`: `which` is already provided by [functions.m]",
        ),
    ] {
        let error = load_error(&text, &functions);
        assert!(error.contains(want), "{text}\n{error}\nwanted: {want}");
        assert!(error.starts_with("functions `"), "{error}");
    }
    // An unknown key is the file's error, as everywhere in a rules file.
    let error = load_error(
        &table("m", "good.wasm", &sha, &["which"], "network = true"),
        &functions,
    );
    assert!(
        error.starts_with("rules file:") && error.contains("unknown field `network`"),
        "{error}"
    );
    // More modules than a file may declare.
    let many: String = (0..=MAX_MODULES)
        .map(|i| table(&format!("m{i}"), "good.wasm", &sha, &["which"], ""))
        .collect();
    assert!(load_error(&many, &functions).contains("17 modules is more than the 16"));
    // Nothing of all that is left loaded.
    assert_eq!(functions.module_host().unwrap().loaded_modules(), 0);
}

/// Every name a rule written for EMQX can call is reserved: the built-in functions
/// here, and every export of EMQX 6.3.1's `emqx_rule_funcs` (its
/// `module_info(exports)`, less the operators) with the names it resolves without
/// exporting.
#[test]
fn every_built_in_and_every_emqx_function_name_is_reserved() {
    const EMQX_6_3_1_EXPORTS: &str = "abs acos acosh ascii asin asinh atan atanh base64_decode \
        base64_encode bin2hexstr bitand bitnot bitor bitsize bitsl bitsr bitxor bool bytesize \
        ceil clientid clientip coalesce coalesce_ne concat contains contains_topic \
        contains_topic_match cos cosh date_to_unix_ts div eq exp find first flag flags float \
        float2str floor fmod format_date getenv gunzip gzip hash hash_to_range hexstr2bin int \
        is_array is_bool is_empty is_float is_int is_map is_not_null is_not_null_var is_null \
        is_null_var is_num is_str join_to_sql_values_string join_to_string jq json_decode \
        json_encode kv_store_del kv_store_get kv_store_put last length log log10 log2 lower \
        ltrim lz4_compress lz4_uncompress map map_get map_keys map_new map_put map_size \
        map_to_entries map_to_range map_to_redis_hset_args map_values md5 mget mod mongo_date \
        mput msgid now_rfc3339 now_timestamp nth null pad payload peerhost power proc_dict_del \
        proc_dict_get proc_dict_put qos random regex_extract regex_match regex_replace replace \
        reverse rfc3339_to_unix_ts rm_prefix round rtrim sha sha256 sin sinh split sprintf_s \
        sqlserver_bin2hexstr sqrt str str_utf16_le str_utf8 strlen subbits sublist substr tan \
        tanh term_decode term_encode timezone_to_offset_seconds timezone_to_second tokens topic \
        trim unescape unix_ts_to_rfc3339 unzip upper username uuid_v4 uuid_v4_no_hyphen zip \
        zip_compress zip_uncompress";
    const RESOLVED_WITHOUT_AN_EXPORT: &str = "sprintf schema_check schema_decode schema_encode \
        sparkplug_decode sparkplug_encode maptab_lookup";
    for name in function_names() {
        assert_eq!(
            reserved_function_name(name),
            Some("a built-in function has that name"),
            "{name}"
        );
    }
    for name in EMQX_6_3_1_EXPORTS
        .split_whitespace()
        .chain(RESOLVED_WITHOUT_AN_EXPORT.split_whitespace())
    {
        assert!(reserved_function_name(name).is_some(), "{name}");
    }
    // The second list holds only what the first does not.
    for name in emqx_only_function_names() {
        assert!(!function_names().contains(name), "{name}");
        assert_eq!(
            reserved_function_name(name),
            Some("EMQX has a function of that name")
        );
    }
    assert_eq!(reserved_function_name("geo_distance"), None);
}

/// Who may point the broker at a file: nobody, beyond the directory of the rules file.
/// The path a table gives is relative, of plain names, ends in `.wasm`, and still lies
/// under that directory once every symlink is resolved.
#[test]
fn a_module_is_only_read_from_under_the_rules_files_directory() {
    let outer = tempfile::tempdir().unwrap();
    let dir = outer.path().join("rules");
    std::fs::create_dir_all(&dir).unwrap();
    let wasm = test_module::plugin("1", b"j1");
    let sha = place(outer.path(), "outside.wasm", &wasm);
    place(&dir, "inside.wasm", &wasm);
    std::fs::write(dir.join("secret.key"), &wasm).unwrap();
    std::fs::create_dir_all(dir.join("folder.wasm")).unwrap();
    let functions = functions_in(&dir);
    let absolute = outer.path().join("outside.wasm");
    for (path, want) in [
        (
            absolute.to_str().unwrap(),
            "a module is named by a relative path under the rules file's directory, without `..`",
        ),
        (
            "../outside.wasm",
            "a module is named by a relative path under the rules file's directory, without `..`",
        ),
        (
            "sub/../inside.wasm",
            "a module is named by a relative path under the rules file's directory, without `..`",
        ),
        (
            "./inside.wasm",
            "a module is named by a relative path under the rules file's directory, without `..`",
        ),
        ("", "a module is named by a relative path"),
        // Whatever it holds: a file that is not named as a module is never opened.
        ("secret.key", "a module file's name ends in .wasm"),
        ("inside", "a module file's name ends in .wasm"),
        ("folder.wasm", "not a regular file"),
    ] {
        let error = load_error(&table("m", path, &sha, &["which"], ""), &functions);
        assert!(error.contains(want), "{path:?}: {error}");
    }
    #[cfg(unix)]
    {
        // A symlink is followed, and where it leads decides: out of the directory is
        // refused though the file is a good module with the pinned hash; within it
        // is the file it names.
        std::os::unix::fs::symlink(&absolute, dir.join("out.wasm")).unwrap();
        std::os::unix::fs::symlink(outer.path(), dir.join("up")).unwrap();
        std::os::unix::fs::symlink(dir.join("inside.wasm"), dir.join("link.wasm")).unwrap();
        for path in ["out.wasm", "up/outside.wasm"] {
            let error = load_error(&table("m", path, &sha, &["which"], ""), &functions);
            assert!(
                error.contains("a symlink leads out of the rules file's directory"),
                "{path}: {error}"
            );
        }
        RuleSet::parse_with(&table("m", "link.wasm", &sha, &["which"], ""), &functions)
            .expect("a link that stays inside");
    }
    RuleSet::parse_with(&table("m", "inside.wasm", &sha, &["which"], ""), &functions)
        .expect("the file in the directory");

    // Rules text loaded without a rules file to stand beside, or by something that
    // loads no modules, declares none.
    let text = table("m", "inside.wasm", &sha, &["which"], "");
    for functions in [Functions::default(), host(ModuleSettings::default())] {
        let error = load_error(&text, &functions);
        assert_eq!(
            error,
            "functions `m`: modules are read from under the directory of the broker's rules \
             file (rules.file), and this text is loaded without one"
        );
    }
    assert!(
        RuleSet::parse(&text).is_err(),
        "the plain loader has no modules"
    );
    // A file without the table loads as it always did.
    let plain = RuleSet::parse("[rules.r]\nsql = 'SELECT 1 FROM \"t\"'\n").unwrap();
    assert!(plain.rules.modules().is_empty() && !plain.rules.calls_sandbox());
}

/// ADR 0086 D7: a module's call runs under its table's limits, and reaching one fails
/// the rule for this message, counted by kind; it is never "no result", and the next
/// message is served.
#[test]
fn a_modules_limits_fail_the_rule_and_are_counted_by_kind() {
    let dir = tempfile::tempdir().unwrap();
    let sha = place(dir.path(), "t.wasm", &test_module::plugin("1", b"j7"));
    // Two tables over one file: the one whose calls are meant to run out of time gets
    // little of it, and the other enough that no slow machine mistakes its limit.
    let rest: Vec<&str> = test_module::FUNCTIONS
        .into_iter()
        .filter(|f| *f != "loop_forever")
        .collect();
    let text = table("lim", "t.wasm", &sha, &["loop_forever"], "timeout_ms = 20")
        + &table(
            "rest",
            "t.wasm",
            &sha,
            &rest,
            "timeout_ms = 10000\nmemory_mib = 2",
        )
        + &rule("time", "SELECT loop_forever() AS x FROM \"time\"")
        + &rule("memory", "SELECT grow_memory() AS x FROM \"memory\"")
        + &rule("stack", "SELECT recurse() AS x FROM \"stack\"")
        + &rule("trap", "SELECT xtrap() AS x FROM \"trap\"")
        + &rule("own", "SELECT err() AS x FROM \"own\"")
        + &rule("claim", "SELECT big() AS x FROM \"claim\"")
        + &rule(
            "where",
            "SELECT 1 AS x FROM \"where\" WHERE loop_forever() = 1",
        )
        + &rule(
            "twice",
            "SELECT ident(payload) AS a, ident(payload) AS b FROM \"twice\"",
        )
        + &rule("ok", "SELECT which() AS x FROM \"#\"");
    let set = RuleSet::parse_with(&text, &functions_in(dir.path()))
        .unwrap()
        .rules;
    for (topic, kind, text) in [
        (
            "time",
            "timeout",
            "loop_forever(): sandbox_limit (timeout): the call ran past its 20 ms (timeout_ms \
             of [functions.lim])",
        ),
        (
            "memory",
            "memory",
            "grow_memory(): sandbox_limit (memory): the module reached its memory limit (2 \
             MiB, memory_mib of [functions.rest])",
        ),
        (
            "stack",
            "stack",
            "recurse(): sandbox_limit (stack): the module's stack overflowed",
        ),
        ("trap", "trap", "xtrap(): the module trapped:"),
        ("own", "module", "err(): status 7: nope"),
        (
            "claim",
            "output",
            "big(): sandbox_limit (output): the result is larger than what is left of the \
             message's 1048576 bytes of function output",
        ),
        // In a WHERE too: cut off is failed, never filtered out.
        (
            "where",
            "timeout",
            "loop_forever(): sandbox_limit (timeout)",
        ),
    ] {
        let seen = publish(&set, topic, b"{}");
        assert_eq!(seen.failures.len(), 1, "{topic}: {seen:?}");
        assert!(seen.failures[0].starts_with(text), "{}", seen.failures[0]);
        let failed: Vec<Option<&str>> = seen
            .calls
            .iter()
            .filter(|c| c.error.is_some())
            .map(|c| c.error)
            .collect();
        assert_eq!(failed, [Some(kind)], "{topic}");
        // The rule that matches everything still ran, on this very message.
        assert_eq!(seen.lines, ["{\"x\":7}"], "{topic}");
    }
    // The output of every call is charged whole to the message's 1 MiB of function
    // output: the same 600 KB passes once and not twice.
    let payload = vec![b'x'; 600 * 1024];
    let seen = publish(&set, "twice", &payload);
    assert_eq!(seen.failures.len(), 1);
    assert!(
        seen.failures[0].starts_with("ident(): sandbox_limit (output)"),
        "{}",
        seen.failures[0]
    );
    let kinds: Vec<Option<&str>> = seen
        .calls
        .iter()
        .filter(|c| &*c.function == "ident")
        .map(|c| c.error)
        .collect();
    assert_eq!(kinds, [None, Some("output")]);
    // A call stopped by the sandbox costs its instance, which the next call replaces:
    // twice for time; for memory, stack, the trap and the two results the host refused
    // to read. The module's own refusal (`err`) costs nothing.
    let [lim, rest] = set.modules() else {
        panic!("two modules")
    };
    assert_eq!(lim.stats().replaced, 2, "{:?}", lim.stats());
    assert_eq!(rest.stats().replaced, 5, "{:?}", rest.stats());
    assert!(rest.stats().instances <= 2, "{:?}", rest.stats());
}

/// `fresh_instance = true`: nothing a module keeps in its memory survives a call.
#[test]
fn a_fresh_instance_per_call_keeps_no_state_between_messages() {
    let dir = tempfile::tempdir().unwrap();
    let sha = place(dir.path(), "t.wasm", &test_module::plugin("1", b"j7"));
    let text = table("m", "t.wasm", &sha, &["which"], "fresh_instance = true")
        + &rule("r", "SELECT which() AS x FROM \"t\"");
    let set = RuleSet::parse_with(&text, &functions_in(dir.path()))
        .unwrap()
        .rules;
    for _ in 0..3 {
        assert_eq!(publish(&set, "t", b"1").lines, ["{\"x\":7}"]);
    }
    let module = &set.modules()[0];
    assert!(module.fresh_instance());
    assert_eq!(module.stats().instances, 0, "none is kept");
}

/// A table's `env` may name only what the broker's own configuration lists
/// (`rules.functions_env`): whoever writes rules cannot show a module the broker's
/// environment. A listed variable that is not set is simply not given.
#[test]
fn a_module_is_granted_only_environment_the_broker_lists() {
    let dir = tempfile::tempdir().unwrap();
    let sha = place(dir.path(), "t.wasm", &test_module::plugin("1", b"j7"));
    let functions = host(ModuleSettings {
        environment: vec![
            ("SITE".to_string(), Some("lab".to_string())),
            ("UNSET".to_string(), None),
        ],
        instances: 1,
        hand_off: None,
    })
    .beside(&dir.path().join("rules.toml"));
    let granted = table(
        "m",
        "t.wasm",
        &sha,
        &["which"],
        "env = [\"SITE\", \"UNSET\", \"SITE\"]\nclock = true",
    );
    let set = RuleSet::parse_with(&granted, &functions).unwrap().rules;
    let module = &set.modules()[0];
    assert_eq!(module.environment().collect::<Vec<_>>(), ["SITE"]);
    assert!(module.clock());
    let error = load_error(
        &table(
            "m",
            "t.wasm",
            &sha,
            &["which"],
            "env = [\"SITE\", \"MQTTD_SECRET\"]",
        ),
        &functions,
    );
    assert!(
        error.contains("env names \"MQTTD_SECRET\", which the broker's rules.functions_env"),
        "{error}"
    );
    // The values are in no debug text.
    assert!(!format!("{functions:?} {module:?}").contains("lab"));
}

/// The acceptance row's third clause: "a live edit swaps a module without dropping
/// in-flight evaluations".
///
/// The first call is caught in flight without a timer: the module's `spin` runs for
/// longer than the first millisecond of a call, so it is suspended once and the rest
/// of it handed to the host's hand-off, which here waits until the test has loaded
/// the new rules and used them.
#[test]
fn a_reload_swaps_a_module_and_a_call_in_flight_finishes_on_the_old_one() {
    let dir = tempfile::tempdir().unwrap();
    let old_sha = place(
        dir.path(),
        "v1.wasm",
        &test_module::plugin_spinning("1", b"j1", 20_000_000),
    );
    let new_sha = place(dir.path(), "v2.wasm", &test_module::plugin("2", b"j2"));
    let (entered_tx, entered) = mpsc::channel::<()>();
    let (release, released) = mpsc::channel::<()>();
    let released = std::sync::Mutex::new(released);
    // Set when the evaluation to hold is about to start.
    let hold = Arc::new(AtomicBool::new(false));
    let first = hold.clone();
    let functions = host(ModuleSettings {
        environment: Vec::new(),
        instances: 2,
        hand_off: Some(Arc::new(move |rest: &mut dyn FnMut()| {
            // Only the evaluation the test is holding: any other runs straight on.
            if first.swap(false, Ordering::SeqCst) {
                entered_tx.send(()).unwrap();
                released
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_mins(1))
                    .expect("the test lets the held call go on");
            }
            rest();
        })),
    })
    .beside(&dir.path().join("rules.toml"));
    let rules = rule("slow", "SELECT spin() AS v FROM \"slow\"")
        + &rule("quick", "SELECT which() AS v FROM \"quick\"");
    let declare = |file: &str, sha: &str| {
        table("m", file, sha, &["spin", "which"], "timeout_ms = 10000") + &rules
    };
    let host = functions.module_host().unwrap().clone();

    let old = Arc::new(
        RuleSet::parse_with(&declare("v1.wasm", &old_sha), &functions)
            .unwrap()
            .rules,
    );
    assert_eq!(publish(&old, "quick", b"1").lines, ["{\"v\":1}"]);

    // One evaluation on the old rules, held in the middle of its call.
    hold.store(true, Ordering::SeqCst);
    let in_flight = {
        let old = old.clone();
        std::thread::spawn(move || publish(&old, "slow", b"1"))
    };
    entered
        .recv_timeout(Duration::from_mins(1))
        .expect("the slow call outlasts its first millisecond");
    assert_eq!(old.modules()[0].stats().in_call, 1);

    // The live edit: the same file with another module pinned. It loads beside the
    // old one, and what is evaluated from here on runs on it.
    let new = Arc::new(
        RuleSet::parse_with(&declare("v2.wasm", &new_sha), &functions)
            .unwrap()
            .rules,
    );
    assert!(!Arc::ptr_eq(&old.modules()[0], &new.modules()[0]));
    assert_eq!(host.loaded_modules(), 2, "the old one is still in a call");
    assert_eq!(publish(&new, "quick", b"1").lines, ["{\"v\":2}"]);
    assert_eq!(new.modules()[0].version(), "2");
    // The broker lets go of the old rules; the evaluation that started on them has not.
    drop(old);
    assert_eq!(host.loaded_modules(), 2);

    release.send(()).unwrap();
    let seen = in_flight.join().unwrap();
    assert_eq!(seen.failures, Vec::<String>::new());
    assert_eq!(
        seen.lines,
        ["{\"v\":1}"],
        "it finished, on the module it began on"
    );
    assert_eq!(seen.calls[0].error, None);
    // With its last evaluation done the old module is gone.
    assert_eq!(host.loaded_modules(), 1);

    // A reload that leaves the declaration as it was keeps the loaded module and its
    // warm instances; one that changes a limit loads it anew.
    let again = RuleSet::parse_with(&declare("v2.wasm", &new_sha), &functions)
        .unwrap()
        .rules;
    assert!(Arc::ptr_eq(&again.modules()[0], &new.modules()[0]));
    assert_eq!(host.loaded_modules(), 1);
    let changed = RuleSet::parse_with(
        &(table(
            "m",
            "v2.wasm",
            &new_sha,
            &["spin", "which"],
            "timeout_ms = 50",
        ) + &rules),
        &functions,
    )
    .unwrap()
    .rules;
    assert!(!Arc::ptr_eq(&changed.modules()[0], &new.modules()[0]));
    // And a file that changed on disk under an unchanged rules file fails the load:
    // the pin is checked on every load, not only the first.
    std::fs::write(dir.path().join("v2.wasm"), test_module::plugin("3", b"j3")).unwrap();
    let error = load_error(&declare("v2.wasm", &new_sha), &functions);
    assert!(
        error.contains("the file does not have the sha256 the table pins"),
        "{error}"
    );
    assert_eq!(publish(&new, "quick", b"1").lines, ["{\"v\":2}"]);
}

/// `mqttd --rule-test` runs one statement: it may call the functions of the modules
/// the configured rules file declares.
#[test]
fn a_statement_on_its_own_calls_the_modules_a_rules_file_declares() {
    let dir = tempfile::tempdir().unwrap();
    let sha = place(dir.path(), "t.wasm", &test_module::plugin("1", b"j5"));
    let functions = functions_in(dir.path());
    let sql = "SELECT which() + 1 AS n FROM \"t/#\"";
    let payload = Bytes::from_static(b"{}");
    let props = mqtt_core::AppProperties::default();
    let input = PublishInput::new("c", "t/1", &payload, 0, &props);
    assert!(test_sql_with(sql, &input, &functions)
        .unwrap_err()
        .contains("unknown function which()"));
    let declaring = functions
        .declaring(&table("m", "t.wasm", &sha, &["which"], ""))
        .unwrap();
    assert_eq!(
        test_sql_with(sql, &input, &declaring).unwrap(),
        ["{\"n\":6}"]
    );
    assert!(statement_sources_with(sql, &declaring).is_ok());
    let error = functions
        .declaring(&table("m", "t.wasm", &"0".repeat(64), &["which"], ""))
        .expect_err("a wrong pin");
    assert!(matches!(error, LoadError::Function { .. }), "{error}");
}

/// What the modules of one name have given up is counted on across a reload that
/// changes the module: `mqttd_rule_module_instances_replaced_total{module}` is one
/// series per name, and must not start again, or stand still, when its module is
/// replaced by another.
#[test]
fn a_names_count_of_replaced_instances_goes_on_when_its_module_changes() {
    let dir = tempfile::tempdir().unwrap();
    let sha = place(
        dir.path(),
        "slow.wasm",
        &test_module::plugin_spinning("1", b"j1", 20_000_000),
    );
    let functions = functions_in(dir.path());
    let with = |timeout: &str| {
        let text = table("m", "slow.wasm", &sha, &["spin"], timeout)
            + &rule("r", "SELECT spin() AS v FROM \"slow\"");
        RuleSet::parse_with(&text, &functions).unwrap().rules
    };
    let first = with("timeout_ms = 1");
    assert_eq!(publish(&first, "slow", b"1").failures.len(), 1);
    assert_eq!(first.modules()[0].stats().replaced, 1);
    let second = with("timeout_ms = 2");
    assert!(!Arc::ptr_eq(&first.modules()[0], &second.modules()[0]));
    assert_eq!(second.modules()[0].stats().replaced, 1, "carried over");
    assert_eq!(publish(&second, "slow", b"1").failures.len(), 1);
    assert_eq!(second.modules()[0].stats().replaced, 2);
    assert_eq!(first.modules()[0].stats().replaced, 1);
}
