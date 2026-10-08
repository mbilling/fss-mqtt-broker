//! Every `MQTTD_*` variable a shipped compose file sets is one something reads: the broker
//! (`mqtt_config::ENV_VARS`) or the admin CLI (`mqttd::admin::cli::CLIENT_ENV_VARS`). A
//! misspelt or retired name is otherwise ignored without a word — the broker does not
//! report environment it does not know — and a demo silently runs without the setting it
//! exists to show (ADR 0084).
//!
//! The files are scanned line by line for environment keys only: `MQTTD_X: value` in an
//! indented map and `- MQTTD_X=value` in a list. Comment lines are skipped, and a name in
//! a value (`${MQTTD_X:-}`) is not a key. No YAML parser: none is in the tree, and the
//! two shapes are all the compose files use.

use std::path::Path;

/// The live rules demo's stack (ADR 0084). It is required like the others: a scan that
/// skipped a missing file would pass without looking.
const RULES_LIVE_COMPOSE: &str = "demo/rules-live/compose.yaml";

/// The compose files scanned, from the repository root.
const COMPOSE_FILES: &[&str] = &[
    "demo/docker-compose.yml",
    "bench/docker-compose.yml",
    RULES_LIVE_COMPOSE,
];

/// Fewer keys than this across the files means the scan stopped seeing them (the two
/// files before the live demo set 51).
const AT_LEAST: usize = 40;

/// The environment keys `text` sets, with their 1-based line numbers.
fn env_keys(text: &str) -> Vec<(usize, String)> {
    text.lines()
        .enumerate()
        .filter_map(|(i, line)| {
            let trimmed = line.trim_start();
            if trimmed.starts_with('#') {
                return None;
            }
            let (rest, listed) = match trimmed.strip_prefix('-') {
                Some(item) => (item.trim_start().trim_start_matches(['"', '\'']), true),
                None => (trimmed, false),
            };
            let name: String = rest
                .chars()
                .take_while(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '_')
                .collect();
            if !name.starts_with("MQTTD_") {
                return None;
            }
            let after = rest[name.len()..].trim_start();
            // A map key is indented (it sits under `environment:` or an anchor); a list
            // item is `NAME=value`.
            let key = if listed {
                after.starts_with('=')
            } else {
                after.starts_with(':') && line.starts_with(char::is_whitespace)
            };
            key.then(|| (i + 1, name))
        })
        .collect()
}

fn known(name: &str) -> bool {
    mqtt_config::ENV_VARS.contains(&name) || mqttd::admin::cli::CLIENT_ENV_VARS.contains(&name)
}

/// The gate: every key in every listed file is known, each file sets some, and together
/// they set enough that the scan is plainly working.
#[test]
fn every_mqttd_variable_a_compose_file_sets_is_one_mqttd_reads() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut unknown = Vec::new();
    let mut seen = 0;
    for file in COMPOSE_FILES {
        let text = std::fs::read_to_string(root.join(file))
            .unwrap_or_else(|e| panic!("{file}: {e} (every listed compose file must exist)"));
        let keys = env_keys(&text);
        assert!(
            !keys.is_empty(),
            "{file} sets no MQTTD_* variable this scan can see"
        );
        seen += keys.len();
        unknown.extend(
            keys.into_iter()
                .filter(|(_, name)| !known(name))
                .map(|(line, name)| format!("{file}:{line}: {name}")),
        );
    }
    assert!(
        unknown.is_empty(),
        "MQTTD_* variables nothing reads (not in mqtt_config::ENV_VARS or \
         mqttd::admin::cli::CLIENT_ENV_VARS): {unknown:#?}"
    );
    assert!(seen >= AT_LEAST, "only {seen} keys found across the files");
}

/// The scan reads keys and nothing else: both shapes, quoted list items, not comments,
/// not a name inside a value, not an unindented key.
#[test]
fn the_scan_reads_environment_keys_and_nothing_else() {
    let text = "\
x-env: &env
  MQTTD_PLAINTEXT_BIND: 0.0.0.0:1883   # a comment after the value
  # MQTTD_COMMENTED: 1
services:
  a:
    environment:
      - MQTTD_NODE_ID=a
      - \"MQTTD_QUOTED=1\"
      - OTHER=${MQTTD_IN_A_VALUE:-}
      PLAIN: $MQTTD_ALSO_A_VALUE
MQTTD_TOP_LEVEL: not under a map
      MQTTD_SPACED : 1
";
    let names: Vec<(usize, String)> = env_keys(text);
    assert_eq!(
        names,
        [
            (2, "MQTTD_PLAINTEXT_BIND".to_string()),
            (7, "MQTTD_NODE_ID".to_string()),
            (8, "MQTTD_QUOTED".to_string()),
            (12, "MQTTD_SPACED".to_string()),
        ]
    );
    assert!(known("MQTTD_PLAINTEXT_BIND") && known("MQTTD_ADMIN_URL"));
    assert!(!known("MQTTD_PLAINTEXT_BIN"));
}
