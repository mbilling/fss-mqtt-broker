//! Resolving a task's environment: manifest defaults, then the user's overrides.
//!
//! Split out because both the headless runner and the UI need the same answer, and two
//! implementations of "what environment does this task actually get" would eventually
//! disagree — with the UI showing one thing and the run using another.

use std::collections::BTreeMap;

use crate::manifest::Task;

/// The variables to set for a run: every declared default that is non-empty, overlaid with
/// whatever the user changed.
///
/// An empty value means **leave it unset** rather than set-to-empty — several of these
/// scripts test `[[ -z "$MQTTD_BIN" ]]` to decide whether to build, and an empty string
/// would be indistinguishable from an unset variable to them anyway. Being explicit about
/// it keeps the UI honest: it shows `(unset)`, and that is what happens.
#[must_use]
pub fn resolve(task: &Task, overrides: &BTreeMap<String, String>) -> Vec<(String, String)> {
    task.env
        .iter()
        .map(|declared| {
            let value = overrides
                .get(&declared.name)
                .unwrap_or(&declared.default)
                .clone();
            (declared.name.clone(), value)
        })
        .filter(|(_, v)| !v.is_empty())
        .collect()
}

/// A task's command-line arguments, with `${VAR}` replaced by the resolved value.
///
/// An argument that references a variable with no value is **dropped entirely**, not passed
/// as an empty string — an empty positional argument becomes a path of "" and produces an
/// error about a file that was never named, which is a worse thing to hand someone than the
/// script's own usage message.
#[must_use]
pub fn args(task: &Task, overrides: &BTreeMap<String, String>) -> Vec<String> {
    let resolved: BTreeMap<&str, &str> = task
        .env
        .iter()
        .map(|e| (e.name.as_str(), current(task, overrides, &e.name)))
        .collect();

    task.args
        .iter()
        .filter_map(|arg| {
            let mut out = arg.clone();
            for (name, value) in &resolved {
                let needle = format!("${{{name}}}");
                if out.contains(&needle) {
                    if value.is_empty() {
                        return None;
                    }
                    out = out.replace(&needle, value);
                }
            }
            Some(out)
        })
        .collect()
}

/// The value a field should show: the user's override if there is one, else the default.
#[must_use]
pub fn current<'a>(task: &'a Task, overrides: &'a BTreeMap<String, String>, name: &str) -> &'a str {
    overrides
        .get(name)
        .map(String::as_str)
        .or_else(|| {
            task.env
                .iter()
                .find(|e| e.name == name)
                .map(|e| e.default.as_str())
        })
        .unwrap_or("")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::EnvVar;

    fn task_with(env: Vec<(&str, &str)>) -> Task {
        Task {
            id: "t".into(),
            group: "g".into(),
            name: "n".into(),
            script: "scripts/gen-status.py".into(),
            about: String::new(),
            requires: vec![],
            optional: vec![],
            duration: String::new(),
            caution: None,
            hidden: false,
            needs_checkout: false,
            args: vec![],
            env: env
                .into_iter()
                .map(|(n, d)| EnvVar {
                    name: n.into(),
                    default: d.into(),
                    help: String::new(),
                })
                .collect(),
        }
    }

    #[test]
    fn defaults_apply_and_overrides_win() {
        let t = task_with(vec![("DURATION", "60"), ("SIZE", "256")]);
        let mut over = BTreeMap::new();
        over.insert("DURATION".to_string(), "5".to_string());

        let resolved: BTreeMap<_, _> = resolve(&t, &over).into_iter().collect();
        assert_eq!(resolved["DURATION"], "5", "the override wins");
        assert_eq!(resolved["SIZE"], "256", "the default applies");
    }

    /// An empty value leaves the variable UNSET rather than setting it to "". Several
    /// scripts branch on `-z "$MQTTD_BIN"` to decide whether to build first.
    #[test]
    fn an_empty_value_leaves_the_variable_unset() {
        let t = task_with(vec![("MQTTD_BIN", "")]);
        assert!(resolve(&t, &BTreeMap::new()).is_empty());

        // ...and clearing an overridden value unsets it again.
        let mut over = BTreeMap::new();
        over.insert("MQTTD_BIN".to_string(), String::new());
        assert!(resolve(&t, &over).is_empty());
    }

    /// A declared variable is not automatically an argument — this is the wiring that makes
    /// "passed as the first argument" true rather than merely written down.
    #[test]
    fn arguments_substitute_the_resolved_value() {
        let mut t = task_with(vec![("INPUT", "/etc/mosquitto/mosquitto.conf")]);
        t.args = vec!["${INPUT}".to_string()];
        assert_eq!(
            args(&t, &BTreeMap::new()),
            vec!["/etc/mosquitto/mosquitto.conf"]
        );

        let mut over = BTreeMap::new();
        over.insert("INPUT".to_string(), "/tmp/mine.conf".to_string());
        assert_eq!(args(&t, &over), vec!["/tmp/mine.conf"]);
    }

    /// An unset variable drops its argument rather than passing "" — otherwise the script
    /// reports a problem with a file nobody named.
    #[test]
    fn an_unset_variable_drops_its_argument() {
        let mut t = task_with(vec![("INPUT", "")]);
        t.args = vec!["${INPUT}".to_string()];
        assert!(args(&t, &BTreeMap::new()).is_empty());
    }

    /// What the UI shows must be what the run gets — one function, so they cannot drift.
    #[test]
    fn the_displayed_value_matches_what_would_be_set() {
        let t = task_with(vec![("ONLY", "")]);
        let mut over = BTreeMap::new();
        over.insert("ONLY".to_string(), "mqttd".to_string());
        assert_eq!(current(&t, &over, "ONLY"), "mqttd");
        assert_eq!(
            resolve(&t, &over),
            vec![("ONLY".to_string(), "mqttd".to_string())]
        );
    }
}
