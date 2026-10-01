//! `mqttd --admin <verb> [args]`: the admin API from a terminal (ADR 0081 §3).
//!
//! Each verb is one endpoint; the CLI adds no logic beyond turning arguments into a
//! request and the JSON answer into a table (or, with `--json`, printing it as-is). It
//! runs in the distroless image, where there is no shell and no curl, so
//! `kubectl exec <pod> -- mqttd --admin node` works.
//!
//! Where to connect and with what certificate:
//!
//! | Option | Environment | Default |
//! |---|---|---|
//! | `--url https://host:port` | `MQTTD_ADMIN_URL` | `admin.bind` from the config, wildcard → loopback |
//! | `--ca <pem>` | `MQTTD_ADMIN_CA` | `admin.client_ca` from the config |
//! | `--cert <pem>` | `MQTTD_ADMIN_CLIENT_CERT` | required |
//! | `--key <pem>` | `MQTTD_ADMIN_CLIENT_KEY` | required |
//! | `--server-name <name>` | `MQTTD_ADMIN_SERVER_NAME` | the URL's host |
//!
//! These variables configure the client, not the broker, so they are not part of the
//! `MQTTD_*` config surface in `docs/CONFIGURATION.md`.

use super::client::{self, Target};
use super::http::percent_encode;
use serde_json::Value;
use std::fmt::Write;
use std::path::Path;
use std::time::Duration;

/// How long the CLI waits for an answer.
const TIMEOUT: Duration = Duration::from_secs(30);

/// One CLI verb: the endpoint it calls, its positional arguments and its optional
/// `--name value` parameters (both become query parameters).
struct Verb {
    name: &'static str,
    method: &'static str,
    path: &'static str,
    /// Required positional arguments, in order, by query-parameter name.
    required: &'static [&'static str],
    /// Optional `--<name> <value>` query parameters.
    optional: &'static [&'static str],
    help: &'static str,
}

const VERBS: &[Verb] = &[
    Verb {
        name: "whoami",
        method: "GET",
        path: "/admin/v1/whoami",
        required: &[],
        optional: &[],
        help: "the certificate subject and role the broker sees for you",
    },
    Verb {
        name: "node",
        method: "GET",
        path: "/admin/v1/node",
        required: &[],
        optional: &[],
        help: "this node's state (the /statusz body)",
    },
    Verb {
        name: "cluster",
        method: "GET",
        path: "/admin/v1/cluster",
        required: &[],
        optional: &[],
        help: "every node's version, readiness, identity and lag, from any node",
    },
    Verb {
        name: "placement",
        method: "GET",
        path: "/admin/v1/placement",
        required: &[],
        optional: &[],
        help: "this node's membership, replication and lease view; do the others agree",
    },
    Verb {
        name: "config",
        method: "GET",
        path: "/admin/v1/config",
        required: &[],
        optional: &[],
        help: "the effective config (secrets fingerprinted) and the file checksum",
    },
    Verb {
        name: "reload",
        method: "POST",
        path: "/admin/v1/reload",
        required: &[],
        optional: &[],
        help: "operator: reload the config file, as SIGHUP does, and report the outcome",
    },
    Verb {
        name: "log-level",
        method: "GET",
        path: "/admin/v1/log-level",
        required: &[],
        optional: &[],
        help: "the configured log filter and any temporary override",
    },
    Verb {
        name: "log-override",
        method: "POST",
        path: "/admin/v1/log-level",
        required: &["filter"],
        optional: &["ttl"],
        help: "operator: log with <filter> for --ttl seconds (default 600, max 3600)",
    },
    Verb {
        name: "log-reset",
        method: "POST",
        path: "/admin/v1/log-level/reset",
        required: &[],
        optional: &[],
        help: "operator: restore the configured log filter now",
    },
    Verb {
        name: "cordon",
        method: "POST",
        path: "/admin/v1/cordon",
        required: &[],
        optional: &[],
        help: "operator: refuse new connections and report not-ready (not persisted)",
    },
    Verb {
        name: "uncordon",
        method: "POST",
        path: "/admin/v1/uncordon",
        required: &[],
        optional: &[],
        help: "operator: accept new connections again",
    },
    Verb {
        name: "kick",
        method: "POST",
        path: "/admin/v1/kick",
        required: &["client"],
        optional: &[],
        help: "operator: disconnect a client (MQTT 5: 0x98); its session stays",
    },
    Verb {
        name: "purge",
        method: "POST",
        path: "/admin/v1/purge",
        required: &["client"],
        optional: &[],
        help: "operator: disconnect a client and delete its session and queue",
    },
    Verb {
        name: "authz",
        method: "GET",
        path: "/admin/v1/authz",
        required: &["user", "action", "target"],
        optional: &["groups", "client"],
        help: "dry run: may <user> publish|subscribe|connect <target>, and which rule decides",
    },
    Verb {
        name: "clients",
        method: "GET",
        path: "/admin/v1/clients",
        required: &[],
        optional: &["prefix", "user", "source", "limit", "cursor"],
        help: "sessions by client id (paged); --all-nodes: on every node",
    },
    Verb {
        name: "session",
        method: "GET",
        path: "/admin/v1/session",
        required: &["client"],
        optional: &[],
        help: "one session: subscriptions, in flight, backlog, will, owner; --all-nodes: wherever it is",
    },
    Verb {
        name: "subscribers",
        method: "GET",
        path: "/admin/v1/subscribers",
        required: &["topic"],
        optional: &["limit"],
        help: "who would receive a publish to <topic>; --all-nodes: on every node",
    },
    Verb {
        name: "backlog",
        method: "GET",
        path: "/admin/v1/backlog",
        required: &[],
        optional: &["top"],
        help: "the sessions with the most messages waiting",
    },
    Verb {
        name: "retained",
        method: "GET",
        path: "/admin/v1/retained",
        required: &[],
        optional: &["prefix", "limit", "cursor"],
        help: "retained messages by topic prefix: count, bytes, list (paged)",
    },
];

/// The verbs that take `--all-nodes` (`scope=cluster`): ask every node and merge.
const ALL_NODES_VERBS: &[&str] = &["clients", "session", "subscribers"];

/// Options that take a value and apply to every verb.
const GLOBAL_OPTIONS: &[&str] = &[
    "--url",
    "--ca",
    "--cert",
    "--key",
    "--server-name",
    "--config",
];

/// A parsed invocation.
#[derive(Debug, Default)]
struct Invocation {
    verb: &'static str,
    /// Query parameters, in order.
    params: Vec<(String, String)>,
    /// Global options given on the command line.
    options: Vec<(String, String)>,
    json: bool,
}

impl Invocation {
    fn option(&self, name: &str) -> Option<&str> {
        self.options
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// Validate the arguments after `--admin`, without touching config, network or
/// environment (the #169/#544 rule: a malformed invocation never reaches a broker).
///
/// # Errors
/// A usage message.
pub fn validate(args: &[String]) -> Result<(), String> {
    parse(args).map(|_| ())
}

fn parse(args: &[String]) -> Result<Invocation, String> {
    let (first, rest) = args
        .split_first()
        .ok_or_else(|| format!("--admin needs a verb: {}", verb_names()))?;
    if first == "help" || first == "--help" || first == "-h" {
        return Ok(Invocation {
            verb: "help",
            ..Invocation::default()
        });
    }
    let verb = VERBS
        .iter()
        .find(|v| v.name == first)
        .ok_or_else(|| format!("unknown admin verb {first:?}; verbs: {}", verb_names()))?;
    let mut inv = Invocation {
        verb: verb.name,
        ..Invocation::default()
    };
    let mut positional = 0usize;
    let mut tokens = rest.iter();
    while let Some(arg) = tokens.next() {
        if arg == "--json" {
            if std::mem::replace(&mut inv.json, true) {
                return Err("repeated option: --json".to_string());
            }
            continue;
        }
        if arg == "--all-nodes" {
            if !ALL_NODES_VERBS.contains(&verb.name) {
                return Err(format!(
                    "--all-nodes is not an option of --admin {}",
                    verb.name
                ));
            }
            if inv.params.iter().any(|(k, _)| k == "scope") {
                return Err("repeated option: --all-nodes".to_string());
            }
            inv.params
                .push(("scope".to_string(), "cluster".to_string()));
            continue;
        }
        if let Some(name) = arg.strip_prefix("--") {
            let is_global = GLOBAL_OPTIONS.contains(&arg.as_str());
            if !is_global && !verb.optional.contains(&name) {
                return Err(format!("{arg} is not an option of --admin {}", verb.name));
            }
            let value = tokens
                .next()
                .filter(|v| !v.is_empty() && !v.starts_with("--"))
                .ok_or_else(|| format!("{arg} requires a value"))?;
            let seen = inv.options.iter().any(|(k, _)| k == arg)
                || inv.params.iter().any(|(k, _)| k == name);
            if seen {
                return Err(format!("repeated option: {arg}"));
            }
            if is_global {
                inv.options.push((arg.clone(), value.clone()));
            } else {
                inv.params.push((name.to_string(), value.clone()));
            }
            continue;
        }
        let Some(name) = verb.required.get(positional) else {
            return Err(format!(
                "unexpected argument {arg:?} for --admin {}",
                verb.name
            ));
        };
        inv.params.push(((*name).to_string(), arg.clone()));
        positional += 1;
    }
    if positional < verb.required.len() {
        return Err(format!(
            "--admin {} needs: {}",
            verb.name,
            verb.required.join(" ")
        ));
    }
    Ok(inv)
}

fn verb_names() -> String {
    VERBS.iter().map(|v| v.name).collect::<Vec<_>>().join(", ")
}

fn usage() -> String {
    let mut s = String::from(
        "USAGE: mqttd --admin <verb> [args] [--json] [--url https://host:port] [--ca <pem>] \
         [--cert <pem>] [--key <pem>] [--server-name <name>] [--config <path>]\n\nVERBS:\n",
    );
    for v in VERBS {
        let mut shape = v.name.to_string();
        for r in v.required {
            let _ = write!(shape, " <{r}>");
        }
        for o in v.optional {
            let _ = write!(shape, " [--{o} <v>]");
        }
        if ALL_NODES_VERBS.contains(&v.name) {
            shape.push_str(" [--all-nodes]");
        }
        let _ = writeln!(s, "  {shape:<44} {}", v.help);
    }
    s.push_str(
        "\nENVIRONMENT: MQTTD_ADMIN_URL, MQTTD_ADMIN_CA, MQTTD_ADMIN_CLIENT_CERT, \
         MQTTD_ADMIN_CLIENT_KEY, MQTTD_ADMIN_SERVER_NAME (the options win).\n",
    );
    s
}

/// A client-side setting from the environment (non-empty only).
fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// The broker's config, for the `admin.bind` / `admin.client_ca` defaults. Like
/// `--probe`, a config that would refuse to boot falls back to defaults + env: this is a
/// client of an already-running broker, not a second one.
fn broker_config(explicit: Option<&str>) -> Option<mqtt_config::Config> {
    let path = explicit.map(String::from).or_else(|| env("MQTTD_CONFIG"));
    mqtt_config::Config::load(path.as_deref().map(Path::new))
        .ok()
        .or_else(|| {
            let mut c = mqtt_config::Config::default();
            c.overlay_env().ok()?;
            Some(c)
        })
}

/// Resolve where to connect and with what material.
fn target(inv: &Invocation) -> Result<Target, String> {
    let config = broker_config(inv.option("--config"));
    let addr = match inv
        .option("--url")
        .map(String::from)
        .or_else(|| env("MQTTD_ADMIN_URL"))
    {
        Some(url) => client::parse_url(&url)?,
        None => config
            .as_ref()
            .and_then(|c| c.admin.bind.clone())
            .map(|b| {
                b.replace("0.0.0.0:", "127.0.0.1:")
                    .replace("[::]:", "[::1]:")
            })
            .ok_or(
                "no admin URL: pass --url https://host:port or set MQTTD_ADMIN_URL \
                    (or admin.bind in the broker config)",
            )?,
    };
    let ca = inv
        .option("--ca")
        .map(String::from)
        .or_else(|| env("MQTTD_ADMIN_CA"))
        .or_else(|| config.as_ref().and_then(|c| c.admin.client_ca.clone()))
        .ok_or("no CA to verify the server with: pass --ca <pem> or set MQTTD_ADMIN_CA")?;
    let cert = inv
        .option("--cert")
        .map(String::from)
        .or_else(|| env("MQTTD_ADMIN_CLIENT_CERT"))
        .ok_or("no client certificate: pass --cert <pem> or set MQTTD_ADMIN_CLIENT_CERT")?;
    let key = inv
        .option("--key")
        .map(String::from)
        .or_else(|| env("MQTTD_ADMIN_CLIENT_KEY"))
        .ok_or("no client key: pass --key <pem> or set MQTTD_ADMIN_CLIENT_KEY")?;
    let connector =
        mqtt_net::tls::client_connector(Path::new(&ca), Path::new(&cert), Path::new(&key))
            .map_err(|e| e.to_string())?;
    let server_name = inv
        .option("--server-name")
        .map(String::from)
        .or_else(|| env("MQTTD_ADMIN_SERVER_NAME"))
        .unwrap_or_else(|| client::host_of(&addr));
    Ok(Target {
        addr,
        server_name,
        connector,
        timeout: TIMEOUT,
    })
}

/// Run `mqttd --admin …` (the arguments after `--admin`); returns the exit code: 0 on
/// success, 1 when the broker refused or could not be reached, 2 on a usage error.
pub async fn run(args: &[String]) -> i32 {
    let inv = match parse(args) {
        Ok(inv) => inv,
        Err(e) => {
            eprintln!("mqttd: {e}");
            eprintln!("Try 'mqttd --admin help'.");
            return 2;
        }
    };
    if inv.verb == "help" {
        print!("{}", usage());
        return 0;
    }
    let target = match target(&inv) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("mqttd: {e}");
            return 2;
        }
    };
    let Some(verb) = VERBS.iter().find(|v| v.name == inv.verb) else {
        return 2;
    };
    let mut path = verb.path.to_string();
    for (i, (k, v)) in inv.params.iter().enumerate() {
        path.push(if i == 0 { '?' } else { '&' });
        path.push_str(&percent_encode(k));
        path.push('=');
        path.push_str(&percent_encode(v));
    }
    match client::call(&target, verb.method, &path, None).await {
        Ok((status, body)) => {
            let value: Value = serde_json::from_str(&body).unwrap_or(Value::String(body));
            if (200..300).contains(&status) {
                if inv.json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&value).unwrap_or_default()
                    );
                } else {
                    print!("{}", render(&value));
                }
                0
            } else {
                // `--json` gets the whole answer, refusals included (a rejected reload
                // carries its `outcome`); exit 1 still says it was refused.
                if inv.json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&value).unwrap_or_default()
                    );
                    return 1;
                }
                let code = value
                    .pointer("/error/code")
                    .and_then(Value::as_str)
                    .unwrap_or("error");
                let message = value
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                eprintln!("mqttd: {status} {code}: {message}");
                if let Some(outcome) = value.get("outcome") {
                    print!("{}", render(outcome));
                }
                1
            }
        }
        Err(e) => {
            eprintln!("mqttd: {e}");
            1
        }
    }
}

/// Render an answer for a terminal: scalars as `key  value` lines (nested keys joined with
/// `.`), then each array of objects as a table under its key.
#[must_use]
pub fn render(value: &Value) -> String {
    let mut out = String::new();
    match value {
        Value::Object(map) => {
            let mut scalars = Vec::new();
            let mut tables = Vec::new();
            for (k, v) in map {
                match v {
                    Value::Array(items)
                        if items.iter().all(Value::is_object) && !items.is_empty() =>
                    {
                        tables.push((k, table(items)));
                    }
                    _ => flatten(k, v, &mut scalars),
                }
            }
            let width = scalars.iter().map(|(k, _)| k.len()).max().unwrap_or(0);
            for (k, v) in scalars {
                let _ = writeln!(out, "{k:<width$}  {v}");
            }
            for (k, t) in tables {
                if !out.is_empty() {
                    out.push('\n');
                }
                let _ = writeln!(out, "{k}:");
                out.push_str(&t);
            }
        }
        Value::Array(items) if items.iter().all(Value::is_object) => out.push_str(&table(items)),
        other => {
            let _ = writeln!(out, "{}", scalar(other));
        }
    }
    out
}

fn flatten(prefix: &str, value: &Value, out: &mut Vec<(String, String)>) {
    match value {
        Value::Object(map) if !map.is_empty() => {
            for (k, v) in map {
                flatten(&format!("{prefix}.{k}"), v, out);
            }
        }
        other => out.push((prefix.to_string(), scalar(other))),
    }
}

fn scalar(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => "-".to_string(),
        Value::Array(items) if items.iter().all(|i| !i.is_object() && !i.is_array()) => {
            items.iter().map(scalar).collect::<Vec<_>>().join(", ")
        }
        other => other.to_string(),
    }
}

/// A fixed-width table over the union of the rows' keys, in first-seen order.
fn table(rows: &[Value]) -> String {
    let mut columns: Vec<String> = Vec::new();
    for row in rows {
        if let Value::Object(map) = row {
            for k in map.keys() {
                if !columns.contains(k) {
                    columns.push(k.clone());
                }
            }
        }
    }
    let cells: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .map(|c| row.get(c).map_or_else(|| "-".to_string(), scalar))
                .collect()
        })
        .collect();
    let widths: Vec<usize> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            cells
                .iter()
                .map(|r| r[i].len())
                .chain([c.len()])
                .max()
                .unwrap_or(0)
        })
        .collect();
    let line = |values: Vec<String>| -> String {
        let mut s = values
            .iter()
            .zip(&widths)
            .map(|(v, w)| format!("{v:<w$}"))
            .collect::<Vec<_>>()
            .join("  ");
        s.truncate(s.trim_end().len());
        s.push('\n');
        s
    };
    let mut out = line(columns.iter().map(|c| c.to_uppercase()).collect());
    for row in cells {
        out.push_str(&line(row));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn args(s: &str) -> Vec<String> {
        s.split_whitespace().map(String::from).collect()
    }

    #[test]
    fn invocations_are_validated_before_anything_runs() {
        assert!(validate(&args("node")).is_ok());
        assert!(validate(&args("node --json --url https://h:1")).is_ok());
        assert!(validate(&args("help")).is_ok());
        assert!(validate(&args("")).is_err());
        assert!(validate(&args("nope")).is_err());
        assert!(validate(&args("node extra")).is_err());
        assert!(validate(&args("node --url")).is_err());
        assert!(validate(&args("node --url https://a:1 --url https://b:1")).is_err());
        assert!(validate(&args("node --bogus x")).is_err());
        assert!(validate(&args("node --json --json")).is_err());
    }

    #[test]
    fn all_nodes_asks_for_the_cluster_scope_on_the_verbs_that_have_one() {
        let inv = parse(&args("session dev-1 --all-nodes")).unwrap();
        assert_eq!(
            inv.params,
            [
                ("client".to_string(), "dev-1".to_string()),
                ("scope".to_string(), "cluster".to_string())
            ]
        );
        assert!(validate(&args("clients --all-nodes --limit 5")).is_ok());
        assert!(validate(&args("subscribers t --all-nodes")).is_ok());
        assert!(validate(&args("clients --all-nodes --all-nodes")).is_err());
        assert!(validate(&args("node --all-nodes")).is_err());
        assert!(validate(&args("kick dev-1 --all-nodes")).is_err());
    }

    #[test]
    fn answers_render_as_tables_and_lines() {
        let table = render(&json!({
            "clients": [{"client_id": "a", "node": "n1"}, {"client_id": "bb", "node": "n2"}],
            "next_cursor": null
        }));
        assert_eq!(
            table,
            "next_cursor  -\n\nclients:\nCLIENT_ID  NODE\na          n1\nbb         n2\n"
        );
        let lines = render(&json!({"node_id": "n1", "store": {"shards": 1}, "keys": ["a", "b"]}));
        assert_eq!(
            lines,
            "keys          a, b\nnode_id       n1\nstore.shards  1\n"
        );
    }
}
