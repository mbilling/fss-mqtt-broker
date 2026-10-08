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
use crate::{out, outln};
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

/// Help lines are wrapped to this width.
const HELP_WIDTH: usize = 100;
/// The column a verb's description starts in.
const HELP_COLUMN: usize = 46;

/// Break `text` into lines of at most `width` characters, at spaces (a longer word gets a
/// line of its own).
fn wrap(text: &str, width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let mut line = String::new();
    for word in text.split_whitespace() {
        if !line.is_empty() && line.len() + 1 + word.len() > width {
            lines.push(std::mem::take(&mut line));
        }
        if !line.is_empty() {
            line.push(' ');
        }
        line.push_str(word);
    }
    if !line.is_empty() {
        lines.push(line);
    }
    lines
}

/// One verb in the help: its shape from column 2, its description from [`HELP_COLUMN`],
/// both wrapped to [`HELP_WIDTH`]. A shape too long for its column gets lines of its own.
fn usage_entry(out: &mut String, shape: &str, help: &str) {
    let shape_lines = wrap(shape, HELP_WIDTH - 4);
    let help_lines = wrap(help, HELP_WIDTH - HELP_COLUMN);
    let pad = " ".repeat(HELP_COLUMN);
    let fits = shape_lines.len() == 1 && shape_lines[0].len() + 3 <= HELP_COLUMN;
    if fits {
        let _ = writeln!(
            out,
            "  {:<w$}{}",
            shape_lines[0],
            help_lines.first().map_or("", String::as_str),
            w = HELP_COLUMN - 2
        );
        for line in help_lines.iter().skip(1) {
            let _ = writeln!(out, "{pad}{line}");
        }
        return;
    }
    for (i, line) in shape_lines.iter().enumerate() {
        let indent = if i == 0 { "  " } else { "      " };
        let _ = writeln!(out, "{indent}{line}");
    }
    for line in &help_lines {
        let _ = writeln!(out, "{pad}{line}");
    }
}

fn usage() -> String {
    let mut s = String::new();
    for (i, line) in wrap(
        "USAGE: mqttd --admin <verb> [args] [--json] [--url https://host:port] [--ca <pem>] \
         [--cert <pem>] [--key <pem>] [--server-name <name>] [--config <path>]",
        HELP_WIDTH - 7,
    )
    .iter()
    .enumerate()
    {
        let indent = if i == 0 { "" } else { "       " };
        let _ = writeln!(s, "{indent}{line}");
    }
    s.push_str("\nVERBS:\n");
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
        usage_entry(&mut s, &shape, v.help);
    }
    s.push('\n');
    for line in wrap(
        "ENVIRONMENT: MQTTD_ADMIN_URL, MQTTD_ADMIN_CA, MQTTD_ADMIN_CLIENT_CERT, \
         MQTTD_ADMIN_CLIENT_KEY, MQTTD_ADMIN_SERVER_NAME (the options win).",
        HELP_WIDTH,
    ) {
        let _ = writeln!(s, "{line}");
    }
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
        out!("{}", usage());
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
                    outln!(
                        "{}",
                        serde_json::to_string_pretty(&value).unwrap_or_default()
                    );
                } else {
                    out!("{}", render_for(inv.verb, &value));
                }
                0
            } else {
                // `--json` gets the whole answer, refusals included (a rejected reload
                // carries its `outcome`); exit 1 still says it was refused.
                if inv.json {
                    outln!(
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
                    out!("{}", render(outcome));
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

/// Render `verb`'s answer for a terminal: the compact cluster view for `cluster`, the
/// generic [`render`] for the rest. `--json` always has the whole answer.
#[must_use]
pub fn render_for(verb: &str, value: &Value) -> String {
    if verb == "cluster" {
        if let Some(out) = render_cluster(value) {
            return out;
        }
    }
    render(value)
}

/// The cluster view in a terminal's width: a summary line, whether the nodes agree, and
/// one short row per node. The full rows (addresses, checksums, the whole cluster id) are
/// in `--json`.
fn render_cluster(value: &Value) -> Option<String> {
    let nodes = value.get("nodes")?.as_array()?;
    let summary = value.get("summary")?;
    let count = |k: &str| summary.get(k).and_then(Value::as_u64).unwrap_or(0);
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{} nodes: {} replied, {} ready (answered by {})",
        count("nodes"),
        count("replied"),
        count("ready"),
        scalar(value.get("answered_by").unwrap_or(&Value::Null)),
    );
    let differ: Vec<&str> = [
        ("same_cluster_id", "cluster id"),
        ("same_version", "version"),
        ("same_config", "config"),
        ("same_membership", "membership"),
    ]
    .iter()
    .filter(|(k, _)| summary.get(*k) == Some(&Value::Bool(false)))
    .map(|(_, label)| *label)
    .collect();
    if differ.is_empty() {
        out.push_str("they agree on cluster id, version, config and membership\n");
    } else {
        let _ = writeln!(out, "they DIFFER on: {}", differ.join(", "));
    }
    out.push('\n');
    let columns = [
        "NODE", "STATE", "LEADER", "EPOCH", "MEMBERS", "LAG", "VERSION", "CLUSTER", "MS", "NOTES",
    ];
    let cells = nodes.iter().map(cluster_row).collect::<Vec<_>>();
    out.push_str(&grid(
        &columns.iter().map(|c| (*c).to_string()).collect::<Vec<_>>(),
        &cells,
    ));
    Some(out)
}

/// One node's compact row: identity, state, lease, size, lag, version, the first 8
/// characters of its cluster id, how long it took to answer, and anything wrong with it.
fn cluster_row(row: &Value) -> Vec<String> {
    let text = |k: &str| row.get(k).map_or_else(|| "-".to_string(), scalar);
    let is = |k: &str| row.get(k) == Some(&Value::Bool(true));
    let replied = is("replied");
    let state = if !replied {
        "no reply"
    } else if is("ready") {
        "ready"
    } else {
        "not ready"
    };
    let mut notes: Vec<String> = Vec::new();
    if replied {
        if row.get("live") == Some(&Value::Bool(false)) {
            notes.push("not live".into());
        }
        for (k, label) in [
            ("quarantined", "quarantined"),
            ("brownout", "brownout"),
            ("swim_isolated", "swim-isolated"),
            ("under_replicated", "under-replicated"),
        ] {
            if is(k) {
                notes.push(label.into());
            }
        }
        if row.get("decommissioning").is_some_and(|d| !d.is_null()) {
            notes.push("decommissioning".into());
        }
    } else {
        notes.push(text("error"));
    }
    let cluster: String = row
        .get("cluster_id")
        .and_then(Value::as_str)
        .map_or_else(|| "-".to_string(), |id| id.chars().take(8).collect());
    let or_dash = |s: String| if replied { s } else { "-".to_string() };
    vec![
        text("node_id"),
        state.to_string(),
        if is("lease_leader") {
            "*".into()
        } else {
            "-".into()
        },
        or_dash(text("lease_epoch")),
        or_dash(text("members")),
        or_dash(text("replica_lag_groups")),
        or_dash(text("version")),
        or_dash(cluster),
        or_dash(text("elapsed_ms")),
        if notes.is_empty() {
            "-".into()
        } else {
            notes.join(", ")
        },
    ]
}

/// Columns that identify a row come first, in this order, whatever the answer; the rest
/// follow in their own order.
const LEADING_COLUMNS: &[&str] = &[
    "node_id",
    "client_id",
    "node",
    "topic",
    "filter",
    "replied",
    "connected",
];

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

/// A fixed-width table over the union of the rows' keys: the [`LEADING_COLUMNS`] present,
/// then the rest in first-seen order.
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
    let lead = |c: &String| LEADING_COLUMNS.iter().position(|l| l == c);
    columns.sort_by_key(|c| lead(c).unwrap_or(LEADING_COLUMNS.len()));
    let cells: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .map(|c| row.get(c).map_or_else(|| "-".to_string(), scalar))
                .collect()
        })
        .collect();
    let headers: Vec<String> = columns.iter().map(|c| c.to_uppercase()).collect();
    grid(&headers, &cells)
}

/// Fixed-width columns: a header line, then one line per row, two spaces apart.
fn grid(columns: &[String], cells: &[Vec<String>]) -> String {
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
    let mut out = line(columns.to_vec());
    for row in cells {
        out.push_str(&line(row.clone()));
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
    fn the_cluster_view_is_compact_and_says_what_is_wrong() {
        let answer = json!({
            "answered_by": "n1",
            "summary": {"nodes": 3, "replied": 2, "ready": 1, "same_cluster_id": true,
                        "same_config": false, "same_membership": true, "same_version": true},
            "nodes": [
                {"node_id": "n1", "replied": true, "ready": true, "live": true,
                 "lease_leader": true, "lease_epoch": 4, "members": 3, "replica_lag_groups": 0,
                 "version": "1.0.18", "cluster_id": "f8995cf8f6cd90e4", "elapsed_ms": 0,
                 "config_checksum": "aaaa", "decommissioning": null},
                {"node_id": "n2", "replied": true, "ready": false, "live": true,
                 "lease_leader": false, "lease_epoch": 4, "members": 3, "replica_lag_groups": 2,
                 "version": "1.0.18", "cluster_id": "f8995cf8f6cd90e4", "elapsed_ms": 41,
                 "brownout": true, "under_replicated": true, "admin_addr": "n2:9443"},
                {"node_id": "n3", "replied": false, "admin_addr": "n3:9443",
                 "error": "connect failed"}
            ]
        });
        let out = render_for("cluster", &answer);
        assert_eq!(
            out,
            "3 nodes: 2 replied, 1 ready (answered by n1)\n\
             they DIFFER on: config\n\
             \n\
             NODE  STATE      LEADER  EPOCH  MEMBERS  LAG  VERSION  CLUSTER   MS  NOTES\n\
             n1    ready      *       4      3        0    1.0.18   f8995cf8  0   -\n\
             n2    not ready  -       4      3        2    1.0.18   f8995cf8  41  brownout, under-replicated\n\
             n3    no reply   -       -      -        -    -        -         -   connect failed\n"
        );
        assert!(out.lines().all(|l| l.len() <= HELP_WIDTH), "{out}");
        // Other verbs keep the generic rendering.
        assert_eq!(render_for("node", &json!({"a": 1})), "a  1\n");
    }

    #[test]
    fn identifying_columns_lead_every_table() {
        let out = render(&json!({
            "sessions": [{"auth": "x", "backlog": 0, "client_id": "c1", "connected": true, "node": "n2"}]
        }));
        assert!(
            out.contains("CLIENT_ID  NODE  CONNECTED  AUTH  BACKLOG"),
            "{out}"
        );
    }

    #[test]
    fn help_wraps_to_the_terminal_width() {
        let help = usage();
        assert!(
            help.lines().all(|l| l.len() <= HELP_WIDTH),
            "a help line is wider than {HELP_WIDTH}:\n{help}"
        );
        // Every verb is still listed, and a long shape keeps its description.
        for v in VERBS {
            assert!(
                help.lines().any(|l| l.trim_start().starts_with(v.name)),
                "{}",
                v.name
            );
        }
        let flowing = help.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(flowing.contains("--all-nodes: on every node"), "{help}");
        assert_eq!(wrap("aa bb cc", 5), ["aa bb", "cc"]);
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
