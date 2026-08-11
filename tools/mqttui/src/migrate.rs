//! Translate a Mosquitto deployment to mqttd configuration, natively (ADR 0056 T10).
//!
//! The one task that is **more useful standalone than in a checkout**: it reads the
//! *user's* `mosquitto.conf`, not ours. As a Python script it needed `python3` and a clone;
//! here it needs neither.
//!
//! Three of the five reviewers in the 2026-08-09 panel named missing migration tooling
//! their single largest blocker, and this is the shortest path from "curious" to "it
//! understands my configuration".
//!
//! ## It is a port, and it must stay one
//!
//! `scripts/migrate/from-mosquitto.py` is not retired: CI already proves *its* output boots
//! the real broker (ADR 0051 T6). Two converters that disagree are worse than one, so this
//! is written to produce **byte-identical** output and a differential test over shared
//! fixtures holds it there.
//!
//! ## What it will and will not do
//!
//! It translates settings with an exact mqttd equivalent, and for everything else it *says
//! so in the output* as a `# TODO(migrate):` comment at the point it belongs. A converter
//! that silently drops a setting is worse than no converter: you would deploy believing the
//! policy came across.

use std::collections::BTreeMap;
use std::fmt::Write as _;

/// A Mosquitto listener with whatever TLS material followed it.
#[derive(Debug, Default)]
struct Listener {
    port: Option<u16>,
    bind: Option<String>,
    tls: BTreeMap<String, String>,
}

/// The accumulated translation.
#[derive(Debug, Default)]
pub struct Conversion {
    /// `section -> key -> already-rendered TOML value`.
    config: BTreeMap<&'static str, Vec<(String, String)>>,
    listeners: Vec<Listener>,
    todos: Vec<String>,
    notes: Vec<String>,
    /// The `acl_file` the config pointed at, if any.
    pub acl_file: Option<String>,
}

impl Conversion {
    fn set(&mut self, section: &'static str, key: &str, value: String) {
        let entry = self.config.entry(section).or_default();
        if let Some(slot) = entry.iter_mut().find(|(k, _)| k == key) {
            slot.1 = value;
        } else {
            entry.push((key.to_string(), value));
        }
    }

    fn todo(&mut self, msg: impl Into<String>) {
        self.todos.push(msg.into());
    }

    /// How many settings had no equivalent.
    #[must_use]
    pub fn todo_count(&self) -> usize {
        self.todos.len()
    }
}

/// Settings with an exact mqttd equivalent: `(section, key, kind)`.
fn direct(name: &str) -> Option<(&'static str, &'static str, &'static str)> {
    Some(match name {
        "max_connections" => ("limits", "max_connections", "int"),
        "max_queued_messages" => ("limits", "max_queued_messages", "int"),
        "max_packet_size" => ("limits", "max_packet_size", "int"),
        "max_inflight_messages" => ("limits", "receive_maximum", "int"),
        "retain_available" => ("limits", "max_retained_messages", "retain"),
        "persistence_location" => ("node", "data_dir", "str"),
        _ => return None,
    })
}

/// Directives that exist in Mosquitto and deliberately have no mqttd equivalent, with the
/// reason. Being explicit about *why* is the point: "unsupported" invites a bug report,
/// "deliberately absent, here is the alternative" does not.
fn no_equivalent(name: &str) -> Option<&'static str> {
    Some(match name {
        "acl_file" => "translated separately into the ACL policy (see --out-acl)",
        "password_file" => {
            "mqttd uses Argon2id password files: set security.password_file to a file of \
             `username:argon2id-hash` lines. mosquitto_passwd hashes are NOT compatible and \
             cannot be converted (they are hashes — the passwords are not recoverable), so \
             each user must be re-hashed from their password: `printf %s '<password>' | \
             mqttd --hash-password <username> >> passwd`"
        }
        "psk_file" => "PSK ciphersuites are not implemented",
        "bridge" => {
            "bridging is a separate process in mqttd (mqtt-bridge) with its own config; see \
             docs/BRIDGE.md"
        }
        "connection" => "bridge connections are configured in mqtt-bridge, not the broker",
        "log_dest" => "mqttd logs to stdout for the container/journal to collect",
        "sys_interval" => "$SYS topics are not implemented; use the Prometheus endpoint",
        "autosave_interval" => "writes are transactional (redb); there is no autosave timer",
        "allow_zero_length_clientid" => {
            "a zero-length client id is accepted with clean session and refused otherwise, \
             per spec; not configurable"
        }
        "plugin" | "auth_plugin" => {
            "there is no plugin API; authentication is JWT/OIDC/mTLS/password"
        }
        _ => return None,
    })
}

const TLS_KEYS: [&str; 6] = [
    "cafile",
    "capath",
    "certfile",
    "keyfile",
    "require_certificate",
    "crlfile",
];

/// Walk `mosquitto.conf`. Listener-scoped keys follow their listener, as Mosquitto does.
#[must_use]
pub fn parse_conf(text: &str) -> Conversion {
    let mut conv = Conversion::default();
    let mut current: Option<usize> = None;

    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.splitn(2, char::is_whitespace);
        let key = parts.next().unwrap_or_default();
        let value = parts.next().unwrap_or_default().trim().to_string();

        if key == "listener" {
            let mut bits = value.split_whitespace();
            let port = bits.next().and_then(|p| p.parse().ok());
            let bind = bits.next().map(ToString::to_string);
            conv.listeners.push(Listener {
                port,
                bind,
                tls: BTreeMap::new(),
            });
            current = Some(conv.listeners.len() - 1);
            continue;
        }

        if TLS_KEYS.contains(&key) {
            // TLS material belongs to the listener it follows; before any listener it is
            // the default one.
            let idx = if let Some(i) = current {
                i
            } else {
                conv.listeners.push(Listener::default());
                current = Some(conv.listeners.len() - 1);
                conv.listeners.len() - 1
            };
            conv.listeners[idx].tls.insert(key.to_string(), value);
            continue;
        }

        match key {
            "allow_anonymous" => {
                if matches!(value.to_lowercase().as_str(), "true" | "yes" | "1") {
                    conv.set("security", "allow_anonymous", "true".into());
                    conv.notes.push(
                        "allow_anonymous was TRUE in mosquitto.conf and has been carried \
                         over — but mqttd defaults it OFF, and anonymous access is how most \
                         broker exposure incidents start. Turn it off unless you are certain."
                            .into(),
                    );
                }
            }
            "acl_file" => conv.acl_file = Some(value),
            "persistence" => {
                if matches!(value.to_lowercase().as_str(), "true" | "yes" | "1") {
                    conv.notes.push(
                        "persistence was on: set node.data_dir (below) and mount a volume, \
                         or durable state is kept in memory only"
                            .into(),
                    );
                }
            }
            _ => {
                if let Some((section, mkey, kind)) = direct(key) {
                    match kind {
                        "int" => conv.set(section, mkey, value),
                        "retain" => {
                            if matches!(value.to_lowercase().as_str(), "false" | "no" | "0") {
                                conv.todo(
                                    "retain_available=false disables retained messages \
                                     entirely; mqttd has no off switch — cap it instead with \
                                     limits.max_retained_messages, or deny retained topics \
                                     in the ACL",
                                );
                            }
                        }
                        _ => conv.set(section, mkey, format!("\"{value}\"")),
                    }
                } else if let Some(why) = no_equivalent(key) {
                    conv.todo(format!("{key}: {why}"));
                } else {
                    conv.todo(format!(
                        "{key}: no direct equivalent — check the mqttd configuration table"
                    ));
                }
            }
        }
    }
    conv
}

/// One translated ACL rule.
#[derive(Debug)]
pub struct Rule {
    identities: Vec<String>,
    actions: Vec<&'static str>,
    effect: &'static str,
    topics: Vec<String>,
}

/// Translate a Mosquitto ACL file.
///
/// Mosquitto's model is **positional**: a `user X` line opens a block and `topic` lines
/// belong to it until the next `user`. `pattern` lines apply to everyone, with
/// substitution. mqttd's model is a list of rules with explicit identities, so this is a
/// regrouping rather than a line-for-line map.
#[must_use]
pub fn parse_acl(text: &str) -> (Vec<Rule>, Vec<String>) {
    let mut rules = Vec::new();
    let mut todos: Vec<String> = Vec::new();
    let mut current_user: Option<String> = None;

    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.splitn(2, char::is_whitespace);
        let key = parts.next().unwrap_or_default();
        let rest = parts.next().unwrap_or_default().trim();

        match key {
            "user" => current_user = Some(rest.to_string()),
            "topic" => {
                let (access, topic) = split_access(rest);
                push_rule(&mut rules, &mut todos, current_user.clone(), access, topic);
            }
            "pattern" => {
                let (access, topic) = split_access(rest);
                // %u -> %i (mqttd's identity); %c means the same thing in both.
                let converted = topic.replace("%u", "%i");
                if topic.contains("%c") {
                    todos.push(format!(
                        "pattern '{topic}' uses %c (client id). mqttd supports %c, but its \
                         substitutions FAIL CLOSED on a value containing / + or # — verify \
                         your client ids do not."
                    ));
                }
                push_rule(&mut rules, &mut todos, None, access, &converted);
            }
            _ => todos.push(format!("unrecognised ACL line: '{line}'")),
        }
    }
    (rules, todos)
}

/// Mosquitto's access word is optional; omitted means `readwrite`.
fn split_access(rest: &str) -> (&str, &str) {
    let mut bits = rest.splitn(2, char::is_whitespace);
    let first = bits.next().unwrap_or_default();
    match bits.next() {
        Some(topic) if matches!(first, "read" | "write" | "readwrite" | "deny") => {
            (first, topic.trim())
        }
        _ => ("readwrite", rest),
    }
}

fn push_rule(
    rules: &mut Vec<Rule>,
    todos: &mut Vec<String>,
    identity: Option<String>,
    access: &str,
    topic: &str,
) {
    let identities = identity.map(|i| vec![i]).unwrap_or_default();
    match access {
        "deny" => rules.push(Rule {
            identities,
            actions: vec!["publish", "subscribe"],
            effect: "deny",
            topics: vec![topic.to_string()],
        }),
        "read" => rules.push(Rule {
            identities,
            actions: vec!["subscribe"],
            effect: "allow",
            topics: vec![topic.to_string()],
        }),
        "write" => rules.push(Rule {
            identities,
            actions: vec!["publish"],
            effect: "allow",
            topics: vec![topic.to_string()],
        }),
        "readwrite" => rules.push(Rule {
            identities,
            actions: vec!["publish", "subscribe"],
            effect: "allow",
            topics: vec![topic.to_string()],
        }),
        other => todos.push(format!("unknown access type '{other}' for topic '{topic}'")),
    }
}

/// Render the translated ACL policy.
#[must_use]
pub fn render_acl(rules: &[Rule], todos: &[String]) -> String {
    let mut out: Vec<String> = vec![
        "# Translated from a Mosquitto acl_file by the mqttd Mosquitto converter".into(),
        "# (`mqttui migrate mosquitto`, or scripts/migrate/from-mosquitto.py).".into(),
        "#".into(),
        "# Mosquitto is positional (a `user` line opens a block); mqttd is a list of".into(),
        "# explicit rules. Read this through before deploying it — a converted policy".into(),
        "# is a draft, not an authority.".into(),
        "#".into(),
        "# mqttd is DENY BY DEFAULT: anything not allowed below is refused.".into(),
        String::new(),
        "default = \"deny\"".into(),
        String::new(),
    ];
    for t in todos {
        out.push(format!("# TODO(migrate): {t}"));
    }
    if !todos.is_empty() {
        out.push(String::new());
    }
    for r in rules {
        out.push("[[rules]]".into());
        if r.identities.is_empty() {
            out.push("# (no identities = applies to every authenticated client)".into());
        } else {
            let ids = r
                .identities
                .iter()
                .map(|i| format!("\"{i}\""))
                .collect::<Vec<_>>()
                .join(", ");
            out.push(format!("identities = [{ids}]"));
        }
        let acts = r
            .actions
            .iter()
            .map(|a| format!("\"{a}\""))
            .collect::<Vec<_>>()
            .join(", ");
        out.push(format!("actions = [{acts}]"));
        out.push(format!("effect = \"{}\"", r.effect));
        let tps = r
            .topics
            .iter()
            .map(|t| format!("\"{t}\""))
            .collect::<Vec<_>>()
            .join(", ");
        out.push(format!("topics = [{tps}]"));
        out.push(String::new());
    }
    out.join("\n") + "\n"
}

/// The listeners section of the rendered config — split out of [`render_config`] purely
/// for size; the output bytes are pinned by the differential test against the Python
/// original, so any behavioural drift here fails CI.
fn render_listeners(conv: &Conversion, out: &mut Vec<String>) {
    if !conv.listeners.is_empty() {
        out.push("# --- Listeners ---".into());
        out.push("#".into());
        out.push("# mqttd binds one listener per protocol rather than repeating a".into());
        out.push("# `listener` block. TLS is 1.3-only by default: a client that cannot".into());
        out.push("# negotiate TLS 1.3 will fail to connect, so check your device fleet.".into());
        // A TOML table may be declared once. The first listener of each protocol becomes
        // the binding; the rest become TODOs — the per-listener emission this replaces
        // produced output tomllib rejects, found by the 2026-08-11 review panel actually
        // running the tool. The Python original is the reference; this stays
        // byte-identical to it (the differential test enforces that).
        let tls_listeners: Vec<&Listener> = conv
            .listeners
            .iter()
            .filter(|l| l.tls.contains_key("certfile"))
            .collect();
        let plain_listeners: Vec<&Listener> = conv
            .listeners
            .iter()
            .filter(|l| !l.tls.contains_key("certfile"))
            .collect();
        for (i, l) in conv.listeners.iter().enumerate() {
            let port = l.port.unwrap_or(1883);
            let host = l.bind.clone().unwrap_or_else(|| "0.0.0.0".into());
            let kind = if l.tls.contains_key("certfile") {
                "TLS"
            } else {
                "PLAINTEXT"
            };
            out.push(format!("#   listener {i}: {kind} on {host}:{port}"));
        }
        out.push("[listeners]".into());
        if let Some(first) = plain_listeners.first() {
            let port = first.port.unwrap_or(1883);
            let host = first.bind.clone().unwrap_or_else(|| "0.0.0.0".into());
            out.push(format!("plaintext_bind = \"{host}:{port}\""));
            out.push(
                "# WARNING: plaintext. mqttd logs this as an INSECURE mode on every start.".into(),
            );
            for extra in &plain_listeners[1..] {
                let eport = extra.port.unwrap_or(1883);
                let ehost = extra.bind.clone().unwrap_or_else(|| "0.0.0.0".into());
                out.push(format!(
                    "# TODO(migrate): additional plaintext listener {ehost}:{eport} — \
                     mqttd binds ONE listener per protocol; consolidate clients onto the \
                     bind above"
                ));
            }
        }
        if let Some(first) = tls_listeners.first() {
            let port = first.port.unwrap_or(1883);
            let host = first.bind.clone().unwrap_or_else(|| "0.0.0.0".into());
            out.push(format!("tls_bind = \"{host}:{port}\""));
            for extra in &tls_listeners[1..] {
                let eport = extra.port.unwrap_or(1883);
                let ehost = extra.bind.clone().unwrap_or_else(|| "0.0.0.0".into());
                out.push(format!(
                    "# TODO(migrate): additional TLS listener {ehost}:{eport} — \
                     mqttd binds ONE listener per protocol; consolidate clients onto the \
                     bind above"
                ));
            }
        }
        if let Some(first) = tls_listeners.first() {
            out.push("[tls]".into());
            let cert = &first.tls["certfile"];
            out.push(format!("cert = \"{cert}\""));
            if let Some(v) = first.tls.get("keyfile") {
                out.push(format!("key = \"{v}\""));
            }
            if let Some(v) = first.tls.get("cafile") {
                // Mosquitto's cafile only VERIFIES certs clients choose to present unless
                // require_certificate is true; mqttd's client_ca MANDATES one. Emitting
                // it unconditionally silently turned cert-optional listeners into mTLS —
                // the silent behaviour change this tool promises never to make.
                let required = first
                    .tls
                    .get("require_certificate")
                    .is_some_and(|r| r.to_lowercase() == "true");
                if required {
                    out.push(format!("client_ca = \"{v}\""));
                } else {
                    out.push(
                        "# TODO(migrate): cafile was set but require_certificate was NOT \
                         true. mqttd's client_ca MANDATES client certificates (mTLS) — \
                         there is no cert-optional mode. Uncomment to require certs \
                         fleet-wide, or leave commented for server-only TLS:"
                            .into(),
                    );
                    out.push(format!("# client_ca = \"{v}\""));
                }
            }
            if let Some(v) = first.tls.get("crlfile") {
                out.push(format!("crl = \"{v}\""));
            }
            if first.tls.contains_key("capath") {
                out.push(
                    "# TODO(migrate): capath (a directory of CAs) is not supported; \
                     concatenate them into one PEM and set client_ca"
                        .into(),
                );
            }
        }
        out.push(String::new());
    }
}

/// Render the translated broker configuration.
#[must_use]
pub fn render_config(conv: &Conversion) -> String {
    let mut out: Vec<String> = vec![
        "# Translated from mosquitto.conf by the mqttd Mosquitto converter".into(),
        "# (`mqttui migrate mosquitto`, or scripts/migrate/from-mosquitto.py).".into(),
        "#".into(),
        "# Review every line, then validate before deploying:".into(),
        "#     mqttd --check-config --config this-file.toml".into(),
        "#".into(),
        "# Settings with no mqttd equivalent are listed as TODO(migrate) rather than".into(),
        "# dropped silently — a converter that quietly loses a setting is worse than".into(),
        "# no converter, because you would deploy believing it came across.".into(),
        String::new(),
    ];
    for n in &conv.notes {
        out.push(format!("# NOTE: {n}"));
    }
    if !conv.notes.is_empty() {
        out.push(String::new());
    }
    for t in &conv.todos {
        out.push(format!("# TODO(migrate): {t}"));
    }
    if !conv.todos.is_empty() {
        out.push(String::new());
    }

    for section in ["node", "listeners", "security", "limits"] {
        let Some(body) = conv.config.get(section) else {
            continue;
        };
        if body.is_empty() {
            continue;
        }
        out.push(format!("[{section}]"));
        for (k, v) in body {
            out.push(format!("{k} = {v}"));
        }
        out.push(String::new());
    }

    render_listeners(conv, &mut out);
    out.join("\n") + "\n"
}

/// `mqttui migrate mosquitto <conf> [--out-config P] [--out-acl P] [--acl-file P]`.
///
/// # Errors
/// If the configuration cannot be read, or an output cannot be written.
#[allow(clippy::similar_names)] // `conf` (the path) and `conv` (the conversion) are the
                                // clearest names for each; renaming either would be worse.
pub fn run(args: &[String]) -> Result<String, String> {
    let mut conf: Option<&String> = None;
    let mut out_config: Option<&String> = None;
    let mut out_acl: Option<&String> = None;
    let mut acl_override: Option<&String> = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--out-config" => {
                out_config = args.get(i + 1);
                i += 1;
            }
            "--out-acl" => {
                out_acl = args.get(i + 1);
                i += 1;
            }
            "--acl-file" => {
                acl_override = args.get(i + 1);
                i += 1;
            }
            other if other.starts_with('-') => {
                return Err(format!("unknown option '{other}'"));
            }
            _ => conf = args.get(i),
        }
        i += 1;
    }
    let Some(conf) = conf else {
        return Err(
            "usage: mqttui migrate mosquitto <mosquitto.conf> [--out-config P] [--out-acl P]"
                .into(),
        );
    };

    let text = std::fs::read_to_string(conf).map_err(|e| format!("cannot read {conf}: {e}"))?;
    let conv = parse_conf(&text);
    let config = render_config(&conv);

    let mut report = String::new();
    if let Some(path) = out_config {
        std::fs::write(path, &config).map_err(|e| format!("cannot write {path}: {e}"))?;
        let _ = writeln!(report, "wrote {path}");
    } else {
        report.push_str(&config);
    }

    let acl_path = acl_override.cloned().or_else(|| conv.acl_file.clone());
    if let Some(acl_path) = acl_path {
        match std::fs::read_to_string(&acl_path) {
            Ok(acl_text) => {
                let (rules, todos) = parse_acl(&acl_text);
                let acl = render_acl(&rules, &todos);
                if let Some(path) = out_acl {
                    std::fs::write(path, &acl).map_err(|e| format!("cannot write {path}: {e}"))?;
                    let _ = writeln!(report, "wrote {path} ({} rules)", rules.len());
                } else {
                    report.push_str(&acl);
                }
            }
            Err(e) => {
                let _ = writeln!(report, "note: could not read acl_file {acl_path}: {e}");
            }
        }
    }

    if conv.todo_count() > 0 {
        let _ = writeln!(
            report,
            "\n{} setting(s) had no direct equivalent and are marked TODO(migrate) in the \
             output. Read them before deploying.",
            conv.todo_count()
        );
    }
    let _ = writeln!(
        report,
        "\nNext: mqttd --check-config --config <the config above>"
    );
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mosquitto's access word is optional and defaults to readwrite; getting this wrong
    /// silently widens or narrows every rule that omits it.
    #[test]
    fn a_missing_access_word_means_readwrite() {
        assert_eq!(split_access("read sensors/#"), ("read", "sensors/#"));
        assert_eq!(split_access("sensors/#"), ("readwrite", "sensors/#"));
        // A topic whose FIRST word happens to look like a topic, not an access word.
        assert_eq!(split_access("devices/+/up"), ("readwrite", "devices/+/up"));
    }

    /// `%u` is Mosquitto's identity placeholder; mqttd spells it `%i`. Missing this would
    /// produce a policy that matches a literal `%u` and therefore nothing at all.
    #[test]
    fn the_identity_placeholder_is_translated() {
        let (rules, _) = parse_acl("pattern read devices/%u/status\n");
        assert_eq!(rules[0].topics[0], "devices/%i/status");
    }

    /// A `%c` pattern is carried across, but flagged: mqttd's substitution fails closed on
    /// client ids containing / + or #, which silently denies rather than misfiring.
    #[test]
    fn a_client_id_pattern_is_carried_but_flagged() {
        let (rules, todos) = parse_acl("pattern read devices/%c/status\n");
        assert_eq!(rules[0].topics[0], "devices/%c/status");
        assert!(todos.iter().any(|t| t.contains("FAIL CLOSED")), "{todos:?}");
    }

    /// A `user` block is positional: its topics belong to it until the next `user`.
    #[test]
    fn user_blocks_are_regrouped_into_explicit_rules() {
        let (rules, _) = parse_acl(
            "user alice\ntopic write up/#\ntopic read down/#\nuser bob\ntopic readwrite b/#\n",
        );
        assert_eq!(rules.len(), 3);
        assert_eq!(rules[0].identities, vec!["alice"]);
        assert_eq!(rules[0].actions, vec!["publish"]);
        assert_eq!(rules[1].identities, vec!["alice"]);
        assert_eq!(rules[1].actions, vec!["subscribe"]);
        assert_eq!(
            rules[2].identities,
            vec!["bob"],
            "the block changed at `user bob`"
        );
    }

    /// Anything without an equivalent must appear in the output, not vanish.
    #[test]
    fn unmapped_settings_become_visible_todos() {
        let conv = parse_conf("sys_interval 10\nsome_future_option 3\n");
        assert_eq!(conv.todo_count(), 2);
        let rendered = render_config(&conv);
        assert!(rendered.contains("TODO(migrate): sys_interval"));
        assert!(rendered.contains("TODO(migrate): some_future_option"));
    }

    /// `allow_anonymous=true` is carried over — it is what the user had — but with the note
    /// saying why they probably should not keep it.
    #[test]
    fn anonymous_is_carried_over_with_a_warning() {
        let conv = parse_conf("allow_anonymous true\n");
        let out = render_config(&conv);
        assert!(out.contains("allow_anonymous = true"));
        assert!(out.contains("NOTE:") && out.contains("exposure incidents"));

        // ...and false is simply the mqttd default, so nothing is emitted.
        let conv = parse_conf("allow_anonymous false\n");
        assert!(!render_config(&conv).contains("allow_anonymous"));
    }

    /// TLS material binds to the listener it FOLLOWS, as Mosquitto scopes it.
    #[test]
    fn tls_material_attaches_to_its_own_listener() {
        let conv = parse_conf(
            "listener 1883 127.0.0.1\nlistener 8883 0.0.0.0\ncertfile /c.crt\nkeyfile /k.key\n",
        );
        let out = render_config(&conv);
        assert!(out.contains("plaintext_bind = \"127.0.0.1:1883\""));
        assert!(out.contains("tls_bind = \"0.0.0.0:8883\""));
        assert!(out.contains("cert = \"/c.crt\""));
    }
}
