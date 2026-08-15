//! Translate a Mosquitto deployment to an mqttd configuration DRAFT, natively (ADR 0056 T10).
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
//! ## What it produces: a DRAFT, where anything undecidable is INERT and named
//!
//! Every security-relevant value this port writes — every `*_bind`, every path under
//! `[tls]`, `client_ca`, `acl_file`, `password_file`, `allow_anonymous`,
//! `mtls_identity_source` and the ACL `default` — goes through ONE gate
//! ([`Provenance::line`]) together with the INPUT KEY it was derived from, and that gate
//! REFUSES to write a live line without one: a value with no provenance comes out
//! COMMENTED OUT beside a TODO naming the decision the operator has to make. Every live
//! security-relevant line carries `# from: <input key>`.
//!
//! That is the structural answer to rounds 1-3's findings, every one of which was the same
//! shape — a live setting the tool had not actually derived from the input
//! (`tls_bind = "0.0.0.0:1883"` fabricated for a config that said `bind_address 127.0.0.1`, a
//! WebSocket listener emitted as a raw-MQTT bind, an mTLS mandate dropped from a listener that
//! was not first). The worst case a FABRICATION can produce is now an INCOMPLETE config the
//! operator completes.
//!
//! ## What the gate does NOT close: MISREADING a real input
//!
//! Round 4 found five of these — a live value genuinely derived from a named input key whose
//! MEANING the converter got wrong, so the gate has nothing to object to. A TLS-PSK listener
//! became a live PLAINTEXT bind carrying an honest `# from: listener 8883` (the gate checks
//! where the VALUE came from; the FIELD is what encodes the transport); an ACL block Mosquitto
//! scopes to ANONYMOUS clients became a grant to every authenticated one; `message_size_limit 0`
//! — the vendor's spelling of *no limit* — became a 1 KiB packet ceiling. All fixed, each pinned
//! by a test, and the class is enumerated in docs/MIGRATION.md's KNOWN GAPS section because it
//! is unbounded across a foreign schema and no invariant over the output can see it.

use std::collections::BTreeMap;
use std::fmt::Write as _;

// ---------------------------------------------------------------------------
// String emission. ONE helper per channel, used by EVERY string this port writes.
//
// The 2026-08-14 review found the class in all three Python converters at once: no value was
// escaped anywhere, and this port inherited it. A Mosquitto ACL `user CORP\jdoe` came out as
// `identities = ["CORP\jdoe"]` and `certfile C:\certs\server.crt` as
// `cert = "C:\certs\server.crt"`, neither of which is valid TOML. A TOML parse failure is a
// WHOLE-DOCUMENT failure, so ONE such line made the broker refuse the entire migrated
// policy — and this port is the converter a `cargo install mqttui` user actually runs.
// ---------------------------------------------------------------------------

/// Escape a value for the inside of a TOML basic string.
fn toml_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\u{8}' => out.push_str("\\b"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\u{c}' => out.push_str("\\f"),
            '\r' => out.push_str("\\r"),
            // TOML 1.0 forbids raw control characters inside a basic string
            // (U+0000-U+0008, U+000A-U+001F, U+007F).
            c if c < ' ' || c == '\u{7f}' => {
                let _ = write!(out, "\\u{:04X}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

/// A complete, quoted TOML basic string.
fn toml_str(value: &str) -> String {
    format!("\"{}\"", toml_escape(value))
}

/// A TOML array of strings, each escaped.
fn toml_list<S: AsRef<str>>(values: &[S]) -> String {
    let items: Vec<String> = values.iter().map(|v| toml_str(v.as_ref())).collect();
    format!("[{}]", items.join(", "))
}

/// Flatten a value to one line so it cannot break out of a `#` comment.
///
/// A newline inside a TODO would end the comment and leave the rest of the sentence as a
/// bare line the TOML parser then rejects — the same "the output must validate" failure as
/// an unescaped backslash, one channel over. Mirrors the Python original's `comment_safe`,
/// which is what keeps the two byte-identical for whitespace-bearing values.
/// `split_whitespace` folds only whitespace, so `\u{0}`-`\u{8}` and `\u{b}`-`\u{1f}` survived
/// into the comment — and TOML 1.0 forbids a raw control character ANYWHERE in a document,
/// comments included, so one such byte in a path made the broker reject the WHOLE file while
/// the converter reported success. Escaped rather than dropped, so the byte is still visible.
/// Found 2026-08-15.
fn comment_safe(text: &str) -> String {
    let flattened = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut out = String::with_capacity(flattened.len());
    for c in flattened.chars() {
        if c < ' ' || c == '\u{7f}' {
            let _ = write!(out, "\\u{:04X}", c as u32);
        } else {
            out.push(c);
        }
    }
    out
}

/// Mosquitto's boolean spellings.
fn truthy(value: &str) -> bool {
    matches!(value.trim().to_lowercase().as_str(), "true" | "yes" | "1")
}

// ---------------------------------------------------------------------------
// PROVENANCE OR NOTHING — the load-bearing structure, mirroring the Python original's.
// ---------------------------------------------------------------------------

/// The fields whose value decides who can connect and what they may do. The ONLY way to
/// write one is [`Provenance::line`], which refuses to emit a live line without the input
/// key the value came from.
const SECURITY_FIELDS: [&str; 15] = [
    // [listeners] — which addresses the broker publishes, on which transport
    "plaintext_bind",
    "tls_bind",
    "ws_bind",
    "wss_bind",
    "quic_bind",
    // [tls] — the server identity, the client mandate and revocation
    "cert",
    "key",
    "client_ca",
    "crl",
    "allow_tls12",
    // [security] — who may connect and what governs them
    "acl_file",
    "password_file",
    "allow_anonymous",
    "mtls_identity_source",
    // the ACL policy's own catch-all
    "default",
];

fn is_security_field(field: &str) -> bool {
    SECURITY_FIELDS.contains(&field)
}

/// The provenance marker, on the line itself — what the property sweep's
/// NO-LIVE-WITHOUT-SOURCE invariant looks for, and what an operator diffing the output
/// against their `mosquitto.conf` reads.
const FROM: &str = "  # from: ";
/// A part of a value the INPUT did not contain, taken from a vendor-documented default of a
/// directive that WAS present. Named on the line so it is never silent, and counted by the
/// property sweep so it cannot be used to smuggle a fabrication.
const DEFAULTED: &str = "; defaulted: ";

/// One security-relevant value, and where it came from.
#[derive(Debug)]
struct Emitted {
    field: String,
    rendered: String,
    source: Option<String>,
    defaulted: Option<String>,
    live: bool,
}

/// The ONE gate every security-relevant emitted value passes through.
#[derive(Debug, Default)]
pub struct Provenance {
    rows: Vec<Emitted>,
}

impl Provenance {
    /// `field = rendered  # from: source`, or an INERT candidate plus a TODO.
    ///
    /// `source` is the INPUT KEY the value was derived from. Without one, a field in
    /// [`SECURITY_FIELDS`] is emitted commented out: `decide` says what the operator has to
    /// settle, and is required in that case (a TODO that does not name the decision is not
    /// a report).
    fn line(
        &mut self,
        field: &str,
        rendered: &str,
        source: Option<&str>,
        defaulted: Option<&str>,
        decide: Option<&str>,
    ) -> Vec<String> {
        if is_security_field(field) && source.is_none() {
            self.rows.push(Emitted {
                field: field.to_string(),
                rendered: rendered.to_string(),
                source: None,
                defaulted: defaulted.map(ToString::to_string),
                live: false,
            });
            let reason = decide.map_or_else(
                || {
                    format!(
                        "nothing in the input named a value for {field}, so it is emitted \
                         COMMENTED OUT rather than guessed at. Decide it yourself and \
                         uncomment"
                    )
                },
                ToString::to_string,
            );
            return vec![
                comment_safe(&format!("# TODO(migrate): {reason}")),
                format!("# {field} = {rendered}"),
            ];
        }
        self.rows.push(Emitted {
            field: field.to_string(),
            rendered: rendered.to_string(),
            source: source.map(ToString::to_string),
            defaulted: defaulted.map(ToString::to_string),
            live: true,
        });
        if !is_security_field(field) {
            return vec![format!("{field} = {rendered}")];
        }
        let mut trailer = format!("{FROM}{}", comment_safe(source.unwrap_or_default()));
        if let Some(d) = defaulted {
            let _ = write!(trailer, "{DEFAULTED}{}", comment_safe(d));
        }
        vec![format!("{field} = {rendered}{trailer}")]
    }

    /// A candidate deliberately NOT activated: a posture change, or an illegal pair.
    fn inert(&mut self, field: &str, rendered: &str, note: &str) -> Vec<String> {
        self.rows.push(Emitted {
            field: field.to_string(),
            rendered: rendered.to_string(),
            source: None,
            defaulted: None,
            live: false,
        });
        let mut line = format!("# {field} = {rendered}");
        if !note.is_empty() {
            let _ = write!(line, "  # {}", comment_safe(note));
        }
        vec![line]
    }

    /// The machine-readable form, for `--provenance-json`. Hand-built rather than pulling in
    /// a JSON crate: this binary's whole point is that it needs nothing.
    fn ledger(&self, tool: &str) -> String {
        let json = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");
        let mut out = String::new();
        let _ = writeln!(out, "{{");
        let _ = writeln!(out, " \"tool\": \"{}\",", json(tool));
        let _ = writeln!(out, " \"emissions\": [");
        for (i, r) in self.rows.iter().enumerate() {
            let comma = if i + 1 == self.rows.len() { "" } else { "," };
            let opt = |v: &Option<String>| {
                v.as_ref()
                    .map_or_else(|| "null".to_string(), |s| format!("\"{}\"", json(s)))
            };
            let _ = writeln!(
                out,
                "  {{\"field\": \"{}\", \"value\": \"{}\", \"source\": {}, \"defaulted\": {}, \
                 \"live\": {}}}{comma}",
                json(&r.field),
                json(&r.rendered),
                opt(&r.source),
                opt(&r.defaulted),
                r.live
            );
        }
        let _ = writeln!(out, " ]");
        let _ = writeln!(out, "}}");
        out
    }
}

/// What a policy with this `default` DOES — derived from the value, never asserted.
///
/// Round 2 and round 3 both found hard-coded deny prose beside an allow-everything file. So
/// no sentence in this tool states what a policy will do: every one of them is generated
/// from the `default` being written, here.
fn policy_effect(default: &str) -> String {
    if default == "allow" {
        return "this policy's `default = \"allow\"` PERMITS EVERY publish and subscribe by \
                every authenticated client, including on topics no client of yours has ever \
                used — a wide open policy, not a migrated one. Set `default = \"deny\"` \
                before deploying it"
            .to_string();
    }
    "this policy's `default = \"deny\"` denies every publish and subscribe that no rule \
     below allows. That is fail-closed, not migrated"
        .to_string()
}

/// The condensed honest-scope block that goes into the generated files themselves. The same
/// six lines as the Python original's `DRAFT_HEADER`.
const DRAFT_HEADER: [&str; 6] = [
    "# THIS IS A DRAFT, NOT A TRANSLATION. Anything this converter could not derive",
    "# from your input is COMMENTED OUT beside a TODO naming the decision, so this",
    "# file may be INCOMPLETE — but no live security setting in it was invented.",
    "# Every live security-relevant line carries `# from: <the input key>`.",
    "# NOT VERIFIED: no live Mosquitto was ever run; no total-coverage claim over",
    "# mosquitto.conf(5) is made.",
];

/// A Mosquitto listener with whatever was scoped to it.
///
/// `port`/`bind` carry the SOURCE KEY they came from, because a bind is the most
/// security-relevant value this tool writes and `0.0.0.0:1883` was fabricated for inputs
/// that named neither. A listener with no port source has NO bind: the bind line comes out
/// commented, with a TODO.
#[derive(Debug, Default)]
struct Listener {
    port: Option<String>,
    port_source: Option<String>,
    bind: Option<String>,
    bind_source: Option<String>,
    protocol: Option<String>,
    tls: BTreeMap<String, String>,
    /// `psk_file` / `psk_hint`: TLS-PSK, which makes the listener ENCRYPTED and unmappable.
    psk: BTreeMap<String, String>,
    caps: BTreeMap<String, String>,
}

impl Listener {
    /// The address to bind, or `None` when the input never gave one.
    fn host(&self) -> Option<String> {
        if let Some(b) = &self.bind {
            if !b.is_empty() {
                return Some(b.clone());
            }
        }
        if self.port_source.is_some() {
            // mosquitto.conf(5): `listener port [address]` with no address, and the default
            // listener with no `bind_address`, listen on every interface. That is a
            // documented default of a directive that WAS present, so it is derived — and it
            // is named as `defaulted:` on the emitted line.
            return Some("0.0.0.0".to_string());
        }
        None
    }

    fn host_defaulted(&self) -> Option<&'static str> {
        if self.bind.as_ref().is_some_and(|b| !b.is_empty()) || self.port_source.is_none() {
            return None;
        }
        Some(
            "the host, because that directive named no address and mosquitto.conf(5) then \
             listens on EVERY interface",
        )
    }

    /// The input key(s) this listener's address was derived from.
    fn source(&self) -> Option<String> {
        let port_source = self.port_source.as_ref()?;
        match &self.bind_source {
            Some(b) if b != port_source => Some(format!("{port_source} + {b}")),
            _ => Some(port_source.clone()),
        }
    }

    fn address(&self) -> Option<String> {
        let host = self.host()?;
        let port = self.port.as_ref()?;
        Some(format!("{host}:{port}"))
    }

    /// Why no address could be derived — named in the TODO that replaces the bind.
    fn address_gap(&self) -> String {
        let no_bind = self.bind.as_ref().is_none_or(String::is_empty);
        if self.port.is_none() && no_bind {
            return "the input named NEITHER a `listener` port, NOR `port`, NOR `bind_address` \
                    for it"
                .to_string();
        }
        if self.port.is_none() {
            return format!(
                "the input gave its address as `bind_address {}` but NEVER a port. \
                 mosquitto.conf(5) documents the default as 1883 — that is a default of the \
                 BROKER, not a value in your file, and a bind on a port nobody wrote is how a \
                 broker ends up published where its operator did not choose (your real port \
                 may well be in an include_dir file, which this converter does not read)",
                self.bind.clone().unwrap_or_default()
            );
        }
        "the input named no address for it".to_string()
    }

    /// The commented placeholder, when no address could be derived.
    fn candidate_address(&self) -> String {
        format!(
            "{}:{}",
            self.bind
                .clone()
                .filter(|b| !b.is_empty())
                .unwrap_or_else(|| "0.0.0.0".into()),
            self.port.clone().unwrap_or_else(|| "1883".into())
        )
    }

    /// How this listener is named in every message about it — never fabricated.
    fn where_(&self) -> String {
        match self.address() {
            Some(addr) => format!("listener {addr}"),
            None => format!("the default listener ({})", self.address_gap()),
        }
    }

    fn is_tls(&self) -> bool {
        self.tls.get("certfile").is_some_and(|v| !v.is_empty())
    }

    /// TLS-PSK: ENCRYPTED, and unmappable, so it must never become a plaintext bind.
    fn is_psk(&self) -> bool {
        !self.psk.is_empty()
    }

    /// `psk_file X, psk_hint Y` — sorted, as the Python original's `sorted(self.psk)` is.
    fn psk_inventory(&self) -> String {
        self.psk
            .iter()
            .map(|(k, v)| format!("{k} {v}"))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// `mqtt`, `websockets`, or `None` when the input named a transport we do not know.
    fn transport(&self) -> Option<&'static str> {
        match &self.protocol {
            // mosquitto.conf(5): "Can be mqtt, the default, or websockets".
            None => Some("mqtt"),
            Some(p) => match p.trim().to_lowercase().as_str() {
                "mqtt" => Some("mqtt"),
                "websockets" => Some("websockets"),
                _ => None,
            },
        }
    }

    fn tls_get(&self, key: &str) -> Option<&str> {
        self.tls.get(key).map(String::as_str)
    }
}

/// The accumulated translation.
#[derive(Debug, Default)]
pub struct Conversion {
    /// `section -> [(key, already-rendered right-hand side, including any `# from:`)]`.
    config: BTreeMap<&'static str, Vec<(String, String)>>,
    listeners: Vec<Listener>,
    todos: Vec<String>,
    notes: Vec<String>,
    /// The `acl_file` the config pointed at, if any.
    pub acl_file: Option<String>,
    /// `per_listener_settings`, which makes Mosquitto's authn/authz keys per-listener.
    per_listener: bool,
    /// Every listener-scoped security key as `(key, where it was set, value)`, in document
    /// order, so a node-wide collapse can be reported rather than silently taken.
    scoped: Vec<(String, String, String)>,
    prov: Provenance,
    /// Directives naming a file whose CONTENTS were never read (`include_dir`, a plugin's
    /// config). Every sentence about "no policy was found" is derived from this, because
    /// "your Mosquitto also authorized everything" is false when the policy was in
    /// `dynamic-security.json`.
    unread: Vec<String>,
    /// Security-relevant candidates that are NOT activated, rendered commented after their
    /// section.
    deferred: BTreeMap<&'static str, Vec<String>>,
}

impl Conversion {
    /// Record a table value. A security-relevant key with no source is NOT set live.
    ///
    /// This is the gate, at the point of assignment: there is no way to put a value into
    /// `[listeners]`, `[tls]` or `[security]` without naming the input key it came from.
    fn set(
        &mut self,
        section: &'static str,
        key: &str,
        value: &str,
        source: Option<&str>,
        defaulted: Option<&str>,
        decide: Option<&str>,
    ) {
        if is_security_field(key) && source.is_none() {
            let lines = self.prov.line(key, value, None, defaulted, decide);
            self.deferred.entry(section).or_default().extend(lines);
            return;
        }
        let mut rendered = value.to_string();
        self.prov.rows.push(Emitted {
            field: key.to_string(),
            rendered: value.to_string(),
            source: source.map(ToString::to_string),
            defaulted: defaulted.map(ToString::to_string),
            live: true,
        });
        let _ = write!(
            rendered,
            "{FROM}{}",
            comment_safe(source.unwrap_or_default())
        );
        if let Some(d) = defaulted {
            let _ = write!(rendered, "{DEFAULTED}{}", comment_safe(d));
        }
        self.insert_raw(section, key, rendered);
    }

    /// A plain (non-security) value: the common case, with no provenance trailer and no
    /// ledger row. `debug_assert` keeps the two paths from being confused — a security field
    /// must go through [`Conversion::set`], which is the gate.
    fn put(&mut self, section: &'static str, key: &str, value: String) {
        debug_assert!(
            !is_security_field(key),
            "{key} is security-relevant: use set() so it carries its source key"
        );
        self.insert_raw(section, key, value);
    }

    fn insert_raw(&mut self, section: &'static str, key: &str, rendered: String) {
        let entry = self.config.entry(section).or_default();
        if let Some(slot) = entry.iter_mut().find(|(k, _)| k == key) {
            slot.1 = rendered;
        } else {
            entry.push((key.to_string(), rendered));
        }
    }

    fn defer(&mut self, section: &'static str, lines: Vec<String>) {
        self.deferred.entry(section).or_default().extend(lines);
    }

    fn todo(&mut self, msg: impl Into<String>) {
        // Flattened HERE, as the Python original does on ingest, so no caller can emit a
        // message that breaks out of its `#` comment. Deduplicated, as the Python original
        // is, so a per-listener sweep cannot print the same sentence twice.
        let msg = comment_safe(&msg.into());
        if !self.todos.contains(&msg) {
            self.todos.push(msg);
        }
    }

    fn note(&mut self, msg: impl Into<String>) {
        let msg = comment_safe(&msg.into());
        if !self.notes.contains(&msg) {
            self.notes.push(msg);
        }
    }

    fn has(&self, section: &str, key: &str) -> bool {
        self.config
            .get(section)
            .is_some_and(|body| body.iter().any(|(k, _)| k == key))
    }

    /// The values a listener-scoped key took, in document order: `(where, value)`.
    fn scoped_sites(&self, key: &str) -> Vec<(&str, &str)> {
        self.scoped
            .iter()
            .filter(|(k, _, _)| k == key)
            .map(|(_, w, v)| (w.as_str(), v.as_str()))
            .collect()
    }

    /// How many settings had no equivalent.
    #[must_use]
    pub fn todo_count(&self) -> usize {
        self.todos.len()
    }
}

/// Settings with an exact mqttd equivalent: `(section, key, kind)`.
///
/// `max_inflight_messages` is deliberately ABSENT: it looks like `[limits] receive_maximum`
/// and is the OPPOSITE DIRECTION. See the named case in [`parse_conf`].
///
/// `max_connections` is deliberately ABSENT TOO, for two reasons found on 2026-08-15: it is
/// a PER-LISTENER directive (mosquitto.conf(5): "Limit the total number of clients connected
/// for the current listener"), so a flat table collapsed several listeners LAST-WINS with no
/// trace; and the vendor's own documented value for unlimited is `-1`, which this table
/// passed straight through into `max_connections = -1`, a config the broker REJECTS
/// ("invalid value: integer `-1`, expected u64"). Both are handled in
/// [`convert_listener_caps`].
fn direct(name: &str) -> Option<(&'static str, &'static str, &'static str)> {
    Some(match name {
        "max_queued_messages" => ("limits", "max_queued_messages", "int"),
        "max_packet_size" | "message_size_limit" => ("limits", "max_packet_size", "int"),
        "max_topic_alias" => ("limits", "topic_alias_max", "u16"),
        "retain_available" => ("limits", "max_retained_messages", "retain"),
        "persistence_location" => ("node", "data_dir", "str"),
        _ => return None,
    })
}

/// The exact TODO the Python original emits for `max_inflight_messages`; the differential
/// test compares these bytes.
fn max_inflight_todo(value: &str) -> String {
    format!(
        "max_inflight_messages {value}: NOT carried over, deliberately. It bounds the \
         messages Mosquitto may have in flight TOWARD a client (outbound); mqttd has no \
         outbound-window setting — it honours each v5 client's OWN Receive Maximum from \
         CONNECT and treats a v3.1.1 client as unlimited (ADR 0012). The similarly named \
         [limits] receive_maximum is the OPPOSITE direction: the inbound window mqttd GRANTS \
         clients, default 256. Setting it from this value would silently shrink your inbound \
         QoS>0 window and throttle publishers after cutover. Cap the inbound window \
         deliberately if you want to: # receive_maximum = <messages>"
    )
}

/// The exact NOTE the Python original emits when nothing set a data dir.
///
/// Without one the broker REFUSES to start (durable sessions are on by default), so a
/// `mosquitto.conf` with no `persistence_location` — the common case, since Mosquitto's
/// persistence is off by default — produced a config `mqttd --check-config` rejects
/// outright. Found on 2026-08-14 by putting `--check-config` in front of this converter's
/// own output, which is exactly the gap its test documented about itself.
const DATA_DIR_NOTE: &str = "mosquitto.conf named no persistence_location, so [node] \
     data_dir was set to mqttd's packaged default /var/lib/mqttd. mqttd's durable sessions \
     are ON by default and REFUSE to start without a data dir, so this value is what makes \
     the config valid — mount a real volume there, or the durable state lives on the \
     container's ephemeral layer. (Mosquitto's persistence was OFF by default, so if you \
     never set it, queued messages did not survive a restart; on-disk is very likely what \
     you actually want, but [durable] enabled = false is the faithful translation.)";

/// Directives that exist in Mosquitto and deliberately have no mqttd equivalent, with the
/// reason. Being explicit about *why* is the point: "unsupported" invites a bug report,
/// "deliberately absent, here is the alternative" does not.
/// `acl_file` is deliberately ABSENT: it is consumed before this table is consulted (it
/// names the policy to translate), so an entry here would be dead text claiming a mapping
/// that never fires.
fn no_equivalent(name: &str) -> Option<&'static str> {
    Some(match name {
        "password_file" => {
            "mqttd uses Argon2id password files: set security.password_file to a file of \
             `username:argon2id-hash` lines. mosquitto_passwd hashes are NOT compatible and \
             cannot be converted (they are hashes — the passwords are not recoverable), so \
             each user must be re-hashed from their password: `printf %s '<password>' | \
             mqttd --hash-password <username> >> passwd`"
        }
        // `psk_file` / `psk_hint` are deliberately ABSENT: they are LISTENER-SCOPED and decide
        // that listener's TRANSPORT, so they are collected per listener and decided in
        // [`convert_psk`] — a flat "not implemented" entry here let a PSK listener fall through
        // to `plaintext_bind` and become a LIVE PLAINTEXT bind. See [`PSK_KEYS`].
        "bridge" => {
            "bridging is a separate process in mqttd (mqtt-bridge) with its own config; see \
             docs/BRIDGE.md"
        }
        "log_dest" => "mqttd logs to stdout for the container/journal to collect",
        "sys_interval" => "$SYS topics are not implemented; use the Prometheus endpoint",
        "autosave_interval" => "writes are transactional (redb); there is no autosave timer",
        "allow_zero_length_clientid" => {
            "a zero-length client id is accepted with clean session and refused otherwise, \
             per spec; not configurable"
        }
        _ => return None,
    })
}

/// A Mosquitto BRIDGE block, key by key. Every one of these HAS an exact equivalent in the
/// `mqtt-bridge` config this repository ships (docs/BRIDGE.md) — and until 2026-08-15 all of
/// them except `connection` fell through to "no direct equivalent — check the mqttd
/// configuration table", which sends the operator to a table that has nothing to find, for
/// settings the repo already translates from EMQX under `--out-bridge`. `bridge_cafile` is the
/// one directive that decides whether the migrated bridge VERIFIES its peer.
fn bridge_key(name: &str) -> Option<&'static str> {
    Some(match name {
        "connection" => {
            "opens a BRIDGE block. Bridging is a SEPARATE PROCESS in mqttd — `mqtt-bridge \
             <config>`, not a broker setting — so nothing below configures it. The keys of this \
             block are named individually in the TODOs that follow; assemble them into an \
             mqtt-bridge config by hand (docs/BRIDGE.md). This converter has no --out-bridge \
             (the EMQX one does)"
        }
        "address" => {
            "the bridge's upstream address -> mqtt-bridge `[[upstreams]] url`. NOT written \
             anywhere by this converter: there is no --out-bridge here, so write it into an \
             mqtt-bridge config yourself (docs/BRIDGE.md)"
        }
        "addresses" => {
            "the bridge's upstream address(es) -> mqtt-bridge `[[upstreams]] url`, ONE per \
             upstream (there is no failover list). Not written by this converter"
        }
        "topic" => {
            "a bridge topic -> mqtt-bridge `[[upstreams.rules]]` — `filter`, `direction` (`out` \
             for Mosquitto's `out`, `in` for `in`, and `both` needs TWO rules), `qos`, and a \
             prefix `remap` for the local/remote prefix pair. Mosquitto's ordering is `topic \
             <pattern> [direction [qos [local-prefix [remote-prefix]]]]`. Not written by this \
             converter"
        }
        "bridge_cafile" => {
            "the CA that verifies the UPSTREAM -> mqtt-bridge `[upstreams.tls] ca`. Not written \
             by this converter — and note that mqtt-bridge's `[upstreams.tls]` is OPTIONAL, so \
             an upstream with no tls block connects in PLAINTEXT: omit it and the bridge's \
             CONNECT, username included, crosses in the clear"
        }
        "bridge_capath" => {
            "a DIRECTORY of CAs for the upstream. mqtt-bridge takes ONE PEM file \
             (`[upstreams.tls] ca`), so concatenate them. THIS CONVERTER DID NOT READ THAT \
             DIRECTORY"
        }
        "bridge_certfile" => {
            "the bridge's own client certificate -> mqtt-bridge `[upstreams.tls] cert` (with \
             `key`; a half identity is refused at startup). Not written by this converter"
        }
        "bridge_keyfile" => {
            "the bridge's own private key -> mqtt-bridge `[upstreams.tls] key`. Not written by \
             this converter, and never copied"
        }
        "remote_username" => {
            "the username the bridge presents UPSTREAM -> mqtt-bridge `[[upstreams]] username`. \
             Not written by this converter"
        }
        "remote_password" => {
            "the password the bridge presents upstream -> mqtt-bridge `[[upstreams]] \
             password_file` (a FILE, never inline). NOT copied: secrets are never transformed"
        }
        "remote_clientid" => {
            "the client id the bridge uses upstream -> mqtt-bridge `[[upstreams]] client_id`, \
             which MUST be unique per instance. Not written by this converter"
        }
        _ => return None,
    })
}

/// Directives that name ANOTHER FILE OR DIRECTORY this converter did not open. The message
/// must say the CONTENTS were not read — "no direct equivalent" reads as "mqttd has no
/// includes, fine" rather than "anything in there, possibly your whole authn/authz, was
/// never seen".
///
/// `plugin` / `plugin_opt_*` are here rather than in [`no_equivalent`] because "there is no
/// plugin API" is true and beside the point: mosquitto.conf(5) recommends the Dynamic
/// Security plugin OVER `password_file`, so for a dynsec deployment the ENTIRE authn/authz
/// policy lives in a JSON file this converter never opened. Found 2026-08-15.
fn not_read(name: &str) -> Option<&'static str> {
    Some(match name {
        "include_dir" => {
            "a DIRECTORY of further .conf files, which Mosquitto loads in case-sensitive \
             alphabetical order (00.conf, 01.conf, A.conf, a.conf, …) as if their contents \
             were pasted into the main file. THIS CONVERTER DID NOT OPEN THAT DIRECTORY AND \
             DID NOT READ ONE BYTE OF IT, so ANY setting it holds — a second listener, \
             another acl_file or password_file, a bridge, a plugin — is absent from the \
             output below and is NOT reported anywhere, because it was never seen. \
             Concatenate the main file with those .conf files in that order and re-run this \
             converter on the result"
        }
        "plugin" => {
            "an authentication/authorization PLUGIN, whose own configuration THIS CONVERTER \
             DID NOT OPEN. mqttd has no plugin API (authentication is JWT/OIDC/mTLS/password, \
             authorization is the ACL policy), but that is not the problem here: \
             mosquitto.conf(5) recommends the Dynamic Security plugin OVER password_file, so \
             if this is mosquitto_dynamic_security.so then your ENTIRE user, role and ACL \
             policy lives in the plugin's JSON config and NONE of it was read or translated. \
             Export it and re-model it as an mqttd ACL policy plus Argon2id password entries \
             before you cut over"
        }
        "auth_plugin" => {
            "the pre-2.0 spelling of `plugin`: an authentication/authorization plugin whose \
             own configuration THIS CONVERTER DID NOT OPEN. mqttd has no plugin API, and \
             whatever policy the plugin enforced is NOT in the output below"
        }
        "plugin_opt_config_file" => {
            "the config file of the plugin named above, which THIS CONVERTER DID NOT OPEN AND \
             DID NOT READ ONE BYTE OF. For the Dynamic Security plugin this file IS the \
             deployment's authentication and authorization: clients, roles and per-role ACL \
             rules. Nothing in it is in the output below"
        }
        _ => return None,
    })
}

/// The message for any other `plugin_opt_*`, quoting the man page's own definition.
const PLUGIN_OPT_NOT_READ: &str = "an option passed to the plugin named above \
     (mosquitto.conf(5): `plugin_opt_*` — Options to be passed to the most recent plugin \
     defined in the configuration file). THIS CONVERTER DID NOT OPEN the plugin or its \
     configuration, so whatever policy they held is NOT in the output below";

/// Listener-SCOPED TLS keys. Mosquitto scopes every one of these to the `listener` block it
/// follows, so they are collected per listener and decided across ALL of them in
/// [`convert_tls`] — never read off `listener[0]` and applied as if global.
const TLS_KEYS: [&str; 9] = [
    "cafile",
    "capath",
    "certfile",
    "keyfile",
    "require_certificate",
    "crlfile",
    "tls_version",
    "use_identity_as_username",
    "use_subject_as_username",
];

/// Listener-scoped keys that are NOT TLS material: the transport and the connection cap.
/// Both were read as if global before 2026-08-15 — `protocol` was not read at all, so a
/// WebSocket listener was emitted as a raw-MQTT bind, and `max_connections` collapsed
/// last-wins.
const LISTENER_KEYS: [&str; 2] = ["protocol", "max_connections"];

/// TLS-PSK, LISTENER-SCOPED, and they decide the listener's TRANSPORT. mosquitto.conf(5) @
/// v2.0.22, verbatim: "The `psk_hint` option enables pre-shared-key support for this listener
/// also acts as an identifier for this listener".
///
/// A PSK listener is ENCRYPTED and mqttd has NO PSK support, so it is UNMAPPABLE — and until
/// 2026-08-15 neither key was in [`TLS_KEYS`] nor in the half-material net, so
/// [`Listener::is_tls`] was false and [`bind_key`] chose `plaintext_bind`: an encrypted
/// listener became a LIVE PLAINTEXT bind. The provenance gate cannot catch that, because the
/// bind carried a genuine `# from: listener 8883` — the gate checks where the VALUE came from
/// and the FIELD is what encodes the transport. See [`convert_psk`].
const PSK_KEYS: [&str; 2] = ["psk_file", "psk_hint"];

/// Whether `address` is a `host:port` mqttd can bind, and if not, WHY.
///
/// Every `*_bind` used to be emitted LIVE with no such check, and `mqttd --check-config` — the
/// verification the generated header, `--help` and the docs all point at — accepts any string
/// there, so the prescribed gate said `config OK` on addresses the broker then refuses at
/// STARTUP. A Mosquitto UNIX-SOCKET listener (`listener 0 /tmp/mosq.sock`) also declares no TCP
/// endpoint at all, so a bind derived from it is a transport fabrication the provenance gate
/// cannot see. Found 2026-08-15.
fn bind_gap(address: &str) -> Option<String> {
    if address.is_empty() {
        return Some("it is empty".to_string());
    }
    let Some((host, port)) = address.rsplit_once(':') else {
        return Some(format!(
            "`{address}` has NO port, and mqttd binds host:port"
        ));
    };
    let host = if host.starts_with('[') && host.ends_with(']') {
        &host[1..host.len() - 1]
    } else if host.contains(':') {
        return Some(format!(
            "`{address}` looks like an IPv6 address without brackets; mqttd needs \
             `[<address>]:<port>`"
        ));
    } else {
        host
    };
    if host.is_empty() {
        return Some(format!(
            "`{address}` names NO host (mqttd needs an explicit address — `0.0.0.0` for every \
             interface — and refuses to resolve an empty one at startup)"
        ));
    }
    if host.contains('/') {
        return Some(format!(
            "`{address}` is not a TCP address: `{host}` is a filesystem path, so this is a \
             UNIX-DOMAIN-SOCKET listener (mosquitto.conf(5): 'the port must be set to 0, and \
             the unix socket path must be given'). mqttd has NO unix-socket transport at all — \
             there is nothing to bind, and turning it into a TCP port would publish on the \
             network a listener that was reachable only through the filesystem"
        ));
    }
    if !port.chars().all(|c| c.is_ascii_digit())
        || !port.parse::<u32>().is_ok_and(|p| (1..=65535).contains(&p))
    {
        return Some(format!(
            "`{port}` is not a TCP port number (1-65535), so `{address}` is not an address \
             mqttd can bind — it passes --check-config and then fails at startup"
        ));
    }
    if !host
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | ':'))
    {
        return Some(format!(
            "`{host}` is not an address or hostname mqttd can resolve, so `{address}` is not \
             one it can bind"
        ));
    }
    None
}

/// Keys Mosquitto makes PER LISTENER when `per_listener_settings` is true.
/// mosquitto.conf(5) @ v2.0.22 names EIGHT — see [`SCOPED_SECURITY_LIST`]. mqttd's
/// `[security]` is node-wide, so if two listeners disagree only one value can survive and
/// that collapse must be reported, not silently taken.
const SCOPED_SECURITY: [&str; 7] = [
    "allow_anonymous",
    "acl_file",
    "password_file",
    "psk_file",
    "allow_zero_length_clientid",
    "auto_id_prefix",
    "plugin",
];

/// The exact list, in the man page's own order, quoted by every surface that names it: the
/// emitted TODO, `mqttui --help` and docs/MIGRATION.md. It used to be asserted as "exactly
/// six" in four places against a document that names eight, and the two omitted are the pair
/// that carries an entire third-party authn/authz backend.
const SCOPED_SECURITY_LIST: &str = "password_file, acl_file, psk_file, allow_anonymous, \
     allow_zero_length_clientid, auto_id_prefix, plugin and plugin_opt_* (mosquitto.conf(5) \
     @ v2.0.22 names those eight)";

/// The TLS material keys, in the fixed order the orphan-listener report lists them.
const TLS_MATERIAL: [&str; 5] = [
    "cafile",
    "capath",
    "keyfile",
    "require_certificate",
    "crlfile",
];

/// transport (from `protocol`) + TLS -> the mqttd bind key. mqttd has FOUR client binds and
/// `protocol websockets` was unread, so a Mosquitto WebSocket listener was emitted as a
/// raw-MQTT bind. Found 2026-08-15.
fn bind_key(transport: &str, tls: bool) -> &'static str {
    match (transport, tls) {
        ("websockets", false) => "ws_bind",
        ("websockets", true) => "wss_bind",
        (_, true) => "tls_bind",
        (_, false) => "plaintext_bind",
    }
}

/// Walk `mosquitto.conf`. Listener-scoped keys follow their listener, as Mosquitto does.
#[must_use]
#[allow(clippy::too_many_lines)] // one arm per directive, in the Python original's order,
                                 // which is what keeps the two byte-identical.
pub fn parse_conf(text: &str) -> Conversion {
    let mut conv = Conversion::default();
    let mut current: Option<usize> = None;
    // The DEFAULT listener, configured with `port` / `bind_address` rather than a `listener`
    // block (mosquitto.conf(5) documents both). Neither directive was read before
    // 2026-08-15: with TLS material present the synthetic listener had no port and no bind,
    // so `tls_bind = "0.0.0.0:1883"` was FABRICATED — a broker the incumbent exposed only on
    // `bind_address 127.0.0.1` published on every interface, on a port the input never
    // mentioned — and with no TLS material there was no [listeners] table at all and nothing
    // said so.
    let mut default_listener: Option<usize> = None;

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
            let mut lst = Listener::default();
            if let Some(p) = bits.next() {
                if p.chars().all(|c| c.is_ascii_digit()) && !p.is_empty() {
                    lst.port = Some(p.to_string());
                    lst.port_source = Some(format!("listener {value}"));
                }
            }
            if let Some(b) = bits.next() {
                lst.bind = Some(b.to_string());
                lst.bind_source = Some(format!("listener {value}"));
            }
            conv.listeners.push(lst);
            current = Some(conv.listeners.len() - 1);
            continue;
        }

        if key == "port" || key == "bind_address" {
            let numeric = !value.is_empty() && value.chars().all(|c| c.is_ascii_digit());
            if key == "port" && !numeric {
                conv.todo(format!(
                    "port {value}: not a port number this converter can use, so NO \
                     [listeners] bind was derived from it. Mosquitto's default listener needs \
                     a numeric port; fix it and re-run"
                ));
                continue;
            }
            if key == "bind_address" && value.is_empty() {
                conv.todo(
                    "bind_address with no value: nothing to derive an address from, so NO \
                     [listeners] bind was written for the default listener",
                );
                continue;
            }
            let idx = *default_listener.get_or_insert_with(|| {
                conv.listeners.push(Listener::default());
                conv.listeners.len() - 1
            });
            if current.is_none() {
                current = Some(idx);
            }
            let lst = &mut conv.listeners[idx];
            if key == "port" {
                lst.port = Some(value.clone());
                lst.port_source = Some(format!("port {value}"));
            } else {
                lst.bind = Some(value.clone());
                lst.bind_source = Some(format!("bind_address {value}"));
                // DELIBERATELY NOT defaulting the port here. mosquitto.conf(5) does document
                // "port ... Defaults to 1883", but a PORT is the one half of a bind this
                // converter will not supply: `bind_address` without `port` is exactly the
                // shape where the real port lives in an include_dir file that was never read.
                // The candidate is emitted COMMENTED OUT instead. Found 2026-08-15 by the
                // fuzz pass, which mutated `port` out of a fixture and watched a live
                // `0.0.0.0:1883` appear.
            }
            continue;
        }

        if TLS_KEYS.contains(&key) || LISTENER_KEYS.contains(&key) {
            // TLS material and the transport belong to the listener they follow; before any
            // listener they are the default one's.
            let idx = if let Some(i) = current {
                i
            } else {
                let i = *default_listener.get_or_insert_with(|| {
                    conv.listeners.push(Listener::default());
                    conv.listeners.len() - 1
                });
                current = Some(i);
                i
            };
            if TLS_KEYS.contains(&key) {
                conv.listeners[idx].tls.insert(key.to_string(), value);
            } else if key == "protocol" {
                conv.listeners[idx].protocol = Some(value);
            } else {
                conv.listeners[idx].caps.insert(key.to_string(), value);
            }
            continue;
        }

        // Every listener-scoped security key is recorded WITH the listener it followed,
        // before it is acted on, so `convert_scoped_security` can report a collapse.
        if SCOPED_SECURITY.contains(&key) || key.starts_with("plugin_opt_") {
            let where_ = current.map_or_else(
                || "the global section".to_string(),
                |i| conv.listeners[i].where_(),
            );
            conv.scoped.push((key.to_string(), where_, value.clone()));
        }

        if PSK_KEYS.contains(&key) {
            // Recorded on the LISTENER, because these decide its transport (see PSK_KEYS) — and
            // decided in `convert_psk`, never here, so the message can name the listener's final
            // address rather than the half-parsed one.
            let idx = if let Some(i) = current {
                i
            } else {
                let i = *default_listener.get_or_insert_with(|| {
                    conv.listeners.push(Listener::default());
                    conv.listeners.len() - 1
                });
                current = Some(i);
                i
            };
            conv.listeners[idx].psk.insert(key.to_string(), value);
            continue;
        }

        match key {
            "per_listener_settings" => conv.per_listener = truthy(&value),
            // Recorded above and DECIDED in `convert_scoped_security` from the last value,
            // deliberately: that is what Mosquitto itself does with per_listener_settings
            // FALSE (its default). Acting on it here kept the first TRUE seen — so
            // `allow_anonymous true` on a retired listener followed by `false` on the live
            // one carried anonymous access forward, and emitted a NOTE saying so beside a
            // config that did not.
            "allow_anonymous" => {}
            "acl_file" => conv.acl_file = Some(value),
            "persistence" => {
                if truthy(&value) {
                    conv.note(
                        "persistence was on: set node.data_dir (below) and mount a volume, \
                         or durable state is kept in memory only",
                    );
                }
            }
            // NOT a mapping, and it looks exactly like one — see `max_inflight_todo`.
            "max_inflight_messages" => {
                let todo = max_inflight_todo(&value);
                conv.todo(todo);
            }
            _ => {
                // The VALUE is named too: "password_file: mqttd uses Argon2id" leaves the
                // operator hunting for which file, and a report that cannot be checked
                // against the input is not a report.
                let named = if value.is_empty() {
                    key.to_string()
                } else {
                    format!("{key} {value}")
                };
                if let Some(why) = not_read(key) {
                    conv.todo(format!("{named}: {why}"));
                    conv.unread.push(named);
                } else if key.starts_with("plugin_opt_") {
                    conv.todo(format!("{named}: {PLUGIN_OPT_NOT_READ}"));
                    conv.unread.push(named);
                } else if let Some((section, mkey, kind)) = direct(key) {
                    match kind {
                        "int" => {
                            // ZERO IS THE VENDOR'S SPELLING OF *NO LIMIT* for both packet-size
                            // keys — mosquitto.conf(5) @ v2.0.22 on message_size_limit: "The
                            // default value is 0, which means that all valid MQTT messages are
                            // accepted", and max_packet_size "Defaults to no limit". Passing 0
                            // through wrote `max_packet_size = 0`, which --check-config ACCEPTS
                            // and mqttd then FLOORS TO 1024, so an unlimited Mosquitto became a
                            // broker refusing any packet over 1 KiB. Found 2026-08-15.
                            if matches!(key, "message_size_limit" | "max_packet_size")
                                && value.trim() == "0"
                            {
                                conv.note(format!(
                                    "{key} {value}, which mosquitto.conf(5) @ v2.0.22 documents \
                                     as NO LIMIT (message_size_limit: 'The default value is 0, \
                                     which means that all valid MQTT messages are accepted'; \
                                     max_packet_size: 'Defaults to no limit'). mqttd spells \
                                     unlimited as the key being ABSENT, so [limits] \
                                     max_packet_size was left UNSET — its own default ceiling \
                                     then applies. Passing the 0 through would have written \
                                     max_packet_size = 0, which --check-config ACCEPTS and the \
                                     broker FLOORS to 1024 bytes, refusing every packet over 1 \
                                     KiB"
                                ));
                                continue;
                            }
                            conv.put(section, mkey, value.clone());
                            if key == "message_size_limit" {
                                // This NOTE used to say mosquitto.conf(5) DEPRECATES
                                // message_size_limit in favour of max_packet_size and that the
                                // two are the SAME QUANTITY. The pinned page says neither — it
                                // marks port, bind_address, allow_duplicate_messages and
                                // clientid_prefixes deprecated and not this one, and the
                                // neighbouring entry states the difference outright. A wrong
                                // reason for a real caveat, in the file the operator DEPLOYS.
                                conv.note(format!(
                                    "message_size_limit {value} became [limits] max_packet_size \
                                     — the NEAREST equivalent, NOT the same quantity. \
                                     mosquitto.conf(5) @ v2.0.22 defines message_size_limit as \
                                     'the maximum publish payload size that the broker will \
                                     allow', while its own max_packet_size 'applies to the full \
                                     MQTT packet, not just the payload' — and mqttd's \
                                     max_packet_size is the PACKET form too ('Largest accepted \
                                     MQTT packet, bytes'). So the cap below is TIGHTER than \
                                     yours by each publish's fixed header, topic and MQTT 5 \
                                     properties: a publish Mosquitto accepted at the boundary is \
                                     REFUSED after cutover. Raise it by your largest topic + \
                                     property overhead if you publish near the limit. If both \
                                     directives were set, the LAST one read is what is below"
                                ));
                            }
                        }
                        "u16" => {
                            if let Ok(n) = value.parse::<u64>() {
                                conv.put(section, mkey, n.min(65535).to_string());
                                if n > 65535 {
                                    conv.todo(format!(
                                        "{key} {value} exceeds the MQTT 5 16-bit field that \
                                         [{section}] {mkey} maps to; it was clamped to 65535"
                                    ));
                                }
                            } else {
                                conv.todo(format!(
                                    "{key} {value}: not an integer this converter can map"
                                ));
                            }
                        }
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
                        _ => {
                            let rendered = toml_str(&value);
                            conv.put(section, mkey, rendered);
                        }
                    }
                } else if let Some(why) = bridge_key(key) {
                    conv.todo(format!("{named}: {why}"));
                } else if let Some(why) = no_equivalent(key) {
                    conv.todo(format!("{named}: {why}"));
                } else {
                    conv.todo(format!(
                        "{named}: no direct equivalent — check the mqttd configuration table"
                    ));
                }
            }
        }
    }
    conv
}

/// `max_connections` is PER LISTENER in Mosquitto and node-wide in mqttd.
///
/// mosquitto.conf(5) @ v2.0.22, verbatim: "Limit the total number of clients connected for
/// the current listener" — and "Set to -1 to have 'unlimited' connections", which is the
/// value the shipped mosquitto.conf carries as its documented default. Both halves were
/// wrong before 2026-08-15: the flat table collapsed several listeners LAST-WINS with no
/// trace (a cap of 100 on a TLS device listener replaced by 100000 from a browser listener),
/// and it passed `-1` straight through into a config the broker REJECTS.
pub fn convert_listener_caps(conv: &mut Conversion) {
    let sites: Vec<(String, String)> = conv
        .listeners
        .iter()
        .filter_map(|l| {
            l.caps
                .get("max_connections")
                .map(|v| (l.where_(), v.clone()))
        })
        .collect();
    if sites.is_empty() {
        return;
    }
    let mut caps: Vec<(String, u64)> = Vec::new();
    let mut unlimited: Vec<(String, String)> = Vec::new();
    let mut bad: Vec<(String, String)> = Vec::new();
    for (where_, value) in &sites {
        let trimmed = value.trim();
        if let Ok(n) = trimmed.parse::<u64>() {
            caps.push((where_.clone(), n));
        } else if trimmed.parse::<i64>().is_ok() {
            unlimited.push((where_.clone(), value.clone()));
        } else {
            bad.push((where_.clone(), value.clone()));
        }
    }
    for (where_, value) in bad {
        conv.todo(format!(
            "{where_} set max_connections {value}, which is not a number this converter can \
             map onto [limits] max_connections. Set it deliberately, or leave it unset for \
             uncapped"
        ));
    }
    for (where_, value) in &unlimited {
        conv.note(format!(
            "{where_} set max_connections {value}, which mosquitto.conf(5) documents as \
             UNLIMITED. mqttd spells unlimited as the key being ABSENT (max_connections is an \
             optional u64 — a negative number is refused outright by --check-config), so \
             [limits] max_connections was left unset, which is also uncapped. Cap it \
             deliberately — docs/SIZING.md has the arithmetic for a fixed RAM budget"
        ));
    }
    if caps.is_empty() {
        return;
    }
    let winner = caps.iter().map(|(_, v)| *v).min().unwrap_or_default();
    conv.put("limits", "max_connections", winner.to_string());
    let mut distinct: Vec<u64> = caps.iter().map(|(_, v)| *v).collect();
    distinct.sort_unstable();
    distinct.dedup();
    if distinct.len() > 1 || !unlimited.is_empty() {
        let listed: Vec<String> = sites.iter().map(|(w, v)| format!("{w}: {v}")).collect();
        conv.todo(format!(
            "max_connections is PER LISTENER in Mosquitto and NODE-WIDE in mqttd, and the \
             listeners disagreed ({}), so only one value can survive: the SMALLEST ({winner}) \
             was used, because a cap set deliberately low on one listener is a budget and \
             raising it silently is the permissive direction. The other values are GONE from \
             the output — raise it deliberately if that is not what you want, and note that \
             the node-wide cap now applies to every listener at once",
            listed.join("; ")
        ));
    }
}

/// Report the per-listener authn/authz keys mqttd can only hold node-wide.
///
/// mosquitto.conf(5) @ v2.0.22 names EIGHT settings that become PER LISTENER under
/// `per_listener_settings` — see [`SCOPED_SECURITY_LIST`]. mqttd has no per-listener
/// security at all, so two listeners that disagreed collapse onto ONE value.
pub fn convert_scoped_security(conv: &mut Conversion) {
    if conv.per_listener {
        conv.todo(format!(
            "per_listener_settings was TRUE, so in Mosquitto these were configured PER \
             LISTENER: {SCOPED_SECURITY_LIST}. mqttd has NO per-listener authentication or \
             authorization — [security] is NODE-WIDE — so every value below applies to EVERY \
             listener at once. Read each one against every listener it now governs, and split \
             the deployment in two if one listener really was anonymous or unauthorized and \
             another was not. Mosquitto's own caveat compounds it: a durable client that had \
             disconnected used the ACL of the listener it was LAST connected to, so the \
             policy a given session ran under may not be the one you are reading"
        ));
    }
    for key in [
        "allow_anonymous",
        "acl_file",
        "password_file",
        "psk_file",
        "plugin",
    ] {
        let sites = conv.scoped_sites(key);
        let mut distinct: Vec<&str> = sites.iter().map(|(_, v)| *v).collect();
        distinct.sort_unstable();
        distinct.dedup();
        if distinct.len() <= 1 {
            continue;
        }
        let listed: Vec<String> = sites.iter().map(|(w, v)| format!("{w}: {v}")).collect();
        let last = sites
            .last()
            .map(|(_, v)| *v)
            .unwrap_or_default()
            .to_string();
        let msg = format!(
            "{key} was set MORE THAN ONCE with DIFFERENT values ({}). mqttd's [security] is \
             node-wide, so only ONE can survive and the LAST one read ({last}) is what this \
             conversion used — which is what Mosquitto itself does with per_listener_settings \
             FALSE. If the listeners genuinely had different postures, that difference is \
             GONE from the output: split them across separate deployments, one per posture",
            listed.join("; ")
        );
        conv.todo(msg);
    }
    // ANONYMOUS ACCESS IS A POSTURE CHANGE, so it is never activated by this tool: mqttd
    // refuses anonymous clients by default, and switching that off for a whole node because
    // one Mosquitto listener allowed it is the fail-OPEN direction. The candidate is emitted
    // COMMENTED OUT with the input key it came from — the #162 precedent, applied without
    // exception (2026-08-15).
    let anon = conv
        .scoped_sites("allow_anonymous")
        .last()
        .filter(|(_, v)| truthy(v))
        .map(|(w, v)| ((*w).to_string(), (*v).to_string()));
    if let Some((where_, value)) = anon {
        let lines = conv.prov.inert(
            "allow_anonymous",
            "true",
            &format!("from allow_anonymous {value} at {where_} — NOT activated; see the TODO"),
        );
        conv.defer("security", lines);
        conv.todo(format!(
            "allow_anonymous was TRUE in mosquitto.conf ({where_}), which let clients connect \
             with NO credentials at all. mqttd refuses anonymous clients by default, and \
             turning that off is a SECURITY POSTURE CHANGE — node-wide, for every listener, \
             because [security] is not per-listener — so it is NOT carried over: the \
             candidate is emitted COMMENTED OUT in [security] below. Uncomment it only if you \
             really mean to keep an unauthenticated broker (anonymous access is how most \
             broker exposure incidents start), or give those clients a credential before \
             cutover — which is the whole point of migrating"
        ));
    }
}

/// Point `[security] acl_file` at the translated policy — or say plainly that authorization
/// is OFF when there is none.
///
/// Without this key mqttd enforces NO authorization at all (`crates/mqtt-config/src/lib.rs`
/// — `acl_file: Option<String>`, `None` by default, "without it authorization is not
/// enforced and loudly logged"). This converter translated a whole ACL policy and then never
/// referenced it from the config it wrote, so the deployed broker authorized nothing while
/// the generated ACL's own header said it denied by default. Found 2026-08-15.
///
/// The "no policy" arm is DERIVED, not asserted: it used to end "That is FAITHFUL to a
/// Mosquitto with no `acl_file` (which also authorized everything)", which is false for the
/// dynsec layout `mosquitto.conf(5)` itself recommends, where the whole role/ACL policy lives
/// in the plugin's JSON.
pub fn convert_acl_reference(conv: &mut Conversion, acl_path: Option<&str>) {
    if let Some(path) = acl_path {
        conv.set(
            "security",
            "acl_file",
            &toml_str("/etc/mqttd/acl.toml"),
            Some(&format!(
                "acl_file {path} (the POLICY is from there; the path below is this \
                 converter's own --out-acl deployment default)"
            )),
            Some("the deployed path itself, which is yours to choose"),
            None,
        );
        conv.note(
            "[security] acl_file points at /etc/mqttd/acl.toml — CHANGE IT if you write the \
             translated policy elsewhere, and keep the two together: mqttd enforces \
             authorization ONLY from the file this key names, and with the key unset it \
             enforces NONE of it (loudly logged on every start). The path is this converter's \
             default, not something discovered in mosquitto.conf",
        );
    } else {
        let unread = conv.unread.join("; ");
        let derived = if unread.is_empty() {
            "With no acl_file and no plugin in this file, Mosquitto also authorized \
             everything — so nothing was lost, and it is still the wrong end state"
                .to_string()
        } else {
            format!(
                "AND THIS FILE NAMED SOMETHING THIS CONVERTER DID NOT READ ({unread}), so do \
                 NOT conclude your old broker authorized everything: if that is a Dynamic \
                 Security plugin, your entire role and ACL policy is in there and NONE of it \
                 was seen. Export it and re-model it as an ACL policy"
            )
        };
        conv.todo(format!(
            "mosquitto.conf named NO acl_file, so no policy was translated and [security] \
             acl_file is NOT set below — which means mqttd will enforce NO authorization at \
             all: every authenticated client may publish and subscribe anywhere. {derived}. \
             Write an ACL policy and set acl_file, or re-run with --acl-file <the real acl \
             file> if the policy lives somewhere this file does not mention"
        ));
    }
}

/// A TLS-PSK listener is ENCRYPTED and UNMAPPABLE, so it must not become a plaintext bind.
///
/// mosquitto.conf(5) @ v2.0.22, verbatim: "The `psk_hint` option enables pre-shared-key support
/// for this listener and also acts as an identifier for this listener", and `psk_file` "Set the
/// path to a pre-shared-key file. This option requires a listener to be have PSK support
/// enabled."
///
/// Before 2026-08-15 neither key was in [`TLS_KEYS`] nor in the half-material safety net, so
/// [`Listener::is_tls`] was false and the listener took `plaintext_bind`: an encrypted listener
/// was published in CLEARTEXT, on its own port, while another TODO in the same file reported
/// that listener's `tls_version`. `mqttd --check-config` said `config OK`.
pub fn convert_psk(conv: &mut Conversion) {
    let mut messages: Vec<String> = Vec::new();
    for lst in conv.listeners.iter().filter(|l| l.is_psk()) {
        let also: Vec<String> = TLS_MATERIAL
            .iter()
            .filter_map(|k| lst.tls.get(*k).map(|v| format!("{k} {v}")))
            .collect();
        if lst.is_tls() {
            // A certificate AND a PSK hint: the certificate half translates, the PSK half
            // cannot, so the listener keeps its tls_bind and the PSK clients are the loss.
            messages.push(format!(
                "{} enabled TLS-PSK ({}) ALONGSIDE a certificate. The certificate half is \
                 translated below; the PSK half is NOT — mqttd has no PSK ciphersuites at all, \
                 so any client that authenticated with a pre-shared key rather than a \
                 certificate CANNOT connect after cutover (it fails in the TLS handshake, which \
                 looks like a network fault, not a policy one). Move those devices onto \
                 certificates or passwords before you cut over",
                lst.where_(),
                lst.psk_inventory()
            ));
            continue;
        }
        messages.push(format!(
            "{} was ENCRYPTED WITH TLS-PSK ({}) and has NO certificate: mosquitto.conf(5) @ \
             v2.0.22 — 'The psk_hint option enables pre-shared-key support for this listener'. \
             mqttd has NO PSK SUPPORT AT ALL (its TLS is certificate-based: TLS 1.3, or 1.2 \
             behind [tls] allow_tls12), so this listener CANNOT be translated. Converting it to \
             a plaintext bind would DOWNGRADE an encrypted transport to cleartext — every PSK \
             identity and every payload on the wire — so NO live bind was written for it: the \
             candidate is COMMENTED OUT in [listeners] below, on the TLS key, because that is \
             what the transport was. Issue certificates for those devices (or keep them on a \
             broker that speaks PSK) and uncomment it with [tls] cert/key set. Do NOT simply \
             move the port{}",
            lst.where_(),
            lst.psk_inventory(),
            if also.is_empty() {
                String::new()
            } else {
                format!(
                    ". That listener also carried {}, which is NOT in the output either",
                    also.join(", ")
                )
            }
        ));
        if let Some(path) = lst.psk.get("psk_file").filter(|p| !p.is_empty()) {
            messages.push(format!(
                "psk_file {path} at {}: a file of `identity:key` lines (mosquitto.conf(5)), \
                 which THIS CONVERTER DID NOT OPEN and could not translate if it had — mqttd has \
                 no PSK store. Those identities need a new credential each: a certificate CN, or \
                 an Argon2id password entry (`printf %s '<password>' | mqttd --hash-password \
                 <identity> >> passwd`), and the ACL translated beside this config must then key \
                 on whatever you choose",
                lst.where_()
            ));
        }
    }
    for msg in messages {
        conv.todo(msg);
    }
}

/// One translated ACL rule.
#[derive(Debug)]
pub struct Rule {
    identities: Vec<String>,
    actions: Vec<&'static str>,
    effect: &'static str,
    topics: Vec<String>,
    /// From a `topic` line BEFORE the first `user` line: Mosquitto scoped it to ANONYMOUS
    /// clients only, so the rendered rule says so.
    anonymous: bool,
}

/// The subject mqttd gives a client that connected with NO credentials
/// (`crates/mqtt-auth/src/basic.rs`: `Credentials::Anonymous if self.allow_anonymous =>
/// Identity { subject: "anonymous" }`). It is what makes Mosquitto's anonymous-scoped ACL block
/// expressible at all, rather than a rule that has to be dropped.
const ANON_IDENTITY: &str = "anonymous";

/// The placeholders mqttd substitutes in a rule's topic patterns
/// (`crates/mqtt-auth/src/acl.rs` `substitute`), unconditionally, in EVERY rule — while
/// Mosquitto substitutes only in a `pattern` line and treats a plain `topic` filter literally.
const SUBSTITUTED: [&str; 2] = ["%c", "%i"];

/// Translate a Mosquitto ACL file.
///
/// Mosquitto's model is **positional**: a `user X` line opens a block and `topic` lines
/// belong to it until the next `user`. `pattern` lines apply to everyone, with
/// substitution. mqttd's model is a list of rules with explicit identities, so this is a
/// regrouping rather than a line-for-line map.
///
/// THE FIRST BLOCK IS ANONYMOUS. mosquitto.conf(5) @ v2.0.22, verbatim: "The first set of topics
/// are applied to anonymous clients, assuming `allow_anonymous` is true. User specific topic
/// are added after a user line". Those pre-`user` lines used to be emitted with NO `identities`,
/// which mqttd applies to EVERY authenticated client ("Both lists empty means everyone",
/// `crates/mqtt-auth/src/acl.rs`) — strictly broader than the source in both postures. Found
/// 2026-08-15.
#[must_use]
pub fn parse_acl(text: &str) -> (Vec<Rule>, Vec<String>) {
    let mut rules = Vec::new();
    let mut todos: Vec<String> = Vec::new();
    let mut current_user: Option<String> = None;
    let mut seen_user = false;
    let mut anonymous_lines = 0_usize;

    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.splitn(2, char::is_whitespace);
        let key = parts.next().unwrap_or_default();
        let rest = parts.next().unwrap_or_default().trim();

        match key {
            "user" => {
                current_user = Some(rest.to_string());
                seen_user = true;
            }
            "topic" => {
                let (access, topic) = split_access(rest);
                // A `topic` filter is LITERAL in Mosquitto — the man page documents substitution
                // for `pattern` ONLY — but mqttd substitutes %c/%i in EVERY rule's topics and
                // has no escape for them, so carrying the filter across verbatim converts a rule
                // on one literal topic nobody publishes to into a live per-client grant. Refused
                // rather than widened. Found 2026-08-15.
                let used: Vec<&str> = SUBSTITUTED
                    .iter()
                    .copied()
                    .filter(|p| topic.contains(p))
                    .collect();
                if used.is_empty() {
                    if seen_user {
                        push_rule(&mut rules, &mut todos, current_user.clone(), access, topic);
                    } else {
                        anonymous_lines += 1;
                        push_anonymous_rule(&mut rules, &mut todos, access, topic);
                    }
                } else {
                    todos.push(format!(
                        "the plain `topic` line '{topic}' contains {}, which Mosquitto treats \
                         LITERALLY on a `topic` line (only `pattern` substitutes there) while \
                         mqttd substitutes %c (client id) and %i (identity) in EVERY rule's \
                         topics and has no escape for them (crates/mqtt-auth/src/acl.rs). \
                         Carrying it over would turn a rule on one literal topic into a live \
                         per-client grant the source never gave, so NO RULE WAS WRITTEN for it. \
                         If a per-client namespace IS what you want, write it as an mqttd rule \
                         deliberately (and note its substitutions FAIL CLOSED on a value \
                         containing / + or #); if the topic really is literal, rename it",
                        used.join(" and ")
                    ));
                }
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
    if anonymous_lines > 0 {
        todos.insert(
            0,
            format!(
                "{anonymous_lines} `topic` line(s) appeared BEFORE the first `user` line. \
                 mosquitto.conf(5) @ v2.0.22, verbatim: 'The first set of topics are applied to \
                 anonymous clients, assuming allow_anonymous is true' — so those lines granted \
                 access to ANONYMOUS clients ONLY, not to every user (the page draws the \
                 distinction explicitly: a `pattern` ACL applies to all users, a leading `topic` \
                 block does not). They are therefore emitted SCOPED to identities = \
                 [\"{ANON_IDENTITY}\"], which is the subject mqttd gives a client that connected \
                 with no credentials (crates/mqtt-auth/src/basic.rs) — NOT as unscoped rules, \
                 which mqttd applies to EVERY authenticated identity and which would be strictly \
                 broader than your Mosquitto policy. Consequences to check: (1) they grant \
                 NOTHING until [security] allow_anonymous is set in the generated config, and it \
                 is emitted COMMENTED OUT because mqttd refuses anonymous clients by default — \
                 if allow_anonymous was FALSE in mosquitto.conf these rules were already dead \
                 and you should delete them; (2) if you have a real named user called \
                 `{ANON_IDENTITY}`, these rules apply to it too — rename that user"
            ),
        );
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
    push_scoped_rule(rules, todos, identity, access, topic, false);
}

/// A `topic` line before the first `user` line: ANONYMOUS clients only, in Mosquitto.
fn push_anonymous_rule(rules: &mut Vec<Rule>, todos: &mut Vec<String>, access: &str, topic: &str) {
    push_scoped_rule(
        rules,
        todos,
        Some(ANON_IDENTITY.to_string()),
        access,
        topic,
        true,
    );
}

fn push_scoped_rule(
    rules: &mut Vec<Rule>,
    todos: &mut Vec<String>,
    identity: Option<String>,
    access: &str,
    topic: &str,
    anonymous: bool,
) {
    let (actions, effect): (Vec<&'static str>, &'static str) = match access {
        "deny" => (vec!["publish", "subscribe"], "deny"),
        "read" => (vec!["subscribe"], "allow"),
        "write" => (vec!["publish"], "allow"),
        "readwrite" => (vec!["publish", "subscribe"], "allow"),
        other => {
            todos.push(format!("unknown access type '{other}' for topic '{topic}'"));
            return;
        }
    };
    // A LITERAL `*` IN A USERNAME CANNOT BE EXPRESSED. mqttd's `identities` are GLOBS where `*`
    // matches any run of characters and there is NO escape (crates/mqtt-auth/src/acl.rs
    // `glob_match`), while Mosquitto matched the username literally, so emitting the rule would
    // grant it to every identity matching the pattern. Found 2026-08-15.
    if let Some(id) = identity.as_ref().filter(|i| i.contains('*')) {
        todos.push(format!(
            "the ACL scoped '{topic}' to the user '{id}', whose name contains a LITERAL `*`. \
             mqttd's rule `identities` are GLOBS — `*` matches any run of characters and there is \
             NO way to escape it (crates/mqtt-auth/src/acl.rs) — while Mosquitto matched that \
             username EXACTLY, so emitting the rule would grant it to every identity matching the \
             pattern (`a*b` would admit `a-admin-b`). NO RULE WAS WRITTEN for it: rename the \
             user, or add a rule by hand naming each identity you actually mean"
        ));
        return;
    }
    rules.push(Rule {
        // `.filter(non-empty)` mirrors the Python original's `[identity] if identity else []`:
        // a bare `user` line with no name scopes nothing, in both.
        identities: identity
            .filter(|i| !i.is_empty())
            .map(|i| vec![i])
            .unwrap_or_default(),
        actions,
        effect,
        topics: vec![topic.to_string()],
        anonymous,
    });
}

/// Mosquitto has NO `no_match` analogue: an `acl_file` is an allow list and anything it does
/// not permit is refused, so deny-by-default carries over exactly. That makes the value a
/// constant here — but it is still routed through [`Provenance`] and [`policy_effect`] rather
/// than written into the prose, because the class of defect this restructuring removes is a
/// sentence that ASSERTS what a computed value does.
pub const ACL_DEFAULT: &str = "deny";
const ACL_DEFAULT_SOURCE: &str = "mosquitto.conf(5): a Mosquitto acl_file is an allow list \
     with no `no_match` equivalent, so anything it does not permit was already refused";

/// Render the translated ACL policy.
#[must_use]
pub fn render_acl(rules: &[Rule], todos: &[String], default: &str) -> String {
    let mut prov = Provenance::default();
    let mut out: Vec<String> = vec![
        "# Translated from a Mosquitto acl_file by the mqttd Mosquitto converter".into(),
        "# (`mqttui migrate mosquitto`, or scripts/migrate/from-mosquitto.py).".into(),
        "#".into(),
    ];
    out.extend(DRAFT_HEADER.iter().map(ToString::to_string));
    out.extend([
        "#".into(),
        "# Mosquitto is positional (a `user` line opens a block); mqttd is a list of".into(),
        "# explicit rules. Read this through before deploying it — a converted policy".into(),
        "# is a draft, not an authority.".into(),
        "#".into(),
        comment_safe(&format!("# {}.", policy_effect(default))),
        "#".into(),
        "# It is enforced ONLY while [security] acl_file in the generated config names this".into(),
        "# file: with acl_file unset mqttd enforces NO authorization at all and says so in".into(),
        "# the log on every start.".into(),
        String::new(),
    ]);
    out.extend(prov.line(
        "default",
        &toml_str(default),
        Some(ACL_DEFAULT_SOURCE),
        None,
        None,
    ));
    out.push(String::new());
    for t in todos {
        out.push(format!("# TODO(migrate): {}", comment_safe(t)));
    }
    if !todos.is_empty() {
        out.push(String::new());
    }
    for r in rules {
        out.push("[[rules]]".into());
        if r.anonymous {
            out.push(
                "# from a `topic` line BEFORE the first `user` line: ANONYMOUS clients only".into(),
            );
        }
        if r.identities.is_empty() {
            out.push("# (no identities = applies to every authenticated client)".into());
        } else {
            out.push(format!("identities = {}", toml_list(&r.identities)));
        }
        out.push(format!("actions = {}", toml_list(&r.actions)));
        out.push(format!("effect = {}", toml_str(r.effect)));
        out.push(format!("topics = {}", toml_list(&r.topics)));
        out.push(String::new());
    }
    out.join("\n") + "\n"
}

/// A derived address the BROKER cannot bind, as an INERT candidate naming why.
///
/// `mqttd --check-config` accepts any string in a `*_bind` and the broker then refuses to start,
/// so the verification the docs point the operator at covered nothing here. See [`bind_gap`].
fn defer_unbindable(conv: &mut Conversion, key: &str, first: usize, addr: &str, why: &str) {
    let decide = format!(
        "{} gives [listeners] {key} as '{addr}', and that is not an address mqttd can bind: \
         {why}. `mqttd --check-config` ACCEPTS any string here and the broker then fails at \
         STARTUP, so the line is emitted COMMENTED OUT rather than live: set an address the \
         broker can bind and uncomment it",
        conv.listeners[first].where_()
    );
    let candidate = toml_str(addr);
    conv.set("listeners", key, &candidate, None, None, Some(&decide));
}

/// A TLS-PSK listener's bind, as an INERT candidate on the TLS key of its transport.
///
/// It is on the TLS key deliberately: that is the transport the input had, and emitting it on the
/// plaintext key — which is what happened until 2026-08-15 — downgrades an encrypted listener to
/// cleartext. See [`convert_psk`], which reports it in full.
fn defer_psk_bind_candidates(conv: &mut Conversion, psk_only: &[usize]) {
    for &i in psk_only {
        let transport = conv.listeners[i].transport().unwrap_or("mqtt");
        let key = bind_key(transport, true);
        let plain = bind_key(transport, false);
        let candidate = toml_str(
            &conv.listeners[i]
                .address()
                .unwrap_or_else(|| conv.listeners[i].candidate_address()),
        );
        let decide = format!(
            "{} was ENCRYPTED WITH TLS-PSK ({}) and mqttd has NO PSK support, so it could not be \
             translated and NO live bind was written for it. It is on the TLS key because that is \
             the transport the input had: converting it to [listeners] {plain} would downgrade an \
             encrypted listener to cleartext. Issue certificates for those clients, set [tls] \
             cert/key, and uncomment — see the TODO above",
            conv.listeners[i].where_(),
            conv.listeners[i].psk_inventory()
        );
        conv.set("listeners", key, &candidate, None, None, Some(&decide));
    }
}

/// mqttd binds ONE listener per protocol, so every other listener of that protocol becomes a
/// TODO naming its address — never a second table entry, and never silence.
fn defer_extra_listeners(conv: &mut Conversion, key: &str, extras: &[usize]) {
    for extra in extras {
        let addr = conv.listeners[*extra]
            .address()
            .unwrap_or_else(|| "(no address in the input)".to_string());
        let line = comment_safe(&format!(
            "# TODO(migrate): additional {} listener {addr} — mqttd binds ONE listener per \
             protocol; consolidate clients onto the bind above",
            key.trim_end_matches("_bind")
        ));
        conv.defer("listeners", vec![line]);
    }
}

/// One bind per (transport, TLS) pair — each one derived, or emitted INERT.
///
/// Four binds exist in mqttd (`plaintext_bind`, `tls_bind`, `ws_bind`, `wss_bind`) and this
/// converter used to write two, treating `protocol websockets` as an unmapped directive: a
/// WSS listener therefore claimed `tls_bind` and its browser clients got a raw-MQTT bind. A
/// listener whose transport cannot be positively identified from the input gets NO bind at
/// all, only a TODO.
pub fn render_listeners(conv: &mut Conversion) {
    let mut groups: BTreeMap<&'static str, Vec<usize>> = BTreeMap::new();
    let mut unknown: Vec<(String, String)> = Vec::new();
    let mut psk_only: Vec<usize> = Vec::new();
    // Only the binds emitted LIVE, so the cleartext warning below is derived from what the file
    // will DO rather than from what was found in the input: a listener whose address is missing
    // or unbindable contributes no bind at all.
    let mut live_binds: Vec<&'static str> = Vec::new();
    for (i, lst) in conv.listeners.iter().enumerate() {
        if lst.port_source.is_none()
            && lst.bind_source.is_none()
            && lst.tls.is_empty()
            && lst.psk.is_empty()
            && lst.protocol.is_none()
        {
            // The pre-`listener` scope, holding only a node-wide setting like
            // max_connections. Mosquitto starts the DEFAULT listener only when `port` or
            // `bind_address` names one, so this scope is not a listener and must not claim a
            // bind — that is how a global setting used to demote a real `listener` line to
            // "additional" and leave the actual bind commented out.
            continue;
        }
        match lst.transport() {
            None => unknown.push((lst.where_(), lst.protocol.clone().unwrap_or_default())),
            // ENCRYPTED-BUT-UNMAPPABLE: a PSK listener must NOT fall through to the plaintext
            // key. Reported by `convert_psk`; the candidate is emitted below on the TLS key of
            // its transport, commented out, because that is the transport the input had.
            Some(_) if lst.is_psk() && !lst.is_tls() => psk_only.push(i),
            Some(transport) => groups
                .entry(bind_key(transport, lst.is_tls()))
                .or_default()
                .push(i),
        }
    }
    for (where_, protocol) in unknown {
        conv.todo(format!(
            "{where_} set protocol '{protocol}', which is neither `mqtt` nor `websockets` \
             (mosquitto.conf(5) has no third value). This converter cannot identify that \
             listener's TRANSPORT from the input, so NO bind was written for it at all — a \
             bind is the one value that must never be guessed, since guessing wrong publishes \
             a raw-MQTT port for WebSocket clients or the reverse. Decide which of \
             plaintext_bind / tls_bind / ws_bind / wss_bind it should be and write it yourself"
        ));
    }

    defer_psk_bind_candidates(conv, &psk_only);

    for key in ["plaintext_bind", "tls_bind", "ws_bind", "wss_bind"] {
        let Some(mut group) = groups.get(key).cloned() else {
            continue;
        };
        // A listener whose address IS derivable takes the bind, whatever the document order:
        // otherwise a `certfile` written before the first `listener` line would leave the
        // bind commented out while a real, addressed listener of the same transport was
        // demoted to "additional". `sort_by_key` is stable, so ties keep document order.
        // An address mqttd cannot BIND is no better than one nobody derived: --check-config
        // accepts any string here and the broker then refuses to start, so the shape is checked
        // before the line goes out live.
        group.sort_by_key(|i| {
            let lst = &conv.listeners[*i];
            lst.address().is_none_or(|addr| bind_gap(&addr).is_some())
        });
        let first = group[0];
        let address = conv.listeners[first].address();
        if let Some(why) = address.as_deref().and_then(bind_gap) {
            defer_unbindable(conv, key, first, &address.clone().unwrap_or_default(), &why);
            defer_extra_listeners(conv, key, &group[1..]);
            continue;
        }
        match address {
            None => {
                // NOT fabricated. The listener exists (TLS material or a `protocol` line
                // attached to it) but nothing in the input named a port or an address, so
                // the bind is emitted commented with the decision named. `0.0.0.0:1883` used
                // to be invented here.
                let decide = format!(
                    "a {} listener was configured (see the settings attached to it) but {} — \
                     so this converter has NO address to put in [listeners] {key} and refuses \
                     to invent one. The commented line below is a PLACEHOLDER, not a value \
                     from your config: set the real address and port and uncomment it, or the \
                     broker binds nothing on that transport",
                    key.trim_end_matches("_bind"),
                    conv.listeners[first].address_gap()
                );
                let candidate = toml_str(&conv.listeners[first].candidate_address());
                conv.set("listeners", key, &candidate, None, None, Some(&decide));
            }
            Some(addr) => {
                live_binds.push(key);
                let source = conv.listeners[first].source();
                let defaulted = conv.listeners[first].host_defaulted();
                conv.set(
                    "listeners",
                    key,
                    &toml_str(&addr),
                    source.as_deref(),
                    defaulted,
                    None,
                );
            }
        }
        defer_extra_listeners(conv, key, &group[1..]);
    }
    if live_binds.contains(&"plaintext_bind") || live_binds.contains(&"ws_bind") {
        conv.defer(
            "listeners",
            vec![
                "# WARNING: plaintext. mqttd logs this as an INSECURE mode on every start."
                    .to_string(),
            ],
        );
    }
    if conv.listeners.is_empty() {
        conv.todo(
            "NO listener was found — mosquitto.conf named no `listener` block and no `port` \
             or `bind_address` for the default listener — so NO [listeners] bind was written \
             and the broker would bind NOTHING and serve no clients. Mosquitto's own default \
             is port 1883 on every interface; that is a default of the BROKER, not a value in \
             this file, so it is not carried over. Set the bind you actually want",
        );
    }
}

/// The ONE `[tls]` table, decided across EVERY TLS listener.
///
/// mqttd builds one rustls acceptor for `tls_bind` AND `wss_bind` and hands the same cert,
/// key and `client_ca` to `quic::server_endpoint` (`crates/mqttd/src/main.rs`), so there is
/// no per-listener TLS to translate into. That makes this function's job reporting, not
/// choosing: every TLS listener is walked and every setting the single table cannot hold
/// becomes a TODO NAMING the listener it came from, and every line it emits goes through
/// [`Provenance::line`] with the listener key it came from.
///
/// Round 2 (2026-08-14) found this code reading `tls_listeners.first()` only — the same
/// fail-open defect the EMQX and `HiveMQ` converters had been remediated for a round earlier,
/// surviving here because nobody was told to look at the Mosquitto pair.
#[allow(clippy::too_many_lines)] // one decision per branch; splitting it would hide the
                                 // three-case posture gate that is the whole point.
pub fn convert_tls(conv: &mut Conversion) -> Vec<String> {
    // -- per-listener keys that are not material: version floor and identity source -----
    let mut version_msgs: Vec<(bool, String)> = Vec::new();
    let mut identity_source: Option<String> = None;
    for lst in &conv.listeners {
        let where_ = lst.where_();
        if let Some(raw) = lst.tls_get("tls_version") {
            let version = raw.trim().to_lowercase();
            // mosquitto.conf(5) @ v2.0.22, verbatim: "Configure the minimum version of the
            // TLS protocol to be used for this listener ... In Mosquitto version 1.6.x and
            // earlier, this option set the only TLS protocol version that was allowed,
            // rather than the minimum." So the MINIMUM reading begins AFTER 1.6.x — at 2.0,
            // the only range this converter's table covers. This comment used to say
            // "since 1.6", which names the last release where the claim was false.
            if version == "tlsv1.3" {
                version_msgs.push((
                    false,
                    format!(
                        "{where_} set tls_version {raw}, which Mosquitto 2.x reads as the \
                     MINIMUM version — so that listener accepted TLS 1.3 only, which is \
                     exactly mqttd's default. Nothing to carry over"
                    ),
                ));
            } else if version == "tlsv1.2" {
                version_msgs.push((
                    true,
                    format!(
                        "{where_} set tls_version {raw}, which Mosquitto 2.x reads as the \
                     MINIMUM version (1.6.x and earlier read it as the ONLY version, so on \
                     those releases this listener was 1.2-ONLY), so it accepted TLS 1.2 AND \
                     1.3. mqttd offers TLS 1.3 ONLY by default and a 1.2-only client fails to \
                     connect in a way that looks like a network fault, not a policy one. If \
                     your fleet needs 1.2, opt in with [tls] allow_tls12 = true — hardened \
                     (ECDHE+AEAD only, Extended Master Secret required), loudly logged on \
                     every start, and applied to EVERY TLS transport — and plan its \
                     retirement"
                    ),
                ));
            } else {
                version_msgs.push((
                    true,
                    format!(
                        "{where_} set tls_version {raw}, a floor BELOW TLS 1.2. mqttd offers 1.3, \
                     plus 1.2 behind [tls] allow_tls12 = true, and nothing older at all: any \
                     client that can only do 1.1 or 1.0 CANNOT connect after cutover. Find \
                     those clients before you move them"
                    ),
                ));
            }
        }
        if let Some(raw) = lst.tls_get("use_identity_as_username") {
            if truthy(raw) {
                identity_source = Some(format!("use_identity_as_username {raw} at {where_}"));
                version_msgs.push((
                    false,
                    format!(
                        "{where_} set use_identity_as_username {raw}, which in Mosquitto takes \
                     the client certificate's CN as the username and then does NOT consult \
                     password_file for that listener (mosquitto.conf(5)). mqttd has an EXACT \
                     equivalent — [security] mtls_identity_source, whose default is already \
                     \"cn\" — and it is written out below explicitly so the mapping is \
                     visible rather than implied. It is NODE-WIDE, so every TLS listener \
                     identifies clients by certificate CN, and the ACL translated beside this \
                     config must key on those CNs"
                    ),
                ));
            } else {
                version_msgs.push((
                    true,
                    format!(
                        "{where_} set use_identity_as_username {raw}, so Mosquitto took the \
                     username from CONNECT (password_file) even for a client that presented a \
                     certificate. mqttd has NO switch for that: whenever a client presents a \
                     verified certificate on a client listener its identity is read FROM THE \
                     CERTIFICATE — the field [security] mtls_identity_source names, default \
                     \"cn\" (crates/mqtt-auth/src/mtls.rs, and there is no fallback to \
                     another field). The identity your ACL matches therefore CHANGES at \
                     cutover from the CONNECT username to the certificate CN: check every \
                     rule in the translated ACL against the CNs your device certificates \
                     actually carry"
                    ),
                ));
            }
        }
        if let Some(raw) = lst.tls_get("use_subject_as_username") {
            if truthy(raw) {
                version_msgs.push((
                    true,
                    format!(
                        "{where_} set use_subject_as_username {raw}, which takes the WHOLE \
                     certificate subject (`CN=…,OU=…,O=…`) as the username. mqttd's \
                     [security] mtls_identity_source offers cn, san-dns, san-uri and \
                     san-email ONLY — there is no full-subject source \
                     (crates/mqtt-config/src/lib.rs) — so this was NOT mapped and no value \
                     was written. Either re-key the ACL onto the CN alone (and set \
                     mtls_identity_source = \"cn\"), or move the identity into a SAN and pick \
                     the matching source"
                    ),
                ));
            } else {
                version_msgs.push((
                    false,
                    format!(
                        "{where_} set use_subject_as_username {raw}, which is off and is also \
                     mqttd's behaviour (the identity is the CN, not the full subject). \
                     Nothing to carry over"
                    ),
                ));
            }
        }
    }
    for (is_todo, msg) in version_msgs {
        if is_todo {
            conv.todo(msg);
        } else {
            conv.note(msg);
        }
    }
    if let Some(source) = identity_source {
        conv.set(
            "security",
            "mtls_identity_source",
            &toml_str("cn"),
            Some(&source),
            None,
            None,
        );
    }

    // -- listeners that carry TLS settings but no certfile were never encrypted ----------
    let orphans: Vec<String> = conv
        .listeners
        .iter()
        // A PSK listener is EXCLUDED: it WAS encrypted, so "Mosquitto served that listener as
        // PLAINTEXT" would be false about it. `convert_psk` reports it, including whatever
        // material it also carried.
        .filter(|l| {
            !l.is_tls() && !l.is_psk() && TLS_MATERIAL.iter().any(|k| l.tls.contains_key(*k))
        })
        .map(|l| {
            let listed: Vec<String> = TLS_MATERIAL
                .iter()
                .filter_map(|k| l.tls.get(*k).map(|v| format!("{k} {v}")))
                .collect();
            format!(
                "{} carried TLS settings ({}) but NO certfile, so Mosquitto served that \
                 listener as PLAINTEXT and nothing here becomes TLS either. If it was meant \
                 to be encrypted, it never was — check it before cutover",
                l.where_(),
                listed.join(", ")
            )
        })
        .collect();
    for msg in orphans {
        conv.todo(msg);
    }

    let tls_idx: Vec<usize> = (0..conv.listeners.len())
        .filter(|i| conv.listeners[*i].is_tls())
        .collect();
    if tls_idx.is_empty() {
        return Vec::new();
    }
    let first = tls_idx[0];
    let first_where = conv.listeners[first].where_();
    let mut out = vec!["[tls]".to_string()];

    let inventory = |l: &Listener| -> String {
        format!(
            "{}: certfile={}, keyfile={}, cafile={}, require_certificate={}, crlfile={}",
            l.where_(),
            l.tls_get("certfile").unwrap_or("unset"),
            l.tls_get("keyfile").unwrap_or("unset"),
            l.tls_get("cafile").unwrap_or("unset"),
            l.tls_get("require_certificate").unwrap_or("unset"),
            l.tls_get("crlfile").unwrap_or("unset"),
        )
    };

    if tls_idx.len() > 1 {
        let listed: Vec<String> = tls_idx
            .iter()
            .map(|i| inventory(&conv.listeners[*i]))
            .collect();
        let msg = format!(
            "{} TLS listeners were found ({}). mqttd has ONE [tls] table and applies it to \
             tls_bind, wss_bind AND quic_bind alike (one shared acceptor plus \
             quic::server_endpoint), so per-listener TLS cannot be expressed at all: \
             {first_where}'s material is what the table below holds, and it is what EVERY TLS \
             transport will use. Read each listener's entry above against the posture the \
             table ends up with",
            tls_idx.len(),
            listed.join("; ")
        );
        conv.todo(msg);
        let mut materials: Vec<(String, String, String)> = tls_idx
            .iter()
            .map(|i| {
                let l = &conv.listeners[*i];
                (
                    l.tls_get("certfile").unwrap_or_default().to_string(),
                    l.tls_get("keyfile").unwrap_or_default().to_string(),
                    l.tls_get("cafile").unwrap_or_default().to_string(),
                )
            })
            .collect();
        materials.sort();
        materials.dedup();
        if materials.len() > 1 {
            conv.todo(format!(
                "those TLS listeners carry DIFFERENT TLS material, and only ONE set can be \
                 referenced: {first_where}'s certfile/keyfile/cafile went into [tls] below and \
                 the other listeners' PEM files are referenced NOWHERE in the generated \
                 config, while their transports are served from the material that IS \
                 referenced. Reissue one certificate covering every name (a SAN per \
                 hostname), or split the listeners across separate deployments"
            ));
        }
    }

    let cert = conv.listeners[first]
        .tls_get("certfile")
        .unwrap_or_default()
        .to_string();
    let cert_line = conv.prov.line(
        "cert",
        &toml_str(&cert),
        Some(&format!("certfile at {first_where}")),
        None,
        None,
    );
    out.extend(cert_line);
    let keyfile = conv.listeners[first]
        .tls_get("keyfile")
        .map(ToString::to_string);
    if let Some(v) = keyfile {
        let line = conv.prov.line(
            "key",
            &toml_str(&v),
            Some(&format!("keyfile at {first_where}")),
            None,
            None,
        );
        out.extend(line);
    } else {
        let decide = format!(
            "{first_where} named a certfile but NO keyfile, so there is nothing to put in \
             [tls] key and the broker REFUSES to start without it. Set key to an UNENCRYPTED \
             PEM private key of your own (mount it from a Secret) and uncomment — the path \
             below is a placeholder, not a value from your config"
        );
        let line = conv.prov.line(
            "key",
            &toml_str("/etc/mqttd/tls/server.key"),
            None,
            None,
            Some(&decide),
        );
        out.extend(line);
    }

    // -- the mTLS mandate, decided across EVERY TLS listener ----------------------------
    //
    // Mosquitto's cafile only VERIFIES a certificate the client CHOOSES to present unless
    // require_certificate is true; mqttd's client_ca MANDATES one, for every TLS transport at
    // once. So only a UNANIMOUS require_certificate is a mapping — the #162 precedent: a
    // mapping that changes SECURITY POSTURE is not a mapping, so the candidate is emitted
    // COMMENTED OUT with a TODO instead.
    let is_required = |l: &Listener| truthy(l.tls_get("require_certificate").unwrap_or("false"));
    let required: Vec<usize> = tls_idx
        .iter()
        .copied()
        .filter(|i| is_required(&conv.listeners[*i]))
        .collect();
    let lax: Vec<usize> = tls_idx
        .iter()
        .copied()
        .filter(|i| !is_required(&conv.listeners[*i]))
        .collect();
    let with_ca: Vec<usize> = tls_idx
        .iter()
        .copied()
        .filter(|i| conv.listeners[*i].tls_get("cafile").is_some())
        .collect();
    let mut cas: Vec<String> = with_ca
        .iter()
        .map(|i| {
            conv.listeners[*i]
                .tls_get("cafile")
                .unwrap_or_default()
                .to_string()
        })
        .collect();
    cas.sort();
    cas.dedup();
    let ca_idx: Option<usize> = required
        .iter()
        .copied()
        .find(|i| conv.listeners[*i].tls_get("cafile").is_some())
        .or_else(|| with_ca.first().copied());
    let ca: Option<String> = ca_idx.map(|i| {
        conv.listeners[i]
            .tls_get("cafile")
            .unwrap_or_default()
            .to_string()
    });
    let joined = |idx: &[usize], conv: &Conversion| -> String {
        idx.iter()
            .map(|i| conv.listeners[*i].where_())
            .collect::<Vec<_>>()
            .join("; ")
    };
    let mandated = !required.is_empty() && lax.is_empty() && ca.is_some();

    if mandated {
        let ca = ca.clone().unwrap_or_default();
        let ca_where = ca_idx.map_or_else(String::new, |i| conv.listeners[i].where_());
        let line = conv.prov.line(
            "client_ca",
            &toml_str(&ca),
            Some(&format!("cafile + require_certificate at {ca_where}")),
            None,
            None,
        );
        out.extend(line);
        let mut required_cas: Vec<String> = required
            .iter()
            .map(|i| {
                conv.listeners[*i]
                    .tls_get("cafile")
                    .unwrap_or_default()
                    .to_string()
            })
            .collect();
        required_cas.sort();
        required_cas.dedup();
        let disagree = if required_cas.len() > 1 {
            format!(
                "; those listeners also disagree on cafile ({}) and only {ca} was used — \
                 concatenate the anchors into one PEM if both are still in use",
                cas.join(", ")
            )
        } else {
            String::new()
        };
        let msg = format!(
            "require_certificate was TRUE on every TLS listener ({}), so mTLS is MANDATORY \
             and [tls] client_ca is set — for tls_bind, wss_bind and quic_bind alike, because \
             mqttd has one posture for every TLS transport. mqttd additionally requires the \
             clientAuth extended key usage on every client certificate and refuses one \
             without it at the handshake, which OpenSSL-based brokers tolerated missing for \
             years. Audit the fleet BEFORE cutover: scripts/migrate/cert-audit.sh \
             <dir-of-client-certs>{disagree}",
            joined(&required, conv)
        );
        conv.note(msg);
    } else if !required.is_empty() && !lax.is_empty() {
        // THE fail-open case: an mTLS MANDATE on a listener that is not first in document
        // order used to vanish entirely. Neither arm is a translation, so neither is silent.
        let lax_detail: Vec<String> = lax
            .iter()
            .map(|i| {
                let l = &conv.listeners[*i];
                format!(
                    "{} (require_certificate {})",
                    l.where_(),
                    l.tls_get("require_certificate").unwrap_or("unset")
                )
            })
            .collect();
        let msg = format!(
            "TLS listeners DISAGREE about client certificates, and mqttd cannot hold both \
             postures: require_certificate was TRUE on {} but NOT on {}. [tls] client_ca \
             MANDATES mTLS for tls_bind, wss_bind and quic_bind AT ONCE — setting it newly \
             demands a certificate from clients that never presented one, and leaving it \
             unset DROPS a mandate you have today. Neither is a translation, so client_ca is \
             emitted COMMENTED OUT below: uncomment it to mandate mTLS fleet-wide (audit \
             every client first with scripts/migrate/cert-audit.sh, and expect the cert-less \
             clients to fail the handshake), or leave it commented and move the mTLS-required \
             clients to a SEPARATE deployment that sets it. Do NOT deploy this file believing \
             the require_certificate listener kept its mandate",
            joined(&required, conv),
            lax_detail.join("; ")
        );
        conv.todo(msg);
        out.push(comment_safe(&format!(
            "# TODO(migrate): client certificates were REQUIRED on {} but not on {}; mqttd \
             has ONE posture for every TLS transport. Uncommenting mandates mTLS EVERYWHERE \
             (see the TODO above):",
            joined(&required, conv),
            joined(&lax, conv)
        )));
        if let (Some(ca), Some(i)) = (&ca, ca_idx) {
            let ca_where = conv.listeners[i].where_();
            let line = conv.prov.inert(
                "client_ca",
                &toml_str(ca),
                &format!("from cafile at {ca_where}"),
            );
            out.extend(line);
        } else {
            let line = conv.prov.inert(
                "client_ca",
                &toml_str("/etc/mqttd/tls/client-ca.crt"),
                "PLACEHOLDER — no cafile was found on the REQUIRED listener, so this path \
                 came from nowhere in your config; supply the anchors",
            );
            out.extend(line);
        }
    } else if !with_ca.is_empty() {
        let detail: Vec<String> = with_ca
            .iter()
            .map(|i| {
                let l = &conv.listeners[*i];
                format!(
                    "{}: cafile={}, require_certificate={}",
                    l.where_(),
                    l.tls_get("cafile").unwrap_or_default(),
                    l.tls_get("require_certificate").unwrap_or("unset")
                )
            })
            .collect();
        out.push(comment_safe(&format!(
            "# TODO(migrate): cafile was set but require_certificate was NOT true on any TLS \
             listener ({}). mqttd's client_ca MANDATES client certificates (mTLS) — there is \
             no cert-optional mode — and it applies to tls_bind, wss_bind and quic_bind at \
             once. Uncomment to require certs fleet-wide (audit them first with \
             scripts/migrate/cert-audit.sh), or leave commented for server-only TLS:",
            detail.join("; ")
        )));
        let mut remaining = cas.clone();
        for i in &with_ca {
            let candidate = conv.listeners[*i]
                .tls_get("cafile")
                .unwrap_or_default()
                .to_string();
            let Some(pos) = remaining.iter().position(|c| *c == candidate) else {
                continue;
            };
            remaining.remove(pos);
            let where_ = conv.listeners[*i].where_();
            let line = conv.prov.inert(
                "client_ca",
                &toml_str(&candidate),
                &format!("from cafile at {where_}"),
            );
            out.extend(line);
        }
    } else if !required.is_empty() {
        let msg = format!(
            "{} set require_certificate true but named NO cafile, so this converter has no \
             trust anchor to put in [tls] client_ca and mTLS is NOT mandated below. Find the \
             CA bundle Mosquitto was verifying against and set client_ca to it, or the \
             mandate is gone",
            joined(&required, conv)
        );
        conv.todo(msg);
    }

    // -- revocation. `crl` is ONLY legal beside an active client_ca ----------------------
    //
    // The broker's own words: `invalid configuration: tls.crl requires tls.client_ca`. This
    // code used to emit `crl` whenever the chosen listener named a crlfile, so the ordinary
    // cafile-without-require_certificate input — which the differential fixture itself has —
    // produced a config `mqttd --check-config` REJECTS. Rule 3, "the output must validate",
    // broken in the converter a `cargo install mqttui` user runs. Found 2026-08-15.
    let crls: Vec<usize> = tls_idx
        .iter()
        .copied()
        .filter(|i| conv.listeners[*i].tls_get("crlfile").is_some())
        .collect();
    if !crls.is_empty() {
        let chosen = conv.listeners[crls[0]]
            .tls_get("crlfile")
            .unwrap_or_default()
            .to_string();
        let chosen_where = conv.listeners[crls[0]].where_();
        let detail: Vec<String> = crls
            .iter()
            .map(|i| {
                let l = &conv.listeners[*i];
                format!(
                    "{}: {}",
                    l.where_(),
                    l.tls_get("crlfile").unwrap_or_default()
                )
            })
            .collect();
        let mut distinct: Vec<&str> = crls
            .iter()
            .map(|i| conv.listeners[*i].tls_get("crlfile").unwrap_or_default())
            .collect();
        distinct.sort_unstable();
        distinct.dedup();
        if mandated {
            let line = conv.prov.line(
                "crl",
                &toml_str(&chosen),
                Some(&format!("crlfile at {chosen_where}")),
                None,
                None,
            );
            out.extend(line);
        } else {
            out.push(comment_safe(&format!(
                "# TODO(migrate): a crlfile was set ({}) but client_ca above is NOT set, and \
                 the broker REFUSES the config outright in that combination — `invalid \
                 configuration: tls.crl requires tls.client_ca`. Revocation is only ever \
                 consulted for a CLIENT certificate, so it is meaningless without the \
                 mandate. Decide the mTLS posture first; uncomment BOTH lines together:",
                detail.join("; ")
            )));
            let line = conv.prov.inert(
                "crl",
                &toml_str(&chosen),
                &format!("from crlfile at {chosen_where}"),
            );
            out.extend(line);
        }
        if distinct.len() > 1 {
            let msg = format!(
                "several TLS listeners named DIFFERENT crlfile values ({}), and mqttd has ONE \
                 [tls] crl: {chosen} is the one in the table and the others are referenced \
                 NOWHERE, so a certificate revoked only in one of them is still accepted. \
                 Concatenate every CRL into one PEM file — it is hot-reloaded on SIGHUP, \
                 which also evicts the live sessions of a revoked client",
                detail.join("; ")
            );
            conv.todo(msg);
        }
    }

    let capaths: Vec<String> = conv
        .listeners
        .iter()
        .filter(|l| l.tls.contains_key("capath"))
        .map(|l| {
            format!(
                "{}: {}",
                l.where_(),
                l.tls_get("capath").unwrap_or_default()
            )
        })
        .collect();
    if !capaths.is_empty() {
        out.push(comment_safe(&format!(
            "# TODO(migrate): capath names a DIRECTORY of CA certificates ({}), which mqttd \
             does not support — and THIS CONVERTER DID NOT READ THAT DIRECTORY, so no anchor \
             inside it was seen or reported. Concatenate the certificates it holds into one \
             PEM and set client_ca to that file",
            capaths.join("; ")
        )));
    }
    out
}

/// Render the translated broker configuration.
#[must_use]
pub fn render_config(conv: &Conversion, tls_lines: &[String]) -> String {
    let mut out: Vec<String> = vec![
        "# Translated from mosquitto.conf by the mqttd Mosquitto converter".into(),
        "# (`mqttui migrate mosquitto`, or scripts/migrate/from-mosquitto.py).".into(),
        "#".into(),
    ];
    out.extend(DRAFT_HEADER.iter().map(ToString::to_string));
    out.extend([
        "#".into(),
        "# Review every line, then validate before deploying:".into(),
        "#     mqttd --check-config --config this-file.toml".into(),
        "#".into(),
        "# Settings with no mqttd equivalent are listed as TODO(migrate) rather than".into(),
        "# dropped silently — a converter that quietly loses a setting is worse than".into(),
        "# no converter, because you would deploy believing it came across.".into(),
        String::new(),
    ]);
    for n in &conv.notes {
        out.push(format!("# NOTE: {}", comment_safe(n)));
    }
    if !conv.notes.is_empty() {
        out.push(String::new());
    }
    for t in &conv.todos {
        out.push(format!("# TODO(migrate): {}", comment_safe(t)));
    }
    if !conv.todos.is_empty() {
        out.push(String::new());
    }

    for section in ["node", "listeners", "security", "limits"] {
        let body = conv.config.get(section);
        let deferred = conv.deferred.get(section);
        let empty_body = body.is_none_or(Vec::is_empty);
        if empty_body && deferred.is_none_or(Vec::is_empty) {
            continue;
        }
        out.push(format!("[{section}]"));
        if let Some(body) = body {
            for (k, v) in body {
                out.push(format!("{k} = {v}"));
            }
        }
        if let Some(deferred) = deferred {
            out.extend(deferred.iter().cloned());
        }
        out.push(String::new());
    }

    if !tls_lines.is_empty() {
        out.push("# --- TLS ---".into());
        out.push("#".into());
        out.push("# mqttd has ONE [tls] table and applies it to tls_bind, wss_bind and".into());
        out.push("# quic_bind alike. TLS is 1.3-only by default: a client that cannot".into());
        out.push("# negotiate TLS 1.3 will fail to connect, so check your device fleet.".into());
        out.extend(tls_lines.iter().cloned());
        out.push(String::new());
    }
    out.join("\n") + "\n"
}

/// `mqttui migrate mosquitto <conf> [--out-config P] [--out-acl P] [--acl-file P]`.
///
/// # Errors
/// If the configuration cannot be read, or an output cannot be written.
#[allow(clippy::similar_names)]
// `conf` (the path) and `conv` (the conversion) are the
// clearest names for each; renaming either would be worse.
#[allow(clippy::too_many_lines)] // argument parsing, then one linear pipeline whose ORDER is
                                 // what the differential test pins; splitting it would move
                                 // the ordering into call sites and hide it.
pub fn run(args: &[String]) -> Result<String, String> {
    let mut conf: Option<&String> = None;
    let mut out_config: Option<&String> = None;
    let mut out_acl: Option<&String> = None;
    let mut acl_override: Option<&String> = None;
    let mut provenance_json: Option<&String> = None;
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
            "--provenance-json" => {
                provenance_json = args.get(i + 1);
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

    // A non-UTF-8 config (saved as UTF-16, or holding a latin-1 path) fails HERE with a
    // message, which is the documented contract — the Python original used to raise a bare
    // UnicodeDecodeError traceback instead.
    let text = std::fs::read_to_string(conf).map_err(|e| {
        format!(
            "cannot read {conf}: {e}. If it is not valid UTF-8, re-save it as UTF-8 (`iconv -f \
             <encoding> -t utf-8`) and re-run"
        )
    })?;
    let mut conv = parse_conf(&text);
    convert_scoped_security(&mut conv);
    convert_psk(&mut conv);
    convert_listener_caps(&mut conv);
    let tls_lines = convert_tls(&mut conv);
    render_listeners(&mut conv);
    // Rule 3, "the output must validate": see DATA_DIR_NOTE. Applied here rather than in
    // parse_conf so the ordering matches the Python original's main(), which the
    // differential test compares byte for byte.
    if !conv.has("node", "data_dir") {
        let rendered = toml_str("/var/lib/mqttd");
        conv.put("node", "data_dir", rendered);
        conv.note(DATA_DIR_NOTE);
    }

    // THE ACL SOURCE IS READ HERE, BEFORE THE CONFIG IS RENDERED, on purpose. When it cannot
    // be read, the gap belongs in the file the operator is about to DEPLOY, not in a report
    // line they scroll past — and the config must not go on naming a policy that was never
    // written.
    let mut report = String::new();
    let acl_path = acl_override.cloned().or_else(|| conv.acl_file.clone());
    let mut acl_text: Option<String> = None;
    if let Some(path) = &acl_path {
        match std::fs::read_to_string(path) {
            Ok(t) => acl_text = Some(t),
            Err(e) => {
                let _ = writeln!(report, "note: could not read acl_file {path}: {e}");
                conv.todo(format!(
                    "THE AUTHORIZATION POLICY WAS NOT TRANSLATED. The Mosquitto ACL file \
                     {path} could not be read ({e}), so NOT ONE RULE from it is in the \
                     generated ACL — which carries the same warning and no rules. [security] \
                     acl_file below still names a policy file, and {}: that is the right \
                     direction and it is NOT a migration. Fix the path (Mosquitto resolves a \
                     relative acl_file against its own working directory) or pass --acl-file, \
                     and re-run before deploying",
                    policy_effect(ACL_DEFAULT)
                ));
            }
        }
    }

    convert_acl_reference(&mut conv, acl_path.as_deref());

    let config = render_config(&conv, &tls_lines);
    if let Some(path) = out_config {
        std::fs::write(path, &config).map_err(|e| format!("cannot write {path}: {e}"))?;
        let _ = writeln!(report, "wrote {path}");
    } else {
        report.push_str(&config);
    }

    if let Some(acl_path) = &acl_path {
        // An unreadable source is not fatal (the contract: exit 0 with the gap named), but
        // the ACL document is still WRITTEN — deny-by-default, zero rules, and the gap
        // stated at the top, so the file the operator deploys says what happened.
        let (rules, todos) = match &acl_text {
            None => (
                Vec::new(),
                vec![format!(
                    "NOTHING WAS TRANSLATED INTO THIS FILE. The Mosquitto ACL file {acl_path} \
                     could not be read, so this policy has NO rules and {}. Fix the path (or \
                     pass --acl-file) and re-run",
                    policy_effect(ACL_DEFAULT)
                )],
            ),
            Some(t) => {
                let (rules, mut todos) = parse_acl(t);
                if rules.is_empty() {
                    todos.insert(
                        0,
                        format!(
                            "NO RULE could be translated from the Mosquitto ACL file. Either \
                             every line landed on a gap listed below, or the file held \
                             nothing this converter recognises. With no rules, {}. Read the \
                             TODOs below before deploying",
                            policy_effect(ACL_DEFAULT)
                        ),
                    );
                }
                (rules, todos)
            }
        };
        let acl = render_acl(&rules, &todos, ACL_DEFAULT);
        if let Some(path) = out_acl {
            std::fs::write(path, &acl).map_err(|e| format!("cannot write {path}: {e}"))?;
            let _ = writeln!(report, "wrote {path} ({} rules)", rules.len());
        } else {
            report.push_str(&acl);
        }
    }

    if let Some(path) = provenance_json {
        std::fs::write(path, conv.prov.ledger("mqttui migrate mosquitto"))
            .map_err(|e| format!("cannot write {path}: {e}"))?;
        let _ = writeln!(report, "wrote {path}");
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
    /// Render a conversion the way `run` does, so a test cannot pass on a path the binary
    /// never takes.
    fn convert(text: &str) -> (Conversion, String) {
        let mut conv = parse_conf(text);
        convert_scoped_security(&mut conv);
        convert_listener_caps(&mut conv);
        let tls_lines = convert_tls(&mut conv);
        render_listeners(&mut conv);
        let acl_path = conv.acl_file.clone();
        convert_acl_reference(&mut conv, acl_path.as_deref());
        let rendered = render_config(&conv, &tls_lines);
        (conv, rendered)
    }

    #[test]
    fn unmapped_settings_become_visible_todos() {
        let (conv, rendered) = convert("sys_interval 10\nsome_future_option 3\n");
        // Two unmapped directives, plus "no acl_file, so authorization is OFF" and "NO
        // listener was found" — the two gaps every such conversion now carries.
        assert_eq!(conv.todo_count(), 4);
        assert!(rendered.contains("TODO(migrate): sys_interval 10"));
        assert!(rendered.contains("TODO(migrate): some_future_option 3"));
    }

    /// An `include_dir` is a whole file tree this converter never opened. Reporting it as
    /// "no direct equivalent" reads as "mqttd has no includes, fine" rather than "your
    /// authn/authz may live in there and was never seen".
    #[test]
    fn an_include_dir_says_its_contents_were_not_read() {
        let (_, rendered) = convert("include_dir /etc/mosquitto/conf.d\n");
        assert!(
            rendered.contains("DID NOT OPEN THAT DIRECTORY")
                && rendered.contains("/etc/mosquitto/conf.d"),
            "{rendered}"
        );
    }

    /// The fail-open case round 2 proved: an mTLS mandate on a TLS listener that is not
    /// first in document order must not vanish.
    #[test]
    fn an_mtls_mandate_on_a_later_listener_is_not_dropped() {
        let (_, rendered) = convert(
            "listener 8884\ncertfile /b.crt\nkeyfile /b.key\n\
             listener 8883\ncertfile /d.crt\nkeyfile /d.key\ncafile /d-ca.crt\n\
             require_certificate true\ncrlfile /d.crl\n",
        );
        assert!(
            rendered.contains("TLS listeners DISAGREE about client certificates"),
            "{rendered}"
        );
        assert!(
            rendered.contains("# client_ca = \"/d-ca.crt\""),
            "{rendered}"
        );
        // ...and a CRL beside an inactive client_ca is a config the broker REJECTS.
        assert!(!rendered.contains("\ncrl = "), "{rendered}");
        assert!(rendered.contains("# crl = \"/d.crl\""), "{rendered}");
    }

    /// A `crlfile` may only be emitted active beside an active `client_ca`: the broker's own
    /// words are `invalid configuration: tls.crl requires tls.client_ca`.
    #[test]
    fn a_crl_is_only_active_when_the_mandate_is() {
        let (_, rendered) = convert(
            "listener 8883\ncertfile /d.crt\nkeyfile /d.key\ncafile /ca.crt\n\
             require_certificate true\ncrlfile /d.crl\n",
        );
        assert!(rendered.contains("client_ca = \"/ca.crt\""), "{rendered}");
        assert!(rendered.contains("\ncrl = \"/d.crl\""), "{rendered}");
    }

    /// `per_listener_settings` makes Mosquitto's authn/authz keys per-listener; mqttd's are
    /// node-wide, so a disagreement collapses and must be reported.
    #[test]
    fn a_per_listener_security_collapse_is_reported() {
        let (_, rendered) = convert(
            "per_listener_settings true\nlistener 1883\nallow_anonymous true\n\
             listener 8883\ncertfile /c.crt\nallow_anonymous false\n",
        );
        assert!(
            rendered.contains("per_listener_settings was TRUE"),
            "{rendered}"
        );
        assert!(
            rendered.contains("allow_anonymous was set MORE THAN ONCE"),
            "{rendered}"
        );
        // The LAST value wins, as Mosquitto does with per_listener_settings false — and the
        // NOTE must not claim anonymous was carried over when it was not.
        assert!(!rendered.contains("allow_anonymous = true"), "{rendered}");
        assert!(!rendered.contains("exposure incidents"), "{rendered}");
    }

    /// A translated ACL that the config never references is not enforced at all.
    #[test]
    fn a_config_with_no_acl_file_says_authorization_is_off() {
        let (_, rendered) = convert("listener 1883\n");
        assert!(
            rendered.contains("acl_file is NOT set below")
                && rendered.contains("NO authorization at all"),
            "{rendered}"
        );
    }

    /// Anonymous access is a POSTURE CHANGE, so the candidate is emitted COMMENTED OUT with
    /// the input key it came from — never activated node-wide from one Mosquitto listener.
    #[test]
    fn anonymous_is_a_commented_candidate_not_a_live_setting() {
        let (_, out) = convert("allow_anonymous true\n");
        assert!(!out.contains("\nallow_anonymous = true"), "{out}");
        assert!(
            out.contains("# allow_anonymous = true  # from allow_anonymous true at"),
            "{out}"
        );
        assert!(out.contains("SECURITY POSTURE CHANGE"), "{out}");

        // ...and false is simply the mqttd default, so nothing is emitted at all.
        let (_, out) = convert("allow_anonymous false\n");
        assert!(!out.contains("allow_anonymous ="), "{out}");
    }

    /// TLS material binds to the listener it FOLLOWS, as Mosquitto scopes it — and every
    /// live bind carries the input key it came from.
    #[test]
    fn tls_material_attaches_to_its_own_listener() {
        let (_, out) = convert(
            "listener 1883 127.0.0.1\nlistener 8883 0.0.0.0\ncertfile /c.crt\nkeyfile /k.key\n",
        );
        assert!(
            out.contains("plaintext_bind = \"127.0.0.1:1883\"  # from: listener 1883 127.0.0.1"),
            "{out}"
        );
        assert!(
            out.contains("tls_bind = \"0.0.0.0:8883\"  # from: listener 8883 0.0.0.0"),
            "{out}"
        );
        assert!(out.contains("cert = \"/c.crt\""));
    }

    /// THE fabrication round 3 found: `port` / `bind_address` configure Mosquitto's DEFAULT
    /// listener, and reading neither produced `tls_bind = "0.0.0.0:1883"` — a loopback-only
    /// broker published on every interface, on a port the input never named.
    #[test]
    fn the_default_listener_form_is_read_rather_than_invented() {
        let (_, out) =
            convert("port 18883\nbind_address 127.0.0.77\ncertfile /c.crt\nkeyfile /k.key\n");
        assert!(
            out.contains(
                "tls_bind = \"127.0.0.77:18883\"  # from: port 18883 + bind_address 127.0.0.77"
            ),
            "{out}"
        );
        assert!(!out.contains("0.0.0.0:1883"), "{out}");

        // With no address anywhere in the input, the bind is INERT rather than invented.
        let (_, out) = convert("certfile /c.crt\nkeyfile /k.key\n");
        assert!(!out.contains("\ntls_bind = "), "{out}");
        assert!(
            out.contains("# tls_bind = \"0.0.0.0:1883\"") && out.contains("refuses to invent one"),
            "{out}"
        );
    }

    /// `protocol websockets` has an exact equivalent in `ws_bind`/`wss_bind`; emitting a
    /// WebSocket listener as a raw-MQTT bind breaks every browser client at cutover.
    #[test]
    fn websocket_listeners_get_the_websocket_binds() {
        let (_, out) = convert(
            "listener 1883\nlistener 9001\nprotocol websockets\n\
             listener 8884\nprotocol websockets\ncertfile /w.crt\nkeyfile /w.key\n",
        );
        assert!(out.contains("plaintext_bind = \"0.0.0.0:1883\""), "{out}");
        assert!(out.contains("ws_bind = \"0.0.0.0:9001\""), "{out}");
        assert!(out.contains("wss_bind = \"0.0.0.0:8884\""), "{out}");
        assert!(!out.contains("tls_bind = "), "{out}");

        // A transport this converter cannot identify gets NO bind at all.
        let (_, out) = convert("listener 9001\nprotocol future-thing\n");
        assert!(!out.contains("_bind = "), "{out}");
        assert!(
            out.contains("cannot identify that listener's TRANSPORT"),
            "{out}"
        );
    }

    /// `max_connections` is per-listener in Mosquitto and node-wide in mqttd, and `-1` is
    /// the vendor's documented spelling of unlimited — which mqttd's u64 REFUSES.
    #[test]
    fn per_listener_connection_caps_collapse_visibly() {
        let (_, out) = convert(
            "listener 8883\nmax_connections 100\ncertfile /c.crt\n\
             listener 1883\nmax_connections 100000\n",
        );
        assert!(out.contains("max_connections = 100\n"), "{out}");
        assert!(out.contains("the SMALLEST (100) was used"), "{out}");

        let (_, out) = convert("listener 1883\nmax_connections -1\n");
        assert!(!out.contains("max_connections = "), "{out}");
        assert!(out.contains("documents as UNLIMITED"), "{out}");
    }

    /// A plugin's own config file is a policy this converter never opened; saying "there is
    /// no plugin API" argues the operator out of the conclusion they need to reach.
    #[test]
    fn a_dynsec_plugin_is_reported_as_a_file_that_was_not_read() {
        let (_, out) = convert(
            "listener 1883\nplugin /usr/lib/mosquitto_dynamic_security.so\n\
             plugin_opt_config_file /etc/mosquitto/dynamic-security.json\n",
        );
        assert!(
            out.contains("DID NOT OPEN AND DID NOT READ ONE BYTE OF"),
            "{out}"
        );
        assert!(
            out.contains("do NOT conclude your old broker authorized everything"),
            "{out}"
        );
    }
}
