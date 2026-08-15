#!/usr/bin/env python3
"""Translate a Mosquitto deployment to an mqttd configuration DRAFT.

Reads `mosquitto.conf` (and the `acl_file` it references) and emits an mqttd TOML
config plus an ACL policy.

Written because three independent evaluators — coming from Mosquitto, EMQX and
HiveMQ — each named "there is no migration tooling" as their single largest
blocker. Hand-translating an ACL file with one entry per device is not a task
anyone will do for an evaluation, so the evaluation does not happen.

## What it is: a DRAFT, where anything undecidable is INERT and named

Three adversarial review rounds fixed what they were shown, honestly, and the
finding count went UP each time — because "every input construct is either
translated or marked TODO(migrate)" is a TOTAL-COVERAGE claim over a foreign
vendor's schema, and total coverage cannot be reached by inspection. Every
serious finding pointed the same way: **the tool emitted a live setting it had
not actually derived from the input** — a bind fabricated as `0.0.0.0:1883` when
the input said `bind_address 127.0.0.1`, an mTLS mandate dropped from a listener
that was not first, an ACL claiming to deny everything beside a file that allowed
it.

So a FABRICATED value is now impossible rather than absent. Every security-relevant
value this tool writes — every `*_bind`, every path under `[tls]`, `client_ca`,
`acl_file`, `password_file`, `allow_anonymous`, `mtls_identity_source` and the ACL
`default` — is emitted through ONE gate (`Provenance.line`) together with the
INPUT KEY it was derived from, and that gate REFUSES to write a live line without
one: a value with no provenance comes out COMMENTED OUT with a TODO naming the
decision the operator has to make. Every live security-relevant line carries
`# from: <input key>` so the claim is checkable against the input by eye and by
`scripts/migrate/property_sweep.py`.

The worst case a fabrication can produce is therefore an INCOMPLETE config the
operator completes.

WHAT THE GATE DOES NOT CLOSE, and round 4 found five of these: MISREADING a real
input — a live value genuinely derived from a named input key whose MEANING this
tool got wrong. A TLS-PSK listener became a live PLAINTEXT bind (the bind carried
an honest `# from: listener 8883`; the gate checks where the VALUE came from and
the FIELD is what encodes the transport); an ACL block Mosquitto scopes to
ANONYMOUS clients became a grant to every authenticated one; `message_size_limit 0`
— the vendor's spelling of *no limit* — became a 1 KiB packet ceiling. All are
fixed and each is pinned by a test, but the class is unbounded across a foreign
schema and no invariant over the output can see it, so every instance known today
is enumerated in docs/MIGRATION.md's KNOWN GAPS section with what to check by hand.
Read the output against your own config before deploying it.

## Usage

    scripts/migrate/from-mosquitto.py /etc/mosquitto/mosquitto.conf \\
        --out-config mqttd.toml --out-acl acl.toml

    # Review, then validate before deploying — this never writes a config the
    # broker has not been asked to check:
    mqttd --check-config --config mqttd.toml

Exit codes: 0 translated (possibly with TODOs), 1 could not read the input.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

VERSIONS = (
    "mappings written against mosquitto.conf(5) from eclipse-mosquitto/mosquitto @ "
    "v2.0.22 (man/mosquitto.conf.5.xml + the shipped mosquitto.conf); NO vendor config "
    "file is pinned as a fixture for this converter and no live Mosquitto broker has ever "
    "been converted by it"
)

# The one sentence docs/MIGRATION.md's version-scope paragraph claims every converter's
# --help repeats. It is the same text in from-emqx.py and from-hivemq.py.
SCOPE = (
    "VERSION SCOPE: the version RANGE in docs/MIGRATION.md's What-ships table is a PARSER "
    "claim and nothing more — one tag per broker is fixture-pinned where a fixture exists at "
    "all, the neighbours rest on "
    "the reader being tolerant, and no mapping is version-pinned. Nothing here was "
    "validated against a live broker."
)

# THE HONEST SCOPE, in the same words in --help, in docs/MIGRATION.md, in README.md and in
# the header of every file this tool writes. It is not a retreat: the difference between a
# tool an operator can trust and one they cannot is that this one says which half is which.
DRAFT = (
    "WHAT THIS PRODUCES: a reviewed DRAFT, not `your config, translated`. Anything this "
    "converter could not DERIVE from your input is emitted INERT — commented out, beside a "
    "TODO(migrate) naming the decision you have to make — so an unread construct can leave "
    "the output INCOMPLETE but can never leave a live security setting nobody derived. "
    "Every live security-relevant line carries `# from: <the input key it came from>`. "
    # PER-CONVERTER, because the shared wording was not true here: this epilog printed "
    # "VERIFIED: fixtures diffed against pinned vendor sources" twenty lines under its own
    # PROVENANCE paragraph saying NO vendor file is pinned for this converter. A VERIFIED
    # line that borrows another converter's evidence is exactly the class the rest of this
    # file exists to remove. Found on 2026-08-15.
    "VERIFIED, for THIS converter: the provenance, no-live-without-source, drop, "
    "contradiction and validity invariants of scripts/migrate/property_sweep.py over "
    "generated and mechanically mutated inputs; `mqttd --check-config` on every generated "
    "config plus the ACL loaded by the real broker; and byte-for-byte agreement with the "
    "Rust twin in mqttui. NOT diffed against vendor bytes: NO Mosquitto config file is "
    "pinned as a fixture (the EMQX and HiveMQ converters do have vendor fixtures with "
    "re-derivable SHA-256s; this one does not), so every mapping here rests on "
    "mosquitto.conf(5) alone. NOT VERIFIED: no live Mosquitto was ever run against this "
    "converter, and NO claim of total coverage over mosquitto.conf(5) is made — a construct "
    "it has never seen is a construct it cannot report, and a construct whose MEANING it "
    "misreads is one it can still translate wrongly (docs/MIGRATION.md's KNOWN GAPS lists "
    "the misreadings found so far)."
)

# The condensed form that goes into the generated files themselves.
DRAFT_HEADER = [
    "# THIS IS A DRAFT, NOT A TRANSLATION. Anything this converter could not derive",
    "# from your input is COMMENTED OUT beside a TODO naming the decision, so this",
    "# file may be INCOMPLETE — but no live security setting in it was invented.",
    "# Every live security-relevant line carries `# from: <the input key>`.",
    "# NOT VERIFIED: no live Mosquitto was ever run; no total-coverage claim over",
    "# mosquitto.conf(5) is made.",
]

# ---------------------------------------------------------------------------
# String emission. ONE helper per channel, used by EVERY string this tool writes.
#
# The 2026-08-14 review found the class across all three converters at once: no
# value was escaped anywhere. A Mosquitto ACL `user CORP\jdoe` came out as
# `identities = ["CORP\jdoe"]` and `certfile C:\certs\server.crt` as
# `cert = "C:\certs\server.crt"`, neither of which is valid TOML — tomllib rejects
# the WHOLE document ("Unescaped '\' in a string"), so ONE such user poisons the
# entire ACL file and the broker refuses to load any of it. Nothing below builds a
# quoted string by hand.
#
# The same helpers are duplicated verbatim in from-emqx.py and from-hivemq.py rather
# than shared through an import, deliberately: each converter is ONE self-contained
# stdlib-only file, run standalone (`mqttui migrate mosquitto`, or copied to the
# machine that holds the vendor config), and an import would make it two files that
# must travel together.
# ---------------------------------------------------------------------------

_TOML_ESCAPES = {
    "\\": "\\\\",
    '"': '\\"',
    "\b": "\\b",
    "\t": "\\t",
    "\n": "\\n",
    "\f": "\\f",
    "\r": "\\r",
}


def toml_escape(value: object) -> str:
    """Escape a value for the inside of a TOML basic string."""
    out: list[str] = []
    for ch in str(value):
        esc = _TOML_ESCAPES.get(ch)
        if esc is not None:
            out.append(esc)
        elif ch < " " or ch == "\x7f":
            # TOML 1.0 forbids raw control characters inside a basic string
            # (U+0000-U+0008, U+000A-U+001F, U+007F).
            out.append(f"\\u{ord(ch):04X}")
        else:
            out.append(ch)
    return "".join(out)


def toml_str(value: object) -> str:
    """A complete, quoted TOML basic string."""
    return '"' + toml_escape(value) + '"'


def toml_list(values) -> str:
    """A TOML array of strings, each escaped."""
    return "[" + ", ".join(toml_str(v) for v in values) + "]"


def comment_safe(text: object) -> str:
    """Flatten a value to one line so it cannot break out of a `#` comment.

    `str.split()` folds only Python whitespace, so `\\x00`-`\\x08` and `\\x0b`-`\\x1f`
    survived into the comment — and TOML 1.0 forbids a raw control character ANYWHERE in a
    document, comments included, so one such byte in a path made `tomllib` and the broker
    reject the WHOLE file while the converter still printed `wrote <file>`. Escaped rather
    than dropped, so the operator can still see what the byte was. Found 2026-08-15.
    """
    flattened = " ".join(str(text).split())
    return "".join(c if c >= " " and c != "\x7f" else f"\\u{ord(c):04X}" for c in flattened)


def truthy(value: object) -> bool:
    """Mosquitto's boolean spellings."""
    return str(value).strip().lower() in ("true", "yes", "1")


# ---------------------------------------------------------------------------
# PROVENANCE OR NOTHING.
#
# The load-bearing structure of this file. Every finding of the three review rounds
# that mattered had one shape — a LIVE security-relevant value the tool had not
# derived from the input:
#
#   * `tls_bind = "0.0.0.0:1883"` fabricated for an input that said
#     `port 18883` / `bind_address 127.0.0.77` (a loopback-only broker published on
#     every interface, on the wrong port);
#   * an mTLS mandate, a CRL and a TLS-version floor dropped from any listener that
#     was not first, while `client_ca` was taken from the one that was;
#   * `allow_anonymous true` from a retired listener carried onto the live one;
#   * an ACL that permits everything beside a comment saying it denies everything.
#
# Fixing those one at a time is unbounded work, because the set of vendor constructs
# nobody has looked at yet is unbounded. So instead: SECURITY_FIELDS names the fields
# whose value decides who can connect and what they may do, and the ONLY way to write
# one is Provenance.line(), which takes the value AND the input key it came from and
# REFUSES to emit a live line without the key. A field with no provenance is emitted
# COMMENTED OUT beside a TODO naming what the operator must decide.
#
# There is therefore no `or "0.0.0.0:1883"`, no `unwrap_or`, no f-string that builds a
# security-relevant line anywhere else in this file — a missed input key can produce
# an INCOMPLETE config, never a live setting nobody derived.
# ---------------------------------------------------------------------------

SECURITY_FIELDS = frozenset(
    {
        # [listeners] — which addresses the broker publishes, on which transport
        "plaintext_bind",
        "tls_bind",
        "ws_bind",
        "wss_bind",
        "quic_bind",
        # [tls] — the server identity, the client mandate and revocation
        "cert",
        "key",
        "client_ca",
        "crl",
        "allow_tls12",
        # [security] — who may connect and what governs them
        "acl_file",
        "password_file",
        "allow_anonymous",
        "mtls_identity_source",
        # the ACL policy's own catch-all
        "default",
    }
)

# The provenance marker, on the line itself. `# from:` is what the property sweep's
# NO-LIVE-WITHOUT-SOURCE invariant looks for, and what an operator diffing the output
# against their mosquitto.conf reads.
FROM = "  # from: "
# A part of a value that the INPUT did not contain, taken from a vendor-documented
# default of a directive that WAS present. Named on the line so it is never silent,
# and counted by the property sweep so it cannot be used to smuggle a fabrication.
DEFAULTED = "; defaulted: "


@dataclass
class Emitted:
    """One security-relevant value, and where it came from."""

    field: str
    rendered: str
    source: str | None
    defaulted: str | None
    live: bool


class Provenance:
    """The ONE gate every security-relevant emitted value passes through."""

    def __init__(self) -> None:
        self.rows: list[Emitted] = []

    def line(
        self,
        field: str,
        rendered: str,
        source: str | None,
        *,
        defaulted: str | None = None,
        decide: str | None = None,
    ) -> list[str]:
        """`field = rendered  # from: source`, or an INERT candidate plus a TODO.

        `source` is the INPUT KEY the value was derived from. Without one, a field in
        SECURITY_FIELDS is emitted commented out: `decide` says what the operator has
        to settle, and is required in that case (a TODO that does not name the
        decision is not a report).
        """
        if field in SECURITY_FIELDS and not source:
            self.rows.append(Emitted(field, rendered, None, defaulted, live=False))
            reason = decide or (
                f"nothing in the input named a value for {field}, so it is emitted "
                "COMMENTED OUT rather than guessed at. Decide it yourself and uncomment"
            )
            return [
                comment_safe(f"# TODO(migrate): {reason}"),
                f"# {field} = {rendered}",
            ]
        self.rows.append(Emitted(field, rendered, source, defaulted, live=True))
        if field not in SECURITY_FIELDS:
            return [f"{field} = {rendered}"]
        trailer = FROM + comment_safe(source)
        if defaulted:
            trailer += DEFAULTED + comment_safe(defaulted)
        return [f"{field} = {rendered}{trailer}"]

    def inert(self, field: str, rendered: str, note: str) -> list[str]:
        """A candidate deliberately NOT activated: a posture change, or an illegal pair.

        Recorded as `live = False` so the ledger and the output agree, and rendered as a
        commented line the operator can uncomment after deciding.
        """
        self.rows.append(Emitted(field, rendered, None, None, live=False))
        return [f"# {field} = {rendered}" + (f"  # {comment_safe(note)}" if note else "")]

    def ledger(self, tool: str) -> str:
        """The machine-readable form, for `--provenance-json`."""
        return json.dumps(
            {
                "tool": tool,
                "emissions": [
                    {
                        "field": r.field,
                        "value": r.rendered,
                        "source": r.source,
                        "defaulted": r.defaulted,
                        "live": r.live,
                    }
                    for r in self.rows
                ],
            },
            indent=1,
        )


def policy_effect(default: str) -> str:
    """What a policy with this `default` DOES — derived from the value, never asserted.

    Round 2 and round 3 both found hard-coded deny prose beside an allow-everything
    file. So no sentence in this tool states what a policy will do: every one of them
    is generated from the `default` being written, here.
    """
    if default == "allow":
        return (
            'this policy\'s `default = "allow"` PERMITS EVERY publish and subscribe by '
            "every authenticated client, including on topics no client of yours has ever "
            'used — a wide open policy, not a migrated one. Set `default = "deny"` before '
            "deploying it"
        )
    return (
        'this policy\'s `default = "deny"` denies every publish and subscribe that no rule '
        "below allows. That is fail-closed, not migrated"
    )


# ---------------------------------------------------------------------------
# Settings with an exact mqttd equivalent. Anything absent from this table is
# reported rather than guessed at.
# ---------------------------------------------------------------------------

# mosquitto directive -> (mqttd TOML section, key, converter)
#
# `max_inflight_messages` is deliberately ABSENT: it looks like [limits]
# receive_maximum and is the OPPOSITE DIRECTION. See the named case in
# parse_mosquitto_conf().
#
# `max_connections` is deliberately ABSENT TOO, for two reasons found on 2026-08-15:
# it is a PER-LISTENER directive (mosquitto.conf(5): "Limit the total number of
# clients connected for the current listener"), so a flat table collapsed several
# listeners LAST-WINS with no trace — the tight cap on a device listener replaced by a
# browser listener's large one; and the vendor's own documented value for unlimited is
# `-1`, which this table passed straight through into `max_connections = -1`, a config
# `mqttd --check-config` REJECTS ("invalid value: integer `-1`, expected u64"). Both
# are handled in convert_listener_caps().
DIRECT: dict[str, tuple[str, str, str]] = {
    "max_queued_messages": ("limits", "max_queued_messages", "int"),
    "max_packet_size": ("limits", "max_packet_size", "int"),
    "message_size_limit": ("limits", "max_packet_size", "int"),
    "max_topic_alias": ("limits", "topic_alias_max", "u16"),
    "retain_available": ("limits", "max_retained_messages", "retain"),
    "persistence_location": ("node", "data_dir", "str"),
}

# Directives that exist in Mosquitto and have no mqttd equivalent, with the
# reason. Being explicit about *why* is the point: "unsupported" invites a bug
# report, "deliberately absent, here is the alternative" does not.
# `acl_file` is deliberately ABSENT: it is consumed before this table is consulted (it
# names the policy to translate), so an entry here would be dead text claiming a mapping
# that never fires.
NO_EQUIVALENT: dict[str, str] = {
    "password_file": "mqttd uses Argon2id password files: set security.password_file "
    "to a file of `username:argon2id-hash` lines. mosquitto_passwd hashes are NOT "
    "compatible and cannot be converted (they are hashes — the passwords are not "
    "recoverable), so each user must be re-hashed from their password: "
    "`printf %s '<password>' | mqttd --hash-password <username> >> passwd`",
    # `psk_file` / `psk_hint` are deliberately ABSENT: they are LISTENER-SCOPED and they decide
    # that listener's TRANSPORT (TLS-PSK), so they are collected per listener and decided in
    # convert_psk() — a flat "not implemented" entry here let a PSK listener fall through to
    # BIND_KEYS[(transport, False)] and become a LIVE PLAINTEXT bind. See PSK_KEYS.
    "bridge": "bridging is a separate process in mqttd (mqtt-bridge) with its own "
    "config; see docs/BRIDGE.md",
    "log_dest": "mqttd logs to stdout for the container/journal to collect",
    "sys_interval": "$SYS topics are not implemented; use the Prometheus endpoint",
    "autosave_interval": "writes are transactional (redb); there is no autosave timer",
    "allow_zero_length_clientid": "a zero-length client id is accepted with clean "
    "session and refused otherwise, per spec; not configurable",
}

# A Mosquitto BRIDGE block, key by key. Every one of these HAS an exact equivalent in the
# `mqtt-bridge` config this repository ships (crates/mqtt-bridge, docs/BRIDGE.md) — and until
# 2026-08-15 all of them except `connection` fell through to "no direct equivalent — check the
# mqttd configuration table", which sends the operator to a table that has nothing to find, for
# settings the repo already translates from EMQX under `--out-bridge`. `bridge_cafile` is the
# one directive that decides whether the migrated bridge VERIFIES its peer, so filing it under
# "no equivalent" is the worst cell of the four. This converter has no `--out-bridge`, so the
# messages say where the value goes and that the file must be written by hand.
BRIDGE_KEYS: dict[str, str] = {
    "connection": "opens a BRIDGE block. Bridging is a SEPARATE PROCESS in mqttd — "
    "`mqtt-bridge <config>`, not a broker setting — so nothing below configures it. The keys "
    "of this block are named individually in the TODOs that follow; assemble them into an "
    "mqtt-bridge config by hand (docs/BRIDGE.md). This converter has no --out-bridge (the "
    "EMQX one does)",
    "address": "the bridge's upstream address -> mqtt-bridge `[[upstreams]] url`. NOT written "
    "anywhere by this converter: there is no --out-bridge here, so write it into an "
    "mqtt-bridge config yourself (docs/BRIDGE.md)",
    "addresses": "the bridge's upstream address(es) -> mqtt-bridge `[[upstreams]] url`, ONE "
    "per upstream (there is no failover list). Not written by this converter",
    "topic": "a bridge topic -> mqtt-bridge `[[upstreams.rules]]` — `filter`, `direction` "
    "(`out` for Mosquitto's `out`, `in` for `in`, and `both` needs TWO rules), `qos`, and a "
    "prefix `remap` for the local/remote prefix pair. Mosquitto's ordering is "
    "`topic <pattern> [direction [qos [local-prefix [remote-prefix]]]]`. Not written by this "
    "converter",
    "bridge_cafile": "the CA that verifies the UPSTREAM -> mqtt-bridge `[upstreams.tls] ca`. "
    "Not written by this converter — and note that mqtt-bridge's `[upstreams.tls]` is "
    "OPTIONAL, so an upstream with no tls block connects in PLAINTEXT: omit it and the "
    "bridge's CONNECT, username included, crosses in the clear",
    "bridge_capath": "a DIRECTORY of CAs for the upstream. mqtt-bridge takes ONE PEM file "
    "(`[upstreams.tls] ca`), so concatenate them. THIS CONVERTER DID NOT READ THAT DIRECTORY",
    "bridge_certfile": "the bridge's own client certificate -> mqtt-bridge `[upstreams.tls] "
    "cert` (with `key`; a half identity is refused at startup). Not written by this converter",
    "bridge_keyfile": "the bridge's own private key -> mqtt-bridge `[upstreams.tls] key`. Not "
    "written by this converter, and never copied",
    "remote_username": "the username the bridge presents UPSTREAM -> mqtt-bridge "
    "`[[upstreams]] username`. Not written by this converter",
    "remote_password": "the password the bridge presents upstream -> mqtt-bridge "
    "`[[upstreams]] password_file` (a FILE, never inline). NOT copied: secrets are never "
    "transformed",
    "remote_clientid": "the client id the bridge uses upstream -> mqtt-bridge `[[upstreams]] "
    "client_id`, which MUST be unique per instance. Not written by this converter",
}

# Directives that name ANOTHER FILE OR DIRECTORY this converter did not open. The message
# must say the CONTENTS were not read — "no direct equivalent" reads as "mqttd has no
# includes, fine" rather than "anything in there, possibly your whole authn/authz, was
# never seen".
#
# `plugin` / `plugin_opt_*` are in here rather than in NO_EQUIVALENT because "there is no
# plugin API" is true and beside the point: mosquitto.conf(5) recommends the Dynamic
# Security plugin OVER password_file ("handles username/password authentication and access
# control in a much more flexible way than a password file"), so for a dynsec deployment
# the ENTIRE authn/authz policy lives in a JSON file this converter never opened. Reporting
# that as a missing mqttd feature argues the operator out of the one conclusion they need to
# reach. Found on 2026-08-15.
NOT_READ: dict[str, str] = {
    "include_dir": "a DIRECTORY of further .conf files, which Mosquitto loads in "
    "case-sensitive alphabetical order (00.conf, 01.conf, A.conf, a.conf, …) as if their "
    "contents were pasted into the main file. THIS CONVERTER DID NOT OPEN THAT DIRECTORY "
    "AND DID NOT READ ONE BYTE OF IT, so ANY setting it holds — a second listener, "
    "another acl_file or password_file, a bridge, a plugin — is absent from the output "
    "below and is NOT reported anywhere, because it was never seen. Concatenate the main "
    "file with those .conf files in that order and re-run this converter on the result",
    "plugin": "an authentication/authorization PLUGIN, whose own configuration THIS "
    "CONVERTER DID NOT OPEN. mqttd has no plugin API (authentication is "
    "JWT/OIDC/mTLS/password, authorization is the ACL policy), but that is not the "
    "problem here: mosquitto.conf(5) recommends the Dynamic Security plugin OVER "
    "password_file, so if this is mosquitto_dynamic_security.so then your ENTIRE user, "
    "role and ACL policy lives in the plugin's JSON config and NONE of it was read or "
    "translated. Export it and re-model it as an mqttd ACL policy plus Argon2id password "
    "entries before you cut over",
    "auth_plugin": "the pre-2.0 spelling of `plugin`: an authentication/authorization "
    "plugin whose own configuration THIS CONVERTER DID NOT OPEN. mqttd has no plugin API, "
    "and whatever policy the plugin enforced is NOT in the output below",
    "plugin_opt_config_file": "the config file of the plugin named above, which THIS "
    "CONVERTER DID NOT OPEN AND DID NOT READ ONE BYTE OF. For the Dynamic Security plugin "
    "this file IS the deployment's authentication and authorization: clients, roles and "
    "per-role ACL rules. Nothing in it is in the output below",
}

# Listener-SCOPED TLS keys. Mosquitto scopes every one of these to the `listener` block it
# follows, so they are collected per listener and decided across ALL of them in
# convert_tls() — never read off listener[0] and applied as if global.
TLS_KEYS = {
    "cafile",
    "capath",
    "certfile",
    "keyfile",
    "require_certificate",
    "crlfile",
    "tls_version",
    "use_identity_as_username",
    "use_subject_as_username",
}

# TLS-PSK. LISTENER-SCOPED, and they decide the listener's TRANSPORT: mosquitto.conf(5) @
# v2.0.22, verbatim — "The psk_hint option enables pre-shared-key support for this listener and
# also acts as an identifier for this listener", and psk_file "Set the path to a
# pre-shared-key file. This option requires a listener to be have PSK support enabled."
#
# A PSK listener is ENCRYPTED (TLS with a PSK ciphersuite instead of a certificate) and mqttd
# has NO PSK support at all, so it is UNMAPPABLE — and until 2026-08-15 neither key was in
# TLS_KEYS nor in the half-material safety net, so `is_tls` was false and
# BIND_KEYS[(transport, False)] chose `plaintext_bind`: a PSK listener became a LIVE PLAINTEXT
# bind, on the same port, while another TODO in the same file said that listener had
# terminated TLS. The provenance gate cannot catch that — the bind carried a real
# `# from: listener 8883` — because the gate checks where the VALUE came from and the FIELD is
# what encodes the transport. So the transport is decided here instead: an encrypted listener
# this converter cannot express gets NO live bind at all.
PSK_KEYS = {"psk_file", "psk_hint"}

# Listener-scoped keys that are NOT TLS material: the transport and the connection cap.
# Both were read as if global before 2026-08-15 — `protocol` was not read at all, so a
# WebSocket listener was emitted as a raw-MQTT bind, and `max_connections` collapsed
# last-wins.
LISTENER_KEYS = {"protocol", "max_connections"}

# Keys Mosquitto makes PER LISTENER when per_listener_settings is true. mosquitto.conf(5) @
# v2.0.22 names EIGHT: password_file, acl_file, psk_file, allow_anonymous,
# allow_zero_length_clientid, auto_id_prefix, plugin and plugin_opt_* — the count was
# asserted as "exactly six" in four places against a document that names eight, and the two
# omitted are the pair that carries an entire third-party authn/authz backend. mqttd's
# [security] is node-wide, so if two listeners disagree only one value can survive and that
# collapse must be reported, not silently taken.
SCOPED_SECURITY = {
    "allow_anonymous",
    "acl_file",
    "password_file",
    "psk_file",
    "allow_zero_length_clientid",
    "auto_id_prefix",
    "plugin",
}

# The exact list, in the man page's own order, quoted by every surface that names it: the
# emitted TODO, the --help epilog and docs/MIGRATION.md.
SCOPED_SECURITY_LIST = (
    "password_file, acl_file, psk_file, allow_anonymous, allow_zero_length_clientid, "
    "auto_id_prefix, plugin and plugin_opt_* (mosquitto.conf(5) @ v2.0.22 names those "
    "eight)"
)

# transport (from `protocol`) + TLS -> the mqttd bind key. mqttd has FOUR client binds and
# `protocol websockets` was unread, so a Mosquitto WebSocket listener was emitted as a
# raw-MQTT bind — every browser client breaks at cutover, and because a WSS listener was
# counted as an ordinary TLS listener it also decided whose material won the one [tls]
# table. Found 2026-08-15.
BIND_KEYS = {
    ("mqtt", False): "plaintext_bind",
    ("mqtt", True): "tls_bind",
    ("websockets", False): "ws_bind",
    ("websockets", True): "wss_bind",
}


def bind_gap(address: str) -> str | None:
    """None when `address` is a `host:port` mqttd can bind; otherwise WHY it cannot.

    Every `*_bind` used to be emitted LIVE with no check that the broker can bind it, and
    `mqttd --check-config` — the verification this tool's own header, --help and docs point the
    operator at — accepts any string there, so the prescribed gate said `config OK` on
    addresses the broker then refuses at STARTUP ("failed to lookup address information"). Two
    of the three reproducers were also fabrications the provenance gate could not see: a
    Mosquitto UNIX-SOCKET listener (`listener 0 /tmp/mosq.sock` — mosquitto.conf(5): "the port
    must be set to 0, and the unix socket path must be given") declares NO TCP endpoint at all,
    and a host with no port cannot be bound. Found 2026-08-15.
    """
    if not address:
        return "it is empty"
    host, sep, port = address.rpartition(":")
    if not sep:
        return f"`{address}` has NO port, and mqttd binds host:port"
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    elif ":" in host:
        return (
            f"`{address}` looks like an IPv6 address without brackets; mqttd needs "
            "`[<address>]:<port>`"
        )
    if not host:
        return (
            f"`{address}` names NO host (mqttd needs an explicit address — `0.0.0.0` for every "
            "interface — and refuses to resolve an empty one at startup)"
        )
    if "/" in host:
        return (
            f"`{address}` is not a TCP address: `{host}` is a filesystem path, so this is a "
            "UNIX-DOMAIN-SOCKET listener (mosquitto.conf(5): 'the port must be set to 0, and "
            "the unix socket path must be given'). mqttd has NO unix-socket transport at all — "
            "there is nothing to bind, and turning it into a TCP port would publish on the "
            "network a listener that was reachable only through the filesystem"
        )
    if not port.isdigit() or not 1 <= int(port) <= 65535:
        return (
            f"`{port}` is not a TCP port number (1-65535), so `{address}` is not an address "
            "mqttd can bind — it passes --check-config and then fails at startup"
        )
    if any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_:" for c in host):
        return (
            f"`{host}` is not an address or hostname mqttd can resolve, so `{address}` is not "
            "one it can bind"
        )
    return None


@dataclass
class Listener:
    """One Mosquitto listener, with whatever was scoped to it.

    `port`/`bind` carry the SOURCE KEY they came from, because a bind is the most
    security-relevant value this tool writes and `0.0.0.0:1883` was fabricated for
    inputs that named neither. A listener with no port source has NO bind: the bind
    line comes out commented, with a TODO.
    """

    port: str | None = None
    port_source: str | None = None
    bind: str | None = None
    bind_source: str | None = None
    protocol: str | None = None
    tls: dict[str, str] = field(default_factory=dict)
    psk: dict[str, str] = field(default_factory=dict)
    caps: dict[str, str] = field(default_factory=dict)

    @property
    def host(self) -> str | None:
        """The address to bind, or None when the input never gave one."""
        if self.bind:
            return self.bind
        if self.port_source:
            # mosquitto.conf(5): `listener port [address]` with no address, and the
            # default listener with no `bind_address`, listen on every interface. That is
            # a documented default of a directive that WAS present, so it is derived —
            # and it is named as `defaulted:` on the emitted line.
            return "0.0.0.0"
        return None

    @property
    def host_defaulted(self) -> str | None:
        if self.bind or not self.port_source:
            return None
        return (
            "the host, because that directive named no address and mosquitto.conf(5) then "
            "listens on EVERY interface"
        )

    @property
    def source(self) -> str | None:
        """The input key(s) this listener's address was derived from."""
        if not self.port_source:
            return None
        if self.bind_source and self.bind_source != self.port_source:
            return f"{self.port_source} + {self.bind_source}"
        return self.port_source

    @property
    def address(self) -> str | None:
        host, port = self.host, self.port
        if host is None or port is None:
            return None
        return f"{host}:{port}"

    @property
    def address_gap(self) -> str:
        """Why no address could be derived — named in the TODO that replaces the bind."""
        if self.port is None and not self.bind:
            return (
                "the input named NEITHER a `listener` port, NOR `port`, NOR `bind_address` "
                "for it"
            )
        if self.port is None:
            return (
                f"the input gave its address as `bind_address {self.bind}` but NEVER a port. "
                "mosquitto.conf(5) documents the default as 1883 — that is a default of the "
                "BROKER, not a value in your file, and a bind on a port nobody wrote is how a "
                "broker ends up published where its operator did not choose (your real port "
                "may well be in an include_dir file, which this converter does not read)"
            )
        return "the input named no address for it"

    @property
    def candidate_address(self) -> str:
        """The commented placeholder, when no address could be derived."""
        return f"{self.bind or '0.0.0.0'}:{self.port or '1883'}"

    @property
    def where(self) -> str:
        """How this listener is named in every message about it — never fabricated."""
        addr = self.address
        if addr is None:
            return f"the default listener ({self.address_gap})"
        return f"listener {addr}"

    @property
    def is_tls(self) -> bool:
        return bool(self.tls.get("certfile"))

    @property
    def is_psk(self) -> bool:
        """TLS-PSK: ENCRYPTED, and unmappable, so it must never become a plaintext bind."""
        return bool(self.psk)

    @property
    def psk_inventory(self) -> str:
        return ", ".join(f"{k} {self.psk[k]}" for k in sorted(self.psk))

    @property
    def transport(self) -> str | None:
        """`mqtt`, `websockets`, or None when the input named a transport we do not know."""
        if self.protocol is None:
            # mosquitto.conf(5): "Can be mqtt, the default, or websockets".
            return "mqtt"
        p = self.protocol.strip().lower()
        return p if p in ("mqtt", "websockets") else None


@dataclass
class Conversion:
    config: dict[str, dict[str, Emitted]] = field(default_factory=dict)
    listeners: list[Listener] = field(default_factory=list)
    todos: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    acl_file: str | None = None
    per_listener: bool = False
    # key -> [(where it was set, value)], in document order.
    scoped: dict[str, list[tuple[str, str]]] = field(default_factory=dict)
    prov: Provenance = field(default_factory=Provenance)
    # Directives naming a file whose CONTENTS were never read (include_dir, a plugin's
    # config). Every sentence about "no policy was found" is derived from this, because
    # "your Mosquitto also authorized everything" is false when the policy was in
    # dynamic-security.json.
    unread: list[str] = field(default_factory=list)
    # Security-relevant candidates that are NOT activated, rendered commented after their
    # section: `(section, lines)`.
    deferred: dict[str, list[str]] = field(default_factory=dict)

    def set(
        self,
        section: str,
        key: str,
        value: str,
        source: str | None = None,
        *,
        defaulted: str | None = None,
        decide: str | None = None,
    ) -> None:
        """Record a table value. A security-relevant key with no source is NOT set live.

        This is the gate, at the point of assignment: there is no way to put a value into
        `[listeners]`, `[tls]` or `[security]` without naming the input key it came from.
        """
        if key in SECURITY_FIELDS and not source:
            self.deferred.setdefault(section, []).extend(
                self.prov.line(key, value, None, defaulted=defaulted, decide=decide)
            )
            return
        if key in SECURITY_FIELDS:
            self.prov.rows.append(Emitted(key, value, source, defaulted, live=True))
        self.config.setdefault(section, {})[key] = Emitted(
            key, value, source, defaulted, live=True
        )

    def defer(self, section: str, lines: list[str]) -> None:
        self.deferred.setdefault(section, []).extend(lines)

    def todo(self, msg: str) -> None:
        # Flattened HERE so no caller can emit a message that breaks out of its `#`
        # comment and leaves a bare line the TOML parser rejects.
        msg = comment_safe(msg)
        if msg not in self.todos:
            self.todos.append(msg)

    def note(self, msg: str) -> None:
        msg = comment_safe(msg)
        if msg not in self.notes:
            self.notes.append(msg)


def parse_mosquitto_conf(text: str, conv: Conversion) -> None:
    """Walk mosquitto.conf, filling `conv`. Listener-scoped keys follow their listener."""
    current: Listener | None = None
    # The DEFAULT listener, configured with `port` / `bind_address` rather than a
    # `listener` block (mosquitto.conf(5) documents both). Neither directive was read
    # before 2026-08-15: with TLS material present the synthetic listener had no port and
    # no bind, so `tls_bind = "0.0.0.0:1883"` was FABRICATED — a broker the incumbent
    # exposed only on `bind_address 127.0.0.1` published on every interface, on a port the
    # input never mentioned — and with no TLS material there was no [listeners] table at
    # all and nothing said so.
    default_listener: Listener | None = None

    def the_default() -> Listener:
        nonlocal default_listener, current
        if default_listener is None:
            default_listener = Listener()
            conv.listeners.append(default_listener)
        if current is None:
            current = default_listener
        return default_listener

    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 1)
        key = parts[0]
        value = parts[1].strip() if len(parts) > 1 else ""

        if key == "listener":
            bits = value.split()
            current = Listener()
            if bits and bits[0].isdigit():
                current.port = bits[0]
                current.port_source = f"listener {value}"
            if len(bits) > 1:
                current.bind = bits[1]
                current.bind_source = f"listener {value}"
            conv.listeners.append(current)
            continue

        if key == "port":
            if not value.isdigit():
                conv.todo(
                    f"port {value}: not a port number this converter can use, so NO "
                    "[listeners] bind was derived from it. Mosquitto's default listener needs "
                    "a numeric port; fix it and re-run"
                )
                continue
            lst = the_default()
            lst.port = value
            lst.port_source = f"port {value}"
            continue

        if key == "bind_address":
            if not value:
                conv.todo(
                    "bind_address with no value: nothing to derive an address from, so NO "
                    "[listeners] bind was written for the default listener"
                )
                continue
            lst = the_default()
            lst.bind = value
            lst.bind_source = f"bind_address {value}"
            # DELIBERATELY NOT defaulting the port here. mosquitto.conf(5) does document
            # "port ... Defaults to 1883", but a PORT is the one half of a bind this converter
            # will not supply: `bind_address` without `port` is exactly the shape where the
            # real port lives in an include_dir file that was never read, and a bind on a port
            # nobody named publishes the broker somewhere the operator did not choose. The
            # candidate is emitted COMMENTED OUT instead, naming 1883 as the vendor default it
            # would be. Found on 2026-08-15 by the fuzz pass, which mutated `port` out of a
            # fixture and watched a live `0.0.0.0:1883` appear.
            continue

        if key in TLS_KEYS:
            # TLS material belongs to the listener it follows; before any listener
            # it is the default one.
            lst = current if current is not None else the_default()
            lst.tls[key] = value
            continue

        if key in LISTENER_KEYS:
            lst = current if current is not None else the_default()
            if key == "protocol":
                lst.protocol = value
            else:
                lst.caps[key] = value
            continue

        # Every listener-scoped security key is recorded WITH the listener it followed,
        # before it is acted on, so convert_scoped_security() can report a collapse.
        if key in SCOPED_SECURITY or key.startswith("plugin_opt_"):
            where = current.where if current is not None else "the global section"
            conv.scoped.setdefault(key, []).append((where, value))

        if key in PSK_KEYS:
            # Recorded on the LISTENER, because these decide its transport (see PSK_KEYS) —
            # and decided in convert_psk(), never here, so the message can name the listener's
            # final address rather than the half-parsed one.
            lst = current if current is not None else the_default()
            lst.psk[key] = value
            continue

        if key == "per_listener_settings":
            conv.per_listener = truthy(value)
            continue

        if key == "allow_anonymous":
            # Recorded above and DECIDED in convert_scoped_security() from the last value,
            # deliberately: that is what Mosquitto itself does with per_listener_settings
            # FALSE (its default). Acting on it here kept the first TRUE seen — so
            # `allow_anonymous true` on a retired listener followed by `false` on the live one
            # carried anonymous access forward, and emitted a NOTE saying so beside a config
            # that did not.
            continue

        if key == "acl_file":
            conv.acl_file = value
            continue

        if key in NOT_READ or key.startswith("plugin_opt_"):
            reason = NOT_READ.get(
                key,
                "an option passed to the plugin named above (mosquitto.conf(5): "
                "`plugin_opt_*` — Options to be passed to the most recent plugin defined in "
                "the configuration file). THIS CONVERTER DID NOT OPEN the plugin or its "
                "configuration, so whatever policy they held is NOT in the output below",
            )
            conv.todo(f"{key} {value}: {reason}" if value else f"{key}: {reason}")
            conv.unread.append(f"{key} {value}".strip())
            continue

        if key == "max_inflight_messages":
            # NOT a mapping, and it looks exactly like one. Mosquitto's
            # max_inflight_messages bounds messages the BROKER may have unacked TOWARD a
            # client (outbound); mqttd's [limits] receive_maximum is the MQTT 5 Receive
            # Maximum it GRANTS clients — the INBOUND window
            # (crates/mqtt-config/src/lib.rs). This table used to map one onto the other,
            # and the EMQX converter copied the error forward; found 2026-08-14.
            conv.todo(
                f"max_inflight_messages {value}: NOT carried over, deliberately. It bounds "
                "the messages Mosquitto may have in flight TOWARD a client (outbound); "
                "mqttd has no outbound-window setting — it honours each v5 client's OWN "
                "Receive Maximum from CONNECT and treats a v3.1.1 client as unlimited "
                "(ADR 0012). The similarly named [limits] receive_maximum is the OPPOSITE "
                "direction: the inbound window mqttd GRANTS clients, default 256. Setting "
                "it from this value would silently shrink your inbound QoS>0 window and "
                "throttle publishers after cutover. Cap the inbound window deliberately if "
                "you want to: # receive_maximum = <messages>"
            )
            continue

        if key in DIRECT:
            section, mkey, kind = DIRECT[key]
            if kind == "int":
                # ZERO IS THE VENDOR'S SPELLING OF *NO LIMIT* for both packet-size keys —
                # mosquitto.conf(5) @ v2.0.22 on message_size_limit: "The default value is 0,
                # which means that all valid MQTT messages are accepted", and max_packet_size
                # "Defaults to no limit". Passing 0 through wrote `max_packet_size = 0`, which
                # --check-config ACCEPTS and mqttd then FLOORS TO 1024
                # (wire_limits_from_config in crates/mqttd/src/main.rs), so an unlimited
                # Mosquitto became a broker that refuses any packet over 1 KiB. Same class as
                # the `-1` max_connections sentinel. Found 2026-08-15.
                if key in ("message_size_limit", "max_packet_size") and value.strip() == "0":
                    conv.note(
                        f"{key} {value}, which mosquitto.conf(5) @ v2.0.22 documents as NO "
                        "LIMIT (message_size_limit: 'The default value is 0, which means that "
                        "all valid MQTT messages are accepted'; max_packet_size: 'Defaults to "
                        "no limit'). mqttd spells unlimited as the key being ABSENT, so "
                        "[limits] max_packet_size was left UNSET — its own default ceiling "
                        "then applies. Passing the 0 through would have written "
                        "max_packet_size = 0, which --check-config ACCEPTS and the broker "
                        "FLOORS to 1024 bytes, refusing every packet over 1 KiB"
                    )
                    continue
                conv.set(section, mkey, value)
                if key == "message_size_limit":
                    # This NOTE used to say mosquitto.conf(5) DEPRECATES message_size_limit in
                    # favour of max_packet_size and that the two are the SAME QUANTITY. The
                    # pinned page says neither: it marks port, bind_address,
                    # allow_duplicate_messages and clientid_prefixes deprecated and not this
                    # one, and the neighbouring entry states the difference outright. A wrong
                    # reason for a real caveat, in the file the operator DEPLOYS. Found
                    # 2026-08-15.
                    conv.note(
                        f"message_size_limit {value} became [limits] max_packet_size — the "
                        "NEAREST equivalent, NOT the same quantity. mosquitto.conf(5) @ "
                        "v2.0.22 defines message_size_limit as 'the maximum publish payload "
                        "size that the broker will allow', while its own max_packet_size "
                        "'applies to the full MQTT packet, not just the payload' — and mqttd's "
                        "max_packet_size is the PACKET form too ('Largest accepted MQTT "
                        "packet, bytes'). So the cap below is TIGHTER than yours by each "
                        "publish's fixed header, topic and MQTT 5 properties: a publish "
                        "Mosquitto accepted at the boundary is REFUSED after cutover. Raise it "
                        "by your largest topic + property overhead if you publish near the "
                        "limit. If both directives were set, the LAST one read is what is below"
                    )
            elif kind == "u16":
                if value.isdigit():
                    conv.set(section, mkey, str(min(int(value), 65535)))
                    if int(value) > 65535:
                        conv.todo(
                            f"{key} {value} exceeds the MQTT 5 16-bit field that "
                            f"[{section}] {mkey} maps to; it was clamped to 65535"
                        )
                else:
                    conv.todo(f"{key} {value}: not an integer this converter can map")
            elif kind == "retain":
                if value.lower() in ("false", "no", "0"):
                    conv.todo(
                        "retain_available=false disables retained messages entirely; "
                        "mqttd has no off switch — cap it instead with "
                        "limits.max_retained_messages, or deny retained topics in the ACL"
                    )
            else:
                conv.set(section, mkey, toml_str(value))
            continue

        if key in BRIDGE_KEYS:
            named = f"{key} {value}" if value else key
            conv.todo(f"{named}: {BRIDGE_KEYS[key]}")
            continue

        if key in NO_EQUIVALENT:
            # The VALUE is named too: "password_file: mqttd uses Argon2id" leaves the
            # operator hunting for which file, and a report that cannot be checked against
            # the input is not a report.
            named = f"{key} {value}" if value else key
            conv.todo(f"{named}: {NO_EQUIVALENT[key]}")
            continue

        if key == "persistence":
            if truthy(value):
                conv.note(
                    "persistence was on: set node.data_dir (below) and mount a volume, "
                    "or durable state is kept in memory only"
                )
            continue

        named = f"{key} {value}" if value else key
        conv.todo(f"{named}: no direct equivalent — check the mqttd configuration table")


def convert_listener_caps(conv: Conversion) -> None:
    """`max_connections` is PER LISTENER in Mosquitto and node-wide in mqttd.

    mosquitto.conf(5) @ v2.0.22, verbatim: "Limit the total number of clients connected
    for the current listener" — and "Set to -1 to have 'unlimited' connections", which is
    the value the shipped mosquitto.conf carries as its documented default.

    Both halves were wrong before 2026-08-15. The flat DIRECT table collapsed several
    listeners LAST-WINS with no TODO, no NOTE and no trace of the discarded value (a cap of
    100 on a TLS device listener replaced by 100000 from a browser listener — a defence
    multiplied by 1000 in silence), and it passed `-1` straight through, producing a config
    the broker REJECTS: "invalid value: integer `-1`, expected u64".
    """
    sites: list[tuple[str, str]] = [
        (l.where, l.caps["max_connections"])
        for l in conv.listeners
        if "max_connections" in l.caps
    ]
    if not sites:
        return
    unlimited = [(w, v) for w, v in sites if v.strip().lstrip("-").isdigit() and int(v) < 0]
    caps = [(w, int(v)) for w, v in sites if v.strip().isdigit()]
    bad = [(w, v) for w, v in sites if not v.strip().lstrip("-").isdigit()]
    for where, value in bad:
        conv.todo(
            f"{where} set max_connections {value}, which is not a number this converter can "
            "map onto [limits] max_connections. Set it deliberately, or leave it unset for "
            "uncapped"
        )
    for where, value in unlimited:
        conv.note(
            f"{where} set max_connections {value}, which mosquitto.conf(5) documents as "
            "UNLIMITED. mqttd spells unlimited as the key being ABSENT (max_connections is "
            "an optional u64 — a negative number is refused outright by --check-config), so "
            "[limits] max_connections was left unset, which is also uncapped. Cap it "
            "deliberately — docs/SIZING.md has the arithmetic for a fixed RAM budget"
        )
    if not caps:
        return
    winner = min(v for _, v in caps)
    conv.set("limits", "max_connections", str(winner))
    if len({v for _, v in caps}) > 1 or unlimited:
        conv.todo(
            "max_connections is PER LISTENER in Mosquitto and NODE-WIDE in mqttd, and the "
            "listeners disagreed ("
            + "; ".join(f"{w}: {v}" for w, v in sites)
            + f"), so only one value can survive: the SMALLEST ({winner}) was used, because "
            "a cap set deliberately low on one listener is a budget and raising it silently "
            "is the permissive direction. The other values are GONE from the output — raise "
            "it deliberately if that is not what you want, and note that the node-wide cap "
            "now applies to every listener at once"
        )


def convert_scoped_security(conv: Conversion) -> None:
    """Report the per-listener authn/authz keys mqttd can only hold node-wide.

    mosquitto.conf(5) @ v2.0.22 names EIGHT settings that become PER LISTENER under
    per_listener_settings: password_file, acl_file, psk_file, allow_anonymous,
    allow_zero_length_clientid, auto_id_prefix, plugin and plugin_opt_*. mqttd has no
    per-listener security at all, so two listeners that disagreed collapse onto ONE value —
    the class of defect where a per-listener reading is applied as if it were global.
    """
    if conv.per_listener:
        conv.todo(
            "per_listener_settings was TRUE, so in Mosquitto these were configured PER "
            "LISTENER: " + SCOPED_SECURITY_LIST + ". mqttd has NO per-listener "
            "authentication or authorization — [security] is NODE-WIDE — so every value "
            "below applies to EVERY listener at once. Read each one against every listener "
            "it now governs, and split the deployment in two if one listener really was "
            "anonymous or unauthorized and another was not. Mosquitto's own caveat "
            "compounds it: a durable client that had disconnected used the ACL of the "
            "listener it was LAST connected to, so the policy a given session ran under may "
            "not be the one you are reading"
        )
    for key in ("allow_anonymous", "acl_file", "password_file", "psk_file", "plugin"):
        sites = conv.scoped.get(key, [])
        if len({v for _, v in sites}) <= 1:
            continue
        conv.todo(
            f"{key} was set MORE THAN ONCE with DIFFERENT values ("
            + "; ".join(f"{w}: {v}" for w, v in sites)
            + "). mqttd's [security] is node-wide, so only ONE can survive and the LAST one "
            f"read ({sites[-1][1]}) is what this conversion used — which is what Mosquitto "
            "itself does with per_listener_settings FALSE. If the listeners genuinely had "
            "different postures, that difference is GONE from the output: split them across "
            "separate deployments, one per posture"
        )
    # ANONYMOUS ACCESS IS A POSTURE CHANGE, so it is never activated by this tool: mqttd
    # refuses anonymous clients by default, and switching that off for a whole node because
    # one Mosquitto listener allowed it is the fail-OPEN direction. The candidate is emitted
    # COMMENTED OUT with the input key it came from — the #162 precedent, applied without
    # exception (2026-08-15).
    sites = conv.scoped.get("allow_anonymous", [])
    if sites and truthy(sites[-1][1]):
        where, value = sites[-1]
        conv.defer(
            "security",
            conv.prov.inert(
                "allow_anonymous",
                "true",
                f"from allow_anonymous {value} at {where} — NOT activated; see the TODO",
            ),
        )
        conv.todo(
            f"allow_anonymous was TRUE in mosquitto.conf ({where}), which let clients "
            "connect with NO credentials at all. mqttd refuses anonymous clients by default, "
            "and turning that off is a SECURITY POSTURE CHANGE — node-wide, for every "
            "listener, because [security] is not per-listener — so it is NOT carried over: "
            "the candidate is emitted COMMENTED OUT in [security] below. Uncomment it only "
            "if you really mean to keep an unauthenticated broker (anonymous access is how "
            "most broker exposure incidents start), or give those clients a credential "
            "before cutover — which is the whole point of migrating"
        )


def convert_psk(conv: Conversion) -> None:
    """A TLS-PSK listener is ENCRYPTED and UNMAPPABLE, so it must not become a plaintext bind.

    mosquitto.conf(5) @ v2.0.22, verbatim: "The psk_hint option enables pre-shared-key support
    for this listener and also acts as an identifier for this listener", and psk_file "Set the
    path to a pre-shared-key file. This option requires a listener to be have PSK support
    enabled."

    Before 2026-08-15 `psk_hint`/`psk_file` were in neither TLS_KEYS nor the half-material
    safety net, so `is_tls` was false and the listener took `plaintext_bind`: an encrypted
    listener was published in CLEARTEXT, on its own port, while another TODO in the same file
    reported that listener's `tls_version`. Every PSK identity that used to be a TLS credential
    would have crossed unencrypted, and `mqttd --check-config` said `config OK`. The provenance
    gate cannot see it — the bind carried a genuine `# from: listener 8883` — because the gate
    checks the VALUE's origin and the FIELD is what encodes the transport.
    """
    material_keys = ("cafile", "capath", "keyfile", "require_certificate", "crlfile")
    for lst in conv.listeners:
        if not lst.is_psk:
            continue
        also = ", ".join(f"{k} {lst.tls[k]}" for k in material_keys if k in lst.tls)
        if lst.is_tls:
            # A certificate AND a PSK hint: the certificate half translates, the PSK half
            # cannot, so the listener keeps its tls_bind and the PSK clients are the loss.
            conv.todo(
                f"{lst.where} enabled TLS-PSK ({lst.psk_inventory}) ALONGSIDE a certificate. "
                "The certificate half is translated below; the PSK half is NOT — mqttd has no "
                "PSK ciphersuites at all, so any client that authenticated with a pre-shared "
                "key rather than a certificate CANNOT connect after cutover (it fails in the "
                "TLS handshake, which looks like a network fault, not a policy one). Move "
                "those devices onto certificates or passwords before you cut over"
            )
            continue
        conv.todo(
            f"{lst.where} was ENCRYPTED WITH TLS-PSK ({lst.psk_inventory}) and has NO "
            "certificate: mosquitto.conf(5) @ v2.0.22 — 'The psk_hint option enables "
            "pre-shared-key support for this listener'. mqttd has NO PSK SUPPORT AT ALL (its "
            "TLS is certificate-based: TLS 1.3, or 1.2 behind [tls] allow_tls12), so this "
            "listener CANNOT be translated. Converting it to a plaintext bind would DOWNGRADE "
            "an encrypted transport to cleartext — every PSK identity and every payload on the "
            "wire — so NO live bind was written for it: the candidate is COMMENTED OUT in "
            "[listeners] below, on the TLS key, because that is what the transport was. Issue "
            "certificates for those devices (or keep them on a broker that speaks PSK) and "
            "uncomment it with [tls] cert/key set. Do NOT simply move the port"
            + (
                f". That listener also carried {also}, which is NOT in the output either"
                if also
                else ""
            )
        )
        if lst.psk.get("psk_file"):
            conv.todo(
                f"psk_file {lst.psk['psk_file']} at {lst.where}: a file of `identity:key` "
                "lines (mosquitto.conf(5)), which THIS CONVERTER DID NOT OPEN and could not "
                "translate if it had — mqttd has no PSK store. Those identities need a new "
                "credential each: a certificate CN, or an Argon2id password entry "
                "(`printf %s '<password>' | mqttd --hash-password <identity> >> passwd`), and "
                "the ACL translated beside this config must then key on whatever you choose"
            )


def convert_tls(conv: Conversion) -> list[str]:
    """The ONE [tls] table, decided across EVERY TLS listener.

    mqttd builds one rustls acceptor for tls_bind AND wss_bind and hands the same cert, key
    and client_ca to quic::server_endpoint (crates/mqttd/src/main.rs), so there is no
    per-listener TLS to translate into. That makes this function's job reporting, not
    choosing: every TLS listener is walked and every setting the single table cannot hold
    becomes a TODO NAMING the listener it came from.

    Round 2 (2026-08-14) found the predecessor of this code reading `tls_listeners[0]` only
    — the same fail-open defect the EMQX and HiveMQ converters had been remediated for a
    round earlier, surviving in the third converter because nobody was told to look at it.
    On the textbook Mosquitto fleet shape (`listener 1883` plaintext, `listener 8883` with
    require_certificate + cafile + crlfile, `listener 8884` for browsers) an mTLS MANDATE on
    any listener that was not first in document order VANISHED: no client_ca, no commented
    candidate, no TODO, and its cafile, crlfile and capath went with it.
    `mqttd --check-config` passes on such a file, so nothing downstream caught it.

    Every line it emits now goes through Provenance.line() with the listener key it came
    from, so a listener this converter failed to read cannot contribute a live value.
    """
    # -- per-listener keys that are not material: version floor and identity source -----
    for lst in conv.listeners:
        raw = lst.tls.get("tls_version")
        if raw is not None:
            version = raw.strip().lower()
            # mosquitto.conf(5) @ v2.0.22, verbatim: "Configure the minimum version of the
            # TLS protocol to be used for this listener ... In Mosquitto version 1.6.x and
            # earlier, this option set the only TLS protocol version that was allowed,
            # rather than the minimum." So the MINIMUM reading begins AFTER 1.6.x — at 2.0,
            # the only range this converter's table covers. The comment here used to say
            # "since 1.6", which names the last release where the claim was false.
            if version == "tlsv1.3":
                conv.note(
                    f"{lst.where} set tls_version {raw}, which Mosquitto 2.x reads as the "
                    "MINIMUM version — so that listener accepted TLS 1.3 only, which is "
                    "exactly mqttd's default. Nothing to carry over"
                )
            elif version == "tlsv1.2":
                conv.todo(
                    f"{lst.where} set tls_version {raw}, which Mosquitto 2.x reads as the "
                    "MINIMUM version (1.6.x and earlier read it as the ONLY version, so on "
                    "those releases this listener was 1.2-ONLY), so it accepted TLS 1.2 AND "
                    "1.3. mqttd offers TLS 1.3 ONLY by default and a 1.2-only client fails "
                    "to connect in a way that looks like a network fault, not a policy one. "
                    "If your fleet needs 1.2, opt in with [tls] allow_tls12 = true — "
                    "hardened (ECDHE+AEAD only, Extended Master Secret required), loudly "
                    "logged on every start, and applied to EVERY TLS transport — and plan "
                    "its retirement"
                )
            else:
                conv.todo(
                    f"{lst.where} set tls_version {raw}, a floor BELOW TLS 1.2. mqttd offers "
                    "1.3, plus 1.2 behind [tls] allow_tls12 = true, and nothing older at "
                    "all: any client that can only do 1.1 or 1.0 CANNOT connect after "
                    "cutover. Find those clients before you move them"
                )
        raw = lst.tls.get("use_identity_as_username")
        if raw is not None:
            if truthy(raw):
                conv.set(
                    "security",
                    "mtls_identity_source",
                    toml_str("cn"),
                    f"use_identity_as_username {raw} at {lst.where}",
                )
                conv.note(
                    f"{lst.where} set use_identity_as_username {raw}, which in Mosquitto "
                    "takes the client certificate's CN as the username and then does NOT "
                    "consult password_file for that listener (mosquitto.conf(5)). mqttd has "
                    "an EXACT equivalent — [security] mtls_identity_source, whose default is "
                    'already "cn" — and it is written out below explicitly so the mapping is '
                    "visible rather than implied. It is NODE-WIDE, so every TLS listener "
                    "identifies clients by certificate CN, and the ACL translated beside "
                    "this config must key on those CNs"
                )
            else:
                conv.todo(
                    f"{lst.where} set use_identity_as_username {raw}, so Mosquitto took the "
                    "username from CONNECT (password_file) even for a client that presented "
                    "a certificate. mqttd has NO switch for that: whenever a client presents "
                    "a verified certificate on a client listener its identity is read FROM "
                    "THE CERTIFICATE — the field [security] mtls_identity_source names, "
                    'default "cn" (crates/mqtt-auth/src/mtls.rs, and there is no fallback to '
                    "another field). The identity your ACL matches therefore CHANGES at "
                    "cutover from the CONNECT username to the certificate CN: check every "
                    "rule in the translated ACL against the CNs your device certificates "
                    "actually carry"
                )
        raw = lst.tls.get("use_subject_as_username")
        if raw is not None:
            if truthy(raw):
                conv.todo(
                    f"{lst.where} set use_subject_as_username {raw}, which takes the WHOLE "
                    "certificate subject (`CN=…,OU=…,O=…`) as the username. mqttd's "
                    "[security] mtls_identity_source offers cn, san-dns, san-uri and "
                    "san-email ONLY — there is no full-subject source "
                    "(crates/mqtt-config/src/lib.rs) — so this was NOT mapped and no value "
                    "was written. Either re-key the ACL onto the CN alone (and set "
                    'mtls_identity_source = "cn"), or move the identity into a SAN and pick '
                    "the matching source"
                )
            else:
                conv.note(
                    f"{lst.where} set use_subject_as_username {raw}, which is off and is "
                    "also mqttd's behaviour (the identity is the CN, not the full subject). "
                    "Nothing to carry over"
                )

    tls_listeners = [l for l in conv.listeners if l.is_tls]
    material_keys = ("cafile", "capath", "keyfile", "require_certificate", "crlfile")
    for lst in conv.listeners:
        # A PSK listener is EXCLUDED here: it was encrypted, so "Mosquitto served that listener
        # as PLAINTEXT" would be false about it. convert_psk() reports it, including whatever
        # material it also carried.
        if lst.is_tls or lst.is_psk or not any(k in lst.tls for k in material_keys):
            continue
        conv.todo(
            f"{lst.where} carried TLS settings ("
            + ", ".join(f"{k} {lst.tls[k]}" for k in material_keys if k in lst.tls)
            + ") but NO certfile, so Mosquitto served that listener as PLAINTEXT and nothing "
            "here becomes TLS either. If it was meant to be encrypted, it never was — check "
            "it before cutover"
        )
    if not tls_listeners:
        return []

    first = tls_listeners[0]
    out = ["[tls]"]

    def inventory(lst: Listener) -> str:
        return (
            f"{lst.where}: certfile={lst.tls.get('certfile') or 'unset'}, "
            f"keyfile={lst.tls.get('keyfile') or 'unset'}, "
            f"cafile={lst.tls.get('cafile') or 'unset'}, "
            f"require_certificate={lst.tls.get('require_certificate') or 'unset'}, "
            f"crlfile={lst.tls.get('crlfile') or 'unset'}"
        )

    if len(tls_listeners) > 1:
        conv.todo(
            f"{len(tls_listeners)} TLS listeners were found ("
            + "; ".join(inventory(l) for l in tls_listeners)
            + "). mqttd has ONE [tls] table and applies it to tls_bind, wss_bind AND "
            "quic_bind alike (one shared acceptor plus quic::server_endpoint), so "
            f"per-listener TLS cannot be expressed at all: {first.where}'s material is what "
            "the table below holds, and it is what EVERY TLS transport will use. Read each "
            "listener's entry above against the posture the table ends up with"
        )
        materials = {
            (l.tls.get("certfile"), l.tls.get("keyfile"), l.tls.get("cafile"))
            for l in tls_listeners
        }
        if len(materials) > 1:
            conv.todo(
                "those TLS listeners carry DIFFERENT TLS material, and only ONE set can be "
                f"referenced: {first.where}'s certfile/keyfile/cafile went into [tls] below "
                "and the other listeners' PEM files are referenced NOWHERE in the generated "
                "config, while their transports are served from the material that IS "
                "referenced. Reissue one certificate covering every name (a SAN per "
                "hostname), or split the listeners across separate deployments"
            )

    out.extend(
        conv.prov.line(
            "cert", toml_str(first.tls["certfile"]), f"certfile at {first.where}"
        )
    )
    if first.tls.get("keyfile"):
        out.extend(
            conv.prov.line(
                "key", toml_str(first.tls["keyfile"]), f"keyfile at {first.where}"
            )
        )
    else:
        out.extend(
            conv.prov.line(
                "key",
                toml_str("/etc/mqttd/tls/server.key"),
                None,
                decide=f"{first.where} named a certfile but NO keyfile, so there is nothing "
                "to put in [tls] key and the broker REFUSES to start without it. Set key to "
                "an UNENCRYPTED PEM private key of your own (mount it from a Secret) and "
                "uncomment — the path below is a placeholder, not a value from your config",
            )
        )

    # -- the mTLS mandate, decided across EVERY TLS listener ----------------------------
    #
    # Mosquitto's cafile only VERIFIES a certificate the client CHOOSES to present unless
    # require_certificate is true; mqttd's client_ca MANDATES one, for every TLS transport at
    # once. So only a UNANIMOUS require_certificate is a mapping — the #162 precedent: a
    # mapping that changes SECURITY POSTURE is not a mapping, so the candidate is emitted
    # COMMENTED OUT with a TODO instead.
    required = [l for l in tls_listeners if truthy(l.tls.get("require_certificate", "false"))]
    lax = [l for l in tls_listeners if not truthy(l.tls.get("require_certificate", "false"))]
    with_ca = [l for l in tls_listeners if l.tls.get("cafile")]
    cas = sorted({l.tls["cafile"] for l in with_ca})
    ca_lst = next((l for l in required if l.tls.get("cafile")), with_ca[0] if with_ca else None)
    ca = ca_lst.tls["cafile"] if ca_lst is not None else None
    mandated = bool(required and not lax and ca)

    if mandated and ca_lst is not None and ca is not None:
        out.extend(
            conv.prov.line(
                "client_ca",
                toml_str(ca),
                f"cafile + require_certificate at {ca_lst.where}",
            )
        )
        conv.note(
            "require_certificate was TRUE on every TLS listener ("
            + "; ".join(l.where for l in required)
            + "), so mTLS is MANDATORY and [tls] client_ca is set — for tls_bind, wss_bind "
            "and quic_bind alike, because mqttd has one posture for every TLS transport. "
            "mqttd additionally requires the clientAuth extended key usage on every client "
            "certificate and refuses one without it at the handshake, which OpenSSL-based "
            "brokers tolerated missing for years. Audit the fleet BEFORE cutover: "
            "scripts/migrate/cert-audit.sh <dir-of-client-certs>"
            + (
                "; those listeners also disagree on cafile "
                f"({', '.join(cas)}) and only {ca} was used — concatenate the anchors into "
                "one PEM if both are still in use"
                if len({l.tls.get("cafile") for l in required}) > 1
                else ""
            )
        )
    elif required and lax:
        # THE fail-open case: an mTLS MANDATE on a listener that is not first in document
        # order used to vanish entirely. Neither arm is a translation, so neither is silent.
        conv.todo(
            "TLS listeners DISAGREE about client certificates, and mqttd cannot hold both "
            "postures: require_certificate was TRUE on "
            + "; ".join(l.where for l in required)
            + " but NOT on "
            + "; ".join(
                f"{l.where} (require_certificate {l.tls.get('require_certificate') or 'unset'})"
                for l in lax
            )
            + ". [tls] client_ca MANDATES mTLS for tls_bind, wss_bind and quic_bind AT ONCE "
            "— setting it newly demands a certificate from clients that never presented one, "
            "and leaving it unset DROPS a mandate you have today. Neither is a translation, "
            "so client_ca is emitted COMMENTED OUT below: uncomment it to mandate mTLS "
            "fleet-wide (audit every client first with scripts/migrate/cert-audit.sh, and "
            "expect the cert-less clients to fail the handshake), or leave it commented and "
            "move the mTLS-required clients to a SEPARATE deployment that sets it. Do NOT "
            "deploy this file believing the require_certificate listener kept its mandate"
        )
        out.append(
            comment_safe(
                "# TODO(migrate): client certificates were REQUIRED on "
                + "; ".join(l.where for l in required)
                + " but not on "
                + "; ".join(l.where for l in lax)
                + "; mqttd has ONE posture for every TLS transport. Uncommenting mandates "
                "mTLS EVERYWHERE (see the TODO above):"
            )
        )
        if ca and ca_lst is not None:
            out.extend(
                conv.prov.inert(
                    "client_ca", toml_str(ca), f"from cafile at {ca_lst.where}"
                )
            )
        else:
            out.extend(
                conv.prov.inert(
                    "client_ca",
                    toml_str("/etc/mqttd/tls/client-ca.crt"),
                    "PLACEHOLDER — no cafile was found on the REQUIRED listener, so this "
                    "path came from nowhere in your config; supply the anchors",
                )
            )
    elif with_ca:
        out.append(
            comment_safe(
                "# TODO(migrate): cafile was set but require_certificate was NOT true on any "
                "TLS listener ("
                + "; ".join(
                    f"{l.where}: cafile={l.tls['cafile']}, require_certificate="
                    f"{l.tls.get('require_certificate') or 'unset'}"
                    for l in with_ca
                )
                + "). mqttd's client_ca MANDATES client certificates (mTLS) — there is no "
                "cert-optional mode — and it applies to tls_bind, wss_bind and quic_bind at "
                "once. Uncomment to require certs fleet-wide (audit them first with "
                "scripts/migrate/cert-audit.sh), or leave commented for server-only TLS:"
            )
        )
        for lst in with_ca:
            candidate = lst.tls["cafile"]
            if candidate not in cas:
                continue
            cas.remove(candidate)
            out.extend(
                conv.prov.inert(
                    "client_ca", toml_str(candidate), f"from cafile at {lst.where}"
                )
            )
    elif required:
        conv.todo(
            "; ".join(l.where for l in required)
            + " set require_certificate true but named NO cafile, so this converter has no "
            "trust anchor to put in [tls] client_ca and mTLS is NOT mandated below. Find the "
            "CA bundle Mosquitto was verifying against and set client_ca to it, or the "
            "mandate is gone"
        )

    # -- revocation. `crl` is ONLY legal beside an active client_ca -----------------------
    #
    # The broker's own words: `invalid configuration: tls.crl requires tls.client_ca`. This
    # code used to emit `crl` whenever any listener named a crlfile, so the ordinary
    # cafile-without-require_certificate input produced a config `mqttd --check-config`
    # REJECTS — rule 3, "the output must validate", broken in the converter the docs call the
    # most mature. Found on 2026-08-15 by running --check-config over a permuted-input sweep.
    crls = [l for l in tls_listeners if l.tls.get("crlfile")]
    if crls:
        chosen_lst = crls[0]
        chosen = chosen_lst.tls["crlfile"]
        distinct = sorted({l.tls["crlfile"] for l in crls})
        if mandated:
            out.extend(
                conv.prov.line(
                    "crl", toml_str(chosen), f"crlfile at {chosen_lst.where}"
                )
            )
        else:
            out.append(
                comment_safe(
                    "# TODO(migrate): a crlfile was set ("
                    + "; ".join(f"{l.where}: {l.tls['crlfile']}" for l in crls)
                    + ") but client_ca above is NOT set, and the broker REFUSES the config "
                    "outright in that combination — `invalid configuration: tls.crl requires "
                    "tls.client_ca`. Revocation is only ever consulted for a CLIENT "
                    "certificate, so it is meaningless without the mandate. Decide the mTLS "
                    "posture first; uncomment BOTH lines together:"
                )
            )
            out.extend(
                conv.prov.inert(
                    "crl", toml_str(chosen), f"from crlfile at {chosen_lst.where}"
                )
            )
        if len(distinct) > 1:
            conv.todo(
                "several TLS listeners named DIFFERENT crlfile values ("
                + "; ".join(f"{l.where}: {l.tls['crlfile']}" for l in crls)
                + f"), and mqttd has ONE [tls] crl: {chosen} is the one in the table and the "
                "others are referenced NOWHERE, so a certificate revoked only in one of them "
                "is still accepted. Concatenate every CRL into one PEM file — it is "
                "hot-reloaded on SIGHUP, which also evicts the live sessions of a revoked "
                "client"
            )

    capaths = [l for l in conv.listeners if l.tls.get("capath")]
    if capaths:
        out.append(
            comment_safe(
                "# TODO(migrate): capath names a DIRECTORY of CA certificates ("
                + "; ".join(f"{l.where}: {l.tls['capath']}" for l in capaths)
                + "), which mqttd does not support — and THIS CONVERTER DID NOT READ THAT "
                "DIRECTORY, so no anchor inside it was seen or reported. Concatenate the "
                "certificates it holds into one PEM and set client_ca to that file"
            )
        )
    return out


# The subject mqttd gives a client that connected with NO credentials
# (crates/mqtt-auth/src/basic.rs: `Credentials::Anonymous if self.allow_anonymous => Identity {
# subject: "anonymous" }`). It is what makes Mosquitto's anonymous-scoped ACL block expressible
# at all, rather than a rule that has to be dropped.
ANON_IDENTITY = "anonymous"

# mqttd substitutes exactly these two placeholders in a rule's topic patterns
# (crates/mqtt-auth/src/acl.rs `substitute`), unconditionally, in EVERY rule — while Mosquitto
# substitutes only in a `pattern` line and treats a plain `topic` filter literally.
SUBSTITUTED = ("%c", "%i")


def parse_acl(text: str) -> tuple[list[dict], list[str]]:
    """Translate a Mosquitto ACL file into mqttd rules.

    Mosquitto's model is *positional*: `user X` opens a block, and `topic` lines
    until the next `user` belong to it. `pattern` lines apply to everyone with
    substitution. mqttd's model is a list of rules with explicit identities, so
    the translation is a regrouping, not a line-for-line map.

    THE FIRST BLOCK IS ANONYMOUS. mosquitto.conf(5) @ v2.0.22, verbatim: "The first set of
    topics are applied to anonymous clients, assuming allow_anonymous is true. User specific
    topic ACLs are added after a user line". Those pre-`user` lines used to be emitted with NO
    `identities`, which mqttd applies to EVERY authenticated client ("Both lists empty means
    everyone", crates/mqtt-auth/src/acl.rs) — strictly broader than the source in both
    postures: under `allow_anonymous false` those topics were reachable by NOBODY, and under
    `true` by unauthenticated clients only. No sweep invariant could see it (the topics DO
    appear in the output, and their strings DO come from the input), and the widening was on
    the artifact docs/MIGRATION.md calls the expensive and dangerous half. Found 2026-08-15.
    """
    rules: list[dict] = []
    todos: list[str] = []
    current_user: str | None = None
    seen_user = False
    anonymous_lines = 0

    def add(identity: str | None, access: str, topic: str, *, anonymous: bool = False) -> None:
        actions = {
            "read": ["subscribe"],
            "write": ["publish"],
            "readwrite": ["publish", "subscribe"],
            "deny": [],
        }.get(access)
        if actions is None:
            todos.append(f"unknown access type {access!r} for topic {topic!r}")
            return
        # A LITERAL `*` IN A USERNAME CANNOT BE EXPRESSED. mqttd's `identities` are GLOBS where
        # `*` matches any run of characters and there is NO escape mechanism
        # (crates/mqtt-auth/src/acl.rs `glob_match`), while every source broker matches the
        # username literally (mosquitto.conf(5) on the ACL `user` line: "The username referred
        # to here is the same as in password_file"). Emitting it would silently grant the rule
        # to every identity matching the pattern, so no rule is emitted at all — the same
        # refusal the EMQX converter already makes for a regex condition. Found 2026-08-15.
        if identity and "*" in identity:
            todos.append(
                f"the ACL scoped {topic!r} to the user {identity!r}, whose name contains a "
                "LITERAL `*`. mqttd's rule `identities` are GLOBS — `*` matches any run of "
                "characters and there is NO way to escape it (crates/mqtt-auth/src/acl.rs) — "
                "while Mosquitto matched that username EXACTLY, so emitting the rule would "
                "grant it to every identity matching the pattern (`a*b` would admit "
                "`a-admin-b`). NO RULE WAS WRITTEN for it: rename the user, or add a rule by "
                "hand naming each identity you actually mean"
            )
            return
        rules.append(
            {
                "identities": [identity] if identity else [],
                "actions": ["publish", "subscribe"] if access == "deny" else actions,
                "effect": "deny" if access == "deny" else "allow",
                "topics": [topic],
                "anonymous": anonymous,
            }
        )

    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 1)
        key = parts[0]
        rest = parts[1].strip() if len(parts) > 1 else ""

        if key == "user":
            current_user = rest
            seen_user = True
            continue

        if key == "topic":
            bits = rest.split(None, 1)
            if len(bits) == 2 and bits[0] in ("read", "write", "readwrite", "deny"):
                access, filt = bits[0], bits[1]
            else:
                # No access type = readwrite in Mosquitto.
                access, filt = "readwrite", rest
            # A `topic` filter is LITERAL in Mosquitto — mosquitto.conf(5) documents
            # substitution for `pattern` ONLY ("It is also possible to define ACLs based on
            # pattern substitution within the topic ... using pattern as the keyword") — but
            # mqttd substitutes %c/%i in EVERY rule's topics. So carrying the filter across
            # verbatim converts a rule on one literal topic nobody publishes to into a live
            # per-client grant on topics that carry real traffic. There is no escape for a
            # literal `%c` in mqttd, so the rule is refused rather than widened. The other two
            # converters already marked this construct; this one accepted it silently. Found
            # 2026-08-15.
            used = [p for p in SUBSTITUTED if p in filt]
            if used:
                todos.append(
                    f"the plain `topic` line {filt!r} contains "
                    + " and ".join(used)
                    + ", which Mosquitto treats LITERALLY on a `topic` line (only `pattern` "
                    "substitutes there) while mqttd substitutes %c (client id) and %i "
                    "(identity) in EVERY rule's topics and has no escape for them "
                    "(crates/mqtt-auth/src/acl.rs). Carrying it over would turn a rule on one "
                    "literal topic into a live per-client grant the source never gave, so NO "
                    "RULE WAS WRITTEN for it. If a per-client namespace IS what you want, "
                    "write it as an mqttd rule deliberately (and note its substitutions FAIL "
                    "CLOSED on a value containing / + or #); if the topic really is literal, "
                    "rename it"
                )
                continue
            if seen_user:
                add(current_user, access, filt)
            else:
                anonymous_lines += 1
                add(ANON_IDENTITY, access, filt, anonymous=True)
            continue

        if key == "pattern":
            bits = rest.split(None, 1)
            access, topic = (
                (bits[0], bits[1])
                if len(bits) == 2 and bits[0] in ("read", "write", "readwrite", "deny")
                else ("readwrite", rest)
            )
            # %u -> %i (mqttd's identity), %c -> %c (client id, same meaning).
            converted = topic.replace("%u", "%i")
            if "%c" in topic:
                todos.append(
                    f"pattern {topic!r} uses %c (client id). mqttd supports %c, but its "
                    "substitutions FAIL CLOSED on a value containing / + or # — verify "
                    "your client ids do not."
                )
            add(None, access, converted)
            continue

        todos.append(f"unrecognised ACL line: {line!r}")

    if anonymous_lines:
        todos.insert(
            0,
            f"{anonymous_lines} `topic` line(s) appeared BEFORE the first `user` line. "
            "mosquitto.conf(5) @ v2.0.22, verbatim: 'The first set of topics are applied to "
            "anonymous clients, assuming allow_anonymous is true' — so those lines granted "
            "access to ANONYMOUS clients ONLY, not to every user (the page draws the "
            "distinction explicitly: a `pattern` ACL applies to all users, a leading `topic` "
            "block does not). They are therefore emitted SCOPED to identities = "
            f'["{ANON_IDENTITY}"], which is the subject mqttd gives a client that connected '
            "with no credentials (crates/mqtt-auth/src/basic.rs) — NOT as unscoped rules, "
            "which mqttd applies to EVERY authenticated identity and which would be strictly "
            "broader than your Mosquitto policy. Consequences to check: (1) they grant NOTHING "
            "until [security] allow_anonymous is set in the generated config, and it is emitted "
            "COMMENTED OUT because mqttd refuses anonymous clients by default — if "
            "allow_anonymous was FALSE in mosquitto.conf these rules were already dead and you "
            "should delete them; (2) if you have a real named user called "
            f"`{ANON_IDENTITY}`, these rules apply to it too — rename that user",
        )

    return rules, todos


# Mosquitto has NO `no_match` analogue: an acl_file is an allow list and anything it does
# not permit is refused, so deny-by-default carries over exactly. That makes the value a
# constant here — but it is still routed through Provenance and policy_effect() rather than
# written into the prose, because the class of defect this restructuring removes is a
# sentence that ASSERTS what a computed value does.
ACL_DEFAULT = "deny"
ACL_DEFAULT_SOURCE = (
    "mosquitto.conf(5): a Mosquitto acl_file is an allow list with no `no_match` "
    "equivalent, so anything it does not permit was already refused"
)


def render_acl(
    rules: list[dict],
    todos: list[str],
    default: str = ACL_DEFAULT,
    prov: Provenance | None = None,
) -> str:
    prov = prov if prov is not None else Provenance()
    out = [
        "# Translated from a Mosquitto acl_file by the mqttd Mosquitto converter",
        "# (`mqttui migrate mosquitto`, or scripts/migrate/from-mosquitto.py).",
        "#",
        *DRAFT_HEADER,
        "#",
        "# Mosquitto is positional (a `user` line opens a block); mqttd is a list of",
        "# explicit rules. Read this through before deploying it — a converted policy",
        "# is a draft, not an authority.",
        "#",
        comment_safe(f"# {policy_effect(default)}."),
        "#",
        "# It is enforced ONLY while [security] acl_file in the generated config names this",
        "# file: with acl_file unset mqttd enforces NO authorization at all and says so in",
        "# the log on every start.",
        "",
    ]
    out.extend(prov.line("default", toml_str(default), ACL_DEFAULT_SOURCE))
    out.append("")
    for t in todos:
        out.append(f"# TODO(migrate): {comment_safe(t)}")
    if todos:
        out.append("")
    for r in rules:
        out.append("[[rules]]")
        if r.get("anonymous"):
            out.append(
                "# from a `topic` line BEFORE the first `user` line: ANONYMOUS clients only"
            )
        if r["identities"]:
            out.append(f"identities = {toml_list(r['identities'])}")
        else:
            out.append("# (no identities = applies to every authenticated client)")
        out.append(f"actions = {toml_list(r['actions'])}")
        out.append(f"effect = {toml_str(r['effect'])}")
        out.append(f"topics = {toml_list(r['topics'])}")
        out.append("")
    return "\n".join(out) + "\n"


def render_listeners(conv: Conversion) -> None:
    """One bind per (transport, TLS) pair — each one derived, or emitted INERT.

    Four binds exist in mqttd (plaintext_bind, tls_bind, ws_bind, wss_bind) and this
    converter used to write two, treating `protocol websockets` as an unmapped directive:
    a WSS listener therefore claimed `tls_bind` and its browser clients got a raw-MQTT
    bind. A listener whose transport this converter cannot positively identify from the
    input gets NO bind at all, only a TODO — the contract's rule for a construct that was
    not read.
    """
    groups: dict[str, list[Listener]] = {}
    psk_only: list[Listener] = []
    # Only the binds emitted LIVE, so the cleartext warning below is derived from what the file
    # will DO rather than from what was found in the input: a listener whose address is missing
    # or unbindable contributes no bind at all.
    live: set[str] = set()
    for lst in conv.listeners:
        if (
            lst.port_source is None
            and lst.bind_source is None
            and not lst.tls
            and not lst.psk
            and lst.protocol is None
        ):
            # The pre-`listener` scope, holding only a node-wide setting like
            # max_connections. Mosquitto starts the DEFAULT listener only when `port` or
            # `bind_address` names one, so this scope is not a listener and must not claim a
            # bind — that is how a global setting used to demote a real `listener` line to
            # "additional" and leave the actual bind commented out.
            continue
        transport = lst.transport
        if transport is None:
            conv.todo(
                f"{lst.where} set protocol {lst.protocol!r}, which is neither `mqtt` nor "
                "`websockets` (mosquitto.conf(5) has no third value). This converter cannot "
                "identify that listener's TRANSPORT from the input, so NO bind was written "
                "for it at all — a bind is the one value that must never be guessed, since "
                "guessing wrong publishes a raw-MQTT port for WebSocket clients or the "
                "reverse. Decide which of plaintext_bind / tls_bind / ws_bind / wss_bind it "
                "should be and write it yourself"
            )
            continue
        if lst.is_psk and not lst.is_tls:
            # ENCRYPTED-BUT-UNMAPPABLE: it must NOT fall through to the plaintext key. Reported
            # by convert_psk(); the candidate is emitted below on the TLS key of its transport,
            # commented out, because that is the transport the input had.
            psk_only.append(lst)
            continue
        key = BIND_KEYS[(transport, lst.is_tls)]
        groups.setdefault(key, []).append(lst)

    for lst in psk_only:
        key = BIND_KEYS[(lst.transport or "mqtt", True)]
        conv.set(
            "listeners",
            key,
            toml_str(lst.address or lst.candidate_address),
            None,
            decide=f"{lst.where} was ENCRYPTED WITH TLS-PSK ({lst.psk_inventory}) and mqttd has "
            "NO PSK support, so it could not be translated and NO live bind was written for it. "
            "It is on the TLS key because that is the transport the input had: converting it to "
            f"[listeners] {BIND_KEYS[(lst.transport or 'mqtt', False)]} would downgrade an "
            "encrypted listener to cleartext. Issue certificates for those clients, set [tls] "
            "cert/key, and uncomment — see the TODO above",
        )

    for key in ("plaintext_bind", "tls_bind", "ws_bind", "wss_bind"):
        group = groups.get(key)
        if not group:
            continue
        # A listener whose address IS derivable takes the bind, whatever the document order:
        # otherwise a `certfile` written before the first `listener` line would leave the
        # bind commented out while a real, addressed listener of the same transport was
        # demoted to "additional".
        # An address mqttd cannot BIND is no better than one nobody derived: --check-config
        # accepts any string here and the broker then refuses to start, so the shape is checked
        # before the line goes out live. A listener whose address is unbindable is sorted behind
        # one whose is, exactly like a listener with no address at all.
        group.sort(key=lambda l: l.address is None or bind_gap(l.address) is not None)
        first = group[0]
        address = first.address
        unbindable = bind_gap(address) if address is not None else None
        if unbindable is not None:
            conv.set(
                "listeners",
                key,
                toml_str(address),
                None,
                decide=f"{first.where} gives [listeners] {key} as {address!r}, and that is not "
                f"an address mqttd can bind: {unbindable}. `mqttd --check-config` ACCEPTS any "
                "string here and the broker then fails at STARTUP, so the line is emitted "
                "COMMENTED OUT rather than live: set an address the broker can bind and "
                "uncomment it",
            )
        elif address is None:
            # NOT fabricated. The listener exists (TLS material or a `protocol` line
            # attached to it) but nothing in the input named a port or an address, so the
            # bind is emitted commented with the decision named. `0.0.0.0:1883` used to be
            # invented here.
            conv.set(
                "listeners",
                key,
                toml_str(first.candidate_address),
                None,
                decide=f"a {key.replace('_bind', '')} listener was configured (see the "
                f"settings attached to it) but {first.address_gap} — so this converter has NO "
                f"address to put in [listeners] {key} and refuses to invent one. The commented "
                "line below is a PLACEHOLDER, not a value from your config: set the real "
                "address and port and uncomment it, or the broker binds nothing on that "
                "transport",
            )
        else:
            live.add(key)
            conv.set(
                "listeners",
                key,
                toml_str(address),
                first.source,
                defaulted=first.host_defaulted,
            )
        for extra in group[1:]:
            conv.defer(
                "listeners",
                [
                    comment_safe(
                        f"# TODO(migrate): additional {key.replace('_bind', '')} listener "
                        f"{extra.address or '(no address in the input)'} — mqttd binds ONE "
                        "listener per protocol; consolidate clients onto the bind above"
                    )
                ],
            )
    if "plaintext_bind" in live or "ws_bind" in live:
        conv.defer(
            "listeners",
            ["# WARNING: plaintext. mqttd logs this as an INSECURE mode on every start."],
        )
    if not conv.listeners:
        conv.todo(
            "NO listener was found — mosquitto.conf named no `listener` block and no `port` "
            "or `bind_address` for the default listener — so NO [listeners] bind was written "
            "and the broker would bind NOTHING and serve no clients. Mosquitto's own default "
            "is port 1883 on every interface; that is a default of the BROKER, not a value "
            "in this file, so it is not carried over. Set the bind you actually want"
        )


def render_config(conv: Conversion, tls_lines: list[str]) -> str:
    out = [
        "# Translated from mosquitto.conf by the mqttd Mosquitto converter",
        "# (`mqttui migrate mosquitto`, or scripts/migrate/from-mosquitto.py).",
        "#",
        *DRAFT_HEADER,
        "#",
        "# Review every line, then validate before deploying:",
        "#     mqttd --check-config --config this-file.toml",
        "#",
        "# Settings with no mqttd equivalent are listed as TODO(migrate) rather than",
        "# dropped silently — a converter that quietly loses a setting is worse than",
        "# no converter, because you would deploy believing it came across.",
        "",
    ]
    for n in conv.notes:
        out.append(f"# NOTE: {comment_safe(n)}")
    if conv.notes:
        out.append("")
    for t in conv.todos:
        out.append(f"# TODO(migrate): {comment_safe(t)}")
    if conv.todos:
        out.append("")

    for section in ("node", "listeners", "security", "limits"):
        body = conv.config.get(section) or {}
        deferred = conv.deferred.get(section) or []
        if not body and not deferred:
            continue
        out.append(f"[{section}]")
        for key, em in body.items():
            trailer = ""
            if key in SECURITY_FIELDS and em.source:
                trailer = FROM + comment_safe(em.source)
                if em.defaulted:
                    trailer += DEFAULTED + comment_safe(em.defaulted)
            out.append(f"{key} = {em.rendered}{trailer}")
        out.extend(deferred)
        out.append("")

    if tls_lines:
        out.append("# --- TLS ---")
        out.append("#")
        out.append("# mqttd has ONE [tls] table and applies it to tls_bind, wss_bind and")
        out.append("# quic_bind alike. TLS is 1.3-only by default: a client that cannot")
        out.append("# negotiate TLS 1.3 will fail to connect, so check your device fleet.")
        out.extend(tls_lines)
        out.append("")
    return "\n".join(out) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__.split("\n", maxsplit=1)[0],
        epilog=(
            "PROVENANCE: " + VERSIONS + ". " + SCOPE + " " + DRAFT + " Mosquitto scopes "
            + SCOPED_SECURITY_LIST
            + " PER LISTENER when per_listener_settings is true, and mqttd has no "
            "per-listener security at all — that collapse is reported, not taken silently. "
            "An include_dir is NOT followed and a plugin's own config file is NOT opened: "
            "their contents are never read."
        ),
    )
    ap.add_argument("conf", type=Path, help="path to mosquitto.conf")
    ap.add_argument("--out-config", type=Path, help="write the mqttd TOML here")
    ap.add_argument("--out-acl", type=Path, help="write the translated ACL here")
    ap.add_argument("--acl-file", type=Path, help="override the acl_file path")
    ap.add_argument(
        "--provenance-json",
        type=Path,
        help="write the provenance ledger (every security-relevant value, its source "
        "key and whether it was emitted live) here, for scripts/migrate/property_sweep.py",
    )
    args = ap.parse_args()

    try:
        text = args.conf.read_text(encoding="utf-8")
    except OSError as e:
        print(f"cannot read {args.conf}: {e}", file=sys.stderr)
        return 1
    except UnicodeDecodeError as e:
        # The documented contract is "exit 0 translated, 1 could not read the input" — a bare
        # traceback is neither, and a config saved as UTF-16 or holding a latin-1 path is an
        # ordinary thing to find. Found 2026-08-15 by the fuzz pass.
        print(
            f"cannot read {args.conf}: it is not valid UTF-8 ({e}). Re-save it as UTF-8 "
            "(`iconv -f <encoding> -t utf-8`) and re-run",
            file=sys.stderr,
        )
        return 1

    conv = Conversion()
    parse_mosquitto_conf(text, conv)
    convert_scoped_security(conv)
    convert_psk(conv)
    convert_listener_caps(conv)
    tls_lines = convert_tls(conv)
    render_listeners(conv)

    # Rule 3, "the output must validate": without a data dir the broker REFUSES to start
    # (durable sessions are on by default), so a mosquitto.conf with no
    # persistence_location — the common case, since Mosquitto's persistence is off by
    # default — used to produce a config `mqttd --check-config` rejects outright. Found on
    # 2026-08-14 by adding --check-config to this converter's own test, which is exactly the
    # gap that test documented about itself. The EMQX and HiveMQ converters already did this.
    if "data_dir" not in conv.config.get("node", {}):
        conv.set("node", "data_dir", toml_str("/var/lib/mqttd"))
        conv.note(
            "mosquitto.conf named no persistence_location, so [node] data_dir was set to "
            "mqttd's packaged default /var/lib/mqttd. mqttd's durable sessions are ON by "
            "default and REFUSE to start without a data dir, so this value is what makes "
            "the config valid — mount a real volume there, or the durable state lives on "
            "the container's ephemeral layer. (Mosquitto's persistence was OFF by default, "
            "so if you never set it, queued messages did not survive a restart; on-disk is "
            "very likely what you actually want, but [durable] enabled = false is the "
            "faithful translation.)"
        )

    # THE ACL SOURCE IS READ HERE, BEFORE THE CONFIG IS RENDERED, on purpose. When it cannot
    # be read, the gap belongs in the file the operator is about to DEPLOY, not in a stderr
    # line they scroll past — and the config must not go on naming a policy that was never
    # written. The EMQX and HiveMQ converters were fixed for exactly this in round 1; this
    # one still reported the failure on stderr only, and wrote no ACL at all.
    acl_path = args.acl_file or (Path(conv.acl_file) if conv.acl_file else None)
    acl_text: str | None = None
    if acl_path is not None:
        try:
            acl_text = acl_path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as e:
            print(f"note: could not read acl_file {acl_path}: {e}", file=sys.stderr)
            conv.todo(
                "THE AUTHORIZATION POLICY WAS NOT TRANSLATED. The Mosquitto ACL file "
                f"{str(acl_path)} could not be read ({e}), so NOT ONE RULE from it is in the "
                "generated ACL — which carries the same warning and no rules. [security] "
                f"acl_file below still names a policy file, and {policy_effect(ACL_DEFAULT)}: "
                "that is the right direction and it is NOT a migration. Fix the path "
                "(Mosquitto resolves a relative acl_file against its own working directory) "
                "or pass --acl-file, and re-run before deploying"
            )

    # [security] acl_file: without it mqttd enforces NO authorization at all
    # (crates/mqtt-config/src/lib.rs — `acl_file: Option<String>`, None by default, "without
    # it authorization is not enforced and loudly logged"). This converter translated a whole
    # ACL policy and then never referenced it from the config it wrote, so the deployed
    # broker authorized nothing while the generated ACL's own header said it denied by
    # default. Found on 2026-08-15 by sweeping the class rather than the finding.
    if acl_path is not None:
        conv.set(
            "security",
            "acl_file",
            toml_str("/etc/mqttd/acl.toml"),
            f"acl_file {acl_path} (the POLICY is from there; the path below is this "
            "converter's own --out-acl deployment default)",
            defaulted="the deployed path itself, which is yours to choose",
        )
        conv.note(
            "[security] acl_file points at /etc/mqttd/acl.toml — CHANGE IT if you write the "
            "translated policy elsewhere, and keep the two together: mqttd enforces "
            "authorization ONLY from the file this key names, and with the key unset it "
            "enforces NONE of it (loudly logged on every start). The path is this "
            "converter's default, not something discovered in mosquitto.conf"
        )
    else:
        # DERIVED, not asserted. The old sentence ended "That is FAITHFUL to a Mosquitto with
        # no acl_file (which also authorized everything)" — a claim about the SOURCE, and
        # false for the dynsec layout mosquitto.conf(5) itself recommends, where the whole
        # role/ACL policy lives in the plugin's JSON. So the second half of the sentence is
        # generated from what was actually seen.
        unread = "; ".join(conv.unread)
        conv.todo(
            "mosquitto.conf named NO acl_file, so no policy was translated and [security] "
            "acl_file is NOT set below — which means mqttd will enforce NO authorization at "
            "all: every authenticated client may publish and subscribe anywhere. "
            + (
                "AND THIS FILE NAMED SOMETHING THIS CONVERTER DID NOT READ ("
                + unread
                + "), so do NOT conclude your old broker authorized everything: if that is "
                "a Dynamic Security plugin, your entire role and ACL policy is in there and "
                "NONE of it was seen. Export it and re-model it as an ACL policy"
                if unread
                else "With no acl_file and no plugin in this file, Mosquitto also authorized "
                "everything — so nothing was lost, and it is still the wrong end state"
            )
            + ". Write an ACL policy and set acl_file, or re-run with --acl-file <the real "
            "acl file> if the policy lives somewhere this file does not mention"
        )

    config = render_config(conv, tls_lines)
    if args.out_config:
        args.out_config.write_text(config, encoding="utf-8")
        print(f"wrote {args.out_config}")
    else:
        print(config)

    if acl_path is not None:
        if acl_text is None:
            # An unreadable source is not fatal (the contract: exit 0 with the gap named),
            # but the ACL document is still WRITTEN — deny-by-default, zero rules, and the
            # gap stated at the top, so the file the operator deploys says what happened.
            rules, todos = [], [
                "NOTHING WAS TRANSLATED INTO THIS FILE. The Mosquitto ACL file "
                f"{str(acl_path)} could not be read, so this policy has NO rules and "
                f"{policy_effect(ACL_DEFAULT)}. Fix the path (or pass --acl-file) and re-run"
            ]
        else:
            rules, todos = parse_acl(acl_text)
            if not rules:
                todos.insert(
                    0,
                    "NO RULE could be translated from the Mosquitto ACL file. Either every "
                    "line landed on a gap listed below, or the file held nothing this "
                    f"converter recognises. With no rules, {policy_effect(ACL_DEFAULT)}. "
                    "Read the TODOs below before deploying",
                )
        acl = render_acl(rules, todos, ACL_DEFAULT, conv.prov)
        if args.out_acl:
            args.out_acl.write_text(acl, encoding="utf-8")
            print(f"wrote {args.out_acl} ({len(rules)} rules)")
        else:
            print(acl)

    if args.provenance_json:
        args.provenance_json.write_text(
            conv.prov.ledger("from-mosquitto.py"), encoding="utf-8"
        )
        print(f"wrote {args.provenance_json}")

    n = len(conv.todos)
    if n:
        print(
            f"\n{n} setting(s) had no direct equivalent and are marked TODO(migrate) "
            "in the output. Read them before deploying.",
            file=sys.stderr,
        )
    print(
        "\nNext: mqttd --check-config --config <the config above>",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
