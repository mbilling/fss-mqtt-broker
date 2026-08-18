#!/usr/bin/env python3
"""Translate a HiveMQ deployment to mqttd configuration (fixtures: HiveMQ CE @ 2026.5 + file-RBAC extension @ 4.6.16; a starting point that needs review, never a validated migration).

Reads HiveMQ's `config.xml` and, with `--credentials`, the File RBAC extension's
`credentials.xml`. Emits an mqttd TOML config plus an mqttd ACL policy.

## What it will and will not do

It translates the settings that have an exact mqttd equivalent, and for
everything else it **says so in the output** rather than guessing. A converter
that silently drops a setting is worse than no converter: you would deploy
believing the policy came across.

Anything not translated is emitted as a `# TODO(migrate):` comment at the point
it belongs, so the gap is visible in the file you are about to deploy rather than
in a report you read once. That includes every unknown XML element: it is reported
by its full element path, which is how HiveMQ **Enterprise**-only sections
(`<cluster>`, `<control-center>`, `<license>`, the Enterprise Security Extension,
the Enterprise Bridge Extension) are handled honestly without this converter ever
having seen their schema.

## Two findings that shape the output

1. **HiveMQ TLS material is a JKS keystore with passwords; mqttd wants PEM paths.**
   There is no conversion — the tool emits a `TODO(migrate)` with a `keytool` +
   `openssl` extraction recipe and never touches the key material itself.
2. **HiveMQ roles cannot become mqttd `groups`.** mqttd populates `groups` only
   from an OIDC token claim or the HTTP auth hook's response body; the password-file
   authenticator always yields an empty group list. So file-RBAC roles are FLATTENED
   into per-user `identities` rules — correct, but a bigger file — with the
   alternative named in a TODO.

## Provenance, and what was NOT verified

Mappings and fixtures were built from HiveMQ Community Edition's own shipped
`config.xsd` and `src/distribution/conf/examples/` at tag **2026.5** (CE is
calendar-versioned; "HiveMQ 4.x" now names the Enterprise line and the extension
SDK), plus the vendor's documented `credentials.xml` / `extension-config.xml`
examples in `hivemq/hivemq-file-rbac-extension` @ **4.6.16**.

**No live HiveMQ broker was run**, CE or Enterprise, and no ground-truth config from
one was used. **HiveMQ Enterprise's config schema is not open source**, so every
Enterprise element is handled by construction (unknown element -> TODO naming its
path), not from a schema this tool has read. Treat the output as a draft to review.

Note also that **CE has no authentication and no authorization at all** — both come
from extensions. If your CE deployment has no `--credentials`, its clients were
anonymous, and mqttd refuses anonymous clients by default.

## Usage

    scripts/migrate/from-hivemq.py /opt/hivemq/conf/config.xml \\
        --credentials /opt/hivemq/extensions/hivemq-file-rbac-extension/credentials.xml \\
        --out-config mqttd.toml --out-acl acl.toml

    # Review, then validate before deploying — this never writes a config the
    # broker has not been asked to check:
    mqttd --check-config --config mqttd.toml

Exit codes: 0 translated (possibly with TODOs), 1 could not read the input.
"""

from __future__ import annotations

import argparse
import json
import shlex
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

VERSIONS = (
    "fixtures/mappings built from hivemq/hivemq-community-edition @ 2026.5 "
    "(config.xsd + src/distribution/conf/examples/) and "
    "hivemq/hivemq-file-rbac-extension @ 4.6.16 (README.adoc examples); "
    "no live HiveMQ broker was run and Enterprise's schema is not open source"
)

# The one sentence docs/MIGRATION.md's version-scope paragraph claims every converter's
# --help repeats. It is the same text in from-mosquitto.py and the other converter.
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
    "TODO naming the decision you have to make — so an unread construct can leave the "
    "output INCOMPLETE but can never leave a live security setting nobody derived. Every "
    "live security-relevant line carries `# from: <the HiveMQ element it came from>`. "
    "VERIFIED: fixtures diffed against pinned vendor sources; the provenance, "
    "no-live-without-source, drop, contradiction and validity invariants of "
    "scripts/migrate/property_sweep.py over generated and mechanically mutated inputs; and "
    "`mqttd --check-config` on every generated config plus the ACL loaded by the real "
    "broker. NOT VERIFIED: no live HiveMQ was EVER run against this converter, HiveMQ "
    "Enterprise's schema is not open source, and NO claim of total coverage over "
    "config.xsd is made — a construct it has never seen is a construct it cannot report, and "
    "a construct whose MEANING it misreads is one it can still translate wrongly: the "
    "provenance gate proves a live value came from a named input element, NOT that the "
    "element means what this converter took it to mean. docs/MIGRATION.md's KNOWN GAPS "
    "section lists every misreading found so far."
)

# The condensed form that goes into the generated files themselves.
DRAFT_HEADER = [
    "# THIS IS A DRAFT, NOT A TRANSLATION. Anything this converter could not derive",
    "# from your input is COMMENTED OUT beside a TODO naming the decision, so this",
    "# file may be INCOMPLETE — but no live security setting in it was invented.",
    "# Every live security-relevant line carries `# from: <the HiveMQ element>`.",
    "# NOT VERIFIED: no live HiveMQ was ever run; Enterprise's schema is not open",
    "# source; no total-coverage claim over config.xsd is made.",
]

# ---------------------------------------------------------------------------
# String emission. ONE helper per channel, used by EVERY string this tool writes.
#
# The 2026-08-14 review found the whole class at once: no value was escaped
# anywhere. A file-RBAC `<name>CORP\jdoe</name>` — an AD-style username, exactly
# what the populations being migrated look like — came out as
# `identities = ["CORP\jdoe"]`, which tomllib REJECTS ("Unescaped '\' in a
# string"). One such user poisons the WHOLE ACL file: the broker refuses to load
# any of it, so the migration stalls on an opaque TOML error instead of a TODO.
# Nothing below builds a quoted string by hand.
#
# The same helpers are duplicated verbatim in from-mosquitto.py and from-emqx.py
# rather than shared through an import, deliberately: each converter is ONE
# self-contained stdlib-only file, run standalone (`mqttui migrate hivemq`, or
# copied to the machine that holds the vendor config), and an import would make it
# two files that must travel together.
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


def shell_arg(value: object) -> str:
    """Quote a value for the shell one-liners this tool prints in comments.

    The re-enrolment line is meant to be COPIED AND RUN, so `CORP\\jdoe` has to
    survive the shell as well as the TOML parser.
    """
    return shlex.quote(str(value))


def comment_safe(text: object) -> str:
    """Flatten a value to one line so it cannot break out of a `#` comment.

    A newline inside a TODO message ends the comment and leaves the rest of the
    sentence as a bare line the TOML parser then rejects — the same
    "the output must validate" failure as an unescaped backslash, one channel over.

    `str.split()` folds only Python whitespace, so `\x00`-`\x08` and `\x0b`-`\x1f` survived
    into the comment — and TOML 1.0 forbids a raw control character ANYWHERE in a document,
    comments included, so one such byte in a path made `tomllib` and the broker reject the
    WHOLE file while this converter still printed `wrote <file>`. Escaped rather than dropped,
    so the operator can still see what the byte was. Found 2026-08-15.
    """
    flattened = " ".join(str(text).split())
    return "".join(c if c >= " " and c != "\x7f" else f"\\u{ord(c):04X}" for c in flattened)


# ---------------------------------------------------------------------------
# What has an mqttd equivalent.
#
# Keys are element paths relative to <hivemq>, so an unknown element is reported
# with the same vocabulary the operator reads in their own file.
# ---------------------------------------------------------------------------

# path -> (mqttd section, key, kind)
DIRECT: dict[str, tuple[str, str, str]] = {
    "mqtt/packets/max-packet-size": ("limits", "max_packet_size", "int"),
    "mqtt/receive-maximum/server-receive-maximum": ("limits", "receive_maximum", "u16"),
    "mqtt/topic-alias/max-per-client": ("limits", "topic_alias_max", "u16"),
    "mqtt/queued-messages/max-queue-size": ("limits", "max_queued_messages", "int"),
    "restrictions/max-connections": ("limits", "max_connections", "int_neg1"),
}

# path -> the reason there is no equivalent. Being explicit about *why* is the point:
# "unsupported" invites a bug report, "deliberately absent, here is the alternative"
# does not.
NO_EQUIVALENT: dict[str, str] = {
    "mqtt/session-expiry/max-interval": "a broker-side cap on the session-expiry "
    "interval is not implemented; a v5 client sets its own Session Expiry Interval and "
    "mqttd honours it. Bound the cost with [limits] max_sessions and "
    "max_queued_messages instead",
    "mqtt/message-expiry/max-interval": "a broker-side cap on message expiry is not "
    "implemented; a v5 client's Message Expiry Interval is honoured as sent",
    "mqtt/keep-alive/max-keep-alive": "mqttd does not send a Server Keep Alive, so it "
    "cannot cap the client's requested interval",
    "mqtt/keep-alive/allow-unlimited": "the client's keepalive is used as sent; there "
    "is no cap to allow or forbid",
    "mqtt/topic-alias/enabled": "topic aliases are always available; set [limits] "
    "topic_alias_max = 0 to disable them",
    "mqtt/quality-of-service/max-qos": "mqttd supports QoS 0/1/2 and cannot cap the "
    "maximum. If you capped QoS to shed load, nothing here reproduces that — the "
    "nearest controls are [limits] max_publish_rate and max_queued_messages",
    "mqtt/wildcard-subscriptions/enabled": "wildcard subscriptions cannot be switched "
    "off. Deny the wildcards you object to in the ACL policy instead",
    "mqtt/shared-subscriptions/enabled": "shared subscriptions cannot be switched off",
    "mqtt/subscription-identifier/enabled": "subscription identifiers are fully "
    "supported and delivered per subscription (issue #266); there is nothing to "
    "enable or disable",
    "mqtt/retained-messages/enabled": "retained messages cannot be switched off; cap "
    "them with [limits] max_retained_messages, or deny retained topics in the ACL",
    "mqtt/queued-messages/strategy": "handled — see the queue_overflow mapping",
    "restrictions/max-client-id-length": "the client-id length limit is not "
    "configurable (the spec's 65535 applies)",
    "restrictions/max-topic-length": "the topic-length limit is not configurable",
    "restrictions/no-connect-idle-timeout": "the pre-CONNECT idle timeout is not "
    "configurable",
    "restrictions/incoming-bandwidth-throttling": "there is no byte-rate limiter. "
    "[limits] max_publish_rate counts MESSAGES per second per connection — a different "
    "control, and it will not bound a few very large payloads",
    "security/allow-empty-client-id": "a zero-length client id is accepted with a "
    "clean session and refused otherwise, per spec; not configurable",
    "security/payload-format-validation": "the v5 Payload Format Indicator is carried "
    "through unvalidated; the broker does not reject a mislabelled payload",
    "security/utf8-validation": "UTF-8 validation is ALWAYS on (an invalid string is a "
    "protocol error); there is nothing to disable",
    "security/allow-request-problem-information": "Request Problem Information is "
    "honoured per spec; not configurable",
    "anonymous-usage-statistics/enabled": "mqttd sends no telemetry anywhere, so there "
    "is nothing to disable. Nothing to carry over",
    "persistence/mode": "handled — see the data_dir note",
    "listeners/websocket-listener/allow-extensions": "WebSocket extensions are not "
    "negotiated",
    "listeners/tls-websocket-listener/allow-extensions": "WebSocket extensions are not "
    "negotiated",
    "mqtt/queued-messages/queue-shared-subscriptions": "not implemented",
}

# Enterprise-only / extension elements. This converter has never seen their schema, so
# they are recognised by NAME and reported, which is the honest handling.
ENTERPRISE: dict[str, str] = {
    "cluster": "a HiveMQ ENTERPRISE cluster block. mqttd clusters, but nothing here "
    "translates: the mesh needs a per-node bus certificate whose Subject CN equals "
    "[node] id (plus a SAN covering peer_advertise, both serverAuth and clientAuth "
    "EKUs, and an ECDSA/Ed25519 key — never RSA), a 64-hex signed-gossip key shared "
    "cluster-wide, and the FOUNDER rule (exactly one seedless node founds the lease "
    "group). Walk docs/SECURED-CLUSTER-TUTORIAL.md and set [cluster] peer_bind, "
    "[cluster.peer_tls] and [cluster.swim] deliberately",
    "control-center": "the HiveMQ Enterprise Control Center has no equivalent, by "
    "design (signal-driven operations, ADR 0020): /metrics for Prometheus, /statusz for "
    "state, an audit log, and config + SIGHUP for changes. Plan the operator workflow "
    "that replaces the Control Center BEFORE cutover, not after",
    "license": "mqttd is Apache-2.0 with no licence file and no licensed features — "
    "clustering included. Nothing to carry over",
    "restrictions/max-connections-per-listener": "per-listener connection caps do not "
    "exist; [limits] max_connections is node-wide",
    "internal": "HiveMQ internal tuning has no equivalent and should not be recreated",
    "overload-protection": "there is no overload-protection subsystem to configure. The "
    "nearest controls are the [limits] memory_max_bytes and [durable] store_max_bytes "
    "BROWNOUT watermarks, which refuse growth writes (an answered refusal to a QoS>=1 "
    "publisher, never a silent drop) rather than throttling clients",
    "mqtt-sn": "MQTT-SN is not implemented; mqttd speaks MQTT 3.1.1/5 only",
    "modules": "there is no module or extension system",
    "extensions": "there is no extension SDK and no ABI to port to. An AUTHENTICATION "
    "extension maps to the password file / JWT / OIDC / mTLS, or to the HTTP auth hook "
    "([security.http_auth], status-code-is-the-verdict) which is the supported path to "
    "LDAP, a SQL user table or OAuth2 introspection — YOU write that adapter. An "
    "AUTHORIZATION extension maps to the ACL policy, optionally keyed on groups from "
    "OIDC/HTTP auth. A message-interceptor extension has NO equivalent and must become "
    "a client-side pipeline",
    "ese": "the HiveMQ Enterprise Security Extension has no equivalent. Its realms "
    "(LDAP, SQL, JWT, OAuth2) map onto mqttd's own JWT/OIDC authenticators where they "
    "fit, and onto [security.http_auth] where they do not — one HTTP hook you write, "
    "reaching the store you already run. Its pipelines and its SQL-backed authorization "
    "must be redesigned as an ACL policy plus groups",
    "bridge": "the HiveMQ Enterprise Bridge Extension maps to mqtt-bridge, a SEPARATE "
    "PROCESS with its own TOML (docs/BRIDGE.md). It is not translated here because a "
    "cutover bridge is a different shape from a steady-state one — see "
    "docs/MIGRATION.md's dual-run section, which spells out the exact config",
}

# ---------------------------------------------------------------------------
# PROVENANCE OR NOTHING.
#
# Every finding of the three review rounds that mattered had one shape — a LIVE
# security-relevant value the tool had not derived from the input: an mTLS mandate dropped
# from any TLS listener that was not first in document order, `client_ca` pointing at a
# placeholder the operator was told to obtain from a step the tool never printed, a WebSocket
# listener emitted as a raw-MQTT bind, an ACL whose comment claimed a posture its own
# `default` contradicted.
#
# So SECURITY_FIELDS names the fields whose value decides who can connect and what they may
# do, and the ONLY way to write one is Provenance.line(), which takes the value AND the
# HiveMQ element it came from and REFUSES to emit a live line without it. A field with no
# provenance is emitted COMMENTED OUT beside a TODO naming what the operator must decide, and
# a value the input did not literally hold — mqttd needs PEM PATHS where HiveMQ has a JAVA
# KEYSTORE, so `cert`/`key`/`client_ca` are necessarily paths the operator will create — says
# so on the line with `defaulted:`, naming the keystore it must be extracted from.
# ---------------------------------------------------------------------------

SECURITY_FIELDS = frozenset(
    {
        "plaintext_bind",
        "tls_bind",
        "ws_bind",
        "wss_bind",
        "quic_bind",
        "cert",
        "key",
        "client_ca",
        "crl",
        "allow_tls12",
        "acl_file",
        "password_file",
        "allow_anonymous",
        "mtls_identity_source",
        "default",
    }
)

FROM = "  # from: "
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
        """`field = rendered  # from: source`, or an INERT candidate plus a TODO."""
        if field in SECURITY_FIELDS and not source:
            self.rows.append(Emitted(field, rendered, None, defaulted, live=False))
            reason = decide or (
                f"nothing in the HiveMQ configuration named a value for {field}, so it is "
                "emitted COMMENTED OUT rather than guessed at. Decide it yourself and "
                "uncomment"
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
        """A candidate deliberately NOT activated: a posture change, or an illegal pair."""
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


QOS_LIKE = {"ZERO", "ONE", "TWO", "ZERO_ONE", "ONE_TWO", "ZERO_TWO", "ALL"}


def bind_gap(address: str) -> str | None:
    """None when `address` is a `host:port` mqttd can bind; otherwise WHY it cannot.

    Every `*_bind` used to be emitted LIVE with no check that the broker can bind it, and
    `mqttd --check-config` — the verification this converter's own header, its `--help` and
    docs/MIGRATION.md all point the operator at — accepts ANY string there. A
    `<port>abc</port>` therefore produced a live `plaintext_bind = "10.0.0.1:abc"`, `config OK`,
    and then `Error { kind: InvalidInput, message: "invalid port value" }` at STARTUP — at the
    maintenance window, on the one value the whole provenance restructuring is about. The same
    check lives in from-mosquitto.py and from-emqx.py: each converter is ONE self-contained
    stdlib-only file. Found 2026-08-15.
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
            f"`{address}` is not a TCP address: `{host}` is a filesystem path, and mqttd has NO "
            "unix-socket transport at all"
        )
    if not port.isdigit() or not 1 <= int(port) <= 65535:
        return (
            f"`{port}` is not a TCP port number (1-65535), so `{address}` is not an address "
            "mqttd can bind — it passes --check-config and then fails at startup"
        )
    if any(
        c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_:"
        for c in host
    ):
        return (
            f"`{host}` is not an address or hostname mqttd can resolve, so `{address}` is not "
            "one it can bind"
        )
    return None


@dataclass
class Listener:
    kind: str
    port: str | None = None
    bind: str | None = None
    name: str | None = None
    path: str | None = None
    subprotocols: list[str] = field(default_factory=list)
    tls: dict[str, str] = field(default_factory=dict)

    @property
    def address(self) -> str | None:
        """The address to bind, or None when the input named no `<port>`.

        `self.port or "1883"` used to stand here: a listener element whose `<port>` this
        converter had not read became a LIVE bind on port 1883, which is a port the input
        never named. `<bind-address>` is genuinely optional in config.xsd and HiveMQ then
        listens on every interface, so THAT half is a documented default (named as
        `defaulted:` on the emitted line); `<port>` is not.
        """
        if not self.port:
            return None
        return f"{self.bind or '0.0.0.0'}:{self.port}"

    @property
    def host_defaulted(self) -> str | None:
        if self.bind:
            return None
        return (
            "the host, because the listener element has no <bind-address> and HiveMQ then "
            "listens on EVERY interface (config.xsd makes it optional)"
        )

    @property
    def source(self) -> str:
        """The HiveMQ elements this listener's address was derived from."""
        parts = [f"listeners/{self.kind}/port"]
        if self.bind:
            parts.append("bind-address")
        if self.name:
            parts.append(f"name={self.name}")
        return " + ".join(parts)

    @property
    def where(self) -> str:
        """How this listener is named in every message about it — never fabricated."""
        addr = self.address
        named = f" named {self.name!r}" if self.name else ""
        if addr is None:
            at = f" (its <bind-address> was {self.bind})" if self.bind else ""
            return f"a {self.kind}{named} with NO <port> in the input{at}"
        return f"{self.kind}{named} on {addr}"


@dataclass
class Conversion:
    config: dict[str, dict[str, Emitted]] = field(default_factory=dict)
    listeners: list[Listener] = field(default_factory=list)
    todos: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    prov: Provenance = field(default_factory=Provenance)
    # Security-relevant candidates that are NOT activated, rendered commented after their
    # section: `section -> lines`.
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
        `[listeners]`, `[tls]` or `[security]` without naming the HiveMQ element it came from.
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

    # todo()/note() flatten to one line HERE, so no caller can emit a message that
    # breaks out of its `#` comment and leaves a bare line the TOML parser rejects.
    def todo(self, msg: str) -> None:
        msg = comment_safe(msg)
        if msg not in self.todos:
            self.todos.append(msg)

    def note(self, msg: str) -> None:
        msg = comment_safe(msg)
        if msg not in self.notes:
            self.notes.append(msg)


LISTENER_KINDS = {
    "tcp-listener": "plaintext_bind",
    "tls-tcp-listener": "tls_bind",
    "websocket-listener": "ws_bind",
    "tls-websocket-listener": "wss_bind",
}


def text_of(el: ET.Element) -> str:
    return (el.text or "").strip()


def parse_listener(el: ET.Element, conv: Conversion) -> Listener:
    lst = Listener(kind=el.tag)
    for child in el:
        tag = child.tag
        if tag == "port":
            lst.port = text_of(child)
        elif tag == "bind-address":
            lst.bind = text_of(child)
        elif tag == "name":
            lst.name = text_of(child)
        elif tag == "path":
            lst.path = text_of(child)
        elif tag == "subprotocols":
            lst.subprotocols = [text_of(s) for s in child if text_of(s)]
        elif tag == "tls":
            parse_tls(child, lst, conv)
        elif tag == "allow-extensions":
            conv.todo(
                f"listeners/{el.tag}/allow-extensions: "
                + NO_EQUIVALENT.get(
                    f"listeners/{el.tag}/allow-extensions",
                    "WebSocket extensions are not negotiated",
                )
            )
        elif tag == "proxy-protocol":
            conv.todo(
                f"listeners/{el.tag}/proxy-protocol: the PROXY protocol is not "
                "supported. A layer-4 load balancer in front of mqttd must preserve the "
                "source address, or per-IP limits and the audit log will see the "
                "balancer's address instead of the client's"
            )
        else:
            conv.todo(
                f"listeners/{el.tag}/{tag}: no direct equivalent — check the mqttd "
                "configuration table (README env-var table / docs/mqttd.example.toml)"
            )
    return lst


def parse_tls(el: ET.Element, lst: Listener, conv: Conversion) -> None:
    # Every TODO below names the listener it came from, PORT INCLUDED where the document has
    # already given one: HiveMQ listener elements have no name attribute, so two
    # <tls-tcp-listener>s would otherwise produce one deduplicated message and the operator
    # could not tell which listener lost the setting.
    where = f"listeners/{lst.kind}" + (f" (port {lst.port})" if lst.port else "")
    for child in el:
        tag = child.tag
        if tag in ("keystore", "truststore"):
            for sub in child:
                if sub.tag == "path":
                    lst.tls[f"{tag}-path"] = text_of(sub)
                elif sub.tag in ("password", "private-key-password"):
                    # A secret. It is never read, never copied, never transformed.
                    lst.tls[f"{tag}-has-password"] = "yes"
                else:
                    conv.todo(
                        f"{where}/tls/{tag}/{sub.tag}: no direct equivalent"
                    )
        elif tag == "client-authentication-mode":
            lst.tls["client-auth"] = text_of(child).upper()
        elif tag in ("protocols", "cipher-suites"):
            values = [text_of(v) for v in child if text_of(v)]
            if tag == "protocols":
                lst.tls["protocols"] = ",".join(values)
            else:
                # NAMED, not counted. "(1 listed)" left the operator unable to check which
                # suite their fleet depends on against a config that no longer selects any —
                # and a report that cannot be checked against the input is not a report.
                conv.todo(
                    f"{where}/tls/cipher-suites listed {values}: cipher suites are not "
                    "configurable in mqttd — TLS 1.3 AEAD suites only, and the hardened "
                    "ECDHE+AEAD subset when [tls] allow_tls12 is on. If any client depends on "
                    "one of those suites specifically, it will not negotiate"
                )
        elif tag == "prefer-server-cipher-suites":
            conv.todo(
                f"{where}/tls/prefer-server-cipher-suites: cipher suites "
                "are not configurable, so neither is their ordering"
            )
        elif tag == "handshake-timeout":
            conv.todo(
                f"{where}/tls/handshake-timeout: the TLS handshake timeout "
                "is not configurable"
            )
        elif tag == "native-ssl":
            conv.todo(
                f"{where}/tls/native-ssl: mqttd's TLS is rustls; there is "
                "no OpenSSL/native provider to select"
            )
        else:
            conv.todo(
                f"{where}/tls/{tag}: no direct equivalent — check the mqttd "
                "configuration table"
            )


def walk(el: ET.Element, conv: Conversion, prefix: str = "") -> None:
    """Depth-first over <hivemq>: map what maps, report everything else by path."""
    for child in el:
        tag = child.tag
        path = f"{prefix}/{tag}" if prefix else tag

        if tag == "listeners" and not prefix:
            for sub in child:
                if sub.tag in LISTENER_KINDS:
                    conv.listeners.append(parse_listener(sub, conv))
                else:
                    conv.todo(
                        f"listeners/{sub.tag}: mqttd has no such transport. It speaks "
                        "MQTT over TCP (plaintext_bind), TLS (tls_bind), WebSocket "
                        "(ws_bind), WSS (wss_bind) and QUIC (quic_bind) only"
                    )
            continue

        if tag in ENTERPRISE and not prefix:
            child_count = sum(1 for _ in child.iter()) - 1
            conv.todo(
                f"<{tag}> ({child_count} nested element(s)): " + ENTERPRISE[tag]
            )
            continue

        if path in ENTERPRISE:
            conv.todo(f"{path}: {ENTERPRISE[path]}")
            continue

        if path in DIRECT:
            section, mkey, kind = DIRECT[path]
            raw = text_of(child)
            if kind == "int_neg1":
                if raw.lstrip("-").isdigit() and int(raw) < 0:
                    conv.note(
                        f"{path} = {raw} means UNLIMITED in HiveMQ, so [{section}] {mkey} "
                        "was left unset (also unlimited). Cap it deliberately — "
                        "docs/SIZING.md has the arithmetic for a fixed RAM budget"
                    )
                elif raw.isdigit():
                    conv.set(section, mkey, raw)
                else:
                    conv.todo(f"{path} = {raw!r}: not an integer mqttd can use")
            elif kind == "u16":
                if not raw.isdigit():
                    conv.todo(f"{path} = {raw!r}: not an integer")
                else:
                    conv.set(section, mkey, str(min(int(raw), 65535)))
                    if int(raw) > 65535:
                        conv.todo(
                            f"{path} = {raw} exceeds the MQTT 5 16-bit field that "
                            f"{section}.{mkey} maps to; clamped to 65535"
                        )
            else:
                if not raw.isdigit():
                    conv.todo(f"{path} = {raw!r}: not an integer")
                else:
                    conv.set(section, mkey, raw)
            continue

        if path == "mqtt/queued-messages/strategy":
            raw = text_of(child).lower()
            # HiveMQ `discard` discards the INCOMING message; `discard-oldest` drops the
            # oldest queued one. mqttd spells those reject-newest / drop-oldest.
            mapped = {"discard": "reject-newest", "discard-oldest": "drop-oldest"}.get(raw)
            if mapped:
                conv.set("limits", "queue_overflow", toml_str(mapped))
                conv.note(
                    f"mqtt/queued-messages/strategy = {raw} became [limits] "
                    f'queue_overflow = "{mapped}". Both arms ACK the publisher and then '
                    "shed a message — that is the one place mqttd acks and drops by "
                    "default, and it is counted as "
                    'mqttd_publish_dropped_total{reason="queue-overflow"} on /metrics '
                    '(the registry prefixes every broker series with `mqttd_`, and the '
                    'Prometheus text form suffixes a counter with `_total` — an alert on '
                    'the bare name returns no data)'
                )
            else:
                conv.todo(
                    f"mqtt/queued-messages/strategy = {raw!r}: mqttd's queue_overflow is "
                    '"drop-oldest" or "reject-newest" only'
                )
            continue

        if path == "persistence/mode":
            raw = text_of(child).lower()
            if raw in ("file", "file-native"):
                conv.note(
                    "persistence mode was FILE, so durable state was on disk. mqttd's "
                    "durable sessions are on by default and need [node] data_dir set to a "
                    "MOUNTED volume — the value below is a default, not a discovered path"
                )
            elif raw == "in-memory":
                conv.todo(
                    "persistence mode was IN-MEMORY. mqttd's durable plane is ON by "
                    "default and REFUSES to start without [node] data_dir; the config "
                    "below sets one. If you truly want in-memory, set [durable] enabled = "
                    "false (the lightweight store — no opt-in needed), or [durable] "
                    "allow_ephemeral = true for dev only, which is loudly WARNed on every "
                    "start. Note that an in-memory HiveMQ lost queued messages on restart "
                    "too, so on-disk is very likely what you actually want"
                )
            else:
                conv.todo(f"persistence/mode = {raw!r}: not a mode this converter knows")
            continue

        if path in NO_EQUIVALENT:
            # The VALUE is named. Every `.../enabled` element under <mqtt>, <security> and
            # <anonymous-usage-statistics> is a DISABLE-ABLE construct, and the table's
            # sentence read identically whether it held the harmless default or the value that
            # says a feature was SWITCHED OFF — so a real posture drop (wildcard subscriptions
            # forbidden, retained messages off) looked exactly like noise. A report that
            # cannot be checked against the input is not a report; the value makes it one.
            raw = text_of(child)
            conv.todo(
                f"{path}" + (f" = {raw!r}" if raw else "") + f": {NO_EQUIVALENT[path]}"
            )
            continue

        # A container element: recurse. A leaf: report it by path.
        if len(child):
            walk(child, conv, path)
            continue
        conv.todo(
            f"{path} = {text_of(child)!r}: no direct equivalent — check the mqttd "
            "configuration table (README env-var table / docs/mqttd.example.toml)"
        )


def build_listeners(conv: Conversion) -> list[str]:
    """One bind per protocol; the extras become inline TODOs in the [listeners] table."""
    out: list[str] = []
    seen: dict[str, Listener] = {}
    extras: list[Listener] = []
    # A listener whose address IS derivable takes the bind, whatever the document order.
    # An address mqttd cannot BIND is no better than one nobody derived, so an unbindable one
    # sorts behind a bindable one exactly like a missing `<port>` does.
    for lst in sorted(
        conv.listeners,
        key=lambda l: l.address is None or bind_gap(l.address) is not None,
    ):
        key = LISTENER_KINDS[lst.kind]
        if key in seen:
            extras.append(lst)
            continue
        seen[key] = lst
        unbindable = bind_gap(lst.address) if lst.address is not None else None
        if unbindable is not None:
            conv.set(
                "listeners",
                key,
                toml_str(lst.address),
                None,
                decide=f"{lst.where} gives [listeners] {key} as {lst.address!r}, and that is "
                f"not an address mqttd can bind: {unbindable}. `mqttd --check-config` ACCEPTS "
                "any string here and the broker then fails at STARTUP, so the line is emitted "
                "COMMENTED OUT rather than live: set an address the broker can bind and "
                "uncomment it",
            )
            continue
        if lst.address is None:
            conv.set(
                "listeners",
                key,
                toml_str("0.0.0.0:1883"),
                None,
                decide=f"{lst.where} — so this converter has NO address to put in "
                f"[listeners] {key} and refuses to invent one (`<port>` is required by "
                "config.xsd, so this input is not something HiveMQ would start on either). "
                "The line below is a PLACEHOLDER, not a value from your config: set the real "
                "port and uncomment it, or the broker serves no clients on that transport",
            )
            continue
        conv.set(
            "listeners",
            key,
            toml_str(lst.address),
            lst.source,
            defaulted=lst.host_defaulted,
        )
    for lst in extras:
        out.append(
            comment_safe(
                f"# TODO(migrate): additional {lst.where} — mqttd binds ONE listener per "
                "protocol; consolidate its clients onto the bind above, or run a second "
                "deployment"
            )
        )
    for lst in conv.listeners:
        if lst.path:
            conv.note(
                f"{lst.kind} served WebSocket at path {lst.path!r}. mqttd accepts the "
                "upgrade on ANY path (verified in crates/mqtt-net/src/ws.rs — it checks "
                "the `mqtt` subprotocol, not the URI), so existing browser clients keep "
                "working unchanged. There is no path to configure"
            )
        extra_protos = [p for p in lst.subprotocols if p != "mqtt"]
        if extra_protos:
            conv.todo(
                f"{lst.kind} offered the WebSocket subprotocol(s) {extra_protos} beside "
                "`mqtt`. mqttd negotiates ONLY `mqtt` and REFUSES an upgrade that does "
                "not offer it, so any client sending one of those instead will fail the "
                "handshake — check your browser clients before cutover"
            )
    # Derived from what the file will actually DO, not from what was found in the input: a
    # listener whose address is missing OR unbindable contributes no live bind, so warning about
    # cleartext that is not there would be one more sentence contradicting the output.
    live_plaintext = [
        k
        for k in ("plaintext_bind", "ws_bind")
        if k in seen
        and seen[k].address is not None
        and bind_gap(seen[k].address) is None
    ]
    if live_plaintext:
        conv.note(
            "a PLAINTEXT listener was carried over ("
            + ", ".join(sorted(live_plaintext))
            + ", each with the HiveMQ listener it came from on the line). mqttd logs "
            "this as an INSECURE mode on every start, and credentials cross it in the "
            "clear. Retire it during the migration if you can"
        )
    return out


def build_tls(conv: Conversion) -> list[str]:
    """The [tls] table, or an honest TODO block where JKS cannot become PEM.

    EVERY TLS listener is walked. mqttd has one [tls] table and applies it to tls_bind,
    wss_bind AND quic_bind alike (one shared rustls acceptor plus quic::server_endpoint,
    crates/mqttd/src/main.rs), so per-listener TLS cannot be expressed at all — which makes
    reporting the only honest option.

    Round 1 found this function reading `tls_listeners[0]` only: on the ordinary HiveMQ
    shape (wss for browsers, tls-tcp for devices) an mTLS MANDATE on the listener that is
    not first in document order VANISHED — no client_ca, no commented candidate, no TODO —
    and its truststore and <protocols> went with it. `mqttd --check-config` passes on such a
    file, so nothing downstream caught it: the operator migrated a broker that REQUIRED
    client certificates and deployed one that accepts those clients with none.
    """
    tls_listeners = [l for l in conv.listeners if l.tls]
    if not tls_listeners:
        return []
    first = tls_listeners[0]
    out: list[str] = []

    def who(lst: Listener) -> str:
        return lst.where

    def mode_of(lst: Listener) -> str:
        return lst.tls.get("client-auth", "NONE").upper() or "NONE"

    if len(tls_listeners) > 1:
        conv.todo(
            f"{len(tls_listeners)} TLS listeners were found ("
            + "; ".join(
                f"{who(l)}: keystore={l.tls.get('keystore-path') or 'unset'}, "
                f"truststore={l.tls.get('truststore-path') or 'unset'}, "
                f"client-authentication-mode={mode_of(l)}"
                for l in tls_listeners
            )
            + "). mqttd has ONE [tls] table and applies it to tls_bind, wss_bind AND "
            "quic_bind alike, so there is no per-listener TLS: the single table below "
            "governs every TLS transport at once. Read each listener's line above against "
            "the posture the table ends up with"
        )
        stores = {l.tls.get("keystore-path") for l in tls_listeners}
        if len(stores) > 1:
            conv.todo(
                "those TLS listeners use DIFFERENT keystores "
                f"({sorted(s for s in stores if s)}). Only ONE cert/key pair can be "
                f"referenced, so extract {who(first)}'s and be aware that EVERY TLS "
                "transport is then served from it — reissue one certificate covering every "
                "name (a SAN per hostname), or split the listeners across separate "
                "deployments"
            )
    ks = first.tls.get("keystore-path")
    truststores = [
        l.tls.get("truststore-path") for l in tls_listeners if l.tls.get("truststore-path")
    ]
    ts = truststores[0] if truststores else None
    required = [l for l in tls_listeners if mode_of(l) == "REQUIRED"]
    optional = [l for l in tls_listeners if mode_of(l) == "OPTIONAL"]
    lax = [l for l in tls_listeners if mode_of(l) != "REQUIRED"]

    # THE conversion that cannot be done: JKS+passwords -> PEM paths.
    #
    # HOW FAR THE RECIPE BELOW WAS VERIFIED, exactly: its openssl half WAS RUN — a
    # PKCS#12 was minted locally, `openssl pkcs12 -nokeys` / `-nocerts -nodes` and
    # `openssl pkey` extracted a PEM pair from it, and the REAL broker booted a TLS
    # listener on the result (bag-attribute preamble and all; rustls skips it). Its two
    # `keytool` steps were NOT run: the machine this converter was authored on has no
    # Java runtime, and no HiveMQ-generated keystore was available. They are transcribed
    # from the JDK's documented interface, so treat them as instructions to check, not
    # as commands this repository has executed.
    out.append("# --- TLS ---")
    out.append("#")
    out.append("# TODO(migrate): HiveMQ's TLS material is a JAVA KEYSTORE (JKS/PKCS#12)")
    out.append("# with passwords; mqttd reads PEM FILES BY PATH and has no keystore")
    out.append("# reader. Nothing here can be converted automatically — and this tool")
    out.append("# never touches key material (secrets are never transformed). Extract it")
    out.append("# yourself; these commands do the whole job.")
    out.append("#")
    out.append("# HOW FAR THIS RECIPE IS VERIFIED: the openssl steps were RUN and the")
    out.append("# real broker booted a TLS listener on their output. The two `keytool`")
    out.append("# steps were NOT run (no Java runtime, and no HiveMQ-generated keystore")
    out.append("# was available) — check them against your JDK.")
    out.append("#")
    # EVERY NUMBERED STEP IS EMITTED ON EVERY PATH, because the [tls] table below references
    # steps 1, 2 and 3 by number and a reference to a step that was not printed is a hole the
    # operator cannot climb out of. Round 2 found exactly that: with REQUIRED mTLS and no
    # <truststore>, `client_ca = …  # TODO(migrate): step 2` was emitted plus a NOTE calling
    # the path "a placeholder until you run step 2", while the step-2 block lived inside the
    # truststore loop — so the recipe ran 1 -> 3 and step 2 did not exist. The broker then
    # refuses to start on the unreadable placeholder, and the cheapest way out of a start
    # failure is to comment out the line you have no recipe for, which silently drops a live
    # mTLS mandate. So each step has a "nothing was configured" arm rather than vanishing.
    if ks:
        out.append(comment_safe(f"#   # 1. server key + chain, from {ks}"))
        out.append(comment_safe(f"#   keytool -importkeystore -srckeystore {shell_arg(ks)} \\"))
        out.append("#       -srcstoretype JKS -destkeystore server.p12 -deststoretype PKCS12")
        out.append("#   openssl pkcs12 -in server.p12 -nokeys  -out server.crt -legacy")
        out.append("#   openssl pkcs12 -in server.p12 -nocerts -nodes -out server.key -legacy")
        out.append("#   # (drop -legacy on OpenSSL 1.x; keep it for a JKS written by an")
        out.append("#   #  older JDK, whose PBE algorithms OpenSSL 3 moved to the legacy")
        out.append("#   #  provider — the symptom without it is 'unsupported' at load)")
    else:
        out.append("#   # 1. server key + chain — NO <keystore> path was found in the config,")
        out.append("#   #    so this converter cannot name the file. HiveMQ cannot serve TLS")
        out.append("#   #    without one, so find it (conf/hivemq.jks, typically) and put it")
        out.append("#   #    where <SRC> is:")
        out.append("#   keytool -importkeystore -srckeystore <SRC> \\")
        out.append("#       -srcstoretype JKS -destkeystore server.p12 -deststoretype PKCS12")
        out.append("#   openssl pkcs12 -in server.p12 -nokeys  -out server.crt -legacy")
        out.append("#   openssl pkcs12 -in server.p12 -nocerts -nodes -out server.key -legacy")
    if truststores:
        for i, store in enumerate(dict.fromkeys(truststores)):
            # EVERY truststore, not just the first listener's: they are the anchors behind
            # whatever client_ca ends up being, and one of them going missing is how an mTLS
            # mandate quietly stops being enforceable.
            out.append(
                comment_safe(
                    f"#   # 2. the client-CA trust anchors, from {store}"
                    + (" (concatenate every one of these into client-ca.crt)" if i else "")
                )
            )
            out.append(
                comment_safe(
                    f"#   keytool -list -rfc -keystore {shell_arg(store)} "
                    + (">> client-ca.crt" if i else "> client-ca.crt")
                )
            )
    else:
        out.append("#   # 2. the client-CA trust anchors — NO <truststore> was configured on")
        out.append("#   #    ANY TLS listener, so this converter has no file to point at.")
        out.append("#   #    HiveMQ falls back to the JVM's own default trust store when")
        out.append("#   #    <truststore> is absent, which means the anchors it verified")
        out.append("#   #    client certificates against are the JDK's cacerts plus whatever")
        out.append("#   #    was imported into it — NOT a file this config names. Find the")
        out.append("#   #    real anchors (the CA that issued your CLIENT certificates, which")
        out.append("#   #    is almost never a public root) and export them yourself:")
        out.append("#   keytool -list -rfc -keystore <THE-CLIENT-CA-STORE> > client-ca.crt")
        out.append("#   #    or, if they are already PEM, concatenate them into client-ca.crt.")
        out.append("#   #    Do NOT reuse the SERVER keystore's CA unless it really is the")
        out.append("#   #    client issuer — trusting the wrong anchor set admits the wrong")
        out.append("#   #    clients.")
    out.append("#   # 3. the extracted key must be an UNENCRYPTED PKCS#8 PEM:")
    out.append("#   openssl pkey -in server.key -out server.key.pem")
    out.append("#   # 4. then point [tls] at the results and re-run --check-config.")
    out.append("#")
    out.append("[tls]")
    # mqttd reads PEM FILES BY PATH and HiveMQ has a JAVA KEYSTORE, so these two paths are
    # necessarily ones the OPERATOR will create by running step 1 of the recipe above — they
    # are not values from the input, and the line says so rather than implying otherwise. The
    # provenance is the keystore element they must be extracted FROM, which is what makes the
    # claim checkable: with no <keystore> anywhere there is nothing to derive them from, and
    # they come out commented.
    ks_source = (
        f"{who(first)}/tls/keystore/path = {ks}" if ks else None
    )
    ks_defaulted = (
        "the PEM paths themselves — mqttd cannot read a JAVA KEYSTORE, so step 1 of the "
        "recipe above has to WRITE these two files; nothing in config.xml names them"
    )
    out.extend(
        conv.prov.line(
            "cert",
            toml_str("/etc/mqttd/tls/server.crt"),
            ks_source,
            defaulted=ks_defaulted,
            decide="a TLS listener was configured but NO <keystore><path> was found on any of "
            "them, so this converter cannot even name the file the server certificate must be "
            "extracted from. HiveMQ cannot serve TLS without a keystore, so check you passed "
            "the real config.xml; then run step 1 above against the keystore you find and "
            "uncomment these two lines",
        )
    )
    out.extend(
        conv.prov.line(
            "key",
            toml_str("/etc/mqttd/tls/server.key"),
            ks_source,
            defaulted=ks_defaulted,
            decide="no <keystore><path> was found on any TLS listener, so there is nothing to "
            "extract a private key from — see the cert TODO above",
        )
    )

    # client-authentication-mode across EVERY TLS listener. mqttd's client_ca mandates
    # mTLS for tls_bind, wss_bind and quic_bind AT ONCE, so only a unanimous REQUIRED is a
    # mapping; a mixture is a posture change in one direction or the other, and the
    # precedent for that (#162) is a COMMENTED-OUT candidate plus a TODO, never a guess.
    if required and not lax:
        out.extend(
            conv.prov.line(
                "client_ca",
                toml_str("/etc/mqttd/tls/client-ca.crt"),
                "client-authentication-mode = REQUIRED on "
                + "; ".join(who(l) for l in required)
                + (f", with truststore {ts}" if ts else ", with NO <truststore>"),
                defaulted="the PEM path itself — step 2 of the recipe above has to WRITE this "
                "file from the trust anchors; nothing in config.xml names a PEM",
            )
        )
        conv.note(
            "client-authentication-mode was REQUIRED on every TLS listener ("
            + "; ".join(who(l) for l in required)
            + "), so mTLS is mandatory and [tls] client_ca is set (path a placeholder until "
            "you run step 2). mqttd additionally requires the clientAuth extended key usage "
            "on every client certificate and refuses one without it at the handshake — "
            "OpenSSL-based brokers tolerated EKU-less device certs for years, so audit the "
            "fleet BEFORE cutover: scripts/migrate/cert-audit.sh <dir-of-client-certs>"
        )
        if not truststores:
            # The mandate is real and the anchors are NOT in this config. Saying so is the
            # difference between an operator finding the right CA and inventing one from the
            # server keystore — which trusts the wrong set and admits the wrong clients.
            conv.todo(
                "client certificates were REQUIRED but NO <truststore> was configured on any "
                "TLS listener, so this config does not name the trust anchors HiveMQ was "
                "verifying them against: with <truststore> absent HiveMQ falls back to the "
                "JVM's DEFAULT trust store (the JDK's cacerts plus whatever was imported into "
                "it). [tls] client_ca below is therefore a PLACEHOLDER PATH, not a translated "
                "value, and the broker REFUSES TO START until it points at a real PEM — do "
                "not resolve that by commenting the line out, because that drops a live mTLS "
                "mandate. Step 2 of the extraction recipe in the `# --- TLS ---` block BELOW "
                "says how to export the anchors (this TODO is in the header block; the "
                "recipe is at the bottom of the file, beside the [tls] table). Find the "
                "CA that issued your CLIENT certificates (it is almost never a public root, "
                "and it is almost never the server keystore's CA)"
            )
    elif required and lax:
        # THE fail-open case round 1 caught: a REQUIRED listener that was not first in
        # document order lost its mandate in silence.
        conv.todo(
            "TLS listeners DISAGREE about client certificates, and mqttd cannot hold both "
            "postures: client-authentication-mode was REQUIRED on "
            + "; ".join(who(l) for l in required)
            + " but "
            + "; ".join(f"{who(l)} was {mode_of(l)}" for l in lax)
            + ". [tls] client_ca MANDATES mTLS for tls_bind, wss_bind and quic_bind AT ONCE "
            "— setting it newly demands a certificate from clients that never presented one "
            "(a browser over wss, typically), and leaving it unset DROPS a mandate you have "
            "today. Neither is a translation, so client_ca is emitted COMMENTED OUT below: "
            "uncomment it to mandate mTLS fleet-wide (audit every client first with "
            "scripts/migrate/cert-audit.sh), or leave it commented and move the "
            "mTLS-required listener's clients to a SEPARATE deployment that sets it. Do NOT "
            "deploy this file believing the REQUIRED listener kept its mandate"
        )
        out.append(
            comment_safe(
                "# TODO(migrate): client certificates were REQUIRED on "
                + "; ".join(who(l) for l in required)
                + " but not on "
                + "; ".join(who(l) for l in lax)
                + "; mqttd has ONE posture for every TLS transport. Uncommenting mandates "
                "mTLS EVERYWHERE (see the TODO above):"
            )
        )
        out.extend(
            conv.prov.inert(
                "client_ca",
                toml_str("/etc/mqttd/tls/client-ca.crt"),
                "PLACEHOLDER written by step 2 of the recipe above, from the anchors behind "
                + ("; ".join(who(l) for l in required))
                + "'s REQUIRED client-authentication-mode — NOT activated, because the other "
                "TLS listeners did not require certificates",
            )
        )
    elif optional:
        out.append(
            comment_safe(
                "# TODO(migrate): client-authentication-mode was OPTIONAL on "
                + "; ".join(who(l) for l in optional)
                + " — a client COULD present a certificate and was verified if it did. "
                "mqttd's client_ca MANDATES mTLS; there is no cert-optional mode, so this "
                "cannot be mapped without changing your security posture. Uncomment to "
                "require certificates fleet-wide (audit them first with "
                "scripts/migrate/cert-audit.sh), or leave it commented for server-only TLS "
                "and give the certificate-bearing clients another credential:"
            )
        )
        out.extend(
            conv.prov.inert(
                "client_ca",
                toml_str("/etc/mqttd/tls/client-ca.crt"),
                "PLACEHOLDER written by step 2 of the recipe above, from the anchors behind "
                + ("; ".join(who(l) for l in optional))
                + "'s OPTIONAL client-authentication-mode — NOT activated, because mqttd's "
                "client_ca MANDATES certificates and OPTIONAL did not",
            )
        )
    elif ts:
        out.append(
            comment_safe(
                "# TODO(migrate): a truststore was configured ("
                + "; ".join(
                    f"{who(l)}: {l.tls['truststore-path']}"
                    for l in tls_listeners
                    if l.tls.get("truststore-path")
                )
                + ") but client-authentication-mode was "
                + "/".join(sorted({mode_of(l) for l in tls_listeners}))
                + ", so client certificates were not verified. mqttd's client_ca MANDATES "
                "mTLS — left commented so the posture does not change:"
            )
        )
        out.extend(
            conv.prov.inert(
                "client_ca",
                toml_str("/etc/mqttd/tls/client-ca.crt"),
                "PLACEHOLDER written by step 2 of the recipe above, from the configured "
                "truststore — NOT activated, because client-authentication-mode never "
                "required a certificate",
            )
        )
    for lst in tls_listeners:
        protocols = lst.tls.get("protocols", "")
        if not protocols:
            continue
        listed = [p.strip() for p in protocols.split(",") if p.strip()]
        if any("1.2" in p or "1.1" in p or "1.0" in p for p in listed):
            conv.todo(
                f"{who(lst)} accepted {listed}. mqttd is TLS 1.3 ONLY by default and a "
                "1.2-only client fails to connect in a way that looks like a network "
                "fault, not a policy one. If your fleet needs it, opt in with [tls] "
                "allow_tls12 = true — hardened (ECDHE+AEAD only, Extended Master Secret "
                "required), loudly logged on every start, and applied to EVERY TLS "
                "transport — and plan its retirement. TLS 1.0/1.1 are not available at all"
            )
        elif not any("1.3" in p for p in listed):
            conv.todo(
                f"{who(lst)} listed {listed}, which does not include TLS 1.3 — the only "
                "version mqttd offers by default. Every client on that transport must "
                "negotiate 1.3, or [tls] allow_tls12 must be set explicitly"
            )
    return out


# ---------------------------------------------------------------------------
# The File RBAC extension's credentials.xml -> mqttd ACL rules.
# ---------------------------------------------------------------------------

ACTIVITY = {
    "PUBLISH": ["publish"],
    "SUBSCRIBE": ["subscribe"],
    "ALL": ["publish", "subscribe"],
}

# The `default` this converter writes into every ACL it produces. HiveMQ's File RBAC has NO
# `no_match` analogue — its permissions are an allow list checked in file order, so
# deny-by-default carries over exactly — which makes this a CONSTANT here, unlike
# from-emqx.py where `authorization.no_match` can flip it.
#
# It is still routed through empty_policy_effect() rather than written into each sentence,
# because the 2026-08-14 round found both of the EMQX converter's zero-rule TODOs asserting
# "fail-closed … default = deny" as fixed prose while the renderer wrote a variable — so a
# wide-open policy could carry a comment saying it denied everything. A sentence about what
# the output WILL DO is derived from the value being emitted, even when that value happens to
# be constant today; the property sweep asserts the invariant over both converters.
ACL_DEFAULT = "deny"


def _upper_first(text: str) -> str:
    """Capitalise the FIRST character only.

    `str.capitalize()` lowercases everything after it, which flattens the emphasis capitals
    these sentences carry deliberately (`PERMITS EVERY publish and subscribe`).
    """
    return text[:1].upper() + text[1:]


def empty_policy_effect(default: str) -> str:
    """What a rule-less policy DOES, derived from the `default` that will be written."""
    if default == "allow":
        return (
            'this policy\'s `default = "allow"` PERMITS EVERY publish and subscribe by every '
            "authenticated client — a wide open policy, not a migrated one"
        )
    return (
        'this policy\'s `default = "deny"` denies every publish and subscribe. That is '
        "fail-closed, not migrated"
    )


@dataclass
class Permission:
    topic: str
    activity: str = "ALL"
    qos: str = "ALL"
    retain: str = "ALL"
    shared: str = "ALL"
    shared_group: str = "#"
    unknown: list[str] = field(default_factory=list)


def parse_credentials(
    text: str,
) -> tuple[list[dict], list[str], list[tuple[str, bool]]]:
    """Flatten file-RBAC users x roles x permissions into mqttd rules.

    Returns (rules, todos, usernames). Roles CANNOT become mqttd `groups`: the
    password-file authenticator yields an empty group list (verified in
    crates/mqtt-auth/src/password.rs), and only OIDC/HTTP auth populate groups. So the
    role's permissions are duplicated onto each member's `identities`.
    """
    todos: list[str] = []
    rules: list[dict] = []
    try:
        root = ET.fromstring(text)
    except ET.ParseError as e:
        return [], [f"credentials file is not parseable XML ({e}) — translate by hand"], []

    # UNKNOWN-ELEMENT HANDLING, as strong as the config walk's.
    #
    # Round 1 found this the weakest surface in the tool: the config walk reports every
    # unknown element by path, but here only an unknown tag INSIDE a <permission> was
    # reported — so an `<enabled>false</enabled>` on a user and an entire unknown policy
    # section passed in silence, on the file that IS the authorization policy. The file-RBAC
    # extension is versioned independently of this converter's 4.6.16 reference, so a newer
    # or vendor-extended element is exactly the input that needs a TODO.
    if root.tag != "file-rbac":
        todos.append(
            f"the credentials document root is <{root.tag}>, not <file-rbac>. This may not "
            "be a File RBAC credentials.xml at all (the extension's own "
            "extension-config.xml? an ese configuration?). It was read anyway, on a "
            "best-effort basis — check that the rules below are the policy you meant"
        )
    for child in root:
        if child.tag not in ("users", "roles"):
            nested = sum(1 for _ in child.iter()) - 1
            todos.append(
                f"UNKNOWN top-level element <{child.tag}> ({nested} nested element(s)) in "
                "the credentials file. This converter's reference is the file-RBAC "
                "extension at 4.6.16, which has <users> and <roles> only, so nothing in it "
                "was translated. If your extension version gives it meaning — a deny list, "
                "a default role, a policy switch — the rules below do NOT reflect it"
            )

    roles: dict[str, list[Permission]] = {}
    if root.find("./roles") is None:
        todos.append(
            "no <roles> section was found, so NO rule could be written and "
            + empty_policy_effect(ACL_DEFAULT)
            + ". If this file really has no roles, your file-RBAC users were authenticated "
            "and authorized nothing; if it should have them, you are looking at the wrong file"
        )
    for role in root.findall("./roles/role"):
        rid = text_of(role.find("id")) if role.find("id") is not None else ""
        for child in role:
            if child.tag not in ("id", "permissions"):
                todos.append(
                    f"role {rid!r} carries an UNKNOWN <{child.tag}> element "
                    f"({text_of(child)!r}), which this converter's 4.6.16 reference does "
                    "not have. It was NOT translated — check what it constrained before "
                    "trusting this role's rules"
                )
        perms: list[Permission] = []
        for pel in role.findall("./permissions/permission"):
            perm = Permission(topic="")
            for child in pel:
                tag, val = child.tag, text_of(child)
                if tag == "topic":
                    perm.topic = val
                elif tag == "activity":
                    perm.activity = val.upper()
                elif tag == "qos":
                    perm.qos = val.upper()
                elif tag == "retain":
                    perm.retain = val.upper()
                elif tag == "shared-subscription":
                    perm.shared = val.upper()
                elif tag == "shared-group":
                    perm.shared_group = val
                else:
                    perm.unknown.append(tag)
            perms.append(perm)
        roles[rid] = perms
        if not perms:
            todos.append(
                f"role {rid!r} has NO permissions. In file-RBAC that grants nothing, and "
                "mqttd is deny-by-default, so its members get nothing here either — "
                "which is faithful. Check it was intended"
            )

    usernames: list[tuple[str, bool]] = []
    if root.find("./users") is None:
        todos.append(
            "no <users> section was found, so NO rule was written for anyone. If you passed "
            "the extension's extension-config.xml (or any other file), pass the real "
            "credentials.xml instead — with no rules, "
            + empty_policy_effect(ACL_DEFAULT)
        )
    for user in root.findall("./users/user"):
        name = text_of(user.find("name")) if user.find("name") is not None else ""
        if not name:
            todos.append("a <user> had no <name>; skipped and reported rather than guessed")
            continue
        # An <enabled>false</enabled> user is a DISABLED thing, and emitting live allow rules
        # for them is the fail-open direction. Round 1 fixed this class on EMQX's
        # authenticators, authz sources and bridges; round 2 found it on EMQX's listeners; the
        # sweep found it here, where the previous fix reported the flag and then said in the
        # same breath that "their rules WERE still emitted below". Under the contract a
        # mapping that changes SECURITY POSTURE is not a mapping: the rules are emitted
        # COMMENTED OUT with a TODO instead of being switched on.
        disabled = False
        for child in user:
            if child.tag in ("name", "password", "roles"):
                continue
            if child.tag == "enabled":
                if not text_of(child).strip().lower() in ("true", "yes", "1"):
                    disabled = True
                    todos.append(
                        f"user {name!r} carries <enabled>{text_of(child)}</enabled>: that "
                        "user was SWITCHED OFF, so their rules were NOT emitted as live "
                        "policy — they appear COMMENTED OUT below and their re-enrolment "
                        "command is commented too. Switching a disabled account back on is a "
                        "posture change, not a translation, so it is not done silently. "
                        "Uncomment both if the user should be live. (This converter's "
                        "file-RBAC 4.6.16 reference does not define <enabled>, so a newer or "
                        "vendor-extended extension is what gave it meaning — check that it "
                        "means what you think before uncommenting.)"
                    )
                else:
                    todos.append(
                        f"user {name!r} carries <enabled>{text_of(child)}</enabled>, which "
                        "this converter's file-RBAC 4.6.16 reference does not define. The "
                        "value reads as ENABLED, so their rules were emitted normally — "
                        "check what your extension version does with the element"
                    )
                continue
            todos.append(
                f"user {name!r} carries an UNKNOWN <{child.tag}> element "
                f"({text_of(child)!r}); this converter's 4.6.16 reference has <name>, "
                "<password> and <roles> only. It was NOT translated — check what it did "
                "before trusting this user's rules"
            )
        usernames.append((name, disabled))
        if any(c in name for c in "#+"):
            todos.append(
                f"username {name!r} contains # or +. The file-RBAC extension PROHIBITS "
                "those and denies the connection; mqttd's %i/%c substitutions also FAIL "
                "CLOSED on them. The two agree, so this user was already broken — fix the "
                "name, do not carry it over"
            )
        role_ids = [text_of(r) for r in user.findall("./roles/id") if text_of(r)]
        for holder in user.findall("./roles"):
            for child in holder:
                if child.tag != "id":
                    todos.append(
                        f"user {name!r} has an UNKNOWN <{child.tag}> element inside "
                        f"<roles> ({text_of(child)!r}); only <id> is read, so this granted "
                        "nothing here. Check whether it named a role"
                    )
        if not role_ids:
            todos.append(
                f"user {name!r} has no roles, so no rule was written for them. Under "
                "mqttd's deny-by-default they can connect (with a password) and do "
                "nothing — faithful to file-RBAC, but check it was intended"
            )
        for rid in role_ids:
            perms = roles.get(rid)
            if perms is None:
                todos.append(
                    f"user {name!r} references role {rid!r}, which is NOT DEFINED in this "
                    "file. No rule was written for it. Find the role (a second credentials "
                    "file? a stale reference?) — this is a silent grant or a silent gap in "
                    "the original too"
                )
                continue
            for perm in perms:
                where = f"role {rid!r} permission topic={perm.topic!r} (user {name!r})"
                if not perm.topic:
                    todos.append(f"{where}: no <topic>; nothing was emitted")
                    continue
                actions = ACTIVITY.get(perm.activity)
                if actions is None:
                    todos.append(
                        f"{where}: activity {perm.activity!r} is not PUBLISH, SUBSCRIBE or "
                        "ALL, so nothing was emitted. Translate it by hand"
                    )
                    continue
                # A LITERAL %c OR %i, checked BEFORE the ${{...}} rewrite so a %c this
                # loop itself produces from ${{clientid}} is never confused with one the
                # source carried. The file-RBAC extension substitutes only ${{clientid}}
                # and ${{username}} (its 4.6.16 documentation — the same reference every
                # other claim in this converter cites), so a %c or %i in a <topic>
                # matched those BYTES literally. mqttd substitutes %c (client id) and %i
                # (identity) in EVERY rule's topics with no escape
                # (crates/mqtt-auth/src/acl.rs), so carrying it across would turn a rule
                # on one literal topic into a live per-client grant the source never gave
                # — the same misread the Mosquitto converter refuses on a plain `topic`
                # line, and until 2026-08-16 this converter emitted it with only a
                # fail-closed caveat beside it. Refused instead. Found via issue #297.
                literal = [t for t in ("%c", "%i") if t in perm.topic]
                if literal:
                    todos.append(
                        f"{where}: the topic contains "
                        + " and ".join(literal)
                        + " LITERALLY — file-RBAC substitutes only ${{clientid}} and "
                        "${{username}} (4.6.16 reference), while mqttd substitutes %c "
                        "(client id) and %i (identity) in EVERY rule's topics with no "
                        "escape (crates/mqtt-auth/src/acl.rs). Carrying it over would "
                        "turn a rule on one literal topic into a live per-client grant "
                        "the source never gave, so NO RULE WAS WRITTEN for it. If a "
                        "per-client namespace IS what you want, write it as an mqttd "
                        "rule deliberately; if the topic really is literal, rename it"
                    )
                    continue
                topic = perm.topic.replace("${{clientid}}", "%c").replace(
                    "${{username}}", "%i"
                )
                if "${{" in topic:
                    todos.append(
                        f"{where}: uses a substitution this converter does not know, so "
                        "the rule was NOT emitted. mqttd supports %i (identity) and %c "
                        "(client id) only"
                    )
                    continue
                if "%i" in topic or "%c" in topic:
                    todos.append(
                        f"{where}: mqttd's %i/%c substitutions FAIL CLOSED when the value "
                        "is empty or contains / + or # — a client whose id or username "
                        "holds one of those matches NOTHING through this rule"
                    )
                if perm.qos != "ALL":
                    todos.append(
                        f"{where}: the permission applied only at QoS {perm.qos}. mqttd "
                        "rules carry NO QoS qualifier, so the rule was emitted covering "
                        "EVERY QoS — BROADER than the original. This is the dangerous "
                        "direction; review it"
                    )
                if perm.retain != "ALL":
                    todos.append(
                        f"{where}: the permission applied only to {perm.retain} messages. "
                        "mqttd rules carry NO retain qualifier, so the rule was emitted "
                        "covering both retained and non-retained publishes — BROADER than "
                        "the original. Review it"
                    )
                if perm.shared != "ALL" or perm.shared_group != "#":
                    todos.append(
                        f"{where}: the permission was scoped to shared subscriptions "
                        f"(shared-subscription={perm.shared}, "
                        f"shared-group={perm.shared_group!r}). mqttd's ACL cannot "
                        "distinguish a shared subscription from a plain one, so the rule "
                        "was emitted covering both. Review it"
                    )
                for tag in perm.unknown:
                    todos.append(
                        f"{where}: unknown <{tag}> element in the permission — reported "
                        "rather than ignored; check what it constrained"
                    )
                if "*" in name:
                    # mqttd's `identities` are GLOBS with NO escape
                    # (crates/mqtt-auth/src/acl.rs glob_match) and file-RBAC matched
                    # <name> EXACTLY, so emitting this would grant the permission to every
                    # identity matching the pattern. Refused, not widened. Found 2026-08-15.
                    todos.append(
                        f"{where}: the user name {name!r} contains a LITERAL `*`. mqttd's rule "
                        "`identities` are GLOBS — `*` matches any run of characters and there "
                        "is NO way to escape it (crates/mqtt-auth/src/acl.rs) — while HiveMQ's "
                        "file-RBAC matched <name> EXACTLY, so emitting this rule would grant it "
                        "to every identity matching the pattern. NO RULE WAS WRITTEN for it: "
                        "rename the user, or add a rule by hand naming each identity you mean"
                    )
                    continue
                rules.append(
                    {
                        "identities": [name],
                        "actions": actions,
                        "effect": "allow",
                        "topics": [topic],
                        "source": f"role {rid} -> user {name}",
                        "disabled": disabled,
                    }
                )
    if roles:
        todos.insert(
            0,
            "ROLES WERE FLATTENED, on purpose. mqttd has a `groups` matcher, but groups "
            "are populated ONLY by an OIDC token's groups_claim or the HTTP auth hook's "
            '{"groups":[...]} response — the Argon2id password file always yields an empty '
            "group list (crates/mqtt-auth/src/password.rs). So each role's permissions "
            "were duplicated onto every member's `identities`: correct, but the file grows "
            "with users x permissions and a role change now means re-running this. If that "
            "matters, move authentication to OIDC or the HTTP hook and rewrite these as "
            "`groups = [...]` rules",
        )
    if not any(not r["disabled"] for r in rules):
        todos.insert(
            0,
            "NO RULE was written into this file. Every user/role/permission either landed "
            "on a gap listed below or was absent, so this policy grants NOTHING and "
            + empty_policy_effect(ACL_DEFAULT)
            + ". Read every TODO below and check you passed the real credentials.xml"
        )
    todos.append(
        "PASSWORDS WERE NOT READ, NOT COPIED AND NOT CONVERTED. file-RBAC hashes are "
        "salt:iterations:hash (PBKDF2-family), not Argon2id, and a hash cannot be "
        "converted into another scheme — the passwords are not recoverable from it. Every "
        "user must be re-enrolled from their password; the commands are listed below"
    )
    return rules, todos, usernames


def render_acl(
    rules: list[dict],
    todos: list[str],
    usernames: list[tuple[str, bool]],
    default: str = ACL_DEFAULT,
    prov: Provenance | None = None,
) -> str:
    prov = prov if prov is not None else Provenance()
    out = [
        "# Translated from a HiveMQ File RBAC credentials.xml by the mqttd HiveMQ",
        "# converter (scripts/migrate/from-hivemq.py). " + VERSIONS + ".",
        "#",
        *DRAFT_HEADER,
        "#",
        "# file-RBAC permissions are an ALLOW LIST checked in file order, so deny-by-",
        "# default carries over exactly. What does NOT carry over is every qualifier on a",
        "# permission (qos, retain, shared-subscription): an mqttd rule matches publish or",
        "# subscribe on a topic filter, full stop. Each dropped qualifier is a TODO below,",
        "# and each one makes its rule BROADER than the original.",
        "#",
        # DERIVED from the value being written, never asserted — the same
        # empty_policy_effect() the zero-rule TODOs use, so a sentence about this policy and
        # the policy itself cannot disagree.
        comment_safe("# " + _upper_first(empty_policy_effect(default)) + ""),
        "#",
        "# It is enforced ONLY while [security] acl_file in the generated config names this",
        "# file: with acl_file unset mqttd enforces NO authorization at all and says so in",
        "# the log on every start.",
        "",
        *prov.line(
            "default",
            toml_str(default),
            "HiveMQ file-RBAC permissions are an ALLOW LIST checked in file order, with no "
            "`no_match` analogue, so deny-by-default carries over exactly",
        ),
        "",
    ]
    for t in todos:
        out.append(f"# TODO(migrate): {comment_safe(t)}")
    if todos:
        out.append("")
    # The re-enrolment block is emitted UNCONDITIONALLY, because [security] password_file in
    # the generated config points at the file these commands create: with no block at all the
    # config referenced a step the output never printed (the same dangling-cross-reference
    # class as the TLS recipe's missing step 2).
    out.append("# Re-enrol each user against the mqttd password file — run these with")
    out.append("# the real passwords, then set [security] password_file to the result.")
    out.append("# file-RBAC hashes cannot be converted, so this is the only way in:")
    if usernames:
        for name, disabled in usernames:
            # shell_arg, because a domain-style `CORP\jdoe` pasted unquoted into a shell
            # is a different username (the backslash escapes the j).
            line = (
                f"#   printf %s '<password>' | mqttd --hash-password {shell_arg(name)} "
                ">> /etc/mqttd/passwd"
            )
            if disabled:
                out.append(
                    comment_safe(
                        f"#   # {name} was <enabled>false</enabled> — left commented so a "
                        "switched-off account is not re-created by accident:"
                    )
                )
                out.append(comment_safe("#   # " + line[4:]))
            else:
                out.append(comment_safe(line))
    else:
        out.append("#   # NO user was found in the credentials file, so there is nobody to")
        out.append("#   # enrol and the password_file the config names would be EMPTY — with")
        out.append("#   # an empty file no client can authenticate at all. Check you passed")
        out.append("#   # the real credentials.xml.")
    out.append("")
    for r in rules:
        out.append(f"# from: {comment_safe(r['source'])}")
        if r["disabled"]:
            # A DISABLED user's grant, emitted as a commented candidate rather than live
            # policy: switching a switched-off account back on is a posture change.
            out.append(
                comment_safe(
                    "# TODO(migrate): this user was <enabled>false</enabled> in file-RBAC, so "
                    "the rule is COMMENTED OUT. Uncomment all four lines to grant it:"
                )
            )
            out.append("# [[rules]]")
            out.append(f"# identities = {toml_list(r['identities'])}")
            out.append(f"# actions = {toml_list(r['actions'])}")
            out.append(f"# effect = {toml_str(r['effect'])}")
            out.append(f"# topics = {toml_list(r['topics'])}")
            out.append("")
            continue
        out.append("[[rules]]")
        out.append(f"identities = {toml_list(r['identities'])}")
        out.append(f"actions = {toml_list(r['actions'])}")
        out.append(f"effect = {toml_str(r['effect'])}")
        out.append(f"topics = {toml_list(r['topics'])}")
        out.append("")
    return "\n".join(out) + "\n"


def render_config(conv: Conversion, listener_extras: list[str], tls_lines: list[str]) -> str:
    out = [
        "# Translated from a HiveMQ config.xml by the mqttd HiveMQ converter",
        "# (scripts/migrate/from-hivemq.py). " + VERSIONS + ".",
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
        if section == "listeners":
            out.extend(listener_extras)
        out.append("")
    if tls_lines:
        out.extend(tls_lines)
        out.append("")
    return "\n".join(out) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__.split("\n", maxsplit=1)[0],
        epilog=(
            "PROVENANCE: " + VERSIONS + ". " + SCOPE + " " + DRAFT + " HiveMQ Community "
            "Edition has NO built-in authentication or authorization — both come from "
            "extensions, so a CE config alone describes an ANONYMOUS broker. Pass "
            "--credentials for the File RBAC extension."
        ),
    )
    ap.add_argument("conf", type=Path, help="path to HiveMQ config.xml")
    ap.add_argument("--out-config", type=Path, help="write the mqttd TOML here")
    ap.add_argument("--out-acl", type=Path, help="write the translated ACL here")
    ap.add_argument(
        "--credentials", type=Path, help="the File RBAC extension's credentials.xml"
    )
    ap.add_argument(
        "--provenance-json",
        type=Path,
        help="write the provenance ledger (every security-relevant value, its HiveMQ source "
        "element and whether it was emitted live) here, for "
        "scripts/migrate/property_sweep.py",
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
    try:
        root = ET.fromstring(text)
    except ET.ParseError as e:
        # Not fatal: report it and still emit a config skeleton with the gap named, so
        # the operator sees what happened in the file they are about to deploy.
        root = None
        conv.todo(
            f"the input is NOT parseable XML ({e}). Nothing below was derived from your "
            "deployment — fix the file and re-run. HiveMQ itself would refuse to start on "
            "it too"
        )
    if root is not None:
        if root.tag != "hivemq":
            conv.todo(
                f"the document root is <{root.tag}>, not <hivemq>. This may not be a "
                "HiveMQ broker config at all (an extension-config.xml? an ese "
                "configuration?). Everything below was read anyway, on a best-effort basis"
            )
        walk(root, conv)

    listener_extras = build_listeners(conv)
    tls_lines = build_tls(conv)

    if not conv.config.get("listeners"):
        conv.todo(
            "NO listener was found, so mqttd would bind nothing and serve no clients. Set "
            "[listeners] tls_bind (and [tls] cert/key) at minimum"
        )
    conv.set("node", "id", toml_str("node-1"))
    conv.note(
        "[node] id was not derived from anything — HiveMQ CE has no node identity in its "
        "config. It must be UNIQUE per node in a cluster and equal that node's "
        "cluster-bus certificate CN (docs/SECURED-CLUSTER-TUTORIAL.md)"
    )
    conv.set("node", "data_dir", toml_str("/var/lib/mqttd"))
    conv.note(
        "[node] data_dir was set to mqttd's packaged default /var/lib/mqttd. Durable "
        "sessions are ON by default and REFUSE to start without a data dir, so this value "
        "is what makes the config valid — mount a real volume there, or the durable state "
        "lives on the container's ephemeral layer"
    )
    # THE CREDENTIALS FILE IS READ HERE, BEFORE THE CONFIG IS RENDERED, on purpose: when it
    # cannot be read, the gap belongs in the files the operator is about to DEPLOY, not in a
    # stderr line they scroll past (round 1 found the config still pointing acl_file at a
    # policy that was never written).
    cred_text: str | None = None
    if args.credentials:
        try:
            cred_text = args.credentials.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as e:
            print(f"note: could not read {args.credentials}: {e}", file=sys.stderr)
            conv.todo(
                f"THE AUTHORIZATION POLICY WAS NOT TRANSLATED: {str(args.credentials)!r} "
                f"could not be read ({e}). The generated ACL says the same and contains NO "
                "rules, so it denies every publish and subscribe — fail-closed, not "
                "migrated. [security] acl_file below still names a policy file because mqttd "
                "REFUSES TO START without the file it names; fix the path and re-run before "
                "deploying"
            )
    if args.credentials:
        conv.set(
            "security",
            "password_file",
            toml_str("/etc/mqttd/passwd"),
            f"--credentials {args.credentials} (the USERS are from there; file-RBAC hashes "
            "cannot be converted, so the file this path names is the one the re-enrolment "
            "commands in the generated ACL create)",
            defaulted="the deployed path itself, which is yours to choose",
        )
        conv.set(
            "security",
            "acl_file",
            toml_str("/etc/mqttd/acl.toml"),
            f"--credentials {args.credentials} (the POLICY is from its <roles>; the path "
            "below is this converter's own --out-acl deployment default)",
            defaulted="the deployed path itself, which is yours to choose",
        )
    else:
        conv.todo(
            "no --credentials was given. HiveMQ Community Edition has NO built-in "
            "authentication and NO authorization — if this deployment ran without an auth "
            "extension, every client was ANONYMOUS and unauthorized. mqttd refuses "
            "anonymous clients by default ([security] allow_anonymous = false, below), so "
            "your clients will NOT connect until you give them credentials. That is the "
            "right end state; plan the credential rollout as part of the cutover"
        )
    conv.set(
        "security",
        "allow_anonymous",
        "false",
        "mqttd's own default. HiveMQ Community Edition has NO built-in authentication, so "
        "nothing in a CE config.xml grants or refuses anonymous access — this is the "
        "fail-CLOSED direction and it is written explicitly so the posture is visible",
        defaulted="the value itself — it is mqttd's default, not a HiveMQ setting",
    )

    config = render_config(conv, listener_extras, tls_lines)
    if args.out_config:
        args.out_config.write_text(config, encoding="utf-8")
        print(f"wrote {args.out_config}")
    else:
        print(config)

    if args.credentials:
        if cred_text is None:
            rules, todos, usernames = (
                [],
                [
                    "NOTHING WAS TRANSLATED INTO THIS FILE. The File RBAC credentials file "
                    f"{str(args.credentials)!r} could not be read, so this policy has NO "
                    "rules and " + empty_policy_effect(ACL_DEFAULT) + ": fix the path and "
                    "re-run"
                ],
                [],
            )
        else:
            rules, todos, usernames = parse_credentials(cred_text)
        acl = render_acl(rules, todos, usernames, ACL_DEFAULT, conv.prov)
        if args.out_acl:
            args.out_acl.write_text(acl, encoding="utf-8")
            print(f"wrote {args.out_acl} ({len(rules)} rules)")
        else:
            print(acl)

    if args.provenance_json:
        args.provenance_json.write_text(
            conv.prov.ledger("from-hivemq.py"), encoding="utf-8"
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
