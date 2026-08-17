#!/usr/bin/env python3
"""Translate an EMQX deployment into an mqttd configuration DRAFT (fixtures: emqx/emqx @ 6.2.2; a reviewed draft, never a validated migration).

Reads EMQX's HOCON configuration (`emqx.conf`, or the dashboard-managed
`data/configs/cluster.hocon`) and, with `--acl-file`, the Erlang-term `acl.conf`.
Emits an mqttd TOML config, an mqttd ACL policy, and — with `--out-bridge` — an
`mqtt-bridge` config for the MQTT bridges it can express.

## What it is: a DRAFT, where anything undecidable is INERT and named

It translates the settings that have an exact mqttd equivalent, and for
everything else it **says so in the output** rather than guessing. A converter
that silently drops a setting is worse than no converter: you would deploy
believing the policy came across.

Anything not translated is emitted as a `# TODO(migrate):` comment at the point
it belongs, so the gap is visible in the file you are about to deploy rather than
in a report you read once. EMQX has a great deal that mqttd deliberately does not
have — the SQL rule engine, data integration, gateways, exhook, plugins, zones,
the dashboard and REST API — and every one of those becomes a TODO naming what you
must decide, not an omission.

**Every security-relevant value goes through one gate.** Three adversarial review
rounds each fixed what they were shown and the count went up, because "every input
construct is either translated or marked TODO" is a total-coverage claim over a
foreign vendor's schema. But every serious finding had ONE shape: a live setting
the tool had not derived from the input — a listener EMQX had switched OFF
converted to a live bind, an mTLS mandate taken from the wrong listener, a bridge
that used TLS converted to a live PLAINTEXT upstream. So every `*_bind`, every path
under `[tls]`, `client_ca`, `acl_file`, `password_file`, every `[security.*]` value,
the ACL `default` and every bridge upstream `url` is now emitted through
`Provenance.line()` **together with the EMQX key it was derived from**, and that
gate refuses to write a live line without one: a value with no provenance comes out
COMMENTED OUT beside a TODO naming the decision. Every live security-relevant line
carries `# from: <the EMQX key>`.

## Provenance, and what was NOT verified

The mappings and the fixtures were built from EMQX's own shipped, documented
example configuration at tag **6.2.2** — `rel/config/examples/*.conf.example` and
`apps/emqx_auth/etc/acl.conf` — plus the documented HOCON shape of
`authentication` / `authorization`. **No live EMQX broker was run**, and no
ground-truth config produced by one was used. 5.x is expected to parse because the
parser is tolerant, not because it was tested. Treat the output as a draft to
review, not as a validated migration.

**If your auth is managed from the dashboard or the REST API**, EMQX persists it to
`data/configs/cluster.hocon`, *not* to `emqx.conf`. Pass that file too (this tool
accepts either), or the `authentication`/`authorization` blocks will be missing —
and silence there means "look in cluster.hocon", never "there was no auth".

## Usage

    scripts/migrate/from-emqx.py /etc/emqx/emqx.conf \\
        --acl-file /etc/emqx/acl.conf \\
        --out-config mqttd.toml --out-acl acl.toml --out-bridge bridge.toml

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
    "fixtures/mappings built from emqx/emqx @ 6.2.2 "
    "(rel/config/examples/*.conf.example, apps/emqx_auth/etc/acl.conf); "
    "no live EMQX broker was run"
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
    "TODO(migrate) naming the decision you have to make — so an unread construct can leave "
    "the output INCOMPLETE but can never leave a live security setting nobody derived. "
    "Every live security-relevant line carries `# from: <the EMQX key it came from>`. "
    "VERIFIED: fixtures diffed against pinned vendor sources; the provenance, "
    "no-live-without-source, drop, contradiction and validity invariants of "
    "scripts/migrate/property_sweep.py over generated and mechanically mutated inputs; and "
    "`mqttd --check-config` on every generated config plus the ACL loaded by the real "
    "broker. NOT VERIFIED: no live EMQX was EVER run against this converter, and NO claim "
    "of total coverage over EMQX's schema is made — a construct it has never seen is a "
    "construct it cannot report, and a construct whose MEANING it misreads is one it can "
    "still translate wrongly: the provenance gate proves a live value came from a named input "
    "key, NOT that the key means what this converter took it to mean. docs/MIGRATION.md's "
    "KNOWN GAPS section lists every misreading found so far."
)

# The condensed form that goes into the generated files themselves.
DRAFT_HEADER = [
    "# THIS IS A DRAFT, NOT A TRANSLATION. Anything this converter could not derive",
    "# from your input is COMMENTED OUT beside a TODO naming the decision, so this",
    "# file may be INCOMPLETE — but no live security setting in it was invented.",
    "# Every live security-relevant line carries `# from: <the EMQX key>`.",
    "# NOT VERIFIED: no live EMQX was ever run; no total-coverage claim over EMQX's",
    "# schema is made.",
]

# ---------------------------------------------------------------------------
# A tolerant HOCON reader.
#
# There is no HOCON parser in the standard library and adding a dependency would
# break this repository's zero-new-dependency posture for the migrate scripts, so
# this reads the subset EMQX actually writes: nested `{}` blocks, dotted keys,
# `key = value`, arrays, `#`/`//` comments, quoted and bare values. It is written
# to NEVER raise: anything it cannot understand is skipped and reported by the
# caller as a TODO, because a converter that crashes on an unexpected key is a
# converter nobody finishes a migration with.
# ---------------------------------------------------------------------------

_KEY_STOP = "=:{[}]\n,"


def strip_comments(text: str) -> str:
    """Remove `#`/`//` comments, respecting double-quoted strings."""
    out: list[str] = []
    i, n, in_str = 0, len(text), False
    while i < n:
        c = text[i]
        if in_str:
            out.append(c)
            if c == "\\" and i + 1 < n:
                out.append(text[i + 1])
                i += 2
                continue
            if c == '"':
                in_str = False
            i += 1
            continue
        if c == '"':
            in_str = True
            out.append(c)
            i += 1
            continue
        if c == "#" or (c == "/" and i + 1 < n and text[i + 1] == "/"):
            while i < n and text[i] != "\n":
                i += 1
            continue
        out.append(c)
        i += 1
    return "".join(out)


class HoconError(Exception):
    """The reader could not make progress. Caught in main(): exit 1 with the position."""


class Hocon:
    """A best-effort HOCON reader producing nested dicts / lists / strings."""

    def __init__(self, text: str) -> None:
        self.s = strip_comments(text)
        self.i = 0
        self.skipped: list[str] = []
        # Structures the input never closed. Reported as a TODO rather than swallowed: a
        # missing `]` means everything after it was read in the wrong scope.
        self.unterminated: list[str] = []

    def _guard(self, before: int, where: str) -> None:
        """Refuse to loop without consuming input.

        The one structural rule that makes a hang impossible rather than absent: every loop
        in this reader calls this with the position it started at, so a stop-character set
        that cannot be advanced past raises instead of spinning.
        """
        if self.i <= before:
            raise HoconError(
                f"the reader stopped making progress at byte {self.i} of {len(self.s)} "
                f"({where}), at {self.s[self.i : self.i + 40]!r}. That is a malformed "
                "structure — most often a missing `]` or `}` — and this reader refuses to "
                "loop on it rather than hanging"
            )

    # -- primitives --------------------------------------------------------
    def _ws(self) -> None:
        while self.i < len(self.s) and self.s[self.i] in " \t\r\n,":
            self.i += 1

    def _quoted(self) -> str:
        self.i += 1  # opening quote
        buf: list[str] = []
        while self.i < len(self.s):
            c = self.s[self.i]
            if c == "\\" and self.i + 1 < len(self.s):
                buf.append(self.s[self.i + 1])
                self.i += 2
                continue
            if c == '"':
                self.i += 1
                break
            buf.append(c)
            self.i += 1
        return "".join(buf)

    def _bare(self, stops: str) -> str:
        start = self.i
        while self.i < len(self.s) and self.s[self.i] not in stops:
            self.i += 1
        return self.s[start : self.i].strip()

    # -- structure ---------------------------------------------------------
    def parse_top(self) -> dict:
        return self._object(top=True)

    def _object(self, top: bool = False) -> dict:
        obj: dict = {}
        while True:
            before = self.i
            self._ws()
            if self.i >= len(self.s):
                if not top:
                    self.unterminated.append("a block that was never closed with `}`")
                return obj
            if self.s[self.i] == "}":
                self.i += 1
                return obj
            if self.s[self.i] in "]=:":
                # Nothing sane can start here; skip it rather than raise.
                self.skipped.append(self.s[self.i])
                self.i += 1
                continue
            key = self._quoted() if self.s[self.i] == '"' else self._bare(_KEY_STOP)
            if not key:
                self.skipped.append(self.s[self.i : self.i + 20])
                self.i += 1
                continue
            self._ws_inline()
            if self.i < len(self.s) and self.s[self.i] in "=:":
                self.i += 1
                self._ws()
            value = self._value()
            self._merge(obj, key.split("."), value)
            if top and self.i >= len(self.s):
                return obj
            self._guard(before, "inside a block")

    def _ws_inline(self) -> None:
        while self.i < len(self.s) and self.s[self.i] in " \t":
            self.i += 1

    def _value(self):
        self._ws_inline()
        if self.i >= len(self.s):
            return ""
        c = self.s[self.i]
        if c == "{":
            self.i += 1
            return self._object()
        if c == "[":
            self.i += 1
            return self._array()
        if c == '"':
            return self._quoted()
        return self._bare("\n,}")

    def _array(self) -> list:
        # THE HANG. `_bare(",]\n}")` cannot advance when the current character is already a
        # stop character, so a `}` reached inside an unterminated array appended "" to
        # `items` and re-entered the loop unchanged: `authentication = [` followed by `}` —
        # two lines, found by delta-debugging a fuzz case down to the minimum — made this
        # spin at 100% CPU with unbounded memory growth (147 MB after 8 s), never printing an
        # error and never exiting. The documented contract is "exit 0 translated, 1 could not
        # read the input"; a wedge is neither, and in CI or a `mqttui migrate` it is an OOM
        # rather than a diagnosable failure. EMQX itself reports a syntax error on that file.
        #
        # Two guards, deliberately belt-and-braces: an unterminated array ENDS at the `}`
        # that closes its enclosing object (leaving the `}` for that object to consume), and
        # any iteration that fails to consume a character raises HoconError — so no future
        # edit to _bare's stop set can reintroduce a non-advancing loop anywhere.
        items: list = []
        while True:
            before = self.i
            self._ws()
            if self.i >= len(self.s):
                self.unterminated.append("an array that was never closed with `]`")
                return items
            if self.s[self.i] == "]":
                self.i += 1
                return items
            c = self.s[self.i]
            if c == "}":
                self.unterminated.append(
                    "an array that was never closed with `]` before the `}` that closed its "
                    "enclosing block"
                )
                return items
            if c == "{":
                self.i += 1
                items.append(self._object())
            elif c == "[":
                self.i += 1
                items.append(self._array())
            elif c == '"':
                items.append(self._quoted())
            else:
                items.append(self._bare(",]\n}"))
            self._guard(before, "inside an array")

    @staticmethod
    def _merge(obj: dict, path: list[str], value) -> None:
        cur = obj
        for part in path[:-1]:
            nxt = cur.get(part)
            if not isinstance(nxt, dict):
                nxt = {}
                cur[part] = nxt
            cur = nxt
        last = path[-1]
        if isinstance(value, dict) and isinstance(cur.get(last), dict):
            for k, v in value.items():
                Hocon._merge(cur[last], [k], v)
        else:
            cur[last] = value


def leaves(tree: dict, prefix: str = "") -> list[tuple[str, object]]:
    """Every scalar/array leaf as (dotted path, value)."""
    out: list[tuple[str, object]] = []
    for k, v in tree.items():
        path = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            out.extend(leaves(v, path))
        else:
            out.append((path, v))
    return out


# ---------------------------------------------------------------------------
# Unit normalisation. HOCON values carry units; mqttd wants plain integers, and a
# unit this table does not know must become a TODO rather than a wrong number.
# ---------------------------------------------------------------------------

_SIZE = {"b": 1, "kb": 1024, "mb": 1024**2, "gb": 1024**3, "tb": 1024**4}
_TIME = {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400}


def as_bytes(value: str) -> int | None:
    v = str(value).strip().strip('"').lower()
    if v.isdigit():
        return int(v)
    for suffix, mult in sorted(_SIZE.items(), key=lambda kv: -len(kv[0])):
        if v.endswith(suffix) and v[: -len(suffix)].strip().replace(".", "", 1).isdigit():
            return int(float(v[: -len(suffix)].strip()) * mult)
    return None


def as_seconds(value: str) -> int | None:
    v = str(value).strip().strip('"').lower()
    if v.isdigit():
        return int(v)
    for suffix, mult in sorted(_TIME.items(), key=lambda kv: -len(kv[0])):
        if v.endswith(suffix) and v[: -len(suffix)].strip().replace(".", "", 1).isdigit():
            return int(float(v[: -len(suffix)].strip()) * mult)
    return None


def as_rate_per_sec(value: str) -> int | None:
    """`"1000/s"` -> 1000. Anything with another window is not a per-second rate."""
    v = str(value).strip().strip('"').lower()
    if v.isdigit():
        return int(v)
    if "/" in v:
        count, _, window = v.partition("/")
        if count.strip().isdigit() and window.strip() in ("s", "1s", "sec", "second"):
            return int(count.strip())
    return None


def truthy(value) -> bool:
    return str(value).strip().strip('"').lower() in ("true", "yes", "on", "1")


def falsey(value) -> bool:
    return str(value).strip().strip('"').lower() in ("false", "no", "off", "0")


# ---------------------------------------------------------------------------
# String emission. ONE helper per channel, used by EVERY string this tool writes.
#
# The 2026-08-14 review found the whole class at once: no value was escaped
# anywhere. An AD-style username (`CORP\jdoe`) came out as `identities =
# ["CORP\jdoe"]` and a Windows certificate path as `cert = "C:\emqx\certs\cert.pem"`,
# neither of which is valid TOML — tomllib rejects the WHOLE document ("Unescaped
# '\' in a string"), so ONE such user poisons the entire ACL file and the broker
# refuses to load any of it. A converter whose output the broker cannot parse has
# failed at its one job, so nothing below builds a quoted string by hand.
#
# The same helpers are duplicated verbatim in from-mosquitto.py and
# from-hivemq.py rather than shared through an import, deliberately: each converter
# is ONE self-contained stdlib-only file, run standalone (`mqttui migrate emqx`, or
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
# PROVENANCE OR NOTHING.
#
# The load-bearing structure. Every finding of the three review rounds that mattered had one
# shape — a LIVE security-relevant value the tool had not derived from the input:
#
#   * a listener EMQX had switched OFF (`enable = false`) converted to a live bind;
#   * `bind = "0.0.0.0:1883"` fabricated by `lst.bind or ("0.0.0.0:1883" if ...)` for a
#     listener whose address the converter never read;
#   * an mTLS mandate, a CRL check and a TLS-version floor taken from `tls_listeners[0]` and
#     applied to every transport;
#   * an EMQX bridge that used TLS converted to a LIVE PLAINTEXT upstream;
#   * an ACL claiming to deny everything beside `default = "allow"`.
#
# Fixing those one at a time is unbounded work, because the set of EMQX constructs nobody has
# looked at yet is unbounded. So instead: SECURITY_FIELDS names the fields whose value decides
# who can connect and what they may do, and the ONLY way to write one is Provenance.line(),
# which takes the value AND the EMQX key it came from and REFUSES to emit a live line without
# the key. A field with no provenance is emitted COMMENTED OUT beside a TODO naming what the
# operator must decide. There is no `or "0.0.0.0:1883"` anywhere in this file.
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
        # [security] and [security.*] — who may connect and what governs them
        "acl_file",
        "password_file",
        "allow_anonymous",
        "mtls_identity_source",
        "url",
        "issuer",
        "audience",
        "hs256_secret_file",
        "rs256_pem_file",
        # the ACL policy's own catch-all, and a bridge upstream's TLS anchors
        "default",
        "ca",
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
                f"nothing in the EMQX configuration named a value for {field}, so it is "
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


# ---------------------------------------------------------------------------
# What has an mqttd equivalent, and what deliberately does not.
#
# Being explicit about *why* is the point: "unsupported" invites a bug report,
# "deliberately absent, here is the alternative" does not.
# ---------------------------------------------------------------------------

# EMQX dotted path -> (mqttd section, key, kind)
#
# `mqtt.max_inflight` is deliberately ABSENT: it looks like [limits] receive_maximum and
# is the OPPOSITE DIRECTION. See the named case in convert_mqtt_and_misc().
DIRECT: dict[str, tuple[str, str, str]] = {
    "node.data_dir": ("node", "data_dir", "str"),
    "mqtt.max_packet_size": ("limits", "max_packet_size", "bytes"),
    "mqtt.max_topic_alias": ("limits", "topic_alias_max", "u16"),
    "mqtt.max_mqueue_len": ("limits", "max_queued_messages", "int"),
    "mqtt.max_subscriptions": ("limits", "max_subscriptions_per_client", "int"),
    "retainer.backend.max_retained_messages": ("limits", "max_retained_messages", "int0"),
}

# Whole EMQX sections with no mqttd equivalent. Reported once per section, with the
# key count, so a 400-line rule_engine block is one honest TODO instead of 400.
SECTION_NO_EQUIVALENT: dict[str, str] = {
    "rule_engine": "EMQX's SQL rule engine has no mqttd equivalent — mqttd is a "
    "broker, not an integration platform. Each rule must be reproduced OUTSIDE the "
    "broker (an ordinary MQTT client that subscribes, transforms and republishes), or "
    "keep EMQX for that path. Decide per rule before cutover; a rule you forget is a "
    "data pipeline that silently stops",
    "connectors": "EMQX data integration (connectors). Only MQTT-type connectors have "
    "an mqttd analogue (mqtt-bridge, see --out-bridge); Kafka/HTTP/JDBC/S3/... sinks "
    "must move to a client-side consumer you own",
    "actions": "EMQX data integration (actions/sinks). An MQTT-type action IS translated "
    "with --out-bridge — its `local_topic` becomes an `out` rule's filter and its "
    "`parameters.topic` becomes the prefix remap — but ONLY the filter, the QoS and the "
    "remap: `parameters.retain` and `parameters.payload` have no mqtt-bridge equivalent and "
    "are listed individually as their own TODOs. Non-MQTT sinks (Kafka/HTTP/JDBC/S3/...) "
    "must move to a client-side consumer you own",
    "sources": "EMQX data integration (sources). An MQTT-type source IS translated with "
    "--out-bridge into an `in` rule (its `parameters.topic` is the filter, its `local_topic` "
    "the prefix remap); every other key is reported individually. Non-MQTT sources must "
    "move outside the broker",
    "bridges": "EMQX 5.x bridges — the v1 shape, which is NOT a root in 6.2.2's schema and "
    "survives only through the vendor's v1 upgrade path (emqx_bridge_compatible_config); a "
    "6.x deployment writes `connectors` + `actions`/`sources` instead. With --out-bridge, an "
    "MQTT-type bridge contributes its "
    "ADDRESS, its username, its topic FILTERS, a per-rule QoS and a prefix remap to an "
    "mqtt-bridge upstream — and NOTHING ELSE. Do not read this count as a count of "
    "translated settings: every other key on the bridge (payload templates, retain "
    "overrides, bridge_mode, clean_start, proto_ver, resource_opts, the pool and buffer "
    "settings) is listed individually below as its own TODO, because none of them has an "
    "mqtt-bridge equivalent. Non-MQTT bridge types have no broker-side equivalent at all",
    "exhook": "there is no gRPC hook API. An AUTH-shaped hook maps to the HTTP "
    "authentication hook ([security.http_auth]); a message-mutating hook does not map "
    "at all and must become a client-side pipeline",
    "plugins": "there is no plugin API and no plugin ABI to port to; authentication is "
    "password file / JWT / OIDC / mTLS / the HTTP hook",
    "gateway": "protocol gateways (CoAP, LwM2M, MQTT-SN, STOMP, ExProto, GBT32960, "
    "OCPP) are not implemented. mqttd speaks MQTT 3.1.1/5 over TCP, TLS, WebSocket, "
    "WSS and QUIC only — a non-MQTT device fleet needs a separate gateway process",
    "dashboard": "there is no web dashboard, by design (signal-driven operations, "
    "ADR 0020): /metrics for Prometheus, /statusz for state, and an audit log. Plan the "
    "operator workflow that replaces the dashboard before cutover, not after",
    "api_key": "there is no REST admin API, by design — nothing to hold an API key. "
    "Runtime changes are config + SIGHUP; observation is /metrics and /statusz",
    "prometheus": "mqttd always serves /metrics on [listeners] health_bind (or a "
    "separate metrics_bind); there is no push-gateway mode. Repoint your scrape config",
    "license": "mqttd is Apache-2.0 with no licence file and no licensed features — "
    "clustering included. Nothing to carry over",
    "psk_authentication": "PSK ciphersuites are not implemented",
    "sys_topics": "$SYS topics are not implemented; the equivalent data is on /metrics "
    "and /statusz. Any client that SUBSCRIBES to $SYS must be rewritten",
    "sysmon": "there is no VM/OS monitor; use node_exporter beside /metrics",
    "alarm": "there is no in-broker alarm table; alert on the Prometheus metrics",
    "conn_congestion": "not implemented; the nearest signals are the queue-overflow "
    "and brownout counters on /metrics",
    "flapping_detect": "the nearest control is [security.auth_penalty] (failures per "
    "SOURCE ADDRESS, never per username), which is NOT the same thing: it boxes out "
    "authentication failures, not reconnect churn. Decide whether you still need it",
    "force_gc": "an Erlang VM control with no analogue",
    "force_shutdown": "an Erlang VM control with no analogue. The mqttd analogues are "
    "the [limits] memory_max_bytes and [durable] store_max_bytes BROWNOUT watermarks, "
    "which refuse growth writes rather than killing the process",
    "delayed": "delayed publish ($delayed/...) is not implemented",
    "log": "mqttd logs to stdout for the container/journal to collect (RUST_LOG "
    "controls the level); there is no file sink, rotation or per-file level config",
    "zones": "per-zone configuration overrides do not exist; mqttd's config is "
    "node-wide. Every zone-scoped setting must collapse to one value, or the zones must "
    "become separate deployments",
    "file_transfer": "the file-transfer feature is not implemented",
    "message_transformation": "in-broker message transformation is not implemented; "
    "transform in a client",
    "schema_registry": "the schema registry is not implemented",
    "schema_validation": "in-broker schema validation is not implemented",
    "slow_subs": "slow-subscriber tracking is not implemented",
    "topic_metrics": "per-topic metrics are not implemented; /metrics is aggregate",
    "banned": "there is no ban table; express denials in the ACL policy (a deny rule "
    "wins over any allow) and reload with SIGHUP",
    "telemetry": "mqttd sends no telemetry anywhere, so there is nothing to disable",
    "durable_sessions": "mqttd's durable sessions are ON BY DEFAULT and quorum-"
    "replicated ([durable] enabled, min_replicas); EMQX's opt-in durable-storage "
    "settings do not map key-for-key. Read docs/adr/0029 and size [durable] yourself",
    "session_persistence": "see durable_sessions — mqttd's durable plane is on by "
    "default and configured under [durable], not key-for-key from EMQX",
    "limiter": "global/zone rate limiters do not exist. The per-connection publish rate "
    "is [limits] max_publish_rate; connection counts are max_connections and "
    "max_connections_per_ip. There is no byte-rate limiter",
    "broker": "internal dispatcher/session-locking tuning is not exposed",
    "crl_cache": "[tls] crl is a FILE, hot-reloaded on SIGHUP; there is no CRL HTTP "
    "fetch or cache to configure",
    "ocsp": "OCSP stapling is not implemented; revocation is a CRL file ([tls] crl)",
    "srs": "not a setting this converter knows — check the mqttd configuration table",
}

# Individual keys inside sections this converter DOES read, with the reason.
KEY_NO_EQUIVALENT: dict[str, str] = {
    "node.cookie": "the Erlang distribution cookie has no analogue: mqttd's cluster bus "
    "is authenticated by per-node mTLS (CN == [node] id) plus a 64-hex signed-gossip "
    "key. See docs/SECURED-CLUSTER-TUTORIAL.md",
    "node.role": "there is no core/replicant split; every mqttd node is a full member",
    "node.max_ports": "an Erlang VM limit; bound connections with [limits] "
    "max_connections and the process file-descriptor limit instead",
    "node.dist_buffer_size": "an Erlang distribution buffer; no analogue",
    "node.global_gc_interval": "an Erlang VM control; no analogue",
    "node.process_limit": "an Erlang VM limit; no analogue",
    "mqtt.idle_timeout": "the pre-CONNECT idle timeout is not configurable",
    "mqtt.max_clientid_len": "the client-id length limit is not configurable "
    "(the spec's 65535 applies)",
    "mqtt.max_topic_levels": "the topic-level limit is not configurable",
    "mqtt.max_qos_allowed": "mqttd supports QoS 0/1/2 and does not cap the maximum; "
    "if you relied on capping QoS, nothing here reproduces it",
    "mqtt.retain_available": "retained messages cannot be switched off; cap them with "
    "[limits] max_retained_messages, or deny retained topics in the ACL",
    "mqtt.wildcard_subscription": "wildcard subscriptions cannot be switched off",
    "mqtt.shared_subscription": "shared subscriptions cannot be switched off",
    "mqtt.exclusive_subscription": "exclusive subscriptions are not implemented",
    "mqtt.subscription_message_filter": "EMQX's `topic?key=value` subscription filters "
    "are an EMQX extension and are not implemented",
    "mqtt.ignore_loop_deliver": "v3.1.1 loop suppression is not configurable; v5 "
    "clients get the spec's No Local subscription option",
    "mqtt.strict_mode": "mqttd always parses strictly (an invalid UTF-8 topic or client "
    "id is a protocol error), so this is on and not configurable",
    "mqtt.response_information": "the CONNACK Response Information property is not sent",
    "mqtt.server_keepalive": "mqttd does not send a Server Keep Alive; the client's "
    "requested keepalive is used",
    "mqtt.keepalive_multiplier": "the keepalive grace multiplier is not configurable",
    "mqtt.keepalive_check_interval": "not configurable",
    "mqtt.upgrade_qos": "QoS upgrade-on-subscription is not implemented (the spec's "
    "min(publish, subscription) applies)",
    "mqtt.retry_interval": "the QoS 1/2 retry timer is not configurable",
    "mqtt.max_awaiting_rel": "the inbound QoS 2 awaiting-PUBREL window is not "
    "configurable; [limits] receive_maximum bounds in-flight inbound instead",
    "mqtt.await_rel_timeout": "the QoS 2 PUBREL wait is not configurable",
    "mqtt.session_expiry_interval": "the v3.1.1 session expiry default is not "
    "configurable; v5 clients set their own Session Expiry Interval",
    "mqtt.message_expiry_interval": "a broker-wide message expiry default is not "
    "implemented; v5 clients set Message Expiry Interval per publish",
    "mqtt.mqueue_store_qos0": "QoS 0 messages are never queued for an offline session",
    "mqtt.mqueue_priorities": "per-topic queue priorities are not implemented",
    "mqtt.mqueue_default_priority": "per-topic queue priorities are not implemented",
    "mqtt.use_username_as_clientid": "not implemented",
    "mqtt.peer_cert_as_clientid": "the certificate cannot be used as the CLIENT ID; "
    "[security] mtls_identity_source sets the AUTHENTICATION identity only",
    "mqtt.shared_subscription_strategy": "the shared-subscription dispatch strategy is "
    "not configurable",
    "mqtt.shared_subscription_initial_sticky_pick": "not configurable",
    "mqtt.client_attrs_init": "client attributes do not exist, so neither do the "
    "${client_attrs.NAME} ACL placeholders that read them",
    "mqtt.clientid_override": "not implemented",
    # `retainer.enable` is deliberately ABSENT: it is a DISABLE-ABLE construct (vendor
    # default true, apps/emqx_retainer/src/emqx_retainer_schema.erl @ 6.2.2), so the honest
    # sentence depends on the value and is written in convert_mqtt_and_misc().
    "retainer.msg_expiry_interval": "a retained-message expiry timer is not "
    "implemented; a retained value is replaced or cleared by an empty publish",
    "retainer.max_payload_size": "there is no separate retained-payload cap; [limits] "
    "max_packet_size bounds every inbound packet",
    "retainer.stop_publish_clear_msg": "an empty retained publish always clears and is "
    "always forwarded, per spec; not configurable",
    "retainer.delivery_rate": "the retained-delivery rate is not throttled separately",
    "retainer.flow_control": "not implemented",
    "retainer.backend.type": "retained messages live in the same store as the rest of "
    "the durable state ([node] data_dir); there is no separate backend to choose",
    "retainer.backend.storage_type": "ram/disc is not a per-subsystem choice; "
    "[durable] enabled + [node] data_dir decide it for everything",
    "retainer.backend.index_specs": "not implemented",
    "authorization.deny_action": "a denied publish/subscribe is refused per spec (v5 "
    "reason code, v3.1.1 SUBACK failure); disconnect-on-deny is not an option",
    "authorization.cache.enable": "there is no authorization cache to configure — ACL "
    "evaluation is in-process against the loaded policy",
    "authorization.cache.max_size": "there is no authorization cache to configure",
    "authorization.cache.ttl": "there is no authorization cache to configure",
    "authorization.cache.excludes": "there is no authorization cache to configure",
}

# Per-listener keys with no equivalent. Reported with the listener's name attached.
LISTENER_NO_EQUIVALENT: dict[str, str] = {
    "proxy_protocol": "the PROXY protocol is not supported. A layer-4 load balancer in "
    "front of mqttd must preserve the source address, or per-IP limits and the audit log "
    "will see the balancer's address instead of the client's",
    "proxy_protocol_timeout": "the PROXY protocol is not supported",
    "mountpoint": "per-listener topic mount points are not implemented. Every client "
    "sees the same topic space; a mount point must move into the client's topic strings "
    "or into an mqtt-bridge remap",
    "access_rules": "per-listener IP allow/deny rules have no equivalent: the mqttd ACL "
    "matches on IDENTITY and topic, never on source address. Enforce address rules in "
    "the network layer (security group, NetworkPolicy, host firewall)",
    "acceptors": "the acceptor pool is not exposed (the async runtime sizes itself)",
    # `enable_authn` is deliberately ABSENT: it is handled in convert_listener_keys with its
    # VALUE, because the fail-open reading (`false` — that listener authenticated nobody) and
    # the harmless default (`true`) need different sentences, and one constant said neither.
    "max_conn_rate": "there is no connection RATE limit. [limits] "
    "max_connections_per_ip is a CONCURRENCY cap — a different control; a reconnect "
    "storm from many addresses is not bounded by it",
    "bytes_rate": "there is no byte-rate limiter; [limits] max_publish_rate counts "
    "MESSAGES per second per connection",
    "max_conn_burst": "there is no rate limiter, so no burst either",
    "messages_burst": "there is no burst window on max_publish_rate",
    "bytes_burst": "there is no byte-rate limiter",
    "zone": "zones do not exist; mqttd's configuration is node-wide",
    "ciphers": "cipher suites are not configurable — TLS 1.3 AEAD suites only "
    "(QUIC mandates 1.3, so a QUIC listener never had a choice either)",
    "tcp_options": "socket tuning (backlog, buffers, watermarks, TCP keepalive) is not "
    "exposed; the OS defaults apply",
    "ssl_options.reuse_sessions": "TLS session resumption is on by default and sized "
    "with [tls] session_cache (0 disables it)",
    "ssl_options.depth": "the certificate-chain depth limit is not configurable",
    "ssl_options.ciphers": "cipher suites are not configurable — TLS 1.3 AEAD suites "
    "only, and the hardened ECDHE+AEAD subset when [tls] allow_tls12 is on",
    "ssl_options.secure_renegotiate": "TLS 1.3 has no renegotiation",
    "ssl_options.client_renegotiation": "TLS 1.3 has no renegotiation",
    "ssl_options.honor_cipher_order": "cipher suites are not configurable",
    "ssl_options.log_level": "TLS logging is not separately configurable; RUST_LOG "
    "controls the broker's log level",
    "ssl_options.hibernate_after": "no analogue",
    "ssl_options.handshake_timeout": "the TLS handshake timeout is not configurable",
    "ssl_options.gc_after_handshake": "no analogue",
    "ssl_options.enable_crl_check": "set [tls] crl to a CRL FILE; there is no CRL "
    "fetching or cache",
    "ssl_options.partial_chain": "partial-chain verification is not configurable",
    "ssl_options.verify_peer_ext_key_usage": "mqttd ALWAYS requires the clientAuth "
    "extended key usage on a client certificate and refuses one without it at the "
    "handshake. Audit your fleet first: scripts/migrate/cert-audit.sh",
    "ssl_options.password": "an encrypted private key is not supported; the [tls] key "
    "must be an unencrypted PEM (mount it from a Secret)",
    "ssl_options.user_lookup_fun": "PSK is not implemented",
    "ssl_options.middlebox_comp_mode": "not configurable",
    "ssl_options.keyfile_passphrase": "an encrypted private key is not supported",
    "websocket.mqtt_piggyback": "not configurable; several MQTT packets may share a "
    "binary frame and one packet may span frames, per the OASIS WebSocket binding",
    "websocket.compress": "permessage-deflate is not implemented",
    "websocket.idle_timeout": "the pre-CONNECT idle timeout is not configurable",
    "websocket.max_frame_size": "there is no separate frame cap; [limits] "
    "max_packet_size bounds the MQTT packet",
    "websocket.fail_if_no_subprotocol": "mqttd ALWAYS refuses a WebSocket upgrade that "
    "does not offer the `mqtt` subprotocol (verified in crates/mqtt-net/src/ws.rs), so "
    "this is effectively always true and is not configurable",
    "websocket.check_origin_enable": "Origin-header checking is not implemented. Put a "
    "reverse proxy in front if you need it",
    "websocket.check_origins": "Origin-header checking is not implemented",
    "websocket.allow_origin_absence": "Origin-header checking is not implemented",
    "websocket.proxy_address_header": "X-Forwarded-For is not honoured; per-IP limits "
    "and the audit log use the socket peer address",
    "websocket.proxy_port_header": "X-Forwarded-Port is not honoured",
    "websocket.deflate_opts": "permessage-deflate is not implemented",
    "websocket.validate_utf8": "UTF-8 validation is always on",
    "websocket.allow_extensions": "WebSocket extensions are not negotiated",
}

# Per-bridge keys with no mqtt-bridge equivalent, keyed on the leaf path inside the
# bridge body. Round 1 found convert_bridges() reading only server/url/credentials/ssl and
# each leg's topic+qos, and dropping EVERYTHING else under a section-level count whose text
# told the operator the bridge had been translated. A payload template is a data
# transformation and a retain override is a policy: dropping either silently changes what
# the far side receives, which is exactly the failure this tool exists to prevent.
BRIDGE_NO_EQUIVALENT: dict[str, str] = {
    "payload": "an EMQX PAYLOAD TEMPLATE. mqtt-bridge forwards the payload BYTE FOR BYTE "
    "and has no templating at all, so this transformation was NOT reproduced — the far "
    "side will receive the ORIGINAL message body, not the rendered template. If a consumer "
    "depends on that shape, the transformation must move into a client that subscribes, "
    "rewrites and republishes (mqttd is a broker, not an integration platform)",
    "retain": "a RETAIN override. mqtt-bridge preserves the SOURCE message's retain bit "
    "(crates/mqtt-bridge/src/client.rs) and cannot set or clear it, so a bridge that "
    "deliberately stripped RETAIN now writes retained values into the far broker's "
    "retained set, and one that forced RETAIN no longer does. Decide this per topic before "
    "cutover — a wrong retained value outlives the mistake",
    "qos": "handled — each leg's qos becomes the rule's fixed per-rule QoS",
    "topic": "handled — each leg's topic becomes the rule's filter or its prefix remap",
    "bridge_mode": "EMQX's bridge_mode sets the MQTT `bridge` connect flag so the far "
    "broker suppresses echo. mqtt-bridge does not set it and does not need it: forwarding "
    "is deny-by-default per rule and a hop counter (hop_count_limit) breaks loops. If the "
    "FAR broker relied on the flag to avoid echoing back, verify the loop is still cut",
    "clean_start": "mqtt-bridge always connects with a clean session and owns its own "
    "durable spool ([spool] dir) instead of relying on a far-side session",
    "proto_ver": "the bridge's MQTT protocol version is not configurable; mqtt-bridge "
    "speaks MQTT 5 and falls back only as its own client library dictates",
    "keepalive": "the bridge keepalive is not configurable",
    "retry_interval": "the bridge retry timer is not configurable; delivery is retried "
    "from the durable spool",
    "max_inflight": "the bridge's in-flight window is not configurable",
    "reconnect_interval": "the reconnect backoff is not configurable",
    "resource_opts": "EMQX's resource_opts (buffering, batching, queue sizing, worker "
    "pools) has no analogue: mqtt-bridge buffers in ONE durable spool ([spool] dir, "
    "max_messages) and forwards one message at a time",
    "egress_pool_size": "there is no worker pool to size",
    "ingress_pool_size": "there is no worker pool to size",
    "pool_size": "there is no worker pool to size",
    "server_name_indication": "SNI is taken from the upstream URL's host",
    "enable": "handled — a disabled bridge is reported rather than written",
}

PROTO_BIND = {
    "tcp": "plaintext_bind",
    "ssl": "tls_bind",
    "ws": "ws_bind",
    "wss": "wss_bind",
    "quic": "quic_bind",
}


@dataclass
class Listener:
    proto: str
    name: str
    bind: str | None = None
    # What normalise_bind() had to default, and why — named on the emitted line so the part
    # of the address the input did not hold is never silent. None when the input gave both
    # host and port.
    bind_defaulted: str | None = None
    # Why no address could be derived, when that is the case. A listener with this set gets
    # NO live bind: the candidate is commented out with this sentence as the decision.
    bind_gap: str | None = None
    keys: dict[str, object] = field(default_factory=dict)
    # `enable` (alias `enabled`), default true, is a real base_listener field in EMQX's own
    # schema (apps/emqx/src/emqx_schema.erl base_listener/1 at tag 6.2.2). It was UNREAD, so
    # a listener EMQX had switched off became a live mqttd bind — the one flip that opens a
    # network port. Round 1 fixed the same class on authenticators, authz sources and
    # bridges; round 2 found it here; this is the sweep that reads it everywhere.
    enabled: bool = True


@dataclass
class BridgeRule:
    direction: str
    filter: str
    qos: int = 1
    prefix: str | None = None
    strip_prefix: str | None = None
    todos: list[str] = field(default_factory=list)


@dataclass
class BridgeUpstream:
    name: str
    url: str
    # The EMQX key the address came from, so the one setting that decides WHERE the bridge
    # connects carries its provenance into the generated bridge TOML.
    url_source: str = ""
    username: str | None = None
    rules: list[BridgeRule] = field(default_factory=list)
    todos: list[str] = field(default_factory=list)
    # The upstream's TLS material, as `(mqtt-bridge key, path, the EMQX key it came from)`.
    # NEVER emitted live: mqtt-bridge's `tls` is Optional (crates/mqtt-bridge/src/config.rs)
    # and absent means PLAINTEXT, so an EMQX bridge that used TLS became a LIVE PLAINTEXT
    # upstream — the bridge's CONNECT, username included, in the clear at an upstream that
    # expected TLS. That is a security-posture change, and the contract for one is a
    # COMMENTED candidate plus a TODO. Found 2026-08-15.
    tls: list[tuple[str, str, str]] = field(default_factory=list)
    tls_todo: str | None = None
    # Whether the EMQX side had TLS ENABLED. Commenting `[upstreams.tls]` out is the posture
    # change, so the value whose liveness DECIDES the posture — the upstream `url` — cannot be
    # live either: with the tls block commented, a completed-as-instructed draft connected to a
    # TLS peer in cleartext, and docs/MIGRATION.md's promise section named that exact shape as
    # "impossible rather than absent" while it was still happening. Found 2026-08-15.
    tls_enabled: bool = False


@dataclass
class Conversion:
    config: dict[str, dict[str, Emitted]] = field(default_factory=dict)
    listeners: list[Listener] = field(default_factory=list)
    todos: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    acl_file: str | None = None
    acl_default: str = "deny"
    # The EMQX key the ACL `default` was derived from — `authorization.no_match` when the
    # input set it, otherwise the vendor's documented default for that field. Either way the
    # value carries its source, because the ACL default IS the policy for everything no rule
    # names.
    acl_default_source: str | None = None
    acl_default_todo: str | None = None
    bridges: list[BridgeUpstream] = field(default_factory=list)
    saw_auth: bool = False
    saw_authz: bool = False
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
        `[listeners]`, `[tls]`, `[security]` or `[security.*]` without naming the EMQX key it
        came from.
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


# ---------------------------------------------------------------------------
# Conversion
# ---------------------------------------------------------------------------


def bind_gap(address: str) -> str | None:
    """None when `address` is a `host:port` mqttd can bind; otherwise WHY it cannot.

    Every `*_bind` used to be emitted LIVE with no check that the broker can bind it, and
    `mqttd --check-config` — the verification this converter's own header, its `--help` and
    docs/MIGRATION.md all point the operator at — accepts ANY string there. So the prescribed
    gate said `config OK` on addresses the broker then refuses at STARTUP ("failed to lookup
    address information"), which is the one value the whole provenance restructuring is about.
    The same check lives in from-mosquitto.py and from-hivemq.py — each converter is ONE
    self-contained stdlib-only file, as the TOML-escape helpers already are. Found 2026-08-15.
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


def normalise_bind(value: object) -> tuple[str | None, str | None]:
    """`(address, what was defaulted)` — or `(None, why)` when it cannot be derived.

    EMQX's `bind` is either `"host:port"` or a bare port (emqx_schema's `ip_port` type). A
    bare port means every interface, which is a documented default of a field that WAS
    present, so it is named as `defaulted:` on the emitted line rather than silently
    resolved. Anything else — a bare host with no port — is NOT something mqttd can bind, and
    the old code appended `:1883` to it, inventing a port the input never named.
    """
    # A NON-SCALAR `bind` was `str()`-ed, so `bind = ["0.0.0.0:1883"]` became the LIVE value
    # `"['0.0.0.0:1883']"` — a Python repr, a string that appears NOWHERE in the input, and one
    # invariant F could not see because for a `*_bind` it compares only the PORT. Rejected
    # outright rather than reshaped. Found 2026-08-15.
    if isinstance(value, (list, dict, tuple, set)):
        return None, (
            f"`bind` is not a single address but a {type(value).__name__} ({value!r}); mqttd "
            "binds ONE address per protocol and this converter will not pick one of them for "
            "you"
        )
    v = str(value).strip().strip('"')
    if not v:
        return None, "the `bind` value was empty"
    if v.isdigit():
        address = f"0.0.0.0:{v}"
        gap = bind_gap(address)
        if gap is not None:
            return None, f"`bind = {v}` is not a port mqttd can bind: {gap}"
        return (
            address,
            "the host, because EMQX's `bind` named only a port and a bare port listens on "
            "every interface (emqx_schema's ip_port type)",
        )
    if ":" not in v:
        return None, (
            f"`bind = {v}` names a host with NO PORT, and mqttd cannot bind that. This "
            "converter will not invent one"
        )
    # EMQX's own `ip_port` accepts forms mqttd cannot bind — `:8085` (host omitted) is the
    # documented "all interfaces" spelling, and mqttd refuses to resolve an empty host at
    # STARTUP while `--check-config` says `config OK`. So the shape is checked here, and an
    # unbindable address becomes an INERT candidate rather than a live bind. Found 2026-08-15.
    gap = bind_gap(v)
    if gap is not None:
        return None, (
            f"`bind = {v}` is not an address mqttd can bind: {gap}. `mqttd --check-config` "
            "ACCEPTS any string in a bind and the broker then fails at STARTUP, so it is not "
            "emitted live"
        )
    return v, None


def convert_listeners(tree: dict, conv: Conversion) -> None:
    """`listeners.<proto>.<name>.<key>` -> the one bind mqttd has per protocol."""
    listeners = tree.get("listeners")
    if not isinstance(listeners, dict):
        return
    for proto, named in listeners.items():
        if not isinstance(named, dict):
            conv.todo(
                f"listeners.{proto}: not a listener block this converter understands — "
                "translate it by hand against the mqttd configuration table"
            )
            continue
        if proto not in PROTO_BIND:
            conv.todo(
                f"listeners.{proto}: mqttd has no {proto} transport. It speaks MQTT over "
                "TCP (plaintext_bind), TLS (tls_bind), WebSocket (ws_bind), WSS "
                "(wss_bind) and QUIC (quic_bind) only"
            )
            continue
        for name, body in named.items():
            if not isinstance(body, dict):
                conv.todo(
                    f"listeners.{proto}.{name}: not a listener body — translate by hand"
                )
                continue
            lst = Listener(proto=proto, name=name)
            for path, value in leaves(body):
                if path == "bind":
                    address, why = normalise_bind(value)
                    lst.bind = address
                    if address is None:
                        lst.bind_gap = why
                    else:
                        lst.bind_defaulted = why
                elif path in ("enable", "enabled"):
                    # EMQX's own alias set; default true.
                    lst.enabled = not falsey(value)
                    lst.keys[path] = value
                else:
                    lst.keys[path] = value
            conv.listeners.append(lst)
    disabled = [l for l in conv.listeners if not l.enabled]
    for lst in disabled:
        # NOT translated: enabling a listener EMQX had switched off is the one posture change
        # that opens a network port. So it is reported, with everything it carried, and it
        # contributes no bind, no TLS material and no connection cap.
        settings = "; ".join(
            f"{k} = {v}" for k, v in sorted(lst.keys.items()) if k not in ("enable", "enabled")
        )
        conv.todo(
            f"listeners.{lst.proto}.{lst.name} has enable = false: the listener was SWITCHED "
            f"OFF in EMQX, so it was NOT translated — no {PROTO_BIND.get(lst.proto, 'bind')} "
            f"was written for its address ({lst.bind or 'unset'}) and mqttd will NOT accept "
            "connections on it. Carrying it over would have opened a network port the "
            "operator had closed, which is why it is reported instead"
            + (f". Everything it carried, for the record: {settings}" if settings else "")
            + ". If it should be live, enable it and re-run — and note that mqttd binds ONE "
            "listener per protocol, so an enabled one of the same protocol already holds that "
            "bind"
        )
    conv.listeners = [l for l in conv.listeners if l.enabled]


def convert_listener_keys(conv: Conversion) -> None:
    """Fold per-listener settings into mqttd's node-wide tables, or report them."""
    conn_caps: list[int] = []
    pub_rates: list[tuple[str, int]] = []
    # Per-listener authentication chains, gathered so the report NAMES the endpoint each one
    # pointed at rather than saying "no direct equivalent" about a construct that has one.
    listener_authn: dict[str, list[str]] = {}
    for lst in conv.listeners:
        where = f"listeners.{lst.proto}.{lst.name}"
        for path, value in lst.keys.items():
            if path == "max_connections":
                if str(value).strip().strip('"') == "infinity":
                    # `infinity` is the vendor's shipped default in every listener
                    # example, so this is the COMMONEST real input — and it used to be
                    # the one path that left no trace at all, while the identically
                    # shaped `infinity` on a DIRECT-mapped key got a NOTE a few hundred
                    # lines away. The end state is correct (unset = uncapped); an
                    # operator diffing their listener settings against the output still
                    # needs to see that it was considered.
                    conv.note(
                        f"{where}.max_connections was `infinity`; [limits] "
                        "max_connections was left UNSET, which is also uncapped. Cap it "
                        "deliberately — docs/SIZING.md has the arithmetic for a fixed RAM "
                        "budget, and note that mqttd's cap is NODE-WIDE, not per listener"
                    )
                    continue
                if str(value).strip().strip('"').isdigit():
                    conn_caps.append(int(str(value).strip().strip('"')))
                    continue
                conv.todo(f"{where}.max_connections = {value}: not a number mqttd can use")
                continue
            if path == "messages_rate":
                # PER LISTENER in EMQX (emqx_schema's base_listener/1 ends
                # `++ emqx_limiter_schema:fields(mqtt)`, whose mqtt_limiter_names() generates
                # messages_rate/messages_burst per listener) and NODE-WIDE in mqttd, so a
                # bare conv.set inside this loop let the LAST listener win: a device listener
                # throttled to 100/s replaced node-wide by a browser listener's 5000/s, a 50x
                # loosening with no line an operator diffing their listeners could catch.
                # The neighbouring max_connections had the smallest-wins treatment already;
                # this is the same key one branch over. Found 2026-08-15.
                rate = as_rate_per_sec(value)
                if rate is None:
                    conv.todo(
                        f"{where}.messages_rate = {value}: [limits] max_publish_rate is "
                        "messages per SECOND per connection; this window is something else"
                    )
                else:
                    pub_rates.append((where, rate))
                continue
            if path in ("enable", "enabled"):
                continue  # handled in convert_listeners: honoured, and reported when false
            if path == "authentication" or path.startswith("authentication."):
                # A real vendor field at the pinned tag:
                # apps/emqx_auth/src/emqx_authn/emqx_authn_schema.erl:76-79,168-178 @ 6.2.2
                # injects `'mqtt.listener' => mqtt_listener_auth_fields()`, the same chain
                # type as the global root. It fell through to the generic line, so the whole
                # entry appeared NOWHERE, `saw_auth` stayed False, and the config carried two
                # contradicting statements: "listeners.ssl.d.authentication: no direct
                # equivalent" (false — an HTTP authenticator maps to [security.http_auth])
                # beside "NO `authentication` block was found" (false — it had just read
                # one). Found 2026-08-15.
                conv.saw_auth = True
                listener_authn.setdefault(where, []).append(f"{path} = {value!r}")
                continue
            if path == "enable_authn":
                # A per-listener authentication BYPASS. The generic table entry read the same
                # whether it was the default `true` or the fail-open `false`, so the value is
                # named and the false case gets its own sentence.
                if falsey(value):
                    conv.todo(
                        f"{where}.enable_authn = {value}: that listener ACCEPTED CLIENTS "
                        "WITHOUT AUTHENTICATING THEM in EMQX. mqttd's authentication is "
                        "node-wide, so there is no per-listener bypass to translate: after "
                        "cutover those clients must present credentials ([security] "
                        "allow_anonymous is false below) or they will not connect. If they "
                        "genuinely must stay anonymous, that listener has to become a "
                        "SEPARATE deployment with allow_anonymous = true — do not set it "
                        "node-wide to make one listener work"
                    )
                else:
                    conv.todo(
                        f"{where}.enable_authn = {value}: authentication is node-wide in "
                        "mqttd ([security] allow_anonymous), not per listener. This value "
                        "matches mqttd's posture, so nothing was lost"
                    )
                continue
            if path.startswith("ssl_options."):
                continue  # handled by convert_tls
            if path == "websocket.mqtt_path":
                conv.note(
                    f"{where}.websocket.mqtt_path = {value}: mqttd accepts the WebSocket "
                    "upgrade on ANY path (verified in crates/mqtt-net/src/ws.rs — it "
                    "checks the `mqtt` subprotocol, not the URI), so existing clients "
                    "keep working unchanged. There is no path to configure"
                )
                continue
            if path == "websocket.supported_subprotocols":
                offered = [
                    p.strip() for p in str(value).replace('"', "").split(",") if p.strip()
                ]
                extra = [p for p in offered if p != "mqtt"]
                if extra:
                    conv.todo(
                        f"{where}.websocket.supported_subprotocols offers {extra} beside "
                        "`mqtt`. mqttd negotiates ONLY `mqtt` and refuses an upgrade that "
                        "does not offer it, so any client that sends one of those instead "
                        "of `mqtt` will fail the handshake — check your browser clients"
                    )
                continue
            # tcp_options.* and ssl_options.* collapse to one report per family.
            family = path.split(".", 1)[0] if "." in path else path
            if family == "tcp_options":
                conv.todo(f"{where}.tcp_options: {LISTENER_NO_EQUIVALENT['tcp_options']}")
                continue
            reason = LISTENER_NO_EQUIVALENT.get(path)
            if reason:
                shown = (
                    "was set (its value is NOT copied — secrets are never transformed)"
                    if any(
                        sec in path.lower() for sec in ("password", "passphrase", "secret")
                    )
                    else f"= {value!r}"
                )
                conv.todo(f"{where}.{path} {shown}: {reason}")
                continue
            # The VALUE is named, not only the key. `mountpoint = "tenant-a/"` is the topic
            # prefix every translated ACL rule would have to be re-keyed onto, and
            # `access_rules = ["allow 10.9.9.0/24"]` IS an address policy — reported by key
            # alone, neither can be checked against the input or acted on. The one exception
            # is a secret, which this tool never copies anywhere (ADR 0051 §3 rule 2).
            shown = (
                "was set (its value is NOT copied — secrets are never transformed)"
                if any(sec in path.lower() for sec in ("password", "passphrase", "secret"))
                else f"= {value!r}"
            )
            conv.todo(
                f"{where}.{path} {shown}: no direct equivalent — check the mqttd "
                "configuration table (README env-var table / docs/mqttd.example.toml)"
            )
    for where, entries in listener_authn.items():
        conv.todo(
            f"{where} carried a LISTENER-SCOPED `authentication` chain (a real EMQX field: "
            "emqx_authn_schema.erl injects the same chain type onto every listener as onto "
            f"the root). mqttd's authentication is NODE-WIDE, so there is nothing per-listener "
            "to translate into and NONE of it was mapped — everything it named, for the "
            "record: "
            + "; ".join(entries)
            + ". If that chain is what authenticates your clients, translate it as if it were "
            "the top-level one (an HTTP authenticator becomes [security.http_auth], a JWT one "
            "[security.jwt] or [security.oidc]) and remember the result applies to EVERY "
            "listener; if two listeners authenticated DIFFERENTLY, that difference cannot be "
            "expressed and they must become separate deployments"
        )
    if pub_rates:
        winner = min(r for _, r in pub_rates)
        conv.set("limits", "max_publish_rate", str(winner))
        if len({r for _, r in pub_rates}) > 1:
            conv.todo(
                "several listeners set DIFFERENT messages_rate values ("
                + "; ".join(f"{w}: {r}/s" for w, r in pub_rates)
                + f"); [limits] max_publish_rate is node-wide, so the SMALLEST ({winner}/s) "
                "was used — raising a deliberately tight per-listener throttle to another "
                "listener's is the permissive direction. The other values are GONE from the "
                "output; raise it deliberately if that is not what you want"
            )
    if conn_caps:
        conv.set("limits", "max_connections", str(min(conn_caps)))
        if len(set(conn_caps)) > 1:
            conv.todo(
                "several listeners set DIFFERENT max_connections "
                f"({sorted(set(conn_caps))}); [limits] max_connections is node-wide, so "
                f"the SMALLEST ({min(conn_caps)}) was used. Raise it deliberately if that "
                "is not what you want"
            )


def convert_tls(conv: Conversion) -> tuple[list[str], list[str]]:
    """Return ([tls] lines, extra TODOs). ONE [tls] table serves EVERY TLS transport.

    mqttd builds one rustls acceptor for tls_bind AND wss_bind and hands the SAME cert,
    key and client_ca to quic::server_endpoint (crates/mqttd/src/main.rs), so there is no
    per-listener TLS to translate into. That makes this function's job reporting, not
    choosing: EVERY TLS listener is walked, and every setting the single table cannot hold
    becomes a TODO NAMING the listener it came from.

    Round 1 found it reading `tls_listeners[0]` only. A later listener's `verify`,
    `versions`, `enable_crl_check`, `ssl_options.password` and `depth` were dropped with NO
    TODO whenever the cert material happened to be identical (which also suppressed the
    material-differs TODO) — a revocation check and a TLS-1.2 acceptance vanishing in
    silence. Worse, the one warning an operator did get said the other listeners' material
    "was NOT carried over", the inverse of the truth: the first listener's material AND its
    mTLS posture are exactly what those transports end up using.
    """
    tls_listeners = [
        l for l in conv.listeners if any(k.startswith("ssl_options.") for k in l.keys)
    ]
    if not tls_listeners:
        return [], []
    todos: list[str] = []
    first = tls_listeners[0]

    def opt(lst: Listener, key: str) -> str | None:
        v = lst.keys.get(f"ssl_options.{key}")
        return None if v is None else str(v).strip().strip('"')

    def who(lst: Listener) -> str:
        return f"listeners.{lst.proto}.{lst.name}"

    def posture(lst: Listener) -> str:
        # certfile/keyfile are named too. Round 3 found the sweep failing here: the inventory
        # reported each listener's VERIFY posture but not its material, and the
        # "material differs" TODO said the other listeners' PEM files were "referenced
        # NOWHERE" without ever naming them — so the dropped paths appeared nowhere in the
        # output and an operator diffing their listener set could not see what was lost.
        return (
            f"certfile = {opt(lst, 'certfile') or 'unset'}, keyfile = "
            f"{opt(lst, 'keyfile') or 'unset'}, verify = "
            f"{opt(lst, 'verify') or 'verify_none'}, fail_if_no_peer_cert = "
            f"{opt(lst, 'fail_if_no_peer_cert') or 'false'}, cacertfile = "
            f"{opt(lst, 'cacertfile') or 'unset'}"
        )

    # -- the inventory, so the operator can see WHICH listeners share one table ---------
    if len(tls_listeners) > 1:
        todos.append(
            f"{len(tls_listeners)} TLS listeners were found "
            f"({', '.join(who(l) for l in tls_listeners)}). mqttd has ONE [tls] table and "
            "applies it to tls_bind, wss_bind AND quic_bind alike (one shared acceptor plus "
            f"quic::server_endpoint), so per-listener TLS cannot be expressed at all: "
            f"{who(first)}'s material and posture below are what EVERY TLS transport will "
            "use. Each of the others is reported separately — read every one against the "
            "transport it now governs: "
            + "; ".join(f"{who(l)} ({posture(l)})" for l in tls_listeners)
        )
        materials = {
            (opt(l, "certfile"), opt(l, "keyfile"), opt(l, "cacertfile"))
            for l in tls_listeners
        }
        if len(materials) > 1:
            dropped = "; ".join(
                f"{who(l)}: certfile = {opt(l, 'certfile') or 'unset'}, keyfile = "
                f"{opt(l, 'keyfile') or 'unset'}, cacertfile = "
                f"{opt(l, 'cacertfile') or 'unset'}"
                for l in tls_listeners
                if l is not first
            )
            todos.append(
                "those TLS listeners carry DIFFERENT TLS material, and only one set can be "
                f"referenced: {who(first)}'s certfile/keyfile/cacertfile went into [tls] "
                "below and these PEM files are referenced NOWHERE in the generated config, "
                f"while their transports are served from the material that IS referenced — "
                f"{dropped}. Reissue one certificate covering every name (a SAN per "
                "hostname), or split the listeners across separate deployments"
            )

    lines = ["[tls]"]
    cert, key = opt(first, "certfile"), opt(first, "keyfile")
    if cert:
        lines.extend(
            conv.prov.line(
                "cert", toml_str(cert), f"{who(first)}.ssl_options.certfile"
            )
        )
    else:
        lines.extend(
            conv.prov.line(
                "cert",
                toml_str("/etc/mqttd/tls/server.crt"),
                None,
                decide=f"{who(first)} is a TLS listener with NO ssl_options.certfile, so this "
                "converter has no server certificate to name and refuses to invent a path. "
                "The line below is a PLACEHOLDER: set cert to your PEM chain and uncomment "
                "it (the broker refuses to start on a tls_bind with no cert)",
            )
        )
    if key:
        lines.extend(
            conv.prov.line("key", toml_str(key), f"{who(first)}.ssl_options.keyfile")
        )
    else:
        lines.extend(
            conv.prov.line(
                "key",
                toml_str("/etc/mqttd/tls/server.key"),
                None,
                decide=f"{who(first)} named no ssl_options.keyfile, so there is nothing to "
                "put in [tls] key and the broker REFUSES to start without it. The line below "
                "is a PLACEHOLDER, not a value from your EMQX config: set key to an "
                "UNENCRYPTED PEM private key (mount it from a Secret) and uncomment it",
            )
        )
    if any(
        v and "${" in v
        for l in tls_listeners
        for v in (opt(l, "certfile"), opt(l, "keyfile"), opt(l, "cacertfile"))
    ):
        conv.note(
            "TLS paths contain ${...} interpolation (EMQX resolves EMQX_ETC_DIR and "
            "friends). mqttd does NOT expand environment variables inside config values: "
            "these paths were copied through UNRESOLVED and must be replaced with real "
            "absolute paths before deploying"
        )

    # -- the mTLS mandate, decided across EVERY TLS listener ---------------------------
    #
    # EMQX's cacertfile only VERIFIES a certificate the client CHOOSES to present unless
    # verify_peer AND fail_if_no_peer_cert are both set. mqttd's client_ca MANDATES one,
    # for every TLS transport at once. So there are three cases, and only the first is a
    # mapping (the #162 precedent: a mapping that changes SECURITY POSTURE is not a
    # mapping — emit the candidate COMMENTED OUT with a TODO instead).
    def mandatory(lst: Listener) -> bool:
        return (opt(lst, "verify") or "verify_none").lower() == "verify_peer" and truthy(
            lst.keys.get("ssl_options.fail_if_no_peer_cert", "false")
        )

    mand = [l for l in tls_listeners if mandatory(l)]
    lax = [l for l in tls_listeners if not mandatory(l)]
    with_ca = [l for l in tls_listeners if opt(l, "cacertfile")]
    ca = opt(mand[0], "cacertfile") if mand else (opt(with_ca[0], "cacertfile") if with_ca else None)

    if mand and not lax and ca:
        lines.extend(
            conv.prov.line(
                "client_ca",
                toml_str(ca),
                f"{who(mand[0])}.ssl_options.cacertfile + verify = verify_peer + "
                "fail_if_no_peer_cert = true",
            )
        )
        conv.note(
            "every TLS listener had verify = verify_peer with fail_if_no_peer_cert = true "
            f"({', '.join(who(l) for l in mand)}), so [tls] client_ca is set and mTLS is "
            "MANDATORY on tls_bind, wss_bind and quic_bind alike. mqttd additionally "
            "requires the clientAuth extended key usage on every client certificate, which "
            "OpenSSL-based brokers tolerated missing. Audit the fleet BEFORE cutover: "
            "scripts/migrate/cert-audit.sh <dir-of-client-certs>"
            + (
                "; the mandating listeners also disagree on cacertfile "
                f"({sorted({opt(l, 'cacertfile') for l in mand})}), and only "
                f"{who(mand[0])}'s was used — concatenate the anchors into one PEM if both "
                "are still in use"
                if len({opt(l, "cacertfile") for l in mand}) > 1
                else ""
            )
        )
    elif mand and lax:
        # THE fail-open case round 1 caught: an mTLS MANDATE on a listener that is not
        # first in document order used to vanish entirely. Neither arm is a translation,
        # so neither is taken silently.
        todos.append(
            "TLS listeners DISAGREE about client certificates, and mqttd cannot hold both "
            f"postures: {', '.join(who(l) for l in mand)} REQUIRED a client certificate "
            f"(verify_peer + fail_if_no_peer_cert), while {', '.join(who(l) for l in lax)} "
            f"did NOT ({'; '.join(f'{who(l)}: {posture(l)}' for l in lax)}). [tls] client_ca "
            "MANDATES mTLS for tls_bind, wss_bind and quic_bind AT ONCE — setting it "
            "newly demands certificates from clients that never presented one, and leaving "
            "it unset DROPS a mandate you have today. It is therefore emitted COMMENTED OUT "
            "in the [tls] table below: uncomment it to mandate mTLS fleet-wide (audit every "
            "client first with scripts/migrate/cert-audit.sh, and expect the cert-less "
            "clients to fail the handshake), or leave it commented and move the "
            "mTLS-required clients to a SEPARATE deployment that sets it. Do not deploy "
            "this file believing the REQUIRED listener kept its mandate"
        )
        lines.append(
            "# TODO(migrate): client certificates were REQUIRED on "
            f"{', '.join(who(l) for l in mand)} but NOT on "
            f"{', '.join(who(l) for l in lax)}; mqttd has one posture for every TLS "
            "transport. Uncommenting mandates mTLS EVERYWHERE (see the TODO above):"
        )
        if ca:
            lines.extend(
                conv.prov.inert(
                    "client_ca",
                    toml_str(ca),
                    f"from {who(mand[0])}.ssl_options.cacertfile",
                )
            )
        else:
            lines.extend(
                conv.prov.inert(
                    "client_ca",
                    toml_str("/etc/mqttd/tls/client-ca.crt"),
                    "PLACEHOLDER — no cacertfile was found on the REQUIRED listener, so this "
                    "path came from nowhere in your EMQX config; supply the anchors",
                )
            )
    elif with_ca:
        lines.append(
            "# TODO(migrate): a cacertfile was set but client certificates were NOT "
            "mandatory on any TLS listener ("
            + "; ".join(f"{who(l)}: {posture(l)}" for l in with_ca)
            + "). mqttd's client_ca MANDATES mTLS — there is no cert-optional mode — and it "
            "applies to tls_bind, wss_bind and quic_bind at once. Uncomment to require "
            "certificates fleet-wide (audit them first with scripts/migrate/cert-audit.sh), "
            "or leave it commented for server-only TLS:"
        )
        emitted: set[str] = set()
        for lst in with_ca:
            candidate = opt(lst, "cacertfile")
            if not candidate or candidate in emitted:
                continue
            emitted.add(candidate)
            lines.extend(
                conv.prov.inert(
                    "client_ca",
                    toml_str(candidate),
                    f"from {who(lst)}.ssl_options.cacertfile",
                )
            )
    elif mand:
        todos.append(
            f"{', '.join(who(l) for l in mand)} REQUIRED a client certificate "
            "(verify_peer + fail_if_no_peer_cert) but named NO cacertfile, so this "
            "converter has no trust anchor to put in [tls] client_ca and mTLS is NOT "
            "mandated below. Find the CA bundle EMQX was verifying against (an OS trust "
            "store? a default?) and set client_ca to it, or the mandate is gone"
        )

    # -- per-listener protocol versions and OCSP, each NAMED --------------------------
    for lst in tls_listeners:
        versions = lst.keys.get("ssl_options.versions")
        if versions is not None:
            vs = [str(v).strip().strip('"').lower() for v in _as_list(versions)]
            if any("1.2" in v or "1_2" in v for v in vs):
                todos.append(
                    f"{who(lst)} accepted TLS 1.2 (versions = {vs}). mqttd is TLS 1.3 ONLY "
                    "by default and a 1.2-only client fails to connect in a way that looks "
                    "like a network fault. If your fleet needs it, opt in with [tls] "
                    "allow_tls12 = true — hardened (ECDHE+AEAD only, Extended Master Secret "
                    "required), loudly logged, and applied to every TLS transport — and "
                    "plan its retirement"
                )
            if not any("1.3" in v or "1_3" in v for v in vs):
                todos.append(
                    f"{who(lst)} did NOT list TLS 1.3 (versions = {vs}); mqttd's default "
                    "listener speaks 1.3 only. Every client on that transport must "
                    "negotiate 1.3, or [tls] allow_tls12 must be set explicitly"
                )
        stapling = lst.keys.get("ssl_options.ocsp.enable_ocsp_stapling")
        if stapling is not None and truthy(stapling):
            todos.append(
                f"{who(lst)} ENABLED OCSP stapling, which is not implemented in mqttd. "
                "Revocation is a CRL file ([tls] crl, hot-reloaded on SIGHUP, which also "
                "evicts live sessions of a revoked client) — set up CRL publication before "
                "you retire OCSP"
            )

    # -- every remaining ssl_option, on EVERY listener, reported by listener ----------
    consumed = {
        "certfile",
        "keyfile",
        "cacertfile",
        "verify",
        "fail_if_no_peer_cert",
        "versions",
    }
    for lst in tls_listeners:
        for path in lst.keys:
            if not path.startswith("ssl_options."):
                continue
            rest = path[len("ssl_options.") :]
            if rest in consumed or rest.startswith("ocsp."):
                continue
            reason = LISTENER_NO_EQUIVALENT.get(path)
            # The VALUE is quoted because `enable_crl_check = true` and `depth = 3` are
            # the whole point — except where the value is a secret, which this tool never
            # copies anywhere (ADR 0051 §3 rule 2).
            shown = (
                "was set (its value is NOT copied — secrets are never transformed)"
                if any(s in rest.lower() for s in ("password", "passphrase", "secret"))
                else f"= {lst.keys[path]!r}"
            )
            todos.append(
                f"{who(lst)}.{path} {shown}: "
                + (reason or "no direct equivalent — check the mqttd configuration table")
            )
    return lines, todos


def _as_list(value) -> list:
    if isinstance(value, list):
        return value
    return [p.strip() for p in str(value).replace('"', "").split(",") if p.strip()]


# ---------------------------------------------------------------------------
# Every OTHER key on a LIVE authenticator.
#
# convert_authn() read only the keys each mechanism branch names, so everything else on an
# authenticator that WAS translated vanished with no TODO: on a live password_based/http
# entry, `method` (REQUIRED in the vendor schema, and it decides GET vs POST), `headers`
# (where the `x-api-key` or `Authorization: Bearer` that authorizes the broker TO the endpoint
# lives), `body`, `pool_size`, `enable_pipelining` and the WHOLE client-TLS block
# (`ssl.cacertfile`, `ssl.certfile`, `ssl.keyfile`, `ssl.verify`,
# `ssl.server_name_indication`) produced not one line of output — while the generated file's
# own header said settings with no equivalent are listed rather than dropped.
#
# The consequence is not cosmetic: without the private-CA anchor mqttd cannot verify an HTTPS
# authn endpoint, and mqttd treats an unreachable endpoint as DENY, so EVERY client fails to
# connect with nothing in the migrated config hinting why. Round 1 added BRIDGE_NO_EQUIVALENT
# for exactly this shape and it was never applied to the authenticator chain — the one
# construct whose failure means nobody can log in. Found 2026-08-15.
# ---------------------------------------------------------------------------

# What each branch of convert_authn() actually consumes. Anything else on the entry is
# reported by name AND value.
AUTHN_COMMON_READ = frozenset({"mechanism", "backend", "enable", "enabled"})
AUTHN_HTTP_READ = AUTHN_COMMON_READ | frozenset({"url", "request_timeout"})
AUTHN_JWT_READ = AUTHN_COMMON_READ | frozenset(
    {
        "secret",
        "public_key",
        "jwks",
        "endpoint",
        "from",
        "use_jwks",
        "acl_claim_name",
        # `verify_claims` IS read by the jwt branch, claim by claim: `iss`/`aud` become
        # [security.jwt] issuer/audience with a NOTE, and every other claim becomes its own
        # TODO. Omitting it here made `report_unread_authn_keys` enumerate its LEAVES, so the
        # same document emitted `issuer = "…"  # from: … verify_claims.iss` AND a TODO saying
        # that exact claim has "no mqttd equivalent" — round 3's contradiction class, in the
        # opposite direction, introduced by round 3's own remediation. The guard it relied on
        # (an AUTHN_NO_EQUIVALENT entry for `verify_claims`) could never fire, because the
        # lookup tested the full path and the leaf and never the parent. Found 2026-08-15.
        "verify_claims",
    }
)

# THE CREDENTIAL STORE, named. `convert_authz` already names a non-file authz source's store and
# query, with the reason spelled out in its own TODO — "it is named here so you can check that the
# source you believe was in force is the source this converter could not translate" — and the
# authn side said nothing at all until 2026-08-15, on the backend the repository's OWN pinned
# fixture exercises.
_STORE = (
    "this NAMES THE CREDENTIAL STORE the authenticator read. mqttd cannot query it: the "
    "supported path is [security.http_auth] — one HTTP hook whose STATUS CODE is the verdict — "
    "in front of the same store, or re-enrolment into an Argon2id password file. It is named "
    "here so you can check that the store you believe was in force is the store this converter "
    "could not translate"
)

# The keys whose loss changes whether authentication works at all, with the reason, so the
# TODO is a repair instruction rather than a list.
AUTHN_NO_EQUIVALENT: dict[str, str] = {
    "method": "mqttd's HTTP authenticator POSTs a JSON body and reads the STATUS CODE; the "
    "verb is not configurable, so a GET-based endpoint must accept POST",
    "headers": "mqttd sends no custom headers — THIS IS WHERE A SHARED SECRET LIVES "
    "(x-api-key, Authorization: Bearer). If your endpoint requires one it will refuse "
    "mqttd's request, and mqttd reads a refusal as DENY, so EVERY client fails to connect. "
    "Put the check behind a gateway that adds the header, or drop the requirement",
    "body": "the request body is fixed (mqttd sends the identity, the client id and the "
    "password); a templated body is not configurable",
    "pool_size": "there is no connection pool to size",
    "enable_pipelining": "there is no pipelining setting",
    "check_ssl_opts": "not applicable — see the ssl.* keys",
    "check_headers": "not applicable — mqttd sends no custom headers",
    "ssl.enable": "mqttd uses https:// in the URL to decide, not a flag",
    "ssl.cacertfile": "mqttd verifies an https:// authn endpoint against the SYSTEM trust "
    "store and has no per-authenticator CA setting. A PRIVATE CA therefore cannot be named "
    "here: install it in the container's trust store (or terminate TLS at a sidecar), "
    "because an endpoint mqttd cannot verify is unreachable, and an unreachable endpoint is "
    "DENY for every client",
    "ssl.certfile": "mqttd presents no client certificate to the authn endpoint; mTLS "
    "TOWARD the endpoint is not supported, so an endpoint that requires it will refuse "
    "mqttd and every client will be denied",
    "ssl.keyfile": "mqttd presents no client certificate to the authn endpoint",
    "ssl.verify": "not configurable — mqttd always verifies an https:// endpoint",
    "ssl.server_name_indication": "SNI is taken from the URL's host",
    "algorithm": "the algorithm follows from which key material you configure "
    "([security.jwt] hs256_secret_file or rs256_pem_file); it is not named separately",
    "disconnect_after_expire": "mqttd does NOT disconnect a client when its token expires — "
    "the token is checked at CONNECT only, and the session lives until it disconnects or is "
    "evicted. A revoked or expired credential therefore keeps its session: shorten session "
    "expiry, or evict the client deliberately",
    "on_missing_jwt": "there is no fall-through: a client with no token is refused unless "
    "[security] allow_anonymous is set. `ignore` in EMQX meant the chain CONTINUED to the "
    "next authenticator, which is a chain mqttd cannot express at all",
    # `verify_claims` used to carry a "handled — …" entry here, relied on to suppress the
    # per-claim reports. It could never fire: the lookup below tests the full path
    # (`verify_claims.iss`) and the leaf (`iss`), never the parent, so the suppression was dead
    # code and the claims were reported as unmapped beside the live mapping. It is now in
    # AUTHN_JWT_READ, which is where "this branch consumes it" belongs. Found 2026-08-15.
    # THE CREDENTIAL STORE, named. `convert_authz` already names a non-file authz source's store
    # and query, with the reason spelled out in its own TODO — "it is named here so you can check
    # that the source you believe was in force is the source this converter could not translate"
    # — and the authn side said nothing at all until 2026-08-15.
    "server": _STORE,
    "servers": _STORE,
    "database": _STORE,
    "query": _STORE,
    "cmd": _STORE,
    "collection": _STORE,
    "selector": _STORE,
    "filter": _STORE,
    "base_dn": _STORE,
    "bind_dn": _STORE,
    "salt_position": "mqttd's password files are Argon2id, which carries its own salt",
    "password_hash_algorithm": "mqttd's password files are Argon2id ONLY, and existing "
    "hashes cannot be converted",
}


def report_unread_authn_keys(
    conv: Conversion, entry: dict, label: str, consumed: frozenset[str]
) -> None:
    """Name every key on a LIVE authenticator that this converter did not read."""
    for path, value in leaves(entry):
        if path in consumed or path.split(".", 1)[0] in consumed:
            continue
        leaf = path.rsplit(".", 1)[-1]
        shown = (
            "was set (its value is NOT copied — secrets are never transformed)"
            if any(sec in leaf.lower() for sec in ("password", "passphrase", "secret"))
            else f"= {value!r}"
        )
        reason = AUTHN_NO_EQUIVALENT.get(path) or AUTHN_NO_EQUIVALENT.get(leaf)
        if reason is None:
            # ...and then an ANCESTOR's entry, so a block whose whole meaning is explained by
            # one row (`password_hash_algorithm` for `password_hash_algorithm.name`) does not
            # fall through to the generic sentence. The missing parent lookup is what made the
            # `verify_claims` suppression dead code.
            parts = path.split(".")
            for i in range(len(parts) - 1, 0, -1):
                reason = AUTHN_NO_EQUIVALENT.get(".".join(parts[:i]))
                if reason:
                    break
        conv.todo(
            f"authentication [{label}].{path} {shown}: "
            + (
                reason
                or "no mqttd equivalent — mqttd's authenticators are an Argon2id password "
                "file, a static-key JWT, OIDC, mTLS and one HTTP hook whose verdict is the "
                "status code, and nothing else. Check docs/MIGRATION.md and decide whether "
                "this setting mattered"
            )
        )


def convert_authn(tree: dict, conv: Conversion) -> None:
    entries = tree.get("authentication")
    if entries is None:
        return
    conv.saw_auth = True
    # EMQX's authentication is an ORDERED CHAIN and may hold two entries of the same kind
    # (two HTTP endpoints, two JWT issuers). mqttd has ONE [security.http_auth] and ONE
    # [security.jwt], so a second entry of a kind used to overwrite the first in silence —
    # the same "one of N was used and nobody said so" class as the TLS listeners. `claimed`
    # remembers which mqttd table an earlier entry already took.
    claimed: dict[str, str] = {}

    def claim(table: str, label: str, detail: str) -> bool:
        """True if `table` is free. Otherwise report the collision and translate nothing."""
        if table in claimed:
            conv.todo(
                f"authentication [{label}] is the SECOND entry to need {table}, and mqttd "
                f"has only one: {claimed[table]} was translated and THIS one was NOT"
                + (f" ({detail})" if detail else "")
                + ". EMQX walks the chain in order and stops at the first authenticator that "
                "answers, so both were reachable; mqttd cannot express the chain. Keep the "
                "one that must survive, and move the other behind a single "
                "[security.http_auth] adapter you write, or into OIDC"
            )
            return False
        claimed[table] = label
        return True

    for entry in entries if isinstance(entries, list) else [entries]:
        if not isinstance(entry, dict):
            conv.todo(
                f"authentication entry {entry!r} is not a block this converter "
                "understands — translate it by hand"
            )
            continue
        mech = str(entry.get("mechanism", "")).strip().strip('"').lower()
        backend = str(entry.get("backend", "")).strip().strip('"').lower()
        label = f"{mech}/{backend}" if backend else mech
        # `enable = false` is how EMQX's dashboard switches an authenticator off without
        # deleting it, so a decommissioned chain entry is a normal thing to find in a real
        # emqx.conf. Translating it as if it were live ENABLES it — the migrated broker
        # would consult a legacy endpoint the operator believes is off, and if that endpoint
        # still answers 200 every client authenticates. Enabling an authenticator is a
        # posture change, so it is reported and NOT translated.
        if any(k in entry for k in ("enable", "enabled")) and falsey(
            entry.get("enable", entry.get("enabled"))
        ):
            # NAMED, not just counted. Reporting "an http authenticator was disabled" without
            # saying WHICH endpoint leaves the operator unable to check whether the thing
            # they think is switched off is the thing that was switched off — the same
            # "reported but not identifiable" gap the TLS inventory had.
            identifying = "; ".join(
                f"{k} = {v}"
                for k, v in sorted(entry.items())
                if k
                in ("url", "endpoint", "jwks", "server", "database", "from", "algorithm")
            )
            conv.todo(
                f"authentication [{label or entry!r}] has enable = false: it was DISABLED "
                "in EMQX, so it was NOT translated and nothing below activates it"
                + (f" — it pointed at {identifying}" if identifying else "")
                + ". If it should be live, re-run against a config where it is enabled, or "
                "configure the mqttd equivalent deliberately — a converter that switched a "
                "decommissioned authenticator back ON would be a silent posture change, and "
                "an endpoint still answering 200 under a different contract would then "
                "admit every client"
            )
            continue
        if mech == "password_based" and backend in ("built_in_database", "mnesia", ""):
            if not claim("[security] password_file", label or "password_based", ""):
                continue
            conv.set(
                "security",
                "password_file",
                toml_str("/etc/mqttd/passwd"),
                f"authentication [{label or 'password_based'}] (the USERS are in EMQX's "
                "built-in database, which this converter cannot read; the path is this "
                "converter's own re-enrolment default)",
                defaulted="the path itself, which is yours to choose",
            )
            conv.todo(
                "authentication [password_based/built_in_database]: EMQX's built-in user "
                "table lives in the data directory / the REST API, not in any config "
                "file, and its sha256/bcrypt/pbkdf2 hashes are NOT Argon2id — they cannot "
                "be converted (they are hashes; the passwords are not recoverable). Export "
                "the usernames, then re-enrol each one: "
                "`printf %s '<password>' | mqttd --hash-password <username> >> "
                "/etc/mqttd/passwd`. [security] password_file above points at that file"
            )
            report_unread_authn_keys(conv, entry, label or "password_based", AUTHN_COMMON_READ)
            continue
        if mech == "password_based" and backend == "http":
            url = entry.get("url")
            if not claim(
                "[security.http_auth]", label, f"url = {url}" if url else ""
            ):
                continue
            if url:
                conv.set(
                    "security.http_auth",
                    "url",
                    toml_str(str(url).strip().strip('"')),
                    f"authentication [{label}] url",
                )
            timeout = as_seconds(entry.get("request_timeout", ""))
            if timeout:
                conv.set("security.http_auth", "timeout_secs", str(timeout))
            report_unread_authn_keys(conv, entry, label, AUTHN_HTTP_READ)
            conv.todo(
                "authentication [password_based/http]: the URL was carried into "
                "[security.http_auth], but THE CONTRACT IS DIFFERENT and your endpoint "
                "almost certainly needs a change. EMQX reads a JSON body "
                '(`{\"result\":\"allow\"}`); mqttd reads the HTTP STATUS CODE — 200 allow, '
                '401/403 deny, anything else (or a timeout, or an unreachable host) DENY. '
                'An optional `{\"groups\":[...]}` body enriches the identity. Verify the '
                "status codes your endpoint returns today before you trust this"
            )
            continue
        if mech == "password_based":
            conv.todo(
                f"authentication [{label}]: mqttd has no {backend} authentication "
                "backend. The supported path is [security.http_auth] — one HTTP hook, "
                "status-code-is-the-verdict, reaching any store you already run. YOU must "
                f"write the small adapter that queries {backend}; it is operator code, not "
                "a shipped feature. Native options are the Argon2id password file, JWT, "
                "OIDC and mTLS"
            )
            # THE CREDENTIAL STORE ITSELF, named. Reporting "mqttd has no mysql backend"
            # without saying WHICH server, database and query it read leaves the operator
            # unable to check that the source they believe was in force is the source this
            # converter could not translate — and this branch is what the repository's OWN
            # pinned fixture exercises (emqx-6.2.2.conf's `backend = mysql`), so
            # `server = "mysql:3306"` and its SELECT appeared NOWHERE in the output while a
            # CI fixture test asserted that nothing is silently dropped. Found 2026-08-15.
            report_unread_authn_keys(conv, entry, label, AUTHN_COMMON_READ)
            continue
        if mech == "jwt":
            # acl_claim_name is an ENTIRE AUTHORIZATION SOURCE, and it used to vanish in
            # total silence: no TODO, no NOTE, no mention of the claim name anywhere, while
            # the same run wrote [security] acl_file and an ACL that presents itself as the
            # policy. The field is real at the pinned tag
            # (apps/emqx_auth_jwt/src/emqx_authn_jwt_schema.erl:131 @ 6.2.2) and the vendor's
            # i18n documents it as "The JWT claim designated for accessing ACL rules",
            # carrying pub/sub/all rules with `eq` matching. Found 2026-08-15.
            if entry.get("acl_claim_name") is not None:
                claim_name = str(entry["acl_claim_name"]).strip().strip('"')
                conv.todo(
                    f"authentication [jwt] acl_claim_name = {claim_name!r}"
                    ": PER-CLIENT AUTHORIZATION WAS DELIVERED INSIDE THE TOKEN, and mqttd "
                    "cannot express that at all — its ACL policy is one file, evaluated "
                    "in-process, and it reads NO rules from a JWT claim. Those rules are NOT "
                    "in the generated ACL and nothing else in this migration replaces them, "
                    "so after cutover those clients are governed ONLY by the file policy: "
                    "they will be locked out where the token granted more, and OVER-PERMITTED "
                    "where the token granted less. The nearest thing mqttd has is GROUPS on "
                    "the identity — an OIDC token's groups_claim, matched by ACL rules with "
                    "`groups = [...]` — which expresses membership, not per-client topic "
                    "rules. Re-model the policy that way, or keep EMQX for those clients"
                )
            if entry.get("jwks") or entry.get("endpoint") or truthy(entry.get("use_jwks")):
                # THE ENDPOINT IS NAMED, because "with JWKS" without the URL left the
                # operator unable to check that the provider this converter is telling
                # them to reconfigure around is the provider they believe was in force —
                # the same rule report_unread_authn_keys() already applies to every other
                # authenticator's server/database/url. Found via issue #297.
                raw_ep = entry.get("endpoint") or entry.get("jwks")
                jwks_url = str(raw_ep).strip().strip('"')
                if not jwks_url or jwks_url.lower() in ("true", "false"):
                    # `use_jwks`/`jwks` was switched on but no URL accompanied it — say
                    # so rather than print a boolean where a URL belongs.
                    jwks_url = "(no URL in the input — only the switch)"
                conv.todo(
                    f"authentication [jwt] fetched its keys from the JWKS endpoint "
                    f"{jwks_url}: mqttd's JWKS path is the SEPARATE [security.oidc] "
                    "authenticator (issuer, audience, jwks_refresh_secs, "
                    "max_stale_secs, groups_claim) — it discovers the JWKS from the "
                    "issuer rather than taking a URL. Set [security.oidc] issuer to the "
                    "identity provider that serves that endpoint; [security.jwt] is for "
                    "STATIC keys only"
                )
            if entry.get("secret") is not None:
                conv.todo(
                    "authentication [jwt] carried an INLINE HS256 secret. This converter "
                    "does not copy secret material into its output (secrets are never "
                    "transformed). Write the secret to a file yourself and set "
                    "[security.jwt] hs256_secret_file to its path"
                )
            if entry.get("public_key") is not None:
                conv.todo(
                    "authentication [jwt] carried an RS256 public key. Write the PEM to a "
                    "file and set [security.jwt] rs256_pem_file to its path (mqttd "
                    "references key material BY PATH, never inline)"
                )
            # ISSUER AND AUDIENCE COME FROM verify_claims, because EMQX HAS NO `issuer` OR
            # `audience` FIELD. This code read `entry["issuer"]` / `entry["audience"]` and
            # docs/MIGRATION.md carried a mapping row for them; re-fetching
            # apps/emqx_auth_jwt/src/emqx_authn_jwt_schema.erl at tag 6.2.2 shows the JWT
            # authenticator's fields are mechanism, acl_claim_name, on_missing_jwt,
            # verify_claims, disconnect_after_expire, from, plus per-type
            # use_jwks/algorithm/secret/public_key/endpoint — `grep -cE 'issuer|audience'`
            # returns 0. So the old mapping could only ever fire on input EMQX cannot produce,
            # while the constraint an EMQX operator really writes —
            # `verify_claims = { iss = ..., aud = ... }`, a MAP of claim name to expected
            # value (verify_claims/1 in that file) — was dropped and described as
            # unimplementable. Those two claims are exactly what mqttd verifies
            # (crates/mqtt-auth/src/token.rs), and only when configured.
            claims = entry.get("verify_claims")
            if not claim("[security.jwt]", label, ""):
                continue
            mapped_claims: list[str] = []
            leftover: list[str] = []
            if isinstance(claims, dict):
                for cname, cvalue in claims.items():
                    key = str(cname).strip().strip('"').lower()
                    shown = str(cvalue).strip().strip('"')
                    if key in ("iss", "issuer"):
                        conv.set(
                            "security.jwt",
                            "issuer",
                            toml_str(shown),
                            f"authentication [{label}] verify_claims.{key}",
                        )
                        mapped_claims.append(f"{key} -> [security.jwt] issuer")
                    elif key in ("aud", "audience"):
                        conv.set(
                            "security.jwt",
                            "audience",
                            toml_str(shown),
                            f"authentication [{label}] verify_claims.{key}",
                        )
                        mapped_claims.append(f"{key} -> [security.jwt] audience")
                    else:
                        leftover.append(f"{key} = {shown}")
            elif claims:
                leftover.append(str(claims))
            if mapped_claims:
                conv.note(
                    "authentication [jwt] verify_claims: "
                    + ", ".join(mapped_claims)
                    + ". Those are the only two claims mqttd verifies, and it verifies them "
                    "ONLY when they are set — so an unset one means any issuer or audience "
                    "with a valid signature is accepted"
                )
            if leftover:
                conv.todo(
                    "authentication [jwt] verify_claims constrains claim(s) mqttd cannot "
                    "check: "
                    + "; ".join(leftover)
                    + ". mqttd verifies the signature, the expiry, and `iss`/`aud` "
                    "([security.jwt] issuer/audience) — arbitrary claim constraints are NOT "
                    "implemented, so those constraints are GONE and a token that satisfies "
                    "only the signature is now admitted. Move the check into "
                    "[security.http_auth], or narrow the issuer so the claim cannot vary"
                )
            # A top-level `issuer`/`audience` is NOT a field in EMQX's JWT authenticator at
            # the pinned tag (emqx_authn_jwt_schema.erl has verify_claims, and
            # `grep -cE 'issuer|audience'` returns 0), so EMQX itself would have ignored it.
            # This loop ran AFTER the verify_claims mapping above and re-`set` the same two
            # keys, so the schema-INVALID value silently DISPLACED the constraint the source
            # really enforced — under a NOTE that claimed verify_claims had been carried over.
            # Round 3's own remediation introduced that; the fix is that a claim mqttd will
            # verify is only ever written from verify_claims, and the dead field is reported
            # as the commented candidate it is. Found 2026-08-15.
            for dead in ("issuer", "audience"):
                if entry.get(dead) is None:
                    continue
                already = conv.config.get("security.jwt", {}).get(dead)
                conv.todo(
                    f"authentication [jwt] carried a top-level `{dead}` "
                    f"({entry[dead]}), which is NOT a field in EMQX's JWT authenticator "
                    "schema at the pinned tag (emqx_authn_jwt_schema.erl has verify_claims, "
                    "not issuer/audience), so EMQX itself would have ignored it and it "
                    "constrained NOTHING. "
                    + (
                        f"[security.jwt] {dead} therefore keeps the value verify_claims "
                        f"really enforced ({already.rendered}); the dead field is emitted "
                        "COMMENTED OUT below. Check which file EMQX was actually running"
                        if already is not None
                        else "It is emitted COMMENTED OUT in [security.jwt] below rather than "
                        "activated: uncomment it only if you know that constraint is the one "
                        "you want, remembering the source never applied it"
                    )
                )
                conv.defer(
                    "security.jwt",
                    conv.prov.inert(
                        dead,
                        toml_str(str(entry[dead]).strip().strip('"')),
                        f"from a top-level `{dead}`, which EMQX's schema does not have — NOT "
                        "activated",
                    ),
                )
            if entry.get("from"):
                conv.todo(
                    "authentication [jwt] `from`: mqttd reads the token from the CONNECT "
                    "password field; taking it from the username is not configurable"
                )
            report_unread_authn_keys(conv, entry, label, AUTHN_JWT_READ)
            continue
        if mech == "scram":
            conv.todo(
                "authentication [scram]: SCRAM / MQTT 5 enhanced authentication with "
                "SCRAM is not implemented. Move those clients to password+TLS, mTLS, or a "
                "JWT/OIDC token"
            )
            report_unread_authn_keys(conv, entry, label or "scram", AUTHN_COMMON_READ)
            continue
        conv.todo(
            f"authentication [{label or entry!r}]: not a mechanism this converter knows — "
            "check the mqttd authentication options (password file, JWT, OIDC, mTLS, HTTP "
            "hook) and translate it by hand"
        )
        # ...and every key it carried, by name: an unknown mechanism is exactly the entry whose
        # contents nobody can guess from the label.
        report_unread_authn_keys(conv, entry, label or str(entry), AUTHN_COMMON_READ)


def convert_authz(tree: dict, conv: Conversion) -> None:
    authz = tree.get("authorization")
    if not isinstance(authz, dict):
        return
    conv.saw_authz = True
    no_match = str(authz.get("no_match", "deny")).strip().strip('"').lower()
    conv.acl_default_source = (
        "authorization.no_match"
        if authz.get("no_match") is not None
        else "authorization (no `no_match` was set; EMQX's own documented default for that "
        "field is deny)"
    )
    if no_match == "allow":
        conv.acl_default = "allow"
        conv.note(
            "authorization.no_match was ALLOW, so the translated ACL is "
            'default = "allow" — FAITHFUL to EMQX and the opposite of mqttd\'s posture. '
            "Anything your rules do not deny is permitted, including topics no client of "
            'yours has ever used. Move to default = "deny" as soon as your allow rules '
            "are complete; that is the single highest-value change in this migration"
        )
    for path, value in leaves(authz):
        if path in ("no_match",):
            continue
        if path.startswith("sources"):
            continue
        reason = KEY_NO_EQUIVALENT.get(f"authorization.{path}")
        if reason:
            conv.todo(f"authorization.{path}: {reason}")
        else:
            conv.todo(
                f"authorization.{path}: no direct equivalent — mqttd's authorization is "
                "one TOML policy file ([security] acl_file), evaluated in-process"
            )
    sources = authz.get("sources")
    for src in sources if isinstance(sources, list) else ([sources] if sources else []):
        if not isinstance(src, dict):
            conv.todo(
                f"authorization.sources entry {src!r} is not a block this converter "
                "understands — translate it by hand"
            )
            continue
        stype = str(src.get("type", "")).strip().strip('"').lower()
        # Same trap as a disabled authenticator, one section over: a disabled authz source
        # was not consulted by EMQX, so treating it as the live policy would be an
        # invention. Reported, not used.
        if any(k in src for k in ("enable", "enabled")) and falsey(
            src.get("enable", src.get("enabled"))
        ):
            named = src.get("path") or src.get("url") or src.get("server")
            conv.todo(
                f"authorization.sources [{stype or '?'}"
                + (f" {named}" if named else "")
                + "] has enable = false: the source was "
                "DISABLED in EMQX, so it was NOT used as the policy for the translated ACL "
                "and its rules are not below. If it should be live, enable it and re-run "
                "(or pass --acl-file explicitly, which always wins) — and check what the "
                "REMAINING enabled sources actually authorized, because a chain with one "
                "entry switched off usually has another entry doing the work"
            )
            continue
        if stype == "file":
            path = src.get("path")
            if path:
                resolved = str(path).strip().strip('"')
                # EMQX's authorization.sources is an ORDERED CHAIN, first match wins, and
                # several `file` sources are legal. This used to assign conv.acl_file in a
                # loop, so the LAST file source silently won and every earlier one's rules
                # vanished with no marker. The FIRST is now kept (the chain's own precedence)
                # and every other one is named.
                if conv.acl_file is None:
                    conv.acl_file = resolved
                else:
                    conv.todo(
                        f"authorization.sources names MORE THAN ONE `file` source, and only "
                        f"one policy file can be translated: {conv.acl_file} (the FIRST, which "
                        "is the one EMQX consults first in the chain) was used, and NOT ONE "
                        f"RULE from {resolved} is in the generated ACL. Concatenate the files "
                        "in chain order and re-run with --acl-file pointing at the result, "
                        "remembering that EMQX stops at the first matching rule while mqttd "
                        "evaluates every rule with deny winning"
                    )
            continue
        if stype in ("built_in_database", "mnesia"):
            conv.todo(
                "authorization.sources [built_in_database]: EMQX's built-in ACL table "
                "lives in the data directory / the REST API, not in a config file, so "
                "THIS CONVERTER CANNOT SEE IT. Export those rules (REST "
                "/api/v5/authorization/sources/built_in_database/rules) and translate "
                "them into the ACL policy by hand — otherwise you will deploy missing "
                "every rule that lived there"
            )
            continue
        # WHICH source, not just its type. A live `{ type = http, url = ..., ssl.cacertfile
        # = ... }` or `{ type = redis, server = ..., cmd = "HGETALL mqtt_acl:${username}" }`
        # was reported as "[http]" / "[redis]" with the URL, the server, the query itself and
        # the TLS anchors appearing NOWHERE — so an operator could not check that the source
        # they believe is in force is the source that was dropped. Round 2 closed exactly this
        # for the TLS inventory and the DISABLED authenticator; it was left open one function
        # over, on the LIVE ones. Found 2026-08-15.
        identifying = "; ".join(
            f"{k} = {v!r}"
            for k, v in leaves(src)
            if k not in ("type", "enable", "enabled")
            and not any(sec in k.lower() for sec in ("password", "passphrase", "secret"))
        )
        if identifying and stype not in ("file",):
            conv.todo(
                f"authorization.sources [{stype or '?'}] read from: {identifying}. NONE of "
                "that is in the generated ACL — it is named here so you can check that the "
                "source you believe was in force is the source this converter could not "
                "translate. Note that any `ssl.*` anchors it lists are gone too, so even a "
                "hand-written replacement has to re-establish that trust itself"
            )
        if stype == "http":
            conv.todo(
                "authorization.sources [http]: there is no per-request authorization "
                "hook. mqttd authorizes from the loaded ACL policy only. The available "
                "dynamic input is GROUPS on the identity — from an OIDC token's "
                "groups_claim or the [security.http_auth] response body — which ACL rules "
                "can match with `groups = [...]`. Redesign the policy around groups, or "
                "keep the rules in the file and reload with SIGHUP"
            )
            continue
        if stype:
            conv.todo(
                f"authorization.sources [{stype}]: mqttd has no {stype} authorization "
                "backend. Either express the policy in the ACL file, or model it as "
                "GROUPS delivered by the OIDC/HTTP authenticator and matched with "
                "`groups = [...]` rules"
            )


def convert_mqtt_and_misc(tree: dict, conv: Conversion) -> None:
    """Everything outside listeners/authn/authz/bridges: map, report, or catch."""
    handled_sections = {
        "listeners",
        "authentication",
        "authorization",
        "cluster",
        "bridges",
        "connectors",
        "actions",
        "sources",
    }
    section_counts: dict[str, int] = {}
    # Security-relevant leaves inside a section reported by COUNT. A section-level
    # "[gateway] (5 setting(s) found)" swallowed `gateway.coap.listeners.dtls.default.bind`
    # and its `dtls_options.cacertfile`, `dashboard.listeners.https.bind` and its
    # `ssl_options.certfile`, `psk_authentication.init_file` and `api_key.bootstrap_file` —
    # listener ADDRESSES and CA/certificate material, i.e. squarely inside the set the
    # provenance invariant covers, reduced to a number. Nothing here fails open (mqttd cannot
    # serve those protocols at all), but an operator diffing their deployment against the
    # output could not see that a DTLS-terminating gateway port and its trust anchors ever
    # existed. Found 2026-08-15.
    section_security: dict[str, list[str]] = {}
    security_leaf = ("bind", "certfile", "keyfile", "cacertfile", "init_file",
                     "bootstrap_file", "password_file", "acl_file", "keystore", "truststore")
    for path, value in leaves(tree):
        top = path.split(".", 1)[0]
        if top in handled_sections and top not in SECTION_NO_EQUIVALENT:
            continue
        if top in SECTION_NO_EQUIVALENT:
            section_counts[top] = section_counts.get(top, 0) + 1
            leaf = path.rsplit(".", 1)[-1]
            if any(leaf.endswith(k) for k in security_leaf) and not any(
                sec in leaf.lower() for sec in ("password", "passphrase", "secret")
            ):
                section_security.setdefault(top, []).append(f"{path} = {value!r}")
            continue
        if path in DIRECT:
            section, mkey, kind = DIRECT[path]
            raw = str(value).strip().strip('"')
            if raw == "infinity":
                conv.note(
                    f"{path} was `infinity`; mqttd leaves {section}.{mkey} UNSET, which "
                    "is also uncapped. Set it deliberately — docs/SIZING.md has the "
                    "arithmetic for a fixed RAM/disk budget"
                )
                continue
            if kind == "bytes":
                n = as_bytes(raw)
                if n is None:
                    conv.todo(f"{path} = {raw}: not a size this converter can normalise")
                else:
                    conv.set(section, mkey, str(n))
            elif kind == "u16":
                if not raw.isdigit():
                    conv.todo(f"{path} = {raw}: not an integer")
                else:
                    n = min(int(raw), 65535)
                    conv.set(section, mkey, str(n))
                    if int(raw) > 65535:
                        conv.todo(
                            f"{path} = {raw} exceeds the MQTT 5 16-bit field that "
                            f"{section}.{mkey} maps to; clamped to 65535"
                        )
            elif kind == "int0":
                if not raw.isdigit():
                    conv.todo(f"{path} = {raw}: not an integer")
                elif int(raw) == 0:
                    conv.note(
                        f"{path} = 0 means UNLIMITED in EMQX, so {section}.{mkey} was "
                        "left unset (also unlimited). An uncapped retained set is a "
                        "memory-growth path — consider capping it (docs/SIZING.md)"
                    )
                else:
                    conv.set(section, mkey, raw)
            elif kind == "int":
                if not raw.isdigit():
                    conv.todo(f"{path} = {raw}: not an integer")
                else:
                    conv.set(section, mkey, raw)
            else:
                conv.set(section, mkey, toml_str(raw))
            continue
        if path == "mqtt.max_inflight":
            # NOT a mapping, and it looks exactly like one. EMQX's max_inflight bounds
            # messages the BROKER may have unacked TOWARD a client (outbound); mqttd's
            # [limits] receive_maximum is the MQTT 5 Receive Maximum it GRANTS clients —
            # the INBOUND window (crates/mqtt-config/src/lib.rs). Mapping one to the other
            # cut the inbound window from mqttd's 256 to EMQX's shipped 32 on every stock
            # conversion, throttling publishers after cutover, while leaving the outbound
            # window the operator actually configured untouched. Found 2026-08-14.
            raw = str(value).strip().strip('"')
            conv.todo(
                f"mqtt.max_inflight = {raw} was NOT mapped, deliberately. It bounds the "
                "messages EMQX may have in flight TOWARD a client (outbound); mqttd has no "
                "outbound-window setting at all — it honours each v5 client's OWN Receive "
                "Maximum from CONNECT and treats a v3.1.1 client as unlimited (ADR 0012). "
                "The similarly named [limits] receive_maximum is the OPPOSITE direction: "
                "the inbound window mqttd GRANTS clients, default 256. Mapping this value "
                "onto it would have silently cut your inbound QoS>0 window (EMQX ships 32) "
                "and throttled publishers after cutover. If you want to bound the inbound "
                "window, set it deliberately: # receive_maximum = <messages>"
            )
            continue
        if path == "node.name":
            raw = str(value).strip().strip('"')
            node_id = raw.split("@", 1)[0] or "node-1"
            conv.set("node", "id", toml_str(node_id))
            conv.note(
                f"node.name = {raw!r} became [node] id = {node_id!r} (the part before @; "
                "mqttd node ids are not host-qualified). In a CLUSTER the id must be "
                "unique per node AND equal the Subject CN of that node's cluster-bus "
                "certificate — see docs/SECURED-CLUSTER-TUTORIAL.md"
            )
            continue
        if path == "mqtt.peer_cert_as_username":
            raw = str(value).strip().strip('"').lower()
            if raw == "disabled":
                continue
            if raw == "cn":
                conv.set(
                    "security",
                    "mtls_identity_source",
                    toml_str("cn"),
                    "mqtt.peer_cert_as_username = cn",
                )
                continue
            conv.todo(
                f"mqtt.peer_cert_as_username = {raw}: mqttd derives the mTLS identity "
                'from the certificate CN (default), or a SAN — "cn", "san-dns", '
                '"san-uri", "san-email" ([security] mtls_identity_source). The full DN, '
                "the DER/PEM body and its MD5 are not options, so every ACL rule and "
                "password entry keyed on that value must be re-keyed"
            )
            continue
        if path == "retainer.enable":
            if truthy(value):
                conv.todo(
                    "retainer.enable = true: retained messages were ON, and mqttd serves them "
                    "always — the retainer is not a separable subsystem, so there is nothing "
                    "to enable. Cap them with [limits] max_retained_messages"
                )
            else:
                conv.todo(
                    "retainer.enable = false: RETAINED MESSAGES WERE SWITCHED OFF in EMQX, and "
                    "mqttd has NO off switch — it serves retained messages always. So the "
                    "migrated broker will start STORING and REPLAYING retained values that "
                    "this deployment never kept, which changes what a fresh subscriber sees on "
                    "connect and adds durable state you were not paying for. Cap it with "
                    "[limits] max_retained_messages, or DENY the retained topics in the ACL "
                    "policy — and note that a bridged dual-run re-syncs retained state in BOTH "
                    "directions, so the incumbent can receive values from mqttd too"
                )
            continue
        if path == "mqtt.retain_available":
            # `true` is mqttd's own behaviour, so nothing is owed. Only the off
            # switch has no equivalent.
            if not truthy(value):
                conv.todo(
                    "mqtt.retain_available = false: "
                    + KEY_NO_EQUIVALENT["mqtt.retain_available"]
                )
            continue
        if path == "include" or path.startswith("include "):
            # CLASS G — silence about what was NOT READ. This used to get the generic
            # unknown-key line, which reads as "mqttd has no includes, fine" rather than
            # "I did not open that file, so anything in it was never seen". EMQX's own
            # documentation recommends the split-config layout, so an `include` holding the
            # whole authentication list and authorization block is a normal input.
            named = str(value).strip().strip('"') or path.partition(" ")[2].strip().strip('"')
            conv.todo(
                f"include {named}: THIS CONVERTER DID NOT OPEN THAT FILE and did not read one "
                "byte of it. It is not an unmapped setting — mqttd has no include mechanism, "
                "but that is not the problem here: EMQX pastes the included file's contents "
                "into this configuration, and the split-config layout EMQX's own "
                "documentation recommends is exactly where an `authentication` list and an "
                "`authorization` block usually live. NOTHING from it is in the output below "
                "and nothing about it is reported anywhere else, because it was never seen. "
                "Concatenate the files (parent first, then each include in order) and re-run "
                "this converter on the result"
            )
            continue
        reason = KEY_NO_EQUIVALENT.get(path)
        if reason:
            conv.todo(f"{path}: {reason}")
            continue
        conv.todo(
            f"{path}: no direct equivalent — check the mqttd configuration table "
            "(README env-var table / docs/mqttd.example.toml)"
        )
    for section, count in sorted(section_counts.items()):
        named = section_security.get(section) or []
        conv.todo(
            f"[{section}] ({count} setting(s) found): {SECTION_NO_EQUIVALENT[section]}"
            + (
                ". Among them, NAMED because they are addresses and certificate material "
                "rather than tuning: "
                + "; ".join(sorted(named))
                + " — none of which is anywhere in the output, because mqttd cannot serve "
                "that protocol at all"
                if named
                else ""
            )
        )


def convert_cluster(tree: dict, conv: Conversion) -> None:
    cluster = tree.get("cluster")
    if not isinstance(cluster, dict):
        return
    strategy = str(cluster.get("discovery_strategy", "")).strip().strip('"') or "unset"
    seeds: list[str] = []
    for key in ("static.seeds", "seeds"):
        cur: object = cluster
        for part in key.split("."):
            cur = cur.get(part) if isinstance(cur, dict) else None
        if cur:
            seeds = [str(s).strip().strip('"') for s in _as_list(cur)]
            break
    conv.todo(
        f"[cluster] (discovery_strategy = {strategy}"
        + (f", {len(seeds)} seed(s): {seeds}" if seeds else "")
        + "): cluster topology is NOT translated, deliberately. mqttd's mesh needs three "
        "things EMQX's discovery has no equivalent for — a per-node bus certificate whose "
        "Subject CN equals [node] id (plus a SAN covering peer_advertise, both serverAuth "
        "and clientAuth EKUs, and an ECDSA/Ed25519 key, never RSA), a 64-hex signed-gossip "
        "key shared cluster-wide, and the FOUNDER rule (exactly one seedless node founds "
        "the lease group). Getting any of those wrong fails at runtime, not at issue time. "
        "Walk docs/SECURED-CLUSTER-TUTORIAL.md and set [cluster] peer_bind / peer_advertise, "
        "[cluster.peer_tls] and [cluster.swim] deliberately"
    )
    for path, _ in leaves(cluster):
        if path in ("discovery_strategy", "static.seeds", "seeds"):
            continue
        conv.todo(
            f"cluster.{path}: see the [cluster] note above — no EMQX discovery setting "
            "maps onto mqttd's mTLS+gossip mesh"
        )


# ---------------------------------------------------------------------------
# Bridges -> mqtt-bridge rules (ADR 0025). Only MQTT-type bridges can map at all.
# ---------------------------------------------------------------------------


def _template_to_remap(template: str, rule: BridgeRule, side: str) -> str:
    """`from_x/${topic}` -> prefix `from_x/`. Anything else is a TODO, not a guess."""
    t = template.strip()
    if t in ("${topic}", "$topic"):
        return ""
    if t.endswith("${topic}"):
        return t[: -len("${topic}")]
    rule.todos.append(
        f"the {side} topic template {template!r} is not a simple prefix + ${{topic}}. "
        "mqtt-bridge remaps can only strip a prefix and prepend a prefix, so this "
        "rewrite must be redesigned (or done by a client). The rule below forwards the "
        "topic UNCHANGED — check that is what you want"
    )
    return ""


# mqtt-bridge's TLS keys, and the EMQX `ssl.*` leaf each one comes from.
BRIDGE_TLS_KEYS = {
    "cacertfile": "ca",
    "certfile": "cert",
    "keyfile": "key",
}


def collect_bridge_tls(up: BridgeUpstream, body: dict, where: str) -> None:
    """Read an EMQX bridge/connector's `ssl.*` block into a COMMENTED candidate.

    `tls: Option<Tls>` in crates/mqtt-bridge/src/config.rs means ABSENT = PLAINTEXT, so an
    EMQX bridge with `ssl.enable = true` used to produce a live `url` and no TLS block at all
    — the upstream downgraded from TLS to cleartext, carrying the bridge's CONNECT and its
    username with it — under a generic "the bridge had TLS options" TODO that named none of
    the paths (`ssl.*` was explicitly skipped by the leaf reporter, so cacertfile/certfile/
    keyfile/verify appeared NOWHERE). Emitting the block LIVE would be just as wrong: the
    paths are EMQX's, on the EMQX host, and mqtt-bridge runs elsewhere. So the candidate is
    written commented, with every dropped path named. Found 2026-08-15.
    """
    ssl = body.get("ssl") if isinstance(body.get("ssl"), dict) else {}
    ssl_options = body.get("ssl_options") if isinstance(body.get("ssl_options"), dict) else {}
    merged: dict[str, object] = {}
    for prefix, block in (("ssl", ssl), ("ssl_options", ssl_options)):
        for path, value in leaves(block) if block else []:
            merged[f"{prefix}.{path}"] = value
    flat_enable = body.get("ssl") if not isinstance(body.get("ssl"), dict) else None
    if not merged and flat_enable is None:
        return
    enabled = any(
        truthy(v) for k, v in merged.items() if k.endswith("enable") or k.endswith("enabled")
    ) or (flat_enable is not None and truthy(flat_enable))
    up.tls_enabled = enabled
    for path, value in sorted(merged.items()):
        leaf = path.rsplit(".", 1)[-1]
        key = BRIDGE_TLS_KEYS.get(leaf)
        if key is None:
            continue
        shown = str(value).strip().strip('"')
        if shown:
            up.tls.append((key, shown, f"{where}.{path}"))
    listed = "; ".join(f"{path} = {value!r}" for path, value in sorted(merged.items()))
    up.tls_todo = (
        f"{where} connected to its upstream over "
        + ("TLS" if enabled else "a configured-but-not-enabled TLS block")
        + ", and mqtt-bridge's [upstreams.tls] is OPTIONAL — ABSENT MEANS PLAINTEXT "
        "(crates/mqtt-bridge/src/config.rs). Emitting the upstream without it would send the "
        "bridge's CONNECT, username included, in the CLEAR to a peer that expected TLS, so "
        "the tls block below is COMMENTED OUT rather than dropped or guessed: uncomment it "
        "and REPLACE THE PATHS, which are EMQX's on the EMQX host while mqtt-bridge runs "
        "somewhere else (ca alone for server verification; cert AND key together for mTLS — "
        "an mTLS half-identity is refused at startup; paths only, no cipher or version "
        f"knobs). Everything the EMQX side named: {listed}"
        + (
            ""
            if enabled
            else ". NOTE that `ssl.enable` was NOT true, so EMQX itself may have been "
            "connecting in the clear already — check before you uncomment"
        )
    )


def convert_bridges(tree: dict, conv: Conversion) -> None:
    """EMQX MQTT bridges / MQTT connectors -> mqtt-bridge upstreams and rules."""
    for section in ("bridges", "connectors"):
        block = tree.get(section)
        if not isinstance(block, dict):
            continue
        for kind, named in block.items():
            if kind != "mqtt":
                continue  # non-MQTT types are reported by SECTION_NO_EQUIVALENT
            if not isinstance(named, dict):
                continue
            for name, body in named.items():
                if not isinstance(body, dict):
                    continue
                where = f"{section}.mqtt.{name}"
                # Everything this function actually READS. Every other leaf of the bridge
                # body is reported below: a payload template or a retain override that
                # vanishes silently changes the message the far side receives, and the
                # section-level count is not a statement that anything was translated.
                consumed: set[str] = {"server", "url", "servers", "username", "password"}
                if any(k in body for k in ("enable", "enabled")) and falsey(
                    body.get("enable", body.get("enabled"))
                ):
                    conv.todo(
                        f"{where} has enable = false: the bridge was DISABLED in EMQX, so "
                        "no mqtt-bridge upstream was written for it. Enable it and re-run "
                        "if it should be forwarding"
                    )
                    continue
                url = str(
                    body.get("server") or body.get("url") or body.get("servers") or ""
                ).strip().strip('"')
                if not url:
                    conv.todo(
                        f"{section}.mqtt.{name}: no server address found, so no "
                        "mqtt-bridge upstream could be written for it"
                    )
                    continue
                url_key = next(
                    (k for k in ("server", "url", "servers") if body.get(k)), "server"
                )
                up = BridgeUpstream(
                    name=str(name), url=url, url_source=f"{where}.{url_key}"
                )
                if body.get("username"):
                    up.username = str(body["username"]).strip().strip('"')
                if body.get("password") is not None:
                    up.todos.append(
                        "the bridge password was NOT copied (secrets are never "
                        "transformed). Write it to a file and set `password_file`"
                    )
                collect_bridge_tls(up, body, where)
                for leg, direction in (("ingress", "in"), ("egress", "out")):
                    spec = body.get(leg)
                    if not isinstance(spec, dict):
                        continue
                    remote = spec.get("remote") if isinstance(spec.get("remote"), dict) else {}
                    local = spec.get("local") if isinstance(spec.get("local"), dict) else {}
                    src = remote if direction == "in" else local
                    dst = local if direction == "in" else remote
                    for side in ("remote", "local"):
                        consumed.add(f"{leg}.{side}.topic")
                        consumed.add(f"{leg}.{side}.qos")
                    filt = str(src.get("topic", "")).strip().strip('"')
                    if not filt:
                        conv.todo(
                            f"{section}.mqtt.{name}.{leg}: no source topic found; no "
                            "bridge rule was written for this leg"
                        )
                        continue
                    rule = BridgeRule(direction=direction, filter=filt)
                    qos = str(src.get("qos", dst.get("qos", "1"))).strip().strip('"')
                    rule.qos = int(qos) if qos.isdigit() and int(qos) <= 2 else 1
                    if not qos.isdigit():
                        rule.todos.append(
                            f"qos was {qos!r} (an EMQX template, most likely "
                            "${qos}); mqtt-bridge forwards at a FIXED per-rule QoS, so "
                            "1 was chosen. QoS 2 is downgraded to 1 by the engine in any "
                            "case (ADR 0025 §7)"
                        )
                    dst_topic = str(dst.get("topic", "")).strip().strip('"')
                    if dst_topic:
                        prefix = _template_to_remap(dst_topic, rule, leg)
                        if prefix:
                            rule.prefix = prefix
                    if filt.startswith("$"):
                        conv.todo(
                            f"{section}.mqtt.{name}.{leg} bridges {filt!r}. mqtt-bridge "
                            "REFUSES a filter starting with `$` (issue #193): $SYS and "
                            "$share are never bridged. That leg cannot be reproduced"
                        )
                        continue
                    up.rules.append(rule)
                if not up.rules:
                    conv.todo(
                        f"{section}.mqtt.{name}: no forwarding rule could be expressed, "
                        "so the upstream was written with none. A bridge with no rules "
                        "forwards NOTHING (deny by default)"
                    )
                # EVERY remaining key on the bridge, named. A count-only section TODO is
                # not a report: the operator must see that `remote.payload` and
                # `local.retain` did not cross, next to the rules that did.
                for path, value in leaves(body):
                    if path in consumed or path.startswith("ssl"):
                        continue
                    leaf = path.rsplit(".", 1)[-1]
                    reason = BRIDGE_NO_EQUIVALENT.get(path) or BRIDGE_NO_EQUIVALENT.get(leaf)
                    if reason and reason.startswith("handled"):
                        continue
                    shown = (
                        "was set (its value is NOT copied — secrets are never transformed)"
                        if any(
                            s in leaf.lower() for s in ("password", "passphrase", "secret")
                        )
                        else f"= {value!r}"
                    )
                    up.todos.append(
                        f"{where}.{path} {shown}: "
                        + (
                            reason
                            or "no mqtt-bridge equivalent — mqtt-bridge expresses an "
                            "upstream address, credentials, a per-rule topic filter, a "
                            "fixed QoS and a prefix remap, and nothing else. Check "
                            "docs/BRIDGE.md and decide whether this setting mattered"
                        )
                    )
                conv.bridges.append(up)
    convert_bridge_actions(tree, conv)


def convert_bridge_actions(tree: dict, conv: Conversion) -> None:
    """`actions.mqtt.*` / `sources.mqtt.*` — the shape EMQX 6.x actually ships.

    CLASS E, found by re-deriving the mapping table from the vendor's schema at the pinned
    tag rather than trusting the row. `bridges` is NOT a root in EMQX 6.2.2's schema at all
    (emqx_conf_schema:roots/0 and emqx_bridge_v2_schema:roots/0 give `connectors`, `actions`
    and `sources`; `bridges.*` survives only through emqx_bridge_compatible_config's v1
    UPGRADE path). Under v2 the connector holds the ADDRESS and credentials while the topic
    rules live in `actions.mqtt.<name>.parameters` (topic/qos/retain/payload, per
    emqx_bridge_mqtt_pubsub_schema:fields(action_parameters) and its own documented example)
    and `sources.mqtt.<name>.parameters` (topic/qos).

    Before this, a genuine 6.x bridge produced an upstream with ZERO rules — forwarding
    nothing — while the section-level TODO said "Only MQTT-type actions map to mqtt-bridge
    rules". The claim was true of the v1 shape and false of the one the vendor ships.
    """
    for section, direction in (("actions", "out"), ("sources", "in")):
        block = tree.get(section)
        if not isinstance(block, dict):
            continue
        named = block.get("mqtt")
        if not isinstance(named, dict):
            continue
        for name, body in named.items():
            if not isinstance(body, dict):
                continue
            where = f"{section}.mqtt.{name}"
            if any(k in body for k in ("enable", "enabled")) and falsey(
                body.get("enable", body.get("enabled"))
            ):
                conv.todo(
                    f"{where} has enable = false: the {section[:-1]} was DISABLED in EMQX, "
                    "so no mqtt-bridge rule was written for it. Enable it and re-run if it "
                    "should be forwarding"
                )
                continue
            conn = str(body.get("connector", "")).strip().strip('"')
            params = body.get("parameters") if isinstance(body.get("parameters"), dict) else {}
            remote_topic = str(params.get("topic", "")).strip().strip('"')
            local_topic = str(body.get("local_topic", "")).strip().strip('"')
            # out: the LOCAL filter is what mqtt-bridge subscribes to and the remote topic is
            # the destination; in: the REMOTE filter is the subscription.
            filt = local_topic if direction == "out" else remote_topic
            dst_topic = remote_topic if direction == "out" else local_topic
            up = next((u for u in conv.bridges if u.name == conn), None)
            if up is None:
                conv.todo(
                    f"{where} names connector {conn or '(none)'}, which is not an MQTT "
                    "connector this converter translated into an upstream, so NO "
                    "mqtt-bridge rule was written for it. Find the connector (a "
                    "non-MQTT type? managed from the dashboard, i.e. in "
                    "data/configs/cluster.hocon?) and translate the pair by hand: without "
                    f"a rule the {'egress' if direction == 'out' else 'ingress'} traffic "
                    "this carried does NOT cross"
                )
                continue
            if not filt:
                conv.todo(
                    f"{where}: no "
                    + ("`local_topic`" if direction == "out" else "`parameters.topic`")
                    + " to subscribe to, so NO mqtt-bridge rule was written and this "
                    f"{section[:-1]}'s traffic does NOT cross. mqtt-bridge is deny-by-"
                    "default per rule; an EMQX rule-engine SQL SELECT feeding this "
                    "action has no equivalent at all"
                )
                continue
            if filt.startswith("$"):
                conv.todo(
                    f"{where} bridges {filt!r}. mqtt-bridge REFUSES a filter starting with "
                    "`$` (issue #193): $SYS and $share are never bridged. That "
                    f"{section[:-1]} cannot be reproduced"
                )
                continue
            rule = BridgeRule(direction=direction, filter=filt)
            qos = str(params.get("qos", "1")).strip().strip('"')
            rule.qos = int(qos) if qos.isdigit() and int(qos) <= 2 else 1
            if not qos.isdigit():
                rule.todos.append(
                    f"{where}.parameters.qos was {qos!r} (an EMQX template); mqtt-bridge "
                    "forwards at a FIXED per-rule QoS, so 1 was chosen. QoS 2 is "
                    "downgraded to 1 by the engine in any case (ADR 0025 §7)"
                )
            if dst_topic:
                prefix = _template_to_remap(dst_topic, rule, where)
                if prefix:
                    rule.prefix = prefix
            up.rules.append(rule)
            consumed = {"connector", "enable", "enabled", "local_topic", "parameters.topic",
                        "parameters.qos", "type", "name", "description", "tags"}
            for path, value in leaves(body):
                if path in consumed:
                    continue
                leaf = path.rsplit(".", 1)[-1]
                reason = BRIDGE_NO_EQUIVALENT.get(leaf)
                if reason and reason.startswith("handled"):
                    continue
                shown = (
                    "was set (its value is NOT copied — secrets are never transformed)"
                    if any(s in leaf.lower() for s in ("password", "passphrase", "secret"))
                    else f"= {value!r}"
                )
                up.todos.append(
                    f"{where}.{path} {shown}: "
                    + (
                        reason
                        or "no mqtt-bridge equivalent — mqtt-bridge expresses an upstream "
                        "address, credentials, a per-rule topic filter, a fixed QoS and a "
                        "prefix remap, and nothing else"
                    )
                )


def render_bridge(conv: Conversion) -> str:
    prov = conv.prov
    out = [
        "# Translated from EMQX MQTT bridges by the mqttd EMQX converter",
        "# (scripts/migrate/from-emqx.py). " + VERSIONS + ".",
        "#",
        *DRAFT_HEADER,
        "#",
        "# mqtt-bridge is a SEPARATE PROCESS (`mqtt-bridge <this-file>`), not a broker",
        "# feature: an ordinary MQTT client to both sides. Forwarding is DENY BY DEFAULT —",
        "# only a topic matching a rule crosses, and only in that rule's direction.",
        "#",
        "# Read every line, then read docs/MIGRATION.md's dual-run section: for a CUTOVER",
        "# you want a `both` rule with NO remap over the shared namespace, which is a",
        "# different shape from the remapped one-way bridges below.",
        "",
        "hop_count_limit = 8",
        '# One instance. An empty share group disables the cluster-side shared subscription',
        '# (ha = "partitioned" with instance/total is the way to run more than one).',
        'share_group = ""',
        "",
        "[local]",
        "# The mqttd cluster, as an ordinary MQTT client. Point this at a node or a VIP.",
        "#",
        "# NOT DERIVED FROM ANYTHING, so it is COMMENTED OUT: the local address is where",
        "# mqtt-bridge will connect to YOUR broker, and nothing in an EMQX configuration",
        "# says what that is. mqtt-bridge refuses to start without it, which is the right",
        "# way round — a default here would silently point the bridge at a loopback broker",
        "# that is not the one you are migrating to.",
        *prov.line(
            "url",
            toml_str("127.0.0.1:1883"),
            None,
            decide="[local] url in the generated bridge config is the address of YOUR mqttd "
            "cluster (a node, or a VIP in front of the cluster). Nothing in an EMQX "
            "configuration names it, so it is emitted COMMENTED OUT: set it and uncomment it, "
            "and remember it must reach a client listener, with credentials if that listener "
            "requires them",
        ),
        'client_id = "mqttd-bridge-1"  # MUST be unique per instance',
        "",
        "[spool]",
        "# A QoS>=1 rule is REFUSED without a durable spool dir (ADR 0060 T4): the source's",
        "# ack is meant to be gated on durability, and an in-memory spool loses acked",
        "# messages on restart. This volume holds production payloads in the clear —",
        "# encrypt it at rest.",
        'dir = "/var/lib/mqtt-bridge"   # TODO(migrate): set a real, encrypted volume',
        "max_messages = 10000",
        "",
    ]
    for up in conv.bridges:
        out.append("[[upstreams]]")
        out.append(f"name = {toml_str(up.name)}")
        if up.tls_enabled:
            # THE POSTURE IS DECIDED BY THIS LINE, not by the commented tls block. mqtt-bridge's
            # `[upstreams.tls]` is Optional and ABSENT MEANS PLAINTEXT, so an upstream whose
            # EMQX side had `ssl.enable = true` must NOT have a live `url`: completing the draft
            # exactly as the file instructs ([local] url + a spool dir) otherwise produced a
            # bridge that connected to a TLS peer in the CLEAR — measured against the real
            # binary. Round 3 commented the TLS block; round 4 found the url still live, and
            # docs/MIGRATION.md claiming that shape was impossible. Found 2026-08-15.
            out.extend(
                prov.line(
                    "url",
                    toml_str(up.url),
                    None,
                    decide=f"the EMQX side of this bridge connected over TLS ({up.url_source} "
                    "names the address), and mqtt-bridge's [upstreams.tls] below is COMMENTED "
                    "OUT because its paths are EMQX's on the EMQX host. An upstream with no tls "
                    "block connects in PLAINTEXT (crates/mqtt-bridge/src/config.rs: tls is "
                    "Optional), so a live `url` here would send this bridge's CONNECT — "
                    "username included — in the CLEAR to a peer that expected TLS. Both lines "
                    "are therefore inert: complete [upstreams.tls] with paths that exist where "
                    "mqtt-bridge runs, THEN uncomment this url. mqtt-bridge refuses to start "
                    "without a url, which is the right way round",
                )
            )
        else:
            out.extend(prov.line("url", toml_str(up.url), up.url_source or None))
        out.append(f"client_id = {toml_str('mqttd-bridge-' + up.name)}")
        if up.username:
            out.append(f"username = {toml_str(up.username)}")
        for t in up.todos:
            out.append(f"# TODO(migrate): {comment_safe(t)}")
        # The TLS candidate: never live (absent = plaintext), always named.
        if up.tls_todo:
            out.append(f"# TODO(migrate): {comment_safe(up.tls_todo)}")
            out.append("# [upstreams.tls]")
            for key, path, source in up.tls:
                out.extend(prov.inert(key, toml_str(path), f"from {source}"))
            if not up.tls:
                out.append(
                    comment_safe(
                        "# ca = \"/etc/mqtt-bridge/upstream-ca.crt\"  # PLACEHOLDER — the "
                        "EMQX side named no CA path this converter could read; supply the "
                        "anchors that verify the upstream"
                    )
                )
        out.append("")
        for rule in up.rules:
            for t in rule.todos:
                out.append(f"# TODO(migrate): {comment_safe(t)}")
            out.append("[[upstreams.rules]]")
            out.append(f"direction = {toml_str(rule.direction)}")
            out.append(f"filter = {toml_str(rule.filter)}")
            out.append(f"qos = {rule.qos}")
            if rule.prefix:
                out.append(f"remap = {{ prefix = {toml_str(rule.prefix)} }}")
            out.append("")
    if not conv.bridges:
        out.append("# No MQTT-type bridge was found in the EMQX configuration, so there")
        out.append("# are no upstreams here. A bridge with no upstreams forwards nothing.")
        out.append("#")
        out.append("# TODO(migrate): if you DO run EMQX bridges, they may be managed from")
        out.append("# the dashboard and persisted to data/configs/cluster.hocon rather than")
        out.append("# emqx.conf — pass that file to this converter as well.")
        out.append("")
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------------------
# The EMQX ACL file (Erlang terms). Tolerant tokeniser: an unparseable term is
# quoted verbatim as a TODO, never skipped.
# ---------------------------------------------------------------------------


def split_terms(text: str) -> list[str]:
    """Split `{...}.`-terminated Erlang terms, ignoring `%` comments."""
    terms: list[str] = []
    buf: list[str] = []
    depth = 0
    in_str = False
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if in_str:
            buf.append(c)
            if c == "\\" and i + 1 < n:
                buf.append(text[i + 1])
                i += 2
                continue
            if c == '"':
                in_str = False
            i += 1
            continue
        if c == '"':
            in_str = True
            buf.append(c)
            i += 1
            continue
        if c == "%":
            while i < n and text[i] != "\n":
                i += 1
            continue
        if c in "{[":
            depth += 1
        elif c in "}]":
            depth -= 1
        elif c == "." and depth <= 0:
            term = "".join(buf).strip()
            if term:
                terms.append(term)
            buf = []
            i += 1
            continue
        buf.append(c)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        terms.append(tail)
    return terms


def parse_term(text: str):
    """Parse one Erlang term into lists (tuples/lists), str (strings), Atom (atoms)."""

    class P:
        def __init__(self, s: str) -> None:
            self.s = s
            self.i = 0

        def ws(self) -> None:
            while self.i < len(self.s) and self.s[self.i] in " \t\r\n":
                self.i += 1

        def value(self):
            self.ws()
            if self.i >= len(self.s):
                return Atom("")
            c = self.s[self.i]
            if c in "{[":
                close = "}" if c == "{" else "]"
                self.i += 1
                items = []
                while True:
                    self.ws()
                    if self.i >= len(self.s):
                        break
                    if self.s[self.i] == close:
                        self.i += 1
                        break
                    if self.s[self.i] == ",":
                        self.i += 1
                        continue
                    items.append(self.value())
                return items
            if c == '"':
                self.i += 1
                buf = []
                while self.i < len(self.s):
                    ch = self.s[self.i]
                    if ch == "\\" and self.i + 1 < len(self.s):
                        buf.append(self.s[self.i + 1])
                        self.i += 2
                        continue
                    if ch == '"':
                        self.i += 1
                        break
                    buf.append(ch)
                    self.i += 1
                return "".join(buf)
            if c == "'":
                self.i += 1
                buf = []
                while self.i < len(self.s) and self.s[self.i] != "'":
                    buf.append(self.s[self.i])
                    self.i += 1
                self.i += 1
                return Atom("".join(buf))
            start = self.i
            while self.i < len(self.s) and self.s[self.i] not in ",{}[] \t\r\n":
                self.i += 1
            return Atom(self.s[start : self.i])

    return P(text).value()


class Atom(str):
    """An Erlang atom (or bare number) — distinguished from a quoted string."""

    __slots__ = ()


PLACEHOLDERS = {
    "${username}": "%i",
    "${clientid}": "%c",
}


def convert_topic(topic: str, todos: list[str], where: str) -> str | None:
    """EMQX placeholders -> mqttd substitutions. `None` = do not emit this rule."""
    # A LITERAL %c OR %i, checked BEFORE any placeholder is rewritten so a %c that this
    # function itself produces from ${clientid} is never confused with one the source
    # carried. EMQX substitutes nothing of that shape: the vendor's own acl.conf schema
    # (scripts/migrate/fixtures/emqx-acl-6.2.2.conf, "Supported placeholders are:") lists
    # ${username}, ${clientid}, ${cert_common_name}, ${client_attrs.NAME} and ${zone} —
    # `%c`/`%i` are not placeholders in EMQX 5/6, so a topic carrying them matched those
    # BYTES literally. mqttd substitutes %c (client id) and %i (identity) in EVERY rule's
    # topics and has no escape (crates/mqtt-auth/src/acl.rs), so carrying the filter across
    # would turn a rule on one literal topic into a live per-client grant the source never
    # gave — the same misread the Mosquitto converter refuses on a plain `topic` line, and
    # until 2026-08-16 this converter emitted it with only a fail-closed caveat beside it.
    # Refused instead. Found via issue #297.
    literal = [t for t in ("%c", "%i") if t in topic]
    if literal:
        todos.append(
            f"{where}: the topic {topic!r} contains "
            + " and ".join(literal)
            + " LITERALLY — EMQX 5/6 substitutes only ${...} placeholders (the pinned "
            "acl.conf schema @ 6.2.2 lists them; %c/%i are not among them), while mqttd "
            "substitutes %c (client id) and %i (identity) in EVERY rule's topics with no "
            "escape (crates/mqtt-auth/src/acl.rs). Carrying it over would turn a rule on "
            "one literal topic into a live per-client grant the source never gave, so NO "
            "RULE WAS WRITTEN for it. If a per-client namespace IS what you want, write "
            "it as an mqttd rule deliberately; if the topic really is literal, rename it. "
            "(EMQX 4.x DID substitute %c/%u in its acl.conf — if this file predates EMQX "
            "5 it is outside this parser's version scope: rewrite those as "
            "${clientid}/${username} and re-run)"
        )
        return None
    out = topic
    for src, dst in PLACEHOLDERS.items():
        out = out.replace(src, dst)
    if "${cert_common_name}" in out:
        out = out.replace("${cert_common_name}", "%i")
        todos.append(
            f"{where}: ${{cert_common_name}} became %i (the mqttd identity SUBJECT). "
            "Those are the same value ONLY when the client authenticated with mTLS and "
            '[security] mtls_identity_source = "cn". Verify that, or the rule matches '
            "the wrong thing"
        )
    if "${client_attrs." in out:
        todos.append(
            f"{where}: uses a ${{client_attrs.NAME}} placeholder. Client attributes do "
            "not exist in mqttd, so this rule CANNOT be expressed and was NOT emitted. "
            "Model the attribute as a GROUP from the OIDC/HTTP authenticator and match it "
            "with `groups = [...]`, or key the rule on the identity"
        )
        return None
    if "${zone}" in out:
        todos.append(
            f"{where}: uses ${{zone}}. Zones do not exist in mqttd, so this rule was NOT "
            "emitted. Collapse the zones, or split them into separate deployments"
        )
        return None
    if "${" in out:
        todos.append(
            f"{where}: contains a placeholder this converter does not know, so the rule "
            "was NOT emitted. mqttd supports %i (identity) and %c (client id) only"
        )
        return None
    if out.startswith("$"):
        todos.append(
            f"{where}: the rule covers the $-prefixed topic {out!r}. mqttd implements no "
            "$SYS tree and no $-namespace of its own (the broker's own telemetry is "
            "/metrics and /statusz), so the rule is INERT — kept below for the record, "
            "but nothing publishes or subscribes there. Any client that read $SYS must be "
            "rewritten against /metrics"
        )
    if "%i" in out or "%c" in out:
        todos.append(
            f"{where}: mqttd's %i/%c substitutions FAIL CLOSED when the value is empty or "
            "contains / + or # — a client whose id or username holds one of those matches "
            "NOTHING through this rule. Verify your naming"
        )
    return out


def parse_acl(text: str) -> tuple[list[dict], list[str], str | None]:
    """Translate EMQX's Erlang-term acl.conf. Returns (rules, todos, default_hint)."""
    rules: list[dict] = []
    todos: list[str] = []
    default_hint: str | None = None
    saw_allow = saw_deny = False

    for raw in split_terms(text):
        term = parse_term(raw)
        shown = " ".join(raw.split())
        if not isinstance(term, list) or len(term) < 2:
            todos.append(f"unparsed ACL term, translate by hand: {shown}.")
            continue
        perm = str(term[0]).strip().lower()
        if perm not in ("allow", "deny"):
            todos.append(f"ACL term with an unknown permission {perm!r}: {shown}.")
            continue

        # {perm(), all} and {perm(), security_profile()} — the catch-all last rule.
        if len(term) == 2:
            second = term[1]
            if isinstance(second, Atom) and str(second) == "all":
                default_hint = perm
                todos.append(
                    f"the catch-all rule {shown}. sets the FALLTHROUGH permission. "
                    f'mqttd expresses that as the document-level `default = "{perm}"` '
                    "(commented out below if it differs from what was written) — but "
                    "READ THE ORDERING NOTE: EMQX evaluates rules in FILE ORDER and stops "
                    "at the first match, while mqttd is deny-wins (any matching deny "
                    "beats every allow, then any allow permits, then the default). A "
                    "policy that relied on an early allow shadowing a later deny changes "
                    "meaning"
                )
                continue
            if isinstance(second, list) and second and str(second[0]) == "security_profile":
                todos.append(
                    f"{shown}. is gated on EMQX_SECURITY_PROFILE (legacy vs hardened, "
                    "EMQX >= 6.3). mqttd has no such switch, so this rule was NOT "
                    f"emitted. Decide explicitly whether it should be an unconditional "
                    f"{perm} rule and write it, or drop it — EMQX's own comment tells you "
                    "to replace it with `{deny, all}.` in production"
                )
                continue
            todos.append(f"two-element ACL term this converter does not know: {shown}.")
            continue

        if len(term) != 4:
            todos.append(
                f"ACL term with {len(term)} elements (expected 2 or 4): {shown}."
            )
            continue

        cond, action, topics = term[1], term[2], term[3]

        # -- condition -> identities -------------------------------------
        identities: list[str] = []
        if isinstance(cond, Atom) and str(cond) == "all":
            identities = []
        elif isinstance(cond, list) and cond:
            kind = str(cond[0]).lower()
            if kind in ("user", "username") and len(cond) >= 2:
                if isinstance(cond[1], list) and cond[1] and str(cond[1][0]) == "re":
                    todos.append(
                        f"{shown}. matches the username by REGULAR EXPRESSION "
                        f"({cond[1][1] if len(cond[1]) > 1 else '?'!r}). mqttd's "
                        "`identities` are GLOBS, and `*` (any run of characters, including "
                        "none) is the ONLY special character — every other byte, `?` "
                        "included, is matched LITERALLY (crates/mqtt-auth/src/acl.rs "
                        "glob_match). So this rule was NOT emitted: rewrite it using `*` "
                        "alone if it can be, or enumerate the identities. Do not translate "
                        "a regex `.` into `?`, which would match the literal character `?` "
                        "and therefore nothing"
                    )
                    continue
                identities = [str(cond[1])]
                # A LITERAL `*` in the username is the same problem one step down: the
                # rejection above knows `*` is the only special character, and then never
                # inspects the literal it emits. EMQX matched the username EXACTLY, so a name
                # containing `*` becomes an mqttd GLOB — `alice*bob` would admit
                # `alice-admin-bob` — and glob_match has NO escape, so the rule cannot be
                # expressed at all. Found 2026-08-15.
                if "*" in identities[0]:
                    todos.append(
                        f"{shown}. names the username {identities[0]!r}, which contains a "
                        "LITERAL `*`. mqttd's `identities` are GLOBS where `*` matches any run "
                        "of characters and there is NO way to escape it "
                        "(crates/mqtt-auth/src/acl.rs glob_match), while EMQX matched that "
                        "username EXACTLY — so emitting this rule would grant it to every "
                        "identity matching the pattern. It was NOT emitted: rename the user, or "
                        "enumerate the identities you mean"
                    )
                    continue
            elif kind in ("client", "clientid") and len(cond) >= 2:
                todos.append(
                    f"{shown}. matches on the CLIENT ID. mqttd's publish/subscribe "
                    "rules match on the IDENTITY (`identities`) — a client-id matcher "
                    "exists only for `connect` rules (`clients = [...]`), which cannot "
                    "carry topics. This rule was NOT emitted. Either key it on the "
                    "identity, or put %c in the TOPIC so each client is confined to its "
                    "own subtree"
                )
                continue
            elif kind in ("ipaddr", "ipaddrs"):
                todos.append(
                    f"{shown}. matches on SOURCE IP ADDRESS. mqttd's ACL has no "
                    "address matcher at all — authorization is identity + topic. This "
                    "rule was NOT emitted. Enforce address policy in the network layer "
                    "(security group, NetworkPolicy, host firewall); it belongs there"
                )
                continue
            elif kind in ("client_attr", "zone", "listener"):
                todos.append(
                    f"{shown}. matches on {kind}, which does not exist in mqttd "
                    "(no client attributes, no zones, no per-listener conditions). The "
                    "rule was NOT emitted"
                )
                continue
            elif kind in ("and", "or"):
                todos.append(
                    f"{shown}. combines conditions with '{kind}'. mqttd rules match "
                    "any-of `identities` and any-of `groups` — there is no boolean "
                    "combinator. The rule was NOT emitted; split it into separate rules "
                    "if the semantics allow, or narrow the condition"
                )
                continue
            elif kind == "security_profile":
                todos.append(
                    f"{shown}. is gated on EMQX_SECURITY_PROFILE; mqttd has no such "
                    "switch. NOT emitted"
                )
                continue
            else:
                todos.append(
                    f"{shown}. has a condition this converter does not know "
                    f"({kind!r}); NOT emitted"
                )
                continue
        else:
            todos.append(f"{shown}. has an unreadable condition; NOT emitted")
            continue

        # -- action ------------------------------------------------------
        qualifier: list | None = None
        if isinstance(action, list) and action:
            qualifier = action[1:] if len(action) > 1 else []
            simple = str(action[0]).lower()
        else:
            simple = str(action).lower()
        actions = {
            "publish": ["publish"],
            "subscribe": ["subscribe"],
            "all": ["publish", "subscribe"],
        }.get(simple)
        if actions is None:
            todos.append(
                f"{shown}. has an action this converter does not know ({simple!r}); "
                "NOT emitted"
            )
            continue
        if qualifier:
            todos.append(
                f"{shown}. qualifies the action with qos/retain flags, which mqttd "
                "rules CANNOT express — a rule matches publish or subscribe, full stop. "
                f"The rule was emitted WITHOUT the qualifier, which makes this {perm} "
                + (
                    "BROADER than it was (it now covers every QoS and both retain "
                    "states) — review it, this is the dangerous direction"
                    if perm == "allow"
                    else "broader than it was, so traffic the original permitted is now "
                    "denied — review it"
                )
            )

        # -- topics ------------------------------------------------------
        topic_list = topics if isinstance(topics, list) else [topics]
        converted: list[str] = []
        for t in topic_list:
            if isinstance(t, list) and t and str(t[0]) == "eq":
                todos.append(
                    f"{shown}. uses {{eq, {t[1] if len(t) > 1 else '?'!r}}} — an "
                    "EXACT filter-string match. mqttd has no such matcher: an allow rule "
                    "matches by filter COVERAGE and a deny rule by OVERLAP, so `{eq, "
                    '"#"}` would become a deny on `#` that overlaps EVERY topic and '
                    "denies everything. The entry was NOT emitted — express the intent "
                    "as an explicit topic list instead"
                )
                continue
            if isinstance(t, Atom) and str(t) == "all":
                todos.append(
                    f"{shown}. uses the special topic `all`, which in EMQX matches "
                    "$-prefixed topics too. mqttd's `#` does NOT match a $-topic, and "
                    "$SYS is not implemented at all. `#` was used; if the rule existed to "
                    "cover $-topics, it has no equivalent"
                )
                converted.append("#")
                continue
            got = convert_topic(str(t), todos, f"{shown}.")
            if got is not None:
                converted.append(got)
        if not converted:
            continue

        if perm == "allow":
            saw_allow = True
        else:
            saw_deny = True
        rules.append(
            {
                "identities": identities,
                "actions": actions,
                "effect": perm,
                "topics": converted,
                "source": shown,
            }
        )

    if saw_allow and saw_deny:
        todos.insert(
            0,
            "THE EVALUATION ORDER CHANGED, and this is the one difference that can make "
            "the converted policy MORE PERMISSIVE than the original. EMQX walks the rules "
            "in FILE ORDER and stops at the first match, so an early `allow` shadows a "
            "later `deny`. mqttd is DENY-WINS: every rule is considered, any matching deny "
            "beats every allow, then any matching allow permits, then `default`. Both an "
            "allow and a deny rule are present below, so read them as a set and check "
            "every overlapping pair — allow rules match by filter COVERAGE (the grant must "
            "subsume the request), deny rules by OVERLAP",
        )
    return rules, todos, default_hint


def _upper_first(text: str) -> str:
    """Capitalise the FIRST character only.

    `str.capitalize()` lowercases everything after it, which flattens the emphasis capitals
    these sentences carry deliberately (`PERMITS EVERY publish and subscribe`).
    """
    return text[:1].upper() + text[1:]


def empty_policy_effect(default: str) -> str:
    """What a rule-less policy DOES, derived from the `default` that will be written.

    CLASS C, and the reason this is a function. Both zero-rule TODOs used to state
    `default = "deny"` and "that is fail-closed" as CONSTANTS, while render_acl writes
    `conv.acl_default` — which is `allow` whenever the source set
    `authorization.no_match = allow`. So the generated file could be a WIDE-OPEN policy whose
    own comment told the operator it denied everything. A sentence about a computed value has
    to be computed from it.
    """
    if default == "allow":
        return (
            'this policy\'s `default = "allow"` (taken from authorization.no_match) PERMITS '
            "EVERY publish and subscribe by every authenticated client — a wide open policy, "
            'not a migrated one. Set `default = "deny"` before deploying it'
        )
    return (
        'this policy\'s `default = "deny"` denies every publish and subscribe. That is '
        "fail-closed, not migrated"
    )


def render_acl(
    rules: list[dict],
    todos: list[str],
    default: str,
    default_hint: str | None,
    default_source: str | None = None,
    prov: Provenance | None = None,
) -> str:
    prov = prov if prov is not None else Provenance()
    out = [
        "# Translated from an EMQX acl.conf by the mqttd EMQX converter",
        "# (scripts/migrate/from-emqx.py). " + VERSIONS + ".",
        "#",
        *DRAFT_HEADER,
        "#",
        "# EMQX evaluates Erlang-term rules in FILE ORDER and stops at the first match;",
        "# mqttd evaluates a SET: any matching deny wins, then any matching allow permits,",
        "# then `default`. The translation is therefore a re-modelling, not a line-for-line",
        "# map. Read it through before deploying — a converted policy is a draft, not an",
        "# authority.",
        "#",
        # DERIVED from the value being written, never asserted: round 2 and round 3 both found
        # hard-coded deny prose beside a `default = "allow"` file.
        comment_safe("# " + _upper_first(empty_policy_effect(default)) + "."),
        "",
    ]
    out.extend(
        prov.line(
            "default",
            toml_str(default),
            default_source
            or "EMQX's documented default for authorization.no_match, which this "
            "configuration did not set",
        )
    )
    out.append("")
    if default_hint and default_hint != default:
        out.append(
            f"# TODO(migrate): the ACL file's catch-all rule said `{default_hint}`, which "
            f'differs from the `default = "{default}"` above (taken from '
            "authorization.no_match). Decide which one you meant; uncomment to take the "
            "file's:"
        )
        out.extend(
            prov.inert(
                "default",
                toml_str(default_hint),
                "from the ACL file's own catch-all rule — NOT activated",
            )
        )
        out.append("")
    for t in todos:
        out.append(f"# TODO(migrate): {comment_safe(t)}")
    if todos:
        out.append("")
    for r in rules:
        out.append(f"# from: {comment_safe(r['source'])}.")
        out.append("[[rules]]")
        if r["identities"]:
            out.append(f"identities = {toml_list(r['identities'])}")
        else:
            out.append("# (no identities = applies to every authenticated client)")
        out.append(f"actions = {toml_list(r['actions'])}")
        out.append(f"effect = {toml_str(r['effect'])}")
        out.append(f"topics = {toml_list(r['topics'])}")
        out.append("")
    return "\n".join(out) + "\n"


def render_config(conv: Conversion, tls_lines: list[str]) -> str:
    out = [
        "# Translated from an EMQX HOCON configuration by the mqttd EMQX converter",
        "# (scripts/migrate/from-emqx.py). " + VERSIONS + ".",
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
        out.extend(_table(body))
        out.extend(deferred)
        if section == "listeners":
            out.extend(_listener_extras(conv))
        out.append("")
    for section in ("security.jwt", "security.http_auth"):
        body = conv.config.get(section) or {}
        deferred = conv.deferred.get(section) or []
        if not body and not deferred:
            continue
        out.append(f"[{section}]")
        out.extend(_table(body))
        out.extend(deferred)
        out.append("")
    if tls_lines:
        out.extend(tls_lines)
        out.append("")
    return "\n".join(out) + "\n"


def _table(body: dict[str, Emitted]) -> list[str]:
    """One TOML table's lines, each security-relevant value carrying its source key."""
    out: list[str] = []
    for key, em in body.items():
        trailer = ""
        if key in SECURITY_FIELDS and em.source:
            trailer = FROM + comment_safe(em.source)
            if em.defaulted:
                trailer += DEFAULTED + comment_safe(em.defaulted)
        out.append(f"{key} = {em.rendered}{trailer}")
    return out


def _listener_extras(conv: Conversion) -> list[str]:
    """Inline TODOs for the listeners mqttd's one-bind-per-protocol shape cannot hold."""
    out: list[str] = []
    by_proto: dict[str, list[Listener]] = {}
    for lst in conv.listeners:
        by_proto.setdefault(lst.proto, []).append(lst)
    for proto, group in sorted(by_proto.items()):
        for extra in group[1:]:
            out.append(
                comment_safe(
                    f"# TODO(migrate): additional {proto} listener "
                    f"{extra.name!r} on {extra.bind or '?'} — mqttd binds ONE listener per "
                    "protocol; consolidate its clients onto the bind above, or run a second "
                    "deployment"
                )
            )
    return out


def build_listener_binds(conv: Conversion) -> None:
    """First listener of each protocol becomes the bind; extras are inline TODOs.

    A bind is the most security-relevant value this tool writes — it decides which addresses
    the broker publishes on — and this function used to end with
    `lst.bind or ("0.0.0.0:1883" if lst.proto == "tcp" else "0.0.0.0:8883")`: a listener
    whose `bind` the converter had not read became a LIVE bind on every interface, on a port
    the input never named. There is no fallback now: an address that cannot be derived is a
    commented candidate plus a TODO, and the transport comes from `listeners.<proto>`, which
    is the input's own word for it.
    """
    seen: set[str] = set()
    # Only the binds actually emitted LIVE, so the plaintext warning below is derived from
    # what the file will do rather than from what was found in the input.
    live: set[str] = set()
    # A listener whose address IS derivable takes the bind, whatever the document order.
    for lst in sorted(conv.listeners, key=lambda l: l.bind is None):
        key = PROTO_BIND.get(lst.proto)
        where = f"listeners.{lst.proto}.{lst.name}"
        # NEITHER of these may fall through silently. An earlier version of this function
        # `continue`d on both, which turned a listener mqttd cannot express into a SILENT
        # DROP — no live line, no commented candidate, no TODO — i.e. exactly the contract
        # violation the provenance gate exists to prevent, introduced by the fix that
        # stopped fabricating binds. mqttd binds ONE listener per protocol, so an extra one
        # genuinely cannot be expressed; it must still be NAMED, with its address, so the
        # operator can decide which listener the single bind should serve.
        if key is None:
            conv.todo(
                f"{where}: mqttd has no bind for the `{lst.proto}` transport, so this "
                f"listener is not served at all"
                + (f" (its address was {lst.bind})" if lst.bind else "")
                + ". Decide whether its clients move to a transport mqttd does serve "
                "(plaintext / tls / ws / wss / quic) or stay on the incumbent"
            )
            continue
        if key in seen:
            conv.todo(
                f"{where}: an ADDITIONAL `{lst.proto}` listener — mqttd binds one listener "
                f"per protocol, so [listeners] {key} is already taken by the first one and "
                f"this listener's address"
                + (f" ({lst.bind})" if lst.bind else " (which this converter could not derive)")
                + " is NOT in the generated config. Consolidate them behind one address, or "
                "run a second node; either way the clients on this listener do not connect "
                "until you do"
            )
            continue
        seen.add(key)
        if lst.bind is not None:
            live.add(key)
        if lst.bind is None:
            conv.set(
                "listeners",
                key,
                toml_str("0.0.0.0:1883"),
                None,
                decide=f"{where} is a listener mqttd can serve, but this converter could not "
                f"derive its ADDRESS: {lst.bind_gap or 'the block named no `bind` at all'}. "
                f"[listeners] {key} is therefore emitted COMMENTED OUT — the line below is a "
                "PLACEHOLDER, not a value from your EMQX config. Set the real address and "
                "port and uncomment it, or the broker serves no clients on that transport",
            )
            continue
        conv.set(
            "listeners",
            key,
            toml_str(lst.bind),
            f"{where}.bind",
            defaulted=lst.bind_defaulted,
        )
    plaintext = sorted(k for k in ("plaintext_bind", "ws_bind") if k in live)
    if plaintext:
        conv.note(
            "a PLAINTEXT listener was carried over ("
            + ", ".join(plaintext)
            + ", each with the EMQX listener it came from on the line). mqttd logs this as "
            "an INSECURE mode on every start, and credentials cross it in the clear. Retire "
            "it during the migration if you can"
        )


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__.split("\n", maxsplit=1)[0],
        epilog=(
            "PROVENANCE: " + VERSIONS + ". " + SCOPE + " " + DRAFT + " Dashboard/"
            "REST-managed EMQX deployments keep their authn/authz in "
            "data/configs/cluster.hocon, not emqx.conf — pass that file too, or the "
            "authentication/authorization blocks are simply missing. An `include` is NOT "
            "followed: its contents are never read."
        ),
    )
    ap.add_argument("conf", type=Path, help="path to emqx.conf (or cluster.hocon)")
    ap.add_argument("--out-config", type=Path, help="write the mqttd TOML here")
    ap.add_argument("--out-acl", type=Path, help="write the translated ACL here")
    ap.add_argument(
        "--out-bridge", type=Path, help="write an mqtt-bridge config for MQTT bridges here"
    )
    ap.add_argument(
        "--acl-file",
        type=Path,
        help="the Erlang-term acl.conf (overrides authorization.sources[].path)",
    )
    ap.add_argument(
        "--provenance-json",
        type=Path,
        help="write the provenance ledger (every security-relevant value, its EMQX source "
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
    reader = Hocon(text)
    try:
        tree = reader.parse_top()
    except HoconError as e:
        # The documented contract: exit 1 with a message, never a wedge. A malformed
        # structure used to make the reader spin at 100% CPU forever (see Hocon._array).
        print(f"cannot read {args.conf}: {e}", file=sys.stderr)
        print(
            "Nothing was written. Fix the syntax (EMQX itself reports an error on this "
            "file too) and re-run.",
            file=sys.stderr,
        )
        return 1
    if reader.unterminated:
        conv.todo(
            "the input has UNTERMINATED structure(s) — "
            + "; ".join(sorted(set(reader.unterminated)))
            + f" ({len(reader.unterminated)} in total). Everything after the missing "
            "bracket was read in the WRONG SCOPE, so a setting may have been attached to "
            "the wrong block or lost entirely. Fix the syntax and re-run before trusting "
            "one line of this output"
        )
    if reader.skipped:
        conv.todo(
            f"{len(reader.skipped)} fragment(s) of the HOCON input could not be parsed "
            f"and were skipped (first: {reader.skipped[0][:60]!r}). Read the source "
            "config beside this output and translate those by hand — this converter's "
            "HOCON reader handles the subset EMQX writes, not every HOCON feature"
        )
    if not tree:
        conv.todo(
            "the input parsed to NOTHING — an empty or unreadable configuration. If the "
            "file really is empty, EMQX is running on its built-in defaults plus whatever "
            "the dashboard wrote to data/configs/cluster.hocon; pass that file instead. "
            "Nothing below was derived from your deployment"
        )

    convert_listeners(tree, conv)
    build_listener_binds(conv)
    convert_listener_keys(conv)
    convert_authn(tree, conv)
    convert_authz(tree, conv)
    convert_cluster(tree, conv)
    convert_bridges(tree, conv)
    convert_mqtt_and_misc(tree, conv)
    tls_lines, tls_todos = convert_tls(conv)
    for t in tls_todos:
        conv.todo(t)
    if conv.bridges and not args.out_bridge:
        conv.todo(
            f"{len(conv.bridges)} MQTT bridge(s) were found in this configuration and "
            "NOTHING about them is in this file: mqtt-bridge is a separate process with its "
            "own TOML. Re-run with --out-bridge <path> to get that config plus a TODO for "
            "every bridge setting mqtt-bridge cannot express (payload templates and retain "
            "overrides among them)"
        )

    # THE ACL SOURCE IS READ HERE, BEFORE THE CONFIG IS RENDERED, on purpose. When it
    # cannot be read the gap belongs in the files the operator is about to DEPLOY, not in
    # a stderr line they scroll past — and EMQX ships the RELATIVE path `etc/acl.conf`, so
    # running this anywhere but the EMQX install root is the DEFAULT outcome, not an edge
    # case. Round 1 found the config still pointing acl_file at a file that was never
    # written, with nothing in its TODO list about the missing policy.
    acl_path = args.acl_file or (Path(conv.acl_file) if conv.acl_file else None)
    acl_text: str | None = None
    if acl_path is not None:
        try:
            acl_text = acl_path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as e:
            print(f"note: could not read acl file {acl_path}: {e}", file=sys.stderr)
            conv.todo(
                "THE AUTHORIZATION POLICY WAS NOT TRANSLATED. The ACL source "
                f"{str(acl_path)!r} could not be read ({e}), so NO rule from it is in the "
                "generated ACL — which contains the same warning and no rules. EMQX's own "
                "shipped value is the RELATIVE path `etc/acl.conf`, resolved against the "
                "EMQX install directory, so this is what happens when the converter runs "
                "anywhere else: re-run with --acl-file pointing at the real file. [security] "
                "acl_file below still names a policy file because mqttd REFUSES TO START "
                "without the file it names. "
                + (
                    "BUT authorization.no_match was ALLOW, so the empty policy that was "
                    'written carries `default = "allow"` and PERMITS EVERYTHING — it does '
                    "NOT fail closed. Do not deploy this under any circumstances until the "
                    "policy is really translated"
                    if conv.acl_default == "allow"
                    else "Authorization then fails CLOSED rather than open, because an "
                    "empty deny-by-default policy denies everything — but do not deploy "
                    "this until the policy is really translated"
                )
            )

    if not conv.saw_auth:
        conv.todo(
            "NO `authentication` block was found. That does NOT mean the deployment had "
            "no authentication: EMQX's dashboard and REST API persist the authn chain to "
            "data/configs/cluster.hocon, and built-in-database users live in the data "
            "directory. Find the real authenticators before cutover — mqttd refuses "
            "anonymous clients by default ([security] allow_anonymous = false), so a "
            "silent gap here fails closed rather than open, but your clients will not "
            "connect"
        )
    if not conv.saw_authz:
        conv.todo(
            "NO `authorization` block was found — same caveat as authentication: check "
            "data/configs/cluster.hocon and the built-in-database ACL source. Without an "
            "[security] acl_file mqttd does NOT enforce authorization (and says so in the "
            "log), which is the wrong end state"
        )
    if "data_dir" not in conv.config.get("node", {}):
        conv.set("node", "data_dir", toml_str("/var/lib/mqttd"))
        conv.note(
            "no node.data_dir was found, so [node] data_dir was set to mqttd's packaged "
            "default /var/lib/mqttd. Durable sessions are ON by default and REFUSE to "
            "start without a data dir, so this value is what makes the config valid — "
            "mount a real volume there, or the durable state lives on the container's "
            "ephemeral layer"
        )
    elif not conv.config["node"]["data_dir"].rendered.strip('"').startswith("/"):
        conv.note(
            f"node.data_dir was RELATIVE ({conv.config['node']['data_dir'].rendered}) — EMQX "
            "resolves it against its install directory. mqttd resolves it against the "
            "process working directory, which in a container is not where you think. "
            "Replace it with an absolute path on a mounted volume"
        )
    if "id" not in conv.config.get("node", {}):
        conv.set("node", "id", toml_str("node-1"))
        conv.note(
            "no node.name was found, so [node] id defaults to node-1. It must be UNIQUE "
            "per node in a cluster and equal that node's bus-certificate CN"
        )
    if not conv.config.get("listeners"):
        conv.todo(
            "NO listener was found, so mqttd would bind nothing and serve no clients. Set "
            "[listeners] tls_bind (and [tls] cert/key) at minimum"
        )
    if conv.acl_file or args.acl_file:
        conv.set(
            "security",
            "acl_file",
            toml_str("/etc/mqttd/acl.toml"),
            (
                f"--acl-file {args.acl_file}"
                if args.acl_file
                else f"authorization.sources[].path = {conv.acl_file}"
            )
            + " (the POLICY is from there; the path below is this converter's own --out-acl "
            "deployment default)",
            defaulted="the deployed path itself, which is yours to choose",
        )
    elif conv.saw_authz:
        # An `authorization` block existed but no usable file source came out of it (every
        # source disabled, or a built_in_database/http source only). Leaving [security]
        # acl_file unset is the FAIL-OPEN direction — mqttd enforces no authorization at
        # all — so it must not pass without a marker.
        conv.todo(
            "an `authorization` block WAS present but no ACL policy file came out of it, so "
            "[security] acl_file is NOT set below and mqttd will enforce NO authorization at "
            "all (it says so in the log on every start): every authenticated client could "
            "publish and subscribe anywhere. Translate the policy by hand into an ACL file "
            "and set acl_file, or re-run with --acl-file <the real acl.conf>"
        )
    if "allow_anonymous" not in conv.config.get("security", {}):
        # mqttd's own default, and the fail-CLOSED direction: nothing in an EMQX config grants
        # anonymous access node-wide (its `enable_authn = false` is PER LISTENER, which mqttd
        # cannot express and which is reported where it is read). Written explicitly so the
        # posture is visible in the file, with the source saying it was not derived from the
        # input rather than implying it was.
        conv.set(
            "security",
            "allow_anonymous",
            "false",
            "mqttd's own default (nothing in the EMQX configuration grants anonymous access "
            "node-wide; a per-listener enable_authn = false is reported separately and CANNOT "
            "be translated)",
            defaulted="the value itself — it is mqttd's default, not an EMQX setting",
        )

    config = render_config(conv, tls_lines)
    if args.out_config:
        args.out_config.write_text(config, encoding="utf-8")
        print(f"wrote {args.out_config}")
    else:
        print(config)

    if acl_path is not None:
        if acl_text is None:
            # An unreadable source is not fatal (the from-mosquitto contract), but the
            # ACL document is still written: deny-by-default, zero rules and the gap
            # stated at the top, so the file the operator deploys says what happened.
            rules, todos, hint = [], [
                "NOTHING WAS TRANSLATED INTO THIS FILE. The EMQX ACL source "
                f"{str(acl_path)!r} could not be read, so this policy has NO rules and "
                f"{empty_policy_effect(conv.acl_default)}. Find the real acl.conf (EMQX "
                "ships the RELATIVE path `etc/acl.conf`, resolved against its install "
                "directory) and re-run with --acl-file, and check whether the policy "
                "actually lived in the built-in database instead, which this converter "
                "cannot see"
            ], None
        else:
            rules, todos, hint = parse_acl(acl_text)
            if not rules:
                todos.insert(
                    0,
                    "NO RULE could be translated from the EMQX ACL source. Either every "
                    "term landed on a gap listed below, or the file contained nothing this "
                    "converter recognises. With no rules, "
                    f"{empty_policy_effect(conv.acl_default)}. Read the TODOs below before "
                    "deploying",
                )
        acl = render_acl(
            rules, todos, conv.acl_default, hint, conv.acl_default_source, conv.prov
        )
        if args.out_acl:
            args.out_acl.write_text(acl, encoding="utf-8")
            print(f"wrote {args.out_acl} ({len(rules)} rules)")
        else:
            print(acl)

    if args.out_bridge:
        args.out_bridge.write_text(render_bridge(conv), encoding="utf-8")
        print(f"wrote {args.out_bridge} ({len(conv.bridges)} upstreams)")

    if args.provenance_json:
        args.provenance_json.write_text(
            conv.prov.ledger("from-emqx.py"), encoding="utf-8"
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
