#!/usr/bin/env python3
"""Property sweep over a converter: permuted inputs, one invariant per defect CLASS.

Why this exists, in one sentence: two adversarial rounds fixed the *named sites* of six
defect classes and the classes survived somewhere else, so this file checks the class
instead of the site.

The fixture tests beside it (`test-from-*.sh`) are example-based: one realistic input, a
list of greps. That shape catches a regression at the exact place a reviewer already looked
and is blind everywhere else — the first-listener-only TLS defect was found three times in
three converters because each converter's harness only ever fed it ONE ordering of ONE
listener set. So this generates many inputs from a cross product (listener ORDER, enable
flags, mTLS posture, `no_match` posture, truststore presence) and asserts properties that
must hold for *every* one of them:

  A. NOTHING SILENTLY DROPPED. Every security-relevant value in the input appears in the
     output — either translated into a key, or inside a `# TODO(migrate):` / `# NOTE:` line
     that names it. Because each generated value is a unique string, "appears" is an exact
     test, and it holds under every permutation rather than the one the fixture happens to
     use. This is what makes "read from listener[0], applied as if global" detectable: move
     the mandate to the second listener and the property fails.

  B. NOTHING DISABLED IS ACTIVATED. A listener/authenticator/user the source switched OFF
     must not appear as a live bind, URL or `[[rules]]` entry — and must still be named.

  C. NO CLAIM CONTRADICTS THE VALUE IT DESCRIBES. Every deny/allow assertion in a generated
     document is checked against the `default` that document actually writes.

  D. NO DANGLING CROSS-REFERENCE. Every `step N` the output tells the operator to run must
     be a step the output actually printed.

  E. THE BROKER ACCEPTS IT. `mqttd --check-config` on every generated config, because
     "parses as TOML" is not the same as "the broker loads it", and rule 3 of the contract is
     that the output must validate.

  F. PROVENANCE. Every security-relevant value in the output must be a value the INPUT
     held. This is the invariant the first version of this file could not express: (A) asks
     only whether a value appears SOMEWHERE, so it passed while `tls_bind` was FABRICATED as
     `0.0.0.0:1883` for an input that said `port 18883` / `bind_address 127.0.0.77` — the
     input's values did appear, inside a TODO that misdescribed them. F reads the other
     direction: for every live `*_bind` the PORT must be a port the input named, and for
     every live path/URL the string must be one the input contained. A part of a value the
     input did not hold is allowed ONLY where the line says `defaulted: <what and why>` (a
     vendor-documented default of a directive that WAS present, or a path the converter
     itself owns) — and every one of those is counted and printed, so the escape hatch
     cannot be used silently.

  G. NO LIVE SETTING WITHOUT A SOURCE. Every uncommented security-relevant line must carry
     `# from: <input key>`. This is the structural half: the converters emit those lines
     through one gate that refuses to write a live security-relevant value without the input
     key it came from, and this invariant is what proves the gate was not bypassed. Together
     F and G make the whole fail-open CLASS detectable rather than re-findable — the class
     every serious finding of three review rounds belonged to.

  H. EVERY LIVE BIND IS AN ADDRESS THE BROKER CAN BIND. `mqttd --check-config` accepts ANY
     string in a `*_bind` and the broker then fails at STARTUP, so E could not see this at
     all: `ws_bind = ":8085"`, `plaintext_bind = "10.0.0.1:abc"` and
     `plaintext_bind = "/tmp/mosq.sock:0"` (a Mosquitto UNIX-socket listener, which declares
     no TCP endpoint at all) each passed every invariant above and then refused to start.
     H parses the host and the port of every live bind. It is the invariant that makes the
     documented verification step cover the one value this whole file is about.

Plus a FUZZ pass (`--fuzz N`): each fixture is mutated mechanically — random lines deleted,
the file truncated mid-structure, listener blocks permuted, enable flags flipped, transports
swapped — and the converter must ALWAYS exit 0 or 1 with a message, never hang, and never
emit a live security-relevant line without provenance. The two-line reproducer that made
from-emqx.py's HOCON reader spin forever at 100% CPU is a seeded case.

Not a replacement for the fixture tests: those pin exact wording and vendor provenance,
which a property test cannot. Run both.

Usage:
    python3 scripts/migrate/property_sweep.py mosquitto|emqx|hivemq [--mqttd PATH]
    python3 scripts/migrate/property_sweep.py mosquitto|emqx|hivemq --fuzz 200

Exit codes: 0 every case held, 1 a property failed (with the case and the reason), 2 the
converter or the broker binary is missing.
"""

from __future__ import annotations

import argparse
import itertools
import json
import random
import re
import subprocess
import sys
import tempfile
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
MIGRATE = ROOT / "scripts" / "migrate"
FIXTURES = MIGRATE / "fixtures"

# How long any single converter run may take. The contract is "exit 0 translated, 1 could not
# read the input"; a run that has not finished in this long is a HANG, which is what
# from-emqx.py's hand-written HOCON reader did on a truncated array — 100% CPU and unbounded
# memory, never printing an error, never exiting.
RUN_TIMEOUT = 20


@dataclass
class Case:
    """One generated input plus what must be true of its output."""

    name: str
    files: dict[str, str]
    argv: list[str]
    outputs: list[str]
    # (what it is, [any one of these substrings must appear in the outputs])
    witnesses: list[tuple[str, list[str]]] = field(default_factory=list)
    # (what it is, a substring that must NOT appear)
    forbidden: list[tuple[str, str]] = field(default_factory=list)
    # (what it is, a substring that must not appear ANYWHERE — comments included). `forbidden`
    # is checked with comment lines stripped, because a commented candidate is the contract's
    # prescribed handling for a posture change; but some defects ARE a comment — a rule emitted
    # with no `identities` prints `# (no identities = applies to every authenticated client)`,
    # which is exactly the widening to catch, and stripping comments made that check vacuous.
    # Found 2026-08-15 while mutation-proving the anonymous-ACL fix.
    forbidden_anywhere: list[tuple[str, str]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# CLASS C — a claim about a computed value.
#
# Phrases that assert a policy DENIES, and phrases that assert it PERMITS. Each is checked
# against the `default` the same document writes. Round 2 found both zero-rule ACL TODOs
# asserting "fail-closed ... default = deny" in a file whose own first line could be
# `default = "allow"`.
# ---------------------------------------------------------------------------

DENY_CLAIMS = (
    "denies every publish",
    "denies everything",
    "refuses every publish",
    "refuse every publish",
    "fail-closed",
    "fails CLOSED",
    "fails closed",
    "DENY BY DEFAULT",
    "anything not allowed below is refused",
)
ALLOW_CLAIMS = (
    "anything not DENIED below is PERMITTED",
    "wide open",
    "permits every publish",
)


def check_class_c(text: str, label: str) -> list[str]:
    """Every deny/allow assertion in a policy document must match its own `default`."""
    m = re.search(r'^default = "(allow|deny)"', text, re.M)
    if not m:
        return []
    actual = m.group(1)
    bad: list[str] = []
    for line in text.splitlines():
        if not line.lstrip().startswith("#"):
            continue
        if actual == "allow":
            for claim in DENY_CLAIMS:
                if claim in line:
                    bad.append(
                        f"{label} writes `default = \"allow\"` but a comment asserts "
                        f"{claim!r}: {line.strip()[:160]}"
                    )
        else:
            for claim in ALLOW_CLAIMS:
                if claim in line:
                    bad.append(
                        f"{label} writes `default = \"deny\"` but a comment asserts "
                        f"{claim!r}: {line.strip()[:160]}"
                    )
    return bad


# ---------------------------------------------------------------------------
# CLASS D — an instruction that references a step the tool did not emit.
# ---------------------------------------------------------------------------

_STEP_REF = re.compile(r"\bsteps?\s+(\d+(?:\s*\+\s*\d+)*)", re.I)
_STEP_DEF = re.compile(r"^#\s*#?\s*(\d+)\.\s")


def check_class_d(text: str, label: str) -> list[str]:
    """Every `step N` the output tells the operator to run must be a step it printed."""
    defined = {
        int(m.group(1)) for m in (_STEP_DEF.match(l.strip()) for l in text.splitlines()) if m
    }
    referenced: set[int] = set()
    for m in _STEP_REF.finditer(text):
        referenced.update(int(n) for n in re.findall(r"\d+", m.group(1)))
    if not referenced:
        return []
    missing = sorted(referenced - defined)
    if missing:
        return [
            f"{label} tells the operator to run step(s) {missing} but only printed "
            f"{sorted(defined) or 'none'} — the recipe has a hole, and the broker refuses to "
            "start on the placeholder those steps were supposed to replace"
        ]
    return []


# ---------------------------------------------------------------------------
# CLASS F / G — PROVENANCE, and NO LIVE SETTING WITHOUT A SOURCE.
#
# The two invariants that make the whole fail-open class detectable. Every serious finding of
# the three 2026-08 review rounds was one shape: a LIVE security-relevant value the converter
# had not derived from the input (a fabricated bind, a listener EMQX had switched off, an
# mTLS mandate taken from the wrong listener, a TLS bridge converted to plaintext). (A) could
# not see any of them, because it only asks whether the input's values appear SOMEWHERE.
# ---------------------------------------------------------------------------

# The keys whose value decides who can connect and what they may do. Kept in step with each
# converter's own SECURITY_FIELDS — deliberately duplicated rather than imported, because a
# check that shares its definition with the thing it checks cannot catch a change to it.
SECURITY_KEYS = frozenset(
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
        "url",
        "ca",
        "issuer",
        "audience",
        "hs256_secret_file",
        "rs256_pem_file",
    }
)

# The `*_bind` keys, whose PORT must always be a port the input named. The host may be a
# vendor-documented default (`listener 8883` with no address binds every interface), but a
# port that appears nowhere in the input is the fabrication this invariant exists to catch.
BIND_KEYS = frozenset(k for k in SECURITY_KEYS if k.endswith("_bind"))

# The keys whose value is an ADDRESS, a PATH or a URL — a string copied out of the input,
# which invariant F can therefore look for in the input. The rest of SECURITY_KEYS hold an
# ENUM or a boolean (`default = "deny"`, `mtls_identity_source = "cn"`, `allow_anonymous`,
# `allow_tls12`): those are the TRANSLATION of an input construct rather than a copy of an
# input string, so F cannot check their value and invariant G — that the line names the input
# key it was derived from — is what governs them.
COPIED_KEYS = BIND_KEYS | frozenset(
    {
        "cert",
        "key",
        "client_ca",
        "crl",
        "acl_file",
        "password_file",
        "url",
        "ca",
        "issuer",
        "audience",
        "hs256_secret_file",
        "rs256_pem_file",
    }
)

_LIVE_LINE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*) = (.+?)\s*$")
_QUOTED = re.compile(r'"((?:[^"\\]|\\.)*)"')


def _unescape(value: str) -> str:
    return value.replace('\\"', '"').replace("\\\\", "\\")


def check_class_g(text: str, label: str) -> list[str]:
    """Every uncommented security-relevant line must name the input key it came from."""
    bad: list[str] = []
    for line in text.splitlines():
        if line.lstrip().startswith("#"):
            continue
        m = _LIVE_LINE.match(line)
        if not m or m.group(1) not in SECURITY_KEYS:
            continue
        if "# from: " not in line:
            bad.append(
                f"{label} emits the security-relevant setting `{m.group(1)}` LIVE with no "
                f"recorded source key (`# from: <input key>`): {line.strip()[:160]}. A value "
                "the converter did not derive from the input must be emitted COMMENTED OUT "
                "with a TODO naming the decision — that is the whole fail-open class"
            )
    return bad


def check_class_f(text: str, label: str, source_text: str) -> tuple[list[str], list[str]]:
    """Every live security-relevant value must be one the INPUT held.

    Returns `(problems, defaulted)` — the second list is every value part the input did not
    hold that the line declared as `defaulted:`, so the escape hatch is visible in the run's
    own output rather than silent.
    """
    bad: list[str] = []
    defaulted_notes: list[str] = []
    for line in text.splitlines():
        if line.lstrip().startswith("#"):
            continue
        m = _LIVE_LINE.match(line)
        if not m or m.group(1) not in COPIED_KEYS:
            continue
        field, rest = m.group(1), m.group(2)
        provenance = rest.partition("# from: ")[2]
        declared_default = "defaulted: " in provenance
        values = [_unescape(v) for v in _QUOTED.findall(rest.partition("  # from: ")[0])]
        if not values:
            continue  # a bare boolean/integer, e.g. allow_anonymous = false
        for value in values:
            parts = [value]
            if field in BIND_KEYS and ":" in value:
                host, _, port = value.rpartition(":")
                # The PORT is never allowed to be defaulted: a bind whose port appears
                # nowhere in the input is exactly the fabrication round 3 found.
                if port not in source_text:
                    bad.append(
                        f"{label} binds `{field} = {value}` but the PORT {port} appears "
                        "NOWHERE in the input. A bind is the most security-relevant value "
                        "this tool writes — a fabricated one publishes the broker on an "
                        "address the operator never chose"
                    )
                parts = [host]
            for part in parts:
                if not part or part in source_text:
                    continue
                if declared_default:
                    defaulted_notes.append(f"{label}: {field} = {part!r} ({provenance[:90]})")
                    continue
                bad.append(
                    f"{label} emits `{field} = {value}` LIVE, and {part!r} is NOWHERE in the "
                    f"input (its stated source is {provenance[:120]!r}). A security-relevant "
                    "value the input did not hold is a fabrication: emit it commented out, or "
                    "name the vendor-documented default it came from with `defaulted:`"
                )
    return bad, defaulted_notes


# ---------------------------------------------------------------------------
# CLASS H — a live bind the broker cannot actually bind.
#
# `mqttd --check-config` accepts any string in a `*_bind` (it is a String in the config
# struct; resolution happens at bind time), so invariant E — the verification every converter's
# header, --help and docs/MIGRATION.md point the operator at — verifies NOTHING about the one
# value the provenance restructuring exists for. An operator who runs the prescribed gate, sees
# `config OK` and schedules the cutover finds out at the maintenance window. Deliberately
# written from the OUTPUT alone, with no import from any converter: a check that shares its
# definition with the thing it checks cannot catch a change to it. Found 2026-08-15.
# ---------------------------------------------------------------------------

_HOST_OK = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_:")


def check_class_h(text: str, label: str) -> list[str]:
    """Every live `*_bind` must be a host:port the broker can bind."""
    bad: list[str] = []
    for line in text.splitlines():
        if line.lstrip().startswith("#"):
            continue
        m = _LIVE_LINE.match(line)
        if not m or m.group(1) not in BIND_KEYS:
            continue
        values = [_unescape(v) for v in _QUOTED.findall(m.group(2).partition("  # from: ")[0])]
        for value in values:
            host, sep, port = value.rpartition(":")
            if host.startswith("[") and host.endswith("]"):
                host = host[1:-1]
            why = None
            if not sep:
                why = "it has no port"
            elif not host:
                why = "it names no host, and the broker refuses to resolve an empty one"
            elif not port.isdigit() or not 1 <= int(port) <= 65535:
                why = f"{port!r} is not a TCP port number (1-65535)"
            elif any(c not in _HOST_OK for c in host):
                why = f"{host!r} is not an address or hostname the broker can resolve"
            if why:
                bad.append(
                    f"{label} emits `{m.group(1)} = {value}` LIVE, and that is NOT an address "
                    f"mqttd can bind: {why}. `--check-config` accepts it and the broker then "
                    "fails at STARTUP — an unbindable address must be emitted COMMENTED OUT "
                    "with a TODO, like any other value the converter could not derive"
                )
    return bad


# ---------------------------------------------------------------------------
# Mosquitto
# ---------------------------------------------------------------------------

_M_PLAIN = ["listener 1883 127.0.0.1"]
_M_BROWSER = [
    "listener 8884 0.0.0.0",
    "certfile /certs/browser.crt",
    "keyfile /certs/browser.key",
    "tls_version tlsv1.2",
    # A per-listener anonymous posture that DISAGREES with the device listener's. mqttd's
    # [security] is node-wide, so one of the two must lose — and the loss must be reported.
    "allow_anonymous true",
]


def mosquitto_cases() -> list[Case]:
    cases: list[Case] = []
    for order, req, per_listener in itertools.product(
        itertools.permutations(("plain", "browser", "device")), ("true", "false"), (True, False)
    ):
        device = [
            "listener 8883 0.0.0.0",
            "certfile /certs/device.crt",
            "keyfile /certs/device.key",
            "cafile /certs/device-ca.crt",
            "crlfile /certs/device.crl",
            f"require_certificate {req}",
            "use_identity_as_username true",
            "allow_anonymous false",
        ]
        blocks = {"plain": _M_PLAIN, "browser": _M_BROWSER, "device": device}
        lines = ["persistence_location /var/lib/mosquitto"]
        if per_listener:
            lines.append("per_listener_settings true")
        lines.append("acl_file aclfile")
        for which in order:
            lines.extend(blocks[which])
        lines.append("include_dir /etc/mosquitto/conf.d")
        name = f"mosquitto order={'-'.join(order)} require_certificate={req} pls={per_listener}"
        # Every one of these strings is a security-relevant VALUE from the input. Each must
        # survive into the output somewhere — translated, or named in a TODO/NOTE.
        witnesses = [
            (k, [k])
            for k in (
                "/certs/browser.crt",
                "/certs/device.crt",
                "/certs/device-ca.crt",
                "/certs/device.crl",
                "tlsv1.2",
                "use_identity_as_username",
                "/etc/mosquitto/conf.d",
            )
        ] + [
            (
                "the node-wide collapse of a per-listener allow_anonymous",
                ["allow_anonymous was set MORE THAN ONCE with DIFFERENT values"],
            )
        ]
        # ...and the mTLS POSTURE ITSELF must be reported, naming the listener that held it.
        # Without this the sweep could not tell "reported as a disagreement" from "reported as
        # a cafile nobody required": reading the gate off listener[0] turns the first into the
        # second whenever the mandating listener is not first, which is precisely the defect.
        if req == "true":
            witnesses.append(
                (
                    "the mTLS MANDATE on listener 0.0.0.0:8883",
                    [
                        "require_certificate was TRUE on listener 0.0.0.0:8883",
                        "REQUIRED on listener 0.0.0.0:8883",
                    ],
                )
            )
        else:
            witnesses.append(
                (
                    "the cert-optional posture, named",
                    ["require_certificate was NOT true on any TLS listener"],
                )
            )
        # mandated only when EVERY TLS listener required a certificate, which is never true
        # here (the browser listener never does), so an ACTIVE client_ca would be invented.
        forbidden = [
            ("an invented mTLS mandate", '\nclient_ca = "'),
            # tls.crl requires tls.client_ca — the broker refuses the pair outright.
            ("a CRL beside no client_ca", "\ncrl = "),
        ]
        cases.append(
            Case(
                name=name,
                files={"mosquitto.conf": "\n".join(lines) + "\n", "aclfile": _M_ACL},
                argv=[
                    "mosquitto.conf",
                    "--out-config",
                    "out.toml",
                    "--out-acl",
                    "acl.toml",
                    "--acl-file",
                    "aclfile",
                    "--provenance-json",
                    "prov.json",
                ],
                outputs=["out.toml", "acl.toml"],
                witnesses=witnesses,
                forbidden=forbidden,
            )
        )
    # The unanimous case, where the mandate IS a mapping and the CRL becomes legal.
    lines = [
        "persistence_location /var/lib/mosquitto",
        "acl_file aclfile",
        "listener 8883 0.0.0.0",
        "certfile /certs/device.crt",
        "keyfile /certs/device.key",
        "cafile /certs/device-ca.crt",
        "crlfile /certs/device.crl",
        "require_certificate true",
    ]
    cases.append(
        Case(
            name="mosquitto unanimous require_certificate",
            files={"mosquitto.conf": "\n".join(lines) + "\n", "aclfile": _M_ACL},
            argv=[
                "mosquitto.conf",
                "--out-config",
                "out.toml",
                "--out-acl",
                "acl.toml",
                "--acl-file",
                "aclfile",
            ],
            outputs=["out.toml", "acl.toml"],
            witnesses=[
                ("the mandate", ['client_ca = "/certs/device-ca.crt"']),
                ("the CRL", ['crl = "/certs/device.crl"']),
                ("acl_file, or the policy is not enforced", ['acl_file = "']),
            ],
        )
    )
    # THE DEFAULT-LISTENER FORM. `port` / `bind_address` are how mosquitto.conf(5) configures
    # the default listener, and the generators here only ever emitted `listener` lines — which
    # is why invariant A passed while the bind was FABRICATED as `0.0.0.0:1883` for an input
    # that said `port 18883` / `bind_address 127.0.0.77`. The values DID appear in the output,
    # inside a TODO that misdescribed them. F is what catches it; this is the input that
    # reaches F.
    for with_tls in (True, False):
        lines = ["persistence_location /v", "port 18883", "bind_address 127.0.0.77"]
        witnesses = [
            ("the port the input named", ["18883"]),
            ("the address the input named", ["127.0.0.77"]),
        ]
        if with_tls:
            lines += ["certfile /certs/default.crt", "keyfile /certs/default.key"]
            witnesses.append(("the TLS bind, from the default-listener form",
                              ['tls_bind = "127.0.0.77:18883"']))
        else:
            witnesses.append(("the plaintext bind, from the default-listener form",
                              ['plaintext_bind = "127.0.0.77:18883"']))
        cases.append(
            Case(
                name=f"mosquitto default-listener form (port/bind_address) tls={with_tls}",
                files={"mosquitto.conf": "\n".join(lines) + "\n"},
                argv=["mosquitto.conf", "--out-config", "out.toml"],
                outputs=["out.toml"],
                witnesses=witnesses,
                # The fabrication itself: a bind on a port that is nowhere in the input.
                forbidden=[("a fabricated bind", '_bind = "0.0.0.0:1883"')],
            )
        )
    # `bind_address` with NO port: the vendor documents 1883 as the default, but a bind is the
    # one value this tool must never supply half of.
    cases.append(
        Case(
            name="mosquitto bind_address with no port",
            files={"mosquitto.conf": "persistence_location /v\nbind_address 10.0.0.5\n"},
            argv=["mosquitto.conf", "--out-config", "out.toml"],
            outputs=["out.toml"],
            witnesses=[
                ("the address the input named", ["10.0.0.5"]),
                ("the missing port, named", ["NEVER a port"]),
            ],
            forbidden=[("a bind whose port nobody wrote", '\nplaintext_bind = ')],
        )
    )
    # `protocol websockets`: mqttd HAS ws_bind/wss_bind, and a WebSocket listener emitted as a
    # raw-MQTT bind breaks every browser client at cutover — while also deciding, silently,
    # whose material wins the single [tls] table.
    cases.append(
        Case(
            name="mosquitto protocol websockets (ws + wss + a real TLS device listener)",
            files={
                "mosquitto.conf": "persistence_location /v\n"
                "listener 1883 127.0.0.1\n"
                "listener 9001 0.0.0.0\nprotocol websockets\n"
                "listener 8084 0.0.0.0\nprotocol websockets\n"
                "certfile /certs/wss.crt\nkeyfile /certs/wss.key\n"
                "listener 8883 0.0.0.0\ncertfile /certs/device.crt\n"
                "keyfile /certs/device.key\n"
            },
            argv=["mosquitto.conf", "--out-config", "out.toml"],
            outputs=["out.toml"],
            witnesses=[
                ("the plaintext bind", ['plaintext_bind = "127.0.0.1:1883"']),
                ("the WebSocket bind", ['ws_bind = "0.0.0.0:9001"']),
                ("the secure WebSocket bind", ['wss_bind = "0.0.0.0:8084"']),
                ("the raw-MQTT TLS bind", ['tls_bind = "0.0.0.0:8883"']),
            ],
            forbidden=[
                # The WSS listener must not claim tls_bind, and the device listener must not
                # be demoted to an "additional TLS listener" TODO.
                ("the wss port bound as raw MQTT", 'tls_bind = "0.0.0.0:8084"'),
            ],
        )
    )
    # An unknown transport is a transport this converter cannot identify: no bind at all.
    cases.append(
        Case(
            name="mosquitto protocol with a value the man page does not have",
            files={
                "mosquitto.conf": "persistence_location /v\nlistener 9001\n"
                "protocol future-thing\n"
            },
            argv=["mosquitto.conf", "--out-config", "out.toml"],
            outputs=["out.toml"],
            witnesses=[("the unidentifiable transport, named", ["future-thing"])],
            forbidden=[("a guessed transport", "_bind = ")],
        )
    )
    # max_connections: PER LISTENER, and `-1` is the vendor's documented spelling of
    # unlimited — which mqttd's u64 REFUSES outright, so invariant E catches it too.
    cases.append(
        Case(
            name="mosquitto per-listener max_connections, including the -1 sentinel",
            files={
                "mosquitto.conf": "persistence_location /v\n"
                "listener 8883 0.0.0.0\ncertfile /certs/d.crt\nkeyfile /certs/d.key\n"
                "max_connections 100\n"
                "listener 1883 127.0.0.1\nmax_connections 100000\n"
            },
            argv=["mosquitto.conf", "--out-config", "out.toml"],
            outputs=["out.toml"],
            witnesses=[
                ("the tight per-listener cap", ["100"]),
                ("the collapse, reported", ["the SMALLEST (100) was used"]),
            ],
            forbidden=[("the loosest cap winning", "max_connections = 100000")],
        )
    )
    cases.append(
        Case(
            name="mosquitto max_connections -1 (the vendor's documented default)",
            files={
                "mosquitto.conf": "persistence_location /v\nlistener 1883\n"
                "max_connections -1\n"
            },
            argv=["mosquitto.conf", "--out-config", "out.toml"],
            outputs=["out.toml"],
            witnesses=[("the sentinel, named", ["documents as UNLIMITED"])],
            forbidden=[("a negative integer the broker refuses", "max_connections = -1")],
        )
    )
    # The Dynamic Security plugin: mosquitto.conf(5) recommends it OVER password_file, and its
    # policy lives in a JSON file this converter never opens.
    cases.append(
        Case(
            name="mosquitto dynamic-security plugin (a policy file that was NOT read)",
            files={
                "mosquitto.conf": "persistence_location /v\nlistener 1883\n"
                "allow_anonymous false\n"
                "plugin /usr/lib/mosquitto_dynamic_security.so\n"
                "plugin_opt_config_file /etc/mosquitto/dynamic-security.json\n"
            },
            argv=["mosquitto.conf", "--out-config", "out.toml"],
            outputs=["out.toml"],
            witnesses=[
                ("the plugin config that was not read", ["dynamic-security.json"]),
                ("the derived conclusion", ["do NOT conclude your old broker authorized"]),
            ],
        )
    )
    # A TLS-PSK LISTENER. ENCRYPTED (mosquitto.conf(5): "The psk_hint option enables
    # pre-shared-key support for this listener") and UNMAPPABLE, so it must never become a
    # plaintext bind. Neither psk key was in TLS_KEYS nor in the half-material net, so `is_tls`
    # was false and the listener took `plaintext_bind` — an encrypted transport downgraded to
    # cleartext, with a genuine `# from:` on the line, which is why F and G both passed.
    for extra in ("", "tls_version tlsv1.2\n"):
        cases.append(
            Case(
                name=f"mosquitto TLS-PSK listener (tls_version={bool(extra)})",
                files={
                    "mosquitto.conf": "persistence_location /v\n"
                    "listener 8883\npsk_file /etc/mosq/psk\npsk_hint pskid\n" + extra
                },
                argv=["mosquitto.conf", "--out-config", "out.toml"],
                outputs=["out.toml"],
                witnesses=[
                    ("the PSK material, named", ["/etc/mosq/psk"]),
                    ("the PSK hint, named", ["pskid"]),
                    ("the downgrade, refused", ["DOWNGRADE an encrypted transport"]),
                ],
                forbidden=[
                    ("an encrypted listener published in cleartext", "plaintext_bind = "),
                    ("a PSK listener served as raw MQTT", "ws_bind = "),
                ],
            )
        )
    # A UNIX-SOCKET listener declares NO TCP endpoint (mosquitto.conf(5): "the port must be set
    # to 0, and the unix socket path must be given"), so a bind derived from it is a transport
    # fabrication — and `--check-config` says `config OK` on it.
    cases.append(
        Case(
            name="mosquitto unix-socket listener (no TCP endpoint at all)",
            files={"mosquitto.conf": "persistence_location /v\nlistener 0 /tmp/mosq.sock\n"},
            argv=["mosquitto.conf", "--out-config", "out.toml"],
            outputs=["out.toml"],
            witnesses=[
                ("the socket path, named", ["/tmp/mosq.sock"]),
                ("the missing transport, named", ["unix-socket transport"]),
            ],
            forbidden=[("a bind the broker cannot bind", "plaintext_bind = ")],
        )
    )
    # THE ANONYMOUS-SCOPED ACL BLOCK. mosquitto.conf(5): "The first set of topics are applied to
    # anonymous clients, assuming allow_anonymous is true" — emitted with NO identities, mqttd
    # applies them to EVERY authenticated client, which is strictly broader in both postures.
    cases.append(
        Case(
            name="mosquitto ACL with a pre-`user` (anonymous) topic block",
            files={
                "mosquitto.conf": "persistence_location /v\nlistener 1883\n"
                "allow_anonymous false\nacl_file aclfile\n",
                "aclfile": "topic read public/#\ntopic readwrite anon/#\n"
                "user alice\ntopic readwrite private/alice/#\n",
            },
            argv=[
                "mosquitto.conf",
                "--out-config",
                "out.toml",
                "--out-acl",
                "acl.toml",
                "--acl-file",
                "aclfile",
            ],
            outputs=["out.toml", "acl.toml"],
            witnesses=[
                ("the anonymous scope, named", ["applied to anonymous clients"]),
                ("the rules, scoped", ['identities = ["anonymous"]']),
                ("the named user's own rule", ['identities = ["alice"]']),
            ],
            forbidden_anywhere=[
                (
                    "an anonymous-only grant widened to every authenticated identity",
                    "# (no identities = applies to every authenticated client)",
                )
            ],
        )
    )
    # A literal `*` in a username, and a literal %c in a plain `topic` filter: mqttd has NO
    # escape for either (crates/mqtt-auth/src/acl.rs), so both would emit a rule BROADER than
    # the source. The strings are byte-identical to the input, so F cannot see them.
    cases.append(
        Case(
            name="mosquitto ACL with a glob-metacharacter username and a %c topic",
            files={
                "mosquitto.conf": "persistence_location /v\nlistener 1883\nacl_file aclfile\n",
                "aclfile": "user alice*bob\ntopic write out/#\n"
                "user bob\ntopic read c/%c/x\n",
            },
            argv=[
                "mosquitto.conf",
                "--out-config",
                "out.toml",
                "--out-acl",
                "acl.toml",
                "--acl-file",
                "aclfile",
            ],
            outputs=["out.toml", "acl.toml"],
            witnesses=[
                ("the literal-star username, named", ["alice*bob"]),
                ("the substituting topic, named", ["c/%c/x"]),
            ],
            forbidden=[
                ("a literal username emitted as a glob", 'identities = ["alice*bob"]'),
                ("a literal filter emitted as a substituting rule", '"c/%c/x"'),
            ],
        )
    )
    # `message_size_limit 0` is the vendor's documented spelling of NO LIMIT ("The default value
    # is 0, which means that all valid MQTT messages are accepted"); mqttd FLOORS
    # max_packet_size to 1024, so passing the 0 through turns an unlimited broker into one that
    # refuses any packet over 1 KiB — accepted by --check-config, so E cannot see it either.
    cases.append(
        Case(
            name="mosquitto message_size_limit 0 (the vendor's spelling of unlimited)",
            files={
                "mosquitto.conf": "persistence_location /v\nlistener 1883\n"
                "message_size_limit 0\n"
            },
            argv=["mosquitto.conf", "--out-config", "out.toml"],
            outputs=["out.toml"],
            witnesses=[("the sentinel, named", ["documents as NO LIMIT"])],
            forbidden=[("a 1 KiB ceiling on an unlimited broker", "max_packet_size = 0")],
        )
    )
    # No acl_file at all: authorization is OFF, and the output must say so.
    cases.append(
        Case(
            name="mosquitto with no acl_file",
            files={"mosquitto.conf": "persistence_location /v\nlistener 1883\n"},
            argv=["mosquitto.conf", "--out-config", "out.toml"],
            outputs=["out.toml"],
            witnesses=[("the authorization gap", ["NO authorization at all"])],
            forbidden=[("an acl_file pointing at nothing", "acl_file = ")],
        )
    )
    return cases


_M_ACL = "user sensor-1\ntopic write sensors/sensor-1/#\npattern read devices/%u/status\n"


# ---------------------------------------------------------------------------
# EMQX
# ---------------------------------------------------------------------------


def _emqx_listener(proto: str, name: str, port: int, body: str) -> str:
    return f"listeners.{proto}.{name} {{\n{body}\n}}\n"


def emqx_cases() -> list[Case]:
    cases: list[Case] = []
    for order, plain_enable, posture, no_match in itertools.product(
        itertools.permutations(("plain", "wss", "ssl")),
        ("true", "false"),
        ("unanimous", "mixed"),
        ("allow", "deny"),
    ):
        wss_verify = "verify_peer" if posture == "unanimous" else "verify_none"
        wss_fail = "true" if posture == "unanimous" else "false"
        blocks = {
            "plain": _emqx_listener(
                "tcp",
                "legacy_plain",
                1883,
                f'  enable = {plain_enable}\n  bind = "0.0.0.0:1883"',
            ),
            "wss": _emqx_listener(
                "wss",
                "browsers",
                8084,
                '  bind = "0.0.0.0:8084"\n'
                '  ssl_options.certfile = "/certs/wss.crt"\n'
                '  ssl_options.keyfile = "/certs/wss.key"\n'
                '  ssl_options.cacertfile = "/certs/wss-ca.crt"\n'
                f"  ssl_options.verify = {wss_verify}\n"
                f"  ssl_options.fail_if_no_peer_cert = {wss_fail}\n"
                "  ssl_options.enable_crl_check = true\n"
                '  ssl_options.versions = ["tlsv1.2"]',
            ),
            "ssl": _emqx_listener(
                "ssl",
                "devices",
                8883,
                '  bind = "0.0.0.0:8883"\n'
                '  ssl_options.certfile = "/certs/ssl.crt"\n'
                '  ssl_options.keyfile = "/certs/ssl.key"\n'
                '  ssl_options.cacertfile = "/certs/ssl-ca.crt"\n'
                "  ssl_options.verify = verify_peer\n"
                "  ssl_options.fail_if_no_peer_cert = true\n"
                "  ssl_options.depth = 3",
            ),
        }
        conf = 'node.name = "emqx@127.0.0.1"\nnode.data_dir = "/var/lib/emqx"\n'
        for which in order:
            conf += blocks[which]
        conf += (
            "authentication = [\n"
            "  { mechanism = password_based, backend = http, enable = false,\n"
            '    url = "http://legacy-authn:8080/auth" }\n'
            "]\n"
            "authorization {\n"
            f"  no_match = {no_match}\n"
            "  sources = [\n"
            '    { type = file, enable = true, path = "acl.conf" }\n'
            "  ]\n"
            "}\n"
        )
        name = (
            f"emqx order={'-'.join(order)} plain.enable={plain_enable} "
            f"posture={posture} no_match={no_match}"
        )
        witnesses = [
            (k, [k])
            for k in (
                "/certs/wss.crt",
                "/certs/wss-ca.crt",
                "/certs/ssl.crt",
                "/certs/ssl-ca.crt",
                "enable_crl_check",
                "tlsv1.2",
                "legacy-authn",  # the DISABLED authenticator must still be named
            )
        ]
        forbidden = [
            (
                "a DISABLED authenticator's URL carried into [security.http_auth]",
                'url = "http://legacy-authn:8080/auth"',
            )
        ]
        if plain_enable == "false":
            # THE class-B case: a listener EMQX switched off must not become a live bind.
            forbidden.append(
                ("a DISABLED listener bound anyway", 'plaintext_bind = "0.0.0.0:1883"')
            )
            witnesses.append(("the disabled listener, named", ["legacy_plain"]))
        else:
            witnesses.append(("the enabled plaintext bind", ['plaintext_bind = "0.0.0.0:1883"']))
        if posture == "mixed":
            forbidden.append(("an invented mTLS mandate", '\nclient_ca = "'))
            # ...and the disagreement must be REPORTED, not merely not-mapped: reading the
            # gate off the first listener silently turns a mandate into a cert-optional CA.
            witnesses.append(
                (
                    "the mixed posture, reported",
                    ["TLS listeners DISAGREE about client certificates"],
                )
            )
        else:
            witnesses.append(("the unanimous mandate", ['client_ca = "']))
        cases.append(
            Case(
                name=name,
                files={"emqx.conf": conf, "acl.conf": _E_ACL},
                argv=[
                "emqx.conf",
                "--out-config",
                "out.toml",
                "--out-acl",
                "acl.toml",
                "--provenance-json",
                "prov.json",
            ],
                outputs=["out.toml", "acl.toml"],
                witnesses=witnesses,
                forbidden=forbidden,
            )
        )
    # THE AXES THE GENERATORS DID NOT HAVE, each one a construct round 3 found by hand and
    # this sweep could not see: a listener-scoped authentication chain, the other keys on a
    # LIVE authenticator, acl_claim_name, a TLS bridge (which needs --out-bridge — invariant E
    # never even looked at the bridge TOML before), and per-listener messages_rate.
    cases.append(
        Case(
            name="emqx listener-scoped authn, live-authn extras, acl_claim_name, TLS bridge",
            files={
                "emqx.conf": 'node.data_dir = "/var/lib/emqx"\n'
                "listeners.ssl.devices {\n"
                '  bind = "0.0.0.0:8883"\n'
                '  ssl_options.certfile = "/S/d.crt"\n'
                '  ssl_options.keyfile = "/S/d.key"\n'
                "  messages_rate = \"100/s\"\n"
                "  authentication = [ { mechanism = password_based, backend = http,\n"
                '      url = "http://AUTHN-ON-LISTENER:8080/a",\n'
                '      ssl.cacertfile = "/S/listener-authn-ca.pem" } ]\n'
                "}\n"
                'listeners.ws.browsers { bind = "0.0.0.0:8083", messages_rate = "5000/s",\n'
                '  mountpoint = "SENTINEL-MOUNT/" }\n'
                "authentication = [\n"
                "  { mechanism = password_based, backend = http,\n"
                '    url = "http://authn:8080/auth", request_timeout = 5s, method = post,\n'
                '    headers { "x-api-key" = "SENTINEL-APIKEY" },\n'
                "    pool_size = 8, ssl.enable = true,\n"
                '    ssl.cacertfile = "/S/authn-ca.pem" }\n'
                '  { mechanism = jwt, acl_claim_name = "SENTINEL-ACLCLAIM",\n'
                "    on_missing_jwt = ignore, disconnect_after_expire = true,\n"
                '    verify_claims = { iss = "https://real/", aud = "real-aud" } }\n'
                "]\n"
                "authorization {\n"
                "  no_match = deny\n"
                '  sources = [ { type = redis, server = "10.1.1.1:6379",\n'
                '      cmd = "HGETALL SENTINEL-ACLQUERY:${username}" } ]\n'
                "}\n"
                'connectors.mqtt.up { server = "10.9.9.9:8883", username = "u",\n'
                '  ssl.enable = true, ssl.cacertfile = "/S/bridge-ca.pem" }\n'
            },
            argv=[
                "emqx.conf",
                "--out-config",
                "out.toml",
                "--out-bridge",
                "bridge.toml",
            ],
            outputs=["out.toml", "bridge.toml"],
            witnesses=[
                ("the listener-scoped authenticator's endpoint", ["AUTHN-ON-LISTENER"]),
                ("its client-TLS anchor", ["/S/listener-authn-ca.pem"]),
                ("the live authenticator's shared secret header", ["x-api-key"]),
                ("its private-CA anchor", ["/S/authn-ca.pem"]),
                ("the token-delivered ACL claim", ["SENTINEL-ACLCLAIM"]),
                ("the authz query that was dropped", ["SENTINEL-ACLQUERY"]),
                ("the mountpoint every ACL rule would need", ["SENTINEL-MOUNT/"]),
                ("the bridge's TLS anchor", ["/S/bridge-ca.pem"]),
                ("the tightest per-listener publish rate", ["max_publish_rate = 100"]),
                ("the rate collapse, reported", ["the SMALLEST (100/s)"]),
            ],
            forbidden=[
                # THE fail-open shape: a TLS upstream converted to a LIVE plaintext one, and
                # the secret copied into the output. Commenting `[upstreams.tls]` IS the posture
                # change, so the value whose liveness DECIDES the posture — the upstream `url` —
                # may not be live either: with the tls block commented, completing the draft
                # exactly as the file instructs produced a bridge that CONNECTED to a TLS peer
                # in cleartext (measured against the real mqtt-bridge binary, 2026-08-15).
                ("a secret copied into the output", "SENTINEL-APIKEY\""),
                ("a TLS upstream reachable in cleartext", 'url = "10.9.9.9:8883"'),
                ("the loosest publish rate winning", "max_publish_rate = 5000"),
            ],
        )
    )
    # A NON-SCALAR `bind`, and an EMQX-legal `bind` mqttd cannot bind. `str(value)` made the
    # first into the Python repr `"['0.0.0.0:1883']"` — a live value that appears NOWHERE in the
    # input, which invariant F missed because for a `*_bind` it compares only the PORT — and
    # `:8085` (host omitted, which EMQX's own ip_port accepts) passed --check-config and then
    # failed at startup with "nodename nor servname provided". Invariant H is what catches both.
    cases.append(
        Case(
            name="emqx non-scalar bind and a host-less bind",
            files={
                "emqx.conf": 'node.data_dir = "/var/lib/emqx"\n'
                'listeners.tcp.a { bind = ["0.0.0.0:1883"] }\n'
                'listeners.ws.w { bind = ":8085" }\n'
            },
            argv=["emqx.conf", "--out-config", "out.toml"],
            outputs=["out.toml"],
            witnesses=[
                ("the non-scalar bind, named", ["not a single address but a list"]),
                ("the host-less bind, named", ["names NO host"]),
            ],
            forbidden=[
                ("a Python repr emitted as a bind", "['0.0.0.0:1883']"),
                ("an address the broker cannot resolve", 'ws_bind = ":8085"'),
            ],
        )
    )
    # A LIVE AUTHENTICATOR ON A NON-http/jwt BACKEND. `report_unread_authn_keys` was wired on
    # the http and jwt branches only, so the credential store this authenticator read — the one
    # fact needed to rebuild it behind [security.http_auth] — appeared nowhere, under a
    # reassuring per-mechanism TODO. This is the shape the repository's OWN pinned fixture
    # exercises (`backend = mysql`), which is why its fixture test passed over it.
    cases.append(
        Case(
            name="emqx authenticator on a backend mqttd has no equivalent for",
            files={
                "emqx.conf": 'node.data_dir = "/var/lib/emqx"\n'
                "authentication = [\n"
                "  { mechanism = password_based, backend = mysql,\n"
                '    server = "SENTINEL-MYSQL:3306", database = mqtt,\n'
                '    query = "SELECT SENTINEL-QUERY FROM mqtt_user WHERE username = ${username}",\n'
                "    password_hash_algorithm { name = sha256, salt_position = suffix } }\n"
                "  { mechanism = SENTINEL-UNKNOWN-MECH, endpoint = \"SENTINEL-ENDPOINT\" }\n"
                "]\n"
            },
            argv=["emqx.conf", "--out-config", "out.toml"],
            outputs=["out.toml"],
            witnesses=[
                ("the credential store's address", ["SENTINEL-MYSQL:3306"]),
                ("the query that authenticated every client", ["SENTINEL-QUERY"]),
                ("the hash scheme", ["sha256"]),
                ("the unknown mechanism's own keys", ["SENTINEL-ENDPOINT"]),
            ],
        )
    )
    # THE CONTRADICTION, in the direction round 3's own remediation introduced: `verify_claims`
    # was absent from AUTHN_JWT_READ, so the leaf reporter enumerated its claims and the same
    # document emitted `issuer = "…"  # from: … verify_claims.iss` AND a TODO saying that exact
    # claim has no mqttd equivalent. Both readings are actionable and one of them is wrong.
    cases.append(
        Case(
            name="emqx jwt verify_claims: mapped, and not also reported as unmappable",
            files={
                "emqx.conf": 'node.data_dir = "/var/lib/emqx"\n'
                "authentication = [ { mechanism = jwt,\n"
                '    verify_claims = { iss = "ISS-SENTINEL", aud = "AUD-SENTINEL",\n'
                '                      tenant = "TENANT-SENTINEL" } } ]\n'
            },
            argv=["emqx.conf", "--out-config", "out.toml"],
            outputs=["out.toml"],
            witnesses=[
                ("the issuer, mapped", ['issuer = "ISS-SENTINEL"']),
                ("the audience, mapped", ['audience = "AUD-SENTINEL"']),
                ("the claim mqttd cannot check, named", ["TENANT-SENTINEL"]),
            ],
            # `forbidden_anywhere`, not `forbidden`: the contradiction IS a comment (a TODO), so
            # the comment-stripped check could never see it.
            forbidden_anywhere=[
                (
                    "a claim reported as unmappable in the same file that maps it",
                    "verify_claims.iss = 'ISS-SENTINEL': no mqttd equivalent",
                )
            ],
        )
    )
    # A literal `*` in an EMQX ACL username: the rejection for a `{re, ...}` condition already
    # knows `*` is the only special character in an mqttd identity, and then never inspected the
    # literal it emitted.
    cases.append(
        Case(
            name="emqx ACL username containing a glob metacharacter",
            files={
                "emqx.conf": 'node.data_dir = "/var/lib/emqx"\n'
                "authorization { no_match = deny, sources = [ { type = file, path = \"acl.conf\" } ] }\n",
                "acl.conf": '{allow, {username, "alice*bob"}, publish, ["out/#"]}.\n',
            },
            argv=["emqx.conf", "--out-config", "out.toml", "--out-acl", "acl.toml"],
            outputs=["out.toml", "acl.toml"],
            witnesses=[("the literal-star username, named", ["alice*bob"])],
            forbidden=[
                ("a literal username emitted as a glob", 'identities = ["alice*bob"]')
            ],
        )
    )
    # A literal %c in an acl.conf topic: EMQX 5/6 substitutes only ${...} placeholders (the
    # pinned schema fixture lists them), so those bytes matched LITERALLY — while mqttd
    # substitutes %c in every rule's topics with no escape. Emitting it converts a rule on one
    # literal topic into a live per-client grant. Same class as the Mosquitto plain-`topic`
    # case above; this converter carried it until issue #297.
    cases.append(
        Case(
            name="emqx ACL topic containing a literal %c",
            files={
                "emqx.conf": 'node.data_dir = "/var/lib/emqx"\n'
                "authorization { no_match = deny, sources = [ { type = file, path = \"acl.conf\" } ] }\n",
                "acl.conf": '{allow, {username, "bob"}, publish, ["c/%c/x"]}.\n',
            },
            argv=["emqx.conf", "--out-config", "out.toml", "--out-acl", "acl.toml"],
            outputs=["out.toml", "acl.toml"],
            witnesses=[("the substituting topic, named", ["c/%c/x"])],
            forbidden=[
                ("a literal filter emitted as a substituting rule", '"c/%c/x"')
            ],
        )
    )
    # A listener whose `bind` names a host with NO PORT: mqttd cannot bind that, and appending
    # `:1883` invented a port.
    cases.append(
        Case(
            name="emqx listener bind with no port",
            files={
                "emqx.conf": 'node.data_dir = "/var/lib/emqx"\n'
                'listeners.tcp.d { bind = "10.0.0.9" }\n'
            },
            argv=["emqx.conf", "--out-config", "out.toml"],
            outputs=["out.toml"],
            witnesses=[("the address the input named", ["10.0.0.9"])],
            forbidden=[("a bind whose port nobody wrote", '\nplaintext_bind = ')],
        )
    )
    # An unreadable ACL source, under both no_match postures: the ACL must still be WRITTEN
    # and must not claim a posture it does not have.
    for no_match in ("allow", "deny"):
        cases.append(
            Case(
                name=f"emqx unreadable acl source no_match={no_match}",
                files={
                    "emqx.conf": 'node.data_dir = "/var/lib/emqx"\n'
                    'listeners.tcp.d { bind = "0.0.0.0:1883" }\n'
                    "authorization {\n"
                    f"  no_match = {no_match}\n"
                    '  sources = [ { type = file, path = "etc/acl.conf" } ]\n'
                    "}\n"
                },
                argv=[
                "emqx.conf",
                "--out-config",
                "out.toml",
                "--out-acl",
                "acl.toml",
                "--provenance-json",
                "prov.json",
            ],
                outputs=["out.toml", "acl.toml"],
                witnesses=[("the untranslated policy", ["WAS NOT TRANSLATED"])],
            )
        )
    return cases


_E_ACL = (
    '{allow, {username, "sensor-1"}, publish, ["devices/${username}/telemetry"]}.\n'
    "{deny, all, subscribe, [\"secret/#\"]}.\n"
)


# ---------------------------------------------------------------------------
# HiveMQ
# ---------------------------------------------------------------------------


def hivemq_cases() -> list[Case]:
    cases: list[Case] = []
    for order, mode, truststore in itertools.product(
        itertools.permutations(("tcp", "wss", "tls")), ("REQUIRED", "NONE", "OPTIONAL"), (True, False)
    ):
        ts = (
            "        <truststore>\n"
            "          <path>/opt/hivemq/conf/truststore.jks</path>\n"
            "          <password>x</password>\n"
            "        </truststore>\n"
            if truststore
            else ""
        )
        blocks = {
            "tcp": "    <tcp-listener>\n      <port>1883</port>\n"
            "      <bind-address>0.0.0.0</bind-address>\n    </tcp-listener>\n",
            "wss": "    <tls-websocket-listener>\n      <port>8000</port>\n"
            "      <bind-address>0.0.0.0</bind-address>\n      <path>/mqtt</path>\n"
            "      <tls>\n        <keystore>\n"
            "          <path>/opt/hivemq/conf/wss-keystore.jks</path>\n"
            "          <password>x</password>\n"
            "        </keystore>\n"
            "        <client-authentication-mode>NONE</client-authentication-mode>\n"
            "      </tls>\n    </tls-websocket-listener>\n",
            "tls": "    <tls-tcp-listener>\n      <port>8883</port>\n"
            "      <bind-address>0.0.0.0</bind-address>\n"
            "      <tls>\n        <keystore>\n"
            "          <path>/opt/hivemq/conf/device-keystore.jks</path>\n"
            "          <password>x</password>\n"
            "        </keystore>\n" + ts + f"        <client-authentication-mode>{mode}"
            "</client-authentication-mode>\n"
            "        <protocols>\n          <protocol>TLSv1.2</protocol>\n"
            "        </protocols>\n"
            "      </tls>\n    </tls-tcp-listener>\n",
        }
        xml = '<?xml version="1.0"?>\n<hivemq>\n  <listeners>\n'
        for which in order:
            xml += blocks[which]
        xml += "  </listeners>\n</hivemq>\n"
        name = f"hivemq order={'-'.join(order)} client-auth={mode} truststore={truststore}"
        witnesses = [
            (k, [k])
            for k in (
                "/opt/hivemq/conf/wss-keystore.jks",
                "/opt/hivemq/conf/device-keystore.jks",
                "TLSv1.2",
                "client-authentication-mode",
            )
        ]
        if truststore:
            witnesses.append(
                ("the truststore", ["/opt/hivemq/conf/truststore.jks"])
            )
        if mode == "REQUIRED":
            # wss is always NONE here, so REQUIRED on the tls-tcp listener is a MIXED posture:
            # the disagreement must be reported, not quietly resolved either way.
            witnesses.append(
                (
                    "the mixed posture, reported",
                    ["TLS listeners DISAGREE about client certificates"],
                )
            )
        else:
            witnesses.append(("the posture, named", [mode]))
        # wss is always NONE, so a REQUIRED tls-tcp listener is a MIXED posture: an active
        # client_ca would silently demand certificates from browsers.
        forbidden = [("an invented mTLS mandate", '\nclient_ca = "')]
        cases.append(
            Case(
                name=name,
                files={"config.xml": xml, "credentials.xml": _H_CREDS},
                argv=[
                    "config.xml",
                    "--out-config",
                    "out.toml",
                    "--out-acl",
                    "acl.toml",
                    "--credentials",
                    "credentials.xml",
                    "--provenance-json",
                    "prov.json",
                ],
                outputs=["out.toml", "acl.toml"],
                witnesses=witnesses
                + [
                    # A user file-RBAC had switched off must not get live allow rules.
                    ("the disabled user, named", ["retired"]),
                    ("the enabled user's rule", ['identities = ["live"]']),
                ],
                forbidden=forbidden
                + [("a DISABLED user's live rule", 'identities = ["retired"]')],
            )
        )
    # A listener element with NO <port>: `self.port or "1883"` used to stand in the address
    # property, so a listener whose port this converter had not read became a live bind on a
    # port the input never named.
    cases.append(
        Case(
            name="hivemq listener with no <port>",
            files={
                "config.xml": '<?xml version="1.0"?>\n<hivemq>\n  <listeners>\n'
                "    <tcp-listener>\n      <bind-address>10.0.0.9</bind-address>\n"
                "    </tcp-listener>\n  </listeners>\n</hivemq>\n"
            },
            argv=["config.xml", "--out-config", "out.toml"],
            outputs=["out.toml"],
            witnesses=[("the address the input named", ["10.0.0.9"])],
            forbidden=[("a bind whose port nobody wrote", "\nplaintext_bind = ")],
        )
    )
    # The Class D case: REQUIRED mTLS on the ONLY TLS listener with NO truststore, so the
    # mandate IS mapped and the extraction recipe must still be complete.
    xml = (
        '<?xml version="1.0"?>\n<hivemq>\n  <listeners>\n'
        "    <tls-tcp-listener>\n      <port>8883</port>\n"
        "      <bind-address>0.0.0.0</bind-address>\n"
        "      <tls>\n        <keystore>\n"
        "          <path>/opt/hivemq/conf/only-keystore.jks</path>\n"
        "          <password>x</password>\n"
        "        </keystore>\n"
        "        <client-authentication-mode>REQUIRED</client-authentication-mode>\n"
        "      </tls>\n    </tls-tcp-listener>\n"
        "  </listeners>\n</hivemq>\n"
    )
    cases.append(
        Case(
            name="hivemq REQUIRED mTLS with no truststore",
            files={"config.xml": xml, "credentials.xml": _H_CREDS},
            argv=[
                "config.xml",
                "--out-config",
                "out.toml",
                "--out-acl",
                "acl.toml",
                "--credentials",
                "credentials.xml",
            ],
            outputs=["out.toml", "acl.toml"],
            witnesses=[
                ("the mandate", ['client_ca = "']),
                ("the keystore", ["/opt/hivemq/conf/only-keystore.jks"]),
            ],
        )
    )
    # A `<port>` mqttd cannot bind. config.xsd types it as xs:int, but nothing here validated
    # it, so `<port>abc</port>` produced a LIVE `plaintext_bind = "10.0.0.1:abc"`, `config OK`
    # from --check-config, and `invalid port value` at STARTUP. Invariant H is what sees it.
    cases.append(
        Case(
            name="hivemq listener whose port is not a port",
            files={
                "config.xml": "<hivemq><listeners><tcp-listener><port>abc</port>"
                "<bind-address>10.0.0.1</bind-address></tcp-listener></listeners></hivemq>\n"
            },
            argv=["config.xml", "--out-config", "out.toml"],
            outputs=["out.toml"],
            witnesses=[
                ("the address the input named", ["10.0.0.1"]),
                ("the unbindable port, named", ["not a TCP port number"]),
            ],
            forbidden=[("a bind the broker cannot bind", "plaintext_bind = ")],
        )
    )
    # A literal `*` in a file-RBAC <name>: file-RBAC matched it EXACTLY, mqttd's `identities` are
    # globs with no escape, so the rule would be strictly broader than the source.
    cases.append(
        Case(
            name="hivemq file-RBAC user name containing a glob metacharacter",
            files={
                "config.xml": "<hivemq><listeners><tcp-listener><port>1883</port>"
                "</tcp-listener></listeners></hivemq>\n",
                "credentials.xml": "<file-rbac><users><user><name>alice*bob</name>"
                "<password>x</password><roles><id>r1</id></roles></user></users>"
                "<roles><role><id>r1</id><permissions><permission><topic>out/#</topic>"
                "</permission></permissions></role></roles></file-rbac>\n",
            },
            argv=[
                "config.xml",
                "--credentials",
                "credentials.xml",
                "--out-config",
                "out.toml",
                "--out-acl",
                "acl.toml",
            ],
            outputs=["out.toml", "acl.toml"],
            witnesses=[("the literal-star user, named", ["alice*bob"])],
            forbidden=[
                ("a literal user name emitted as a glob", 'identities = ["alice*bob"]')
            ],
        )
    )
    # A literal %c in a file-RBAC <topic>: the extension substitutes only ${{clientid}} and
    # ${{username}} (4.6.16 reference), so those bytes matched LITERALLY — while mqttd
    # substitutes %c in every rule's topics with no escape. Same class as the Mosquitto and
    # EMQX cases; this converter carried it until issue #297.
    cases.append(
        Case(
            name="hivemq file-RBAC topic containing a literal %c",
            files={
                "config.xml": "<hivemq><listeners><tcp-listener><port>1883</port>"
                "</tcp-listener></listeners></hivemq>\n",
                "credentials.xml": "<file-rbac><users><user><name>bob</name>"
                "<password>x</password><roles><id>r1</id></roles></user></users>"
                "<roles><role><id>r1</id><permissions><permission><topic>c/%c/x</topic>"
                "</permission></permissions></role></roles></file-rbac>\n",
            },
            argv=[
                "config.xml",
                "--credentials",
                "credentials.xml",
                "--out-config",
                "out.toml",
                "--out-acl",
                "acl.toml",
            ],
            outputs=["out.toml", "acl.toml"],
            witnesses=[("the substituting topic, named", ["c/%c/x"])],
            forbidden=[
                ("a literal filter emitted as a substituting rule", '"c/%c/x"')
            ],
        )
    )
    return cases


_H_CREDS = """<?xml version="1.0"?>
<file-rbac>
  <users>
    <user>
      <name>live</name>
      <password>not-a-real-hash</password>
      <roles><id>r1</id></roles>
    </user>
    <user>
      <name>retired</name>
      <password>not-a-real-hash</password>
      <enabled>false</enabled>
      <roles><id>r1</id></roles>
    </user>
  </users>
  <roles>
    <role>
      <id>r1</id>
      <permissions>
        <permission>
          <topic>fleet/#</topic>
          <activity>ALL</activity>
        </permission>
      </permissions>
    </role>
  </roles>
</file-rbac>
"""


CONVERTERS = {
    "mosquitto": ("from-mosquitto.py", mosquitto_cases),
    "emqx": ("from-emqx.py", emqx_cases),
    "hivemq": ("from-hivemq.py", hivemq_cases),
}


def run_case(script: Path, case: Case, mqttd: Path | None, defaulted: list[str]) -> list[str]:
    """Run one case and return every property it violated."""
    problems: list[str] = []
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        for fname, body in case.files.items():
            (work / fname).write_text(body, encoding="utf-8")
        source_text = "\n".join(case.files.values())
        try:
            proc = subprocess.run(
                [sys.executable, str(script), *case.argv],
                cwd=work,
                capture_output=True,
                text=True,
                check=False,
                timeout=RUN_TIMEOUT,
            )
        except subprocess.TimeoutExpired:
            return [
                f"the converter HUNG (no exit in {RUN_TIMEOUT}s). The contract is exit 0 "
                "translated, 1 could not read the input — a wedge is neither"
            ]
        if proc.returncode != 0:
            return [
                f"the converter exited {proc.returncode} (the contract is exit 0 with the "
                f"gaps named): {proc.stderr.strip()[:400]}"
            ]
        texts: dict[str, str] = {}
        for out in case.outputs:
            path = work / out
            if not path.is_file():
                problems.append(f"{out} was not written at all")
                continue
            texts[out] = path.read_text(encoding="utf-8")
            try:
                tomllib.loads(texts[out])
            except tomllib.TOMLDecodeError as e:
                problems.append(f"{out} is not valid TOML: {e}")
        if problems:
            return problems
        combined = "\n".join(texts.values())
        # `forbidden` is always about an ACTIVE setting, so it is checked against the output
        # with every comment line removed. A commented-out candidate beside a TODO is the
        # contract's prescribed handling for a posture change — the whole point is that it is
        # NOT active — so matching it here would fail the very shape the fixes install.
        active = "\n" + "\n".join(
            l for l in combined.splitlines() if not l.lstrip().startswith("#")
        )

        # -- CLASS A/B: every security-relevant value survives, named ------------------
        for label, alternatives in case.witnesses:
            if not any(alt in combined for alt in alternatives):
                problems.append(
                    f"{label} is NOWHERE in the output — not translated, and not named in "
                    "any TODO/NOTE. That is a silent drop"
                )
        for label, needle in case.forbidden:
            if needle in active:
                problems.append(
                    f"{label}: the output has {needle!r} as a LIVE setting, not commented out"
                )
        for label, needle in case.forbidden_anywhere:
            if needle in combined:
                problems.append(f"{label}: the output contains {needle!r}")

        # -- CLASS C, D, F and G, on each document separately --------------------------
        for out, text in texts.items():
            problems.extend(check_class_c(text, out))
            problems.extend(check_class_d(text, out))
            problems.extend(check_class_g(text, out))
            problems.extend(check_class_h(text, out))
            f_bad, f_defaulted = check_class_f(text, out, source_text)
            problems.extend(f_bad)
            defaulted.extend(f_defaulted)

        # -- the PROVENANCE LEDGER agrees with the output ------------------------------
        #
        # `--provenance-json` writes what the converter BELIEVES it emitted. That is only
        # worth anything if it matches the file it wrote, so where a case asks for it the two
        # are cross-checked: every row the ledger calls LIVE must appear as an uncommented
        # line, and every row it calls INERT must not. A ledger that disagreed with the output
        # would make invariant G checkable against the wrong document.
        ledger_path = work / "prov.json"
        if ledger_path.is_file():
            try:
                ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as e:
                problems.append(f"the provenance ledger is not valid JSON: {e}")
                ledger = {"emissions": []}
            live_lines = [l for l in combined.splitlines() if not l.lstrip().startswith("#")]
            for row in ledger.get("emissions", []):
                needle = f"{row['field']} = {row['value']}"
                present = any(l.startswith(needle) for l in live_lines)
                if row["live"] and not present:
                    problems.append(
                        f"the provenance ledger records `{needle}` as LIVE, but no "
                        "uncommented line in the output says so"
                    )
                if not row["live"] and present:
                    problems.append(
                        f"the provenance ledger records `{needle}` as INERT (no source key), "
                        "and the output emits it as a LIVE setting anyway"
                    )
                if row["live"] and not row["source"]:
                    problems.append(
                        f"the provenance ledger records `{needle}` as LIVE with no source key"
                    )

        # -- CLASS E: the broker accepts it --------------------------------------------
        if mqttd is not None:
            config = next((o for o in case.outputs if o.endswith("out.toml")), None)
            if config and (work / config).is_file():
                check = subprocess.run(
                    [str(mqttd), "--check-config", "--config", str(work / config)],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if check.returncode != 0:
                    problems.append(
                        "the broker REJECTED the generated config: "
                        f"{(check.stderr or check.stdout).strip()[:300]}"
                    )
    return problems


# ---------------------------------------------------------------------------
# THE FUZZ PASS.
#
# The generators above enumerate axes their author thought of, which is how round 2's blocking
# defect survived round 1. This pass does not think: it takes each pinned fixture and mutates
# it mechanically, then asserts only the properties that must hold for ANY byte sequence —
#
#   * the converter EXITS, 0 or 1, with a message (the documented contract). A run that never
#     returns is the defect this found: `Hocon._array` fell through to `_bare(",]\n}")`, which
#     cannot advance when the current character is already a stop character, so a `}` reached
#     inside an array appended "" forever — 100% CPU, unbounded memory, no error, no exit, on
#     a file as small as `authentication = [` followed by `}`.
#   * whatever it does write is valid TOML, and contains no live security-relevant line
#     without provenance (invariant G). A half-truncated input is exactly where a converter
#     starts inventing.
#
# Deliberately seeded, so a failure is reproducible from the printed case id.
# ---------------------------------------------------------------------------

# The two-line reproducer, kept as a NAMED case rather than left to chance: delta-debugging
# found it once, and a fuzzer that happens not to generate it again would let the hang back in.
SEEDED: dict[str, list[tuple[str, str]]] = {
    "emqx": [
        ("truncated array then a closing brace", "authentication = [\n}\n"),
        ("array that never closes", 'listeners.tcp.d { bind = "0.0.0.0:1883"\n'),
        ("bare open brace", "{\n"),
        ("a key with no value at EOF", "authorization {\n  sources = [ { type = file\n"),
        # The same control-character class, one converter over (see the mosquitto seeds).
        (
            "a control character in a value",
            'listeners.tcp.d { bind = "0.0.0.0:1883" }\n'
            'authorization { sources = [ { type = file, path = "/tmp/\x1bacl.conf" } ] }\n',
        ),
    ],
    "mosquitto": [
        ("a listener line with no port", "listener\ncertfile /c.crt\n"),
        ("a directive with no value", "acl_file\nport\nbind_address\n"),
        # A CONTROL CHARACTER IN A VALUE. TOML 1.0 forbids one ANYWHERE in a document, comments
        # included, so this used to be copied into a TODO and the broker rejected the WHOLE file
        # while the converter printed `wrote <file>`. The mutator cannot generate this class at
        # all — it mutates the DECODED text of a fixture, so no mutation can introduce a control
        # byte — which is why it is a seeded case rather than left to the fuzz. Found 2026-08-15.
        ("a control character in a path", "listener 1883\nacl_file /tmp/\x1bacl.acl\n"),
    ],
    "hivemq": [
        ("truncated element", "<hivemq><listeners><tcp-listener><port>18"),
        ("no root", ""),
        # The same control-character class, one converter over (see the mosquitto seeds).
        (
            "a control character in an element value",
            "<hivemq><listeners><tcp-listener><port>1883</port></tcp-listener>"
            "<tls-tcp-listener><port>8883</port><tls><keystore><path>/k/\x1bstore.jks</path>"
            "</keystore></tls></tls-tcp-listener></listeners></hivemq>",
        ),
    ],
}

# What each converter is invoked with during the fuzz pass, and the fixtures it is fed.
FUZZ_TARGETS = {
    "mosquitto": (
        ["FIXTURE", "--out-config", "out.toml", "--out-acl", "acl.toml"],
        ["mosquitto.conf"],
    ),
    "emqx": (
        [
            "FIXTURE",
            "--out-config",
            "out.toml",
            "--out-acl",
            "acl.toml",
            "--out-bridge",
            "bridge.toml",
        ],
        ["emqx-6.2.2.conf", "emqx-adversarial.conf", "emqx-silent-drops.conf"],
    ),
    "hivemq": (
        ["FIXTURE", "--out-config", "out.toml", "--out-acl", "acl.toml"],
        ["hivemq-2026.5-config.xml", "hivemq-multi-tls.xml", "hivemq-adversarial.xml"],
    ),
}

# A Mosquitto fixture is not in fixtures/ (that converter has no pinned vendor file), so the
# fuzz pass builds one from the shape the man page documents.
_M_FUZZ_SEED = """\
per_listener_settings true
persistence_location /var/lib/mosquitto
port 1883
bind_address 127.0.0.1
acl_file aclfile
listener 8883 0.0.0.0
protocol mqtt
max_connections 100
certfile /certs/device.crt
keyfile /certs/device.key
cafile /certs/device-ca.crt
crlfile /certs/device.crl
require_certificate true
listener 8084 0.0.0.0
protocol websockets
certfile /certs/wss.crt
keyfile /certs/wss.key
allow_anonymous true
include_dir /etc/mosquitto/conf.d
plugin /usr/lib/mosquitto_dynamic_security.so
plugin_opt_config_file /etc/mosquitto/dynamic-security.json
"""


def mutate(text: str, rng: random.Random) -> tuple[str, str]:
    """One mechanical mutation. Returns `(what it did, the mutated text)`."""
    lines = text.splitlines(keepends=True)
    which = rng.choice(
        ["delete", "truncate", "permute", "flip", "swap", "duplicate", "chop-line"]
    )
    if which == "delete" and lines:
        n = rng.randint(1, max(1, len(lines) // 8))
        for _ in range(n):
            if lines:
                lines.pop(rng.randrange(len(lines)))
        return (f"deleted {n} line(s)", "".join(lines))
    if which == "truncate" and text:
        cut = rng.randrange(1, len(text))
        return (f"truncated at byte {cut} of {len(text)}", text[:cut])
    if which == "permute" and len(lines) > 4:
        # Move a random slice somewhere else: listener blocks lose their order, which is the
        # axis the first-listener-only defect lived on.
        start = rng.randrange(len(lines) - 2)
        end = min(len(lines), start + rng.randint(2, 8))
        chunk = lines[start:end]
        del lines[start:end]
        at = rng.randrange(len(lines) + 1)
        lines[at:at] = chunk
        return (f"moved lines {start}..{end} to {at}", "".join(lines))
    if which == "flip":
        flipped = (
            text.replace("true", "false", 1)
            if rng.random() < 0.5
            else text.replace("false", "true", 1)
        )
        for a, b in (("REQUIRED", "NONE"), ("verify_peer", "verify_none"), ("enable", "enabled")):
            if rng.random() < 0.3:
                flipped = flipped.replace(a, b, 1)
        return ("flipped a boolean/enum", flipped)
    if which == "swap":
        for a, b in (
            ("listeners.tcp", "listeners.wss"),
            ("listeners.ssl", "listeners.quic"),
            ("tcp-listener", "tls-websocket-listener"),
            ("protocol mqtt", "protocol websockets"),
        ):
            if a in text:
                return (f"swapped transport {a} -> {b}", text.replace(a, b, 1))
        return ("no transport to swap", text)
    if which == "duplicate" and lines:
        i = rng.randrange(len(lines))
        lines.insert(i, lines[i])
        return (f"duplicated line {i}", "".join(lines))
    if lines:
        i = rng.randrange(len(lines))
        lines[i] = lines[i][: max(1, len(lines[i]) // 2)] + "\n"
        return (f"chopped line {i} in half", "".join(lines))
    return ("nothing to mutate", text)


def fuzz(converter: str, script: Path, rounds: int, seed: int) -> int:
    """Mutate the fixtures; the converter must always exit with a message, never hang."""
    argv_template, fixtures = FUZZ_TARGETS[converter]
    seeds: list[tuple[str, str]] = list(SEEDED.get(converter, []))
    for name in fixtures:
        path = FIXTURES / name
        if path.is_file():
            seeds.append((name, path.read_text(encoding="utf-8")))
    if converter == "mosquitto":
        seeds.append(("mosquitto.conf (built from mosquitto.conf(5))", _M_FUZZ_SEED))
    if not seeds:
        print(f"FATAL: no fuzz seed for {converter}", file=sys.stderr)
        return 2

    rng = random.Random(seed)
    failures = 0
    ran = 0
    # Every seed is run UNMUTATED first: a seeded reproducer is not a mutation, and a fixture
    # that already hangs must fail here rather than by luck.
    plan: list[tuple[str, str]] = [(f"{n} (unmutated)", t) for n, t in seeds]
    for i in range(rounds):
        name, text = seeds[i % len(seeds)]
        what, mutated = mutate(text, rng)
        plan.append((f"{name} #{i} [{what}]", mutated))

    for case_id, body in plan:
        ran += 1
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            fname = "input.conf" if converter != "hivemq" else "input.xml"
            (work / fname).write_text(body, encoding="utf-8")
            argv = [fname if a == "FIXTURE" else a for a in argv_template]
            try:
                proc = subprocess.run(
                    [sys.executable, str(script), *argv],
                    cwd=work,
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=RUN_TIMEOUT,
                )
            except subprocess.TimeoutExpired:
                failures += 1
                print(
                    f"  FAIL — {case_id}: the converter HUNG (no exit in {RUN_TIMEOUT}s, "
                    "and it was still burning CPU). The documented contract is exit 0 "
                    "translated / 1 could not read the input; a wedge is neither, and in CI "
                    "or a `mqttui migrate` it is an OOM rather than a diagnosable failure"
                )
                continue
            if proc.returncode not in (0, 1):
                failures += 1
                print(
                    f"  FAIL — {case_id}: exited {proc.returncode} "
                    f"({(proc.stderr or proc.stdout).strip()[:200]!r})"
                )
                continue
            if proc.returncode == 1 and not (proc.stderr.strip() or proc.stdout.strip()):
                failures += 1
                print(f"  FAIL — {case_id}: exited 1 with NO message at all")
                continue
            for out in ("out.toml", "acl.toml", "bridge.toml"):
                path = work / out
                if not path.is_file():
                    continue
                written = path.read_text(encoding="utf-8")
                try:
                    tomllib.loads(written)
                except tomllib.TOMLDecodeError as e:
                    failures += 1
                    print(f"  FAIL — {case_id}: {out} is not valid TOML: {e}")
                    continue
                f_bad, _ = check_class_f(written, out, body)
                for problem in check_class_g(written, out) + check_class_h(written, out) + f_bad:
                    failures += 1
                    print(f"  FAIL — {case_id}: {problem}")
    if failures:
        print(
            f"  {failures} fuzz case(s) violated the exit/validity contract "
            f"(seed {seed}, {ran} runs).",
            file=sys.stderr,
        )
        return 1
    print(f"  ok   — {ran} fuzzed {converter} inputs all exited with a message, no hangs")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", maxsplit=1)[0])
    ap.add_argument("converter", choices=sorted(CONVERTERS))
    ap.add_argument(
        "--mqttd",
        type=Path,
        help="the broker binary; every generated config is put through --check-config",
    )
    ap.add_argument(
        "--fuzz",
        type=int,
        metavar="N",
        help="instead of the generated cases, run N mechanical mutations of the fixtures and "
        "assert only that the converter always exits with a message and never invents a live "
        "security-relevant setting",
    )
    ap.add_argument("--seed", type=int, default=20260815, help="the fuzz seed")
    args = ap.parse_args()

    script_name, generator = CONVERTERS[args.converter]
    script = MIGRATE / script_name
    if not script.is_file():
        print(f"FATAL: {script} is missing", file=sys.stderr)
        return 2
    mqttd = args.mqttd
    if mqttd is not None and not mqttd.is_file():
        print(f"FATAL: {mqttd} is not built", file=sys.stderr)
        return 2

    if args.fuzz:
        return fuzz(args.converter, script, args.fuzz, args.seed)

    cases = generator()
    failures = 0
    defaulted: list[str] = []
    for case in cases:
        problems = run_case(script, case, mqttd, defaulted)
        if problems:
            failures += 1
            print(f"  FAIL — {case.name}")
            for p in problems:
                print(f"         {p}")
    total = len(cases)
    if failures:
        print(
            f"  {failures}/{total} generated {args.converter} case(s) violated a property.",
            file=sys.stderr,
        )
        return 1
    # Invariant F's escape hatch, printed rather than trusted: every value part the input did
    # not hold, and the vendor default the line claims it came from.
    unique = sorted(set(defaulted))
    print(f"  ok   — {total} generated {args.converter} cases hold every property")
    if unique:
        print(
            f"         ({len(unique)} distinct value part(s) declared `defaulted:` — a "
            "vendor-documented default or a path the converter owns, named on the line:)"
        )
        for note in unique[:8]:
            print(f"           {note}")
        if len(unique) > 8:
            print(f"           … and {len(unique) - 8} more")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
