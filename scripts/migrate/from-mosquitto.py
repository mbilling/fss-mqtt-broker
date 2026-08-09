#!/usr/bin/env python3
"""Translate a Mosquitto deployment to mqttd configuration.

Reads `mosquitto.conf` (and the `acl_file` it references) and emits an mqttd TOML
config plus an ACL policy.

Written because three independent evaluators — coming from Mosquitto, EMQX and
HiveMQ — each named "there is no migration tooling" as their single largest
blocker. Hand-translating an ACL file with one entry per device is not a task
anyone will do for an evaluation, so the evaluation does not happen.

## What it will and will not do

It translates the settings that have an exact mqttd equivalent, and for
everything else it **says so in the output** rather than guessing. A converter
that silently drops a setting is worse than no converter: you would deploy
believing the policy came across.

Anything not translated is emitted as a `# TODO(migrate):` comment at the point
it belongs, so the gap is visible in the file you are about to deploy rather than
in a report you read once.

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
import sys
from dataclasses import dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# Settings with an exact mqttd equivalent. Anything absent from this table is
# reported rather than guessed at.
# ---------------------------------------------------------------------------

# mosquitto directive -> (mqttd TOML section, key, converter)
DIRECT: dict[str, tuple[str, str, str]] = {
    "max_connections": ("limits", "max_connections", "int"),
    "max_queued_messages": ("limits", "max_queued_messages", "int"),
    "max_packet_size": ("limits", "max_packet_size", "int"),
    "max_inflight_messages": ("limits", "receive_maximum", "int"),
    "retain_available": ("limits", "max_retained_messages", "retain"),
    "persistence_location": ("node", "data_dir", "str"),
}

# Directives that exist in Mosquitto and have no mqttd equivalent, with the
# reason. Being explicit about *why* is the point: "unsupported" invites a bug
# report, "deliberately absent, here is the alternative" does not.
NO_EQUIVALENT: dict[str, str] = {
    "acl_file": "translated separately into the ACL policy (see --out-acl)",
    "password_file": "mqttd uses Argon2id password files: set security.password_file "
    "to a file of `username:argon2id-hash` lines (mosquitto_passwd hashes are NOT "
    "compatible — re-hash them)",
    "psk_file": "PSK ciphersuites are not implemented",
    "bridge": "bridging is a separate process in mqttd (mqtt-bridge) with its own "
    "config; see docs/BRIDGE.md",
    "connection": "bridge connections are configured in mqtt-bridge, not the broker",
    "log_dest": "mqttd logs to stdout for the container/journal to collect",
    "sys_interval": "$SYS topics are not implemented; use the Prometheus endpoint",
    "autosave_interval": "writes are transactional (redb); there is no autosave timer",
    "allow_zero_length_clientid": "a zero-length client id is accepted with clean "
    "session and refused otherwise, per spec; not configurable",
    "plugin": "there is no plugin API; authentication is JWT/OIDC/mTLS/password",
    "auth_plugin": "there is no plugin API; authentication is JWT/OIDC/mTLS/password",
}

TLS_KEYS = {"cafile", "capath", "certfile", "keyfile", "require_certificate", "crlfile"}


@dataclass
class Listener:
    """One Mosquitto listener, with whatever TLS material was attached to it."""

    port: int | None = None
    bind: str | None = None
    tls: dict[str, str] = field(default_factory=dict)


@dataclass
class Conversion:
    config: dict[str, dict[str, str]] = field(default_factory=dict)
    listeners: list[Listener] = field(default_factory=list)
    todos: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    acl_file: str | None = None

    def set(self, section: str, key: str, value: str) -> None:
        self.config.setdefault(section, {})[key] = value

    def todo(self, msg: str) -> None:
        self.todos.append(msg)


def parse_mosquitto_conf(text: str, conv: Conversion) -> None:
    """Walk mosquitto.conf, filling `conv`. Listener-scoped keys follow their listener."""
    current: Listener | None = None
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 1)
        key = parts[0]
        value = parts[1].strip() if len(parts) > 1 else ""

        if key == "listener":
            bits = value.split()
            current = Listener(port=int(bits[0]) if bits and bits[0].isdigit() else None)
            if len(bits) > 1:
                current.bind = bits[1]
            conv.listeners.append(current)
            continue

        if key in TLS_KEYS:
            # TLS material belongs to the listener it follows; before any listener
            # it is the default one.
            if current is None:
                current = Listener(port=None)
                conv.listeners.append(current)
            current.tls[key] = value
            continue

        if key == "allow_anonymous":
            if value.lower() in ("true", "yes", "1"):
                conv.set("security", "allow_anonymous", "true")
                conv.notes.append(
                    "allow_anonymous was TRUE in mosquitto.conf and has been carried "
                    "over — but mqttd defaults it OFF, and anonymous access is how most "
                    "broker exposure incidents start. Turn it off unless you are certain."
                )
            continue

        if key == "acl_file":
            conv.acl_file = value
            continue

        if key in DIRECT:
            section, mkey, kind = DIRECT[key]
            if kind == "int":
                conv.set(section, mkey, value)
            elif kind == "retain":
                if value.lower() in ("false", "no", "0"):
                    conv.todo(
                        "retain_available=false disables retained messages entirely; "
                        "mqttd has no off switch — cap it instead with "
                        "limits.max_retained_messages, or deny retained topics in the ACL"
                    )
            else:
                conv.set(section, mkey, f'"{value}"')
            continue

        if key in NO_EQUIVALENT:
            conv.todo(f"{key}: {NO_EQUIVALENT[key]}")
            continue

        if key == "persistence":
            if value.lower() in ("true", "yes", "1"):
                conv.notes.append(
                    "persistence was on: set node.data_dir (below) and mount a volume, "
                    "or durable state is kept in memory only"
                )
            continue

        conv.todo(f"{key}: no direct equivalent — check the mqttd configuration table")


def parse_acl(text: str) -> tuple[list[dict], list[str]]:
    """Translate a Mosquitto ACL file into mqttd rules.

    Mosquitto's model is *positional*: `user X` opens a block, and `topic` lines
    until the next `user` belong to it. `pattern` lines apply to everyone with
    substitution. mqttd's model is a list of rules with explicit identities, so
    the translation is a regrouping, not a line-for-line map.
    """
    rules: list[dict] = []
    todos: list[str] = []
    current_user: str | None = None

    def add(identity: str | None, access: str, topic: str) -> None:
        actions = {
            "read": ["subscribe"],
            "write": ["publish"],
            "readwrite": ["publish", "subscribe"],
            "deny": [],
        }.get(access)
        if actions is None:
            todos.append(f"unknown access type {access!r} for topic {topic!r}")
            return
        if access == "deny":
            rules.append(
                {
                    "identities": [identity] if identity else [],
                    "actions": ["publish", "subscribe"],
                    "effect": "deny",
                    "topics": [topic],
                }
            )
            return
        rules.append(
            {
                "identities": [identity] if identity else [],
                "actions": actions,
                "effect": "allow",
                "topics": [topic],
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
            continue

        if key == "topic":
            bits = rest.split(None, 1)
            if len(bits) == 2 and bits[0] in ("read", "write", "readwrite", "deny"):
                add(current_user, bits[0], bits[1])
            else:
                # No access type = readwrite in Mosquitto.
                add(current_user, "readwrite", rest)
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

    return rules, todos


def render_acl(rules: list[dict], todos: list[str]) -> str:
    out = [
        "# Translated from a Mosquitto acl_file by scripts/migrate/from-mosquitto.py.",
        "#",
        "# Mosquitto is positional (a `user` line opens a block); mqttd is a list of",
        "# explicit rules. Read this through before deploying it — a converted policy",
        "# is a draft, not an authority.",
        "#",
        "# mqttd is DENY BY DEFAULT: anything not allowed below is refused.",
        "",
        'default = "deny"',
        "",
    ]
    for t in todos:
        out.append(f"# TODO(migrate): {t}")
    if todos:
        out.append("")
    for r in rules:
        out.append("[[rules]]")
        if r["identities"]:
            ids = ", ".join(f'"{i}"' for i in r["identities"])
            out.append(f"identities = [{ids}]")
        else:
            out.append("# (no identities = applies to every authenticated client)")
        acts = ", ".join(f'"{a}"' for a in r["actions"])
        out.append(f"actions = [{acts}]")
        out.append(f'effect = "{r["effect"]}"')
        tps = ", ".join(f'"{t}"' for t in r["topics"])
        out.append(f"topics = [{tps}]")
        out.append("")
    return "\n".join(out) + "\n"


def render_config(conv: Conversion) -> str:
    out = [
        "# Translated from mosquitto.conf by scripts/migrate/from-mosquitto.py.",
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
        out.append(f"# NOTE: {n}")
    if conv.notes:
        out.append("")
    for t in conv.todos:
        out.append(f"# TODO(migrate): {t}")
    if conv.todos:
        out.append("")

    for section in ("node", "listeners", "security", "limits"):
        body = conv.config.get(section)
        if not body:
            continue
        out.append(f"[{section}]")
        for k, v in body.items():
            out.append(f"{k} = {v}")
        out.append("")

    if conv.listeners:
        out.append("# --- Listeners ---")
        out.append("#")
        out.append("# mqttd binds one listener per protocol rather than repeating a")
        out.append("# `listener` block. TLS is 1.3-only: a client that cannot negotiate")
        out.append("# TLS 1.3 will fail to connect, so check your device fleet.")
        for i, lst in enumerate(conv.listeners):
            port = lst.port if lst.port is not None else 1883
            host = lst.bind or "0.0.0.0"
            if lst.tls.get("certfile"):
                out.append(f"#   listener {i}: TLS on {host}:{port}")
                out.append("[listeners]")
                out.append(f'tls_bind = "{host}:{port}"')
                out.append("[tls]")
                out.append(f'cert = "{lst.tls["certfile"]}"')
                if lst.tls.get("keyfile"):
                    out.append(f'key = "{lst.tls["keyfile"]}"')
                if lst.tls.get("cafile"):
                    out.append(f'client_ca = "{lst.tls["cafile"]}"')
                if lst.tls.get("crlfile"):
                    out.append(f'crl = "{lst.tls["crlfile"]}"')
                if lst.tls.get("capath"):
                    out.append(
                        "# TODO(migrate): capath (a directory of CAs) is not supported; "
                        "concatenate them into one PEM and set client_ca"
                    )
            else:
                out.append(f"#   listener {i}: PLAINTEXT on {host}:{port}")
                out.append("[listeners]")
                out.append(f'plaintext_bind = "{host}:{port}"')
                out.append(
                    "# WARNING: plaintext. mqttd logs this as an INSECURE mode on every start."
                )
            out.append("")
    return "\n".join(out) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", maxsplit=1)[0])
    ap.add_argument("conf", type=Path, help="path to mosquitto.conf")
    ap.add_argument("--out-config", type=Path, help="write the mqttd TOML here")
    ap.add_argument("--out-acl", type=Path, help="write the translated ACL here")
    ap.add_argument("--acl-file", type=Path, help="override the acl_file path")
    args = ap.parse_args()

    try:
        text = args.conf.read_text(encoding="utf-8")
    except OSError as e:
        print(f"cannot read {args.conf}: {e}", file=sys.stderr)
        return 1

    conv = Conversion()
    parse_mosquitto_conf(text, conv)

    config = render_config(conv)
    if args.out_config:
        args.out_config.write_text(config, encoding="utf-8")
        print(f"wrote {args.out_config}")
    else:
        print(config)

    acl_path = args.acl_file or (Path(conv.acl_file) if conv.acl_file else None)
    if acl_path:
        try:
            acl_text = acl_path.read_text(encoding="utf-8")
        except OSError as e:
            print(f"note: could not read acl_file {acl_path}: {e}", file=sys.stderr)
        else:
            rules, todos = parse_acl(acl_text)
            acl = render_acl(rules, todos)
            if args.out_acl:
                args.out_acl.write_text(acl, encoding="utf-8")
                print(f"wrote {args.out_acl} ({len(rules)} rules)")
            else:
                print(acl)

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
