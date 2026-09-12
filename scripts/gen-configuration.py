#!/usr/bin/env python3
"""Generate docs/CONFIGURATION.md from the typed config surface.

The MQTTD_* environment overlay lives in crates/mqtt-config/src/lib.rs:
`ENV_VARS` is the curated inventory, `Config::overlay_from` is what actually
applies each variable. This script reads both, plus the rustdoc that names each
variable, and writes the operator-facing reference. CI checks the output is
committed (`--check`), the same way `gen-status.py` holds STATUS.md.

Usage:
    python3 scripts/gen-configuration.py            # write the file
    python3 scripts/gen-configuration.py --check    # exit 1 if it would change
"""

from __future__ import annotations

import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONFIG_RS = ROOT / "crates" / "mqtt-config" / "src" / "lib.rs"
OUT = ROOT / "docs" / "CONFIGURATION.md"
VERSION_TOML = ROOT / "Cargo.toml"

SECTION_ORDER = [
    ("node", "Node identity"),
    ("listeners", "Listeners"),
    ("tls", "TLS (client listeners)"),
    ("security", "Authentication and authorization"),
    ("cluster", "Cluster transport and membership"),
    ("durable", "Durable sessions"),
    ("limits", "Resource governance"),
    ("observability", "Observability"),
    ("runtime", "Runtime"),
    ("backup", "Online backup and restore"),
    ("audit", "Audit export"),
    ("meta", "Meta (not a config field)"),
    ("experimental", "Experimental store knobs (not in mqtt-config)"),
]


def workspace_version() -> str:
    text = VERSION_TOML.read_text(encoding="utf-8")
    m = re.search(r'^version = "([^"]+)"', text, re.M)
    if not m:
        raise SystemExit("Cargo.toml workspace version not found")
    return m.group(1)


def parse_env_vars(src: str) -> list[str]:
    m = re.search(r"pub const ENV_VARS: &\[&str\] = &\[(.*?)\];", src, re.S)
    if not m:
        raise SystemExit("ENV_VARS not found in mqtt-config")
    return re.findall(r'"(MQTTD_[A-Z0-9_]+)"', m.group(1))


def parse_overlay_vars(src: str) -> set[str]:
    """Every MQTTD_* overlay_from actually consumes."""
    start = src.find("pub fn overlay_from")
    end = src.find("pub fn validate(", start)
    body = src[start:end]
    found = set(re.findall(r'"(MQTTD_[A-Z0-9_]+)"', body))
    found.update(re.findall(r'get\("(MQTTD_[A-Z0-9_]+)"\)', body))
    return found


def parse_toml_keys(src: str) -> dict[str, str]:
    """Best-effort env → TOML key from overlay_from assignment targets."""
    start = src.find("pub fn overlay_from")
    end = src.find("pub fn validate(", start)
    body = src[start:end]
    mapping: dict[str, str] = {}

    # on!("MQTTD_FOO", v, { self.a.b.c = ... })
    for m in re.finditer(
        r'on!\(\s*"(MQTTD_[A-Z0-9_]+)"\s*,\s*\w+\s*,\s*\{(.*?)\}\s*\)\s*;',
        body,
        re.S,
    ):
        var, block = m.group(1), m.group(2)
        assign = re.search(r"self\.((?:[A-Za-z0-9_]+(?:\.)?)+)\s*=", block)
        if assign:
            mapping[var] = assign.group(1)

    # if get("MQTTD_FOO").is_some() { self.a.b = true; }
    for m in re.finditer(
        r'get\("(MQTTD_[A-Z0-9_]+)"\)\.is_some\(\)\s*\{\s*self\.([A-Za-z0-9_.]+)\s*=',
        body,
    ):
        mapping[m.group(1)] = m.group(2)

    return mapping


def parse_rustdoc_by_var(src: str) -> dict[str, str]:
    """Rustdoc immediately above a struct field, keyed by any MQTTD_* it names.

    Function-level docs (overlay_from, validate) mention several variables as
    examples; attaching those to a field would dump the mapping-engine essay
    onto `MQTTD_ALLOW_ANONYMOUS`. Only field docs are the knob description.
    """
    docs: dict[str, list[str]] = defaultdict(list)
    current: list[str] = []
    for line in src.splitlines():
        if line.startswith("    ///"):
            current.append(line[7:].strip())
            continue
        if current:
            # Attributes (`#[serde(...)]`) sit between rustdoc and the field;
            # skip them so the docs still attach.
            if not line.strip() or line.strip().startswith("#["):
                continue
            para = " ".join(p for p in current if p)
            is_field = bool(re.match(r"\s+pub [a-z0-9_]+(?:\s*:|\()", line))
            if is_field:
                for var in re.findall(r"`(MQTTD_[A-Z0-9_]+)`", para):
                    clean = re.sub(r"\[`([^`]+)`\]\([^)]+\)", r"`\1`", para)
                    clean = re.sub(r"\[`([^`]+)`\]", r"`\1`", clean)
                    docs[var].append(clean)
            current = []
    return {k: " ".join(v) for k, v in docs.items()}


def section_for(toml_key: str, var: str) -> str:
    if var == "MQTTD_CONFIG":
        return "meta"
    if var in ("MQTTD_STORE_SHARDS", "MQTTD_STORE_LINGER"):
        return "experimental"
    if not toml_key:
        # Shared-selection lives on cluster even though the env name does not say so.
        if "SHARED" in var:
            return "cluster"
        return "node"
    top = toml_key.split(".", 1)[0]
    if top in {s for s, _ in SECTION_ORDER}:
        return top
    return "node"


def side_channel_knobs() -> list[tuple[str, str, str]]:
    """MQTTD_* read outside mqtt-config overlay (experimental pins)."""
    knobs = []
    for path in sorted((ROOT / "crates").rglob("*.rs")):
        if "mqtt-config" in path.parts:
            continue
        if path.name.endswith("tests.rs") or "/tests/" in str(path):
            continue
        text = path.read_text(encoding="utf-8")
        prod, _, _ = text.partition("#[cfg(test)]")
        for var in sorted(set(re.findall(r'env::var\(\s*"(MQTTD_[A-Z0-9_]+)"', prod))):
            rel = path.relative_to(ROOT)
            knobs.append((var, str(rel), "read via `std::env::var` (not `mqtt_config::overlay_from`)"))
    return knobs


def render(env_vars: list[str], toml_keys: dict[str, str], docs: dict[str, str]) -> str:
    version = workspace_version()
    rows_by_section: dict[str, list[tuple[str, str, str]]] = defaultdict(list)

    for var in env_vars:
        key = toml_keys.get(var, "")
        desc = docs.get(var, "")
        if not desc:
            desc = "See the matching TOML key; description is the rustdoc on that field."
        rows_by_section[section_for(key, var)].append((var, key or "—", desc))

    # MQTTD_CONFIG is deliberately absent from ENV_VARS.
    rows_by_section["meta"].append(
        (
            "MQTTD_CONFIG",
            "—",
            "Meta variable naming the config *file*. Read by the binary to locate "
            "the file, not overlaid as a field, so it is deliberately absent from "
            "`ENV_VARS`. `--config <path>` overrides it.",
        )
    )

    for var, where, why in side_channel_knobs():
        if var in env_vars:
            continue
        desc = docs.get(
            var,
            f"{why}. Source: `{where}`. Experimental; warned loudly when engaged "
            "(ADR 0076: K>1 sharding and linger were measured slower and stay off by default).",
        )
        rows_by_section["experimental"].append((var, "—", desc))

    lines = [
        "# Configuration reference",
        "",
        f"**Generated against `v{version}`.** Do not edit this file by hand — "
        "run `python3 scripts/gen-configuration.py`. CI checks it matches the "
        "config code (`scripts/gen-configuration.py --check`), the same way "
        "`docs/delivery/STATUS.md` is held to delivery front-matter.",
        "",
        "Source of truth: `mqtt_config::ENV_VARS` and `Config::overlay_from` in "
        "`crates/mqtt-config/src/lib.rs`. Precedence is **defaults < TOML file < "
        "`MQTTD_*` env < CLI flags** (ADR 0046). Point at a file with `--config` or "
        "`MQTTD_CONFIG`; with neither, the config is defaults plus this overlay. "
        "`mqttd --check-config` validates the effective config and binds nothing.",
        "",
        "The annotated template is [`mqttd.example.toml`](mqttd.example.toml). "
        "Capacity arithmetic lives in [SIZING.md](SIZING.md); day-2 procedures in "
        "[OPERATIONS.md](OPERATIONS.md). This page is the inventory of knobs, not "
        "the runbook.",
        "",
        f"Documented overlay variables: **{len(env_vars)}** in `ENV_VARS`, plus "
        "`MQTTD_CONFIG` (meta) and any experimental side-channel knobs below.",
        "",
    ]

    for sid, title in SECTION_ORDER:
        rows = rows_by_section.get(sid)
        if not rows:
            continue
        lines += [f"## {title}", "", "| Variable | TOML key | Purpose |", "|---|---|---|"]
        for var, key, desc in rows:
            desc = desc.replace("|", "\\|").replace("\n", " ")
            lines.append(f"| `{var}` | `{key}` | {desc} |")
        lines.append("")

    lines += [
        "## Documented exceptions",
        "",
        "- `security.require_client_cert` is *derived* from whether a client CA is "
        "configured, not set by an env var.",
        "- `MQTTD_CONFIG` names the file; it is not a field on `Config`.",
        "- Experimental store knobs (`MQTTD_STORE_SHARDS`, `MQTTD_STORE_LINGER`) are "
        "read by the durable plane, not by `overlay_from`. ADR 0076 measured both "
        "slower than the defaults (K=1, linger off); they exist so the finding stays "
        "falsifiable. They are **not** a prerequisite for QoS 2 work (#403/#405).",
        "",
    ]
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    check = "--check" in sys.argv
    src = CONFIG_RS.read_text(encoding="utf-8")
    env_vars = parse_env_vars(src)
    overlay = parse_overlay_vars(src)
    listed = set(env_vars)

    problems = []
    missing = sorted(overlay - listed)
    extra = sorted(listed - overlay)
    if missing:
        problems.append(
            "overlay_from consumes variables not in ENV_VARS: " + ", ".join(missing)
        )
    if extra:
        problems.append(
            "ENV_VARS lists variables overlay_from does not consume: " + ", ".join(extra)
        )
    if problems:
        print("CONFIGURATION generator: inventory mismatch:", file=sys.stderr)
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        print(
            "\nENV_VARS is the curated overlay surface; add the mapping or drop the var.",
            file=sys.stderr,
        )
        return 1

    docs = parse_rustdoc_by_var(src)
    # Bridge knobs live in mqtt-bridge, not mqtt-config (ADR 0025). Their rustdoc
    # still names MQTTD_* so the generated page describes them instead of the
    # generic "experimental / ADR 0076" fallback used for un-documented side channels.
    docs.update(
        parse_rustdoc_by_var(
            (ROOT / "crates" / "mqtt-bridge" / "src" / "config.rs").read_text(
                encoding="utf-8"
            )
        )
    )
    text = render(env_vars, parse_toml_keys(src), docs)
    if check:
        if not OUT.exists() or OUT.read_text(encoding="utf-8") != text:
            print(
                "out of date (run scripts/gen-configuration.py): CONFIGURATION.md",
                file=sys.stderr,
            )
            return 1
        print(f"CONFIGURATION.md is current ({len(env_vars)} ENV_VARS)")
        return 0
    OUT.write_text(text, encoding="utf-8")
    print(f"wrote {OUT.relative_to(ROOT)} ({len(env_vars)} ENV_VARS)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
