# Security policy

This broker is built to be a top-tier choice for security-sensitive deployments.
Handling vulnerability reports well is part of that promise. This document is the
contract: how to report, what to expect, and how fixes ship.

## Reporting a vulnerability

**Do not open a public issue for a suspected vulnerability.** Public disclosure
before a fix exists puts every operator at risk.

Report privately through GitHub's coordinated-disclosure channel:

- Go to the repository's **Security → Report a vulnerability** tab (GitHub
  private vulnerability reporting), which opens a private advisory visible only
  to you and the maintainers.

A good report includes:

- the affected version or commit,
- the component (client-facing MQTT, the peer/cluster bus, the gossip plane,
  the durable store, config/auth parsing),
- a description of the impact (what an attacker gains),
- and, where possible, a reproducer — a packet capture, a crashing input, or a
  minimal fuzz artifact.

If you found it with the in-repo fuzz harness (see below), the crashing input
under `fuzz/artifacts/` is the ideal reproducer.

## What to expect

This is a pre-1.0 project maintained on a best-effort basis; these are targets,
not contractual SLAs, and they will firm up at 1.0.

- **Acknowledgement** within a few days of a report.
- **Triage** — a severity assessment and whether it is confirmed — to follow.
  Severity uses the usual lens: attacker position (unauthenticated network,
  authenticated client, cluster peer, local operator), and impact
  (durability/consistency violation, authentication or authorization bypass,
  denial of service, information disclosure).
- **A fix or documented mitigation** prioritized by severity. A remotely
  reachable pre-authentication issue is dropped-everything work; a local or
  authenticated-peer issue is scheduled.
- **Coordinated disclosure**: a fix is prepared privately, then released
  together with a GitHub Security Advisory crediting the reporter (unless you
  ask to remain anonymous). We ask reporters to hold public details until the
  patched release is out.

## How fixes ship

- Every confirmed vulnerability gets a regression test before it is closed —
  for a parser crash, the crashing input becomes a permanent corpus entry and a
  `darksky` protocol-violation case, so the same class cannot silently return.
- Fixes land on `main` and, post-1.0, in a patch release of every supported
  line, with the advisory naming the fixed versions.
- The advisory states the attacker position and impact plainly, so operators
  can judge their own exposure.

## What is in scope

The broker's trust boundaries, roughly in order of exposure:

- **The gossip plane** — anyone who can send a UDP datagram to the SWIM port
  reaches `SwimAuth::open` before any authentication. Continuously fuzzed.
- **Client-facing MQTT** — the packet codec decodes attacker-controlled bytes on
  every connection. Continuously fuzzed.
- **The peer/cluster bus** — mTLS-gated, but a compromised or buggy peer can
  send malformed frames; the frame decoder is continuously fuzzed.
- **Config and auth parsing** — the CRL (DER) and ACL (TOML) parsers consume
  operator-supplied, hot-reloaded files; both are continuously fuzzed.
- **The durability contract** — a silent loss of an acknowledged message, or a
  consistency violation under fault, is a security-relevant defect here and is
  treated as one.

## Our own continuous testing

Security is asserted continuously, not audited once
([ADR 0044](docs/adr/0044-release-readiness-assurance.md)):

- **Fuzzing** (`cargo +nightly fuzz`, one harness per attacker-reachable
  parser): `packet_decode` (MQTT codec), `peer_decode` (peer frames),
  `swim_message` and `gossip_open` (gossip plane), `crl_parse` and `acl_parse`
  (config/auth). Committed seed corpora live under each crate's `fuzz/seeds/`;
  the nightly CI tier runs every target and any finding becomes a regression.
- **The acked-facts oracle** across in-process and out-of-process fault
  schedules, real crashes, partitions, disk-full, and rolling upgrades — a
  broken durability or recovery promise fails the build.
- **`darksky`** protocol-violation and abuse suite, and the resource-governance
  admission caps (ADR 0041) that bound what an unauthenticated peer can consume.

If you want to help: run the fuzz targets, and report anything that panics.
