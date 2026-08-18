# Changelog

**The canonical changelog is [GitHub Releases](https://github.com/mbilling/fss-mqtt-broker/releases).**

This file is a pointer, deliberately. A hand-maintained `CHANGELOG.md` in a repo
that already generates its release notes is a second source of truth, and the
second one is always the one that goes stale — this project has already retired
one hand-maintained catalogue for exactly that reason (see
[`docs/adr/README.md`](docs/adr/README.md)).

## Where to look for what

| You want | Look at |
|---|---|
| What changed in a release, and the signed artifacts | [GitHub Releases](https://github.com/mbilling/fss-mqtt-broker/releases) |
| Whether a capability is built, and the evidence for it | [delivery dashboard](docs/delivery/STATUS.md) — per-task status, generated and CI-checked |
| Why something was designed the way it is | [`docs/adr/`](docs/adr/) |
| What is *not* built yet | [Limitations](README.md#limitations) |
| Upgrade and version-skew rules | [ADR 0039](docs/adr/0039-versioning-and-upgrade-policy.md) |

## Versioning

Semantic versioning, with the compatibility guarantees of
[ADR 0039](docs/adr/0039-versioning-and-upgrade-policy.md) **in force since
v1.0.0**: adjacent-release version skew (N ↔ N+1, the rolling-upgrade state),
sequential majors through a gateway minor, patches for the three most recent
minor lines, and every schema bump shipping its migration in the same PR.
The pre-1.0 reshape window closed at that tag. MQTT itself is unaffected —
clients speak the published 3.1.1 and 5.0 specifications, which this policy
does not touch.

Release mechanics — signing, SBOM, provenance, and how to verify a downloaded
binary — are in [RELEASING.md](RELEASING.md).
