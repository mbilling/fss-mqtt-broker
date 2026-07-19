# syntax=docker/dockerfile:1
#
# Hardened runtime image for mqttd (ADR 0045 T4).
#
# Base is distroless/static: a CA bundle, tzdata, and a nonroot user — and NO
# libc, shell, or package manager. The broker binary is a fully-static musl
# build (scripts/release/build-repro.sh), so it needs nothing from the base but
# the CA roots; there is no dynamic loader and no OS userland to carry CVEs or
# give a foothold. The `:nonroot` tag runs as uid 65532, so the broker never
# runs as root.
#
# The binary is built reproducibly OUTSIDE the image and copied in — the image
# therefore contains the *exact* signed, checksummed binary from the release, not
# a second one compiled here that could drift from it. The release pipeline
# stages the per-arch binary at `dist/mqttd` before building.
#
# Build (from repo root, after staging the binary):
#   cp target/<musl-triple>/release/mqttd dist/mqttd
#   docker build -t mqttd:dev .
FROM gcr.io/distroless/static-debian12:nonroot

# OCI metadata — the source of truth for provenance readers. `revision`/`version`
# are filled by the release pipeline via --build-arg.
ARG VCS_REF=unknown
ARG VERSION=0.0.0-dev
LABEL org.opencontainers.image.title="mqttd" \
      org.opencontainers.image.description="Fast, Secure, Scalable MQTT broker" \
      org.opencontainers.image.source="https://github.com/mbilling/fss-mqtt-broker" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.version="${VERSION}"

# The pre-built reproducible binary for this image's architecture, staged by the
# release pipeline (or by hand for a local build) at dist/mqttd.
COPY --chmod=0755 dist/mqttd /usr/local/bin/mqttd

# Conventional MQTT ports (plaintext / TLS). Actual binds are operator-chosen via
# MQTTD_* config; EXPOSE here is documentation, not a binding.
EXPOSE 1883 8883

# Configuration (ADR 0046): configure via a TOML file, MQTTD_* env vars, or both —
# precedence is defaults < file < MQTTD_* env < CLI flags.
#   - File:    mount a ConfigMap and point at it, e.g.
#                -v ./mqttd.toml:/etc/mqttd/mqttd.toml  +  MQTTD_CONFIG=/etc/mqttd/mqttd.toml
#              (or append `--config /etc/mqttd/mqttd.toml` after the entrypoint).
#              See docs/mqttd.example.toml for a fully-commented template.
#   - Env:     pass -e MQTTD_*=... ; env overrides the file per-setting.
#   - Secrets: reference by PATH (TLS keys, password_file, JWT keys via *_FILE,
#              MQTTD_SWIM_KEY_FILE) mounted from a Secret — keep them out of the file.
#   - Validate a config without starting the broker:  mqttd --check-config --config <path>
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/mqttd"]
