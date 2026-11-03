# syntax=docker/dockerfile:1
# Build the mqttd broker binary, then ship it on a slim runtime. Build context is the
# repo root (see docker-compose.yml `build.context: ..`).
# Base images are digest-pinned (OpenSSF Pinned-Dependencies); Dependabot's docker
# ecosystem entry for /demo proposes the digest bumps.
FROM rust:1-bookworm@sha256:9181b56c2a21ab1d5e73cffe04fc762426d30aa7fac3c91141dd033f49a7fc8b AS builder
WORKDIR /src
COPY . .
# Pure-Rust TLS (rustls/ring) + OTLP-over-rustls: no OpenSSL/system crypto needed.
# Also build the MQTT-over-QUIC demo client (ADR 0036) used by the `quic-demo` service.
RUN cargo build --release --bin mqttd --example quic_demo

FROM debian:bookworm-slim@sha256:99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/mqttd /usr/local/bin/mqttd
COPY --from=builder /src/target/release/examples/quic_demo /usr/local/bin/quic-demo
# MQTT (1883), peer bus (7001/tcp), SWIM gossip (7946/udp), health+metrics (8080).
EXPOSE 1883 7001 7946/udp 8080
ENTRYPOINT ["mqttd"]
