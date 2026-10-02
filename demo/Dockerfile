# syntax=docker/dockerfile:1
# Build the mqttd broker binary, then ship it on a slim runtime. Build context is the
# repo root (see docker-compose.yml `build.context: ..`).
# Base images are digest-pinned (OpenSSF Pinned-Dependencies); Dependabot's docker
# ecosystem entry for /demo proposes the digest bumps.
FROM rust:1-bookworm@sha256:59037199c44290f2befcdd58dcc540164763fc296950255aaefeef096a1866b0 AS builder
WORKDIR /src
COPY . .
# Pure-Rust TLS (rustls/ring) + OTLP-over-rustls: no OpenSSL/system crypto needed.
# Also build the MQTT-over-QUIC demo client (ADR 0036) used by the `quic-demo` service.
RUN cargo build --release --bin mqttd --example quic_demo

FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/mqttd /usr/local/bin/mqttd
COPY --from=builder /src/target/release/examples/quic_demo /usr/local/bin/quic-demo
# MQTT (1883), peer bus (7001/tcp), SWIM gossip (7946/udp), health+metrics (8080).
EXPOSE 1883 7001 7946/udp 8080
ENTRYPOINT ["mqttd"]
