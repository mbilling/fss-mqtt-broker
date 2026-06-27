# syntax=docker/dockerfile:1
# Build the mqttd broker binary, then ship it on a slim runtime. Build context is the
# repo root (see docker-compose.yml `build.context: ..`).
FROM rust:1-bookworm AS builder
WORKDIR /src
COPY . .
# Pure-Rust TLS (rustls/ring) + OTLP-over-rustls: no OpenSSL/system crypto needed.
# Also build the MQTT-over-QUIC demo client (ADR 0036) used by the `quic-demo` service.
RUN cargo build --release --bin mqttd --example quic_demo

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/mqttd /usr/local/bin/mqttd
COPY --from=builder /src/target/release/examples/quic_demo /usr/local/bin/quic-demo
# MQTT (1883), peer bus (7001/tcp), SWIM gossip (7946/udp), health+metrics (8080).
EXPOSE 1883 7001 7946/udp 8080
ENTRYPOINT ["mqttd"]
