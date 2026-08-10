# mqttui

A terminal UI and headless runner for the [mqttd](https://github.com/mbilling/fss-mqtt-broker)
MQTT broker's demo, migration and operational scripts.

It carries the examples inside the binary, so it works with no clone of the repository:

```
cargo install mqttui
mqttui                       # the terminal UI
mqttui --list                # every task, grouped
mqttui --show demo-stack     # what it does, what it needs, what it costs
mqttui --run demo-stack      # run it
```

## What it can do without a checkout

- **the demo stack** — seven brokers, Prometheus, Grafana, a load generator
- **the Kubernetes examples** — the Helm chart and the CRDs
- **the compose reference deployment**
- **the Mosquitto migration converter** — `mqttui migrate mosquitto /etc/mosquitto/mosquitto.conf`,
  which needs neither Python nor a checkout

Tasks that operate *on the repository* — building it, diffing its rendered output, checking
its own documentation — cannot be carried by any binary and are marked `-` in `--list`, with
the reason stated rather than quietly omitted. So are the tasks whose fixtures are too large
to bundle. Both run from a clone.

## Why the examples are embedded rather than fetched

The whole surface is 190 KB compressed. Embedding it means the tool works offline, the
examples are version-locked to the binary that was tested with them, and — the property that
matters — **nothing that arrived over the network is executed**. The broker's releases are
cosign-signed with SLSA provenance and an SBOM; downloading shell from a mutable branch and
running it on every launch would discard that with one command.

## Install

`cargo install mqttui` compiles it locally. Signed, reproducible static binaries for
x86_64 and aarch64 Linux are published with every release and are the recommended install —
they are cosign-signed and carry SLSA provenance, verifiable with `cosign verify-blob`.

## License

Apache-2.0. See the [repository](https://github.com/mbilling/fss-mqtt-broker) for the broker
itself and the architecture decision records, including
[ADR 0056](https://github.com/mbilling/fss-mqtt-broker/blob/main/docs/adr/0056-mqttui.md),
which is why this tool exists and why it is a separate workspace.
