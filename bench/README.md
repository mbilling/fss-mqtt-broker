# Comparative benchmark harness (ADR 0048)

Reproducible, one-broker-at-a-time load harness comparing **mqttd** against
**Mosquitto 2.0.20** and **EMQX 5.8.6**, driven by
[`emqtt-bench` 0.6.3](https://github.com/emqx/emqtt-bench) — deliberately **EMQX's own
load tool**, so no home-field driver flatters us
([ADR 0048](../docs/adr/0048-comparative-benchmarking.md) §3).

```sh
cd bench
./run.sh smoke                  # seconds-long harness check, all three brokers
./run.sh                        # full pass (60 s per scenario, 5k connections)
./run.sh full emqx              # one broker
./summarize.py results/<stamp>  # markdown table from the raw logs
```

Raw output lands in `results/<stamp>/<broker>/` plus `env.txt` (versions, parameters,
host). Raw logs are the record — `summarize.py` only extracts and links back to them.

## Scenarios (ADR 0048 T2 — the selection metrics)

| Scenario | What it measures |
|---|---|
| `conn` | connection-establishment rate, and **memory per idle connection** (broker RSS snapshotted before/after the ramp) |
| `pubsub-qos0/1/2` | sustained pub/sub throughput, N publishers → N subscribers, 256-byte payloads, and the **end-to-end latency distribution** |
| `tls-conn` | connection establishment under **mTLS** (client certificates required) |
| `tls-pubsub-qos1` | throughput + latency under mTLS — the security cost, shown |

**Latency method:** publishers stamp payloads (`--payload-hdrs ts`); each subscriber
exposes emqtt-bench's `e2e_latency` Prometheus **histogram**, scraped to
`<scenario>.prom`. `summarize.py` reports p50/p99/p999 as **bucket upper bounds**
(1/5/10/25/50/100/500/1000 ms resolution) — coarse, but it cannot flatter: the true
percentile is at most the reported bound.

## Postures — held constant and disclosed

- **Plaintext (1883):** anonymous, in-memory sessions — the competitors' out-of-the-box
  behaviour. For mqttd that means `MQTTD_DURABLE_SESSIONS=0` (**explicitly opting out of
  our durable-by-default**, ADR 0029) and `MQTTD_ALLOW_ANONYMOUS=1`; both are disclosed
  because ADR 0048 §4 forbids buying "fast" by quietly turning security off.
- **mTLS (8883):** TLS with **required client certificates** on every broker, from the
  throwaway PKI in `tls/` (`tls/gen-certs.sh`; client cert carries `clientAuth` EKU —
  rustls enforces it).

Configs are each broker's documented reasonable minimum, committed here (`configs/`,
`docker-compose.yml`) — not ours tuned and theirs default.

## Honesty rules that bind this harness

- **Dev-grade vs publishable:** anything run on a laptop/dev machine is dev-grade — it
  proves the harness and guides work, and is never published. Publishable numbers come
  from a dedicated, documented host with the driver separated from the broker.
- **The scaling curve (T3) never runs single-host**: a consensus-backed cluster is
  fsync-bound, and N nodes on one disk scale *negatively* — a laptop artifact that would
  manufacture false evidence (see the
  [2026-07-14 post-mortem](../docs/postmortems/2026-07-14-ha-bridge-durable-refused.md)).
  One small host per node, or the curve is not published.
- **Losing dimensions get printed** (ADR 0048 §4): Mosquitto wins footprint; mTLS costs
  connection setup; mqttd's durable-session capacity is bounded by the lease voter cap
  (ADR 0021/0049). The results table says all of this next to whatever we win.

## Harness lessons encoded

- `emqtt_bench conn` **holds its connections and never exits** — it is run detached in a
  timed window and must not be used as a readiness probe (a plain TCP probe is).
- One broker at a time (compose profiles) — brokers must not contend for the host while
  being measured.
