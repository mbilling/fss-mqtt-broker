# Comparative benchmark harness (ADR 0048)

Reproducible, one-broker-at-a-time load harness comparing **mqttd** against
**Mosquitto 2.0.20** and **EMQX 5.8.6**, driven by
[`emqtt-bench` 0.6.3](https://github.com/emqx/emqtt-bench) — deliberately **EMQX's own
load tool**, so no home-field driver flatters us
([ADR 0048](../docs/adr/0048-comparative-benchmarking.md) §3).

```sh
cd bench
./run.sh smoke      # seconds-long harness check, all three brokers
./run.sh            # full pass (60 s per scenario)
./run.sh full emqx  # one broker
```

Raw output lands in `results/<stamp>/<broker>/<scenario>.log` plus `env.txt` (versions,
parameters, host). Raw logs are the record — any summary table links back to them.

## Scenarios

| Scenario | What it measures |
|---|---|
| `conn` | connection-establishment rate (`-c` conns at `-R`/s) |
| `pubsub-qos0/1/2` | sustained pub/sub throughput, N publishers → N subscribers, 256-byte payloads |
| `mem` | broker RSS after load (dev-grade proxy; per-connection memory is T2) |

## Posture (T1) — held constant and disclosed

All brokers: **plaintext, anonymous, in-memory sessions** — the competitors'
out-of-the-box behaviour. For mqttd that means `MQTTD_DURABLE_SESSIONS=0`
(**explicitly opting out of our durable-by-default**, ADR 0029) and
`MQTTD_ALLOW_ANONYMOUS=1`; both are disclosed here because ADR 0048 §4 forbids buying
"fast" by quietly turning security off. The **TLS/mTLS posture** (security cost shown,
like-for-like) is T2.

Configs are each broker's documented reasonable minimum, committed in this directory
(`configs/`) — not ours tuned and theirs default.

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
