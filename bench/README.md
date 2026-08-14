# Comparative benchmark harness (ADR 0048)

Reproducible, one-broker-at-a-time load harness comparing **mqttd** against
**Mosquitto 2.0.20**, **EMQX 5.8.6**, **VerneMQ 2.1.1**, and **NanoMQ 0.25.5**
(the set widened per ADR 0048's amendments, 2026-07-27 and 2026-08-03), driven by
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

> **`results/` is untracked scratch, and nothing may cite it.** Its `.gitignore` is `*`,
> so a path under it is unreachable for every reader but the person who ran it — which is
> exactly the dangling-citation defect issue
> [#244](https://github.com/mbilling/fss-mqtt-broker/issues/244) was filed about.
> Published numbers go in **`docs/benchmarks/`**, and `scripts/check-readme-facts.py`
> fails the build if the README, `docs/COMPARISON.md` or any `docs/benchmarks/*.md`
> cites a path git does not track.

## This harness measures the NON-durable, single-node path

Worth being explicit, because it is easy to assume otherwise: the compose profiles run
mqttd with `MQTTD_DURABLE_SESSIONS=0` on **one** node, for like-for-like comparison
against brokers whose default sessions are not quorum-replicated. So this harness cannot
produce a durable-path or cluster number at all.

The durable path — acked QoS 1/2 throughput and latency against a real quorum — is
measured by a different harness, `crates/mqttd/tests/durable_bench.rs`, and published in
[`docs/benchmarks/DURABLE-PATH.md`](../docs/benchmarks/DURABLE-PATH.md). That one also
carries the **multi-host** invocation (documented, not yet run).

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
- **Losing dimensions get printed** (ADR 0048 §4): Mosquitto and NanoMQ win footprint;
  mTLS costs connection setup; mqttd's durable-session capacity is bounded by the lease
  voter cap (ADR 0021/0049). The results table says all of this next to whatever we win.

## Per-broker fairness notes (ADR 0048 amendment, 2026-08-03)

- **VerneMQ 2.1.1** — the official image is EULA-licensed, free for testing; running it
  here is testing. Its default persists queues to node-local LevelDB (single-node
  durability; documented: offline messages on a dead node are lost) while mqttd's default
  is quorum-replicated — the harness runs mqttd with `MQTTD_DURABLE_SESSIONS=0` for
  like-for-like, and any durable-posture run is labeled as carrying a guarantee VerneMQ
  does not offer. Fault scenarios (none in the current set) must state the partition
  regime: VerneMQ's default fails closed cluster-wide on a detected netsplit. Pinning:
  `latest` on Docker Hub is stale (digest-identical to 2.0.1) and 2.1.2+ are flagged
  pre-release — 2.1.1 is the current stable.
- **NanoMQ 0.25.5** — the `-slim` image variant is used for *both* postures: the default
  variant is built without TLS, and switching variants between postures would compare two
  different builds. NanoMQ's shipped config marks the inflight window
  (`max_inflight_window`/`max_awaiting_rel`) "unsupported now", so its QoS 1/2 numbers are
  not shaped by broker-side flow control the way every other broker's are — footnoted in
  any table that prints them.
- **EMQX pin is due for review at publication**: 5.8.6 is the last Apache-2.0 line,
  EOL'd 2026-02-28; current EMQX is the BSL 1.1 single-distribution line (6.x — single
  node free, no license key needed for this harness). Publishing numbers against an EOL'd
  version would be its own honesty problem; the 0048-T4 publishable run re-pins by
  maintainer decision.

## Harness lessons encoded

- `emqtt_bench conn` **holds its connections and never exits** — it is run detached in a
  timed window and must not be used as a readiness probe (a plain TCP probe is).
- One broker at a time (compose profiles) — brokers must not contend for the host while
  being measured.
