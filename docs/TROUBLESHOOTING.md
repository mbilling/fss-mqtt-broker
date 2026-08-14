# Troubleshooting

The failures a first deployment actually hits, and how to read them. Terms in *italics*
are in the [glossary](GLOSSARY.md).

## Clients cannot connect over TLS

**Symptom:** the client reports a TLS handshake failure, a connection reset, or a bare
timeout — something that looks like a network problem.

- **TLS 1.3-only by default.** Older device firmware that cannot negotiate TLS 1.3 fails to
  connect, and the failure surfaces as a transport error, not a policy one. Check your
  fleet's TLS version support. If some devices genuinely cannot do 1.3, opt into the
  hardened TLS 1.2 mode per listener (`MQTTD_TLS_ALLOW_TLS12`) — it stays off by default so
  the advertised posture holds. See [Limitations](../README.md#limitations).

## A client with a certificate is rejected (mTLS)

**Symptom:** a client presenting a certificate is refused at the handshake, while the same
CA and cert worked with another broker (e.g. Mosquitto).

- **The `clientAuth` EKU is REQUIRED.** rustls/webpki rejects a client certificate that does
  not carry the `clientAuth` Extended Key Usage — even if the certificate is otherwise valid
  and issued by a trusted CA. OpenSSL-based brokers tolerated EKU-less device certs for
  years, so a fleet minted without it connects everywhere *except* here. Check a cert with:

  ```sh
  openssl x509 -in device.crt -noout -ext extendedKeyUsage
  ```

  It must list **TLS Web Client Authentication**. Re-issue any cert that doesn't (add
  `extendedKeyUsage=clientAuth` to the signing extensions).
- **A CRL entry** revokes a client cert at the handshake (`MQTTD_TLS_CRL`). If a device that
  should work is refused, confirm its serial is not on the CRL.

## Devices report publish success but the data never arrives

**Symptom:** a client publishes, gets no error (a QoS 1 PUBACK arrives), yet subscribers see
nothing and there is no obvious failure.

- **A denied v3.1.1 publish is acknowledged; an MQTT 5 one is told `0x87`.** An ACL denial
  drops the message in both versions, but what the publisher sees differs: MQTT 5 gets
  PUBACK/PUBREC `0x87 Not authorized` on QoS 1/2 — check the client's reason code first —
  while MQTT 3.1.1 is acknowledged as success (the protocol has no negative PUBACK), so to
  a 3.1.1 client debugging "missing data" the denial is invisible. In either version the
  place denials are recorded is the **audit log** — every authorization decision lands
  there. Check the ACL (*allow-covers / deny-overlaps*): a topic the publisher is not
  granted `write:` on is dropped.
- **No ACL configured is worse, not better:** with no `MQTTD_ACL_FILE` the broker runs
  permissive and logs an INSECURE warning at startup — every client may publish anywhere, so
  a *different* client's data may be landing where you don't expect.
- **Brownout is (almost) no longer a cause of this symptom.** It used to be: above a
  watermark the broker refused the durable enqueue and acked the publisher anyway. Since
  issue #238 it refuses the publisher instead, which surfaces as the next entry rather
  than as silence. Two acked-and-shed arms remain, stated where the durability claim is
  made (README): a v3.1.1 **retained** publish under brownout or over the retained quota
  is acked and delivered live but its retained *value* is not stored, and the offline
  queue's overflow policy (default `drop-oldest`) truncates already-acked entries at the
  cap — check `publish_dropped{reason="queue-overflow"}` and
  `quota_rejections_total{reason="retained"}`.

## Publishes are refused (`0x97`) or the connection closes without a PUBACK

**Symptom:** a v5 publisher gets `PUBACK`/`PUBREC` with reason `0x97 Quota exceeded`; a
v3.1.1 publisher gets **no PUBACK at all** and its connection is closed (a
`CleanSession=0` publisher resends the message on reconnect; a clean-session one does
not).

- **Most likely: brownout.** Above `MQTTD_STORE_MAX_BYTES` or `MQTTD_MEMORY_MAX_BYTES` a
  `QoS` ≥ 1 publish that needs a durable append (any persistent subscriber) is refused
  rather than acked — v3.1.1 has no reason byte, so a close is the only honest way to say
  it. The watermark may be on a **different node** than the one the publisher dialed: the
  refusal crosses the peer bus as a verdict, so check `mqttd_brownout` on every node.
  (Mid-rolling-upgrade, a link to an older build degrades to a withheld ack and a close,
  so a v5 publisher can briefly see the v3.1.1 answer.) **Nothing acked was lost** — the
  publisher was answered instead of lied to. Whether the message is re-sent is the
  application's decision, not a protocol guarantee: a v5 `PUBACK`/`PUBREC` with reason
  ≥ `0x80` completes the packet-id lifecycle, so no client library retransmits it on its
  own.
- **Diagnose:** `mqttd_brownout{axis}` says whether it is on and which axis;
  `rate(mqttd_quota_rejections_total{reason="brownout-publish"}[5m])` says publishers are
  being refused; compare `mqttd_store_bytes` with `mqttd_store_max_bytes` and
  `mqttd_process_resident_bytes` with `mqttd_memory_max_bytes`; `/statusz` reports the
  onset.
- **Remedy:** expand the disk / raise the watermark / prune retained topics / let
  subscribers drain their queues. Brownout refuses only *growth*, so consumption, deletes
  and expiry shrink the store until the edge lifts on its own.
- **Also possible: the retained quota.** `MQTTD_MAX_RETAINED_MESSAGES` answers a v5
  retained publish creating a *new* topic with the same `0x97`. For **v3.1.1 retained**
  publishes, note that both the quota *and* brownout keep the plain PUBACK when no durable
  enqueue is owed (delivered live, not retained) — so a plain PUBACK does **not** rule
  brownout out. A *close* with no PUBACK points at brownout's durable-enqueue refusal; to
  discriminate, read `mqttd_brownout{axis}` and the counters —
  `quota_rejections_total{reason="retained"}` (retained growth, quota or brownout) vs
  `{reason="brownout-publish"}` (a refused durable enqueue).

## `/readyz` returns not-ready but the broker is running

**Symptom:** the process is up, `/livez` is fine, but `/readyz` is false and Kubernetes will
not route traffic.

- **Readiness gates on cluster membership, not liveness.** A node reports **not ready** when
  it cannot see the expected number of members — a *quorum*-safe design, so a node that
  cannot reach its peers does not serve durable sessions it cannot replicate. Check: are the
  other nodes up? Is *SWIM gossip* working (same `MQTTD_SWIM_KEY` on every node — a mismatch
  silently partitions them)? Is this a fresh cluster where the *founder* has not formed the
  group yet?
- **A lone durable node is healthy alone**, by design; readiness only fails when *more than
  one* member is expected and fewer are present.
- **On a mutually-authenticated cluster bus, look for a certificate mismatch.** If the logs
  carry `peer Hello node id does not match its certificate Common Name`, every peer link is
  being dropped after a *successful* handshake: the bus binds node identity to the
  certificate's Subject CN, so each node needs **its own** leaf with `CN` = its node id (on
  Kubernetes, its pod name). One certificate shared by the whole cluster produces exactly
  this — brokers that start, log signed gossip, and never reach readiness. See
  [Cluster-bus certificates](OPERATIONS.md#cluster-bus-certificates--one-per-node-and-why-that-is-not-negotiable).
  Two more shapes of the same mistake: a leaf with **no SAN** covering the node's
  `peer_advertise` host fails name verification at the dialer (rustls checks SANs only, never
  the CN), and a leaf **missing the `clientAuth` EKU** is rejected when the node dials out.

## The broker refuses to start: `EPHEMERAL durability REFUSED`

**Symptom:** durable sessions are on (the default), no data dir is set, and startup (or
`--check-config`, or a config reload) fails with an error naming `MQTTD_DATA_DIR` and
`MQTTD_ALLOW_EPHEMERAL_DURABILITY`.

- **This is deliberate (issue #240).** Durable-on with no `MQTTD_DATA_DIR` keeps the
  replicated state only in RAM: it survives one node's loss (peers hold it) but a
  correlated restart of a quorum loses acknowledged messages — so the combination is
  refused rather than warned about. Three ways out:
  - **Set `MQTTD_DATA_DIR` and mount a volume** — real durability, the production answer.
    In the container image the writable path is `/var/lib/mqttd`.
  - **`MQTTD_ALLOW_EPHEMERAL_DURABILITY=1`** (`[durable] allow_ephemeral = true`) — the
    explicit dev/test opt-in; the broker then boots and logs the `EPHEMERAL durability`
    warning on every start while it is on.
  - **`MQTTD_DURABLE_SESSIONS=0`** — the lightweight in-memory store, if you never wanted
    the durable plane; an explicit choice that needs no flag.

## `UNDER-REPLICATED` warnings in the log

**Symptom:** the broker logs that a placement group holds fewer copies than the configured
replication factor.

- **Too few nodes are alive** to hold R copies of every group. Replica sets truncate to
  `min(R, members)`, so durability is weaker than intended. It is not corruption: what was
  already acknowledged is still held at the quorum it was committed at, and reads, QoS 0 and
  acked-driven truncation and removal keep serving throughout. QoS 2 in-flight bookkeeping
  does **not**: it writes durable state, so it is refused with everything else (see below).
- **Whether NEW durable writes still commit depends on the write floor** — and by default
  they do not, once the group is down to a single copy. See the next section. Restore
  cluster capacity; the broker logs again when it recovers.
- **Which state you are in is on `/statusz`,** in the `replication` block:
  `min_actual < desired` is under-replicated (warn), `min_actual < write_floor` is
  *refusing* (page).

## Durable writes are refused: no PUBACKs, refused CONNECTs, failed SUBACKs

**Symptom:** any of these, on a cluster that otherwise looks healthy — reads, QoS 0 and
`mqttd --check-config` all fine:

- publishers using QoS 1 or 2 receive no acknowledgement, redeliver forever, and see their
  connection closed and reopened;
- a QoS 2 publisher is disconnected part-way through the handshake;
- **new** persistent sessions are refused at CONNECT with `0x88` after a ~5 s pause;
- SUBSCRIBE on a persistent session comes back `0x80` (failure);
- an in-flight QoS 2 *delivery* stalls without completing.

They share one cause, and each has its **own** signal — the publisher log line below is not
emitted for the others, so do not conclude "the floor is not firing" from its absence:

| Refused write | What the client sees | The signal to look for |
|---|---|---|
| Offline/online enqueue (QoS≥1 publish) | no PUBACK, then disconnect | `failed to enqueue message; withholding the publisher's ack` + `mqttd_durable_append_failures_total{reason="unavailable"}` |
| Inbound QoS 2 dedup record | no PUBREC, then disconnect | `QoS2 dedup store write failed; withholding PUBREC (fail closed)` — **no counter** |
| New persistent session (`claim_session`) | CONNACK `0x88` after ~5 s | `mqttd_durable_recovery_failures_total{reason="deadline"}` — indistinguishable from real quorum loss, so corroborate with `/statusz` |
| Persistent SUBSCRIBE (`set_subscriptions`) | SUBACK `0x80` | `durable subscription write failed` — **no counter** |
| Outbound QoS 2 id bookkeeping | PUBLISH or PUBREL never sent | `mqttd_publish_dropped_total{reason="outbound-id-write-failed"}` |

Because three of those five have no dedicated counter, **`/statusz` is the reliable
discriminator** (step 2 below), not the log line.

This is the **min-replicas write floor** refusing to make a durability promise it cannot
keep (`durable.min_replicas`, default `majority` — issues #167/#239). A placement group
whose replica set has shrunk below the floor refuses new durable writes rather than acking
them on a single copy. Withholding the ack is deliberate: the source keeps the message and
redelivers, so nothing is lost. The connection close is a consequence of the withheld ack,
not a separate fault.

**Confirm it, in this order:**

1. The broker log names it exactly, on the node the publisher is attached to:

   ```text
   WARN mqttd::hub: failed to enqueue message; withholding the publisher's ack (ADR 0041 T5)
        client=<durable-session-id>
        error=storage temporarily unavailable: replica set holds 1 of the configured floor of 2 copies
   ```

   A withheld ack with a *different* `error=` is a different problem. The exact strings the
   broker renders: **`no replication quorum`** is recovery/quorum loss (restore members, or
   wait for the catch-up sweep — watch `replica_groups` on `/statusz`), and **`not the owning
   node for this group`** is a transient ownership hand-off.
2. `/statusz` → `"replication":{"desired":3,"min_actual":1,"under_replicated":true,`
   `"write_floor":2,"write_floor_source":"derived"}`. `min_actual < write_floor` is the
   refusal condition; `write_floor_source` tells you whether the floor was derived from the
   membership this node knows (`derived`) or set explicitly (`configured`).
3. `/metrics` → `mqttd_replication_min_actual < mqttd_replication_write_floor`, and
   `mqttd_durable_append_failures_total{reason="unavailable"}` climbing once per refused
   publish. This pair is the alert rule in
   [OPERATIONS.md](OPERATIONS.md#monitoring-for-the-operator-and-humans).

**Fix it:** restore the missing members. The refusal is transient and lifts by itself, with
no operator action, the moment the replica set is back at the floor.

**If the members are not coming back** (an unconsented loss — two of three nodes gone for
good, an AZ loss, or a DR restore of a single node's `data_dir`), the floor stays armed
because the witness is the *quorum-committed* raft roster, which cannot shrink without a
quorum. Consent explicitly on the surviving node:

```toml
[durable]
min_replicas = 1   # accept single-copy durable acks
```

`durable` is **restart-scoped** (`requires_restart`), so a SIGHUP reload does not apply it: it
logs `config reload: settings changed that require a RESTART to take effect` with
`sections=durable` and keeps the running value. Restart the node. Lowering
`runtime.ready_min_members` does **not** help here: it only bounds the witness from below,
and the persisted roster arms the floor on its own. Setting `min_replicas = 1` means you are
accepting that an acknowledged message may exist on one copy only, and one further loss can
lose it — the broker logs a warning saying so on every start while
`ready_min_members >= 2`.

## An unrecognised flag or `mqttd --version`

- `mqttd --version` prints the version and exits; `mqttd --help` lists every flag. An
  unrecognised flag is an **error** (exit 2), not a silent broker start.

---

Still stuck? `mqttd --check-config` validates the effective configuration without binding
anything, and the [OPERATIONS.md](OPERATIONS.md) runbook covers day-2 procedures (cert and
gossip-key rotation, decommissioning, split-brain checks).
