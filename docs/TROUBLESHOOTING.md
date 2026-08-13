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

- **A denied publish is acknowledged.** This is correct MQTT behaviour: an ACL denial does
  not fail the publish, it drops the message after acknowledging it. To a client debugging
  "missing data" this is invisible. The place to look is the **audit log** — every
  authorization decision is recorded there. Check the ACL (*allow-covers / deny-overlaps*):
  a topic the publisher is not granted `write:` on is silently dropped.
- **No ACL configured is worse, not better:** with no `MQTTD_ACL_FILE` the broker runs
  permissive and logs an INSECURE warning at startup — every client may publish anywhere, so
  a *different* client's data may be landing where you don't expect.

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

## The broker started but "durability" isn't surviving restarts

**Symptom:** durable sessions are on, but a restart loses acknowledged messages.

- **Durable-on with no `MQTTD_DATA_DIR` is in-memory.** The replicated state lives only in
  RAM: it survives one node's loss (peers hold it) but not a correlated restart of a quorum.
  The broker logs an `EPHEMERAL durability` warning on every start in this mode. Set
  `MQTTD_DATA_DIR` **and** mount a volume for real durability. In the container image the
  writable path is `/var/lib/mqttd`.

## `UNDER-REPLICATED` warnings in the log

**Symptom:** the broker logs that a placement group holds fewer copies than the configured
replication factor.

- **Too few nodes are alive** to hold R copies of every group, so durable appends there
  commit on a smaller quorum than you configured. It is not corruption — data is still
  quorum-durable at the reduced size — but durability is weaker than intended. Restore
  cluster capacity to return to the configured factor; the broker logs again when it
  recovers.

## An unrecognised flag or `mqttd --version`

- `mqttd --version` prints the version and exits; `mqttd --help` lists every flag. An
  unrecognised flag is an **error** (exit 2), not a silent broker start.

---

Still stuck? `mqttd --check-config` validates the effective configuration without binding
anything, and the [OPERATIONS.md](OPERATIONS.md) runbook covers day-2 procedures (cert and
gossip-key rotation, decommissioning, split-brain checks).
