# A secured three-node cluster, without Kubernetes

This is the end-to-end walkthrough for the broker's headline feature — clustering —
outside Kubernetes. You start from a checkout and end with **three brokers on one
host** forming one cluster with:

- **TLS 1.3** on every client listener, and **no plaintext listener at all**;
- a **mutually-authenticated cluster bus** (`MQTTD_PEER_TLS_*`), one certificate
  per node, the node's identity bound to its certificate CN;
- **per-node signed SWIM gossip** on top of a generated shared key
  (`MQTTD_SWIM_KEY_FILE` + `MQTTD_SWIM_SIGNED=require`);
- **authentication on** (Argon2id password file) and a **deny-by-default topic
  ACL**;
- **majority-aware readiness** (`MQTTD_READY_MIN_MEMBERS`) on every node,
  including — after one deliberate step you will perform — the founder;
- durable sessions on a volume per node.

It builds on the shipped reference deployment in
[`deploy/compose/`](../deploy/compose/) — the same files
[`scripts/compose-smoke.sh`](../scripts/compose-smoke.sh) brings up in CI — rather
than a parallel copy that could drift. Every command here is copy-paste. At the
end, the [starter PKI is mapped to a real CA](#7-map-the-starter-pki-to-a-real-ca)
and to [three real machines](#8-from-one-host-to-three-machines).

Budget about ten minutes, most of it waiting for the lease group to form.

## 0. Prerequisites

- **Docker with Compose**, and the **Mosquitto client tools** on the host
  (`brew install mosquitto` / `apt install mosquitto-clients`). Step 4's wrong-CA
  check also uses `openssl`.
- **An mqttd image newer than v0.9.0**, named in `MQTTD_IMAGE`. The published
  `:latest` is still v0.9.0, which predates both `mqttd --hash-password` (used by
  the bootstrap step) and the `--probe` health-check fast path — against it the
  containers never go healthy (issue #263). Until the tag moves, use an image
  built from this repository:

  ```sh
  # Either: your own tag, if you build/push images already
  export MQTTD_IMAGE=<your-registry>/mqttd:<tag>

  # Or: let the smoke script build one from this checkout (tag: mqttd:compose-smoke).
  # It then also runs this tutorial's bring-up and checks end to end — expect ~3
  # minutes — and tears its copy down again:
  ./scripts/compose-smoke.sh
  export MQTTD_IMAGE=mqttd:compose-smoke
  ```

Host ports `8883`–`8885` (client TLS) and loopback `8080`–`8082` (health) must be
free.

## 1. What you are about to run

```text
      you (mosquitto_sub/pub, TLS 1.3, username+password)
        │ 8883              │ 8884              │ 8885
   ┌─────────┐         ┌─────────┐         ┌─────────┐
   │ mqttd-1 │◄───────►│ mqttd-2 │◄───────►│ mqttd-3 │   cluster bus :7001 — mTLS,
   │ founder │         │         │         │         │   one cert per node, CN = node id
   └─────────┘         └─────────┘         └─────────┘   gossip :7946 — keyed + per-node
     volume              volume              volume      signed (ADR 0022)
```

Three services in [`deploy/compose/compose.yaml`](../deploy/compose/compose.yaml),
each hardened (`read_only`, `cap_drop: ALL`, `no-new-privileges`, non-root, a
memory limit) and each carrying the same environment shape you would write on bare
metal — `MQTTD_TLS_BIND` + per-node `MQTTD_TLS_CERT`/`_KEY`, the
`MQTTD_PEER_TLS_CA`/`_CERT`/`_KEY` trio for the bus, `MQTTD_SWIM_KEY_FILE`,
`MQTTD_PASSWORD_FILE`, `MQTTD_ACL_FILE`, `MQTTD_DATA_DIR`, and
`MQTTD_READY_MIN_MEMBERS: 2`, so a node that cannot see a majority of the three
drops out of rotation instead of serving clients from a store it cannot write.

**The founder rule and the seed lists, spelled out.** Gossip needs an existing
member to join through — a *seed* — and exactly one node must start with **no**
seeds, which is what makes it *found* the cluster. The shipped file encodes that:

| node | `MQTTD_SWIM_SEEDS` | `MQTTD_READY_MIN_MEMBERS` | why |
|---|---|---|---|
| `mqttd-1` | *(empty — the founder)* | `1` at first boot | seedless so it waits for nobody; floor 1 so it can report ready alone and bring-up can begin |
| `mqttd-2` | `mqttd-1:7946` | `2` | joins through the founder |
| `mqttd-3` | `mqttd-1:7946,mqttd-2:7946` | `2` | two seeds, so a node-1 outage does not stop it rejoining |

Both founder settings are first-boot compromises, and [step 5](#5-arm-the-founder)
removes them once the cluster exists. On Kubernetes the chart derives seeds from
the StatefulSet; here they are yours — the same rule stated in
[OPERATIONS](OPERATIONS.md#seed-lists-automatic-on-kubernetes-yours-everywhere-else).

## 2. Mint the secrets

```sh
cd deploy/compose
./bootstrap.sh
```

This writes, into the gitignored `secrets/`:

- `mqttd-swim-key` — the gossip authentication key;
- `mqttd-passwd` — Argon2id `username:hash` lines for the sample users
  (`device-a`, `device-b`, `backend`, `probe` — the identities
  [`acl.toml`](../deploy/compose/acl.toml) grants);
- `PASSWORDS.txt` — the generated plaintext passwords, for you to hand out and
  then delete. The broker never reads it.

It does **not** mint the cluster PKI — that happens inside the next step.

## 3. Bring it up

```sh
docker compose up -d
docker compose ps        # all three healthy in ~60s (lease-group formation)
```

The first `up` runs an `init` one-shot **before any broker starts**: it mints a
throwaway starter CA and one leaf per node (CN = the node id, which the bus
enforces), stages the secrets where the brokers' non-root uid can read them, keeps
the CA private key in a volume **no broker mounts**, and drops `secrets/ca.pem` —
the trust anchor your clients on this host will use. It is idempotent; later `up`s
reuse the same PKI.

The health checks are real probes (`mqttd --probe /readyz`), and `/readyz` is
majority-aware — so "healthy" in that `ps` output means *formed a cluster*, not
*process exists*.

## 4. Verify it

**Cross-node delivery over TLS** — a subscriber on node 1 receives a publish sent
to node 3. Take the passwords from `secrets/PASSWORDS.txt`:

```sh
mosquitto_sub -h 127.0.0.1 -p 8883 --cafile secrets/ca.pem \
              -t 'devices/+/up/#' -u backend -P '<backend password>' &
mosquitto_pub -h 127.0.0.1 -p 8885 --cafile secrets/ca.pem \
              -t 'devices/device-a/up/temp' -m 21.5 -q 1 \
              -u device-a -P '<device-a password>'
```

The `21.5` arrives on the subscriber. Port `8883` is node 1 and `8885` is node 3:
the message crossed the mutually-authenticated bus.

**A wrong-CA client is refused.** Mint a decoy CA and try to verify the broker
against it — the TLS handshake fails before MQTT is ever spoken:

```sh
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout /tmp/wrong-ca.key -out /tmp/wrong-ca.pem -subj '/CN=not-this-cluster'
mosquitto_pub -h 127.0.0.1 -p 8883 --cafile /tmp/wrong-ca.pem \
  -t 'devices/device-a/up/temp' -m nope -u device-a -P '<device-a password>'
# → TLS error: certificate verify failed
```

**No TLS, no service; wrong password, no service:**

```sh
mosquitto_pub -h 127.0.0.1 -p 8883 -t x -m y                  # no --cafile → refused
mosquitto_pub -h 127.0.0.1 -p 8883 --cafile secrets/ca.pem \
  -t 'devices/device-a/up/temp' -m x -u device-a -P wrong     # → refused
```

**And what the brokers say about themselves:**

```sh
docker compose logs | grep -i insecure          # prints NOTHING
docker compose logs | grep -c 'SIGNED per-node' # prints 3 — per-node signed gossip
```

## 5. Arm the founder

`mqttd-1` is still seedless with a readiness floor of 1 — the first-boot
compromises from step 1. Left that way, a lost volume would let it found a
*second* cluster, and alone in a partition it would still report ready while QoS 1
publishes to it hang. Now that the cluster exists, fix both in one step:

```sh
printf 'MQTTD_1_SEEDS=mqttd-2:7946,mqttd-3:7946\nMQTTD_1_READY_MIN_MEMBERS=2\n' >> .env
docker compose up -d mqttd-1
docker compose exec mqttd-1 /usr/local/bin/mqttd --probe /readyz   # still ready: it sees 3
```

## 6. Watch the majority rule work

The payoff of step 5: stop two of three nodes and the survivor **takes itself out
of rotation** instead of pretending.

```sh
docker compose stop mqttd-2 mqttd-3
docker compose exec mqttd-1 /usr/local/bin/mqttd --probe /livez    # exit 0 — alive
docker compose exec mqttd-1 /usr/local/bin/mqttd --probe /readyz   # exit non-zero — a
                                                                   # minority is NOT ready
```

(The flip takes a few seconds — gossip has to declare the peers dead first.)
`live but not ready` is the state a load balancer should read as "pull it, do not
restart it". Bring the majority back and readiness returns:

```sh
docker compose up -d
docker compose exec mqttd-1 /usr/local/bin/mqttd --probe /readyz   # exit 0 again
```

That is the tutorial's cluster, complete. What remains is mapping it to
production trust and to real hardware.

## 7. Map the starter PKI to a real CA

The starter CA is a bring-up convenience: self-signed, trusted by nobody, living
unencrypted in a Docker volume, regenerated after `down -v`. Replacing it means
recognising that this deployment has **three separate trust roles**, which must
not share a CA:

1. **The client-facing listener** (`MQTTD_TLS_CERT` / `MQTTD_TLS_KEY`) is an
   ordinary TLS server. Give each node a certificate for its real DNS name from
   **any CA your clients already trust — including ACME/Let's Encrypt** — and
   clients drop `--cafile` entirely. There are no special constraints on this
   keypair. Renewal is file replacement + `SIGHUP` (or the `MQTTD_CONFIG_WATCH`
   poller): TLS material hot-reloads without dropping live connections
   (ADR 0032), so an ACME renew hook is just "write files, signal".
2. **The cluster bus** (`MQTTD_PEER_TLS_CA` / `_CERT` / `_KEY`) must stay on a
   **private CA you run**, because the bus trusts *every* leaf under that CA as a
   mesh credential. A public CA can never play this role: you would be trusting
   everything it ever signs, and it will not attest your node ids anyway. Keep
   the four rules the reference deployment's PKI satisfies — (1) CN equals
   `MQTTD_NODE_ID`; (2) the SAN covers the host part of `MQTTD_PEER_ADVERTISE`;
   (3) `serverAuth` **and** `clientAuth` EKUs, since every node dials and is
   dialed; (4) an **ECDSA P-256/P-384 or Ed25519 key, not RSA** — the same key
   signs gossip (ADR 0022), which rejects RSA at startup.
   [`deploy/systemd/gen-certs.sh`](../deploy/systemd/gen-certs.sh) mints exactly
   this shape from an admin machine, and
   [OPERATIONS](OPERATIONS.md#cluster-bus-certificates--one-per-node-and-why-that-is-not-negotiable)
   explains why it is one certificate per node.
3. **Client mTLS**, if you add it (`MQTTD_TLS_CLIENT_CA`, shipped commented-out),
   needs a **third CA, never the bus CA** — a device certificate signed by the
   bus CA would be a cluster credential. `compose.yaml`'s comment on that line
   carries the full argument.

The gossip key is independent of all three: rotate it with the three-roll
procedure in [OPERATIONS](OPERATIONS.md#swim-gossip-key-rotation--three-config-rolls-manual-by-design).

## 8. From one host to three machines

Compose on one host survives a *process* failure, not a machine failure. The same
environment shape moves to three machines two ways:

- **systemd** ([`deploy/systemd/`](../deploy/systemd/)): `gen-certs.sh ca` once on
  an admin box, `gen-certs.sh node <id> <hostname>` per machine, install the
  hardened unit and the annotated env file. Five marked lines to edit per node —
  and they are exactly this tutorial's concepts: node id, the TLS material paths,
  `MQTTD_PEER_ADVERTISE` (an address the *other machines* can dial, never
  `0.0.0.0`), the seed list, and the readiness floor.
- **Compose per host**: keep one broker service per machine's compose file and
  make `MQTTD_PEER_ADVERTISE` a routable name; the founder rule and seed lists
  are unchanged.

Either way, day-2 procedures — rotation, decommission, backup, the founder rule's
sharp edges — are in [OPERATIONS](OPERATIONS.md#bare-metal-equivalents), and
capacity arithmetic is in [SIZING](SIZING.md).

## How this tutorial cannot rot

Two CI lanes run these files, so the walkthrough stays true rather than
aspirational:

- [`scripts/compose-smoke.sh`](../scripts/compose-smoke.sh) (nightly, real
  containers; also `mqttui --run compose-smoke`) runs the bring-up and the
  verification steps above: bootstrap → `up -d` → three healthy, the node-3 →
  node-1 TLS delivery, the no-TLS / wrong-CA / wrong-password refusals, the
  no-`INSECURE` and signed-gossip log checks, **arming the founder**, and the
  minority node going live-but-not-ready and recovering. Always with
  `MQTTD_IMAGE` supplied — the published default tag is not covered (issue #263,
  step 0).
- [`scripts/deploy-smoke.sh`](../scripts/deploy-smoke.sh) (every PR) boots three
  real broker processes from the shipped values and proves the posture semantics,
  including that an acked QoS 1 message survives `SIGKILL` of the node that
  accepted it — the durability claim behind `MQTTD_DATA_DIR`.
