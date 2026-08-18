# Deploying mqttd

Four packagings of the same broker. Pick by what your fleet already runs — none of them
is a downgrade, and they are configured identically (`MQTTD_*` environment, secrets by
path).

| | Where it lives | Use it when | Tested by |
|---|---|---|---|
| **Helm chart** | [`helm/mqttd`](helm/mqttd) | You run Kubernetes | `helm lint` + kubeconform + [`scripts/k8s/peer-tls-check.sh`](../scripts/k8s/peer-tls-check.sh) + a kind end-to-end job with the cluster bus ON |
| **Compose** | [`compose/`](compose) | One host, or a small fleet with Docker | [`scripts/deploy-smoke.sh`](../scripts/deploy-smoke.sh) per PR + [`scripts/compose-smoke.sh`](../scripts/compose-smoke.sh) (real containers, nightly) |
| **systemd** | [`systemd/`](systemd) | Bare metal or VMs, no container runtime | [`scripts/deploy-smoke.sh`](../scripts/deploy-smoke.sh) |
| **Operator** | [`helm/mqttd-operator`](helm/mqttd-operator) | You run Kubernetes and want `MqttdCluster` CRD management (split-brain fencing, brownout remediation — opt-in, alert-only by default). Image publishes from release `v0.9.1` on (forward-pinned; gate-proven). [`operator/`](operator) is the dev/e2e manifest, not the install path | `helm lint` + kubeconform + the CRD schema pin + a nightly kind e2e (fence, GC, RBAC `auth can-i`) |

The Helm chart is the most automated: it derives node identity, seed lists and the
readiness floor from StatefulSet ordinals, so there is nothing per-node to maintain.
Everywhere else **the seed list is yours** — see
[Seed lists](../docs/OPERATIONS.md#seed-lists-automatic-on-kubernetes-yours-everywhere-else).

## What "tested" means here

`scripts/deploy-smoke.sh` boots a real three-node cluster using the values from the
shipped `deploy/systemd/mqttd.env.example` — it parses that file rather than restating
it, so a variable renamed in the artifact and not in the test fails loudly — and proves:

- every client connection is over **TLS**, using a PKI minted by the shipped
  `deploy/compose/init.sh` — so the tested configuration is the shipped one — and a
  cleartext client is refused because there is no plaintext listener to fall back to;
- the systemd packaging's [`systemd/gen-certs.sh`](systemd/gen-certs.sh) is **executed**, and
  two more nodes are booted from its output and made to route to each other: neither shipped
  PKI recipe is a recipe nothing runs;
- no node logs `INSECURE`, and every node logs `SWIM gossip is SIGNED per-node`: the
  cluster bus is mutually authenticated, which is also what makes gossip per-node signed;
- anonymous clients are refused, and a wrong password is refused;
- a password file made with `mqttd --hash-password` authenticates;
- the ACL confines a device to its own subtree (SUBACK `128` for another device's topics,
  a granted QoS for its own — both halves, so a broker that denied everything would fail);
- a publish on node 1 reaches a subscriber on node 3;
- **an acknowledged QoS 1 message survives `SIGKILL`ing the node that accepted it.**

It additionally validates `compose.yaml` with `docker compose config` — both alone and with
the opt-in plaintext overlay, asserting that the default renders no `1883` and the overlay
does — and runs `systemd-analyze verify` on the unit. Both are **skipped loudly** when
those tools are absent rather than passing silently.

It runs in CI on every push. It also asserts, on the rendered `compose.yaml`, the two
properties that are claims about the file itself: each broker mounts **only its own** TLS
volume while the CA-key volume reaches the `init` one-shot alone, and the founder's readiness
floor renders `1` by default and `2` once `MQTTD_1_READY_MIN_MEMBERS=2` arms it.

What it cannot do is bring containers up, so
[`scripts/compose-smoke.sh`](../scripts/compose-smoke.sh) does exactly that on the nightly
image lane: `./bootstrap.sh && docker compose up -d` on the shipped file, three healthy
containers, a TLS publish on node 3 delivered to a TLS subscriber on node 1, no host port
`1883` published (and no cleartext accepted there, when that port was free to begin with),
each broker's TLS volume holding its own key and no other node's, and the missing-secrets
case failing with a message that names `./bootstrap.sh`.

**The image is covered too** (the close of issue #263): `compose.yaml`'s default is
**pinned** to `ghcr.io/mbilling/fss-mqtt-broker:0.9.1` — a published release whose binary
has every flag these artifacts use — and a nightly lane runs this exact file against that
published default with no override, while a per-PR gate proves each flag exists in the
binary at the pinned tag. (`compose-smoke.sh` still also runs per-PR with `MQTTD_IMAGE`
set to a build from this repository, covering HEAD.)

## What none of these do for you

- **A production trust root.** TLS is **on** in every packaging you can install today — the
  chart is TLS-only, Compose mints a throwaway starter PKI in a one-shot at bring-up (so
  `up -d` is TLS on the first run), and the systemd packaging ships the TLS and cluster-bus
  lines *uncommented* plus [`systemd/gen-certs.sh`](systemd/gen-certs.sh) to mint the
  material, as a marked install step, and on Kubernetes
  [`helm/mqttd/bootstrap.sh`](helm/mqttd/bootstrap.sh) mints and self-verifies the whole set
  (server TLS, per-node cluster-bus leaves, gossip key) into Secrets. What none of them gives you is a **real** CA: every
  starter PKI here is self-signed, unrevocable and disposable. Bring your own before
  production — and when you do, satisfy the four rules the cluster bus enforces on the peer
  certificate, which [`systemd/mqttd.env.example`](systemd/mqttd.env.example) lists at the
  point of use (CN = `MQTTD_NODE_ID`; SAN covers the `MQTTD_PEER_ADVERTISE` host; `serverAuth`
  **and** `clientAuth`; an ECDSA or Ed25519 key, never RSA, because that key also signs
  gossip). One CA for the whole cluster, and its private key on none of the broker hosts:
  the bus binds node identity to the certificate CN, so a host that can read the CA key can
  claim any node's identity. Where a packaging mints per-node material it keeps them apart
  too: the compose one-shot puts each node's key in a volume mounted into that broker only,
  because all three containers run as the same uid and the mount list is the only boundary
  left. And if you add **client** mTLS, its CA must be a *different* CA from the cluster-bus
  one — the bus trusts every leaf under its CA as a mesh member, so a client certificate from
  that CA is a cluster credential (`systemd/mqttd.env.example` says so at the setting).
- **Load balancing.** MQTT is long-lived and stateful; any TCP load balancer works, but
  point its health check at `/readyz` (majority-aware) rather than at the MQTT port.
- **Backups.** Durable state is quorum-replicated, so the primary recovery story is the
  cluster itself — but quorum is not a backup: it replicates operator error faithfully. Take
  an **online** export with `mqttd --backup` on **every** node (a per-node export is not a
  cluster snapshot) and restore into a fresh cluster with `MQTTD_RESTORE_FROM`; see
  [Backup and disaster recovery](../docs/OPERATIONS.md#backup-and-disaster-recovery) for the
  window it guarantees, the RPO/RTO, and the gaps it leaves open.
  **Neither packaging here mounts a backup destination by default** — the chart ships no
  backup volume or `MQTTD_BACKUP_*` plumbing, and the systemd unit's `ProtectSystem=strict`
  grants `ReadWritePaths=/var/lib/mqttd` only, while `backup.dir` must live *outside* the
  data dir. OPERATIONS carries the minimum opt-in for each (chart `extraVolumes` /
  `extraVolumeMounts` / `extraEnv` against a separate PVC; a `systemctl edit` drop-in adding
  `ReadWritePaths`), and until you add one, `kubectl exec mqttd-0 -- mqttd --backup` exits 2.
  In the pod, pass `--config /config/mqttd.toml` — the chart renders `node.data_dir` into
  that file, and `--backup` must load an effective config to know where to watch.
