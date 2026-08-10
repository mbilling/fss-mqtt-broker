# Deploying mqttd

Four packagings of the same broker. Pick by what your fleet already runs — none of them
is a downgrade, and they are configured identically (`MQTTD_*` environment, secrets by
path).

| | Where it lives | Use it when | Tested by |
|---|---|---|---|
| **Helm chart** | [`helm/mqttd`](helm/mqttd) | You run Kubernetes | `helm lint` + kubeconform + a kind end-to-end job |
| **Compose** | [`compose/`](compose) | One host, or a small fleet with Docker | [`scripts/deploy-smoke.sh`](../scripts/deploy-smoke.sh) |
| **systemd** | [`systemd/`](systemd) | Bare metal or VMs, no container runtime | [`scripts/deploy-smoke.sh`](../scripts/deploy-smoke.sh) |
| **Operator** | [`operator/`](operator) | **Not installable yet** — no published image (ADR 0055 T8) | kind e2e only |

The Helm chart is the most automated: it derives node identity, seed lists and the
readiness floor from StatefulSet ordinals, so there is nothing per-node to maintain.
Everywhere else **the seed list is yours** — see
[Seed lists](../docs/OPERATIONS.md#seed-lists-automatic-on-kubernetes-yours-everywhere-else).

## What "tested" means here

`scripts/deploy-smoke.sh` boots a real three-node cluster using the values from the
shipped `deploy/systemd/mqttd.env.example` — it parses that file rather than restating
it, so a variable renamed in the artifact and not in the test fails loudly — and proves:

- anonymous clients are refused, and a wrong password is refused;
- a password file made with `mqttd --hash-password` authenticates;
- the ACL confines a device to its own subtree (SUBACK `128` for another device's topics,
  a granted QoS for its own — both halves, so a broker that denied everything would fail);
- a publish on node 1 reaches a subscriber on node 3;
- **an acknowledged QoS 1 message survives `SIGKILL`ing the node that accepted it.**

It additionally runs `docker compose config` and `systemd-analyze verify` when those
tools are present, and **says so loudly when they are not** rather than passing silently.

It runs in CI on every push.

## What none of these do for you

- **Certificates.** TLS is off in the reference configs (plaintext on a trusted network);
  every artifact carries the commented-out lines and the file paths. Bring your own PKI.
- **Load balancing.** MQTT is long-lived and stateful; any TCP load balancer works, but
  point its health check at `/readyz` (majority-aware) rather than at the MQTT port.
- **Backups.** Durable state is quorum-replicated, so the primary recovery story is the
  cluster itself. For disaster recovery see
  [Backup](../docs/OPERATIONS.md#backup) — snapshot a **stopped** node's data directory.
