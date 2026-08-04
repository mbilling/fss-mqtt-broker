# Operations — day-2 procedures (Kubernetes-first)

The Helm chart (`deploy/helm/mqttd`, [ADR 0047](adr/0047-kubernetes-deployment.md))
encodes the deployment contracts: StatefulSet with per-pod volumes, decommission-drain
on scale-down, one-at-a-time rolls, a PodDisruptionBudget, and `--check-config` before
serving. This page is the rest: the procedures an operator runs *after* day 1. Signals
and files are the control surface — there is deliberately no admin API
([README](../README.md#principles)).

## Certificate / ACL / CRL rotation — automatic

The chart sets `[runtime] config_watch_secs = 30`: the broker polls the mounted policy
files (TLS cert/key/client-CA, ACL, CRLs, password/JWT files — [ADR 0033](adr/0033-config-file-watch-reload.md))
and reloads through the validate-before-swap path when any changes on disk.

**Procedure:** update the Secret/ConfigMap (or let cert-manager renew it). The kubelet
projects the new content into the mounted volume (allow up to ~a minute of kubelet sync
+ the 30 s poll); the broker reloads, and — per [ADR 0040](adr/0040-revocation-reaches-live-state.md)
— the new policy **sweeps live state** (a CRL'd client's session ends, a removed grant
stops its flow).

**Verify:** `security_reloads_total{trigger="watch"}` increments; the reload is
audit-logged. A malformed file is rejected and the running policy kept — fix the file
and the watcher retries on the next poll.

## SWIM gossip key rotation — three config rolls (manual by design)

The broker supports a dual-key window (`[cluster.swim] key_accept`,
[config reference](mqttd.example.toml)): datagrams are *sealed* with `key_file` and
*accepted* under `key_file` ∪ `key_accept`. The gossip key is not in the file-watch
reload scope — each phase is a config change that rolls the pods (one at a time,
drain-safe):

1. **Widen acceptance.** Keep sealing with the old key, start accepting the new one:
   `key_file` → old key, `key_accept = ["<new-hex>"]`. Update the Secret + config,
   `helm upgrade`; wait for the roll to complete.
2. **Flip the sealer.** `key_file = new`, `key_accept = [old]`. `helm upgrade`; roll.
   Every node now seals with the new key; laggards' old-key datagrams still accepted.
3. **Close the window.** Remove `key_accept`. `helm upgrade`; roll.

Never skip phase 2: jumping old→new directly partitions the gossip plane mid-roll
(half the fleet rejects the other half's datagrams).

**Verify each phase** (ADR 0054 T3): `mqttd_swim_keys_accepted` reads 2 on every node
while the window is open and must return to 1 after phase 3 — alert if it stays at 2
longer than a rotation should take. `curl <pod>:8080/statusz | jq .keys` shows the
accepted-key *fingerprints* (never material), so you can confirm every node staged the
same new key before flipping the sealer. Config convergence after each roll:
`jq .config.checksum` identical across pods (`mqttd_config_info{checksum}` is the
alertable equivalent).

## Scaling

- **Up:** `kubectl scale sts <name> --replicas=N` or `helm upgrade --set replicaCount=N`.
  New pods seed to the first two stable ordinals and back-fill behind the caught-up
  watermark ([ADR 0043](adr/0043-elastic-cluster-resize.md)). Note `helm upgrade`
  re-renders the config (the joiner readiness floor is a majority of the new count) and
  therefore rolls the fleet; `kubectl scale` grows without a roll but leaves existing
  pods' floor at the old majority until the next upgrade.
- **Down: one step at a time.** Each termination runs the full decommission drain
  (`preStop` → SIGUSR1): the departing node hands every held key to the surviving
  replica set and verifies before exiting. Watch `/readyz` and the drain logs; then
  take the next step. Never drop below a durable quorum
  (`lease_voters`, default 5 → keep ≥ 3 running).
- **PVCs:** on Kubernetes ≥ 1.27 the chart deletes the scaled-down pod's PVC
  (`persistence.retentionPolicy.whenScaled: Delete`) — safe *because* the drain ran
  first, and it prevents a later scale-up from reattaching a stale data dir under the
  same node id. On older clusters delete the orphaned PVC by hand after the drain
  completes. Deleting the whole release keeps PVCs (`whenDeleted: Retain`).

## Split-brain detection (ADR 0054 T2)

Every node carries a **cluster identity**: the founder mints it at first bootstrap
(persisted as `cluster-id` in the data dir), joiners adopt it over gossip, and gossip
from a *different* identity is dropped and counted — a second, separately-founded
cluster is **contained**, not merged.

**The check:** `curl <pod>:8080/statusz | jq .cluster_id` across all pods — a healthy
cluster reports exactly one value. Alertable equivalents:
`mqttd_cluster_info{cluster_id}` (two distinct label values across the fleet =
split brain), `mqttd_foundings_total` (any increment after a brand-new cluster's
first boot = something founded again), and
`mqttd_gossip_rejected_total{reason="cluster-mismatch"}` (> 0 = a foreign cluster's
gossip is arriving — find the re-founded node and fence it).

**Recovery:** the node holding the *minority/new* identity is the wrong one. Stop it,
wipe its data dir (including `cluster-id`), and rejoin it with seeds — it adopts the
surviving cluster's identity and back-fills per ADR 0043.

## The founder rule (read before touching pod-0's storage)

Pod-0 renders with **no seeds**: a pod-0 that starts with an *empty data dir and no
seeds* founds a **new** lease group. With its PV intact this never happens (it knows it
is initialized and rejoins). But **never delete pod-0's PVC while a cluster is live** —
a fresh, seedless pod-0 would found a second cluster beside the survivors (split
brain). If pod-0's volume is lost:

1. Scale the StatefulSet so pod-0 is excluded from client traffic (it will not pass
   the joiner readiness floor of other pods; the founder floor is 1 — act promptly).
2. Recreate its PVC empty and give it seeds for recovery: temporarily set the
   config's seed rendering via `helm upgrade --set-string` overriding `config` so
   ordinal 0 also seeds to ordinal 1 (a values override of the `config` blob), let it
   join as a *replacement* node ([ADR 0043](adr/0043-elastic-cluster-resize.md)'s
   replace motion) and back-fill from the survivors.
3. Restore the normal config with the next `helm upgrade`.

## Rolling upgrades

`helm upgrade` with a new image tag rolls one pod at a time (OrderedReady +
RollingUpdate + PDB `maxUnavailable: 1`); each pod drains via `preStop` before its
restart and rejoins behind the caught-up watermark. Version-skew rules are
[ADR 0039](adr/0039-versioning-and-upgrade-policy.md) (pre-1.0: no cross-version
compatibility is promised; post-1.0: adjacent releases negotiate). A roll currently
pays the full drain on every pod — a known cost, recorded in the ADR 0047 amendment.

## Backup

Durable state is quorum-replicated: the primary recovery story is *the cluster itself*
(a lost node's state rebuilds from survivors — wipe-and-rejoin). For disaster recovery
beyond quorum loss, snapshot the PVs of a **stopped** node (redb files are only
crash-consistent under snapshot while running) or use storage-class volume snapshots;
restore = recreate the StatefulSet over restored PVs with the same pod names.

## Bare-metal equivalents

Same procedures, different transport: rotation = replace the files (the watcher path
is identical) or `kill -HUP`; decommission = `mqttd --decommission --timeout <secs>`
or `kill -USR1` + wait for exit; the founder rule applies to whichever node you
bootstrap with empty seeds.
