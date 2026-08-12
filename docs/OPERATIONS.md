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

**Trust note (audit #203):** because revocation sweeps live state, *write access to the
mounted CRL file is the power to evict any mTLS client* — an attacker (or a bad
automation) who can modify that file can force-disconnect healthy sessions
cluster-wide, a denial of service through the security machinery doing its job.
Protect the policy volume (Secret RBAC, no wide ConfigMap write grants) to the same
standard as the credentials it revokes; the audit log records each reload with its
trigger, which is the forensic trail if it happens.

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

**The odd one out takes itself out of rotation.** A node that is **alone** in its
membership view and is **hearing another cluster's gossip** is the divergent side of a
split brain — it rejects every foreign datagram, so it never learns a peer, while the
healthy majority has each other. It reports `NotReady` rather than serve clients an empty
session and retained store, and says why, so this is not mistaken for a slow start:

```
curl -s <pod>:8080/statusz | jq .quarantine
{ "active": true, "reason": "refounded-beside-live-cluster" }
```

Alert on `mqttd_refound_quarantine == 1`: unlike an ordinary NotReady pod, this one
never recovers on its own. Two consequences to expect while it sits there — under
`podManagementPolicy: OrderedReady` a NotReady pod-0 blocks replacement of higher
ordinals, and it counts against the PodDisruptionBudget, so node drains stall. Both are
the incident being loud, not a second fault. Clear it with the recovery above.

A genuine first bootstrap is alone too, but hears no foreign gossip, so the guard cannot
misfire there. The verdict is live, not latched: a node that rejoins properly serves
again on its own. To
re-bootstrap deliberately beside a cluster you are abandoning, set
`MQTTD_REFOUND_GUARD=false` (env, so it needs no config edit or roll) or
`[cluster] refound_guard = false`.

## Volumes outlive pods (and why scaling to zero is not a restart)

Both PVC retention policies default to **Retain** (issue #97), so a scale-down leaves the
departed pod's volume behind:

```
kubectl -n <ns> get pvc            # data-<sts>-3 is still Bound after shrinking to 3
kubectl -n <ns> delete pvc data-<sts>-3   # reclaim it once you are satisfied
```

That orphan is deliberate. `whenScaled: Delete` is a reasonable setting *while a survivor
remains* — an ordinary shrink drains first (ADR 0043), so the departing volume holds only
superseded state. But Kubernetes applies the policy uniformly, and at `--replicas=0`
there is no survivor to drain to: the same setting erases the only copy of every session,
every retained message, and the cluster identity. Silently, from an operation that looks
like an ordinary restart. Set `persistence.retentionPolicy.whenScaled: Delete` only if
you shrink often, never scale to zero, and would rather not reclaim by hand.

**To restart a whole cluster, do not scale to zero.** Use a rolling restart, which is
quorum-safe and keeps volumes and identity:

```
kubectl -n <ns> rollout restart statefulset/<sts>
```

A full-fleet restart *does* recover on its own (verified: pods return, identity and lease
group intact, all Ready in ~15 s) — the danger was never the restart, it was the volume
deletion that `--replicas=0` used to trigger.

**Reusing an ordinal after a shrink.** With Retain, scaling back up reattaches the old
volume rather than starting empty. That is usually what you want (the node rejoins with
its state and back-fills). If you intend the ordinal to come back *fresh*, delete its PVC
before scaling up.

## Seed lists: automatic on Kubernetes, yours everywhere else

On Kubernetes the chart and the operator derive each joiner's seeds from stable ordinals
(pod-0, pod-1), which is safe because a StatefulSet keeps ordinals stable and removes the
**highest** first on scale-down — so a seed target cannot be decommissioned out from under
a joiner, and a joiner retries its seeds every protocol period until one answers.

Anywhere else — bare metal, compose, systemd — `MQTTD_SWIM_SEEDS` is **yours to maintain**.
If you decommission a node that other nodes name as a seed, update their seed lists;
nothing rewrites them at runtime. A joiner whose every seed is gone will retry forever
without joining.

## The founder rule (read before touching pod-0's storage)

Pod-0 renders with **no seeds**: a pod-0 that starts with an *empty data dir and no
seeds* founds a **new** lease group. With its PV intact this never happens (it knows it
is initialized and rejoins). But **never delete pod-0's PVC while a cluster is live** —
a fresh, seedless pod-0 would found a second cluster beside the survivors (split brain).

If pod-0's volume is lost, the broker contains the damage itself: the re-founded pod-0
is alone and hears the surviving cluster, so it self-quarantines (above) within about a
second of its gossip socket binding — before it could pass readiness — and never enters
the client Service. This holds across restarts, including the operator's fence deleting
and recreating it. Recovery is then the
ordinary replace motion — wipe its data dir and give it seeds so it JOINS
([ADR 0043](adr/0043-elastic-cluster-resize.md)) and back-fills from the survivors.

> **Note (2026-08-06):** an earlier version of this runbook said to give ordinal 0 seeds
> with `helm upgrade --set-string` overriding `config`. That could not work: the
> ordinal-0 "no seeds" decision lives in the **init-container script**, not in the
> `config` blob, so overriding `config` cannot reach it. Rendering a pod-0 that joins
> instead of founding is tracked as ADR 0055 T9; until it lands, recovery is the
> self-quarantine above plus the manual wipe-and-rejoin.

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

## Monitoring for the operator (and humans)

The signals the future controller will reconcile on — equally useful today as alert
rules ([ADR 0054](adr/0054-operator-facing-state-surface.md); Grafana panels ship in
the demo dashboard's "Operator signals" row):

| Condition | Rule | Action |
|---|---|---|
| **Split brain** | `count(count by (cluster_id) (mqttd_cluster_info == 1)) > 1` across the fleet | Fence the new founder (see split-brain detection above) |
| **Unexpected founding** | `increase(mqttd_foundings_total[1h]) > 0` after day one | Same — a node founded a second cluster |
| **Foreign gossip arriving** | `rate(mqttd_gossip_rejected_total{reason="cluster-mismatch"}[5m]) > 0` | Contained, but find and fix the re-founded node |
| **Node self-quarantined** | `mqttd_refound_quarantine == 1` | This node re-founded beside a live cluster and took itself out of rotation; it never recovers on its own — wipe and rejoin it (see the founder rule) |
| **Brownout** | `mqttd_brownout == 1` (page); `sum(mqttd_store_bytes) / mqttd_store_max_bytes > 0.8` (warn) | Expand the PVC / raise the watermark / prune retained |
| **Stuck drain** | `mqttd_decommission_state == 1` and `mqttd_decommission_pending` not decreasing for 10m | Inspect the drain logs; the grace deadline will fall back to crash semantics |
| **Replication lag** | `mqttd_replica_groups_tracked - mqttd_replica_groups_current > 0` sustained | Node not catch-up-current; takeover from it would be degraded |
| **Quorum thinning** | `mqttd_voters < 3` (with `lease_voters = 5`) | One more loss risks durable writes; restore nodes |
| **Prolonged rotation window** | `mqttd_swim_keys_accepted > 1` for > 1h | A rotation phase was never closed (see key rotation above) |
| **Config divergence** | `count(count by (checksum) (mqttd_config_info == 1)) > 1` for > 15m | A config roll did not converge; check the stuck pod |
| **Degraded durable plane** | `mqttd_lease_quorum_ack_ms` growing (ADR 0049) | fsync-bound consensus; check disks before sessions are refused |

`curl <pod>:8080/statusz` is the human-readable superset of all of it.

## Bare-metal equivalents

Same procedures, different transport: rotation = replace the files (the watcher path
is identical) or `kill -HUP`; decommission = `mqttd --decommission --timeout <secs>`
or `kill -USR1` + wait for exit; the founder rule applies to whichever node you
bootstrap with empty seeds.
