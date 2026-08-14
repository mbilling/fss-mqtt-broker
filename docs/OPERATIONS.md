# Operations — day-2 procedures (Kubernetes-first)

The Helm chart (`deploy/helm/mqttd`, [ADR 0047](adr/0047-kubernetes-deployment.md))
encodes the deployment contracts: StatefulSet with per-pod volumes, decommission-drain
on scale-down, one-at-a-time rolls, a PodDisruptionBudget, and `--check-config` before
serving. This page is the rest: the procedures an operator runs *after* day 1. Signals
and files are the control surface — there is deliberately no admin API
([README](../README.md#principles)).

## Certificate / ACL / CRL rotation — automatic

The chart sets `[runtime] config_watch_secs = 30`: the broker polls the mounted policy
files (TLS cert/key/client-CA, ACL, CRLs, password/JWT files, and the cluster-bus
CA/cert/key — [ADR 0033](adr/0033-config-file-watch-reload.md))
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

## Cluster-bus certificates — one per node, and why that is not negotiable

The inter-node bus (`secrets.peerTls`) is **mutually authenticated per node**, so its
Secret holds `ca.crt` plus **one leaf per pod**, keyed by pod name (`<pod>.crt` /
`<pod>.key`). The chart hands each pod its own by path
(`MQTTD_PEER_TLS_CERT=/etc/mqttd/cluster/$(POD_NAME).crt`), which is what makes a per-pod
identity possible at all: a StatefulSet pod template cannot select a different Secret per
ordinal, but every pod can select a different *key* inside one Secret.

A single shared certificate cannot work, however it is minted (issue #262). Four rules,
three of which fail at runtime rather than at issue time:

1. **CN == node id == pod name.** A peer may only claim the id its certificate attests to
   ([ADR 0004](adr/0004-authentication-authorization.md)); a mismatch logs `peer Hello node
   id does not match its certificate Common Name` and drops the link.
2. **A SAN covering that pod's advertise host** —
   `<pod>.<release>-mqttd-headless.<ns>.svc.cluster.local`. That is the name a dialing peer
   verifies, and rustls checks SANs only: rule 1 satisfies nothing here.
3. **Both `serverAuth` and `clientAuth` EKUs** — every node dials and is dialed.
4. **An ECDSA P-256/P-384 or Ed25519 PKCS#8 key, never RSA.** The same key is the per-node
   gossip signing key ([ADR 0022](adr/0022-signed-gossip.md)), which accepts nothing else.

`deploy/helm/mqttd/bootstrap.sh` mints all of this and **verifies every property it
prints** before installing anything; the CA private key stays on your workstation and never
enters the cluster. For production, cert-manager's csi-driver is the better path — it is the
only option that also keeps each pod to its own key, and the chart supports it directly:
mount the csi ephemeral volume via `extraVolumes`/`extraVolumeMounts` and set
`secrets.peerTls.dir` to its mountPath (the chart then derives `MQTTD_PEER_TLS_*` from the
csi layout `tls.crt`/`tls.key`/`ca.crt`; the annotated example is in `values.yaml`). The
attributes that satisfy the bus's rules: `common-name: "${POD_NAME}"`,
`key-algorithm: ECDSA`, `key-encoding: PKCS8`, `key-usages: "server auth,client auth"`.

**Symptoms.** Pods running but never Ready, with `does not match its certificate Common
Name` in the logs → a leaf's CN is not its pod name. A startup failure naming the *gossip
signing key* → the key is RSA. `INSECURE: starting PLAINTEXT peer listener` while a
peerTls Secret is set → the paths are not reaching the broker (the chart derives them; a
hand-rolled `extraEnv` override may be shadowing them). A pod stuck in `Init:Error` whose
init container says `no cluster-bus certificate for <pod>` → see Scaling, below.

**Rotation is hot** (issue #269): the peer-bus CA/cert/key are in the broker's file-watch
scope, and the per-node gossip signing identity — the same key file — is rebuilt in the
same validate-before-swap reload. Replace the files (or let cert-manager renew them) and,
within a watch tick, the rotated leaf is served on the next peer handshake **and** signs
(and is embedded in) the next outgoing gossip datagram. Mid-rotation is safe one node at a
time: verification is per-datagram against the CA, so an already-rotated node and a
not-yet-rotated one accept each other's gossip. `SIGHUP` triggers the same reload where the
watcher is off; a rolling restart still works and remains the recovery path if a node's
mounted material is wrong. Revocation is hot too: a revoked peer's established link is torn
down when the cluster CRL (`MQTTD_PEER_TLS_CRL`) lands. Note the *shared gossip HMAC key*
(`swim.key_file`) is a different procedure — see the next section; and rotating the **CA
itself** is not hot (the gossip verifier pins the startup CA): plan a rolling restart for a
CA change.

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

- **Up — mint the new nodes' certificates FIRST if the cluster bus is on.** A new ordinal is
  a new node id and a new DNS name, so it needs its own leaf (see *Cluster-bus certificates*
  above). Run `REPLICAS=<n> ./deploy/helm/mqttd/bootstrap.sh` before raising the count: it
  re-verifies and keeps the existing CA, leaves and gossip key, mints only the new ordinals,
  and re-applies the Secret. A pod whose ordinal has no leaf fails its **init container**
  with a message naming that command, so the rollout stalls visibly at that ordinal instead
  of crash-looping obscurely — and, with `OrderedReady`, no higher ordinal is created.
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
- **Down and the write floor:** a *consented* decommission shrinks the quorum-committed
  durable roster, so the derived `durable.min_replicas` floor follows it down by itself —
  the common 3→2 and 5→3 shrinks need no action, and neither does 3→1 once the drain has
  committed the 1-member roster. Two cases still need explicit consent, and for both the
  knob is the same one:
  - a shrink to a **single** member while the node still declares
    `ready_min_members >= 2` — `declared` is a lower bound on the witness, so the floor
    stays at 2;
  - an **unconsented** loss with no drain behind it: two of three nodes gone for good, an
    AZ loss, or a DR restore of one node's `data_dir`. Here the committed roster still
    names three members and arms the floor **by itself**, so lowering
    `ready_min_members` changes nothing.

  In both cases the remedy is `durable.min_replicas = 1` on the surviving node. It is a
  `[durable]` edit and `durable` is restart-scoped, so a SIGHUP reload does **not** apply
  it (it logs a requires-RESTART warning with `sections=durable` and keeps the running
  value) — restart the node. Setting it means accepting single-copy durable acks, and the
  broker logs a warning saying so on every start while `ready_min_members >= 2`. Full
  symptom-to-remedy path: [TROUBLESHOOTING](TROUBLESHOOTING.md).
- **PVCs:** the chart ships `persistence.retentionPolicy.whenScaled: Retain` (since
  issue #97), so a scale-down leaves the departed pod's PVC behind **deliberately** —
  see "That orphan is deliberate" below for why (`Delete` applied uniformly erases
  everything at `--replicas=0`), and for when flipping it to `Delete` is reasonable.
  If you keep `Retain`, delete the orphaned PVC by hand once the scale-down is
  permanent — **before** any later scale-up re-creates the ordinal, so the new pod
  starts clean instead of over the drained node's stale data dir.
  Deleting the whole release keeps PVCs (`whenDeleted: Retain`).

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
compatibility is promised; post-1.0: adjacent releases negotiate). A roll still pays
the decommission drain on every pod (the `preStop` hook cannot tell a roll from a
shrink — the recorded operator-shaped follow-up, ADR 0047's 2026-08-04 amendment), but
the cost is **measured, per-pod-local, and not a fleet-wide reconnect storm**
(issue #248). `crates/mqttd/tests/roll_cost.rs` drives the chart's exact motion
(SIGUSR1 drain → exit → restart over the same PV → rejoin) on every PR and prints
these numbers; the nightly upgrade suite prints the same per-roll figures under
two-binary skew.

**What one rolled pod costs** (measured on a 3-node cluster under acked `QoS` 1
load; the client-facing cost scales with the rolled pod's share, not the fleet):

- **Reconnects: the sessions the pod hosted — nothing else.** Its
  directly-connected clients plus the clients other nodes relocated to it at
  CONNECT time ([ADR 0005](adr/0005-session-affinity.md)) — ≈ 1/N of the fleet
  under even placement (measured: 4 of 9 clients when 1 of 3 nodes rolls; the
  other clients kept their TCP connections through the whole roll and kept
  receiving). A full N-pod roll therefore reconnects each client roughly **once,
  spread across the roll** — not N fleet-wide storms.
- **Drain-to-exit: sub-second when replicas are caught up** (measured 0.3–0.4 s).
  The drain *verifies* rather than copies ([ADR 0043](adr/0043-elastic-cluster-resize.md)),
  so it scales with replica lag, not data size — budget
  `terminationGracePeriodSeconds` for the lagging-replica worst case.
- **Restart-to-readmission: seconds** (measured 5–11 s to full membership + a
  ready lease group on every node).
- **`QoS` 1 publishes into a moving group are refused, never silently dropped:**
  acks are withheld for a seconds-scale ownership-takeover window (measured
  worst ≈ 5 s mid-roll) and publishers retry; everything acked is delivered.
- **The straggler wart (issue #284):** a client that *resumes* in the seconds
  around the rolled pod's readmission can be routed onto a stale owner and
  receive nothing — acks toward it refused — until its keepalive fires and it
  reconnects once more (measured: 1 of 9). Until #284 lands, keep client
  reconnect backoff + jitter at least as long as the readmission time above, so
  stragglers resume after placement settles.

**Pacing.** The chart already enforces the safe motion — OrderedReady +
RollingUpdate roll one pod at a time and the PDB (`maxUnavailable: 1`) stops a
node drain from taking two. Keep it that way (never parallelize a roll), let each
pod reach Ready before the next (the StatefulSet controller does this for you),
and for very large fleets budget the LB and auth path for ≈ fleet/N reconnects
per rolled pod rather than the whole fleet.

A one-at-a-time roll does **not** trip the min-replicas write floor: with two of three
members live the replica set holds two copies and the write quorum already required two
acks, so the default (derived) floor of 2 refuses nothing. The floor is capped at the
replication factor, so this holds in wider topologies too — on 5 or 7 nodes it is still 2.
See the monitoring table's *Durable writes refused* row for the condition that would trip
it, and [TROUBLESHOOTING](TROUBLESHOOTING.md) for the symptom and the remedy.

**Rolling back across the write-floor change.** `durable.min_replicas` widened from an
integer to integer-or-`"majority"`, and `docs/mqttd.example.toml` ships the word form. A
config carrying `min_replicas = "majority"` is a **type mismatch** for the previous
release, and `runtime.config_unknown_keys = "warn"` does not rescue type mismatches
([ADR 0058](adr/0058-one-dot-zero-stability-contract.md) §E). If a config must stay readable by the
release you might roll back to, spell the floor as an integer or omit the key entirely —
omitting it takes the same derived default.

## Running the operator (optional)

Everything on this page works with the plain chart — the operator
([ADR 0055](adr/0055-kubernetes-operator.md)) is the *optional* reconciler for the parts
that are multi-step reactions to observed state: split-brain **fencing**, brownout
**PVC expansion** (with the watermark following the volume), and continuous
status/conditions (`kubectl get mqc`) from the same `/statusz` you would read by hand.
Every destructive remediation is **opt-in per cluster and alert-only by default**, and
no remediation can delete data — the operator's RBAC has no PVC `delete` verb, and the
nightly e2e asserts that with `kubectl auth can-i`.

**Install** (one release per namespace that runs brokers — the RBAC and the watch are
namespaced by design):

```sh
helm install mqttd-operator deploy/helm/mqttd-operator -n <ns>
kubectl -n <ns> apply -f deploy/helm/mqttd-operator/example-mqttdcluster.yaml
```

The operator image is cut by the same release pipeline as the broker — signed
(cosign, keyless), SBOM-attested, reproducible ([RELEASING](RELEASING.md)) — and the
chart forward-pins the first release that publishes it (`v0.9.1`, the same
gate-proven pin as the compose default; until that tag is pushed the image is not on
GHCR). Render parity between the operator and the chart is a per-PR CI gate: both
produce the same objects for equivalent inputs, so switching paths is not a migration.

**Upgrading:** `helm upgrade` updates the operator, but Helm never upgrades CRDs it
installed from `crds/` — after upgrading, apply the CRD for the new version yourself:
`kubectl apply -f deploy/crds/mqttd.io_mqttdclusters.json` (from the release's tag).

**CRD stability posture:** `mqttd.io/v1alpha1`. The schema is pinned in CI — a golden
test regenerates it from the operator's own Rust types and fails on any drift, and the
chart's packaged copy must be byte-identical — so within a release the installed schema
is exactly the tested one. Pre-1.0 the schema may still change **between** releases
(`v1alpha1` means exactly that); changes are called out in release notes, and the
chart-only path remains the stability-conservative choice until the CRD graduates.

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
| **Brownout** | `mqttd_brownout == 1` (page); `sum(mqttd_store_bytes) / mqttd_store_max_bytes > 0.8 and mqttd_store_max_bytes > 0` (warn); `max by (store) (mqttd_store_bytes) / scalar(mqttd_store_max_bytes) > 0.6 and on() mqttd_store_max_bytes > 0` (warn) | Expand the PVC / raise the watermark / prune retained. The per-store rule finds which store is eating the budget — the mark is **aggregate on purpose** (`replicas`/`lease` grow from peers' committed appends and from consensus, with no client write to refuse), and the broker itself warns once, naming the store, above 70% of the mark. **Every watermark ratio here needs its guard clause:** an unset mark is exported as a literal `0`, so a bare divide is `+Inf` and fires on the default configuration, and a per-store numerator carries a `store` label the mark does not, so it needs `scalar()` or it matches nothing and never fires. Timing: a transition is seen within `MQTTD_WATERMARK_POLL` seconds (default 10) and within `max(1s, poll/10)` once inside 10% of the mark — which is also how long a *cleared* brownout takes to lift, i.e. how long the publish refusals outlive the pressure |
| **Memory pressure short of brownout** | `mqttd_process_resident_bytes / mqttd_memory_max_bytes > 0.9 and mqttd_memory_max_bytes > 0` for 5m (warn) | The last warning before the memory axis browns out. The **container/cgroup limit is the ceiling**, not this watermark: check one is actually set (the Helm chart ships `resources: {}`) and that the watermark is 75-85% of it — the gap is the overshoot allowance (`poll x allocation rate`), see [SIZING](SIZING.md) |
| **Publishers being refused** | `rate(mqttd_quota_rejections_total{reason="brownout-publish"}[5m]) > 0` (the Prometheus label is `reason`; the OTel attribute is `kind`) | `QoS` ≥ 1 publish availability is degraded, not silently lost: above the watermark a publish needing a durable append is refused (v5 `0x97`, v3.1.1 no ack + close — cross-node too, as a peer-bus verdict; an older link mid-rolling-upgrade degrades to a withheld ack + close). Re-delivery is the publishing application's decision — a v5 reason ≥ `0x80` completes the packet-id lifecycle, and only a `CleanSession=0` v3.1.1 publisher resends on reconnect. Expand the PVC / raise the watermark / prune retained / let subscribers drain. `mqttd_brownout{axis}` plus `store_bytes` vs `store_max_bytes` and `process_resident_bytes` vs `memory_max_bytes` say which axis; `/statusz` gives the onset timestamp |
| **Stuck drain** | `mqttd_decommission_state == 1` and `mqttd_decommission_pending` not decreasing for 10m | Inspect the drain logs; the grace deadline will fall back to crash semantics |
| **Replication lag** | `mqttd_replica_groups_tracked - mqttd_replica_groups_current > 0` sustained | Node not catch-up-current; takeover from it would be degraded |
| **Quorum thinning** | `mqttd_voters < 3` (with `lease_voters = 5`) | One more loss risks durable writes; restore nodes |
| **Durable writes refused (under-replicated)** | `mqttd_replication_min_actual < mqttd_replication_write_floor` (page); `mqttd_replication_min_actual < mqttd_replication_desired` (warn) | **Page**: a group is below the min-replicas write floor, so durable writes are being REFUSED — QoS≥1 publishers get no ack, redeliver, and are disconnected; retained mutations queue; reads, QoS 0, acked-driven truncation and removal keep serving, but QoS 2 in-flight bookkeeping does not. Corroborate with `mqttd_durable_append_failures_total{reason="unavailable"}` climbing. **Warn**: a group merely holds fewer copies than R. Either way: restore the missing members ([TROUBLESHOOTING](TROUBLESHOOTING.md)). Do **not** lower `durable.min_replicas` to silence it unless you are consciously accepting single-copy acks. Non-durable clusters (`durable.enabled = false`) report a floor of 1, so this rule cannot fire there |
| **Prolonged rotation window** | `mqttd_swim_keys_accepted > 1` for > 1h | A rotation phase was never closed (see key rotation above) |
| **Config divergence** | `count(count by (checksum) (mqttd_config_info == 1)) > 1` for > 15m | A config roll did not converge; check the stuck pod |
| **Degraded durable plane** | `mqttd_lease_quorum_ack_ms` growing (ADR 0049) | fsync-bound consensus; check disks before sessions are refused |
| **Hub loop held** | `histogram_quantile(0.99, rate(mqttd_hub_dispatch_seconds_bucket[5m])) > 0.1` sustained 5m (page) | Something is blocking the single-threaded hub loop again — every client on the node queues behind it (the head-of-line failure issue #242 removed; the `command` label says which class). Since ADR 0061 the publish path's durable appends, outbound-id records, and packet-id reservations all run off-loop, so a **publish-class tail means an inline await regressed** (the one documented exception: the backlog-overflow eviction truncate, reachable only past a 10 000-entry per-session backlog); an **ack-class tail** is the documented residual — `truncate_acked`, QoS 2 phase advances (`advance_outbound`), and `clear_outbound` still run on-loop against a degraded store; an **attach-class tail** means replay reads/truncates are degraded — check `mqttd_durable_append_latency_seconds` and the durable-plane rows above |
| **Append lane saturating** | `mqttd_append_lane_jobs` growing sustained (warn); `rate(mqttd_publish_dropped_total{reason="append-backlog-full"}[5m]) > 0` (page) | A session's placement group is not keeping up (degraded follower set: each append or QoS 2 outbound-id record is bounded by the 5s replication RPC timeout, FIFO per session — 256 queued jobs max per session, then the NEWEST publish is withheld so its publisher retries; a detach spill past the cap+headroom sheds into this same counter). Only that group's sessions are affected — connects, subscribes and other groups' publishes keep flowing (issue #242). The degraded-group signals are per-session ones: this gauge/counter pair, `rate(mqttd_publish_dropped_total{reason="outbound-id-write-failed"}[5m])` (a QoS 2 outbound-id record write failed; the delivery is re-queued and retried on the next drain), and end-to-end QoS 2 delivery latency to that group's subscribers — NOT hub dispatch tails, which stay flat by design. Find the degraded group's followers: `mqttd_replica_groups_tracked - mqttd_replica_groups_current`, `mqttd_durable_append_failures_total`, and the *Durable writes refused* row |

`curl <pod>:8080/statusz` is the human-readable superset of all of it.

## Bare-metal equivalents

Same procedures, different transport: rotation = replace the files (the watcher path
is identical) or `kill -HUP`; decommission = `mqttd --decommission --timeout <secs>`
or `kill -USR1` + wait for exit; the founder rule applies to whichever node you
bootstrap with empty seeds.

Standing a secured cluster up *outside* Kubernetes in the first place — three nodes
with client TLS, the mutually-authenticated bus, a gossip key, deny-by-default ACLs
and majority-aware readiness, plus how the starter PKI maps to a real CA — is the
[secured three-node tutorial](SECURED-CLUSTER-TUTORIAL.md), built on
[`deploy/compose/`](../deploy/compose/) and CI-exercised by
`scripts/compose-smoke.sh`.
