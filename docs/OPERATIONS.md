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

**Verify:** `mqttd_security_reloads_total{trigger="watch"}` increments; the reload is
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
  worst ≈ 5 s mid-roll) and publishers retry. The mechanism is the *withheld* ack,
  not a promise about acks already given: while a node cannot durably append for a
  group, it answers with silence and the publisher resends (see the rehome bullet
  below for the live-session case).
  **One window is NOT covered, and it is pre-existing:** a node releases a moved
  session's routing when it observes that *it* is no longer the owner — never on
  evidence that the new owner has materialised the session. Between those two facts
  (an inherited-session scan on the old node, then the new owner claiming the
  session) a publish can match nobody anywhere and be acked with nothing stored.
  That is the same hole for a session that was already offline as for one this
  release closes, it long predates issue #284, and closing it needs a
  witnessed-release protocol that is deliberately NOT part of this change (issue
  filed; the reasoning is in ADR 0043's as-delivered note). Do not read this bullet
  as "an `Accepted` always means stored" during an ownership move.
- **A straggler that resumes into the readmission window is rehomed, promptly**
  (issue #284, delivered): a client that resumes in the seconds around the rolled
  pod's readmission can be placed on the group's *interim* owner — the ring hands
  the readmitted pod its groups back a couple of seconds after it turns Ready, and
  relocation is decided once, at CONNECT. That pod now **closes the connection
  itself** within a second or two of the ownership move (v5 clients get DISCONNECT
  `0x9C` Use another server), so the client relocates on its next CONNECT rather
  than waiting out a keepalive on dead air. Measured on the three-node roll harness
  (`roll_cost`, three runs, 2026-08-15): 1 of 9 clients rehomed per roll, 0 clients
  left to discover the problem themselves, worst post-roll `QoS` 1 ack stall **0.2 s**
  (0.1-0.2 s across four runs; it was 26 s before the fix, with no self-heal at all if the client never
  reconnected). Watch `mqttd_session_rehomes_total{reason="stale-owner"}`; a handful
  per roll is normal.
- **The close touches the connection and NOTHING else, so no publish is acked into
  the void.** The closing node keeps routing — and keeps advertising — the session's
  subscriptions afterwards, exactly as it did before the close, until its own
  inherited-session scan releases them on the pre-existing cadence. While it holds
  them, a `QoS` ≥ 1 publish toward that session is answered exactly as before the
  fix: the durable append is refused (`not the owning node for this group`) and the
  **publisher's ack is withheld**, so it retries. That holds at every entry point,
  including a publisher on a third node, where the fan-out reaches both nodes and the
  old node's failure withholds the ack even if the new owner's copy was stored first.
  **The cost, stated:** for as long as the old node holds the routing, *both* nodes
  advertise the session's filters, so publishers to those filters keep retrying even
  once the session is healthy on its owner. Inside a roll that is **about two seconds** —
  measured 1.8-2.4 s on the three-node roll harness (reconnect the closed client
  immediately, then publish toward its topic from a third node until the ack stops
  being withheld); the readmission's membership change arms eager scans on every
  node, which is what keeps it seconds rather than the 30 s cadence below. For a lease move
  with **no** membership change (an assigner rebalance, a lease-leader change, or a
  paced elastic-resize drain) it is up to the 30 s reconcile cadence. Releasing sooner
  was considered and rejected: the release is not witnessed by the new owner, so
  accelerating it widens a window in which a publish is *acked* with nothing stored —
  a bounded lie in place of an unbounded honest refusal.
- **Each rehome close publishes the session's Last Will.** A server DISCONNECT is
  not a client DISCONNECT, so the spec keeps the will armed ([MQTT-3.1.2-8],
  §3.14.4) — consistent with session takeover and `evict`, and with the fix for
  issue #265. **A roll therefore emits one LWT per rehomed session, and a
  scale-out/scale-in emits roughly one per moved session** (paced: at most 32
  closes per node per second, `mqttd_session_rehomes_total{reason="deferred"}`
  counts each session that had to wait, once). Suppress device-offline alerting while
  `mqttd_session_rehomes_total{reason="stale-owner"}` is climbing; treat that
  counter as the suppressor signal. Honouring the MQTT 5 Will Delay Interval
  cluster-wide is the follow-up that would remove the false event — it needs the
  delay and its cancellation to survive the client reconnecting on a *different*
  node, which no peer frame expresses today.

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

## Backup and disaster recovery

Quorum replication is the primary recovery story, and it is a good one: a lost node's
state rebuilds from survivors (wipe-and-rejoin). It is a **durability** story, not a
**backup** story — it does not protect you from operator error, a bad migration, or
correlated corruption, because every replica faithfully replicates the mistake. That is
what backups are for ([ADR 0062](adr/0062-online-backup-and-restore.md), issue #249).

### What the online export guarantees — and what it does not

An export is taken from the **live** node. Nothing is stopped, no client is disconnected,
and no second store handle is opened. It is **not** an instant, and the difference matters
when you plan a recovery. The guarantee, in one sentence:

> Every fact durably committed before the export's `started_unix_ms` is present; facts
> committed inside `[started_unix_ms, finished_unix_ms]` may or may not be; facts
> committed after `finished_unix_ms` are not.

Both instants are written into every file's trailer, so the window is a number you can
read, not a promise you have to trust. It is **one-directional**: the uncertainty is only
ever "a fact from inside the window may be missing", never "a fact that was never committed
is present". Within the window two stronger properties hold: retained messages are one
atomic whole-store snapshot, and each session is a self-consistent record whose queue and
metadata cannot skew in the dangerous direction (worst case is a redelivery that MQTT
already permits, never a reused packet id). There is **no cross-store atomic cut and no
cluster-wide instant** — the ADR says why, from the code.

**Three sentences that bound the whole feature:**

- **A per-node export is one node's readable state** — the sessions this node owns, plus
  its view of retained state. In a cluster a foreign session answers `NotOwner`, so no node
  can export the whole cluster, and the file name carries the node id.
- **A cluster backup is the SET of every node's export**, taken close together. Each node
  writes into its trailer the client ids it skipped as owned-elsewhere, and into its header
  the members it could see, so the set's completeness is *checked* at restore time rather
  than left to your bookkeeping.
- **A restore rebuilds DATA, not identity and not consensus.** Sessions (with their
  subscriptions, owner binding, offline queues, packet-id high-water and both QoS-2
  windows) and retained values come back. Cluster id, node ids, the lease/Raft log and
  `replicas.redb` do not: the target cluster keeps its own identity and elects its own
  leaders, and each node imports the slice the *new* ring gives it.

### Configuration

```toml
[backup]
dir = "/var/backups/mqttd"   # MQTTD_BACKUP_DIR — a volume SEPARATE from node.data_dir
every_secs = 3600            # MQTTD_BACKUP_EVERY — 0 (default) = on demand only
keep = 7                     # MQTTD_BACKUP_KEEP — kept per node id
```

`--check-config` refuses three shapes: a `backup.dir` inside `node.data_dir` (exports there
would grow the volume the disk watermark protects, counted by nothing), `every_secs > 0`
with no `dir`, and `keep = 0`. It validates the *setting*, **not the volume** — a
nonexistent or unwritable `backup.dir` passes `--check-config` and fails at the first run,
with the path and the OS error in the log, `/statusz`'s `backup.last_error`, and
`mqttd_backup_runs_total{outcome="error"}`. Take one backup by hand after a config change
rather than discovering it at 03:00.

Files are written `0600`, fsynced, then renamed into place as
`mqttd-backup-<node-id>-<YYYY-MM-DD_HHMMSS>-<mmm>.ndjson` (UTC, milliseconds included so two
exports in one second are two files); a `.ndjson.partial` file is an interrupted run, never
read by a restore and never counted by retention. The name is sortable, but a **restore
orders by the header's `created_unix_ms`, never by the file name** — renaming a file changes
nothing.

#### The backup directory on the surfaces this repo ships

Neither shipped deployment surface mounts one by default — **it is an opt-in you add**, and
both need a writable path *outside* `node.data_dir`:

**Helm.** The chart ships no backup volume and no `MQTTD_BACKUP_*` plumbing, so on a default
install there is nowhere to write and `mqttd --backup` exits `2`. Add it through the
chart's existing extension points (verified with `helm template`); a separate PVC, because
an `emptyDir` would put the backup on the pod it is protecting:

```yaml
# values.yaml — pair this with a ReadWriteMany PVC named mqttd-backups
extraEnv:
  - name: MQTTD_BACKUP_DIR
    value: /var/backups/mqttd
  - name: MQTTD_BACKUP_EVERY
    value: "3600"
extraVolumes:
  - name: backups
    persistentVolumeClaim:
      claimName: mqttd-backups
extraVolumeMounts:
  - name: backups
    mountPath: /var/backups/mqttd
```

`readOnlyRootFilesystem: true` does not block this: it makes the *root* filesystem
read-only, and a mounted volume stays writable (the data PVC works the same way). Note the
`check-config` init container is not given `extraEnv`, so these settings are validated by
the broker at startup rather than by the init container.

**systemd.** `deploy/systemd/mqttd.service` is `ProtectSystem=strict` with
`ReadWritePaths=/var/lib/mqttd` only, so the example `dir = "/var/backups/mqttd"` is
unwritable there by construction. Grant it with a drop-in rather than editing the shipped
unit:

```sh
install -d -o mqttd -g mqttd -m 0700 /var/backups/mqttd
systemctl edit mqttd            # writes /etc/systemd/system/mqttd.service.d/override.conf
# [Service]
# ReadWritePaths=/var/backups/mqttd
systemctl daemon-reload && systemctl restart mqttd
```

Then add `MQTTD_BACKUP_DIR=/var/backups/mqttd` to `/etc/mqttd/mqttd.env`. Keep the path off
the data volume — that is what `--check-config` enforces — and remember the export is
plaintext data-plane content (see the gap list below).

### Taking a backup

Run it **on every node**, ideally close together:

```sh
# In the container (distroless: no shell needed, this IS the entrypoint binary).
# --config is REQUIRED wherever the broker's own settings live in a file rather than in the
# environment — the Helm chart renders node.data_dir into /config/mqttd.toml, and without
# it `--backup` cannot load an effective config and exits 2.
kubectl exec mqttd-0 -- mqttd --backup --config /config/mqttd.toml
```

`--backup` signals the broker (`SIGUSR2`, `--pid` default `1`) and waits for a new file to
appear under `[backup] dir` (`--timeout` default `3600` seconds). It prints the path it
wrote and exits `0`; `1` on timeout; `2` on a usage error, a config that will not load, no
`[backup] dir`, or a pid it cannot signal. A schedule (`every_secs`) does the same thing on
a timer. Copy the directory off the node's volume — an export sitting on the disk you are
protecting against is not a backup.

**`SIGUSR2` is safe on a node with no `[backup] dir`.** The handler is installed
unconditionally at startup, so a monitoring or cron rollout that lands before the config
does logs `SIGUSR2 received … but no [backup] dir is configured` and keeps serving, instead
of terminating the broker (which is `SIGUSR2`'s default disposition).

### RPO and RTO

Both are formulas whose terms this repository measures:
[docs/benchmarks/BACKUP-RESTORE.md](benchmarks/BACKUP-RESTORE.md) (development-grade, one
host — read its preamble).

- **RPO ≤ `every_secs` + W**, where `W` is the export's own window width. Measured:
  **W = 51 ms for a 21,000-record node** (1,000 sessions × 10 queued + 10,000 retained, 256 B
  payloads, release build, one developer machine), so at any sane schedule the RPO *is* the
  schedule. Every run records its own `W`: `finished_unix_ms − started_unix_ms` in the file's
  trailer, and `backup.window_ms` on `/statusz`. `mqttd_backup_duration_ms` is the **whole
  run's** wall clock — the reads *plus* the write, fsync and rename — so it is an upper
  bound on `W`, not `W` itself; alert on it if you want a single series, and read the
  trailer when you want the exact number.
- **RTO ≈ fresh-cluster start + records / durable-write rate.** The second term dominates:
  every restored record is one fsync (plus a quorum round-trip in cluster mode), and there
  is deliberately no batch path. Measured on that host: **162–173 records/s single-node**
  over two consecutive runs, i.e. ~2 min for 21,000 records and ~10 min for 100,000. Treat
  the *shape* as the guidance and the constant as yours: an earlier session on the same host
  measured 74 records/s at the same fixture, and a different developer machine measured 3×
  again. Re-run the harness on your own volume. The record count is the trailer's
  `sessions + queued + retained`, summed over the set.

**Alert on the age of the last successful export, or the RPO is fiction:**

```promql
time() - mqttd_backup_last_success_timestamp_seconds > 2 * 3600
  and mqttd_backup_last_success_timestamp_seconds > 0
```

The `> 0` guard is not optional: a node with no backup configured exports a literal `0`,
so a bare comparison fires on every default installation and tells you nothing. A run that
fails deliberately does **not** advance that series (it increments
`mqttd_backup_runs_total{outcome="error"}` instead), so a partially-readable node shows up
as a stale backup rather than as a fresh lie.

### Restoring

A restore rebuilds **data** into a **fresh** cluster. It never merges into a serving one.

1. Stand up a new cluster with **empty** data dirs (no `sessions.redb`, `retained.redb`,
   `replicas.redb`, `lease.redb` — the node refuses otherwise).
2. Put **every** node's export in one directory, reachable by every node. **One cluster's
   exports only**: a set naming two `cluster_id`s is refused, but a directory holding only
   the *wrong* cluster's set restores that cluster — check a header's `cluster_id` before
   you point at it (`head -1 <file>`).
3. Set, on every node:
   ```
   MQTTD_RESTORE_FROM=/restore          # a file or a directory
   MQTTD_READY_MIN_MEMBERS=3            # = your node count, so the import waits for the
                                        #   assembled ring before placing sessions
   ```
4. Start the nodes. Each verifies the whole set first (format stamp, sha-256, one
   generation per node, a single cluster id, coverage), then waits for the durable plane,
   then imports the sessions **it** owns; the others are imported by their owners from the
   same files. `/readyz` reports `NotReady` with reason `restore-in-progress` and no client
   port is bound until the import finishes; `/statusz` carries a `restore` block, and
   `mqttd_restore_state` is `1` while it runs, `2` on success, `3` on failure. Any failure
   exits the process non-zero: a broker never starts on a half-imported store.
5. Check `mqttd_restore_state == 2` on every node, then let clients reconnect.

**Leave `MQTTD_RESTORE_FROM` in place afterwards.** A completed restore writes a
`restored-from` stamp (a JSON record of the source, the instant, the files, the set digest
and anything forfeited) into the data dir, and the node's next ordinary start — a
reschedule, an OOM kill, a rolling upgrade — reads it, reports
`backup.restore_from is INERT this boot`, and starts normally on the data it already holds.
Pointing an already-restored node at a *different* source is refused: that would be a merge.

**Several generations in one directory are fine.** `keep` defaults to 7, so the directory
you copied off the volume normally holds several exports per node. The restore selects the
**newest export of each node** by its header's `created_unix_ms`, logs the older ones as
`superseded`, and never merges two generations record by record. Two exports of one node
sharing a `created_unix_ms` are refused, naming both.

#### If a node's data AND its export are both gone

The coverage check refuses an incomplete set by default, which is right almost always — and
wrong in exactly one case: the disaster took a node's volume *and* the copy of its export,
so an all-or-nothing check would hold the surviving nodes' backups hostage to the one file
that no longer exists. The escape hatch says what it costs:

```
MQTTD_RESTORE_PARTIAL_ACCEPT_DATA_LOSS=1      # backup.restore_partial_accept_data_loss
```

Only `1`, `true`, `on` or `yes` enables it — a flag that forfeits data is not turned on by a
stray value. With it set the restore **imports the surviving nodes' data and FORFEITS
everything the missing nodes held**: their sessions, those sessions' queued messages, and
any retained topic no surviving node had cached. It warns at startup, warns again naming
every forfeited node and client id, records
`PARTIAL (data forfeited): …` in `/statusz`'s `restore.detail`, and writes `"partial": true`
with the forfeited names into the `restored-from` stamp permanently. Unset it once the
restore is done, so the next incident starts from the safe default.

#### What a restore refuses

Every refusal imports **nothing** and exits non-zero. The **data dir** is judged first,
before a file is opened: store files with no `restored-from` stamp to explain them, or a
stamp naming a different source. Then the **set**: a `format_version` newer than this build
(naming the build that wrote the file) or older ("no migration path exists pre-1.0"); two
exports of one node with the same `created_unix_ms`; a missing or malformed trailer, a
sha-256 that does not match the bytes, a trailer saying the export was incomplete, or an
unknown record kind; exports from two different clusters, naming both ids; and finally a
set missing a member's export or a session named as owned-elsewhere and present nowhere
(unless the partial opt-in above is set). An interrupted restore is **not** resumable —
start again on an empty data dir.

### Not covered by 1.0 (read this before you rely on it)

These are deliberate gaps, not oversights. Each has a reason in
[ADR 0062](adr/0062-online-backup-and-restore.md):

1. **Lease/Raft state (`lease.redb`) is never exported or restored.** It holds the
   persisted vote and log; re-injecting them is a consensus-safety violation, not a
   recovery. Lease-group recovery = rejoin from survivors, or found a fresh cluster and
   import.
2. **Cluster id and node id are provenance only** — recorded in the export so you can
   prove where it came from, never written back. Restoring them would manufacture a second
   cluster carrying a live cluster's identity. The consequence is that the *target* cluster
   is never verified against the backup's `cluster_id`: mixing two clusters in one
   directory is refused, but restoring the wrong cluster's complete set is not detectable.
3. **Replica copies (`replicas.redb`) are not exported.** A session another node owns is
   unreadable here; it is a coverage entry, not data.
4. **No cluster-wide consistent instant.** Per-node windows make the skew visible instead
   of asserting it away. Run the exports close together.
5. **A session that changed owner between two nodes' exports may be in neither** — the
   coverage check catches it and refuses the restore rather than losing it silently.
6. **No restore into a live or non-fresh node**, no selective per-session restore, no
   point-in-time recovery, no incremental/differential backup (every export is full).
7. **A partial restore is lossy by definition and is not a supported steady state.** It
   exists for the one disaster above; what it forfeits is named in the log and in the
   stamp, and nothing later reconciles it.
8. **The bridge spool is out of scope.** `mqttd-bridge` holds acked-but-unforwarded
   messages in its own redb spool; that is a different process's durable state.
9. **Non-durable state is not exported**: QoS 0 queues, live connection state, topic
   aliases, pending wills.
10. **Config, ACLs, PKI, passwords are not exported.** That is GitOps' job and
    `--check-config`'s gate.
11. **A node with no durable store refuses to export.** A file that looks like a backup of
    nothing is worse than an error.
12. **The export file is plaintext data-plane content** — every retained payload, every
    queued message, every client id and owner subject. It is created `0600`, and it is as
    sensitive as the broker's data volume: encrypt it at rest, restrict the directory, and
    treat a shared backup volume as a lateral-movement path.

### Rolling back across the `[backup]` section

`[backup]` is a **new config section**, so a config file carrying it is a config with an
unknown key for the previous release — and the default `runtime.config_unknown_keys =
"refuse"` fails that load. Unlike a type mismatch this one *is* rescuable: set
`config_unknown_keys = "warn"` (or `MQTTD_CONFIG_UNKNOWN_KEYS=warn`) on the release you
might roll back to, or configure backups through `MQTTD_BACKUP_*` env vars, which an older
binary simply ignores. Nothing on disk changes: no store schema version moves and the
export lives outside the data dir, so a rollback needs no data migration
([ADR 0062](adr/0062-online-backup-and-restore.md), [ADR 0058](adr/0058-one-dot-zero-stability-contract.md) §E).

### The byte-level path, still supported, as the complement

For what the online export deliberately does not cover — lease/Raft state, or an exact
image of one node — snapshot the volumes of a **stopped** node (redb files are only
crash-consistent, not application-consistent, while running) or use storage-class volume
snapshots; restore by recreating the StatefulSet over the restored PVs with the same pod
names. The cost is the reason it is no longer the only path: stopping a node means a full
decommission drain, whose measured per-pod cost is issue #248's, and during it the cluster
runs one replica short. Use it deliberately, not as routine DR.

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
| **Backup stale (RPO breached)** | `time() - mqttd_backup_last_success_timestamp_seconds > 2 * <every_secs> and mqttd_backup_last_success_timestamp_seconds > 0` (page) | No successful export in two schedule periods. The `> 0` guard is mandatory — an unconfigured backup exports a literal `0`. Check `mqttd_backup_runs_total{outcome="error"}` and the node's log: an INCOMPLETE session scan fails the run on purpose (a file missing sessions is worse than none), so the usual cause is a group that could not be read (no quorum) at export time |
| **Restore stuck or failed** | `mqttd_restore_state == 1` for longer than the expected RTO, or `== 3` (page) | `1` = importing (the node is `NotReady` and its client port is closed, by design); `3` = the restore was refused or failed and the process exited non-zero. `/statusz`'s `restore.detail` and the log carry the reason — a format stamp from another build, a digest mismatch, an uncovered session set, two clusters or two generations in one directory, or a data dir that is not fresh. `2` means completed, and reads `2` on **every later boot too**, because the `restored-from` stamp makes the setting inert; `restore.detail` then says `this boot imported nothing`. A detail beginning `PARTIAL (data forfeited)` means the set was incomplete and imported anyway under `MQTTD_RESTORE_PARTIAL_ACCEPT_DATA_LOSS` — the forfeited nodes and sessions are named there and in the stamp |
| **Memory pressure short of brownout** | `mqttd_process_resident_bytes / mqttd_memory_max_bytes > 0.9 and mqttd_memory_max_bytes > 0` for 5m (warn) | The last warning before the memory axis browns out. The **container/cgroup limit is the ceiling**, not this watermark: check one is actually set (the Helm chart ships `resources: {}`) and that the watermark is 75-85% of it — the gap is the overshoot allowance (`poll x allocation rate`), see [SIZING](SIZING.md) |
| **Publishers being refused** | `rate(mqttd_quota_rejections_total{reason="brownout-publish"}[5m]) > 0` (the Prometheus label is `reason`; the OTel attribute is `kind`) | `QoS` ≥ 1 publish availability is degraded, not silently lost: above the watermark a publish needing a durable append is refused (v5 `0x97`, v3.1.1 no ack + close — cross-node too, as a peer-bus verdict; an older link mid-rolling-upgrade degrades to a withheld ack + close). Re-delivery is the publishing application's decision — a v5 reason ≥ `0x80` completes the packet-id lifecycle, and only a `CleanSession=0` v3.1.1 publisher resends on reconnect. Expand the PVC / raise the watermark / prune retained / let subscribers drain. `mqttd_brownout{axis}` plus `store_bytes` vs `store_max_bytes` and `process_resident_bytes` vs `memory_max_bytes` say which axis; `/statusz` gives the onset timestamp |
| **Stuck drain** | `mqttd_decommission_state == 1` and `mqttd_decommission_pending` not decreasing for 10m | Inspect the drain logs; the grace deadline will fall back to crash semantics |
| **Replication lag** | `mqttd_replica_groups_tracked - mqttd_replica_groups_current > 0` sustained | Node not catch-up-current; takeover from it would be degraded |
| **Quorum thinning** | `mqttd_voters < 3` (with `lease_voters = 5`) | One more loss risks durable writes; restore nodes |
| **Durable writes refused (under-replicated)** | `mqttd_replication_min_actual < mqttd_replication_write_floor` (page); `mqttd_replication_min_actual < mqttd_replication_desired` (warn) | **Page**: a group is below the min-replicas write floor, so durable writes are being REFUSED — QoS≥1 publishers get no ack, redeliver, and are disconnected; retained mutations queue; reads, QoS 0, acked-driven truncation and removal keep serving, but QoS 2 in-flight bookkeeping does not. Corroborate with `mqttd_durable_append_failures_total{reason="unavailable"}` climbing. **Warn**: a group merely holds fewer copies than R. Either way: restore the missing members ([TROUBLESHOOTING](TROUBLESHOOTING.md)). Do **not** lower `durable.min_replicas` to silence it unless you are consciously accepting single-copy acks. Non-durable clusters (`durable.enabled = false`) report a floor of 1, so this rule cannot fire there |
| **Sessions rehoming** | `rate(mqttd_session_rehomes_total{reason="stale-owner"}[5m]) > 0` (the Prometheus label is `reason`; the OTel attribute is `kind`) | A node found itself hosting a live persistent session for a placement group it does not own and closed the connection so the client relocates to the owner (issue #284). Expected **in ones after a node roll** — each is one immediate client reconnect, and the alternative was an undeliverable session until the client's keepalive fired. A *sustained* rate means group ownership is churning, or client traffic is being opened before the lease topology has converged onto the voter set — check `mqttd_voters`, `mqttd_lease_epoch` and the *Replication lag* row. **Each close also publishes that client's Last Will**, so suppress device-offline alerting while this counter climbs |
| **Sessions stuck misplaced** | `mqttd_misplaced_sessions > 0` for 2m, or `rate(mqttd_session_rehomes_total{reason="unrelocatable"}[5m]) > 0` | A live persistent session is hosted on a node that does not own its group and **cannot be rehomed**, because the owner's peer-link address is unknown to that node — so ADR 0005's degrade-don't-refuse keeps serving it locally rather than closing it into a reconnect loop. Those sessions **are undeliverable**: every publish toward them is refused and the publisher's ack withheld (`not the owning node for this group` in the hosting node's logs). This is a peer-mesh/gossip problem, not a session problem — check `mqttd_peer_links` against `mqttd_cluster_members` and the peer-link TLS rows |
| **Rehome closes being deferred** | `rate(mqttd_session_rehomes_total{reason="deferred"}[5m]) > 0` for 5m | More sessions want rehoming than the per-tick close cap (32/node/s) allows, so the drain is paced (issue #284). The counter increments **once per session per deferral episode**, so its increase is the size of the backlog, not the number of ticks it took to drain. Expected for a few seconds after a scale-out or scale-in, where ~1/N of groups change owner at once — the cap is also the LWT-storm cap. Sustained means ownership is churning faster than the drain: check the *Sessions rehoming* row's causes |
| **Prolonged rotation window** | `mqttd_swim_keys_accepted > 1` for > 1h | A rotation phase was never closed (see key rotation above) |
| **Config divergence** | `count(count by (checksum) (mqttd_config_info == 1)) > 1` for > 15m | A config roll did not converge; check the stuck pod |
| **Degraded durable plane** | `mqttd_lease_quorum_ack_ms` growing (ADR 0049) | fsync-bound consensus; check disks before sessions are refused |
| **Hub loop held** | `histogram_quantile(0.99, rate(mqttd_hub_dispatch_seconds_bucket[5m])) > 0.1` sustained 5m (page) | Something is blocking the single-threaded hub loop again — every client on the node queues behind it (the head-of-line failure issue #242 removed; the `command` label says which class). Since ADR 0061 the publish path's durable appends, outbound-id records, and packet-id reservations all run off-loop, so a **publish-class tail means an inline await regressed** (the one documented exception: the backlog-overflow eviction truncate — no longer "reachable only past a 10 000-entry backlog" since issue #241, because a low `MQTTD_MAX_BACKLOG_BYTES` makes it fire on ordinary traffic, roughly one on-loop store ack per publish to that subscriber; if you see this tail, check that knob against `MQTTD_MAX_PACKET_SIZE` before hunting a regression. Routing that truncate through the session's append lane is the ADR 0061 residual that removes it); an **ack-class tail** is the documented residual — `truncate_acked`, QoS 2 phase advances (`advance_outbound`), and `clear_outbound` still run on-loop against a degraded store; an **attach-class tail** means replay reads/truncates are degraded — check `mqttd_durable_append_latency_seconds` and the durable-plane rows above |
| **Acked messages being shed for a slow subscriber** | `increase(mqttd_publish_dropped_total{reason="backlog-overflow"}[5m]) > 0` (warn), alongside `mqttd_backlog_bytes` | A subscriber is not keeping up and the broker is truncating **already-acked** messages out of its in-memory flow-control backlog — the publisher was told nothing (issue #241, ADR 0041 T10). The WARN line names which bound fired (`bound="messages"`, `"bytes"`, or `"messages+bytes"` when one arrival tripped both), how many entries went (`dropped`), and the configured caps. Non-zero right after you set `MQTTD_MAX_BACKLOG_BYTES` means the cap is tighter than the subscriber's lag: raise it, or bound memory with `MQTTD_MAX_INFLIGHT_MESSAGES`, which gates the wire window rather than shedding — but note it does NOT remove this risk: the surplus it holds back waits in this same drop-oldest backlog, so a tight in-flight ceiling with a tight backlog bound sheds MORE, not less. `mqttd_backlog_bytes_max` (sampled on the session sweep) is the number to size the cap against — it is the LARGEST single session's backlog, which is what a per-subscriber cap must cover; `mqttd_backlog_bytes` sums every session and is the node's total RAM in backlogs, not a per-subscriber number, and a rising value with a flat counter is the warning *before* shedding starts. `queue-overflow` is a different arm — the DURABLE offline queue — and does not move with this one |
| **Append lane saturating** | `mqttd_append_lane_jobs` growing sustained (warn); `rate(mqttd_publish_dropped_total{reason="append-backlog-full"}[5m]) > 0` (page) | A session's placement group is not keeping up (degraded follower set: each append or QoS 2 outbound-id record is bounded by the 5s replication RPC timeout, FIFO per session — 256 queued jobs max per session, then the NEWEST publish is withheld so its publisher retries; a detach spill past the cap+headroom sheds into this same counter). Only that group's sessions are affected — connects, subscribes and other groups' publishes keep flowing (issue #242). The degraded-group signals are per-session ones: this gauge/counter pair, `rate(mqttd_publish_dropped_total{reason="outbound-id-write-failed"}[5m])` (a QoS 2 outbound-id record write failed; the delivery is re-queued and retried on the next drain), and end-to-end QoS 2 delivery latency to that group's subscribers — NOT hub dispatch tails, which stay flat by design. Find the degraded group's followers: `mqttd_replica_groups_tracked - mqttd_replica_groups_current`, `mqttd_durable_append_failures_total`, and the *Durable writes refused* row |

`curl <pod>:8080/statusz` is the human-readable superset of all of it.

## Migrating onto mqttd

Day 0, not day 2, but it belongs beside these procedures because it *is* one: converting a
Mosquitto / EMQX / HiveMQ configuration ([`scripts/migrate/`](../scripts/migrate/), every
unmapped setting emitted as a `TODO(migrate)` in the file you are about to deploy) and then
moving live traffic across.

The second part is the one with a trap in it: **mqttd cannot import another broker's session
state.** A moved client loses its offline queue, its subscriptions and any in-flight QoS 2
exchange, and must resubscribe. Retained state *does* cross, by itself, through the bridge.
So cutover is a **dual run** — bridge both brokers, move clients in cohorts, verify, cut,
and roll back by re-widening the bridge rule because the incumbent is still live.

**That retained sync has a hazard, and this page is where you will hit it.** The re-sync runs
in **both** directions on every reconnect, so a retained value deleted on one side while the
bridge is down is **resurrected** from the other: the surviving copy wins, a tombstone is not
idempotent under this scheme, and nothing logs the resurrection. Reproduced on the playbook's
exact `both`/no-remap shape — value crossed to mqttd, bridge stopped, cleared on the incumbent
with `mosquitto_pub -r -n` and confirmed gone, bridge restarted, value **back** on the
incumbent. So when the brownout rows under
[Monitoring for the operator](#monitoring-for-the-operator-and-humans) tell you to prune retained
state during a dual run, **prune with the bridge running and then check both sides** (measured:
gone on both, and still gone after a bridge restart). Full write-up in
[MIGRATION.md](MIGRATION.md#step-3--bridge-them) and [BRIDGE.md](BRIDGE.md).

The converters, the per-broker mapping tables, and that playbook — written against
`mqtt-bridge`'s actual refusals, with its bridge step exercised against a real third-party
broker and every untested step marked — are the
[migration guide](MIGRATION.md). Start with `scripts/migrate/cert-audit.sh`: mqttd refuses a
client certificate without the `clientAuth` extended key usage at the handshake, which
OpenSSL-based brokers tolerated, so a migrating fleet otherwise discovers it by outage.

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
