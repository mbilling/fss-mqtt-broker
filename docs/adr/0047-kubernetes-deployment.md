# ADR 0047 — Kubernetes deployment (Helm chart, StatefulSet, safe scale-down)

- **Status:** Accepted
- **Date:** 2026-07-17
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0047-kubernetes-deployment.md](../delivery/0047-kubernetes-deployment.md) — plan, progress, and changelog
- **Related:** [ADR 0045](0045-release-engineering-and-distribution.md) (the hardened image
  this deploys), [ADR 0046](0046-file-based-configuration.md) (the config file mounted from
  a ConfigMap), [ADR 0043](0043-elastic-cluster-resize.md) (the decommission drain a
  scale-down must trigger — pulling a pod is a planned removal, not a crash), [ADR 0019](0019-graceful-shutdown.md)
  (the graceful shutdown a pod termination must honor), [ADR 0020](0020-metrics-and-observability.md)
  (the `/livez` + `/readyz` probes and `/metrics` a k8s deployment wires), [ADR 0018](0018-on-disk-persistence.md)
  (the redb data dir that needs a PersistentVolume per pod), [ADR 0039](0039-versioning-and-upgrade-policy.md)
  (the one-at-a-time rolling upgrade a k8s rollout must respect)

> This record states the decision only. How it is being built and how far along it is live
> in the [delivery doc](../delivery/0047-kubernetes-deployment.md).

## Context

The broker is cluster-native and the image (ADR 0045) will exist, but there is no supported
way to **run a cluster on Kubernetes** — the platform most operators will reach for. Naively
deploying it wrong loses the very guarantees the broker works hard to provide:

- A **`Deployment` with ephemeral storage** throws away the durable session store (ADR 0018)
  on every pod reschedule — turning a data-safe broker into a lossy one by misconfiguration.
- **Scaling down a `Deployment`/`StatefulSet`** by deleting a pod is, to the cluster, a node
  *crash* — survivors recover (they must), but it skips the ADR 0043 **decommission drain**
  that makes a *planned* removal lose nothing and demote cleanly. A shrink should drain, not
  crash.
- A **rollout that replaces pods too fast** violates ADR 0039's one-node-at-a-time upgrade
  motion, and a rollout with no `PodDisruptionBudget` lets a node drain take out quorum.
- The health probes (ADR 0020) and the config file (ADR 0046) exist but nothing wires them
  into the k8s primitives (readiness gating, ConfigMap mount, Secret mount) out of the box.

The result today is that "runs on Kubernetes" is true only for an expert who assembles all
of this by hand — and gets it subtly wrong in ways that surface as data loss under
scale/upgrade.

## Decision

A **supported Kubernetes deployment** ships — a Helm chart (and plain manifests) that
encode the broker's operational contract so the safe path is the default. Five parts:

### 1. StatefulSet with per-pod persistent storage

The broker runs as a **StatefulSet** with a `volumeClaimTemplate`, so each pod gets a stable
identity and its **own PersistentVolume** for the redb data dir (ADR 0018). A rescheduled
pod reattaches its volume and recovers its durable state — never the ephemeral-storage
data-loss trap. `MQTTD_NODE_ID` is derived from the stable pod name; gossip seeds point at
the headless service, so the mesh forms itself (ADR 0016).

### 2. Config via ConfigMap, secrets via Secret

The config file (ADR 0046) is a **ConfigMap** mounted at a path, so a `helm upgrade` /
GitOps commit is the unit of change; TLS material, password/JWT keys, and the gossip key are
**Secret** mounts referenced by path. `--check-config` runs as an init container or CI gate,
so a bad config fails the rollout before a pod serves.

**As delivered, "referenced by path" means the chart DERIVES the paths from the Secret
names** rather than asking the operator to restate them (`MQTTD_PEER_TLS_{CA,CERT,KEY}`,
`MQTTD_SWIM_KEY_FILE`). Mounting and referencing were separate steps until issue #262, and
that gap was the whole defect: a cluster-bus Secret could be mounted while nothing read it,
so the bus stayed plaintext under a deployment that looked mutually authenticated. See the
2026-08-13 amendment.

### 3. Probes and services wired to the broker's real signals

`readinessProbe` → `/readyz` (which already reports membership + lease-group readiness +
decommission progress, ADR 0020), `livenessProbe` → `/livez`, and a `ServiceMonitor`/scrape
annotation for `/metrics`. A `Service` fronts the client listeners; a **headless** Service
backs gossip discovery and the peer mesh.

### 4. Scale-down is a decommission, not a crash

Removing a replica triggers the ADR 0043 **decommission drain**: a `preStop` hook sends
`SIGUSR1` (drain — hand every held key to the post-departure replica set, verify, then leave
gracefully) and the pod's `terminationGracePeriodSeconds` is set long enough for the drain
plus the ADR 0019 graceful shutdown to complete. A scale-down therefore loses nothing and
demotes voters cleanly, exactly as `SIGUSR1` does outside k8s; a hard kill (grace exceeded)
falls back to crash semantics the survivors already handle.

### 5. Upgrades and disruption respect quorum

The StatefulSet's **`RollingUpdate` with `partition`/one-at-a-time** ordering enacts
ADR 0039's one-node-at-a-time motion — each pod rolls, rejoins, and reaches the caught-up
watermark before the next (the ADR 0044 P3 rolling-upgrade test proves the broker survives
exactly this). A **`PodDisruptionBudget`** (`maxUnavailable: 1`) stops a node drain or
voluntary disruption from taking two nodes — and thus quorum — at once.

## Consequences

- "Runs on Kubernetes" becomes true by default, with the durability, safe-shrink, and
  safe-upgrade guarantees intact rather than lost to misconfiguration.
- The chart is executable operator documentation: the ADR 0043/0039/0019 contracts become
  `preStop` hooks, grace periods, update strategy, and a PDB — checked by a kind/k3d smoke
  test in CI (an out-of-cluster analog of the ADR 0044 quickstart-as-test).
- The broker gains a Kubernetes dependency *surface* (chart maintenance, k8s version skew)
  but no code coupling — the chart drives the same binary and signals as any other operator;
  bare-metal/systemd/Docker-Compose deployments stay first-class.
- A StatefulSet's stable-identity model fits HRW placement (ADR 0001) well: a pod keeps its
  id and volume across reschedule, so ownership and durable state move together.

## Alternatives considered

- **Deployment + ephemeral storage:** the common default, and wrong here — it discards the
  durable store on reschedule. A StatefulSet with a PVC is the only correct shape for a
  stateful, durable broker. Rejected.
- **N single-replica Deployments, each with its own dedicated PVC (the "manual
  StatefulSet"):** a legitimate pattern, and it gets the thing that matters most right — each
  node keeps a stable, dedicated PersistentVolume that survives reschedule, so on the
  *durability* axis it is a wash with a StatefulSet. It loses on the *lifecycle mechanics*,
  and in three concrete ways. (a) The redb data dir is a `ReadWriteOnce` volume, and a
  Deployment's default `RollingUpdate` surges the new pod up before the old releases the
  volume — a Multi-Attach deadlock that forces `strategy: Recreate` (our replication makes the
  resulting per-node gap survivable, but it is a trap you must know to avoid). (b) Nothing
  coordinates independent Deployments, so a single `apply`/GitOps sync rolls all of them at
  once and takes out quorum — the one-at-a-time ordering ADR 0039 and part 5 depend on must be
  re-imposed by hand (sync-waves, `dependsOn`, a per-Deployment PDB). (c) Scaling becomes
  hand-authored boilerplate — a new node is a whole new Deployment *and* PVC manifest rather
  than one replica count against a `volumeClaimTemplate`. A StatefulSet packages exactly our
  topology — *N pods, each with a stable identity and its own volume, updated one at a time* —
  so the manual approach rebuilds ordered rollout and template provisioning worse. Our
  replication makes each failure mode gentler (a quorum survives a clumsy rollout; a lost
  volume triggers catch-up, not data loss), so this is not *wrong* — just more moving parts for
  guarantees the StatefulSet gives in one object. Rejected.
- **A custom operator (CRD + controller) instead of a Helm chart:** more power (automated
  decommission on scale, orchestrated upgrades), but a large surface to build and maintain
  before there are users. A Helm chart that encodes the contracts covers the need now; an
  operator is a plausible post-1.0 follow-on if demand appears. Deferred, not rejected.
- **Letting scale-down be a plain crash and relying on survivor recovery:** correctness
  holds (survivors do recover), but it needlessly forfeits the ADR 0043 clean-drain
  guarantee and can transiently degrade under load. Wiring `preStop → SIGUSR1` makes the
  intended, lossless path the default. Rejected.
- **No `PodDisruptionBudget`:** simpler, but a routine node drain could evict two brokers at
  once and lose quorum — precisely the failure the broker's durability model assumes cannot
  happen silently. A PDB is not optional for a quorum system. Rejected.

## Amendment (2026-08-04): the operator question, revisited — still no, with named triggers

A pre-release review asked whether the chart should grow a companion operator
(CRD + controller). The review inventoried every day-2 sequence the chart could not
carry and found four: policy/cert rotation reaching running pods, SWIM key rotation
(the `key_accept` dual-key window), PVC lifecycle after scale-down (stale-data
reattach), and founder-PVC-loss (a fresh, seedless pod-0 founding a second cluster).

**Decision: still no operator — because three of the four dissolved at chart level**
(the 2026-08-04 chart hardening: `config_watch_secs` makes rotation automatic per
ADR 0033; `persistentVolumeClaimRetentionPolicy` closes the PVC trap; a per-pod
readiness floor plus the [OPERATIONS.md](../OPERATIONS.md) founder rule fences the
split-brain vector), and the fourth (key-rotation orchestration) is a rare,
operator-initiated act a runbook carries honestly. What remains genuinely
operator-shaped is small: orchestrating the three-phase key rotation unattended, and
distinguishing a shrink from a roll so upgrades skip the full drain. Neither justifies
a controller today, and a CRD is a far stickier public API than values.yaml — the
worst possible thing to iterate on pre-1.0.

**Named triggers that reopen this decision** (any one suffices; replacing the
original's bare "post-1.0 if demand appears"):

1. A real deployment needs **unattended SWIM key rotation** at fleet scale (the
   runbook's three `helm upgrade` rolls become an operational burden, not an event).
2. **Fleet/multi-cluster management** demand — many mqttd clusters under one control
   plane.
3. The **drain-on-every-roll cost is measured as pain** (upgrade windows dominated by
   drains that a shrink/roll-aware controller would skip).
4. Post-1.0 **user demand** for CRD-native management, as originally recorded.

**2026-08-05 update:** the maintainer engaged this path — an operator handling
split-brain and brownouts is now planned. The acting-signals land first
([ADR 0054](0054-operator-facing-state-surface.md): cluster identity + containment,
brownout/drain gauges, `/statusz`); the controller follows against proven signals.

If reopened, the operator wraps the existing contracts (drain via SIGUSR1, readiness
via `/readyz`, config via the same TOML) rather than inventing new control surfaces —
the no-code-coupling property this ADR's consequences noted is what keeps that cheap.

## Amendment (2026-08-06): PVC retention defaults to Retain on scale-down (issue #97)

The chart shipped `persistentVolumeClaimRetentionPolicy.whenScaled: Delete`, documented as
"correct here, not data loss", on the reasoning that a scale-down runs the ADR 0043
decommission drain first — the departing node hands every held key to the surviving
replica set, so its volume afterwards holds only superseded state, and deleting it closes
the stale-rejoin trap when a later scale-up reuses the ordinal.

The reasoning is sound and carries an unstated precondition: **a survivor must remain to
receive the drain.** Kubernetes applies the policy uniformly and has no notion of "shrink
to a floor" versus "shrink to nothing", so at `--replicas=0` — the obvious way to restart
a fleet or quiesce an environment — the drain has nowhere to hand state to and the
deletion erases the only copy. Verified on kind: every PVC gone, and the cluster came back
with a different identity and an empty store, with no warning and nothing in the runbook
against it.

**Both policies now default to `Retain`.** The cost is a visible, reversible orphan after
each shrink (`kubectl delete pvc data-<sts>-<n>` when you are satisfied) and a reattached
volume if an ordinal is reused; the benefit is that no scale operation can destroy the
only copy of anything. Operators who shrink often and never scale to zero can set
`whenScaled: Delete` back. The stale-rejoin trap the original setting closed is now the
operator's to close deliberately, which is documented in OPERATIONS.md alongside the
reuse-an-ordinal note.

Found while testing an unrelated hypothesis about full-fleet restarts — which, separately,
was refuted: a rolling restart or a delete-all recovers on its own with identity and lease
group intact. The danger was never the restart; it was the volume deletion that scaling to
zero used to trigger.

## Amendment (2026-08-10): the same deployment, off Kubernetes (T9)

This record decided how mqttd is deployed *on Kubernetes*, and stopped there. What that
left, in practice, was a project that looked deployable only on Kubernetes: the chart was
the one artifact a user could copy, the operator is not installable (T8), and everything
else was prose in `OPERATIONS.md` — accurate prose, with nothing behind it. Every outside
reviewer read it the same way, and it is not what the broker actually requires: it is a
single static binary configured entirely by environment.

**`deploy/compose/` and `deploy/systemd/` now ship**, configured identically to the chart
(`MQTTD_*` environment, secrets by path) and secure by default: authentication on,
deny-by-default topic ACLs, an authenticated gossip mesh, majority-aware readiness, and a
memory bound — because the broker still has no total-memory knob, so the cgroup limit is
the bound (ADR 0041 T8 remains open, and `docs/SIZING.md` says so).

### What Kubernetes was doing for us that these cannot

**Seed lists and the founder rule.** The chart derives node identity, seeds and the
readiness floor from StatefulSet ordinals. Off Kubernetes the operator maintains them.
Exactly one node bootstraps with an *empty* seed list — that is what makes it found the
lease group — and it must be given seeds afterwards, or a lost data directory makes it
found a second cluster. This is stated in both READMEs and in the annotated env file.
The broker still contains the failure (a re-founder self-quarantines, ADR 0054), but
containment is the backstop, not the plan.

**Health checks.** Kubernetes probes with `httpGet`, performed by the kubelet outside the
container, so the image never needed an HTTP client. Compose, Podman and systemd all
express health as a *command the container runs*, and the image is distroless — no shell,
no `curl`. The only thing available to put in a healthcheck was `--check-config`, which
validates configuration and says nothing about whether the broker is serving. That is a
health check that cannot fail, and it is worse than none: the orchestrator reports a
wedged broker as healthy.

`mqttd --probe [/readyz|/livez]` closes that. It asks this node's own health endpoint over
a hand-rolled HTTP/1.0 request — matching the equally hand-rolled server, and adding no
dependency — and exits `0` only on `200`. The distinction it preserves is the one that
matters operationally: a minority node answers `/livez` (do not restart me) while failing
`/readyz` (do not send me clients), and only a probe that tells those apart can express
"pull this from the load balancer but leave it running".

### Tested, not merely written

`scripts/deploy-smoke.sh` runs in CI. It **parses `deploy/systemd/mqttd.env.example`**
rather than restating its values, so a setting renamed in the artifact and not in the test
fails loudly instead of drifting; it boots three real nodes from those values; and it
asserts the claims the artifacts make — anonymous refused, a `--hash-password` line
authenticating, the ACL granting a device its own subtree and returning SUBACK `128` for
another's, cross-node routing, an acknowledged `QoS` 1 message surviving `SIGKILL` of the
node that accepted it, and a minority node reporting live-but-not-ready. `docker compose
config` and `systemd-analyze verify` run when available and **skip loudly** when not.

The `systemd-analyze` check earns its place for a specific reason: systemd *silently
ignores* a directive it does not recognise, so a typo'd hardening option leaves a unit that
looks hardened and is not.

## Amendment (2026-08-13): the cluster bus needs one certificate per NODE (issue #262)

`bootstrap.sh` minted the cluster-bus material as **one shared leaf for the whole cluster**
— `CN=<release>-node`, no SANs, an RSA key — created a Secret from it, and printed
`--set secrets.peerTls.secretName=mqttd-peer-tls` as the way to turn the mutually
authenticated bus on. Each of those three properties is independently fatal, and the chart
had a fourth problem that made the first three invisible.

**Why a shared certificate cannot work here, at all.** The bus binds node identity to the
certificate: a peer may only claim the node id its Subject CN attests to (ADR 0004), and the
gossip plane enforces the same CN against the datagram sender (ADR 0022). Every pod
presenting `CN=<release>-node` while claiming its own pod name means **every** link is
dropped after a successful handshake. Separately, name verification uses **SANs only**, so a
leaf with none can never satisfy a dialer whose `ServerName` is the peer's advertise host —
CN and SAN are two different requirements, and satisfying one says nothing about the other.
Separately again, the peer key **is** the per-node gossip signing key, which accepts only
ECDSA P-256/P-384 or Ed25519 in PKCS#8; an RSA leaf gives a working TLS handshake and then a
hard startup failure. Signed gossip arms *itself* once peer TLS and a gossip key are both
present, so that last one is not opt-in.

This is the same defect class as issue #254: a shipped artifact the project's own words
present as usable, which nobody had executed.

### What ships instead

**One CA plus one leaf per node**, keyed in the Secret by pod name (`<pod>.crt` /
`<pod>.key` alongside `ca.crt`). In a StatefulSet the identities are knowable in advance —
node id = pod name = `<fullname>-<ordinal>` — so the mint loops over ordinals. Each leaf
carries `CN` = the pod name, a SAN covering that pod's
`<pod>.<headless>.<ns>.svc.cluster.local` advertise host, both `serverAuth` and `clientAuth`,
and an ECDSA P-256 PKCS#8 key.

**The chart hands each pod its own leaf by path**, through Kubernetes' dependent-env
expansion of `$(POD_NAME)`. That indirection is what makes a per-node CN possible at all: a
StatefulSet pod template cannot select a different *Secret* per ordinal, but every pod can
select a different *key inside one Secret*. The alternative shapes were considered and
rejected — one Secret per pod cannot be mounted by ordinal (a pod template cannot vary a
Secret name per ordinal; `subPathExpr` could vary a path, but subPath-mounted Secrets never
receive updates, foreclosing any future hot-rotation work), and an init container minting its
own leaf requires the CA private key in every pod, which is precisely the custody rule this
project states elsewhere. Rotation itself is **not hot** — the peer-bus CA/cert/key are not
file-watched and the gossip signer is a startup snapshot even across SIGHUP, so a rotation is
completed by a rolling restart; an earlier draft of this section claimed otherwise
(issue #262, corrected in place; hot rotation is tracked as follow-up work).

**Announcing a Secret now implies consuming it.** The chart derives
`MQTTD_PEER_TLS_{CA,CERT,KEY}` and `MQTTD_SWIM_KEY_FILE` from the secret names, so the
mounted-but-unread state is no longer expressible. This is the structural half of the fix,
and the more important half: the three certificate defects only ever *armed* once an
operator additionally hand-wrote those paths, which means the shipped script's own
instructions produced a mounted secret, a plaintext bus, and a log line saying so that
nobody was told to look for.

**The mint verifies itself.** Every property it prints is read back out of the file with
`openssl` — CA:TRUE and self-verification for the CA; CN, SANs, both EKUs, CA:FALSE,
named-curve P-256, `ecdsa-with-SHA256` and a successful `openssl verify` for each leaf —
failures accumulate so one run reports all of them, and material is staged and installed
only after it passes. Reuse **re-verifies what is on disk** instead of testing that files
exist, so a mint this script rejected is never blessed by the next run. The recipe is
written to the intersection LibreSSL (`/usr/bin/openssl` on macOS) and OpenSSL 3 both get
right, because it runs on the operator's workstation rather than in a pinned image; it is
verified on both, and a non-conforming openssl fails with the remedy named.

**Scale-up has an answer.** A new ordinal is a new node id and a new DNS name, so it needs
its own leaf: `REPLICAS=<n> ./bootstrap.sh` re-verifies and keeps the CA, the existing leaves
and the gossip key, mints only what is missing, and re-applies the Secret. A pod whose
ordinal has no leaf fails its **init container** with a message naming that command, instead
of crash-looping on an unreadable path.

### The residual, stated

On this starter path one Secret holds every node's key, and Kubernetes gives a pod the whole
Secret — so **every broker pod can read every other node's peer key**. The CN binding stops
an outsider, not a compromised pod. The CA private key never enters the cluster, which is the
custody line that matters most, but per-pod *key* isolation needs a per-pod issuer:
cert-manager's csi-driver, which is documented as the production path and is the only option
expressing all four rules while keeping each pod to its own key. Note that a single
cert-manager `Certificate` listing every pod's DNS name — the shape RabbitMQ's inter-node
example uses, and evidently the shape this script was copied from — does **not** work here:
it has one CN, and rule 1 is per-pod. Erlang distribution does not bind node identity to the
CN; this bus does.

### Why it rotted, and what now stops it

Nothing had ever run it. `values.yaml` shipped `peerTls.secretName: ""`, the kind smoke
pinned it to `""` explicitly, and no lane at either tier set it — so the one path that could
have caught an unusable certificate never executed. Three gates now exist: the kind smoke
runs with the bus **on**, minting through the shipped script and asserting `mtls=true`,
signed per-node gossip and the *absence* of CN-binding drops on every pod;
`scripts/k8s/peer-tls-check.sh` runs at PR time with no cluster, checking each leaf's CN
against the pod names the chart renders and its SAN against the advertise host the chart's
own init script computes, that the render consumes what it mounts, and that a real broker
boots on a minted leaf — with the original certificate as a **negative control** that must
be refused; and the render-parity gate gained a third pass with the bus on, so the operator
path (which had the same mounted-but-unread gap) cannot drift from the chart.
