# ADR 0088 — Cluster credentials have a lifetime the deployer sets, the broker enforces it strictly, and whoever operates the cluster rotates in time

- **Status:** Proposed
- **Date:** 2026-10-13
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0088-credential-lifetimes-and-rotation.md](../delivery/0088-credential-lifetimes-and-rotation.md) — plan, progress, and changelog
- **Related:** [ADR 0004](0004-identity-and-authentication.md) (a node is the Common Name
  of its certificate), [ADR 0003](0003-gossip-authentication.md) (the shared gossip key and
  its dual-key window), [ADR 0022](0022-signed-gossip.md) (every gossip datagram signed
  with the node's certificate key, expiry and revocation checked per datagram),
  [ADR 0040](0040-revocation-reaches-live-state.md) (certificates and revocation lists reloaded
  without a restart; a revoked peer's link torn down), [ADR 0033](0033-config-file-watch-reload.md)
  (the file watcher), [ADR 0055](0055-kubernetes-operator.md) (the operator; its T5,
  unattended gossip key rotation, deferred), [ADR 0087](0087-delivery-across-a-false-dead-verdict.md)
  (D5: a node keeps dialing a peer it holds `Dead`, which is what raised the question this
  record answers)

> **Direction decided 2026-10-13** (the maintainer). Four statements, recorded as given:
>
> 1. *When deploying a cluster the certificate expiry should be set. The operator, whether
>    Kubernetes or a person, needs to rotate the certificates in due time.* The broker
>    issues nothing.
> 2. *Keep it strict*: a node whose certificate has expired is out of the cluster. There is
>    no grace period.
> 3. Certificates should be short-lived (the aim is hours to a day), and the gossip key
>    should be replaced when a node has left the cluster, whatever the reason.
> 4. Two gossip keys are alive while one replaces the other, and only then: *old gossip
>    keys should be evicted, so an older compromised node can't use them.*
>
> The record stays **Proposed** until it is accepted; four sub-decisions in §4 are still
> open, each with a proposal.

## Context

A node's place in a cluster rests on two credentials.

**Its certificate.** One leaf per node from a dedicated cluster CA, Common Name equal to
the node id, both server and client usage, an ECDSA or Ed25519 key. It authenticates both
ends of every peer link, and its key signs every gossip datagram the node sends.

**The gossip key.** One 256-bit secret shared by every node. A datagram carries an HMAC
under it; a receiver that cannot verify the HMAC drops the datagram before it looks at
anything else.

What is true of them today (checked against the tree at the date above):

| | Certificate | Gossip key |
|---|---|---|
| Who makes it | The deployer: `deploy/systemd/gen-certs.sh`, the chart's `bootstrap.sh`, or cert-manager | The deployer |
| Lifetime | `gen-certs.sh` defaults to 365 days without being asked; cert-manager's is whatever the `Certificate` says | None. It is the same for the life of the cluster unless someone rotates it by hand |
| Replaced without a restart | Yes: the file watcher and SIGHUP re-read the peer certificate, key, CA and CRL (`watched_policy_paths`), and a node on its new certificate and one on its old accept each other | No. `cluster.swim.key_file` and `key_accept` are read at start; the key is outside the watcher's scope |
| Two alive at once | Not needed: each certificate is verified against the CA by itself | Yes: a node seals with `key` and accepts `key` ∪ `key_accept` (ADR 0003) |
| Rotation procedure | Replace the files | Three configuration changes, each a rolling restart of every node (OPERATIONS, "SWIM gossip key rotation") |
| Expiry honoured | On gossip, per datagram (`expired`). On a peer link, at the handshake only: nothing ends an established link when its peer's certificate expires | — |
| Revocation | A cluster-CA-signed CRL, checked at every accept, every dial and every datagram, reloaded live; an established link of a revoked peer is torn down | — |
| Revocation tooling | None: `gen-certs.sh` has `ca` and `node`, no `revoke` | — |
| Told that expiry is near | No. `mqttd --check-tls` reports the validity window when run; there is no metric, no alert and no log line as the date approaches | — |
| What a node's departure changes | Nothing. `mqttd --decommission` hands the node's data over; its certificate stays valid until it expires, and no step of the procedure revokes it | Nothing. A node that left still knows the key |

So a certificate minted with the script's default is good for a year after the node it
names is gone, the broker says nothing as any certificate nears its end, and a short
lifetime would not actually bound an established link. The gossip key cannot be replaced
without three rolls, which is why nobody replaces it.

How much the gossip key protects matters for the priorities below. Every datagram is also
signed by its sender's certificate and checked for expiry and revocation, so a holder of
the gossip key alone cannot make a statement about membership that any node believes. What
the key alone buys is getting a datagram past the first gate: work for the receiver, and
since ADR 0016's 2026-10-12 amendment one small, rate-limited certificate request in reply.
Short-lived certificates therefore do most of the work of putting a departed node out;
replacing the key is the second line.

## 1. Decision: certificates

### D1 — The lifetime is chosen at deployment, in every path, and stated

- `gen-certs.sh` takes the leaf lifetime as an explicit choice and prints the expiry of
  what it minted with the date by which it must be renewed. Its silent 365-day default
  goes, and nothing replaces it: without a stated lifetime it refuses (O1).
- The Helm chart and the operator expose the node certificate's lifetime and renewal lead
  as values, and pass them to cert-manager where cert-manager issues.
- The lifetime is not broker configuration. The broker reads it from the certificate.

### D2 — The broker says when the time is running out

- A gauge per loaded certificate (peer, client-facing listener, admin): seconds until
  `notAfter`, and the fraction of its lifetime already spent.
- A warning in the log, repeated at a bounded rate, once a configurable fraction of the
  lifetime is spent (default: two thirds), and an error once it has expired, each naming
  the file.
- The chart's alert rules gain one for it. `mqttd --check-tls` and `--check-config` apply
  the same threshold and say so.
- The same for the cluster CA and the CRL's `nextUpdate`, which fail a cluster the same
  way.

This is the part "rotate in due time" depends on: the duty is the operator's, and the
broker's part is that the operator cannot be surprised.

### D3 — Expiry is enforced on established links

A peer link whose peer's certificate has passed `notAfter` is ended, as a link whose
peer is revoked is ended today (the link already records what the handshake presented).
The node's own expired certificate ends its links from the other side by the same rule.
Checked on a timer no coarser than a minute; a link is not kept past expiry by the absence
of traffic. Without this a short lifetime bounds new links and nothing else.

Client-facing listeners: a client's certificate is checked at its handshake, and a session
outliving it is the subject of ADR 0040's sweep; expiry joins that sweep behind a setting
that is off by default (O2).

### D4 — Strict

A node whose certificate has expired cannot open or keep a peer link and its gossip is
dropped. There is no grace period, no "accept for a while after `notAfter`", and no
fallback to the gossip key alone. An operator who did not rotate has a node, or a cluster,
that is down until they do; D2 is what stands between that and a surprise. A small
allowance for clock difference applies to `notBefore` only (O3).

A node that was down for longer than its certificate's lifetime comes back with an expired
certificate and stays out until it is given a new one. That is the same act as
commissioning it.

### D5 — Renewing and revoking are commands, not recipes

`gen-certs.sh` gains `renew <node>` (a new leaf, and by default a new key, for an existing
identity, checked against the same rules `node` checks) and `revoke <node>` (the
cluster-CA-signed CRL with that certificate added, ready to distribute; the broker reloads
it live). Decommissioning a node ends with revoking its certificate; the procedure says so
and `mqttd --decommission` prints it. With lifetimes of hours revocation matters less,
and it remains the only way to put a node out before its certificate runs out.

### D6 — The broker does not issue

There is no enrolment protocol, no issuing key inside the cluster and no renewal over the
peer link. On Kubernetes cert-manager issues, per pod; elsewhere the deployer's CA does,
by `gen-certs.sh` or their own. A built-in issuer was considered (see Alternatives) and is
not part of this record.

## 2. Decision: the gossip key

### D7 — The gossip key is replaced without a restart

`cluster.swim.key_file` and the accepted keys join the file watcher's and SIGHUP's scope.
A replaced key takes effect on the next datagram sealed or opened. The dual-key window
stays what makes a replacement safe: every node accepts the new key before any node seals
with it, and every node seals with it before the old one stops being accepted.

### D8 — One file, several keys (a keyring), and the cluster orders the change itself

Proposed, and the sub-decision with the most consequence (O4):

- The key material becomes a keyring: an ordered list of keys, each with a short
  identifier. A node accepts every key in its keyring.
- Each node gossips the identifiers of the keys it accepts. A node seals with the newest
  key that it sees every current member accepting, and with the key it sealed with before
  until then.
- Replacing the key is then two writes with any amount of time between them: add the new
  key to every node's keyring; later, remove the old one. No write has to wait for a
  particular moment, and a node that received the first write late delays the switch
  instead of being cut off.
- A datagram names the key it was sealed with, so a receiver verifies one HMAC, not one
  per accepted key.
- A keyring holds at most two keys: the one in use and its successor. A third is a
  configuration error. There is no list of past keys to accept.

The alternative is the present three-phase procedure with D7 making each phase a file
replacement: simpler in the broker, and its ordering is then the operator's to get right
(a phase applied before the previous one has reached every node partitions membership).

Either way the single `key` + `key_accept` form stays accepted, so a cluster rolls from
v1.1.0 without changing its configuration.

### D9 — Who replaces it, and when

- **On Kubernetes, the operator.** A rotation controller writes the keyring Secret: on a
  schedule and when a pod leaves the cluster for good (a scale-down, a replacement).
  The schedule is daily (O5). It waits for every pod to report the new key accepted before it retires the old one, by
  the broker's metrics, not by sleeping: a mounted Secret reaches a pod when the kubelet
  syncs it.
- **Elsewhere, a person or their automation,** by the same two writes; the documented
  procedure and a check (`mqttd --check-config` naming the keys a node accepts and seals
  with) replace the three rolls.
- **A node that crashed or was wrongly declared `Dead` and remains a member** needs no
  replacement: it is still entitled to the key and reads the current keyring when it
  starts.
- **The old key is evicted, by the broker itself, and promptly.** The overlap exists for
  the change and for nothing else. Once every current member seals with the new key, a
  node stops accepting the old one after a short, bounded overlap (O6: proposed ten
  minutes) whether or not the second write has removed it from the file, logs that it has,
  and warns for as long as the file still lists it. The operator's retiring write follows
  as soon as the switch is seen; it is tidying, not what the eviction depends on. A key
  that has been superseded is never accepted again, so a copy of it on a node that left,
  was replaced or was compromised opens nothing.
- **A node that returns with only an evicted key is out of gossip** until it is given the
  current keyring: a restarted pod reads the current Secret and is unaffected; a host that
  was unreachable through a whole change is re-provisioned with the key, as it would be
  with a certificate that expired meanwhile. This is the price of the eviction, and it is
  the same strictness as D4.

### D10 — What the broker shows

The identifiers of the keys a node accepts and the one it seals with, as metrics and in
`--check-config`; a counter of datagrams dropped for an unknown key identifier; a log line
when the sealing key changes. Never the keys.

## 3. What this does not do

- It does not make the gossip key a cluster-managed secret that nodes hand to each other.
  Admission to gossip stays "has the file".
- It does not bind a peer link to a cluster identity. A link is admitted by the cluster
  CA, the CRL and the node id its certificate attests; two clusters under one CA are not
  told apart on the link (found while reviewing ADR 0087's T4). That is its own question
  (O7).
- It does not change how a client authenticates.

## 4. Sub-decisions

Decided by the maintainer on 2026-10-13:

- **O1 — `gen-certs.sh` without a stated lifetime: it refuses.** There is no default leaf
  lifetime; the command names the option and stops. (The CA keeps a long life.)
- **O2 — Client certificates: a client's session ends when its certificate expires**, as
  it does when it is revoked, **behind a setting that is off by default**: devices with
  long sessions and short certificates exist.
- **O5 — The operator replaces the gossip key daily** when nobody has left, with the
  certificates, and at once when a member leaves for good.
- **O6, the eviction itself — old keys are evicted** and are not kept for late nodes; a
  node holding only an evicted key is re-provisioned (D9).
- **O7 — A cluster identity on the peer link: yes**, as a separate record; it changes the
  `Hello` frame.

Still open, each with its proposal:

- **O3 — Clock difference:** proposed 60 s of tolerance on `notBefore`, none on
  `notAfter`.
- **O4 — D8's keyring with the cluster ordering the change, or three operator-ordered
  phases.** Proposed: the keyring. It costs more broker code and removes the one mistake
  that partitions a cluster.
- **O6, the duration — how long the overlap may last once every member seals with the new
  key.** Proposed: ten minutes, configurable down to zero, never unbounded.
- **O8 — The default threshold for D2's warning.** Proposed: two thirds of the lifetime
  spent.

## Alternatives considered

- **A built-in issuer** (a node renews its own certificate from the cluster, new
  identities by a join token). It removes the external dependency and puts a signing key
  inside the cluster, where a compromised node is next to it. Rejected by the direction
  above: rotation is the operator's.
- **A cluster-managed gossip key** (members hand a fresh key to each other over the
  mutually authenticated links whenever membership changes). It would make "replace on
  every exit" automatic everywhere, and needs a bootstrap step before a node can gossip
  and a rule for two halves of a partition that each replaced the key. Not needed once
  the operator replaces the key and certificates are short.
- **A grace period after expiry.** It turns a missed rotation from an outage into a
  warning, and makes the lifetime a suggestion. Rejected (D4).
- **Dropping the shared gossip key**, relying on signatures alone. The HMAC is what lets a
  node discard an outsider's datagram without parsing a certificate; kept.

## Consequences

- A cluster's credentials stop being good long after the nodes they name are gone: a
  departed node is out within one certificate lifetime without anyone acting, and a gossip
  key it knew stops opening anything minutes after the cluster has moved to the next.
- A node that misses a whole key change must be given the key again. Membership by
  "has the file" becomes membership by "has the current file".
- A missed rotation is an outage. The broker's warnings are what prevent it, and a
  deployment without alerting takes that risk knowingly.
- Short lifetimes make a correct clock a requirement of membership.
- The rig and every systemd deployment gain a rotation they must perform; on Kubernetes
  the operator performs it.
- Every renewal makes each peer learn a new certificate. ADR 0016's 2026-10-12 amendment
  (the first datagram to each destination carries it) is what makes that uneventful.

## Not verified

- That nothing ends an established peer link at its peer's certificate expiry was read
  from `crates/mqttd/src/peer.rs` (admission checks the CRL; no expiry check or timer was
  found), not shown by a test. T3 starts by writing that test.
- cert-manager's behaviour with lifetimes of hours under the chart's csi-driver layout was
  not run.
- The cost of gossiping accepted-key identifiers was not measured.
