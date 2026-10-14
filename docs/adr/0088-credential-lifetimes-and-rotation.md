# ADR 0088 — Cluster credentials have a lifetime the deployer sets, the broker enforces it strictly, and whoever operates the cluster rotates in time

- **Status:** Accepted
- **Date:** 2026-10-13 (accepted the same day)
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
> **Accepted 2026-10-13** by the maintainer, with the four sub-decisions that were still
> open (O3, O4, O6's duration, O8) taken as proposed (§4).

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

Decided (O4):

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
  node stops accepting the old one after a short, bounded overlap (O6: ten minutes by
  default) whether or not the second write has removed it from the file, logs that it has,
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

Accepted as proposed on 2026-10-13, with the record:

- **O3 — Clock difference:** 60 s of tolerance on `notBefore`, none on `notAfter`.
- **O4 — The keyring, with the cluster ordering the change** (D8), not three
  operator-ordered phases. It costs more broker code and removes the one mistake that
  partitions a cluster.
- **O6, the duration — the overlap once every member seals with the new key:** ten
  minutes by default, configurable down to zero, never unbounded.
- **O8 — The default threshold for D2's warning:** two thirds of the lifetime spent.

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

## Amendment (2026-10-13): expiry made visible, as built in T1

D2 is built as written. What it left open, and how each was settled:

- **The series and their label.** `mqttd_tls_expiry_seconds{role}` (seconds to the end;
  negative once expired, so "expired" is `< 0`) and `mqttd_tls_lifetime_spent_ratio{role}`
  (0 when new and before `notBefore`, 1 at the end, above 1 after it). `role` is the only
  label and a fixed set of eight: `listener`, `client_ca`, `client_crl` (the `[tls]`
  files), `peer`, `peer_ca`, `peer_crl` (`[cluster.peer_tls]`), `admin`, `admin_ca`
  (`[admin]`). D2 named the three certificates; the two client-facing CA bundles and the
  client CRL fail their listeners the same way and are loaded by the same code, so they
  are exported too. A role is exported only while something serves it.
- **A file with several certificates is reported by the one that expires first.** A chain
  file whose intermediate ends before its leaf is the intermediate's series, and a CA
  bundle is its oldest CA's. Both numbers of a series come from that one certificate. A
  separate `…_chain` role was the alternative; it adds series that are almost always
  redundant, and an operator who watches `peer` would not be watching it.
- **A CRL without `nextUpdate` has no series.** It has no window to spend. `--check-tls`
  says so.
- **The threshold is per mille:** `tls.expiry_warn_permille`, `1..=1000`, default `667`.
  The config structs derive `Eq`, so a float is not available, and one unit in the file
  and the environment avoids the trap `cluster.shared_local_bias_permille` documents.
  O8's two thirds is therefore 66.7%, not 66.67%. It is read at startup, as the rest of
  `[tls]` is, and applies to every role.
- **The node exports its threshold** (`mqttd_tls_expiry_warn_ratio`), so the chart's
  warning compares each series with the node's own setting and holds no copy of it.
- **The warning alert waits five minutes; the page does not wait.** cert-manager renews
  at two thirds of `duration` unless `renewBefore` says otherwise, which is the default
  threshold: a routine renewal crosses the line for as long as the new Secret takes to
  reach the pod. The alert holds for that; the log does not, and OPERATIONS says to set
  `renewBefore` above a third.
- **Material that is not valid yet is warned about** in the log, at the same rate. On
  the gauges it reads as new (0 spent), and whoever checks it against the clock refuses
  it.
- **Over OTLP all three are observable instruments,** read when the export runs.
- **The log's rate:** a line when a role is first seen past the threshold or expired (at
  startup, after the reload that loaded the file, or within a minute of the crossing),
  then at most once an hour per role. A reload that leaves a file unchanged does not
  restart the hour.
- **The page is capped at 30 days.** `MqttdTlsExpiryImminent` needs less than a tenth of
  the lifetime *and* less than 30 days left (or expired). Without the cap a ten-year CA
  pages for its last year. The cap does not touch lifetimes under 300 days.
- **`--check-tls` loses its fixed 30 days** for the same fraction, and gains the `[admin]`
  set. A certificate that lives a day always had "under 30 days left". Exit statuses are
  unchanged: an expired certificate fails, a stale CRL warns.
- **Rule tests.** The chart had none. `promtool test rules` now runs in the helm CI job
  over the rendered `PrometheusRule`.

Two things T1 found and did not change:

- **A long-lived CA warns for a long time.** Two thirds of ten years leaves more than
  three years of warning, hourly in the log. D2 asks for "the same for the cluster CA",
  and that is what is built; whether a CA should have its own threshold is a question
  for whoever lives with it.
- **The QUIC listener does not reload its certificate** (ADR 0036). On a node whose only
  client-facing TLS is QUIC, the `listener` and `client_ca` series stay those of startup
  after a reload, because that is what QUIC still serves.

And one imprecision that is accepted: the windows are read from the files just after a
reload has built its TLS contexts from them, not from the contexts. A file replaced
between the two reads is described before it is served; the watcher's next reload,
which that replacement causes, loads it. `--check-tls` also checks `[admin]` whenever its
certificate is configured, while the running broker loads it only with `admin.bind`.

## Amendment (2026-10-14): stated, renewable, revocable, as built in T2

D1 and D5 are built as written. What they left open, and how each was settled:

- **How a lifetime is written.** `--lifetime <N>h`, `<N>d` or `<N>m` on `node` and
  `renew`; `LEAF_LIFETIME` in the environment states it too, and so does a set
  `LEAF_DAYS`, the variable the script read before. A bare number is refused: it would be
  days to one reader and hours to the next. The refusal exits 2 and writes nothing.
- **Hours need `openssl ca`.** `openssl x509 -req` counts in whole days on every OpenSSL
  before 3.4 and on LibreSSL. Leaves are signed by `openssl ca` with an explicit start and
  end, and each is checked against the stated lifetime (`-checkend`) before it is
  installed.
- **The CA keeps a ledger**, because `openssl ca` writes one and a revocation list is
  generated from it: `ca/index.txt`, `ca/issued/` (certificates, no keys) and
  `ca/crlnumber`, next to the CA key. Serials are random.
- **The renew-by date is the broker's warning point**: `RENEW_AT_PERMILLE` thousandths of
  the lifetime, `667` unless set, which is `tls.expiry_warn_permille`'s default. The
  script cannot read a broker's configuration, so a deployment that changed the threshold
  sets both.
- **`renew` reads the identity from what it replaces** (CN and SANs out of the node's
  current certificates) and does not check them against the clock: an expired node is
  the one that most needs it. A new key by default; `--reuse-key` keeps it.
- **`revoke` takes a node id and revokes every certificate the CA issued to it**, not one
  file: after two renewals three certificates name the node, and each is good until it
  ends. A certificate from before the ledger is revoked from the node's directory, and
  `renew` or `node` copies the certificates it replaces into `ca/issued/` first, so the
  first renewal of a node minted by the old script does not lose the only copy of a
  certificate that is still valid. The same id can be renewed afterwards; the new serials
  are not on the list.
- **`notBefore` is a minute before the mint.** Until T3's tolerance exists, a peer whose
  clock is a few seconds behind would refuse a freshly renewed node. The end is the
  stated lifetime from the moment of minting, and the renew-by point is measured over
  the whole window, as the broker measures the fraction spent.
- **The list's `nextUpdate` is announced, not asked for**: 30 days unless
  `--crl-lifetime`, printed with its own re-issue-by date, as the CA's end is printed.
  `crl` issues the empty list and re-issues the current one.
- **The list is live only where it was configured at startup.** `cluster.peer_tls.crl` is
  a path read at start; the file behind it is what reloads. So the documented deployment
  sets it from the first day, pointing at the empty list. The shipped
  `mqttd.env.example` still has the line commented out: uncommenting it by default would
  make a list every systemd deployment has to keep re-issuing.
- **cert-manager issues per pod through its csi-driver, so there is no `Certificate`
  object.** One `Certificate` has one Common Name, and the bus binds a node to its own.
  The lifetime and the renewal lead are the volume's `csi.cert-manager.io/duration` and
  `csi.cert-manager.io/renew-before`. The chart now renders that volume
  (`secrets.peerTls.certManager`) and the operator does too
  (`spec.secrets.peerTlsCertManager`); both require the two values. A volume written by
  hand through `extraVolumes` is not checked.
- **The chart's `bootstrap.sh` is a deployer too**: `LEAF_DAYS` lost its default. It
  counts in whole days and has no revoke; it is the starter path.
- **`mqttd --decommission` prints the step on every completed drain** and says it is for
  a node leaving for good. The command cannot tell that from a restart: a Kubernetes
  `preStop` runs it for both.

Left as it was: `deploy/compose/init.sh` mints with a default lifetime (`CERT_DAYS`), and
nothing revokes a departed pod's certificate on Kubernetes, where the lifetime is the
bound.

## Amendment (2026-10-14): expiry enforced, as built in T3

D3 and D4 are built as written, with O3's tolerance; O2's setting is built and off by
default. What T3 found first, and what had to be decided:

- **The characterisation** (`crates/mqttd/tests/peer_link_expiry.rs`, its first commit):
  two spawned brokers, one with a certificate that ends forty seconds after it is minted.
  With gossip unsigned the link stayed up past `notAfter + 65 s` — nothing but the
  handshake read the certificate. With gossip signed (the default) the expired node's
  datagrams were dropped as `expired`, the failure detector declared it dead six seconds
  later and that verdict dropped the link, **and the link was back within half a second**:
  the dialer keeps dialing a peer it holds dead (ADR 0087 D5), the peer bus resumed TLS
  sessions, and a resumed TLS 1.3 handshake does not verify the certificate again. It
  flapped like that for as long as it was watched. The record's "new peer handshakes
  fail" was therefore not true of a peer this node had linked before.
- **The peer bus no longer resumes TLS sessions.** `mqtt_net::tls::peer_acceptor` and
  `peer_connector` replace the general builders for the cluster bus: no session store,
  no tickets, `Resumption::disabled()`. Peer links are few and long-lived; a full
  handshake each costs nothing that matters, and every link now verifies the certificate
  it runs on.
- **Admission checks both ends** (`peer.rs` `admit_peer_link`, both directions): the
  peer's leaf `notAfter`, as a backstop to the TLS verifier, and this node's own. A node
  whose own certificate has run out neither dials nor accepts; the peers refuse it at the
  handshake anyway, so this matters against a peer on older code. Refusals log a `WARN`
  at most once a minute.
- **A link ends at the earlier of the two certificates of its handshake.** Each side
  records the peer's `notAfter` and its own when the link forms (`LinkCertEnds`, carried
  on `HubCommand::PeerConnected`); the hub's once-a-second sweep ends the link the first
  second after the earlier one, by the same path as the CRL's peer sweep: removing the
  peer entry closes the link's lanes, its pump and its socket. D3 asked for a timer no
  coarser than a minute; the hub's existing tick is a second, and it looks at nothing
  until the earliest known end has passed. Measured end to end: 1.7 to 2.7 s after
  `notAfter`, of which up to a second is the test's own polling.
- **Own end is the certificate that is served.** It is read at startup and stored again
  by every reload that commits a new peer certificate; a link keeps the value from its
  handshake, because a TLS session keeps the certificates it was made with.
- **So a renewal ends the links made before it**, at the old certificate's end, and the
  link is made again at once with the new one. TLS 1.3 cannot re-authenticate an
  established connection, and keeping the link on the strength of gossip signed with the
  new certificate would mean a link authenticated by something other than its handshake.
  One short link loss per link per renewal; the cluster rides it as it rides any link that
  drops (ADR 0087). With renewal at two thirds of the lifetime, the links of a renewed
  node are a third of a lifetime on the old certificate.
- **The reason labels** are `mqttd_revocation_evictions_total`'s, which the CRL sweep
  already uses: `peer-cert-expired` and `own-cert-expired` for links, `cert-expired` for
  client sessions. The log line is `peer link ended: a certificate it was admitted under
  has expired (ADR 0088)`. Gossip from an expired signer was already dropped and counted
  (`mqttd_gossip_rejected_total{reason="expired"}`, ADR 0022 T7); T3 adds nothing there
  but the tolerance below.
- **A link ended for expiry is a lost link, not an erasure** (ADR 0087): the hub calls
  `peer_disconnected`, which keeps the peer's interest, its `$share` members, its
  negotiated protocol and its standing, and makes no verdict. What follows is
  membership's: the expired node's gossip is dropped, so the failure detector declares it
  dead, `peer_dead` holds it as unreachable, and the hold runs out as for any node that
  does not come back (0087-T2's eight ticks, until 0087-T3 replaces them). That is D4's
  "out of the cluster", reached by the ordinary path.
- **O3's tolerance**: a certificate whose `notBefore` is up to 60 s ahead of the clock is
  accepted, at the peer bus's TLS verifiers (a wrapper that retries a "not valid yet"
  verdict at `now + 60 s`, so the allowance can never extend `notAfter`) and in signed
  gossip (`signed_gossip::NOT_BEFORE_TOLERANCE_SECS`). The client listeners keep
  webpki's strict check: O3 is about cluster credentials. `gen-certs.sh` keeps minting a
  minute back; the two together cover two minutes of skew for a fresh certificate.
- **O2**: `tls.end_sessions_at_cert_expiry` (`MQTTD_TLS_END_SESSIONS_AT_CERT_EXPIRY`,
  `1`/`0`), off by default, read at startup. On, a client session admitted with a
  certificate is evicted the second after its `notAfter`, as a revoked one is (the
  identity sweep's `evict`). A session relocated to another node (ADR 0005) carries no
  certificate there and is not covered; ADR 0040's revocation sweep has the same gap.

Not verified by T3: the end of a link at the expiry of the node's **own** certificate is
shown in the hub's tests and in the two-process test only as "one side or the other ends
it" (whichever side's sweep looks first); a mixed cluster with a peer on v1.1.0, which
would not end the link from its side, was not run.

## Not verified

- T1 did not test what the broker does with a **cluster CA** that is past `notAfter`, or
  with a **CRL** past `nextUpdate`. D2 says they "fail a cluster the same way". The CRL
  does not: `--check-tls` already documented that a stale list still loads, and no code
  path reads `nextUpdate` to refuse one. The CA was not checked either way. Their expiry
  is exported and warned about regardless.
- ~~That nothing ends an established peer link at its peer's certificate expiry was read
  from `crates/mqttd/src/peer.rs`, not shown by a test.~~ Shown, and found worse, by T3
  (amendment of 2026-10-14 below): the link outlived the certificate in both gossip
  postures, and with signed gossip it was dropped by the failure detector and came straight
  back on a resumed TLS session.
- cert-manager's behaviour with lifetimes of hours under the chart's csi-driver layout was
  not run. T2's volume is rendered and compared between the chart and the operator; no
  cert-manager has issued from it. The `fs-group` attribute it sets (the pod's `fsGroup`)
  is from the csi-driver's documentation, not from a run.
- T2's test renews and revokes with SIGHUP as the trigger. The file watcher picking a
  renewed leaf or a new list up was not run for this material (it shares the reload path).
- The cost of gossiping accepted-key identifiers was not measured.
