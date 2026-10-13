---
adr: "0088"
title: "Cluster credentials have a lifetime the deployer sets, the broker enforces it strictly, and whoever operates the cluster rotates in time"
adr_status: Accepted
tasks:
  - id: 0088-T1
    title: "D2: expiry is visible — a gauge per loaded certificate (seconds to notAfter, fraction of lifetime spent), CA and CRL nextUpdate included; a rate-bounded warning past the threshold and an error at expiry; the chart's alert; the same threshold in --check-tls and --check-config"
    status: planned
    issue: 988
    notes: "Size S-M. mqtt-observability, mqttd (tls_check.rs, reload.rs, the periodic tick), the chart's PrometheusRule (re-vendor the mqttui bundle), OPERATIONS. No behaviour change: safe first. O8: two thirds of the lifetime."
  - id: 0088-T2
    title: "D1, D5: the lifetime is stated at deployment — gen-certs.sh takes it explicitly and prints the renew-by date, gains renew and revoke; the chart and the operator expose lifetime and renewal lead and pass them to cert-manager; decommission ends with revoke, in the procedure and in what mqttd --decommission prints"
    status: planned
    issue: 989
    notes: "Size M. deploy/systemd/gen-certs.sh and its README, deploy/helm (values, bootstrap.sh, the cert-manager example), mqttd-operator CRD, OPERATIONS, HARDENING. O1 decided: it refuses without a stated lifetime. The scale rig mints its PKI with gen-certs.sh: keep it working."
  - id: 0088-T3
    title: "D3, D4: expiry is enforced on established peer links — a link whose peer's certificate (or this node's own) has passed notAfter is ended within a minute, as a revoked peer's is; no grace; notBefore tolerance per O3; a test that first shows today's link outliving its certificate"
    status: planned
    issue: 990
    notes: "Size M. mqttd/src/peer.rs (the link already records what the handshake presented for the revocation sweep), reload.rs. Starts with the characterisation test ADR 0088 'Not verified' asks for. Interacts with ADR 0087 (a link ended for expiry is a lost link: the peer is unreachable, not erased). O3: 60 s on notBefore, none on notAfter. O2 decided: client sessions join, behind a setting off by default."
  - id: 0088-T4
    title: "D7: the gossip key and the accepted keys are re-read by the file watcher and SIGHUP; a replaced key takes effect on the next datagram; the three-phase procedure becomes three file replacements"
    status: planned
    issue: 991
    notes: "Size S-M. mqttd main.rs (watched_policy_paths, the SwimAuth construction), reload.rs, mqtt-cluster swim_auth.rs (swap the keys under the driver), OPERATIONS. Useful by itself and the base of T5."
  - id: 0088-T5
    title: "D8, D9, D10: the keyring — at most two keys with identifiers, the key identifier on the datagram, members gossip the identifiers they accept, a node seals with the newest key every current member accepts; the superseded key is evicted by the broker after a bounded overlap whether or not the file still lists it, and never accepted again; metrics, --check-config and log lines for the keys accepted and sealed with"
    status: planned
    issue: 992
    notes: "Size L. mqtt-cluster (swim_auth.rs, swim.rs gossip of accepted identifiers, the datagram form: additive, the v1.1.0 single-key form still spoken; pinned bytes, the fuzz target), mqtt-config, mqttd. The formation simulation (swim_formation_sim.rs) gains a key change under formation, a late node, and a node holding only an evicted key. O4: the keyring. O6: ten minutes by default. Mixed-version rule: a cluster with a v1.1.0 member cannot leave the single-key form."
  - id: 0088-T6
    title: "D9 on Kubernetes: the operator's rotation controller — the keyring Secret written on a schedule and when a pod leaves for good, gated on every pod reporting the new key accepted; node certificates through cert-manager with the configured lifetime; a kind test that rotates both under publish load with no member lost"
    status: planned
    issue: 993
    notes: "Size L. mqttd-operator (controller, CRD), the chart, the nightly kube-smoke job. Supersedes ADR 0055 T5 (deferred). O5 decided: daily."
  - id: 0088-T7
    title: "D9 elsewhere, and the evidence: the documented procedure for a person (certificates and key), the same on the scale rig — a run that renews every node's certificate and replaces the gossip key under load with a ledger, and a node returning with an expired certificate and an evicted key staying out"
    status: planned
    issue: 994
    notes: "Size M, plus rig time. deploy/systemd README, OPERATIONS, HARDENING, THREAT-MODEL rows (a departed node's credentials; a missed rotation), bench/scale (a rotation step between rungs). Last: it certifies the rest."
---

# Delivery 0088 — credential lifetimes and rotation

**ADR:** [docs/adr/0088-credential-lifetimes-and-rotation.md](../adr/0088-credential-lifetimes-and-rotation.md)

The plan, progress, and changelog for ADR 0088. Task status lives in the frontmatter
above; the table below is generated from it.

The direction was decided on 2026-10-13: the deployer sets a certificate lifetime, whoever
operates the cluster rotates in time, the broker issues nothing and enforces expiry
strictly; the gossip key is replaced without a restart, with two keys alive only while one
replaces the other, and old keys are evicted. Every sub-decision is settled and the ADR was accepted the
same day.

Order: T1 first (it changes no behaviour and is what makes a short lifetime safe to
adopt), then T2 and T3 (certificates: stated, renewable, revocable, enforced), T4 (the key
reloads), T5 (the keyring and eviction), T6 (the operator), T7 (the procedure and the rig).
T1–T4 are useful each by itself.

Each task has its own issue; #984 tracks the record as a whole.

## Plan

| Task | Follows | Acceptance |
|---|---|---|
| **0088-T1** Expiry is visible (D2) | — | Every loaded certificate, the cluster CA and the CRL export their time to expiry; a node whose certificate is past the threshold warns at a bounded rate and the chart's alert fires in its test; `--check-tls` and `--check-config` report the same threshold; nothing else changes. |
| **0088-T2** Stated, renewable, revocable (D1, D5) | — | `gen-certs.sh` mints nothing without a lifetime that was chosen or announced, prints the renew-by date, and `renew` and `revoke` produce material the broker accepts and reloads live (tested against a running broker); the chart and the operator pass lifetime and renewal lead to cert-manager; the decommission procedure and `mqttd --decommission` end with revoke. |
| **0088-T3** Expiry enforced on links (D3, D4) | T1 | A test shows a peer link outliving its peer's certificate on today's code, and the same test shows the link ended within a minute of `notAfter` afterwards; an expired node's gossip is dropped and it cannot dial or be dialled; no setting keeps it in; a link ended for expiry leaves the peer unreachable, not erased (ADR 0087). |
| **0088-T4** The key reloads (D7) | — | Replacing `key_file` or the accepted keys on disk changes what a running node seals with and accepts, with no restart and no datagram lost to the change in a three-node test; the documented rotation is three file replacements. |
| **0088-T5** Keyring and eviction (D8, D9, D10) | T4 | One write adds a key and the cluster moves to it by itself, a late node delaying the switch instead of being cut off (formation simulation, 3–30 nodes); after the bounded overlap no node accepts the superseded key whether or not its file still lists it, and a datagram sealed with it is dropped and counted; a third key is refused at load; a v1.1.0 member keeps the cluster on the single-key form; metrics and `--check-config` name the keys by identifier and never by value. |
| **0088-T6** The operator rotates (D9) | T2, T5 | In kind, under publish load: node certificates are renewed through cert-manager at the configured lifetime and the gossip key is replaced on schedule and after a scale-down, each step gated on the brokers' metrics; no member is lost and no acknowledged message is lost. |
| **0088-T7** Procedure and rig (D9) | T3, T5 | A person can rotate both from the documented steps alone; on the rig a ledgered run renews every certificate and replaces the key between rungs with nothing lost, and a node brought back with an expired certificate and an evicted key stays out until re-provisioned. |

## Progress

<!-- status-table:0088 -->
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0088-T1 | ⬜ planned | [#988](https://github.com/mbilling/fss-mqtt-broker/issues/988) | — | "Size S-M. mqtt-observability, mqttd (tls_check.rs, reload.rs, the periodic tick), the chart's PrometheusRule (re-vendor the mqttui bundle), OPERATIONS. No behaviour change: safe first. O8: two thirds of the lifetime." |
| 0088-T2 | ⬜ planned | [#989](https://github.com/mbilling/fss-mqtt-broker/issues/989) | — | "Size M. deploy/systemd/gen-certs.sh and its README, deploy/helm (values, bootstrap.sh, the cert-manager example), mqttd-operator CRD, OPERATIONS, HARDENING. O1 decided: it refuses without a stated lifetime. The scale rig mints its PKI with gen-certs.sh: keep it working." |
| 0088-T3 | ⬜ planned | [#990](https://github.com/mbilling/fss-mqtt-broker/issues/990) | — | "Size M. mqttd/src/peer.rs (the link already records what the handshake presented for the revocation sweep), reload.rs. Starts with the characterisation test ADR 0088 'Not verified' asks for. Interacts with ADR 0087 (a link ended for expiry is a lost link: the peer is unreachable, not erased). O3: 60 s on notBefore, none on notAfter. O2 decided: client sessions join, behind a setting off by default." |
| 0088-T4 | ⬜ planned | [#991](https://github.com/mbilling/fss-mqtt-broker/issues/991) | — | "Size S-M. mqttd main.rs (watched_policy_paths, the SwimAuth construction), reload.rs, mqtt-cluster swim_auth.rs (swap the keys under the driver), OPERATIONS. Useful by itself and the base of T5." |
| 0088-T5 | ⬜ planned | [#992](https://github.com/mbilling/fss-mqtt-broker/issues/992) | — | "Size L. mqtt-cluster (swim_auth.rs, swim.rs gossip of accepted identifiers, the datagram form: additive, the v1.1.0 single-key form still spoken; pinned bytes, the fuzz target), mqtt-config, mqttd. The formation simulation (swim_formation_sim.rs) gains a key change under formation, a late node, and a node holding only an evicted key. O4: the keyring. O6: ten minutes by default. Mixed-version rule: a cluster with a v1.1.0 member cannot leave the single-key form." |
| 0088-T6 | ⬜ planned | [#993](https://github.com/mbilling/fss-mqtt-broker/issues/993) | — | "Size L. mqttd-operator (controller, CRD), the chart, the nightly kube-smoke job. Supersedes ADR 0055 T5 (deferred). O5 decided: daily." |
| 0088-T7 | ⬜ planned | [#994](https://github.com/mbilling/fss-mqtt-broker/issues/994) | — | "Size M, plus rig time. deploy/systemd README, OPERATIONS, HARDENING, THREAT-MODEL rows (a departed node's credentials; a missed rotation), bench/scale (a rotation step between rungs). Last: it certifies the rest." |
<!-- /status-table:0088 -->

## Changelog

- **2026-10-13** — Accepted by the maintainer, with O3, O4, O6's duration and O8 as
  proposed. One issue per task opened (#988–#994).
- **2026-10-13** — Sub-decisions settled by the maintainer: O1 the script refuses without a
  stated lifetime; O2 a client session ends at its certificate's expiry, behind a setting off
  by default; O5 the gossip key is replaced daily; O7 a cluster identity on the peer link,
  as its own record.
- **2026-10-13** — Record and plan written. Direction decided by the maintainer the same
  day: the operator (Kubernetes or a person) rotates; expiry is strict; two gossip keys are
  alive only during a change and old keys are evicted. No code.
