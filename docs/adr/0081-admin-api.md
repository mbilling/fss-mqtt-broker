# 0081. An authenticated admin API: reads cluster-wide, actions audited, config stays in the file

- **Status:** Accepted
- **Date:** 2026-09-29
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0081-admin-api.md](../delivery/0081-admin-api.md) — plan, progress, and changelog
- **Revisited by:** [ADR 0084](0084-watching-and-editing-rules-live.md) — §3's one verb
  per endpoint, §5's "set configuration" (for the rules file only) and the `$SYS`
  alternative; the rest stands
- **Related:** [ADR 0020](0020-metrics-and-observability.md) (the ops listener and its trust
  model), [ADR 0032](0032-hot-reloadable-security-policy.md) and
  [ADR 0033](0033-config-file-watch-reload.md) (which rejected an admin endpoint as the reload
  trigger), [ADR 0046](0046-file-based-configuration.md) (the file as the source of truth),
  [ADR 0054](0054-operator-facing-state-surface.md) (`/statusz`),
  [ADR 0055](0055-kubernetes-operator.md) (the operator acts through Kubernetes objects),
  [ADR 0062](0062-online-backup-and-restore.md) (signal-driven backup)

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0081-admin-api.md).

## Context

An operator running a cluster has three kinds of command-line work:

- **Day 0 (before first boot):** validate the config (`--check-config`, `--preflight`),
  create password-file lines (`--hash-password`). These exist.
- **Day 1 (bring-up):** is the node live and ready (`--probe`), has it joined, does every
  node agree on the cluster identity. The first exists; the rest means reading `/statusz` on
  each pod and comparing them by eye.
- **Day 2 (steady state and incidents):** which clients are connected and from where, why
  is this session's queue growing, who receives topic T, is user U allowed to publish to T,
  did the reload take, disconnect this misbehaving client, stop routing new connections to
  this node. **None of these has a command.** The broker holds the answers in memory; the
  only way to get them is to guess from metrics or read logs.

The project has refused an admin endpoint several times:

- ADR 0032 and ADR 0033 rejected one **as the reload trigger**: a new network surface that
  must authenticate and authorize or become the weakest link, and a push model that
  competes with the file as the source of truth.
- ADR 0051 lists "no HTTP admin API" among the deliberate absences; ADR 0055 keeps the
  operator on Kubernetes objects and signals.

Those objections still hold for what they rejected: pushing config, and triggers that
signals already cover. They do not address the day-2 gap, which is mostly **reads** of
state no file or signal can expose. `/statusz` cannot absorb them either: it sits on the
unauthenticated ops listener (ADR 0020 §2, ADR 0054), and client ids, usernames, source
addresses and subscriptions are identifying data that must not be served there.

## Decision

### 1. A separate, authenticated admin listener, off by default

- `[admin] bind` / `MQTTD_ADMIN_BIND`. Unset means no admin listener, so an upgrade adds no
  surface. It is never the health or metrics listener. Those stay unauthenticated,
  read-only and unchanged.
- **TLS with a required client certificate** (mTLS), using the broker's single TLS stack
  (ADR 0053; the FIPS option applies unchanged). There is no plaintext mode and no
  anonymous access.
- **Two roles, mapped from the client certificate's subject:** `[admin] viewers` and
  `[admin] operators` (lists of subject DNs). A certificate that matches neither list is
  refused, except a node certificate from the cluster CA, which gets the read-only `peer`
  role: it may read that node's own state and nothing else. `viewer` may call every `GET`;
  `operator` may also call the actions (§4). The role lists hot-reload with the security
  policy (ADR 0032).
- **Every request is audited** to the existing audit log (`docs/AUDIT-SCHEMA.md`): who, the
  role, the endpoint, the target, and the outcome. Reads of client and session detail are
  audited too, because they reveal identifying data.
- Paths are versioned (`/admin/v1/…`). Responses are JSON. Refusals carry a stable reason
  code, as the broker's other refusals do.
- Every list is **bounded and paged** (`limit`, `cursor`). No response grows with the
  number of sessions.

### 2. Reads come first, and any node answers for the cluster

- **Per node:** `GET /admin/v1/node` returns what `/statusz` does, plus the detail
  `/statusz` must never carry.
- **Per cluster:** `GET /admin/v1/cluster` and `/admin/v1/placement` are answered by any
  node. It asks every member's **admin listener** for `/admin/v1/node`, presenting its
  cluster certificate (admitted there as the `peer` role, which can read only that
  node's own state), and merges the answers. Every node's row states whether it replied,
  and if not, why; a partitioned node shows up as silent rather than as healthy. A peer
  without an admin listener, or a node without cluster TLS, is listed as "not
  queryable". (Amended 2026-09-29: originally over the peer bus; see the amendment.)
- **Clients and sessions:** list (filter by client id prefix, username, source address;
  paged), one session in detail (subscriptions, inflight, queue depth and limits, will,
  expiry, owning node), the sessions matching a topic filter, the top N sessions by
  backlog, and retained messages by topic prefix (count, size, list).
- **Authorization dry run:** `GET /admin/v1/authz?user=…&action=publish|subscribe|connect&target=…`
  evaluates the **loaded** policy and returns the verdict and the rule that decided it. It
  changes nothing.
- **Effective config:** `GET /admin/v1/config` returns the merged config (file, env,
  defaults) with every secret replaced by a fingerprint (the ADR 0054 rule), plus a hash,
  so a GitOps check can compare the running config with the committed one.

### 3. A CLI client in the same binary

`mqttd --admin <verb> [args]` calls a running broker's admin API, the same way `--probe`
calls the health endpoint. It works in the distroless image (no shell, no curl) and
through `kubectl exec`. The URL and the client certificate come from flags or the
`MQTTD_ADMIN_*` environment. Output is a table on a terminal and JSON with `--json`. The
verbs follow the endpoints one to one; the CLI has no logic of its own.

> **Revisited by [ADR 0084](0084-watching-and-editing-rules-live.md) (2026-10-08).** The
> rules endpoints' per-rule `PUT` and `test` take structured JSON bodies and have no verb;
> `rules-apply` reads a local file. Every other verb still follows its endpoint.

### 4. Actions: a short, fixed list, each also audited on the node it acts on

Operator role only:

| Action | Effect | Why here and not a signal |
|---|---|---|
| `reload` | Runs the ADR 0032 reload routine, the one `SIGHUP` runs, and **returns the outcome**: which components changed, or why the new policy was rejected. | Today the result is only in the log. The file is still the only input. |
| `kick <client>` | Disconnects the client: MQTT 5 `DISCONNECT` with `0x98` (Administrative action); 3.1.1 closes the connection. The session remains. | A signal cannot name a client. |
| `purge <client>` | Deletes the persistent session (subscriptions and queue) cluster-wide, disconnecting the client first. | As above. |
| `cordon` / `uncordon` | Stops accepting new connections and reports not-ready on `/readyz`; existing sessions stay connected. Not persisted, so a restart clears it. `/statusz` shows it. | Draining without decommissioning had no control. |
| `log-level <filter> <ttl>` | Replaces the tracing filter until the TTL expires (at most one hour), then restores the configured one. `/statusz` shows the override. | The filter is fixed at startup today. |

An action on a client connected to another node is **forwarded** to that node's admin
listener with the caller's identity and audited on both nodes. (Amended 2026-09-29:
originally over the peer bus.)

### 5. What the API will not do

- **Set configuration.** The file (or the operator's rendered ConfigMap) stays the only
  source of configuration (ADR 0046). An API that sets parameters recreates the drift ADR
  0033 rejected: a value set through the API is lost on restart or overwritten by the next
  reconcile, and many parameters (listeners, storage, placement, the replication factor's
  committed value) cannot change on a running node at all. Changes go through the file,
  then `reload`, and `reload` returns the result. The single exception is the expiring log
  filter (§4), which is diagnostic state rather than configuration.

  > **Revisited by [ADR 0084](0084-watching-and-editing-rules-live.md) (2026-10-08).** One
  > more exception, off by default: a subject listed in `[rules] admin_writers` may replace
  > the rules file, or one rule in it, through the API. The write goes to the file itself and
  > is applied by the ordinary reload, so the file stays the only source. No other
  > configuration is written, and there is still no admin web UI.
- **Replace the signals.** `--decommission` and `--backup` stay signal-driven; decommission
  has to block a `preStop` hook until the process exits, which an HTTP call cannot do.
  The API reports their progress.
- **Change the operator.** ADR 0055 stands: the operator acts through Kubernetes objects and
  reads `/statusz`.
- **Serve a dashboard.** There is no admin web UI, now or as a later task. The API and the
  CLI are the operator surface. A browser-facing UI would add login sessions and
  CSRF/XSS exposure to the admin plane and a frontend to maintain, and for metrics it
  would duplicate Grafana (ADR 0020). Anyone who wants a dashboard can build one on the
  JSON API, whose compatibility is versioned (`/admin/v1/`). Decided 2026-10-01.
- **Delete retained messages** in bulk. It is easy to delete far more than intended, and
  retained topics can be cleared by publishing an empty retained message. Deferred until
  there is a demonstrated need and a dry-run design.

## Consequences

- Day 1 and day 2 get answers that currently need log reading, and the CLI makes them
  available where an operator already is (`kubectl exec`, a terminal on the host).
- The broker gains an authenticated network surface. It is off by default, mTLS-only and
  audited, but it is new attack surface: `docs/THREAT-MODEL.md` and `docs/HARDENING.md`
  gain an entry for it before it ships.
- The earlier "no admin API" statements (ADR 0032, 0033 and 0051 alternatives,
  `docs/COMPARISON.md`) become partly out of date. On acceptance each gets a note
  pointing here: the reasons they gave still hold for pushing config, and this record keeps
  that rejection.
- The peer protocol is unchanged: node-to-node admin traffic uses the admin listeners
  (amendment below).
- Endpoint paths and JSON field names become a compatibility surface. The `[admin]` keys
  fall under ADR 0058's frozen config surface. ADR 0058 does not enumerate HTTP interfaces;
  this one follows mqttd's semantic version (decided 2026-09-30 — the line first said
  "unstable until 1.0", but the API shipped after v1.0.0): a patch release does not change
  it, a minor release only adds to it, and only a major release may break it, with the path
  version following the major (`/admin/v2` in mqttd 2.x).

## Alternatives considered

- **Extend `/statusz`.** Rejected: it is unauthenticated by design, and client-level
  detail is identifying data.
- **Read-write config through the API.** Rejected (§5): a second source of configuration
  that the file and the operator would both fight.
- **MQTT `$SYS` topics.** A familiar pattern, but it puts operator data on the client
  listener under the client authorization model, cannot page, and does not answer
  questions such as "who matches topic T" or "is U allowed to publish".

  > **Revisited by [ADR 0084](0084-watching-and-editing-rules-live.md) (2026-10-08).** The
  > admin data stays here. `$SYS/` is now reserved for the broker, which publishes opt-in
  > per-rule statistics and an opt-in rule trace there, under the ACL; the admin API serves
  > the same rule data to its roles.
- **Bearer tokens (OIDC, ADR 0050) instead of mTLS.** Useful for human operators behind SSO;
  deferred as a second authenticator on the same role model. mTLS comes first because
  it needs no external service and matches how the cluster already authenticates peers.
- **Per-node API only, with no fan-out.** Simpler, but the cluster view is the most
  requested read, and assembling it from outside needs a route to every pod.

## Amendment (2026-09-29): node-to-node admin traffic uses the admin listeners

§2 and §4 first sent the cluster query and forwarded actions over the peer bus, as new
frames behind the next peer protocol version. While T3 was being built, ADR 0080 T1 was
raising `PROTO_MAX` to 9 for the replication factor. Two unrelated features bumping the same
per-link version would have to be sequenced against each other, and the admin plane would
share a version gate, a codec and a failure domain with message delivery.

Instead, each node reaches its peers' admin listeners over mTLS:

- The admin listener trusts the cluster CA as well as the admin client CA. A certificate
  the cluster CA issued that is in no role list gets the `peer` role, decided by
  re-verifying the chain against the cluster CA alone, never by subject. `peer` can read
  only the node's own state (`/admin/v1/node`, `/admin/v1/whoami`) and, from T7, receive
  forwarded actions.
- A node finds a peer's admin listener at the host of the peer's cluster-bus address and
  `admin.peer_port` (`MQTTD_ADMIN_PEER_PORT`, default: the port of `admin.bind`).
- It verifies the peer's admin server certificate against the admin client CA or the
  cluster CA. The simplest setup serves each node's admin listener with the node's cluster
  certificate, which already names the node and chains to the cluster CA.

What this costs: every node that should appear in the cluster view needs its admin
listener on, and a node answers for the cluster only if it has cluster TLS (its peers are
otherwise listed as not queryable). What it avoids: a peer protocol bump, and any coupling
between the admin plane and the data plane.
