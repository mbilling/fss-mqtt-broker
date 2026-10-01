# The admin API

**Verified against `main` after `v1.0.18` (2026-09-30)**, every response below captured from a
live three-node cluster (`scripts/admin-e2e.sh`). The command-line client for it is
[ADMIN-CLI.md](ADMIN-CLI.md); the decision record is [ADR 0081](adr/0081-admin-api.md).

An authenticated HTTPS API on its own listener, for the questions `/statusz` cannot answer
(which clients are here, why is this queue growing, why is this client denied) and a short,
fixed list of audited actions. **It never writes configuration**: the config file stays the
only source, and `reload` re-reads it.

- [Turning it on](#turning-it-on)
- [Authentication and roles](#authentication-and-roles)
- [Conventions](#conventions) · [Errors](#errors) · [Limits](#limits) · [Audit](#audit)
- [The cluster view](#the-cluster-view) · [Actions on a client: where they run](#actions-on-a-client-where-they-run)
- [Endpoint reference](#endpoint-reference)
- [Compatibility](#compatibility)

## Turning it on

The listener is **off unless `admin.bind` is set**, and it is never the health or metrics
listener (the config refuses the same address).

```toml
[admin]
bind = "0.0.0.0:9443"
cert = "/etc/mqttd/admin/server.pem"      # this listener's server certificate
key = "/etc/mqttd/admin/server.key"
client_ca = "/etc/mqttd/admin/ca.pem"     # issues admin client certificates
viewers = ["CN=oncall"]                   # every read
operators = ["CN=sre-lead, O=example"]    # reads + actions
# peer_port = 9443                        # other nodes' admin port, if not the same as ours
```

| Key | Environment | Notes |
|---|---|---|
| `admin.bind` | `MQTTD_ADMIN_BIND` | unset = no admin API |
| `admin.cert`, `admin.key` | `MQTTD_ADMIN_CERT`, `MQTTD_ADMIN_KEY` | required with `bind`; restart-scoped |
| `admin.client_ca` | `MQTTD_ADMIN_CLIENT_CA` | required with `bind`; restart-scoped. Use a dedicated admin CA (see below) |
| `admin.viewers` | `MQTTD_ADMIN_VIEWERS` (`;`-separated) | hot-reloadable |
| `admin.operators` | `MQTTD_ADMIN_OPERATORS` (`;`-separated) | hot-reloadable |
| `admin.peer_port` | `MQTTD_ADMIN_PEER_PORT` | default: the port of `admin.bind` |

The config is refused if `bind` is set without the certificate, key and CA, or with both role
lists empty. A reload that changes `bind`, `cert`, `key`, `client_ca` or `peer_port` logs
`admin` among the requires-restart sections; the role lists apply to the next request.

**On a cluster**, run the admin listener on every node (same port, or set `peer_port`) and
give the node `[cluster.peer_tls]`. The simplest server certificate is the node's own cluster
certificate — `admin.cert = <peer_tls.cert>`, `admin.key = <peer_tls.key>` — because it already
names the node and chains to the cluster CA, which is what [the cluster view](#the-cluster-view)
needs.

**On Kubernetes**, the Helm chart's `admin` values and the operator's `spec.admin` wire exactly
that on every pod, on port 9443 of the headless Service. See
[the chart README](../deploy/helm/mqttd/README.md#admin-api).

## Authentication and roles

TLS 1.3 with a **required client certificate**; there is no plaintext mode, no anonymous access,
and no session resumption (every connection is fully verified). The certificate must chain to
`admin.client_ca` or, on a cluster node, to the cluster CA. Its subject then decides the role:

| Role | Granted to | May call |
|---|---|---|
| `operator` | a subject in `admin.operators` | everything |
| `viewer` | a subject in `admin.viewers` | every `GET` |
| `peer` | a certificate the **cluster CA** issued whose subject is in neither list | `whoami`, `node`, and forwarded `kick`/`purge` and `clients`/`session`/`subscribers` reads (below) — a node's own state, so any node can answer for the cluster |
| none | anyone else | nothing: `403 forbidden` |

A list entry is either the whole subject (`CN=sre-lead, O=example`, spaces after commas
ignored) or `CN=<name>`, which matches any subject with that Common Name. `operators` wins
when a subject is in both. The `peer` role is granted by re-verifying the chain against the
cluster CA alone, never by subject. **Use a dedicated admin CA**: if `client_ca` is the
cluster CA, every unlisted certificate it issued is admitted as `peer` instead of refused (the
broker warns at startup).

## Conventions

- **Paths** are versioned: `/admin/v1/…`.
- **Methods**: reads are `GET`; actions are `POST` with no body.
- **Parameters** are query parameters, percent-encoded. A `+` is a literal `+` (not a space):
  encode a space as `%20`.
- **Responses** are JSON, with `Content-Type: application/json` and `Cache-Control: no-store`.
- **One request per connection** (`Connection: close`); chunked request bodies are refused.
- **Lists are paged**: `limit` (default 100, at most 1000) and `cursor` (the previous page's
  `next_cursor`; `null` on the last page).

```text
$ curl -i --cacert cluster-ca.pem --cert oncall.pem --key oncall.key https://mqttd-1:19443/admin/v1/whoami
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 84
Cache-Control: no-store
Connection: close

{"cn":"oncall","node_id":"mqttd-1","role":"viewer","subject":"CN=oncall, O=example"}
```

## Errors

Every refusal is `{"error":{"code":"…","message":"…"}}`. Match on `code`; the message is for
people.

| Status | `code` | When |
|---|---|---|
| 400 | `bad-request` | a missing or malformed parameter, a wildcard where a topic name is needed, an invalid filter, an unreadable request |
| 403 | `forbidden` | the certificate's subject is in no role list, or its role is below the endpoint's |
| 404 | `not-found` | no such endpoint, or no such session / connection |
| 405 | `method-not-allowed` | a known path with the wrong method |
| 409 | `reload-rejected` | the reloaded config or policy was refused; the running one is kept (the body also carries `outcome`) |
| 413 | `too-large` | the request exceeds the size limits |
| 503 | `unavailable` | a part of the node the endpoint needs is not running or not wired |
| 503 | `timeout` | the answer took longer than 30 s |

```text
HTTP/1.1 403 Forbidden

{"error":{"code":"forbidden","message":"this endpoint needs the operator role; this certificate has viewer"}}
```

## Limits

| Limit | Value |
|---|---|
| time to send the request after the TLS handshake | 10 s |
| request head / body | 16 KiB / 64 KiB |
| concurrent connections | 32 (more wait in the accept backlog) |
| time to answer | 30 s |
| rows per page | 1000 |
| cluster view, per peer | 3 s |

Listing and ranking (`clients`, `backlog`, `subscribers`) visit every session on the node once
per request on the hub's loop — milliseconds for tens of thousands of sessions. On a node
with millions, narrow with `prefix` and a small `limit`.

## Audit

Every request, reads and refusals included, is one `admin.request` record in the
hash-chained audit log ([AUDIT-SCHEMA.md](AUDIT-SCHEMA.md)), with the certificate subject:

```text
kind="admin.request" subject="CN=root, O=example"  role=operator POST /admin/v1/reload -> 200
```

The target is re-encoded, so a record is always one line. A forwarded action is recorded on
both nodes; the receiving node's record names the peer, and the target carries
`forwarded_for=<the operator's subject>`. A reload is also recorded as `security.reload`
with `trigger=admin`.

## The cluster view

`GET /admin/v1/cluster` and `/placement` answer for every node, from any node. The node you ask
reads its membership, then asks each member's admin listener for `/admin/v1/node` in
parallel, presenting its **cluster certificate** (admitted there as `peer`):

- it reaches a member at the host of the member's cluster-bus address and `admin.peer_port`;
- it verifies the member's server certificate against the admin client CA or the cluster CA;
- a member that does not answer is a row with `replied: false` and the reason;
- a node without cluster TLS lists its peers as not queryable.

`peer` cannot call `/cluster` or `/placement`, so the fan-out never recurses. This traffic
uses the admin listeners, not the cluster bus: the admin plane shares no protocol version
and no failure with message delivery (ADR 0081, 2026-09-29 amendment).

## Cluster-wide reads: `scope=cluster`

`clients`, `session` and `subscribers` answer for the node you ask. Add `scope=cluster`
(CLI: `--all-nodes`) and that node answers for every node instead, so you do not need to
know which node holds a client. It asks each member's admin listener for the same request
in parallel, as the cluster view does, presenting its cluster certificate and adding
`forwarded_for`, `forwarded_role` and `forwarded_from`. Then it merges the answers:

- `clients`: every node's sessions by client id, each row with its `node`. Paging works
  across the cluster: `next_cursor` is a client id, and `matched` is the sum over the nodes
  that answered. A client id held on two nodes is never split across pages, so a page can
  hold a row or two more than `limit`.
- `session`: the connected copy, else the placement owner's, else the first by node, with
  `found_on` listing every node that holds one. A `404` names every node asked, and any
  that did not answer.
- `subscribers`: every node's subscribers to the topic, each with its `node`.

Every cluster-scope answer adds `scope: "cluster"`, `answered_by` and `nodes[]`: one row
per node with `replied`, and `error` when it did not answer. A partial answer is
therefore never mistaken for a complete one. Any other `scope` value is `400`.

The `peer` role may read these three only with `forwarded_for` set, and a forwarded read
is always answered at node scope, so the fan-out never recurses.

```json
{"scope": "cluster", "answered_by": "mqttd-1", "matched": 2, "next_cursor": null,
 "sessions": [{"client_id": "dev-on-1", "node": "mqttd-1", "connected": true, "…": "…"},
              {"client_id": "dev-on-3", "node": "mqttd-3", "connected": true, "…": "…"}],
 "nodes": [{"node_id": "mqttd-1", "replied": true}, {"node_id": "mqttd-2", "replied": true},
           {"node_id": "mqttd-3", "replied": true}]}
```

## Actions on a client: where they run

A persistent session lives on its placement owner (a relocated connection is proxied there),
but a clean session stays on the node the client connected to. So `kick` and `purge` on any
node:

1. act on what that node holds;
2. otherwise forward to the session's placement owner, then to every other member, under
   the node's cluster certificate, adding `forwarded_for`, `forwarded_role` and
   `forwarded_from` to the query;
3. relay the first answer that acted (with `forwarded_to`), or `404` naming every node asked.

The `peer` role may call `kick` and `purge` only with `forwarded_for` set, and a forwarded
request is never forwarded again. A non-owner that holds nothing never removes a stored
session.

## Endpoint reference

| Method | Path | Role | CLI |
|---|---|---|---|
| GET | [`/admin/v1/whoami`](#get-adminv1whoami) | peer | `whoami` |
| GET | [`/admin/v1/node`](#get-adminv1node) | peer | `node` |
| GET | [`/admin/v1/cluster`](#get-adminv1cluster) | viewer | `cluster` |
| GET | [`/admin/v1/placement`](#get-adminv1placement) | viewer | `placement` |
| GET | [`/admin/v1/clients`](#get-adminv1clients) | viewer (peer when forwarded) | `clients` |
| GET | [`/admin/v1/session`](#get-adminv1session) | viewer (peer when forwarded) | `session` |
| GET | [`/admin/v1/subscribers`](#get-adminv1subscribers) | viewer (peer when forwarded) | `subscribers` |
| GET | [`/admin/v1/backlog`](#get-adminv1backlog) | viewer | `backlog` |
| GET | [`/admin/v1/retained`](#get-adminv1retained) | viewer | `retained` |
| GET | [`/admin/v1/authz`](#get-adminv1authz) | viewer | `authz` |
| GET | [`/admin/v1/config`](#get-adminv1config) | viewer | `config` |
| POST | [`/admin/v1/reload`](#post-adminv1reload) | operator | `reload` |
| POST | [`/admin/v1/kick`](#post-adminv1kick-post-adminv1purge) | operator (peer when forwarded) | `kick` |
| POST | [`/admin/v1/purge`](#post-adminv1kick-post-adminv1purge) | operator (peer when forwarded) | `purge` |
| POST | [`/admin/v1/cordon`](#post-adminv1cordon-post-adminv1uncordon) | operator | `cordon` |
| POST | [`/admin/v1/uncordon`](#post-adminv1cordon-post-adminv1uncordon) | operator | `uncordon` |
| GET | [`/admin/v1/log-level`](#get-adminv1log-level) | viewer | `log-level` |
| POST | [`/admin/v1/log-level`](#post-adminv1log-level) | operator | `log-override` |
| POST | [`/admin/v1/log-level/reset`](#post-adminv1log-levelreset) | operator | `log-reset` |

### `GET /admin/v1/whoami`

The caller as this node sees it.

```json
{"cn":"oncall","node_id":"mqttd-1","role":"viewer","subject":"CN=oncall, O=example"}
```

### `GET /admin/v1/node`

This node's state: the `/statusz` body (identity, version, readiness, members, lease,
replication, store, brownout, backup, config checksum, protocol range), plus `cordon` and
`log_filter` when active. The fields are those of `/statusz`
([ADR 0054](adr/0054-operator-facing-state-surface.md)).

### `GET /admin/v1/cluster`

Every node, from any node ([the cluster view](#the-cluster-view)).

| Field | Meaning |
|---|---|
| `answered_by` | the node that answered |
| `summary.nodes`, `.replied`, `.ready` | how many members, how many answered, how many are ready |
| `summary.same_cluster_id` | every replying node reports the same cluster identity (the split-brain check) |
| `summary.same_version`, `.same_config`, `.same_membership` | convergence checks across replying nodes |
| `nodes[]` | one row per member (below) |

A row: `node_id`, `replied` (and `error` when not), `admin_addr`, `elapsed_ms`, `version`,
`ready`, `live`, `cluster_id`, `members` (how many it sees), `lease_leader`, `lease_epoch`,
`replica_lag_groups`, `under_replicated`, `brownout`, `quarantined`, `swim_isolated`,
`decommissioning`, `config_checksum`, `proto_max`.

```json
{
  "answered_by": "mqttd-1",
  "summary": {"nodes": 3, "replied": 3, "ready": 3, "same_cluster_id": true,
              "same_config": true, "same_membership": true, "same_version": true},
  "nodes": [
    {"node_id": "mqttd-1", "replied": true, "elapsed_ms": 0, "version": "1.0.18", "ready": true,
     "live": true, "cluster_id": "f8995cf8f6cd90e4ab4a321e17c07585", "members": 3,
     "lease_leader": true, "lease_epoch": 1, "replica_lag_groups": 0, "under_replicated": false,
     "brownout": false, "quarantined": false, "swim_isolated": false, "decommissioning": null,
     "config_checksum": "545674d7…", "proto_max": 9},
    {"node_id": "mqttd-2", "admin_addr": "mqttd-2:9443", "replied": true, "elapsed_ms": 44, "…": "…"}
  ]
}
```

### `GET /admin/v1/placement`

This node's placement view and whether the others agree: `members[]` (`id`, `addr`,
`failure_domain`), `replication` (`desired`, `min_actual`, `under_replicated`, `write_floor`,
`write_floor_source`), `lease` (`leader`, `epoch`, `group_ready`, `ownership_domain`,
`replica_groups`), and `views[]` — for each other node: `node_id`, `replied`,
`same_membership`, `members` (or `error`).

### `GET /admin/v1/clients`

Sessions on this node, by client id; with `scope=cluster`, on every node
([cluster-wide reads](#cluster-wide-reads-scopecluster)).

| Parameter | Meaning |
|---|---|
| `scope` | `node` (default) or `cluster` |
| `prefix` | client ids starting with this |
| `user` | connected clients authenticated as exactly this principal |
| `source` | connected clients whose `ip:port` starts with this |
| `limit`, `cursor` | paging |

Returns `sessions[]`, `next_cursor`, and `matched` (the total across pages). A session:

| Field | Meaning |
|---|---|
| `client_id` | the client id |
| `connected` | a connection is attached now |
| `user`, `auth`, `protocol`, `source`, `connected_secs` | connected clients: principal, auth method (`anonymous`, `password`, `token`, `certificate`, `enhanced`), `3.1.1` or `5`, source address (absent for a relocated session), seconds since attach |
| `persistent`, `expiry_secs`, `expires_at` | survives disconnect; Session Expiry Interval (`4294967295` = never); when a disconnected session expires (Unix seconds) |
| `subscriptions`, `inflight`, `backlog`, `backlog_bytes` | counts: subscriptions held, QoS>0 messages sent and unacknowledged, messages waiting for Receive Maximum quota and their bytes |

```json
{
  "matched": 2,
  "next_cursor": null,
  "sessions": [
    {"client_id": "sensor-7", "connected": true, "user": "anonymous", "auth": "anonymous",
     "protocol": "5", "source": "192.168.65.1:53942", "connected_secs": 4, "persistent": false,
     "expiry_secs": null, "expires_at": null, "subscriptions": 1, "inflight": 0, "backlog": 0,
     "backlog_bytes": 0}
  ]
}
```

### `GET /admin/v1/session`

One session. Parameters: `client` (required), `scope` (`cluster` finds it on whichever node
holds it). Every [`clients`](#get-adminv1clients) field, plus:

| Field | Meaning |
|---|---|
| `subscription_list[]` | `filter` (as subscribed, `$share/<group>/` included), `qos`, `shared_group`, `no_local`, `retain_as_published`, `subscription_id` — at most 1000; `subscription_list_truncated` says if there are more |
| `inflight_states` | in-flight messages by state: `awaiting_puback`, `awaiting_pubrec`, `awaiting_pubcomp`, `completed_qos2_cleanup`, `staged` |
| `receive_maximum` | the client's Receive Maximum |
| `will` | `topic`, `qos`, `retain`, `payload_bytes`, `delay_secs` — never the payload |
| `will_due_in_secs` | seconds until a delayed Will is published |
| `node`, `owner_node` | the node that answered; where placement puts the session |
| `queued`, `queued_capped` | a disconnected persistent session's stored messages, counted up to 10 000 (`queued_error` if the store could not be read) |

`404 not-found` when this node holds no such session; the message names the owner. With
`scope=cluster`, `found_on` lists every node holding a copy.

```json
{
  "client_id": "sensor-7", "connected": true, "protocol": "5", "node": "mqttd-1",
  "owner_node": "mqttd-2", "receive_maximum": 20, "inflight_states": {},
  "subscription_list": [{"filter": "plant/+/temp", "qos": 1, "shared_group": null,
                         "no_local": false, "retain_as_published": false,
                         "subscription_id": null}],
  "subscription_list_truncated": false, "will": null, "will_due_in_secs": null, "…": "…"
}
```

### `GET /admin/v1/subscribers`

Who on this node receives a publish to a topic. Parameters: `topic` (required, a topic name —
`+` or `#` is `400`), `limit`, `scope` (`cluster`: on every node, each row with its `node`). Returns `topic`, `truncated`, and `subscribers[]`: `client_id`,
`filter`, `qos`, `shared_group` (one member of a share group receives each message),
`connected`.

```json
{"topic": "plant/7/temp", "truncated": false,
 "subscribers": [{"client_id": "sensor-7", "filter": "plant/+/temp", "qos": 1,
                  "shared_group": null, "connected": true}]}
```

### `GET /admin/v1/backlog`

The sessions with the most messages in flight plus waiting. Parameter: `top` (default 20).
Returns `sessions[]` in the [`clients`](#get-adminv1clients) shape, largest first.

### `GET /admin/v1/retained`

Retained messages this node serves under a topic prefix. Parameters: `prefix`, `limit`,
`cursor`. A prefix ending in `/` is answered from the store's match index; any other prefix is
a plain string prefix over every topic. Returns `prefix`, `count` and `payload_bytes` (of all
matches), `retained[]` (`topic`, `qos`, `payload_bytes`, `expires_at`; never payloads) and
`next_cursor`.

```json
{"prefix": "plant/", "count": 2, "payload_bytes": 13, "next_cursor": null,
 "retained": [{"topic": "plant/7/status", "qos": 1, "payload_bytes": 6, "expires_at": null},
              {"topic": "plant/8/status", "qos": 1, "payload_bytes": 7, "expires_at": null}]}
```

### `GET /admin/v1/authz`

The authorization dry run against the **live** policy. Changes nothing.

| Parameter | Meaning |
|---|---|
| `user` | the principal (identity subject) |
| `action` | `publish`, `subscribe` or `connect` |
| `target` | the topic (publish; no wildcards), the filter (subscribe; must be valid), or the client id (connect) |
| `groups` | comma-separated groups, as the authenticator would report them |
| `client` | the client id `%c` expands to (default: `user`; for `connect`, the target) |

Returns `allowed`, `reason`, `rule` (`index` in `[[rules]]` from 0, `effect`, `pattern` as
written, `expanded` after `%i`/`%c` substitution — absent when the policy default decided),
and the inputs echoed back with `notes`.

```json
{
  "allowed": false,
  "reason": "rule 1 denies it (pattern \"secret/#\"); a deny wins",
  "rule": {"index": 1, "effect": "deny", "pattern": "secret/#", "expanded": "secret/#"},
  "user": "anonymous", "groups": [], "client_id": "anonymous",
  "action": "publish", "target": "secret/x",
  "notes": ["groups are as given here; at runtime they come from the authenticator (token claims, the HTTP hook)"]
}
```

The dry run and enforcement share one evaluator, so they cannot disagree.

### `GET /admin/v1/config`

The committed config (never a reload's unvalidated candidate): `config` (every section, with
secret values — gossip keys, URL credentials and query strings — replaced by `sha256:`
fingerprints; paths shown as configured), `file_checksum` (SHA-256 of the config file),
`generation` (applied loads), `redaction`.

```json
{
  "config": {"admin": {"bind": "0.0.0.0:9443", "viewers": ["CN=oncall"], "…": "…"}, "…": "…"},
  "file_checksum": "545674d7895addba89367d92f8d9e2b91557d9a97fb8281ab3c89452135a6405",
  "generation": 1,
  "redaction": "secret values are replaced by sha256 fingerprints; paths are shown as configured"
}
```

### `POST /admin/v1/reload`

Runs the reload `SIGHUP` runs: re-read the config file, validate it, rebuild the policy and
TLS material, and swap only if everything built. No input.

`200` when applied:

```json
{"trigger": "admin", "applied": true, "error": null,
 "changed_sections": ["limits"], "requires_restart": []}
```

`409 reload-rejected` when refused (the running config and policy are kept):

```json
{
  "error": {"code": "reload-rejected",
            "message": "config: config parse error: TOML parse error at line 24, column 8 …"},
  "outcome": {"trigger": "admin", "applied": false, "error": "config: …",
              "changed_sections": [], "requires_restart": []}
}
```

`changed_sections` are the top-level config sections that differ; `requires_restart` the ones
among them that are staged until a restart. Reloads are serialized with `SIGHUP` and the file
watcher.

### `POST /admin/v1/kick`, `POST /admin/v1/purge`

Parameter: `client` (required). `kick` closes the connection (MQTT 5: `DISCONNECT` reason
`0x98`; 3.1.1: the connection closes); the session stays and the Will is published as for any
server-side close. `purge` disconnects if connected, then deletes the session: subscriptions,
in-flight state, the stored queue and the durable copy. Runs on any node
([where they run](#actions-on-a-client-where-they-run)).

```json
{"action": "purge", "client_id": "archiver", "node": "mqttd-2", "disconnected": false,
 "session_found": true, "forwarded_to": "mqttd-2", "forwarded_for": "CN=root, O=example"}
```

`404 not-found` when no node holds the client:

```json
{"error": {"code": "not-found",
           "message": "no node holds a session or connection for \"nobody\" (asked: mqttd-1, mqttd-2, mqttd-3)"}}
```

### `POST /admin/v1/cordon`, `POST /admin/v1/uncordon`

This node only. Cordoned, the node refuses every new connection at accept (counted as
`admission_rejected{reason="cordon"}`), and `/readyz` answers 503 with
`"cordon":{"active":true,"reason":"cordoned-by-operator"}`; `/livez` stays 200 and connected
sessions stay. Not persisted.

```json
{"node": "mqttd-1", "cordoned": true, "changed": true}
```

### `GET /admin/v1/log-level`

```json
{"base": "info", "override_filter": null, "override_remaining_secs": null}
```

### `POST /admin/v1/log-level`

Parameters: `filter` (required, `RUST_LOG` syntax), `ttl` (seconds, default 600, at most
3600). Replaces this node's log filter until the TTL runs out, then the configured one
returns; a newer override replaces an older one and its timer. Every override keeps
`audit=info`, and a filter naming the `audit` target is `400`. Returns the
[`log-level`](#get-adminv1log-level) shape.

```json
{"base": "info", "override_filter": "mqttd::hub=debug,audit=info", "override_remaining_secs": 59}
```

### `POST /admin/v1/log-level/reset`

Restores the configured filter now. Returns the [`log-level`](#get-adminv1log-level) shape.

## Compatibility

The admin API follows mqttd's own [semantic version](https://semver.org): the paths,
parameters, JSON field names and error codes are the public interface.

| mqttd release | The admin API |
|---|---|
| patch (`1.2.x`) | does not change |
| minor (`1.x.0`) | may add endpoints, parameters, response fields and error codes; nothing existing changes or goes away — ignore fields you do not know |
| major (`2.0.0`) | may change or remove anything; the path version moves with it (`/admin/v2/…` in mqttd 2.x) and the release notes list every break |

The `[admin]` config keys are part of the config surface the same contract covers
([ADR 0058](adr/0058-one-dot-zero-stability-contract.md)).
