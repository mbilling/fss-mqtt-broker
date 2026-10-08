# The admin API

**Verified against `main` after `v1.0.18` (2026-09-30)**, every response below captured from a
live three-node cluster (`scripts/admin-e2e.sh`), except the [rules endpoints](#rules)
(2026-10-08), whose examples follow the implementation and its tests. The command-line
client for it is [ADMIN-CLI.md](ADMIN-CLI.md); the decision records are
[ADR 0081](adr/0081-admin-api.md) and, for the rules, [ADR 0084](adr/0084-watching-and-editing-rules-live.md).

An authenticated HTTPS API on its own listener, for the questions `/statusz` cannot answer
(which clients are here, why is this queue growing, why is this client denied) and a short,
fixed list of audited actions. **It writes no configuration, with one opt-in exception**: a
subject listed in `[rules] admin_writers` may replace the rules file, or one rule in it, and
the ordinary reload applies it ([Rules](#rules)). The config file stays the only source of
everything else, and `reload` re-reads it.

- [Turning it on](#turning-it-on)
- [Authentication and roles](#authentication-and-roles)
- [Conventions](#conventions) · [Errors](#errors) · [Limits](#limits) · [Audit](#audit)
- [The cluster view](#the-cluster-view) · [Actions on a client: where they run](#actions-on-a-client-where-they-run)
- [Rules](#rules): read, check, test and write the rules file
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
| `admin.cert`, `admin.key` | `MQTTD_ADMIN_CERT`, `MQTTD_ADMIN_KEY` | required with `bind`; hot-reloadable |
| `admin.client_ca` | `MQTTD_ADMIN_CLIENT_CA` | required with `bind`; hot-reloadable. Use a dedicated admin CA (see below) |
| `admin.viewers` | `MQTTD_ADMIN_VIEWERS` (`;`-separated) | hot-reloadable |
| `admin.operators` | `MQTTD_ADMIN_OPERATORS` (`;`-separated) | hot-reloadable |
| `admin.peer_port` | `MQTTD_ADMIN_PEER_PORT` | default: the port of `admin.bind` |

The config is refused if `bind` is set without the certificate, key and CA, or with both role
lists empty. The role lists apply to the next request.

**Rotating the certificates is a reload, not a restart.** On every reload (`SIGHUP`, the file
watcher, or `POST /admin/v1/reload`) the node rebuilds the admin TLS from the live config:
- the listener's certificate and key;
- the client CA, plus the cluster CA it also admits;
- the cluster-CA check behind the `peer` role;
- the certificate it presents to its peers' admin listeners.

The next admin connection uses the rebuilt material, and connections already open are
undisturbed. This happens in the same validate-before-swap step as the rest of the reload:
a certificate, key or CA that does not load rejects the whole reload (`admin tls: …` in
its outcome and in the `security.reload` audit record), and the running TLS stays.

To rotate the client CA without a gap, first reload with a bundle holding both the old
and the new CA. Re-issue the admin certificates, then reload with the new CA alone. A
reload that changes `bind` or `peer_port` logs `admin` among the requires-restart
sections.

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
| `operator` | a subject in `admin.operators` | everything; a rules **write** also needs the subject in `[rules] admin_writers` |
| `viewer` | a subject in `admin.viewers` | every `GET` except `/admin/v1/rules/source` |
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
- **Methods**: reads are `GET`; actions are `POST` with no body. The rules endpoints are the
  exception: `rules/check` and `rules/test` are `POST`s and the writes are `PUT`s, each with
  a JSON body (`Content-Type: application/json`), and `DELETE /admin/v1/rule` has no body.
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

Every refusal is `{"error":{"code":"…","message":"…"}}`; a rules endpoint adds the `node` to
the refusals it makes itself (all but a `503`). Match on `code`; the message is for people.

| Status | `code` | When |
|---|---|---|
| 400 | `bad-request` | a missing or malformed parameter, a wildcard where a topic name is needed, an invalid filter, an unreadable request |
| 403 | `forbidden` | the certificate's subject is in no role list, or its role is below the endpoint's |
| 404 | `not-found` | no such endpoint, no such session / connection, or no such rule (`DELETE /admin/v1/rule`, `rules/test`'s `only`) |
| 405 | `method-not-allowed` | a known path with the wrong method |
| 400 | `topic-reserved` | `rules/test` was given a topic in the broker-reserved `$SYS` tree, which no client can publish to |
| 403 | `rules-read-only` | a rules write while `[rules] admin_writers` is empty |
| 403 | `not-a-rules-writer` | a rules write by an operator whose subject is not in `[rules] admin_writers` |
| 409 | `reload-rejected` | the reloaded config or policy was refused; the running one is kept (the body also carries `outcome`). After a rules write, the file **was** written (`written: true`) and the reload was refused for another reason |
| 409 | `rules-file-unset` | no rules file is configured (`rules.file` / `MQTTD_RULES_FILE`): `rules/source`, a `check` or `test` with `rule`, and every write |
| 409 | `rules-file-unreadable` | the configured rules file is missing or cannot be read: `rules/source`, a `check` or `test` with `rule`, and a per-rule `PUT` or `DELETE` (a whole-file `PUT` with `if_match=*` creates a missing file instead) |
| 409 | `rules-file-invalid` | a per-rule check, test or write while the rules file on disk is not TOML or not the rules file's shape |
| 409 | `rules-layout-unsupported` | a per-rule check, test or write on a file it cannot edit in place: a rule written with dotted keys, inline tables or `[[rules.<id>.actions]]`, or CRLF line ends; replace the whole file instead |
| 409 | `rules-file-unwritable` | the rules file cannot be replaced: permissions (EACCES, EPERM), a read-only mount (EROFS), a busy file (EBUSY), another filesystem (EXDEV) or a directory that is gone; the rules file was not changed; the body adds `os_error` |
| 412 | `digest-mismatch` | a rules write whose `if_match` is not the digest of the file on disk; the body adds `file_digest` and `running_digest` |
| 413 | `too-large` | the request exceeds the size limits |
| 422 | `rules-invalid` | rules text that does not load; `error.details` says where |
| 428 | `precondition-required` | `PUT /admin/v1/rules` without `if_match` |
| 500 | `rules-edit-failed` | a per-rule edit would have changed more than that rule; nothing was written |
| 500 | `rules-write-failed` | writing the rules file failed for another reason than those of `rules-file-unwritable` (a full disk, an I/O error); the rules file was not changed; the body adds `os_error` |
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
| request head / body | 16 KiB / 64 KiB; 1 MiB for `rules/check`, `rules/test` and the rules writes when the caller's role may call them (decided before the body is read) |
| rules parses, checks, tests and writes | one at a time per node, off the async workers |
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
with `trigger=admin`. A rules write is also a `rules.write` record, written after the file
is replaced, and its reload a `security.reload` with `trigger=admin-rules`:

```text
kind="rules.write" subject="CN=rules-ui"  op=put-rule rule=high_temp old=2f027340… new=8c1d55e0… applied=true
```

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

## Rules

The rules endpoints ([ADR 0084](adr/0084-watching-and-editing-rules-live.md)) show the
rules this node runs, check and dry-run candidates, and, for a listed writer, write the
rules file. Every answer names the `node`: rules are per-node configuration, and none of
these endpoints asks another node. [RULES.md](RULES.md#through-the-admin-api) is the
user's view of them.

| Who | May |
|---|---|
| viewer | `GET /admin/v1/rules`, without SQL, actions, load warnings or error text (`redacted: true`) |
| operator | everything else that reads or dry-runs: the source, `check`, `test`, and the full `GET` |
| operator **and** a subject in `[rules] admin_writers` | the writes |

A viewer never gets the SQL or the actions, because a rules file can hold a secret, such as
a pseudonym's salt. A rule's `def` cannot give one away: it is a hash keyed per broker
process. The running `digest` is the file's plain SHA-256, though, and `/statusz`,
`/metrics` and `$SYS` publish it too, so whoever knows the rest of the file can test
guesses of a secret in it offline. Make a secret in a rules file long and random
(`openssl rand -hex 16`), never a name or a date.

`admin_writers` (`MQTTD_RULES_ADMIN_WRITERS`, `;`-separated) lists subjects the way
`admin.operators` does. It is empty by default, and empty means no writes
(`403 rules-read-only`). It hot-reloads with the rest of the config. A writer has the rules
file's trust, which is the ACL file's: a rule can derive messages onto any topic from any
accepted publish. Give it to whoever may edit the ACL file, not to everyone who may kick a
client.

**Writes start from the file on disk.** `if_match` is compared with the SHA-256 of the
file as it is on disk (`file_digest`), which can differ from the running rules (`digest`)
after an edit nobody has reloaded yet or a reload that was refused. Read the file with
`rules/source`, edit it, and send its digest back as `if_match`; a `412 digest-mismatch`
means someone changed it in between, so read it again. `if_match=*` overwrites whatever is
there, on purpose. It is required on a whole-file `PUT` and optional on a per-rule one.

**What a write does**, one write at a time per node:

1. Read the file on disk and check `if_match` against it.
2. Build the new text: the body's `source`, or the file with one `[rules.<id>]` table
   changed. Load it as the broker would; if it does not load, answer `422 rules-invalid`
   and stop, having written nothing.
3. Replace the file atomically. The configured path is resolved first, so a symlink's
   target is replaced, not the link. A new file `.<name>.<pid>.<random>.tmp` is created in
   the same directory, written and fsynced, with the old file's mode and group. When the
   broker cannot give it that group, it keeps the mode without the group bits, so what one
   group could read never becomes readable by another. A file that did not exist is
   created with mode `0600`. The old file is kept as `<file>.prev` the same way; the new one
   is renamed over it, and the directory fsynced. Any failure removes the temporary files
   and leaves the rules file as it was. A failure of the last step, the rename over the
   file (EBUSY for a file bind-mounted on its own, say), comes after `<file>.prev` was
   replaced with a copy of the current file. Permissions, a read-only mount, a busy file, a
   cross-device link or a missing directory answer `409 rules-file-unwritable`; any other
   I/O error answers `500 rules-write-failed`; both carry the OS error in `os_error`.
4. Run the ordinary reload, with the trigger `admin-rules`, and read the digest of the
   rules that are running after it.
5. Record `rules.write` in the audit log: the operation, the rule, the old and new digests,
   and whether the reload applied it.

A `200` says `applied: true` only when the reload applied **and** the running digest is the
one written. A reload refused for another reason (a broken ACL file, say) answers
`409 reload-rejected` with `written: true`: the new file is on disk, the old rules run,
and fixing the other file and reloading applies it. A `503 timeout` on a write does not
mean nothing was written: read the file again before retrying. With
`MQTTD_CONFIG_WATCH` on, the watcher reloads the same file again within one poll, harmlessly.

A write whose text is the file on disk byte for byte writes nothing: `written: false`, no
`.prev`, no `rules.write` record. It reloads only when the running rules differ from the
file (otherwise `reload: null` and `applied: true`). `if_match=*` on a missing file creates
it, with mode `0600`: readable by the broker's user alone. To give it a group, create the
file yourself first with the mode you want and a group the broker's user belongs to; a
write keeps both. A group the broker is not in cannot be kept: the write drops the group's
bits (step 3).

Two writes cannot both win on the same file: the node runs them one at a time, from
reading the file through the reload, so when two name the same `if_match`, the one that
runs second gets `412 digest-mismatch` if the first changed the file.

At startup and on every reload the broker logs a WARN when `admin_writers` names someone
but the rules file's directory is not writable. None of the shipped layouts lets the broker
write there (a root-owned file under `/etc`, a read-only Compose mount, a ConfigMap): keep
`admin_writers` empty with them, and give a writable rules file a directory of its own.

**A per-rule edit** (`PUT` or `DELETE /admin/v1/rule`) changes one `[rules.<id>]` table
and nothing else:

- an update writes only the values that differ, inside the rule's existing table: an
  unchanged SQL keeps its `'''` form, and a value keeps its comment;
- a new rule is appended at the end of the file;
- a delete removes the table's header and keys; the comment lines above its header stay
  where they are, so a file header or a section banner above the first rule survives (and
  so does that rule's own comment block). Inserting a rule and deleting it again gives
  back the file byte for byte, unless the file ended with blank lines: deleting the last
  rule drops the blank lines that would then end the file;
- before anything is written, the old and new text are both loaded and compared: every
  other rule must be unchanged and the edited one exactly as asked, or the answer is
  `500 rules-edit-failed`.

Only rules written as `[rules.<id>]` tables of plain keys can be edited one at a time
(`409 rules-layout-unsupported` otherwise); the whole-file `PUT` works for any file.

**Dry runs have no side effects.** `check` and `test` never touch the running rules'
counters, last errors, trace or WARN rate limit, and never log a `console` output. A
`test` against the running rules evaluates the rules a client publish would meet, so a
rule that pseudonymizes with a salt in its SQL computes the pseudonym of any input it is
given: that is why `test`, like the source, needs the operator role.

**Clusters.** A write changes the node that answered and is never forwarded: apply it to
each node. `/statusz` carries the running `rules` digest, and the
[cluster view](#get-adminv1cluster) compares it across nodes (`same_rules`).

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
| GET | [`/admin/v1/rules`](#get-adminv1rules) | viewer (redacted) | `rules` |
| GET | [`/admin/v1/rules/source`](#get-adminv1rulessource) | operator | `rules-source` |
| POST | [`/admin/v1/rules/check`](#post-adminv1rulescheck) | operator | — |
| POST | [`/admin/v1/rules/test`](#post-adminv1rulestest) | operator | — |
| PUT | [`/admin/v1/rules`](#put-adminv1rules) | operator + writer | `rules-apply` |
| PUT | [`/admin/v1/rule`](#put-adminv1rule-delete-adminv1rule) | operator + writer | — |
| DELETE | [`/admin/v1/rule`](#put-adminv1rule-delete-adminv1rule) | operator + writer | `rule-delete` |

The per-rule `PUT`, `check` and `test` take structured JSON bodies and have no verb
(ADR 0084 D7); call them over HTTPS, as the [demo's editor](../demo/rules-live/README.md)
does.

### `GET /admin/v1/whoami`

The caller as this node sees it.

```json
{"cn":"oncall","node_id":"mqttd-1","role":"viewer","subject":"CN=oncall, O=example"}
```

### `GET /admin/v1/node`

This node's state: the `/statusz` body (identity, version, readiness, members, lease,
replication, store, brownout, backup, config checksum, protocol range, and the running rules'
`rules` block: `digest`, `rules`, `enabled`), plus `cordon` and `log_filter` when active. The fields are those of `/statusz`
([ADR 0054](adr/0054-operator-facing-state-surface.md)).

### `GET /admin/v1/cluster`

Every node, from any node ([the cluster view](#the-cluster-view)).

| Field | Meaning |
|---|---|
| `answered_by` | the node that answered |
| `summary.nodes`, `.replied`, `.ready` | how many members, how many answered, how many are ready |
| `summary.same_cluster_id` | every replying node reports the same cluster identity (the split-brain check) |
| `summary.same_version`, `.same_config`, `.same_membership`, `.same_rules` | convergence checks across replying nodes (`same_rules`: the same running rules digest) |
| `nodes[]` | one row per member (below) |

A row: `node_id`, `replied` (and `error` when not), `admin_addr`, `elapsed_ms`, `version`,
`ready`, `live`, `cluster_id`, `members` (how many it sees), `lease_leader`, `lease_epoch`,
`replica_lag_groups`, `under_replicated`, `brownout`, `quarantined`, `swim_isolated`,
`decommissioning`, `config_checksum`, `rules_digest`, `proto_max`.

```json
{
  "answered_by": "mqttd-1",
  "summary": {"nodes": 3, "replied": 3, "ready": 3, "same_cluster_id": true,
              "same_config": true, "same_membership": true, "same_rules": true,
              "same_version": true},
  "nodes": [
    {"node_id": "mqttd-1", "replied": true, "elapsed_ms": 0, "version": "1.0.18", "ready": true,
     "live": true, "cluster_id": "f8995cf8f6cd90e4ab4a321e17c07585", "members": 3,
     "lease_leader": true, "lease_epoch": 1, "replica_lag_groups": 0, "under_replicated": false,
     "brownout": false, "quarantined": false, "swim_isolated": false, "decommissioning": null,
     "config_checksum": "545674d7…", "rules_digest": "2f027340…", "proto_max": 9},
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

The dry run and enforcement share one evaluator, so they cannot disagree. A publish to
`$SYS` or under `$SYS/` is answered before the ACL is asked, because the broker refuses it
whatever the ACL says ([ADR 0084](adr/0084-watching-and-editing-rules-live.md) D1). The
one exception is a Mosquitto bridge's `$SYS/broker/connection/<id>/state`, which the ACL
decides like any topic:

```json
{"allowed": false, "reason": "reserved: $SYS/ is the broker's (ADR 0084)", "…": "…"}
```

A subscribe deny also matches the filter inside `$share/<group>/<filter>`, and the reason
says so. An allow reaches a `$`-rooted `<filter>` only when the pattern's own inner filter
covers it: `$share/+/#` does not grant `$share/g/$SYS/…`.

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

### `GET /admin/v1/rules`

The rules this node runs, with what they have done.

| Field | Meaning |
|---|---|
| `digest`, `file_digest`, `in_sync` | the running rules' SHA-256, the SHA-256 of the file on disk now, and whether they are equal |
| `writable` | `true` only for an operator listed in `[rules] admin_writers`, when a rules file is configured and the broker may write the directory it resolves to (`access(2)`, write and search). Always `false` for a viewer or an operator who is not a writer |
| `rules[]` | in evaluation order (by id): `id`, `enabled`, `description`, `from` (topic filters), `events`, `actions` (how many), `def`, `counts`, `last_active_at`, `last_error`, as on [`$SYS`](RULES.md#statistics-on-sys); for an operator also `sql`, `actions_spec` (the actions as the file writes them, as JSON) and `last_error.message` (the error's text, at most 256 bytes); for a viewer `redacted: true` |
| `warnings` | operator only: the loader's warnings for the running rules (a warning can quote SQL text, so a viewer's answer has none) |
| `reload` | the last reload attempt: `at`, `trigger`, `applied`, `error_kind`, `repeats`, and for an operator `error` (the full text); `null` before the first |
| `redacted` | `true` in a viewer's answer |

The counts are the same as on `$SYS` and are served whether or not the `$SYS` statistics
are on. `last_active_at` is not: the statistics task sets it, so it is `null` until the
statistics have been on and seen the rule run, and while `sys_interval_secs = 0` it keeps
the time they last saw, however often the rule has run since. `sql` and `action` last errors are
kept either way, but a `delivery` last error is written only by the statistics task. A last
error about an earlier definition of the rule (another `def`) is not shown. Keys arrive in
alphabetical order. An operator's answer, statistics off (`def` is per broker process, so
yours differs):

```json
{"digest":"2f027340235af0b20eafed114c6c36869b05aac8568546125274382b66e892ad",
 "file_digest":"2f027340235af0b20eafed114c6c36869b05aac8568546125274382b66e892ad",
 "in_sync":true,"node":"node-local","reload":null,
 "rules":[{"actions":1,
   "actions_spec":[{"args":{"payload":"${.}","topic":"alerts/${clientid}"},"function":"republish"}],
   "counts":{"actions_failed":0,"actions_ok":1,"failed":0,"matched":2,"no_result":1,"passed":1},
   "def":"25bb28ee8d743603","description":"Alert on hot sensors","enabled":true,"events":[],
   "from":["sensors/+/data"],"id":"high_temp","last_active_at":null,"last_error":null,
   "sql":"SELECT payload.temp AS temp, clientid, qos\nFROM \"sensors/+/data\"\nWHERE payload.temp > 30\n"}],
 "warnings":[],"writable":true}
```

### `GET /admin/v1/rules/source`

Operator. The rules file as it is on disk: `file` (the configured path), `source` (its
text), `digest` (its SHA-256: send it back as `if_match`), `running_digest`, `in_sync` and
`bytes`.

```json
{"node": "mqttd-1", "file": "/etc/mqttd/rules/rules.toml", "bytes": 261,
 "digest": "2f027340…", "running_digest": "2f027340…", "in_sync": true,
 "source": "[rules.high_temp]\ndescription = \"Alert on hot sensors\"\nsql = '''\n…"}
```

### `POST /admin/v1/rules/check`

Operator. Loads rules text as the broker would and writes nothing. The body is either a
whole file, `{"source": "…"}`, or one rule, `{"rule": {"id", "sql", "actions",
"description", "enable"}}` (`description` and `enable` optional), which is spliced into the
file on disk as a per-rule `PUT` would splice it, so the check says exactly what that write
would say.

`200` when it loads: `{"node", "valid": true, "rules", "enabled", "digest", "warnings"}`,
the digest being that of the checked text. `422 rules-invalid` when it does not, with the
body a write would get. `details.scope` is `toml` (the line and column in the file), `sql`
(the rule, and the line and column within its `sql` string), or `rule` (the rule alone: an
error about its id, its actions or how many it has). A `rule` body's left-out
`description` and `enable` take the on-disk rule's values, or `""` and `true` for a new
rule.

```json
{"error": {"code": "rules-invalid",
           "message": "rule `high_temp`: expected an expression (line 4, column 1, near `end of statement`)",
           "details": {"scope": "sql", "rule": "high_temp", "line": 4, "column": 1}},
 "node": "mqttd-1"}
```

### `POST /admin/v1/rules/test`

Operator. Runs one simulated message through rules and returns what each rule did,
actions included. Nothing changes: no counter, last error, trace or log line.

| Body field | Meaning |
|---|---|
| `source` or `rule` | the rules to run: a candidate file, or one rule (as for `check`), which is run even when disabled. Neither: the running rules |
| `only` | with the running rules or a `source`, run only this rule id |
| `topic`, `payload` | the message (required); `payload_encoding` is `utf8` (default) or `base64` |
| `qos`, `retain`, `clientid`, `username` | the rest of the message |
| `event` | run against a sample client or session event instead of a publish, as `mqttd --rule-test --event` does |

Defaults: `qos` 0, `retain` false, `clientid` `"test-client"`, no `username` (the SQL sees
`undefined`), `payload_encoding` `utf8`. `topic` and `payload` are required strings even
with `event`. There, `topic` is a `session.*` sample's filter and may be `""` for a
`client.*` event, and `event` is `client.connected`, `client.disconnected`,
`session.subscribed`, `session.unsubscribed` or its `$events/…` topic. An event sample
carries no peer address (`peername` and `peerhost` are `undefined`); `mqttd --rule-test
--event` uses 127.0.0.1:52345. Without `rule` or `only`, `results` lists the enabled rules
whose `FROM` selects the message or event, and leaves the rest out; `no_match` appears
only for the rule `rule` or `only` names.

Returns `results[]`, one per rule run: `rule`, `enabled` (`false` for a disabled rule run
anyway), `result` (`passed`, `no_result`, `failed`, or `no_match` with a `reason` when the
topic matches none of the rule's `FROM` filters), `error`, and `outputs[]` in the trace's
shape ([RULES.md](RULES.md#the-trace)), with payloads up to 64 KiB. A topic in the
broker-reserved `$SYS` tree is `400 topic-reserved`: no client can publish there.

```json
{"node": "mqttd-1",
 "results": [{"rule": "high_temp", "enabled": true, "result": "passed", "error": null,
              "outputs": [{"action": "republish", "topic": "alerts/kitchen", "qos": 1,
                           "retain": false,
                           "payload": "{\"temp\":35,\"clientid\":\"kitchen\",\"qos\":1}",
                           "payload_encoding": "utf8", "payload_bytes": 40,
                           "truncated": false}]}]}
```

### `PUT /admin/v1/rules`

Operator and writer. Replaces the whole rules file ([what a write does](#rules)). Parameter:
`if_match` (required: the digest of the file on disk, or `*`). Body: `{"source": "…"}`.
With `*` and no file on disk, it creates the file, with mode `0600`.

```json
{"node": "mqttd-1", "written": true, "digest": "8c1d55e0…", "running_digest": "8c1d55e0…",
 "applied": true, "rules": 1, "enabled": 1, "warnings": [],
 "reload": {"trigger": "admin-rules", "applied": true, "error": null,
            "changed_sections": [], "requires_restart": []}}
```

A stale `if_match`:

```json
{"error": {"code": "digest-mismatch",
           "message": "the rules file on disk is not the one if_match names: read it again (GET /admin/v1/rules/source) and redo the change"},
 "node": "mqttd-1", "file_digest": "8c1d55e0…", "running_digest": "8c1d55e0…"}
```

### `PUT /admin/v1/rule`, `DELETE /admin/v1/rule`

Operator and writer. Parameters: `id` (required), `if_match` (optional: the digest of the
file on disk, or `*`). `PUT` inserts or updates `[rules.<id>]` from the body
`{"sql", "actions", "description", "enable"}`, all four required (`actions` as JSON, in the
file's shape: `[{"function": "republish", "args": {…}}]`). `DELETE` removes it. The rest of
the file stays byte for byte ([a per-rule edit](#rules)). The answer is the whole-file
`PUT`'s.

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
