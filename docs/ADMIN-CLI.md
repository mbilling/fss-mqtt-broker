# The admin command line: `mqttd --admin`

**Verified against `main` after `v1.0.18` (2026-09-30)**, every example below captured from a
live three-node cluster (`scripts/admin-e2e.sh`). The HTTP interface behind each command is
[ADMIN-API.md](ADMIN-API.md); the decision record is [ADR 0081](adr/0081-admin-api.md).

`mqttd --admin <verb>` asks a running broker's admin API a question or tells it to act, and
prints the answer. It is the same `mqttd` binary as the broker, so it works wherever the
broker does — including the distroless container image, which has no shell and no curl:

```sh
kubectl exec mqttd-0 -- mqttd --admin cluster
```

- [Before you start](#before-you-start)
- [Connecting](#connecting)
- [Output and exit codes](#output-and-exit-codes)
- [Verbs](#verbs) — [who am I](#whoami) · [the cluster](#node-cluster-placement) ·
  [clients and sessions](#clients-session-subscribers-backlog-retained) ·
  [authorization](#authz) · [config and reload](#config-reload) ·
  [kick and purge](#kick-purge) · [cordon](#cordon-uncordon) · [logging](#log-level-log-override-log-reset)
- [Recipes](#recipes)
- [Other `mqttd` commands for operators](#other-mqttd-commands-for-operators)
- [Try it without a deployment](#try-it-without-a-deployment)

## Before you start

The admin API is **off unless the broker sets `admin.bind`**, and it only answers clients that
present a certificate its `admin.client_ca` issued, with a subject listed in `admin.viewers`
or `admin.operators`. The broker side is in
[ADMIN-API.md § Turning it on](ADMIN-API.md#turning-it-on). You need:

- the admin **URL** of a node (`https://host:port`);
- the **CA** that issued that node's admin server certificate, to verify the server;
- a **client certificate and key** from the admin CA, whose subject is in a role list.

| Role | Can |
|---|---|
| `viewer` | every read: `whoami`, `node`, `cluster`, `placement`, `clients`, `session`, `subscribers`, `backlog`, `retained`, `authz`, `config`, `log-level` |
| `operator` | everything a viewer can, plus the actions: `reload`, `kick`, `purge`, `cordon`, `uncordon`, `log-override`, `log-reset` |

Every request is written to the broker's audit log (`admin.request`) with your certificate
subject, role, the command and its outcome.

## Connecting

Each setting is a flag or an environment variable; a flag wins.

| Flag | Environment | Default |
|---|---|---|
| `--url https://host:port` | `MQTTD_ADMIN_URL` | `admin.bind` from the local config, with `0.0.0.0` → `127.0.0.1` |
| `--ca <pem>` | `MQTTD_ADMIN_CA` | `admin.client_ca` from the local config |
| `--cert <pem>` | `MQTTD_ADMIN_CLIENT_CERT` | none — required |
| `--key <pem>` | `MQTTD_ADMIN_CLIENT_KEY` | none — required |
| `--server-name <name>` | `MQTTD_ADMIN_SERVER_NAME` | the URL's host |
| `--config <path>` | `MQTTD_CONFIG` | where to find the local config for the defaults above |

`--server-name` is the name the server's certificate must carry. Set it when you connect by
an address the certificate does not name — a port-forward to `127.0.0.1`, say:

```sh
export MQTTD_ADMIN_URL=https://127.0.0.1:19443
export MQTTD_ADMIN_SERVER_NAME=mqttd-1
export MQTTD_ADMIN_CA=/etc/mqttd/admin/cluster-ca.pem
export MQTTD_ADMIN_CLIENT_CERT=~/.mqttd/root.pem
export MQTTD_ADMIN_CLIENT_KEY=~/.mqttd/root.key
mqttd --admin whoami
```

On a broker host or inside a pod, the URL and CA default from the broker's own config, so
only the client certificate is needed. These variables configure the client, not the broker,
so they are not in [CONFIGURATION.md](CONFIGURATION.md).

## Output and exit codes

By default the answer is printed for a terminal: `key  value` lines (nested fields joined
with `.`), and each list as a table under its name. A table's identifying columns come
first, always in this order: `NODE_ID`, `CLIENT_ID`, `NODE`, `TOPIC`, `FILTER`, `REPLIED`,
`CONNECTED`. The rest follow alphabetically. `cluster` prints a compact view instead
([below](#node-cluster-placement)). `--json` prints the API's JSON, for scripts and `jq`,
refusals included, so a rejected `reload` still shows its outcome. `help` wraps to 100
columns.

| Exit | Meaning |
|---|---|
| `0` | the broker answered and did it |
| `1` | the broker refused (the message says why: `403 forbidden`, `404 not-found`, `409 reload-rejected`, …) or could not be reached |
| `2` | a usage error (unknown verb, missing argument, no URL or certificate) — nothing was sent |

A refusal prints the HTTP status, a stable code, and a message:

```text
$ mqttd --admin kick nobody            # as a viewer
mqttd: 403 forbidden: this endpoint needs the operator role; this certificate has viewer
[exit 1]
```

The error codes are listed in [ADMIN-API.md § Errors](ADMIN-API.md#errors). `mqttd --admin help`
prints every verb.

## Verbs

`<x>` is a required argument, `[--x <v>]` an optional one. Unless a verb says otherwise, it
answers for **the node you ask**; the ones that answer for the whole cluster say so.

### `whoami`

The certificate subject and role the broker sees for you, and which node answered. The first
thing to run with a new certificate.

```text
$ mqttd --admin whoami
cn       root
node_id  mqttd-1
role     operator
subject  CN=root, O=example
```

### `node`, `cluster`, `placement`

| Verb | Answers |
|---|---|
| `node` | this node's full state: the `/statusz` body (identity, readiness, members, lease, replication, store, brownout, cordon, log override, …) |
| `cluster` | **every** node, from any node: one row per member, and a summary of whether they agree |
| `placement` | this node's membership, replication and lease view, and whether each other node's membership matches |

`cluster` asks every member's admin listener for its state, in parallel (3 s each), and
merges the answers. A node that does not answer is a row with `replied: false` and the
reason — never left out, never shown as healthy. The summary is the split-brain and
convergence check: `same_cluster_id`, `same_version`, `same_config`, `same_membership`.

The terminal view fits in 100 columns: a summary line, whether the nodes agree (or which
check differs), and one short row per node.

```text
$ mqttd --admin cluster
3 nodes: 3 replied, 3 ready (answered by mqttd-1)
they agree on cluster id, version, config and membership

NODE     STATE  LEADER  EPOCH  MEMBERS  LAG  VERSION  CLUSTER   MS  NOTES
mqttd-1  ready  *       1      3        0    1.0.18   f8995cf8  0   -
mqttd-2  ready  -       1      3        0    1.0.18   f8995cf8  44  -
mqttd-3  ready  -       1      3        0    1.0.18   f8995cf8  39  -
```

| Column | Meaning |
|---|---|
| `STATE` | `ready`, `not ready`, or `no reply` |
| `LEADER`, `EPOCH` | `*` on the lease leader; the lease epoch |
| `MEMBERS`, `LAG` | how many members the node sees; replica groups it lags in |
| `CLUSTER` | the first 8 characters of the node's cluster id |
| `MS` | how long the node took to answer |
| `NOTES` | what is wrong: `quarantined`, `brownout`, `swim-isolated`, `under-replicated`, `decommissioning`, `not live`, or why the node did not reply |

`--json` has every field of every row: admin address, full cluster id, config checksum,
protocol version. For every node to appear, each must run its admin listener and the node
you ask must have cluster TLS ([ADMIN-API.md § The cluster view](ADMIN-API.md#the-cluster-view)).

### `clients`, `session`, `subscribers`, `backlog`, `retained`

These answer for **the node you ask**: the sessions it holds and the retained messages it
serves. `--all-nodes` on `clients`, `session` and `subscribers` asks every node instead and
merges the answers, each row naming its `node`, so you do not need to know where a client
is. A `nodes` table lists which nodes answered; a node that did not is a row with the
reason, so a partial answer says so
([ADMIN-API.md § Cluster-wide reads](ADMIN-API.md#cluster-wide-reads-scopecluster)).

| Verb | Answers |
|---|---|
| `clients [--prefix <p>] [--user <u>] [--source <s>] [--limit <n>] [--cursor <c>] [--all-nodes]` | sessions by client id, 100 per page: connected or not, user, auth method, protocol, source address, age, persistence and expiry, subscription / in-flight / backlog counts |
| `session <client> [--all-nodes]` | one session: every subscription with its options, in-flight messages by acknowledgement state, receive maximum, backlog, the Will (without its payload), the owning node, and for a disconnected persistent session its `queued` count |
| `subscribers <topic> [--limit <n>] [--all-nodes]` | who receives a publish to `<topic>` (a topic name, no wildcards): ordinary and `$share` subscriptions |
| `backlog [--top <n>]` | the sessions with the most messages in flight or waiting (default 20) |
| `retained [--prefix <p>] [--limit <n>] [--cursor <c>]` | retained messages under a topic prefix: count and bytes, then topics (no payloads), paged |

Filters: `--prefix` matches the start of the client id; `--user` the authenticated principal
exactly; `--source` the start of `ip:port`. `--user` and `--source` match connected clients
only. Pages: when `next_cursor` is set, pass it as `--cursor` for the next page; `matched` is
the total across all pages.

```text
$ mqttd --admin clients
matched      2
next_cursor  -

sessions:
CLIENT_ID  CONNECTED  AUTH       BACKLOG  BACKLOG_BYTES  CONNECTED_SECS  EXPIRES_AT  EXPIRY_SECS  INFLIGHT  PERSISTENT  PROTOCOL  SOURCE              SUBSCRIPTIONS  USER
me         true       anonymous  0        0              0               -           -            0         false       3.1.1     192.168.65.1:26450  1              anonymous
sensor-7   true       anonymous  0        0              4               -           -            0         false       5         192.168.65.1:53942  1              anonymous

$ mqttd --admin subscribers plant/7/temp
topic      plant/7/temp
truncated  false

subscribers:
CLIENT_ID  FILTER        CONNECTED  QOS  SHARED_GROUP
sensor-7   plant/+/temp  true       1    -

$ mqttd --admin retained --prefix plant/
count          2
next_cursor    -
payload_bytes  13
prefix         plant/

retained:
TOPIC           EXPIRES_AT  PAYLOAD_BYTES  QOS
plant/7/status  -           6              1
plant/8/status  -           7              1
```

A session the node does not hold is a `404` that says where placement puts it; with
`--all-nodes` the node finds it wherever it is:

```text
$ mqttd --admin session archiver
mqttd: 404 not-found: this node (mqttd-1) holds no session for "archiver"; placement puts it on mqttd-2
$ mqttd --admin session archiver --all-nodes --json | jq '{node, found_on, connected}'
{"node": "mqttd-2", "found_on": ["mqttd-2"], "connected": false}
```

`session` and `clients` take their `--json` form from the same data; see
[ADMIN-API.md § `/session`](ADMIN-API.md#get-adminv1session) for every field.

### `authz`

`authz <user> <publish|subscribe|connect> <target> [--groups a,b] [--client <id>]` asks the
**live** policy — the one the last reload published — whether `<user>` may act, and which rule
decides. It changes nothing.

- `<target>` is the topic for `publish` (no wildcards), the filter for `subscribe`, and the
  client id for `connect`.
- `--groups` are the principal's groups as its authenticator would report them (token
  claims, the HTTP hook); the dry run takes them as given.
- `--client` is the client id that `%c` in a rule expands to (default: `<user>`).

```text
$ mqttd --admin authz anonymous publish plant/7/temp
action         publish
allowed        true
client_id      anonymous
groups
notes          groups are as given here; at runtime they come from the authenticator (token claims, the HTTP hook)
reason         rule 0 allows it (pattern "#") and no deny rule matches
rule.effect    allow
rule.expanded  #
rule.index     0
rule.pattern   #
target         plant/7/temp
user           anonymous

$ mqttd --admin authz anonymous publish secret/x --json | jq '{allowed, reason}'
{
  "allowed": false,
  "reason": "rule 1 denies it (pattern \"secret/#\"); a deny wins"
}
```

`rule.index` counts the policy file's `[[rules]]` from 0. With no rule matching, `rule` is
absent and the reason names the policy default. For `connect`, the session-owner guard
(ADR 0031) still applies on top of the policy.

### `config`, `reload`

| Verb | Role | Does |
|---|---|---|
| `config` | viewer | the effective config (defaults < file < `MQTTD_*` env) with every secret value replaced by a `sha256:` fingerprint, plus `file_checksum` (the SHA-256 of the config file — compare it with the committed file) and `generation` |
| `reload` | operator | the reload `SIGHUP` runs, reporting what happened |

The config file stays the only source of configuration: `reload` takes no input, it re-reads
the file. A good edit:

```text
$ mqttd --admin reload
applied           true
changed_sections  limits
error             -
requires_restart
trigger           admin
```

`requires_restart` lists changed sections that are staged but not live until a restart
(listeners, TLS material, the cluster, the admin listener's bind and certificates, …). A bad
edit is refused and the running config and policy are kept:

```text
$ mqttd --admin reload
mqttd: 409 reload-rejected: config: config parse error: TOML parse error at line 24, column 8
   |
24 | [limits
   |        ^
unclosed table, expected `]`

applied           false
…
[exit 1]
```

Reloads are serialized: `SIGHUP`, the file watcher and `reload` never interleave.

### `kick`, `purge`

| Verb | Role | Does |
|---|---|---|
| `kick <client>` | operator | closes the client's connection: an MQTT 5 client receives `DISCONNECT` with reason `0x98` (Administrative action), a 3.1.1 client just loses the connection. The session stays, and the Will is published as for any server-side close |
| `purge <client>` | operator | disconnects the client if connected, then deletes its session everywhere: subscriptions, in-flight state, queued messages, the durable copy. A persistent reconnect then starts clean |

Run either on **any node**. The node you ask acts if it holds the client; otherwise it asks
the session's placement owner, then every other node, and the answer says which one acted:

```text
$ mqttd --admin purge archiver --json
{
  "action": "purge",
  "client_id": "archiver",
  "disconnected": false,
  "forwarded_for": "CN=root, O=example",
  "forwarded_to": "mqttd-2",
  "node": "mqttd-2",
  "session_found": true
}

$ mqttd --admin kick nobody
mqttd: 404 not-found: no node holds a session or connection for "nobody" (asked: mqttd-1, mqttd-2, mqttd-3)
```

A kicked client may reconnect straight away (most client libraries do). To keep it out,
change its credentials or its ACL and `reload`.

### `cordon`, `uncordon`

`cordon` (operator, **this node only**) refuses every new connection and makes `/readyz`
report not-ready, so load balancers and Kubernetes Services stop sending new clients.
Connected sessions stay, and `/livez` stays healthy, so nothing restarts the node. `uncordon`
reverses it. Not persisted: a restart comes back uncordoned.

```text
$ mqttd --admin cordon
changed   true
cordoned  true
node      mqttd-1
```

To move the connected clients off too, follow with `kick`; to remove the node for good, use
[`mqttd --decommission`](#other-mqttd-commands-for-operators).

### `log-level`, `log-override`, `log-reset`

| Verb | Role | Does |
|---|---|---|
| `log-level` | viewer | the configured filter (`RUST_LOG`, default `info`) and any override with its seconds left |
| `log-override <filter> [--ttl <s>]` | operator | log with `<filter>` (`RUST_LOG` syntax) for `--ttl` seconds (default 600, at most 3600); then the configured filter returns on its own |
| `log-reset` | operator | restore the configured filter now |

```text
$ mqttd --admin log-override 'mqttd::hub=debug' --ttl 60
base                     info
override_filter          mqttd::hub=debug,audit=info
override_remaining_secs  59
```

The audit trail always keeps logging: every override carries `audit=info`, and a filter that
names the `audit` target is refused. The override is per node and not persisted.

## Recipes

**Is the cluster healthy and converged?**

```sh
mqttd --admin cluster --json | jq '.summary'
```

**Did every node pick up the config change?** Compare `file_checksum` with the committed file:

```sh
sha256sum mqttd.toml
mqttd --admin cluster --json | jq -r '.nodes[] | "\(.node_id) \(.config_checksum)"'
```

**Why can't this device publish?**

```sh
mqttd --admin authz device-7 publish plant/7/temp
```

**Which sessions are piling up messages?**

```sh
mqttd --admin backlog --top 10
mqttd --admin session <the client at the top>
```

**Take a node out of rotation for maintenance, without disconnecting anyone:**

```sh
mqttd --admin cordon   --url https://mqttd-2.mqttd:9443
# … maintenance …
mqttd --admin uncordon --url https://mqttd-2.mqttd:9443
```

**Walk every page of a large client list:**

```sh
cursor=""
while :; do
  page=$(mqttd --admin clients --prefix sensor- --limit 1000 --json ${cursor:+--cursor "$cursor"})
  jq -r '.sessions[].client_id' <<<"$page"
  cursor=$(jq -r '.next_cursor // empty' <<<"$page")
  [ -z "$cursor" ] && break
done
```

**More detail from one node for fifteen minutes:**

```sh
mqttd --admin log-override 'mqttd::hub=debug,mqttd::conn=debug' --ttl 900
```

## Other `mqttd` commands for operators

These run from the same binary and need no admin API:

| Command | Does |
|---|---|
| `mqttd --check-config [--config <path>]` | validate the effective config and exit, binding nothing |
| `mqttd --check-config --preflight` | … and check this host: every referenced file readable, every bind free |
| `mqttd --print-config [--config <path>]` | print the effective config with secrets fingerprinted |
| `mqttd --check-tls [--config <path>]` | check each configured certificate chain: key match, expiry, SANs |
| `mqttd --hash-password [<user>]` | print an Argon2id password-file line |
| `mqttd --probe [/readyz\|/livez] [--url <host:port>]` | query the running broker's health endpoint; exit 0 when healthy |
| `mqttd --backup [--pid <n>]` | take an online backup on the running broker and wait for it |
| `mqttd --decommission [--pid <n>]` | drain this node's data to the others and stop it (the Kubernetes `preStop`) |

A day 0 / day 1 / day 2 table of which command answers which question is in
[OPERATIONS.md](OPERATIONS.md#day-0-1-and-2-from-the-command-line).

## Try it without a deployment

```sh
scripts/admin-e2e.sh up     # three nodes in Docker with the admin API; prints the exports above
scripts/admin-e2e.sh test   # every command on this page, checked (CLI and curl)
scripts/admin-e2e.sh down

scripts/admin-e2e.sh scenarios        # the operator scenarios, each on a fresh cluster (~3 min after the build)
scripts/admin-e2e.sh scenarios drift  # just the ones whose name matches
scripts/admin-e2e.sh list             # the scenario names
```

The scenarios script real incidents and check what the admin API shows during and after
each: a node killed (its row turns `replied: false` with the reason, then it rejoins), a
network partition seen from both sides, a fourth node joining, `mqttd --decommission`
draining a node out, a founder restarting without its state and quarantining itself
(`same_cluster_id: false`), queued messages surviving their owner's death, retained
replication, a cross-node session takeover, an ACL change and a password-file change
reaching already-connected clients, config drift (`same_config: false`), the peer, viewer
and unlisted-certificate role boundaries, a cordon → kick → restart → uncordon drill, a
reloaded session quota, a 3-second log override expiring on its own, and the audit records
of it all. They use their own compose project and ports (42010 and up), so they can run
beside a cluster from `up`, and they run nightly in CI; a failed scenario leaves its node
logs under `target/admin-scenarios/<name>/logs/`.
