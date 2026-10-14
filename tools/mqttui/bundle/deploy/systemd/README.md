# A three-node mqttd cluster on systemd

For bare metal and VMs. Same broker and same configuration as the Compose packaging;
the difference is that the machines are yours to name.

## Install, per host

Two things below are **cluster-wide, not per host**, and they are exactly the two that turn
into three brokers which start, log `SWIM gossip is SIGNED per-node` and never become ready
if you run them once per machine: **step 5b** (the gossip key) and **step 6** (the TLS
material). Each is generated once, on one machine, and *copied* to every host.

```sh
# 1. The binary. Signed release artefacts: github.com/mbilling/fss-mqtt-broker/releases
sudo install -m 0755 mqttd /usr/local/bin/mqttd

# 2. A service account that owns nothing else.
sudo useradd --system --no-create-home --shell /usr/sbin/nologin mqttd

# 3. Config and state.
sudo install -d -m 0750 -o root  -g mqttd /etc/mqttd
sudo install -d -m 0700 -o mqttd -g mqttd /var/lib/mqttd

# 4. The unit and this host's environment.
sudo install -m 0644 mqttd.service /etc/systemd/system/mqttd.service
sudo install -m 0640 -o root -g mqttd mqttd.env.example /etc/mqttd/mqttd.env
sudo $EDITOR /etc/mqttd/mqttd.env        # five marked lines — see below

# 5. Secrets.
printf %s 'their-password' | mqttd --hash-password device-a | sudo tee -a /etc/mqttd/passwd
sudo chown root:mqttd /etc/mqttd/passwd && sudo chmod 0640 /etc/mqttd/passwd
sudo install -m 0640 -o root -g mqttd acl.toml /etc/mqttd/acl.toml   # from ../compose/

# 5b. The gossip key. ONE KEY FOR THE WHOLE CLUSTER — generate it ONCE, on ONE machine,
#     and copy that same file to every host. Do NOT run `openssl rand` per host: three
#     hosts with three different keys form no mesh, and what you see is three brokers that
#     start, log "SWIM gossip is SIGNED per-node" and never become ready — the same symptom
#     as a per-host CA (below), so the obvious suspect is the wrong one.
#
#     ON YOUR ADMIN MACHINE, once for the cluster:
#       openssl rand -hex 32 > swim.key && chmod 0600 swim.key
#     THEN, for every host including the first (scp preserves the mode, so tighten FIRST —
#     otherwise the cluster-wide key sits 0644 in world-readable /tmp on every host):
#       scp -p swim.key <host>:/tmp/swim.key
#       ssh <host> 'sudo install -m 0640 -o root -g mqttd /tmp/swim.key /etc/mqttd/swim.key \
#                   && rm -f /tmp/swim.key'
#     Keep it next to the CA key, and treat it the same way: anything holding it can join
#     the gossip mesh.

# 6. TLS material — see "Certificates" below. NOT optional: the shipped env file enables
#    TLS and the cluster bus, so an unedited install fails CLOSED at startup rather than
#    serving cleartext, naming the setting and the path it could not read:
#      Error: "cannot read MQTTD_PEER_TLS_CA (/etc/mqttd/tls/peer-ca.pem): No such file or directory (os error 2)"
#    Mint it with ./gen-certs.sh on an ADMIN machine, then copy this node's files here and
#    install them one at a time with the commands that script prints. The CA private key is
#    NOT one of the files that comes to this host.

# 7. Go.
sudo systemctl daemon-reload
sudo systemctl enable --now mqttd
mqttd --probe /readyz        # exits 0 once this node is ready
```

## Certificates

[`gen-certs.sh`](gen-certs.sh) mints them. It is a script rather than a recipe in a comment
because a recipe nobody runs is a recipe that does not run: `scripts/deploy-smoke.sh` boots
two real nodes from this script's output on every CI run, and asserts the certificate
properties the cluster bus enforces.

Everything it prints about a certificate it first **reads back out of the file** — the CA's
`basicConstraints`, each leaf's CN, SANs and EKUs, the named curve, the signature algorithm,
and `openssl verify` of each leaf against the CA. Material is minted into a temporary
directory and installed only once that passes, and re-running `ca` **re-verifies** the CA on
disk instead of just noticing that the files exist. That matters because `openssl` is not one
program: macOS's `/usr/bin/openssl` is LibreSSL, which needs a different recipe to produce a
usable CA at all (the script uses the shape both implementations get right, tested on
LibreSSL 3.3.6 and OpenSSL 3.6). If yours still cannot, the failure says so and names the fix
— point it at another build:

```sh
OPENSSL="$(brew --prefix openssl@3)/bin/openssl" ./gen-certs.sh ca
```

**Run it on an admin workstation, not on a broker host.** The CA private key is the cluster's
trust root, and the cluster bus binds node identity to a certificate's Subject CN — so
anything that can read that key can mint a leaf claiming any node's identity and forge that
node's gossip signatures. `gen-certs.sh` keeps it in its own directory and never lists it
among the files to copy to a node.

```sh
cd deploy/systemd

# ONCE, for the whole cluster. Not once per host: three self-signed CAs are three
# mutually-untrusting trust roots, and three brokers that all start, log
# "SIGNED per-node" and never become ready.
./gen-certs.sh ca

# Once per node: --lifetime, then <MQTTD_NODE_ID> <MQTTD_PEER_ADVERTISE host> [names your
# CLIENTS dial]. The lifetime is yours to state: there is no default (see below).
./gen-certs.sh node --lifetime 24h mqttd-1 mqttd-1.internal.example.com mqtt.example.com
./gen-certs.sh node --lifetime 24h mqttd-2 mqttd-2.internal.example.com mqtt.example.com
./gen-certs.sh node --lifetime 24h mqttd-3 mqttd-3.internal.example.com mqtt.example.com

# The (empty) revocation list, so that putting a node out later is a file replacement.
./gen-certs.sh crl
```

Each `node` run prints when what it minted ends and the date to renew it by, then the exact
`scp` and per-file `install` commands for that host — five
files (`peer-ca.pem`, `peer.pem`/`peer.key`, `server.pem`/`server.key`), each installed with
its own mode. There is deliberately **no `chmod /etc/mqttd/tls/*.key`** step: a glob hands
every key in that directory to the `mqttd` group, which is precisely how a CA key ends up
readable by the service account it must never be readable by.

Then keep `mqttd-pki/ca/peer-ca.key` on the admin machine or move it to offline storage. You
need it again to add a node, to renew one and to revoke one, which with short lifetimes is
often: plan where it lives accordingly.

### The lifetime is stated, and renewing is yours

`node` and `renew` mint **nothing** without `--lifetime` (`<N>h`, `<N>d` or `<N>m`;
`LEAF_LIFETIME` in the environment states it too). There is no default: how long a node's
certificate lives is decided by whoever deploys the cluster
([ADR 0088](../../docs/adr/0088-credential-lifetimes-and-rotation.md)). Without it the
script names the option and exits 2.

Each run prints when the certificates end and the date to **renew by**:

```
  lifetime      24h, as stated (--lifetime 24h) — checked against both certificates
  valid until   2026-10-15T02:34:41Z   (Oct 15 02:34:41 2026 GMT, read back from peer.pem)
  RENEW BY      2026-10-14T18:35:09Z   — 667 thousandths of the lifetime, where a broker
                starts warning about it (tls.expiry_warn_permille). ...
```

The renew-by date is two thirds of the lifetime, which is where the broker starts warning
about the file (`tls.expiry_warn_permille`, default `667`; see
[OPERATIONS](../../docs/OPERATIONS.md#certificate-lifetimes-what-the-broker-tells-you-adr-0088)).
If your brokers run another threshold, set `RENEW_AT_PERMILLE` to it. Expiry is strict: a
node whose certificate has ended is out of the cluster until it is given a new one. Short
is the aim (hours to a day), because a certificate is good until it ends whether or not the
node it names is still yours; a short lifetime is also a renewal you owe that often, so
automate it (a timer on the admin machine that runs `renew` and the install below) before
you choose hours.

The CA and the revocation list are announced instead of asked for: the CA lives `CA_DAYS`
(3650) and every `ca` run prints its end; the list's `nextUpdate` is 30 days unless
`--crl-lifetime` says otherwise, printed with its own re-issue-by date.

### Renewing a node

```sh
./gen-certs.sh renew --lifetime 24h mqttd-1
```

New certificates and new keys (`--reuse-key` keeps the keys) for a node this PKI directory
already holds, with the same CN and the same SANs, read back from the certificates being
replaced. It prints the `scp` and `install` commands for the four leaf files. Install them
over the old ones and run `sudo systemctl reload mqttd`, or wait for the file watcher if
`MQTTD_CONFIG_WATCH` is set: the broker serves the new certificate on the next
handshake and signs its next gossip datagram with the new key, with no restart. A node on
its new certificate and one on its old accept each other, so renew one node at a time, in
any order. `mqttd --check-tls` on the node shows the new window.

A node that was down past the end of its certificate is renewed the same way; it stays out
until the new files are on it.

### Revoking a node, and decommissioning one

`mqttd --decommission` hands a node's data to the others and stops it. It does not make the
node's certificate stop working: that certificate is good until it ends. So removing a node
for good has a last step, on the admin machine:

```sh
./gen-certs.sh revoke mqttd-3
```

Every certificate this CA issued to that node id goes on the cluster's revocation list,
`mqttd-pki/ca/peer-crl.pem`, which the script verifies against the CA before writing it.
Copy it to every **remaining** node as the file `MQTTD_PEER_TLS_CRL` names and reload: each
node ends the revoked node's established links, refuses its handshakes and drops its
gossip, with no restart. This is also how a node is put out *before* its certificate ends,
which is the reason to do it when a host is lost or a key may have leaked.

The reload is live only on a broker that already runs with `MQTTD_PEER_TLS_CRL`: the path
is configuration, read at startup. Set it when you deploy, pointing at the empty list
`./gen-certs.sh crl` issues (it is commented out in `mqttd.env.example`). Re-issue the list
before its `nextUpdate` with `./gen-certs.sh crl` and distribute it the same way; a list
past it still loads and is enforced as it is, and the broker warns about it.

The CA keeps a ledger for this next to its key (`ca/index.txt`, `ca/issued/`,
`ca/crlnumber`): which certificates were issued and which are revoked. It holds no key.
`scripts/deploy-smoke.sh` runs `renew` and `revoke` against two running nodes on every CI
run.

**Four rules the peer certificate must satisfy**, listed in `mqttd.env.example` at the point
of use because they carry over to your own CA — three of the four fail at runtime rather than
at issue time:

1. Subject CN **equals** `MQTTD_NODE_ID` (a peer may only claim the id its certificate
   attests to; a mismatch drops the link).
2. A SAN covering the **host part of `MQTTD_PEER_ADVERTISE`** — the name a dialing peer
   verifies against.
3. **Both** `serverAuth` *and* `clientAuth`: every node dials and is dialed, and rustls
   rejects a client certificate without `clientAuth`.
4. An **ECDSA P-256/P-384 or Ed25519** key, **never RSA** — that key is also the per-node
   gossip signing key (ADR 0022), and an RSA one gives you a clean TLS handshake followed by
   a hard startup failure reading `unsupported or unparseable gossip signing key`.

This is a **starter PKI**: self-signed, a CA key in a local directory, disposable. Replace
it before production; the four rules, a stated lifetime and a revocation list every node
reads are the parts that survive.

**Client mTLS needs a SECOND CA.** `MQTTD_TLS_CLIENT_CA` must not point at `peer-ca.pem`,
and nothing here mints a client CA — you bring one. The cluster bus trusts every leaf under
its own CA as a mesh member: such a peer can vouch for any MQTT identity it names, and one
whose CN equals a node id joins the mesh as that node. Point the client CA at the bus CA and
every device certificate becomes a cluster credential, which is *less* security than the
password file it was meant to add to. `mqttd.env.example` says so at the setting.

## The five lines you edit

`mqttd.env.example` marks them. Everything else is identical on every host.

1. **`MQTTD_NODE_ID`** and **`MQTTD_PEER_ADVERTISE`** — this node's identity, and the
   address *other nodes* dial to reach it. It must be routable from them: a hostname or
   the real IP, never `0.0.0.0`, never `127.0.0.1` on a multi-host cluster.
2. **`MQTTD_SWIM_SEEDS`** — where to look for the cluster. **Exactly one node bootstraps
   with an empty list**; that is what makes it found the cluster. Every other node names
   two others, so one node being down does not block a rejoin.
3. **`MQTTD_READY_MIN_MEMBERS`** — a majority of your node count (2 for three, 3 for five).
   The founder needs `1` on its first boot only, or it can never come up alone.
4. **The secret paths** — password file, ACL, gossip key. Referenced by path so the
   environment file itself holds no secrets and can be managed by configuration
   management like any other file.
5. **The TLS paths** — the client keypair and the cluster-bus CA/keypair (see
   [Certificates](#certificates)). These are *uncommented* in the shipped file, so a host
   that has not been given certificates refuses to start instead of serving cleartext. The
   plaintext listener is present but commented out and labelled at the point of use; turning
   it on is a deliberate, loudly-logged choice, not the default. With the gossip key and the
   cluster-bus material both present, gossip is per-node signed (ADR 0022).

   **There is no rolling upgrade to this posture from a plaintext cluster.** Neither half has
   a mixed mode: signed gossip has none, and the peer bus has none either — a node with
   peer-TLS material accepts only mTLS links, with no sniffing and no plaintext fallback, so
   a rolled node forms no bus link with an un-rolled one whatever `MQTTD_SWIM_SIGNED` is set
   to. Mid-roll you get two halves that cannot replicate or route to each other, and the
   minority half loses lease-group readiness while its listener still accepts clients.
   Restart the whole cluster; `/var/lib/mqttd` holds the durable state, so that is downtime,
   not data loss.

### Arm the founder once the cluster exists

The seedless founder must not stay seedless forever. With its data directory intact it
rejoins correctly, but if that directory is ever lost, a seedless node founds a **second**
cluster. After the cluster is formed:

```sh
sudo sed -i 's|^MQTTD_SWIM_SEEDS=$|MQTTD_SWIM_SEEDS=node2.example.com:7946,node3.example.com:7946|' \
  /etc/mqttd/mqttd.env
sudo sed -i 's|^MQTTD_READY_MIN_MEMBERS=1$|MQTTD_READY_MIN_MEMBERS=2|' /etc/mqttd/mqttd.env
sudo systemctl restart mqttd
```

(The broker contains the failure regardless — a re-founder hears the survivors and
self-quarantines before it can pass readiness, ADR 0054 — but containment is the backstop,
not the plan.)

## Operating it

```sh
systemctl reload mqttd     # SIGHUP: re-read ACL, passwords, TLS material, in place
systemctl status mqttd
journalctl -u mqttd -f
mqttd --probe /livez       # is the process wedged?  (restart it)
mqttd --probe /readyz      # should it get client traffic?  (pull it from the LB)
```

`reload` goes through validate-before-swap: a bad edit is rejected and the running config
kept (ADR 0032). A tightened ACL reaches already-connected clients (ADR 0040).

**Rolling restart:** one node at a time, waiting for `--probe /readyz` to pass before
moving on. `TimeoutStopSec=60` is longer than `MQTTD_SHUTDOWN_GRACE=30` on purpose, so
systemd never `SIGKILL`s a broker that is still draining.

**Decommissioning a node for good:** run `mqttd --decommission` first — it hands every key
this node owns to its post-departure replicas and waits — then stop the unit and remove it
from the other nodes' seed lists. Nothing rewrites seed lists at runtime; a joiner whose
every seed has been decommissioned retries forever without joining.

## Rules that call `jq`, or declare a WebAssembly module: one line to change

The rule engine ([docs/RULES.md](../../docs/RULES.md)) has two things that run as
WebAssembly compiled to machine code inside the broker: EMQX's `jq` function, and
functions of your own that a rules file declares as modules (`[functions.<name>]`). Both
are **off unless you ask for them**: `jq` needs `rules.jq = true` (`MQTTD_RULES_JQ=1`),
and a module needs a table in the rules file. A broker that uses neither creates none of
it, and nothing in this section applies to it.

Compiled code needs memory the process may map executable, and this unit forbids exactly
that with `MemoryDenyWriteExecute=true`. The decision recorded in
[ADR 0086](../../docs/adr/0086-wasm-rule-functions.md) (D13) is:

- **The unit stays as it is.** `jq` is off by default, and an installation that does not
  use it keeps the restriction.
- **You change it, on purpose, when you switch `jq` on or declare a module.** The broker
  cannot lift a restriction its service manager imposed, and you should see that you are
  trading it for the feature. There is no slower mode that keeps both.

On every node that will run such rules:

```sh
sudo systemctl edit mqttd          # opens /etc/systemd/system/mqttd.service.d/override.conf
```

```ini
[Service]
MemoryDenyWriteExecute=false
```

```sh
echo 'MQTTD_RULES_JQ=1' | sudo tee -a /etc/mqttd/mqttd.env    # for jq; a module needs no switch
sudo systemctl restart mqttd       # the unit's properties and rules.jq are read at start
systemctl show mqttd -p MemoryDenyWriteExecute                # MemoryDenyWriteExecute=no
```

Use the drop-in; do not edit the unit file, so that an upgrade of the unit keeps your
change and `systemctl cat mqttd` shows it as yours. Everything else the unit restricts
stays in force.

**If you forget the drop-in, nothing crashes.** A rules file that calls `jq` or declares a
module fails to load and says why. At start the broker exits with that error (and
`Restart=on-failure` retries it five times in five minutes, then stops: read the journal);
on `systemctl reload` the running rules stay and the reload is rejected. Rules that use
neither load as before. The message names the line to change:

> ``rule `shape`: jq is not available: the WebAssembly compiler cannot run on this host (it needs memory it may map executable): unable to make memory executable: failed to make memory executable: Permission denied (os error 13). Compiled code needs memory the process may map executable: under systemd set `MemoryDenyWriteExecute=false` for the service (`systemctl edit mqttd`), or lift the seccomp or SELinux rule that forbids it; see RULES.md``

To ask a host the question before a rule depends on the answer, run the config check
under the property itself. It prints a `warning:` line with the same explanation when
compiled code is refused, and `rules.jq is on: this host allows compiled code` when it is
not, and exits 0 either way:

```sh
sudo systemd-run --wait --pipe --collect --quiet -p MemoryDenyWriteExecute=true \
  -E MQTTD_RULES_JQ=1 -E MQTTD_DURABLE_SESSIONS=0 /usr/local/bin/mqttd --check-config
```

CI runs the broker this way under this unit's hardening properties on every change to the
unit or the sandbox, and requires both halves: refused with the message above while the
property is `true`, loading and running the same rules once it is `false`.

What you give up by setting it to `false`, and how to bound what `jq` and modules can
cost, is in [docs/HARDENING.md](../../docs/HARDENING.md) (H-7.12 to H-7.15). In
particular keep `MemoryMax` below: concurrent long `jq` calls are not capped by the
broker, and the cgroup limit is the hard bound.

## What the unit does for you

`ProtectSystem=strict` with `/var/lib/mqttd` as the only writable path,
`CapabilityBoundingSet=` (empty — the reference ports are `8883` and `8080`, so no
capabilities at all; add `AmbientCapabilities=CAP_NET_BIND_SERVICE` only if you bind `:443`
for WSS),
`SystemCallFilter=@system-service`, `MemoryDenyWriteExecute` (which the rule engine's
`jq` and WebAssembly modules cannot live with: [above](#rules-that-call-jq-or-declare-a-webassembly-module-one-line-to-change)),
`NoNewPrivileges`, and address families restricted to INET/INET6/UNIX.

`MemoryMax=2G` is a real bound, not decoration: the broker has no total-memory knob yet
(`docs/SIZING.md` says so plainly), so the cgroup limit is what stands between a runaway
queue and the host. `MemoryHigh` throttles first so you get a warning before an OOM kill.

Verify any edit before deploying it:

```sh
systemd-analyze verify /etc/systemd/system/mqttd.service
```

A typo'd hardening directive is *silently ignored* by systemd — the unit looks hardened
and is not. CI runs this check on the shipped file for the same reason.
