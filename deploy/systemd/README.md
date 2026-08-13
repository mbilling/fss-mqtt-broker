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

# Once per node: <MQTTD_NODE_ID> <MQTTD_PEER_ADVERTISE host> [names your CLIENTS dial]
./gen-certs.sh node mqttd-1 mqttd-1.internal.example.com mqtt.example.com
./gen-certs.sh node mqttd-2 mqttd-2.internal.example.com mqtt.example.com
./gen-certs.sh node mqttd-3 mqttd-3.internal.example.com mqtt.example.com
```

Each `node` run prints the exact `scp` and per-file `install` commands for that host — five
files (`peer-ca.pem`, `peer.pem`/`peer.key`, `server.pem`/`server.key`), each installed with
its own mode. There is deliberately **no `chmod /etc/mqttd/tls/*.key`** step: a glob hands
every key in that directory to the `mqttd` group, which is precisely how a CA key ends up
readable by the service account it must never be readable by.

Then keep `mqttd-pki/ca/peer-ca.key` on the admin machine or move it to offline storage. You
need it again only to add a node.

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

This is a **starter PKI**: self-signed, unrevocable, disposable. Replace it before
production; the four rules are the part that survives.

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

## What the unit does for you

`ProtectSystem=strict` with `/var/lib/mqttd` as the only writable path,
`CapabilityBoundingSet=` (empty — the reference ports are `8883` and `8080`, so no
capabilities at all; add `AmbientCapabilities=CAP_NET_BIND_SERVICE` only if you bind `:443`
for WSS),
`SystemCallFilter=@system-service`, `MemoryDenyWriteExecute`, `NoNewPrivileges`, and
address families restricted to INET/INET6/UNIX.

`MemoryMax=2G` is a real bound, not decoration: the broker has no total-memory knob yet
(`docs/SIZING.md` says so plainly), so the cgroup limit is what stands between a runaway
queue and the host. `MemoryHigh` throttles first so you get a warning before an OOM kill.

Verify any edit before deploying it:

```sh
systemd-analyze verify /etc/systemd/system/mqttd.service
```

A typo'd hardening directive is *silently ignored* by systemd — the unit looks hardened
and is not. CI runs this check on the shipped file for the same reason.
