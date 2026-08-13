# A three-node mqttd cluster with Docker Compose

```sh
cd deploy/compose
./bootstrap.sh              # gossip key + Argon2id password file + the plaintext to hand out
docker compose up -d
docker compose ps           # all three healthy in ~60s
```

The first `up` runs an `init` one-shot before any broker starts: it mints a **throwaway
starter CA** and one certificate per node, stages the secrets where the brokers' uid can
read them, and writes `secrets/ca.pem` for clients on this host. It is idempotent, so
later `up`s reuse the same PKI. Each node's key lands in its **own** volume, and the CA
private key in one that no broker mounts.

> **If your containers never go healthy** — `docker compose ps` showing all three
> `unhealthy` forever, or `./bootstrap.sh` writing log lines into `secrets/mqttd-passwd` —
> you are running the published `:latest` image, which is still **v0.9.0**. That build
> predates `mqttd --hash-password` and the `--probe` fast path this file's healthcheck uses.
> Tracked in **issue #263**. Until it is retagged, run against a broker built from this
> repository: `MQTTD_IMAGE=<your-tag> docker compose up -d` (and export the same
> `MQTTD_IMAGE` before `./bootstrap.sh`, which uses it for hashing). This is also why
> `scripts/compose-smoke.sh` always sets `MQTTD_IMAGE`, and why nothing here claims the
> default tag is tested.

Then, from `secrets/PASSWORDS.txt`:

```sh
mosquitto_sub -h 127.0.0.1 -p 8883 --cafile secrets/ca.pem \
              -t 'devices/+/up/#' -u backend -P '<password>'
mosquitto_pub -h 127.0.0.1 -p 8885 --cafile secrets/ca.pem \
              -t 'devices/device-a/up/temp' -m 21.5 -q 1 \
              -u device-a -P '<password>'
```

Note the ports differ: that publish went to **node 3** and the subscriber is on **node 1**.
Cross-node routing is the point of the cluster.

To look at the handshake itself:

```sh
openssl s_client -connect 127.0.0.1:8883 -CAfile secrets/ca.pem </dev/null
```

## What you get

- **TLS 1.3 on `8883`, and no plaintext listener at all.** `MQTTD_PLAINTEXT_BIND` appears
  nowhere in `compose.yaml`. That is deliberate and structural: the password file is
  Argon2id-hashed at rest, and a plaintext listener would put the passwords themselves on
  the wire. TLS 1.2 is off by default too.
- **A mutually-authenticated cluster bus.** Each node carries its own certificate whose
  Subject CN *is* its node id, and the bus enforces that binding — a peer may only claim
  the id its certificate attests to. Nothing that can merely reach `:7001` can speak to it.
  Every leaf under that CA *is* a mesh credential, which is why client mTLS needs a
  different CA (see "Before production").
- **One TLS volume per broker.** `mqttd-tls-1/2/3` each hold one node's key plus the CA
  *certificate*, mounted read-only into that broker alone; `mqttd-ca` holds the CA private
  key and is mounted into the `init` one-shot only. All three brokers run as the same uid,
  so a shared volume would let any of them read the others' peer identity — and the bus
  binds identity to the certificate CN. `scripts/compose-smoke.sh` lists each volume in the
  running stack to check it, and `scripts/deploy-smoke.sh` checks the mount list on every PR.
- **Per-node signed gossip** ([ADR 0022](../../docs/adr/0022-signed-gossip.md)), because
  both the shared gossip key and the cluster-bus certificates are present. Each broker
  logs `SWIM gossip is SIGNED per-node` at startup; `MQTTD_SWIM_SIGNED` is set explicitly (to
  `require`, the same value the material would imply anyway), so removing the certificates is
  a startup error rather than a quiet downgrade.
- **Three brokers**, each with its own volume, forming one cluster over authenticated gossip.
- **Authentication on.** `MQTTD_ALLOW_ANONYMOUS` appears nowhere in this directory. The
  broker denies by default and the compose file does not undo that.
- **Deny-by-default topic ACLs** ([`acl.toml`](acl.toml)) scoping each device to its own
  subtree via `%i`.
- **Real health checks**, on a broker newer than v0.9.0 (see the note above and issue #263).
  `mqttd --probe /readyz` — the image is distroless (no shell, no curl), so the binary probes
  itself. `/readyz` is majority-aware: a node that cannot see a quorum reports unhealthy
  instead of serving clients from a store it cannot write. That holds for nodes 2 and 3 from
  the first boot and for **node 1 once you arm it** — it starts with a floor of 1 so the
  cluster can be founded at all, and stays exempt from the majority rule until you raise it
  ("After the first bring-up", below).
  The health ports are published on **loopback only**, because `/metrics` is unauthenticated.
- **Bounded memory.** `docs/SIZING.md` is explicit that the broker has no total-memory
  knob yet, so the container limit *is* the bound. It is set, not left open.

## Plaintext, if you really want it (opt-in, insecure)

```sh
docker compose -f compose.yaml -f compose.plaintext.yaml up -d
```

That adds a plaintext listener on `127.0.0.1:1883` / `1884` / `1885` alongside the TLS one.
What you give up: passwords and payloads cross the wire in cleartext. Every broker logs
`INSECURE: starting PLAINTEXT MQTT listener` on every start, and keeps doing so — the label
is not a one-time warning you can dismiss.

It is not called `compose.override.yaml` on purpose. Compose loads *that* filename
automatically, with no flag and nothing in the output saying so, and a stray copy would
downgrade a cluster invisibly. The `-f … -f …` form appears in shell history, in CI logs,
and in the bug report somebody eventually files.

## Upgrading an existing plaintext cluster

If you already run a cluster from an older copy of this file, **there is no rolling upgrade.
Take the outage:**

```sh
docker compose down && docker compose up -d
```

The volumes hold the durable state, so that is downtime, not data loss. Clients must also be
given `secrets/ca.pem` and moved from `1883` to `8883` — the plaintext listener is gone.

Why no rolling `up -d`: **none of the three things that change has a mixed mode.**

- *Signed gossip* has none — a node with the new certificates and one without cannot verify
  each other's gossip.
- *The cluster bus* has none either. A node with `MQTTD_PEER_TLS_*` set starts a TLS peer
  acceptor and only that one; there is no sniffing and no plaintext fallback
  (`crates/mqttd/src/peer.rs::serve_listener` takes the plaintext branch only when its TLS
  context is `None`). So a rolled node forms **no** bus link with an un-rolled one.
- *The client listener* has none: a rolled node no longer serves `1883` at all, which is the
  port the existing cluster's clients are connected to.

`MQTTD_SWIM_SIGNED=off` therefore does **not** buy a no-downtime roll. It keeps the gossip
half whole and moves the split onto the peer bus: mid-roll, the rolled and un-rolled halves
cannot replicate or route to each other, and the half that is in the minority fails
`lease_group_ready` (`crates/mqttd/src/health.rs`) and drops out of rotation while its
listener still accepts clients — so QoS 1 durable publishes to it hang or fail. That is an
outage with extra steps, which is why only one route is documented here.

The variable stays interpolated for a narrower job. `compose.yaml` sets
`MQTTD_SWIM_SIGNED: ${MQTTD_SWIM_SIGNED:-require}` — spelled out rather than left implicit, so
deleting the peer-TLS lines becomes a startup error instead of a quiet downgrade, and
interpolated so that choosing the shared-key gossip posture is one `.env` line rather than an
edit to the shipped file.

## After the first bring-up: arm the founder

`mqttd-1` starts with **no seeds** and a **readiness floor of 1**, and that pair is exactly
what lets it found the cluster: seedless so it does not wait for anyone, and able to report
ready while it is alone. Both are first-boot compromises.

- Leaving it seedless is safe while its volume survives (it knows it is initialised and
  rejoins). It is *not* safe if that volume is ever lost: a seedless node with an empty data
  directory founds a **second** cluster.
- Leaving the floor at 1 makes node 1 **permanently exempt from the majority rule** the rest
  of this file relies on. Alone in a partition it still answers `/readyz` with `ok`, so Docker
  keeps it "healthy" and a load balancer keeps sending it clients — whose QoS 1 publishes then
  hang, because it cannot reach a quorum to durably store them.

Once the cluster is formed, fix both in one step:

```sh
printf 'MQTTD_1_SEEDS=mqttd-2:7946,mqttd-3:7946\nMQTTD_1_READY_MIN_MEMBERS=2\n' >> .env
docker compose up -d mqttd-1
docker compose exec mqttd-1 /usr/local/bin/mqttd --probe /readyz   # still ready: it sees 3
```

Both variables are interpolated in `compose.yaml` for exactly this, and
`scripts/deploy-smoke.sh` asserts both renderings — `1` by default, `2` with
`MQTTD_1_READY_MIN_MEMBERS=2` set — so the step cannot silently stop working. What the floor
then buys is proven separately in the same script: a node that can no longer see a majority
of three reports live-but-not-ready.

The broker contains the re-founding failure even if you forget the seeds — a re-founder hears
the survivors and self-quarantines within about a second, before it can pass readiness
(ADR 0054) — but you want this not to happen, not to be contained. Nothing contains a lone
node with a floor of 1.

## Operating it

```sh
docker compose logs -f mqttd-1
docker compose kill -s HUP mqttd-1 mqttd-2 mqttd-3   # reload ACL / passwords / TLS in place
docker compose restart mqttd-2                        # one at a time; wait for healthy
docker compose exec mqttd-1 /usr/local/bin/mqttd --probe /readyz
```

A tightened ACL reaches **already-connected** clients on reload (ADR 0040); it does not
wait for them to reconnect.

**Adding a user** — no restart needed, but there is an extra step now: the brokers read a
*staged copy* of the password file (out of a volume the `init` one-shot owns), not the
`secrets/` directory directly, so appending a line and only sending `HUP` is a silent
no-op.

```sh
printf %s 'their-password' | docker run --rm -i "$MQTTD_IMAGE" \
  --hash-password device-c >> secrets/mqttd-passwd
docker compose up -d init                            # restage it (prints what it staged)
docker compose kill -s HUP mqttd-1 mqttd-2 mqttd-3   # reload it in place
```

`$MQTTD_IMAGE` — the same broker your cluster runs — and **not** the published `:latest`:
v0.9.0 has no `--hash-password`, so that variant writes its startup log into the password
file and every broker then refuses to load it (`duplicate username in password file`).
Issue #263, same cause as the note at the top.

`acl.toml` already grants `device-*`, so `device-c` needs no policy edit.

**Adding a node** — there are now **three** edit points, and only the first is obvious:

1. copy the `mqttd-3` service block to `mqttd-4` (new `MQTTD_NODE_ID`, `hostname`,
   `MQTTD_PEER_ADVERTISE`, seeds, its own data volume, its own published ports, and
   `mqttd-tls-4:/etc/mqttd/tls:ro`);
2. add `mqttd-4` to the `init` service's **`MQTTD_NODES`** list, which is what decides how
   many leaves the PKI one-shot mints; and
3. declare `mqttd-tls-4` under `volumes:` and mount it into `init` as well, at
   `/certs/mqttd-4`.

Miss step 2 and node 4 has no certificate. It fails closed — `cannot read
MQTTD_PEER_TLS_CERT (/etc/mqttd/tls/mqttd-4.pem): No such file or directory (os error 2)` —
which is the right behaviour but says nothing about `MQTTD_NODES`. Miss step 3 and `init.sh`
stops with `no directory /certs/mqttd-4 — nothing is mounted for node 'mqttd-4'`, naming the
volume, rather than minting a leaf into a container filesystem that is about to disappear.
`init.sh` is idempotent per node, so `docker compose up -d` mints only the missing leaf under
the existing CA; the running nodes keep theirs. Raise `MQTTD_READY_MIN_MEMBERS` to a majority of the new count (3 for five
nodes) as a separate, deliberate roll — **and raise `MQTTD_1_READY_MIN_MEMBERS` in `.env`
with it.** That is a second, independent floor: `compose.yaml` overrides the shared value for
the founder (`MQTTD_READY_MIN_MEMBERS: ${MQTTD_1_READY_MIN_MEMBERS:-1}`) so it can come Ready
alone on first bring-up, so changing only the shared setting leaves mqttd-1 at its old floor
and silently exempt from the majority rule the other nodes now follow.

## Before production

- **Replace the starter PKI.** The CA `init.sh` mints is self-signed, trusts nobody in
  particular, lives unencrypted in a Docker volume, and is regenerated the moment you
  `down -v`. It makes the *default* secure; it is not your production trust root. Bring
  your own CA and mount real certificates in place of the `mqttd-tls-1/2/3` volumes (keeping
  one volume per broker, and the CA private key out of all of them). For client mTLS,
  `MQTTD_TLS_CLIENT_CA` is present but commented in `compose.yaml` — the session identity
  then becomes the client certificate's CN — and it **must be a different CA from
  `MQTTD_PEER_TLS_CA`**, which the shipped starter PKI does not give you: you bring it. The
  bus trusts every leaf under its own CA as a mesh member (it can vouch for any MQTT identity
  it names, and a CN equal to a node id joins the mesh as that node), so pointing the client
  CA at the bus CA turns each device certificate into a cluster credential — strictly worse
  than the password file it was meant to strengthen. Keep all **four**
  cluster-bus rules: (1) CN must equal `MQTTD_NODE_ID`; (2) the SAN must cover the host part
  of `MQTTD_PEER_ADVERTISE`; (3) `serverAuth` **and** `clientAuth`, since every node dials and
  is dialed; (4) the key must be **ECDSA P-256/P-384 or Ed25519, not RSA** — that key is also
  the per-node gossip signing key (ADR 0022), and an RSA one hands you a working TLS handshake
  followed by `unsupported or unparseable gossip signing key` at startup. The client-facing
  keypair has none of these constraints. (`../systemd/gen-certs.sh` satisfies all four and is
  the same PKI shape in script form, if you want a starting point outside a container.)
- **Compose is one host.** Three brokers on one machine survive a *process* failure, not a
  machine failure. For real availability run them on three machines (the systemd packaging,
  or compose on three hosts with routable `MQTTD_PEER_ADVERTISE` values).
- **Size it.** Memory limits here are a starting point; `docs/SIZING.md` has the formulas.

## `docker compose down` will not delete your data — but `-v` will

The volumes hold durable sessions, retained messages, and the cluster identity.
`docker compose down -v` deletes all eight: the three data volumes, the three per-node TLS
volumes, the CA volume **and** the staged secrets. There is no undo, and a cluster that loses every volume is a new
cluster. After a `-v` wipe the next `up` mints a brand-new CA, so every client must
re-trust the fresh `secrets/ca.pem`.

## Verifying this by hand

`scripts/compose-smoke.sh` does all of this in CI — against an image built from this
repository, never the default tag (issue #263) — and the same checks by hand:

```sh
cd deploy/compose
export MQTTD_IMAGE=<a broker newer than v0.9.0>       # see the note at the top
./bootstrap.sh && docker compose up -d
docker compose ps                                     # three healthy
docker compose logs | grep -i insecure                # must print NOTHING
docker compose logs | grep -c 'SIGNED per-node'       # must print 3
mosquitto_pub -h 127.0.0.1 -p 8883 -t x -m y          # must FAIL (no --cafile, no plaintext port)
docker compose down
```
