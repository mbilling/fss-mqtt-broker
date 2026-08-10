# A three-node mqttd cluster with Docker Compose

```sh
cd deploy/compose
./bootstrap.sh              # gossip key + Argon2id password file + the plaintext to hand out
docker compose up -d
docker compose ps           # all three healthy in ~60s
```

Then, from `secrets/PASSWORDS.txt`:

```sh
mosquitto_sub -h 127.0.0.1 -p 1883 -t 'devices/+/up/#' -u backend -P '<password>'
mosquitto_pub -h 127.0.0.1 -p 1885 -t 'devices/device-a/up/temp' -m 21.5 -q 1 \
              -u device-a -P '<password>'
```

Note the ports differ: that publish went to **node 3** and the subscriber is on **node 1**.
Cross-node routing is the point of the cluster.

## What you get

- **Three brokers**, each with its own volume, forming one cluster over authenticated gossip.
- **Authentication on.** `MQTTD_ALLOW_ANONYMOUS` appears nowhere in this directory. The
  broker denies by default and the compose file does not undo that.
- **Deny-by-default topic ACLs** ([`acl.toml`](acl.toml)) scoping each device to its own
  subtree via `%i`.
- **Real health checks.** `mqttd --probe /readyz` — the image is distroless (no shell, no
  curl), so the binary probes itself. `/readyz` is majority-aware: a node that cannot see
  a quorum reports unhealthy instead of serving clients from a store it cannot write.
- **Bounded memory.** `docs/SIZING.md` is explicit that the broker has no total-memory
  knob yet, so the container limit *is* the bound. It is set, not left open.

## After the first bring-up: arm the founder

`mqttd-1` starts with **no seeds**, and that is exactly what makes it found the cluster.
Leaving it that way is safe while its volume survives (it knows it is initialised and
rejoins). It is *not* safe if that volume is ever lost: a seedless node with an empty data
directory founds a **second** cluster.

Once the cluster is formed, give node 1 seeds too:

```sh
echo 'MQTTD_1_SEEDS=mqttd-2:7946,mqttd-3:7946' >> .env
docker compose up -d mqttd-1
```

The broker contains the failure even if you forget — a re-founder hears the survivors and
self-quarantines within about a second, before it can pass readiness (ADR 0054) — but you
want this not to happen, not to be contained.

## Operating it

```sh
docker compose logs -f mqttd-1
docker compose kill -s HUP mqttd-1 mqttd-2 mqttd-3   # reload ACL / passwords / TLS in place
docker compose restart mqttd-2                        # one at a time; wait for healthy
docker compose exec mqttd-1 /usr/local/bin/mqttd --probe /readyz
```

A tightened ACL reaches **already-connected** clients on reload (ADR 0040); it does not
wait for them to reconnect.

**Adding a user** — no restart needed, the password file is reloadable:

```sh
printf %s 'their-password' | docker run --rm -i ghcr.io/mbilling/fss-mqtt-broker:latest \
  --hash-password device-c >> secrets/mqttd-passwd
docker compose kill -s HUP mqttd-1 mqttd-2 mqttd-3
```

`acl.toml` already grants `device-*`, so `device-c` needs no policy edit.

## Before production

- **TLS.** This runs plaintext on `1883`. On anything but a trusted network, set
  `MQTTD_TLS_BIND` / `MQTTD_TLS_CERT` / `MQTTD_TLS_KEY` and drop the plaintext listener —
  and set `MQTTD_PEER_TLS_*` too, or the cluster bus is plaintext to anything that can
  reach `:7001`.
- **Compose is one host.** Three brokers on one machine survive a *process* failure, not a
  machine failure. For real availability run them on three machines (the systemd packaging,
  or compose on three hosts with routable `MQTTD_PEER_ADVERTISE` values).
- **Size it.** Memory limits here are a starting point; `docs/SIZING.md` has the formulas.

## `docker compose down` will not delete your data — but `-v` will

The volumes hold durable sessions, retained messages, and the cluster identity.
`docker compose down -v` deletes all three. There is no undo, and a cluster that loses
every volume is a new cluster.
