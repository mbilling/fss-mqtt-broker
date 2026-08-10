# A three-node mqttd cluster on systemd

For bare metal and VMs. Same broker and same configuration as the Compose packaging;
the difference is that the machines are yours to name.

## Install, per host

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
sudo $EDITOR /etc/mqttd/mqttd.env        # four marked lines — see below

# 5. Secrets.
printf %s 'their-password' | mqttd --hash-password device-a | sudo tee -a /etc/mqttd/passwd
sudo openssl rand -hex 32 | sudo tee /etc/mqttd/swim.key >/dev/null
sudo chown root:mqttd /etc/mqttd/passwd /etc/mqttd/swim.key
sudo chmod 0640       /etc/mqttd/passwd /etc/mqttd/swim.key
sudo install -m 0640 -o root -g mqttd acl.toml /etc/mqttd/acl.toml   # from ../compose/

# 6. Go.
sudo systemctl daemon-reload
sudo systemctl enable --now mqttd
mqttd --probe /readyz        # exits 0 once this node is ready
```

## The four lines you edit

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
`CapabilityBoundingSet=` (empty — the reference ports are 1883/8883, so no capabilities at
all; add `AmbientCapabilities=CAP_NET_BIND_SERVICE` only if you bind `:443` for WSS),
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
