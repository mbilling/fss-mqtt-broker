# Durable QoS 1 scale-out at 10, 12 and 14 nodes — one provisioning

Campaign card. Env:
[`qos1-campaign/durable-scaleout-10-12-14.env`](qos1-campaign/durable-scaleout-10-12-14.env).
Driver: [`482-per-node-knee.sh`](482-per-node-knee.sh). Method and harness safeguards:
[`482-per-node-knee.md`](482-per-node-knee.md).

**This is a diagnostic scale-out, read by a simple rule. It is not a certified curve.**
Its figures are for saying "the cluster carried this much at this size" in plain words.
They are not published-curve points (ADR 0048), and 12 and 14 nodes say nothing about
what the cluster survives (see [Placement](#placement-two-groups-above-10-brokers)).

Not run yet. This card describes the campaign; results go here when it has run.

## The question

Does durable QoS 1 keep growing when brokers are added past 10? Concretely: do 12 and
14 nodes carry the same per-node rate that 10 nodes carry, on the same hosts?

## The reading rule

A rung **holds** if:

1. **no message is lost**: the ledger shows `missing = 0` for the rung
   (`qos1-campaign/ledger-report.py`: every message identity sent, acknowledged and
   received, reconciled across the whole `$share` group), and no broker dropped
   anything, and
2. **p99 stays below 1 s**: the rung's end-to-end p99 is in the ≤ 1000 ms bucket or
   lower (`summarize-curve.py`).

The rate a rung holds at is the brokers' own: each broker's delivered counter over its
window. `summarize-curve.py` computes the rule and prints it per rung, with its inputs,
under "Reading from the brokers' counters", beside the rig's verdict and apart from it.

That is the maintainer's rule for this campaign: not 100% scientifically correct, but
easy to communicate. What it leaves out, on purpose:

- **The rig's own verdict is stricter.** `summarize-curve.py` also grades a rung NOT
  CARRIED when publishers could not keep their timers (late publishes, a sent rate below
  the offer). On 2026-10-12 every 10-node rung above the drift control was NOT CARRIED
  for that reason with zero loss and p99 ≤ 500 ms. Under this card's rule those rungs
  hold. Report the delivered rate, not the offered one, when the two differ.
- **The crossing gate is reported, not applied** (next section).
- **One 60 s window per rung, no repetitions.** A rung that holds once is a reading, not
  a median.

## Crossing: reported per arm, not gated

`extract-lane-e.py --crossing-gate 0.5` prints, per arm, the share of publishes each
broker forwarded to another broker. The shape predicts about 0%: every broker holds a
local member of every site's `$share` group.

**That prediction does not hold for persistent consumers.** In the 2026-10-12 durable
campaign both 10-node arms failed this gate: 13–17% of publishes were forwarded between
brokers at 25–40 sites, and the 3-node arm measured 0.00%. A persistent session lives on
the broker that owns it, not on the one its client dialled, so some brokers hold no
member of a site's group
([`durable-knees-2026-10-12.md`](durable-knees-2026-10-12.md#cross-node-traffic)). Expect
forwarding here too: with 28 consumers per site, (1 − 1/N)^28 is 12.6% at 14 nodes, 8.7%
at 12 and 5.2% at 10. That is a prediction, not a result.

This campaign prints the crossing of every arm and, per the reading rule, does not gate
on it. The consequence to keep in mind: an arm that forwards carries work an arm that
does not forward is spared, so compare the three sizes' crossing before comparing their
rates.

## Shape

- **One provisioning:** 14 × CCX23 brokers (4 dedicated vCPU, local NVMe) + 16 × CCX33
  drivers (8 vCPU), Hetzner `fsn1`.
- **Quota:** 14 × 4 + 16 × 8 = 56 + 128 = **184 vCPU on 30 servers**. The project allows
  200 vCPU and 30 servers, so the server count is exactly at its limit.
- **Arms**, largest first, each re-formed as a fresh cluster on a prefix of the same
  hosts (`resize-cluster.sh`), all 16 drivers in every arm:

  | arm | brokers | left out (stopped, still billed) | sites | offered msg/s | per node |
  |---|---|---|---|---|---|
  | 1 | 14 | none | 1 · 35 · 42 | 6k · 210k · 252k | 15k · 18k |
  | 2 | 12 | mqttd-13, mqttd-14 | 1 · 30 · 36 | 6k · 180k · 216k | 15k · 18k |
  | 3 | 10 | mqttd-11 … mqttd-14 | 1 · 25 · 30 | 6k · 150k · 180k | 15k · 18k |
  | 4 | 14 | none | 1 · 35 | 6k · 210k | 15k (drift control) |

  Each arm closes with a repeated 1-site control. Arm 4 repeats arm 1's 35-site rung:
  if the two differ, the hosts drifted and the sizes between them are not comparable.
- **A site** is 600 QoS 1 publishers at 10 msg/s (6,000 msg/s, 200 B) into a `$share`
  group of **28 persistent** QoS 1 consumers (`LANE_E_SESSION_EXPIRY=30`): every
  delivery waits for fsync and replication. One publisher container and two consumer
  containers per site. 60 s windows.
- **Store:** the default (redb), 2 copies. `MQTTD_REPLICA_STORE` and `MQTTD_REPLICAS`
  are unset.
- **Binary:** pinned in the shell, not in the env (`MQTTD_VERSION`, or `MQTTD_URL` +
  `MQTTD_SHA256` + `BENCH_GIT_REF`), as is the audit driver (`QOS1_DRIVER_ARCHIVE`).

### 28 consumers per site, not 20

The 2026-10-12 campaign used 20 consumers per site in two containers. Each container's
10 clients dial 10 consecutive brokers, and the second container starts one broker
later, so the two reach 11 brokers. That was every broker at 10 nodes. At 14 it leaves
three brokers without a local member, and the shape check refuses it: "20 consumers in 2
containers cover only 11 of 14 brokers … ~21% of publishes would forward". 28 puts 14
clients in each container, so each one reaches every broker at every size.

Consequences: the 10-node arm here is not the 2026-10-12 10-node shape (28 consumers per
site, not 20), and at 12 and 10 nodes a container's 14 clients land two on some brokers
and one on others.

## Risks

- **Drivers are fully packed at 42 sites.** 42 sites × 3 containers = 126 one-vCPU
  containers on the 128 vCPUs the 16 drivers have: 8 containers on an 8-vCPU driver. The
  shape check accepts it. A driver with no idle core is the shape in which a driver limit
  reads as a broker limit, so read the 42-site rung's driver CPU and its late-publish
  share before blaming the brokers. The 35-site rung uses 105 containers, 7 per driver.
- **No server to spare.** 30 of 30 servers. `replace-node.sh` swaps a bad host with
  `tofu apply -replace`, which must delete the old server before it creates the new one
  to stay inside the quota.
- **First clusters above 10 nodes on this rig.** Formation at 12 and 14 (founder first,
  13 followers joining at once, the founder re-armed at a majority of 8), the full peer
  mesh (91 links at 14) and the settling time after formation have not been exercised.
- **The forwarding canary covers 182 directed pairs at 14 nodes** (132 at 12, 90 at
  10): 100 QoS 1 messages each, 18,200 in all, inside one deadline for the whole
  control. The env raises that deadline from 90 s to 180 s. Against loopback fakes the
  14-node control takes about 4 s of client time; against real brokers it has not run.
- **Left-out brokers are the highest-numbered**, which are the ones in the second
  placement group. The 10-node arm therefore runs entirely on the first group and the
  12- and 14-node arms add hosts of the second.

## Placement: two groups above 10 brokers

Hetzner admits at most 10 servers to a spread placement group. Brokers 1–10 are in the
original group and brokers 11–14 in a second one (`terraform/layout.tf`). Each group
keeps its own members on distinct physical hosts. Nothing keeps a broker of one group off
a host that carries a broker of the other. So at 12 and 14 nodes two brokers may share a
physical machine: the result is a throughput figure, and makes no claim about failure
independence.

## Reproduce

```sh
cd bench/scale
export HCLOUD_TOKEN=...          # a dedicated Hetzner project, 30 servers / 200 vCPU
set -a && . qos1-campaign/durable-scaleout-10-12-14.env && set +a
export MQTTD_VERSION=...         # or MQTTD_URL=... MQTTD_SHA256=... BENCH_GIT_REF=<commit>
export QOS1_DRIVER_ARCHIVE=/absolute/path/to/driver.tar.gz   # qos1-driver/build.sh <dir>

PREFLIGHT_ONLY=1 ./482-per-node-knee.sh   # offline: every arm's shape, budgets, self-tests
./482-per-node-knee.sh                    # PAID: 14 -> 12 -> 10 -> 14 on 30 servers

# per arm: the two things the reading rule needs, then the crossing
python3 qos1-campaign/ledger-report.py .runs/knee-<stamp>/<arm>      # missing must be 0
python3 summarize-curve.py .runs/knee-<stamp>/<arm>/results          # p99, delivered/s
python3 extract-lane-e.py --crossing-gate 0.5 .runs/knee-<stamp>/<arm>/results   # reported, not gated
```

Arm directories are `1-n14`, `2-n12`, `3-n10`, `4-n14`. `RESIZED.txt` in each resized arm
names the brokers kept and left out.
