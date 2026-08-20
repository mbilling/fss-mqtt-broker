# bench/scale — the multi-host scaling curve rig (ADR 0048 T3)

Provisions real Hetzner Cloud clusters of 1, 3 and 5 broker hosts (one broker,
one local NVMe disk per host — ADR 0048 §2's hardware rule), drives the same
workload against each size from separate load-generator hosts, and collects the
raw results that become `docs/benchmarks/SCALE-CURVE.md`. Runs from a laptop,
**never from CI** — no cloud credential exists in this repository or its
workflows.

## Prerequisites (once)

1. **A dedicated Hetzner Cloud project** (e.g. `mqttd-bench`). Dedicated so the
   label-scoped leak sweeper (`teardown.sh --force`) can never touch anything
   that is not this rig's. The default 10-server limit fits (max 5 brokers + 3
   drivers).
2. **A Read & Write API token** for that project → `export HCLOUD_TOKEN=...` in
   the shell that runs the rig. Never committed, never a CI secret.
3. **An SSH keypair.** The default is `~/.ssh/id_ed25519`; for any other key
   (say `~/.ssh/hetzner`), `export SSH_KEY=~/.ssh/hetzner` — the rig uploads
   `${SSH_KEY}.pub` via Terraform and dials every host with that identity.
4. Laptop tools: `terraform` ≥ 1.7 (or OpenTofu), `jq`, and — on macOS —
   `openssl@3` (`brew install openssl@3`; the system LibreSSL cannot mint the
   cluster PKI and `deploy/systemd/gen-certs.sh` refuses it loudly). The
   `hcloud` CLI is optional but lets `teardown.sh` audit by label.
5. Sanity-check current CCX23/CCX33 pricing and availability in `fsn1`
   (fallbacks: `nbg1`, `hel1` via the `location` variable).

## Cost

| run | servers | ≈ wall time | ≈ cost |
|---|---|---|---|
| `smoke` | 1×CCX23 + 2×CCX33 | 20–30 min | <€0.50 |
| `full` (1+3+5) | up to 5×CCX23 + 2×CCX33, one size at a time | 4–5 h | €1.50–2 |
| forgotten 5-node stack | — | per day | ≈€8.50 |

Budget **€10** and the worst case is covered. The last line is why teardown is
trapped on EXIT/INT/TERM, why `teardown.sh` exists separately, and why step 1
says *dedicated project*: after any run, `hcloud server list` (or the console)
must show zero servers.

## Running

```sh
cd bench/scale
export HCLOUD_TOKEN=...
./run.sh smoke     # proves token → apply → PKI → bring-up → lanes → destroy
./run.sh full      # the curve; or ./run.sh full 3 5 for a subset
python3 summarize-curve.py .runs/<stamp>/results   # markdown for the doc
```

Each size is applied **fresh and destroyed** before the next — a grown cluster
keeps replica groups tracked under an earlier membership and never re-greens,
so growing 1→3→5 would measure a known-degraded configuration. `RESUME`: a
re-run with `RUN_DIR=.runs/<stamp>` skips sizes already marked done.
`KEEP_INFRA=1` skips teardown for debugging and prints a red reminder that the
meter is running. Ctrl-C is safe (trapped); `kill -9` is not — after one, run
`./teardown.sh`.

## What runs where

- **This machine:** Terraform; PKI minting (`deploy/systemd/gen-certs.sh` — the
  CA keys never leave); orchestration over SSH; result collection.
- **Broker hosts (CCX23):** the pinned, signed release binary, checksum-verified
  by cloud-init, under the shipped `deploy/systemd/mqttd.service` plus a
  disclosed drop-in (memory ceiling, fd limits); kernel tuning in
  `terraform/files/sysctl-broker.conf`. The founder-first start + arm sequence
  is `scripts/deploy-smoke.sh`'s, executed by `bootstrap-cluster.sh`.
- **Driver hosts (CCX33):** emqtt-bench 0.6.3 in docker (lanes B/C); driver-1
  builds `crates/mqttd/tests/durable_bench.rs` from a pinned ref for lane A and
  serves the binary to the brokers for the per-host fsync **barrier probes**
  that gate Curve 1.

Lane definitions, the ladder, and every validity rule live in `run-curve.sh`
and are documented in `docs/benchmarks/SCALE-CURVE.md` — the method is fixed
before the first paid run.

## Honesty notes

- Raw results under `.runs/` are untracked scratch; the published record is
  `docs/benchmarks/SCALE-CURVE.md`, and it may cite only tracked paths
  (`scripts/check-readme-facts.py` enforces this).
- A smoke run's numbers prove the pipeline, never the broker.
- Known blocker for the durable curve at N>1: issue #358 (spread-ownership
  durable acks stall), found by this rig's harness before any money was spent.
