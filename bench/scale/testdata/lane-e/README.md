# Lane E extractor fixtures (#482)

Real mqttd captures that `extract-lane-e.py --self-test` certifies — or refuses —
through `forward-canary.py`'s own ledger. Tests copy these trees into a temp dir
and mutate the copies; **never edit the exposition files here**. Every `.prom`
file is a byte-for-byte scrape of a real mqttd `/metrics` endpoint.

| tree | what | provenance |
|---|---|---|
| `smoke-n1/nodes=1/laneE/sites-1/` | a complete aligned N=1 rung: `metrics-{before,window-open,window-close,drain,after}-broker0.prom`, `window.tsv` (4 columns, pre-MainPID harness), `rung.txt`, `cpu/cpu-{broker0,driver0}.txt`, `cpu/samplers.tsv` | the aligned-window harness on Hetzner (1 broker, 1 driver), 2026-09-14 smoke run `20260914T202954Z`, candidate binary sha256 `0f517c94…96de` (reports `version="1.0.16"`); files exactly as `run-curve.sh` wrote them. No forwarded family anywhere: N=1 never forwards. |
| `local-proof-n3/nodes=3/laneE/forward-canary/` | a passing full-mesh forwarding positive control, `count=100`: `metrics-{pre,post}-broker{0,1,2}.prom`, `clients.tsv`, `timeline.tsv`, `run.stderr`, `mainpid-broker{0,1,2}.txt`; `forward-canary.txt` beside it is `verify`'s output | `python3 forward-canary.py local-proof --mqttd <candidate> --nodes 3 --capture <dir>`: the same candidate binary as three local processes on loopback, driven by this tree's `forward-canary.py run` over stdin exactly as a driver runs it |
| `local-proof-n3/nodes=3/laneE/sites-1/` | the zero-crossing rung that followed: `metrics-{before,window-open,window-close,after}-broker{0,1,2}.prom`, `window.tsv` (5 columns), `rung.txt` | the same launch, seconds later: one `$share` member per node, 500 messages from each node's publisher to its own node only; forwarded stays flat at the canary's 204 with the family present |
| `local-proof-n3/restart/metrics-restart-broker2.prom` | broker2's first scrape after the proof SIGKILLed and respawned it: peer links back at 2, the received, delivered and forwarded families absent; `mainpid-restart-broker2.txt` is the respawned PID | the same launch, after the rung |

One process lifetime per broker covers the canary and the rung, so the rung's
counters sit exactly on the canary's floors and the PIDs are the real processes'.

## What the proof wrote rather than scraped

- `window.tsv`: `start_ms`/`end_ms` are the proof's wall clock immediately
  before and after each window scrape, and `main_pid` is the PID of the local
  `mqttd` process (what `systemctl show -p MainPID` reports on a cloud broker).
  There is no driver row: no consumer containers ran.
- `mainpid-broker{0,1,2}.txt` and `mainpid-restart-broker2.txt`: the same PIDs,
  standing in for what `run-curve.sh` reads over ssh after the canary.
- `rung.txt`: the harness's format with `cpu_window=missing` (no CPU sampling)
  and `source=local-proof`.
