//! Deterministic, seed-reproducible **simulation harness** for the SWIM membership
//! protocol ([ADR 0024](../../../docs/adr/0024-deterministic-testing.md) T7).
//!
//! The flakes ADR 0024 chased were distributed *ordering races*: events reaching nodes in
//! an order a wall-clock test assumed but did not enforce. The durable fix for that class is
//! a deterministic simulation — a virtual clock + a simulated network whose every choice
//! (latency, loss, reordering, partitions, scheduling) comes from **one seed**, so a run is
//! a pure function of that seed and a failure reruns identically.
//!
//! This harness realizes that for the **SWIM layer** — the most race-prone distributed
//! protocol and the source of the real flakes (`swim_cluster`). It exploits the fact that
//! [`Swim`] is a pure, I/O-free, clock-free state machine: `tick(now)` and `handle(msg, now)`
//! take a millisecond clock and return [`Action`]s, so the harness owns time and the network
//! entirely. No production code changes, no new dependencies (the network RNG is the same
//! hand-rolled xorshift style the codebase uses).
//!
//! Each scenario asserts an invariant across **many seeds**; on the first violation it panics
//! with the offending seed, which reruns the exact schedule (set [`REPRO_SEED`] to focus it).
//! The lease/replication layer (openraft) is async-I/O-entangled and out of scope here — the
//! natural extension once a seam to drive it deterministically exists.

use std::collections::HashMap;

use mqtt_cluster::swim::{Action, Config, MemberState, Swim};
use mqtt_cluster::NodeId;

/// Set to `Some(seed)` to run a single seed (e.g. to reproduce a reported failure).
const REPRO_SEED: Option<u64> = None;

/// A seeded xorshift64 RNG — deterministic, matching the codebase's hand-rolled style (no
/// `rand` dependency).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // Avoid the zero state (xorshift fixed point); mix the seed first.
        Rng((seed ^ 0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    /// `true` with probability `num/den`.
    fn chance(&mut self, num: u64, den: u64) -> bool {
        den == 0 || self.next() % den < num
    }
    /// A value in `[lo, hi)` (or `lo` if the range is empty).
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            lo
        } else {
            lo + self.next() % (hi - lo)
        }
    }
}

/// A datagram in flight on the simulated network.
struct InFlight {
    deliver_at: u64,
    to: String,
    msg: mqtt_cluster::swim::Message,
    /// A monotonic tie-breaker so equal-time deliveries have a deterministic order.
    seqno: u64,
}

/// Network conditions for a run.
#[derive(Clone, Copy)]
struct Net {
    /// Per-message drop probability, as `loss_num/loss_den`.
    loss_num: u64,
    loss_den: u64,
    /// Latency range, ms.
    lat_lo: u64,
    lat_hi: u64,
}

/// The deterministic simulation: N SWIM nodes over a virtual clock + simulated network.
struct Sim {
    nodes: Vec<Swim>,
    index: HashMap<String, usize>,
    /// Whether each node's "process" is up (a killed node neither ticks nor receives).
    up: Vec<bool>,
    /// Partition group per node; messages cross only within a group while `partitioned`.
    group: Vec<u8>,
    partitioned: bool,
    /// `views[i][subject_id]` = node i's last-observed [`MemberState`] of `subject_id`
    /// (exactly what the routing layer learns, via `StateChange` actions).
    views: Vec<HashMap<String, MemberState>>,
    queue: Vec<InFlight>,
    net: Net,
    rng: Rng,
    now: u64,
    seqno: u64,
}

impl Sim {
    /// `n` nodes, each seeded to every other (so a single lost Join cannot strand a node —
    /// the pure `Swim` greets its seeds only once at bootstrap; join-retry is the driver's
    /// job in production, so the harness gives the gossip layer, which is what we test here,
    /// a fair chance to form the cluster even under heavy loss).
    fn new(n: usize, net: Net, seed: u64) -> Self {
        let cfg = sim_cfg();
        let mut nodes = Vec::new();
        let mut index = HashMap::new();
        for i in 0..n {
            let a = format!("n{i}:7946");
            let seeds: Vec<String> = (0..n)
                .filter(|&j| j != i)
                .map(|j| format!("n{j}:7946"))
                .collect();
            nodes.push(Swim::new(
                NodeId(format!("n{i}")),
                a.clone(),
                format!("{a}-peer"),
                cfg.clone(),
                seeds,
            ));
            index.insert(a, i);
        }
        Sim {
            nodes,
            index,
            up: vec![true; n],
            group: vec![0; n],
            partitioned: false,
            views: vec![HashMap::new(); n],
            queue: Vec::new(),
            net,
            rng: Rng::new(seed),
            now: 0,
            seqno: 0,
        }
    }

    fn n(&self) -> usize {
        self.nodes.len()
    }

    /// Apply a node's emitted actions: schedule sends on the network, record state changes.
    fn apply(&mut self, from: usize, actions: Vec<Action>) {
        for a in actions {
            match a {
                Action::Send { to, msg } => {
                    let Some(&j) = self.index.get(&to) else {
                        continue;
                    };
                    if !self.up[j] {
                        continue; // destination process is down
                    }
                    if self.partitioned && self.group[from] != self.group[j] {
                        continue; // severed by the partition
                    }
                    if self.rng.chance(self.net.loss_num, self.net.loss_den) {
                        continue; // randomly dropped
                    }
                    let lat = self.rng.range(self.net.lat_lo, self.net.lat_hi);
                    self.seqno += 1;
                    self.queue.push(InFlight {
                        deliver_at: self.now + lat,
                        to,
                        msg,
                        seqno: self.seqno,
                    });
                }
                Action::StateChange { id, state, .. } => {
                    self.views[from].insert(id.0, state);
                }
            }
        }
    }

    /// Advance the virtual clock by `dt` ms: tick every up node, then deliver every datagram
    /// now due (oldest first, ties broken by send order — fully deterministic).
    fn step(&mut self, dt: u64) {
        self.now += dt;
        for i in 0..self.n() {
            if self.up[i] {
                let acts = self.nodes[i].tick(self.now);
                self.apply(i, acts);
            }
        }
        self.queue.sort_by_key(|m| (m.deliver_at, m.seqno));
        let mut due = Vec::new();
        let mut rest = Vec::new();
        for m in self.queue.drain(..) {
            if m.deliver_at <= self.now {
                due.push(m);
            } else {
                rest.push(m);
            }
        }
        self.queue = rest;
        for m in due {
            let j = self.index[&m.to];
            if self.up[j] {
                let acts = self.nodes[j].handle(m.msg, self.now);
                self.apply(j, acts);
            }
        }
    }

    /// Run until `pred` holds or `max_steps` of `dt` ms elapse; returns whether it held.
    fn run_until(&mut self, dt: u64, max_steps: u32, pred: impl Fn(&Sim) -> bool) -> bool {
        for _ in 0..max_steps {
            self.step(dt);
            if pred(self) {
                return true;
            }
        }
        pred(self)
    }

    fn kill(&mut self, i: usize) {
        self.up[i] = false;
    }

    /// Every up node sees every *other* up node as `Alive`.
    fn fully_converged(&self) -> bool {
        for i in 0..self.n() {
            if !self.up[i] {
                continue;
            }
            for j in 0..self.n() {
                if i == j || !self.up[j] {
                    continue;
                }
                if self.views[i].get(&format!("n{j}")) != Some(&MemberState::Alive) {
                    return false;
                }
            }
        }
        true
    }

    /// Every up node has marked node `dead` as `Dead`.
    fn all_see_dead(&self, dead: usize) -> bool {
        for i in 0..self.n() {
            if !self.up[i] || i == dead {
                continue;
            }
            if self.views[i].get(&format!("n{dead}")) != Some(&MemberState::Dead) {
                return false;
            }
        }
        true
    }
}

/// SWIM timings for the simulation: brisk, with a comfortable suspicion window so the
/// partition-heal scenario stays in `Suspect` (refutable) rather than tombstoning.
fn sim_cfg() -> Config {
    Config {
        protocol_period_ms: 100,
        ack_timeout_ms: 30,
        suspicion_timeout_ms: 1500,
        suspicion_min_timeout_ms: 600,
        suspicion_confirmations: 3,
        dead_ttl_ms: 5000,
        indirect_probes: 2,
        gossip_fanout: 8,
        gossip_multiplier: 4,
        awareness_max: 8,
    }
}

const RELIABLE: Net = Net {
    loss_num: 0,
    loss_den: 1,
    lat_lo: 1,
    lat_hi: 20,
};

/// The seeds each scenario sweeps. A failure on any one panics with that seed (deterministic
/// reproduction); `REPRO_SEED` narrows the sweep to a single seed.
fn seeds() -> Vec<u64> {
    match REPRO_SEED {
        Some(s) => vec![s],
        None => (0..48).collect(),
    }
}

#[test]
fn a_cluster_converges_under_a_lossy_reordering_network() {
    // 20% loss + jittery latency (reordering): every node must still learn every other.
    let net = Net {
        loss_num: 1,
        loss_den: 5,
        lat_lo: 1,
        lat_hi: 60,
    };
    for seed in seeds() {
        let mut sim = Sim::new(5, net, seed);
        let ok = sim.run_until(20, 1200, Sim::fully_converged);
        assert!(
            ok,
            "seed {seed}: cluster did not converge under loss (re-run with REPRO_SEED = Some({seed}))"
        );
    }
}

#[test]
fn a_stopped_node_is_detected_as_dead_by_every_survivor() {
    for seed in seeds() {
        let mut sim = Sim::new(5, RELIABLE, seed);
        assert!(
            sim.run_until(20, 800, Sim::fully_converged),
            "seed {seed}: precondition (converge) failed"
        );
        // Crash node 3: it stops ticking and receiving. The survivors must detect it.
        sim.kill(3);
        let ok = sim.run_until(20, 1500, |s| s.all_see_dead(3));
        assert!(
            ok,
            "seed {seed}: a crashed node was not detected dead by all survivors \
             (re-run with REPRO_SEED = Some({seed}))"
        );
    }
}

#[test]
fn a_healed_partition_reconverges() {
    for seed in seeds() {
        let mut sim = Sim::new(5, RELIABLE, seed);
        assert!(
            sim.run_until(20, 800, Sim::fully_converged),
            "seed {seed}: precondition (converge) failed"
        );
        // Split {0,1} | {2,3,4} for a window kept **under** the shrunk suspicion floor
        // (`suspicion_min_timeout_ms`, which the 3-node side reaches with 3 independent
        // suspecters), so cross-partition members go `Suspect` but are never tombstoned
        // `Dead`. This exercises the clean refutation→reconverge path on heal; the slower
        // Dead+tombstone+rejoin path is covered by the swim unit tests.
        sim.group = vec![0, 0, 1, 1, 1];
        sim.partitioned = true;
        for _ in 0..20 {
            sim.step(20);
        }
        // Heal: members refute and the whole cluster returns to all-Alive.
        sim.partitioned = false;
        let ok = sim.run_until(20, 1500, Sim::fully_converged);
        assert!(
            ok,
            "seed {seed}: the cluster did not reconverge after a healed partition \
             (re-run with REPRO_SEED = Some({seed}))"
        );
    }
}

/// The harness itself is deterministic: the same seed yields the identical schedule and
/// therefore the identical final membership view (the property that makes a failure
/// reproducible).
#[test]
fn the_simulation_is_reproducible_for_a_fixed_seed() {
    let net = Net {
        loss_num: 1,
        loss_den: 4,
        lat_lo: 1,
        lat_hi: 50,
    };
    let run = || {
        let mut sim = Sim::new(5, net, 12345);
        sim.run_until(20, 1000, Sim::fully_converged);
        // Snapshot every node's full view of the cluster.
        sim.views
            .iter()
            .map(|v| {
                let mut kv: Vec<(String, u8)> =
                    v.iter().map(|(k, s)| (k.clone(), *s as u8)).collect();
                kv.sort();
                kv
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(run(), run(), "the same seed must produce the same run");
}
