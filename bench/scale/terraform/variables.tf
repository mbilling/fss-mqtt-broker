variable "node_count" {
  description = "Broker nodes in this cluster. ADR 0048 §2 fixes the curve's canonical points at 1, 3 and 5; 7 is the past-the-voter-cap extension (durable capacity is architecturally flat there per ADR 0049 unless the MQTTD_LEASE_VOTERS variant is run beside it — run-curve.sh does both and the doc publishes both)."
  type        = number

  validation {
    condition     = contains([1, 3, 5, 7, 10], var.node_count)
    error_message = "node_count must be 1, 3, 5, 7 or 10 (the curve points plus the ADR 0073 scale-out extensions; 10 exactly fills the spread placement group's limit)."
  }
}

variable "driver_count" {
  description = "Load-generator hosts. Two carry the 50k-connection lanes; a third is headroom if a lane proves driver-bound (a caveat the harness reports rather than hides)."
  type        = number
  default     = 2

  validation {
    condition     = var.driver_count >= 1 && var.driver_count <= 20 && floor(var.driver_count) == var.driver_count
    error_message = "driver_count must be an integer between 1 and 20. Six CCX33s offer ~240k msg/s on lane B (the fan-out knee hunt at 7 nodes); lane E deals sites round-robin at 3 one-vCPU containers each, so the busiest driver holds ceil(sites/drivers)*3 <= 8 and more drivers are what reach more sites. The cap was 8 while the project allowed 100 vCPUs, then 12; since 2026-09-15 the project allows 30 servers / 200 vCPUs, and 10 x ccx23 + 20 x ccx33 = 200 vCPU on 30 servers is exactly that. 12 was a shape that fits rather than the largest one: it refused a 7-node run at 9 sites, which needs 18 drivers to hold the proven 3 containers per driver and costs 172 vCPU on 25 servers. quota.tf remains the authority and refuses the combinations that do not fit."
  }
}

variable "vcpu_quota" {
  description = "vCPUs the Hetzner project is allowed to run at once. Enforced by quota.tf BEFORE any server is created, because a quota rejection part way through an apply leaves the already-created servers running and billing with no teardown reached."
  type        = number
  # Raised from 100 on 2026-09-15, when Hetzner lifted the dedicated project to
  # 30 servers / 200 vCPUs. Keep this equal to the project's real limit: a value
  # above it lets an apply fail half way, a value below it refuses runs that fit.
  default = 200
}

variable "server_quota" {
  description = "Servers the Hetzner project may run at once (brokers + drivers). Enforced by quota.tf before any server is created, for the same reason as vcpu_quota: Hetzner rejects the server that crosses the limit part way through an apply, and the ones already made keep billing. 30 since 2026-09-15."
  type        = number
  default     = 30
}

variable "broker_docker" {
  description = "Install Docker on the broker hosts. Off for measurement runs — the published curve's broker must run the shipped binary under the shipped unit, with nothing else on the host. `run.sh compare` turns it on, because the cross-broker comparison (ADR 0048 T4) runs every broker under test, mqttd included, as a container so they share one runtime."
  type        = bool
  default     = false
}

variable "broker_nic_spread" {
  description = "Spread network softirq across every broker core (RPS, plus ethtool -L when the NIC supports it). Default false so the published curve keeps the untuned kernel it was measured on. ADR 0077 / issue #505: lane E measured ONE core per broker carrying all softirq and saturating at ~96% while the other three idled at 61-78% and mqttd itself held only ~11-20% of that core — this is the knob that tests whether that single queue is the ceiling."
  type        = bool
  default     = false
}

variable "broker_server_type" {
  description = "Dedicated-vCPU type for brokers. Shared-vCPU (cx/cpx) steal ruins p99 honesty; local NVMe is required — a network volume would make the per-host fsync floor a measurement of Ceph, not the disk."
  type        = string
  default     = "ccx23"
}

variable "driver_server_type" {
  description = "Dedicated-vCPU type for drivers. The driver must provably not be the bottleneck, so it gets more cores than a broker."
  type        = string
  default     = "ccx33"
}

variable "location" {
  description = "Hetzner location. All hosts share one location so the private network is LAN-class. Fallbacks if ccx types are out of stock: nbg1, hel1."
  type        = string
  default     = "fsn1"
}

variable "image" {
  description = "OS image for every host."
  type        = string
  default     = "ubuntu-24.04"
}

variable "ssh_public_key_path" {
  description = "Path to the SSH public key injected into every host via cloud-init (never registered with the hcloud API — that 409s when the key already exists in the project); its private half is how the orchestrator reaches every host. Destroy evaluates file(pathexpand(...)) too — pass the same path apply used (run.sh / teardown.sh do this when SSH_KEY is set)."
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
}

variable "admin_cidr" {
  description = "CIDR allowed to reach SSH on the public interface. Narrow it to your own address if you have a stable one."
  type        = string
  default     = "0.0.0.0/0"
}

variable "mqttd_version" {
  description = "Released broker version (no leading v). The brokers run this signed, byte-reproducible release artifact — the published curve is attributable to it. run.sh ALWAYS passes this (it refuses to run without MQTTD_VERSION); the default exists only so teardown.sh's destroy has a value and must not be relied on."
  type        = string
  default     = "1.0.0"
}

variable "mqttd_url" {
  description = <<-EOT
    Fetch the broker from this URL instead of the published release for
    `mqttd_version`. Empty (the default) = the release, which is what every
    published number must come from.

    Set ONLY to measure a binary that has not shipped — a candidate under test,
    a pre-release, a build from a branch. `mqttd_sha256` becomes MANDATORY when
    this is set: the release path can fall back to the `.sha256` published
    beside the artifact, and an arbitrary URL has no such companion, so without
    a hash there would be no verification at all. run.sh refuses the
    combination before terraform is invoked.

    A run using this is stamped `UNRELEASED` in its run directory. The rig's
    whole claim is that a published curve is attributable to a signed release
    (see `mqttd_version`); a number measured against an unreleased binary is
    not, and must never be quoted as one.
  EOT
  type        = string
  default     = ""
}

variable "mqttd_sha256" {
  description = "Optional pinned sha256 of the mqttd release binary. Empty = verify against the .sha256 file published with the release (integrity only); set = verify against a hash you obtained independently."
  type        = string
  default     = ""
}

variable "bench_git_ref" {
  description = "Commit/tag of this repository that driver-1 clones and builds the durable_bench driver from. Pin a commit so the driver binary is reproducible from the published doc."
  type        = string
  default     = "main"
}

variable "run_label" {
  description = "Label stamped on every resource of this run; the teardown sweeper deletes by `purpose`, this narrows a sweep to one run when debugging."
  type        = string
  default     = "manual"
}

variable "build_bench" {
  type = bool
  default = true
  description = "Build lane A driver; disable for lane E-only campaigns."
}
