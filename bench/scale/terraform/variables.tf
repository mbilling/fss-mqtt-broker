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
    condition     = var.driver_count >= 1 && var.driver_count <= 6
    error_message = "driver_count must be between 1 and 6 (six CCX33s offer ~240k msg/s on lane B — the fan-out knee hunt at 7 nodes; the 100-vCPU quota bounds the rest)."
  }
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
  description = "Path to the SSH public key injected into every host via cloud-init (never registered with the hcloud API — that 409s when the key already exists in the project); its private half is how the orchestrator reaches every host."
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
