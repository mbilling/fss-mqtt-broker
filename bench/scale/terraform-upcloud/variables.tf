variable "node_count" {
  description = "Broker nodes in this cluster. Same canonical points as the Hetzner rig (see ../terraform/variables.tf)."
  type        = number

  validation {
    condition     = contains([1, 3, 5, 7, 10], var.node_count)
    error_message = "node_count must be 1, 3, 5, 7 or 10."
  }
}

variable "driver_count" {
  description = "Load-generator hosts. The UpCloud account cap (cores) is enforced by quota.tf together with vcpu_quota."
  type        = number
  default     = 2

  validation {
    condition     = var.driver_count >= 1 && var.driver_count <= 12 && floor(var.driver_count) == var.driver_count
    error_message = "driver_count must be an integer between 1 and 12."
  }
}

variable "vcpu_quota" {
  description = "vCPUs the UpCloud account is allowed to run at once (resource_limits.cores). Enforced BEFORE any server is created, because a rejection part way through an apply leaves the already-created servers running and billing with no teardown reached."
  type        = number
  default     = 100
}

variable "public_ip_quota" {
  description = "Public IPv4 addresses the account may hold (resource_limits.public_ipv4). One per host — brokers and drivers each need one."
  type        = number
  default     = 20
}

variable "broker_server_type" {
  description = "UpCloud plan for brokers. The default has 4 vCPUs and 8 GB RAM; equivalent core counts do not imply equivalent CPU guarantees or performance across providers."
  type        = string
  default     = "4xCPU-8GB"
}

variable "driver_server_type" {
  description = "UpCloud plan for drivers. Defaults to four vCPUs, not Hetzner's eight. The inventory exposes vcpus so lane E can cap its container budget to the smallest driver. Set the site ladder and driver count together."
  type        = string
  default     = "4xCPU-8GB"
}

variable "zone" {
  description = "UpCloud zone. All hosts share one zone so the SDN private network is LAN-class."
  type        = string
  default     = "de-fra1"
}

variable "image" {
  description = "OS template for every host."
  type        = string
  default     = "Ubuntu Server 24.04 LTS (Noble Numbat)"
}

variable "storage_size_gb" {
  description = "Boot-disk size per host. The 4xCPU-8GB plan carries 160 GB maxiops; 100 keeps headroom for the broker's data dir and probe scratch without eating the account's storage cap."
  type        = number
  default     = 100
}

variable "ssh_public_key_path" {
  description = "Path to the SSH public key, delivered through the server's login block (never registered as an account-level key)."
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
}

variable "admin_cidr" {
  description = "CIDR allowed to reach SSH on the public interface. Narrow it to your own address if you have a stable one."
  type        = string
  default     = "0.0.0.0/0"

  validation {
    condition     = can(cidrnetmask(var.admin_cidr))
    error_message = "admin_cidr must be an IPv4 CIDR (the rig provisions IPv4 public interfaces)."
  }
}

variable "mqttd_version" {
  description = "Released broker version (no leading v). Same disclosure rule as the Hetzner rig — run.sh ALWAYS passes this."
  type        = string
  default     = "1.0.0"
}

variable "mqttd_url" {
  description = "Fetch the broker from this URL instead of the published release (see ../terraform/variables.tf for the full rule)."
  type        = string
  default     = ""
}

variable "mqttd_sha256" {
  description = "Optional pinned sha256 of the mqttd release binary."
  type        = string
  default     = ""
}

variable "bench_git_ref" {
  description = "Commit/tag of this repository that driver-1 clones and builds the durable_bench driver from."
  type        = string
  default     = "main"
}

variable "run_label" {
  description = "Label stamped on every resource of this run."
  type        = string
  default     = "manual"
}
