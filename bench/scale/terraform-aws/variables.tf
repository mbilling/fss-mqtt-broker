# The same names as bench/scale/terraform/variables.tf wherever the meaning is the same,
# so rig2/run.sh passes one set of -var flags to either cloud.

variable "node_count" {
  description = "Number of mqttd brokers."
  type        = number
}

variable "driver_count" {
  description = "Number of load-generator hosts."
  type        = number
  default     = 2
}

variable "broker_server_type" {
  description = "EC2 instance type of the brokers (m6i.8xlarge: 32 vCPU, the generation of HiveMQ 4.18's m6a.8xlarge)."
  type        = string
  default     = "m6i.xlarge"
}

variable "driver_server_type" {
  description = "EC2 instance type of the load generators."
  type        = string
  default     = "m6i.2xlarge"
}

variable "region" {
  description = "AWS region. An account on the new AWS experience can only use its project's region."
  type        = string
  default     = "eu-north-1"
}

variable "availability_zone" {
  description = "One AZ for every host (traffic between them is then free and LAN-class); empty = the region's first that offers both types."
  type        = string
  default     = ""
}

variable "placement" {
  description = "none, or cluster: every host in one cluster placement group (EC2's lowest-latency placement; an ordinary deployment gets none)."
  type        = string
  default     = "none"

  validation {
    condition     = contains(["none", "cluster"], var.placement)
    error_message = "placement must be none or cluster."
  }
}

variable "root_volume_gb" {
  description = "The gp3 root volume of every host; the brokers' data dir (/var/lib/mqttd) lives on it."
  type        = number
  default     = 100
}

variable "root_volume_iops" {
  description = "gp3 IOPS of the root volume (3000 = the gp3 baseline, no extra charge). The durable store's fsyncs see this."
  type        = number
  default     = 3000
}

variable "root_volume_throughput" {
  description = "gp3 throughput of the root volume in MB/s (125 = the gp3 baseline)."
  type        = number
  default     = 125
}

variable "vcpu_quota" {
  description = "The account's on-demand standard vCPU quota in the region (Service Quotas L-1216C47A); a plan above it fails before anything is created."
  type        = number
  default     = 256
}

variable "broker_docker" {
  description = "Install Docker on the brokers (see bench/scale/terraform)."
  type        = bool
  default     = false
}

variable "broker_nic_spread" {
  description = "Spread network softirq over every broker core (see bench/scale/terraform). ENA is already multi-queue."
  type        = bool
  default     = false
}

variable "broker_data_tmpfs" {
  description = "DIAGNOSTIC ONLY: the brokers' data dir on tmpfs (see bench/scale/terraform)."
  type        = bool
  default     = false
}

variable "ssh_public_key_path" {
  description = "The operator's public key, for root on every host."
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
}

variable "admin_cidr" {
  description = "Where SSH is allowed from."
  type        = string
  default     = "0.0.0.0/0"
}

variable "mqttd_version" {
  description = "The mqttd release cloud-init installs (rig2 can push a local build over it)."
  type        = string
  default     = "1.0.0"
}

variable "mqttd_url" {
  description = "An unreleased mqttd binary's URL (with mqttd_sha256)."
  type        = string
  default     = ""
}

variable "mqttd_sha256" {
  description = "The sha256 of the mqttd binary."
  type        = string
  default     = ""
}

variable "bench_git_ref" {
  description = "Git ref the first driver builds the Rust bench from, when build_bench is set."
  type        = string
  default     = "main"
}

variable "run_label" {
  description = "The run's label, on every resource as the tag run."
  type        = string
  default     = "manual"
}

variable "build_bench" {
  description = "Build the Rust bench on the first driver."
  type        = bool
  default     = true
}

variable "driver_emqtt_bench" {
  description = "Install emqtt-bench (Docker) on the drivers."
  type        = bool
  default     = true
}
