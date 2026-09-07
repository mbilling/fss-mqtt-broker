# The account's vCPU quota, enforced BEFORE anything is created — the same
# plan-time guard as the Hetzner rig's quota.tf, because a rejection part way
# through an apply is the expensive failure mode (servers already made keep
# running and billing, and the trap's destroy may itself hit the quota).
#
# This is a local budget guard, not an API check of remaining account quota.
# Unknown plans fail closed: assuming 64 cores could undercount a larger plan.
# Update the explicit table from the provider catalog before using a new plan.
locals {
  cores_by_plan = {
    "1xCPU-1GB"            = 1
    "1xCPU-2GB"            = 1
    "2xCPU-2GB"            = 2
    "2xCPU-4GB"            = 2
    "4xCPU-8GB"            = 4
    "4xCPU-16GB"           = 4
    "6xCPU-16GB"           = 6
    "8xCPU-16GB"           = 8
    "8xCPU-32GB"           = 8
    "12xCPU-48GB"          = 12
    "16xCPU-64GB"          = 16
    "24xCPU-96GB"          = 24
    "PREMIUM-24xCPU-96GB"  = 24
    "32xCPU-128GB"         = 32
    "HICPU-32xCPU-48GB"    = 32
    "HICPU-32xCPU-64GB"    = 32
    "PREMIUM-32xCPU-64GB"  = 32
    "PREMIUM-32xCPU-128GB" = 32
    "38xCPU-192GB"         = 38
    "PREMIUM-38xCPU-192GB" = 38
    "48xCPU-256GB"         = 48
    "PREMIUM-48xCPU-96GB"  = 48
    "PREMIUM-48xCPU-256GB" = 48
    "64xCPU-384GB"         = 64
    "HICPU-64xCPU-96GB"    = 64
    "HICPU-64xCPU-128GB"   = 64
    "PREMIUM-64xCPU-128GB" = 64
    "HICPU-8xCPU-12GB"     = 8
    "HICPU-8xCPU-16GB"     = 8
    "HICPU-16xCPU-24GB"    = 16
    "HICPU-16xCPU-32GB"    = 16
    "PREMIUM-4xCPU-8GB"    = 4
    "PREMIUM-4xCPU-16GB"   = 4
    "PREMIUM-8xCPU-32GB"   = 8
    "PREMIUM-8xCPU-64GB"   = 8
    "HIMEM-4xCPU-32GB"     = 4
    "HIMEM-4xCPU-64GB"     = 4
    "HIMEM-8xCPU-192GB"    = 8
    "HIMEM-12xCPU-256GB"   = 12
    "HIMEM-16xCPU-384GB"   = 16
  }

  broker_cores = lookup(local.cores_by_plan, var.broker_server_type, 0)
  driver_cores = lookup(local.cores_by_plan, var.driver_server_type, 0)
  total_cores  = var.node_count * local.broker_cores + var.driver_count * local.driver_cores
  total_hosts  = var.node_count + var.driver_count
}

resource "terraform_data" "quota_guard" {
  input = local.total_cores

  lifecycle {
    precondition {
      condition     = local.broker_cores > 0 && local.driver_cores > 0
      error_message = "Unknown UpCloud plan: update cores_by_plan from the API catalog before provisioning. No guessed core counts are allowed."
    }

    precondition {
      condition = local.total_cores <= var.vcpu_quota
      error_message = join("", [
        "this run needs ${local.total_cores} cores (${var.node_count} x ${var.broker_server_type} = ",
        "${var.node_count * local.broker_cores}, plus ${var.driver_count} x ${var.driver_server_type} = ",
        "${var.driver_count * local.driver_cores}) but the account cap is ${var.vcpu_quota}. ",
        "Lower node_count or driver_count, use a smaller plan, or raise vcpu_quota if the account's ",
        "resource_limits.cores allows it."
      ])
    }

    precondition {
      condition = local.total_hosts <= var.public_ip_quota
      error_message = join("", [
        "this run needs ${local.total_hosts} public IPv4 addresses (one per host) but the account cap ",
        "is ${var.public_ip_quota}. Lower node_count or driver_count, or raise public_ip_quota."
      ])
    }
  }
}
