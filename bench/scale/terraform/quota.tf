# The project's vCPU quota, enforced BEFORE anything is created.
#
# node_count and driver_count are validated independently, which was safe only
# while driver_count was capped at 6: the worst combination the old bounds could
# express was 10 brokers + 6 drivers = 88 vCPU. Raising the driver cap to 8 (so
# lane E can reach 16 sites) makes 10 + 8 = 104 vCPU expressible, and Hetzner
# rejects the servers that cross the quota PART WAY THROUGH an apply — leaving
# the ones it already made running and billing, with no teardown having been
# reached. That failure has happened on this rig for other reasons and is
# expensive; a precondition turns it into a plan-time error instead.
locals {
  # Dedicated- and shared-vCPU counts for the types this rig is run with. An
  # unlisted type falls back to the largest we know about, so a type we forgot
  # can only make this guard MORE conservative, never silently switch it off.
  vcpus_by_server_type = {
    ccx13 = 2
    ccx23 = 4
    ccx33 = 8
    ccx43 = 16
    ccx53 = 32
    cpx41 = 8
    cpx51 = 16
  }

  broker_vcpus = lookup(local.vcpus_by_server_type, var.broker_server_type, 32)
  driver_vcpus = lookup(local.vcpus_by_server_type, var.driver_server_type, 32)
  total_vcpus  = var.node_count * local.broker_vcpus + var.driver_count * local.driver_vcpus
}

resource "terraform_data" "quota_guard" {
  input = local.total_vcpus

  lifecycle {
    precondition {
      condition = local.total_vcpus <= var.vcpu_quota
      error_message = join("", [
        "this run needs ${local.total_vcpus} vCPUs (${var.node_count} x ${var.broker_server_type} = ",
        "${var.node_count * local.broker_vcpus}, plus ${var.driver_count} x ${var.driver_server_type} = ",
        "${var.driver_count * local.driver_vcpus}) but the project quota is ${var.vcpu_quota}. ",
        "Lower node_count or driver_count, use a smaller server type, or raise vcpu_quota if ",
        "Hetzner has actually raised the project's limit."
      ])
    }
  }
}
