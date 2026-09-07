# The scale-curve rig on UpCloud. Mirrors ../terraform (Hetzner) resource for
# resource, with the deltas the platform dictates:
#   - auth: UPCLOUD_TOKEN (a Bearer API token) instead of HCLOUD_TOKEN
#   - SDN private network with built-in DHCP (no netplan fix-up in cloud-init)
#   - firewall rules are PER SERVER, not a shared group object
#   - anti-affinity via a strict server group, not a spread placement group
#   - maxiops storage is network-replicated, NOT local NVMe — ADR 0048 §2's
#     hardware rule is relaxed by necessity; fsync floors are not comparable
#     to the Hetzner curve
# Applied per cluster size from an operator's machine — never from CI.

terraform {
  required_version = ">= 1.7"

  required_providers {
    upcloud = {
      source  = "UpCloudLtd/upcloud"
      version = "~> 5.40"
    }
  }
}

provider "upcloud" {
  # UPCLOUD_TOKEN from the environment.
}
