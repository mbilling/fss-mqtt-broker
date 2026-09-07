# One private SDN network; every measured byte (MQTT, SWIM, peer bus, health
# scrapes) stays on it. Addresses are FIXED — brokers 10.99.1.11.., drivers
# 10.99.1.21.. — the same scheme as the Hetzner rig, so certificate SANs, seed
# lists and lane configs are identical per size and need no discovery step.
#
# UpCloud's SDN serves DHCP on the network, and the DHCP address for an
# interface with a fixed IP reserved in terraform IS that address — so the
# cloud-init netplan fix-up the Hetzner rig needs (its private NIC can be
# hot-attached after first boot) is unnecessary here: the interface exists
# before first boot and DHCP answers with the reserved address.

locals {
  common_labels = {
    purpose = "mqttd-bench-scale"
    run     = var.run_label
  }

  broker_ips = [for i in range(var.node_count) : "10.99.1.${11 + i}"]
  driver_ips = [for i in range(var.driver_count) : "10.99.1.${21 + i}"]
}

resource "upcloud_network" "bench" {
  name = "mqttd-bench-${var.run_label}"
  zone = var.zone

  depends_on = [terraform_data.quota_guard]

  ip_network {
    address            = "10.99.1.0/24"
    dhcp               = true
    dhcp_default_route = false
    family             = "IPv4"
  }
}
