# One private network; every measured byte (MQTT, SWIM, peer bus, health scrapes)
# stays on it. Addresses are FIXED — brokers 10.99.1.11.., drivers 10.99.1.21.. —
# so certificate SANs, seed lists and lane configs are deterministic per size and
# need no discovery step.

locals {
  common_labels = {
    purpose = "mqttd-bench-scale"
    run     = var.run_label
  }

  broker_ips = [for i in range(var.node_count) : "10.99.1.${11 + i}"]
  driver_ips = [for i in range(var.driver_count) : "10.99.1.${21 + i}"]
}

resource "hcloud_network" "bench" {
  name     = "mqttd-bench"
  ip_range = "10.99.0.0/16"
  labels   = local.common_labels
}

resource "hcloud_network_subnet" "bench" {
  network_id   = hcloud_network.bench.id
  type         = "cloud"
  network_zone = "eu-central"
  ip_range     = "10.99.1.0/24"
}

resource "hcloud_ssh_key" "admin" {
  name       = "mqttd-bench-admin"
  public_key = file(pathexpand(var.ssh_public_key_path))
  labels     = local.common_labels
}
