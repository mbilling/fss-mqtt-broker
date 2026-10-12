# One private network; every measured byte (MQTT, SWIM, peer bus, health scrapes)
# stays on it. Addresses are FIXED (layout.tf: brokers 10.99.1.11.., drivers
# 10.99.1.21.., brokers past the tenth 10.99.1.41..) so certificate SANs, seed
# lists and lane configs are deterministic per size and need no discovery step.

locals {
  common_labels = {
    purpose = "mqttd-bench-scale"
    run     = var.run_label
  }
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

# NOTE: no hcloud_ssh_key resource, deliberately. Registering the key via the
# API 409s ("SSH key not unique") whenever the operator already uploaded the
# same key in the console — the first live run died exactly there. The public
# key is injected through cloud-init (`ssh_authorized_keys`) instead, which is
# indifferent to the project's key inventory.
