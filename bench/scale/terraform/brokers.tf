# One broker per host, one local NVMe disk per broker — ADR 0048 §2's hardware
# rule, learned from the single-host post-mortem it cites. The spread placement
# group makes Hetzner guarantee distinct physical machines, which is the strongest
# "independent disks" statement a cloud VM can make; the per-host barrier probe
# then MEASURES what the disk actually does, per run.
#
# That guarantee holds for up to ten brokers, the most one spread group admits.
# Sizes 12 and 14 put brokers 11+ in a second group: distinct hosts within each
# group, no guarantee between the two (layout.tf, variables.tf `node_count`).

resource "hcloud_placement_group" "brokers" {
  name   = "mqttd-bench-brokers"
  type   = "spread"
  labels = local.common_labels
}

# The second group, for brokers 11+ (layout.tf). Created only when there are
# more than ten brokers, so every shape of ten or fewer plans as it always has.
# It carries the same labels as the first: teardown.sh's label audit lists and
# deletes placement groups by `purpose`, so it finds this one with no change.
resource "hcloud_placement_group" "brokers_b" {
  count = local.second_placement_group ? 1 : 0

  name   = "mqttd-bench-brokers-b"
  type   = "spread"
  labels = local.common_labels
}

# Indexed by layout.tf's broker_placement_group (0 or 1). The second entry is
# null while the second group does not exist, and no broker of a shape of ten or
# fewer ever selects it.
locals {
  placement_group_ids = [
    hcloud_placement_group.brokers.id,
    one(hcloud_placement_group.brokers_b[*].id),
  ]
}

resource "hcloud_server" "broker" {
  count = var.node_count

  name               = "mqttd-${count.index + 1}"
  server_type        = var.broker_server_type
  image              = var.image
  location           = var.location
  placement_group_id = local.placement_group_ids[local.broker_placement_group[count.index]]
  firewall_ids       = [hcloud_firewall.bench.id]
  labels             = merge(local.common_labels, { role = "broker" })

  network {
    network_id = hcloud_network.bench.id
    ip         = local.broker_ips[count.index]
  }

  user_data = templatefile("${path.module}/templates/cloud-init-broker.yaml.tftpl", {
    ssh_public_key = trimspace(file(pathexpand(var.ssh_public_key_path)))
    cloud          = "hcloud"
    private_ip     = local.broker_ips[count.index]
    mqttd_version  = var.mqttd_version
    mqttd_sha256   = var.mqttd_sha256
    mqttd_url      = var.mqttd_url
    # The SHIPPED unit, verbatim — the rig reuses the reference deployment
    # artifact instead of restating it, and a drop-in carries the bench deltas.
    mqttd_service_unit = file("${path.module}/../../../deploy/systemd/mqttd.service")
    mqttd_override     = file("${path.module}/files/mqttd-override.conf")
    sysctl_conf        = file("${path.module}/files/sysctl-broker.conf")
    nic_spread         = var.broker_nic_spread
    data_tmpfs         = var.broker_data_tmpfs
    docker             = var.broker_docker
  })

  depends_on = [hcloud_network_subnet.bench]
}
