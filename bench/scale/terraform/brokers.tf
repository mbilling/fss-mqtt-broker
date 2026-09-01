# One broker per host, one local NVMe disk per broker — ADR 0048 §2's hardware
# rule, learned from the single-host post-mortem it cites. The spread placement
# group makes Hetzner guarantee distinct physical machines, which is the strongest
# "independent disks" statement a cloud VM can make; the per-host barrier probe
# then MEASURES what the disk actually does, per run.

resource "hcloud_placement_group" "brokers" {
  name   = "mqttd-bench-brokers"
  type   = "spread"
  labels = local.common_labels
}

resource "hcloud_server" "broker" {
  count = var.node_count

  name               = "mqttd-${count.index + 1}"
  server_type        = var.broker_server_type
  image              = var.image
  location           = var.location
  placement_group_id = hcloud_placement_group.brokers.id
  firewall_ids       = [hcloud_firewall.bench.id]
  labels             = merge(local.common_labels, { role = "broker" })

  network {
    network_id = hcloud_network.bench.id
    ip         = local.broker_ips[count.index]
  }

  user_data = templatefile("${path.module}/templates/cloud-init-broker.yaml.tftpl", {
    ssh_public_key = trimspace(file(pathexpand(var.ssh_public_key_path)))
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
  })

  depends_on = [hcloud_network_subnet.bench]
}
