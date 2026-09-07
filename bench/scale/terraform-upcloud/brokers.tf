# One broker per host. The strict anti-affinity server group is UpCloud's
# strongest "independent machines" statement — the analog of the Hetzner
# spread placement group; the per-host barrier probe then MEASURES what the
# disk actually does, per run.
#
# DEVIATION from ADR 0048 §2 (recorded, not hidden): UpCloud maxiops storage
# is network-replicated, not local NVMe. The barrier probe still measures what
# the broker commits on — but that number is not comparable to the Hetzner
# curve's, and no doc point should mix the two clouds.

# The group is attached AT SERVER CREATION via the server resource's
# server_group property (track_members=false on the group to avoid a state
# cycle) — anti-affinity is applied on server start, so a strict group MUST be
# wired this way: attaching running servers after the fact would demand live
# migrations UpCloud refuses (STRICT_ANTI_AFFINITY_NOT_MET, the failure this
# file's first revision hit).
resource "upcloud_server_group" "brokers" {
  title                = "mqttd-bench-brokers-${var.run_label}"
  anti_affinity_policy = "strict"
  track_members        = false
  depends_on           = [terraform_data.quota_guard]
}

resource "upcloud_server" "broker" {
  count = var.node_count

  hostname = "mqttd-${count.index + 1}"
  zone     = var.zone
  plan     = var.broker_server_type
  metadata = true
  firewall = true

  labels = merge(local.common_labels, { role = "broker" })

  # Distinct physical machines, enforced at placement (see the group above).
  server_group = upcloud_server_group.brokers.id

  # Key via the login block (never an account-level SSH key), password
  # delivery off — key-only access, matching the Hetzner rig's posture.
  login {
    user              = "root"
    keys              = [trimspace(file(pathexpand(var.ssh_public_key_path)))]
    password_delivery = "none"
  }

  template {
    storage = var.image
    size    = var.storage_size_gb
    tier    = "maxiops"
  }

  network_interface {
    index             = 1
    type              = "public"
    ip_address_family = "IPv4"
  }

  network_interface {
    index             = 2
    type              = "private"
    network           = upcloud_network.bench.id
    ip_address_family = "IPv4"
    ip_address        = local.broker_ips[count.index]
  }

  user_data = templatefile("${path.module}/templates/cloud-init-broker.yaml.tftpl", {
    private_ip    = local.broker_ips[count.index]
    mqttd_version = var.mqttd_version
    mqttd_sha256  = var.mqttd_sha256
    mqttd_url     = var.mqttd_url
    # The SHIPPED unit, verbatim — the rig reuses the reference deployment
    # artifact instead of restating it, and a drop-in carries the bench deltas.
    mqttd_service_unit = file("${path.module}/../../../deploy/systemd/mqttd.service")
    mqttd_override     = file("${path.module}/../terraform/files/mqttd-override.conf")
    sysctl_conf        = file("${path.module}/../terraform/files/sysctl-broker.conf")
  })

  depends_on = [upcloud_network.bench]
}
