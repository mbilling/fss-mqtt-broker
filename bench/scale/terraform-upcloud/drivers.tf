# Load-generator hosts. Separate machines from the brokers on purpose — the
# same reasoning as the Hetzner rig (the driver's CPU must stay observable and
# generous). Driver 1 additionally builds the durable_bench driver from a
# pinned ref of this repository.

resource "upcloud_server" "driver" {
  count = var.driver_count

  hostname = "bench-driver-${count.index + 1}"
  zone     = var.zone
  plan     = var.driver_server_type
  metadata = true
  firewall = true

  labels = merge(local.common_labels, { role = "driver" })

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
    ip_address        = local.driver_ips[count.index]
  }

  user_data = templatefile("${path.module}/templates/cloud-init-driver.yaml.tftpl", {
    private_ip    = local.driver_ips[count.index]
    build_bench   = count.index == 0
    bench_git_ref = var.bench_git_ref
    sysctl_conf   = file("${path.module}/../terraform/files/sysctl-driver.conf")
  })

  depends_on = [upcloud_network.bench]
}
