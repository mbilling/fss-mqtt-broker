# Load-generator hosts. Separate machines from the brokers on purpose: the
# in-driver driver-bound check cannot run against remote brokers, so keeping the
# driver's CPU observable (and generous) is what stands in for it.
#
# Driver 1 additionally builds the durable_bench driver (and, as a byproduct, the
# mqttd test binary whose #[ignore]d barrier probes the orchestrator copies to
# each broker host). It builds from a pinned ref of this repository — there is no
# released artifact for a test binary.

resource "hcloud_server" "driver" {
  count = var.driver_count

  name         = "bench-driver-${count.index + 1}"
  server_type  = var.driver_server_type
  image        = var.image
  location     = var.location
  firewall_ids = [hcloud_firewall.bench.id]
  labels       = merge(local.common_labels, { role = "driver" })

  network {
    network_id = hcloud_network.bench.id
    ip         = local.driver_ips[count.index]
  }

  user_data = templatefile("${path.module}/templates/cloud-init-driver.yaml.tftpl", {
    ssh_public_key = trimspace(file(pathexpand(var.ssh_public_key_path)))
    build_bench    = count.index == 0
    bench_git_ref  = var.bench_git_ref
    sysctl_conf    = file("${path.module}/files/sysctl-driver.conf")
  })

  depends_on = [hcloud_network_subnet.bench]
}
