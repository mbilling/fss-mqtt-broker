# The inventory every bench/scale script consumes — byte-for-byte the same
# schema as the Hetzner rig's output, so run.sh / run-curve.sh / collect.sh /
# bootstrap-cluster.sh cannot tell the clouds apart:
#   tofu output -json inventory > ../.runs/<stamp>/inventory-<N>.json

output "inventory" {
  description = "Hosts of this cluster size, with the fixed private addresses the harness drives."
  value = {
    node_count    = var.node_count
    location      = var.zone
    mqttd_version = var.mqttd_version
    run_label     = var.run_label

    brokers = [for i, s in upcloud_server.broker : {
      name        = s.hostname
      node_id     = s.hostname # MQTTD_NODE_ID == hostname == peer-cert CN
      public_ip   = [for ni in s.network_interface : ni.ip_address if ni.type == "public"][0]
      private_ip  = local.broker_ips[i]
      mqtt_plain  = "${local.broker_ips[i]}:1883"
      mqtt_tls    = "${local.broker_ips[i]}:8883"
      health      = "${local.broker_ips[i]}:8080"
      peer        = "${local.broker_ips[i]}:7001"
      swim        = "${local.broker_ips[i]}:7946"
      server_type = s.plan
    }]

    drivers = [for i, s in upcloud_server.driver : {
      name         = s.hostname
      public_ip    = [for ni in s.network_interface : ni.ip_address if ni.type == "public"][0]
      private_ip   = local.driver_ips[i]
      builds_bench = i == 0
      server_type  = s.plan
    }]
  }
}
