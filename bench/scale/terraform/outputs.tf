# The inventory every bench/scale script consumes:
#   terraform output -json inventory > ../.runs/<stamp>/inventory-<N>.json

output "inventory" {
  description = "Hosts of this cluster size, with the fixed private addresses the harness drives."
  value = {
    node_count    = var.node_count
    location      = var.location
    mqttd_version = var.mqttd_version
    run_label     = var.run_label

    brokers = [for i, s in hcloud_server.broker : {
      name        = s.name
      node_id     = s.name # MQTTD_NODE_ID == server name == peer-cert CN
      public_ip   = s.ipv4_address
      private_ip  = local.broker_ips[i]
      mqtt_plain  = "${local.broker_ips[i]}:1883"
      mqtt_tls    = "${local.broker_ips[i]}:8883"
      health      = "${local.broker_ips[i]}:8080"
      peer        = "${local.broker_ips[i]}:7001"
      swim        = "${local.broker_ips[i]}:7946"
      server_type = s.server_type
    }]

    drivers = [for i, s in hcloud_server.driver : {
      name         = s.name
      public_ip    = s.ipv4_address
      private_ip   = local.driver_ips[i]
      builds_bench = i == 0
      server_type  = s.server_type
    }]
  }
}
