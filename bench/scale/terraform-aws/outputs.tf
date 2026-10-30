# The same `inventory` as bench/scale/terraform/outputs.tf, field for field, plus the AZ.
output "inventory" {
  description = "Hosts of this cluster size, with the fixed private addresses the harness drives."
  value = {
    node_count    = var.node_count
    location      = local.az
    mqttd_version = var.mqttd_version
    run_label     = var.run_label
    brokers = [for i, s in aws_instance.broker : {
      name        = "mqttd-${i + 1}"
      node_id     = "mqttd-${i + 1}" # MQTTD_NODE_ID == host name == peer-cert CN
      public_ip   = s.public_ip
      private_ip  = local.broker_ips[i]
      mqtt_plain  = "${local.broker_ips[i]}:1883"
      mqtt_tls    = "${local.broker_ips[i]}:8883"
      health      = "${local.broker_ips[i]}:8080"
      peer        = "${local.broker_ips[i]}:7001"
      swim        = "${local.broker_ips[i]}:7946"
      server_type = s.instance_type
    }]
    drivers = [for i, s in aws_instance.driver : {
      name         = "bench-driver-${i + 1}"
      public_ip    = s.public_ip
      private_ip   = local.driver_ips[i]
      builds_bench = i == 0
      server_type  = s.instance_type
    }]
  }
}
