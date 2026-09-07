# UpCloud firewalls are PER SERVER (no shared firewall object like Hetzner's),
# and once rules exist the server defaults to DROP for unmatched inbound.
# Posture identical to the Hetzner rig's shared firewall: SSH + ICMP in from
# the public interface, everything in from the private mesh, all outbound.
#
# MQTTD_HEALTH_BIND is 0.0.0.0:8080 on the brokers and the drivers scrape it —
# rule 3 keeps that bind reachable solely over the private network.

locals {
  bench_firewall_rules = [
    {
      comment   = "SSH from admin"
      direction = "in"
      action    = "accept"
      protocol  = "tcp"
      family    = "IPv4"
      dport_s   = "22"
      dport_e   = "22"
      source    = var.admin_cidr
    },
    {
      comment   = "ICMP in"
      direction = "in"
      action    = "accept"
      protocol  = "icmp"
      family    = "IPv4"
      dport_s   = ""
      dport_e   = ""
      source    = "0.0.0.0/0"
    },
    {
      comment   = "Everything from the private mesh"
      direction = "in"
      action    = "accept"
      protocol  = ""
      family    = "IPv4"
      dport_s   = ""
      dport_e   = ""
      source    = "10.99.1.0/24"
    },
    {
      comment   = "All outbound IPv4"
      direction = "out"
      action    = "accept"
      protocol  = ""
      family    = "IPv4"
      dport_s   = ""
      dport_e   = ""
      source    = "0.0.0.0/0"
    },
  ]
}

resource "upcloud_firewall_rules" "broker" {
  count = var.node_count

  server_id = upcloud_server.broker[count.index].id

  dynamic "firewall_rule" {
    for_each = local.bench_firewall_rules
    content {
      comment                = firewall_rule.value.comment
      direction              = firewall_rule.value.direction
      action                 = firewall_rule.value.action
      protocol               = firewall_rule.value.protocol == "" ? null : firewall_rule.value.protocol
      family                 = firewall_rule.value.family
      destination_port_start = firewall_rule.value.dport_s == "" ? null : firewall_rule.value.dport_s
      destination_port_end   = firewall_rule.value.dport_e == "" ? null : firewall_rule.value.dport_e
      source_address_start   = cidrhost(firewall_rule.value.source, 0)
      source_address_end     = cidrhost(firewall_rule.value.source, -1)
    }
  }
}

resource "upcloud_firewall_rules" "driver" {
  count = var.driver_count

  server_id = upcloud_server.driver[count.index].id

  dynamic "firewall_rule" {
    for_each = local.bench_firewall_rules
    content {
      comment                = firewall_rule.value.comment
      direction              = firewall_rule.value.direction
      action                 = firewall_rule.value.action
      protocol               = firewall_rule.value.protocol == "" ? null : firewall_rule.value.protocol
      family                 = firewall_rule.value.family
      destination_port_start = firewall_rule.value.dport_s == "" ? null : firewall_rule.value.dport_s
      destination_port_end   = firewall_rule.value.dport_e == "" ? null : firewall_rule.value.dport_e
      source_address_start   = cidrhost(firewall_rule.value.source, 0)
      source_address_end     = cidrhost(firewall_rule.value.source, -1)
    }
  }
}
