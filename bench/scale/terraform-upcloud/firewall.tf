# UpCloud firewalls are PER SERVER and cover public/utility interfaces only;
# private SDN traffic bypasses them. Explicitly drop unmatched public inbound
# traffic rather than relying on an implicit provider default. SSH + ICMP are
# public; the MQTT/health/peer listeners remain reachable over private SDN.

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
      sport_s   = ""
      sport_e   = ""
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
      sport_s   = ""
      sport_e   = ""
      source    = "0.0.0.0/0"
    },
    {
      comment   = "All outbound IPv4"
      direction = "out"
      action    = "accept"
      protocol  = ""
      family    = "IPv4"
      dport_s   = ""
      dport_e   = ""
      sport_s   = ""
      sport_e   = ""
      source    = "0.0.0.0/0"
    },
    # UpCloud's firewall does not admit replies to outbound UDP on its own, so with
    # the drop rule below chrony never heard back from its pool (2026-10-05: every
    # chronyc poll had refid 00000000 and the run died at "NTP reference failed to
    # synchronize"). Admit the replies the hosts need, by SOURCE port: NTP, DNS, and
    # HTTP(S) for the package and binary downloads the rig does after boot. The
    # firewall is stateless, so a packet merely SENT from one of those ports would
    # pass too: the destination is bounded to unprivileged ports, where replies
    # land (drivers widen ip_local_port_range to 1024-65535), which keeps SSH and
    # every other privileged listener behind admin_cidr and the drop rule.
    {
      comment   = "NTP replies"
      direction = "in"
      action    = "accept"
      protocol  = "udp"
      family    = "IPv4"
      dport_s   = "1024"
      dport_e   = "65535"
      sport_s   = "123"
      sport_e   = "123"
      source    = "0.0.0.0/0"
    },
    {
      comment   = "DNS replies"
      direction = "in"
      action    = "accept"
      protocol  = "udp"
      family    = "IPv4"
      dport_s   = "1024"
      dport_e   = "65535"
      sport_s   = "53"
      sport_e   = "53"
      source    = "0.0.0.0/0"
    },
    {
      comment   = "HTTP replies"
      direction = "in"
      action    = "accept"
      protocol  = "tcp"
      family    = "IPv4"
      dport_s   = "1024"
      dport_e   = "65535"
      sport_s   = "80"
      sport_e   = "80"
      source    = "0.0.0.0/0"
    },
    {
      comment   = "HTTPS replies"
      direction = "in"
      action    = "accept"
      protocol  = "tcp"
      family    = "IPv4"
      dport_s   = "1024"
      dport_e   = "65535"
      sport_s   = "443"
      sport_e   = "443"
      source    = "0.0.0.0/0"
    },
    {
      comment   = "Drop unmatched public inbound"
      direction = "in"
      action    = "drop"
      protocol  = ""
      family    = "IPv4"
      dport_s   = ""
      dport_e   = ""
      sport_s   = ""
      sport_e   = ""
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
      source_port_start      = firewall_rule.value.sport_s == "" ? null : firewall_rule.value.sport_s
      source_port_end        = firewall_rule.value.sport_e == "" ? null : firewall_rule.value.sport_e
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
      source_port_start      = firewall_rule.value.sport_s == "" ? null : firewall_rule.value.sport_s
      source_port_end        = firewall_rule.value.sport_e == "" ? null : firewall_rule.value.sport_e
      source_address_start   = cidrhost(firewall_rule.value.source, 0)
      source_address_end     = cidrhost(firewall_rule.value.source, -1)
    }
  }
}
