# The public interface admits SSH (from admin_cidr) and ICMP, nothing else.
# MQTTD_HEALTH_BIND is 0.0.0.0:8080 on the brokers because the drivers must
# scrape it — Hetzner firewalls filter the PUBLIC interface only, so that bind
# is reachable solely over the private network. The orchestrator's own health
# checks go through `ssh <host> curl localhost:8080/...`.

resource "hcloud_firewall" "bench" {
  name   = "mqttd-bench"
  labels = local.common_labels

  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "22"
    source_ips = [var.admin_cidr]
  }

  rule {
    direction  = "in"
    protocol   = "icmp"
    source_ips = ["0.0.0.0/0", "::/0"]
  }
}
