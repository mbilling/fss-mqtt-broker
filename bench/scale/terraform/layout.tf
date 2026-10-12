# Where each broker lands: its private address and its placement group. Locals
# only, no provider — test-upcloud-quota.py evaluates this file offline.
#
# ADDRESSES. Brokers 1-10 are 10.99.1.11-20 and drivers 1-20 are 10.99.1.21-40,
# as they have always been. The broker range was sized for ten, so an eleventh
# broker at 10.99.1.21 would take driver 1's address. Brokers 11+ therefore
# continue at 10.99.1.41, past the last driver, and nothing that already exists
# moves: a shape of ten or fewer plans exactly as it did.
#
# PLACEMENT GROUPS. Hetzner admits at most 10 servers to a spread placement
# group. Brokers 1-10 stay in the original group; brokers 11+ go into a second
# one (brokers.tf creates it only when node_count > 10). Each group keeps ITS
# members on distinct physical hosts, but the two groups know nothing of each
# other: a broker of the second may share a host with a broker of the first.
locals {
  placement_group_capacity = 10

  broker_ips = [
    for i in range(var.node_count) :
    "10.99.1.${i < local.placement_group_capacity ? 11 + i : 31 + i}"
  ]
  driver_ips = [for i in range(var.driver_count) : "10.99.1.${21 + i}"]

  # 0 = the original group, 1 = the second. Indexed like hcloud_server.broker.
  broker_placement_group = [
    for i in range(var.node_count) : i < local.placement_group_capacity ? 0 : 1
  ]
  second_placement_group = var.node_count > local.placement_group_capacity
}
