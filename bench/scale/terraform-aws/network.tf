# A VPC of the rig's own, 10.99.0.0/16 with the subnet 10.99.1.0/24, so every host gets
# the same private address as on Hetzner (bench/scale/terraform/layout.tf's
# 10.99.1.11+ brokers, 10.99.1.21+ drivers), and an internet gateway for SSH and for the
# hosts' package and release downloads. EC2 reserves .0-.3 and .255 of a subnet; the
# rig's addresses start at .11.

locals {
  common_tags = {
    purpose = "mqttd-bench-scale"
    run     = var.run_label
  }

  placement_group_capacity = 10
  broker_ips = [
    for i in range(var.node_count) :
    "10.99.1.${i < local.placement_group_capacity ? 11 + i : 31 + i}"
  ]
  driver_ips = [for i in range(var.driver_count) : "10.99.1.${21 + i}"]
}

data "aws_ec2_instance_type_offerings" "both" {
  for_each = toset([var.broker_server_type, var.driver_server_type])

  filter {
    name   = "instance-type"
    values = [each.key]
  }
  location_type = "availability-zone"
}

locals {
  # The AZs that offer both types; the first, unless one is named.
  common_azs = sort(setintersection([for o in data.aws_ec2_instance_type_offerings.both : toset(o.locations)]...))
  az         = var.availability_zone != "" ? var.availability_zone : local.common_azs[0]
}

resource "aws_vpc" "bench" {
  cidr_block           = "10.99.0.0/16"
  enable_dns_hostnames = true
  tags                 = { Name = "mqttd-bench" }
}

resource "aws_subnet" "bench" {
  vpc_id                  = aws_vpc.bench.id
  cidr_block              = "10.99.1.0/24"
  availability_zone       = local.az
  map_public_ip_on_launch = true
  tags                    = { Name = "mqttd-bench" }
}

resource "aws_internet_gateway" "bench" {
  vpc_id = aws_vpc.bench.id
  tags   = { Name = "mqttd-bench" }
}

resource "aws_route_table" "bench" {
  vpc_id = aws_vpc.bench.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.bench.id
  }
  tags = { Name = "mqttd-bench" }
}

resource "aws_route_table_association" "bench" {
  subnet_id      = aws_subnet.bench.id
  route_table_id = aws_route_table.bench.id
}

# SSH from admin_cidr and ICMP from anywhere (as bench/scale/terraform/firewall.tf),
# and every protocol between the rig's own hosts (Hetzner's private network is open).
resource "aws_security_group" "bench" {
  name        = "mqttd-bench-${var.run_label}"
  description = "mqttd bench scale rig ${var.run_label}"
  vpc_id      = aws_vpc.bench.id
  tags        = { Name = "mqttd-bench" }
}

resource "aws_vpc_security_group_ingress_rule" "ssh" {
  security_group_id = aws_security_group.bench.id
  ip_protocol       = "tcp"
  from_port         = 22
  to_port           = 22
  cidr_ipv4         = var.admin_cidr
}

resource "aws_vpc_security_group_ingress_rule" "icmp" {
  security_group_id = aws_security_group.bench.id
  ip_protocol       = "icmp"
  from_port         = -1
  to_port           = -1
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_vpc_security_group_ingress_rule" "within" {
  security_group_id            = aws_security_group.bench.id
  ip_protocol                  = "-1"
  referenced_security_group_id = aws_security_group.bench.id
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.bench.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}
