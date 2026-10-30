# Brokers and load generators: Ubuntu 24.04 (Canonical's SSM parameter), the shared boot
# templates of bench/scale/terraform rendered for cloud = "aws", a fixed private address
# each, and a gp3 root volume that holds the brokers' data dir.

data "aws_ssm_parameter" "ubuntu" {
  name = "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
}

resource "aws_key_pair" "bench" {
  key_name   = "mqttd-bench-${var.run_label}"
  public_key = trimspace(file(pathexpand(var.ssh_public_key_path)))
}

resource "aws_placement_group" "bench" {
  count    = var.placement == "cluster" ? 1 : 0
  name     = "mqttd-bench-${var.run_label}"
  strategy = "cluster"
}

data "aws_ec2_instance_type" "broker" {
  instance_type = var.broker_server_type
}

data "aws_ec2_instance_type" "driver" {
  instance_type = var.driver_server_type
}

locals {
  templates       = "${path.module}/../terraform/templates"
  files           = "${path.module}/../terraform/files"
  placement_group = one(aws_placement_group.bench[*].name)
  total_vcpus = (var.node_count * data.aws_ec2_instance_type.broker.default_vcpus +
  var.driver_count * data.aws_ec2_instance_type.driver.default_vcpus)
}

resource "terraform_data" "quota_guard" {
  input = local.total_vcpus

  lifecycle {
    precondition {
      condition = local.total_vcpus <= var.vcpu_quota
      error_message = join("", [
        "this run needs ${local.total_vcpus} vCPUs (${var.node_count} x ${var.broker_server_type} + ",
        "${var.driver_count} x ${var.driver_server_type}) but vcpu_quota is ${var.vcpu_quota}: lower ",
        "the counts or the types, or raise vcpu_quota once AWS has raised the account's ",
        "on-demand standard quota (L-1216C47A) in ${var.region}."
      ])
    }
  }
}

resource "aws_instance" "broker" {
  count = var.node_count

  ami                    = data.aws_ssm_parameter.ubuntu.insecure_value
  instance_type          = var.broker_server_type
  subnet_id              = aws_subnet.bench.id
  private_ip             = local.broker_ips[count.index]
  vpc_security_group_ids = [aws_security_group.bench.id]
  key_name               = aws_key_pair.bench.key_name
  placement_group        = local.placement_group

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.root_volume_gb
    iops                  = var.root_volume_iops
    throughput            = var.root_volume_throughput
    delete_on_termination = true
  }

  metadata_options {
    http_tokens = "required"
  }

  user_data = templatefile("${local.templates}/cloud-init-broker.yaml.tftpl", {
    ssh_public_key     = trimspace(file(pathexpand(var.ssh_public_key_path)))
    cloud              = "aws"
    private_ip         = local.broker_ips[count.index]
    mqttd_version      = var.mqttd_version
    mqttd_sha256       = var.mqttd_sha256
    mqttd_url          = var.mqttd_url
    mqttd_service_unit = file("${path.module}/../../../deploy/systemd/mqttd.service")
    mqttd_override     = file("${local.files}/mqttd-override.conf")
    sysctl_conf        = file("${local.files}/sysctl-broker.conf")
    nic_spread         = var.broker_nic_spread
    data_tmpfs         = var.broker_data_tmpfs
    docker             = var.broker_docker
  })
  user_data_replace_on_change = true

  tags = { Name = "mqttd-${count.index + 1}", role = "broker" }
  # default_tags do not reach the root volume: tag it, so the teardown's audit sees one left behind.
  volume_tags = merge(local.common_tags, { role = "broker" })

  depends_on = [aws_route_table_association.bench, terraform_data.quota_guard]
}

resource "aws_instance" "driver" {
  count = var.driver_count

  ami                    = data.aws_ssm_parameter.ubuntu.insecure_value
  instance_type          = var.driver_server_type
  subnet_id              = aws_subnet.bench.id
  private_ip             = local.driver_ips[count.index]
  vpc_security_group_ids = [aws_security_group.bench.id]
  key_name               = aws_key_pair.bench.key_name
  placement_group        = local.placement_group

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 30
    delete_on_termination = true
  }

  metadata_options {
    http_tokens = "required"
  }

  user_data = templatefile("${local.templates}/cloud-init-driver.yaml.tftpl", {
    ssh_public_key = trimspace(file(pathexpand(var.ssh_public_key_path)))
    cloud          = "aws"
    private_ip     = local.driver_ips[count.index]
    build_bench    = var.build_bench && count.index == 0
    emqtt_bench    = var.driver_emqtt_bench
    bench_git_ref  = var.bench_git_ref
    sysctl_conf    = file("${local.files}/sysctl-driver.conf")
  })
  user_data_replace_on_change = true

  tags = { Name = "bench-driver-${count.index + 1}", role = "driver" }
  # default_tags do not reach the root volume: tag it, so the teardown's audit sees one left behind.
  volume_tags = merge(local.common_tags, { role = "driver" })

  depends_on = [aws_route_table_association.bench, terraform_data.quota_guard]
}
