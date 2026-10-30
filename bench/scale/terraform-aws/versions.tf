# The rig on AWS EC2 (CLOUD=aws). The same hosts, private addresses, boot templates and
# `inventory` output as bench/scale/terraform (Hetzner), so everything after provisioning
# is shared. Credentials come from the AWS CLI's profile (AWS_PROFILE; aws login).
terraform {
  required_version = ">= 1.7"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = local.common_tags
  }
}
