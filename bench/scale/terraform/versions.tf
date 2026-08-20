# The scale-curve rig (ADR 0048 T3). Applied per cluster size from an operator's
# machine — never from CI. Credentials come from HCLOUD_TOKEN in the environment;
# nothing here reads or stores a secret.

terraform {
  required_version = ">= 1.7"

  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.50"
    }
  }
}

provider "hcloud" {
  # HCLOUD_TOKEN from the environment.
}
