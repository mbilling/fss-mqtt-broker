#!/usr/bin/env bash
# Provider selection shared by provisioning and standalone teardown.
# Source after lib.sh. No API calls or secret output.
select_scale_cloud() {
	CLOUD="${CLOUD:-hcloud}"
	case "$CLOUD" in
	hcloud) TFDIR="$SCALE_DIR/terraform" ;;
	upcloud) TFDIR="$SCALE_DIR/terraform-upcloud" ;;
	aws) TFDIR="$SCALE_DIR/terraform-aws" ;;
	*) die "unknown CLOUD=$CLOUD (supported: hcloud, upcloud, aws)" ;;
	esac
	# The resource type of a server in TFDIR's state (the "state still tracks servers" guard).
	case "$CLOUD" in
	hcloud) TF_SERVER_TYPE=hcloud_server ;;
	upcloud) TF_SERVER_TYPE=upcloud_server ;;
	aws) TF_SERVER_TYPE=aws_instance ;;
	esac
}

require_scale_token() {
	case "$CLOUD" in
	hcloud) [ -n "${HCLOUD_TOKEN:-}" ] || die "HCLOUD_TOKEN is not set (Read & Write token from the dedicated Hetzner project)" ;;
	upcloud) [ -n "${UPCLOUD_TOKEN:-}" ] || die "UPCLOUD_TOKEN is not set (a Bearer API token; CLOUD=upcloud)" ;;
	aws)
		command -v aws >/dev/null || die "the aws CLI is required for CLOUD=aws (bench/scale/netprobe/AWS.md)"
		aws sts get-caller-identity --query Account --output text >/dev/null 2>&1 ||
			die "the aws CLI has no working credentials: aws login --profile <name>, then AWS_PROFILE=<name> (bench/scale/netprobe/AWS.md)"
		;;
	esac
}
# AWS_REGION for CLOUD=aws: AWS_REGION, else the CLI profile's region, else eu-north-1. An
# account on the new AWS experience can only use its project's region.
aws_region() {
	[ -n "${AWS_REGION:-}" ] || AWS_REGION=$(aws configure get region 2>/dev/null || true)
	AWS_REGION="${AWS_REGION:-eu-north-1}"
	export AWS_REGION
}
# cloud_tofu_env: what OpenTofu needs in its environment, fresh for each call. CLOUD=aws:
# the CLI's current session as plain environment credentials (aws configure
# export-credentials), because the provider cannot read every kind of CLI profile (an
# `aws login` session among them) and those sessions are short: a destroy at the end of a
# three-hour ladder must not run on keys exported at its start. Nothing is printed.
cloud_tofu_env() {
	[ "${CLOUD:-hcloud}" = aws ] || return 0
	local env
	env=$(aws configure export-credentials --format env ${AWS_PROFILE:+--profile "$AWS_PROFILE"} 2>/dev/null) ||
		die "aws configure export-credentials failed: the CLI session expired? aws login --profile ${AWS_PROFILE:-<name>}"
	eval "$env"
}
