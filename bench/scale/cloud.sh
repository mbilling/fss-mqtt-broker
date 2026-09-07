#!/usr/bin/env bash
# Provider selection shared by provisioning and standalone teardown.
# Source after lib.sh. No API calls or secret output.
select_scale_cloud() {
	CLOUD="${CLOUD:-hcloud}"
	case "$CLOUD" in
	hcloud) TFDIR="$SCALE_DIR/terraform" ;;
	upcloud) TFDIR="$SCALE_DIR/terraform-upcloud" ;;
	*) die "unknown CLOUD=$CLOUD (supported: hcloud, upcloud)" ;;
	esac
}

require_scale_token() {
	case "$CLOUD" in
	hcloud) [ -n "${HCLOUD_TOKEN:-}" ] || die "HCLOUD_TOKEN is not set (Read & Write token from the dedicated Hetzner project)" ;;
	upcloud) [ -n "${UPCLOUD_TOKEN:-}" ] || die "UPCLOUD_TOKEN is not set (a Bearer API token; CLOUD=upcloud)" ;;
	esac
}
