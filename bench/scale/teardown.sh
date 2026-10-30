#!/usr/bin/env bash
# Standalone teardown / leak sweeper. Normal runs destroy themselves; this is
# for a dead laptop, a lost state file, or paranoia before reading the invoice.
#
#   ./teardown.sh           # tofu destroy from local state, then AUDIT by label
#   ./teardown.sh --force   # also DELETE whatever the label audit finds (needs hcloud CLI)
#
# Non-default SSH_KEY is passed through to tofu destroy as ${SSH_KEY}.pub (the
# same path apply used). That file must exist: OpenTofu reads ssh_public_key_path
# while tearing down.
#
# Everything this rig creates carries the label purpose=mqttd-bench-scale, and
# the README tells the operator to use a DEDICATED Hetzner project — so a forced
# sweep can never touch anything that is not ours.

set -euo pipefail
. "$(dirname "$0")/lib.sh"
. "$SCALE_DIR/cloud.sh"
select_scale_cloud

FORCE=0
[ "${1:-}" = --force ] && FORCE=1

TF=$(command -v tofu) || die "OpenTofu (tofu) is required; Terraform is not supported"

if [ "$CLOUD" = upcloud ]; then
	# No hcloud commands here: a provider mismatch could delete an unrelated
	# Hetzner benchmark while leaving every UpCloud server billing.
	[ "$FORCE" = 0 ] || die "CLOUD=upcloud does not support --force; use state-backed teardown or inspect the UpCloud console"
	require_scale_token
	[ -f "$TFDIR/terraform.tfstate" ] || die "UpCloud state missing; inspect servers, storage, networks and server groups in the UpCloud console (do not use the Hetzner sweeper)"
	(cd "$TFDIR" && "$TF" init -input=false && "$TF" destroy -auto-approve -input=false -var node_count=1 "${TOFU_SSH_PUBKEY_ARGS[@]}") ||
		die "UpCloud destroy failed; preserve state and inspect the UpCloud console before retrying"
	say "UpCloud resources tracked in this state destroyed. No account-wide leak audit was performed; verify orphaned resources in the UpCloud console."
	exit 0
fi

if [ "$CLOUD" = aws ]; then
	require_scale_token
	aws_region
	TEARDOWN_TRIES=${TEARDOWN_TRIES:-3}
	TEARDOWN_RETRY_SECS=${TEARDOWN_RETRY_SECS:-20}
	if [ -f "$TFDIR/terraform.tfstate" ]; then
		destroyed=0
		for ((try = 1; try <= TEARDOWN_TRIES; try++)); do
			say "OpenTofu destroy from local state (attempt $try of $TEARDOWN_TRIES)"
			cloud_tofu_env
			if (cd "$TFDIR" && "$TF" destroy -auto-approve -var node_count=1 -var region="$AWS_REGION" "${TOFU_SSH_PUBKEY_ARGS[@]}"); then
				destroyed=1
				break
			fi
			[ "$try" -lt "$TEARDOWN_TRIES" ] && sleep "$TEARDOWN_RETRY_SECS"
		done
		[ "$destroyed" = 1 ] || warn "OpenTofu destroy failed $TEARDOWN_TRIES times — continuing to the tag audit"
	fi
	# Every resource the rig creates carries the tag purpose=mqttd-bench-scale (the
	# provider's default_tags). An audit that cannot list a kind is a FAILURE, never an
	# all-clear (the 2026-10-23 rule below).
	say "auditing by tag purpose=mqttd-bench-scale in $AWS_REGION"
	F=(--filters Name=tag:purpose,Values=mqttd-bench-scale)
	LEAKED=0
	UNAUDITED=()
	audit() { # audit <kind> <aws ec2 describe-… arguments…>: what of <kind> is left
		local kind="$1" out="" listed=0
		shift
		for ((try = 1; try <= 5; try++)); do
			if out=$(aws --region "$AWS_REGION" ec2 "$@" --output text 2>/dev/null); then
				listed=1
				break
			fi
			sleep "$TEARDOWN_RETRY_SECS"
		done
		if [ "$listed" = 0 ]; then
			UNAUDITED+=("$kind")
			return
		fi
		out=$(grep -v '^\s*$' <<<"$out" || true)
		if [ -n "$out" ]; then
			LEAKED=1
			printf '\033[1;31mLEAKED %s:\033[0m\n%s\n' "$kind" "$out" >&2
		fi
	}
	audit instance describe-instances "${F[@]}" Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down \
		--query 'Reservations[].Instances[].[InstanceId,InstanceType,State.Name]'
	audit volume describe-volumes "${F[@]}" --query 'Volumes[].[VolumeId,State,Size]'
	audit security-group describe-security-groups "${F[@]}" --query 'SecurityGroups[].[GroupId,GroupName]'
	audit key-pair describe-key-pairs "${F[@]}" --query 'KeyPairs[].KeyName'
	audit placement-group describe-placement-groups "${F[@]}" --query 'PlacementGroups[].GroupName'
	audit subnet describe-subnets "${F[@]}" --query 'Subnets[].SubnetId'
	audit route-table describe-route-tables "${F[@]}" --query 'RouteTables[].RouteTableId'
	audit internet-gateway describe-internet-gateways "${F[@]}" --query 'InternetGateways[].InternetGatewayId'
	audit vpc describe-vpcs "${F[@]}" --query 'Vpcs[].VpcId'
	if [ "${#UNAUDITED[@]}" -gt 0 ]; then
		die "the tag audit could NOT reach the AWS API for: ${UNAUDITED[*]}. Resources may still be billing. Re-run ./teardown.sh when the network is back, and check the EC2 console in $AWS_REGION"
	fi
	if [ "$LEAKED" = 1 ]; then
		if [ "$FORCE" = 1 ]; then
			ids=$(aws --region "$AWS_REGION" ec2 describe-instances "${F[@]}" Name=instance-state-name,Values=pending,running,stopping,stopped \
				--query 'Reservations[].Instances[].InstanceId' --output text)
			if [ -n "$ids" ]; then
				# shellcheck disable=SC2086 # ids, split on purpose
				aws --region "$AWS_REGION" ec2 terminate-instances --instance-ids $ids >/dev/null && say "terminated $ids"
			fi
			die "--force terminates the instances (the billing part); the VPC, security group, key pair and the rest cost nothing but must go in order: run ./teardown.sh again once the instances are gone (tofu destroy from state), or delete them in the VPC console"
		fi
		die "leaked resources found (listed above). Re-run with --force to terminate the instances."
	fi
	say "nothing leaked — you are not paying for anything"
	exit 0
fi
# A network blip must not turn into a fleet left billing: retry the destroy, and an
# audit that cannot reach the API is a FAILURE, never an all-clear. On 2026-10-23 the
# laptop's DNS went away for a few minutes; the destroy failed, every audit listing
# failed into an empty answer, and "nothing leaked" was printed over 30 running servers.
TEARDOWN_TRIES=${TEARDOWN_TRIES:-3}
TEARDOWN_RETRY_SECS=${TEARDOWN_RETRY_SECS:-20}

if [ -f "$TFDIR/terraform.tfstate" ]; then
	destroyed=0
	for ((try = 1; try <= TEARDOWN_TRIES; try++)); do
		say "OpenTofu destroy from local state (attempt $try of $TEARDOWN_TRIES)"
		if (cd "$TFDIR" && "$TF" destroy -auto-approve -var node_count=1 "${TOFU_SSH_PUBKEY_ARGS[@]}"); then
			destroyed=1
			break
		fi
		[ "$try" -lt "$TEARDOWN_TRIES" ] && sleep "$TEARDOWN_RETRY_SECS"
	done
	[ "$destroyed" = 1 ] || warn "OpenTofu destroy failed $TEARDOWN_TRIES times — continuing to the label audit"
fi

if ! command -v hcloud >/dev/null; then
	warn "hcloud CLI not installed — cannot audit by label."
	warn "Verify by hand in the Hetzner console that NO servers remain in the bench project."
	exit 0
fi

say "auditing by label purpose=mqttd-bench-scale"
LEAKED=0
UNAUDITED=()
AUDIT_ERR=$(mktemp)
trap 'rm -f "$AUDIT_ERR"' EXIT
for kind in server network firewall placement-group ssh-key; do
	out="" listed=0
	for ((try = 1; try <= 5; try++)); do
		if out=$(hcloud "$kind" list -l purpose=mqttd-bench-scale -o noheader 2>"$AUDIT_ERR"); then
			listed=1
			break
		fi
		sleep "$TEARDOWN_RETRY_SECS"
	done
	if [ "$listed" = 0 ]; then
		UNAUDITED+=("$kind")
		warn "could not list ${kind}s: $(head -c 200 "$AUDIT_ERR")"
		continue
	fi
	if [ -n "$out" ]; then
		LEAKED=1
		printf '\033[1;31mLEAKED %s:\033[0m\n%s\n' "$kind" "$out" >&2
		if [ "$FORCE" = 1 ]; then
			echo "$out" | awk '{print $1}' | while read -r id; do
				hcloud "$kind" delete "$id" && say "deleted $kind $id"
			done
		fi
	fi
done

if [ "${#UNAUDITED[@]}" -gt 0 ]; then
	die "the label audit could NOT reach the Hetzner API for: ${UNAUDITED[*]}. Resources may still be billing. Re-run ./teardown.sh --force when the network is back, and check 'hcloud server list'"
fi
if [ "$LEAKED" = 1 ] && [ "$FORCE" != 1 ]; then
	die "leaked resources found (listed above). Re-run with --force to delete them."
fi
say "nothing leaked — you are not paying for anything"
