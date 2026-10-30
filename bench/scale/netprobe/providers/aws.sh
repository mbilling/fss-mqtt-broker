# netprobe provider: AWS EC2, with the aws CLI (sourced by run.sh). See AWS.md for the
# account setup: a profile, the IAM policy in aws-policy.json, and the vCPU quota.
#
#   AWS_PROFILE    the CLI profile to use (AWS.md); or any credentials the CLI finds
#   REGION         default: the profile's region (aws configure get region), else eu-north-1;
#                  an account on the new AWS experience can only use its project's region
#   TYPE           instance type (default m6i.8xlarge: 32 vCPU, 12.5 Gbit/s, the generation
#                  of HiveMQ 4.18's m6a.8xlarge, which eu-north-1 does not offer)
#   AZ             availability zone (default: the region's first that offers TYPE)
#   PLACEMENT      none (default) or cluster: a cluster placement group, the lowest-latency
#                  placement EC2 offers; none is what an ordinary deployment gets
#
# Two on-demand instances in the default VPC's subnet of one AZ (traffic between them
# on private addresses is free within an AZ), Ubuntu 24.04 (Canonical's SSM parameter),
# a security group with SSH from anywhere and everything between its own members, and
# an imported key pair; an 8 GB gp3 root volume each, deleted with the instance. All
# tagged purpose=mqttd-netprobe and run=<stamp>; the teardown terminates by that tag and
# audits instances, volumes, the security group, key pair and placement group.

REGION="${REGION:-$(aws configure get region 2>/dev/null || true)}"
REGION="${REGION:-eu-north-1}"
TYPE="${TYPE:-m6i.8xlarge}"
PLACEMENT="${PLACEMENT:-none}"
AMI_PARAM=/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id
ec2() { aws --region "$REGION" ec2 "$@"; }
TAGS="{Key=purpose,Value=mqttd-netprobe},{Key=run,Value=$STAMP}"

provider_check() {
	command -v aws >/dev/null || die "the aws CLI is required (AWS.md: brew install awscli)"
	aws sts get-caller-identity --output text --query Account >/dev/null 2>&1 ||
		die "the aws CLI has no working credentials (AWS.md: aws configure sso --profile mqttd-bench, then AWS_PROFILE=mqttd-bench)"
	case "$PLACEMENT" in none | cluster) ;; *) die "PLACEMENT must be none or cluster" ;; esac
	local vcpu quota
	vcpu=$(ec2 describe-instance-types --instance-types "$TYPE" --query 'InstanceTypes[0].VCpuInfo.DefaultVCpus' --output text 2>/dev/null) ||
		die "unknown instance type $TYPE in $REGION"
	quota=$(aws --region "$REGION" service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A \
		--query 'Quota.Value' --output text 2>/dev/null || echo "")
	if [ -n "$quota" ] && [ "$quota" != None ]; then
		python3 -c "import sys; sys.exit(0 if 2 * $vcpu <= float('$quota') else 1)" ||
			die "2 x $TYPE needs $((2 * vcpu)) vCPU; the account's on-demand standard quota in $REGION is $quota (AWS.md: raise L-1216C47A)"
	else
		say "aws: could not read the vCPU quota (servicequotas:GetServiceQuota); 2 x $TYPE needs $((2 * vcpu)) vCPU"
	fi
}

provider_quote() {
	local usd
	usd=$(aws --region us-east-1 pricing get-products --service-code AmazonEC2 --max-items 1 --output json --filters \
		"Type=TERM_MATCH,Field=instanceType,Value=$TYPE" "Type=TERM_MATCH,Field=regionCode,Value=$REGION" \
		"Type=TERM_MATCH,Field=operatingSystem,Value=Linux" "Type=TERM_MATCH,Field=tenancy,Value=Shared" \
		"Type=TERM_MATCH,Field=preInstalledSw,Value=NA" "Type=TERM_MATCH,Field=capacitystatus,Value=Used" 2>/dev/null |
		jq -r '.PriceList[0] // empty | fromjson | .terms.OnDemand[].priceDimensions[].pricePerUnit.USD' 2>/dev/null | head -1)
	if [ -n "$usd" ]; then
		say "aws: 2 x $TYPE in $REGION = USD $(python3 -c "print(f'{2 * $usd:.4f}')")/h on demand; ~10 min = USD $(python3 -c "print(f'{2 * $usd * 10 / 60:.2f}')") (EC2 bills per second, 60 s minimum; plus 2 x 8 GB gp3)"
	else
		say "aws: 2 x $TYPE in $REGION — no price from the pricing API (pricing:GetProducts); check https://aws.amazon.com/ec2/pricing/on-demand/"
	fi
}

provider_create() {
	local ami vpc subnet sg az pg_arg=() ids
	ami=$(aws --region "$REGION" ssm get-parameter --name "$AMI_PARAM" --query Parameter.Value --output text) ||
		die "no Ubuntu 24.04 AMI in $REGION"
	vpc=$(ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
	[ -n "$vpc" ] && [ "$vpc" != None ] || die "no default VPC in $REGION (AWS.md)"
	az="${AZ:-$(ec2 describe-instance-type-offerings --location-type availability-zone \
		--filters "Name=instance-type,Values=$TYPE" --query 'InstanceTypeOfferings[].Location' --output text | tr '\t' '\n' | sort | head -1)}"
	[ -n "$az" ] || die "$TYPE is not offered in any AZ of $REGION"
	subnet=$(ec2 describe-subnets --filters "Name=vpc-id,Values=$vpc" "Name=availability-zone,Values=$az" "Name=default-for-az,Values=true" \
		--query 'Subnets[0].SubnetId' --output text)
	[ -n "$subnet" ] && [ "$subnet" != None ] || die "no default subnet in $az"
	ec2 import-key-pair --key-name "$NAME" --public-key-material "fileb://$KEY.pub" \
		--tag-specifications "ResourceType=key-pair,Tags=[$TAGS]" >/dev/null
	sg=$(ec2 create-security-group --group-name "$NAME" --description "mqttd netprobe $STAMP" --vpc-id "$vpc" \
		--tag-specifications "ResourceType=security-group,Tags=[$TAGS]" --query GroupId --output text)
	echo "$sg" >"$RUN/aws-sg"
	ec2 authorize-security-group-ingress --group-id "$sg" --protocol tcp --port 22 --cidr 0.0.0.0/0 >/dev/null
	ec2 authorize-security-group-ingress --group-id "$sg" --protocol -1 --source-group "$sg" >/dev/null
	if [ "$PLACEMENT" = cluster ]; then
		ec2 create-placement-group --group-name "$NAME" --strategy cluster \
			--tag-specifications "ResourceType=placement-group,Tags=[$TAGS]" >/dev/null
		pg_arg=(--placement "GroupName=$NAME")
	fi
	# Root login with the same key, as on Hetzner: cloud-init gives root a key that only
	# prints "log in as ubuntu"; the image's sshd already allows keys for root.
	printf '#!/bin/bash\ninstall -m 600 /home/ubuntu/.ssh/authorized_keys /root/.ssh/authorized_keys\n' >"$RUN/user-data.sh"
	say "creating 2 x $TYPE in $az ($subnet, placement $PLACEMENT)"
	ids=$(ec2 run-instances --image-id "$ami" --instance-type "$TYPE" --count 2 --key-name "$NAME" \
		--security-group-ids "$sg" --subnet-id "$subnet" "${pg_arg[@]}" \
		--user-data "file://$RUN/user-data.sh" --metadata-options HttpTokens=required \
		--block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=8,VolumeType=gp3,DeleteOnTermination=true}' \
		--tag-specifications "ResourceType=instance,Tags=[$TAGS,{Key=Name,Value=$NAME}]" "ResourceType=volume,Tags=[$TAGS]" \
		--query 'Instances[].InstanceId' --output text)
	echo "$ids" >"$RUN/aws-instances"
	# shellcheck disable=SC2086 # two ids, split on purpose
	ec2 wait instance-running --instance-ids $ids
	# shellcheck disable=SC2086
	ec2 describe-instances --instance-ids $ids --output json >"$RUN/instances.json"
	read -r A APRIV B BPRIV < <(jq -r '[.Reservations[].Instances[]] | sort_by(.InstanceId) |
		"\(.[0].PublicIpAddress) \(.[0].PrivateIpAddress) \(.[1].PublicIpAddress) \(.[1].PrivateIpAddress)"' "$RUN/instances.json")
	echo "$PLACEMENT $az" >"$RUN/aws-placement"
}

provider_teardown() {
	local ids sg sel=(--filters "Name=tag:purpose,Values=mqttd-netprobe" "Name=tag:run,Values=$STAMP")
	ids=$(ec2 describe-instances "${sel[@]}" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
		--query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null || true)
	if [ -n "$ids" ]; then
		# shellcheck disable=SC2086
		ec2 terminate-instances --instance-ids $ids >/dev/null || true
		# shellcheck disable=SC2086
		ec2 wait instance-terminated --instance-ids $ids || true
	fi
	sg=$(cat "$RUN/aws-sg" 2>/dev/null || true)
	[ -z "$sg" ] || ec2 delete-security-group --group-id "$sg" >/dev/null 2>&1 || true
	ec2 delete-key-pair --key-name "$NAME" >/dev/null 2>&1 || true
	[ "$PLACEMENT" != cluster ] || ec2 delete-placement-group --group-name "$NAME" >/dev/null 2>&1 || true
	{
		ec2 describe-instances "${sel[@]}" "Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down" \
			--query 'Reservations[].Instances[].[InstanceId,State.Name]' --output text
		ec2 describe-volumes "${sel[@]}" --query 'Volumes[].[VolumeId,State]' --output text
		ec2 describe-security-groups "${sel[@]}" --query 'SecurityGroups[].[GroupId,GroupName]' --output text
		ec2 describe-key-pairs "${sel[@]}" --query 'KeyPairs[].KeyName' --output text
		ec2 describe-placement-groups "${sel[@]}" --query 'PlacementGroups[].GroupName' --output text
	} 2>&1 | grep -v '^\s*$' || true
}
