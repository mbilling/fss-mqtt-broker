# netprobe on AWS: account setup

The probe runs the same tests on AWS as on Hetzner (`run.sh`, `PROVIDER=aws`): two
instances in one availability zone, iperf3 and neper, and every counter that can drop or
delay a packet. The default type is m6a.8xlarge, the instance HiveMQ 4.18 published its
270k msg/s run on, in eu-central-1 (Frankfurt).

You do the account steps below yourself: the credentials go into the AWS CLI on your
machine and never into a chat, a file in this repository, or an environment variable you
paste anywhere.

## 1. Install the AWS CLI

```bash
brew install awscli
```

```bash
aws --version
```

## 2. Give the probe a narrow identity

Use an identity that can do only what the probe needs. The policy is
[`aws-policy.json`](aws-policy.json): it reads instance types, prices, the vCPU quota and
the Ubuntu image; it launches instances only when they are tagged `purpose=mqttd-netprobe`;
and it can terminate or delete only resources carrying that tag.

**Preferred: IAM Identity Center (short-lived credentials, nothing stored long-term).**

1. In the AWS console, open IAM Identity Center and enable it if it is not on yet.
2. Permission sets → Create permission set → Custom → paste `aws-policy.json` as the
   inline policy. Name it `mqttd-netprobe`; a session duration of 4 hours is plenty.
3. AWS accounts → your account → Assign users or groups → yourself → the
   `mqttd-netprobe` permission set.
4. Note the AWS access portal URL (Settings → Identity source).

Then, on your machine:

```bash
aws configure sso --profile mqttd-bench
```

It asks for the portal URL and the region (`eu-central-1`), opens the browser to sign in,
and lets you pick the account and the `mqttd-netprobe` role. Later sessions only need:

```bash
aws sso login --profile mqttd-bench
```

**Alternative: an IAM user with an access key** (long-lived; delete the key when done).
IAM → Users → Create user `mqttd-netprobe` (no console access) → attach `aws-policy.json`
as an inline policy → Security credentials → Create access key (CLI). Then:

```bash
aws configure --profile mqttd-bench
```

and type the key ID and secret at its prompts, with region `eu-central-1`.

Check either way:

```bash
aws sts get-caller-identity --profile mqttd-bench
```

## 3. Raise the vCPU quota if needed

New accounts often have a small on-demand limit. Two m6a.8xlarge need **64 vCPU** of
"Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances" (quota code
`L-1216C47A`) in eu-central-1. A later like-for-like broker run with HiveMQ's shape
(3 brokers of 32 vCPU plus load generators) needs about **256 vCPU**.

Check it:

```bash
aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A --region eu-central-1 --profile mqttd-bench
```

Raise it in the console (Service Quotas → Amazon EC2 → the quota above → Request increase
at account level), or ask for 256 now so the broker run needs no second request. Increases
are usually granted within minutes to a day. The policy above does not include the
request itself (`servicequotas:RequestServiceQuotaIncrease`), on purpose.

The account also needs its **default VPC** in eu-central-1 (every account has one unless it
was deleted; VPC → Your VPCs shows it).

## 4. Run the probe

The quote first, with nothing created:

```bash
AWS_PROFILE=mqttd-bench PROVIDER=aws PRICE_ONLY=1 bench/scale/netprobe/run.sh
```

Then the run (it asks before creating anything when run in a terminal):

```bash
AWS_PROFILE=mqttd-bench PROVIDER=aws bench/scale/netprobe/run.sh
```

Variants:

- `TYPE=m6a.4xlarge`: another size, to see whether the limits scale with it (on AWS they
  are documented to: network bandwidth and the packet-per-second allowance grow with the
  instance).
- `PLACEMENT=cluster`: both instances in a cluster placement group, EC2's lowest-latency
  placement. Default `none` is what an ordinary deployment gets.
- `TESTS="rr" RR_FLOWS="1000 10000 20000"`: only the connection-count test.

Cost: two m6a.8xlarge on demand, billed per second, for a run of about 10 minutes, plus two
8 GB gp3 volumes for the same time. The script prints the hourly price from the AWS pricing
API before it creates anything. Traffic between the two instances on private addresses in
one availability zone is free.

## 5. Cleanup

The script terminates its instances and deletes its security group, key pair and placement
group on exit, also after an error or Ctrl-C, and then audits by tag: it prints
`nothing leaked` or lists what is left. To check by hand at any time:

```bash
aws ec2 describe-instances --region eu-central-1 --profile mqttd-bench --filters Name=tag:purpose,Values=mqttd-netprobe Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].[InstanceId,State.Name]' --output text
```

## What it measures

The same three tests as on Hetzner (see `run.sh`): UDP's packet rate per VM, neper
tcp_rr's transactions/s, latency and retransmits at 1,000, 10,000 and 20,000 connections,
and TCP bulk bandwidth. The Hetzner results are in PR #1156: on every CCX type tried (8 to
48 vCPU), 10,000 connections with packets in flight retransmit ~28,000 segments/s with a
p99 of 870 ms, while 1,000 connections lose nothing. If AWS handles 20,000 connections
cleanly, the HiveMQ comparison runs there.
