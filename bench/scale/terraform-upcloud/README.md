# UpCloud scale rig

Select this module with `CLOUD=upcloud`. The default remains Hetzner; results
from the two platforms must not be combined into one scaling curve. UpCloud's
`maxiops` disks are network-replicated, not the local NVMe required by ADR 0048.
The inventory records provider, storage tier, plans and vCPU counts. Matching
core counts do not establish matching CPU guarantees or driver headroom.

## Before provisioning

- Export `UPCLOUD_TOKEN` (Bearer API token) and an explicit `MQTTD_VERSION`.
  Never put tokens in tfvars, scripts or committed files.
- Review the cost, zone availability, strict broker anti-affinity and available
  account quotas. The local guards cover configured CPU/IPv4 budgets only, not
  current usage, RAM or storage limits. Unknown plans are refused until their
  core counts are added to `quota.tf` from the provider catalog.
- Defaults: `de-fra1`, four-vCPU/eight-GB brokers and drivers, 100-GB maxiops boot
  disks, two drivers (one for smoke). These are not Hetzner smoke plan names.
- Set `SSH_KEY` for a non-default key; narrow `TF_VAR_admin_cidr` to an IPv4
  admin CIDR where practical. Public inbound allows SSH/ICMP and explicitly
  drops everything else. UpCloud's firewall does not filter private SDN.
- `BROKER_NIC_SPREAD` is unsupported and rejected before provisioning.
- Validate the workload shape before paying. Lane E uses the smallest reported
  driver vCPU count as its default container ceiling, not a fixed eight.
  An explicit ceiling override is an experimental oversubscription choice, not
  evidence of driver capacity. Lane B's five-publisher/three-subscriber default
  also needs adjustment for four-core drivers (or choose larger drivers).
- Only one run at a time per provider/state directory; do not overlap applies,
  destroys or recovery commands. Do not reuse a completed run directory across
  providers, since resume markers are keyed by node count.

Example (creates paid resources; run only with an approved budget):

```sh
export CLOUD=upcloud
export MQTTD_VERSION=<release-without-v>
# Supply UPCLOUD_TOKEN securely in the environment.
bench/scale/run.sh smoke
```

Run traps destroy the selected provider's resources, unless `KEEP_INFRA=1`.
Strict anti-affinity covers brokers only; separate driver VMs are not a guarantee
of separate physical driver/broker hosts.

## Recovery

```sh
CLOUD=upcloud bench/scale/teardown.sh
```

This initializes the selected module and destroys resources in its local state.
Keep the state and the same operator environment/key available. It does **not**
audit the account for resources missing from state. If state is missing, or
`--force` is requested, teardown fails explicitly without running any Hetzner
commands. There is no UpCloud label-based force sweeper in this PR.

After a lost state, interrupted apply/destroy, or failed teardown, inspect the
UpCloud console for this run's servers, attached/unattached storage, private
network and server group. Verify ownership before deleting or importing them;
never interpret an empty local state as proof of zero account-wide charges.

## Offline checks (no provisioning)

```sh
python3 bench/scale/test-cloud.py
python3 bench/scale/test-upcloud-quota.py  # requires tofu or terraform
# Provider download only; no cloud resources:
tofu -chdir=bench/scale/terraform-upcloud init -backend=false
tofu -chdir=bench/scale/terraform-upcloud fmt -check -recursive
tofu -chdir=bench/scale/terraform-upcloud validate
```

Provider selection/recovery and driver-budget regression tests run in CI using
stub CLIs. Quota tests evaluate the real HCL in an isolated module containing
only the built-in `terraform_data` resource, with no provider or credentials.
