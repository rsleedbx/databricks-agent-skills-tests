# Classic compute egress: finding it, and the fixes

- **The user knows it:** ask for it, and add it as `CLASSIC='<ip>/32'` (or the NAT range) to `fw.env`.
- **Azure workspace with a Databricks-managed VNet, in a subscription you can read:** the egress is the NAT gateway's public IP in the workspace's managed resource group:

  ```bash
  eval "$(az databricks workspace show -g <workspace-rg> -n <workspace-name> -o json \
    | yq -p json -o shell '{"MRG": ((.managedResourceGroupId | split("/") | .[-1]) // "")}')"
  eval "$(az network public-ip list -g "$MRG" -o json \
    | yq -p json -o shell '{"CLASSIC": ([.[].ipAddress | select(. != null) | . + "/32"] | join(" "))}' | tee -a fw.env)"
  ```

- **VNet-injected (Azure) or customer-managed VPC (AWS, GCP):** it's the customer's NAT; ask the user.
- **Not in an account you can read:** ask Databricks. [`scripts/classic-egress.sh`](../scripts/classic-egress.sh) has two methods, and both print a `CLASSIC=` line for `fw.env`:
  1. **The audit log, first.** API calls from classic cluster drivers (user agent `Databricks-Service/driver`) record the public IP they came from, so `--warehouse <id>` reads every NAT IP used in the last 7 days (`--days N`) in seconds, without starting anything. It needs `SELECT` on `system.access.audit` and a SQL warehouse. It finds nothing on a workspace with no classic activity in the window. Private addresses (internal paths) are dropped.
  2. **A one-off single-node notebook job**, when the audit log isn't readable or is empty. The job reports the address it leaves from, then the notebook is deleted. It takes 5-10 minutes while the cluster starts. `--runs 3` keeps every distinct IP, since a cluster in another subnet can leave from a second NAT IP.
  ```bash
  skills/lfc-source-firewall/scripts/classic-egress.sh <PROFILE> --warehouse <WAREHOUSE_ID> | tail -1 | tee -a fw.env   # CLASSIC='<ip>/32'
  skills/lfc-source-firewall/scripts/classic-egress.sh <PROFILE> | tail -1 | tee -a fw.env                               # the job
  ```
  Both were tested on an AWS classic workspace and gave the same IP; with it allowed, the gateway connected.
- **Can't determine it:** add the laptop and serverless IPs anyway, and **warn the user** that any part of the pipeline that runs on classic compute (for example a standard CDC gateway) may not be able to reach the database until its egress IP is added. Serverless parts will still work.

**When a pipeline on classic can't reach the source.** In the event log it shows as `Extractor STARTED`, then about a minute later `Extractor FAILED` with `INGESTION_GATEWAY_CONNECTION_ERROR.INCORRECT_SOURCE_HOSTNAME_OR_PORT_SPECIFIED_FAILURE`. `bin/compute_report.py` reads it as `ran_on classic`, `started yes`, `reached_source no`. There are two fixes, and the records keep them apart (lfc-test-results, `compute`):

1. **Allow the classic egress IP**, found with one of the ways above, then rerun with the same setting (`serverless` unset). If the rerun reaches the source, record it as such: no `switched_from`, plus `notes` saying the classic IP was added.
2. **Switch to serverless**, which the test matrix does by default (`LFC_SERVERLESS=true`): record `switched_from: unset`, `switch_reason: firewall`.

Use 1 to test what the default does on a classic workspace, and 2 when only the data path matters.

## Classic compute that never starts

A classic standard gateway on `m5.large` or `m6g.large` stayed `PENDING` at "Finding instances" for 40 minutes or more, and creating an instance pool returned "You are not authorized to create instance pools" (tested 2026-10-25, SQL Server 2022 on a classic workspace). Ask a workspace admin for instance-pool permission and for a node type the workspace has capacity for, or run the gateway on serverless.
