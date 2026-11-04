---
name: lfc-source-firewall
description: "Open a Lakeflow Connect source database's firewall to the caller's laptop IP and the Databricks workspace's serverless egress IPs (and classic compute egress when known), on Azure, AWS, or GCP. Use after creating a source with an lfc-source-* skill, or when a Lakeflow Connect pipeline or remote_query can't reach the database."
parent: lfc-source-setup
---

# Firewall rules for a Lakeflow Connect source

A source database has to accept connections from two places: **your laptop**, so you can set it up, and **the Databricks workspace**, so Lakeflow Connect can read it. This skill finds both sets of IPs and adds them to the database's firewall, nothing more. Use it after any `lfc-source-*` skill creates a database, or to repair one that pipelines can't reach.

## What gets allowed

| Source of traffic | How to find it | When it's needed |
|---|---|---|
| Your laptop | `curl -fsS https://api.ipify.org` (fallback `https://ifconfig.me/ip`), as a `/32` | Setting up the database, seeding, running test DML |
| Workspace **serverless** egress | Databricks' public [ip-ranges.json](https://www.databricks.com/networking/v1/ip-ranges.json), outbound IPv4 for the workspace's cloud and region | Query-based pipelines, integrated CDC on serverless, `remote_query`, serverless gateways |
| Workspace **classic** compute egress | Not published. It's the workspace's NAT / egress IP, set per workspace (see [Classic compute egress](#classic-compute-egress)) | Standard CDC gateways and integrated CDC that run on classic compute: `serverless: false`, or `serverless` unset on a workspace with classic (seen on Azure), and anything else running on classic clusters. The test prompts set `serverless: true` so this isn't needed |

Open only the database port, only to these addresses. Never open `0.0.0.0/0`.

## Which cloud: the workspace's first

Put the source in **the workspace's cloud and region** (Step 1 finds them as `CLOUD` and `REGION`). Lakeflow Connect's reads then stay in one region, so there's no cross-cloud or inter-region egress charge, and only that region's serverless ranges are needed, which fit every target below.

Rule limits per target (Azure NSG, Flexible Server, Azure SQL, RDS and Aurora, Cloud SQL, VPC firewall), and what fits one region or every region: [references/limits.md](references/limits.md).

Counts are from `ip-ranges.json` at the time of writing; recount with the commands below. For a source that serves **one workspace**, any target in its cloud and region works. For a source shared by workspaces in **many regions**, an Azure VM with an NSG can hold every serverless range; reads from other clouds or regions are billed as egress either way.

**On Azure, keep one shared NSG per region** and attach every test VM (and the subnets of the shared VNet) to it: the rules are synced once and every new VM reuses them, including all Podman engines on the VM's port range `30000-30063`. The NSG can't front a Flexible Server or Azure SQL that serverless must reach: those need public access and their own server firewall rules, synced from the same IP lists. See [references/azure.md](references/azure.md).

**An NSG that can't be the shared one** (a SQL Managed Instance subnet's: MI adds its own rules to it) gets the shared NSG's sources for its one port: `RG=<resource-group> bash scripts/nsg-copy.sh <shared-nsg> <subnet-nsg> 3342 [--apply]`. It copies every inbound Allow rule's source list (serverless, VPN, workspace egress, known hosts) under the same name and priority, TCP to that port only; rerun it after the shared NSG is synced.

**Azure SQL Database and Flexible Server** take start-end rules, capped at 128 (SQL Database), 500 (MySQL) and 500 (Postgres), and the shared NSG's every-region lists merge to about 1,200 ranges. `RG=<resource-group> python3 scripts/server-fw.py sql|pg|mysql <server> --nsg <shared-nsg> --regions <cloud>:<region>,... [--profiles <p>,...] [--cidrs <classic egress>] [--apply]` takes the shared NSG's other sources (VPN, office, known hosts), the serverless ranges of each named workspace's region, the extra CIDRs and this machine's IP, merges them, checks the cap, and syncs only its own `lfc-<n>` rules. Three regions came to about 60 rules. A Flexible Server adds a rule in about a minute, so put the test workspaces and this machine on first: `--skip . --profiles <test workspaces> --no-drop --apply` (`--skip .` leaves out the shared NSG's rules; `--no-drop` keeps `lfc-` rules outside the list instead of deleting them), then the full list later with `--no-drop`.

## Inputs

| Input | How to get it |
|---|---|
| Databricks CLI profile | Ask; never pick one (see **databricks-core**) |
| Target type and name | From the `lfc-source-*` skill that created it: Flexible Server / Azure SQL server / NSG / security group / Cloud SQL instance / VPC network |
| Database port | 5432 Postgres, 3306 MySQL, 1433 SQL Server, 1521 Oracle, 3342 SQL Managed Instance public endpoint |
| Azure resource group and NSG | The saved `AZ_RESOURCE_GROUP` (**lfc-test-settings**, asked once: an existing group the user picked) and its shared NSG for the VM's region (`az network nsg list --subscription "$AZ_SUBSCRIPTION" -g "$AZ_RESOURCE_GROUP"`). Never create a resource group; ask before creating an NSG |
| Scope | This workspace's region (default), or every serverless region for a shared source |
| Does the pipeline use classic compute? | A standard CDC gateway or integrated CDC with `serverless: false`, or unset on a workspace with classic compute; or classic query-based (Beta). Read `update.config.serverless` from `pipelines get-update` |
| Classic egress IPs | Only if classic compute is used: from the user, or found as below |

Shared resource groups can hit their quotas. If creation fails with a quota or limit error in the shared group, ask for another resource group rather than deleting someone else's resources.

**Identities are per resource.** Each resource records the identity it was created with (the create's `IDENTITY` line: Azure tenant and subscription, AWS profile and account, GCP account and project), and every later call for it passes that identity explicitly. Never rely on the CLI's current default: one login can reach several tenants, accounts and projects. Keep the values in your private env file, not in the repo.

## Shell conventions

These follow **shell-json-scripting**. The agent's shell doesn't keep variables between commands, so:

- **Every value lives in `fw.env`.** Start every command block with `set -a; . ./fw.env`. `set -a` exports every variable assigned from then on, including the ones `eval` sets, so DuckDB can read them. Don't `set +a` mid-block.
- **CLI JSON → shell variables** with yq, keeping the `// ""` (for a missing value yq prints `null`):

  ```bash
  eval "$(<cli> ... -o json | yq -p json -o shell '{"NAME": (.path.to.value // "")}' | tee -a fw.env)"
  ```

- **Lists are space-separated** variables (`SERVERLESS`, `CLASSIC`, `WANT`, `HAVE`, `MISSING`): CIDRs never contain spaces, and `for c in $MISSING` just works.
- **Use the simplest tool for each step.** A filter or reshape of one JSON input is a yq expression. A loop or membership test over a short list is plain shell. Only the steps that need IP-range math, joins across inputs, or ranking are DuckDB SQL: coverage (`missing.sql`, and the per-cloud cover queries), the rule plan (`plan.sql`), and the AWS group pick.
- **DuckDB steps** are `.sql` files that read inputs with `getvariable('NAME')` and JSON files with `read_json`, and return one row. `q.sh` fills every `getvariable` from the env var of the same name and prints the row as shell assignments: `eval "$($Q $FW/<step>.sql | tee -a fw.env)"`. `q.py` does the same when the caller is Python (`uv run q.py`), so the SQL files are shared.
- **Never put passwords in `fw.env`:** it holds IPs and resource names only.

The DuckDB steps are files in [`scripts/`](scripts/): [`lib.sql`](scripts/lib.sql) (the IPv4 and port macros every step uses), [`collapse.sql`](scripts/collapse.sql), [`missing.sql`](scripts/missing.sql), [`plan.sql`](scripts/plan.sql), and the per-cloud [`sg_pick.sql`](scripts/sg_pick.sql), [`nsg_cover.sql`](scripts/nsg_cover.sql), [`fw_cover.sql`](scripts/fw_cover.sql). They run with **shell-json-scripting**'s runner, `q.sh` (or `q.py` from Python), which reads `lib.sql` from the query's directory:

```bash
Q=skills/shell-json-scripting/scripts/q.sh; FW=skills/lfc-source-firewall/scripts     # adjust to where the skills are
```

## Step 1: collect the IPs

```bash
set -a; umask 077; : > fw.env
# Laptop
eval "$({ curl -fsS --max-time 5 'https://api.ipify.org?format=json' || curl -fsS --max-time 5 https://ifconfig.me/all.json; } \
  | yq -p json -o shell '{"MYIP": (.ip // .ip_addr)}' | tee -a fw.env)"

# Workspace cloud and region: global_metastore_id is <cloud>:<region>:<id>
eval "$(databricks metastores summary --profile <PROFILE> -o json \
  | yq -p json -o shell '(.global_metastore_id | split(":")) as $g | {"CLOUD": $g[0], "GREGION": $g[1], "REGION": .region}' \
  | tee -a fw.env)"
[ "$GREGION" = "$REGION" ] || echo "WARN: metastore region mismatch ($GREGION vs $REGION): check the workspace region by hand"

# Serverless egress (outbound, IPv4). SCOPE=all takes every cloud and region, for a source shared by many workspaces.
echo "SCOPE=region" >> fw.env
set -a; . ./fw.env
eval "$(curl -fsSL https://www.databricks.com/networking/v1/ip-ranges.json | yq -p json -o shell '{"SERVERLESS": ([.prefixes[]
  | select(.type == "outbound" and (strenv(SCOPE) == "all" or (.platform == strenv(CLOUD) and .region == strenv(REGION))))
  | .ipv4Prefixes[]] | unique | join(" "))}' | tee -a fw.env)"
eval "$($Q $FW/collapse.sql | tee -a fw.env)"     # drop ranges inside other ranges: they overlap across regions
echo "laptop $MYIP, serverless: $(echo $SERVERLESS | wc -w) CIDRs"
```

`collapse.sql` matters for `SCOPE=all`: the file has ranges that sit inside others from another region (763 → 760 when this was written), and an Azure NSG rule rejects overlapping prefixes (`OverlappingSubnetsNotPermittedInSecurityRule`). It's harmless for one region.

If `SERVERLESS` is empty, the region isn't in the file: stop and tell the user, don't guess.

## Classic compute egress

Classic clusters leave the workspace through its own NAT / egress IP, which isn't in `ip-ranges.json`. It differs per workspace and is often set by hand when the workspace is created.

Finding the classic compute egress IP (the user, the Azure NAT gateway, `scripts/classic-egress.sh`), what to do when it can't be found, and the fixes for a pipeline on classic that can't reach the source: [references/classic-egress.md](references/classic-egress.md).

Private paths instead of allowlisting (the Azure private network gateway and AWS PrivateLink; not tested yet): [references/private-paths.md](references/private-paths.md).

Then record the full wanted list:

```bash
set -a; . ./fw.env
echo "WANT='$MYIP/32 $SERVERLESS ${CLASSIC:-}'" >> fw.env
```

## Step 2: reuse what's already there, then add only what's missing

Shared test accounts usually already have security groups, firewall rules, or NSGs that allow a workspace's serverless ranges and some laptops. **Search for them before creating anything**, especially on AWS, where rule and security-group limits are tight. Each cloud reference shows how to set `HAVE` (the CIDRs or `start-end` ranges existing rules already allow on the database port); then:

```bash
set -a; . ./fw.env
[ -n "${WANT:-}" ] || echo "STOP: WANT is empty; run the last block of Step 1 first"
eval "$($Q $FW/missing.sql | tee -a fw.env)"
echo "missing: $(echo $MISSING | wc -w)"      # 0 means existing rules already cover everything
```

An empty `WANT` (Step 1's last block skipped) also gives 0 missing, so check it before trusting a 0.

A CIDR counts as covered when it falls inside an existing allowed range. Rules open to everyone (`0.0.0.0/0`, `*`, `Internet`) never count: don't rely on them, and point them out to the user.

**Leave your own `lfc-` rules out of `HAVE`.** They're recomputed from `MISSING` on every run; counting them as existing coverage would make the next step remove them.

## Step 3: apply the rules

Add only `MISSING`. Every rule you create gets an `lfc-` name (or description, on AWS) so it can be found, updated, and removed without touching anyone else's. Where a rule holds one CIDR, `rule_name` in `lib.sql` gives a stable name from its purpose and value (`lfc-myip-203-0-113-7-32`, `lfc-serverless-198-51-100-0-28`), so reruns are idempotent. Compare against your existing `lfc-` rules (`LFC_RULES`, as `<name>` or `<rule-id>=<name>`) to get what to add and what to drop:

```bash
set -a; . ./fw.env
eval "$($Q $FW/plan.sql | tee -a fw.env)"
```

`TO_ADD_SPEC` gives each new rule as `<cidr>,<name>,<first-ip>,<last-ip>`, for clouds that want names or IP ranges: `for s in $TO_ADD_SPEC; do IFS=, read -r cidr name first last <<< "$s"; ...; done`.

Then follow the cloud reference:

| Target | Commands |
|---|---|
| Azure Flexible Server (Postgres, MySQL), Azure SQL, NSG on a VM | [references/azure.md](references/azure.md) |
| AWS RDS and Aurora security groups | [references/aws.md](references/aws.md) |
| GCP Cloud SQL, VPC firewall on a VM | [references/gcp.md](references/gcp.md) |

Show the user the full list of rules you'll add or remove (counts per group, the laptop IP, any classic IPs) and wait for a "yes" before applying.

## Step 4: verify

- **From the laptop:** connect with the SQL client (`psql`, `mysql`, `sqlcmd`).
- **From Databricks serverless:** run a read-only `remote_query` through the UC connection:

  ```bash
  cat > ping.sql <<'EOF'
  SELECT * FROM remote_query('<connection>', database => '<database>', query => "SELECT 1 AS ok")
  EOF
  databricks experimental aitools tools query --file ping.sql --profile <PROFILE> -o csv
  ```

  (MySQL: leave out `database =>`.) A timeout here means serverless still can't reach the database: recheck the region, the port, and that the rules applied.
- **From classic compute:** only the pipeline itself proves it. If the classic egress IP wasn't added, say so in the result.

## Gotchas

- **The laptop's IP changes, even without moving.** A corporate VPN hands out a different egress IP from a pool from one session to the next: seen on the same day, `203.0.113.82` then `203.0.113.16`, and SSH to the AWS VM timed out until the rule moved. So **check the laptop IP at the start of every session**, before the first SSH or SQL connection: rerun Step 1, and if it changed, add the new IP and remove the old one in each `lfc-myip-*` rule, keeping the rule's name or description (on AWS, authorize the new CIDR with the same description, then revoke the old one). A timeout on SSH or the database port, where it worked before, usually means this.

More gotchas (VPN egress, a VPN that doesn't reach every cloud, checking whether packets arrive, `ip-ranges.json` changes, Azure service tags, GCP authorized networks, AWS limits, NCC): [references/gotchas.md](references/gotchas.md).
