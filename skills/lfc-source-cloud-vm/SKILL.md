---
name: lfc-source-cloud-vm
description: Provision databases on Azure, AWS and GCP cloud VMs for Lakeflow Connect. Covers native database packages or Podman containers with manual setup steps.
parent: lfc-source-setup
---

# Cloud VMs for Lakeflow Connect

When managed cloud databases don't exist for your engine (Oracle 19c, SQL Server Always On, DB2, Informix) or when you need full control over replication settings, provision a plain Ubuntu VM on your cloud and install the database yourself.

This skill covers **Azure VMs**, **AWS EC2** instances and **GCP Compute Engine** instances. All support:
1. **Native packages**: Install MySQL, PostgreSQL, SQL Server from apt/yum
2. **Podman containers**: Run database images in Podman

The workflow is manual so you control every step and understand the configuration.

## When to use

- Your engine has no managed service (Oracle 19c, DB2, Informix)
- You need a specific database version not in managed tiers
- You require unusual replication modes (Always On AGs, full control of CDC)
- You need to run multiple database versions on one host (Podman)
- You're testing before committing to production infrastructure

## Two installation models

| Model | Engines | Setup time | Best for |
|-------|---------|-----------|----------|
| **Native Packages** | MySQL, PostgreSQL, SQL Server | ~5–10 min | Testing, non-critical sources |
| **Podman Containers** | All (Oracle, DB2, Informix, MySQL, PostgreSQL, SQL Server) | ~10–20 min | Production-like, multi-engine hosts |

## Inputs to collect

| Input | Choices | Example | Notes |
|-------|---------|---------|-------|
| **Cloud** | `azure` or `gcp` | — | Determines VM API and firewall |
| **Region** | Azure: `eastus`, `centralus`, `westus`; GCP: `us-central1`, `us-east1` | `eastus` (Azure) / `us-central1` (GCP) | Close to Databricks workspace |
| **VM size** | Azure: `Standard_D4s_v5`, `Standard_B4ms`; GCP: `n1-standard-4`, `e2-standard-4` | `Standard_D4s_v5` (in a zone, for a Premium SSD v2 data disk) / `n1-standard-4` | 4 vCPU, 8–16 GB recommended |
| **Installation model** | `native` or `podman` | — | Based on engine availability |
| **Engine & version** | MySQL 8.0, PostgreSQL 16, SQL Server 2022, Oracle 23ai, etc. | — | Determines install script |

## Before creating: cost check and approval

Summarize what you're about to create:

```
About to create on <CLOUD>:
  - VM: {VM_NAME} ({SKU}, 4 vCPU, 8 GB)
    Region: {REGION}
    OS disk: 64 GB
    Data disk: 128 GB (for Podman)
    Estimated cost: ~$60–80/month
  - Public IP: {IP_NAME}
  - Firewall rules (allow {DB_PORT} from your IP / Databricks CIDR)
  - Tags: purpose=lfc-test, owner={prefix}, removal_date=YYYY-MM-DD

Proceed? (yes/no)
```

Wait for explicit confirmation before calling cloud APIs.

## Prerequisites

- **Tools**: see **lfc-source-setup** "Tools" (`check-tools.sh azure` or `gcp`)
- **Cloud CLI**: `az login` (Azure) or `gcloud auth login` (GCP)
- **SSH key**: `ssh-keygen -t rsa -b 4096 -f ~/.ssh/lfc_test_rsa -N ""`
- **Quotas**: Check regional vCPU limits
- **Network**: Ensure firewall rules can reach your Databricks workspace

**Identities are per resource.** Each resource records the identity it was created with (the create's `IDENTITY` line: Azure tenant and subscription, AWS profile and account, GCP account and project), and every later call for it passes that identity explicitly. Never rely on the CLI's current default: one login can reach several tenants, accounts and projects. Keep the values in your private env file, not in the repo.

## Create the VM

### Azure

See `references/azure-vm.md` for detailed Azure VM creation.

Quick command:

```bash
SUBSCRIPTION="<sub-id>"
RESOURCE_GROUP="<resource-group>"     # or the user's own group: they are free to choose
LOCATION="eastus"
ZONE="1"                        # Premium SSD v2 attaches only to a VM in an availability zone
VM_NAME="jane-doe-mysql-vm-eastus-$(date +%s)"
VM_SIZE="Standard_D4s_v5"

# every az call names its subscription: no az account set (it changes the default for every later call)
az group create --subscription "$SUBSCRIPTION" --name "$RESOURCE_GROUP" --location "$LOCATION"

az vm create --subscription "$SUBSCRIPTION" \
  --resource-group "$RESOURCE_GROUP" \
  --name "$VM_NAME" \
  --image Ubuntu2204 \
  --size "$VM_SIZE" \
  --zone "$ZONE" \
  --admin-username azureuser \
  --ssh-key-values ~/.ssh/lfc_test_rsa.pub

# the data disk: Premium SSD v2, 3,000 IOPS and 125 MB/s included at any size (a 128 GiB Premium SSD v1 is a 500-IOPS P10)
az disk create --subscription "$SUBSCRIPTION" --resource-group "$RESOURCE_GROUP" --name "${VM_NAME}-data" --location "$LOCATION" --zone "$ZONE" \
  --sku PremiumV2_LRS --size-gb 128 --disk-iops-read-write 3000 --disk-mbps-read-write 125
az vm disk attach --subscription "$SUBSCRIPTION" --resource-group "$RESOURCE_GROUP" --vm-name "$VM_NAME" --name "${VM_NAME}-data" \
  --lun 0 --caching None          # LUN 0: /dev/disk/azure/scsi1/lun0; v2 takes no host caching
```

**The data disk is Premium SSD v2** by default: its IOPS and throughput are set apart from its size, so a 128 GiB disk gets the 3,000 IOPS / 125 MB/s an AWS gp3 has, with no burst credits to mislead a short test. Premium SSD v1 at the same size (a P10) has 500 IOPS: the TPC SF 1 builds took up to 5.8 times as long on it ([disk-io-limits.md](references/disk-io-limits.md)). Raise IOPS or MB/s later without detaching (`az disk update --disk-iops-read-write 6000`). If `az vm disk attach` fails with `InvalidApiVersionParameter`, attach through `az rest` as [disk-setup.md](references/disk-setup.md) shows. A VM created without a zone can't take a v2 disk; it keeps Premium SSD v1, and a P30 (1 TiB, 5,000 IOPS) is the v1 size that doesn't stall a load.

### AWS

See `references/aws-vm.md` for EC2 creation: default VPC, a shared `lfc-test-<n>` security group, a separate gp3 data volume (its stable path is `/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_<volume-id-without-dash>`), Podman.

### GCP

See `references/gcp-vm.md` for detailed GCP Compute Engine creation.

Quick command:

```bash
PROJECT="my-project"
ZONE="us-central1-a"
VM_NAME="jane-doe-mysql-vm-uscentral1-$(date +%s)"
VM_TYPE="n1-standard-4"

gcloud compute instances create "$VM_NAME" \
  --zone="$ZONE" \
  --machine-type="$VM_TYPE" \
  --image-family=ubuntu-2404-lts \
  --image-project=ubuntu-os-cloud \
  --create-disk name="${VM_NAME}-data",device-name=data,size=128GB,type=pd-ssd   # /dev/disk/by-id/google-data
```

## Links to what you created

Every cloud resource created, or suggested for review, gets a clickable link, from the scripts and in replies. After the VM is up, and whenever someone asks what's running:

```bash
SUBSCRIPTION=<subscription_id> AWS_PROFILE=<profile> AWS_REGION=<region> GCP_ACCOUNT=<account> GCP_PROJECT=<project> \
  bash skills/lfc-source-cloud-vm/scripts/cloud-links.sh <vm-public-ip> [--resource-id <storage account id>]
```

Give the identity the VM was created with, for each cloud it may be in (a cloud with none is skipped, and the line says which flag to give). It prints one `LINK <what>: <url>` line per resource, and one `IDENTITY` line where it finds the VM (the record to keep):

| Cloud | Resources | Link form |
|---|---|---|
| Azure | VM, NIC, network security group (on the NIC or its subnet), public IP, OS and data disks; any other resource with `--resource-id` (the master snapshot's storage account) | `https://portal.azure.com/#@<tenant>/resource<resource id>` |
| AWS | instance, security groups, EBS volumes (`AWS_REGION` or `--region`) | `https://<region>.console.aws.amazon.com/ec2/home?region=<region>#InstanceDetails:instanceId=<id>` (`#SecurityGroup:groupId=`, `#VolumeDetails:volumeId=`) |
| GCP | instance, disks, VPC firewall rules | `https://console.cloud.google.com/compute/instancesDetail/zones/<zone>/instances/<name>?project=<project>` |

On Azure it searches `SUBSCRIPTION` (or `--subscription`), then, with none given, every enabled subscription by id; it never uses the CLI's current one, and it ignores a default resource group set in `az config` (that scoped every list to one group and hid the VM). Tested on Azure (2026-10-21, and read-only 2026-10-28 with the CLI's default in another tenant); the AWS and GCP parts haven't run against a VM yet.

## Install the database

### Native packages

SSH into the VM and install:

```bash
# Get SSH command from cloud console or:
# Azure: az vm list-ip-addresses ...
# GCP: gcloud compute instances describe ...

ssh -i ~/.ssh/lfc_test_rsa azureuser@<VM_IP>

# Then see references/ for engine-specific install:
# references/install-native.md for MySQL, PostgreSQL, SQL Server
```

### Podman containers

For complex engines or multiple database versions:

1. **Format the data disk as XFS first**, mounted at `/var/lib/containers`: run `scripts/disk-setup.sh` as `references/disk-setup.md` shows right after the VM is created. It finds the disk by its stable path (not `/dev/sdb`, which on Azure is usually the temporary disk), never wipes a disk that already has a filesystem, and checks for XFS with `ftype=1`. Don't install Podman until it prints `OK`.
2. Install Podman and run the containers with `sudo` (rootful), so images and volumes are stored on the XFS disk: see `references/install-podman.md` for container examples (Oracle, DB2, Informix, MySQL, PostgreSQL, SQL Server).
3. **Large test databases:** don't populate one per person. Pull a shared master from object storage once, then clone it per test with XFS reflinks (instant), and throw the clone away or save it back: see `references/master-snapshots.md`. To leave the master in object storage and branch it in place (no full pull, a small local cache), use JuiceFS: `references/juicefs-branches.md`. Per-engine owners, clean stops and measured times: `references/engine-masters.md`. For several people on one VM, each with their own database from a master (one running at a time, parked ones in object storage): `scripts/slot.sh`, `references/shared-vm-slots.md` (engines: postgres, mysql, sqlserver, oracle, mariadb, db2, informix, cockroachdb, yugabytedb; masters for the first three).

### More SQL Server containers for parallel tests

[scripts/add-mssql-container.sh](scripts/add-mssql-container.sh) `<port>` starts one more SQL Server container on the VM (2 CPUs, 4 GB, SQL Server Agent on), creates the test login from a private password file through standard input, creates its Unity Catalog connection and checks it with `remote_query`. Run half as many containers as the VM has vCPUs, each on its own port in the firewall range; lfc-schema-change-test `scripts/parallel.sh` then spreads test runs over them. The new container's sa password is generated on the VM and stays there. Tested by hand, step by step, on 2026-10-23 (the script itself not yet end to end).

## Configure for Lakeflow Connect

After the database is running, see the parent skill **lfc-source-setup** for engine-specific configuration:
- User creation and grants (via shared `references/engine-*.md`)
- CDC / Change Tracking setup
- Replication slot / publication creation

Quick commands in `references/configure-lfc.md`.

## Networking for Databricks serverless

> **Firewall rules:** use **lfc-source-firewall** to add your laptop IP and the workspace's serverless egress IPs (and classic compute egress, when the pipeline uses classic compute) to this database. It covers rule limits and keeps the rules in sync.

Databricks serverless must reach the VM's database via public IP.

### Azure: attach the VM to the shared NSG

Don't create per-VM rules. Attach the VM's NIC (or its subnet) to the region's shared NSG, whose rules already allow your laptop and the workspace's serverless egress on the Podman port range `30000-30063` (and SSH from the laptop only):

```bash
az network nsg list -g "$RESOURCE_GROUP" --query "[?location=='$LOCATION'].name" -o tsv     # the shared NSG for this region, if any
az network nic update -g "$RESOURCE_GROUP" -n <vm-nic> --network-security-group <shared-nsg>
```

If there's no shared NSG in the VM's region yet, or its rules are out of date (new laptop IP, new serverless ranges), set it up with **lfc-source-firewall** ([shared NSG](../lfc-source-firewall/references/azure.md#shared-nsg-vms-and-the-shared-vnet-set-up-once-reuse-everywhere)). A native install on the engine's own port needs that port in the NSG's rules too.

### GCP: VPC firewall rule on the VM's tag

Use **lfc-source-firewall** ([GCP steps](../lfc-source-firewall/references/gcp.md)) to create the `lfc-*` rules for the VM's network tag; every VM with that tag reuses them.

## Register the UC connection

### Create connection JSON (private file)

```bash
umask 077
cat > conn.json <<EOF
{"name": "<firstname><lastname>-mysql-vm", "connection_type": "MYSQL",
 "options": {"host": "<VM_PUBLIC_IP>", "port": "<published-port>", "user": "lfc_user", "password": "$(cat lfc_user.pw)", "trustServerCertificate": "true"}}
EOF
```

### Create in Databricks

```bash
databricks connections create --json @conn.json --profile <PROFILE>
```

### Clean up

```bash
rm conn.json /tmp/podman-secrets/* 2>/dev/null
```

## Verify connection

Test the UC connection from Databricks SQL:

```sql
SELECT * FROM <connection_name>.information_schema.tables LIMIT 5;
```

## Lifecycle: stop, start, delete

**Stop:**
```bash
# Azure
az vm deallocate --resource-group "$RESOURCE_GROUP" --name "$VM_NAME"

# GCP
gcloud compute instances stop "$VM_NAME" --zone="$ZONE"
```

**Start:**
```bash
# Azure
az vm start --resource-group "$RESOURCE_GROUP" --name "$VM_NAME"

# GCP
gcloud compute instances start "$VM_NAME" --zone="$ZONE"
```

**Delete:**
```bash
# Azure
az vm delete --resource-group "$RESOURCE_GROUP" --name "$VM_NAME" --yes

# GCP
gcloud compute instances delete "$VM_NAME" --zone="$ZONE" --quiet
```

For more details, see `references/lifecycle.md`.

## Long-running steps on the VM

Builds, fills, uploads and benchmarks take minutes to hours. Run each as a systemd unit, not inside an SSH session, so a dropped connection doesn't kill it: `ssh <vm> 'sudo systemd-run --unit=lfc-<job> --collect bash /tmp/<job>.sh'`, then check with new, short SSH calls (`systemctl is-active lfc-<job>`, `journalctl -u lfc-<job>`). See **watch-progress**.

## Top gotchas

1. **SSH connection refused**: Verify public IP, SSH key, and firewall allows port 22 from your IP. See `references/troubleshooting.md`.

2. **Database port not reachable from Databricks**: Verify database is running, bind address is `0.0.0.0`, and firewall/NSG allows workspace serverless CIDR. See `references/troubleshooting.md`.

3. **Podman image pull timeout**: Large images (Oracle ~5 GB) may timeout on slow networks. Increase timeout or use a mirror.

4. **Podman data not on XFS**: format the data disk by its stable path (`/dev/disk/azure/scsi1/lun0`, `/dev/disk/by-id/google-data`), never `/dev/sdb`, and check `findmnt -no FSTYPE /var/lib/containers` says `xfs` before installing Podman. See `references/disk-setup.md`.

5. **Disk filled by change logs**: a large load writes as much again to MySQL's binary logs or Postgres' WAL, which CDC reads and so keeps. Size the data disk for the data plus its logs, watch `df -h /var/lib/containers`, and purge logs only when nothing reads them.

6. **CDC/Change Tracking not capturing**: Verify replication mode enabled in database config and LFC user has required permissions. See parent skill `references/engine-*.md`.

7. **Finding a database's containers by name matches the wrong one**: `podman ps --filter name=<x>` is an unanchored regex, so `name=db2` also matches `build-crdb25-tpc-sf1`. Find containers by label: every container these scripts start has `lfc.engine` and `lfc.purpose` (`slot`, `tpc-master`, `branch-test`), masters also `lfc.master`, slots `lfc-slot.owner` and `lfc-slot.name`: `podman ps -a --filter label=lfc.engine=db2`. By name, anchor it: `--filter 'name=^build-db2'`, or `--format '{{.Names}}' | grep -x '<name>'`. Before removing a container, read its labels (`podman inspect -f '{{.Config.Labels}}' <name>`).

8. **Db2 `SQL0293N` on `CREATE DATABASE`**: the data disk has 4096-byte logical sectors (Azure Premium SSD v2's default). `db2set DB2_4K_DEVICE_SUPPORT=ON` before creating the database (`slot.sh` and `tpc-master.sh` do), or a disk made with `--logical-sector-size 512`. See `references/disk-setup.md`, "Logical sector size".

## Additional resources

- **Parent skill**: [lfc-source-setup](../lfc-source-setup/SKILL.md) — engine-specific configuration
- **VM creation**: [azure-vm.md](references/azure-vm.md), [aws-vm.md](references/aws-vm.md), [gcp-vm.md](references/gcp-vm.md)
- **Database install**: [install-native.md](references/install-native.md), [install-podman.md](references/install-podman.md)
- **Reusable master database**: [master-snapshots.md](references/master-snapshots.md) (rclone pull + XFS reflink clones), [juicefs-branches.md](references/juicefs-branches.md) (JuiceFS branches in object storage), [engine-masters.md](references/engine-masters.md) (per engine, with `scripts/branch-test.sh`), [shared-vm-slots.md](references/shared-vm-slots.md) (one database per person on a shared VM, `scripts/slot.sh`, `scripts/slot-ssh.sh`; a new VM set up by `scripts/slot-vm-setup.sh`)
- **Disk sizing**: [disk-io-limits.md](references/disk-io-limits.md) — each cloud's disk and VM I/O limits, how to tell a load is disk-bound, and what lifts it (an Azure 128 GiB P10 is a 500-IOPS disk)
- **Configuration**: [configure-lfc.md](references/configure-lfc.md)
- **Lifecycle**: [lifecycle.md](references/lifecycle.md)
- **Troubleshooting**: [troubleshooting.md](references/troubleshooting.md)
