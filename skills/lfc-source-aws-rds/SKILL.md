---
name: lfc-source-aws-rds
description: "Provision AWS RDS databases (MySQL, PostgreSQL, SQL Server, Oracle) for Databricks LakeFlow Connect via AWS CLI"
parent: lfc-source-setup
---

# AWS RDS Source for LakeFlow Connect

Provision a source database on AWS RDS for Lakeflow Connect (LFC) using the AWS CLI. Supports MySQL, PostgreSQL, SQL Server SE, and Oracle SE2. All operations use `aws rds`, `aws ec2`, and `databricks` CLIs.

## When to use

- **LFC ingest from AWS RDS**: MySQL, PostgreSQL, SQL Server SE (Standard Edition), or Oracle SE2
- **Production-grade networking**: Databricks serverless must reach the DB via public IP + security group
- **Shared infrastructure**: Subnet groups, security groups, and parameter groups are created once per VPC and reused
- **Billable resources**: RDS instances and data transfer charges apply

## Supported engines

| Engine | Port | Admin User | CDC Mode | License |
|--------|------|------------|----------|---------|
| PostgreSQL | 5432 | `pgadmin` | Logical Replication | Open source |
| MySQL | 3306 | `mysqladmin` | Binlog | Open source |
| SQL Server SE | 1433 | `sqladmin` | Change Tracking | License-included |
| Oracle SE2 | 1521 | `admin` | LogMiner | License-included |

## Inputs to collect

**Saved settings first** (**lfc-test-settings**): `AWS_PROFILE`, `AWS_REGION`, `AWS_VPC` (unset: the default VPC) and `OWNER` come from `settings.sh env`; `settings.sh require AWS_PROFILE AWS_REGION OWNER` lists only what to ask. Before creating, collect the rest:

| Input | Purpose | Example |
|-------|---------|---------|
| **engine** | Database engine | `postgres`, `mysql`, `sqlserver`, `oracle` |
| **version** | Engine version | `16` (postgres), `8.0` (mysql), `2022` (sqlserver) |
| **aws-region** | `AWS_REGION` (saved) | `us-east-1`, `eu-west-1` |
| **instance-type** | EC2 instance class | `db.t3.micro`, `db.t3.small` |
| **storage-gb** | Allocated storage | `20`, `50`, `100` |
| **user-prefix** | Prefix for resource names, from `OWNER` (saved) | `jane-doe` |

## Prerequisites

- **AWS CLI** configured: `aws sts get-caller-identity`
- **Default VPC**: Or custom VPC with subnet IDs known
- **Databricks CLI**: `databricks auth profiles` shows available profile

**Identities are per resource.** Each resource records the identity it was created with (the create's `IDENTITY` line: Azure tenant and subscription, AWS profile and account, GCP account and project), and every later call for it passes that identity explicitly. Never rely on the CLI's current default: one login can reach several tenants, accounts and projects. Keep the values in your private env file, not in the repo.

## Naming pattern

Resources use: `{arm}-{service}-{region}-{epoch}`

Examples:
- RDS instance: `jane-doe-pg-useast1-1778687039`
- Logical database: `jane_doe_pg_db_1778687039`
- Security groups, parameter groups, subnet groups: `lfc-test-{suffix}` (shared)

## Shared infrastructure

**Created once per VPC and reused:**

- **DB Subnet Group** (`lfc-test-vpc-*`): Contains 2+ subnets in different AZs
- **LFC Security Group** (`lfc-test-lfc-vpc-*`): Allows Databricks serverless egress CIDRs on the database port
- **User Security Group** (`lfc-test-user-vpc-*`): Allows your /24 CIDR on the database port
- **Parameter Groups** (`lfc-test-*-params`): CDC parameters applied at group level

**Not deleted on individual instance teardown.** Shared across all users in the VPC.

## Workflow

### 1. Check AWS context

```bash
aws sts get-caller-identity
aws ec2 describe-vpcs --filters "Name=isDefault,Values=true" --query 'Vpcs[0].VpcId'
```

### 2. Ensure shared infrastructure

**DB subnet group:** Create once per VPC if needed.

```bash
aws rds create-db-subnet-group \
  --db-subnet-group-name lfc-test-vpc-{vpc-id} \
  --db-subnet-group-description "LFC shared subnet group" \
  --subnet-ids subnet-a subnet-b
```

**Parameter group (PostgreSQL example):**

```bash
aws rds create-db-parameter-group \
  --db-parameter-group-name lfc-test-pg-params \
  --db-parameter-group-family postgres16 \
  --description "lfc-test-pg-params"

aws rds modify-db-parameter-group \
  --db-parameter-group-name lfc-test-pg-params \
  --parameters \
    ParameterName=rds.logical_replication,ParameterValue=1,ApplyMethod=immediate \
    ParameterName=max_replication_slots,ParameterValue=10,ApplyMethod=pending-reboot
```

**Security groups:** Create for LFC and user access.

```bash
aws ec2 create-security-group \
  --group-name lfc-test-lfc-vpc-{short} \
  --description "LFC shared SG for Databricks egress CIDRs" \
  --vpc-id {vpc-id}

# Add Databricks egress CIDR rules (example: 34.67.0.0/16)
aws ec2 authorize-security-group-ingress \
  --group-id sg-lfc \
  --protocol tcp --port 5432 --cidr 34.67.0.0/16
```

### 3. Create the RDS instance

Generate and store admin password safely:

```bash
umask 077
# letters and digits only: RDS refuses /, @ and " in a master password, which base64 output can contain
umask 077; mkdir -p ~/.lfc/pw; echo "Lfc1$(openssl rand -hex 12)" > ~/.lfc/pw/rds-admin.pw
export RDS_ADMIN_PASSWORD="$(cat ~/.lfc/pw/rds-admin.pw)"
```

Create the instance (PostgreSQL example):

```bash
INSTANCE_NAME="jane-doe-pg-useast1-$(date +%s)"
DB_NAME="jane_doe_pg_db_$(date +%s)"

aws rds create-db-instance \
  --db-instance-identifier "$INSTANCE_NAME" \
  --db-instance-class db.t3.micro \
  --engine postgres \
  --engine-version 16.1 \
  --master-username pgadmin \
  --master-user-password "$RDS_ADMIN_PASSWORD" \
  --allocated-storage 20 \
  --storage-type gp3 \
  --storage-encrypted \
  --publicly-accessible \
  --db-subnet-group-name lfc-test-vpc-{vpc-id} \
  --vpc-security-group-ids sg-lfc sg-user \
  --db-parameter-group-name lfc-test-pg-params \
  --db-name "$DB_NAME" \
  --backup-retention-period 1 \
  --port 5432 \
  --tags Key=Owner,Value=jane-doe Key=RemoveAfter,Value=$(date -u -d '+90 days' +%Y-%m-%d)
```

Wait for availability (~5–10 minutes):

```bash
aws rds describe-db-instances \
  --db-instance-identifier "$INSTANCE_NAME" \
  --query 'DBInstances[0].DBInstanceStatus'
```

Get the endpoint:

```bash
ENDPOINT="$(aws rds describe-db-instances \
  --db-instance-identifier "$INSTANCE_NAME" \
  --query 'DBInstances[0].Endpoint.Address' --output text)"
echo "Instance ready at $ENDPOINT:5432"
```

### 4. Configure the database

After the instance is available, configure users and CDC settings. Follow the parent skill's engine-specific guides:

- **PostgreSQL**: See `lfc-source-setup/references/engine-postgres.md`
- **MySQL**: See `lfc-source-setup/references/engine-mysql.md`
- **SQL Server**: See `lfc-source-setup/references/engine-sqlserver.md`
- **Oracle**: See `lfc-source-setup/references/engine-oracle.md`

These guides cover creating LFC users, enabling replication, and testing connectivity.

### 5. Register the UC connection

Create a Databricks Unity Catalog connection. See the parent skill's "Register the UC connection" section.

**Briefly:**
1. Generate password: `umask 077; mkdir -p ~/.lfc/pw; echo "Lfc1$(openssl rand -hex 12)" > ~/.lfc/pw/rds-lfc.pw` (letters and digits: RDS refuses `/`, `@`, `"`)
2. Create `conn.json` with `umask 077; cat > conn.json <<EOF...EOF` (read password from file)
3. Create: `databricks connections create --json @conn.json --profile <PROFILE>`
4. Delete: `rm conn.json ~/.lfc/pw/rds-lfc.pw`

### 6. Verify the connection

Run a read-only query via Databricks SQL or notebook. See the parent skill's "Verify" section.

## Networking: Databricks serverless connectivity

> **Firewall rules:** use **lfc-source-firewall** to add your laptop IP and the workspace's serverless egress IPs (and classic compute egress, when the pipeline uses classic compute) to this database. It covers rule limits and keeps the rules in sync.

The RDS instance is publicly accessible and reachable via security group rules:

- **PubliclyAccessible**: `true` (set during create)
- **Inbound rules** on port {port} from:
  - Databricks serverless egress CIDR (added to LFC security group)
  - Your laptop /24 CIDR (added to user security group)

Test from your machine:

```bash
nc -zv $ENDPOINT 5432  # or telnet
```

## Lifecycle: stop, start, delete

### Stop (pause compute)

```bash
aws rds stop-db-instance --db-instance-identifier "$INSTANCE_NAME"
```

Resumes automatically after 7 days; stored data continues to incur charges.

### Start

```bash
aws rds start-db-instance --db-instance-identifier "$INSTANCE_NAME"

# Wait for available
aws rds describe-db-instances \
  --db-instance-identifier "$INSTANCE_NAME" \
  --query 'DBInstances[0].DBInstanceStatus'
```

### Delete

Delete the UC connection first, then the instance:

```bash
databricks connections delete --name "{instance-name}" --profile <PROFILE>

aws rds delete-db-instance \
  --db-instance-identifier "$INSTANCE_NAME" \
  --skip-final-snapshot \
  --delete-automated-backups
```

**Shared infrastructure (parameter groups, subnet groups, security groups) are NOT deleted.** Clean up manually if decommissioning all instances in the VPC.

## Gotchas

1. **Parameter group family mismatch** — Family must match engine and major version (e.g., `postgres16`, not `postgres`). Lists families: `aws rds describe-db-engine-versions --engine postgres`

2. **SQL Server license-included** — Requires `--license-model license-included`; bills per-second license fee on top of compute/storage

3. **MySQL binlog and backup retention** — Default backup retention is 0 (disabled). CDC cannot work without binlog; set `--backup-retention-period 1` at create time

4. **Security group rule limits** — Each group has ≤200 inbound/outbound rules. LFC CIDR lists grow over time; collapse redundant rules (e.g., `/8` covers many `/24` CIDRs)

5. **Oracle DB name** — `--db-name` must be uppercase and ≤8 characters (e.g., `ORCL`); becomes the PDB service name

6. **Shared resources** — Never delete shared parameter/subnet/security groups on individual instance teardown; they break all other instances using them
