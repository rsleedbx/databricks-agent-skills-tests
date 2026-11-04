---
name: lfc-source-aws-aurora
description: Provision Aurora MySQL or Aurora PostgreSQL clusters for Databricks Lakeflow Connect
parent: lfc-source-setup
---

# AWS Aurora for Lakeflow Connect

Provision **Aurora MySQL 8.0+** or **Aurora PostgreSQL 13+** clusters as CDC sources for Databricks Lakeflow Connect using the AWS CLI.

Aurora uses a **cluster + writer instance** structure with auto-scaling storage and cluster writer endpoint. For standard RDS instances (non-Aurora), use the **lfc-source-aws-rds** skill.

## When to use Aurora

- **Scaling workloads** — auto-scaling compute within a cluster
- **CDC support** — binlog (MySQL) or logical replication (PostgreSQL)
- **Engines** — Aurora MySQL 8.0+, Aurora PostgreSQL 13+

(Use **lfc-source-aws-rds** for single-instance workloads or simpler setup.)

## Inputs to collect

**Saved settings first** (**lfc-test-settings**): `AWS_PROFILE`, `AWS_REGION`, `AWS_VPC` (unset: the default VPC) and `OWNER` come from `settings.sh env`; `settings.sh require AWS_PROFILE AWS_REGION OWNER` lists only what to ask. The rest below is per cluster.

| Parameter | Default | Example |
|-----------|---------|---------|
| **Engine** | — | `mysql-aurora` or `pg-aurora` |
| **Version** | Latest stable | MySQL `8.0`, PostgreSQL `16` |
| **Instance class** | `db.t3.medium`: the smallest T3 class Aurora PostgreSQL and Aurora MySQL 3 (MySQL 8.0) support; `db.t3.small` is Aurora MySQL 2 only | `db.r6g.large` |
| **Region** | `AWS_REGION` (saved) | `us-east-1`, `eu-west-1` |
| **Environment** | `dev` | `dev` (1-day backups), `staging` (7-day), `prod` (7-day + DeletionProtection) |

## Prerequisites

- **AWS CLI** configured: `aws sts get-caller-identity`
- **VPC with 2+ subnets** in different AZs
- **Databricks CLI**: `databricks auth profiles` shows available profile
- **IAM permissions**: `rds:CreateDBCluster`, `rds:CreateDBInstance`, `ec2:CreateSecurityGroup`, `rds:CreateDBSubnetGroup`

**Identities are per resource.** Each resource records the identity it was created with (the create's `IDENTITY` line: Azure tenant and subscription, AWS profile and account, GCP account and project), and every later call for it passes that identity explicitly. Never rely on the CLI's current default: one login can reach several tenants, accounts and projects. Keep the values in your private env file, not in the repo.

## Naming pattern

Resources use: `{arm}-{service}-{location}-{epoch}`

Examples:
- Cluster: `jane-doe-mysql-aurora-useast1-1778687039`
- Writer instance: `jane-doe-mysql-aurora-useast1-1778687039-primary`
- Logical database: `jane_doe_mysql_aurora_db_1778687039`
- Shared resources: `lfc-test-{suffix}` (parameter groups, subnet groups, security groups)

## Shared infrastructure

**Created once per VPC and reused:**

- **Cluster parameter group** (`lfc-test-aurora-{engine}-cpg`): CDC parameters (binlog for MySQL, logical replication for PostgreSQL)
- **DB subnet group** (`lfc-test-vpc-{vpc-id}`): Contains 2+ subnets in different AZs
- **LFC security group** (`lfc-test-lfc-vpc-{short}`): Allows Databricks serverless egress CIDRs
- **User security group** (`lfc-test-user-vpc-{short}`): Allows your /24 CIDR

**Not deleted on individual cluster teardown.** Shared across all users in the VPC.

## Workflow

### 1. Check AWS context

```bash
aws sts get-caller-identity
aws ec2 describe-vpcs --filters "Name=isDefault,Values=true" --query 'Vpcs[0].VpcId'
```

### 2. Ensure shared infrastructure

**DB subnet group:**

```bash
aws rds create-db-subnet-group \
  --db-subnet-group-name lfc-test-vpc-{vpc-id} \
  --db-subnet-group-description "LFC shared subnet group" \
  --subnet-ids subnet-a subnet-b
```

**Cluster parameter group (MySQL example):**

```bash
aws rds create-db-cluster-parameter-group \
  --db-cluster-parameter-group-name lfc-test-aurora-mysql-cpg \
  --db-parameter-group-family aurora-mysql8.0 \
  --description "lfc-test-aurora-mysql-cpg"

aws rds modify-db-cluster-parameter-group \
  --db-cluster-parameter-group-name lfc-test-aurora-mysql-cpg \
  --parameters \
    ParameterName=binlog_format,ParameterValue=ROW,ApplyMethod=pending-reboot \
    ParameterName=binlog_row_image,ParameterValue=FULL,ApplyMethod=pending-reboot
```

**PostgreSQL cluster parameter group:**

```bash
aws rds create-db-cluster-parameter-group \
  --db-cluster-parameter-group-name lfc-test-aurora-pg-cpg \
  --db-parameter-group-family aurora-postgresql16 \
  --description "lfc-test-aurora-pg-cpg"

aws rds modify-db-cluster-parameter-group \
  --db-cluster-parameter-group-name lfc-test-aurora-pg-cpg \
  --parameters \
    ParameterName=rds.logical_replication,ParameterValue=1,ApplyMethod=immediate \
    ParameterName=max_replication_slots,ParameterValue=10,ApplyMethod=immediate
```

**Security groups:**

```bash
aws ec2 create-security-group \
  --group-name lfc-test-lfc-vpc-{short} \
  --description "LFC shared SG for Databricks egress CIDRs" \
  --vpc-id {vpc-id}

# Add Databricks egress CIDR (example: 34.67.0.0/16)
aws ec2 authorize-security-group-ingress \
  --group-id sg-lfc \
  --protocol tcp --port 3306 --cidr 34.67.0.0/16
```

### 3. Create the cluster

Generate and store admin password safely:

```bash
umask 077
# letters and digits only: Aurora refuses /, @ and " in a master password, which base64 output can contain
umask 077; mkdir -p ~/.lfc/pw; echo "Lfc1$(openssl rand -hex 12)" > ~/.lfc/pw/aurora-admin.pw
export AURORA_ADMIN_PASSWORD="$(cat ~/.lfc/pw/aurora-admin.pw)"
```

**MySQL cluster:**

```bash
CLUSTER_ID="jane-doe-mysql-aurora-useast1-$(date +%s)"
DB_NAME="jane_doe_mysql_aurora_db_$(date +%s)"

aws rds create-db-cluster \
  --db-cluster-identifier "$CLUSTER_ID" \
  --engine aurora-mysql \
  --engine-version 8.0.mysql_aurora.3.07.1 \
  --master-username mysqladmin \
  --master-user-password "$AURORA_ADMIN_PASSWORD" \
  --db-subnet-group-name lfc-test-vpc-{vpc-id} \
  --vpc-security-group-ids sg-lfc sg-user \
  --db-cluster-parameter-group-name lfc-test-aurora-mysql-cpg \
  --port 3306 \
  --storage-encrypted \
  --database-name "$DB_NAME" \
  --backup-retention-period 1 \
  --tags Key=Name,Value="$CLUSTER_ID" Key=RemoveAfter,Value=$(date -u -d '+90 days' +%Y-%m-%d)
```

**PostgreSQL cluster:**

```bash
CLUSTER_ID="jane-doe-pg-aurora-useast1-$(date +%s)"
DB_NAME="jane_doe_pg_aurora_db_$(date +%s)"

aws rds create-db-cluster \
  --db-cluster-identifier "$CLUSTER_ID" \
  --engine aurora-postgresql \
  --engine-version 16.2 \
  --master-username pgadmin \
  --master-user-password "$AURORA_ADMIN_PASSWORD" \
  --db-subnet-group-name lfc-test-vpc-{vpc-id} \
  --vpc-security-group-ids sg-lfc sg-user \
  --db-cluster-parameter-group-name lfc-test-aurora-pg-cpg \
  --port 5432 \
  --storage-encrypted \
  --database-name "$DB_NAME" \
  --backup-retention-period 1 \
  --tags Key=Name,Value="$CLUSTER_ID" Key=RemoveAfter,Value=$(date -u -d '+90 days' +%Y-%m-%d)
```

Wait for cluster availability (~3–5 minutes):

```bash
aws rds describe-db-clusters \
  --db-cluster-identifier "$CLUSTER_ID" \
  --query 'DBClusters[0].Status'
```

### 4. Create the writer instance

A cluster is empty until you add an instance. Create the single writer instance:

**MySQL:**

```bash
aws rds create-db-instance \
  --db-instance-identifier "${CLUSTER_ID}-primary" \
  --db-cluster-identifier "$CLUSTER_ID" \
  --db-instance-class db.t3.medium \
  --engine aurora-mysql \
  --publicly-accessible
```

**PostgreSQL:**

```bash
aws rds create-db-instance \
  --db-instance-identifier "${CLUSTER_ID}-primary" \
  --db-cluster-identifier "$CLUSTER_ID" \
  --db-instance-class db.t3.medium \
  --engine aurora-postgresql \
  --publicly-accessible
```

Wait for instance availability and **reboot to apply CDC parameters:**

```bash
aws rds reboot-db-instance \
  --db-instance-identifier "${CLUSTER_ID}-primary"

# Poll until available
aws rds describe-db-instances \
  --db-instance-identifier "${CLUSTER_ID}-primary" \
  --query 'DBInstances[0].DBInstanceStatus'
```

### 5. Configure the database

After the instance is available and rebooted, configure users and CDC settings. Follow the parent skill's engine-specific guides:

- **Aurora MySQL**: See `lfc-source-setup/references/engine-mysql.md`
- **Aurora PostgreSQL**: See `lfc-source-setup/references/engine-postgres.md`

### 6. Register the UC connection

See the parent skill's "Register the UC connection" section.

**Briefly:**
1. Generate password: `umask 077; mkdir -p ~/.lfc/pw; echo "Lfc1$(openssl rand -hex 12)" > ~/.lfc/pw/aurora-lfc.pw` (letters and digits: Aurora refuses `/`, `@`, `"`)
2. Get cluster endpoint: `aws rds describe-db-clusters --db-cluster-identifier $CLUSTER_ID --query 'DBClusters[0].Endpoint'`
3. Create `conn.json` with `umask 077; cat > conn.json <<EOF...EOF` (read password from file)
4. Create: `databricks connections create --json @conn.json --profile <PROFILE>`
5. Delete: `rm conn.json ~/.lfc/pw/aurora-lfc.pw`

### 7. Verify the connection

Run a read-only query via Databricks SQL or notebook. See the parent skill's "Verify" section.

## Networking: Databricks serverless connectivity

> **Firewall rules:** use **lfc-source-firewall** to add your laptop IP and the workspace's serverless egress IPs (and classic compute egress, when the pipeline uses classic compute) to this database. It covers rule limits and keeps the rules in sync.

Aurora is created publicly accessible but requires security group rules:

- **PubliclyAccessible**: `true` (set at instance create)
- **Inbound rules** on port {3306 for MySQL, 5432 for PostgreSQL} from:
  - Databricks serverless egress CIDR (LFC security group)
  - Your laptop /24 CIDR (user security group)

Test from your machine:

```bash
nc -zv {cluster-endpoint} 3306  # or 5432 for PostgreSQL
```

## Lifecycle: stop, start, delete

### Stop cluster (pause compute)

```bash
aws rds stop-db-cluster --db-cluster-identifier "$CLUSTER_ID"
```

Storage persists and continues to incur charges. Takes 5–10 minutes.

### Start cluster

```bash
aws rds start-db-cluster --db-cluster-identifier "$CLUSTER_ID"

# Wait for available
aws rds describe-db-clusters \
  --db-cluster-identifier "$CLUSTER_ID" \
  --query 'DBClusters[0].Status'
```

### Delete cluster

**Deletion order:** Delete writer instance first (RDS requirement), then cluster.

```bash
# Delete instance
aws rds delete-db-instance \
  --db-instance-identifier "${CLUSTER_ID}-primary" \
  --skip-final-snapshot

# Delete cluster
aws rds delete-db-cluster \
  --db-cluster-identifier "$CLUSTER_ID" \
  --skip-final-snapshot

# Delete UC connection
databricks connections delete --name "{connection-name}" --profile <PROFILE>
```

**Shared infrastructure (parameter groups, subnet groups, security groups) are NOT deleted.** Clean up manually if decommissioning all clusters in the VPC.

## Gotchas

1. **Cluster parameter groups are shared** — Modifying one affects all clusters using it. Changes to `binlog_format` or `rds.logical_replication` require a reboot of all member instances

2. **Instance reboot after cluster creation** — Aurora MySQL CDC requires a reboot to apply cluster parameter group changes (binlog settings use `pending-reboot` apply method). Do not skip this step

3. **PostgreSQL: pgaudit plugin** — Aurora PostgreSQL uses `pgaudit.log` parameter; the plugin must be preloaded via the cluster parameter group

4. **Storage auto-scaling** — Aurora storage scales automatically on the cluster; there is no `--storage` parameter as in RDS

5. **Multi-AZ only at cluster level** — Aurora does not have a separate multi-AZ flag for instances. HA is managed at the cluster level

6. **Binlog retention** — Aurora does not expose a separate binlog retention parameter like RDS does. Retention is managed via automated backups. Ensure `--backup-retention-period >= 1` for CDC

7. **Deletion protection** — When `DeletionProtection: true` (prod/staging), both cluster and instances are protected. You must explicitly remove protection before deleting
