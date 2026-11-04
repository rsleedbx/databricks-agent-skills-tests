---
name: lfc-source-gcp-cloudsql
description: Provision Google Cloud SQL databases (MySQL, PostgreSQL, SQL Server) as sources for Databricks Lakeflow Connect using the gcloud CLI
parent: lfc-source-setup
---

# Google Cloud SQL for Lakeflow Connect

Provision MySQL, PostgreSQL, or SQL Server instances on Google Cloud SQL as replication sources for Databricks Lakeflow Connect (LFC). This guide uses the `gcloud` CLI.

## When to use

- You need a managed cloud database for LFC CDC (Change Data Capture)
- Your organization uses Google Cloud Platform (GCP)
- You prefer not to manage database infrastructure yourself (automated backups, patching, HA)
- You want a production-grade engine with Databricks connectivity

Not suitable for: local development (use Podman), existing on-premises databases, or when you need the lowest possible cost (GCP Cloud SQL has minimum charges).

## Supported engines and defaults

| Engine | Version | Port | Replication |
|--------|---------|------|-------------|
| **MySQL** | `MYSQL_8_0` | 3306 | Binary logging (binlog) |
| **PostgreSQL** | `POSTGRES_16` | 5432 | Logical replication (wal_level=logical) |
| **SQL Server** | `SQLSERVER_2022_STANDARD` | 1433 | Change Tracking (CT) or CDC |

## Input parameters to collect

**Saved settings first** (**lfc-test-settings**): `GCP_PROJECT`, `GCP_REGION` and `OWNER` come from `settings.sh env`; `settings.sh require GCP_PROJECT GCP_REGION OWNER` lists only what to ask. The rest below is per instance.

| Input | Example | Notes |
|-------|---------|-------|
| **GCP project ID** | `company-dev-project` | `GCP_PROJECT` (saved), on every `gcloud` call (`--project`) |
| **GCP region** | `us-central1` | `GCP_REGION` (saved) |
| **Engine** | `mysql` \| `postgres` \| `sqlserver` | Determines version + flags |
| **Instance tier** | `db-f1-micro` (dev) / `db-custom-1-3840` (SQL Server min) | GCP machine type |
| **Storage (GB)** | `10` | GCP minimum is engine-dependent |
| **User email prefix** | `jane-doe` | From `OWNER` (saved): lowercase, `.` / `_` → `-`, max 12 chars |

## Before creating: cost check and approval

**Billable resources.** Cloud SQL instances incur hourly compute charges (~$0.01–$1.00/hour) plus storage and backup charges. Get approval before provisioning.

Summarize what you're about to create:

```
About to create on GCP:
  - Cloud SQL Instance: {INSTANCE_NAME} ({ENGINE} {VERSION})
    Region: {REGION}
    Tier: {TIER}
    Storage: {STORAGE_GB} GB
    Estimated cost: ~$15–50/month (free tier first 30 days for small tiers)
  - UC connection: {CONNECTION_NAME}

Proceed? (yes/no)
```

Wait for explicit confirmation before calling cloud APIs.

## Prerequisites

- **gcloud CLI** installed: `gcloud --version`
- **Authentication**: `gcloud auth application-default login`
- **Project set**: `gcloud config set project PROJECT_ID`
- **APIs enabled**: `gcloud services enable sqladmin.googleapis.com compute.googleapis.com`
- **Databricks CLI** with profile: `databricks auth profiles`

**Identities are per resource.** Each resource records the identity it was created with (the create's `IDENTITY` line: Azure tenant and subscription, AWS profile and account, GCP account and project), and every later call for it passes that identity explicitly. Never rely on the CLI's current default: one login can reach several tenants, accounts and projects. Keep the values in your private env file, not in the repo.

## Create the instance

### Step 1: Set variables

```bash
PROJECT_ID="your-gcp-project"
REGION="us-central1"
ENGINE="mysql"              # or "postgres" or "sqlserver"
TIER="db-f1-micro"
STORAGE_GB="10"
USER_PREFIX="jane-doe"
EPOCH=$(date +%s)

INSTANCE_NAME="${USER_PREFIX}-${ENGINE:0:2}-${REGION//-/}-${EPOCH}"
```

### Step 2: Create instance

See `references/cloudsql-create.md` for detailed engine-specific creation commands with backup and flag configuration.

Quick example (MySQL):

```bash
gcloud sql instances create "${INSTANCE_NAME}" \
  --database-version=MYSQL_8_0 \
  --region="${REGION}" \
  --tier="${TIER}" \
  --storage-size="${STORAGE_GB}GB" \
  --enable-bin-log \
  --project="${PROJECT_ID}"
```

Wait 15–25 minutes for the instance to become `RUNNABLE`.

### Step 3: Create users and database

See `references/cloudsql-users.md` for detailed user creation and password handling.

Quick example:

> `gcloud sql users create` and `gcloud sql instances create` take passwords only as flags, so they're briefly visible in this machine's process list while the command runs. Run them on a machine you control, or create the Lakeflow Connect user in SQL instead (with `PGPASSWORD` / `MYSQL_PWD` / `SQLCMDPASSWORD`), and delete the `.pw` files once the UC connection exists.

```bash
umask 077
echo "Lfc1$(openssl rand -hex 12)" > admin.pw
echo "Lfc1$(openssl rand -hex 12)" > lfc_user.pw

gcloud sql users create root \
  --instance="${INSTANCE_NAME}" \
  --password="$(cat admin.pw)" \
  --project="${PROJECT_ID}"

gcloud sql users create lfc_user \
  --instance="${INSTANCE_NAME}" \
  --password="$(cat lfc_user.pw)" \
  --project="${PROJECT_ID}"
```

**Note:** The password value is briefly visible in the process list (`ps aux`) during the `gcloud` command execution. To minimize exposure, use a saved file and pass via `--password` flag as shown. The visibility window is only a few milliseconds.

### Step 4: Configure for LFC

For engine-specific configuration (grants, CDC setup, replication users), see the parent skill **lfc-source-setup** links to `references/engine-*.md`.

## Networking for Databricks serverless

> **Firewall rules:** use **lfc-source-firewall** to add your laptop IP and the workspace's serverless egress IPs (and classic compute egress, when the pipeline uses classic compute) to this database. It covers rule limits and keeps the rules in sync.

Databricks serverless subnets must reach your Cloud SQL instance via public IP.

### Authorize your laptop and Databricks egress

Use **lfc-source-firewall** ([GCP steps](../lfc-source-firewall/references/gcp.md)): it finds your laptop IP and the workspace's serverless egress ranges from Databricks' published `ip-ranges.json`, and sets `--authorized-networks` (which replaces the whole list, so it keeps existing entries).

Consult your Databricks admin or workspace settings for exact ranges before opening firewall rules.

## Register the UC connection

### Create connection JSON (private file only)

```bash
umask 077
eval "$(gcloud sql instances describe "${INSTANCE_NAME}" \
  --project="${PROJECT_ID}" --format=json \
  | yq -p json -o shell '{"HOST": (.ipAddresses[0].ipAddress // "")}')"

cat > conn.json <<EOF
{"name": "jane-doe-mysql-uscentral1-${EPOCH}", "connection_type": "MYSQL",
 "options": {"host": "$HOST", "port": "3306", "user": "lfc_user", "password": "$(cat lfc_user.pw)", "trustServerCertificate": "true"}}
EOF
```

### Create connection in Databricks

```bash
databricks connections create --json @conn.json --profile <PROFILE>
```

### Clean up

```bash
rm conn.json admin.pw lfc_user.pw
```

## Verify connection

Test the UC connection from Databricks SQL:

```sql
SELECT * FROM <connection_name>.information_schema.tables LIMIT 5;
```

## Lifecycle: stop, start, delete

**Stop** (pause compute charges):
```bash
gcloud sql instances patch "${INSTANCE_NAME}" \
  --activation-policy=NEVER --project="${PROJECT_ID}"
```

**Start** (resume):
```bash
gcloud sql instances patch "${INSTANCE_NAME}" \
  --activation-policy=ALWAYS --project="${PROJECT_ID}"
```

**Delete**:
```bash
gcloud sql instances delete "${INSTANCE_NAME}" --quiet --project="${PROJECT_ID}"
```

After deletion, the UC connection in Databricks remains but will fail on test. Delete it: `databricks connections delete <connection_name>`.

For more details, see `references/cloudsql-lifecycle.md`.

## Top gotchas

1. **Instance name already exists**: GCP reserves names ~7 days after deletion. Use a different `EPOCH` or wait.

2. **MySQL binlog + backup error**: For CDC, ensure `--enable-bin-log` and backup enabled. See `references/cloudsql-create.md`.

3. **PostgreSQL logical_decoding**: Cloud SQL flag `cloudsql.logical_decoding=on` is required; set via `--database-flags` during instance creation.

4. **SQL Server Change Tracking not captured**: Ensure the database has CT enabled and the LFC user has `VIEW DATABASE STATE` permission. See parent skill `references/engine-sqlserver.md`.

5. **Authorization failed from Databricks**: Verify (1) public IP assigned, (2) workspace serverless CIDR in authorized networks, (3) LFC user password matches UC connection, (4) database exists (MySQL/PostgreSQL).

## Additional resources

- **Parent skill**: [lfc-source-setup](../lfc-source-setup/SKILL.md) — engine-specific configuration, shared references
- **Create details**: [cloudsql-create.md](references/cloudsql-create.md)
- **User management**: [cloudsql-users.md](references/cloudsql-users.md)
- **Networking**: [cloudsql-networking.md](references/cloudsql-networking.md)
- **Lifecycle**: [cloudsql-lifecycle.md](references/cloudsql-lifecycle.md)
- **GCP Cloud SQL docs**: [https://cloud.google.com/sql/docs](https://cloud.google.com/sql/docs)
