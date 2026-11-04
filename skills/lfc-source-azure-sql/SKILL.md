---
name: lfc-source-azure-sql
description: "Create an Azure SQL Database or SQL Managed Instance for Lakeflow Connect testing, configure it for query-based ingestion (Change Tracking or CDC), and register the UC connection."
parent: lfc-source-setup
---

# Azure SQL Database and SQL Managed Instance for Lakeflow Connect

Provision a SQL Server source on Azure for Lakeflow Connect with either **Azure SQL Database** (cheaper, faster provisioning) or **Azure SQL Managed Instance** (MI: slower, expensive, full control).

## When to use

| Option | Best for | Cost | Provisioning |
|--------|----------|------|--------------|
| **SQL Database** | Testing, demos, time-sensitive work | $$-$$$ per month | Minutes |
| **SQL Managed Instance** | Full SQL Server compatibility, CDC | $$$$-$$$$$$ per month | 4–6 hours (first in subnet) |

Both support Change Tracking (CT) and CDC. Both incur data transfer costs if Databricks is in a different region.

## Inputs to collect

**Saved settings first** (**lfc-test-settings**): `AZ_SUBSCRIPTION`, `AZ_REGION`, `AZ_RESOURCE_GROUP` and `OWNER` come from `settings.sh env`; `settings.sh require AZ_SUBSCRIPTION AZ_REGION AZ_RESOURCE_GROUP OWNER` lists only what to ask. The rest below is per database.

| Input | Source | Example |
|-------|--------|---------|
| Azure region | `AZ_REGION` (saved) | `eastus`, `westeurope` |
| Resource group | `AZ_RESOURCE_GROUP` (saved): an existing group the user picked; never created, re-tagged or deleted | `<resource-group>` |
| Subscription ID | `AZ_SUBSCRIPTION` (saved), on every `az` call | `<subscription id>` |
| Owner tag | `OWNER` (saved) | `jane-doe` |
| Replication mode | Query-based only / CT / CDC / BOTH | See parent [engine-sqlserver.md](../lfc-source-setup/references/engine-sqlserver.md) |
| **SQL Database:** Service Objective (SKU) | Default | `GP_S_Gen5_1` for testing (sqldb.sh's `VCORES=1`) |
| **SQL Database:** Storage (GB) | Minimum 32 | `32`, `64` |
| **SQL MI:** vCore count | Must be 4, 8, 16, 24, 32, 40, 64, or 80 | `4` (minimum) |
| **SQL MI:** Storage (GB) | Minimum 32 | `32`, `128` |

## Ask before creating

List every resource (server/instance, VNet, firewall/PE rules) and estimated monthly cost:
- **SQL Database:** ~$50–100/month
- **SQL MI:** ~$400–1000/month (plus provisioning wait time: 4–6 hours first, 30–60 min after)

Wait for a **yes** before proceeding.

## Prerequisites

```bash
# Confirm cloud login
az account show --query "{Name: name, SubscriptionId: id, State: state}" -o table

# Name the subscription on every az call (--subscription <subscription_id>); az account set would change the default for all of them

# Resource group: the saved AZ_RESOURCE_GROUP (lfc-test-settings), an existing one; never created here
az group exists --subscription "$AZ_SUBSCRIPTION" --name "$AZ_RESOURCE_GROUP"
```

**Identities are per resource.** Each resource records the identity it was created with (the create's `IDENTITY` line: Azure tenant and subscription, AWS profile and account, GCP account and project), and every later call for it passes that identity explicitly. Never rely on the CLI's current default: one login can reach several tenants, accounts and projects. Keep the values in your private env file, not in the repo.

## Create: SQL Database vs. SQL Managed Instance

### Option A: SQL Database (recommended for testing)

[`scripts/sqldb.sh`](scripts/sqldb.sh) makes a logical server with public access and no firewall rules, then a serverless General Purpose database (Gen5, 1 vCore, minimum 0.5) that **auto-pauses** after `AUTO_PAUSE` minutes idle (default 60). Then [`scripts/lfc-setup.sh`](scripts/lfc-setup.sh) adds the test login, the seed tables and change tracking + CDC, and **lfc-source-firewall**'s `server-fw.py` opens it to the same sources as every other source, scoped to fit the server's 128-rule cap.

```bash
export RG=<resource-group> LOCATION=<location> SERVER=<pfx>-mssql-<loc>-<epoch> DB=<database>
S=skills/lfc-source-azure-sql/scripts
bash $S/sqldb.sh plan
ADMIN_PW_FILE=admin.pw bash $S/sqldb.sh create
python3 skills/lfc-source-firewall/scripts/server-fw.py sql $SERVER --nsg <shared-nsg> --regions <cloud>:<region>,... --apply
HOST=$SERVER.database.windows.net ADMIN_PW_FILE=admin.pw LOGIN_PW_FILE=lfc_user.pw bash $S/lfc-setup.sh
bash $S/sqldb.sh status            # Online, Paused or Resuming
```

- **Auto-pause:** an idle database pauses itself; there's nothing to stop by hand. The next login resumes it, and that first login fails with error 40613 ("Database ... is not currently available") until it's online, about a minute: `sqldb.sh wake` logs in until it answers. Run it before a test that starts with a paused database.
- **The free offer** (`FREE=1`: `--use-free-limit --free-limit-exhaustion-behavior AutoPause`) failed with `InternalServerError` on every try in the test subscription; the same database without it was created at once. Leave `FREE` unset unless the subscription is known to have the offer.
- **The test login's password can't contain part of its name:** `lfc_user` splits at `_`, so a password starting `Lfc` fails with error 40632 ("not complex enough") although it has upper, lower and digits.
- `lfc-setup.sh` creates the login and its user in `master` over a separate connection (SQL Database has no `USE master`), and passes every batch as a file: go-sqlcmd doesn't read a batch from standard input.

### Option B: SQL Managed Instance (production-like)

**Warning:** First MI in a new subnet can take **4–6 hours** to provision (one General Purpose 4-vCore instance took 2 hours on 2026-10-26); later ones 30–60 minutes.

[`scripts/mi.sh`](scripts/mi.sh) does every step. It puts the instance in its own `/27` subnet in an existing VNet, delegated to `Microsoft.Sql/managedInstances`, with the subnet's own NSG (`<subnet>-nsg`) and route table (`0.0.0.0/0` to Internet), which MI needs before it's created. MI adds its own management rules to that NSG; don't attach the shared VM NSG to the subnet. The NSG starts with no rule for the public data endpoint (port 3342), so the instance is closed to everything outside the VNet until **lfc-source-firewall** syncs the laptop and Databricks egress ranges into `<subnet>-nsg`: the same list as every other source.

```bash
export RG=<resource-group> VNET=<vnet> MI_NAME=<pfx>-mssql-mi-<location>-<epoch>   # SUBSCRIPTION, SUBNET_CIDR, VCORES, STORAGE_GB, PRICING: see the script
S=skills/lfc-source-azure-sql/scripts
bash $S/mi.sh plan                                    # read-only: what create makes
umask 077; echo "Lfc1$(openssl rand -hex 12)" > admin.pw
ADMIN_PW_FILE=admin.pw bash $S/mi.sh create           # returns at once
bash $S/mi.sh wait                                    # in the background (watch-progress); hours
bash $S/mi.sh endpoint                                # <name>.public.<zone>.database.windows.net,3342
bash $S/mi.sh stop                                    # pause compute between test runs; start to resume
```

Then copy the shared NSG's sources into it for port 3342: **lfc-source-firewall**'s `scripts/nsg-copy.sh <shared-nsg> <subnet>-nsg 3342 --apply` (rerun after the shared NSG is synced). `PRICING=Freemium` uses the free offer (one instance per subscription). Details and the private endpoint path: [azure-sql-mi.md](references/azure-sql-mi.md), [mi-networking.md](references/mi-networking.md).

## Configure for LFC

Create the LFC user and enable Change Tracking or CDC via T-SQL (see parent [engine-sqlserver.md](../lfc-source-setup/references/engine-sqlserver.md)):

```sql
-- Create LFC user
CREATE LOGIN [lfc_user] WITH PASSWORD = N'<password-from-file>';
CREATE USER [lfc_user] FOR LOGIN [lfc_user] WITH DEFAULT_SCHEMA = dbo;
ALTER ROLE db_owner ADD MEMBER [lfc_user];
ALTER ROLE db_ddladmin ADD MEMBER [lfc_user];

-- Enable Change Tracking (or CDC; see parent reference)
ALTER DATABASE [<db>] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 3 DAYS, AUTO_CLEANUP = ON);

-- Create seed tables (see parent reference)
CREATE TABLE dbo.intpk (pk INT IDENTITY NOT NULL PRIMARY KEY, dt DATETIME, ops VARCHAR(255) DEFAULT 'insert');
CREATE TABLE dbo.dtix (pk BIGINT, dt DATETIME, ops VARCHAR(255) DEFAULT 'insert');
```

**The utility objects script** is optional for tables read through change tracking (the same schema evolution without it) and required for CDC: without its CDC objects the first update fails with `INGESTION_GATEWAY_DDL_OBJECTS_MISSING`, for every table in that database. `CLEANUP` mode removes only the DDL support objects; [`scripts/uninstall-lakeflow.sql`](scripts/uninstall-lakeflow.sql) removes everything, checked with a before/after catalog diff. What each step adds and removes: [utility-objects.md](references/utility-objects.md).

## Register the UC connection

```bash
# Read the LFC user password from the secure file
LFC_PASSWORD="$(cat lfc_user.pw)"

# Create connection JSON (restricted permissions)
umask 077
cat > conn.json <<EOF
{
  "name": "<connection-name>",
  "connection_type": "SQLSERVER",
  "options": {
    "host": "<server-fqdn>",
    "port": "<1433-or-3342>",
    "user": "lfc_user",
    "password": "${LFC_PASSWORD}",
    "trustServerCertificate": "true"
  }
}
EOF

# Create the UC connection and delete the files immediately
databricks connections create --json @conn.json --profile <PROFILE> && rm conn.json lfc_user.pw

# Clear the password variable
unset LFC_PASSWORD
```

**Host and port:**
- **SQL Database:** `<pfx>-mssql-<location>-<epoch>.database.windows.net:1433`
- **SQL MI (public endpoint):** `<public_fqdn>:3342`, the host `mi.sh endpoint` prints (`<name>.public.<dns zone>.database.windows.net`)

## Verify connectivity

### From your laptop

```bash
sqlcmd -S <server>,<port> -U <user> -P <password> -d <database> -Q "SELECT 1;"
```

### From Databricks (test the UC connection)

```bash
cat > /tmp/test_query.sql <<'EOF'
SELECT * FROM remote_query('<connection-name>', 
  database => '<database-name>', 
  query => "SELECT 1 AS ok")
EOF

databricks experimental aitools tools query --file /tmp/test_query.sql \
  --profile <PROFILE> -o csv
```

## Networking

> **Firewall rules:** use **lfc-source-firewall** to add your laptop IP and the workspace's serverless egress IPs (and classic compute egress, when the pipeline uses classic compute) to this database. It covers rule limits and keeps the rules in sync.

**Default: the same IP list for every source.** SQL Database: public access with server firewall rules. SQL MI: the public endpoint (port 3342), allowed by the rules in its subnet's NSG. **lfc-source-firewall** syncs both from one list (laptop, serverless egress, classic egress).

**Alternative: Private endpoint** (both Database and MI). Link the private DNS zone (`privatelink.database.windows.net`) to the VNet. See [mi-networking.md](references/mi-networking.md) for detailed private endpoint setup.

## Lifecycle

See [azure-sql-lifecycle.md](references/azure-sql-lifecycle.md):
- **Pause/Resume** (SQL Database only)
- **Delete** (Database vs. MI; MI takes 30–60 minutes)
- **Cleanup** shared resources (VNet, DNS zones)

## Top gotchas

1. **MI provisioning is slow:** 4–6 hours for first MI in a new subnet.
2. **SQL Database CDC tier required:** Not available on `Basic` or `S0`–`S2`; the serverless `GP_S_Gen5` database `sqldb.sh` makes has it.
3. **Password complexity:** three of upper, lower, digits, symbols; no spaces; and no part of the login's name (error 40632).
4. **LFC user grants:** Must have `db_owner` and `db_ddladmin` roles.
5. **Collation is fixed at creation:** Changing collation later requires migration. Specify at database creation time if you need case-sensitive.

See [azure-sql-gotchas.md](references/azure-sql-gotchas.md) for more troubleshooting.

## Reference

- Parent skill **lfc-source-setup**: overview, naming, shared infrastructure
- [engine-sqlserver.md](../lfc-source-setup/references/engine-sqlserver.md) — user, grants, CT/CDC, seed tables
- [azure-sql-database.md](references/azure-sql-database.md) — SQL Database detailed setup (server, database, firewall, PE)
- [azure-sql-mi.md](references/azure-sql-mi.md) — SQL MI detailed setup (subnet, NSG, provisioning monitoring)
- [mi-networking.md](references/mi-networking.md) — NSG, private endpoint, public endpoint networking details
- [azure-sql-lifecycle.md](references/azure-sql-lifecycle.md) — pause, resume, delete, cleanup
- [azure-sql-gotchas.md](references/azure-sql-gotchas.md) — troubleshooting
