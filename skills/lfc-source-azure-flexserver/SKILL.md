---
name: lfc-source-azure-flexserver
description: "Provision Azure Database for MySQL/PostgreSQL Flexible Server as a source for Databricks Lakeflow Connect."
parent: lfc-source-setup
---

# Azure Database for MySQL / PostgreSQL Flexible Server Setup for LFC

Provision a managed MySQL or PostgreSQL Flexible Server instance on Azure and configure it as a Lakeflow Connect (LFC) source.

## When to use

- You have a Databricks workspace and Azure subscription.
- You want managed MySQL 5.7+ or PostgreSQL 12+ (not self-hosted or podman-local).
- You want LFC to replicate data via binlog (MySQL) or logical replication (PostgreSQL).

**Not suitable for:**
- Azure SQL Database or SQL Server Managed Instance — use `lfc-source-azure-sql` instead.
- Local testing on a single machine — use local Docker/podman instead.

## Inputs to collect

**Saved settings first** (**lfc-test-settings**): `AZ_SUBSCRIPTION`, `AZ_REGION`, `AZ_RESOURCE_GROUP` and `OWNER` come from `settings.sh env`; `settings.sh require AZ_SUBSCRIPTION AZ_REGION AZ_RESOURCE_GROUP OWNER` lists only what to ask. The rest below is per database.

| Input | Example | Notes |
|-------|---------|-------|
| Azure subscription ID | `12345678-1234-1234-1234-123456789012` | `AZ_SUBSCRIPTION` (saved), on every `az` call |
| Azure region | `eastus`, `westeurope` | `AZ_REGION` (saved): the workspace's region |
| Resource group | `<resource-group>` | `AZ_RESOURCE_GROUP` (saved): an existing group the user picked; never created, re-tagged or deleted |
| Engine | `mysql` or `postgres` | Binlog (MySQL) or logical replication (PostgreSQL) |
| Version | MySQL `8.0` / PostgreSQL `15`, `16` | Use LTS for stability |
| SKU | `Standard_B1s` or `Standard_B2ms` | Compute size; verify availability in region |
| Storage (GB) | `32` (minimum) | Scales automatically if enabled |
| Access type | `public` or `private-endpoint` | Default: public firewall rules; private endpoint alternative |

## Ask before creating

Confirm with the user what will be created (Flexible Server, VNet, firewall/PE rules) and estimated monthly cost ($50–200 for dev/test). See [flexserver-create-details.md](references/flexserver-create-details.md) for full prerequisites and naming details.

**Identities are per resource.** Each resource records the identity it was created with (the create's `IDENTITY` line: Azure tenant and subscription, AWS profile and account, GCP account and project), and every later call for it passes that identity explicitly. Never rely on the CLI's current default: one login can reach several tenants, accounts and projects. Keep the values in your private env file, not in the repo.

## Create the server

### Quick start (core steps)

[`scripts/flex.sh`](scripts/flex.sh) `pg|mysql create` makes the server (Burstable `Standard_B1ms`, 32 GB) with public access and **no firewall rules**, the database, and the CDC parameters (next section), restarting once if one changed. [`scripts/lfc-setup.sh`](scripts/lfc-setup.sh) then adds the test user and the seed tables, and **lfc-source-firewall**'s `server-fw.py` opens the server to the same sources as every other source, scoped to fit its cap (MySQL 250 rules, Postgres 500).

```bash
export RG=<resource-group> LOCATION=<location> SERVER=<pfx>-<engine>-<loc>-<epoch> DB=<database>
S=skills/lfc-source-azure-flexserver/scripts
bash $S/flex.sh pg plan                                     # or mysql
ADMIN_PW_FILE=admin.pw bash $S/flex.sh pg create            # 5-10 minutes
python3 skills/lfc-source-firewall/scripts/server-fw.py pg $SERVER --nsg <shared-nsg> --regions <cloud>:<region>,... --apply
HOST=<fqdn> ADMIN_PW_FILE=admin.pw LOGIN_PW_FILE=lfc_user.pw bash $S/lfc-setup.sh pg
bash $S/flex.sh pg stop                                     # between test runs; start to resume
```

- **Public access with no rules** is `--public-access Enabled` for Postgres (its `None` turns public access off, and then firewall rules are refused: "Firewall rule operations are not supported for a server without public access enabled") and `--public-access None` for MySQL.
- **Firewall rules go on one at a time:** about a minute each on a Flexible Server, so 60 rules take about an hour. Keep the list scoped (`server-fw.py --regions`).
- **Stopped servers start again by themselves after 7 days.** A stopped server costs storage only.
- **MySQL:** Azure's default `sql_generate_invisible_primary_key=ON` gives a keyless table a hidden primary key; `flex.sh` turns it off so `dtix` stays keyless.
- **Postgres:** `lfc-setup.sh` makes the test user the owner of the seed tables, with `REPLICATION`. The publication and the slot are yours to create, as the test user, one pair per gateway, and the pipeline names them; without them the gateway reports they don't exist and nothing loads (tested 2026-10-26):
  ```sql
  CREATE PUBLICATION <pub> FOR TABLE public.<t1>, public.<t2>;
  SELECT pg_create_logical_replication_slot('<slot>', 'pgoutput');
  ```
  ```yaml
  source_configurations:
    - catalog:
        source_catalog: <database>
        postgres:
          slot_config: {slot_name: <slot>, publication_name: <pub>}
  ```
  Drop both after the pipeline is deleted (`SELECT pg_drop_replication_slot('<slot>'); DROP PUBLICATION <pub>;`): a slot left behind keeps WAL on the server.
- **MySQL:** the test user gets `caching_sha2_password` (`lfc-setup.sh` sets it): the CDC gateway refuses `mysql_native_password`, Azure's default, with `CDC_NOT_ENABLED`. The UC connection needs `trustServerCertificate: "true"` (or the server's CA in `userProvidedServerCertificate`): without either a CDC gateway fails with `INGESTION_GATEWAY_CONNECTION_ERROR.TLS_VALIDATION_FAILURE` (`require_secure_transport=ON`). Query-based ingestion works without it.

## Server parameters for LFC

Run the commands in [flexserver-parameters.md](references/flexserver-parameters.md) to configure binlog/logical replication parameters.

**Quick summary:**
- **MySQL:** `binlog_row_image=FULL`, `binlog_expire_logs_seconds` at least 604800 (7 days), `sql_generate_invisible_primary_key=OFF`. Row binlog format is Azure's default. `require_secure_transport` stays `ON`: the UC connection uses TLS.
- **PostgreSQL:** `wal_level=logical` (a restart), `max_replication_slots` and `max_wal_senders` at least 10.

`flex.sh create` sets these and restarts once when one changed.

## Networking

> **Firewall rules:** use **lfc-source-firewall** to add your laptop IP and the workspace's serverless egress IPs (and classic compute egress, when the pipeline uses classic compute) to this database. It covers rule limits and keeps the rules in sync.

**Default: Public firewall rules.** Add your laptop IP and Databricks workspace egress CIDR to allow inbound connections on 3306 (MySQL) or 5432 (PostgreSQL).

**Alternative: Private endpoint.** For Databricks serverless, use a private endpoint in the VNet for encrypted network isolation. See [flexserver-networking.md](references/flexserver-networking.md) for both options.

## Configure the database and enable LFC

Create the database:

```bash
ADMIN_PASSWORD="$(cat /tmp/admin_password.txt)"

if [ "${ENGINE}" = "mysql" ]; then
  mysql -h "${SERVER_NAME}.mysql.database.azure.com" -u "${ADMIN_USER}" \
    -p"${ADMIN_PASSWORD}" --ssl-mode=REQUIRED \
    -e "CREATE DATABASE IF NOT EXISTS \`${DB_NAME}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
elif [ "${ENGINE}" = "postgres" ]; then
  export PGPASSWORD="${ADMIN_PASSWORD}"
  psql -h "${SERVER_NAME}.postgres.database.azure.com" -U "${ADMIN_USER}" \
    -c "CREATE DATABASE \"${DB_NAME}\" WITH ENCODING 'UTF8';"
  unset PGPASSWORD
fi
```

Then follow the engine-specific guide to create the LFC user, enable CDC, and seed tables:
- **MySQL:** [engine-mysql.md](../lfc-source-setup/references/engine-mysql.md)
- **PostgreSQL:** [engine-postgres.md](../lfc-source-setup/references/engine-postgres.md)

## Register the UC connection

```bash
# Read the admin password
ADMIN_PASSWORD="$(cat /tmp/admin_password.txt)"

# Create connection JSON (restricted permissions)
umask 077
cat > /tmp/uc_connection.json <<EOF
{
  "name": "<connection-name>",
  "connection_type": "MYSQL" or "POSTGRES",
  "options": {
    "host": "${SERVER_NAME}.mysql.database.azure.com",
    "port": "3306" or "5432",
    "user": "${ADMIN_USER}",
    "password": "${ADMIN_PASSWORD}",
    "trustServerCertificate": "true"
  }
}
EOF

# Create and verify the UC connection
databricks connections create --json @/tmp/uc_connection.json --profile <PROFILE>
rm /tmp/uc_connection.json /tmp/admin_password.txt
unset ADMIN_PASSWORD
```

## Verify connectivity

### From your laptop

```bash
if [ "${ENGINE}" = "mysql" ]; then
  mysql -h "${SERVER_NAME}.mysql.database.azure.com" -u "${ADMIN_USER}" -p \
    --ssl-mode=REQUIRED -e "SELECT VERSION();"
elif [ "${ENGINE}" = "postgres" ]; then
  export PGPASSWORD="<password>"
  psql -h "${SERVER_NAME}.postgres.database.azure.com" -U "${ADMIN_USER}" \
    -c "SELECT version();"
fi
```

### From Databricks (test the UC connection)

Use a read-only remote query to verify the connection:

```bash
cat > /tmp/test_query.sql <<'EOF'
SELECT * FROM remote_query('<connection-name>', 
  database => '<database-name>', 
  query => "SELECT 1 AS ok")
EOF

databricks experimental aitools tools query --file /tmp/test_query.sql \
  --profile <PROFILE> -o csv
```

(For MySQL, omit the `database =>` argument.)

## Lifecycle

See [flexserver-lifecycle.md](references/flexserver-lifecycle.md) for stopping, starting, and deleting servers, plus cleanup.

## Top gotchas

1. **Server creation slow:** Provisioning takes 5–15 minutes. Check status with `az mysql flexible-server show`.
2. **PostgreSQL parameters require restart:** `wal_level` changes need `az postgres flexible-server restart`.
3. **Password rotation:** Use `az mysql flexible-server update --admin-password`.
4. **Firewall rules incomplete:** Verify Databricks workspace egress CIDR is added.
5. **SSL certificate errors:** Download Azure's CA certificate if clients fail verification.

See [flexserver-gotchas.md](references/flexserver-gotchas.md) for more troubleshooting.

## Reference

- Parent skill **lfc-source-setup**: overview, naming, shared infrastructure
- [engine-mysql.md](../lfc-source-setup/references/engine-mysql.md) — MySQL user, grants, binlog setup
- [engine-postgres.md](../lfc-source-setup/references/engine-postgres.md) — PostgreSQL user, grants, logical replication setup
- [flexserver-create-details.md](references/flexserver-create-details.md) — prerequisites, resource provider registration, detailed provisioning steps
- [flexserver-parameters.md](references/flexserver-parameters.md) — server parameter configuration for CDC
- [flexserver-networking.md](references/flexserver-networking.md) — private endpoint and firewall rule setup
- [flexserver-lifecycle.md](references/flexserver-lifecycle.md) — stop, start, delete, cleanup
- [flexserver-gotchas.md](references/flexserver-gotchas.md) — troubleshooting
