---
name: lfc-source-setup
description: "Create a source database for Lakeflow Connect testing and demos on AWS (RDS, Aurora), Azure (Flexible Server, Azure SQL, SQL Managed Instance), GCP (Cloud SQL), a cloud VM, or local Podman; configure it for query-based or CDC ingestion; and register the Unity Catalog connection. Use when a Lakeflow Connect test or demo needs a real external database and none exists yet."
---

# Source databases for Lakeflow Connect

Lakeflow Connect needs a source: a SaaS app or a database. This skill and its children create a **database** source in a cloud account (or locally), configure it the way Lakeflow Connect expects, and register the Unity Catalog connection. After that, the **databricks-lakeflow-connect** skill builds the pipeline.

Use it for testing and demos. For a customer's production database, the customer owns the database: use the source-setup sections of **databricks-lakeflow-connect** instead (CDC / change tracking, replication user, grants).

**No Databricks credentials or database are needed for a Lakebase source:** if a Postgres stand-in is enough, use the Lakebase demo in **databricks-lakeflow-connect** (`references/6-lakebase-as-postgres-source.md`). Use this skill when you need SQL Server, MySQL, Oracle, or Postgres with CDC, or a specific managed service.

## Pick the target

| Target | Skill | Engines | Notes |
|---|---|---|---|
| AWS RDS | **lfc-source-aws-rds** | MySQL, PostgreSQL, SQL Server, Oracle | Stop/start supported |
| AWS Aurora | **lfc-source-aws-aurora** | Aurora MySQL, Aurora PostgreSQL | Cluster + writer instance |
| Azure Flexible Server | **lfc-source-azure-flexserver** | MySQL, PostgreSQL | Stop/start supported |
| Azure SQL Database / SQL Managed Instance | **lfc-source-azure-sql** | SQL Server | Change tracking or CDC; MI is slow to create and costly |
| Google Cloud SQL | **lfc-source-gcp-cloudsql** | MySQL, PostgreSQL, SQL Server | Stop/start via activation policy |
| Cloud VM (Azure, AWS, GCP) | **lfc-source-cloud-vm** | MySQL, PostgreSQL, SQL Server (native); more via Podman on the VM | Full control of versions and CDC settings |
| Test data for any of the above | **lfc-source-datagen** | PostgreSQL, MySQL (SQL Server, Oracle planned) | Generated inside the database from `generate_series` or a seed table; resumable; fast min/max across 10,000+ tables |
| A copy shaped like a customer's database, from their statistics only | **lfc-source-statschema** | PostgreSQL, MySQL, either direction | Collects catalog statistics (no table scans, data values only if allowed), builds a scaled-down schema, fills it with lfc-source-datagen, and checks its statistics |
| How big a source is: total for the infrastructure team, data Lakeflow Connect moves, rows, row size | **lfc-source-sizing** | PostgreSQL, MySQL, SQL Server, Oracle | Catalog only, through the UC connection; no table scans; cross-checks stale statistics |
| The same connections in many workspaces, and an inventory of what exists | **lfc-connection-sync** | — | One private targets file, an idempotent create-or-update script, per-workspace or account-token logins |
| A new test workspace: connector previews, a CDC-capable catalog, access for other people | **lfc-workspace-setup** | — | Settings API for previews; Default Storage check; workspace assignment, catalog binding, grants |
| What's been tested, where, and how it went | **lfc-test-results** | — | `results/runs/*.jsonl` and dated write-ups in this repo; record every run |
| Firewall rules for any of the above | **lfc-source-firewall** | — | Laptop IP + workspace serverless egress (+ classic egress when known) |
| Local Podman (Lima on macOS) | **lfc-source-podman** | MySQL, MariaDB, PostgreSQL, SQL Server (incl. Availability Groups), Oracle, Db2, Informix, MongoDB | **Local testing only:** Databricks serverless can't reach a laptop container, and an SSH tunnel doesn't change that. To ingest from Databricks, use a managed service or a cloud VM |

How to choose:

- **The user names a cloud or service:** use it.
- **Otherwise, match the workspace's cloud and region.** Lakeflow Connect reads the whole source on every snapshot and keeps reading for CDC, and that traffic is billed as network egress when it crosses clouds or regions. Find the workspace's cloud and region with `databricks metastores summary --profile <PROFILE> -o json` (`global_metastore_id` is `<cloud>:<region>:<id>`), then prefer a source there: a workspace on Azure means an Azure source, on AWS an AWS source, on GCP a GCP source, in the same region. Say so if the user picks another cloud or region, so they know it adds egress cost.
- **The engine decides it:** Oracle on a managed service means RDS. Db2, Informix, and SQL Server Availability Groups need a cloud VM to be reachable from Databricks (local Podman is for testing the engine setup only).
- **The Lakeflow Connect mode decides it:** CDC needs server settings that some tiers don't allow (for example SQL Server CDC on Azure SQL needs a tier that supports it; Postgres CDC needs logical replication). Each child skill says what its target supports.
- **One source shared by workspaces in many clouds or regions:** some reads will cross clouds whatever you pick, so pick by firewall capacity: an **Azure** VM's NSG can hold every region's serverless ranges (about 760 CIDRs). Put it in the cloud and region of the workspace that reads it most.
- Ask which cloud the user has an account in before creating anything; don't create anything in a cloud they didn't confirm.

Engine-specific database setup (users, grants, change tracking / CDC, replication, binlog) is the same on every target and lives in shared references, which each child skill links to:

| Engine | Reference |
|---|---|
| MySQL, MariaDB | [engine-mysql.md](references/engine-mysql.md) |
| PostgreSQL | [engine-postgres.md](references/engine-postgres.md) |
| SQL Server (incl. Availability Groups) | [engine-sqlserver.md](references/engine-sqlserver.md) |
| Oracle | [engine-oracle.md](references/engine-oracle.md) |
| Db2 | [engine-db2.md](references/engine-db2.md) |
| Informix | [engine-informix.md](references/engine-informix.md) |
| MongoDB | [engine-mongodb.md](references/engine-mongodb.md) |

## Tools

Check what's installed first: [`scripts/check-tools.sh`](scripts/check-tools.sh) prints each tool's version and the install command for anything missing (`base` by default; `local`, `clients`, `aws`, `azure`, `gcp`, or `all`).

```bash
bash scripts/check-tools.sh all
```

| Group | Tools | macOS (Homebrew) |
|---|---|---|
| base | Databricks CLI, uv (runs every Python script, and downloads a Python if none fits), jq, yq (the Go one), DuckDB | `brew install databricks/tap/databricks uv jq yq duckdb` |
| local | Podman, Lima (local databases) | `brew install podman lima` |
| clients | psql, mysql, sqlcmd, mongosh (`sqlplus` runs inside the Oracle container) | `brew install libpq mysql-client sqlcmd mongosh`, then `export PATH="$(brew --prefix libpq)/bin:$(brew --prefix mysql-client)/bin:$PATH"` (both are keg-only) |
| aws / azure / gcp | the cloud CLIs, only for the clouds you use | `brew install awscli`, `brew install azure-cli`, `brew install --cask gcloud-cli` |

On Linux, the same tools: `curl -LsSf https://astral.sh/uv/install.sh \| sh` for uv, the [Databricks CLI install script](https://docs.databricks.com/aws/en/dev-tools/cli/install), and the distribution's packages (`apt install jq postgresql-client mysql-client podman`; yq and DuckDB as single binaries from their releases).

**Python: always `uv run --no-project --with <packages> python3 <script>.py`**, never `pip install` into a shared Python: uv makes a cached environment with exactly those packages (a Python too, if needed: `uv python install 3.12` to pin one). The skills' scripts say their packages in their first line.

**Identities are per resource.** Each resource records the identity it was created with (the create's `IDENTITY` line: Azure tenant and subscription, AWS profile and account, GCP account and project), and every later call for it passes that identity explicitly. Never rely on the CLI's current default: one login can reach several tenants, accounts and projects. Keep the values in your private env file, not in the repo.

## Rules for every target

1. **Ask before creating anything.** Cloud databases bill by the hour, and some (SQL Managed Instance, large VMs) cost a lot or take hours to create. Collect every input in one message, list every resource you'll create (server or instance, shared network pieces, firewall rules, parameter groups, the UC connection) with the expected cost class, and wait for one "yes".
2. **Check the cloud login first:** `aws sts get-caller-identity`, `az account show`, or `gcloud config list`. Never pick an account, subscription, or project for the user: show what's active and ask.
3. **Passwords:** generate them into a file (`umask 077; echo "Lfc1$(openssl rand -hex 12)" > admin.pw`: 28 characters with uppercase, lowercase and digits, starting with a letter, which meets SQL Server's and Azure SQL's policy of three of four character classes and Oracle's 30-byte limit; bare `openssl rand -hex` output is only lowercase and digits, and SQL Server refuses it), pass them to the CLI from the file, and never print them or paste them into the conversation. Delete the files once the UC connection exists, and tell the user where any remaining copy is. To keep a password in the OS keyring instead of a file (macOS Keychain, Windows Credential Manager, Linux Secret Service): `uv run scripts/pw.py new <name>` (or `import <name> <file>` for an existing file, then delete the file), and pass `keyring:<name>` wherever a script takes a password file (`ADMIN_PW_FILE`, `LOGIN_PW_FILE`, sync.sh's `password_file` column). Scripts read either through [`scripts/pw.sh`](scripts/pw.sh); a VM with no keyring keeps the private file.
4. **Reachability:** the database has to accept your laptop (for setup) and the workspace's serverless egress, plus its classic egress if any part of the pipeline runs on classic compute. Use **lfc-source-firewall** to find those IPs and add them. Open only the database port, never `0.0.0.0/0`, and say what you opened.
5. **Azure resource group:** ask whether the user has a resource group of their own. If not, use the team's shared test group, `<resource-group>` (check it exists: `az group show -n <resource-group>`); the user is free to choose a different one. Don't create a resource group without asking. Shared groups can run out of quota: if creation fails with a quota or limit error, ask for another group rather than removing other people's resources.
6. **Tag everything** you create (owner, purpose `lfc-test`, and a creation date), so it can be found and cleaned up later.
7. **Clean up when done**, or tell the user exactly what's still running and billing.
8. **Large test databases (tens of GB and up):** populate once, keep the stopped data directory in object storage in the workspace's cloud and region, and clone it per test on a cloud VM's XFS disk instead of every user loading their own. See **lfc-source-cloud-vm** `references/master-snapshots.md`.
9. **Shell and scripts:** ask every CLI for JSON and turn it into shell variables with `eval "$(<cli> ... -o json | yq -p json -o shell '{"NAME": (.path // "")}')"` (yq quotes the values, so `eval` is safe), not `--query ... -o tsv` or `awk`. Use the simplest tool per step: yq for one-input filters, plain shell for short list loops, and DuckDB SQL files (inputs via `getvariable()`, shared by shell and Python) only for IP-range math, joins across inputs, and ranking. See **shell-json-scripting** for the pattern and gotchas.

10. **Long waits** (a managed instance for hours, a VM or database for minutes, a large fill, a pipeline's first snapshot): start the operation without blocking (`--no-wait`, background task), then check for progress often at first and less often once it moves, by a signal that changes (a percent complete, rows, bytes, a state), not a fixed timer. A deadline is a reason to look, not to kill. See **watch-progress**.

## Names

Every name starts from a short owner prefix, so resources from different people never collide:

| Part | Rule | Example |
|---|---|---|
| `<pfx>` | The cloud identity's user name (email without the domain), lowercased, `.` / `_` → `-`, other characters dropped, at most 12 characters (shorten by dropping trailing vowels, then truncate) | `jane-doe` |
| `<service>` | Short engine name: `mysql`, `pg`, `mssql`, `ora`, `mysql-aurora`, `pg-aurora`, `mssql-mi` | `pg` |
| `<epoch>` | Unix seconds when the resource was first created, kept for the resource's lifetime | `1778687039` |

Server and instance names combine these, for example `<pfx>-<service>-<location>-<epoch>`. Each child skill gives the exact pattern its cloud allows (length limits, allowed characters, global uniqueness). Shared network pieces (security groups, subnet groups, parameter groups, firewall rules) use a shared prefix such as `lfc-test` instead of `<pfx>`, so every user reuses them.

**The Unity Catalog connection**, when the user doesn't name it, is `<firstname><lastname>-<engine>-<target>`, with `-2`, `-3` if taken (check `databricks connections list`):

- `<firstname><lastname>`: `name.givenName` + `name.familyName` from `databricks current-user me --profile <PROFILE>`, lowercased, letters and digits only (Jane Doe → `janedoe`); if those are empty, the part of `userName` before `@` without dots. This is the Databricks user, not the cloud identity behind `<pfx>`.
- `<engine>`: `mysql`, `pg`, `mssql`, `ora`; `<target>`: where the source runs, e.g. `azvm`, `awsvm`, `rds`, `flex`.
- Example: `janedoe-mysql-awsvm`. Pipelines on it use the connection name as their base (**databricks-lakeflow-connect**), without repeating the prefix.

## After the database exists

1. **Configure the engine** for the ingestion mode (engine reference above): the Lakeflow Connect user, grants, and CDC / change tracking / replication where needed.
   To fill it with test data, use **lfc-source-datagen**: rows are generated inside the database, so none cross the network. If the customer can't share data but can share statistics, use **lfc-source-statschema** to build a copy with their shape.
2. **Register the UC connection** from a request body in a private file, then delete it. `host` is the address serverless reaches (a public IP or DNS name, never `127.0.0.1`):

   ```bash
   umask 077
   cat > conn.json <<EOF
   {"name": "<connection>", "connection_type": "<POSTGRESQL|MYSQL|SQLSERVER|ORACLE>",
    "options": {"host": "<host>", "port": "<port>", "user": "<lfc_user>", "password": "$(cat lfc_user.pw)", "trustServerCertificate": "true"}}
   EOF
   databricks connections create --json @conn.json --profile <PROFILE> && rm conn.json
   ```

   - **A TLS error: trust the certificate, never turn TLS off.** When a connection fails on the server's certificate, set the trust option (below; in a TYPE JDBC connection's URL, the driver's own: `trustServerCertificate=true` for SQL Server, `sslmode=require` for PostgreSQL, `sslMode=REQUIRED` for MySQL), never `sslmode=disable`/`prefer`, `encrypt=false` or `useSSL=false`, which send the login in clear text. "The server does not support SSL" means the source has TLS off: turn it on at the source (PostgreSQL: `ssl = on` and a certificate, [engine-postgres.md](references/engine-postgres.md#tls-off-on-the-server)); no client option fixes that.
   - **A self-signed certificate** (a VM or container install): add `"trustServerCertificate": "true"` to `options`. SQL Server needs it for every query (otherwise `FAILED_JDBC.CONNECTION`); MySQL needs it for CDC (the extractor fails with `TLS_VALIDATION_FAILURE`), although query-based ingestion works without it. PostgreSQL with `verify-ca` needs it too (`TLS_CONNECTION_FAILURE`; [engine-postgres.md](references/engine-postgres.md)). Managed services (Azure SQL, RDS) have trusted certificates and don't need it.
   - **Oracle:** `user` in uppercase (`C##LFC_USER`), and `"service_name"` set to the root container's service, which is the CDB's name (`FREE` on Oracle Free; `lsnrctl status` lists the services). Integrated CDC refuses a connection without it. The docs' "`CDB$ROOT`" means that service, not the literal name: the listener doesn't serve `CDB$ROOT`, and queries fail. Pipelines reach the PDB through `source_catalog` (`FREEPDB1`). The gateway takes the base service name in `connection_parameters` (`{"source_catalog": "FREE"}`), not the literal `CDB$ROOT` its message names: [engine-oracle.md](references/engine-oracle.md). SQL through the connection (`remote_query`, step 3, sizing) runs in the connection's service and ignores a `service_name =>` argument, so for the PDB's tables register a second connection, `<firstname><lastname>-orapdb-<target>`, with `"service_name": "<PDB>"`.

3. **Verify** from serverless with a read-only `remote_query` through the connection. Write it to a file and run `databricks experimental aitools tools query --file ping.sql --profile <PROFILE> -o csv`:

   | Engine | `ping.sql` |
   |---|---|
   | MySQL | `SELECT * FROM remote_query('<connection>', query => "SELECT @@version AS v, @@log_bin AS log_bin")` |
   | PostgreSQL | `SELECT * FROM remote_query('<connection>', database => '<database>', query => "SELECT version() AS v")` |
   | SQL Server | `SELECT * FROM remote_query('<connection>', database => '<database>', query => "SELECT @@VERSION AS v")` |
   | Oracle | `SELECT * FROM remote_query('<PDB connection>', query => "SELECT sys_context('USERENV', 'CON_NAME') AS con, banner_full AS v FROM v$version")`: `con` must be the PDB |

   `FAILED_JDBC.CONNECTION ... Failed to connect to the database` means the network path (firewall, NSG not attached, wrong port) or, for SQL Server, an untrusted certificate. Check from the laptop first (`nc -z <host> <port>`): if that fails too, it's the firewall. More in Step 0 of `references/3-query-based-connectors.md` in **databricks-lakeflow-connect**.
4. **Hand off** to **databricks-lakeflow-connect** for the pipeline, its pre-flight checks, and the rows-per-run report. Gateway create: `spark_conf` can't set `spark.databricks.cluster.profile` or `spark.master` (refused: "The Gateway pipeline does not allow spark config [...]"); `num_workers: 0` is accepted (tested 2026-10-25, SQL Server 2022). A zero-worker gateway on classic compute never completed an update, so check the classic egress first (**lfc-source-firewall**).
5. **Record the results** of every test you ran (passes, failures, and cells the workspace refused) with **lfc-test-results**, and check it first for what's already been tested.

## Lifecycle

| Action | Where |
|---|---|
| Stop / start (to stop billing compute) | Each child skill; not every target supports it |
| Delete | Each child skill. Delete the UC connection too (`databricks connections delete <connection>`). Drop any Postgres replication slot first; a slot the pipeline named stays after the delete, so drop it by hand if you forgot (tested 2026-10-25). Deleting an integrated MySQL pipeline drops its destination tables (tested 2026-10-25): copy them out first |
| Find leftovers | List resources by the owner tag and `<pfx>` in each cloud |

Each child skill is a plain-CLI recipe (`aws`, `az`, `gcloud`, SQL clients, and the Databricks CLI), derived from provisioning logic that's been used to stand up these sources for Lakeflow Connect testing.
