---
name: lfc-connection-sync
description: "Create or update the same Unity Catalog connections in many Databricks workspaces from one private targets file, and list every workspace's connections. Use when a set of test sources must be reachable from several workspaces (AWS, Azure, GCP; serverless and classic), or to see which workspaces already have which connections."
parent: lfc-source-setup
---

# Sync UC connections across workspaces

A source database is often tested from several workspaces: one per cloud, serverless and classic, sandbox and dogfood. Each workspace needs its own Unity Catalog connection to the same source. This skill keeps them identical: one private targets file, one idempotent script, and an inventory to check what exists.

The connection options per engine (`trustServerCertificate`, Oracle's `service_name`, the uppercase Oracle user) are in **lfc-source-setup** step 2. The firewall must allow each workspace's serverless ranges (and classic egress, for classic gateways): **lfc-source-firewall**, with `SCOPE=all` when the source serves workspaces in many regions or clouds.

## Targets

One tab-separated row per connection per workspace. The file names password files; it never holds a password. Keep it private (`umask 077`) and out of any repository:

```text
# profile	name	connection_type	host	port	user	password_file	extra options (JSON)
ws-azure	janedoe-pg-azvm	POSTGRESQL	203.0.113.10	30002	lfc_user	~/.lfc-test/src/pg.lfc.pw
ws-aws	janedoe-pg-azvm	POSTGRESQL	203.0.113.10	30002	lfc_user	~/.lfc-test/src/pg.lfc.pw
ws-aws	janedoe-mssql-azvm	SQLSERVER	203.0.113.10	30003	lfc_user	~/.lfc-test/src/mssql.lfc.pw	{"trustServerCertificate": "true"}
ws-aws	janedoe-ora-azvm	ORACLE	203.0.113.10	30004	C##LFC_USER	~/.lfc-test/src/ora.lfc.pw	{"service_name": "FREE"}
ws-aws	janedoe-my-azvm	MYSQL	203.0.113.10	30005	lfc_user	keyring:my-lfc
```

Use the default names (`<firstname><lastname>-<engine>-<target>`, **lfc-source-setup** "Names") so a connection's name says which source it reaches, whatever the workspace.

## Sync

```bash
S=skills/lfc-connection-sync/scripts
python3 $S/sync.py targets.tsv
```

[`sync.py`](scripts/sync.py) creates a connection that doesn't exist and updates one that does (with every option, so a changed password or host is applied), and prints one line per row: `created`, `updated`, or `FAILED <class>: <error>`. Rerunning is safe. Tested on an Azure serverless-only and an AWS classic workspace: first run `created` / `updated`, rerun `updated` everywhere.

| Class | Meaning | Fix |
|---|---|---|
| `no_create_connection`, `permission` | The user lacks `CREATE CONNECTION` on the metastore (create) or ownership / `MANAGE` (update) | Ask the workspace or metastore admin; skip that workspace |
| `connection_limit` | The metastore's connection quota is used up | Delete stale connections (inventory below), or use another workspace |
| `unsupported_option` | An option the connection type doesn't take (e.g. `database` on `SQLSERVER`) | Fix the row's options |
| `auth` | The profile's login expired, or the token is for another account | Log in again (below) |

Then check each connection from that workspace's serverless compute: **lfc-source-setup** step 3 (`remote_query`). A `FAILED_JDBC.CONNECTION` there is the network path or the source being down, not the connection object.

## Inventory

```bash
python3 $S/inventory.py ws-azure ws-aws > inventory.tsv     # profile, name, type, host, port, owner, created_at
grep janedoe- inventory.tsv | cut -f1-5 | column -t       # yours
```

[`inventory.py`](scripts/inventory.py) lists every connection in each workspace (the API never returns passwords). Use it before a sync (what exists, what would collide with someone else's name), after one (every workspace has every row), and to find stale connections to delete. Before sharing the output, remember it holds hosts and other people's connection names: keep it out of public repositories.

## Logging in to many workspaces

Every workspace needs a CLI profile. Two ways:

- **One login per workspace:** `databricks auth login --host <workspace-url> --profile <name>`. Works everywhere; slow for many workspaces.
- **One account login, a token per workspace** ([account-token-fallback](../account-token-fallback/SKILL.md) does this automatically when a workspace login has expired): log in to the account once (`databricks auth login --host <accounts-url> --account-id <account-id> --profile <account-profile>`), then mint a token and use it for any workspace in that account: `export DATABRICKS_HOST=<workspace-url> DATABRICKS_TOKEN=$(databricks auth token --host <accounts-url> --account-id <account-id> --profile <account-profile> -o json | jq -r '.access_token')`. Some accounts reject account tokens on workspace APIs (`invalid access token` or 401 on the first call): use a workspace login for those. The same hostname pattern can belong to different accounts, so pick the account by the workspace's account ID (`databricks metastores summary` shows the metastore; the account console shows the workspace's account), not by its URL.

Never print a token; keep it in the environment of the command that uses it.
