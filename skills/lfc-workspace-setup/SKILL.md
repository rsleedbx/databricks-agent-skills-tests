---
name: lfc-workspace-setup
description: "Prepare a Databricks workspace for Lakeflow Connect tests: check and enable the connector previews through the Settings API, pick a target catalog that works for CDC, and let other people log in and use the catalog (workspace assignment, catalog binding, grants). Use on a new test workspace, or when a pipeline is refused because a connector or a catalog isn't available."
parent: lfc-source-setup
---

# Prepare a workspace for Lakeflow Connect

A fresh workspace usually needs three things before Lakeflow Connect tests run: the connector previews on, a catalog the pipelines can write to, and access for the other people testing in it. Each is a few API calls; this skill lists them, with what was checked on an Azure serverless-only and an AWS classic workspace.

## The one-command check: `ready.sh`

```bash
bash scripts/ready.sh [<PROFILE>] [--catalog <catalog>] [--principal "account users"] [--egress-file <file>] [--no-enable]
```

It checks, in order: login; the metastore's cloud and region (put the sources there); serverless and classic compute; the connector previews (step 1); a target catalog (step 2); access for you and `--principal` through `check.sh` (step 4); the serverless egress ranges for the source's firewall ([lfc-source-firewall](../lfc-source-firewall/SKILL.md)); the connections already here. One line each: `PASS`, `FAIL`, `WARN`, `INFO` or `ASK`, with the next command. Without `<PROFILE>` or `--catalog`, it uses the saved settings (**lfc-test-settings**: `DATABRICKS_PROFILE`, `LFC_CATALOG`). `ASK` means stop and ask the user (no writable target catalog). Exit 1 on any `FAIL` or `ASK`.

- The one change it makes: as a workspace admin, it turns on a connector preview that's off. `--no-enable` makes it read-only.
- `--egress-file <file>` keeps the egress list; a later run shows the ranges added and removed as a `WARN`. Rerun at least monthly.

## 1. Connector previews (Settings API)

Each Lakeflow Connect connector in preview is a workspace setting. The CLI's `workspace-settings-v2` commands list, read and change them (workspace admin); they page through the list themselves:

```bash
P=<PROFILE>
databricks workspace-settings-v2 list-workspace-settings-metadata --profile $P -o json \
  | yq -p json -o tsv '.[] | select(((.display_name // "") + " " + (.description // "")) | downcase | (test("lakeflow connect") or (test("join pushdown") and test("federat")))) | [.name, .display_name]'
databricks workspace-settings-v2 get-public-workspace-setting integrated_cdc_oracle_connector --profile $P -o json \
  | jq -r 'if has("boolean_val") then .boolean_val.value else .effective_boolean_val.value end'
databricks workspace-settings-v2 patch-public-workspace-setting integrated_cdc_oracle_connector \
  --json '{"name": "integrated_cdc_oracle_connector", "boolean_val": {"value": true}}' --profile $P                 # enable
```

The same calls work through `databricks api get|patch /api/2.1/settings-metadata` and `/api/2.1/settings/<name>`; there, the list's `page_token` is the next setting's name, passed as is.

- About 65 of 275 settings match on both test workspaces. The ones that matter for database sources: `integrated_cdc_sql_server_connector`, `integrated_cdc_mysql_connector`, `integrated_cdc_oracle_connector` (integrated CDC per engine), `direct_cdc_connector`, `conn_cdc_col_select` (column selection for database sources), and `ds_v2_join_pushdown` (join pushdown for federated queries, off by default).
- A value is either `boolean_val` (set on this workspace) or `effective_boolean_val` (inherited). Read both; don't use a "first non-null" shortcut, because `false` counts as missing in `yq` and `jq` `//`.
- Some settings aren't writable through the public API yet ("not enabled for public api"): leave them and say so.
- **Not a setting:** standard CDC (gateway) per engine. A workspace without it refuses the gateway with `Ingestion source '<ENGINE>' is not supported in your workspace`; only the Databricks account team can enable it. Integrated CDC was enabled through its setting on both test workspaces while the MySQL and PostgreSQL gateways stayed refused.

## 2. A target catalog

Query-based pipelines can write to any catalog. CDC pipelines (integrated and standard) need one with its **own managed storage**, not Default Storage, and a working storage credential: see **databricks-lakeflow-connect** SKILL.md, "A Unity Catalog target". Check a candidate:

```bash
databricks catalogs get <catalog> --profile $P -o json | yq -p json -oy '{"isolation": .isolation_mode, "storage_root": .storage_root}'
databricks external-locations get __databricks_managed_storage_location --profile $P -o json | yq -p json .url   # Default Storage starts with this
```

The workspace's default catalog (`databricks settings default-namespace get --profile $P -o json`, `.namespace.value`) can be a candidate, but check it: on one test workspace it was `system`, which pipelines can't write to (nor to `hive_metastore`). Otherwise create one with its own managed location, or use one the workspace's provisioning made for you (check `catalogs list` for catalogs where you have `ALL_PRIVILEGES`).

Some workspaces delete a catalog on the date in its `RemoveAfter` tag, and give an untagged catalog a date a couple of weeks out. [`keep-catalogs.sh`](scripts/keep-catalogs.sh) sets the tag a month out (never earlier than it is) on the catalogs named, and on each `--connection` and its foreign catalogs (connections take tags too, through `entity-tag-assignments ... connections`); `bin/run_batch.sh` runs it on `$LFC_CATALOG` and the env's connections, and `lfc-connection-sync` after each connection:

```bash
bash scripts/keep-catalogs.sh $P <catalog> --connection <connection>   # KEEP_UNTIL=YYYY-MM-DD for another date
```

## 3. Access for everyone testing in the workspace

Three separate grants, in this order: logging in (an assignment plus entitlements), seeing the catalog, using it.

**Log in: workspace assignment.** Assign the account-wide group `account users` (everyone in the workspace's Databricks account), or an account group (not a workspace-local group). A **workspace admin** can do it from the workspace itself, with no account login: the workspace answers account SCIM queries for its own account, and has its own assignment API:

```bash
P=<PROFILE>; G="account users"                                                     # or an account group's name
q=$(printf 'displayName eq "%s"' "$G" | jq -sRr @uri)
ID=$(databricks api get "/api/2.0/account/scim/v2/Groups?filter=$q" --profile $P -o json | jq -r '.Resources[0].id')
databricks api get /api/2.0/preview/permissionassignments --profile $P -o json \
  | yq -p json -o tsv '.permission_assignments[] | [(.principal.display_name // .principal.user_name), (.permissions | join(","))]'   # who's in now
databricks api put /api/2.0/preview/permissionassignments/principals/$ID --json '{"permissions": ["USER"]}' --profile $P
```

**Assignment alone doesn't let anyone in.** What members can do comes from **entitlements**: `workspace-access` to open the workspace, `databricks-sql-access` for SQL, `allow-cluster-create` and so on. Members of `ADMIN` principals get them all through the built-in `admins` group; `USER` principals get only what the built-in `users` group or their own group has, and that's often nothing. Check, then give the entitlements to the assigned group itself:

```bash
q=$(printf 'displayName eq "%s"' users | jq -sRr @uri)
databricks api get "/api/2.0/preview/scim/v2/Groups?filter=$q&attributes=entitlements" --profile $P -o json \
  | jq -r '[.Resources[0].entitlements[]?.value] | join(", ")'                # empty: USER members get nothing
cat > ent.json <<'EOF'
{"schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
 "Operations": [{"op": "add", "path": "entitlements", "value": [{"value": "workspace-access"}, {"value": "databricks-sql-access"}]}]}
EOF
databricks api patch /api/2.0/preview/scim/v2/Groups/$ID --json @ent.json --profile $P     # $ID: the assigned account group
```

Tested on an AWS and an Azure sandbox workspace: the built-in `users` group had no entitlements, and changing it was refused (`Entitlement modifications on the workspace-users and admins groups are not permitted for this workspace`). Assigning `account users` as `USER` and patching the entitlements onto `account users` itself worked (both accounts had fewer than 10,000 identities, see below): everyone in the account can open the workspace and use SQL without being an admin. Workspaces shared this way otherwise assign named groups as `ADMIN`, which works because `admins` carries every entitlement, but makes every member an admin.

An account admin can do the same from the account: `databricks account workspace-assignment update <workspace-id> <principal-id> --json '{"permissions": ["USER"]}' --profile <account-profile>`. `ADMIN` instead of `USER` makes every member a workspace admin.

**A workspace holds at most 10,000 users and service principals** ([docs](https://docs.databricks.com/aws/en/admin/users-groups/); also at most 5,000 groups). Assigning a group adds **all its members, nested ones included**, and the total of everything assigned counts. So `account users` works only in an account with fewer than 10,000 identities. In a larger account the assignment fails with `Cannot add group <id> to workspace: member count <n> exceeds workspace limit 10000` (seen with 44,618). Assign subgroups instead, for example the teams that test in the workspace, whose combined membership stays under the limit, and give those groups the entitlements. The account-level limit (also 10,000) can be raised, which is why large accounts exist; the per-workspace limit is the one that bites. Check the size first: `databricks api get "/api/2.0/account/scim/v2/Users?count=1&attributes=id" --profile $P -o json | yq -p json .totalResults`.

Groups belong to an account, and test workspaces often sit in different accounts: a team group present in one account can be missing in another (seen: present in an AWS sandbox's account, missing in an Azure sandbox's). Check with the SCIM query above before assigning; `account users` exists in every account.

**A workspace shared by many teams**, for comparison (read with the same calls): about 150 assignments, all groups and service principals, with broad organization groups as `ADMIN` and an all-users group as `USER`; its shared catalogs `OPEN`, with `USE CATALOG`, `USE SCHEMA` and read or read-write privileges granted to `account users`. A fresh single-owner test workspace has only its owner assigned and its catalog `ISOLATED` with no grants: nobody else can log in or see the data until both steps here are done.

**See the catalog: binding.** An `ISOLATED` catalog is visible only in the workspaces it's bound to; an `OPEN` one in every workspace on its metastore. Keep test catalogs `ISOLATED` and bind each workspace that needs one (from a workspace where the catalog is already visible):

```bash
databricks workspace-bindings get <catalog> --profile $P -o json        # the workspaces it's bound to
databricks workspace-bindings update <catalog> --json '{"assign_workspaces": [<workspace-id>]}' --profile $P
```

Bindings are per workspace, not per user, and never cross metastores: a catalog can be bound only to workspaces on its own metastore, which usually means the same cloud, region, and account. Bind the catalog's storage credential and external location the same way if people create tables or volumes in it.

**Use the catalog: grants**, to the same principal:

```sql
GRANT USE CATALOG, USE SCHEMA, CREATE SCHEMA, SELECT, MODIFY, CREATE TABLE ON CATALOG <catalog> TO `account users`;
GRANT USE CONNECTION ON CONNECTION <connection> TO `account users`;          -- for pipelines on a shared connection
```

`MANAGE` on the catalog also lets members bind more workspaces; leave it out if they shouldn't. Catalog privileges don't give workspace login, and workspace login doesn't show an `ISOLATED` catalog that isn't bound: all three are needed.

## 4. Check and grant per principal

What a user, a group, or `account users` needs to run Lakeflow Connect in a workspace (from the public pipeline docs, plus what the tests showed):

| Where | Needs | For |
|---|---|---|
| Workspace | Unity Catalog; the connector's preview on (step 1) | every mode |
| Login | an assignment (directly or through a group) and `workspace-access`; `databricks-sql-access` for SQL and `remote_query` | every mode |
| Metastore | `CREATE CONNECTION` to create connections (else use existing ones); `CREATE CATALOG` for foreign catalogs | creating connections; QBC through a foreign catalog |
| Connection | `USE CONNECTION` (or owning it); `CREATE FOREIGN CATALOG` for a foreign catalog | every pipeline on it |
| Target catalog | `USE CATALOG`, `USE SCHEMA`, `CREATE SCHEMA` (or an existing schema with `CREATE TABLE`), `CREATE TABLE`; visible here (bound, if `ISOLATED`) | every mode |
| CDC | `CREATE VOLUME` (staging), on a catalog with its own storage (step 2) | integrated and standard CDC |
| Compute | serverless network path to the source (**lfc-source-firewall**); for a classic gateway, cluster creation or a cluster policy | |

Two scripts, run as a workspace admin:

```bash
S=skills/lfc-workspace-setup/scripts
bash $S/check.sh <PROFILE> "<user-email | group | account users>" --catalog <catalog> --connection <connection> --modes qbc,icdc,fcqbc
bash $S/grant.sh <PROFILE> "<principal>" --level cdc --catalog <catalog> --connection <connection> [--foreign-catalogs] [--create-connections]   # the plan
bash $S/grant.sh ... --apply                                                    # make the changes, then run check.sh again
```

[`check.sh`](scripts/check.sh) is read-only. It prints one line per requirement (`PASS`, `FAIL`, `WARN`) with the command or SQL that fixes each failure, and exits 1 if anything fails. It works on effective privileges, so grants that come through a group count, and it counts ownership: owning a securable gives every privilege on it, but Unity Catalog's effective-privileges API doesn't report it. On both test sandboxes the metastore's owner was `account users`, so everyone in the account could create connections and catalogs, while the target catalog still needed grants. [`grant.sh`](scripts/grant.sh) gives exactly the chosen level: `login` (assignment and entitlements), `qbc` (plus connection and catalog privileges), `cdc` (plus `CREATE VOLUME`). It prints its plan and changes nothing without `--apply`. Grants go through the Unity Catalog REST API, so no SQL warehouse is needed.

**Run as a non-admin**, the workspace returns no assignments and the account directory hides other people's groups, so `check.sh` doesn't count "can't read" as "no": checking yourself, being logged in proves the assignment and `workspace-access`, and your groups come from `/api/2.0/preview/scim/v2/Me`; anything it can't see is a `WARN` ("not visible to a non-admin"), never a `FAIL`. Run it as a workspace admin to check other principals.

Tested on an Azure serverless-only and an AWS classic sandbox. A workspace admin who owns the connection and the catalog passed every check. `account users` passed login (after step 3) and the metastore checks (as the metastore's owner), and failed exactly the connection and catalog grants, each with its fix. `grant.sh --apply` for a user went through every call.

