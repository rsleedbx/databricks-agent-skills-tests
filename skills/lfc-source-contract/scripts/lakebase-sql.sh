#!/usr/bin/env bash
# lakebase-sql.sh [database]: a contract's sql_cmd for a Lakebase (Postgres) source: SQL from stdin, run as the
# project owner (your Databricks login) through `databricks psql`, so a Lakebase project stands in for a customer's
# PostgreSQL in every test that takes a source contract. No password: the CLI mints an OAuth token per call.
#   Needs LB_PROJECT (the Lakebase project id) and LB_PROFILE (the CLI profile of its workspace). Optional LB_BRANCH
#   (default production). The database argument picks the database; `postgres` (the server-level name lib.sh passes)
#   means Lakebase's default database, databricks_postgres. Output: rows tab-separated, no headers, as sql-client.sh.
#   Arguments after the database go to psql after these, so a caller can change them (lfc-source-datagen's fill.sh,
#   which reads | between fields: PSQL="lakebase-sql.sh <db> -F |").
#   Each call renews an expired workspace login from your account login when it can (account-token-fallback
#   ws-token.sh, about 1.6 s; nothing when the profile's own login works), so a run longer than the token's hour goes on.
#   In a contract: "sql_cmd": "env LB_PROJECT=lfc-demo LB_PROFILE=<profile> /abs/path/lakebase-sql.sh"
set -uo pipefail
: "${LB_PROJECT:?set LB_PROJECT to the Lakebase project id}" "${LB_PROFILE:?set LB_PROFILE to the workspace profile}"
DB=${1:-databricks_postgres}; [ "$DB" = postgres ] && DB=databricks_postgres
WS=$(cd "$(dirname "$0")/../../account-token-fallback/scripts" 2>/dev/null && pwd)/ws-token.sh
if [ -x "$WS" ] || [ -f "$WS" ]; then CFG=$(bash "$WS" "$LB_PROFILE" 2>/dev/null) && [ -n "$CFG" ] && export DATABRICKS_CONFIG_FILE=$CFG; fi
BR=()
[ -n "${LB_BRANCH:-}" ] && BR=(--branch "$LB_BRANCH")
# the CLI's own lines (which project, branch, endpoint; "Error: exit status N") go; SQL errors stay on stderr
exec databricks psql --project "$LB_PROJECT" ${BR[@]+"${BR[@]}"} --profile "$LB_PROFILE" -- \
  -d "$DB" -v ON_ERROR_STOP=1 -qtAX -F $'\t' "${@:2}" \
  2> >(grep -v -E '^(Connecting to |Project: |Branch: |Endpoint: |Error: exit status [0-9]+$)' >&2)
