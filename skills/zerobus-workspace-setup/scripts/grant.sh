#!/usr/bin/env bash
# grant.sh <profile> --table <catalog.schema.table> --sp <application id> [--apply]
#   Grant a service principal what a Zerobus producer needs, directly on each securable: USE CATALOG, USE SCHEMA, and
#   SELECT and MODIFY on the table (inherited grants may not satisfy the table-scoped token: error 4024). Run as the
#   table's owner or someone with MANAGE. Prints the plan; changes nothing without --apply. Uses the Unity Catalog
#   REST API, so no SQL warehouse is needed.
set -uo pipefail
P=$1; shift; TABLE=; SP=; APPLY=no
while [ $# -gt 0 ]; do case $1 in --table) TABLE=$2; shift 2;; --sp) SP=$2; shift 2;; --apply) APPLY=yes; shift;; *) echo "unknown $1"; exit 2;; esac; done
[ -n "$TABLE" ] && [ -n "$SP" ] || { echo "--table and --sp are required"; exit 2; }
IFS=. read -r CAT SCH TBL <<< "$TABLE"
for g in "catalog $CAT USE_CATALOG" "schema $CAT.$SCH USE_SCHEMA" "table $TABLE SELECT,MODIFY"; do set -- $g
  have=$(databricks grants get "$1" "$2" --principal "$SP" --profile "$P" -o json 2>/dev/null | yq -p json -o tsv '[.privilege_assignments[]?.privileges[]?] | unique | join(",")')
  add=$(echo "$3" | tr , '\n' | while read -r p; do case ",$have," in (*",$p,"*|*",ALL_PRIVILEGES,"*) ;; (*) printf '%s ' "$p";; esac; done)
  [ -z "$add" ] && { printf '%-7s %-50s already: %s\n' "$1" "$2" "$have"; continue; }
  printf '%-7s %-50s grant: %s\n' "$1" "$2" "$add"
  [ $APPLY = yes ] || continue
  J=$(printf '{"changes": [{"principal": "%s", "add": [%s]}]}' "$SP" "$(for p in $add; do printf '"%s",' "$p"; done | sed 's/,$//')")
  databricks grants update "$1" "$2" --json "$J" --profile "$P" >/dev/null && echo "        done" || echo "        refused: run as the owner, or with MANAGE"
done
HOST=$(databricks auth describe --profile "$P" -o json 2>/dev/null | jq -r '.details.host // ""' | sed 's:/*$::')
SPID=$(databricks service-principals list --filter "applicationId eq \"$SP\"" --profile "$P" -o json 2>/dev/null | jq -r '.[0].id // ""')
echo "LINK table $TABLE (Permissions tab): $HOST/explore/data/$CAT/$SCH/$TBL"
[ -n "$SPID" ] && echo "LINK service principal $SP: $HOST/settings/workspace/identity-and-access/service-principals/$SPID"
[ $APPLY = yes ] || echo "plan only: run again with --apply"
