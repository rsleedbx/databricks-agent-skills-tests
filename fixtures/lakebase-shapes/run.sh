#!/usr/bin/env bash
# Product-level shape test (no agent): create the shape pipelines from pipelines/*.json, run them,
# apply mutate.sql, run again, and print verify.sql. See README.md for the expected results.
#
# Usage (after `LFC_ENV=<name> source env.sh` and seeding, see README.md):
#   fixtures/lakebase-shapes/run.sh
#
# Needs: LFC_CATALOG, LFC_SHAPES_CONNECTION, LFC_LAKEBASE_PROJECT, LFC_NAME_PREFIX, PIPELINE_WS

set -e
D="$(cd "$(dirname "$0")" && pwd)"
P="--profile ${PIPELINE_WS:?}"
OUT="${RESULTS_ROOT:-/tmp}/shapes"; mkdir -p "$OUT"
fill() { perl -pe 's/\$\{(\w+)\}/defined $ENV{$1} ? $ENV{$1} : die "unset variable $1\n"/ge' "$1"; }

for s in shapes_scd1 shapes_scd2 shapes_append shapes_probe; do
  databricks schemas create "$s" "$LFC_CATALOG" $P >/dev/null 2>&1 || true
done
: > "$OUT/ids.txt"
for k in scd1 scd2 append probe_scd1_nokey probe_scd2_nokey_dups probe_nokey_nocursor; do
  fill "$D/pipelines/$k.json" > "$OUT/$k.json"
  if id=$(databricks pipelines create --json @"$OUT/$k.json" $P -o json 2>"$OUT/$k.err" | jq -r .pipeline_id); then
    echo "$k $id" >> "$OUT/ids.txt"; echo "created $k $id"
  else
    echo "create FAILED $k: $(head -2 "$OUT/$k.err")"
  fi
done

run_all() {
  while read -r k id; do echo "$k $id $(databricks pipelines start-update "$id" $P -o json | jq -r .update_id)"; done < "$OUT/ids.txt" > "$OUT/updates.txt"
  while :; do
    left=0; line=""
    while read -r k id u; do
      st=$(databricks pipelines get-update "$id" "$u" $P -o json | jq -r .update.state)
      line="$line $k=$st"; case $st in COMPLETED|FAILED|CANCELED) ;; *) left=1;; esac
    done < "$OUT/updates.txt"
    [ $left = 0 ] && { echo "$(date -u +%H:%M:%S)$line"; break; }; sleep 20
  done
}

echo "### run 1"; run_all
echo "### mutate"; databricks psql --project "$LFC_LAKEBASE_PROJECT" $P -- -X -q -v ON_ERROR_STOP=1 -f "$D/mutate.sql"
echo "### run 2"; run_all
fill "$D/verify.sql" > "$OUT/verify.sql"
databricks experimental aitools tools query --file "$OUT/verify.sql" $P -o csv
echo "pipeline ids: $OUT/ids.txt (delete them when done)"
