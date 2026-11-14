#!/usr/bin/env bash
# workload.sh --model <model> --sf <scale> --dialect <tsql|postgres> --out <dir> [--spec]: write a workload for a statschema model (schemas/<model>.yaml,
# the TPC models) shaped at a scale: <dir>/model.yaml (shape.py, checked against model format 1), <dir>/model.sql (ddl.py for the
# dialect), and <dir>/workload.json (workload contract 1, lfc-workload-contract), whose fill_cmd and bundle_cmd are this folder's
# copies.sh and bundle.sh. Prints the workload file's path. Needs uv (shape.py and ddl.py run through it). Exit 2 with one line
# when a step fails; the workload file is checked by the contract's workload-env.sh before the path is printed.
#   --spec: TPC-H or TPC-B as tpc/tpc-schema.sh writes them from the spec (keys, prices, and dates across tables: ship = order
#   + 1..121), read back by ddl-to-schema.py, in place of the statschema model; TPC-H also gets <dir>/verify.sql (the spec's
#   checks, tpc/tpch-verify.sql, for the dialect: postgres so far), named in the workload file's verify.
#   bash skills/lfc-source-datagen/scripts/workload.sh --model tpcc --sf 0.01 --dialect tsql --out /abs/work/tpcc
#   bash skills/lfc-source-datagen/scripts/workload.sh --model tpch --sf 0.1 --dialect postgres --spec --out /abs/work/tpch
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); SK=$HERE/../..
SCH=$SK/lfc-source-statschema; CON=$SK/lfc-workload-contract/scripts
MODEL= SF= DIALECT= OUT= SPEC=
die() { echo "workload.sh: $*" >&2; exit 2; }
usage() { sed -n 2,5p "$0" >&2; exit 2; }

while [ $# -gt 0 ]; do
  [ "$1" = --spec ] && { SPEC=1; shift; continue; }
  [ $# -ge 2 ] || usage
  case $1 in
    --model) MODEL=$2 ;;
    --sf) SF=$2 ;;
    --dialect) DIALECT=$2 ;;
    --out) OUT=$2 ;;
    *) usage ;;
  esac
  shift 2
done
[ -n "$MODEL" ] && [ -n "$SF" ] && [ -n "$DIALECT" ] && [ -n "$OUT" ] || usage

case $MODEL in *[!a-z0-9_]*|"") die "--model must be a lowercase name: $MODEL" ;; esac
if [ -n "$SPEC" ]; then
  case $MODEL in tpch|tpcb) ;; *) die "--spec: tpch or tpcb (the models tpc-schema.sh writes from the spec): $MODEL" ;; esac
else
  [ -r "$SCH/schemas/$MODEL.yaml" ] || die "no model $MODEL (no schemas/$MODEL.yaml in lfc-source-statschema)"
fi
case $SF in *[!0-9.]*|""|.*|*.|*.*.*) die "--sf must be a number such as 0.01 or 10: $SF" ;; esac
awk -v s="$SF" 'BEGIN { exit !(s + 0 > 0) }' || die "--sf must be above 0: $SF"
# the fill command is copies.sh, which fills SQL Server (ms) and PostgreSQL (pg); other dialects get their DDL from ddl.py alone
case $DIALECT in
  tsql) ENGINE=ms ;;
  postgres) ENGINE=pg ;;
  *) die "--dialect $DIALECT: only tsql and postgres have a fill command so far (copies.sh fills SQL Server and PostgreSQL)" ;;
esac
command -v uv > /dev/null 2>&1 || die "needs uv (shape.py and ddl.py run through it)"

mkdir -p "$OUT" || die "can't make $OUT"
OUT=$(cd "$OUT" && pwd)
case $OUT$HERE in *" "*) die "the paths of the output folder and of this skill can't have a space (the workload file's paths are words)" ;; esac
TMPD=$(mktemp -d); trap 'rm -rf "${TMPD:?}"' EXIT

if [ -n "$SPEC" ]; then
  bash "$HERE/tpc/tpc-schema.sh" "$MODEL" "$SF" 2> "$TMPD/spec.err" | uv run -q "$SCH/scripts/ddl-to-schema.py" --dialect mysql > "$OUT/model.yaml" 2>> "$TMPD/spec.err" \
    || die "tpc-schema.sh | ddl-to-schema.py failed: $(tail -2 "$TMPD/spec.err" | tr '\n' ' ')"
else
  uv run -q "$SCH/scripts/shape.py" "$SCH/schemas/$MODEL.yaml" --sf "$SF" --expected "$TMPD/expected.json" > "$OUT/model.yaml" 2> "$TMPD/shape.err" \
    || die "shape.py failed: $(tail -2 "$TMPD/shape.err" | tr '\n' ' ')"
fi
python3 "$CON/validate-model.py" "$OUT/model.yaml" > "$TMPD/model.err" \
  || die "model.yaml breaks model format 1: $(head -3 "$TMPD/model.err" | tr '\n' ' ')"
uv run -q "$SCH/scripts/ddl.py" "$OUT/model.yaml" --dialect "$DIALECT" > "$OUT/model.sql" 2> "$TMPD/ddl.err" \
  || die "ddl.py failed: $(tail -2 "$TMPD/ddl.err" | tr '\n' ' ')"
case $DIALECT in
  tsql) TB=$(grep -o -E '^CREATE TABLE \[[A-Za-z_0-9]+\]' "$OUT/model.sql" | sed -E 's/.*\[(.*)\]/\1/') ;;
  postgres) TB=$(grep -o -E '^CREATE TABLE "?[A-Za-z_0-9]+"?' "$OUT/model.sql" | sed -E 's/^CREATE TABLE "?([A-Za-z_0-9]+)"?/\1/') ;;
esac
[ -n "$TB" ] || die "no CREATE TABLE in $OUT/model.sql"
TABLES=$(printf '%s\n' "$TB" | jq -R -s -c 'split("\n") | map(select(length > 0))')
# the spec's checks for the dialect: rows "bad: <what>|<count>|..." (each count must be 0), unqualified names
VERIFY=
if [ -n "$SPEC" ] && [ "$MODEL" = tpch ] && [ "$DIALECT" = postgres ]; then
  sed -E 's/DATEDIFF\(([^,]+), ([^)]+)\)/(\1 - \2)/g' "$HERE/tpc/tpch-verify.sql" > "$OUT/verify.sql" || die "can't write $OUT/verify.sql"
  VERIFY=$OUT/verify.sql
fi

jq -n -c --arg model "$MODEL" --argjson scale "$SF" --arg dialect "$DIALECT" --argjson tables "$TABLES" \
  --arg ddl "$OUT/model.sql" --arg fill "bash $HERE/copies.sh $ENGINE" --arg bundle "bash $HERE/bundle.sh" --arg verify "$VERIFY" \
  '{contract: 1, model: $model, scale: $scale, dialect: $dialect, tables: $tables, ddl: $ddl, fill_cmd: $fill, bundle_cmd: $bundle}
   + (if $verify != "" then {verify: $verify} else {} end)' \
  > "$OUT/workload.json" || die "can't write $OUT/workload.json"
bash "$CON/workload-env.sh" "$OUT/workload.json" > /dev/null || die "the workload file fails its contract (the line above)"
echo "workload.sh: $MODEL${SPEC:+ (spec)} at SF $SF, $(printf '%s\n' "$TB" | wc -l | tr -d ' ') tables, $DIALECT" >&2
echo "$OUT/workload.json"
