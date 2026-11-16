# tpc-schema.sh <tpcb|tpcc|tpch|tpcds|tpce|tpcdi> [SF]: prints the workload's tables as MySQL DDL, with the lfc tags in the comments
# (cast.sql): lfc:rows= target, lfc:v={...} a column's value as an integer formula in x.n, lfc:n={...} n from a row (for
# composite keys, key formulas, and tables without a key), lfc:by={...} the column n rises with. Keys, foreign keys, and
# checks are the spec's; a table the spec gives no primary key has none. lfc-source-statschema's ddl-to-schema.py turns
# it into schema.yaml, and its ddl.py into any engine's DDL.
#   TPC-B and TPC-H are written from the spec (lfc_source_datagen/tpc_schema.py, run from here). TPC-C, TPC-DS, TPC-E, and TPC-DI come from statschema's canonical schemas
#   (schemas/<workload>.yaml): shape.py sets each table's rows at SF (row_count_per_sf x SF; fixed tables stay as they are)
#   and writes the tags, ddl.py writes MySQL. A text column's word-list tag (lfc:e) longer than MySQL's 1,024-character
#   comment goes without its tag: the planner's default text cast, as for TPC-H's text columns (TPC-DS: 11 columns, TPC-E: 7),
#   so the DDL has no side table (ddl.py's lfc_seed.tags) and every tag sits in its comment. Timestamps stay LFC_EPOCH(col)
#   and LFC_DAYS(col), which each engine's ddl.py transpiles.
# Formulas use only integer arithmetic, DIV, %, and CASE, which transpile exactly.
W=$1; SF=${2:-1}
case $W in
tpcb|tpch)   # written from the spec in Python (one copy): lfc_source_datagen/tpc_schema.py, which a notebook imports too
  exec python3 "$(dirname "$0")/../lfc_source_datagen/tpc_schema.py" "$W" "$SF"
  ;;
tpcc|tpcds|tpce|tpcdi)                  # statschema's canonical schema, shaped to SF, as MySQL DDL (see the header)
  SS=$(dirname "$0")/../../../lfc-source-statschema/scripts; Y=$(mktemp); D=$(mktemp)
  uv run -q "$SS/shape.py" "$SS/../schemas/$W.yaml" --sf "$SF" --expected /dev/null > "$Y" && uv run -q "$SS/ddl.py" "$Y" --dialect mysql > "$D" ||
    { rm -f "$Y" "$D"; echo "tpc-schema.sh: $W from statschema failed" >&2; exit 1; }
  # ddl.py writes LFC_EPOCH and LFC_DAYS as MySQL's TIMESTAMPDIFF and DATEDIFF, which no other engine's planner reads back:
  # put the portable forms back (each engine's ddl.py writes its own); drop the side table's statements and the 'lfc:ref'
  # stubs that stand in for the long tags
  sed -e "s/TIMESTAMPDIFF(SECOND, ''1970-01-01 00:00:00'', \([A-Za-z_][A-Za-z_0-9]*\))/LFC_EPOCH(\1)/g" \
      -e "s/DATEDIFF(\([A-Za-z_][A-Za-z_0-9]*\), ''1970-01-01'')/LFC_DAYS(\1)/g" \
      -e '/lfc_seed/d' -e "s/ COMMENT 'lfc:ref'//" -e "s/ lfc:ref'/'/" "$D"; rm -f "$Y" "$D"
  ;;
*) echo "unknown workload: $W" >&2; exit 1 ;;
esac
