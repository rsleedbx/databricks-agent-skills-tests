#!/usr/bin/env bash
# schema.sh <profile> <catalog>.<schema> [--warehouse <id>] [--tables t1,t2] > columns.tsv
# Every column of every table in a Unity Catalog schema, as TSV with a header: table, column, type, position.
# Run it on the Lakeflow Connect destination schema and on the Fivetran one (Fivetran lands in the same metastore, or
# export its information_schema the same way), then compare them with schema-diff.py.
set -euo pipefail
[ $# -ge 2 ] || { sed -n 2,5p "$0"; exit 2; }
PROFILE=$1 FQ=$2; shift 2
WH="" TL=""
while [ $# -gt 0 ]; do
  case $1 in --warehouse) WH=$2; shift ;; --tables) TL=$2; shift ;; *) sed -n 2,5p "$0"; exit 2 ;; esac
  shift
done
. "$(dirname "$0")/lib.sh"
[[ $FQ =~ ^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$ ]] || { echo "want <catalog>.<schema>: $FQ" >&2; exit 2; }
[[ -z $TL || $TL =~ ^[a-z0-9_,]+$ ]] || { echo "--tables: comma-separated names" >&2; exit 2; }
CAT=${FQ%%.*} SCH=${FQ#*.}
filter=""; [ -z "$TL" ] || filter="AND table_name IN ('${TL//,/\',\'}')"
printf 'table\tcolumn\ttype\tposition\n'
sql "SELECT table_name, column_name, full_data_type, ordinal_position FROM $(ident "$CAT").information_schema.columns
     WHERE table_schema = '$SCH' $filter AND table_name NOT LIKE '\\_\\_%' AND table_name NOT LIKE 'event\\_log\\_%' ORDER BY table_name, ordinal_position"
