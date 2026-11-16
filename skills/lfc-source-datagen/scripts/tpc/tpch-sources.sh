#!/usr/bin/env bash
# tpch-sources.sh: TPC-H as several business sources in one Postgres database, one schema per source, each at its own
# scale, ready for Lakeflow Connect CDC: n lines of business (or acquired companies) with the same tables and
# repeating keys, for an n:1 test (lfc-medallion). Per source: workload.sh's spec TPC-H at the source's scale, filled
# as copy N, checked against the spec (tpch-verify.sql), renamed to the source, the spec's words in its text columns
# (tpch-labels.sql), optionally lead times a model can learn (tpch-lead-times.sql), and SELECT for the contract's login.
# Then one publication for every source's schema. The slot is a separate step, after the pipelines exist, so nothing
# holds WAL before a gateway reads it.
#   SOURCE_JSON=<contract> tpch-sources.sh [--labels spec|anonymous] [--publication <name>] <database> \
#                                          <name>:<sf>[:<lead times>] ...
#   SOURCE_JSON=<contract> tpch-sources.sh slot <database> <slot> [--drop]
#     <lead times>: commit,handle_min,handle_span,slow_nation,slow_every,air,truck,sea,jitter, whole numbers (days but
#     slow_every), tpch-lead-times.sql's variables; e.g. express:0.05:1,0,1,1,5,0,0,1,2 (references/tpch-sources.md).
#     --labels: spec names (default) or anonymous (Nation 01 .. 25); --publication: default <database>_pub.
#     slot: create the pgoutput slot (kept when it exists) and print what it holds; --drop drops it when not active.
#   The contract (lfc-source-contract) gives the administrator's sql_cmd (it creates the database and the publication)
#   and the login (user) the grants go to. A schema that already exists is kept as it is. Needs uv (workload.sh).
#   Prints one row per source: name, lines, % late, median days from order to receipt.
#   WORKLOAD_SH: another workload.sh (the offline test's stub).
#   The same in Python, for a notebook or a job: lfc_source_datagen.tpch_sources (references/tpch-sources.md).
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); DG=$(cd "$HERE/.." && pwd); SK=$(cd "$DG/../.." && pwd)
die() { echo "tpch-sources: $*" >&2; exit 2; }
: "${SOURCE_JSON:?set SOURCE_JSON to a source contract (lfc-source-contract)}"
SE=$("$SK/lfc-source-contract/scripts/source-env.sh" "$SOURCE_JSON") || exit 2
eval "$SE"
[ "$SRC_ENGINE" = postgres ] || die "the contract's engine is $SRC_ENGINE; this writes Postgres"
SQL=${SRC_SQL:-}; [ -n "$SQL" ] || die "the contract needs sql_cmd (an administrator's SQL command)"
ident() { echo "$1" | grep -Eq '^[a-z_][a-z0-9_]{0,62}$' || die "$2 '$1': lower-case letters, digits and _ only"; }
one() { echo "$1" | $SQL "$2"; }

if [ "${1:-}" = slot ]; then
  [ $# -ge 3 ] || die "usage: tpch-sources.sh slot <database> <slot> [--drop]"
  DB=$2; SLOT=$3; ident "$DB" database; ident "$SLOT" slot
  if [ "${4:-}" = --drop ]; then
    one "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '$SLOT' AND NOT active;" "$DB" > /dev/null
  else
    [ "$(one "SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = '$SLOT';" "$DB")" = 1 ] \
      || one "SELECT pg_create_logical_replication_slot('$SLOT', 'pgoutput');" "$DB" > /dev/null
  fi
  one "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) FROM pg_replication_slots WHERE slot_name = '$SLOT';" "$DB"
  exit 0
fi

LABELS=spec; PUB=
while [ $# -gt 0 ]; do
  case $1 in
    --labels) LABELS=${2:-}; shift 2 ;;
    --publication) PUB=${2:-}; shift 2 ;;
    -*) die "unknown option $1" ;;
    *) break ;;
  esac
done
case $LABELS in spec|anonymous) ;; *) die "--labels spec or anonymous" ;; esac
[ $# -ge 2 ] || die "usage: tpch-sources.sh [--labels spec|anonymous] [--publication <name>] <database> <name>:<sf>[:<lead times>] ..."
DB=$1; shift; ident "$DB" database; PUB=${PUB:-${DB}_pub}; ident "$PUB" publication
OUT=${OUT:-$(mktemp -d)}; mkdir -p "$OUT"

# check every source before writing anything
NAMES=
for s in "$@"; do
  IFS=: read -r name sf lt <<< "$s"
  ident "$name" source
  echo "$sf" | grep -Eq '^[0-9]*\.?[0-9]+$' || die "$name: scale factor '$sf'"
  if [ -n "$lt" ]; then
    echo "$lt" | grep -Eq '^[0-9]+(,[0-9]+){8}$' || die "$name: lead times are 9 whole numbers, comma-separated"
    IFS=, read -r _ _ span _ every _ _ _ jit <<< "$lt"
    [ "$span" -ge 1 ] && [ "$every" -ge 1 ] && [ "$jit" -ge 1 ] || die "$name: handle_span, slow_every and jitter at least 1"
  fi
  case " $NAMES " in *" $name "*) die "source $name twice" ;; esac
  NAMES="$NAMES $name"
done

[ "$(one "SELECT COUNT(*) FROM pg_database WHERE datname = '$DB';" postgres)" = 1 ] || one "CREATE DATABASE $DB;" postgres > /dev/null
N=0
for s in "$@"; do
  IFS=: read -r name sf lt <<< "$s"; N=$((N + 1))
  if [ "$(one "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = '$name';" "$DB")" != 0 ]; then
    echo "tpch-sources: $DB.$name exists: kept" >&2; continue
  fi
  # 1. spec TPC-H at the source's scale, filled as copy N, checked against the spec
  WL=$(bash "${WORKLOAD_SH:-$DG/workload.sh}" --model tpch --sf "$sf" --dialect postgres --spec --out "$OUT/$name")
  eval "$(bash "$SK/lfc-workload-contract/scripts/workload-env.sh" "$WL")"
  CP=$(printf '%s_%04d' "$WL_MODEL" "$N")
  PSQL="$SQL $DB -F |" $WL_FILL "$WL_DDL" "$WL_MODEL" "$N" "$N" 1 > "$OUT/$name.fill.log"
  BAD=$({ echo "SET search_path = $CP;"; sed -E 's/DATEDIFF\(([^,]+), ([^)]+)\)/(\1 - \2)/g' "$WL_VERIFY"; } | $SQL "$DB" \
        | grep '^bad' | cut -f2- | tr '\t|' '\n\n' | awk '$1 != 0' | wc -l | tr -d ' ')
  [ "$BAD" = 0 ] || die "$BAD TPC-H rules broken in $DB.$CP (fill log: $OUT/$name.fill.log)"
  # 2. the source's name, the spec's words, and the lead times
  one "ALTER SCHEMA $CP RENAME TO $name;" "$DB" > /dev/null
  { echo "SET search_path = $name;"; cat "$HERE/tpch-labels.sql"; } | $SQL "$DB" -v names="$LABELS" > /dev/null
  if [ -n "$lt" ]; then
    IFS=, read -r c hmin hspan slow every air truck sea jit <<< "$lt"
    { echo "SET search_path = $name;"; cat "$HERE/tpch-lead-times.sql"; } | $SQL "$DB" -v commit="$c" -v handle_min="$hmin" \
      -v handle_span="$hspan" -v slow_nation="$slow" -v slow_every="$every" -v air="$air" -v truck="$truck" -v sea="$sea" \
      -v jitter="$jit" > /dev/null
  fi
  # 3. the connection's login reads it
  printf 'GRANT CONNECT ON DATABASE %s TO "%s";\nGRANT USAGE ON SCHEMA %s TO "%s";\nGRANT SELECT ON ALL TABLES IN SCHEMA %s TO "%s";\n' \
    "$DB" "$CONN_USER" "$name" "$CONN_USER" "$name" "$CONN_USER" | $SQL "$DB" > /dev/null
done

# 4. one publication for every source. Postgres 15 on: by schema, so a table added later is in it; before 15 (no TABLES
#    IN SCHEMA): the tables there now, by name. One that already has exactly these is kept: changing a publication's
#    schemas (ALTER ... SET TABLES IN SCHEMA) needs a superuser, which a managed service's owner (Lakebase) isn't
LIST=$(echo $NAMES | tr ' ' ','); IN="'$(echo $NAMES | sed "s/ /','/g")'"
if [ "$(one "SHOW server_version_num;" "$DB")" -ge 150000 ]; then
  WHAT="TABLES IN SCHEMA $LIST"; WANT=$(echo $NAMES | tr ' ' '\n' | LC_ALL=C sort | paste -sd, -)
  HAVE="SELECT string_agg(n.nspname, ',' ORDER BY n.nspname COLLATE \"C\") FROM pg_publication p JOIN pg_publication_namespace pn
    ON pn.pnpubid = p.oid JOIN pg_namespace n ON n.oid = pn.pnnspid WHERE p.pubname = '$PUB';"
else
  WANT=$(one "SELECT string_agg(format('%I.%I', table_schema, table_name), ', ' ORDER BY format('%I.%I', table_schema, table_name) COLLATE \"C\") FROM information_schema.tables
    WHERE table_type = 'BASE TABLE' AND table_schema IN ($IN);" "$DB"); WHAT="TABLE $WANT"
  HAVE="SELECT string_agg(format('%I.%I', schemaname, tablename), ', ' ORDER BY format('%I.%I', schemaname, tablename) COLLATE \"C\") FROM pg_publication_tables WHERE pubname = '$PUB';"
fi
if [ "$(one "SELECT COUNT(*) FROM pg_publication WHERE pubname = '$PUB';" "$DB")" = 0 ]; then
  one "CREATE PUBLICATION $PUB FOR $WHAT;" "$DB" > /dev/null
elif [ "$(one "$HAVE" "$DB")" = "$WANT" ]; then
  echo "tpch-sources: publication $PUB already has these sources: kept" >&2
elif ! one "ALTER PUBLICATION $PUB SET $WHAT;" "$DB" > /dev/null; then
  die "publication $PUB has other tables, and changing it failed (schemas need a superuser on Postgres 15 on): drop it as its owner (DROP PUBLICATION $PUB, after stopping the gateway that reads it) and rerun, or name another with --publication"
fi

# 5. what each source looks like
for name in $NAMES; do
  one "SELECT '$name', COUNT(*), ROUND(100.0 * AVG((l_receiptdate > l_commitdate)::int), 1),
         PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY l_receiptdate - o_orderdate)
       FROM $name.lineitem JOIN $name.orders ON o_orderkey = l_orderkey;" "$DB"
done
echo "tpch-sources: $DB ready: sources$NAMES, publication $PUB" >&2
