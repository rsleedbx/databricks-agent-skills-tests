#!/usr/bin/env bash
# postgres-tpch-cdc.sh: Lakeflow Connect standard CDC (gateway + ingestion pipeline) from a PostgreSQL source holding
# TPC-H: the snapshot, then ROUNDS rounds of inserts, updates and deletes that keep the TPC-H spec's rules, each checked
# table by table against the source, and the spec's date rules checked on the destination.
#
# Why Lakebase: to test Lakeflow Connect's PostgreSQL CDC connector on purpose, with Lakebase standing in for a
# customer's PostgreSQL (same workspace, no VM or firewall). To get Lakebase data into Unity Catalog for its own sake,
# Lakebase's Lakehouse Sync needs no connector; this test is about the connector.
# Why contracts: the source comes only through SOURCE_JSON (lfc-source-contract) and the tables only through
# WORKLOAD_JSON (lfc-workload-contract), so the same test runs unchanged on a customer's own PostgreSQL (RDS, Azure
# Flexible Server, Cloud SQL, on-premises), a slot VM or a container: point the contract at it and the run shows how
# Lakeflow Connect behaves in that environment.
#
#   The workload file: the workload.sh of lfc-source-datagen, with --model tpch --sf 0.1 --dialect postgres --spec.
#   SOURCE_JSON=<contract> WORKLOAD_JSON=<workload> PROFILE=<profile> CATALOG=<catalog> bash postgres-tpch-cdc.sh
#   The contract's user is the connection's login (REPLICATION, and SELECT through the grants src_db gives it); its
#   sql_cmd runs SQL as an administrator who can CREATE DATABASE and CREATE PUBLICATION ... FOR TABLES IN SCHEMA
#   (Lakebase: lfc-source-contract's lakebase-sql.sh, the project owner). The workload must be TPC-H on postgres with
#   its verify file (workload.sh --spec).
#   Optional: ROUNDS (3), NEW (orders added per round, 100), DEL (orders deleted, 40), UPD (lineitem rows updated, 200),
#   FILL_PSQL (the client the fill uses; default "$SRC_SQL <db> -F |"), MODE, KEEP, KEEP_DAYS, OUT (the result
#   markdown), as lib.sh.
#   Prints one row per claim (PASS/FAIL) and exits 1 if one failed.
set -uo pipefail
ENGINE=postgres
: "${WORKLOAD_JSON:?set WORKLOAD_JSON (lfc-source-datagen workload.sh --model tpch --dialect postgres --spec writes one)}"
WE=$("${LFC_WORKLOAD_ENV:-$(dirname "$0")/../../lfc-workload-contract/scripts/workload-env.sh}" "$WORKLOAD_JSON") || exit 2
eval "$WE"
[ "$WL_MODEL" = tpch ] && [ "$WL_DIALECT" = postgres ] && [ -n "${WL_VERIFY:-}" ] \
  || { echo "WORKLOAD_JSON: needs TPC-H on postgres with its verify file (workload.sh --spec); got $WL_MODEL, $WL_DIALECT"; exit 2; }
SRC_SCHEMA=${WL_MODEL}_0001   # the workload's first copy: its schema holds the tables
. "$(dirname "$0")/lib.sh"
ROUNDS=${ROUNDS:-3}; NEW=${NEW:-100}; DEL=${DEL:-40}; UPD=${UPD:-200}
TABLES=$WL_TABLES; X=$SRC_SCHEMA
lfc_init tpchcdc
# shellcheck disable=SC2034  # FP_TABLES: read by lib.sh
FP_TABLES=$TABLES

# --- the source: one copy of the workload, filled inside the database ------------------------------------------------
src_db < /dev/null
T0=$(date +%s)
PSQL=${FILL_PSQL:-"$SRC_SQL $DB -F |"} $WL_FILL "$WL_DDL" "$WL_MODEL" 1 1 1 > "$W/fill.out" 2>&1
srows() { echo "SELECT $(for t in $TABLES; do printf '(SELECT COUNT(*) FROM %s.%s),' "$X" "$t"; done | sed 's/,$//');" | src | tr '\t' ' '; }
tail -1 "$W/fill.out" | grep -qx "all done" && claim source-fill PASS "$WL_NTABLES TPC-H tables at SF $WL_SCALE filled in $(( $(date +%s) - T0 ))s ($(srows | tr ' ' '\n' | awk '{s += $1} END {print s}') rows)" \
  || { claim source-fill FAIL "$(tail -2 "$W/fill.out" | tr '\n' ' ' | cut -c1-200)"; exit 1; }
verify() { { echo "SET search_path = $X;"; cat "$WL_VERIFY"; } | src | grep '^bad' | cut -f2- | tr '\t' '\n' | awk '$1 != 0' | wc -l | tr -d ' '; }
b=$(verify); [ "$b" = 0 ] && claim source-spec-rules PASS "the workload's verify: every TPC-H rule holds on the source (keys, prices, 4 lines per order, ship/commit/receipt dates)" \
  || claim source-spec-rules FAIL "$b TPC-H rules broken on the source"

# --- the pipelines --------------------------------------------------------------------------------------------------
OBJS=""; for t in $TABLES; do OBJS+="${OBJS:+,}$(obj "$t")"; done
pipelines_create "$OBJS"
drows() { q "SELECT $(for t in $TABLES; do printf '(SELECT COUNT(*) FROM %s.%s),' "$T" "$t"; done | sed 's/,$//')" 2>&1 | tr '\t' ' '; }
# a fingerprint the rounds change: order keys and the round's comments and receipt dates
SFP="SELECT (SELECT SUM(o_orderkey) FROM $X.orders), (SELECT COUNT(*) FROM $X.lineitem WHERE l_comment LIKE 'round %'), (SELECT SUM(l_receiptdate - DATE '1992-01-01') FROM $X.lineitem);"
DFP="SELECT (SELECT SUM(o_orderkey) FROM $T.orders), (SELECT COUNT(*) FROM $T.lineitem WHERE l_comment LIKE 'round %'), (SELECT SUM(DATEDIFF(l_receiptdate, DATE '1992-01-01')) FROM $T.lineitem)"
same() { local s d; s="$(srows) $(echo "$SFP" | src | tr '\t' ' ')"; d="$(drows) $(q "$DFP" 2>&1 | tr '\t' ' ')"; [ "$s" = "$d" ]; }
sleep "$WAIT"; update
same || { sleep "$WAIT"; update; }
same && claim snapshot PASS "every table's rows and the fingerprint match the source: $(srows)" \
  || claim snapshot FAIL "source $(srows) vs destination $(drows); $ERRS"

# --- rounds: changes that keep TPC-H's rules ------------------------------------------------------------------------
# NEW orders copied from the newest ones under new keys that follow the spec's sparse key rule ((k - 1) % 32 < 8), with
# their 4 lines (dates and prices as the copied order's, so every rule holds); DEL of the oldest orders and their lines;
# UPD lines get the round's comment, another ship mode and a new receipt date that stays ship + 1 .. 30.
round() {
  cat <<EOF | src > "$W/round$1.out" 2>&1
BEGIN;
CREATE TEMP TABLE nk AS SELECT k, ROW_NUMBER() OVER (ORDER BY k) AS rn FROM generate_series((SELECT MAX(o_orderkey) FROM $X.orders) + 1, (SELECT MAX(o_orderkey) FROM $X.orders) + 32 * ($NEW / 8 + 1)) k WHERE (k - 1) % 32 < 8 ORDER BY k LIMIT $NEW;
CREATE TEMP TABLE so AS SELECT o_orderkey AS ok, ROW_NUMBER() OVER (ORDER BY o_orderkey) AS rn FROM (SELECT o_orderkey FROM $X.orders ORDER BY o_orderkey DESC LIMIT $NEW) x;
INSERT INTO $X.orders (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment)
  SELECT nk.k, o.o_custkey, o.o_orderstatus, o.o_totalprice, o.o_orderdate, o.o_orderpriority, o.o_clerk, o.o_shippriority, 'round $1' FROM nk JOIN so USING (rn) JOIN $X.orders o ON o.o_orderkey = so.ok;
INSERT INTO $X.lineitem (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
  SELECT nk.k, l.l_partkey, l.l_suppkey, l.l_linenumber, l.l_quantity, l.l_extendedprice, l.l_discount, l.l_tax, l.l_returnflag, l.l_linestatus, l.l_shipdate, l.l_commitdate, l.l_receiptdate, l.l_shipinstruct, l.l_shipmode, l.l_comment
  FROM nk JOIN so USING (rn) JOIN $X.lineitem l ON l.l_orderkey = so.ok;
CREATE TEMP TABLE dk AS SELECT o_orderkey AS k FROM $X.orders ORDER BY o_orderkey LIMIT $DEL;
DELETE FROM $X.lineitem WHERE l_orderkey IN (SELECT k FROM dk);
DELETE FROM $X.orders WHERE o_orderkey IN (SELECT k FROM dk);
UPDATE $X.lineitem l SET l_comment = 'round $1', l_shipmode = (ARRAY['AIR','AIR REG','SHIP','TRUCK','RAIL','MAIL','FOB'])[1 + (l.l_orderkey + $1) % 7],
    l_receiptdate = l.l_shipdate + 1 + ((l.l_orderkey + l.l_linenumber + $1) % 30)::int
  FROM (SELECT l_orderkey AS k, l_linenumber AS ln FROM $X.lineitem WHERE l_orderkey > (SELECT MIN(o_orderkey) FROM $X.orders) ORDER BY l_orderkey, l_linenumber OFFSET $(( 1000 * $1 )) LIMIT $UPD) u
  WHERE l.l_orderkey = u.k AND l.l_linenumber = u.ln;
COMMIT;
EOF
}
for i in $(seq 1 "$ROUNDS"); do
  round "$i" || { claim "round-$i" FAIL "the round's changes failed on the source: $(grep -m1 -i error "$W/round$i.out" | cut -c1-200)"; continue; }
  sleep "$WAIT"; update
  same || { sleep "$WAIT"; update; }
  same && claim "round-$i" PASS "$NEW orders (+$((NEW * 4)) lines) inserted, $DEL orders (-$((DEL * 4)) lines) deleted, $UPD lines updated: rows and fingerprint match ($(srows))" \
    || claim "round-$i" FAIL "source $(srows) $(echo "$SFP" | src | tr '\t' ' ') vs destination $(drows) $(q "$DFP" 2>&1 | tr '\t' ' '); $ERRS"
done
b=$(verify); [ "$b" = 0 ] && claim source-spec-rules-after PASS "the workload's verify: every TPC-H rule still holds on the source after the rounds" \
  || claim source-spec-rules-after FAIL "$b TPC-H rules broken on the source after the rounds"

# --- the destination keeps the spec's date rules (what a late-shipment or lead-time query relies on) ----------------
DR=$(q "SELECT SUM(CASE WHEN DATEDIFF(l.l_shipdate, o.o_orderdate) NOT BETWEEN 1 AND 121 THEN 1 ELSE 0 END), SUM(CASE WHEN DATEDIFF(l.l_commitdate, o.o_orderdate) NOT BETWEEN 30 AND 90 THEN 1 ELSE 0 END), SUM(CASE WHEN DATEDIFF(l.l_receiptdate, l.l_shipdate) NOT BETWEEN 1 AND 30 THEN 1 ELSE 0 END), COUNT(*) FROM $T.lineitem l JOIN $T.orders o ON o.o_orderkey = l.l_orderkey" 2>&1 | tr '\t' ' ')
case $DR in "0 0 0 "*) claim destination-date-rules PASS "ship = order + 1..121, commit = order + 30..90, receipt = ship + 1..30 on all ${DR##* } destination lines";;
  *) claim destination-date-rules FAIL "violations (ship, commit, receipt, lines): $DR";; esac
LATE=$(q "SELECT ROUND(100 * AVG(CASE WHEN l_receiptdate > l_commitdate THEN 1 ELSE 0 END), 1) FROM $T.lineitem" 2>&1)
observe late-share "lines received after their commit date: ${LATE}% (a signal a late-shipment model or Genie question can use)"

CP_S=$(echo "SELECT octet_length(l_shipmode), length(l_shipmode) FROM $X.lineitem ORDER BY l_orderkey, l_linenumber LIMIT 1;" | src | tr '\t' ' ')
CP_D=$(q "SELECT length(l_shipmode) FROM $T.lineitem ORDER BY l_orderkey, l_linenumber LIMIT 1" 2>&1)
observe char-padding "l_shipmode CHAR(10): the source stores ${CP_S%% *} bytes (length ${CP_S##* } without the blanks); the destination string has length $CP_D"

example "A round's changed line" "l_orderkey|l_linenumber|l_shipmode|l_receiptdate|l_comment" \
  "SELECT l_orderkey, l_linenumber, l_shipmode, l_receiptdate, l_comment FROM $X.lineitem WHERE l_comment = 'round $ROUNDS' ORDER BY 1, 2 LIMIT 2;" \
  "l_orderkey|l_linenumber|l_shipmode|l_receiptdate|l_comment" \
  "SELECT l_orderkey, l_linenumber, l_shipmode, l_receiptdate, l_comment FROM $T.lineitem WHERE l_comment = 'round $ROUNDS' ORDER BY 1, 2 LIMIT 2"
result_md "PostgreSQL TPC-H CDC ($SRC_NAME)"
exit "${FAILED:-0}"
