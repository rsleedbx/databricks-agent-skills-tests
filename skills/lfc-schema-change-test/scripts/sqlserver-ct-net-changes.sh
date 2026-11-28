#!/usr/bin/env bash
# sqlserver-ct-net-changes.sh: with a gateway that runs all the time, does a change-tracking table get every update of a
# row, and is every table read on every poll? (the field question: rows updated every minute, the gateway polls every few
# seconds, not every update shows.) Change tracking keeps which rows changed, not each value: a read returns the row's
# value at that moment, so the updates between two reads of a table arrive as one.
#
# The result ends with the data: the values written (with their source time) next to what each kind of table kept.
#
# Tables (10 rows each): ct1 (change tracking, SCD type 1), c2 (CDC, SCD type 2: the control, which keeps every
# change), t01..tNN (change tracking, SCD type 2). Burst: every BURST_GAP seconds for BURST_N rounds, id 1 of every
# table gets v = i<round>; then id 2 gets f1..f5 back to back (5 commits within milliseconds). After the gateway caught up
# and an ingestion update applied it, an SCD type 2 table holds one version per value its reads saw: the gaps between
# kept rounds show how often the gateway read that table.
#
#   TABLES_N (20) change-tracking SCD type 2 tables; BURST_N (60) rounds, BURST_GAP (2) seconds apart.
#   The connection's login gets VIEW SERVER STATE for the run (SCD type 2 on change tracking needs it).
#   Needs and options: as lib.sh (PROFILE CONNECTION CATALOG SRC_SQL CONN_USER; SRC_ENV, KEEP, OUT)
set -uo pipefail
MODE=standard
. "$(dirname "$0")/lib.sh"
lfc_init ctnet
TABLES_N=${TABLES_N:-20}; BURST_N=${BURST_N:-60}; BURST_GAP=${BURST_GAP:-2}
conf tables "$TABLES_N"; conf burst_rounds "$BURST_N"; conf burst_gap_s "$BURST_GAP"
CTS=$(for i in $(seq -w 1 "$TABLES_N"); do printf 't%02d ' "$((10#$i))"; done); ALL="ct1 c2 $CTS"
# BUSY_PCT (100): the share of the change-tracking SCD type 2 tables the burst updates, spread evenly through the list
# (20: every fifth table); the rest stay idle, to see whether the busy ones are read often enough when most are idle
BUSY_PCT=${BUSY_PCT:-100}; conf busy_pct "$BUSY_PCT"
BUSY_CTS=$(i=0; for t in $CTS; do i=$((i + 1)); [ $((i * BUSY_PCT / 100)) -gt $(((i - 1) * BUSY_PCT / 100)) ] && printf '%s ' "$t"; done)
B1=${BUSY_CTS%% *}; BUSY="ct1 c2 $BUSY_CTS"; n_busy=$(echo $BUSY | wc -w | tr -d ' ')
trap 'echo "REVOKE VIEW SERVER STATE FROM $CONN_USER;" | $SRC_SQL master >/dev/null 2>&1; cleanup' EXIT
for t in $ALL; do echo "CREATE TABLE lfcddemo.$t (id INT NOT NULL PRIMARY KEY, v VARCHAR(20));"; done | src_db
for t in $ALL; do printf "INSERT INTO lfcddemo.%s (id, v) SELECT TOP 10 ROW_NUMBER() OVER (ORDER BY (SELECT 1)), 'before' FROM sys.all_objects;\n" "$t"; done | src >/dev/null   # fill, one call
src_util; track ct ct1 $CTS; track cdc c2
echo "GRANT VIEW SERVER STATE TO $CONN_USER;" | $SRC_SQL master >/dev/null || { echo "grant VIEW SERVER STATE failed"; exit 2; }
OBJS="$(obj ct1),$(SCD=SCD_TYPE_2 obj c2)"; for t in $CTS; do OBJS+=",$(SCD=SCD_TYPE_2 obj "$t")"; done
pipelines_create "$OBJS"
n_all=$(echo $ALL | wc -w | tr -d ' ')
loaded() { q "SELECT $(for t in $ALL; do printf '(SELECT COUNT(*) FROM %s.%s) + ' "$T" "$t"; done) 0" 2>&1; }
settle_rows "SELECT $(for t in $ALL; do printf '(SELECT COUNT(*) FROM %s.%s) + ' "$T" "$t"; done) 0" $((n_all * 10))
[ "$SETTLED" = 1 ] || { claim first-load FAIL "rows $ROWS of $((n_all * 10)) after the first load; errors ${ERRS:-none}"; result_md sqlserver-ct-net-changes; exit 1; }
observe first-load "$n_all tables, 10 rows each"

# the burst: one T-SQL batch, each UPDATE its own commit; burst_log (not tracked) keeps each value with its source time
echo "CREATE TABLE lfcddemo.burst_log (id INT, v VARCHAR(20), t DATETIME2(3));" | src >/dev/null
{ echo "SET NOCOUNT ON; DECLARE @i INT = 0; WHILE @i < $BURST_N BEGIN"
  echo "INSERT INTO lfcddemo.burst_log VALUES (1, CONCAT('i', @i), SYSUTCDATETIME());"
  for t in $BUSY; do echo "UPDATE lfcddemo.$t SET v = CONCAT('i', @i) WHERE id = 1;"; done
  printf "WAITFOR DELAY '%02d:%02d:%02d'; SET @i += 1; END\n" $((BURST_GAP / 3600)) $((BURST_GAP / 60 % 60)) $((BURST_GAP % 60))
  echo "DECLARE @j INT = 1; WHILE @j <= 5 BEGIN"
  echo "INSERT INTO lfcddemo.burst_log VALUES (2, CONCAT('f', @j), SYSUTCDATETIME());"
  for t in $BUSY; do echo "UPDATE lfcddemo.$t SET v = CONCAT('f', @j) WHERE id = 2;"; done
  echo "SET @j += 1; END"; } > "$W/burst.sql"
b0=$(date +%s); src < "$W/burst.sql" >/dev/null || { echo "burst failed"; exit 2; }; b1=$(date +%s)
round=$(echo "scale=2; ($b1 - $b0) / $BURST_N" | bc); metric burst_round_s "$round"
observe burst "$BURST_N rounds over $((b1 - b0)) s (a round every $round s) on id 1 of $n_busy busy tables ($BUSY_PCT% of the change-tracking SCD type 2 tables; $n_all tables in all), then f1..f5 on id 2"

last=i$((BURST_N - 1))
cur() { local c="__END_AT IS NULL AND "; [ "$1" = ct1 ] && c=""; echo "(SELECT COUNT(*) FROM $T.$1 WHERE ${c}((id = 1 AND v = '$last') OR (id = 2 AND v = 'f5')))"; }
settle_rows "SELECT $(for t in $BUSY; do printf '%s + ' "$(cur "$t")"; done) 0" $((n_busy * 2))
[ "$SETTLED" = 1 ] || observe catch-up "final values in $ROWS of $((n_busy * 2)) rows after 4 updates; errors ${ERRS:-none}"

# per SCD type 2 table: kept rounds of id 1, the largest gap between kept rounds, kept f values of id 2
stats() { local s=""; for t in c2 $BUSY_CTS; do s+="${s:+ UNION ALL }SELECT '$t', (SELECT COUNT(*) FROM $T.$t WHERE id = 1 AND v LIKE 'i%'), (SELECT COALESCE(MAX(g), 0) FROM (SELECT k - LAG(k) OVER (ORDER BY k) g FROM (SELECT CAST(SUBSTR(v, 2) AS INT) k FROM $T.$t WHERE id = 1 AND v LIKE 'i%'))), (SELECT COUNT(*) FROM $T.$t WHERE id = 2 AND v LIKE 'f%')"; done; q "$s"; }
stats > "$W/stats.tsv"; sed 's/^/  /' "$W/stats.tsv"
read -r _ ck _ cf < <(awk -F'\t' '$1 == "c2"' "$W/stats.tsv")
[ "${ck:-0}" = "$BURST_N" ] && [ "${cf:-0}" = 5 ] && claim cdc-keeps-every-change PASS "c2 (CDC) kept all $BURST_N rounds and f1..f5" \
  || claim cdc-keeps-every-change FAIL "c2 (CDC) kept ${ck:-?} of $BURST_N rounds and ${cf:-?} of 5 f values"
awk -F'\t' '$1 != "c2"' "$W/stats.tsv" > "$W/ct.tsv"
# macOS awk has no asort: sort the kept counts first
eval "$(sort -t$'\t' -k2,2n "$W/ct.tsv" | awk -F'\t' '{k[NR]=$2; if ($3+0 > mg+0) mg=$3; f1+=($4==1)} END {printf "kmin=%s kmed=%s kmax=%s maxgap=%s fone=%s nct=%s", k[1], k[int((NR+1)/2)], k[NR], mg+0, f1+0, NR}')"
metric ct_kept_rounds_min "$kmin"; metric ct_kept_rounds_median "$kmed"; metric ct_kept_rounds_max "$kmax"
metric ct_max_gap_rounds "$maxgap"; metric ct_max_gap_s "$(echo "$maxgap * $round" | bc)"
# "back to back" per table only while one f round over all tables is shorter than a read: with 22 tables they arrived as
# one version (f5) everywhere; with 202, one round over all tables outlasted a read and tables kept 2 or 3 of f1..f5
fgap=$(printf "SET NOCOUNT ON; SELECT DATEDIFF(MILLISECOND, MIN(t), MAX(t)) / 4 FROM lfcddemo.burst_log WHERE id = 2;\n" | src | tr -d '[:space:]')
metric back_to_back_gap_ms "${fgap:-0}"
if [ "${fgap:-0}" -lt 100 ]; then
  [ "$fone" = "$nct" ] && claim ct-back-to-back-one-version PASS "on all $nct change-tracking SCD type 2 tables the 5 updates of id 2 (${fgap} ms apart) arrived as one version (f5)" \
    || claim ct-back-to-back-one-version FAIL "$fone of $nct change-tracking tables kept exactly one of f1..f5 (${fgap} ms apart)"
else
  observe ct-back-to-back "the 5 updates of id 2 were ${fgap} ms apart per table (one round over $n_busy tables); $fone of $nct change-tracking tables kept one version"
fi
# one value per read of the table: all rounds kept means every table was read at least once a round (seen with 22
# tables and 2 s rounds); fewer means some rounds fell between two reads and merged
observe ct-kept-rounds "change-tracking SCD type 2 tables kept $kmin to $kmax (median $kmed) of $BURST_N rounds"
# missed data: a table kept more than 2 values fewer than the median table (a field question: the highest-id table kept 2-21
# of 30 while the others kept 29-30); the gap alone misses a mild case (165 tables: 6 s gap, 9 values lost)
low=$(sort -t$'\t' -k2,2n "$W/ct.tsv" | head -1 | cut -f1)
if [ $((kmed - kmin)) -gt 2 ]; then
  claim no-table-missed-data FAIL "$low kept $kmin of $BURST_N values while the median table kept $kmed: its updates between reads merged"
else
  claim no-table-missed-data PASS "every change-tracking SCD type 2 table kept within 2 of the median ($kmed of $BURST_N); lowest $low, $kmin"
fi
observe ct-read-gap "the largest gap between kept rounds on a change-tracking table: $maxgap rounds (about $(echo "$maxgap * $round" | bc) s)"
v1=$(q "SELECT v FROM $T.ct1 WHERE id = 1" 2>&1); v2=$(q "SELECT v FROM $T.ct1 WHERE id = 2" 2>&1)
[ "$v1" = "$last" ] && [ "$v2" = f5 ] && claim ct-scd1-final-values PASS "ct1 (SCD type 1) ends with the source's final values ($v1, $v2)" \
  || claim ct-scd1-final-values FAIL "ct1 (SCD type 1) has $v1 / $v2, the source $last / f5"

# what the gateway captured: its staging files, per table: updates staged, reads (distinct read times), the gap between
# reads, and the values (the field question: with 202 tables, one table was read far less often than the rest)
V=$(stg_cdc); SLOW=""
if [ -n "$V" ]; then
  C="WITH c AS (SELECT table_id.name t, regexp_extract(to_json(data), '\"([if][0-9]+)\"', 1) v, cursor.timestamp ts, cursor.extractionTimestamp ex FROM read_files('$V', format => 'json') WHERE operation = 'UPDATE'), g AS (SELECT t, v, ts, ex, ex - LAG(ex) OVER (PARTITION BY t ORDER BY ex) gap FROM c)"
  q "$C SELECT t, COUNT(*), COUNT(DISTINCT ex), ROUND(PERCENTILE(gap, 0.5) / 1000, 1), ROUND(MAX(gap) / 1000, 1) FROM g WHERE t <> 'c2' GROUP BY t ORDER BY MAX(gap) DESC" > "$W/reads.tsv"
  SLOW=$(head -1 "$W/reads.tsv" | cut -f1)
  read -r gmed g95 gmax < <(q "$C SELECT ROUND(PERCENTILE(gap, 0.5) / 1000, 1), ROUND(PERCENTILE(gap, 0.95) / 1000, 1), ROUND(MAX(gap) / 1000, 1) FROM g WHERE t <> 'c2'")
  metric stg_read_gap_median_s "$gmed"; metric stg_read_gap_p95_s "$g95"; metric stg_read_gap_max_s "$gmax"
  observe gateway-reads "change-tracking tables read every $gmed s (median), 95th percentile $g95 s, longest gap $gmax s; slowest tables: $(head -3 "$W/reads.tsv" | awk -F'\t' '{printf "%s (%s reads, longest gap %s s) ", $1, $3, $5}')"
  observe gateway-staged-slowest "$SLOW staged: $(q "$C SELECT CONCAT_WS(' ', TRANSFORM(SORT_ARRAY(COLLECT_LIST(STRUCT(ts, v))), x -> x.v)) FROM c WHERE t = '$SLOW'")"
else
  observe gateway-reads "no gateway staging volume found in $STG"
fi

# each case as one line: the values that went in, what came out (the form the field question asked for)
staged() { [ -n "$V" ] && q "$C SELECT COUNT(*) FROM c WHERE t = '$1' AND v LIKE '$2%'" || echo "?"; }
observe example "id 1 set to i0, i1 ... i$((BURST_N - 1)), $round s apart -> CDC SCD type 2 (c2) kept $ck versions; change-tracking SCD type 2 kept $kmin to $kmax (median $kmed)"
observe example "the same on ct1 (change tracking, SCD type 1) -> the gateway staged $(staged ct1 i) of $BURST_N values; the destination shows $v1 (the latest, as of the ingestion update)"
observe example "id 2 set to f1 ... f5 back to back -> c2 kept $cf versions; $fone of $nct change-tracking SCD type 2 tables kept one (f5), the gateway staged $(staged "$B1" f) of 5 on $B1"
[ -n "$SLOW" ] && observe example "$SLOW, the slowest-read table (longest gap $gmax s) -> the gateway staged $(staged "$SLOW" i) of $BURST_N values: $(q "$C SELECT CONCAT_WS(' ', TRANSFORM(SORT_ARRAY(COLLECT_LIST(STRUCT(ts, v))), x -> x.v)) FROM c WHERE t = '$SLOW' AND v LIKE 'i%'")"

# the data: the first rounds and the back-to-back updates as written, and what each kind of table kept
IN_SQL="SELECT CONCAT_WS(CHAR(9), id, v, CONVERT(VARCHAR(23), t, 121)) FROM lfcddemo.burst_log WHERE (id = 1 AND TRY_CAST(SUBSTRING(v, 2, 9) AS INT) < 6) OR id = 2 ORDER BY id, t;"
for x in "$B1:change tracking, SCD type 2" "c2:CDC, SCD type 2 (the control)"; do
  example "${x#*:} (${x%%:*})" "id|value written|source time (UTC)" "$IN_SQL" "id|v|__START_AT|__END_AT" \
    "SELECT id, v, __START_AT, __END_AT FROM $T.${x%%:*} WHERE (id = 1 AND v LIKE 'i%' AND TRY_CAST(SUBSTR(v, 2) AS INT) < 6) OR (id = 2 AND v LIKE 'f%') ORDER BY id, __START_AT"
done
# the slowest-read table: every value written, what the gateway staged (with when it read it), what the destination kept
[ -n "$SLOW" ] && [ "$SLOW" != "$B1" ] && example "slowest-read table ($SLOW): written, then kept" "id|value written|source time (UTC)" \
  "SELECT CONCAT_WS(CHAR(9), id, v, CONVERT(VARCHAR(23), t, 121)) FROM lfcddemo.burst_log WHERE id = 1 ORDER BY t;" "id|v|__START_AT|__END_AT" \
  "SELECT id, v, __START_AT, __END_AT FROM $T.$SLOW WHERE id = 1 AND v LIKE 'i%' ORDER BY __START_AT"
[ -n "$SLOW" ] && { echo "### What the gateway staged for $SLOW (UTC)"; echo; md_head "gateway read|source commit|value"
  q "$C SELECT from_unixtime(ex / 1000, 'HH:mm:ss.SSS'), from_unixtime(ts / 1000, 'HH:mm:ss.SSS'), v FROM c WHERE t = '$SLOW' ORDER BY ts" | md_rows; } > "$W/slow.md" \
  && EXAMPLES+=("$(cat "$W/slow.md")" "")
example "change tracking, SCD type 1 (ct1)" "id|value written|source time (UTC)" \
  "SELECT CONCAT_WS(CHAR(9), id, v, CONVERT(VARCHAR(23), t, 121)) FROM (SELECT TOP 2 * FROM lfcddemo.burst_log WHERE id = 1 ORDER BY t DESC) a UNION ALL SELECT CONCAT_WS(CHAR(9), id, v, CONVERT(VARCHAR(23), t, 121)) FROM (SELECT TOP 2 * FROM lfcddemo.burst_log WHERE id = 2 ORDER BY t DESC) b;" \
  "id|v" "SELECT id, v FROM $T.ct1 WHERE id IN (1, 2) ORDER BY id"

# what the gateway reports about its reads (for "how long did each poll take")
dbx api get "/api/2.0/pipelines/$GW/events?max_results=250" -o json > "$W/gw_ev.json"
observe gateway-events "event types: $(yq -r '.events[].event_type' "$W/gw_ev.json" | sort | uniq -c | awk '{printf "%s %s, ", $2, $1}' | sed 's/, $//')"
yq -r '.events[] | select(.message | test("(?i)version|lag|poll|latency")) | .event_type + ": " + .message' "$W/gw_ev.json" | head -5 | cut -c1-240 | sed 's/^/  gateway: /'
# cdc_extraction_details: what the gateway says about its reads; kept whole (cleanup deletes the pipeline)
yq -o json -I0 '[.events[] | select(.event_type == "cdc_extraction_details")]' "$W/gw_ev.json" > "${OUT:-$W/x}.extraction.json"
echo "  cdc_extraction_details: $(yq 'length' "${OUT:-$W/x}.extraction.json") events in ${OUT:-$W/x}.extraction.json; first: $(yq -o json -I0 '.[-1] | {"message": .message, "details": .details}' "${OUT:-$W/x}.extraction.json" | cut -c1-400)"
result_md sqlserver-ct-net-changes
exit $FAILED
