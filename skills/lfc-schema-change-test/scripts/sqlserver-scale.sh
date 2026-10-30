#!/usr/bin/env bash
# sqlserver-scale.sh: many tables in one pipeline. Answers a field question (INGESTION_GATEWAY_OPERATION_TIMEOUT.REINITIALIZATION:
# when does a pipeline with many tables hit it?). COPIES copies of a TPC model (MODEL, default tpcc: 9 tables) in source
# schemas <model>_0001 .. <model>_NNNN, generated inside SQL Server by lfc-source-statschema (shape.py, ddl.py) and
# lfc-source-datagen (fill.sh); change tracking on keyed tables and CDC on keyless ones; then one pipeline over all of it.
#
#   COPIES=10  SF=0.01  MODEL=tpcc  FILLS=4 (copies filled at once; at 8 the VM's sshd started refusing connections)
#   OBJECTS=schema  one `schema` object per source schema, each into its own destination schema <S>_<model>_NNNN
#                   (a schema object can't rename tables, and every copy has the same table names)
#   OBJECTS=table   one `table` object per table, all into the destination schema <S>, named <table>_NNNN
#   The source database is kept for the next run (KEEP_SOURCE=0 drops it): SOURCE_DB, default
#   <prefix>_scale_<model>_x<copies>_sf<sf>. A run reuses it when every table is there and tracked, else restores it from
#   the backup the first build wrote (/var/opt/mssql/backup/<db>.bak in the container), else builds it.
#   DISK_MBPS  the source disk limit the run is under, in MiB/s (vm-throttle.sh <MiB/s> --cold before, off after): recorded only
#   Needs and options: as sqlserver-add-column.sh (PROFILE CONNECTION CATALOG SRC_SQL CONN_USER; SRC_ENV, MODE, KEEP, OUT),
#   and uv (for the statschema scripts).
#
# Records: pipeline create accepted or refused; time from the first update's start until every table has its source row
# count; errors (REINITIALIZATION timeouts among them); then one change per copy and whether each arrives.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
COPIES=${COPIES:-10}; SF=${SF:-0.01}; MODEL=${MODEL:-tpcc}; OBJECTS=${OBJECTS:-schema}; FILLS=${FILLS:-4}
SK=${SK:-$LIB/../..}   # the skills folder (set SK when running a copy of this script)
lfc_init scale
copy() { printf '%s_%04d' "$MODEL" "$1"; }
DB=${SOURCE_DB:-${PREFIX}_scale_${MODEL}_x${COPIES}_sf${SF//./p}}; export KEEP_SOURCE=${KEEP_SOURCE:-1}
BAK=/var/opt/mssql/backup/$DB.bak
echo "source database $DB (kept after the run: $( [ "$KEEP_SOURCE" = 1 ] && echo yes || echo no))"

# --- the model's DDL, once ---------------------------------------------------------------------------------------------
uv run "$SK/lfc-source-statschema/scripts/shape.py" "$SK/lfc-source-statschema/schemas/$MODEL.yaml" --sf "$SF" > "$W/model.yaml" 2>"$W/shape.err" \
  || { echo "shape.py failed: $(tail -2 "$W/shape.err")"; exit 2; }
uv run "$SK/lfc-source-statschema/scripts/ddl.py" "$W/model.yaml" --dialect tsql > "$W/model.sql" 2>"$W/ddl.err" \
  || { echo "ddl.py failed: $(tail -2 "$W/ddl.err")"; exit 2; }
TB=$(grep -o -E '^CREATE TABLE \[[a-z_0-9]+\]' "$W/model.sql" | sed -E 's/.*\[(.*)\]/\1/')
NT=$(echo "$TB" | wc -l | tr -d ' '); RE=$(echo "$TB" | paste -sd'|' -)
echo "$MODEL at SF $SF: $NT tables per copy, $COPIES copies = $((NT * COPIES)) tables"

# --- source: reuse the kept database, or restore its backup, or build it ----------------------------------------------
tracked() { printf "SET NOCOUNT ON; IF DB_ID('%s') IS NULL SELECT -1 ELSE SELECT (SELECT COUNT(*) FROM [%s].sys.change_tracking_tables) + (SELECT COUNT(*) FROM [%s].sys.tables WHERE is_tracked_by_cdc = 1);\n" "$DB" "$DB" "$DB" | $SRC_SQL master | tr -d '[:space:]'; }
HAVE=$(tracked)
if [ "$HAVE" = -1 ] && echo "SET NOCOUNT ON; RESTORE HEADERONLY FROM DISK = '$BAK';" | $SRC_SQL master >/dev/null 2>&1; then
  echo "restoring $DB from $BAK"
  echo "RESTORE DATABASE [$DB] FROM DISK = '$BAK' WITH KEEP_CDC;" | $SRC_SQL master > "$W/restore.out" 2>&1 || { echo "restore failed: $(grep -m1 Msg "$W/restore.out")"; exit 2; }
  echo "EXEC sys.sp_cdc_add_job @job_type = 'capture'; EXEC sys.sp_cdc_add_job @job_type = 'cleanup';" | src >/dev/null 2>&1
  HAVE=$(tracked)
fi
# tracked() counts every tracked table; the model's own come to NT * COPIES (the utility objects track their DDL audit table too)
if [ "${HAVE:--1}" -ge $((NT * COPIES)) ] 2>/dev/null; then
  echo "reusing $DB: $HAVE tracked tables"
  echo "CREATE USER $CONN_USER FOR LOGIN $CONN_USER;" | src >/dev/null 2>&1
  SCHEMAS=$(for i in $(seq 1 "$COPIES"); do copy "$i"; done | paste -sd, -)
  echo "EXEC dbo.lakeflowFixPermissions @User = '$CONN_USER', @Tables = 'SCHEMAS:$SCHEMAS';" | src >/dev/null
  REUSED=1
else
  [ "$HAVE" != -1 ] && { echo "$DB is incomplete ($HAVE tracked): dropping it to build again"
    printf 'USE %s;\nEXEC sys.sp_cdc_disable_db;\nGO\nUSE master;\nALTER DATABASE %s SET SINGLE_USER WITH ROLLBACK IMMEDIATE;\nDROP DATABASE %s;\n' "$DB" "$DB" "$DB" | $SRC_SQL master >/dev/null 2>&1; }
  REUSED=0
fi
if [ "$REUSED" = 0 ]; then
# one schema per copy, filled inside SQL Server --------------------------------------------------------------
echo "SELECT 1;" | src_db
# The build runs on the database's VM (lfc-source-datagen bundle.sh + copies.sh), not as hundreds of SSH calls from
# here: a laptop's network drop then only delays the polling below. BUILD_HERE=1 builds from this machine instead.
T0=$(date +%s); RUN=/tmp/lfc-build-$DB
if [ "${BUILD_HERE:-0}" = 1 ]; then
  MSSQL="env SQLCMD_FLAGS=-b\ -y\ 0 $SRC_SQL $DB" bash "$SK/lfc-source-datagen/scripts/copies.sh" ms "$W/model.sql" "$MODEL" 1 "$COPIES" "$FILLS" \
    | tee "$W/build.log" | grep -v '^done' || { echo "source build failed"; exit 2; }
else
  : "${VM_IP:?BUILD_HERE=0 builds on the VM: set VM_IP and MSSQL_CONTAINER}" "${MSSQL_CONTAINER:?}"
  SSHV=(ssh -i "${SSH_KEY:-$HOME/.ssh/lfc_test_rsa}" -o BatchMode=yes -o ConnectTimeout=15 "${VM_USER:-azureuser}@$VM_IP")
  SCPV=(scp -q -i "${SSH_KEY:-$HOME/.ssh/lfc_test_rsa}" -o BatchMode=yes -o ConnectTimeout=15)
  bash "$SK/lfc-source-datagen/scripts/bundle.sh" "$W/datagen.tgz" >/dev/null
  # the VM-side client: SQL on stdin into sqlcmd in the container, the sa password read inside it
  cat > "$W/sq" <<SQ
#!/usr/bin/env bash
podman --remote --url unix:///run/podman/podman.sock exec -i $MSSQL_CONTAINER sh -c 'f=\$(mktemp); cat > \$f; /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "\$MSSQL_SA_PASSWORD" -b -y 0 -d $DB -i \$f; rc=\$?; rm -f \$f; exit \$rc'
SQ
  "${SSHV[@]}" "rm -rf $RUN && mkdir -p $RUN" && "${SCPV[@]}" "$W/datagen.tgz" "$W/model.sql" "$W/sq" "${VM_USER:-azureuser}@$VM_IP:$RUN/" \
    || { echo "copy to the VM failed"; exit 2; }
  "${SSHV[@]}" "cd $RUN && tar xzf datagen.tgz && chmod +x sq && MSSQL=$RUN/sq nohup bash copies.sh ms model.sql $MODEL 1 $COPIES $FILLS > build.log 2>&1 < /dev/null &" \
    || { echo "starting the build on the VM failed"; exit 2; }
  echo "building on the VM: $RUN/build.log"
  last=""
  while :; do
    sleep 30
    st=$("${SSHV[@]}" "tail -1 $RUN/build.log; grep -c '^done' $RUN/build.log; grep '^failed' $RUN/build.log | head -3" 2>/dev/null) || continue   # a drop: try again
    cur=$(sed -n 2p <<< "$st"); [ "$cur" != "$last" ] && { echo "  $cur of $COPIES copies built"; last=$cur; }
    case $(head -1 <<< "$st") in
      "all done") break;;
      "finished with failures") echo "source build failed: $(sed -n '3,$p' <<< "$st" | tr '\n' ' ')"; exit 2;;
    esac
  done
fi
echo "source built in $(( $(date +%s) - T0 ))s"
fi
srcrows() { printf "SET NOCOUNT ON; SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name, SUM(p.rows) FROM sys.tables t JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id < 2 WHERE SCHEMA_NAME(t.schema_id) LIKE '%s[_]%%' GROUP BY t.schema_id, t.name ORDER BY 1;\n" "$MODEL" | src | tr -s ' \t' '\t'; }
srcrows > "$W/src.tsv"; [ "$(wc -l < "$W/src.tsv" | tr -d ' ')" = $((NT * COPIES)) ] || { echo "source has $(wc -l < "$W/src.tsv") tables, expected $((NT * COPIES))"; exit 2; }

if [ "$REUSED" = 0 ]; then
# change tracking on every keyed table (by schema), CDC on the keyless ones (change tracking needs a primary key)
src_util
for i in $(seq 1 "$COPIES"); do echo "$(copy "$i")"; done | paste -sd, - > "$W/schemas.txt"
SCHEMAS=$(cat "$W/schemas.txt")
echo "EXEC dbo.lakeflowSetupChangeTracking @Tables = 'SCHEMAS:$SCHEMAS', @User = '$CONN_USER';" | src > "$W/ct.out" 2>&1
printf "SET NOCOUNT ON; SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name FROM sys.tables t WHERE SCHEMA_NAME(t.schema_id) LIKE '%s[_]%%' AND NOT EXISTS (SELECT 1 FROM sys.key_constraints k WHERE k.parent_object_id = t.object_id AND k.type = 'PK');\n" "$MODEL" | src | tr -d ' ' | grep -v '^$' > "$W/keyless.txt"
while read -r k; do echo "EXEC dbo.lakeflowSetupChangeDataCapture @Tables = '$k', @User = '$CONN_USER';"; done < "$W/keyless.txt" | src > "$W/cdc.out" 2>&1
echo "EXEC dbo.lakeflowFixPermissions @User = '$CONN_USER', @Tables = 'SCHEMAS:$SCHEMAS';" | src >/dev/null
TRACKED=$(printf "SET NOCOUNT ON; SELECT (SELECT COUNT(*) FROM sys.change_tracking_tables c JOIN sys.tables t ON t.object_id = c.object_id WHERE SCHEMA_NAME(t.schema_id) LIKE '%s[_]%%') + (SELECT COUNT(*) FROM sys.tables t WHERE is_tracked_by_cdc = 1 AND SCHEMA_NAME(t.schema_id) LIKE '%s[_]%%');\n" "$MODEL" "$MODEL" | src | tr -d '[:space:]')
[ "$TRACKED" = $((NT * COPIES)) ] || { echo "tracked $TRACKED of $((NT * COPIES)) tables; CT: $(grep -m1 -i error "$W/ct.out")"; exit 2; }
echo "tracking: $(( NT * COPIES - $(wc -l < "$W/keyless.txt") )) change tracking, $(wc -l < "$W/keyless.txt") CDC"
  echo "BACKUP DATABASE [$DB] TO DISK = '$BAK' WITH INIT, COMPRESSION;" | $SRC_SQL master > "$W/backup.out" 2>&1 \
    && echo "backup: $BAK" || echo "backup failed: $(grep -m1 Msg "$W/backup.out")"
fi

# --- the pipeline -------------------------------------------------------------------------------------------------------
DEST=()   # destination schemas besides $S, deleted at cleanup
OBJS=""
if [ "$OBJECTS" = schema ]; then
  for i in $(seq 1 "$COPIES"); do
    s=$(copy "$i"); d="${S}_$s"; DEST+=("$d"); dbx schemas create "$d" "$CATALOG" >/dev/null || exit 2
    OBJS+="${OBJS:+,}{\"schema\":{\"source_catalog\":\"$DB\",\"source_schema\":\"$s\",\"destination_catalog\":\"$CATALOG\",\"destination_schema\":\"$d\"}}"
  done
else
  while IFS=$'\t' read -r st _; do
    s=${st%%.*}; t=${st#*.}; n=${s##*_}
    OBJS+="${OBJS:+,}{\"table\":{\"source_catalog\":\"$DB\",\"source_schema\":\"$s\",\"source_table\":\"$t\",\"destination_catalog\":\"$CATALOG\",\"destination_schema\":\"$S\",\"destination_table\":\"${t}_$n\"}}"
  done < "$W/src.tsv"
fi
# the extra destination schemas go with the rest at cleanup
trap 'for d in ${DEST[@]+"${DEST[@]}"}; do dbx schemas delete "$CATALOG.$d" --force 2>/dev/null; done; cleanup' EXIT
dest() {   # dest <schema.table> -> its destination table
  local s=${1%%.*} t=${1#*.}
  if [ "$OBJECTS" = schema ]; then echo "\`$CATALOG\`.${S}_$s.$t"; else echo "$T.${t}_${s##*_}"; fi
}
FP_TABLES=""   # the early stop counts these: the first table of each copy is enough
CREATE_SOFT=1 pipelines_create "$OBJS" || { observe pipeline-create "refused with $((NT * COPIES)) tables ($OBJECTS objects): $CREATE_ERR"; exit 0; }
observe pipeline-create "accepted: $((NT * COPIES)) tables in $( [ "$OBJECTS" = schema ] && echo "$COPIES schema objects" || echo "$((NT * COPIES)) table objects")"

# --- first load: until every table has its source rows ------------------------------------------------------------------
matched() {   # tables whose destination row count equals the source's
  local q="" st n
  while IFS=$'\t' read -r st n; do q+="${q:+ UNION ALL }SELECT '$st' AS t, $n AS want, (SELECT COUNT(*) FROM $(dest "$st")) AS got"; done < "$W/src.tsv"
  q "SELECT SUM(CASE WHEN want = got THEN 1 ELSE 0 END) FROM ($q)" 2>/dev/null | tr -d '[:space:]'
}
T0=$(date +%s); M=0; ALLERRS=""
for round in $(seq 1 12); do
  update; ALLERRS+=" $ERRS"
  M=$(matched); echo "round $round: $M of $((NT * COPIES)) tables match the source after $(( $(date +%s) - T0 ))s"
  [ "$M" = $((NT * COPIES)) ] && break
done
ELAPSED=$(( $(date +%s) - T0 ))
metric tables $((NT * COPIES)); metric first_load_s "$ELAPSED"; metric first_load_updates "$round"
metric s_per_table "$(awk "BEGIN { printf \"%.2f\", $ELAPSED / ($NT * $COPIES) }")"
conf model "$MODEL"; conf sf "$SF"; conf copies "$COPIES"; conf objects "$OBJECTS"
[ -n "${DISK_MBPS:-}" ] && conf disk_mbps "$DISK_MBPS"   # set when the run is under vm-throttle.sh <MiB/s> (MiB/s)
ALLERRS=$(echo $ALLERRS | tr ' ' '\n' | sort -u | grep -v '^$' | paste -sd' ' -)
[ "$M" = $((NT * COPIES)) ] && claim first-load-complete PASS "$M tables at their source row counts after ${ELAPSED}s ($round update rounds); errors seen: ${ALLERRS:-none}" \
  || claim first-load-complete FAIL "$M of $((NT * COPIES)) tables match after ${ELAPSED}s; errors: ${ALLERRS:-none}"
[[ "$ALLERRS" == *REINITIALIZATION* ]] && observe reinitialization-timeout "INGESTION_GATEWAY_OPERATION_TIMEOUT.REINITIALIZATION seen" \
  || observe reinitialization-timeout "not seen"

# --- then one change per copy: update one row of the first table (a column that isn't in the key, set to itself; change
# tracking and CDC still record it), and see the destination table of the first and the last copy get a new version -----
FIRST=$(head -1 <<< "$TB")
COL=$(printf "SET NOCOUNT ON; SELECT TOP 1 c.name FROM sys.columns c WHERE c.object_id = OBJECT_ID('%s.%s') AND c.column_id NOT IN (SELECT ic.column_id FROM sys.index_columns ic JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id WHERE i.is_primary_key = 1 AND ic.object_id = c.object_id) ORDER BY c.column_id;\n" "$(copy 1)" "$FIRST" | src | tr -d '[:space:]')
ver() { q "SELECT MAX(version) FROM (DESCRIBE HISTORY $(dest "$1"))" 2>&1 | tr -d '[:space:]'; }
A0=$(ver "$(copy 1).$FIRST"); Z0=$(ver "$(copy "$COPIES").$FIRST")
for i in $(seq 1 "$COPIES"); do printf "UPDATE TOP (1) %s.%s SET %s = %s;\n" "$(copy "$i")" "$FIRST" "$COL" "$COL"; done | src >/dev/null
# with hundreds of tables the gateway takes longer to go round them all: up to 4 updates, and the time it took
TC=$(date +%s)
for _ in 1 2 3 4; do
  sleep "$WAIT"; update
  A1=$(ver "$(copy 1).$FIRST"); Z1=$(ver "$(copy "$COPIES").$FIRST")
  [ "${A1:-0}" -gt "${A0:-0}" ] && [ "${Z1:-0}" -gt "${Z0:-0}" ] && break
done
observe change-latency "both changes in the destination after $(( $(date +%s) - TC ))s (or not, see below)"
metric change_latency_s $(( $(date +%s) - TC ))
[ "${A1:-0}" -gt "${A0:-0}" ] && [ "${Z1:-0}" -gt "${Z0:-0}" ] \
  && claim changes-arrive PASS "an update in the first and the last copy reached the destination (versions $A0 -> $A1, $Z0 -> $Z1)" \
  || claim changes-arrive FAIL "destination versions $A0 -> $A1 (first copy), $Z0 -> $Z1 (last copy); errors: ${ERRS:-none}"

result_md "sqlserver-scale ($COPIES x $MODEL, $OBJECTS objects)"
exit $FAILED
