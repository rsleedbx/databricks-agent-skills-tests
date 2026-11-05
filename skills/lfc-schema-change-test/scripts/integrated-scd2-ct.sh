#!/usr/bin/env bash
# integrated-scd2-ct.sh: SCD_TYPE_2 on a change-tracking table, on integrated CDC. Tables (10 rows each): s1ct (change
# tracking, SCD type 1), s2ct (change tracking, SCD type 2), s2cdc (CDC, SCD type 2). Step A inserts 2 rows per table, A2
# 2 more, B adds a nullable column and inserts 2. After each, it prints destination rows, s2ct's ids and the changes in
# the CDC staging table per table. Seen 2026-10-20: s2ct got none of A's or A2's rows until B's ADD COLUMN released
# them, and then held B's; no error. SCD_TYPE_2 needs CDC on SQL Server.
# Needs PROFILE, CONNECTION, CATALOG, CONN_USER, SRC_SQL (as sqlserver-add-column.sh), or SOURCE_JSON (a source contract,
# lfc-source-contract) for the last three.
# PIPELINE_CONF (a JSON object) sets pipeline settings, e.g. a knob under test.
# SERVER_STATE=1 grants the connection's login VIEW SERVER STATE for the run (revoked at the end): on change tracking,
# SCD type 2 reads each change's commit time from sys.dm_tran_commit_table, which shows a login without it no rows.
set -uo pipefail
export PATH=/opt/homebrew/bin:$PATH
dbx() { databricks "$@" --profile "$PROFILE"; }
HERE=$(cd "$(dirname "$0")" && pwd)
if [ -n "${SOURCE_JSON:-}" ]; then
  SE=$("${LFC_SOURCE_ENV:-$HERE/../../lfc-source-contract/scripts/source-env.sh}" "$SOURCE_JSON") || exit 2
  eval "$SE"
fi
PREFIX=${PREFIX:-$(dbx current-user me -o json | yq '.userName' | cut -d@ -f1 | tr -d '.-' | tr 'A-Z' 'a-z')}
E=$(date +%s); DB=${PREFIX}_scd2ct_$E; S=${PREFIX}_scd2ct_$E; W=$(mktemp -d); T="\`$CATALOG\`.$S"
WAREHOUSE=$(dbx warehouses list -o json | yq '[.[] | select(.enable_serverless_compute)][0].id'); export WAREHOUSE
q() { export Q=$1; yq -n -o json '.warehouse_id = strenv(WAREHOUSE) | .wait_timeout = "50s" | .statement = strenv(Q)' > "$W/st.json"
  dbx api post /api/2.0/sql/statements --json @"$W/st.json" -o json > "$W/st.out"; yq -r '.result.data_array[] | @tsv' "$W/st.out" 2>/dev/null || yq '.status.error.message' "$W/st.out"; }
TABLES="s1ct s2ct s2cdc"
cleanup() {
  [ "${SERVER_STATE:-0}" = 1 ] && echo "REVOKE VIEW SERVER STATE FROM $CONN_USER;" | $SRC_SQL master >/dev/null 2>&1
  [ -n "${AP:-}" ] && dbx pipelines delete "$AP"; sleep 30; dbx schemas delete "$CATALOG.$S" --force 2>/dev/null
  for _ in 1 2 3; do
    printf 'USE %s;\nEXEC sys.sp_cdc_disable_db;\nGO\nUSE master;\nALTER DATABASE %s SET SINGLE_USER WITH ROLLBACK IMMEDIATE;\nDROP DATABASE %s;\n' "$DB" "$DB" "$DB" | $SRC_SQL master >/dev/null 2>&1
    [ "$(printf "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name = '%s';\n" "$DB" | $SRC_SQL master | tr -d '[:space:]')" = 0 ] && break; sleep 30
  done
  echo "cleanup: source database left: $(printf "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name = '%s';\n" "$DB" | $SRC_SQL master | tr -d '[:space:]')"; rm -rf "${W:?}"
}
trap cleanup EXIT
$SRC_SQL master >/dev/null <<SQL
CREATE DATABASE $DB;
GO
USE $DB;
GO
CREATE SCHEMA lfcddemo;
GO
CREATE TABLE lfcddemo.s1ct (id INT NOT NULL PRIMARY KEY, v VARCHAR(20));
CREATE TABLE lfcddemo.s2ct (id INT NOT NULL PRIMARY KEY, v VARCHAR(20));
CREATE TABLE lfcddemo.s2cdc (id INT NOT NULL PRIMARY KEY, v VARCHAR(20));
GO
INSERT INTO lfcddemo.s1ct (id, v) SELECT TOP 10 ROW_NUMBER() OVER (ORDER BY (SELECT 1)), 'before' FROM sys.objects;
INSERT INTO lfcddemo.s2ct (id, v) SELECT TOP 10 ROW_NUMBER() OVER (ORDER BY (SELECT 1)), 'before' FROM sys.objects;
INSERT INTO lfcddemo.s2cdc (id, v) SELECT TOP 10 ROW_NUMBER() OVER (ORDER BY (SELECT 1)), 'before' FROM sys.objects;
CREATE USER $CONN_USER FOR LOGIN $CONN_USER;
SQL
curl -fsSL -o "$W/u.sql" https://learn.microsoft.com/en-us/azure/databricks/_extras/documents/utility_script.sql
$SRC_SQL "$DB" < "$W/u.sql" >/dev/null
$SRC_SQL "$DB" >/dev/null <<SQL
EXEC dbo.lakeflowSetupChangeTracking @Tables = 'lfcddemo.s1ct', @User = '$CONN_USER', @CreateDdlSupportingObjects = 1;
EXEC dbo.lakeflowSetupChangeTracking @Tables = 'lfcddemo.s2ct', @User = '$CONN_USER', @CreateDdlSupportingObjects = 1;
EXEC dbo.lakeflowSetupChangeDataCapture @Tables = 'lfcddemo.s2cdc', @User = '$CONN_USER';
EXEC dbo.lakeflowFixPermissions @User = '$CONN_USER', @Tables = 'SCHEMAS:lfcddemo';
SQL
# a table left untracked (CDC setup can fail when several runs set it up at once) fails later as if it were a product result
TRACKED=$(printf "SET NOCOUNT ON; SELECT (SELECT COUNT(*) FROM sys.change_tracking_tables WHERE object_id IN (OBJECT_ID('lfcddemo.s1ct'), OBJECT_ID('lfcddemo.s2ct'))), (SELECT COUNT(*) FROM sys.tables WHERE object_id = OBJECT_ID('lfcddemo.s2cdc') AND is_tracked_by_cdc = 1);\n" | $SRC_SQL "$DB" | tr -s ' \t' ' ' | xargs)
[ "$TRACKED" = "2 1" ] || { echo "source tracking not set up (change tracking, CDC tables): $TRACKED"; exit 2; }
[ "${SERVER_STATE:-0}" = 1 ] && echo "GRANT VIEW SERVER STATE TO $CONN_USER;" | $SRC_SQL master >/dev/null
echo "commit-time rows the login sees in sys.dm_tran_commit_table: $(printf "SET NOCOUNT ON; EXECUTE AS LOGIN = '%s'; SELECT COUNT(*) FROM sys.dm_tran_commit_table; REVERT;\n" "$CONN_USER" | $SRC_SQL "$DB" | tr -d '[:space:]')"
dbx schemas create "$S" "$CATALOG" >/dev/null
obj() { echo "{\"table\":{\"source_catalog\":\"$DB\",\"source_schema\":\"lfcddemo\",\"source_table\":\"$1\",\"destination_catalog\":\"$CATALOG\",\"destination_schema\":\"$S\",\"table_configuration\":{\"scd_type\":\"$2\"}}}"; }
# PIPELINE_CONF='{"pipelines.<key>":"<value>"}': pipeline settings (a knob under test)
PC=${PIPELINE_CONF:-'{}'}
AP=$(dbx pipelines create -o json --json "{\"name\":\"${PREFIX}_scd2ct_$E\",\"catalog\":\"$CATALOG\",\"schema\":\"$S\",\"serverless\":true,\"configuration\":$PC,
  \"ingestion_definition\":{\"connection_name\":\"$CONNECTION\",\"connector_type\":\"CDC\",\"objects\":[$(obj s1ct SCD_TYPE_1),$(obj s2ct SCD_TYPE_2),$(obj s2cdc SCD_TYPE_2)]}}" | yq '.pipeline_id')
echo "pipeline $AP"
bash "$(dirname "$0")/../../lfc-pipeline-diagram/scripts/show-diagram.sh" "$AP"   # what was created (DIAGRAM=0 skips it)
STG="$T.__${AP//-/_}_cdc_staging_table"
fp() { q "SELECT (SELECT COUNT(*) FROM $T.s1ct), (SELECT COUNT(*) FROM $T.s2ct), (SELECT COUNT(*) FROM $T.s2cdc), (SELECT COUNT(*) FROM $STG)" 2>/dev/null | tr '\t' ' '; }
# run: one update, stopped once it has run 3 minutes and fp() is unchanged for 3 polls
run() {
  local u st f pf="" stable=0 t0; t0=$(date +%s)
  u=$(dbx pipelines start-update "$AP" -o json 2>/dev/null | yq '.update_id'); [ -z "$u" ] || [ "$u" = null ] && u=$(dbx pipelines list-updates "$AP" --max-results 1 -o json | yq '.updates[0].update_id')
  for _ in $(seq 1 90); do
    st=$(dbx pipelines get-update "$AP" "$u" -o json | yq '.update.state'); case $st in COMPLETED|FAILED|CANCELED) break;; esac
    if [ "$st" = RUNNING ]; then f=$(fp); if [ -n "$f" ] && [ "$f" = "$pf" ]; then stable=$((stable + 1)); else stable=0; fi; pf=$f
      [ $stable -ge 3 ] && [ $(( $(date +%s) - t0 )) -ge 180 ] && { dbx pipelines stop "$AP" >/dev/null 2>&1; st="STOPPED after $(( $(date +%s) - t0 ))s"; break; }; fi
    sleep 20
  done
  echo "  update $st; errors: $(dbx api get "/api/2.0/pipelines/$AP/events?max_results=250" -o json | U=$u yq -r '[.events[] | select(.origin.update_id == strenv(U)) | .error.exceptions[]?.error_class] | unique | join(" ")')"
}
show() {
  echo "  destination rows (s1ct s2ct s2cdc): $(q "SELECT (SELECT COUNT(*) FROM $T.s1ct), (SELECT COUNT(*) FROM $T.s2ct), (SELECT COUNT(*) FROM $T.s2cdc)" | tr '\t' ' '); s2ct ids: $(q "SELECT CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(id))) FROM $T.s2ct")"
  echo "  staging changes by table: $(q "SELECT CONCAT_WS(' ', COLLECT_LIST(c)) FROM (SELECT __table__ || ':' || operation || '=' || CAST(COUNT(*) AS STRING) AS c FROM $STG GROUP BY __table__, operation ORDER BY 1)")"
}
echo "initial load"; run; show
echo "step A: 2 inserts per table, no DDL"
$SRC_SQL "$DB" >/dev/null <<'SQL'
INSERT INTO lfcddemo.s1ct (id, v) VALUES (11, 'a'), (12, 'a');
INSERT INTO lfcddemo.s2ct (id, v) VALUES (11, 'a'), (12, 'a');
INSERT INTO lfcddemo.s2cdc (id, v) VALUES (11, 'a'), (12, 'a');
SQL
run; show; run; show
echo "step A2: 2 more inserts per table, no DDL"
$SRC_SQL "$DB" >/dev/null <<'SQL'
INSERT INTO lfcddemo.s1ct (id, v) VALUES (21, 'a2'), (22, 'a2');
INSERT INTO lfcddemo.s2ct (id, v) VALUES (21, 'a2'), (22, 'a2');
INSERT INTO lfcddemo.s2cdc (id, v) VALUES (21, 'a2'), (22, 'a2');
SQL
run; show
echo "step B: nullable ADD COLUMN + 2 inserts per table"
$SRC_SQL "$DB" >/dev/null <<'SQL'
ALTER TABLE lfcddemo.s1ct ADD c1 INT NULL;
ALTER TABLE lfcddemo.s2ct ADD c1 INT NULL;
ALTER TABLE lfcddemo.s2cdc ADD c1 INT NULL;
GO
INSERT INTO lfcddemo.s1ct (id, v, c1) VALUES (13, 'b', 1), (14, 'b', 1);
INSERT INTO lfcddemo.s2ct (id, v, c1) VALUES (13, 'b', 1), (14, 'b', 1);
INSERT INTO lfcddemo.s2cdc (id, v, c1) VALUES (13, 'b', 1), (14, 'b', 1);
SQL
run; show; run; show
echo "source change tracking: $(printf "SET NOCOUNT ON; SELECT STRING_AGG(t, ' ') FROM (SELECT 's1ct=' + CAST(COUNT(*) AS varchar) t FROM CHANGETABLE(CHANGES lfcddemo.s1ct, 0) c UNION ALL SELECT 's2ct=' + CAST(COUNT(*) AS varchar) FROM CHANGETABLE(CHANGES lfcddemo.s2ct, 0) c) x;\n" | $SRC_SQL "$DB")"
${LFC_RUNTIME:-$HERE/../../../bin/lfc-runtime.sh} --summary "$PROFILE" "$AP"
