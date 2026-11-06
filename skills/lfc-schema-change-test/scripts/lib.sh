# lib.sh: shared steps of the SQL Server Lakeflow Connect scenario tests. Source it from a test script after setting
# PROFILE, CONNECTION, CATALOG, SRC_SQL, CONN_USER (see sqlserver-add-column.sh for what each is), or SOURCE_JSON=<contract
# file> for the source (lfc-source-contract), then:
# (ENGINE=postgres for a PostgreSQL source: SRC_SQL is then scripts/vm-psql.sh or alike, src_util and track are not
# used, and each pipeline gets its own publication and slot, both named $SLOT)
#
#   lfc_init <short-name>      pre-flight checks; sets PREFIX, WAREHOUSE, E (epoch), DB, S, STG, GWN, APN, W, T
#   src_db  <<'EOF' ... EOF    create the fresh source database $DB and run the T-SQL in it (schema lfcddemo, tables)
#   src_util                   install the utility objects in $DB
#   src <<'EOF' ... EOF        run T-SQL in $DB
#   obj <table> [json]         one ingestion object (SCD=SCD_TYPE_2 for type 2; extra table_configuration JSON)
#   pipelines_create <objs>    MODE=standard: gateway + ingestion pipeline; MODE=integrated: one pipeline. Sets GW, AP
#   update [t1,t2]             start an update (full refresh of the listed tables), follow the chain until idle
#                              (LFC_DEADLINE epoch seconds or LFC_DEADLINE_MIN minutes: stop and report "timed out")
#   settle <table> <column>    after a source change, update until an error or <column> shows in the destination
#   expect <want> <sql>        query; after an early stop that didn't give <want>, one more update and query again
#   errs_for <table>, needs_refresh <table>, q <sql>, claim, observe, result_md <title>
#
# FP_TABLES (space-separated destination tables) drives the integrated-CDC early stop. The EXIT trap stops the
# pipelines and keeps them, the schemas and the source database for KEEP_DAYS days (default 7), recorded in $LFC_KEPT;
# kept-sweep.sh deletes them after that, or earlier on request. KEEP_DAYS=0 deletes at exit; KEEP=1 leaves everything
# as it is (running) and unrecorded.
export PATH=/opt/homebrew/bin:$PATH
# SOURCE_JSON=<contract file> (lfc-source-contract): the source comes from it (SRC_SQL, CONN_USER, CONNECTION, ...); the
# variables it doesn't give stay as they are in the environment. LFC_SOURCE_ENV: where source-env.sh is (parallel.sh sets it)
if [ -n "${SOURCE_JSON:-}" ]; then
  SE=$("${LFC_SOURCE_ENV:-$(dirname "${BASH_SOURCE[0]}")/../../lfc-source-contract/scripts/source-env.sh}" "$SOURCE_JSON") || exit 2
  eval "$SE"
fi
: "${PROFILE:?}" "${CONNECTION:?}" "${CATALOG:?}" "${SRC_SQL:?}" "${CONN_USER:?}"
LIB=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WAIT=${GATEWAY_WAIT:-90}
ENGINE=${ENGINE:-sqlserver}   # sqlserver | postgres
MODE=${MODE:-standard}   # standard: gateway + ingestion pipeline; integrated: one pipeline (connector_type CDC)
case $MODE in standard|integrated) ;; *) echo "MODE must be standard or integrated"; exit 2;; esac
[ "$MODE" = integrated ] && WAIT=0   # integrated CDC reads the source inside each update
dbx() { "${DATABRICKS_CLI:-databricks}" "$@" --profile "$PROFILE"; }   # DATABRICKS_CLI: another CLI (a test stub)
RESULTS=(); FAILED=0
claim() { RESULTS+=("| \`$1\` | $2 | $3 |"); [ "$2" = PASS ] || FAILED=1; echo "$2 $1: $3"; }
observe() { RESULTS+=("| \`$1\` | OBSERVED | $2 |"); echo "OBSERVED $1: $2"; }   # no expected answer yet
# example <title> <source columns> <source query> <destination columns> <destination query>: what went in and what came
# out, side by side in the result. Lakeflow Connect is tested as a process whose behavior we don't assume: change the
# input, read the output, and show both, so a reader sees the behavior in the data, not only in a claim. Columns are
# "a|b|c"; the source query (run with src) returns tab-separated rows (CONCAT_WS(CHAR(9), ...) on SQL Server), the
# destination query (run with q) its columns. Keep each to a few rows: the ones that show the behavior.
EXAMPLES=()
md_rows() { awk -F'\t' '{printf "|"; for (i = 1; i <= NF; i++) printf " %s |", $i; print ""}'; }
md_head() { echo "| ${1//|/ | } |"; echo "|$(echo "$1" | tr -cd '|' | sed 's/|/---|/g')---|"; }
example() {
  local in out; in=$(printf 'SET NOCOUNT ON; %s\n' "$3" | src 2>&1 | md_rows); out=$(q "$5" 2>&1 | md_rows)
  EXAMPLES+=("### $1" "" "Input (source):" "" "$(md_head "$2")" "$in" "" "Output (destination):" "" "$(md_head "$4")" "$out" "")
  printf 'EXAMPLE %s\n%s\n%s\n' "$1" "$in" "$out"
}

lfc_init() {
  ME=$(dbx current-user me -o json | yq '.userName')
  PREFIX=${PREFIX:-$(echo "$ME" | cut -d@ -f1 | tr -d '.-' | tr 'A-Z' 'a-z')}
  local root def
  root=$(dbx catalogs get "$CATALOG" -o json | yq '.storage_root // ""')
  def=$(dbx external-locations get __databricks_managed_storage_location -o json 2>/dev/null | yq '.url // ""')
  # no storage root of its own: the catalog uses the metastore's (e.g. main), which CDC accepts; only Default Storage fails
  [ -n "$root" ] || echo "catalog $CATALOG has no storage root of its own: using the metastore's"
  [ -n "$def" ] && [[ "$root" == "$def"* ]] && { echo "catalog $CATALOG is on Default Storage: CDC refuses it"; exit 2; }
  local ct=SQLSERVER; [ "$ENGINE" = postgres ] && ct=POSTGRESQL
  [ "$(dbx connections get "$CONNECTION" -o json | yq '.connection_type')" = $ct ] || { echo "$CONNECTION is not $ct"; exit 2; }
  WAREHOUSE=${WAREHOUSE:-$(dbx warehouses list -o json | yq '[.[] | select(.enable_serverless_compute)][0].id')}
  export WAREHOUSE
  E=$(date +%s); [ -n "${LFC_DEADLINE_MIN:-}" ] && [ -z "${LFC_DEADLINE:-}" ] && LFC_DEADLINE=$((E + LFC_DEADLINE_MIN * 60))
  DB=${PREFIX}_$1_$E; BASE=${PREFIX}_$(echo "$CONNECTION" | tr -- '-' '_' | sed "s/^${PREFIX}_//")
  S=${BASE}_$E; STG=${S}_stg; GWN=${BASE}_gw_$E; APN=${BASE}_ap_$E; W=$(mktemp -d)
  T="\`$CATALOG\`.$S"; SLOT=${PREFIX}_$1_$E
  echo "run $E ($MODE): source database $DB, catalog $CATALOG, schemas $S and $STG"
  trap cleanup EXIT
}

# sql "<statement>": run on the warehouse, print rows tab-separated
sql() {
  # JSON_ARRAY + INLINE: without them some warehouses answer with an Arrow attachment and no data_array
  yq -n -o json '.warehouse_id = strenv(WAREHOUSE) | .wait_timeout = "50s" | .format = "JSON_ARRAY" | .disposition = "INLINE" | .statement = strenv(Q)' > "$W/st.json"
  dbx api post /api/2.0/sql/statements --json @"$W/st.json" > "$W/st.out"
  # a statement still running after the 50 s wait (a count over hundreds of tables): poll it, up to 15 minutes
  local id st n=0; st=$(yq '.status.state' "$W/st.out"); id=$(yq '.statement_id' "$W/st.out")
  while { [ "$st" = PENDING ] || [ "$st" = RUNNING ]; } && [ $n -lt 90 ]; do
    sleep 10; n=$((n + 1)); dbx api get "/api/2.0/sql/statements/$id" > "$W/st.out"; st=$(yq '.status.state' "$W/st.out")
  done
  [ "$(yq '.status.state' "$W/st.out")" = SUCCEEDED ] || { yq '.status.state + ": " + (.status.error.message // "")' "$W/st.out" | cut -c1-300 >&2; return 1; }
  yq -r '.result.data_array[]? | @tsv' "$W/st.out"
}
q() { export Q=$1; sql; }
# rq "<T-SQL>": run a query on the source through the UC connection (what serverless sees)
rq() { q "SELECT * FROM remote_query('$CONNECTION', database => '$DB', query => '${1//\'/\'\'}')"; }

# --- source ---------------------------------------------------------------------------------------------------------
src_db() {
  if [ "$ENGINE" = postgres ]; then
    echo "CREATE DATABASE $DB;" | $SRC_SQL postgres >/dev/null || { echo "source setup failed"; exit 2; }
    SRC_CREATED=1
    { echo "CREATE SCHEMA lfcddemo;"; cat
      printf 'GRANT CONNECT ON DATABASE %s TO %s;\nGRANT USAGE ON SCHEMA lfcddemo TO %s;\nGRANT SELECT ON ALL TABLES IN SCHEMA lfcddemo TO %s;\nALTER DEFAULT PRIVILEGES IN SCHEMA lfcddemo GRANT SELECT ON TABLES TO %s;\n' \
        "$DB" "$CONN_USER" "$CONN_USER" "$CONN_USER" "$CONN_USER"; } | $SRC_SQL "$DB" >/dev/null || { echo "source setup failed"; exit 2; }
    return
  fi
  { printf 'CREATE DATABASE %s;\nGO\nUSE %s;\nGO\nCREATE SCHEMA lfcddemo;\nGO\n' "$DB" "$DB"; cat
    printf '\nGO\nCREATE USER %s FOR LOGIN %s;\n' "$CONN_USER" "$CONN_USER"; } | $SRC_SQL master >/dev/null \
    || { echo "source setup failed"; exit 2; }
  SRC_CREATED=1
}
src() { $SRC_SQL "$DB"; }
src_util() {
  curl -fsSL -o "$W/utility_script.sql" https://learn.microsoft.com/en-us/azure/databricks/_extras/documents/utility_script.sql
  $SRC_SQL "$DB" < "$W/utility_script.sql" >/dev/null || { echo "utility objects install failed"; exit 2; }
  UTIL=$(printf 'SET NOCOUNT ON; SELECT dbo.lakeflowUtilityVersion();\n' | src | head -1 | tr -d '[:space:]')
}
# track <ct|cdc> <table> [table...]: change tracking or CDC on lfcddemo.<table>, with the connection login's grants
# All tables in one setup call (a comma list), then one query lists the ones not tracked (sys.tables.is_tracked_by_cdc,
# sys.change_tracking_tables), which are set up again, up to 3 times: enabling CDC in several databases at once
# (parallel runs) can fail, and a table left untracked fails the run later with INGESTION_GATEWAY_CDC_NOT_ENABLED,
# which looks like a product result. One call per table made ~3 SSH calls a table and lost one of 200 (2026-10-23).
track() {
  local kind=$1 n left proc=lakeflowSetupChangeDataCapture tv="(SELECT object_id FROM sys.tables WHERE is_tracked_by_cdc = 1)"; shift
  local ddl=""
  [ "$kind" = ct ] && { proc=lakeflowSetupChangeTracking; tv="(SELECT object_id FROM sys.change_tracking_tables)"; ddl=", @CreateDdlSupportingObjects = 1"; }
  untracked() { printf "SET NOCOUNT ON; SELECT STRING_AGG(n, ' ') FROM (VALUES %s) v(n) WHERE OBJECT_ID('lfcddemo.' + n) IS NULL OR OBJECT_ID('lfcddemo.' + n) NOT IN %s;\n" \
    "$(printf "('%s')," "$@" | sed 's/,$//')" "$tv" | src | tr -s '[:space:]' ' ' | sed 's/^ //; s/ $//; s/^NULL$//'; }
  left="$*"
  for n in 1 2 3; do
    # change tracking: utility objects 1.7 create the DDL audit table and trigger only on request, and the pipelines
    # still need them (INGESTION_GATEWAY_DDL_OBJECTS_MISSING without them, dlt 18.3.10)
    echo "EXEC dbo.$proc @Tables = '$(printf 'lfcddemo.%s,' $left | sed 's/,$//')', @User = '$CONN_USER'${ddl:-};" | src > "$W/track.out" 2>&1
    left=$(untracked "$@"); [ -z "$left" ] && break
    echo "track $kind: not tracked after try $n: $left ($(grep -i -m1 -E 'error|msg [0-9]' "$W/track.out" | cut -c1-160))" >&2; sleep 10
  done
  [ -z "$left" ] || { echo "could not set up $kind on lfcddemo: $left"; exit 2; }
  echo "EXEC dbo.lakeflowFixPermissions @User = '$CONN_USER', @Tables = 'SCHEMAS:lfcddemo';" | src >/dev/null
}
# fill <table> <n> [first id]: n rows (id, v='before')
fill() {
  [ "$ENGINE" = postgres ] && { printf "INSERT INTO lfcddemo.%s (id, v) SELECT g, 'before' FROM generate_series(%s, %s) g;\n" "$1" "${3:-1}" "$(( ${3:-1} + $2 - 1 ))" | src >/dev/null; return; }
  printf "INSERT INTO lfcddemo.%s (id, v) SELECT TOP %s %s + ROW_NUMBER() OVER (ORDER BY (SELECT 1)) - 1, 'before' FROM sys.all_objects;\n" "$1" "$2" "${3:-1}" | src >/dev/null; }

# --- pipelines ------------------------------------------------------------------------------------------------------
obj() { echo "{\"table\":{\"source_catalog\":\"$DB\",\"source_schema\":\"lfcddemo\",\"source_table\":\"$1\",\"destination_catalog\":\"$CATALOG\",\"destination_schema\":\"$S\",\"table_configuration\":{\"scd_type\":\"${SCD:-SCD_TYPE_1}\"${2:+,$2}}}}"; }
AFR='"auto_full_refresh_policy":{"enabled":true,"min_interval_hours":1}'
# gateway_create: a continuous gateway with its own staging schema $STG, serverless (GATEWAY_COMPUTE=classic: a classic
# cluster with the default node type; dogfood-azure's serverless gateway failed with DeploymentException on 2026-11-06). Sets GW
gateway_create() {
  local pc=${PIPELINE_CONF:-'{}'} compute='"serverless":true'
  [ "${GATEWAY_COMPUTE:-serverless}" = classic ] && compute='"clusters":[{"label":"default"}]'
  dbx schemas create "$STG" "$CATALOG" >/dev/null 2>&1
  GW=$(dbx pipelines create -o json --json "{\"name\":\"$GWN\",\"catalog\":\"$CATALOG\",\"schema\":\"$STG\",$compute,\"continuous\":true,\"configuration\":$pc,
    \"gateway_definition\":{\"connection_name\":\"$CONNECTION\",\"gateway_storage_catalog\":\"$CATALOG\",\"gateway_storage_schema\":\"$STG\",\"gateway_storage_name\":\"$GWN\"}}" | yq '.pipeline_id')
  [ -n "$GW" ] && [ "$GW" != null ] || { echo "gateway create failed"; exit 2; }
}
pipelines_create() {
  dbx schemas create "$S" "$CATALOG" >/dev/null || exit 2
  if [ "$MODE" = standard ]; then
    gateway_create
    SRCDEF="\"ingestion_gateway_id\":\"$GW\""
  else
    SRCDEF="\"connection_name\":\"$CONNECTION\",\"connector_type\":\"CDC\""
  fi
  # Postgres: one publication per pipeline, for the whole test schema (PostgreSQL 15+), so a table added later is in it
  [ "$ENGINE" = postgres ] && { echo "CREATE PUBLICATION ${SLOT}_pub FOR TABLES IN SCHEMA lfcddemo;" | src >/dev/null || { echo "publication create failed"; exit 2; }; }
  [ "$ENGINE" = postgres ] && SRCDEF+=",\"source_type\":\"POSTGRESQL\",\"source_configurations\":[{\"catalog\":{\"source_catalog\":\"$DB\",\"postgres\":{\"slot_config\":{\"slot_name\":\"$SLOT\",\"publication_name\":\"${SLOT}_pub\"}}}}]"
  # CREATE_SOFT=1: a refused create returns 1 with the error in CREATE_ERR (a test whose question is "is it refused?")
  # PIPELINE_CONF='{"pipelines.<key>":"<value>"}': pipeline settings (a knob under test), on the gateway too; recorded with the run
  [ -n "${PIPELINE_CONF:-}" ] && conf pipeline_conf "$PIPELINE_CONF"
  local pc=${PIPELINE_CONF:-'{}'}
  dbx pipelines create -o json --json "{\"name\":\"$APN\",\"catalog\":\"$CATALOG\",\"schema\":\"$S\",\"serverless\":true,\"configuration\":$pc,
    \"ingestion_definition\":{$SRCDEF,\"objects\":[$1]}}" > "$W/create.out" 2>&1
  AP=$(yq '.pipeline_id' "$W/create.out" 2>/dev/null)
  if [ -z "$AP" ] || [ "$AP" = null ]; then
    AP=""; CREATE_ERR=$(grep -v -E '^(Profile|Host|Auth type|Next steps|  -|$)' "$W/create.out" | head -2 | tr '\n' ' ' | cut -c1-300)
    [ "${CREATE_SOFT:-0}" = 1 ] && return 1
    echo "ingestion pipeline create failed: $CREATE_ERR"; exit 2
  fi
  # Postgres: the slot after the pipelines exist, so a refused create leaves none holding WAL
  [ "$ENGINE" = postgres ] && { echo "SELECT pg_create_logical_replication_slot('$SLOT', 'pgoutput');" | src >/dev/null || { echo "slot create failed"; exit 2; }; }
  echo "LINK pipeline $(dbx auth describe -o json 2>/dev/null | yq -r '.details.host // ""')/pipelines/$AP"
  # the pipeline as a diagram (lfc-pipeline-diagram): text on stderr, text + Mermaid in $W/diagram.md; DIAGRAM=0 skips it
  [ "${DIAGRAM:-1}" = 1 ] && pipeline_diagram
  return 0
}
pipeline_diagram() {
  DIAGRAM_MD="$W/diagram.md" bash "$(cd "$(dirname "${BASH_SOURCE[0]}")/../../lfc-pipeline-diagram/scripts" && pwd)/show-diagram.sh" "$AP"
}
# set_objects <objs>: replace the ingestion pipeline's table list (pipelines update needs the whole spec)
set_objects() {
  dbx pipelines get "$AP" -o json | O="[$1]" yq -o json '.spec | .ingestion_definition.objects = (strenv(O) | from_json)' > "$W/spec.json"
  dbx pipelines update "$AP" --json @"$W/spec.json" >/dev/null
}

# stg_cdc: (standard CDC) the gateway's change files in its staging volume, one JSON line per change: table_id.name,
# operation, data (the row, columns under generated names), cursor.timestamp (source commit, ms) and
# cursor.extractionTimestamp (when the gateway read it, ms). Read with read_files(..., format => 'json'). They show what
# the gateway captured and when it read each table, before the ingestion pipeline applies it.
stg_cdc() {
  local v; v=$(dbx volumes list "$CATALOG" "$STG" -o json | yq -r '.[].name | select(test("^__databricks_ingestion_gateway_staging_data-"))' | head -1)
  [ -n "$v" ] && echo "/Volumes/$CATALOG/$STG/$v/${v#__databricks_ingestion_gateway_staging_data-}/cdc/"
}

# --- updates --------------------------------------------------------------------------------------------------------
# update [table,table]: start an ingestion update (full refresh of the listed tables) and wait until the pipeline is idle.
# A failed or canceled update can be followed by one the pipeline starts itself (cause RETRY_ON_FAILURE or SCHEMA_CHANGE;
# seen on integrated CDC after an added column, 2026-10-20), and while that one runs start-update is refused. So the
# wait follows the chain: STATE is the last update's state, ERRS the error classes of every update in it, CHAIN the
# cause and state of each.
# Integrated CDC keeps an update running until its smart closure (20 minutes or more) even when nothing is left to do.
# So in that mode (unless EARLY_STOP=0) an update that has run 3 minutes is stopped once fp() is unchanged for 3 polls;
# EARLY=1 then, and ok_state accepts it in place of COMPLETED.
fp() { q "SELECT $(for t in $FP_TABLES "__${AP//-/_}_cdc_staging_table"; do printf '(SELECT COUNT(*) FROM %s.%s),' "$T" "$t"; done)
  (SELECT COUNT(*) FROM \`$CATALOG\`.information_schema.columns WHERE table_schema = '$S')" 2>/dev/null | tr '\t' ' '; }
ok_state() { [ "$STATE" = COMPLETED ] || [ "${EARLY:-0}" = 1 ]; }
# expect <expected> <sql>: print the query's result. When it isn't <expected> and the last update was stopped early, run
# one more update first (it picks up what the stopped one left over) and query again. The last update's events are
# kept, so errs_for still reads the update the step ran.
expect() {
  local r; r=$(q "$2" 2>&1)
  if [ "$r" != "$1" ] && [ "${EARLY:-0}" = 1 ]; then
    cp "$W/ev_last.json" "$W/ev_keep.json"; echo "after an early stop got '${r//$'\t'/ }': one more update" >&2
    update >&2; r=$(q "$2" 2>&1); cp "$W/ev_keep.json" "$W/ev_last.json"
  fi
  echo "$r"
}
# snaps_done <update> <table,table>: the extractor finished reading every listed table in that update (its
# extractor_..._<table>_snapshot_flow reported COMPLETED). A full refresh re-reads the table and counts can sit still
# while it does, so for a refresh the early stop only counts stable polls after this. ("Snapshot of ... has been
# APPLIED" is no use here: it comes when the update closes.)
snaps_done() {
  local t n tok="" pg; : > "$W/ev_snap.jsonl"
  for pg in 1 2 3 4; do   # newest first, 250 a page: read on until a page has this update's oldest events
    dbx api get "/api/2.0/pipelines/$AP/events?max_results=250${tok:+&page_token=$tok}" -o json > "$W/ev_snap_page.json"
    yq -o json -I0 '.events[]' "$W/ev_snap_page.json" >> "$W/ev_snap.jsonl"
    tok=$(yq '.next_page_token // ""' "$W/ev_snap_page.json"); [ -z "$tok" ] && break
  done
  yq -p json -o json ea '[.] | {"events": .}' "$W/ev_snap.jsonl" > "$W/ev_snap.json"
  for t in ${2//,/ }; do
    n=$(U=$1 T1=$t yq '[.events[] | select(.origin.update_id == strenv(U) and .event_type == "flow_progress" and .details.flow_progress.status == "COMPLETED" and ((.origin.flow_name // "") | test("^extractor_.*_" + strenv(T1) + "_snapshot_flow$")))] | length' "$W/ev_snap.json")
    [ "${n:-0}" -ge 1 ] || return 1
  done
}
# past_deadline: LFC_DEADLINE (epoch seconds; or LFC_DEADLINE_MIN minutes after lfc_init) has passed. update() then
# stops the running update and reports it as "timed out (deadline)", so a batch with a hard stop ends on time.
past_deadline() { [ -n "${LFC_DEADLINE:-}" ] && [ "$(date +%s)" -ge "$LFC_DEADLINE" ]; }
newest() { dbx pipelines list-updates "$AP" --max-results 1 -o json | yq '.updates[0].update_id'; }
update() {
  local body='{}' out u ids=() p tok=""; [ -n "${1:-}" ] && body=$(echo "$1" | yq -o json -I0 'split(",") | {"full_refresh_selection": .}')
  out=$(dbx pipelines start-update "$AP" --json "$body" -o json 2>&1); u=$(echo "$out" | yq '.update_id' 2>/dev/null)
  if [ -z "$u" ] || [ "$u" = null ]; then
    u=$(newest); echo "start-update refused ($(echo "$out" | head -1 | cut -c1-120)); following $u"
  fi
  CHAIN=""; EARLY=0
  while :; do
    ids+=("$u"); local t0 f pf="" stable=0 early=""; t0=$(date +%s)
    for _ in $(seq 1 90); do
      STATE=$(dbx pipelines get-update "$AP" "$u" -o json | yq '.update.state')
      case $STATE in COMPLETED|FAILED|CANCELED) break;; esac
      if past_deadline; then
        dbx pipelines stop "$AP" >/dev/null 2>&1; early=" (timed out: deadline)"; break
      fi
      if [ "$MODE" = integrated ] && [ "${EARLY_STOP:-1}" = 1 ] && [ "$STATE" = RUNNING ]; then
        f=$(fp); if [ -n "$f" ] && [ "$f" = "$pf" ]; then stable=$((stable + 1)); else stable=0; fi; pf=$f
        [ -n "${1:-}" ] && ! snaps_done "$u" "$1" && stable=0   # a refresh: count only after the extractor read the tables
        if [ $stable -ge 3 ] && [ $(( $(date +%s) - t0 )) -ge 180 ]; then
          dbx pipelines stop "$AP" >/dev/null 2>&1; EARLY=1; early=" (stopped early after $(( $(date +%s) - t0 ))s)"
          STATE=$(dbx pipelines get-update "$AP" "$u" -o json | yq '.update.state'); break
        fi
      fi
      sleep 20
    done
    CHAIN+="${CHAIN:+ -> }$(dbx pipelines get-update "$AP" "$u" -o json | yq '.update.cause') $STATE$early"
    case $STATE in COMPLETED|FAILED|CANCELED) ;; *) break;; esac   # timed out: don't follow further
    sleep 30   # a follow-up update starts within seconds of the last one ending
    p=$(newest); [ "$p" = "$u" ] && break; u=$p
  done
  : > "$W/ev_chain.json"
  for _ in 1 2 3 4; do   # events of every update in the chain; newest first, 250 a page
    dbx api get "/api/2.0/pipelines/$AP/events?max_results=250${tok:+&page_token=$tok}" -o json > "$W/ev_page.json"
    yq -o json -I0 '.events[]' "$W/ev_page.json" >> "$W/ev_chain.json"
    tok=$(yq '.next_page_token // ""' "$W/ev_page.json"); [ -z "$tok" ] && break
    IDS="${ids[0]}" yq -e '[.events[] | select(.origin.update_id == strenv(IDS))] | length > 0' "$W/ev_page.json" >/dev/null 2>&1 && break
  done
  for p in "${ids[@]}"; do U=$p yq -p json -o json -I0 'select(.origin.update_id == strenv(U))' "$W/ev_chain.json"; done > "$W/ev_sel.json"
  if [ -s "$W/ev_sel.json" ]; then yq -p json -o json ea '[.]' "$W/ev_sel.json" > "$W/ev_last.json"; else echo '[]' > "$W/ev_last.json"; fi
  ERRS=$(yq -r '.[] | .error.exceptions[]?.error_class // ""' "$W/ev_last.json" | grep -v '^$' | sort -u | tr '\n' ' ')
  echo "update ${1:+(full refresh $1) }$STATE ${ERRS}[$CHAIN]"
  # the message of each error, so a failure can be read after cleanup has deleted the pipeline
  [ -n "${ERRS// /}" ] && yq -r '.[] | .error.exceptions[]? | select(.error_class) | .message | @json' "$W/ev_last.json" \
    | sort -u | cut -c1-300 | sed 's/^/  error: /'
  return 0
}

# errs_for <table>: error classes the last update reported for that table's flows
# needs_refresh <table>: the last update reported an error for that table that asks for a full refresh. The hand-off
# errors of a schema change (CDC_APPLIER_SCHEMA_CHANGED_DURING_STREAMING, FLOW_SCHEMA_CHANGED) don't, and a needless
# refresh resets an SCD type 2 table's history.
needs_refresh() { errs_for "$1" | grep -qE 'CDC_INCOMPATIBLE_SCHEMA_CHANGES|PRIMARY_KEY_CHANGE|CDC_LOG_TRUNCATED'; }
errs_for() { T1=$1 yq -r '.[] | select((.origin.flow_name // "") | test("_" + strenv(T1) + "_")) | .error.exceptions[]?.error_class // ""' "$W/ev_last.json" | grep -v '^$' | sort -u | tr '\n' ' '; }
# msgs_for <table>: the messages of those errors, one per line
msgs_for() { T1=$1 yq -r '.[] | select((.origin.flow_name // "") | test("_" + strenv(T1) + "_")) | .error.exceptions[]? | select(.error_class) | .message' "$W/ev_last.json" | sort -u | cut -c1-200; }

# settle <table> <column>: after a source change, run updates until one reports an error, or <column> shows up in the
# destination <table> (applied inline). The gateway can take longer than one fixed wait to pick up a change, and an
# update that ran before it did proves nothing.
settle() {
  for _ in 1 2 3 4; do
    sleep "$WAIT"; update
    [ -n "${ERRS// /}" ] && return
    q "SELECT $2 FROM $T.$1 LIMIT 0" >/dev/null 2>&1 && return
  done
}
# settle_rows <sql> <want> [table]: after a source DML change, update until <sql> returns <want> or an update reports
# an error (4 tries): any error, or with <table> only that table's (errs_for). SETTLED=1 when it matched; ROWS is the
# last answer, TERRS the table's errors.
settle_rows() {
  SETTLED=0; ROWS=""; TERRS=""
  for _ in 1 2 3 4; do
    sleep "$WAIT"; update
    ROWS=$(q "$1" 2>&1); [ -n "${3:-}" ] && TERRS=$(errs_for "$3")
    [ "$ROWS" = "$2" ] && { SETTLED=1; return; }
    if [ -n "${3:-}" ]; then [ -n "${TERRS// /}" ] && return; else [ -n "${ERRS// /}" ] && return; fi
  done
}

# src_owned: 1 when this run made the source database. Decided by its name (it ends in this run's epoch), not by a
# variable src_db sets: a script that pipes into src_db (`... | src_db`) runs it in a subshell, and the variable was lost,
# so cleanup left those databases behind while reporting none left.
src_owned() { [[ "${DB:-}" == *"_${E:-none}" ]] && echo 1 || echo 0; }
LFC_KEPT=${LFC_KEPT:-$HOME/.local/state/lfc-tests/kept.jsonl}
KEEP_DAYS=${KEEP_DAYS:-7}
# A script's own resources besides AP, GW, S and STG (space-separated): kept and deleted with the run.
#   EXTRA_PIPELINES="<id> ..."  EXTRA_SCHEMAS="<catalog>.<schema> ..."  EXTRA_SQL="<SQL run in $DB when the run is deleted>"
EXTRA_PIPELINES=${EXTRA_PIPELINES:-}; EXTRA_SCHEMAS=${EXTRA_SCHEMAS:-}; EXTRA_SQL=${EXTRA_SQL:-}
cleanup() {
  [ "${KEEP:-0}" = 1 ] && { echo "KEEP=1: left $DB, $GWN, $APN, $CATALOG.$S"; return; }
  if [ "$KEEP_DAYS" != 0 ]; then keep_run; rm -rf "${W:?}"; return; fi
  delete_run
  rm -rf "${W:?}"
}
# kept_lock / kept_unlock: one writer of $LFC_KEPT at a time (keep_run's append, kept-sweep.sh's rewrite); a mkdir lock,
# as flock isn't on macOS. A lock older than 10 minutes is taken over (a writer that died).
kept_lock() {
  local n=0; mkdir -p "$(dirname "$LFC_KEPT")"
  until mkdir "$LFC_KEPT.lock" 2>/dev/null; do
    n=$((n + 1)); [ $n -ge 600 ] && { rm -rf "${LFC_KEPT:?}.lock"; n=0; }; sleep 1
  done
}
kept_unlock() { rm -rf "${LFC_KEPT:?}.lock"; }
# drop_slot: (PostgreSQL) drop the run's replication slot; retried while a stopped or deleted pipeline's session lingers
drop_slot() {
  local n
  for _ in 1 2 3; do
    echo "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '$SLOT' AND NOT active;" | $SRC_SQL postgres > "$W/slot.out" 2>&1
    n=$(echo "SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = '$SLOT';" | $SRC_SQL postgres)
    [ "${n//[[:space:]]/}" = 0 ] && return 0; sleep "${LFC_RETRY_SLEEP:-30}"
  done
  return 1
}
# keep_run: stop the pipelines (a continuous gateway too) and record the run in $LFC_KEPT with a delete-after date.
# A stopped pipeline costs nothing. PostgreSQL: the slot is dropped now, so no WAL is held for the week; the kept
# tables stay readable, the pipeline can't resume from its position (a full refresh would).
keep_run() {
  local p after host stopped=true slot_dropped=""
  for p in ${AP:-} ${GW:-} $EXTRA_PIPELINES; do
    [ -n "$p" ] && [ "$p" != null ] || continue
    dbx pipelines stop "$p" >/dev/null 2>&1 || { stopped=false; echo "WARNING: could not stop pipeline $p: it may still be running" >&2; }
  done
  if [ "$ENGINE" = postgres ] && [ -n "${SLOT:-}" ]; then
    if drop_slot; then slot_dropped=true; else slot_dropped=false; echo "WARNING: replication slot $SLOT not dropped: it holds WAL until it is" >&2; fi
  fi
  after=$(jq -rn --argjson d "$KEEP_DAYS" 'now + $d * 86400 | strftime("%Y-%m-%d")')
  host=$(dbx auth describe -o json 2>/dev/null | jq -r '.details.host // ""')
  kept_lock
  jq -nc --arg id "${S:-}" --arg after "$after" --arg host "$host" --arg profile "$PROFILE" --arg ap "${AP:-}" --arg gw "${GW:-}" \
    --arg catalog "$CATALOG" --arg s "${S:-}" --arg stg "${STG:-}" --arg engine "$ENGINE" --arg db "$([ "$(src_owned)" = 1 ] && echo "$DB")" \
    --arg src_db "$DB" --arg slot "${SLOT:-}" --arg conn "$CONNECTION" --arg conn_user "$CONN_USER" --arg src_sql "$SRC_SQL" \
    --arg source_json "${SOURCE_JSON:-}" --arg kept "$(date -u +%FT%TZ)" --argjson stopped "$stopped" --arg slot_dropped "$slot_dropped" \
    --arg keep_source "${KEEP_SOURCE:-0}" --arg xp "$EXTRA_PIPELINES" --arg xs "$EXTRA_SCHEMAS" --arg xsql "$EXTRA_SQL" \
    '{id: $id, kept: $kept, delete_after: $after, host: $host, profile: $profile, catalog: $catalog, schema: $s, staging: $stg,
      pipelines: ([$ap, $gw] | map(select(. != "" and . != "null"))), stopped: $stopped, engine: $engine, db: $db,
      src_db: $src_db, slot: $slot, slot_dropped: $slot_dropped, connection: $conn, conn_user: $conn_user, src_sql: $src_sql,
      source_json: $source_json, keep_source: $keep_source, extra_pipelines: ($xp | split(" ") | map(select(. != ""))),
      extra_schemas: ($xs | split(" ") | map(select(. != ""))), extra_sql: $xsql}' >> "$LFC_KEPT"
  kept_unlock
  echo "kept until $after ($([ "$stopped" = true ] && echo stopped || echo "STOP FAILED")$([ "$slot_dropped" = true ] && echo ", slot dropped"); delete earlier: kept-sweep.sh --now $S): ${host:+$host/pipelines/}${AP:-} ${GW:+gateway $GW,} schemas $CATALOG.$S${STG:+ and $STG}${EXTRA_SCHEMAS:+ $EXTRA_SCHEMAS}${DB:+, source database $DB}"
}
# delete_run: delete the pipelines, the schemas and (when this run made it) the source database; then EXTRA_*.
# kept-sweep.sh calls it with the variables of a kept run.
delete_run() {
  [ -n "${AP:-}" ] && [ "$AP" != null ] && dbx pipelines delete "$AP"; [ -n "${GW:-}" ] && [ "$GW" != null ] && dbx pipelines delete "$GW"; sleep "${LFC_DELETE_WAIT:-15}"
  for p in $EXTRA_PIPELINES; do dbx pipelines delete "$p"; done
  dbx schemas delete "$CATALOG.$S" --force 2>/dev/null; dbx schemas delete "$CATALOG.$STG" --force 2>/dev/null
  for x in $EXTRA_SCHEMAS; do dbx schemas delete "$x" --force 2>/dev/null; done
  # shellcheck disable=SC2034  # EXTRA_LEFT: read by kept-sweep.sh
  EXTRA_LEFT=0
  # shellcheck disable=SC2034
  [ -n "$EXTRA_SQL" ] && [ "$(src_owned)" != 1 ] && { echo "$EXTRA_SQL" | src >/dev/null || { EXTRA_LEFT=1; echo "EXTRA_SQL failed in $DB"; }; }
  # shellcheck disable=SC2034  # SLOT_LEFT: read by kept-sweep.sh
  SLOT_LEFT=0   # PostgreSQL, a source database the run didn't create: its slot (dropping the database drops an owned one)
  # shellcheck disable=SC2034
  if [ "$ENGINE" = postgres ] && [ -n "${SLOT:-}" ] && [ "$(src_owned)" != 1 ]; then drop_slot || { SLOT_LEFT=1; echo "replication slot $SLOT not dropped"; }; fi
  local left=0
  if [ "$(src_owned)" = 1 ] && [ "$ENGINE" = postgres ]; then
    for _ in 1 2 3; do   # the slot is held while the deleted gateway's session lingers
      printf "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '%s' AND NOT active;\nDROP DATABASE IF EXISTS %s WITH (FORCE);\n" "$SLOT" "$DB" \
        | $SRC_SQL postgres > "$W/drop.out" 2>&1
      left=$(printf "SELECT (SELECT COUNT(*) FROM pg_database WHERE datname = '%s') + (SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = '%s');\n" "$DB" "$SLOT" | $SRC_SQL postgres)
      [ "${left//[[:space:]]/}" = 0 ] && break; sleep "${LFC_RETRY_SLEEP:-30}"
    done
    [ "${left//[[:space:]]/}" = 0 ] || { echo "source database $DB or slot $SLOT not dropped:"; tail -3 "$W/drop.out"; }
  elif [ "$(src_owned)" = 1 ] && [ "${KEEP_SOURCE:-0}" = 1 ]; then
    echo "KEEP_SOURCE=1: source database $DB kept"
  elif [ "$(src_owned)" = 1 ]; then
    for _ in 1 2 3; do   # a pipeline that was just deleted can still hold a session for a while
      # CDC off in its own call: with sqlcmd -b an error there (no CDC on the database) would skip the DROP
      echo "EXEC sys.sp_cdc_disable_db;" | $SRC_SQL "$DB" > "$W/drop.out" 2>&1
      printf "IF DB_ID(N'%s') IS NOT NULL BEGIN ALTER DATABASE [%s] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [%s]; END\n" \
        "$DB" "$DB" "$DB" | $SRC_SQL master >> "$W/drop.out" 2>&1
      left=$(printf "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name = '%s';\n" "$DB" | $SRC_SQL master)
      [ "${left//[[:space:]]/}" = 0 ] && break; sleep "${LFC_RETRY_SLEEP:-30}"
    done
    [ "${left//[[:space:]]/}" = 0 ] || { echo "source database $DB not dropped:"; tail -3 "$W/drop.out"; }
  fi
  # shellcheck disable=SC2034  # read by kept-sweep.sh
  DB_LEFT=${left//[[:space:]]/}   # 0 when the source database is gone (kept-sweep.sh checks it)
  if [ "${KEEP_SOURCE:-0}" = 1 ]; then echo "cleanup: pipelines and schemas deleted; source database $DB kept"
  else echo "cleanup: pipelines and schemas deleted; source database left: ${left//[[:space:]]/}"; fi
}

# metric <name> <number>: a number this run measured (seconds, rows, tables, ...). record_run saves them with the run's
# config, so cost, latency and throughput can be recalled and compared later (lfc-test-results recall.py, DuckDB).
METRICS="{}"; RUN_CONF="{}"
metric() { METRICS=$(echo "$METRICS" | K=$1 V=$2 yq -o json -I0 '.[strenv(K)] = (strenv(V) | tonumber)'); }
conf() { RUN_CONF=$(echo "$RUN_CONF" | K=$1 V=$2 yq -o json -I0 '.[strenv(K)] = strenv(V)'); }
# record_run <scenario>: one record per run into results/runs/<date>-lfc-schema-change.jsonl via lfc-test-results record.py:
# what combination was tested and how it came out, no numbers (those go to the private store below)
# (it refuses anything that looks internal: the repo is public). RECORD=0 skips it.
record_run() {
  [ "${RECORD:-1}" = 1 ] || return 0
  local rec=${LFC_RECORD:-$LIB/../../lfc-test-results/scripts/record.py} ver cloud region
  [ -f "$rec" ] || { echo "record_run: $rec not found; not recorded" >&2; return 0; }
  ver=$(if [ "$ENGINE" = postgres ]; then echo 'SHOW server_version;' | $SRC_SQL postgres | head -1 | cut -d' ' -f1
        else printf "SET NOCOUNT ON; SELECT CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(32));\n" | $SRC_SQL master | head -1; fi | tr -d '[:space:]')
  eval "$(dbx metastores summary -o json 2>/dev/null | yq -p json -o shell '(.global_metastore_id | split(":")) as $g | {"cloud": $g[0], "region": .region}')"
  local res; res=$(printf '%s\n' "${RESULTS[@]}" | grep -c '| PASS |'); res="$res pass, $(printf '%s\n' "${RESULTS[@]}" | grep -c '| FAIL |') fail, $(printf '%s\n' "${RESULTS[@]}" | grep -c '| OBSERVED |') observed"
  jq -n -c --arg date "$(date -u +%F)" --arg run "$(date -u +%F)-lfc-schema-change" --arg test "$( [ "$MODE" = integrated ] && echo icdc || echo scdc)" \
    --arg engine "$ENGINE" --arg ver "$ver" --arg outcome "$( [ "$FAILED" = 0 ] && echo pass || echo fail)" --arg res "$1: $res" \
    --arg cloud "${cloud:-}" --arg region "${region:-}" --argjson metrics "$METRICS" --argjson conf "$RUN_CONF" --arg scen "$1" --arg mode "$MODE" \
    --arg claims "$(printf '%s\n' "${RESULTS[@]}" | sed -E 's/^\| `([^`]*)` \| ([A-Z]+) \|.*/\1=\2/' | paste -sd' ' -)" \
    '{date:$date, run:$run, test:$test, subject:{engine:$engine, version:$ver}, outcome:$outcome, result:$res,
      workspace:{cloud:$cloud, region:$region}, setup:({scenario:$scen, mode:$mode, tool:"lfc-schema-change-test"} + $conf),
      notes:$claims}' | uv run "$rec" >&2 && echo "recorded: $1 ($res)" || echo "record_run: not recorded (see above)" >&2
  # LFC_PERF_DIR=<a perf/runs folder with record.sh one level up>: also keep the full config and the numbers there (a
  # private results store can hold the workspace and the source host, which this public repo can't)
  if [ -n "${LFC_PERF_DIR:-}" ] && [ -x "$LFC_PERF_DIR/../record.sh" ] && [ "$METRICS" != "{}" ]; then
    jq -n -c --arg id "$(date -u +%F)-$1-$E" --arg date "$(date -u +%F)" --arg scen "$1" --arg mode "$MODE" --arg engine "$ENGINE" \
      --arg ver "$ver" --arg ws "$PROFILE" --arg cloud "${cloud:-}" --arg region "${region:-}" \
      --arg rt "$( "${LFC_RUNTIME:-$LIB/../../../bin/lfc-runtime.sh}" --summary "$PROFILE" "$AP" 2>/dev/null | sed 's/^Lakeflow runtime: //' | head -1)" \
      --argjson conf "$RUN_CONF" --argjson metrics "$METRICS" --arg claims "$(printf '%s\n' "${RESULTS[@]}" | sed -E 's/^\| `([^`]*)` \| ([A-Z]+) \|.*/\1=\2/' | paste -sd' ' -)" \
      '{run_id:$id, date:$date, scenario:$scen, config:({source:$engine, source_version:$ver, mode:$mode, workspace:$ws, cloud:$cloud, region:$region, runtime:$rt} + $conf), metrics:$metrics, notes:$claims}' \
      | LFC_PERF_DIR=$LFC_PERF_DIR "$LFC_PERF_DIR/../record.sh" >&2 || echo "record_run: performance record not written" >&2
  fi
}

# result_md <title>: the runtime each pipeline ran on, the source, and the claim table; to stdout and $OUT
# src_shape: the common setup every run records, whatever it tests: the captured tables (SQL Server: with change tracking
# or CDC; PostgreSQL: the test schema's) and their columns in all
# (row counts and the DML rate depend on what a test is for, so each script records its own). Sets SHAPE_TABLES,
# SHAPE_COLUMNS and the run's conf src_tables, src_columns.
src_shape() {
  local r
  if [ "$ENGINE" = postgres ]; then
    r=$(printf "SELECT COUNT(DISTINCT table_name) || ' ' || COUNT(*) FROM information_schema.columns WHERE table_schema = 'lfcddemo';\n" | src 2>/dev/null | head -1)
  else
    r=$(printf "SET NOCOUNT ON; SELECT CONCAT(COUNT(DISTINCT c.object_id), ' ', COUNT(*)) FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE SCHEMA_NAME(t.schema_id) = 'lfcddemo' AND (t.is_tracked_by_cdc = 1 OR t.object_id IN (SELECT object_id FROM sys.change_tracking_tables));\n" | src 2>/dev/null | head -1)
  fi
  read -r SHAPE_TABLES SHAPE_COLUMNS <<< "$r"
  [ -n "${SHAPE_TABLES:-}" ] && { conf src_tables "$SHAPE_TABLES"; conf src_columns "$SHAPE_COLUMNS"; }
}

result_md() {
  local rt=${LFC_RUNTIME:-$LIB/../../../bin/lfc-runtime.sh} runtime   # set LFC_RUNTIME when running a copy
  [ "$(src_owned)" = 1 ] && src_shape
  "$rt" "$PROFILE" "$AP" >&2 || true
  runtime="- Ingestion pipeline: $("$rt" --summary "$PROFILE" "$AP" 2>/dev/null | sed 's/^Lakeflow runtime: //')"
  [ -n "${GW:-}" ] && runtime+=$'\n'"- Gateway: $("$rt" --summary "$PROFILE" "$GW" 2>/dev/null | sed 's/^Lakeflow runtime: //')"
  {
    echo "# $1 run $E"
    echo
    echo "- Date (UTC): $(date -u +%F)"
    [ "$MODE" = standard ] && echo "- Mode: standard CDC, ${GATEWAY_COMPUTE:-serverless} gateway, serverless ingestion pipeline" || echo "- Mode: integrated CDC, one serverless pipeline"
    echo "$runtime"
    [ -n "${SRC_ENV:-}" ] && $SRC_ENV | sed 's/^/- /'
    [ -n "${SHAPE_TABLES:-}" ] && echo "- Test schema: $SHAPE_TABLES tables, $SHAPE_COLUMNS columns in all"
    if [ "$ENGINE" = postgres ]; then echo "- Source: $(echo 'SELECT version();' | $SRC_SQL postgres | head -1 | cut -d, -f1)"; else
    echo "- Source: $(printf "SET NOCOUNT ON; SELECT CONCAT('SQL Server ', CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(64)), ' ', CAST(SERVERPROPERTY('ProductUpdateLevel') AS nvarchar(64)), ' ', CAST(SERVERPROPERTY('ProductUpdateReference') AS nvarchar(64)), ', ', CAST(SERVERPROPERTY('Edition') AS nvarchar(128)));\n" | $SRC_SQL master | head -1); utility objects ${UTIL:-?}"; fi
    echo
    echo "| Claim | Result | Observation |"
    echo "|---|---|---|"
    printf '%s\n' "${RESULTS[@]}"
    [ ${#EXAMPLES[@]} -gt 0 ] && { echo; echo "## Input and output"; echo; printf '%s\n' "${EXAMPLES[@]}"; }
  } | tee "${OUT:-/dev/null}"
  record_run "${SCENARIO:-$(basename "$0" .sh)}"
}
