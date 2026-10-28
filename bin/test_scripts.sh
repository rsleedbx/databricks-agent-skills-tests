#!/usr/bin/env bash
# bin/test_scripts.sh: regression tests for the skills' scripts.
#   Offline (always): shell syntax, every markdown link, the script rules (bin/lint_scripts.py) and SCRIPTS.md, the six TPC schemas through shape.py and ddl.py for seven
#   dialects, every TPC workload from tpc-schema.sh through ddl-to-schema.py and ddl.py for seven dialects, a DDL import
#   round trip, the Python fill planner compiles, and the shared DuckDB runners.
#   With databases (PSQL and/or MYSQL set, e.g. PSQL="podman exec -i dg-pg psql -U postgres",
#   MYSQL="podman exec -i dg-my mysql -uroot"): fill each of the six workloads from tpc-schema.sh at a small scale (TPC-C 1
#   warehouse, TPC-H SF 0.1, TPC-B SF 1 (its smallest), TPC-DS, TPC-E, TPC-DI SF 0.01) on each, and check every table is at its
#   target, TPC-H's spec rules, and the audit (0 bad rows). Schemas test_* are dropped first.
#   The planner files must be readable by the client: with podman exec, copy them in and set DIR_PG / DIR_MY to that path.
#   VM mode: the same checks on a live database on the cloud VM (LIVE_PSQL, LIVE_MYSQL, or LIVE_ENGINE with LIVE_PY, the
#   connection options for fill.py: --host ... --port ... --user ... --password-file ... --database ...), e.g. a slot from
#   `slot.sh up <engine> <name> --from <master>` on the VM. Db2 and Informix have no SQL planner: their rows come from fill.py,
#   and the checks are the row counts (no spec rules or audit). Laptop default (PSQL/MYSQL) is unchanged.
set -uo pipefail
R=$(cd "$(dirname "$0")/.." && pwd); SK=$R/skills; SS=$SK/lfc-source-statschema/scripts; DG=$SK/lfc-source-datagen/scripts
T=$(mktemp -d); fails=0
ok() { printf "  ok    %s\n" "$1"; }
no() { printf "  FAIL  %s\n" "$1"; fails=$((fails + 1)); }
check() { local name=$1; shift; if "$@" > "$T/out" 2>&1; then ok "$name"; else no "$name"; sed 's/^/        /' "$T/out" | tail -5; fi; }

echo "== offline"
for f in $(find "$SK" -name '*.sh'); do check "bash -n ${f#$SK/}" bash -n "$f"; done
check "links in every skill" python3 "$R/bin/check_links.py" "$SK"
check "script rules: headers, SKILL.md size, rm guards, first-argument flags, jq for one value" python3 "$R/bin/lint_scripts.py"
check "SCRIPTS.md is current" python3 "$R/bin/scripts_index.py" --check
for w in tpcb tpcc tpch tpcdi tpcds tpce; do
  check "shape.py $w" sh -c "uv run -q '$SS/shape.py' '$SK/lfc-source-statschema/schemas/$w.yaml' --sf 0.1 --expected '$T/$w.exp.json' > '$T/$w.yaml'"
  for d in postgres mysql tsql oracle db2 informix databricks; do
    check "ddl.py $w $d" sh -c "uv run -q '$SS/ddl.py' '$T/$w.yaml' --dialect $d | grep -c 'CREATE TABLE' | grep -qv '^0$'"
  done
  check "validate.py $w" uv run -q "$SS/validate.py" "$T/$w.yaml"
done
tpc_dialects() {                              # tpc_dialects <workload>: its DDL, read back, then written for seven dialects
  bash "$DG/tpc/tpc-schema.sh" "$1" 1 | uv run -q "$SS/ddl-to-schema.py" --dialect mysql > "$T/t-$1.yaml" 2>/dev/null || return 1
  for d in postgres mysql tsql oracle db2 informix databricks; do uv run -q "$SS/ddl.py" "$T/t-$1.yaml" --dialect $d > "$T/t-$1.$d.sql" && grep -q 'CREATE TABLE' "$T/t-$1.$d.sql" || return 1; done
  uv run -q "$SS/validate.py" "$T/t-$1.yaml"
}
for w in tpcb tpcc tpch tpcds tpce tpcdi; do
  n=$(case $w in tpcb) echo 4;; tpcc) echo 9;; tpch) echo 8;; tpcds) echo 24;; tpce) echo 32;; tpcdi) echo 18;; esac)
  check "tpc-schema.sh $w (SF 1): $n tables, through ddl-to-schema and ddl.py to seven dialects" tpc_dialects $w
  check "tpc-schema.sh $w (SF 1): $n tables in the schema" test "$(grep -c '^- name:' "$T/t-$w.yaml")" -eq $n
done
check "ddl-to-schema.py round trip (TPC-H spec)" bash -c "bash '$DG/tpc/tpc-schema.sh' tpch 0.1 | uv run -q '$SS/ddl-to-schema.py' --dialect mysql > '$T/spec.yaml' \
  && uv run -q '$SS/ddl.py' '$T/spec.yaml' --dialect mysql | uv run -q '$SS/ddl-to-schema.py' --dialect mysql > '$T/spec2.yaml' \
  && python3 -c \"import sys, yaml; f = lambda p: [{k: ([{c: v for c, v in x.items() if c != 'native_type'} for x in v] if k == 'columns' else v) for k, v in t.items()} for t in yaml.safe_load(open(p))['tables']]; sys.exit(f('$T/spec.yaml') != f('$T/spec2.yaml'))\""
check "python fill planner compiles" python3 -m py_compile "$DG/python/fill.py" "$DG/python/planner.py" "$DG/python/drivers.py" "$DG"/python/engines/*.py
check "results/runs/*.jsonl valid, nothing internal" python3 "$SK/lfc-test-results/scripts/record.py" --check
check "results/*.md: nothing internal" python3 -c "import sys, glob, re; sys.path.insert(0, '$SK/lfc-test-results/scripts'); import record; bad = [(f, w) for f in glob.glob('$R/results/*.md') for w in record.leaks({'text': open(f).read()})]; print(bad); sys.exit(bool(bad))"   # the same check as record.py's, version masking included
check "recall.py runs" sh -c "python3 '$SK/lfc-test-results/scripts/recall.py' > /dev/null"
check "secret-handling scripts on a fake secret" sh -c "'$SK/secret-handling/scripts/test-secret-scripts.sh' > /dev/null"
check "options-first: ranking, pick, check, index" sh -c "'$SK/options-first/scripts/test-options.sh' > /dev/null"
check "options-first: every options.yaml in the skills is valid" sh -c "files=\$(find '$SK' -name options.yaml); [ -z \"\$files\" ] || uv run -q '$SK/options-first/scripts/options.py' check \$files"
export WANT="10.0.0.0/24 10.9.0.0/24" HAVE="10.0.0.0/16"
check "q.sh missing.sql" sh -c "'$SK/shell-json-scripting/scripts/q.sh' '$SK/lfc-source-firewall/scripts/missing.sql' | grep -qE \"^MISSING='?10.9.0.0/24'?$\""
check "q.py missing.sql" sh -c "uv run -q '$SK/shell-json-scripting/scripts/q.py' '$SK/lfc-source-firewall/scripts/missing.sql' | grep -qE \"^MISSING='?10.9.0.0/24'?$\""

# the schema-change matrix: the results validate and render to the committed table; the commands run offline
DM=$SK/lfc-schema-change-test/scripts
check "ddl-matrix: every result file validates" uv run -q "$DM/ddl-matrix-validate.py" "$R"/results/ddl-matrix/*.yaml
check "ddl-matrix: the rendered table is the committed one" sh -c "uv run -q '$DM/ddl-matrix.py' render | diff -q - '$SK/lfc-schema-change-test/references/ddl-matrix-results.md'"
check "ddl-matrix: sql prints the change-tracking setup and the case" sh -c "uv run -q '$DM/ddl-matrix.py' sql sqlserver ct --cases int-to-bigint | grep -q 'ENABLE CHANGE_TRACKING'"
check "ddl-matrix: bundle has one table per case" sh -c "uv run -q '$DM/ddl-matrix.py' bundle --connection c --catalog k --schema s --source-catalog d --cases int-to-bigint,drop-column --out '$T/b.yml' && grep -c source_table '$T/b.yml' | grep -qx 2"
check "ddl-matrix: collect on saved events: a restart with its row, a stop, and an unfinished update" sh -c "
  mkdir -p '$T/cm' && python3 - '$T/cm' <<'PY'
import json, sys
d = sys.argv[1]
flow = lambda t: f\"main.s.applier_main_s_x_{t}_cdc_flow\"
ev = [{'event_type': 'update_progress', 'level': 'INFO', 'message': 'Update u1 is RUNNING.', 'timestamp': '2026-10-26T22:05:00.000Z'},
      {'event_type': 'runtime_details', 'level': 'INFO', 'message': 'Resolved runtime', 'timestamp': '2026-10-26T22:05:01.000Z', 'origin': {'cloud': 'AWS', 'region': 'us-west-2', 'update_id': 'u1'}, 'details': {'runtime_details': {'runtime_version': {'dbr_version': '18.3', 'channel': 'CURRENT', 'image_key': 'dlt:18.3.8-delta-pipelines-aarch64-photon-dlt-release-dp-20261015-rc0-commit-48da011-image-5bafb35'}}}},
      {'event_type': 'flow_progress', 'level': 'WARN', 'message': 'Flow ' + flow('int_to_bigint') + ' has terminated.', 'timestamp': '2026-10-26T22:06:00.000Z', 'origin': {'flow_name': flow('int_to_bigint')},
       'error': {'exceptions': [{'message': '[FLOW_SCHEMA_CHANGED] Flow terminated since it encountered a schema change.'}]}},
      {'event_type': 'flow_progress', 'level': 'ERROR', 'message': 'Failed to resolve flow', 'timestamp': '2026-10-26T22:07:00.000Z', 'origin': {'flow_name': flow('bigint_to_int')},
       'error': {'exceptions': [{'message': \"[CDC_INCOMPATIBLE_SCHEMA_CHANGES.UNSUPPORTED_DATA_TYPE_CHANGE] incompatible\"}]}},
      {'event_type': 'update_progress', 'level': 'INFO', 'message': 'Update u1 is COMPLETED.', 'timestamp': '2026-10-26T22:20:00.000Z'}]
tables = {'int_to_bigint': {'columns': {'id': 'int', 'n': 'bigint'}, 'row': {'id': '3', 'n': '3000000000'}},
          'bigint_to_int': {'columns': {'id': 'int', 'n': 'bigint'}, 'row': None}}
json.dump(ev, open(d + '/events.json', 'w')); json.dump(tables, open(d + '/tables.json', 'w'))
open(d + '/env.yaml', 'w').write('environment:\n  engine: sqlserver\n  product: Azure SQL Database\n  version: test\n  mechanism: ct\n  setup: none\n  source_setup: test\n  pipeline_mode: integrated\n  compute: serverless\n  workspace: {cloud: AWS, region: us-west-2}\n  date: 2026-10-26\n  method: one-case-at-a-time\n  caveats: [test fixture]\n')
PY
  uv run -q '$DM/ddl-matrix.py' collect --events-json '$T/cm/events.json' --tables-json '$T/cm/tables.json' --env '$T/cm/env.yaml' --cases int-to-bigint,bigint-to-int --out '$T/cm/sqlserver-ct-none-2026-10-26.yaml' > '$T/cm/out.txt' && grep -q '^PASS ' '$T/cm/out.txt' && grep -q '^STOP ' '$T/cm/out.txt' && grep -q 'dp_build: dp-20261015-rc0' '$T/cm/sqlserver-ct-none-2026-10-26.yaml'"

seed_my() {   # seed_my <client>: lfc_seed.seed (1 .. 2^20, r a random permutation), by doubling, as tpc-master.sh builds it
  { echo "CREATE DATABASE IF NOT EXISTS lfc_seed; CREATE TABLE IF NOT EXISTS lfc_seed.seed_i (i bigint PRIMARY KEY);"
    echo "INSERT IGNORE INTO lfc_seed.seed_i (i) VALUES $(printf '(%s),' $(seq 1 1024) | sed 's/,$//');"
    for k in $(seq 10); do echo "INSERT IGNORE INTO lfc_seed.seed_i (i) SELECT s.i + b.m FROM lfc_seed.seed_i s, (SELECT MAX(i) AS m FROM lfc_seed.seed_i) b;"; done
    echo "DROP TABLE IF EXISTS lfc_seed.seed; CREATE TABLE lfc_seed.seed (i bigint PRIMARY KEY, r bigint NOT NULL);"
    echo "INSERT INTO lfc_seed.seed SELECT i, ROW_NUMBER() OVER (ORDER BY RAND()) FROM lfc_seed.seed_i;"; echo "DROP TABLE lfc_seed.seed_i;"; } | $1
}
db() {   # db <pg|my|py> <dialect> <client> <planner dir>: fill and check each workload at a small scale in test_<w>
  # pg: the Postgres planners through <client> (psql); my: the MySQL planners through <client> (mysql); py: fill.py for the
  # engine named by <client> (db2, informix, cockroachdb, ...), its connection options from LIVE_PY, DDL through sq.py
  local kind=$1 dial=$2 cli=$3 dir=$4 w db sf y out n short bad a S
  echo "== $dial ($kind)"
  [ "$kind" = my ] && { $cli -N -e "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'lfc_seed' AND table_name = 'seed' AND column_name = 'r'" 2>/dev/null | grep -qx 1 || seed_my "$cli"; }
  for w in tpcb tpcc tpch tpcds tpce tpcdi; do db=test_$w
    case $w in tpcb|tpcc) sf=1 ;; tpch) sf=0.1 ;; *) sf=0.01 ;; esac
    y=$T/$w.live.yaml; bash "$DG/tpc/tpc-schema.sh" $w $sf | uv run -q "$SS/ddl-to-schema.py" --dialect mysql > "$y" 2>/dev/null
    uv run -q "$SS/ddl.py" "$y" --dialect $dial > "$T/$db.sql"
    S=$(echo $db | tr a-z A-Z)
    case $kind in
    pg) { echo "DROP SCHEMA IF EXISTS $db CASCADE; CREATE SCHEMA $db; SET search_path = $db;"; cat "$T/$db.sql"; } | $cli -q -v ON_ERROR_STOP=1 > /dev/null 2>&1
        out=$(PSQL="$cli" DIR=$dir bash "$DG/fill.sh" pg $db 2>&1) ;;
    my) { echo "DROP DATABASE IF EXISTS $db; CREATE DATABASE $db; USE $db;"; cat "$T/$db.sql"; } | $cli > /dev/null 2>&1
        out=$(MYSQL="$cli" DIR=$dir bash "$DG/fill.sh" my $db 2>&1) ;;
    py)  # the engine's own DDL path: Db2 a schema per workload; Informix a database per workload; the rest a schema
        case $cli in
        db2)      { echo "CREATE SCHEMA $S;"; cat "$T/$db.sql"; } | $PY "$DG/python/sq.py" db2 --schema $S $LIVE_PY > "$T/ddl.out" 2>&1 ;;
        informix) { echo "DROP DATABASE IF EXISTS $db; CREATE DATABASE $db IN ${LIVE_IFX_DBSPACE:-datadbs} WITH LOG;"; } | $PY "$DG/python/sq.py" ifx --database sysmaster $LIVE_PY > "$T/ddl.out" 2>&1
                  $PY "$DG/python/sq.py" ifx --database $db $LIVE_PY < "$T/$db.sql" >> "$T/ddl.out" 2>&1 ;;
        *)        { echo "DROP SCHEMA IF EXISTS $db CASCADE; CREATE SCHEMA $db; SET search_path = $db;"; cat "$T/$db.sql"; } | $PY "$DG/python/sq.py" $cli $LIVE_PY > "$T/ddl.out" 2>&1 ;;
        esac
        out=$($PY "$DG/python/fill.py" $cli $db $LIVE_PY --sessions ${LIVE_SESSIONS:-2} 2>&1) ;;
    esac
    n=$(echo "$out" | awk -F'|' 'NF == 5' | wc -l); short=$(echo "$out" | awk -F'|' 'NF == 5 && $4 != $5' | wc -l)
    [ "$n" -gt 0 ] && [ "$short" -eq 0 ] && ok "$dial $db: $n tables at target" || { no "$dial $db: $short of $n short"; echo "$out" | tail -3 | sed 's/^/        /'; }
    if [ "$w" = tpch ] && [ "$kind" != py ]; then
      if [ "$kind" = pg ]; then bad=$({ echo "SET search_path = $db;"; sed -E 's/DATEDIFF\(([^,]+), ([^)]+)\)/(\1 - \2)/g' "$DG/tpc/tpch-verify.sql"; } | $cli -At | grep '^bad' | cut -d'|' -f2- | tr '|' '\n' | awk '$1 != 0' | wc -l)
      else bad=$($cli -N $db < "$DG/tpc/tpch-verify.sql" | grep '^bad' | cut -f2- | tr '\t' '\n' | awk '$1 != 0' | wc -l); fi
      [ "$bad" -eq 0 ] && ok "$dial $db: TPC-H spec rules" || no "$dial $db: $bad spec rules broken"
    fi
    if [ "$w" = tpcb ] && [ "$kind" != py ]; then                  # tpcb-verify.sql: one "bad: ... <count>" row per check (psql: |, mysql: tab)
      if [ "$kind" = pg ]; then bad=$({ echo "SET search_path = $db;"; cat "$DG/tpc/tpcb-verify.sql"; } | $cli -At | awk -F'|' '/^bad/ && $NF != 0' | wc -l)
      else bad=$($cli -N $db < "$DG/tpc/tpcb-verify.sql" | awk -F'\t' '/^bad/ && $NF != 0' | wc -l); fi
      [ "$bad" -eq 0 ] && ok "$dial $db: TPC-B scaling and relationships" || no "$dial $db: $bad TPC-B checks broken"
    fi
    if [ "$kind" = pg ]; then a=$(cat "$DG/postgres/cast.sql" "$DG/postgres/plan-audit.sql" | $cli -At -q -v schema=$db -v plain_every=4 | $cli -At | awk -F'|' '{ b += $3 } END { print b + 0 }')
    elif [ "$kind" = my ]; then a=$({ echo "SET @db='$db', @plain_every=4, @block=1000;"; cat "$DG/mysql/cast.sql" "$DG/mysql/plan-audit.sql"; } | $cli -N -B -r $db | $cli -N $db | awk -F'\t' '{ b += $3 } END { print b + 0 }')
    else a=0; fi                                    # no audit planner for the driver path (Db2, Informix, CockroachDB): see lfc-source-datagen
    [ "$kind" = py ] || { [ "$a" = 0 ] && ok "$dial $db: audit 0 bad rows" || no "$dial $db: audit $a bad rows"; }
  done
}
[ -n "${PSQL:-}" ] && db pg postgres "$PSQL" "${DIR_PG:-$DG/postgres}"
[ -n "${MYSQL:-}" ] && db my mysql "$MYSQL" "${DIR_MY:-$DG/mysql}"
# VM mode (run on the cloud VM, where the clients and the slot's database are; a database from slot.sh up): LIVE_PSQL (a psql
# command line for Postgres-protocol servers: postgres, YugabyteDB with LIVE_DIALECT=postgres), LIVE_MYSQL (mysql or mariadb
# with LIVE_DIALECT=mysql), or LIVE_PY=<connection options> with LIVE_ENGINE=<db2|informix|cockroachdb> (fill.py and sq.py)
[ -n "${LIVE_PSQL:-}" ] && db pg "${LIVE_DIALECT:-postgres}" "$LIVE_PSQL" "${DIR_PG:-$DG/postgres}"
[ -n "${LIVE_MYSQL:-}" ] && db my mysql "$LIVE_MYSQL" "${DIR_MY:-$DG/mysql}"
[ -n "${LIVE_ENGINE:-}" ] && PY=${PY:-python3} && db py "${LIVE_DIALECT:-$LIVE_ENGINE}" "$LIVE_ENGINE" ""
rm -rf "${T:?}"
echo; [ $fails -eq 0 ] && echo "all passed" || echo "$fails failed"; exit $((fails > 0))
