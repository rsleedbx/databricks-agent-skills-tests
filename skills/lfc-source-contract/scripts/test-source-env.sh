#!/usr/bin/env bash
# test-source-env.sh: offline checks of the source contract (contract 1): source-env.sh on every fixture (valid, missing
# field, bad port or engine, contract 2), each engine's default client (fake clients on PATH record their arguments and
# environment: the password is never an argument), and a Kafka broker's contract. A slot's contract (slot-source.sh) is checked by lfc-source-cloud-vm's
# test-slot-source.sh. Prints "all passed" or the failures; exit 1 on any failure.
#   bash skills/lfc-source-contract/scripts/test-source-env.sh
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); FX=$HERE/fixtures; SE=$HERE/source-env.sh
T=$(mktemp -d); fails=0
ok() { printf "  ok    %s\n" "$1"; }
no() { printf "  FAIL  %s\n" "$1"; fails=$((fails + 1)); }
check() { local name=$1; shift; if "$@" > "$T/out" 2>&1; then ok "$name"; else no "$name"; sed 's/^/        /' "$T/out" | tail -5; fi; }
json() { jq -r "$2" "$FX/$1.json"; }   # json <fixture> <jq path>

# ---- source-env.sh on the fixtures ------------------------------------------------------------------------------------
refused() {   # refused <fixture> <words that must be in the refusal>: exit 2, nothing on stdout, the words on stderr
  local f=$FX/$1.json out rc w; shift
  out=$("$SE" "$f" 2> "$T/err"); rc=$?
  [ "$rc" = 2 ] && [ -z "$out" ] || return 1
  for w in "$@"; do grep -qF -- "$w" "$T/err" || return 1; done
}
exports_of() { "$SE" "$FX/$1.json" 2> /dev/null; }
no_password_in_exports() { ! exports_of sqlserver | grep -qF -- "$(json sqlserver .password)"; }
round_trip() {   # the exports of a valid contract set the variables the tests read
  eval "$(exports_of postgres)"
  [ "$SRC_HOST" = 203.0.113.10 ] && [ "$SRC_PORT" = 5432 ] && [ "$CONN_USER" = lfc_user ] && [ "$SRC_DB" = lfc ] && [ "$SRC_ENGINE" = postgres ]
}
oracle_service() { eval "$(exports_of oracle)"; [ "$SRC_DB" = FREEPDB1 ]; }
connection_and_facts() {   # a connection, facts (resolved beside the file) and sql_cmd come through
  eval "$(exports_of sql-cmd-and-connection)"
  case $SRC_SQL in *'/usr/bin/true') ;; *) return 1 ;; esac
  [ "$CONNECTION" = me_pg1 ] && [ "$SRC_FACTS" = "$FX/facts.json" ]
}
connection_kept() {   # a contract without a connection leaves CONNECTION as the environment had it
  CONNECTION=keep; eval "$(exports_of postgres)"; [ "$CONNECTION" = keep ]
}
kafka_broker() {   # a broker, not a database: its fields, and no SQL client
  local err; unset SRC_SQL SRC_PASSWORD_FILE
  eval "$("$SE" "$FX/kafka.json" 2> "$T/kerr")"; err=$(cat "$T/kerr")
  [ "$SRC_ENGINE" = kafka ] && [ "$SRC_HOST" = demo-ns.servicebus.windows.net ] && [ "$SRC_PORT" = 9093 ] &&
    [ "$CONN_USER" = '$ConnectionString' ] && [ "$SRC_TOPIC" = tag-observations ] && [ "$SRC_SASL" = PLAIN ] &&
    [ "$SRC_PASSWORD_FILE" = "$FX/demo-ns.pw" ] && [ -z "${SRC_SQL:-}" ] && [ -z "$err" ]
}
no_client_no_sql() {   # db2 has no default client: SRC_SQL stays unset, and the reason is said
  local out; unset SRC_SQL; eval "$(exports_of db2)"
  [ -z "${SRC_SQL:-}" ] && "$SE" "$FX/informix.json" 2>&1 > /dev/null | grep -q 'no default SQL client'
}
insecure_default() { eval "$(exports_of cockroachdb)"; [ -n "$SRC_SQL" ] && [ "$SRC_SQL" = "$HERE/sql-client.sh" ]; }
usage() { local out; out=$("$SE" 2>&1); case $out in *'source-env.sh <source.json>'*) ;; *) return 1 ;; esac; }

echo "== source-env.sh"
check "no arguments: the usage" usage
for e in sqlserver postgres mysql mariadb oracle db2 informix cockroachdb yugabytedb sql-cmd-and-connection; do
  check "valid $e: exits 0" "$SE" "$FX/$e.json"
done
check "valid: the password is not in the exports" no_password_in_exports
check "valid: SRC_HOST, SRC_PORT, CONN_USER, SRC_DB, SRC_ENGINE are set" round_trip
check "oracle: SRC_DB is the service" oracle_service
check "contract's connection, facts (resolved beside the file) and sql_cmd are exported" connection_and_facts
check "no connection in the contract: CONNECTION is left as the environment had it" connection_kept
check "db2: no default client, so SRC_SQL is not set, and the reason is said" no_client_no_sql
check "insecure cockroachdb: the default client, without a password" insecure_default
check "kafka: bootstrap host and port, user, topic, SASL, the password file beside the contract; no SRC_SQL, no warning" kafka_broker
check "missing host: refused, named" refused missing-host "missing host"
check "kafka without a topic: refused, named" refused kafka-no-topic "missing topic"
check "oracle without a service: refused, named" refused missing-service "missing service"
check "port as text: refused" refused bad-port "port must be a whole number"
check "unknown engine: refused, the engines listed" refused bad-engine "is not one of"
check "contract 2: refused, the version named" refused contract-2 "contract 2"
check "no contract field: refused" refused no-contract "missing contract"
not_json() { echo 'not json' > "$T/bad.json"; ! "$SE" "$T/bad.json" > /dev/null 2>&1; }
check "a file that isn't JSON: refused" not_json

# ---- the default clients ------------------------------------------------------------------------------------------------
# a fake client: logs its name, arguments, the password variables it was given, and its stdin
FB=$T/bin; mkdir -p "$FB"
for c in sqlcmd psql mysql mariadb sqlplus; do
  cat > "$FB/$c" <<'FAKE'
#!/usr/bin/env bash
# fake client for test-source-env.sh: logs its arguments, the password variables and its stdin
{ echo "ARGV $(basename "$0") $*"
  echo "ENV $(env | grep -E '^(SQLCMDPASSWORD|PGPASSWORD|MYSQL_PWD)=' | tr '\n' ' ')"
  echo "STDIN"; cat; } >> "$FAKE_LOG"
exit 0
FAKE
  chmod +x "$FB/$c"
done
client_case() {   # client_case <engine> <client binary>: SRC_SQL runs the client; the password only in its environment
  local e=$1 client=$2 log=$T/log.$1 pw; pw=$(json "$e" '.password // ""'); : > "$log"
  export PATH=$FB:$PATH FAKE_LOG=$log; eval "$(exports_of "$e")"
  printf 'SELECT 1;\n' | $SRC_SQL mydb > /dev/null 2>&1
  grep -q "^ARGV $client " "$log" || return 1
  grep -q '^STDIN' "$log" && grep -q 'SELECT 1' "$log" || return 1
  if [ -n "$pw" ]; then
    ! grep '^ARGV' "$log" | grep -qF -- "$pw" || return 1           # never an argument
    [ "$client" = sqlplus ] || grep -qF -- "$pw" "$log" || return 1  # in the client's environment (sqlplus: in its SQL)
  fi
}
for pair in sqlserver:sqlcmd postgres:psql cockroachdb:psql yugabytedb:psql mysql:mysql mariadb:mariadb oracle:sqlplus; do
  check "$pair: SRC_SQL runs the client, the password is in its environment, never an argument" client_case "${pair%%:*}" "${pair#*:}"
done
check "sqlserver: the database is an argument (-d mydb), the server and port in -S" grep -q 'ARGV sqlcmd .*-S 203.0.113.10,1433 .*-d mydb' "$T/log.sqlserver"
check "postgres: -p 5432 and -d mydb" grep -q 'ARGV psql .*-p 5432 .*-d mydb' "$T/log.postgres"
check "oracle: the password is in the SQL (CONNECT on stdin)" grep -q 'CONNECT lfc_user/' "$T/log.oracle"

rm -rf "${T:?}"
echo; [ $fails -eq 0 ] && echo "all passed" || echo "$fails failed"; exit $((fails > 0))
