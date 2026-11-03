#!/usr/bin/env bash
# source-env.sh <source.json>: validate a source contract (contract 1, lfc-source-contract) and print the exports the tests
# use: eval "$(source-env.sh source.json)". Exit 2 with one line when the file breaks the contract (a missing field, a bad
# engine or port, a contract version this script doesn't read).
#   Exports SRC_ENGINE, SRC_HOST, SRC_PORT, SRC_DB (the database, or the Oracle service), SRC_NAME, CONN_USER (the login),
#   SRC_CONTRACT (the file, absolute), and SRC_SQL, CONNECTION and SRC_FACTS when the contract gives them. SRC_SQL is the
#   contract's sql_cmd, else sql-client.sh (the default client for the engine, which reads the password from SRC_CONTRACT
#   itself). Variables the contract doesn't give are left as they are. The password is never printed.
#   engine kafka (a Kafka broker, not a database): also SRC_TOPIC, SRC_SASL (default PLAIN) and SRC_PASSWORD_FILE (the
#   file holding the SASL password, absolute); no SRC_SQL.
set -uo pipefail
[ $# -ge 1 ] || { sed -n 2,10p "$0"; exit 2; }
F=$1
HERE=$(cd "$(dirname "$0")" && pwd)
ABS_DIR=$(cd "$(dirname "$F")" 2>/dev/null && pwd) || { echo "source-env: no such folder for $F" >&2; exit 2; }
ABS=$ABS_DIR/$(basename "$F")
bad() { echo "source-env: $F: $*" >&2; exit 2; }
[ -r "$F" ] || bad "can't read it"
jq -e 'type == "object"' "$F" > /dev/null 2>&1 || bad "not a JSON object"
CV=$(jq -c '.contract // null' "$F")
[ "$CV" != null ] || bad "missing contract (the version: \"contract\": 1)"
[ "$CV" = 1 ] || { echo "source-env: $F is contract $CV; this reads contract 1 only: use a source-env.sh that reads $CV, or have the producer write contract 1" >&2; exit 2; }

# every problem, one per line; none means the contract holds
PROBLEMS=$(jq -r '
  def engines: ["sqlserver", "postgres", "mysql", "oracle", "mariadb", "db2", "informix", "cockroachdb", "yugabytedb", "kafka"];
  def db_engines: ["db2", "informix", "cockroachdb", "yugabytedb"];
  def req($k): if (.[$k] | type) == "string" and (.[$k] | length) > 0 then empty else "missing \($k)" end;
  def opt($k): if has($k) and .[$k] != null and ((.[$k] | type) != "string") then "\($k) must be a string" else empty end;
  (.engine // "") as $engine
  | [
    (if ($engine | length) == 0 then "missing engine"
     elif (engines | any(. == $engine)) | not then "engine \($engine) is not one of " + (engines | join(", "))
     else empty end),
    req("host"), req("user"),
    (if (.port | type) == "number" and .port == (.port | floor) and .port >= 1 and .port <= 65535 then empty
     elif has("port") | not then "missing port"
     else "port must be a whole number from 1 to 65535" end),
    (if $engine == "oracle" then req("service")
     elif (db_engines | any(. == $engine)) then req("database")
     elif $engine == "kafka" then req("topic")
     else empty end),
    opt("database"), opt("service"), opt("password"), opt("sql_cmd"), opt("connection"), opt("facts"), opt("name"),
    opt("topic"), opt("sasl_mechanism"), opt("password_file")
  ] | .[]' "$F")
[ -z "$PROBLEMS" ] || bad "$(printf '%s' "$PROBLEMS" | paste -sd, - | sed 's/,/, /g')"

# the fields, as shell assignments (yq quotes each value)
eval "$(yq -p json -o shell '{
  "SRC_ENGINE": (.engine // ""), "SRC_HOST": (.host // ""), "SRC_PORT": (.port | tostring), "SRC_NAME": (.name // ""),
  "CONN_USER": (.user // ""), "SRC_DB": (.database // .service // ""), "SRC_CMD": (.sql_cmd // ""),
  "SRC_CONN": (.connection // ""), "SRC_FACTS": (.facts // ""), "SRC_HAS_PW": ((.password // "") | length | tostring),
  "SRC_TOPIC_V": (.topic // ""), "SRC_SASL_V": (.sasl_mechanism // "PLAIN"), "SRC_PWF": (.password_file // "")
}' "$F")"

qv() { printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\\\\''/g")"; }   # one value, single-quoted for eval
out() { printf 'export %s=' "$1"; qv "$2"; printf '\n'; }
out SRC_ENGINE "$SRC_ENGINE"
out SRC_HOST "$SRC_HOST"
out SRC_PORT "$SRC_PORT"
out SRC_DB "$SRC_DB"
out SRC_NAME "$SRC_NAME"
out CONN_USER "$CONN_USER"
out SRC_CONTRACT "$ABS"
[ -n "$SRC_CONN" ] && out CONNECTION "$SRC_CONN"
if [ -n "$SRC_FACTS" ]; then case $SRC_FACTS in /*) FACTS=$SRC_FACTS ;; *) FACTS=$ABS_DIR/$SRC_FACTS ;; esac; out SRC_FACTS "$FACTS"; fi

abs() { case $1 in /*) printf '%s' "$1" ;; *) printf '%s' "$ABS_DIR/$1" ;; esac; }   # relative to the file
if [ "$SRC_ENGINE" = kafka ]; then   # a broker: no SQL client
  out SRC_TOPIC "$SRC_TOPIC_V"
  out SRC_SASL "$SRC_SASL_V"
  [ -n "$SRC_PWF" ] && out SRC_PASSWORD_FILE "$(abs "$SRC_PWF")"
  exit 0
fi
case $SRC_ENGINE in sqlserver|postgres|mysql|mariadb|oracle|cockroachdb|yugabytedb) HAS_CLIENT=1 ;; *) HAS_CLIENT=0 ;; esac
if [ -n "$SRC_CMD" ]; then
  out SRC_SQL "$SRC_CMD"
elif [ "$HAS_CLIENT" = 1 ] && { [ "$SRC_HAS_PW" != 0 ] || [ "$SRC_ENGINE" = cockroachdb ] || [ "$SRC_ENGINE" = yugabytedb ]; }; then
  out SRC_SQL "$HERE/sql-client.sh"   # insecure CockroachDB and YugabyteDB have no password
elif [ "$HAS_CLIENT" = 1 ]; then
  echo "source-env: $F: no sql_cmd and no password: SRC_SQL is not set (the default client needs the password)" >&2
else
  echo "source-env: $F: no default SQL client for $SRC_ENGINE: SRC_SQL is not set (give sql_cmd)" >&2
fi
