#!/usr/bin/env bash
# sql-client.sh [database [psql arguments]]: run SQL from stdin with the default client for the contract's engine. source-env.sh sets SRC_SQL
# to this when a contract has no sql_cmd. It connects as the contract's user with its password, and the password reaches
# the client only in an environment variable (SQLCMDPASSWORD, PGPASSWORD, MYSQL_PWD), or in the SQL for sqlplus.
#   Needs SRC_CONTRACT (the contract file; source-env.sh sets it). The argument is the database (SQL Server, PostgreSQL,
#   MySQL, MariaDB); it overrides the contract's database. Oracle takes the contract's service and ignores the argument.
#   PostgreSQL, CockroachDB, YugabyteDB: arguments after the database go to psql after the defaults (fill.sh: -F '|').
#   Output: rows tab-separated, no headers (psql -At, sqlcmd -h -1, mysql -N -B). The exit status is the client's.
#   Engines: sqlserver (sqlcmd -C), postgres, cockroachdb, yugabytedb (psql), mysql, mariadb, oracle (sqlplus).
#   db2 and informix have no default client: their contract needs sql_cmd.
set -uo pipefail
: "${SRC_CONTRACT:?set SRC_CONTRACT to the source contract (source-env.sh sets it)}"
eval "$(yq -p json -o shell '{
  "ENG": (.engine // ""), "HOST": (.host // ""), "PORT": (.port | tostring), "USR": (.user // ""), "PW": (.password // ""),
  "DBN": (.database // ""), "SVC": (.service // "")
}' "$SRC_CONTRACT")"
DB=${1:-$DBN}
case $ENG in
  sqlserver)
    export SQLCMDPASSWORD="$PW"
    exec sqlcmd -C -S "$HOST,$PORT" -U "$USR" -b -W -h -1 -d "${DB:-master}" ;;
  postgres|cockroachdb|yugabytedb)
    export PGPASSWORD="$PW"
    exec psql -h "$HOST" -p "$PORT" -U "$USR" -d "${DB:-postgres}" -v ON_ERROR_STOP=1 -qtAX -F $'\t' "${@:2}" ;;
  mysql|mariadb)
    export MYSQL_PWD="$PW"
    exec "$ENG" -h "$HOST" -P "$PORT" -u "$USR" -N -B ${DB:+"$DB"} ;;
  oracle)
    # the password is in the SQL (CONNECT on stdin, its quotes doubled), not on the command line
    PWQ=${PW//\"/\"\"}
    { echo "WHENEVER SQLERROR EXIT FAILURE"
      printf 'CONNECT %s/"%s"@//%s:%s/%s\n' "$USR" "$PWQ" "$HOST" "$PORT" "$SVC"
      echo "SET HEADING OFF FEEDBACK OFF PAGESIZE 0 TRIMSPOOL ON"
      cat
      echo "EXIT"; } | sqlplus -s /nolog ;;
  *)
    echo "sql-client: no default client for $ENG: give the contract a sql_cmd" >&2
    exit 2 ;;
esac
