#!/usr/bin/env bash
# sqlserver-log-state.sh <database> [--no-per-table]: run sqlserver-log-state.sql in <database> and print its JSON
# document (one line; pipe it to jq). --no-per-table skips the per-table CHANGETABLE and change-table reads.
# How it connects:
#   SQL_CMD   a command that reads T-SQL on stdin and takes sqlcmd's -d/-v/-y/-b flags, for example
#             SQL_CMD='podman exec -i <container> /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P <pw>'
#             (default: sqlcmd -C, with SQLCMDSERVER, SQLCMDUSER and SQLCMDPASSWORD from the environment)
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
DB=${1:?usage: sqlserver-log-state.sh <database> [--no-per-table]}
PER=1; [ "${2:-}" = --no-per-table ] && PER=0
[[ $DB =~ ^[A-Za-z0-9_.-]+$ ]] || { echo "bad database name: $DB" >&2; exit 2; }
# SQL_CMD runs through bash -c, so it can hold quotes; the sqlcmd flags follow it as arguments.
# -y 0: the whole nvarchar(max) value on one line (sqlcmd refuses it with -h); -b: exit nonzero on an error
out=$(bash -c "${SQL_CMD:-sqlcmd -C} \"\$@\"" sql-cmd -d "$DB" -v PER_TABLE="$PER" -y 0 -b \
  < "$here/sqlserver-log-state.sql")
json=$(grep -m1 '^{' <<<"$out") || { echo "no JSON from the batch:" >&2; echo "$out" >&2; exit 1; }
jq -c . <<<"$json"
