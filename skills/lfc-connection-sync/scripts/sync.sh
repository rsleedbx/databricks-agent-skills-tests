#!/usr/bin/env bash
# sync.sh <targets.tsv>: create or update one UC connection per row, in each row's workspace. Idempotent: rerun after a fix.
#   targets.tsv (keep it private, umask 077), tab-separated, one row per connection per workspace, '#' comments allowed:
#     profile  name  connection_type  host  port  user  password_file  [extra options as JSON, e.g. {"service_name": "FREE"}]
#   Every connection gets trustServerCertificate=true (TLS on, the server certificate not verified) unless the extra options
#   give userProvidedServerCertificate (a CA PEM, verified): a pipeline created after TLS enforcement fails a MySQL
#   connection with neither (INGESTION_GATEWAY_CONNECTION_ERROR.TLS_VALIDATION_FAILURE), and with both.
#   password_file is a private file or keyring:<name> (secret-handling scripts/pw.py).
#   Prints one line per row: created | updated | FAILED <class>: <error>; after created/updated, keep_catalogs.py's lines for the
#   connection and its foreign catalogs (RemoveAfter a month out). The password is read from its file into a
#   private request body that's deleted right away; it never appears on a command line or in the output.
set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../../secret-handling/scripts/pw.sh"   # pw/pw_check: a password file or keyring:<name>
T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT; umask 077
class() { case "$1" in
  *"CREATE CONNECTION"*|*"CREATE_CONNECTION"*) echo no_create_connection ;;
  *"does not have"*|*PERMISSION_DENIED*) echo permission ;;
  *"limit"*|*"LIMIT"*|*"quota"*) echo connection_limit ;;
  *"not supported"*|*"does not support"*) echo unsupported_option ;;
  *"invalid access token"*|*"token"*|*"401"*|*"Unauthorized"*) echo auth ;;
  *) echo other ;; esac; }
# split on a non-whitespace separator: with IFS=tab, read merges runs of tabs, so an empty column shifted every later field
while IFS= read -r line; do IFS=$'\x1f' read -r profile name type host port user pwfile extra <<< "${line//$'\t'/$'\x1f'}"
  case "$profile" in ''|'#'*) continue ;; esac
  pw_check "$pwfile" 2>/dev/null || { echo "$profile $name: FAILED missing_password: $pwfile"; continue; }
  opts=$(PW="$(pw "$pwfile")" H="$host" PO="$port" U="$user" E="${extra:-{\}}" yq -n -o json -I0 '({"host": strenv(H), "port": strenv(PO), "user": strenv(U), "password": strenv(PW), "trustServerCertificate": "true"} * (strenv(E) | from_json)) | (select(has("userProvidedServerCertificate")) |= del(.trustServerCertificate))')
  if databricks connections get "$name" --profile "$profile" -o json > /dev/null 2>&1; then
    echo "{\"options\": $opts}" > "$T/body.json"; op=updated
    databricks connections update "$name" --json @"$T/body.json" --profile "$profile" -o json > "$T/out" 2>&1
  else
    echo "{\"name\": \"$name\", \"connection_type\": \"$type\", \"options\": $opts}" > "$T/body.json"; op=created
    databricks connections create --json @"$T/body.json" --profile "$profile" -o json > "$T/out" 2>&1
  fi
  rc=$?; rm -f "$T/body.json"
  if [ $rc -eq 0 ]; then echo "$profile $name: $op"; python3 -I "$(dirname "${BASH_SOURCE[0]}")/../../lfc-workspace-setup/scripts/keep_catalogs.py" "$profile" --connection "$name" | sed 's/^/  /'; else err=$(grep -m1 -i "error" "$T/out" | cut -c1-200); echo "$profile $name: FAILED $(class "$err"): $err"; fi
done < "$1"
