#!/usr/bin/env bash
# test-kept.sh: checks lib.sh's keep/delete at exit and kept-sweep.sh against a fake databricks CLI and a fake source
#   (no workspace, no database): cleanup's default stops and records (fields, delete-after date, extras), KEEP_DAYS=0
#   deletes and records nothing, and the sweep deletes an expired run, keeps one whose check fails with an auth error,
#   keeps a current one until --now, leaves the file as it is on --dry-run, and carries over a run kept during the sweep.
#   One PASS/FAIL line per check; exit 1 on any FAIL.
# shellcheck disable=SC2034  # the run variables are read by lib.sh; rc by the checks (eval)
set -uo pipefail
S=$(cd "$(dirname "$0")" && pwd); T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT; fail=0
ok() { if eval "$2"; then echo "PASS $1"; else echo "FAIL $1"; fail=1; fi; }
# the fake CLI: calls logged to $FT/calls; "get" of a name listed in $FT/alive succeeds, of one in $FT/authfail fails
# with an auth error, of anything else says it doesn't exist
cat > "$T/databricks" <<'EOF'
#!/usr/bin/env bash
echo "$*" >> "$FT/calls"
case "$1 $2" in
  "pipelines get"|"schemas get")
    grep -qx "$3" "$FT/alive" 2>/dev/null && { echo '{}'; exit 0; }
    grep -qx "$3" "$FT/authfail" 2>/dev/null && { echo "Error: invalid access token" >&2; exit 1; }
    echo "Error: RESOURCE_DOES_NOT_EXIST: $3 does not exist" >&2; exit 1;;
  "auth describe") echo '{"details": {"host": "https://ws.example"}}';;
esac
exit 0
EOF
printf '#!/usr/bin/env bash\ncat >> "$FT/sql"; echo 0\n' > "$T/src.sh"
chmod +x "$T/databricks" "$T/src.sh"
export FT=$T DATABRICKS_CLI=$T/databricks LFC_KEPT=$T/kept.jsonl
env_lib() { echo "PROFILE=p CONNECTION=c CATALOG=main SRC_SQL=$T/src.sh CONN_USER=u"; }

# --- cleanup at exit ----------------------------------------------------------------------------------------------
at_exit() {   # at_exit <KEEP_DAYS or ""> : source lib.sh, set a run's variables, call cleanup
  : > "$T/calls"
  ( eval "export $(env_lib)"; [ -n "$1" ] && export KEEP_DAYS=$1
    . "$S/lib.sh"; trap - EXIT
    E=1790000000; DB=x_$E; S=run_$E; STG=${S}_stg; AP=ap1; GW=gw1; W=$(mktemp -d); GWN=g; APN=a
    EXTRA_SCHEMAS=" main.extra_$E"; EXTRA_PIPELINES="dp1"
    cleanup ) > "$T/out" 2>&1
}
at_exit ""
ok "default: stops both pipelines and the extra one, deletes nothing" \
  '[ "$(grep -c "^pipelines stop" "$T/calls")" = 3 ] && ! grep -q "delete" "$T/calls"'
ok "default: one record with the run's fields" \
  '[ "$(wc -l < "$LFC_KEPT")" -eq 1 ] && jq -e ".id == \"run_1790000000\" and .pipelines == [\"ap1\",\"gw1\"] and .db == \"x_1790000000\" and .staging == \"run_1790000000_stg\" and .extra_schemas == [\"main.extra_1790000000\"] and .extra_pipelines == [\"dp1\"] and .stopped == true and .host == \"https://ws.example\"" "$LFC_KEPT" >/dev/null'
ok "default: delete_after is 7 days out" \
  '[ "$(jq -r .delete_after "$LFC_KEPT")" = "$(jq -rn "now + 7 * 86400 | strftime(\"%Y-%m-%d\")")" ]'
cp "$LFC_KEPT" "$T/kept.first"; : > "$LFC_KEPT"
at_exit 0
ok "KEEP_DAYS=0: deletes the pipelines, schemas, extras and the source database; records nothing" \
  'grep -q "^pipelines delete ap1" "$T/calls" && grep -q "^pipelines delete gw1" "$T/calls" && grep -q "^pipelines delete dp1" "$T/calls" && grep -q "^schemas delete main.run_1790000000 " "$T/calls" && grep -q "^schemas delete main.extra_1790000000" "$T/calls" && grep -q "DROP DATABASE" "$T/sql" && [ ! -s "$LFC_KEPT" ]'

# --- the sweep -----------------------------------------------------------------------------------------------------
rec() {   # rec <id> <delete_after> <pipeline>: a kept run on a source database of its own
  jq -nc --arg id "$1" --arg after "$2" --arg ap "$3" --arg s "$T/src.sh" \
    '{id:$id, kept:"2026-10-01T00:00:00Z", delete_after:$after, host:"h", profile:"p", catalog:"main", schema:$id, staging:"",
      pipelines:[$ap], stopped:true, engine:"sqlserver", db:"db_1790000000", src_db:"db_1790000000", slot:"", connection:"c",
      conn_user:"u", src_sql:$s, source_json:"", extra_pipelines:[], extra_schemas:[], extra_sql:""}'
}
{ rec old 2026-10-08 ap_old; rec stuck 2026-10-08 ap_stuck; rec new 2099-01-01 ap_new; } > "$LFC_KEPT"
echo ap_stuck > "$T/authfail"
cp "$LFC_KEPT" "$T/before"
bash "$S/kept-sweep.sh" --dry-run > "$T/out" 2>&1
ok "--dry-run: lists the two expired runs, file unchanged" \
  '[ "$(grep -c "^would delete" "$T/out")" = 2 ] && cmp -s "$LFC_KEPT" "$T/before"'
: > "$T/calls"; bash "$S/kept-sweep.sh" > "$T/out" 2>&1; rc=$?
ok "sweep: the expired run is deleted and its line removed" \
  'grep -q "^pipelines delete ap_old" "$T/calls" && ! jq -e "select(.id == \"old\")" "$LFC_KEPT" >/dev/null'
ok "sweep: an auth error on the check keeps the run, exit 1" \
  '[ $rc = 1 ] && jq -e "select(.id == \"stuck\")" "$LFC_KEPT" >/dev/null && grep -q "pipeline ap_stuck: not deleted" "$T/out"'
ok "sweep: the current run is untouched" '! grep -q "ap_new" "$T/calls" && jq -e "select(.id == \"new\")" "$LFC_KEPT" >/dev/null'
: > "$T/authfail"; bash "$S/kept-sweep.sh" --now new > "$T/out" 2>&1
ok "--now: deletes the named run before its date (and the stuck one now its check passes)" '[ ! -s "$LFC_KEPT" ]'
# a run kept while the sweep runs: its line, appended after the sweep read the file, survives the rewrite
rec slow 2026-10-08 ap_slow > "$LFC_KEPT"
cat > "$T/databricks.slow" <<EOF
#!/usr/bin/env bash
case "\$1 \$2" in "pipelines delete") cat "$T/kept.first" >> "$LFC_KEPT";; esac
exec "$T/databricks" "\$@"
EOF
chmod +x "$T/databricks.slow"
DATABRICKS_CLI=$T/databricks.slow bash "$S/kept-sweep.sh" > "$T/out" 2>&1
ok "a run kept during the sweep is carried over" '[ "$(jq -r .id "$LFC_KEPT")" = run_1790000000 ] && [ ! -e "$LFC_KEPT.lock" ]'
exit $fail
