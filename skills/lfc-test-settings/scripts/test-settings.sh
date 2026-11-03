#!/usr/bin/env bash
# test-settings.sh: settings.sh offline on a temporary settings file (LFC_TESTS_SETTINGS), with fake databricks and az on
#   PATH: set/get/unset with values that need quoting, file and folder modes, require (missing keys, no CLI call, the
#   30-day note), env, workspaces (numbered by cloud, saved ones marked, login state), azure (saved and default marked).
#   One PASS/FAIL line per check; exit 1 on any FAIL. Runs on bash 3.2 and 5.
set -uo pipefail
S=$(cd "$(dirname "$0")" && pwd)/settings.sh; fail=0
ok() { if eval "$2"; then echo "PASS $1"; else echo "FAIL $1"; fail=1; fi; }
T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT; mkdir -p "$T/bin"
export LFC_TESTS_SETTINGS=$T/cfg/settings.env FAKE_LOG=$T/calls
cat > "$T/bin/databricks" <<'F'
#!/usr/bin/env bash
echo "databricks $*" >> "$FAKE_LOG"
cat <<'J'
{"profiles": [
 {"name": "ws-b", "host": "https://adb-2.2.azuredatabricks.net", "valid": false, "account_id": "acct-az"},
 {"name": "ws-a", "host": "https://dbc-1.cloud.databricks.com", "valid": true, "account_id": "acct-aws"},
 {"name": "ws-c", "host": "https://dbc-3.cloud.databricks.com", "valid": false, "account_id": "acct-aws"},
 {"name": "acct", "host": "https://accounts.cloud.databricks.com", "valid": true, "account_id": "acct-aws"}]}
J
F
cat > "$T/bin/az" <<'F'
#!/usr/bin/env bash
echo "az $*" >> "$FAKE_LOG"
echo '[{"name": "sub-two", "id": "22", "tenantId": "t2", "isDefault": true}, {"name": "sub-one", "id": "11", "tenantId": "t1", "isDefault": false}]'
F
chmod +x "$T/bin/databricks" "$T/bin/az"
st() { PATH="$T/bin:$PATH" bash "$S" "$@"; }
mode() { stat -f %Lp "$1" 2>/dev/null || stat -c %a "$1"; }

st set "OWNER=a b" "TAGS=x='y' \$z" SOURCE_HOST=vm
ok "set/get: a value with spaces, quotes and \$ comes back as it was" '[ "$(st get OWNER)" = "a b" ] && [ "$(st get TAGS)" = "x='"'"'y'"'"' \$z" ]'
ok "the file is 600 and its folder 700" '[ "$(mode "$LFC_TESTS_SETTINGS")" = 600 ] && [ "$(mode "$T/cfg")" = 700 ]'
st set SOURCE_HOST=podman
ok "set replaces a key, one line each" '[ "$(st get SOURCE_HOST)" = podman ] && [ "$(grep -c "^SOURCE_HOST=" "$LFC_TESTS_SETTINGS")" = 1 ]'
st set "bad-key=1" > /dev/null 2>&1; rc=$?
ok "set refuses a key that isn't capitals, digits and _" '[ $rc = 2 ] && ! grep -q bad-key "$LFC_TESTS_SETTINGS"'
st unset TAGS
ok "unset drops the key; get exits 1" '! st get TAGS > /dev/null'
: > "$FAKE_LOG"; out=$(st require OWNER AZ_SUBSCRIPTION DATABRICKS_PROFILE); rc=$?
ok "require: exit 1, the missing keys one per line, no CLI call" '[ $rc = 1 ] && [ "$out" = "$(printf "AZ_SUBSCRIPTION\nDATABRICKS_PROFILE")" ] && [ ! -s "$FAKE_LOG" ]'
st set SETTINGS_CHECKED=2020-01-01
ok "require: all there exits 0, and a check over 30 days old is noted on stderr" 'st require OWNER 2> "$T/err" && grep -q "last checked 2020-01-01" "$T/err"'
st checked
ok "checked: today's date, so require is quiet" '[ "$(st get SETTINGS_CHECKED)" = "$(date -u +%Y-%m-%d)" ] && st require OWNER 2> "$T/err" && [ ! -s "$T/err" ]'
ok "env: export lines that eval back to the values" '( eval "$(st env)"; [ "$OWNER" = "a b" ] && [ "$SOURCE_HOST" = podman ] )'
st set WORKSPACES=ws-c DATABRICKS_PROFILE=ws-a
w=$(st workspaces)
ok "workspaces: account profiles left out, by cloud then name, numbered" '[ "$(printf "%s\n" "$w" | cut -f3 | paste -sd, -)" = "ws-a,ws-c,ws-b" ] && ! printf "%s" "$w" | grep -q acct$'
ok "workspaces: saved ones marked *, login state per profile" \
  'printf "%s\n" "$w" | grep -q "^1\*	aws	ws-a	login ok" && printf "%s\n" "$w" | grep -q "^2\*	aws	ws-c	expired, covered by account login acct" && printf "%s\n" "$w" | grep -q "^3	azure	ws-b	expired; the Azure CLI may cover it"'
st set AZ_SUBSCRIPTION=11
a=$(st azure)
ok "azure: sorted by name, the saved one and the CLI default marked" '[ "$a" = "$(printf "sub-one\t11\tt1\tsaved\nsub-two\t22\tt2\tdefault")" ]'
exit $fail
