#!/usr/bin/env bash
# test-settings.sh: settings.sh offline on a temporary settings file (LFC_TESTS_SETTINGS), with fake databricks and az on
#   PATH: set/get/unset with values that need quoting, file and folder modes, require (missing keys, no CLI call, the
#   30-day note), env, workspaces (numbered by cloud, saved ones marked, login state), azure (saved and default marked),
#   setup (the resource-group suggestions, AWS, a saved group that's gone, no az user), a copy without the exec bit.
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
case "$1 $2" in
  "account list") echo '[{"name": "sub-two", "id": "22", "tenantId": "t2", "isDefault": true}, {"name": "sub-one", "id": "11", "tenantId": "t1", "isDefault": false}]' ;;
  "account show") [ -n "${FAKE_ME:-}" ] && echo "$FAKE_ME" || exit 1 ;;
  "group show") case "$*" in *"-n rg-mine "*) echo centralus ;; *) exit 3 ;; esac ;;
  "group list") echo '[{"name": "rg-mine", "location": "centralus", "tags": {"Owner": "me@x.com"}},
    {"name": "rg-made", "location": "eastus", "tags": {"Creator": "ME@X.COM"}},
    {"name": "rg-other", "location": "eastus", "tags": {"Owner": "other@x.com"}},
    {"name": "rg-untagged", "location": "eastus"}]' ;;
esac
F
cat > "$T/bin/aws" <<'F'
#!/usr/bin/env bash
case "$*" in
  "configure list-profiles") echo p1 ;;
  "configure get sso_account_id --profile p1") echo 111122223333 ;;
  "configure get region --profile p1") echo us-east-2 ;;
esac
F
cat > "$T/bin/gcloud" <<'F'
#!/usr/bin/env bash
case "$*" in "config configurations list"*) printf 'default\tproj-a\tus-central1\tTrue\nother\t\t\tFalse\n' ;; esac
F
chmod +x "$T/bin/databricks" "$T/bin/az" "$T/bin/aws" "$T/bin/gcloud"
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
st set AZ_RESOURCE_GROUP=rg-mine
FAKE_ME=me@x.com st setup > "$T/setup" 2>&1
ok "setup: the saved group checked, and only the groups tagged with the az user (any case) suggested" \
  'grep -q "saved: rg-mine, exists, in centralus" "$T/setup" && grep -q "^  rg-mine  centralus" "$T/setup" && grep -q "^  rg-made  eastus" "$T/setup" && ! grep -q "rg-other\|rg-untagged" "$T/setup"'
ok "setup: the AWS profile with its account and region, the subscriptions, the workspaces" \
  'grep -q "^p1	111122223333	us-east-2" "$T/setup" && grep -q "^sub-one	11	t1	saved" "$T/setup" && grep -q "ws-a	login ok" "$T/setup"'
ok "setup: the GCP configurations (project, region, the active one), ? for an unset value" \
  'grep -q "^default	proj-a	us-central1	active" "$T/setup" && grep -q "^other	?	?$" "$T/setup"'
st set AZ_RESOURCE_GROUP=rg-gone
st setup > "$T/setup" 2>&1
ok "setup: a saved group that's gone is said so; with no az user, no group is suggested (not the untagged ones)" \
  'grep -q "saved: rg-gone, NOT found" "$T/setup" && grep -q "no groups suggested" "$T/setup" && ! grep -q "rg-untagged" "$T/setup"'
st set SETTINGS_CHECKED=2020-01-01; cp "$S" "$T/copy.sh"; chmod 644 "$T/copy.sh"
( cd "$T" && PATH="$T/bin:$PATH" bash copy.sh checked )
ok "checked works from a copy without the exec bit, run as bash <file> from its folder" '[ "$(st get SETTINGS_CHECKED)" = "$(date -u +%Y-%m-%d)" ]'
ok "defaults: one line per default, then the exceptions" '[ "$(st defaults | grep -c "^- [A-Z]")" = "$(yq ".defaults | length" "$(dirname "$S")/../../../defaults.yaml")" ] && st defaults | grep -q "^- (exception)"'
grep -q "== defaults" "$T/setup"; d=$?
ok "setup shows the defaults first" '[ $d = 0 ]'
st set REMOVE_AFTER_DAYS=30
ok "remove-after: today + REMOVE_AFTER_DAYS" '[ "$(st remove-after)" = "$(jq -rn "now + 30 * 86400 | strftime(\"%Y-%m-%d\")")" ]'
st unset REMOVE_AFTER_DAYS
ok "remove-after: 7 days when REMOVE_AFTER_DAYS isn't set" '[ "$(st remove-after)" = "$(jq -rn "now + 7 * 86400 | strftime(\"%Y-%m-%d\")")" ]'
exit $fail
