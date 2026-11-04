#!/usr/bin/env bash
# test-delete.sh: sqldb.sh's and mi.sh's delete and create tags offline, with a fake az on PATH (it logs every call; FA_TAGS: the
#   resource's tags as JSON, FA_SHOW=missing: not found, FA_SHOW=error: an expired login). delete removes only a resource
#   tagged CreatedBy=<this skill>, or one made before that tag (purpose=lfc-test, this owner); not found exits 3, a failed
#   lookup 1, another's 2, with nothing deleted. One PASS/FAIL line per check; exit 1 on any FAIL.
set -uo pipefail
S=$(cd "$(dirname "$0")" && pwd); fail=0
ok() { if eval "$2"; then echo "PASS $1"; else echo "FAIL $1"; fail=1; fi; }
T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT; mkdir -p "$T/bin"
cat > "$T/bin/az" <<'FA'
#!/usr/bin/env bash
echo "$*" >> "$FA_LOG"
case "$*" in
  "account show"*) echo tenant-1 ;;   # the subscription check every command makes first
  *" show "*) case ${FA_SHOW:-} in missing) echo "ERROR: (ResourceNotFound) not found" >&2; exit 3;; error) echo "ERROR: AADSTS700082: expired" >&2; exit 1;; esac
            echo "${FA_TAGS:-null}" ;;
esac
FA
chmod +x "$T/bin/az"
export LFC_TESTS_SETTINGS=$T/settings.env FA_LOG=$T/log RG=rg1 SUBSCRIPTION=sub1 SERVER=srv1 MI_NAME=mi1 VNET=vnet1 DB=db1 LOCATION=centralus OWNER=jane
run() { : > "$T/log"; PATH="$T/bin:$PATH" bash "$S/$1" "${@:2}" > "$T/out" 2>&1; }
deleted() { grep -q " delete " "$T/log"; }
for c in "sqldb.sh lfc-source-azure-sql" "mi.sh lfc-source-azure-sql"; do
  set -- $c; f=$1; shift; mine=$1; shift
  FA_TAGS="{\"CreatedBy\": \"$mine\"}" run $f "$@" delete; rc=$?
  ok "$f: its own (CreatedBy=$mine): deleted" '[ $rc = 0 ] && deleted'
  FA_TAGS='{"purpose": "lfc-test", "owner": "jane"}' run $f "$@" delete; rc=$?
  ok "$f: made before the tag, this owner: deleted" '[ $rc = 0 ] && deleted'
  FA_TAGS='{"CreatedBy": "lfc-source-eventhubs", "purpose": "lfc-test", "owner": "jane"}' run $f "$@" delete; rc=$?
  ok "$f: another skill's, same owner: not deleted (rc 2)" '[ $rc = 2 ] && ! deleted'
  FA_TAGS='{"purpose": "lfc-test", "owner": "someone-else"}' run $f "$@" delete; rc=$?
  ok "$f: another owner's: not deleted (rc 2)" '[ $rc = 2 ] && ! deleted'
  FA_TAGS=null run $f "$@" delete; rc=$?
  ok "$f: untagged: not deleted (rc 2)" '[ $rc = 2 ] && ! deleted'
  FA_SHOW=missing run $f "$@" delete; rc=$?
  ok "$f: not found: rc 3, nothing deleted" '[ $rc = 3 ] && ! deleted && grep -q "nothing to delete" "$T/out"'
  FA_SHOW=error run $f "$@" delete; rc=$?
  ok "$f: lookup fails (expired login): rc 1, the error shown, nothing deleted" '[ $rc = 1 ] && ! deleted && grep -q AADSTS "$T/out"'
done
exit $fail
