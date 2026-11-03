#!/usr/bin/env bash
# test-eventhubs.sh: eventhubs.sh with a fake az on PATH (it logs every call; FA_GROUP=1: the group exists; FA_NS=<tag>:
#   the namespace exists with that CreatedBy tag, "none" for no tag). Checks that the group is never created, re-tagged or
#   deleted, that a failed lookup isn't read as "nothing there", that a namespace this script didn't make is left alone by up and down, and that its own is created tagged
#   and deleted. One PASS/FAIL line per check; exit 1 on any FAIL.
set -uo pipefail
S=$(cd "$(dirname "$0")" && pwd); fail=0
ok() { if eval "$2"; then echo "PASS $1"; else echo "FAIL $1"; fail=1; fi; }
T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT; mkdir -p "$T/bin"
cat > "$T/bin/az" <<'FA'
#!/usr/bin/env bash
echo "$*" >> "$FA_LOG"
case "$*" in
  "group show"*) [ -n "${FA_GROUP:-}" ] ;;
  "eventhubs namespace show"*)
    [ -z "${FA_LOOKUP_FAILS:-}" ] || { echo "ERROR: AADSTS700082: The refresh token has expired" >&2; exit 1; }
    [ -z "${FA_OTHER_SUB:-}" ] || { echo "ERROR: (ResourceGroupNotFound) Resource group 'rg1' could not be found." >&2; exit 3; }
    [ -n "${FA_NS:-}" ] || { echo "ERROR: (ResourceNotFound) The Resource was not found" >&2; exit 3; }
    [ "$FA_NS" = none ] && echo "" || echo "$FA_NS" ;;
  *"authorization-rule keys list"*) echo "Endpoint=sb://x.servicebus.windows.net/;SharedAccessKeyName=kafka-test;SharedAccessKey=k" ;;
  *) : ;;
esac
FA
chmod +x "$T/bin/az"
eh() { : > "$T/log"; FA_LOG=$T/log PATH="$T/bin:$PATH" bash "$S/eventhubs.sh" "$@" > "$T/out" 2>&1; }
calls() { cat "$T/log"; }

FA_GROUP= eh up rg1 ns1 centralus --password-file "$T/pw" --apply; rc=$?
ok "up, group missing: stops (rc 2), never creates it" '[ $rc = 2 ] && grep -q "never creates one" "$T/out" && ! calls | grep -q "group create" && ! calls | grep -q "namespace create"'
FA_GROUP=1 FA_NS=none eh up rg1 ns1 centralus --password-file "$T/pw" --apply; rc=$?
ok "up, a namespace of that name it didn't make: left alone (rc 2)" '[ $rc = 2 ] && ! calls | grep -q -E "namespace create|eventhub create|rule create"'
FA_GROUP=1 FA_NS= eh up rg1 ns1 centralus --password-file "$T/pw" --tags "Owner=a RemoveAfter=2026-11-09" --apply; rc=$?
ok "up, new: the namespace is created tagged CreatedBy (and the given tags), the group untouched, the password file 0600" \
  '[ $rc = 0 ] && calls | grep "namespace create" | grep -q -- "--tags CreatedBy=lfc-kafka-freshness-test Owner=a RemoveAfter=2026-11-09" && ! calls | grep -q -E "^group (create|update|delete)" && [ "$(stat -f %Lp "$T/pw" 2>/dev/null || stat -c %a "$T/pw")" = 600 ]'
FA_GROUP=1 FA_NS=lfc-kafka-freshness-test eh up rg1 ns1 centralus --password-file "$T/pw" --apply; rc=$?
ok "up, its own namespace from before: reused, not created again" '[ $rc = 0 ] && grep -q reused "$T/out" && ! calls | grep -q "namespace create"'
AZ_SUBSCRIPTION=sub-1 FA_GROUP=1 FA_NS= eh up rg1 ns1 centralus --password-file "$T/pw" --apply
ok "AZ_SUBSCRIPTION goes on every az call" '[ "$(calls | grep -c -v -- "--subscription sub-1")" = 0 ]'
FA_NS=none eh down rg1 ns1 --apply; rc=$?
ok "down, a namespace it didn't make: not deleted (rc 2)" '[ $rc = 2 ] && ! calls | grep -q delete'
FA_NS=lfc-kafka-freshness-test eh down rg1 ns1 --apply; rc=$?
ok "down, its own: the namespace deleted, never the group" '[ $rc = 0 ] && calls | grep -q "namespace delete" && ! calls | grep -q "group delete"'
FA_NS= eh down rg1 ns1 --apply; rc=$?
ok "down, nothing there: rc 0, no delete" '[ $rc = 0 ] && ! calls | grep -q delete'
FA_LOOKUP_FAILS=1 FA_NS=lfc-kafka-freshness-test eh down rg1 ns1 --apply; rc=$?
ok "down, the lookup fails (expired login): rc 1, no delete, the error shown" '[ $rc = 1 ] && ! calls | grep -q delete && grep -q AADSTS "$T/out"'
FA_OTHER_SUB=1 eh down rg1 ns1 --apply; rc=$?
ok "down, the group isn't in this subscription: rc 1, not \"nothing to delete\"" '[ $rc = 1 ] && ! calls | grep -q delete && grep -q "check the subscription" "$T/out"'
exit $fail
