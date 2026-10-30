#!/usr/bin/env bash
# test-ws-token.sh: checks ws-token.sh's TO (its timeout: perl alarm, as timeout(1) isn't on macOS), the line from the
#   script itself: a command's exit status passes through, and a command that hangs is stopped. One PASS/FAIL line per
#   check; exit 1 on any FAIL.
set -uo pipefail
S=$(cd "$(dirname "$0")" && pwd); fail=0
ok() { if eval "$2"; then echo "PASS $1"; else echo "FAIL $1"; fail=1; fi; }
eval "$(grep -E '^TO=\(' "$S/ws-token.sh")"
[ ${#TO[@]} -gt 0 ] || { echo "FAIL no TO=( line in ws-token.sh"; exit 1; }
"${TO[@]}" 5 true; rc=$?
ok "exit 0 passes through" '[ $rc = 0 ]'
"${TO[@]}" 5 sh -c 'exit 3'; rc=$?
ok "exit 3 passes through" '[ $rc = 3 ]'
s=$SECONDS; "${TO[@]}" 1 sleep 5 2>/dev/null; rc=$?
ok "a hung command is stopped after 1 s" '[ $rc != 0 ] && [ $((SECONDS - s)) -le 3 ]'
"${TO[@]}" 5 no-such-command-x 2>/dev/null; rc=$?
ok "a missing command fails" '[ $rc != 0 ]'
exit $fail
