#!/usr/bin/env bash
# test-tpch-verify.sh: offline checks of postgres-tpch-cdc.sh's verify() and broken(), taken from the script and fed a
# stub src: all rules hold (0), some broken (their count), a client that fails or prints no "bad" row (error), and
# broken()'s wording for both. Prints "all passed" or the failures; exit 1 on any failure.
#   bash skills/lfc-schema-change-test/scripts/test-tpch-verify.sh
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
T=$(mktemp -d); fails=0
ok() { printf "  ok    %s\n" "$1"; }
no() { printf "  FAIL  %s\n" "$1"; fails=$((fails + 1)); }
# the two functions, as the script defines them
sed -n -e '/^verify() {/,/^}/p' -e '/^broken() {/p' "$HERE/postgres-tpch-cdc.sh" > "$T/f.sh"
grep -q '^verify() {' "$T/f.sh" && grep -q '^broken() {' "$T/f.sh" || { echo "verify() or broken() not found"; exit 1; }
. "$T/f.sh"
X=s; WL_VERIFY=$T/verify.sql; echo "SELECT 1;" > "$WL_VERIFY"
OUT='' RC=0
src() { cat > /dev/null; printf '%b' "$OUT"; return "$RC"; }   # the stub client: canned output and exit status
is() {   # is <name> <expected verify output> <canned output> [exit status]
  OUT=$3 RC=${4:-0}; local got; got=$(verify)
  [ "$got" = "$2" ] && ok "$1" || no "$1 (got '$got', expected '$2')"
}
is "every rule holds: 0" 0 'bad: keys\t0\t0\nbad: dates\t0\n'
is "two rules broken: 2" 2 'bad: keys\t0\t3\t1\nbad: dates\t0\n'
is "no bad row: error" error 'ERROR:  relation "lineitem" does not exist\n'
is "client fails: error" error 'bad: keys\t0\n' 2
[ "$(broken error)" = "the verify query failed or printed no rule" ] && ok "broken error: says the query failed" || no "broken error"
[ "$(broken 2)" = "2 TPC-H rules broken" ] && ok "broken 2: the count" || no "broken 2"
rm -rf "${T:?}"
echo; [ $fails -eq 0 ] && echo "all passed" || echo "$fails failed"; exit $((fails > 0))
