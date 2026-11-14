#!/usr/bin/env bash
# test-workload-env.sh: offline checks of the workload contract (contract 1): workload-env.sh on the fixtures (the valid file with
# and without bundle_cmd, each refusal with its reason, a secret never printed, the exports set by eval). The fixtures name
# absolute paths, so each is copied to a temp folder with __FX__ replaced by the fixtures folder. Also runs test-validate-model.sh.
# Prints "all passed" or the failures; exit 1 on any failure.
#   bash skills/lfc-workload-contract/scripts/test-workload-env.sh
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); FX=$HERE/fixtures; WE=$HERE/workload-env.sh
T=$(mktemp -d); fails=0
ok() { printf "  ok    %s\n" "$1"; }
no() { printf "  FAIL  %s\n" "$1"; fails=$((fails + 1)); }
check() { local name=$1; shift; if "$@" > "$T/out" 2>&1; then ok "$name"; else no "$name"; sed 's/^/        /' "$T/out" | tail -5; fi; }

render() { sed "s#__FX__#$FX#g" "$FX/$1" > "$T/$1"; echo "$T/$1"; }   # the fixture with its absolute paths, in $T
refused() {   # refused <fixture> <words that must be in the refusal>: exit 2, nothing on stdout, the words on stderr
  local f rc w; f=$(render "$1"); shift
  "$WE" "$f" > "$T/o" 2> "$T/e"; rc=$?
  [ "$rc" = 2 ] && [ ! -s "$T/o" ] || return 1
  for w in "$@"; do grep -qF -- "$w" "$T/e" || return 1; done
}
valid_exits() { local f; f=$(render wl-valid.json); "$WE" "$f" > /dev/null; }
exports_round_trip() {   # the exports of the valid file set what a consumer reads, and WL_BUNDLE from the file
  local f; f=$(render wl-valid.json)
  ( eval "$("$WE" "$f")"
    [ "$WL_MODEL" = tpcb ] && [ "$WL_SCALE" = 0.01 ] && [ "$WL_DIALECT" = tsql ] && [ "$WL_NTABLES" = 4 ] &&
    [ "$WL_TABLES" = "branch teller account history" ] && [ "$WL_DDL" = "$FX/model.sql" ] &&
    [ "$WL_FILL" = "bash $FX/fake-fill.sh ms" ] && [ "$WL_FILL_IN_BUNDLE" = "bash fake-fill.sh ms" ] &&
    [ "$WL_BUNDLE" = "bash $FX/fake-bundle.sh" ] )
}
no_bundle_unsets() {   # no bundle_cmd in the file: WL_BUNDLE is unset, even when the environment had it
  local f; f=$(render wl-no-bundle.json)
  ( WL_BUNDLE=stale; eval "$("$WE" "$f")"; [ -z "${WL_BUNDLE+x}" ] && [ "$WL_NTABLES" = 4 ] )
}
verify_exported() {   # verify in the file: WL_VERIFY is its path; without it (the valid file), WL_VERIFY is unset
  local f g; f=$(render wl-verify.json); g=$(render wl-valid.json)
  ( eval "$("$WE" "$f")"; [ "$WL_VERIFY" = "$FX/model.sql" ] && [ "$WL_DIALECT" = postgres ] ) &&
  ( WL_VERIFY=stale; eval "$("$WE" "$g")"; [ -z "${WL_VERIFY+x}" ] )
}
secret_not_printed() { refused wl-secret.json "a secret" && ! grep -q hunter2 "$T/e"; }
usage() { local out; out=$("$WE" 2>&1); [ $? = 2 ] || return 1; case $out in *'workload-env.sh <workload.json>'*) ;; *) return 1 ;; esac; }
unreadable() { "$WE" "$T/none.json" > /dev/null 2>&1; [ $? = 2 ]; }

echo "== workload-env.sh"
check "usage with no arguments: exit 2, the usage" usage
check "valid file: exits 0" valid_exits
check "valid file: the exports hold what the consumers read (WL_TABLES, WL_NTABLES, the paths, WL_FILL_IN_BUNDLE)" exports_round_trip
check "no bundle_cmd: WL_BUNDLE is unset" no_bundle_unsets
check "a file that isn't there: exit 2" unreadable
check "contract 2: refused, the version named" refused wl-contract-2.json "contract 2" "reads contract 1 only"
check "no contract field: refused" refused wl-no-contract.json "missing contract"
check "not JSON: refused" refused wl-not-json.json "not a JSON object"
check "no model: refused" refused wl-no-model.json "missing model"
check "model with a space: refused" refused wl-bad-model.json "model must be a lowercase name"
check "scale as text: refused" refused wl-bad-scale.json "scale must be a number above 0"
check "relative ddl: refused" refused wl-relative-ddl.json "ddl must be an absolute path"
check "ddl file missing: refused" refused wl-no-ddl.json "is not a file"
check "empty tables: refused" refused wl-empty-tables.json "tables must be a non-empty list"
check "table name with a space: refused" refused wl-bad-table.json "not a plain identifier"
check "table listed twice: refused" refused wl-dup-table.json "listed twice"
check "fill_cmd with quotes: refused" refused wl-quote-fill.json "fill_cmd must be words"
check "fill_cmd names a file that isn't there: refused" refused wl-fill-missing.json "is not a file"
check "a password in the file: refused, and its value not printed" secret_not_printed
check "verify: WL_VERIFY is its path; unset without it" verify_exported
check "verify names a file that isn't there: refused" refused wl-verify-missing.json "verify" "is not a file"

echo "== validate-model.py"
check "fixtures: model format 1 checks" bash "$HERE/test-validate-model.sh"

rm -rf "${T:?}"
echo; [ $fails -eq 0 ] && echo "all passed" || echo "$fails failed"; exit $((fails > 0))
