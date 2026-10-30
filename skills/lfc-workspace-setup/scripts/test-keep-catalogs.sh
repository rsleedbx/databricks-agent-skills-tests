#!/usr/bin/env bash
# test-keep-catalogs.sh: checks keep-catalogs.sh against a fake databricks CLI (no workspace): which catalogs a
#   --connection picks (the connection and its foreign catalogs), the never-earlier rule, the month-out default (month ends, leap years), a bad date, set/update/fail, a failed catalogs list. One PASS/FAIL line per check; exit 1 on any FAIL.
set -uo pipefail
S=$(cd "$(dirname "$0")" && pwd); T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT; fail=0
ok() { if eval "$2"; then echo "PASS $1"; else echo "FAIL $1"; fail=1; fi; }
mkdir "$T/bin"
# the fake CLI: catalogs list from $T/list (exit 1 if $T/list-fails), a tag from $T/tag.<catalog>, writes logged to $T/calls
cat > "$T/bin/databricks" <<'EOF'
#!/usr/bin/env bash
case "$1 $2" in
  "catalogs list") [ -e "$FT/list-fails" ] && { echo "Error: invalid access token" >&2; exit 1; }; cat "$FT/list";;
  "entity-tag-assignments get") [ -e "$FT/tag.$4" ] || { echo "Error: not found" >&2; exit 1; }; printf '{"tag_value": "%s"}\n' "$(cat "$FT/tag.$4")";;
  "entity-tag-assignments create") echo "create $3 ${*:4}" >> "$FT/calls"; [ "$3" = denied ] && { echo "Error: PERMISSION_DENIED" >&2; exit 1; }; echo '{}';;
  "entity-tag-assignments update") echo "update $4 $*" >> "$FT/calls"; echo '{}';;
  *) echo "unexpected: $*" >&2; exit 1;;
esac
EOF
chmod +x "$T/bin/databricks"
run() { : > "$T/calls"; out=$(FT=$T PATH="$T/bin:$PATH" KEEP_UNTIL=2026-12-01 bash "$S/keep-catalogs.sh" p "$@"); rc=$?; }
echo '[{"name": "m", "catalog_type": "MANAGED_CATALOG"}, {"name": "a-fc", "catalog_type": "FOREIGN_CATALOG", "connection_name": "a"},
       {"name": "b-fc", "catalog_type": "FOREIGN_CATALOG", "connection_name": "b"}]' > "$T/list"

run --connection a
ok "connection: itself and only its foreign catalog" '[ "$(cut -d" " -f2,4 "$T/calls" | paste -sd, -)" = "a connections,a-fc catalogs" ] && [[ "$out" == "set     connection a RemoveAfter=2026-12-01"* ]] && [ $rc = 0 ]'
echo 2027-01-01 > "$T/tag.later"; echo 2026-12-01 > "$T/tag.same"; echo 2026-11-01 > "$T/tag.early"
run later same
ok "later or equal date: kept, no write" '[ ! -s "$T/calls" ] && [[ "$out" == *"kept    later RemoveAfter=2027-01-01"* ]] && [[ "$out" == *"kept    same"* ]]'
run early
ok "earlier date: updated" '[[ "$out" == "updated early RemoveAfter=2026-12-01 (was 2026-11-01)" ]] && grep -q "^update early .*entity-tag-assignments update catalogs early RemoveAfter .*2026-12-01" "$T/calls"'
echo 2026-11-01 > "$T/tag.a"; run --connection a
ok "connection with an earlier date: updated" '[[ "$out" == *"updated connection a RemoveAfter=2026-12-01 (was 2026-11-01)"* ]] && grep -q "^update a .*entity-tag-assignments update connections a RemoveAfter" "$T/calls"'
rm "$T/tag.a"
run fresh
ok "no tag: set" '[[ "$out" == "set     fresh RemoveAfter=2026-12-01" ]] && grep -qx "create fresh RemoveAfter catalogs --tag-value 2026-12-01 --profile p -o json" "$T/calls"'
run denied fresh
ok "failed create: exit 1, the others still run" '[ $rc = 1 ] && [[ "$out" == *"failed  denied: Error: PERMISSION_DENIED"* ]] && [[ "$out" == *"set     fresh"* ]]'
run --connection z
ok "connection with no foreign catalog: none" '[ $rc = 0 ] && [[ "$out" == *"none    foreign catalogs of z"* ]] && [[ "$out" == *"set     connection z"* ]]'
for d in 2026-01-31:2026-02-28 2028-01-31:2028-02-29 2026-12-31:2027-01-31 2026-03-31:2026-04-30 2026-05-15:2026-06-15; do
  : > "$T/calls"; out=$(FT=$T PATH="$T/bin:$PATH" KEEP_FROM=${d%:*} bash "$S/keep-catalogs.sh" p fresh)
  ok "month out from ${d%:*}: ${d#*:}" '[[ "$out" == "set     fresh RemoveAfter=${d#*:}" ]]'
done
for e in KEEP_FROM=x KEEP_UNTIL=12/01/2026; do
  : > "$T/calls"; out=$(env FT=$T PATH="$T/bin:$PATH" "$e" bash "$S/keep-catalogs.sh" p fresh 2>&1); rc=$?
  ok "bad date ($e): exit 2, no write" '[ $rc = 2 ] && [ ! -s "$T/calls" ] && [[ "$out" == *"no RemoveAfter date"* ]]'
done
echo '{"not": "a list"}' > "$T/list"; run --connection a
ok "listing not a JSON list: failed, exit 1" '[ $rc = 1 ] && [[ "$out" == "failed  foreign catalogs of a: "* ]]'
touch "$T/list-fails"; run --connection a fresh
ok "failed catalogs list: exit 1, named catalogs still tagged" '[ $rc = 1 ] && [[ "$out" == *"failed  catalogs list: Error: invalid access token"* ]] && [[ "$out" == *"set     fresh"* ]]'
exit $fail
