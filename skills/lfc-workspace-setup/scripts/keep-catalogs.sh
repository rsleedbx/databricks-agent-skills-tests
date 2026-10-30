#!/usr/bin/env bash
# keep-catalogs.sh <profile> [--connection N]... [catalog]...: set the RemoveAfter tag on test catalogs and connections to a month out.
#   Some workspaces tag a catalog that has no RemoveAfter tag with a date a couple of weeks out and delete it on that
#   date. This sets RemoveAfter to today + 1 month (KEEP_UNTIL=YYYY-MM-DD for another date; KEEP_FROM=YYYY-MM-DD counts
#   the month from that day) on each catalog named, and
#   with --connection N on connection N and every foreign catalog made from it. It never moves a later date earlier.
#   One line per catalog (a connection's line says connection): kept (already later), set or updated, or failed with the CLI's error; none for a --connection
#   with no foreign catalog. Exit 1 if any failed (a failed catalogs list too).
#   Runs from the harness (bin/run_batch.sh) on $LFC_CATALOG and the env's connections, and after a script creates a
#   connection. Needs APPLY TAG on the catalog or connection (or ownership); a governed RemoveAfter tag also needs ASSIGN on its policy.
set -uo pipefail
P=${1:?usage: keep-catalogs.sh <profile> [--connection N]... [catalog]...}; shift
CONNS=(); CATS=()   # CATS: "<entity type> <name>"
while [ $# -gt 0 ]; do case $1 in --connection) CONNS+=("${2:?--connection needs a name}"); shift 2;; -*) echo "unknown $1"; exit 2;; *) CATS+=("catalogs $1"); shift;; esac; done
# a month out; Jan 31 -> Feb 28 (day 0 of the month mktime rolled into), as date -v+1m does
UNTIL=${KEEP_UNTIL:-$(jq -rn --arg f "${KEEP_FROM:-}" '(if $f == "" then now else $f + "T12:00:00Z" | fromdate end) | gmtime as $t | ($t | .[1] += 1 | mktime | gmtime) as $n
  | (if $n[2] != $t[2] then $n | .[2] = 0 else $n end) | mktime | strftime("%F")')}
bad=0
if [ ${#CONNS[@]} -gt 0 ]; then
  E=$(mktemp); trap 'rm -f "${E:?}"' EXIT
  if ! all=$(databricks catalogs list --profile "$P" -o json 2> "$E"); then
    echo "failed  catalogs list: $(head -c 200 "$E" | tr '\n' ' ')"; bad=1; all='[]'; fi
  for c in ${CONNS[@]+"${CONNS[@]}"}; do
    CATS+=("connections $c")
    if ! found=$(echo "$all" | jq -r --arg c "$c" '.[] | select(.catalog_type == "FOREIGN_CATALOG" and .connection_name == $c) | .name' 2> "$E"); then
      echo "failed  foreign catalogs of $c: catalogs list isn't a JSON list ($(head -c 120 "$E" | tr '\n' ' '))"; bad=1; continue; fi
    [ -n "$found" ] || { [ $bad = 0 ] && echo "none    foreign catalogs of $c"; }
    while IFS= read -r n; do [ -n "$n" ] && CATS+=("catalogs $n"); done <<< "$found"
  done
fi
[ ${#CATS[@]} -gt 0 ] || exit $bad
for e in ${CATS[@]+"${CATS[@]}"}; do
  t=${e%% *}; n=${e#* }; [ $t = connections ] && l="connection $n" || l=$n
  cur=$(databricks entity-tag-assignments get $t "$n" RemoveAfter --profile "$P" -o json 2>/dev/null | jq -r '.tag_value // ""')
  if [ -n "$cur" ] && [[ ! "$cur" < "$UNTIL" ]]; then echo "kept    $l RemoveAfter=$cur"; continue; fi
  if [ -n "$cur" ]; then verb=updated; out=$(databricks entity-tag-assignments update $t "$n" RemoveAfter tag_value --json "{\"tag_value\": \"$UNTIL\"}" --profile "$P" -o json 2>&1)
  else verb=set; out=$(databricks entity-tag-assignments create "$n" RemoveAfter $t --tag-value "$UNTIL" --profile "$P" -o json 2>&1); fi
  if [ $? -eq 0 ]; then printf "%-7s %s RemoveAfter=%s%s\n" "$verb" "$l" "$UNTIL" "${cur:+ (was $cur)}"
  else echo "failed  $l: $(echo "$out" | head -c 200 | tr '\n' ' ')"; bad=1; fi
done
exit $bad
