#!/usr/bin/env bash
# workload-env.sh <workload.json>: validate a workload file (contract 1, lfc-workload-contract) and print the exports the
# consumers use: eval "$(workload-env.sh workload.json)". Exit 2 with one line when the file breaks the contract (a missing
# field, a bad name or value, a path that isn't there, a secret in the file, a contract version this script doesn't read).
#   Exports WL_MODEL, WL_SCALE, WL_DIALECT, WL_TABLES (space-separated), WL_NTABLES, WL_DDL (the DDL file), WL_FILL (fill_cmd),
#   WL_FILL_IN_BUNDLE (fill_cmd as it runs from an unpacked bundle: each absolute word becomes its base name), and WL_BUNDLE
#   (bundle_cmd; unset when the file has none), and WL_VERIFY (verify: the model's rule checks; unset when the file has
#   none). Nothing else is set, and no password is ever in the file.
set -uo pipefail
set -f                                    # the command words are split on spaces only: no globbing
[ $# -ge 1 ] || { sed -n 2,7p "$0"; exit 2; }
F=$1
bad() { echo "workload-env: $F: $*" >&2; exit 2; }
[ -r "$F" ] || bad "can't read it"
jq -e 'type == "object"' "$F" > /dev/null 2>&1 || bad "not a JSON object"
CV=$(jq -c '.contract // null' "$F")
[ "$CV" != null ] || bad "missing contract (the version: \"contract\": 1)"
[ "$CV" = 1 ] || { echo "workload-env: $F is contract $CV; this reads contract 1 only: use a workload-env.sh that reads $CV, or have the producer write contract 1" >&2; exit 2; }

# every problem in the JSON, one per line; none means the fields hold
PROBLEMS=$(jq -r '
  def plain: test("^[A-Za-z_][A-Za-z0-9_]*$");
  def safe_words: type == "string" and length > 0 and (test("[^A-Za-z0-9_./ -]") | not) and (split(" ") | any(. == "") | not);
  def safe_path: type == "string" and startswith("/") and (test("[^A-Za-z0-9_./-]") | not);
  [
    (if has("model") | not then "missing model"
     elif (.model | type) != "string" or (.model | test("^[a-z][a-z0-9_]*$") | not)
       then "model must be a lowercase name (letters, digits, _): the copies are named <model>_NNNN" else empty end),
    (if has("scale") | not then "missing scale"
     elif (.scale | type) != "number" or .scale <= 0 then "scale must be a number above 0" else empty end),
    (if has("dialect") | not then "missing dialect"
     elif (.dialect | type) != "string" or (.dialect | test("^[a-z][a-z0-9_]*$") | not)
       then "dialect must be a lowercase name (tsql, postgres, ...)" else empty end),
    (if has("tables") | not then "missing tables"
     elif (.tables | type) != "array" or (.tables | length) == 0 then "tables must be a non-empty list of names"
     elif ([.tables[] | type == "string" and plain] | all) | not then "a table name is not a plain identifier (letters, digits, _)"
     elif (.tables | unique | length) != (.tables | length) then "a table name is listed twice"
     else empty end),
    (if has("ddl") | not then "missing ddl"
     elif (.ddl | safe_path) | not then "ddl must be an absolute path of letters, digits, _ . / - (no spaces)" else empty end),
    (if has("fill_cmd") | not then "missing fill_cmd"
     elif (.fill_cmd | safe_words) | not then "fill_cmd must be words separated by one space: letters, digits, _ . / - (no quotes or shell operators)"
     else empty end),
    (if has("verify") and .verify != null and ((.verify | safe_path) | not)
       then "verify must be an absolute path of letters, digits, _ . / - (no spaces)" else empty end),
    (if has("bundle_cmd") and .bundle_cmd != null and ((.bundle_cmd | safe_words) | not)
       then "bundle_cmd must be words separated by one space: letters, digits, _ . / - (no quotes or shell operators)" else empty end),
    (keys[] | select(test("password|passwd|secret|token|credential|pwd"; "i")) | "\(.) is a secret: a workload file holds no secrets")
  ] | .[]' "$F")
[ -z "$PROBLEMS" ] || bad "$(printf '%s' "$PROBLEMS" | paste -sd';' - | sed 's/;/; /g')"

# the files the commands and the DDL name must be there
FILE_PROBLEMS=""
DDL=$(jq -r '.ddl' "$F")
[ -f "$DDL" ] || FILE_PROBLEMS="ddl $DDL is not a file"
VF=$(jq -r '.verify // ""' "$F")
[ -z "$VF" ] || [ -f "$VF" ] || FILE_PROBLEMS="${FILE_PROBLEMS:+$FILE_PROBLEMS, }verify $VF is not a file"
for w in $(jq -r '[.fill_cmd, (.bundle_cmd // "")] | join(" ")' "$F"); do
  case $w in /*) [ -f "$w" ] || FILE_PROBLEMS="${FILE_PROBLEMS:+$FILE_PROBLEMS, }$w is not a file" ;; esac
done
[ -z "$FILE_PROBLEMS" ] || bad "$FILE_PROBLEMS"

jq -r '
  "export WL_MODEL=" + (.model | @sh),
  "export WL_SCALE=" + (.scale | tostring | @sh),
  "export WL_DIALECT=" + (.dialect | @sh),
  "export WL_TABLES=" + (.tables | join(" ") | @sh),
  "export WL_NTABLES=" + (.tables | length | tostring),
  "export WL_DDL=" + (.ddl | @sh),
  "export WL_FILL=" + (.fill_cmd | @sh),
  "export WL_FILL_IN_BUNDLE=" + (.fill_cmd | split(" ") | map(if startswith("/") then (split("/") | .[-1]) else . end) | join(" ") | @sh),
  (if (.bundle_cmd // null) != null then "export WL_BUNDLE=" + (.bundle_cmd | @sh) else "unset WL_BUNDLE" end),
  (if (.verify // null) != null then "export WL_VERIFY=" + (.verify | @sh) else "unset WL_VERIFY" end)
' "$F"
