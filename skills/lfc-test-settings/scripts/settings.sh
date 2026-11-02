#!/usr/bin/env bash
# settings.sh show|path|get KEY|set KEY=VALUE...|unset KEY...|require KEY...|env|azure: the answers a test skill asks for
#   once (cloud identity, region, workspace profile, catalog, resource group, owner), kept per user in one file outside
#   the repo, so later runs read them instead of asking again. File: $LFC_TESTS_SETTINGS, default
#   ~/.lfc/tests/settings.env (mode 600, folder 700); lines KEY=value, shell-quoted, never committed.
#   show          every key and value (no secrets belong here: passwords stay in their own 0600 files)
#   get KEY       the value; exit 1 if it isn't set
#   set K=V ...   add or replace keys (KEY: capital letters, digits and _)
#   require K ... exit 1 and list the missing keys, one per line, so the skill asks for only those
#   env           export lines for eval: eval "$(settings.sh env)"
#   azure         the subscriptions this az login reaches (name, id, tenant), the saved one marked "saved", the CLI's
#                 default marked "default": what to show the user when AZ_SUBSCRIPTION is missing
set -uo pipefail
F=${LFC_TESTS_SETTINGS:-$HOME/.lfc/tests/settings.env}
usage() { sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
valid() { case $1 in ''|*[!A-Z0-9_]*|[0-9]*) return 1 ;; esac; }
value() {   # value KEY: the saved value, unquoted by the shell that wrote it
  [ -f "$F" ] || return 1
  local line; line=$(grep -E "^$1=" "$F" | tail -1) || return 1
  ( eval "$line"; eval "printf '%s' \"\${$1}\"" )
}
init() { mkdir -p "$(dirname "$F")" && chmod 700 "$(dirname "$F")" && { [ -f "$F" ] || : > "$F"; } && chmod 600 "$F"; }
cmd=${1:-}; [ -n "$cmd" ] || usage; shift
case $cmd in
  path) echo "$F" ;;
  show) [ -f "$F" ] && cat "$F" ;;
  get) [ $# = 1 ] && valid "$1" || usage; v=$(value "$1") && [ -n "$v" ] || exit 1; printf '%s\n' "$v" ;;
  set)
    [ $# -gt 0 ] || usage; init || exit 1
    for kv; do k=${kv%%=*}; valid "$k" && [ "$k" != "$kv" ] || { echo "not KEY=VALUE: $kv" >&2; exit 2; }; done
    t=$(mktemp "$F.XXXXXX") || exit 1; chmod 600 "$t"
    keys=$(for kv; do printf '%s\n' "${kv%%=*}"; done)
    grep -v -E "^($(printf '%s' "$keys" | paste -sd'|' -))=" "$F" > "$t" || :
    for kv; do printf '%s=%q\n' "${kv%%=*}" "${kv#*=}" >> "$t"; done
    mv -f "$t" "$F" ;;
  unset)
    [ $# -gt 0 ] || usage; [ -f "$F" ] || exit 0
    for k; do valid "$k" || usage; done
    t=$(mktemp "$F.XXXXXX") || exit 1; chmod 600 "$t"
    grep -v -E "^($(printf '%s\n' "$@" | paste -sd'|' -))=" "$F" > "$t" || :
    mv -f "$t" "$F" ;;
  require)
    [ $# -gt 0 ] || usage; miss=0
    for k; do valid "$k" || usage; v=$(value "$k") && [ -n "$v" ] || { echo "$k"; miss=1; }; done
    exit $miss ;;
  env) [ -f "$F" ] || exit 0; grep -E '^[A-Z_][A-Z0-9_]*=' "$F" | sed 's/^/export /' ;;
  azure)
    saved=$(value AZ_SUBSCRIPTION 2>/dev/null || true)
    az account list --all -o json | jq -r --arg s "$saved" '
      sort_by(.name)[] | [.name, .id, .tenantId,
        ([if .id == $s or .name == $s then "saved" else empty end, if .isDefault then "default" else empty end] | join(" "))]
      | @tsv' ;;
  *) usage ;;
esac
