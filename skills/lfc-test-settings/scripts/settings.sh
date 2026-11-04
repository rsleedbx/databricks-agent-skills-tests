#!/usr/bin/env bash
# settings.sh setup|show|path|get KEY|set KEY=VALUE...|unset KEY...|require KEY...|checked|env|azure: the answers a test
#   skill asks for once (cloud accounts, region, workspaces, Podman or a VM, catalog, resource group, owner), kept per user
#   in one file outside the repo, so later runs read them instead of asking or listing again. File: $LFC_TESTS_SETTINGS,
#   default ~/.lfc/tests/settings.env (mode 600, folder 700); lines KEY=value, shell-quoted, never committed.
#   setup         the first run (or a refresh): one parallel pass over what changes rarely (Azure subscriptions, AWS
#                 profiles and their accounts, GCP projects from the gcloud config, Databricks workspace profiles and
#                 whether each login works, local Podman),
#                 next to what's saved, so every question is asked at once. Then `set` the answers and `checked`.
#   show          every key and value (no secrets belong here: passwords stay in their own 0600 files)
#   get KEY       the value; exit 1 if it isn't set
#   set K=V ...   add or replace keys (KEY: capital letters, digits and _)
#   require K ... exit 1 and list the missing keys, one per line, so the skill asks for only those. Reads the file only
#                 (no CLI calls); notes on stderr when the settings were last checked over 30 days ago
#   AZ_RESOURCE_GROUP is an existing group, asked for: setup suggests the groups in AZ_SUBSCRIPTION tagged with the az
#   user as Owner or Creator. A subscription allows only so many groups, so the skills never create, re-tag or delete one.
#   checked       record today as SETTINGS_CHECKED (after setup, or after a failed call made you re-check a value)
#   env           export lines for eval: eval "$(settings.sh env)"
#   workspaces    the workspace profiles of `databricks auth profiles`, numbered, by cloud: host, and whether the login
#                 works or an account login covers it (* = saved in WORKSPACES or DATABRICKS_PROFILE). Show it and ask
#                 which ones the user works in often: those become WORKSPACES, the most used DATABRICKS_PROFILE
#   azure         the subscriptions this az login reaches (name, id, tenant), the saved one marked "saved", the CLI's
#                 default marked "default": what to show the user when AZ_SUBSCRIPTION is missing
set -uo pipefail
F=${LFC_TESTS_SETTINGS:-$HOME/.lfc/tests/settings.env}
SELF=$(cd "$(dirname "$0")" && pwd)/$(basename "$0")   # its own subcommands, as bash: no exec bit or ./ needed
usage() { sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
TO=(perl -e 'alarm shift; exec @ARGV or die "$ARGV[0]: $!\n"')   # TO <seconds> <cmd>...: timeout(1) isn't on macOS
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
    c=$(value SETTINGS_CHECKED 2>/dev/null || true)
    if [ -n "$c" ]; then
      age=$(jq -n --arg d "$c" '((now - ($d + "T00:00:00Z" | fromdateiso8601)) / 86400) | floor' 2>/dev/null || echo 0)
      [ "${age:-0}" -le 30 ] || echo "settings: last checked $c ($age days ago): run setup to refresh them" >&2
    fi
    exit $miss ;;
  checked) init || exit 1; bash "$SELF" set "SETTINGS_CHECKED=$(date -u +%Y-%m-%d)" ;;
  setup)
    T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT
    { command -v az > /dev/null && "${TO[@]}" 60 bash "$SELF" azure; } > "$T/azure" 2>/dev/null &
    { command -v aws > /dev/null && for p in $(aws configure list-profiles 2>/dev/null); do   # config only: no network
        a=$(aws configure get sso_account_id --profile "$p" 2>/dev/null); r=$(aws configure get region --profile "$p" 2>/dev/null)
        printf '%s\t%s\t%s\n' "$p" "${a:-?}" "${r:-?}"; done; } > "$T/aws" 2>/dev/null &
    { command -v gcloud > /dev/null && { "${TO[@]}" 30 gcloud config configurations list --format='value(name,properties.core.project,properties.compute.region,is_active)' 2>/dev/null \
        | awk -F'\t' '{printf "%s\t%s\t%s%s\n", $1, ($2 == "" ? "?" : $2), ($3 == "" ? "?" : $3), ($4 == "True" ? "\tactive" : "")}'; }; } > "$T/gcp" 2>/dev/null &   # config only: no network
    { command -v databricks > /dev/null && bash "$SELF" workspaces; } > "$T/dbx" 2>/dev/null &
    { if command -v podman > /dev/null; then
        if [ "$(uname -s)" = Darwin ]; then
          command -v limactl > /dev/null && "${TO[@]}" 30 limactl list --format '{{.Name}} {{.Status}}' 2>/dev/null | sed 's/^/lima vm: /'
          command -v limactl > /dev/null || echo "podman on macOS without Lima: see lfc-source-podman"
        else echo "native podman $(podman --version 2>/dev/null | awk '{print $NF}')"; fi
      else echo "no podman (lfc-source-podman to install, or a cloud VM: lfc-source-cloud-vm)"; fi; } > "$T/podman" 2>&1 &
    rg=$(value AZ_RESOURCE_GROUP 2>/dev/null || true); sub=$(value AZ_SUBSCRIPTION 2>/dev/null || true)
    { if ! command -v az > /dev/null || [ -z "$sub" ]; then echo "(AZ_SUBSCRIPTION not set: choose the subscription first)"
      else
        me=$("${TO[@]}" 30 az account show --query user.name -o tsv 2>/dev/null)
        [ -z "$rg" ] || { "${TO[@]}" 60 az group show --subscription "$sub" -n "$rg" --query location -o tsv 2>/dev/null \
          | sed "s/^/saved: $rg, exists, in /" | grep . || echo "saved: $rg, NOT found in the subscription"; }
        if [ -z "$me" ]; then echo "yours: (the az user couldn't be read: az account show; no groups suggested)"
        else echo "yours (Owner or Creator tag = $me):"
        "${TO[@]}" 60 az group list --subscription "$sub" -o json 2>/dev/null | jq -r --arg me "$me" \
          '.[] | select(((.tags.Owner // "") | ascii_downcase) == ($me | ascii_downcase) or ((.tags.Creator // "") | ascii_downcase) == ($me | ascii_downcase)) | "  \(.name)  \(.location)"'
        fi
      fi; } > "$T/rg" 2>&1 &
    wait
    echo "== saved ($F)"; [ -f "$F" ] && cat "$F"; echo
    echo "== Azure resource group for every test resource (AZ_RESOURCE_GROUP: an existing one; skills never create or delete a group)"; cat "$T/rg"; echo
    echo "== Azure subscriptions (name, id, tenant, saved/default)"; cat "$T/azure"; echo
    echo "== AWS profiles (profile, account, region)"; cat "$T/aws"; echo
    echo "== GCP configurations (name, project, region, active)"; cat "$T/gcp"; echo
    echo "== Databricks workspace profiles: which do you work in often? (number, cloud, profile, login, host)"; cat "$T/dbx"; echo
    echo "== local Podman"; cat "$T/podman" ;;
  workspaces)
    mine=",$(value WORKSPACES 2>/dev/null || true),$(value DATABRICKS_PROFILE 2>/dev/null || true),"
    "${TO[@]}" 90 databricks auth profiles -o json 2>/dev/null | jq -r --arg mine "$mine" '
      def cloud: (.host // "") | if test("azuredatabricks") then "azure" elif test("gcp[.]databricks") then "gcp" else "aws" end;
      def acct: (.host // "") | test("^https://accounts[.]");
      [.profiles[] | select(acct)] as $a
      | [.profiles[] | select(acct | not)] | sort_by(cloud, .name) | to_entries[]
      | .value as $w | ($a | map(select(.valid and .account_id == $w.account_id and .account_id != null)) | .[0].name // "") as $cov
      | [ "\(.key + 1)\(if ($mine | contains("," + $w.name + ",")) then "*" else "" end)", ($w | cloud), $w.name,
          (if $w.valid then "login ok" elif $cov != "" then "expired, covered by account login \($cov)"
           elif ($w | cloud) == "azure" then "expired; the Azure CLI may cover it" else "expired" end), $w.host ] | @tsv' ;;
  env) [ -f "$F" ] || exit 0; grep -E '^[A-Z_][A-Z0-9_]*=' "$F" | sed 's/^/export /' ;;
  azure)
    saved=$(value AZ_SUBSCRIPTION 2>/dev/null || true)
    az account list --all -o json | jq -r --arg s "$saved" '
      sort_by(.name)[] | [.name, .id, .tenantId,
        ([if .id == $s or .name == $s then "saved" else empty end, if .isDefault then "default" else empty end] | join(" "))]
      | @tsv' ;;
  *) usage ;;
esac
