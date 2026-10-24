#!/usr/bin/env bash
# ws-token.sh <profile> [--host URL [--account-id ID]] [--cfg FILE] [--force]: keep a workspace profile usable when its
#   OAuth login has expired, with a token from another login you already have: the Azure CLI (Azure), or the account
#   login for the same account.
#   --host: a workspace you have no profile for yet (one just created in an account you're logged in to). <profile> is
#     then only the name the config file gives it. With --account-id the account profiles for that account are tried;
#     without it, every account profile on the workspace's cloud.
#   - The profile's own login works: prints nothing, exits 0 (keep using ~/.databrickscfg).
#   - It doesn't: on Azure, tries an Entra ID token from the Azure CLI (any workspace in its tenant); then an account
#     profile (from `databricks auth profiles`) with the same account_id, minting a short-lived token from it (databricks auth token
#     --host <accounts host> --account-id <id>). Checks the token against the workspace, writes a
#     one-profile config FILE (mode 600; default $TMPDIR/ws-token-<profile>.cfg) and prints FILE. Use it as
#       CFG=$(ws-token.sh <profile>) && [ -n "$CFG" ] && export DATABRICKS_CONFIG_FILE=$CFG
#     so `--profile <profile>` keeps working. The token lasts about an hour: rerun when calls start failing.
#   - Exit 1 with the reason when neither works (then: databricks auth login --profile <profile>).
#   --force mints even when the profile's login works (to test the fallback). The token is never printed.
set -uo pipefail
P=${1:?ws-token.sh <profile> [--host URL [--account-id ID]] [--cfg FILE] [--force]}; shift; CFG=; FORCE=; HOST_ARG=; AID_ARG=
while [ $# -gt 0 ]; do case $1 in --cfg) CFG=$2; shift 2;; --force) FORCE=1; shift;; --host) HOST_ARG=${2%/}; shift 2;;
  --account-id) AID_ARG=$2; shift 2;; *) echo "unknown $1" >&2; exit 2;; esac; done
CFG=${CFG:-${TMPDIR:-/tmp}/ws-token-$P.cfg}
say() { echo "ws-token: $*" >&2; }
# run with the real profiles: DATABRICKS_CONFIG_FILE_SOURCE if set, else the CLI's default config (not a token file this
# script wrote earlier). Profile fields come from `databricks auth profiles`; this script never reads the config file.
real() { if [ -n "${DATABRICKS_CONFIG_FILE_SOURCE:-}" ]; then DATABRICKS_CONFIG_FILE=$DATABRICKS_CONFIG_FILE_SOURCE "$@"; else env -u DATABRICKS_CONFIG_FILE "$@"; fi; }
PROFILES=$(real timeout 60 databricks auth profiles --skip-validate -o json 2> /dev/null)
# field $2 of profile $1
field() { printf '%s' "$PROFILES" | yq -p json -o tsv ".profiles[] | select(.name == \"$1\") | .$2 // \"\""; }

HOST=$(field "$P" host); AID=$(field "$P" account_id)
if [ -n "$HOST_ARG" ] && [ "${HOST%/}" != "$HOST_ARG" ]; then HOST=$HOST_ARG; AID=$AID_ARG   # no profile of its own
elif [ -z "$FORCE" ] && real timeout 60 databricks current-user me --profile "$P" -o json > /dev/null 2>&1; then
  say "$P: its own login works"; exit 0; fi
[ -n "$HOST" ] || { say "$P: no such profile (databricks auth profiles); for a workspace without one, pass --host"; exit 1; }
# write the one-profile config for token $1 and check it; prints the file and exits 0 when the workspace accepts it
try() { ( umask 077; printf '[%s]\nhost  = %s\ntoken = %s\n' "$P" "$HOST" "$1" > "$CFG" )
  if DATABRICKS_CONFIG_FILE=$CFG timeout 60 databricks current-user me --profile "$P" -o json > /dev/null 2> "$CFG.err"; then
    rm -f "$CFG.err"; say "$P: using a token from $2 (about an hour)"; echo "$CFG"; exit 0; fi
  ERR=$(head -c 400 "$CFG.err"); rm -f "$CFG" "$CFG.err"; return 1; }
# Azure: an Entra ID token from the Azure CLI for the Azure Databricks resource works for every workspace in its tenant,
# no account profile needed. A workspace in another tenant names it in the error; retry there if az knows that tenant.
case $HOST in *.azuredatabricks.net*)
  R=2ff814a6-3304-4ab8-85cb-cd0e6f879c1d
  tok=$(timeout 60 az account get-access-token --resource $R --query accessToken -o tsv 2> /dev/null)
  if [ -n "$tok" ]; then
    try "$tok" "the Azure CLI login"
    ten=$(printf '%s' "$ERR" | grep -oE 'Expected iss claim to be: https://sts.windows.net/[0-9a-f-]+' | grep -oE '[0-9a-f-]{36}')
    if [ -n "$ten" ]; then
      tok=$(timeout 60 az account get-access-token --resource $R --tenant "$ten" --query accessToken -o tsv 2> /dev/null)
      [ -n "$tok" ] && try "$tok" "the Azure CLI login, tenant ${ten:0:8}..."
      say "$P: its workspace is in Entra tenant ${ten:0:8}..., which the Azure CLI isn't logged in to: az login --tenant $ten"
    else say "$P: the Azure CLI token isn't accepted"; fi
  fi; unset tok;;
esac
# account profiles: an accounts.* host and the same account_id; with --host and no account id, every account profile
# on the workspace's cloud (accounts.cloud.databricks.com, accounts.azuredatabricks.net, accounts.gcp.databricks.com)
case $HOST in *.azuredatabricks.net*) AHOST=https://accounts.azuredatabricks.net;; *.gcp.databricks.com*) AHOST=https://accounts.gcp.databricks.com;;
  *) AHOST=https://accounts.cloud.databricks.com;; esac
if [ -n "$AID" ]; then SEL=".account_id == \"$AID\" and ((.host // \"\") | test(\"^https://accounts[.]\"))"
elif [ -n "$HOST_ARG" ]; then SEL="(.host // \"\") == \"$AHOST\" and (.account_id // \"\") != \"\""
else say "$P: no account_id in its profile, so no account login to fall back on: databricks auth login --profile $P"; exit 1; fi
for A in $(printf '%s' "$PROFILES" | yq -p json -o tsv ".profiles[] | select($SEL) | .name"); do
  AH=$(field "$A" host); AA=$(field "$A" account_id)
  tok=$(real timeout 60 databricks auth token --host "$AH" --account-id "$AA" --profile "$A" -o json 2> /dev/null \
        | yq -p json -o tsv '.access_token // ""')
  [ -n "$tok" ] || { say "account profile $A: no token (login expired?): databricks auth login --profile $A"; continue; }
  try "$tok" "account profile $A"; unset tok
  say "account profile $A: its token isn't accepted by $HOST (some workspaces, such as field sandboxes, take only their own login)"
done
if [ -n "$HOST_ARG" ]; then say "$P: no account profile${AID:+ for account ${AID:0:8}...} could mint a token $HOST accepts: log in to its account (add-account.sh <account-id> <cloud>), or to the workspace: databricks auth login --host $HOST --profile $P"
elif [ -n "$AID" ]; then say "$P: its login has expired and no account profile for account ${AID:0:8}... could mint a token: databricks auth login --profile $P"
fi
exit 1
