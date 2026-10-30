#!/usr/bin/env bash
# check-tools.sh [all|base|local|aws|azure|gcp|clients]: which of the lfc-source-* skills' tools are installed, their
# versions, and the install command for the missing ones (Homebrew on macOS; see lfc-source-setup "Tools" for Linux).
# Exits 1 if anything asked for is missing. Default: base.
set -u
want=${1:-base}
missing=()
check() {                                  # check <group> <command> <brew install args> [version args]
  local group=$1 cmd=$2 pkg=$3 ver=${4:---version}
  [ "$want" = all ] || [ "$want" = "$group" ] || { [ "$want" = local ] && [ "$group" = base ]; } || return 0
  if command -v "$cmd" > /dev/null; then printf "%-11s %-8s %s\n" "$cmd" ok "$($cmd $ver 2>&1 | grep -v -i warning | grep -m1 -E '[0-9]+\.[0-9]+' | cut -c1-70)"
  else printf "%-11s %-8s %s\n" "$cmd" MISSING "brew install $pkg"; missing+=("$pkg"); fi
}
check base    databricks "databricks/tap/databricks" -v
check base    uv         uv
check base    jq         jq
check base    yq         yq                       # the Go yq (mikefarah), not the Python wrapper
check base    duckdb     duckdb
check local   podman     podman
check local   limactl    lima
check clients psql       libpq                    # keg-only: add $(brew --prefix libpq)/bin to PATH
check clients mysql      mysql-client             # keg-only: add $(brew --prefix mysql-client)/bin to PATH
check clients sqlcmd     sqlcmd "-?"              # Microsoft's Go sqlcmd (the older ODBC one works too)
check clients mongosh    mongosh
check aws     aws        awscli
check azure   az         azure-cli
check gcp     gcloud     "--cask gcloud-cli"
if [ ${#missing[@]} -gt 0 ]; then
  echo; echo "install: brew install ${missing[*]}" | sed 's/ --cask gcloud-cli//'
  printf '%s\n' ${missing[@]+"${missing[@]}"} | grep -q cask && echo "         brew install --cask gcloud-cli"
  exit 1
fi
