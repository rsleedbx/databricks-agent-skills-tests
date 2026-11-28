#!/usr/bin/env bash
# Test environment -- source this before running tests:
#
#   source env.sh                                   # uses env.local.sh
#   LFC_ENV=<name> source env.sh                    # uses env.<name>.local.sh
#
# Put each workspace's values in its own env.<name>.local.sh (gitignored; copy env.example.sh).
# Results and scratch dirs go under /tmp/lfc-tests/<LFC_ENV>/ so workspaces don't overwrite each other.

_ENV_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -n "$LFC_ENV" ]; then
  if [ -f "$_ENV_DIR/env.$LFC_ENV.local.sh" ]; then source "$_ENV_DIR/env.$LFC_ENV.local.sh"
  else echo "ERROR: $_ENV_DIR/env.$LFC_ENV.local.sh not found (copy env.example.sh)"; return 1 2>/dev/null || exit 1; fi
elif [ -f "$_ENV_DIR/env.local.sh" ]; then
  source "$_ENV_DIR/env.local.sh"
fi

export LFC_ENV="${LFC_ENV:-default}"
export SKILLS_REPO="${SKILLS_REPO:-$_ENV_DIR/../databricks-agent-skills}"
export OMNIGENT_WS="${OMNIGENT_WS:-DEFAULT}"   # profile for the workspace file bridge (sync.py)
export PIPELINE_WS="${PIPELINE_WS:-DEFAULT}"   # profile passed to Claude as DATABRICKS_CONFIG_PROFILE
# What the matrix prompts ask for compute (MATRIX.md, "Workspace requirements"): true (the default: the test firewall
# allows only the serverless ranges), false, or unset (take the product's default; record what it ran on)
export LFC_SERVERLESS="${LFC_SERVERLESS:-true}"
case $LFC_SERVERLESS in
  true|false) export LFC_COMPUTE_ASK="Set serverless: $LFC_SERVERLESS on every pipeline and gateway." ;;
  unset)      export LFC_COMPUTE_ASK="Leave serverless unset on every pipeline and gateway, to take the default." ;;
  *) echo "ERROR: LFC_SERVERLESS must be true, false or unset"; return 1 2>/dev/null || exit 1 ;;
esac
# a run with another setting gets its own results dir, so the two can be compared
_SUFFIX=; [ "$LFC_SERVERLESS" != true ] && _SUFFIX="-serverless-$LFC_SERVERLESS"
export RESULTS_ROOT="${RESULTS_ROOT:-/tmp/lfc-tests/$LFC_ENV$_SUFFIX}"

echo "LFC_ENV=$LFC_ENV"
echo "SKILLS_REPO=$SKILLS_REPO"
echo "PIPELINE_WS=$PIPELINE_WS"
echo "RESULTS_ROOT=$RESULTS_ROOT"
echo "LFC_SERVERLESS=$LFC_SERVERLESS"
