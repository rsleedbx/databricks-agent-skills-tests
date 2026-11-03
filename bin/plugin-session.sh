#!/usr/bin/env bash
# plugin-session.sh: the lfc-tests plugin's SessionStart hook. Lets the skills run from any folder when this repo is
#   installed as a Claude Code plugin rather than opened as a checkout: sets LFC_TESTS to this copy and puts its bin/
#   on PATH for every Bash command (through $CLAUDE_ENV_FILE), and tells the session (stdout becomes context) that the
#   skills' repo-relative paths (skills/<skill>/..., bin/..., results/...) are under $LFC_TESTS. Also names the local-test
#   tools that are missing (command -v only, installs nothing), so the session can point to check-tools.sh first, and
#   whether the user's test settings (lfc-test-settings) are saved: which keys, and when they were last checked (the file
#   only, no CLI call), so a test reads them instead of asking again.
set -uo pipefail
here=$(cd "$(dirname "$0")/.." && pwd)
if [ -n "${CLAUDE_ENV_FILE:-}" ]; then
  { printf 'export LFC_TESTS=%q\n' "$here"; printf 'export PATH=%q:"$PATH"\n' "$here/bin"; } >> "$CLAUDE_ENV_FILE"
fi
printf 'lfc-tests plugin: paths in its skills that start with skills/, bin/, results/ or docs/ are relative to $LFC_TESTS (%s); run them as "$LFC_TESTS/<path>", from any folder.\n' "$here"
tools="databricks uv jq yq duckdb podman"; [ "$(uname -s)" = Darwin ] && tools="$tools limactl"
gone=""; for t in $tools; do command -v "$t" > /dev/null 2>&1 || gone="$gone $t"; done
[ -z "$gone" ] || printf 'lfc-tests plugin: missing for local tests:%s. Before the first test, run bash "$LFC_TESTS/skills/cli-setup/scripts/check-tools.sh" local and give the user its install commands; install nothing without asking.\n' "$gone"
st="$here/skills/lfc-test-settings/scripts/settings.sh"; sf=${LFC_TESTS_SETTINGS:-$HOME/.lfc/tests/settings.env}
if [ -s "$sf" ]; then
  keys=$(grep -oE '^[A-Z][A-Z0-9_]*' "$sf" | grep -v '^SETTINGS_CHECKED$' | paste -sd' ' -)
  printf 'lfc-tests plugin: test settings saved in %s (%s; checked %s). Before asking the user which subscription, region, resource group, workspace or source host, read them: eval "$(bash "$LFC_TESTS/skills/lfc-test-settings/scripts/settings.sh" env)"; ask only for keys "settings.sh require <keys>" lists.\n' \
    "$sf" "$keys" "$(bash "$st" get SETTINGS_CHECKED 2>/dev/null || echo never)"
else
  printf 'lfc-tests plugin: no test settings saved yet. The first test that needs a cloud account, region, resource group or workspace runs bash "$LFC_TESTS/skills/lfc-test-settings/scripts/settings.sh" setup, asks every question once with a recommendation, and saves the answers (lfc-test-settings).\n'
fi
exit 0
