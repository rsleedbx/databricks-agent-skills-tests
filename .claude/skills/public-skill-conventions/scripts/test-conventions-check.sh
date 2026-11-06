#!/usr/bin/env bash
# test-conventions-check.sh: conventions-check.py on a throwaway git repo: a path into another skill's reference that
# isn't at the base fails, a missing own reference fails, a long description without ". Use when", a link inside a
# code block and a bare reference file name warn, and a clean skill has none of these.
set -euo pipefail
S="$(cd "$(dirname "$0")" && pwd)/conventions-check.py"
T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT
out=""
fail() { echo "FAIL: $1"; echo "$out"; exit 1; }
cd "$T" && git init -q && mkdir -p skills/databricks-alpha/references skills/databricks-beta/references
long=$(printf 'x%.0s' $(seq 1 210))
fm() { printf -- '---\nname: %s\ndescription: "%s"\n---\n\n# %s\n\n' "$1" "$2" "$1"; }
fm databricks-beta "Beta things. Use when beta." > skills/databricks-beta/SKILL.md
printf '# One\n' > skills/databricks-beta/references/one.md
fm databricks-alpha "Alpha things. Use when alpha." > skills/databricks-alpha/SKILL.md
printf -- '- [Steps](references/steps.md)\n- Owner: **databricks-beta** (`references/one.md`).\n' >> skills/databricks-alpha/SKILL.md
printf '# Steps\n\nSee [Steps](steps.md).\n' > skills/databricks-alpha/references/steps.md
git add -A && git -c user.name=t -c user.email=t@t commit -qm base > /dev/null 2>&1
base=$(git rev-parse HEAD)
out=$(python3 "$S" skills databricks-alpha --base "$base" 2>&1) || fail "clean skill failed"
for w in "Use when" "code block" "bare '" "doesn't exist" "isn't at"; do
  if echo "$out" | grep -q "$w"; then fail "clean skill reported: $w"; fi
done
# break each rule
printf '# Two\n' > skills/databricks-beta/references/two.md
fm databricks-alpha "$long" > skills/databricks-alpha/SKILL.md
printf -- '- [Steps](references/steps.md)\n- Rotation: **databricks-beta** (`references/two.md`).\n- Gone: references/gone.md\n' >> skills/databricks-alpha/SKILL.md
printf '# Steps\n\n```yaml\nname: x  # see steps.md#names\n// [Steps](steps.md)\n```\n' > skills/databricks-alpha/references/steps.md
out=$(python3 "$S" skills databricks-alpha --base "$base" 2>&1) && fail "expected exit 1"
echo "$out" | grep -q "databricks-beta/references/two.md isn't at" || fail "path into another skill not at base"
echo "$out" | grep -q "databricks-alpha/references/gone.md doesn't exist" || fail "missing own reference"
echo "$out" | grep -q "no '. Use when'" || fail "long description"
echo "$out" | grep -q "link inside a code block" || fail "link in code block"
echo "$out" | grep -q "steps.md:4: bare 'steps.md'" || fail "bare reference name"
echo "test-conventions-check.sh: ok"
