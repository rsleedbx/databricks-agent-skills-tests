#!/usr/bin/env bash
# test-moved-lines.sh: moved-lines.py on a throwaway git repo: a line moved to a sibling skill is kept, a dropped
# `term` is reported, and the same word in an unrelated line of another skill doesn't count as kept.
set -euo pipefail
S="$(cd "$(dirname "$0")" && pwd)/moved-lines.py"
T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT
cd "$T" && git init -q && mkdir -p skills/a/references skills/b skills/c
printf '# A\n\n- The pipeline sets `channel: PREVIEW` for the preview runtime.\n' > skills/a/SKILL.md
printf '| Symptom | Fix |\n|---|---|\n| OAuth connection stuck in `PENDING` | Finish the browser sign-in |\n' > skills/a/references/issues.md
printf '# B\n' > skills/b/SKILL.md
printf '# C\n\n- A job run is `PENDING` while its cluster starts.\n' > skills/c/SKILL.md
git add -A && git -c core.hooksPath=/dev/null -c user.name=t -c user.email=t@t commit -qm base
base=$(git rev-parse HEAD)
# move the channel line to skill b, drop the PENDING row
printf '# B\n\n- The pipeline sets `channel: PREVIEW` for the preview runtime.\n' > skills/b/SKILL.md
printf '# A\n' > skills/a/SKILL.md
rm skills/a/references/issues.md
out=$(python3 "$S" skills a --base "$base" 2>&1) && { echo "FAIL: expected exit 1"; echo "$out"; exit 1; }
echo "$out" | grep -q '^PENDING' || { echo "FAIL: PENDING not reported"; echo "$out"; exit 1; }
if echo "$out" | grep -q 'channel: PREVIEW'; then echo "FAIL: moved line reported"; echo "$out"; exit 1; fi
# put the row back in skill b: now nothing is gone
printf '| OAuth connection stuck in `PENDING` | Finish the browser sign-in |\n' >> skills/b/SKILL.md
python3 "$S" skills a --base "$base" > /dev/null 2>&1 || { echo "FAIL: expected exit 0 once the row moved"; exit 1; }
echo ok
