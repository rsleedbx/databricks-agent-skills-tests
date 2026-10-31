#!/usr/bin/env bash
# test-moved-lines.sh: moved-lines.py on a throwaway git repo: a line moved to a sibling skill is kept, a dropped
# `term` is reported, the same word in an unrelated line of another skill doesn't count as kept, --lines lists a
# dropped prose line, and --links lists other skills' lines naming the skill but not a longer skill name.
set -euo pipefail
S="$(cd "$(dirname "$0")" && pwd)/moved-lines.py"
T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT
out=""
fail() { echo "FAIL: $1"; echo "$out"; exit 1; }
cd "$T" && git init -q && mkdir -p skills/alpha/references skills/beta skills/gamma
printf '# Alpha\n\n- The pipeline sets `channel: PREVIEW` for the preview runtime.\n- Keep the gateway running while the source keeps its change log.\n' > skills/alpha/SKILL.md
printf '| Symptom | Fix |\n|---|---|\n| OAuth connection stuck in `PENDING` | Finish the browser sign-in |\n' > skills/alpha/references/issues.md
printf '# Beta\n' > skills/beta/SKILL.md
printf '# Gamma\n\n- A job run is `PENDING` while its cluster starts.\n- See alpha for pull connectors.\n- See alpha-extra for something else.\n' > skills/gamma/SKILL.md
git add -A && git -c user.name=t -c user.email=t@t commit -qm base > /dev/null 2>&1
base=$(git rev-parse HEAD)
# move the channel line to beta; drop the PENDING row and the gateway sentence
printf '# Beta\n\n- The pipeline sets `channel: PREVIEW` for the preview runtime.\n' > skills/beta/SKILL.md
printf '# Alpha\n\nSee beta for the runtime.\n' > skills/alpha/SKILL.md
rm skills/alpha/references/issues.md
out=$(python3 "$S" skills alpha --base "$base" 2>&1) && fail "expected exit 1"
echo "$out" | grep -q '^PENDING' || fail "PENDING not reported"
if echo "$out" | grep -q 'channel: PREVIEW'; then fail "moved term reported"; fi
out=$(python3 "$S" skills alpha --base "$base" --lines 2>&1) || true
echo "$out" | sed -n '/^lines with no close match/,$p' | grep -q 'Keep the gateway running' || fail "--lines: dropped line not listed"
if echo "$out" | sed -n '/^lines with no close match/,$p' | grep -q 'sets .channel'; then fail "--lines: moved line listed"; fi
out=$(python3 "$S" skills alpha --base "$base" --links 2>&1) || true
echo "$out" | grep -q 'gamma/SKILL.md:4' || fail "--links: line naming alpha not listed"
if echo "$out" | grep -q 'alpha-extra'; then fail "--links: longer skill name listed"; fi
if echo "$out" | sed -n '/^other skills/,$p' | grep -q '^  alpha/'; then fail "--links: the skill's own file listed"; fi
# put the row back in beta: now no term is gone
printf '| OAuth connection stuck in `PENDING` | Finish the browser sign-in |\n' >> skills/beta/SKILL.md
out=$(python3 "$S" skills alpha --base "$base" 2>&1) || fail "expected exit 0 once the row moved"
echo ok
