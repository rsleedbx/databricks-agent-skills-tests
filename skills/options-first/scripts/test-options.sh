#!/usr/bin/env bash
# test-options.sh: checks options.py on sample files: ranking (status, then effort, then breaks), the pick, check
#   failures and index. Prints one PASS/FAIL line per check; exit 1 on any FAIL.
set -uo pipefail
S=$(cd "$(dirname "$0")" && pwd); T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT; fail=0
ok() { if eval "$2"; then echo "PASS $1"; else echo "FAIL $1"; fail=1; fi; }
O=(uv run -q "$S/options.py")
mkdir -p "$T/a" "$T/b"
cat > "$T/a/options.yaml" <<'EOF'
topic: Sample topic
question: Which way?
options:
  - {name: Custom code, setup: {effort: 5, what: lots}, operate: {effort: 4, what: you own it}, status: tested}
  - {name: Built-in setting, setup: {effort: 1, what: one flag}, operate: {effort: 1, what: none}, status: not-supported}
  - {name: Small pipeline, setup: {effort: 2, what: one pipeline}, operate: {effort: 2, what: scans}, breaks: [refresh], status: tested}
  - {name: Smaller untested, setup: {effort: 1, what: tiny}, operate: {effort: 1, what: none}, status: untested}
EOF
printf 'topic: Broken\noptions:\n  - {name: x, setup: {effort: 9}, operate: {effort: 1}, status: maybe}\n' > "$T/b/options.yaml"

out=$("${O[@]}" render "$T/a/options.yaml")
row() { grep -n "^| [0-9] | $1 |" <<<"$out" | cut -d: -f1; }   # the line of an option's table row
ok "render: tested and simpler first" '[ "$(row "Small pipeline")" -lt "$(row "Custom code")" ]'
ok "render: untested after tested" '[ "$(row "Smaller untested")" -gt "$(row "Custom code")" ]'
ok "render: not-supported last" '[[ "$(grep "^| 4 |" <<<"$out")" == *"Built-in setting"* ]]'
ok "render: pick is the simplest tested" '[[ "$out" == *"Pick: **Small pipeline** (tested)."* ]]'
"${O[@]}" check "$T/a/options.yaml" 2>/dev/null; ok "check: valid file passes" '[ $? = 0 ]'
err=$("${O[@]}" check "$T/b/options.yaml" 2>&1); rc=$?
ok "check: bad file fails" '[ $rc = 1 ] && [[ "$err" == *"missing question"* && "$err" == *"effort must be 1 to 5"* && "$err" == *"status must be"* ]]'
idx=$("${O[@]}" index "$T" 2>/dev/null)
ok "index: lists the valid topic and its pick" '[[ "$idx" == *"| Sample topic | 4 | Small pipeline | a/options.yaml |"* ]]'
exit $fail
