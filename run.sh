#!/usr/bin/env bash
# Skill test runner -- runs headless Claude sessions against a prompt file.
#
# Usage:
#   ./run.sh [skills-repo] [prompts-file]
#
# Arguments:
#   skills-repo   Path to the databricks-agent-skills checkout whose skills are tested
#                 (default: $SKILLS_REPO, else ../databricks-agent-skills next to this repo)
#   prompts-file  Prompts file, relative to this repo or absolute (default: prompts/lakeflow-connect.txt)
#
# Environment variables (set via env.sh, which sources env.local.sh):
#   PIPELINE_WS   Databricks CLI profile passed to Claude as DATABRICKS_CONFIG_PROFILE (default: DEFAULT)
#   CLAUDE_ALLOWED_TOOLS  Comma-separated tool allowlist for deploy tests, passed as one --allowedTools
#                 argument so rules with spaces work, e.g. 'Bash(databricks:*),Bash(rm -f conn.json)'
#   CLAUDE_ARGS   Other extra flags for claude -p; split on spaces, so no values with spaces (default: none)
#   WORK_DIR      Directory each session runs in (default: a fresh /tmp/<prompts-name>-work). Keep it
#                 outside the skills repo so skills load from the plugin cache, not the checkout.
#
# Prompts file: one `tag|prompt` per line. A `tag+|text` line is a follow-up turn: it resumes
# the session started by `tag` and appends to the same output file (use it to answer the
# questions a skill asks before creating resources). `${VAR}` in a prompt is replaced with the
# environment variable VAR (see env.example.sh); an unset variable stops the run.
#
# Examples:
#   source env.sh
#   ./run.sh
#   ./run.sh ~/github/databricks-agent-skills prompts/lakeflow-connect.txt
#   bin/run_batch.sh prompts/lfc-matrix/qbc-*.txt

PIPELINE_WS="${PIPELINE_WS:-DEFAULT}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="${1:-${SKILLS_REPO:-$SCRIPT_DIR/../databricks-agent-skills}}"
if [ ! -d "$REPO_DIR/skills" ]; then
  echo "ERROR: no skills/ in $REPO_DIR (pass the skills repo path or set SKILLS_REPO)"
  exit 1
fi
PROMPTS_ARG="${2:-prompts/lakeflow-connect.txt}"
WORK_DIR="${WORK_DIR:-${RESULTS_ROOT:-/tmp}/$(basename "${2:-lakeflow-connect}" .txt)-work}"
mkdir -p "$WORK_DIR"
WORK_DIR="$(cd "$WORK_DIR" && pwd)"

# Resolve prompts file: if relative, look under SCRIPT_DIR
if [ "${PROMPTS_ARG:0:1}" = "/" ]; then
  PROMPTS="$PROMPTS_ARG"
else
  PROMPTS="$SCRIPT_DIR/$PROMPTS_ARG"
fi

if [ ! -f "$PROMPTS" ]; then
  echo "ERROR: Prompts file not found: $PROMPTS"
  exit 1
fi

# Derive results dir from the prompts file name (e.g. lakeflow-connect -> /tmp/lakeflow-connect-results)
SKILL_NAME=$(basename "$PROMPTS" .txt)
RESULTS_DIR="${RESULTS_ROOT:-/tmp}/${SKILL_NAME}-results"
mkdir -p "$RESULTS_DIR"

echo "=== Skill Test Runner ==="
echo "Repo:        $(cd "$REPO_DIR" && pwd)"
echo "Prompts:     $PROMPTS"
echo "Results:     $RESULTS_DIR"
echo "Pipeline WS: $PIPELINE_WS  (DATABRICKS_CONFIG_PROFILE for Claude)"
echo "Work dir:    $WORK_DIR"
# An expired workspace login: mint a token from the account login for the same account (account-token-fallback),
# so the sessions' --profile "$PIPELINE_WS" keeps working. Nothing changes while the profile's own login works.
WS_CFG=$(bash "$SCRIPT_DIR/skills/account-token-fallback/scripts/ws-token.sh" "$PIPELINE_WS") || {
  echo "ERROR: $PIPELINE_WS can't log in (see above): databricks auth login --profile $PIPELINE_WS"; exit 1; }
if [ -n "$WS_CFG" ]; then export DATABRICKS_CONFIG_FILE="$WS_CFG"; echo "Auth:        token minted from the account login (about an hour)"; fi
[ -n "$CLAUDE_ALLOWED_TOOLS" ] && echo "Allowed:     $CLAUDE_ALLOWED_TOOLS"
[ -n "$CLAUDE_ARGS" ] && echo "Claude args: $CLAUDE_ARGS"
echo ""

# --- Plugin cache: patch with local skill files ---

# Extract skill name from the prompts directory or let it find all lakeflow-connect-like paths
CACHE_DIRS=$(find ~/.claude/plugins/cache -path "*/databricks/*/skills/databricks-*" -type d 2>/dev/null)

if [ -z "$CACHE_DIRS" ]; then
  echo "Plugin cache missing. Rebuilding with a quick claude call..."
  (cd "$REPO_DIR" && DATABRICKS_CONFIG_PROFILE="$PIPELINE_WS" claude -p "hello" --output-format text > /dev/null 2>&1) < /dev/null
  CACHE_DIRS=$(find ~/.claude/plugins/cache -path "*/databricks/*/skills/databricks-*" -type d 2>/dev/null)
fi

if [ -n "$CACHE_DIRS" ]; then
  CACHE_ROOT=$(echo "$CACHE_DIRS" | head -1 | sed 's|/skills/databricks-.*|/skills|')
  echo "Plugin cache skills dir: $CACHE_ROOT"

  # Copy all local skill directories into the cache
  if [ -d "$REPO_DIR/skills" ]; then
    for skill_dir in "$REPO_DIR"/skills/databricks-*/; do
      skill_basename=$(basename "$skill_dir")
      if [ -d "$CACHE_ROOT/$skill_basename" ]; then
        cp "$skill_dir/SKILL.md" "$CACHE_ROOT/$skill_basename/" 2>/dev/null
        if [ -d "$skill_dir/references" ]; then
          mkdir -p "$CACHE_ROOT/$skill_basename/references"
          cp "$skill_dir/references/"*.md "$CACHE_ROOT/$skill_basename/references/" 2>/dev/null
        fi
      fi
    done
    echo "Plugin cache updated with local skill files."
  fi
else
  echo "WARNING: Could not find or create plugin cache. Tests may use stale skill content."
fi
echo ""

# --- Run prompts ---

pass=0
fail=0
skipped=0
# A tag (with its follow-up turns) runs only when what it depends on changed since it last passed on this machine:
# its prompts as filled, the workspace profile and the skills repo's skills/ (bin/test_skip, runner run.sh; local,
# never committed). A pass: every turn exited 0 and ended in a result event that isn't an error; whether the answer
# met the test's expectations is summarize.py's call, so check those before trusting a skip. A skipped tag leaves
# <tag>.skipped (no <tag>.json) in the results. TEST_SKIP_AGAIN=1 runs every tag.
GATE="$SCRIPT_DIR/bin/test_skip/scripts/test_skip.py"
G=$(mktemp -d)
gate_args() {   # <tag> <prompt as filled>: the arguments that fingerprint a tag
  printf '%s\n' "$(basename "$PROMPTS" .txt):$1" --value "$PIPELINE_WS" --value "$2" \
    --value "$(grep "^$1+|" "$PROMPTS")" "$REPO_DIR/skills"
}

while IFS='|' read -r tag prompt; do
  [ -z "$tag" ] && continue
  if [ -e "$G/skip-${tag%+}" ] && [ "${tag%+}" != "$tag" ]; then
    echo "[$tag] skipped with ${tag%+}"; continue
  fi
  # Fill ${VAR} placeholders from the environment; stop on an unset one
  if ! prompt=$(printf '%s' "$prompt" | perl -pe 's/\$\{(\w+)\}/defined $ENV{$1} ? $ENV{$1} : die "unset variable $1\n"/ge'); then
    echo "[$tag] SKIPPED: prompt uses an unset variable (see env.example.sh)"
    fail=$((fail + 1)); : > "$G/fail-${tag%+}"
    continue
  fi
  resume_args=()
  if [ "${tag%+}" != "$tag" ]; then
    # Follow-up turn: resume the base tag's session and append to its output
    tag="${tag%+}"
    outfile="$RESULTS_DIR/${tag}.json"
    session_id=$(grep -m1 -o '"session_id":"[^"]*"' "$outfile" 2>/dev/null | cut -d'"' -f4)
    if [ -z "$session_id" ]; then
      echo "[$tag+] SKIPPED: no session to resume in $outfile"
      fail=$((fail + 1)); : > "$G/fail-$tag"
      continue
    fi
    resume_args=(--resume "$session_id")
    echo "[$tag+] Follow-up: $prompt"
  else
    gargs=(); while IFS= read -r a; do gargs+=("$a"); done < <(gate_args "$tag" "$prompt")
    if last=$(python3 "$GATE" skip run.sh "${gargs[@]}"); then
      echo "[$tag] SKIPPED: $last"; : > "$G/skip-$tag"; skipped=$((skipped + 1))
      rm -f "$RESULTS_DIR/$tag.json" "$RESULTS_DIR/$tag.err"   # an older run's output must not pass for this one's
      echo "$last" > "$RESULTS_DIR/$tag.skipped"; continue
    fi
    printf '%s\n' "${gargs[@]}" > "$G/args-$tag"; echo "$tag" >> "$G/ran"
    outfile="$RESULTS_DIR/${tag}.json"
    : > "$outfile"
    : > "$RESULTS_DIR/${tag}.err"
    rm -f "$RESULTS_DIR/${tag}.skipped"
    echo "[$tag] Running: $prompt"
  fi
  errfile="$RESULTS_DIR/${tag}.err"

  (
    cd "$WORK_DIR"
    # shellcheck disable=SC2086  # CLAUDE_ARGS is intentionally word-split
    DATABRICKS_CONFIG_PROFILE="$PIPELINE_WS" claude -p "$prompt" \
      "${resume_args[@]}" \
      ${CLAUDE_ALLOWED_TOOLS:+--allowedTools "$CLAUDE_ALLOWED_TOOLS"} \
      --verbose \
      --output-format stream-json \
      $CLAUDE_ARGS \
      >> "$outfile" 2>>"$errfile"
  ) < /dev/null

  exit_code=$?
  lines=$(wc -l < "$outfile" 2>/dev/null || echo 0)

  # a pass for the skip ledger: exit 0 and the session's last result event a success (not an error result)
  tail -1 "$outfile" | grep -q '"type":"result"' && ! tail -1 "$outfile" | grep -q '"is_error":true' \
    || : > "$G/fail-$tag"
  if [ $exit_code -ne 0 ]; then
    echo "[$tag] FAILED (exit $exit_code)"
    [ -s "$errfile" ] && echo "[$tag] stderr: $(head -5 "$errfile")"
    fail=$((fail + 1)); : > "$G/fail-$tag"
  else
    echo "[$tag] done -> $outfile ($lines lines)"
    pass=$((pass + 1))
  fi
  echo ""
done < "$PROMPTS"

# record each tag that ran: a pass when it and all its follow-ups succeeded
if [ -s "$G/ran" ]; then
  while IFS= read -r t; do
    gargs=(); while IFS= read -r a; do gargs+=("$a"); done < "$G/args-$t"
    key=${gargs[0]}; rest=("${gargs[@]:1}")
    python3 "$GATE" record run.sh "$key" "$([ -e "$G/fail-$t" ] && echo fail || echo pass)" "${rest[@]}"
  done < "$G/ran"
fi
rm -rf "${G:?}"
echo "=== $pass passed, $fail failed, $skipped skipped (unchanged since they passed; TEST_SKIP_AGAIN=1 runs them) ==="
echo "Summarize: python3 $SCRIPT_DIR/summarize.py $RESULTS_DIR"
