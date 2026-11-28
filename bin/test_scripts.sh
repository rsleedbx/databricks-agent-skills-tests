#!/usr/bin/env bash
# test_scripts.sh: a shim for bin/test_scripts.py (the suite is Python now). Kept only because .claude/skills/skill-token-budget/SKILL.md names this path: remove it when that names the .py.
exec python3 "$(dirname "$0")/test_scripts.py" "$@"
