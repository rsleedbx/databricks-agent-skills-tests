#!/usr/bin/env bash
# source-env.sh: kept only because saved source contracts and other repos name this path; the code is lfc_source_contract/source_env.py (Python).
exec python3 -I "$(cd "$(dirname "$0")" && pwd)/lfc_source_contract/source_env.py" "$@"
