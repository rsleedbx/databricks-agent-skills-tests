#!/usr/bin/env bash
# lakebase-sql.sh: kept only because saved source contracts and other repos name this path; the code is lfc_source_contract/lakebase_sql.py (Python).
exec python3 -I "$(cd "$(dirname "$0")" && pwd)/lfc_source_contract/lakebase_sql.py" "$@"
