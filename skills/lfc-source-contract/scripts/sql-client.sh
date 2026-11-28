#!/usr/bin/env bash
# sql-client.sh: kept only because saved source contracts and other repos name this path; the code is lfc_source_contract/sql_client.py (Python).
exec python3 -I "$(cd "$(dirname "$0")" && pwd)/lfc_source_contract/sql_client.py" "$@"
