#!/usr/bin/env bash
# slot-sql.sh [database] [--env]: SQL from stdin as the engine's admin in a slot database, through ssh. Kept for the source
# contracts saved before this was Python (their sql_cmd names this file); it runs lfc-source-contract's slot.py sql.
#   Needs SLOT_HOST and SLOT_NAME; SLOT_KEY, or SLOT_OWNER + SLOT_ADMIN_KEY [+ SLOT_ADMIN_USER] (see slot.py).
exec python3 -I "$(cd "$(dirname "$0")" && pwd)/../../lfc-source-contract/scripts/lfc_source_contract/slot.py" sql "$@"
