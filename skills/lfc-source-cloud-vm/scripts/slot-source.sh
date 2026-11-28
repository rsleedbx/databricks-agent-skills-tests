#!/usr/bin/env bash
# slot-source.sh [--connection <name>] [--facts <file>] [--out <file>] < up.json: the source contract for a database from
# lfc-slot up (its JSON, the last line on stdin). Runs lfc-source-contract's slot.py source; see it for the settings
# (SLOT_HOST, SLOT_KEY, SLOT_OWNER + SLOT_ADMIN_KEY). Kept for callers that name this file.
exec python3 -I "$(cd "$(dirname "$0")" && pwd)/../../lfc-source-contract/scripts/lfc_source_contract/slot.py" source "$@"
