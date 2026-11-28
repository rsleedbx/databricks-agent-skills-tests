#!/usr/bin/env bash
# lfc-events.sh: a shim for bin/lfc_events.py, kept only because a tool outside this repo runs this exact path (its --json).
# Remove it once that caller runs the .py.
exec python3 "$(dirname "$0")/lfc_events.py" "$@"
