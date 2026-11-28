#!/usr/bin/env bash
# lfc-runtime.sh: a shim for bin/lfc_runtime.py, kept only because the skills' shell scripts run this exact path
# (lfc-schema-change-test: lib.sh, parallel.sh, integrated-scd2-ct.sh, sqlserver-add-column.sh). Remove it when they call the .py.
exec python3 "$(dirname "$0")/lfc_runtime.py" "$@"
