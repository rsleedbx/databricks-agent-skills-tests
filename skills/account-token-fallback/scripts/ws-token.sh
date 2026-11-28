#!/usr/bin/env bash
# ws-token.sh: kept for the tools that run this path; it runs ws_token.py (same arguments, output and exit status).
exec python3 "$(cd "$(dirname "$0")" && pwd)/ws_token.py" "$@"
