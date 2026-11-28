# pw.sh: sourced by the lfc-source-* and lfc-connection-sync scripts (not run). A shim, kept on purpose: those scripts
# source this path and call pw and pw_check. The logic is in pw.py: a password file, or keyring:<name> (the OS keyring).
#   pw <ref>        prints the password
#   pw_check <ref> says whether it can be read, without printing it
_PW_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
pw() { case "$1" in keyring:*) uv run -q --script "$_PW_DIR/pw.py" read "$1" ;; *) python3 "$_PW_DIR/pw.py" read "$1" ;; esac; }
pw_check() { case "$1" in keyring:*) uv run -q --script "$_PW_DIR/pw.py" check "$1" ;; *) python3 "$_PW_DIR/pw.py" check "$1" ;; esac; }
