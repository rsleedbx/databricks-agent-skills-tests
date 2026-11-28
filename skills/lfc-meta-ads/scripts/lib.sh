# shellcheck shell=bash disable=SC2034
# lib.sh: shared helpers for the Meta Ads scripts. Source it after setting PROFILE (and WH for SQL).
#   dbx <args>     databricks CLI with --profile, retried on connection failures (reads and idempotent calls only)
#   sql <stmt>     rows as TSV from a SQL warehouse ($WH; picked by pick_wh when empty); exit 1 with the error on stderr
#   link <what> <path>   prints "<what>: <host><path>"
#   need <cmd>...  exit 2 unless every command is on PATH
export PATH=/opt/homebrew/bin:$PATH

need() { for c in "$@"; do command -v "$c" >/dev/null || { echo "needs $c on PATH" >&2; exit 2; }; done; }
need databricks jq

dbx() {
  local i err
  err=$(mktemp)
  for i in 1 2 3 4; do
    if databricks "$@" --profile "$PROFILE" 2>"$err"; then rm -f "$err"; return 0; fi
    grep -qiE 'connection (reset|refused)|timed? ?out|EOF|TLS handshake|no such host|503|502' "$err" || break
    sleep $((i * 5))
  done
  cat "$err" >&2; rm -f "$err"; return 1
}

host() { [ -n "${HOST:-}" ] || HOST=$(dbx auth describe -o json | jq -r '.details.host // .host' | sed 's:/*$::'); echo "$HOST"; }
link() { echo "$1: $(host)$2"; }

pick_wh() {   # the first RUNNING serverless warehouse, else the first serverless one, else any
  [ -n "${WH:-}" ] && return 0
  WH=$(dbx warehouses list -o json | jq -r '([.[] | select(.state == "RUNNING" and .enable_serverless_compute)]
        + [.[] | select(.enable_serverless_compute)] + .)[0].id // ""')
  [ -n "$WH" ] || { echo "no SQL warehouse in this workspace; pass --warehouse" >&2; exit 2; }
}

sql() {
  pick_wh
  local out
  out=$(dbx api post /api/2.0/sql/statements -o json --json "$(jq -n --arg w "$WH" --arg s "$1" \
    '{warehouse_id: $w, statement: $s, wait_timeout: "50s", on_wait_timeout: "CONTINUE", format: "JSON_ARRAY"}')") || return 1
  local id st
  id=$(jq -r .statement_id <<<"$out"); st=$(jq -r .status.state <<<"$out")
  while [ "$st" = PENDING ] || [ "$st" = RUNNING ]; do
    sleep 5; out=$(dbx api get "/api/2.0/sql/statements/$id" -o json); st=$(jq -r .status.state <<<"$out")
  done
  [ "$st" = SUCCEEDED ] || { echo "SQL $st: $(jq -r '.status.error.message // ""' <<<"$out" | cut -c1-400)" >&2; return 1; }
  jq -r '.result.data_array[]? | map(. // "") | @tsv' <<<"$out"
  # results over one chunk (25 MiB or 2^16 rows inline) are not fetched: these scripts only read small aggregates
}

# SQL identifiers and literals from arguments: allow only what UC names and dates need
ident() { [[ $1 =~ ^[A-Za-z0-9_.-]+$ ]] || { echo "not a plain name: $1" >&2; exit 2; }; echo "\`${1//./\`.\`}\`"; }
uuid() { [[ $1 =~ ^[0-9a-f-]{36}$ ]] || { echo "not a UUID: $1" >&2; exit 2; }; }
