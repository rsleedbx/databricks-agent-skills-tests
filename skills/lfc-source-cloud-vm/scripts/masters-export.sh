#!/usr/bin/env bash
# masters-export.sh: as root on a VM that has the masters (slot.sh's ROOT/masters), copy them to a Blob container so that
#   slot-vm-setup.sh (MASTERS_URL) can take them to other VMs: <master>.tar.zst (tar --numeric-owner, zstd -T0), each engine's
#   admin env file as <engine>.env, and manifest.json. Environment: MASTERS_URL (https://<account>.blob.core.windows.net/<container>),
#   MASTERS_SAS_FILE (root-only, mode 600: the container's SAS with read and write, e.g. sp=racwl; read from the file, never on a
#   command line), MASTERS (all, or a comma list), EXPORT_TMP (default /var/lib/containers/masters-export: the archive is written
#   there, after a free-space check), MASTER_ENGINES (name=engine,... for a master with no <master>.engine file beside it).
#   A master whose blob has the same sha256 in the manifest isn't uploaded again. Blocks: MASTERS_BLOCK_MB (1024) above
#   MASTERS_PUT_BLOB_MAX_MB (5000). One JSON line per master on stdout; the rest on stderr.
# bash32: exempt (runs on the Linux VM)
set -euo pipefail
die() { echo "masters-export: $*" >&2; exit 1; }
private_file() { local m; m=$(stat -c %a "$1" 2>/dev/null || stat -f %Lp "$1"); case $m in *00) return 0 ;; esac; return 1; }   # no group or other bits
# a setting from /etc/lfc-slot.env (as slot.sh reads it), else the environment, else the default
lfc_setting() { local v; v=$(sed -n "s/^$1=//p" /etc/lfc-slot.env 2>/dev/null | tail -1); echo "${v:-${!1:-$2}}"; }
ROOT=$(lfc_setting ROOT /mnt/jfs); SECRETS=$(lfc_setting SECRETS /root/lfc-src)
U=${MASTERS_URL:?MASTERS_URL=https://<account>.blob.core.windows.net/<container>}; U=${U%/}
SASF=${MASTERS_SAS_FILE:?MASTERS_SAS_FILE=<root-only file with the container SAS>}
[ -f "$SASF" ] || die "no $SASF"
private_file "$SASF" || die "$SASF: chmod 600 it (no group or other access)"
SAS=$(tr -d ' \r\n' < "$SASF"); SAS=${SAS#\?}
[ -n "$SAS" ] || die "$SASF is empty"
MASTERS=${MASTERS:-all}; EXPORT_TMP=${EXPORT_TMP:-/var/lib/containers/masters-export}
MASTERS_BLOCK_MB=${MASTERS_BLOCK_MB:-1024}; MASTERS_PUT_BLOB_MAX_MB=${MASTERS_PUT_BLOB_MAX_MB:-5000}
V=2023-11-03
install -d -m 700 "$EXPORT_TMP"
T=$(mktemp -d "$EXPORT_TMP/run.XXXXXX"); trap 'rm -rf "${T:?}"' EXIT

url_cfg() { printf 'url = "%s"\n' "$1"; }   # the URL, with the SAS, reaches curl through -K (a pipe): never on its command line
bl() { local b=$1 q=$2; shift 2; curl -sS --retry 3 -K <(url_cfg "$U/$b?${q:+$q&}$SAS") "$@"; }   # bl <blob> <query> curl args...
retry() { local k; for k in 1 2 3 4 5; do "$@" && return 0; sleep $((k * 5)); done; return 1; }
fsize() { stat -c %s "$1" 2>/dev/null || stat -f %z "$1"; }
sha_stdin() { if command -v sha256sum >/dev/null; then sha256sum | cut -c1-64; else shasum -a 256 | cut -c1-64; fi; }
sha_of() { sha_stdin < "$1"; }
put_block() {   # put_block <file> <blob> <index> <length> <encoded id>: one slice by dd, its length sent (no chunked upload)
  dd if="$1" bs=1048576 skip=$(( $3 * MASTERS_BLOCK_MB )) count=$(( ($4 + 1048575) / 1048576 )) 2>/dev/null \
    | bl "$2" "comp=block&blockid=$5" -fsS -X PUT -H "x-ms-version: $V" -H "Content-Length: $4" --data-binary @- -o /dev/null
}
verify_size() {   # the length the blob has in storage is the one sent
  local got; got=$(bl "$1" "" -sSI | tr -d '\r' | awk 'tolower($1) == "content-length:" {print $2}')
  [ "$got" = "$2" ] || die "$1: $got bytes in storage, $2 sent"
}
put_file() {   # put_file <file> <blob> <content type>: Put Blob up to the limit, else Put Block per block, then Put Block List
  local f=$1 b=$2 ct=$3 size blk n i len raw id ids=""
  size=$(fsize "$f"); blk=$(( MASTERS_BLOCK_MB * 1048576 ))
  if [ "$size" -le $(( MASTERS_PUT_BLOB_MAX_MB * 1048576 )) ]; then
    retry bl "$b" "" -fsS -X PUT -H 'x-ms-blob-type: BlockBlob' -H "x-ms-version: $V" -H "Content-Type: $ct" -T "$f" -o /dev/null \
      || die "upload of $b failed"
  else
    n=$(( (size + blk - 1) / blk ))
    for (( i = 0; i < n; i++ )); do
      len=$(( size - i * blk )); [ "$len" -le "$blk" ] || len=$blk
      raw=$(printf '%06d' "$i" | base64 | tr -d '\n')          # six bytes: eight base64 characters, the same length for all
      id=$(printf '%s' "$raw" | sed 's/+/%2B/g; s|/|%2F|g; s/=/%3D/g')
      retry put_block "$f" "$b" "$i" "$len" "$id" || die "block $i of $b failed"
      ids="$ids<Latest>$raw</Latest>"
    done
    printf '<?xml version="1.0" encoding="utf-8"?><BlockList>%s</BlockList>' "$ids" > "$T/blocklist.xml"
    retry bl "$b" "comp=blocklist" -fsS -X PUT -H "x-ms-version: $V" -H "x-ms-blob-content-type: $ct" -H 'Content-Type: application/xml' \
      --data-binary @"$T/blocklist.xml" -o /dev/null || die "block list of $b failed"
  fi
  verify_size "$b" "$size"
}
put_manifest() {
  jq --arg c "$(date -u +%Y-%m-%dT%H:%M:%SZ)" '.created = $c' "$T/new.json" > "$T/m.json" && mv "$T/m.json" "$T/new.json"
  retry bl manifest.json "" -fsS -X PUT -H 'x-ms-blob-type: BlockBlob' -H "x-ms-version: $V" -H 'Content-Type: application/json' \
    -T "$T/new.json" -o /dev/null || die "manifest.json upload failed"
}

# the manifest so far (a read: the SAS needs read as well as write); a container without one starts it
code=$(curl -sS --retry 3 -K <(url_cfg "$U/manifest.json?$SAS") -o "$T/manifest.json" -w '%{http_code}') || die "no connection to $U"
case $code in
  200) ;;
  404) echo '{"masters":{},"envs":[]}' > "$T/manifest.json" ;;
  *) die "manifest.json: HTTP $code (the SAS needs read and write)" ;;
esac
jq -e '.masters and .envs' "$T/manifest.json" >/dev/null || die "manifest.json has no masters or envs"
cp "$T/manifest.json" "$T/new.json"

engine_of() {   # <master>: the sidecar beside it (tpc-master.sh writes it), else MASTER_ENGINES
  local e
  e=$(cat "$ROOT/masters/$1.engine" 2>/dev/null || true)
  [ -n "$e" ] || e=$(printf '%s\n' "${MASTER_ENGINES:-}" | tr ',' '\n' | awk -F= -v n="$1" '$1 == n {print $2}')
  case $e in
    postgres|mysql|mariadb|sqlserver|oracle|db2|informix|cockroachdb|yugabytedb) echo "$e" ;;
    *) die "$1: no engine (write $ROOT/masters/$1.engine, or MASTER_ENGINES=$1=<engine>)" ;;
  esac
}
export_master() {
  local n=$1 eng a sha bytes files cur state e esha ebytes need avail
  [[ $n =~ ^[A-Za-z0-9._-]+$ ]] || die "bad master name: $n"
  [ -d "$ROOT/masters/$n" ] || die "no master $n in $ROOT/masters"
  eng=$(engine_of "$n")
  need=$(du -sk "$ROOT/masters/$n" | awk '{print $1 * 1024}')
  avail=$(df -Pk "$EXPORT_TMP" | awk 'NR == 2 {print $4 * 1024}')
  [ "$avail" -ge "$need" ] || die "no room for the archive in $EXPORT_TMP: $avail bytes free, $n is $need (set EXPORT_TMP to a bigger disk)"
  a=$T/$n.tar.zst
  tar --numeric-owner -C "$ROOT/masters" -cf - "$n" | zstd -T0 -q -c > "$a" || die "tar or zstd failed on $n"
  sha=$(sha_of "$a"); bytes=$(fsize "$a")
  cur=$(jq -r --arg n "$n" '.masters[$n].sha256 // empty' "$T/new.json")
  if [ "$cur" = "$sha" ]; then
    state=skipped; echo "masters-export: $n: the blob has this sha256 already" >&2
  else
    echo "masters-export: $n: uploading $bytes bytes" >&2
    put_file "$a" "$n.tar.zst" application/octet-stream
    state=uploaded
    files=$(find "$ROOT/masters/$n" -type f | wc -l | tr -d ' ')
    jq --arg n "$n" --arg e "$eng" --argjson b "$bytes" --arg s "$sha" --argjson f "$files" \
      '.masters[$n] = {engine: $e, bytes: $b, sha256: $s, files: $f}' "$T/new.json" > "$T/m.json" && mv "$T/m.json" "$T/new.json"
  fi
  # the admin env of the engine: the one slot.sh gives the clones (a secret: the private container only)
  e=$SECRETS/$eng.env
  [ -f "$e" ] || die "no $e: the admin env of the $eng masters"
  esha=$(sha_of "$e"); ebytes=$(fsize "$e")
  if [ "$(jq -r --arg e "$eng" '[.envs[] | select(.engine == $e) | .sha256][0] // empty' "$T/new.json")" != "$esha" ]; then
    put_file "$e" "$eng.env" text/plain
    jq --arg e "$eng" --argjson b "$ebytes" --arg s "$esha" \
      '.envs = ([.envs[] | select(.engine != $e)] + [{engine: $e, file: ($e + ".env"), bytes: $b, sha256: $s}])' "$T/new.json" > "$T/m.json" \
      && mv "$T/m.json" "$T/new.json"
  fi
  put_manifest
  rm -f "$a"
  jq -cn --arg m "$n" --arg e "$eng" --argjson b "$bytes" --arg s "$sha" --arg st "$state" \
    '{master: $m, engine: $e, bytes: $b, sha256: $s, state: $st}'
}

if [ "$MASTERS" = all ]; then
  list=$(for d in "$ROOT"/masters/*/; do [ -d "$d" ] && basename "$d"; done || true)
else
  list=$(printf '%s\n' "$MASTERS" | tr ',' '\n')
fi
[ -n "$list" ] || die "no masters in $ROOT/masters"
for n in $list; do export_master "$n"; done
