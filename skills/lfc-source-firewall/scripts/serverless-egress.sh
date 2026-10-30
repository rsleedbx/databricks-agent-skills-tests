#!/usr/bin/env bash
# serverless-egress.sh <cloud> <region> [--state FILE]: the serverless egress ranges a source's firewall must allow,
#   from Databricks' published ip-ranges.json (outbound, IPv4, for one cloud and region: aws us-east-2, azure centralus,
#   gcp us-central1). Prints when the file was published, then one CIDR per line.
#   The list changes as often as every 30 days, and new ranges become active as soon as 60 days after they're published,
#   so refresh at least monthly. With --state FILE: compare with the list saved there by the last run (lines starting
#   "+" were added, "-" removed), then save the current list in FILE for next time. Exit 0; 1 if the file can't be read
#   or has no ranges for that region; 3 if the list changed since FILE (update the firewall).
set -uo pipefail
CLOUD=$1; REGION=$2; shift 2; STATE=
while [ $# -gt 0 ]; do case $1 in --state) STATE=$2; shift 2;; *) echo "unknown $1" >&2; exit 2;; esac; done
URL=https://www.databricks.com/networking/v1/ip-ranges.json
T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT
curl -fsSL "$URL" -o "$T/r.json" || { echo "can't download $URL" >&2; exit 1; }
TS=$(yq -p json -o tsv '.timestampSeconds // ""' "$T/r.json")
C="$CLOUD" R="$REGION" yq -p json -o tsv '[.prefixes[] | select(.type == "outbound" and .platform == strenv(C) and .region == strenv(R)) | .ipv4Prefixes[]] | unique | .[]' "$T/r.json" > "$T/now"
[ -s "$T/now" ] || { echo "no outbound ranges for $CLOUD $REGION in $URL" >&2; exit 1; }
when=$( [ -n "$TS" ] && jq -rn --argjson t "$TS" '$t | strftime("%Y-%m-%d")' )
echo "# $CLOUD $REGION: $(wc -l < "$T/now" | tr -d ' ') serverless egress ranges, ip-ranges.json published ${when:-?} (timestampSeconds $TS)"
rc=0
if [ -n "$STATE" ] && [ -f "$STATE" ]; then
  grep -v '^#' "$STATE" | sort > "$T/old"; sort "$T/now" > "$T/new"
  if cmp -s "$T/old" "$T/new"; then echo "# unchanged since the list saved in $STATE ($(head -1 "$STATE" | sed 's/^# //'))"
  else rc=3; echo "# CHANGED since $STATE: add the + lines to the firewall now; remove the - lines once nothing uses them"
       comm -13 "$T/old" "$T/new" | sed 's/^/+ /'; comm -23 "$T/old" "$T/new" | sed 's/^/- /'; fi
fi
cat "$T/now"
if [ -n "$STATE" ]; then { echo "# $CLOUD $REGION timestampSeconds=$TS fetched=$(date -u +%Y-%m-%d)"; cat "$T/now"; } > "$STATE"; fi
exit $rc
