#!/usr/bin/env bash
# Copy the skills listed in genie-skills.txt into a workspace's Genie Code skills folder,
# one folder per skill directly under it, so every user of the workspace sees them.
#
# Usage:
#   bin/genie_skills_sync.sh <profile> [--dest <workspace dir>] [--adopt] [--prune] [--dry-run]
#
# --dest defaults to /Workspace/.assistant/skills (workspace-wide; needs workspace admin).
# Each copied folder gets a .synced-from file (repo commit). A folder already there
# without one belongs to someone else and is left alone, unless --adopt (it was copied
# from this repo by hand). --prune deletes folders with a .synced-from whose skill is
# no longer listed.
set -euo pipefail

usage() { sed -n 's/^#   //p' "$0"; exit 1; }
[ $# -ge 1 ] || usage
PROFILE=$1; shift
DEST=/Workspace/.assistant/skills
DRY= ADOPT= PRUNE=
while [ $# -gt 0 ]; do
  case $1 in
    --dest) DEST=${2:?--dest needs a path}; shift 2;;
    --dry-run) DRY=1; shift;;
    --adopt) ADOPT=1; shift;;
    --prune) PRUNE=1; shift;;
    *) usage;;
  esac
done

ROOT=$(cd "$(dirname "$0")/.." && pwd)
LIST=$ROOT/genie-skills.txt
SKILLS=()
while IFS= read -r s; do
  case $s in ''|'#'*) continue;; esac
  [ -f "$ROOT/skills/$s/SKILL.md" ] || { echo "not a skill: $s (in genie-skills.txt)" >&2; exit 1; }
  SKILLS+=("$s")
done < "$LIST"

# Links from a listed skill to a skill that isn't listed won't resolve in the workspace.
for s in "${SKILLS[@]}"; do
  for t in $(grep -rhoE '\.\./(\.\./)?[a-z0-9-]+/' "$ROOT/skills/$s" --include='*.md' 2>/dev/null \
             | sed -E 's#(\.\./)+##; s#/$##' | sort -u); do
    [ -d "$ROOT/skills/$t" ] || continue
    case " ${SKILLS[*]} " in *" $t "*) ;; *) echo "note: $s links to $t, which is not listed";; esac
  done
done

COMMIT=$(git -C "$ROOT" rev-parse --short HEAD)
DIRTY=$(git -C "$ROOT" status --porcelain -- skills genie-skills.txt | head -1)
[ -z "$DIRTY" ] || COMMIT="$COMMIT+uncommitted"

# Folders already in the destination: ours (with .synced-from) or someone else's.
existing=$(databricks workspace list "$DEST" --profile "$PROFILE" -o json 2>/dev/null \
  | jq -r '.[]? | select(.object_type=="DIRECTORY") | .path | sub(".*/"; "")' || true)

STAGE=$(mktemp -d)
trap 'rm -rf "${STAGE:?}"' EXIT
for s in "${SKILLS[@]}"; do
  if [ -z "$ADOPT" ] && printf '%s\n' "$existing" | grep -qx -- "$s" \
     && ! databricks workspace get-status "$DEST/$s/.synced-from" --profile "$PROFILE" >/dev/null 2>&1; then
    echo "skip $s: $DEST/$s exists and was not copied by this script"
    continue
  fi
  rsync -a --exclude __pycache__ --exclude '*.pyc' --exclude .DS_Store "$ROOT/skills/$s/" "$STAGE/$s/"
  printf 'databricks-agent-skills-tests %s\n' "$COMMIT" > "$STAGE/$s/.synced-from"
  echo "copy $s -> $DEST/$s"
done

STALE=()
if [ -n "$PRUNE" ]; then
  for d in $existing; do
    case " ${SKILLS[*]} " in *" $d "*) continue;; esac
    databricks workspace get-status "$DEST/$d/.synced-from" --profile "$PROFILE" >/dev/null 2>&1 || continue
    STALE+=("$d"); echo "prune $DEST/$d (no longer listed)"
  done
fi

[ -n "$(ls -A "$STAGE")" ] || [ ${#STALE[@]} -gt 0 ] || { echo "nothing to do"; exit 0; }
[ -z "$DRY" ] || { echo "dry run: nothing written"; exit 0; }
if [ -n "$(ls -A "$STAGE")" ]; then
  databricks workspace mkdirs "$DEST" --profile "$PROFILE"
  databricks workspace import-dir "$STAGE" "$DEST" --overwrite --profile "$PROFILE" >/dev/null
fi
for d in ${STALE[@]+"${STALE[@]}"}; do
  databricks workspace delete "$DEST/$d" --recursive --profile "$PROFILE"
done
HOST=$(databricks auth describe --profile "$PROFILE" -o json | jq -r '.details.host // .host // empty')
echo "done ($COMMIT): ${HOST%/}/#workspace${DEST}"
