#!/usr/bin/env bash
# Copy the skills listed in genie-skills.txt into a workspace's Genie Code skills folder,
# one folder per skill directly under it, so every user of the workspace sees them.
#
# Usage:
#   bin/genie_skills_sync.sh <profile> [--dest <workspace dir>] [--adopt] [--prune] [--dry-run]
#
# Each copied SKILL.md gets genie-skill-note.md after its frontmatter: run the skill's Python in the notebook's
# own process, never through a shell.
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
NOTE=$ROOT/genie-skill-note.md     # added to each copied SKILL.md (the repo's SKILL.md files stay as they are)
SKILLS=()
while IFS= read -r s; do
  case $s in ''|'#'*) continue;; esac
  [ -f "$ROOT/skills/$s/SKILL.md" ] || { echo "not a skill: $s (in genie-skills.txt)" >&2; exit 1; }
  SKILLS+=("$s")
done < "$LIST"
[ ${#SKILLS[@]} -gt 0 ] || { echo "no skills in $LIST" >&2; exit 1; }

# Links from a listed skill to a skill that isn't listed won't resolve in the workspace.
for s in ${SKILLS[@]+"${SKILLS[@]}"}; do
  for t in $(grep -rhoE '\.\./(\.\./)?[a-z0-9-]+/' "$ROOT/skills/$s" --include='*.md' 2>/dev/null \
             | sed -E 's#(\.\./)+##; s#/$##' | sort -u); do
    [ -d "$ROOT/skills/$t" ] || continue
    case " ${SKILLS[*]} " in *" $t "*) ;; *) echo "note: $s links to $t, which is not listed";; esac
  done
done

COMMIT=$(git -C "$ROOT" rev-parse --short HEAD)
DIRTY=$(git -C "$ROOT" status --porcelain -- skills genie-skills.txt | head -1)
[ -z "$DIRTY" ] || COMMIT="$COMMIT+uncommitted"

STAGE=$(mktemp -d)
trap 'rm -rf "${STAGE:?}" "${STAGE:?}.nb"' EXIT

# Folders already in the destination: ours (with .synced-from) or someone else's. Only a missing
# destination counts as empty: any other failure stops here, before the ownership check is skipped.
if listed=$(databricks workspace list "$DEST" --profile "$PROFILE" -o json 2>"$STAGE/.err"); then
  existing=$(printf '%s' "$listed" | jq -r '.[]? | select(.object_type=="DIRECTORY") | .path | sub(".*/"; "")')
elif grep -qiE "does ?n.t exist|RESOURCE_DOES_NOT_EXIST" "$STAGE/.err"; then
  existing=
else
  echo "cannot list $DEST: $(head -1 "$STAGE/.err")" >&2; exit 1
fi
rm -f "$STAGE/.err"
for s in ${SKILLS[@]+"${SKILLS[@]}"}; do
  if [ -z "$ADOPT" ] && printf '%s\n' "$existing" | grep -qx -- "$s" \
     && ! databricks workspace get-status "$DEST/$s/.synced-from" --profile "$PROFILE" >/dev/null 2>&1; then
    echo "skip $s: $DEST/$s exists and was not copied by this script"
    continue
  fi
  # only the files git tracks (their working-tree text), so nothing untracked or ignored is published
  (cd "$ROOT/skills/$s" && git ls-files -z) | rsync -a --from0 --files-from=- "$ROOT/skills/$s/" "$STAGE/$s/"
  printf 'databricks-agent-skills-tests %s\n' "$COMMIT" > "$STAGE/$s/.synced-from"
  # genie-skill-note.md right after the frontmatter, the first thing Genie Code reads: no shell in a notebook
  awk 'FNR == NR { note = note $0 "\n"; next }
       FNR == 1 && /^---$/ { fm = 1; print; next }
       fm && /^---$/ { print; printf "\n%s", note; fm = 0; next }
       { print }' "$NOTE" "$ROOT/skills/$s/SKILL.md" > "$STAGE/$s/SKILL.md"
  grep -q 'In Genie Code or a notebook' "$STAGE/$s/SKILL.md" || { echo "$s: no frontmatter for the note" >&2; exit 1; }
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
  # import-dir makes a notebook (and drops .py) of every file starting "# Databricks notebook source"; the skills
  # name and read them as files, so those go in one by one as RAW files, after any notebook a past run left
  NB=$(cd "$STAGE" && grep -rlx --include='*.py' -m1 '# Databricks notebook source' . | sed 's#^\./##' || true)
  for f in $NB; do mkdir -p "$STAGE.nb/$(dirname "$f")" && mv "$STAGE/$f" "$STAGE.nb/$f"; done
  databricks workspace mkdirs "$DEST" --profile "$PROFILE"
  databricks workspace import-dir "$STAGE" "$DEST" --overwrite --profile "$PROFILE" >/dev/null
  for f in $NB; do
    old=$(databricks workspace get-status "$DEST/${f%.py}" --profile "$PROFILE" -o json 2>/dev/null | jq -r '.object_type // empty' || true)
    [ "$old" != NOTEBOOK ] || databricks workspace delete "$DEST/${f%.py}" --profile "$PROFILE"
    databricks workspace mkdirs "$DEST/$(dirname "$f")" --profile "$PROFILE"
    databricks workspace import "$DEST/$f" --file "$STAGE.nb/$f" --format RAW --overwrite --profile "$PROFILE"
  done
  rm -rf "${STAGE:?}.nb"
  # each copied folder must carry its marker, or the next run would take it for someone else's
  for s in "$STAGE"/*/; do
    s=$(basename "$s")
    databricks workspace get-status "$DEST/$s/.synced-from" --profile "$PROFILE" >/dev/null 2>&1 ||
      { echo "$DEST/$s/.synced-from missing after the import" >&2; exit 1; }
  done
fi
for d in ${STALE[@]+"${STALE[@]}"}; do
  databricks workspace delete "$DEST/$d" --recursive --profile "$PROFILE"
done
HOST=$(databricks auth describe --profile "$PROFILE" -o json | jq -r '.details.host // .host // empty')
echo "done ($COMMIT): ${HOST%/}/#workspace${DEST}"
