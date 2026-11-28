#!/usr/bin/env python3
"""genie_skills_sync.py <profile> [--dest <workspace dir>] [--adopt] [--prune] [--dry-run]: copy the skills listed in
genie-skills.txt into a workspace's Genie Code skills folder, one folder per skill directly under it, so every user of
the workspace sees them.

Each copied SKILL.md gets genie-skill-note.md after its frontmatter: run the skill's Python in the notebook's own
process, never through a shell. --dest defaults to /Workspace/.assistant/skills (workspace-wide; needs workspace
admin). Each copied folder gets a .synced-from file (repo commit). A folder already there without one belongs to
someone else and is left alone, unless --adopt (it was copied from this repo by hand). --prune deletes folders with a
.synced-from whose skill is no longer listed. REST through lfc_rest (bin/workspace_files.py), no CLI.
"""
from __future__ import annotations

import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

R = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(R / "skills" / "lfc-rest" / "scripts"))
from lfc_rest import Rest, not_found  # noqa: E402
import workspace_files as wf  # noqa: E402

NOTE_MARK = "In Genie Code or a notebook"
NOTEBOOK_HEADER = "# Databricks notebook source"
LINK = re.compile(r"\.\./(?:\.\./)?[a-z0-9-]+/")
USAGE = ("usage: genie_skills_sync.py <profile> [--dest <workspace dir>] [--adopt] [--prune] [--dry-run]\n"
         "(see the header: what each option does)")


def listed_skills(root: Path) -> list[str]:
    """The skills in genie-skills.txt (comments and blank lines skipped); each must be a skill folder here."""
    out = []
    for line in (root / "genie-skills.txt").read_text().splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        if not (root / "skills" / line / "SKILL.md").is_file():
            sys.exit(f"not a skill: {line} (in genie-skills.txt)")
        out.append(line)
    if not out:
        sys.exit("no skills in genie-skills.txt")
    return out


def note_links(root: Path, skills: list[str]) -> None:
    """A link from a listed skill to a skill that isn't listed won't resolve in the workspace."""
    for s in skills:
        targets = set()
        for md in (root / "skills" / s).rglob("*.md"):
            for m in LINK.finditer(md.read_text(errors="replace")):
                targets.add(re.sub(r"^(\.\./)+", "", m.group(0)).rstrip("/"))
        for t in sorted(targets):
            if (root / "skills" / t).is_dir() and t not in skills:
                print(f"note: {s} links to {t}, which is not listed")


def commit_label(root: Path) -> str:
    rev = subprocess.run(["git", "-C", str(root), "rev-parse", "--short", "HEAD"], capture_output=True, text=True,
                         check=True).stdout.strip()
    dirty = subprocess.run(["git", "-C", str(root), "status", "--porcelain", "--", "skills", "genie-skills.txt"],
                           capture_output=True, text=True, check=True).stdout.strip()
    return rev if not dirty else rev + "+uncommitted"


def tracked_files(skill_dir: Path) -> list[str]:
    """The files git tracks under a skill folder: only their working-tree text is published."""
    out = subprocess.run(["git", "-C", str(skill_dir), "ls-files", "-z"], capture_output=True, check=True).stdout
    return [p for p in out.decode().split("\0") if p]


def with_note(skill_md: str, note: str) -> str:
    """The note right after the frontmatter, the first thing Genie Code reads (a blank line, then the note)."""
    lines = skill_md.split("\n")
    if not lines or lines[0] != "---" or "---" not in lines[1:]:
        return skill_md
    end = lines.index("---", 1)
    return "\n".join(lines[:end + 1] + [""] + note.rstrip("\n").split("\n") + lines[end + 1:])


def main(argv: list[str]) -> int:
    if not argv:
        print(USAGE, file=sys.stderr)
        return 1
    profile, rest = argv[0], argv[1:]
    dest, dry, adopt, prune = "/Workspace/.assistant/skills", False, False, False
    while rest:
        a = rest.pop(0)
        if a == "--dest" and rest:
            dest = rest.pop(0)
        elif a == "--dry-run":
            dry = True
        elif a == "--adopt":
            adopt = True
        elif a == "--prune":
            prune = True
        else:
            print(USAGE, file=sys.stderr)
            return 1

    skills = listed_skills(R)
    note_links(R, skills)
    commit = commit_label(R)
    note = (R / "genie-skill-note.md").read_text()   # added to each copied SKILL.md (the repo's files stay as they are)
    api = Rest.connect(profile)

    try:
        existing = [Path(o["path"]).name for o in wf.listing(api, dest) if o.get("object_type") == "DIRECTORY"]
    except Exception as e:  # noqa: BLE001  only a missing destination counts as empty
        if not not_found(e):
            sys.exit(f"cannot list {dest}: {str(e).splitlines()[0] if str(e) else e}")
        existing = []

    stage = Path(tempfile.mkdtemp())
    try:
        staged = []
        for s in skills:
            if not adopt and s in existing and wf.status(api, f"{dest}/{s}/.synced-from") is None:
                print(f"skip {s}: {dest}/{s} exists and was not copied by this script")
                continue
            src = R / "skills" / s
            for rel in tracked_files(src):
                if rel != "SKILL.md":
                    (stage / s / rel).parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(src / rel, stage / s / rel)
            (stage / s / ".synced-from").write_text(f"databricks-agent-skills-tests {commit}\n")
            copied = with_note((src / "SKILL.md").read_text(), note)
            if NOTE_MARK not in copied:
                print(f"{s}: no frontmatter for the note", file=sys.stderr)
                return 1
            (stage / s / "SKILL.md").write_text(copied)
            staged.append(s)
            print(f"copy {s} -> {dest}/{s}")

        stale = []
        if prune:
            for d in existing:
                if d not in skills and wf.status(api, f"{dest}/{d}/.synced-from") is not None:
                    stale.append(d)
                    print(f"prune {dest}/{d} (no longer listed)")

        files = sorted(p.relative_to(stage).as_posix() for p in stage.rglob("*") if p.is_file())
        if not files and not stale:
            print("nothing to do")
            return 0
        if dry:
            print("dry run: nothing written")
            return 0
        if files:
            # every file goes in as RAW (a file, as the skills read it, never a notebook); a notebook a past run left
            # at the path without its .py is removed first
            wf.mkdirs(api, dest)
            for rel in files:
                path = f"{dest}/{rel}"
                data = (stage / rel).read_bytes()
                if rel.endswith(".py") and NOTEBOOK_HEADER in data.decode(errors="replace").split("\n"):
                    old = wf.status(api, path[:-3])
                    if old and old.get("object_type") == "NOTEBOOK":
                        wf.delete(api, path[:-3])
                wf.mkdirs(api, path.rsplit("/", 1)[0])
                wf.put(api, path, data, fmt="RAW", overwrite=True)
            # each copied folder must carry its marker, or the next run would take it for someone else's
            for s in staged:
                if wf.status(api, f"{dest}/{s}/.synced-from") is None:
                    print(f"{dest}/{s}/.synced-from missing after the import", file=sys.stderr)
                    return 1
        for d in stale:
            wf.delete(api, f"{dest}/{d}", recursive=True)
    finally:
        shutil.rmtree(stage, ignore_errors=True)
    print(f"done ({commit}): {api.host.rstrip('/')}/#workspace{dest}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
