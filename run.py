#!/usr/bin/env python3
"""run.py: skill test runner -- runs headless Claude sessions against a prompt file.

Usage:
  ./run.py [skills-repo] [prompts-file]

Arguments:
  skills-repo   Path to the databricks-agent-skills checkout whose skills are tested
                (default: $SKILLS_REPO, else ../databricks-agent-skills next to this repo)
  prompts-file  Prompts file, relative to this repo or absolute (default: prompts/lakeflow-connect.txt)

Environment variables (set via env.sh, which sources env.local.sh):
  PIPELINE_WS   Databricks CLI profile passed to Claude as DATABRICKS_CONFIG_PROFILE (default: DEFAULT)
  CLAUDE_ALLOWED_TOOLS  Comma-separated tool allowlist for deploy tests, passed as one --allowedTools
                argument so rules with spaces work, e.g. 'Bash(databricks:*),Bash(rm -f conn.json)'
  CLAUDE_ARGS   Other extra flags for claude -p; split on spaces, so no values with spaces (default: none)
  WORK_DIR      Directory each session runs in (default: a fresh /tmp/<prompts-name>-work). Keep it
                outside the skills repo so skills load from the plugin cache, not the checkout.

Prompts file: one `tag|prompt` per line. A `tag+|text` line is a follow-up turn: it resumes
the session started by `tag` and appends to the same output file (use it to answer the
questions a skill asks before creating resources). `${VAR}` in a prompt is replaced with the
environment variable VAR (see env.example.sh); an unset variable stops the run.

A tag (with its follow-up turns) runs only when what it depends on changed since it last passed on this machine
(bin/test_skip, runner run.py; local, never committed): its prompt as filled, the workspace profile, the claude
version, CLAUDE_ARGS, the allowed tools and the skills repo's skills/. A pass: every turn exited 0 and ended in a
result event that isn't an error; whether the answer met the test's expectations is summarize.py's call. A skipped
tag leaves <tag>.skipped (no <tag>.json) in the results. TEST_SKIP_AGAIN=1 runs every tag.

Examples:
  source env.sh
  ./run.py
  ./run.py ~/github/databricks-agent-skills prompts/lakeflow-connect.txt
  bin/run_batch.py prompts/lfc-matrix/qbc-*.txt
"""
from __future__ import annotations

import fnmatch
import glob
import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
GATE = HERE / "bin" / "test_skip" / "scripts" / "test_skip.py"
RUNNER = "run.py"
VAR = re.compile(r"\$\{(\w+)\}")
SESSION = re.compile(r'"session_id":"([^"]*)"')


def fill(text: str) -> str:
    """${VAR} replaced from the environment; ValueError naming the first unset one."""
    def sub(m: re.Match) -> str:
        if m.group(1) not in os.environ:
            raise ValueError(m.group(1))
        return os.environ[m.group(1)]
    return VAR.sub(sub, text)


def fill_lenient(text: str) -> str:
    """${VAR} replaced where the environment has it; the placeholder stays where it doesn't."""
    return VAR.sub(lambda m: os.environ.get(m.group(1), m.group(0)), text)


def split_line(line: str) -> tuple[str, str]:
    """`tag|prompt` (the prompt keeps any further |)."""
    tag, _, prompt = line.partition("|")
    return tag, prompt


def gate_args(prompts: Path, tag: str, prompt: str, repo_skills: Path, pipeline_ws: str,
              claude_version: str) -> list[str]:
    """The arguments that fingerprint a tag: its prompt as filled, the run's settings, and its follow-up turns as
    filled."""
    followups = "\n".join(fill_lenient(ln) for ln in prompts.read_text().splitlines() if ln.startswith(f"{tag}+|"))
    return [f"{prompts.stem}:{tag}", "--value", pipeline_ws, "--value", prompt,
            "--value", f"claude {claude_version}", "--value", f"args {os.environ.get('CLAUDE_ARGS', '')}",
            "--value", f"tools {os.environ.get('CLAUDE_ALLOWED_TOOLS', '')}", "--value", followups,
            str(repo_skills)]


def cache_root() -> str:
    """The first plugin cache folder of a databricks skill (the one this runner patches), or ""."""
    base = os.path.expanduser("~/.claude/plugins/cache")
    for dirpath, dirnames, _ in os.walk(base):
        if fnmatch.fnmatch(dirpath, "*/databricks/*/skills/databricks-*"):
            return dirpath.split("/skills/databricks-")[0] + "/skills"
    return ""


def patch_cache(repo: Path, root: str) -> None:
    """Copy the repo's databricks-* SKILL.md and references into the plugin cache folder of the same name."""
    for skill in sorted(glob.glob(str(repo / "skills" / "databricks-*"))):
        name = os.path.basename(skill)
        dest = os.path.join(root, name)
        if not os.path.isdir(dest):
            continue
        if os.path.isfile(os.path.join(skill, "SKILL.md")):
            shutil.copy(os.path.join(skill, "SKILL.md"), dest)
        refs = os.path.join(skill, "references")
        if os.path.isdir(refs):
            os.makedirs(os.path.join(dest, "references"), exist_ok=True)
            for f in glob.glob(os.path.join(refs, "*.md")):
                shutil.copy(f, os.path.join(dest, "references"))


def gate(args: list[str]) -> tuple[bool, str]:
    """test_skip.py with args: (skip, its output line)."""
    p = subprocess.run([sys.executable, str(GATE), *args], capture_output=True, text=True)
    return p.returncode == 0, p.stdout.strip()


def main(argv: list[str]) -> int:
    pipeline_ws = os.environ.get("PIPELINE_WS") or "DEFAULT"
    os.environ["PIPELINE_WS"] = pipeline_ws
    default_repo = os.environ.get("SKILLS_REPO") or HERE.parent / "databricks-agent-skills"
    repo = Path(argv[0] if argv and argv[0] else default_repo)
    repo = repo.resolve()
    if not (repo / "skills").is_dir():
        print(f"ERROR: no skills/ in {repo} (pass the skills repo path or set SKILLS_REPO)")
        return 1
    prompts_arg = argv[1] if len(argv) > 1 else "prompts/lakeflow-connect.txt"
    results_root = os.environ.get("RESULTS_ROOT") or "/tmp"
    base_name = Path(argv[1]).stem if len(argv) > 1 else "lakeflow-connect"
    work = Path(os.environ.get("WORK_DIR") or f"{results_root}/{base_name}-work")
    work.mkdir(parents=True, exist_ok=True)
    work = work.resolve()

    prompts = Path(prompts_arg) if prompts_arg.startswith("/") else HERE / prompts_arg
    if not prompts.is_file():
        print(f"ERROR: Prompts file not found: {prompts}")
        return 1
    skill_name = prompts.stem
    results = Path(f"{results_root}/{skill_name}-results")
    results.mkdir(parents=True, exist_ok=True)

    print("=== Skill Test Runner ===")
    print(f"Repo:        {repo}")
    print(f"Prompts:     {prompts}")
    print(f"Results:     {results}")
    print(f"Pipeline WS: {pipeline_ws}  (DATABRICKS_CONFIG_PROFILE for Claude)")
    print(f"Work dir:    {work}")
    # An expired workspace login: mint a token from the account login for the same account (account-token-fallback),
    # so the sessions' profile keeps working. Nothing changes while the profile's own login works.
    ws = subprocess.run(["bash", str(HERE / "skills/account-token-fallback/scripts/ws-token.sh"), pipeline_ws],
                        stdout=subprocess.PIPE, text=True)
    if ws.returncode != 0:
        print(f"ERROR: {pipeline_ws} can't log in (see above): databricks auth login --profile {pipeline_ws}")
        return 1
    if ws.stdout.strip():
        os.environ["DATABRICKS_CONFIG_FILE"] = ws.stdout.strip()
        print("Auth:        token minted from the account login (about an hour)")
    if os.environ.get("CLAUDE_ALLOWED_TOOLS"):
        print(f"Allowed:     {os.environ['CLAUDE_ALLOWED_TOOLS']}")
    if os.environ.get("CLAUDE_ARGS"):
        print(f"Claude args: {os.environ['CLAUDE_ARGS']}")
    print("")

    # --- plugin cache: patch with local skill files ---
    root = cache_root()
    if not root:
        print("Plugin cache missing. Rebuilding with a quick claude call...")
        try:
            subprocess.run(["claude", "-p", "hello", "--output-format", "text"], cwd=repo, capture_output=True,
                           stdin=subprocess.DEVNULL, env={**os.environ, "DATABRICKS_CONFIG_PROFILE": pipeline_ws})
        except FileNotFoundError:
            pass
        root = cache_root()
    if root:
        print(f"Plugin cache skills dir: {root}")
        patch_cache(repo, root)
        print("Plugin cache updated with local skill files.")
    else:
        print("WARNING: Could not find or create plugin cache. Tests may use stale skill content.")
    print("")

    # --- prompts ---
    passed = failed = skipped = 0
    skipped_tags: set[str] = set()
    failed_tags: set[str] = set()
    ran: list[tuple[str, str]] = []   # (tag, fingerprint line) of the tags that ran
    claude_version = _claude_version()
    for line in prompts.read_text().splitlines():
        tag, prompt = split_line(line)
        if not tag:
            continue
        base = tag[:-1] if tag.endswith("+") else tag
        if base != tag and base in skipped_tags:
            print(f"[{tag}] skipped with {base}")
            continue
        try:
            prompt = fill(prompt)
        except ValueError:
            print(f"[{tag}] SKIPPED: prompt uses an unset variable (see env.example.sh)")
            failed += 1
            failed_tags.add(base)
            continue
        resume: list[str] = []
        outfile = results / f"{base}.json"
        errfile = results / f"{base}.err"
        if base != tag:
            tag = base
            m = SESSION.search(outfile.read_text()) if outfile.exists() else None
            if not m or not m.group(1):
                print(f"[{tag}+] SKIPPED: no session to resume in {outfile}")
                failed += 1
                failed_tags.add(tag)
                continue
            resume = ["--resume", m.group(1)]
            print(f"[{tag}+] Follow-up: {prompt}")
        else:
            args = gate_args(prompts, tag, prompt, repo / "skills", pipeline_ws, claude_version)
            skip, last = gate(["skip", RUNNER, *args])
            if skip:
                print(f"[{tag}] SKIPPED: {last}")
                skipped += 1
                skipped_tags.add(tag)
                for f in (results / f"{tag}.json", results / f"{tag}.err"):
                    f.unlink(missing_ok=True)
                (results / f"{tag}.skipped").write_text(last + "\n")
                continue
            ran.append((tag, f"{args[0]} {last.removeprefix('run ')}"))
            outfile.write_text("")
            errfile.write_text("")
            (results / f"{tag}.skipped").unlink(missing_ok=True)
            print(f"[{tag}] Running: {prompt}")

        claude_args = shlex.split(os.environ.get("CLAUDE_ARGS", ""))
        cmd = ["claude", "-p", prompt, *resume]
        if os.environ.get("CLAUDE_ALLOWED_TOOLS"):
            cmd += ["--allowedTools", os.environ["CLAUDE_ALLOWED_TOOLS"]]
        cmd += ["--verbose", "--output-format", "stream-json", *claude_args]
        env = {**os.environ, "DATABRICKS_CONFIG_PROFILE": pipeline_ws}
        with outfile.open("a") as out, errfile.open("a") as err:
            try:
                code = subprocess.run(cmd, cwd=work, stdin=subprocess.DEVNULL, stdout=out, stderr=err,
                                      env=env).returncode
            except FileNotFoundError:   # as a shell's "command not found"
                err.write("claude: command not found\n")
                code = 127
        text = outfile.read_text()
        lines = text.count("\n")
        last_line = text.splitlines()[-1] if text.splitlines() else ""
        if not ('"type":"result"' in last_line and '"is_error":true' not in last_line):
            failed_tags.add(tag)   # a pass for the skip ledger: exit 0 and a success result event as the last line
        if code != 0:
            print(f"[{tag}] FAILED (exit {code})")
            head = errfile.read_text().splitlines()[:5]
            if head:
                print(f"[{tag}] stderr: " + "\n".join(head))
            failed += 1
            failed_tags.add(tag)
        else:
            print(f"[{tag}] done -> {outfile} ({lines} lines)")
            passed += 1
        print("")

    # record each tag that ran: a pass when it and all its follow-ups succeeded
    for tag, fp_line in ran:
        key, _, fp = fp_line.partition(" ")
        gate(["record", RUNNER, key, "fail" if tag in failed_tags else "pass", "--fingerprint", fp])
    print(f"=== {passed} passed, {failed} failed, {skipped} skipped (unchanged since they passed; "
          "TEST_SKIP_AGAIN=1 runs them) ===")
    print(f"Summarize: python3 {HERE}/summarize.py {results}")
    return 0


def _claude_version() -> str:
    try:
        out = subprocess.run(["claude", "--version"], capture_output=True, text=True).stdout
    except OSError:
        return ""
    return out.splitlines()[0] if out else ""


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
