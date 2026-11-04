"""results_dir.py: where the result scripts read and write results/<kind> (runs, findings, ddl-matrix).

In a checkout: the repo's results/. Installed as a Claude Code plugin (this copy is under ~/.claude/plugins/cache/ and
is replaced on every update): new results go to ~/.lfc/tests/results/<kind>, and reads take the shipped results plus
those. LFC_TESTS_RESULTS=<dir> sets the folder for new results in either case.
"""
import json
import re, os
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]          # skills/lfc-test-results/scripts/results_dir.py
PLUGIN = "/.claude/plugins/cache/" in REPO.as_posix()

def root():
    if os.environ.get("LFC_TESTS_RESULTS"): return Path(os.environ["LFC_TESTS_RESULTS"]).expanduser()
    return Path.home() / ".lfc" / "tests" / "results" if PLUGIN else REPO / "results"

def write_dir(kind): return root() / kind

def read_dirs(kind):                                # shipped first, then the user's: later files win on the same name
    return list(dict.fromkeys([REPO / "results" / kind, write_dir(kind)]))

def show(path):                                     # repo-relative in a checkout, absolute elsewhere
    try: return str(path.relative_to(REPO))
    except ValueError: return str(path)

def plugin_version():                               # installed as a plugin (no .git): its version, else None
    if not PLUGIN: return None
    try: return json.loads((REPO / ".claude-plugin" / "plugin.json").read_text())["version"]
    except Exception: pass
    # no version in plugin.json: Claude Code versions the plugin by its commit and names the install folder after it
    return REPO.name if re.fullmatch(r"[0-9a-f]{7,40}", REPO.name) else None
