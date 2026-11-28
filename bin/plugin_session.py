#!/usr/bin/env python3
"""plugin_session.py: the lfc-tests plugin's SessionStart hook (.claude-plugin/plugin.json runs it). Lets the skills run
from any folder when this repo is installed as a Claude Code plugin rather than opened as a checkout: sets LFC_TESTS to
this copy and puts its bin/ on PATH for every Bash command (through $CLAUDE_ENV_FILE), and tells the session (stdout
becomes context) that the skills' repo-relative paths (skills/<skill>/..., bin/..., results/...) are under $LFC_TESTS.
Also names the local-test tools that are missing (no more than a PATH lookup; installs nothing), so the session can
point to check-tools.sh first, and whether the user's test settings (lfc-test-settings) are saved: which keys, and
when they were last checked (the file only, no CLI call), so a test reads them instead of asking again.
"""
from __future__ import annotations

import importlib.util
import os
import platform
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path


def main(argv: list[str]) -> int:
    here = Path(__file__).resolve().parent.parent
    env_file = os.environ.get("CLAUDE_ENV_FILE")
    if env_file:
        with open(env_file, "a") as f:
            f.write(f"export LFC_TESTS={shlex.quote(str(here))}\n")
            f.write(f'export PATH={shlex.quote(str(here / "bin"))}:"$PATH"\n')
    print(f"lfc-tests plugin: paths in its skills that start with skills/, bin/, results/ or docs/ are relative to "
          f'$LFC_TESTS ({here}); run them as "$LFC_TESTS/<path>", from any folder.')
    tools = "databricks uv jq yq duckdb podman" + (" limactl" if platform.system() == "Darwin" else "")
    gone = " ".join(t for t in tools.split() if shutil.which(t) is None)
    if gone:
        print(f'lfc-tests plugin: missing for local tests: {gone}. Before the first test, run bash '
              '"$LFC_TESTS/skills/cli-setup/scripts/check-tools.sh" local and give the user its install commands; '
              "install nothing without asking.")
    if importlib.util.find_spec("databricks.sdk") is None:   # lfc_rest signs in through it, in every converted tool
        print("lfc-tests plugin: missing for the workspace calls: the Python package databricks-sdk (pip install "
              "databricks-sdk). The CLI alone doesn't serve the scripts (they call REST through lfc_rest).")
    settings_script = here / "skills/lfc-test-settings/scripts/settings.sh"
    sf = Path(os.environ.get("LFC_TESTS_SETTINGS") or Path.home() / ".lfc/tests/settings.env")
    if sf.is_file() and sf.stat().st_size > 0:
        keys = " ".join(k for k in re.findall(r"^[A-Z][A-Z0-9_]*", sf.read_text(), re.M) if k != "SETTINGS_CHECKED")
        got = subprocess.run(["bash", str(settings_script), "get", "SETTINGS_CHECKED"], stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL, text=True)
        checked = got.stdout.rstrip("\n") if got.returncode == 0 else "never"
        print(f"lfc-tests plugin: test settings saved in {sf} ({keys}; checked {checked}). Before asking the user "
              'which subscription, region, resource group, workspace or source host, read them: eval "$(bash '
              '"$LFC_TESTS/skills/lfc-test-settings/scripts/settings.sh" env)"; ask only for keys "settings.sh '
              'require <keys>" lists.')
    else:
        print("lfc-tests plugin: no test settings saved yet. The first test that needs a cloud account, region, "
              'resource group or workspace runs bash "$LFC_TESTS/skills/lfc-test-settings/scripts/settings.sh" setup, '
              "which shows the defaults the skills use (tell the user, and ask what to change), then asks every "
              "question once with a recommendation, and saves the answers (lfc-test-settings).")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
