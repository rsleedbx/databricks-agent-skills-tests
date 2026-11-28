"""fake_az: a fake `az` for the offline tests of the source skills' Azure scripts. install(folder, body) writes an
executable named az into folder (the test puts folder first on PATH). Each call appends its argument words, one line,
to the file in FA_LOG, then runs body: Python that decides the answer from `args`, `line` (the words joined by spaces)
and the environment. A body that doesn't exit writes nothing and exits 0.
"""
from __future__ import annotations

import os
import stat
import sys

HEAD = """import os, sys
args = sys.argv[1:]
line = " ".join(args)
with open(os.environ["FA_LOG"], "a") as _f:
    _f.write(line + "\\n")
"""


def install(folder: str, body: str) -> str:
    path = os.path.join(folder, "az")
    with open(path, "w") as f:
        f.write(f"#!{sys.executable}\n" + HEAD + body)
    os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return path
