"""lfc-vm <group> [args]: the source VM's commands.
  slot [--owner <owner>] <command> ...   the test databases (lfc-slot; installed as /usr/local/sbin/lfc-slot)
  masters <build|export|import> ...      the masters a database clones from (lfc-vm: a link to lfc-slot)
  build <file>                           this package as one executable zipapp (python3 -I), to install on a VM
Run under the name lfc-slot, the words are slot's; under any other (lfc-vm), these."""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import zipapp
from pathlib import Path

PKG = Path(__file__).resolve().parent


def build(out: str) -> str:
    """lfc_vm as an executable zipapp: `#!/usr/bin/python3 -I`, runs cli.main; out is replaced whole."""
    with tempfile.TemporaryDirectory() as t:
        shutil.copytree(PKG, Path(t, "lfc_vm"), ignore=shutil.ignore_patterns("__pycache__", "tests", "*.pyc"))
        tmp = f"{out}.new"
        zipapp.create_archive(t, tmp, interpreter="/usr/bin/python3 -I", main="lfc_vm.cli:run")
        os.chmod(tmp, 0o755)
        os.replace(tmp, out)
    return out


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if os.path.basename(sys.argv[0]) == "lfc-slot":    # installed as lfc-slot: its words are slot's
        argv = ["slot", *argv]
    if argv[:1] == ["slot"]:
        from . import slots
        return slots.main(argv[1:])
    if argv[:1] == ["masters"]:
        from . import masters
        return masters.main(argv[1:])
    if argv[:1] == ["build"] and len(argv) == 2:
        print(build(argv[1]))
        return 0
    print(__doc__.strip(), file=sys.stderr)
    return 2


def run() -> None:
    sys.exit(main())
