"""source: a test database on a slot VM (lfc-source-cloud-vm's slot.sh), as the rounds need it: its login with the
password (from the slot's `up`, never printed), admin SQL through the slot (`sql`), and, for the engines whose capture
needs it (Db2's ASN tools), a shell in its container through the VM's admin login.

    s = Slot(host="203.0.113.10", name="dbz-ms", key="~/.ssh/lfc_slot_ed25519")
    conn = s.up("sqlserver")                      # {"host", "port", "user", "password", ...}; starts it if parked
    s.sql("SELECT 1;")                            # as the engine's admin
    s.container("whoami", admin_key="~/.ssh/<admin key>")   # Db2 only
"""
from __future__ import annotations

import json
import os
import shlex
import subprocess
import time

SLOT_ENGINES = {"sqlserver": "sqlserver", "mysql": "mysql", "oracle": "oracle", "db2": "db2", "postgres": "postgres",
                "informix": "informix"}


class Slot:
    def __init__(self, host: str, name: str, key: str = "~/.ssh/lfc_slot_ed25519", user: str = "slot",
                 owner: str | None = None, admin_user: str = "azureuser"):
        self.host, self.name, self.key, self.user = host, name, os.path.expanduser(key), user
        self.owner, self.admin_user = owner, admin_user

    def _ssh(self, key: str, user: str) -> list:
        return ["ssh", "-i", key, "-o", "BatchMode=yes", "-o", "ConnectTimeout=20", f"{user}@{self.host}"]

    def _run(self, words: list, stdin: str | None = None, tries: int = 4) -> str:
        """slot.sh's words through the slot login (a forced command); ssh's own failures (exit 255) retried."""
        for i in range(tries):
            p = subprocess.run(self._ssh(self.key, self.user) + [" ".join(words)], input=stdin, text=True,
                               capture_output=True)
            if p.returncode != 255 or "Permission denied" in p.stderr:
                break
            time.sleep(5 * 3 ** i)
        if p.returncode:
            raise RuntimeError(f"slot {words[0]} {self.name}: {p.stderr.strip()[-400:]}")
        return p.stdout

    def up(self, engine: str, over_limit: bool = False) -> dict:
        """Start (or find running) the database; its login, with the password. Never print the result whole."""
        words = ["up", SLOT_ENGINES[engine], self.name, "--empty"] + (["--over-limit"] if over_limit else [])
        info = json.loads(self._run(words).strip().splitlines()[-1])
        info.setdefault("owner", self.owner)
        return info

    def sql(self, text: str, database: str | None = None) -> str:
        """text on the engine's admin client in the slot's container (slot.sh sql); its output."""
        return self._run(["sql", self.name] + ([database] if database else []), stdin=text)

    def container_name(self, owner: str) -> str:
        return f"lfc-slot-{owner}-{self.name}"

    def container(self, cmd: str, admin_key: str, owner: str, detached: bool = False, root: bool = True) -> str:
        """cmd as root in the database's container, through the VM admin login (podman exec). For setup an engine's
        capture needs beyond SQL (Db2's ASN tools); detached: started and left running (a capture agent); root False:
        as the container's own user (Informix's image runs as informix, whose login shell has dbaccess)."""
        c = self.container_name(owner)
        remote = (f"sudo podman exec {'-d ' if detached else ''}{'-u 0 ' if root else ''}{shlex.quote(c)} "
                  f"bash -lc {shlex.quote(cmd)}")
        p = subprocess.run(self._ssh(os.path.expanduser(admin_key), self.admin_user) + [remote], text=True,
                           capture_output=True)
        if p.returncode:
            raise RuntimeError(f"container {c}: {cmd[:80]}: {(p.stderr or p.stdout).strip()[-600:]}")
        return p.stdout

    def setup(self, steps: list, admin_key: str | None = None, owner: str | None = None, log=print) -> None:
        """Run an engine's setup steps (engines.Engine.setup): sql through the slot, shell in the container."""
        for kind, text in steps:
            log(f"setup {self.name}: {kind} {text.splitlines()[0][:100] if text else ''}")
            if kind == "sql":
                out = self.sql(text)
                if out.strip():
                    log("  " + out.strip().replace("\n", "\n  ")[-800:])
            elif kind in ("shell", "shell_detached", "shell_default"):
                if not admin_key or not owner:
                    raise ValueError(f"{self.name}: a shell step needs the VM admin login (admin_key) and the owner")
                out = self.container(text, admin_key, owner, detached=kind == "shell_detached",
                                     root=kind != "shell_default")
                if out.strip():
                    log("  " + out.strip()[-800:])
            elif kind == "sleep":
                time.sleep(int(text))
            else:
                raise ValueError(f"unknown setup step {kind}")
