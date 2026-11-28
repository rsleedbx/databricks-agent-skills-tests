#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["keyring"]
# ///
"""pw.py: test-database passwords in the OS keyring (macOS Keychain, Windows Credential Manager, Linux Secret Service)
instead of a plain-text file. Every script that takes a password file (ADMIN_PW_FILE, LOGIN_PW_FILE, sync.py's
password_file column) also takes keyring:<name>.

  uv run pw.py new <name>            # a random password (Lfc1 + 24 hex: the three classes Azure SQL and SQL Server want)
  uv run pw.py set <name> < file     # store what's on standard input (trailing newline dropped)
  uv run pw.py import <name> <file>  # store a password file's content; then delete the file yourself
  uv run pw.py get <name>            # print it (for a pipe or $(...); never on a terminal you share)
  uv run pw.py rm <name>
  uv run pw.py read <ref>            # a password file's content, or keyring:<name>'s password (for $(...))
  uv run pw.py check <ref>           # exit 0 when the reference can be read, without printing it

The password never goes on a command line. Service name: lfc-test (LFC_KEYRING_SERVICE to change it). A headless Linux
VM usually has no keyring backend: keep a private file there (umask 077).
"""
from __future__ import annotations

import os
import secrets
import sys

try:   # the keyring is optional for a caller that reads only password files (a workspace has none)
    import keyring
    from keyring.errors import KeyringError, NoKeyringError
except ImportError:
    keyring = None

    class KeyringError(Exception):
        pass

    class NoKeyringError(KeyringError):
        pass

SERVICE = os.environ.get("LFC_KEYRING_SERVICE", "lfc-test")


def read_ref(ref: str) -> str | None:
    """The password a reference names (a file's content, or keyring:<name>'s password) without its trailing newlines;
    None when it can't be read. Nothing is printed."""
    if ref.startswith("keyring:"):
        if keyring is None:   # plain python3 without the keyring package: run this file under uv, which installs it
            import subprocess
            r = subprocess.run(["uv", "run", "-q", "--script", os.path.abspath(__file__), "read", ref],
                               capture_output=True, text=True, check=False)
            return r.stdout.rstrip("\n") if r.returncode == 0 else None
        try:
            pw = keyring.get_password(SERVICE, ref[len("keyring:"):])
        except KeyringError:
            return None
        return None if pw is None else pw.rstrip("\n")
    try:
        with open(ref, encoding="utf-8") as f:
            return f.read().rstrip("\n")
    except (OSError, UnicodeDecodeError):
        return None


def ref_command(what: str, ref: str) -> int:
    """read | check <ref> (the shell's pw and pw_check): read prints the password; check says whether it can be read,
    without printing it."""
    pw = read_ref(ref)
    if pw is None:
        print(f"can't read {ref}", file=sys.stderr)
        return 1
    if what == "read":
        sys.stdout.write(pw + "\n")
    return 0


def main(argv: list[str]) -> int:
    if len(argv) == 2 and argv[0] in ("read", "check"):
        return ref_command(argv[0], argv[1])
    if len(argv) < 2 or argv[0] not in ("new", "set", "import", "get", "rm"):
        print(__doc__.split("\n\n")[1], file=sys.stderr)
        return 2
    what, name = argv[0], argv[1]
    try:
        if what == "get":
            pw = keyring.get_password(SERVICE, name)
            if pw is None:
                print(f"pw.py: no password {name!r} in the keyring (service {SERVICE}): uv run pw.py new {name}", file=sys.stderr)
                return 1
            sys.stdout.write(pw + "\n")
        elif what == "rm":
            keyring.delete_password(SERVICE, name)
        else:
            if what == "new":
                pw = "Lfc1" + secrets.token_hex(12)
            elif what == "set":
                pw = sys.stdin.read().rstrip("\n")
            else:
                with open(argv[2]) as f:
                    pw = f.read().rstrip("\n")
            if not pw:
                print("pw.py: empty password, nothing stored", file=sys.stderr)
                return 2
            keyring.set_password(SERVICE, name, pw)
            print(f"stored {name} (service {SERVICE}); use keyring:{name} where a password file goes", file=sys.stderr)
    except NoKeyringError:
        print("pw.py: no keyring backend on this machine; use a private password file (umask 077)", file=sys.stderr)
        return 2
    except KeyringError as e:
        print(f"pw.py: keyring {what} {name}: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
