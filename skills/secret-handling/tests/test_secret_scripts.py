"""test_secret_scripts.py: checks secret_run.py, secret_find.py, secret_perms.py and block-secret-prints.py on throwaway files
with a fake secret (never a real one). Run: python3 -m unittest -q skills/secret-handling/tests/test_secret_scripts.py
"""
from __future__ import annotations

import os
import secrets
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest

S = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts")


def py(script: str, *args: str, stdin: str | None = None):
    p = subprocess.run([sys.executable, os.path.join(S, script), *args], capture_output=True, text=True, input=stdin,  # noqa: PLW1510
                       timeout=120)
    return p.returncode, p.stdout, p.stderr


class SecretScriptsTest(unittest.TestCase):
    def setUp(self):
        self.t = tempfile.mkdtemp()
        self.fake = "fake-" + secrets.token_hex(8)
        self.creds = os.path.join(self.t, "app_credentials.json")
        with open(self.creds, "w") as fh:
            fh.write('{"host": "h", "dba": {"user": "u", "password": "' + self.fake + '"}}\n')
        self.pw = os.path.join(self.t, "db.pw")
        with open(self.pw, "w") as fh:
            fh.write(self.fake + "\n")
        repo = os.path.join(self.t, "a", "repo")
        os.makedirs(repo)
        self.git(repo, "init", "-q")
        with open(os.path.join(repo, "leak.txt"), "w") as fh:
            fh.write("x=" + self.fake + "\n")
        self.git(repo, "add", "leak.txt")
        self.git(repo, "commit", "-q", "-m", "leak")
        self.git(repo, "rm", "-q", "leak.txt")
        self.git(repo, "commit", "-q", "-m", "clean")

    def tearDown(self):
        shutil.rmtree(self.t, ignore_errors=True)

    @staticmethod
    def git(repo, *args):
        subprocess.run(["git", "-C", repo, "-c", "user.email=t@example.com", "-c", "user.name=t", *args], check=True,
                       capture_output=True)

    def test_run_json_field_reaches_the_command(self):
        code = "import os,sys; sys.stdout.write(str(len(os.environ['P'])))"
        rc, out, _ = py("secret_run.py", self.creds, ".dba.password", "P", "--", sys.executable, "-c", code)
        self.assertEqual((rc, out), (0, str(len(self.fake))))

    def test_run_plain_file_has_its_newline_removed(self):
        code = "import os,sys; sys.stdout.write(str(len(os.environ['P'])))"
        rc, out, _ = py("secret_run.py", self.pw, "-", "P", "--", sys.executable, "-c", code)
        self.assertEqual((rc, out), (0, str(len(self.fake))))

    def test_run_prints_nothing_itself(self):
        rc, out, err = py("secret_run.py", self.creds, ".dba.password", "P", "--", sys.executable, "-c", "pass")
        self.assertEqual((rc, out, err), (0, "", ""))

    def test_find_exit_1_on_a_leak_and_lists_the_file_and_the_commit(self):
        rc, out, _ = py("secret_find.py", self.creds, ".dba.password", self.t)
        self.assertEqual(rc, 1)
        self.assertIn(f"file: {self.pw}", out)
        self.assertIn("commit: ", out)
        self.assertIn("leak", out)
        self.assertNotIn(self.fake, out)

    def test_perms_reports_644_and_fix_makes_it_600(self):
        os.chmod(self.pw, 0o644)
        rc, _, _ = py("secret_perms.py", self.pw)
        self.assertEqual(rc, 1)
        rc, _, _ = py("secret_perms.py", "--fix", self.pw)
        self.assertEqual(rc, 0)
        self.assertEqual(stat.S_IMODE(os.stat(self.pw).st_mode), 0o600)
        self.assertEqual(py("secret_perms.py", self.pw)[0], 0)

    def test_hook_self_test_and_blocks_a_cat_of_a_pw_file(self):
        hook = os.path.join(S, "block-secret-prints.py")
        self.assertEqual(subprocess.run([sys.executable, "-I", hook, "--self-test"], capture_output=True, check=False).returncode, 0)
        payload = '{"tool_name":"Bash","tool_input":{"command":"cat PATH"}}'.replace("PATH", self.pw)
        p = subprocess.run([sys.executable, "-I", hook], input=payload, capture_output=True, text=True, check=False)
        self.assertEqual(p.returncode, 2)


if __name__ == "__main__":
    unittest.main()
