#!/usr/bin/env python3
"""test_copies.py: offline checks of copies.py pg with a fake psql and a stubbed fill (FILL): the table names it reads
from quoted and unquoted CREATE TABLE, the DDL it sends for a new copy (schema, then search_path, then the model), a copy
whose schema already has tables skips the DDL, and a failed DDL is reported.
    python3 -I test_copies.py
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
COPIES = os.path.join(HERE, "copies.py")

FAKE_PSQL = '''#!{py}
# fake psql for test_copies.py: logs each call's arguments and stdin; answers the table count with TABLES_IN; fails a DDL
# when FAIL_DDL is set
import os, sys
data = sys.stdin.read()
with open(os.environ["LOG"], "a") as f:
    f.write("CALL " + " ".join(sys.argv[1:]) + "\\n" + data + "\\n")
if "information_schema.tables" in data:
    print(os.environ.get("TABLES_IN", "0"))
elif "CREATE TABLE" in data and os.environ.get("FAIL_DDL"):
    print('ERROR:  relation "nation" already exists')
    sys.exit(3)
'''

FAKE_FILL = '''#!{py}
# stub fill for test_copies.py: logs its arguments and the table list copies.py exports
import os, sys
with open(os.environ["LOG"], "a") as f:
    f.write("FILL " + " ".join(sys.argv[1:]) + " RE=" + os.environ.get("RE", "") + "\\n")
'''


def read(path):
    with open(path) as f:
        return f.read()


class CopiesPg(unittest.TestCase):
    def setUp(self):
        self.T = tempfile.mkdtemp(prefix="test-copies-")
        self.model = os.path.join(self.T, "model.sql")
        with open(self.model, "w") as f:
            f.write('CREATE TABLE "nation" (n_nationkey INT);\nCREATE TABLE region (r_regionkey INT);\n'
                    "COMMENT ON TABLE region IS 'x';\n")
        self.psql = os.path.join(self.T, "psql")
        self.fill = os.path.join(self.T, "fill.py")
        for path, body in ((self.psql, FAKE_PSQL), (self.fill, FAKE_FILL)):
            with open(path, "w") as f:
                f.write(body.format(py=sys.executable))
            os.chmod(path, 0o755)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.T, ignore_errors=True)

    def run_copies(self, log, **env):
        """copies.py pg on the fixture, copies 1 and 2, parallel 1: (exit code, stdout)."""
        open(log, "w").close()
        e = dict(os.environ, LOG=log, PSQL=self.psql, FILL=f"{sys.executable} {self.fill}")
        e.update(env)
        p = subprocess.run([sys.executable, "-I", COPIES, "pg", self.model, "m", "1", "2", "1"], capture_output=True,
                           text=True, env=e, check=False)
        return p.returncode, p.stdout + p.stderr

    def test_new_copies(self):
        log = os.path.join(self.T, "new")
        rc, out = self.run_copies(log)
        self.assertEqual(rc, 0, out)
        self.assertIn("done m_0001 ", out)
        self.assertIn("done m_0002 ", out)
        self.assertIn("all done", out)
        text = read(log)
        self.assertIn("FILL pg m_0001 RE=nation|region", text)
        # copy 1's DDL: the schema, then the search_path, then the model's tables, in that order
        s = text.index("CREATE SCHEMA IF NOT EXISTS m_0001;")
        p = text.index("SET search_path = m_0001;")
        t = text.index('CREATE TABLE "nation"')
        self.assertTrue(s < p < t)

    def test_existing_copy_skips_ddl(self):
        log = os.path.join(self.T, "old")
        rc, out = self.run_copies(log, TABLES_IN="2")
        self.assertEqual(rc, 0, out)
        text = read(log)
        self.assertNotIn("CREATE", text)
        self.assertIn("FILL pg m_0002 RE=nation|region", text)

    def test_failed_ddl_is_reported(self):
        log = os.path.join(self.T, "bad")
        rc, out = self.run_copies(log, FAIL_DDL="1")
        self.assertNotEqual(rc, 0)
        self.assertIn('failed m_0001 DDL: ERROR:  relation "nation" already exists', out)
        self.assertIn("finished with failures", out)
        self.assertNotIn("FILL", read(log))


if __name__ == "__main__":
    unittest.main()
