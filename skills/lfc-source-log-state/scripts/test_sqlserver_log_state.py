"""sqlserver-log-state.py offline: the batch's JSON line printed compact, the per-table switch (PER_TABLE), a batch with
no JSON or a failing batch, a bad database name, and the usage; SQL_CMD is a fake sqlcmd (a shell script)."""
from __future__ import annotations

import contextlib
import io
import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import importlib.util

spec = importlib.util.spec_from_file_location("sqlserver_log_state", HERE / "sqlserver-log-state.py")
log_state = importlib.util.module_from_spec(spec)
spec.loader.exec_module(log_state)

FAKE = """#!/bin/sh
cat > /dev/null
echo "sqlcmd noise"
echo "{\\"args\\": \\"$*\\", \\"b\\": 2, \\"a\\": [1]}"
"""
FAILING = "#!/bin/sh\ncat > /dev/null\necho 'Msg 208, invalid object' >&2\nexit 1\n"


class LogState(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()

    def fake(self, body: str) -> str:
        path = os.path.join(self.d, "sqlcmd")
        Path(path).write_text(body)
        os.chmod(path, stat.S_IRWXU)
        return path

    def run_main(self, argv, body=FAKE):
        out = io.StringIO()
        with mock.patch.dict(os.environ, {"SQL_CMD": self.fake(body)}), contextlib.redirect_stdout(out):
            rc = log_state.main(argv)
        return rc, out.getvalue()

    def test_prints_the_json_line_compact_with_the_flags(self):
        rc, out = self.run_main(["mydb"])
        self.assertEqual(rc, 0)
        self.assertEqual(out, '{"args":"-d mydb -v PER_TABLE=1 -y 0 -b","b":2,"a":[1]}\n')

    def test_no_per_table_sets_PER_TABLE_0(self):
        rc, out = self.run_main(["mydb", "--no-per-table"])
        self.assertEqual(rc, 0)
        self.assertIn("PER_TABLE=0", out)

    def test_no_json_is_an_error(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            rc, _ = self.run_main(["mydb"], body="#!/bin/sh\ncat >/dev/null\necho 'no json here'\n")
        self.assertEqual(rc, 1)
        self.assertIn("no JSON from the batch", err.getvalue())

    def test_a_failing_batch_exits_with_its_code(self):
        with contextlib.redirect_stderr(io.StringIO()):
            rc, out = self.run_main(["mydb"], body=FAILING)
        self.assertEqual((rc, out), (1, ""))   # the failed batch's output is not printed

    def test_bad_name_and_usage(self):
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(log_state.main(["bad;name"]), 2)
            self.assertEqual(log_state.main([]), 1)

    def test_the_batch_file_is_the_sibling_sql(self):
        self.assertTrue(log_state.SQL_FILE.name == "sqlserver-log-state.sql" and log_state.SQL_FILE.is_file())


if __name__ == "__main__":
    unittest.main()
