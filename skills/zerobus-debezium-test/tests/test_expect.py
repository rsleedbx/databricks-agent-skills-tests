"""Offline check of debezium-zerobus.sh's short(), the check behind every change-arrival claim (changes-<n>): it is
taken from the script as it is and run in bash with the change log's counts faked. An empty log must be short, a log
with every expected event must not, and a manifest it can't read must be short too (a jq error once made it expect
nothing, so an empty log passed).

    python3 -m unittest skills/zerobus-debezium-test/tests/test_expect.py
"""
import json
import os
import shutil
import subprocess
import tempfile
import unittest

SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts", "debezium-zerobus.sh")


def short_function() -> str:
    """short() as the script defines it: from its first line to the next line that starts at the margin."""
    with open(SCRIPT) as f:
        lines = f.read().splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith("short() {"))
    end = next(i for i in range(start + 1, len(lines)) if lines[i] and not lines[i][0].isspace())
    return "\n".join(lines[start:end])


@unittest.skipUnless(shutil.which("jq"), "needs jq")
class Short(unittest.TestCase):
    def run_short(self, have: str, manifest) -> str:
        with tempfile.TemporaryDirectory() as w:
            path = os.path.join(w, "m1before-1.json")
            with open(path, "w") as f:
                f.write(manifest if isinstance(manifest, str) else json.dumps(manifest))
            body = (f"W={w}; NSRC=1; DESTP=(x shop1)\nhave() {{ printf '%b' '{have}'; }}\n{short_function()}\n"
                    "short 1before")
            return subprocess.run(["bash", "-c", body], capture_output=True, text=True, check=True).stdout

    MANIFEST = {"expect": {"public.orders": {"create": 3, "update": 1, "delete": 1}}}

    def test_an_empty_log_is_short(self):
        self.assertIn("shop1.public.orders create 0 of 3", self.run_short("", self.MANIFEST))

    def test_every_event_arrived(self):
        have = ("shop1.public.orders\\tcreate\\t3\\nshop1.public.orders\\tupdate\\t1\\n"
                "shop1.public.orders\\tdelete\\t1\\n")
        self.assertEqual(self.run_short(have, self.MANIFEST), "")

    def test_an_unreadable_manifest_is_short(self):
        self.assertIn("no expected events", self.run_short("", "not json"))


if __name__ == "__main__":
    unittest.main()
