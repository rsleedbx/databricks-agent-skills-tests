"""Offline tests of lfc_source_datagen.tpc_schema: the text is what tpc-schema.sh wrote before it ran this (sha-256 of
its output, taken from the shell version at TPC-H 0.1 and 3, TPC-B 1, and TPC-B 2 with 777 history rows), but for
TPC-B history's lfc:n, which now reads the row number from h_time (so a signal can rewrite h_delta), and the
lfc:sf_rows tags; tpc-schema.sh still prints the same; the scale-factor floor; an unknown model.

  python3 -m unittest test_tpc_schema.py
"""
import hashlib
import re
import os
import subprocess
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from lfc_source_datagen.tpc_schema import tpc_schema

SHELL = {("tpch", 0.1, None): "e60755f6499d7a307affffdda2c19d8001d6040603c9123dfa33f96426629c96", ("tpcb", 1, None): "598597c1b985d9b73df556671f78334b41f4b02ca54614b911ce62a6319bfed3", ("tpch", 3, None): "0b165088a11e1f774ab1ffc219e1b6926e0db18237151a56e938b3fd26291903", ("tpcb", 2, 777): "d6687d58e9718e04b2e69e1332e7036bbeb950cfcd2fd017a60ba55f66084fa8"}
# the changes since the shell version: TPC-B history's row number from h_time, not h_delta; every scaling table's
# lfc:sf_rows (its rows per scale factor, for an append by scale factor)
CHANGED = {"tpcb": ("lfc:n={LFC_EPOCH(h_time) - 1700000000}", "lfc:n={CAST(h_delta * 100 AS SIGNED)}")}
SF_ROWS = re.compile(r" lfc:sf_rows=[0-9.]+")


def sha(text):
    return hashlib.sha256(SF_ROWS.sub("", text).encode()).hexdigest()


class TpcSchema(unittest.TestCase):
    def test_same_text_as_the_shell_version(self):
        for (model, sf, hist), want in SHELL.items():
            text = tpc_schema(model, sf, hist)
            if model in CHANGED:
                now, was = CHANGED[model]
                self.assertEqual(text.count(now), 1, model)
                text = text.replace(now, was)
            self.assertEqual(sha(text), want, (model, sf, hist))

    def test_scaling_tables_say_their_rows_per_sf(self):
        b = tpc_schema("tpcb", 2, 777)
        self.assertEqual(re.findall(r"lfc:sf_rows=([0-9.]+)", b), ["1", "10", "100000", "388.5"])
        h = tpc_schema("tpch", 0.1)
        self.assertEqual(len(re.findall(r"lfc:sf_rows=", h)), 6)          # all but region and nation, which don't scale
        self.assertNotRegex(h, r"lfc:rows=(5|25) lfc:sf_rows")

    def test_tpc_schema_sh_runs_it(self):
        out = subprocess.run(["bash", os.path.join(HERE, "tpc", "tpc-schema.sh"), "tpch", "0.1"], capture_output=True,
                             text=True, check=True).stdout
        self.assertEqual(sha(out), SHELL[("tpch", 0.1, None)])

    def test_floor_and_unknown(self):
        with self.assertRaisesRegex(ValueError, "0.03 or more"):
            tpc_schema("tpch", 0.01)
        with self.assertRaisesRegex(ValueError, "tpcb or tpch"):
            tpc_schema("tpcc", 1)
        r = subprocess.run(["bash", os.path.join(HERE, "tpc", "tpc-schema.sh"), "tpch", "0.01"], capture_output=True, text=True, check=False)
        self.assertEqual((r.returncode, r.stderr.strip()),
                         (1, "tpc-schema.sh: TPC-H needs scale factor 0.03 or more (230 suppliers); got 0.01"))


if __name__ == "__main__":
    unittest.main()
