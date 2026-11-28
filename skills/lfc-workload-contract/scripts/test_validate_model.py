#!/usr/bin/env python3
"""test_validate_model.py: offline checks of validate-model.py (model format 1) on fixtures/model-*.yaml: both indentations
(tables at 0 and at 2 spaces), flow and block columns, a value list over two lines; and each refusal with its reason (a
missing or float version, a missing or doubled row count, a duplicate table or column, a column without a type, a block
scalar, a tab, a flow collection not closed). Prints "all passed" or the failures.
    python3 -I test_validate_model.py
"""
from __future__ import annotations

import os
import subprocess
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
FX = os.path.join(HERE, "fixtures")
VM = os.path.join(HERE, "validate-model.py")


def run_vm(fixture):
    p = subprocess.run([sys.executable, "-I", VM, os.path.join(FX, fixture)], capture_output=True, text=True, check=False)
    return p.returncode, p.stdout + p.stderr


class ValidateModel(unittest.TestCase):
    def passes(self, fixture):
        rc, out = run_vm(fixture)
        self.assertEqual((rc, out), (0, ""), fixture)

    def refused(self, fixture, *words):
        rc, out = run_vm(fixture)
        self.assertEqual(rc, 1, out)
        for w in words:
            self.assertIn(w, out)

    def test_indent0(self):
        self.passes("model-indent0.yaml")

    def test_indent2(self):
        self.passes("model-indent2.yaml")

    def test_folded_value(self):
        self.passes("model-folded.yaml")

    def test_unclosed_quote(self):
        self.refused("model-unclosed-quote.yaml", "quoted string is not closed")

    def test_no_version(self):
        self.refused("model-no-version.yaml", "missing version")

    def test_float_version(self):
        self.refused("model-version-float.yaml", 'needs the string "1.0"')

    def test_no_rows(self):
        self.refused("model-no-rows.yaml", "needs exactly one of")

    def test_both_rows(self):
        self.refused("model-both-rows.yaml", "needs exactly one of")

    def test_text_rows(self):
        self.refused("model-bad-rows.yaml", "row_count must be a whole number")

    def test_no_columns(self):
        self.refused("model-no-columns.yaml", "columns must be a non-empty list")

    def test_duplicate_table(self):
        self.refused("model-dup-table.yaml", "duplicate table name")

    def test_duplicate_column(self):
        self.refused("model-dup-column.yaml", "listed twice")

    def test_no_type(self):
        self.refused("model-no-type.yaml", "has no type")

    def test_block_scalar(self):
        self.refused("model-block-scalar.yaml", "model-block-scalar.yaml:4:", "block scalars")

    def test_tab(self):
        self.refused("model-tab.yaml", "model-tab.yaml:4:", "a tab")

    def test_unclosed_flow(self):
        self.refused("model-unclosed.yaml", "not closed")

    def test_no_file_is_usage(self):
        p = subprocess.run([sys.executable, "-I", VM], capture_output=True, text=True, check=False)
        self.assertEqual(p.returncode, 2)


if __name__ == "__main__":
    unittest.main()
