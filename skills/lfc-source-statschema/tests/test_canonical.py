"""statschema.tools.canonical.convert: a raw collector document (every field it reads: keys, unique, indexes,
defaults, generated columns, most common values, foreign keys with actions, a keyless table) -> the schema and stats
documents in tests/canonical/, as to-canonical.py wrote them before the conversion became a function.

    uv run --no-project --with pyyaml --with sqlglot python -m unittest skills/lfc-source-statschema/tests/test_canonical.py
"""
import json
import os
import sys
import unittest

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
from statschema.tools.canonical import convert  # noqa: E402


class Convert(unittest.TestCase):
    def test_the_documents_are_as_before(self):
        d = os.path.join(HERE, "canonical")
        schema, stats = convert(json.load(open(os.path.join(d, "raw.json"))))
        self.assertEqual(schema, yaml.safe_load(open(os.path.join(d, "schema.yaml"))))
        self.assertEqual(stats, yaml.safe_load(open(os.path.join(d, "stats.yaml"))))


if __name__ == "__main__":
    unittest.main()
