"""Tests for shape.py step 3 (order between columns) on a fixture table: an offset rule, a rejected offset rule, and the
GREATEST fallback. Each shaped formula is evaluated for every row (the formulas are integer arithmetic on x.n), so the
checks are on the values a fill would write, not on the formula text.

    python3 -m unittest bin/test_shape_order.py      (needs uv: shape.py runs with uv run)
"""

from __future__ import annotations

import re
import subprocess
import tempfile
import unittest
from pathlib import Path

SHAPE = Path(__file__).resolve().parent.parent / "skills/lfc-source-statschema/scripts/shape.py"
ROWS = 500
DATE = {"type": "date", "not_null": True}

SCHEMA = f"""version: "1.0"
tables:
  - name: t
    row_count_per_sf: {ROWS}
    temporal_ordering_constraints: ["a <= b", "a < c", "d >= a", "a < e"]
    columns:
      - {{name: id, type: integer, not_null: true, primary_key: true, generation: {{distribution: sequential, min_value: 1, unique: true}}}}
      - {{name: a, type: date, not_null: true, generation: {{distribution: uniform, min_value: "1995-01-01", max_value: "1995-12-31"}}}}
      - {{name: b, type: date, not_null: true, generation: {{distribution: offset, distribution_params: {{from: a, min: 1, max: 30}}}}}}
      - {{name: c, type: date, not_null: true, generation: {{distribution: uniform, min_value: "1994-01-01", max_value: "1996-12-31"}}}}
      - {{name: d, type: date, not_null: true, generation: {{distribution: uniform, min_value: "1994-01-01", max_value: "1996-12-31"}}}}
      - name: e
        type: date
        not_null: true
        description: "an offset from a with max < min: rejected, so e keeps its formula from the statistics and a < e needs GREATEST"
        generation: {{distribution: offset, distribution_params: {{from: a, min: 5, max: 1}}}}
"""

# statistics for e only: they give e a formula of its own before step 3 (a rejected offset rule leaves it in place)
STATS = f"""tables:
  - name: t
    row_count: {ROWS}
    columns:
      - {{name: e, n_distinct: 900, min_value: "1994-01-01", max_value: "1996-12-31"}}
"""


def shape() -> tuple[dict[str, str], str]:
    """Shape the fixture: {column: formula} from the lfc:v comments, and shape.py's log."""
    with tempfile.TemporaryDirectory() as d:
        src, st = Path(d, "s.yaml"), Path(d, "stats.yaml")
        src.write_text(SCHEMA)
        st.write_text(STATS)
        r = subprocess.run(["uv", "run", "-q", str(SHAPE), str(src), "--stats", str(st), "--sf", "1",
                            "--expected", str(Path(d, "e.json"))],
                           capture_output=True, text=True, check=True)
    f = dict(re.findall(r"- name: (\w+)\n(?:    .*\n)*?    comment: lfc:v=\{(.*)\}", r.stdout))
    return f, r.stderr


def value(formula: str, n: int) -> int:
    """The formula's value for row n: SQL integer arithmetic (operands are non-negative, so % and DIV match Python's)."""
    py = formula.replace("x.n", str(n)).replace(" DIV ", " // ").replace("GREATEST", "max")
    assert re.fullmatch(r"[\d\s()+\-*/%,max]+", py), py
    return eval(py)  # digits, operators and max only (checked above)


class ShapeOrder(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.f, cls.log = shape()
        cls.v = {c: [value(cls.f[c], n) for n in range(1, ROWS + 1)] for c in "abcde"}

    def test_offset_spread(self):
        """b = a + 1 .. 30: every row in range, every offset used (no off-by-one in the draw)."""
        gaps = [b - a for a, b in zip(self.v["a"], self.v["b"])]
        self.assertEqual((min(gaps), max(gaps)), (1, 30))
        self.assertEqual(len(set(gaps)), 30)
        self.assertIn("a <= b holds", self.log)

    def test_greatest_strict(self):
        """a < c by GREATEST: c >= a + 1 on every row, and c keeps its own value where it already was."""
        self.assertTrue(self.f["c"].startswith("GREATEST("), self.f["c"])
        self.assertTrue(all(c > a for a, c in zip(self.v["a"], self.v["c"])))
        self.assertTrue(any(c > a + 1 for a, c in zip(self.v["a"], self.v["c"])))

    def test_greatest_not_strict(self):
        """d >= a by GREATEST(d, a + 0): ties allowed."""
        self.assertTrue(self.f["d"].endswith("+ 0)"), self.f["d"])
        self.assertTrue(all(d >= a for a, d in zip(self.v["a"], self.v["d"])))

    def test_rejected_offset_falls_back(self):
        """An offset rule shape.py rejects doesn't count as keeping the order: a < e still holds, through GREATEST."""
        self.assertIn("e: offset from a needs", self.log)
        self.assertTrue(self.f["e"].startswith("GREATEST("), self.f["e"])
        self.assertTrue(all(e > a for a, e in zip(self.v["a"], self.v["e"])))


if __name__ == "__main__":
    unittest.main()
