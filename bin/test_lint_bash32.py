"""Tests for lint_scripts.py rule bash32: each check on a line it must report and on bash 3.2 forms it must leave alone."""

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path

_spec = importlib.util.spec_from_file_location("lint_scripts", Path(__file__).with_name("lint_scripts.py"))
ls = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ls)

BAD = [  # one problem each
    "declare -A m",
    "local -A seen",
    "mapfile -t a < f",
    'echo "${x,,}"',
    'echo "${a[-1]}"',
    "cmd &>> log",
    "cmd |& tee log",
    "wait -n",
    "date -d now +%s",
    "date -u -v-1d +%F",
]
GOOD = [  # bash 3.2 forms: no problem
    "date -u +%s",
    "date +%s && grep -d skip f",
    'echo "${a[${#a[@]}-1]}"',
    "cmd 2>&1 | tee log",
    "cmd >> log 2>&1",
    "sed -E 's/(&&|\\|\\||[;|&])/x/g' f",
    "# declare -A in a comment",
    "jq -rn 'now | strftime(\"%F\")'",
]


def problems(body: str) -> list[str]:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "s.sh"
        p.write_text("#!/usr/bin/env bash\n" + body + "\n")
        return ls.bash32(p, str)


class RuleTests(unittest.TestCase):
    def test_each_bash4_line_is_reported_once(self) -> None:
        for line in BAD:
            with self.subTest(line=line):
                self.assertEqual(len(problems(line)), 1, problems(line))

    def test_bash32_forms_are_left_alone(self) -> None:
        for line in GOOD:
            with self.subTest(line=line):
                self.assertEqual(problems(line), [])

    def test_exempt_script_is_skipped(self) -> None:
        self.assertEqual(problems("# bash32: exempt (runs on the VM)\ndeclare -A m"), [])


class EmptyArrayTests(unittest.TestCase):
    EXPAND = 'A=()\nfor x in "${A[@]}"; do :; done'

    def test_reported_under_each_spelling_of_set_u(self) -> None:
        for s in ("set -euo pipefail", "set -Eeuo pipefail", "  set -u", "set -o nounset"):
            with self.subTest(set=s):
                self.assertEqual(len(problems(f"{s}\n{self.EXPAND}")), 1)

    def test_not_reported_without_set_u_or_when_guarded(self) -> None:
        self.assertEqual(problems(f"set -eo pipefail\n{self.EXPAND}"), [])
        self.assertEqual(problems('set -u\nA=()\nfor x in ${A[@]+"${A[@]}"}; do :; done'), [])


if __name__ == "__main__":
    unittest.main()
