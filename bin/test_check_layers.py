"""bin/test_check_layers.py: bin/check_layers.py on a fixture tree (python3 -m unittest -q bin/test_check_layers.py)."""
from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("check_layers", HERE / "check_layers.py")
cl = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cl)

LAYERS = """\
# a fixture: four layers
layers:
  common:
    may_use: []
    skills:
      - util
  shape:
    may_use: [common]
    skills:
      - gen
  lakeflow:
    may_use: [common]
    skills:
      - flow
    pending:
      - later
    bin:
      - run.sh
  sources:
    may_use: [shape, common]
    skills:
      - db
  repo:
    may_use: [all]
    bin:
      - check.py
"""

FILES = {
    "skills/util/SKILL.md": "# util\n",
    "skills/gen/SKILL.md": "# gen\n",
    "skills/flow/SKILL.md": "# flow\n",
    "skills/db/SKILL.md": "# db\n",
    "bin/run.sh": "#!/bin/bash\n# run.sh: a fixture\n",
    "bin/check.py": "# check.py\n",
}


def tree(files: dict, layers: str = LAYERS, baseline: str | None = None) -> Path:
    root = Path(tempfile.mkdtemp())
    (root / "layers.yaml").write_text(layers)
    for rel, text in {**FILES, **files}.items():
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text(text)
    if baseline is not None:
        (root / "layers-baseline.txt").write_text(baseline)
    return root


def run(root: Path):
    model = cl.load_model(root)
    cl.check_inventory(model, root)
    edges, problems, stale, lines = cl.evaluate(model, root)
    return edges, model.problems + problems, stale, lines


class ParseYaml(unittest.TestCase):
    def test_block_and_flow_lists_and_comments(self):
        got = cl.parse_yaml("layers:\n  a:\n    may_use: [x, y]  # comment\n    skills:\n      - one\n      - two\n"
                            "  b:\n    may_use: []\n")
        self.assertEqual(got, {"layers": {"a": {"may_use": ["x", "y"], "skills": ["one", "two"]},
                                          "b": {"may_use": []}}})


class Edges(unittest.TestCase):
    def test_clean_tree_passes_and_reports_allowed_edges(self):
        root = tree({
            "skills/flow/SKILL.md": "uses ../util/x and skills/util/y and $SK/util/z and util/scripts/a.sh\n",
            "skills/db/scripts/a.sh": '. "$SK/gen/x.sh"\n',
            "bin/run.sh": "# run.sh: the fixture's own text\n",
        })
        edges, problems, stale, _ = run(root)
        self.assertEqual(problems, [])
        self.assertEqual(stale, [])
        self.assertGreaterEqual(sum(e["allowed"] for e in edges), 5)

    def test_tool_caches_are_not_skills(self):
        root = tree({})
        for d in ("skills/.ruff_cache", "skills/__pycache__"):
            (root / d).mkdir()
        _, problems, _, _ = run(root)
        self.assertEqual(problems, [])

    def test_each_path_form_is_found(self):
        root = tree({"skills/flow/SKILL.md": "../../util/a and skills/util and $SK/util/b and ${SK}/util/c "
                                             "and util/scripts/d and .claude/skills/util/e\n"})
        edges, _, _, _ = run(root)
        found = {(e["from"], e["to"]) for e in edges}
        self.assertEqual(found, {("flow", "util")})
        self.assertEqual(len(edges), 6)

    def test_bare_name_in_prose_is_not_an_edge(self):
        root = tree({"skills/flow/SKILL.md": "See util for the rules, and db too.\n"})
        edges, problems, _, _ = run(root)
        self.assertEqual(edges, [])
        self.assertEqual(problems, [])

    def test_self_reference_is_not_an_edge(self):
        root = tree({"skills/flow/SKILL.md": "flow/scripts/x.sh\n"})
        edges, _, _, _ = run(root)
        self.assertEqual(edges, [])

    def test_disallowed_edge_fails_and_names_the_fix(self):
        root = tree({"skills/util/scripts/b.sh": "source ../../flow/x.sh\n"})
        _, problems, _, lines = run(root)
        self.assertEqual(len(problems), 1)
        self.assertIn("disallowed edge util (common) -> flow (lakeflow)", problems[0])
        self.assertIn("util -> flow: skills/util/scripts/b.sh:flow/x.sh", problems[0])
        self.assertEqual(lines, ["util -> flow: skills/util/scripts/b.sh:flow/x.sh"])

    def test_lakeflow_may_not_use_sources_or_repo(self):
        root = tree({"skills/flow/SKILL.md": "db/scripts/x.sh and bin/check.py\n"})
        _, problems, _, _ = run(root)
        self.assertEqual(len(problems), 2)

    def test_sources_may_use_shape(self):
        root = tree({"skills/db/SKILL.md": "gen/scripts/x.py\n"})
        _, problems, _, _ = run(root)
        self.assertEqual(problems, [])

    def test_bin_scripts_are_classified_too(self):
        root = tree({"bin/run.sh": "# run.sh\nbash skills/db/x.sh\n"})
        _, problems, _, _ = run(root)
        self.assertEqual(len(problems), 1)
        self.assertIn("bin/run.sh (lakeflow) -> db (sources)", problems[0])


class Baseline(unittest.TestCase):
    BAD = {"skills/util/scripts/b.sh": "source ../../flow/x.sh\n"}

    def test_baselined_edge_passes(self):
        root = tree(self.BAD, baseline="util -> flow: skills/util/scripts/b.sh:flow/x.sh\n")
        _, problems, stale, _ = run(root)
        self.assertEqual((problems, stale), ([], []))

    def test_pattern_must_be_on_the_line(self):
        root = tree(self.BAD, baseline="util -> flow: skills/util/scripts/b.sh:nothing-like-this\n")
        _, problems, stale, _ = run(root)
        self.assertEqual(len(problems), 1)
        self.assertEqual(len(stale), 1)

    def test_stale_entry_fails_and_says_delete(self):
        root = tree({}, baseline="util -> flow: skills/util/scripts/b.sh:flow/x.sh\n")
        _, problems, stale, _ = run(root)
        self.assertEqual(problems, [])
        self.assertEqual(len(stale), 1)
        self.assertIn("delete it", stale[0])

    def test_comments_and_blank_lines_are_ignored(self):
        root = tree(self.BAD, baseline="# note\n\nutil -> flow: skills/util/scripts/b.sh:flow/x.sh\n")
        _, problems, stale, _ = run(root)
        self.assertEqual((problems, stale), ([], []))

    def test_malformed_line_is_an_error(self):
        root = tree({}, baseline="this is not an entry\n")
        with self.assertRaises(SystemExit):
            run(root)


class Inventory(unittest.TestCase):
    def test_unknown_skill_fails(self):
        root = tree({"skills/newone/SKILL.md": "# new\n"})
        _, problems, _, _ = run(root)
        self.assertTrue(any("unknown skill newone: add it to layers.yaml" in p for p in problems))

    def test_unknown_bin_script_fails(self):
        root = tree({"bin/new.sh": "# new.sh\n"})
        _, problems, _, _ = run(root)
        self.assertTrue(any("unknown bin script new.sh" in p for p in problems))

    def test_listed_skill_that_is_missing_fails(self):
        layers = LAYERS.replace("      - util\n", "      - util\n      - ghost\n")
        _, problems, _, _ = run(tree({}, layers=layers))
        self.assertTrue(any("layers.yaml lists ghost" in p for p in problems))

    def test_pending_skill_may_be_absent_or_present(self):
        _, problems, _, _ = run(tree({}))
        self.assertEqual(problems, [])
        _, problems, _, _ = run(tree({"skills/later/SKILL.md": "# later\n"}))
        self.assertEqual(problems, [])

    def test_a_name_in_two_layers_fails(self):
        layers = LAYERS.replace("      - gen\n", "      - gen\n      - util\n")
        _, problems, _, _ = run(tree({}, layers=layers))
        self.assertTrue(any("listed in two layers" in p for p in problems))


class Paths(unittest.TestCase):
    def test_a_layers_paths_are_its_existing_skills_and_bin_files(self):
        root = tree({"skills/later/SKILL.md": "# later\n"})
        model = cl.load_model(root)
        self.assertEqual(cl.layer_paths(model, root, ["lakeflow"]),
                         ["bin/run.sh", "skills/flow", "skills/later"])
        self.assertEqual(cl.layer_paths(model, root, ["common", "shape"]), ["skills/gen", "skills/util"])
        with self.assertRaises(SystemExit):
            cl.layer_paths(model, root, ["nope"])


if __name__ == "__main__":
    unittest.main()
