"""test_conventions_check.py: conventions-check.py on a throwaway git repo: a path into another skill's reference that
isn't at the base fails, a missing own reference fails, a long description without ". Use when", a link inside a code
block and a bare reference file name warn, and a clean skill has none of these. Run from this folder:
python3 -m unittest -q test_conventions_check.py"""
import os
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fixtures import git, make_repo, remove, write

CHECK = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts", "conventions-check.py")


def fm(name: str, desc: str, body: str = "") -> str:
    return f'---\nname: {name}\ndescription: "{desc}"\n---\n\n# {name}\n\n{body}'


class ConventionsCheckTest(unittest.TestCase):
    def check(self, root, base):
        p = subprocess.run([sys.executable, CHECK, "skills", "databricks-alpha", "--base", base], cwd=root,  # noqa: PLW1510
                           capture_output=True, text=True)
        return p.returncode, p.stdout + p.stderr

    def test_clean_skill_has_no_findings(self):
        root = make_repo({
            "skills/databricks-beta/SKILL.md": fm("databricks-beta", "Beta things. Use when beta."),
            "skills/databricks-beta/references/one.md": "# One\n",
            "skills/databricks-alpha/SKILL.md": fm("databricks-alpha", "Alpha things. Use when alpha.",
                                                   "- [Steps](references/steps.md)\n- Owner: **databricks-beta** "
                                                   "(`references/one.md`).\n"),
            "skills/databricks-alpha/references/steps.md": "# Steps\n\nSee [Steps](steps.md).\n",
        })
        try:
            base = git(root, "rev-parse", "HEAD")
            rc, out = self.check(root, base)
            self.assertEqual(rc, 0, out)
            for w in ("Use when", "code block", "bare '", "doesn't exist", "isn't at"):
                self.assertNotIn(w, out)
        finally:
            remove(root)

    def test_each_rule_breaks_loudly(self):
        root = make_repo({
            "skills/databricks-beta/SKILL.md": fm("databricks-beta", "Beta things. Use when beta."),
            "skills/databricks-beta/references/one.md": "# One\n",
            "skills/databricks-alpha/SKILL.md": fm("databricks-alpha", "Alpha things. Use when alpha.",
                                                   "- [Steps](references/steps.md)\n"),
            "skills/databricks-alpha/references/steps.md": "# Steps\n",
        })
        try:
            base = git(root, "rev-parse", "HEAD")
            write(root, "skills/databricks-beta/references/two.md", "# Two\n")
            long = "x" * 210
            write(root, "skills/databricks-alpha/SKILL.md", fm(
                "databricks-alpha", long, "- [Steps](references/steps.md)\n- Rotation: **databricks-beta** "
                "(`references/two.md`).\n- Gone: references/gone.md\n"))
            write(root, "skills/databricks-alpha/references/steps.md",
                  "# Steps\n\n```yaml\nname: x  # see steps.md#names\n// [Steps](steps.md)\n```\n")
            rc, out = self.check(root, base)
            self.assertEqual(rc, 1, out)
            self.assertIn("databricks-beta/references/two.md isn't at", out)
            self.assertIn("databricks-alpha/references/gone.md doesn't exist", out)
            self.assertIn("no '. Use when'", out)
            self.assertIn("link inside a code block", out)
            self.assertIn("steps.md:4: bare 'steps.md'", out)
        finally:
            remove(root)


if __name__ == "__main__":
    unittest.main()
