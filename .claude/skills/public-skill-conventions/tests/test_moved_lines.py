"""test_moved_lines.py: moved-lines.py on a throwaway git repo: a line moved to a sibling skill is kept, a dropped `term`
is reported, the same word in an unrelated line of another skill doesn't count as kept, --lines lists a dropped prose
line, and --links lists other skills' lines naming the skill but not a longer skill name. Run from this folder:
python3 -m unittest -q test_moved_lines.py"""
import os
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fixtures import git, make_repo, remove, write

CHECK = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts", "moved-lines.py")


def run(root, *args):
    p = subprocess.run([sys.executable, CHECK, "skills", "alpha", *args], cwd=root, capture_output=True, text=True, check=False)
    return p.returncode, p.stdout + p.stderr


class MovedLinesTest(unittest.TestCase):
    def setUp(self):
        self.root = make_repo({
            "skills/alpha/SKILL.md": "# Alpha\n\n- The pipeline sets `channel: PREVIEW` for the preview runtime.\n"
                                     "- Keep the gateway running while the source keeps its change log.\n",
            "skills/alpha/references/issues.md": "| Symptom | Fix |\n|---|---|\n| OAuth connection stuck in `PENDING` "
                                                 "| Finish the browser sign-in |\n",
            "skills/beta/SKILL.md": "# Beta\n",
            "skills/gamma/SKILL.md": "# Gamma\n\n- A job run is `PENDING` while its cluster starts.\n"
                                     "- See alpha for pull connectors.\n- See alpha-extra for something else.\n",
        })
        self.base = git(self.root, "rev-parse", "HEAD")
        write(self.root, "skills/beta/SKILL.md",
              "# Beta\n\n- The pipeline sets `channel: PREVIEW` for the preview runtime.\n")
        write(self.root, "skills/alpha/SKILL.md", "# Alpha\n\nSee beta for the runtime.\n")
        os.remove(os.path.join(self.root, "skills/alpha/references/issues.md"))

    def tearDown(self):
        remove(self.root)

    def test_dropped_term_reported_moved_term_not(self):
        rc, out = run(self.root, "--base", self.base)
        self.assertEqual(rc, 1, out)
        self.assertIn("PENDING", out)
        self.assertNotIn("channel: PREVIEW", out)

    def test_lines_lists_the_dropped_prose_line_only(self):
        _, out = run(self.root, "--base", self.base, "--lines")
        section = out[out.index("lines with no close match"):]
        self.assertIn("Keep the gateway running", section)
        self.assertNotIn("sets `channel", section)

    def test_links_lists_other_skills_lines_naming_the_skill_not_longer_names(self):
        _, out = run(self.root, "--base", self.base, "--links")
        self.assertIn("gamma/SKILL.md:4", out)
        self.assertNotIn("alpha-extra", out)

    def test_row_moved_back_means_no_term_is_gone(self):
        with open(os.path.join(self.root, "skills/beta/SKILL.md"), "a") as fh:
            fh.write("| OAuth connection stuck in `PENDING` | Finish the browser sign-in |\n")
        rc, out = run(self.root, "--base", self.base)
        self.assertEqual(rc, 0, out)


if __name__ == "__main__":
    unittest.main()
