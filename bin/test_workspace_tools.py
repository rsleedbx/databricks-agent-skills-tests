"""Offline tests for the workspace file tools (workspace_files, genie_skills_sync, sync): a fake REST answer for
the Workspace API, and a throwaway repo for the Genie skills copy (its commit, its note after the frontmatter, the
marker)."""
from __future__ import annotations

import base64
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "skills" / "lfc-rest" / "scripts"))
from lfc_rest import Rest  # noqa: E402

import genie_skills_sync  # noqa: E402
import sync  # noqa: E402
import workspace_files as wf  # noqa: E402


class FakeWorkspace:
    """A Rest over an in-memory workspace: objects by path, every import recorded."""

    def __init__(self, objects=None):
        self.objects = dict(objects or {})
        self.imports = []
        self.mkdirs = []
        self.deleted = []
        self.api = Rest(self.do, name="fake")

    def do(self, method, path, query, body, **kw):
        q = query or {}
        if path == "/api/2.0/workspace/get-status":
            return self.get_status(q["path"])
        if path == "/api/2.0/workspace/list":
            return {"objects": [o for o in self.objects.values() if os.path.dirname(o["path"]) == q["path"]]}
        if path == "/api/2.0/workspace/import":
            self.imports.append(body)
            self.objects[body["path"]] = {"path": body["path"], "object_type": "FILE"}
            return {}
        if path == "/api/2.0/workspace/mkdirs":
            self.mkdirs.append(body["path"])
            return {}
        if path == "/api/2.0/workspace/delete":
            self.deleted.append(body["path"])
            return {}
        if path == "/api/2.0/workspace/export":
            return {"content": base64.b64encode(self.objects[q["path"]].get("data", b"")).decode()}
        raise AssertionError(f"unexpected {method} {path}")

    def get_status(self, path):
        if path not in self.objects:
            raise RuntimeError("RESOURCE_DOES_NOT_EXIST: no such path")
        return self.objects[path]


class WorkspaceFiles(unittest.TestCase):
    def test_put_get_and_status(self):
        w = FakeWorkspace()
        wf.put(w.api, "/Workspace/x/a.py", b"print(1)\n", fmt="SOURCE", language="PYTHON")
        self.assertEqual(w.imports[0]["format"], "SOURCE")
        self.assertEqual(w.imports[0]["language"], "PYTHON")
        self.assertEqual(base64.b64decode(w.imports[0]["content"]), b"print(1)\n")
        w.objects["/Workspace/x/a.py"]["data"] = b"hello"
        self.assertEqual(wf.get(w.api, "/Workspace/x/a.py"), b"hello")
        self.assertIsNone(wf.status(w.api, "/Workspace/x/missing"))
        self.assertEqual(wf.status(w.api, "/Workspace/x/a.py")["object_type"], "FILE")


class GenieNote(unittest.TestCase):
    def test_note_goes_after_the_frontmatter(self):
        text = "---\nname: x\n---\n# Body\n"
        self.assertEqual(genie_skills_sync.with_note(text, "NOTE\n"), "---\nname: x\n---\n\nNOTE\n# Body\n")

    def test_no_frontmatter_leaves_the_text(self):
        self.assertEqual(genie_skills_sync.with_note("# no frontmatter\n", "NOTE\n"), "# no frontmatter\n")


class GenieSync(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        skill = self.root / "skills" / "a"
        (skill / "scripts").mkdir(parents=True)
        (skill / "SKILL.md").write_text("---\nname: a\n---\n# A\nSee [b](../b/SKILL.md).\n")
        (skill / "scripts" / "x.py").write_text("print('x')\n")
        (self.root / "skills" / "b").mkdir()
        (self.root / "skills" / "b" / "SKILL.md").write_text("---\nname: b\n---\n")
        (self.root / "skills" / "only").mkdir()   # a listed skill whose only tracked file is its SKILL.md
        (self.root / "skills" / "only" / "SKILL.md").write_text("---\nname: only\n---\n# Only\n")
        (self.root / "genie-skill-note.md").write_text("In Genie Code or a notebook: run the Python here.\n")
        (self.root / "genie-skills.txt").write_text("# listed\na\nonly\n")
        for cmd in (["init", "-q"], ["add", "-A"], ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "c"]):
            subprocess.run(["git", "-C", str(self.root), *cmd], check=True, capture_output=True)
        self.old_root, self.old_connect = genie_skills_sync.R, Rest.connect

    def tearDown(self):
        genie_skills_sync.R = self.old_root
        Rest.connect = self.old_connect
        shutil.rmtree(self.root, ignore_errors=True)

    def test_listed_skills_and_unlisted_links(self):
        self.assertEqual(genie_skills_sync.listed_skills(self.root), ["a", "only"])
        (self.root / "genie-skills.txt").write_text("missing\n")
        with self.assertRaises(SystemExit):
            genie_skills_sync.listed_skills(self.root)

    def test_copy_marks_and_notes_the_skill(self):
        ws = FakeWorkspace()
        genie_skills_sync.R = self.root
        Rest.connect = staticmethod(lambda profile: ws.api)
        self.assertEqual(genie_skills_sync.main(["p", "--dest", "/Workspace/s"]), 0)
        paths = {i["path"]: i for i in ws.imports}
        skill = base64.b64decode(paths["/Workspace/s/a/SKILL.md"]["content"]).decode()
        self.assertIn("In Genie Code or a notebook", skill)
        self.assertEqual(paths["/Workspace/s/a/SKILL.md"]["format"], "RAW")
        self.assertIn("/Workspace/s/a/scripts/x.py", paths)
        self.assertIn("/Workspace/s/only/.synced-from", paths)   # the folder with only SKILL.md still gets its marker
        self.assertTrue(base64.b64decode(paths["/Workspace/s/a/.synced-from"]["content"]).startswith(
            b"databricks-agent-skills-tests "))

    def test_folder_without_marker_is_left_alone(self):
        ws = FakeWorkspace({"/Workspace/s": {"path": "/Workspace/s", "object_type": "DIRECTORY"},
                            "/Workspace/s/a": {"path": "/Workspace/s/a", "object_type": "DIRECTORY"}})
        genie_skills_sync.R = self.root
        Rest.connect = staticmethod(lambda profile: ws.api)
        self.assertEqual(genie_skills_sync.main(["p", "--dest", "/Workspace/s"]), 0)
        self.assertFalse([i for i in ws.imports if i["path"].startswith("/Workspace/s/a/")])   # not ours: left alone
        self.assertTrue([i for i in ws.imports if i["path"].startswith("/Workspace/s/only/")])


class SyncHelpers(unittest.TestCase):
    def test_strip_header_is_tail_from_line_two(self):
        self.assertEqual(sync.strip_header(b"header\nbody\n"), b"body\n")
        self.assertEqual(sync.strip_header(b"only"), b"")

    def test_default_and_test_file_lists(self):
        repo = Path(tempfile.mkdtemp())
        try:
            (repo / "skills/databricks-lakeflow-connect/references").mkdir(parents=True)
            (repo / "skills/databricks-lakeflow-connect/SKILL.md").write_text("x")
            (repo / "skills/databricks-lakeflow-connect/references/b.md").write_text("x")
            (repo / "skills/databricks-lakeflow-connect/references/a.md").write_text("x")
            self.assertEqual(sync.default_files(repo), [
                "CLAUDE.md", "manifest.json", "skills/databricks-lakeflow-connect/SKILL.md",
                "skills/databricks-lakeflow-connect/references/a.md",
                "skills/databricks-lakeflow-connect/references/b.md"])
            (repo / "run.py").write_text("x")
            (repo / "bin").mkdir()
            (repo / "bin/t.py").write_text("x")
            self.assertEqual(sync.test_files(repo), ["bin/t.py", "run.py"])
        finally:
            shutil.rmtree(repo, ignore_errors=True)

    def test_push_uploads_a_file_as_source(self):
        repo = Path(tempfile.mkdtemp())
        ws = FakeWorkspace()
        old_connect, old_env = Rest.connect, dict(os.environ)
        try:
            (repo / "CLAUDE.md").write_text("# c\n")
            Rest.connect = staticmethod(lambda profile: ws.api)
            os.environ.update({"OMNIGENT_WS": "bridge", "DATABRICKS_WS_USER": "someone",
                               "SKILLS_REPO": str(repo)})
            self.assertEqual(sync.main(["push", "CLAUDE.md"]), 0)
            imp = ws.imports[0]
            self.assertEqual((imp["path"], imp["format"], imp["language"]),
                             ("/Users/someone/skill-files/CLAUDE.md", "SOURCE", "PYTHON"))
        finally:
            Rest.connect = old_connect
            os.environ.clear()
            os.environ.update(old_env)
            shutil.rmtree(repo, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
