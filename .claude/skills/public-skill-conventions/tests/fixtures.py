"""fixtures.py: a throwaway git repo for the contributor checks (conventions-check.py, moved-lines.py), built from a dict
of relative path -> text, committed, and removed by the caller's tearDown."""
import os
import shutil
import subprocess
import tempfile


def make_repo(files: dict) -> str:
    root = tempfile.mkdtemp()
    subprocess.run(["git", "init", "-q", root], check=True)
    for rel, text in files.items():
        path = os.path.join(root, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as fh:
            fh.write(text)
    git(root, "add", "-A")
    git(root, "commit", "-qm", "base")
    return root


def git(root: str, *args: str) -> str:
    return subprocess.run(["git", "-C", root, "-c", "user.name=t", "-c", "user.email=t@t", *args], check=True,
                          capture_output=True, text=True).stdout.strip()


def write(root: str, rel: str, text: str) -> None:
    path = os.path.join(root, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        fh.write(text)


def remove(root: str) -> None:
    shutil.rmtree(root, ignore_errors=True)
