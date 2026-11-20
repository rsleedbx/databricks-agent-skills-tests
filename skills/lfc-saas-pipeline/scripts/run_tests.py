#!/usr/bin/env python3
"""run_tests.py: each SaaS connector's bundle and notebook deployed on a workspace, their rows compared; a connector
runs only when what it depends on changed since it last passed.

  uv run --with databricks-sdk --with pyyaml skills/lfc-saas-pipeline/scripts/run_tests.py --profile <p>
      [--only marketo,netsuite] [--again] [--ref <branch>] [--catalog main] [--prefix <name>]

Per connector (a notebooks/<slug>-pipeline.py with its bundles/<slug>/), one at a time:
  1. skip it when its notebook, its bundle folder, the package (scripts/lfc_saas_pipeline), its UC connection's
     updated_at and the workspace are what they were at its last pass (bin/test_skip, runner lfc-saas-pipeline);
  2. the bundle: `databricks bundle deploy` and `bundle run` into <catalog>.<prefix>_<slug>_dab_<epoch>
     (development mode prefixes it with dev_<user>_);
  3. the notebook: imported to your workspace folder lfc-saas-pipeline/, its %pip line pointed at --ref (default: the
     current branch, pushed, so a package change is what's tested), run as a one-time job into
     <prefix>_<slug>_nb_<epoch>, then its pipeline's first update waited for;
  4. rows per source table in both destinations: a pass is both updates COMPLETED and equal counts.
Each result prints as it comes and goes to the ledger; --again runs every connector. What it creates stays (the
schemas, pipelines and bundle deployment carry the epoch); delete them when done.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

SKILL = Path(__file__).resolve().parents[1]
REPO = SKILL.parents[1]
sys.path.insert(0, str(SKILL / "scripts"))
sys.path.insert(0, str(REPO / "bin/test_skip/scripts"))
import test_skip  # noqa: E402

RUNNER = "lfc-saas-pipeline"
ARCHIVE = "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/{ref}.zip"


def connectors(only: str | None) -> list[str]:
    slugs = sorted(p.name.removesuffix("-pipeline.py") for p in (SKILL / "notebooks").glob("*-pipeline.py"))
    if only:
        want = [s.strip() for s in only.split(",")]
        unknown = [s for s in want if s not in slugs]
        if unknown:
            sys.exit(f"no connector {unknown}; connectors: {slugs}")
        slugs = want
    return [s for s in slugs if (SKILL / "bundles" / s).is_dir()]


def cli(profile: str, *args: str, check: bool = True, cwd: Path | None = None) -> str:
    r = subprocess.run(["databricks", *args, "--profile", profile], capture_output=True, text=True, cwd=cwd)
    if check and r.returncode != 0:
        raise RuntimeError(f"databricks {' '.join(args[:3])}: {(r.stderr or r.stdout).strip().splitlines()[-1][:300]}")
    return r.stdout


def connection_of(slug: str) -> str:
    """The connection the bundle defaults to (databricks.yml variables.connection.default)."""
    text = (SKILL / "bundles" / slug / "databricks.yml").read_text()
    m = re.search(r"connection:\s*\n(?:\s+\w+:.*\n)*?\s+default:\s*(\S+)", text)
    return m.group(1) if m else ""


def tables_of(slug: str) -> list[str]:
    text = "".join(p.read_text() for p in sorted((SKILL / "bundles" / slug / "resources").glob("*.yml")))
    return re.findall(r"source_table: ([a-z_0-9]+)", text)


def pipeline_key(slug: str) -> str:
    """The bundle's pipeline resource key (resources: pipelines: <key>:)."""
    text = "".join(p.read_text() for p in sorted((SKILL / "bundles" / slug / "resources").glob("*.yml")))
    return re.search(r"\n  pipelines:\n    ([\w-]+):", text).group(1)


def current_ref() -> str:
    r = subprocess.run(["git", "-C", str(REPO), "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True, text=True)
    return r.stdout.strip() or "main"


def counts(profile: str, catalog: str, schema: str, tables: list[str]) -> dict[str, int | None]:
    out: dict[str, int | None] = {}
    for t in tables:
        try:
            rows = json.loads(cli(profile, "experimental", "aitools", "tools", "query",
                                  f"SELECT count(*) AS n FROM `{catalog}`.`{schema}`.`{t}`", "-o", "json"))
            out[t] = int(rows[0]["n"])
        except Exception:  # noqa: BLE001  a table that didn't land
            out[t] = None
    return out


def run_bundle(profile: str, slug: str, schema: str) -> tuple[str, str]:
    """Deploy and run the bundle; (destination schema as created, update result)."""
    b = SKILL / "bundles" / slug
    cli(profile, "bundle", "deploy", "--var", f"schema={schema}", cwd=b)
    r = subprocess.run(["databricks", "bundle", "run", pipeline_key(slug), "--var", f"schema={schema}",
                        "--profile", profile], capture_output=True, text=True, cwd=b)
    summary = json.loads(cli(profile, "bundle", "summary", "--var", f"schema={schema}", "-o", "json", cwd=b))
    created = next(iter(summary["resources"]["schemas"].values()))["name"]
    state = "COMPLETED" if r.returncode == 0 and "is COMPLETED" in (r.stdout + r.stderr) else "FAILED"
    return created, state


def run_notebook(profile: str, slug: str, schema: str, ref: str) -> str:
    """Import the notebook (its %pip line at ref), run it as a one-time job, wait for its pipeline's update."""
    me = json.loads(cli(profile, "current-user", "me", "-o", "json"))["userName"]
    path = f"/Users/{me}/lfc-saas-pipeline/{slug}-pipeline"
    text = (SKILL / "notebooks" / f"{slug}-pipeline.py").read_text().replace(
        ARCHIVE.format(ref="main"), ARCHIVE.format(ref=ref))
    with tempfile.TemporaryDirectory() as d:
        f = Path(d) / "nb.py"
        f.write_text(text)
        cli(profile, "workspace", "mkdirs", f"/Users/{me}/lfc-saas-pipeline")
        cli(profile, "workspace", "import", path, "--file", str(f), "--format", "SOURCE", "--language", "PYTHON",
            "--overwrite")
    run = {"run_name": f"lfc-saas-pipeline test {slug} {schema}",
           "tasks": [{"task_key": "notebook", "notebook_task": {"notebook_path": path,
                                                                "base_parameters": {"schema": schema}}}]}
    out = json.loads(cli(profile, "jobs", "submit", "--json", json.dumps(run), "-o", "json", check=False) or "{}")
    if (out.get("state") or {}).get("result_state") != "SUCCESS":
        return "FAILED (notebook job)"
    pipes = json.loads(cli(profile, "pipelines", "list-pipelines", "--filter", f"name LIKE '{schema}'", "-o", "json"))
    if not pipes:
        return "FAILED (no pipeline)"
    pid = pipes[0]["pipeline_id"]
    for _ in range(240):   # up to an hour for the first update
        st = (json.loads(cli(profile, "pipelines", "get", pid, "-o", "json")).get("latest_updates") or [{}])[0]
        if st.get("state") in ("COMPLETED", "FAILED", "CANCELED"):
            return st["state"]
        time.sleep(15)
    return "TIMEOUT"


def test_one(a: argparse.Namespace, slug: str, host: str) -> bool | None:
    """One connector: None when skipped, else whether it passed."""
    conn = connection_of(slug)
    updated = json.loads(cli(a.profile, "connections", "get", conn, "-o", "json", check=False) or "{}").get("updated_at")
    fp = test_skip.fingerprint([SKILL / "notebooks" / f"{slug}-pipeline.py", SKILL / "bundles" / slug,
                                SKILL / "scripts/lfc_saas_pipeline"], [host, f"{conn}@{updated}"])
    skip, last = test_skip.should_skip(RUNNER, slug, fp, a.again)
    if skip:
        print(f"  skip  {slug}: passed {last['when']}, unchanged since")
        return None
    if updated is None:
        print(f"  FAIL  {slug}: connection {conn} isn't in this workspace's metastore")
        test_skip.record(RUNNER, slug, fp, False)
        return False
    epoch, tables = int(time.time()), tables_of(slug)
    base = f"{a.prefix}_{slug.replace('-', '_')}"
    dab_schema, dab_state = run_bundle(a.profile, slug, f"{base}_dab_{epoch}")
    nb_schema = f"{base}_nb_{epoch}"
    nb_state = run_notebook(a.profile, slug, nb_schema, a.ref)
    dab_n, nb_n = counts(a.profile, a.catalog, dab_schema, tables), counts(a.profile, a.catalog, nb_schema, tables)
    ok = dab_state == "COMPLETED" and nb_state == "COMPLETED" and dab_n == nb_n and None not in dab_n.values()
    rows = ", ".join(f"{t} {dab_n[t]}/{nb_n[t]}" for t in tables)
    print(f"  {'ok  ' if ok else 'FAIL'}  {slug}: bundle {dab_state}, notebook {nb_state}; rows bundle/notebook: {rows}")
    test_skip.record(RUNNER, slug, fp, ok)
    return ok


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--profile", required=True)
    ap.add_argument("--only", help="comma-separated connector slugs (notebooks/<slug>-pipeline.py)")
    ap.add_argument("--again", action="store_true", help="also test connectors unchanged since they passed")
    ap.add_argument("--ref", default="", help="the branch or tag the notebooks pip-install from (default: this one)")
    ap.add_argument("--catalog", default="main")
    ap.add_argument("--prefix", default="lfc_saas_pipeline_test")
    a = ap.parse_args(argv)
    a.ref = a.ref or current_ref()
    host = json.loads(cli(a.profile, "auth", "describe", "-o", "json", check=False) or "{}").get("details", {}).get(
        "host", a.profile)
    print(f"lfc-saas-pipeline tests on {host} (notebooks install the package from {a.ref}):")
    results = [test_one(a, s, host) for s in connectors(a.only)]
    ran = [r for r in results if r is not None]
    print(f"{sum(ran)} passed, {len(ran) - sum(ran)} failed, {results.count(None)} skipped (unchanged; --again runs them)")
    return 0 if all(ran) else 1


if __name__ == "__main__":
    sys.exit(main())
