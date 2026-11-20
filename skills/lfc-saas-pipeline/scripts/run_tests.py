#!/usr/bin/env python3
"""run_tests.py: each SaaS connector's bundle and notebook deployed on a workspace, their rows compared; a connector
runs only when what it depends on changed since it last passed.

  uv run --with databricks-sdk --with pyyaml skills/lfc-saas-pipeline/scripts/run_tests.py --profile <p>
      [--only marketo,netsuite] [--again] [--ref <branch>] [--catalog main] [--prefix <name>]

Per connector (a notebooks/<slug>-pipeline.py with its bundles/<slug>/), one at a time:
  1. skip it when its notebook, its bundle folder, the package (scripts/lfc_saas_pipeline), its UC connection's
     updated_at and the workspace are what they were at its last pass (bin/test_skip, runner lfc-saas-pipeline);
  2. the bundle: `databricks bundle deploy` and `bundle run` from a copy named <bundle>_<epoch> (its own deployment,
     so nothing deployed earlier is replaced) into <catalog>.<prefix>_<slug>_dab_<epoch> (development mode prefixes
     it with dev_<user>_);
  3. the notebook: imported to your workspace folder lfc-saas-pipeline/, its %pip line pointed at --ref (default: the
     current branch, pushed, so a package change is what's tested), run as a one-time job into
     <prefix>_<slug>_nb_<epoch>, then its pipeline's first update waited for;
  4. rows per source table in both destinations: a pass is both updates COMPLETED and equal counts (an event-log
     table in CURSORS: rows before the bundle run's start, since entries keep arriving between the two runs).
A bundle with a start_datetime variable gets START_DAYS ago in both runs (those connectors default to a year back);
an update still running after UPDATE_TIMEOUT_S is stopped and fails the connector.
Each result prints as it comes and goes to the ledger; --again runs every connector. What it creates stays (the
schemas, pipelines and bundle deployment carry the epoch); delete them when done.
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

SKILL = Path(__file__).resolve().parents[1]
REPO = SKILL.parents[1]
sys.path.insert(0, str(SKILL / "scripts"))
sys.path.insert(0, str(REPO / "bin/test_skip/scripts"))
import test_skip

RUNNER = "lfc-saas-pipeline"
START_DAYS = 7   # start_datetime the tests pass, days back, where the bundle has that variable
UPDATE_TIMEOUT_S = 3600   # a first update longer than this fails the connector (and is stopped)
# Event logs keep getting entries between the two runs: their rows are compared up to the bundle run's start, by
# the table's cursor column (from the connector's manifest; an expression for a non-timestamp one).
CURSORS = {
    "wiz-audit-logs": {"audit_log_entries": "`timestamp`"},
    "atlassian-audit-logs": {"audit_logs": "`time`"},
    "openai": {"audit_logs": "timestamp_seconds(effective_at)"},
}
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
    r = subprocess.run(["databricks", *args, "--profile", profile], capture_output=True, text=True, cwd=cwd, check=False)
    if check and r.returncode != 0:
        err = [ln for ln in (r.stderr or r.stdout).strip().splitlines() if ln.startswith("Error")] or \
            (r.stderr or r.stdout).strip().splitlines()[-1:]
        raise RuntimeError(f"databricks {' '.join(args[:3])}: {err[0][:300]}")
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


def pushed_state(ref: str) -> tuple[str, str]:
    """(the package folder's tree id at ref on GitHub, so a push that doesn't touch the package keeps passes; why the
    local package differs from what the notebook installs: '' if it doesn't)."""
    def git(*x: str) -> subprocess.CompletedProcess:
        return subprocess.run(["git", "-C", str(REPO), *x], capture_output=True, text=True, check=False)

    remote = (git("ls-remote", "origin", f"refs/heads/{ref}").stdout.split() or [""])[0]
    pkg = str((SKILL / "scripts/lfc_saas_pipeline").relative_to(REPO))
    if git("status", "--porcelain", "--", pkg).stdout.strip():
        return remote, "uncommitted changes"
    if not remote:
        return remote, f"{ref} isn't pushed"
    if git("diff", "--quiet", remote, "HEAD", "--", pkg).returncode != 0:   # 1: differs; other: commit not here
        return remote, f"local commits not on {ref} at GitHub"
    tree = git("rev-parse", f"{remote}:{pkg}").stdout.strip()   # the package folder the notebook installs, not the commit
    return tree or remote, ""


def current_ref() -> str:
    r = subprocess.run(["git", "-C", str(REPO), "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True, text=True,
                       check=False)
    return r.stdout.strip() or "main"


def counts(profile: str, catalog: str, schema: str, tables: list[str], until: dict[str, str],
           cutoff: str) -> dict[str, int | None]:
    """Rows per table; a table in until counts only rows whose cursor is before cutoff (UTC)."""
    out: dict[str, int | None] = {}
    for t in tables:
        where = f" WHERE {until[t]} < timestamp'{cutoff}'" if t in until else ""
        try:
            rows = json.loads(cli(profile, "experimental", "aitools", "tools", "query",
                                  f"SELECT count(*) AS n FROM `{catalog}`.`{schema}`.`{t}`{where}", "-o", "json"))
            out[t] = int(rows[0]["n"])
        except Exception:  # noqa: BLE001  a table that didn't land
            out[t] = None
    return out


def start_vars(slug: str, start: str) -> dict[str, str]:
    """{start_datetime: start} when the bundle has that variable (connectors that backfill a year by default)."""
    return {"start_datetime": start} if "\n  start_datetime:" in (SKILL / "bundles" / slug / "databricks.yml").read_text() \
        else {}


def run_bundle(profile: str, slug: str, catalog: str, schema: str, epoch: int,
               extra: dict[str, str]) -> tuple[str, str]:
    """Deploy and run the bundle from a copy named <bundle>_<epoch>: its own deployment, so no earlier deployment's
    resources are replaced. (destination schema as created, update result)."""
    with tempfile.TemporaryDirectory() as d:
        b = Path(d) / slug
        shutil.copytree(SKILL / "bundles" / slug, b, ignore=shutil.ignore_patterns(".databricks"))
        yml = b / "databricks.yml"
        yml.write_text(re.sub(r"(\nbundle:\n  name: )(\S+)", rf"\g<1>\g<2>_{epoch}", yml.read_text(), count=1))
        v = ["--var", f"schema={schema}", "--var", f"catalog={catalog}"]
        for k, val in extra.items():
            v += ["--var", f"{k}={val}"]
        cli(profile, "bundle", "deploy", *v, cwd=b)
        try:
            r = subprocess.run(["databricks", "bundle", "run", pipeline_key(slug), *v, "--profile", profile],
                               capture_output=True, text=True, cwd=b, check=False, timeout=UPDATE_TIMEOUT_S)
            out = r.stdout + r.stderr if r.returncode == 0 else ""
        except subprocess.TimeoutExpired:
            out = None
        summary = json.loads(cli(profile, "bundle", "summary", *v, "-o", "json", cwd=b))
    created = next(iter(summary["resources"]["schemas"].values()))["name"]
    if out is None:   # stop the update, so a timed-out test doesn't keep running
        pid = next(iter(summary["resources"]["pipelines"].values()))["id"]
        cli(profile, "pipelines", "stop", pid, "--no-wait", check=False)
        return created, "TIMEOUT"
    return created, "COMPLETED" if "is COMPLETED" in out else "FAILED"


def run_notebook(profile: str, slug: str, catalog: str, schema: str, ref: str, extra: dict[str, str]) -> str:
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
                                                                "base_parameters": {"schema": schema,
                                                                                    "catalog": catalog, **extra}}}]}
    out = json.loads(cli(profile, "jobs", "submit", "--json", json.dumps(run), "-o", "json", check=False) or "{}")
    if (out.get("state") or {}).get("result_state") != "SUCCESS":
        return "FAILED (notebook job)"
    pipes = json.loads(cli(profile, "pipelines", "list-pipelines", "--filter", f"name LIKE '{schema}'", "-o", "json"))
    if not pipes:
        return "FAILED (no pipeline)"
    pid = pipes[0]["pipeline_id"]
    for _ in range(UPDATE_TIMEOUT_S // 15):
        st = (json.loads(cli(profile, "pipelines", "get", pid, "-o", "json")).get("latest_updates") or [{}])[0]
        if st.get("state") in ("COMPLETED", "FAILED", "CANCELED"):
            return st["state"]
        time.sleep(15)
    cli(profile, "pipelines", "stop", pid, "--no-wait", check=False)
    return "TIMEOUT"


def test_one(a: argparse.Namespace, slug: str, host: str) -> bool | None:
    """One connector: None when skipped, else whether it passed."""
    conn = connection_of(slug)
    updated = json.loads(cli(a.profile, "connections", "get", conn, "-o", "json", check=False) or "{}").get("updated_at")
    fp = test_skip.fingerprint([SKILL / "notebooks" / f"{slug}-pipeline.py", SKILL / "bundles" / slug,
                                SKILL / "scripts/lfc_saas_pipeline", Path(__file__)],
                               [host, f"{conn}@{updated}", f"package {a.ref_sha}", a.catalog, a.prefix])
    skip, last = test_skip.should_skip(RUNNER, slug, fp, a.again)
    if skip:
        print(f"  skip  {slug}: passed {last['when']}, unchanged since")
        return None
    if updated is None:
        print(f"  FAIL  {slug}: connection {conn} isn't in this workspace's metastore")
        test_skip.record(RUNNER, slug, fp, False)
        return False
    try:
        return run_one(a, slug, fp)
    except Exception:   # recorded as a failure first, so an earlier pass can't skip a connector that just failed
        test_skip.record(RUNNER, slug, fp, False)
        raise


def run_one(a: argparse.Namespace, slug: str, fp: str) -> bool:
    """Deploy both ways, compare, record the result."""
    epoch, tables = int(time.time()), tables_of(slug)
    cutoff = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(epoch))
    extra = start_vars(slug, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(epoch - START_DAYS * 86400)))
    base = f"{a.prefix}_{slug.replace('-', '_')}"
    dab_schema, dab_state = run_bundle(a.profile, slug, a.catalog, f"{base}_dab_{epoch}", epoch, extra)
    nb_schema = f"{base}_nb_{epoch}"
    nb_state = run_notebook(a.profile, slug, a.catalog, nb_schema, a.ref, extra)
    until = CURSORS.get(slug, {})
    dab_n = counts(a.profile, a.catalog, dab_schema, tables, until, cutoff)
    nb_n = counts(a.profile, a.catalog, nb_schema, tables, until, cutoff)
    ok = dab_state == "COMPLETED" and nb_state == "COMPLETED" and dab_n == nb_n and None not in dab_n.values()
    rows = ", ".join(f"{t} {dab_n[t]}/{nb_n[t]}" + (f" (before {cutoff} UTC)" if t in until else "") for t in tables)
    print(f"  {'ok  ' if ok else 'FAIL'}  {slug}: bundle {dab_state}, notebook {nb_state}; rows bundle/notebook: {rows}")
    if ok and a.unpushed:
        print(f"        not recorded as a pass: the package has changes the notebook can't install ({a.unpushed})")
    else:
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
    sys.stdout.reconfigure(line_buffering=True)   # each result shows as it comes, also when redirected to a file
    a.ref = a.ref or current_ref()
    a.ref_sha, a.unpushed = pushed_state(a.ref)
    host = json.loads(cli(a.profile, "auth", "describe", "-o", "json", check=False) or "{}").get("details", {}).get(
        "host", a.profile)
    print(f"lfc-saas-pipeline tests on {host} (notebooks install the package from {a.ref}):")
    results = []
    for s in connectors(a.only):   # one at a time; an error is that connector's failure, the others still run
        try:
            results.append(test_one(a, s, host))
        except Exception as err:  # noqa: BLE001
            print(f"  FAIL  {s}: {str(err)[:300]}")
            results.append(False)
    ran = [r for r in results if r is not None]
    print(f"{sum(ran)} passed, {len(ran) - sum(ran)} failed, {results.count(None)} skipped (unchanged; --again runs them)")
    return 0 if all(ran) else 1


if __name__ == "__main__":
    sys.exit(main())
