"""fanin_test.py: one database end to end through the fan-in, the test behind each docs/database-connect page.

  engine   your side, zerobus-debezium-test (SOURCE, MODEL=tpch, PHASE=2, KEEP=1): the database, the snapshot as of T
           in a Volume, Debezium Server with the Zerobus sink, the change log, and the skill's own silver checked
           against the source. Its ROUND_HOOK runs `round` after each of its rounds. Then both silver pipelines'
           update times, and that run's containers removed from the VM (their logs kept first when it failed).
  round    the fan-in after the skill's round ROUND: this skill's bundle (fanin_dbz_zbus) on the run's log and
           Volume, into its own schema, deployed on round 1, its job run, and every fan-in silver table against the
           skill's silver of that round (compare.py). One line a round in <source>-rounds.jsonl.

    PROFILE=<cli profile> CATALOG=<catalog> VM_IP=<vm> WAREHOUSE=<warehouse id> \\
      python3 -I skills/lfc-fanin/scripts/fanin_test.py engine --source pg --source-type POSTGRESQL

RESULTS (default ~/.lfc-test/results/fanin): <source>-skill.md/.log/.json, <source>-rounds.jsonl, <source>-times.json;
fanin_page.py writes the docs page from them. VM_USER (azureuser) and SSH_KEY: the VM's login, as for the skill.
ARCHIVE: where the bundle installs lfc-change-source and lfc-fanin from (default: this repo's main). Every process it
starts gets no standard input, and arguments go as lists: nothing waits on a terminal or splits on a comma.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
ARCHIVE = "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip"
TERMINAL = ("TERMINATED", "INTERNAL_ERROR", "SKIPPED")


def results_dir() -> Path:
    d = Path(os.environ.get("RESULTS") or Path.home() / ".lfc-test/results/fanin").expanduser()
    d.mkdir(parents=True, exist_ok=True)
    return d


def run(cmd: list[str], log: Path | None = None, cwd: Path | None = None, env: dict | None = None) -> int:
    """A command with no standard input, its output appended to `log` (or discarded): its exit status."""
    with (open(log, "a") if log else open(os.devnull, "w")) as out:
        return subprocess.run(cmd, stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT, cwd=cwd,
                              env={**os.environ, **(env or {})}, check=False).returncode


def names(source: str, schema: str) -> tuple[str, str]:
    """The fan-in's name and schema for a run of the skill (its schema ends in the run's id)."""
    e = schema.rsplit("_", 1)[-1]
    return f"fanin-{source}-{e}", os.environ.get("FANIN_SCHEMA") or f"fanin_{source}_{e}"


def bundle_env(name: str, schema: str, fan_schema: str, source_type: str) -> dict:
    cat = os.environ["CATALOG"]
    return {"BUNDLE_VAR_pipeline_name": name, "BUNDLE_VAR_connector_type": "fanin_dbz_zbus",
            "BUNDLE_VAR_dest_catalog": cat, "BUNDLE_VAR_dest_schema": fan_schema,
            "BUNDLE_VAR_change_log": f"{cat}.{schema}.cdc_log", "BUNDLE_VAR_snapshot_volume": f"/Volumes/{cat}/{schema}/snapshot",
            "BUNDLE_VAR_source_type": source_type, "BUNDLE_VAR_tests_repo_archive": os.environ.get("ARCHIVE") or ARCHIVE}


def wait_job(w, name: str, timeout: int = 3600) -> str:
    """The latest run of the job whose name ends in `name`, waited for: "<life cycle> <result>". The CLI's own wait
    can drop on a network blip; this reads the run's state."""
    jobs = [j for j in w.jobs.list(name=None) if (j.settings.name or "").endswith(name)]
    if not jobs:
        return "no job"
    t0 = time.time()
    while True:
        runs = list(w.jobs.list_runs(job_id=jobs[0].job_id, limit=1))
        st = runs[0].state if runs else None
        life = st.life_cycle_state.value if st and st.life_cycle_state else ""
        if life in TERMINAL or time.time() - t0 > timeout:
            return f"{life} {st.result_state.value if st and st.result_state else ''}".strip()
        time.sleep(30)


def fanin_round(source: str, source_type: str, schema: str, rnd: int) -> dict:
    """One fan-in round (module doc): appended to <source>-rounds.jsonl, and returned."""
    from databricks.sdk import WorkspaceClient

    import compare
    import pipeline_times
    profile, res = os.environ["PROFILE"], results_dir()
    w = WorkspaceClient(profile=profile)
    name, fan = names(source, schema)
    env, log, bundle, t0 = bundle_env(name, schema, fan, source_type), res / f"{source}-fanin.log", HERE.parent / "bundle", time.time()
    deployed = rnd != 1 or run(["databricks", "bundle", "deploy", "-p", profile], log, bundle, env) == 0
    if deployed:
        run(["databricks", "bundle", "run", "fanin_job", "-p", profile], log, bundle, env)
    st = wait_job(w, name) if deployed else "not deployed"
    sys.path.insert(0, str(HERE.parent.parent / "lfc-change-source" / "scripts"))
    from lfc_change_source.engine import Warehouse
    cmp = compare.compare(Warehouse(os.environ["WAREHOUSE"], w), w, os.environ["CATALOG"], schema, fan)
    upd = (pipeline_times.times(w, f"{name}-silver") or [{}])[-1]
    out = {"round": rnd, "fanin_job": st, "fanin_schema": fan, "job_s": round(time.time() - t0),
           "compare": {"tables": cmp["tables"], "equal": cmp["equal"]}, "silver_update": upd}
    with open(res / f"{source}-rounds.jsonl", "a") as f:
        f.write(json.dumps(out) + "\n")
    return out


def round_passed(r: dict) -> bool:
    return r["fanin_job"] == "TERMINATED SUCCESS" and r["compare"]["tables"] > 0 and r["compare"]["equal"] == r["compare"]["tables"]


def vm(cmd: str) -> subprocess.CompletedProcess:
    ssh = ["ssh", "-o", "ConnectTimeout=20"] + (["-i", os.environ["SSH_KEY"]] if os.environ.get("SSH_KEY") else [])
    return subprocess.run(ssh + [f"{os.environ.get('VM_USER', 'azureuser')}@{os.environ['VM_IP']}", cmd],
                          stdin=subprocess.DEVNULL, capture_output=True, text=True, check=False)


def engine(source: str, source_type: str) -> dict:
    """One database end to end (module doc): the skill with the round hook, both silvers' times, the VM cleaned."""
    from databricks.sdk import WorkspaceClient

    import pipeline_times
    for k in ("PROFILE", "CATALOG", "VM_IP", "WAREHOUSE"):
        if not os.environ.get(k):
            sys.exit(f"{k} is needed")
    res, skills = results_dir(), HERE.parent.parent
    (res / f"{source}-rounds.jsonl").unlink(missing_ok=True)
    copy = Path(tempfile.mkdtemp(prefix=f"fanin-{source}."))   # a copy: editing the skills can't change this run
    shutil.copytree(skills, copy / "skills", ignore=shutil.ignore_patterns("__pycache__"))
    hook = f"{sys.executable} -I {HERE / 'fanin_test.py'} round --source {source} --source-type {source_type}"
    t0 = time.time()
    rc = run(["bash", "scripts/debezium-zerobus.sh"], res / f"{source}-skill.log", copy / "skills/zerobus-debezium-test",
             {"SOURCE": source, "MODEL": "tpch", "NSRC": "1", "PHASE": "2", "KEEP": "1", "OUT": str(res / f"{source}-skill.md"),
              "ROUND_HOOK": hook})
    text = (res / f"{source}-skill.log").read_text(errors="replace")
    e = (re.search(r"run (\d+):", text) or [None, ""])[1]
    schema = (re.search(rf"schema {re.escape(os.environ['CATALOG'])}\.([a-z0-9_]+)", text) or [None, ""])[1]
    skill = {"source": source, "source_type": source_type, "skill_exit": rc, "skill_pass": len(re.findall(r"^PASS", text, re.MULTILINE)),
             "skill_fail": len(re.findall(r"^FAIL", text, re.MULTILINE)), "run": e, "schema": schema, "skill_s": round(time.time() - t0)}
    (res / f"{source}-skill.json").write_text(json.dumps(skill) + "\n")
    w = WorkspaceClient(profile=os.environ["PROFILE"])
    skp = (re.search(rf'pipeline "([A-Za-z0-9_.-]*-dbz-silver-{e})"', text) or [None, ""])[1]
    name = names(source, schema)[0] if schema else ""
    (res / f"{source}-times.json").write_text(json.dumps({"skill": pipeline_times.times(w, skp) if skp else [],
                                                           "fanin": pipeline_times.times(w, f"{name}-silver") if name else []}) + "\n")
    if e:
        if rc != 0:   # keep the containers' state and last lines before they go
            got = vm(f"for c in $(sudo podman ps -a --format '{{{{.Names}}}}' | grep -E -- '-{e}(-|$)'); do echo == $c; "
                     f"sudo podman inspect --format '{{{{.State.Status}}}} exit={{{{.State.ExitCode}}}} oom={{{{.State.OOMKilled}}}}' $c; "
                     "sudo podman logs --tail 40 $c 2>&1; done")
            (res / f"{source}-containers.log").write_text(got.stdout + got.stderr)
        vm(f"sudo podman ps -a --format '{{{{.Names}}}}' | grep -E -- '-{e}(-|$)' | xargs -r sudo podman rm -f")
    shutil.rmtree(copy, ignore_errors=True)
    rf = res / f"{source}-rounds.jsonl"
    rounds = [json.loads(x) for x in rf.read_text().splitlines() if x.strip()] if rf.exists() else []
    return {**skill, "rounds": [{k: r[k] for k in ("round", "fanin_job", "compare")} for r in rounds],
            "passed": rc == 0 and not skill["skill_fail"] and len(rounds) >= 2 and all(round_passed(r) for r in rounds)}


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description="One database end to end through the fan-in")
    ap.add_argument("step", choices=["engine", "round"])
    ap.add_argument("--source", required=True, help="zerobus-debezium-test's SOURCE: mssql, pg, mysql, oracle, db2, informix, crdb, yb")
    ap.add_argument("--source-type", required=True, help="lfc-fanin's source_type: SQLSERVER, POSTGRESQL, ...")
    a = ap.parse_args(argv)
    if a.step == "round":   # from the skill's ROUND_HOOK: its ROUND, CATALOG and SCHEMA
        r = fanin_round(a.source, a.source_type, os.environ["SCHEMA"], int(os.environ["ROUND"]))
        print(json.dumps(r))
        sys.exit(0 if round_passed(r) else 1)
    r = engine(a.source, a.source_type)
    print(json.dumps(r))
    sys.exit(0 if r["passed"] else 1)


if __name__ == "__main__":
    main()
