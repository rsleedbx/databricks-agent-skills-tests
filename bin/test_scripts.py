#!/usr/bin/env python3
# ruff: noqa: E501  (the check names are the suite's names, long as they were in test_scripts.sh)
#!/usr/bin/env python3
"""bin/test_scripts.py: regression tests for the skills' scripts.
  Offline (always): shell syntax (also under bash 3.2 when /bin/bash is 3.2, or BASH32=<path>), every markdown link, the
  script rules (bin/lint_scripts.py), SCRIPTS.md, the layers (bin/check_layers.py: layers.yaml and layers-baseline.txt),
  the six TPC schemas through shape.py and ddl.py for seven dialects, every TPC workload from tpc-schema.sh through
  ddl-to-schema.py and ddl.py for seven dialects, a DDL import round trip, the Python fill planner compiles, the shared
  DuckDB runners, and the workload contract (model format 1 on every schema, workload-env.sh on its fixtures, one
  workload.sh run).
  With databases (PSQL and/or MYSQL set, e.g. PSQL="podman exec -i dg-pg psql -U postgres",
  MYSQL="podman exec -i dg-my mysql -uroot"): fill each of the six workloads from tpc-schema.sh at a small scale (TPC-C 1
  warehouse, TPC-H SF 0.1, TPC-B SF 1 (its smallest), TPC-DS, TPC-E, TPC-DI SF 0.01) on each, and check every table is at
  its target, TPC-H's spec rules, and the audit (0 bad rows). Schemas test_* are dropped first.
  The planner files must be readable by the client: with podman exec, copy them in and set DIR_PG / DIR_MY to that path.
  VM mode: the same checks on a live database on the cloud VM (LIVE_PSQL, LIVE_MYSQL, or LIVE_ENGINE with LIVE_PY, the
  connection options for fill.py: --host ... --port ... --user ... --password-file ... --database ...), e.g. a slot from
  `slot.sh up <engine> <name> --from <master>` on the VM. Db2 and Informix have no SQL planner: their rows come from fill.py,
  and the checks are the row counts (no spec rules or audit). Laptop default (PSQL/MYSQL) is unchanged.

Usage: bin/test_scripts.py (or the shell entry point bin/test_scripts.sh, which runs this).
Each check prints ok or FAIL and its last lines of output; TEST_TIMINGS (default ~/.cache/test_scripts/timings.tsv) gets
each check's seconds and result (bin/test_timings.py summarizes it). A check runs only when what it depends on changed
since it last passed (bin/test_gate.py: its command, its repo paths widened to the skills they reference, or the whole
repo for a bin tool); one that names no repo path always runs, and so does a function check (its commands aren't
visible to the gate). The checks that pass files through the temp dir run or skip as one group. TEST_SKIP_AGAIN=1 runs
every check. Exit 1 when any check failed.
"""
from __future__ import annotations

import glob
import hashlib
import io
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from contextlib import redirect_stdout
from pathlib import Path

SELF = Path(__file__).resolve()
R = SELF.parent.parent
SK = R / "skills"
SS = SK / "lfc-source-statschema" / "scripts"
DG = SK / "lfc-source-datagen" / "scripts"
WC = SK / "lfc-workload-contract" / "scripts"
DM = SK / "lfc-schema-change-test" / "scripts"
GATE = R / "bin" / "test_gate.py"
PLACE = {"@R@": str(R), "@SK@": str(SK), "@SS@": str(SS), "@DG@": str(DG), "@WC@": str(WC), "@DM@": str(DM)}
GROUP = "files passed through $T"


def sub(template: str, t: Path) -> str:
    """A shell command's text with this checkout's paths (@R@, @SK@, @SS@, @DG@, @WC@, @DM@) and the temp dir (@T@)."""
    for k, v in PLACE.items():
        template = template.replace(k, v)
    return template.replace("@T@", str(t))


class Suite:
    def finish(self) -> int:
        """Record this run's group (when it ran), remove the temp dir, and print the summary: the exit status is 1 when a
        check failed."""
        if not self.tgroup.startswith("skip"):
            status = "pass" if self.tfails == 0 else "fail"
            subprocess.run(["python3", str(GATE), "group-record", GROUP, str(SELF), str(self.T / "group-inputs"), status,
                            "--value", self.gate_env])
        shutil.rmtree(self.T, ignore_errors=True)
        print()
        if self.skipped:
            print(f"{self.skipped} skipped: unchanged since they passed (TEST_SKIP_AGAIN=1 runs them)")
        print("all passed" if self.fails == 0 else f"{self.fails} failed")
        return 1 if self.fails else 0

    def __init__(self) -> None:
        self.fails = 0
        self.skipped = 0
        self.tfails = 0
        self.T = Path(tempfile.mkdtemp())
        self.timings = Path(os.environ.get("TEST_TIMINGS") or Path.home() / ".cache/test_scripts/timings.tsv")
        self.timings.parent.mkdir(parents=True, exist_ok=True)
        self.run_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        db_env = [os.environ.get(k, "") for k in ("PSQL", "MYSQL", "LIVE_PSQL", "LIVE_MYSQL", "LIVE_ENGINE",
                                                  "LIVE_DIALECT", "LIVE_PY")]
        self.gate_env = "|".join(db_env) + "|" + hashlib.sha1(SELF.read_bytes()).hexdigest()[:16]
        self.tgroup = self.gate_out(["group-decide", GROUP, str(SELF), "--value", self.gate_env])
        self.gate_group_ok = True

    # --- output and bookkeeping ---
    def ok(self, name: str) -> None:
        print(f"  ok    {name}")

    def no(self, name: str) -> None:
        print(f"  FAIL  {name}")
        self.fails += 1

    def timing(self, name: str, secs: int, result: str) -> None:
        with self.timings.open("a") as f:
            f.write(f"{self.run_at}\t{secs}\t{result}\t{name}\n")

    def gate_out(self, args: list[str]) -> str:
        """test_gate.py's answer line (its exit status is the decision, read by the caller where it matters)."""
        p = subprocess.run(["python3", str(GATE), *args], stdout=subprocess.PIPE, text=True)
        self.last_rc = p.returncode
        return p.stdout.rstrip("\n")

    def sh(self, template: str) -> list[str]:
        return ["sh", "-c", sub(template, self.T)]

    # --- running one check ---
    def run_check(self, name: str, cmd) -> bool:
        """Run one check (an argv list, or a function returning True/False); ok or FAIL with its last lines of output."""
        t0 = time.monotonic()
        text = ""
        if callable(cmd):
            buf = io.StringIO()
            try:
                with redirect_stdout(buf):
                    passed = bool(cmd())
            except Exception as e:  # noqa: BLE001  a check that raises is a failed check
                passed = False
                buf.write(f"{e}\n")
            text = buf.getvalue()
        else:
            out = self.T / "out"
            with out.open("w") as f:
                try:
                    passed = subprocess.run([str(x) for x in cmd], stdout=f, stderr=subprocess.STDOUT).returncode == 0
                except OSError as e:   # not on PATH (as the shell's exit 127): the check fails, the suite goes on
                    f.write(f"{e}\n")
                    passed = False
            text = out.read_text(errors="replace")
        dur = int(time.monotonic() - t0)
        if passed:
            print(f"  ok    {name}{f' ({dur}s)' if dur >= 2 else ''}")
            self.timing(name, dur, "pass")
            return True
        self.fails += 1
        print(f"  FAIL  {name} ({dur}s)")
        for line in text.splitlines()[-5:]:
            print(f"        {line}")
        self.timing(name, dur, "fail")
        return False

    def check(self, name: str, cmd) -> None:
        """The gate: skip when unchanged since a pass, group the checks that pass files through the temp dir."""
        if callable(cmd):   # a function: its commands aren't seen by the gate, so it always runs
            self.run_check(name, cmd)
            return
        argv = [str(x) for x in cmd]
        if str(self.T) in " ".join(argv):   # the temp dir's files: one group
            if self.tgroup.startswith("skip"):
                print(f"  skip  {name} (unchanged since {self.tgroup[len('skip '):]})")
                self.skipped += 1
                return
            if subprocess.run(["python3", str(GATE), "group-add", str(self.T / "group-inputs"), "--cache",
                               str(self.T / "edges.json"), "--", *argv]).returncode != 0:
                self.tfails += 1
            if not self.run_check(name, argv):
                self.tfails += 1
            return
        d = self.gate_out(["decide", name, "--value", self.gate_env, "--value", " ".join(argv), "--cache",
                           str(self.T / "edges.json"), "--", *argv])
        if d.startswith("skip"):
            print(f"  skip  {name} (unchanged since {d[len('skip '):]})")
            self.skipped += 1
            self.timing(name, 0, "skip")
            return
        if d == "run -":   # nothing to compare: always run, never recorded
            self.run_check(name, argv)
            return
        fp = d[len("run "):]
        status = "pass" if self.run_check(name, argv) else "fail"
        self.gate_out(["record", name, fp, status])

    # --- the DB modes ---
    def seed_my(self, cli: list[str]) -> None:
        """lfc_seed.seed (1 .. 2^20, r a random permutation), by doubling, as lfc-vm masters build builds it."""
        vals = ",".join(f"({i})" for i in range(1, 1025))
        script = ["CREATE DATABASE IF NOT EXISTS lfc_seed; CREATE TABLE IF NOT EXISTS lfc_seed.seed_i (i bigint PRIMARY KEY);",
                  f"INSERT IGNORE INTO lfc_seed.seed_i (i) VALUES {vals};"]
        script += ["INSERT IGNORE INTO lfc_seed.seed_i (i) SELECT s.i + b.m FROM lfc_seed.seed_i s, "
                   "(SELECT MAX(i) AS m FROM lfc_seed.seed_i) b;"] * 10
        script += ["DROP TABLE IF EXISTS lfc_seed.seed; CREATE TABLE lfc_seed.seed (i bigint PRIMARY KEY, r bigint NOT NULL);",
                   "INSERT INTO lfc_seed.seed SELECT i, ROW_NUMBER() OVER (ORDER BY RAND()) FROM lfc_seed.seed_i;",
                   "DROP TABLE lfc_seed.seed_i;"]
        subprocess.run(cli, input="\n".join(script) + "\n", text=True, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)

    def tpc_sql(self, w: str, sf: str) -> Path:
        """The workload's DDL for the dialect checks: tpc-schema.sh, read back as a schema (mysql syntax)."""
        y = self.T / f"{w}.live.yaml"
        sh_out = subprocess.run(["bash", str(DG / "tpc/tpc-schema.sh"), w, sf], stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL, text=True).stdout
        conv = subprocess.run(["uv", "run", "-q", str(SS / "ddl-to-schema.py"), "--dialect", "mysql"], input=sh_out,
                              stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True).stdout
        y.write_text(conv)
        return y

    def db(self, kind: str, dial: str, cli_text: str, dir_: str) -> None:
        """fill and check each workload at a small scale in test_<w> on a database: kind pg (psql), my (mysql), py (fill.py
        for the engine named by cli_text, its connection options from LIVE_PY)."""
        cli = cli_text.split()
        print(f"== {dial} ({kind})")
        if kind == "my":
            probe = subprocess.run(cli + ["-N", "-e", "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = "
                                                      "'lfc_seed' AND table_name = 'seed' AND column_name = 'r'"],
                                   stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True).stdout
            if "1" not in probe.split("\n"):
                self.seed_my(cli)
        py = os.environ.get("PY", "python3")
        live_py = os.environ.get("LIVE_PY", "").split()
        for w in ["tpcb", "tpcc", "tpch", "tpcds", "tpce", "tpcdi"]:
            db = f"test_{w}"
            sf = "1" if w in ("tpcb", "tpcc") else ("0.1" if w == "tpch" else "0.01")
            y = self.tpc_sql(w, sf)
            ddl = subprocess.run(["uv", "run", "-q", str(SS / "ddl.py"), str(y), "--dialect", dial],
                                 stdout=subprocess.PIPE, text=True, stderr=subprocess.DEVNULL).stdout
            (self.T / f"{db}.sql").write_text(ddl)
            S = db.upper()
            sqltext = ddl
            out = ""
            if kind == "pg":
                subprocess.run(cli + ["-q", "-v", "ON_ERROR_STOP=1"],
                               input=f"DROP SCHEMA IF EXISTS {db} CASCADE; CREATE SCHEMA {db}; SET search_path = {db};\n"
                                     + sqltext, text=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                out = self._fill_sh({"PSQL": " ".join(cli), "DIR": dir_}, ["bash", str(DG / "fill.sh"), "pg", db])
            elif kind == "my":
                subprocess.run(cli, input=f"DROP DATABASE IF EXISTS {db}; CREATE DATABASE {db}; USE {db};\n" + sqltext,
                               text=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                out = self._fill_sh({"MYSQL": " ".join(cli), "DIR": dir_}, ["bash", str(DG / "fill.sh"), "my", db])
            else:
                out = self._py_fill(cli_text.strip(), db, S, py, live_py, sqltext)
            rows = [ln for ln in out.split("\n") if ln.count("|") == 4]
            short = [ln for ln in rows if ln.split("|")[3] != ln.split("|")[4]]
            if rows and not short:
                self.ok(f"{dial} {db}: {len(rows)} tables at target")
            else:
                self.no(f"{dial} {db}: {len(short)} of {len(rows)} short")
                for ln in out.splitlines()[-3:]:
                    print(f"        {ln}")
            if w == "tpch" and kind != "py":
                bad = self._spec_bad(kind, cli, db)
                self.ok(f"{dial} {db}: TPC-H spec rules") if bad == 0 else self.no(f"{dial} {db}: {bad} spec rules broken")
            if w == "tpcb" and kind != "py":
                bad = self._tpcb_bad(kind, cli, db)
                self.ok(f"{dial} {db}: TPC-B scaling and relationships") if bad == 0 else \
                    self.no(f"{dial} {db}: {bad} TPC-B checks broken")
            if kind != "py":
                a = self._audit(kind, cli, db, dir_)
                self.ok(f"{dial} {db}: audit 0 bad rows") if a == "0" else self.no(f"{dial} {db}: audit {a} bad rows")

    def _fill_sh(self, env_extra: dict, argv: list[str]) -> str:
        env = {**os.environ, **env_extra}
        p = subprocess.run(argv, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        return p.stdout

    def _py_fill(self, engine: str, db: str, S: str, py: str, live_py: list[str], sqltext: str) -> str:
        """The engine's own DDL path (Db2 a schema per workload; Informix a database per workload; the rest a schema),
        then fill.py."""
        sq = [py, str(DG / "python/sq.py")]
        if engine == "db2":
            subprocess.run(sq + ["db2", "--schema", S, *live_py], input=f"CREATE SCHEMA {S};\n" + sqltext, text=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        elif engine == "informix":
            dbspace = os.environ.get("LIVE_IFX_DBSPACE") or "datadbs"
            subprocess.run(sq + ["ifx", "--database", "sysmaster", *live_py],
                           input=f"DROP DATABASE IF EXISTS {db}; CREATE DATABASE {db} IN {dbspace} WITH LOG;\n",
                           text=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
            subprocess.run(sq + ["ifx", "--database", db, *live_py], input=sqltext, text=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        else:
            subprocess.run(sq + [engine, *live_py],
                           input=f"DROP SCHEMA IF EXISTS {db} CASCADE; CREATE SCHEMA {db}; SET search_path = {db};\n"
                                 + sqltext, text=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        sessions = os.environ.get("LIVE_SESSIONS") or "2"
        return subprocess.run([py, str(DG / "python/fill.py"), engine, db, *live_py, "--sessions", sessions],
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True).stdout

    def _spec_bad(self, kind: str, cli: list[str], db: str) -> int:
        verify = (DG / "tpc/tpch-verify.sql").read_text()
        if kind == "pg":
            text = f"SET search_path = {db};\n" + re.sub(r"DATEDIFF\(([^,]+), ([^)]+)\)", r"(\1 - \2)", verify)
            out = subprocess.run(cli + ["-At"], input=text, text=True, stdout=subprocess.PIPE,
                                 stderr=subprocess.DEVNULL).stdout
            vals = [v for ln in out.splitlines() if ln.startswith("bad") for v in ln.split("|")[1:]]
        else:
            out = subprocess.run(cli + ["-N", db], input=verify, text=True, stdout=subprocess.PIPE,
                                 stderr=subprocess.DEVNULL).stdout
            vals = [v for ln in out.splitlines() if ln.startswith("bad") for v in ln.split("\t")[1:]]
        return sum(1 for v in vals if not _is_zero(v))

    def _tpcb_bad(self, kind: str, cli: list[str], db: str) -> int:
        verify = (DG / "tpc/tpcb-verify.sql").read_text()
        if kind == "pg":
            out = subprocess.run(cli + ["-At"], input=f"SET search_path = {db};\n" + verify, text=True,
                                 stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).stdout
            return sum(1 for ln in out.splitlines() if ln.startswith("bad") and not _is_zero(ln.split("|")[-1]))
        out = subprocess.run(cli + ["-N", db], input=verify, text=True, stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL).stdout
        return sum(1 for ln in out.splitlines() if ln.startswith("bad") and not _is_zero(ln.split("\t")[-1]))

    def _audit(self, kind: str, cli: list[str], db: str, dir_: str) -> str:
        """The plan audit's bad rows: the sum of the third column of the audit query's output."""
        if kind == "pg":
            cast = (DG / "postgres/cast.sql").read_text() + (DG / "postgres/plan-audit.sql").read_text()
            first = subprocess.run(cli + ["-At", "-q", "-v", f"schema={db}", "-v", "plain_every=4"], input=cast, text=True,
                                   stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).stdout
            out = subprocess.run(cli + ["-At"], input=first, text=True, stdout=subprocess.PIPE,
                                 stderr=subprocess.DEVNULL).stdout
            return _awk_sum(out.splitlines(), "|")
        if kind == "my":
            text = f"SET @db='{db}', @plain_every=4, @block=1000;\n" + (DG / "mysql/cast.sql").read_text() \
                + (DG / "mysql/plan-audit.sql").read_text()
            first = subprocess.run(cli + ["-N", "-B", "-r", db], input=text, text=True, stdout=subprocess.PIPE,
                                   stderr=subprocess.DEVNULL).stdout
            out = subprocess.run(cli + ["-N", db], input=first, text=True, stdout=subprocess.PIPE,
                                 stderr=subprocess.DEVNULL).stdout
            return _awk_sum(out.splitlines(), "\t")
        return "0"   # no audit planner for the driver path (Db2, Informix, CockroachDB): see lfc-source-datagen


def _is_zero(v: str) -> bool:
    """awk's `$1 != 0`: a number equal to zero (a non-number is not)."""
    try:
        return float(v) == 0
    except ValueError:
        return False


def _awk_sum(lines: list[str], sep: str) -> str:
    """awk's `{ b += $3 } END { print b + 0 }` on fields split by sep (a missing or non-number field adds 0)."""
    total = 0.0
    for ln in lines:
        fields = ln.split(sep)
        if len(fields) >= 3:
            m = re.match(r"\s*[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?", fields[2])
            total += float(m.group(0)) if m else 0.0
    return str(int(total)) if float(total).is_integer() else str(total)


def tpc_dialects(s: Suite, w: str) -> bool:
    """tpc-schema.sh's DDL for one workload, read back, then written for seven dialects, then validated."""
    t = s.T
    sh_out = subprocess.run(["bash", str(DG / "tpc/tpc-schema.sh"), w, "1"], stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, text=True)
    conv = subprocess.run(["uv", "run", "-q", str(SS / "ddl-to-schema.py"), "--dialect", "mysql"], input=sh_out.stdout,
                          stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    if conv.returncode != 0:
        return False
    (t / f"t-{w}.yaml").write_text(conv.stdout)
    for d in ["postgres", "mysql", "tsql", "oracle", "db2", "informix", "databricks"]:
        p = subprocess.run(["uv", "run", "-q", str(SS / "ddl.py"), str(t / f"t-{w}.yaml"), "--dialect", d],
                           stdout=subprocess.PIPE, text=True)
        if p.returncode != 0 or "CREATE TABLE" not in p.stdout:
            return False
        (t / f"t-{w}.{d}.sql").write_text(p.stdout)
    v = subprocess.run(["uv", "run", "-q", str(SS / "validate.py"), str(t / f"t-{w}.yaml")], stdout=subprocess.PIPE,
                       stderr=subprocess.STDOUT, text=True)
    print(v.stdout, end="")   # the check's output: shown only when it fails (run_check)
    return v.returncode == 0


def wl_offline(s: Suite) -> bool:
    """workload.sh writes a tpcb workload that workload-env.sh accepts (4 tables); a postgres spec tpch one with its
    verify file (8 tables); a dialect without a fill command (oracle) is refused."""
    t = s.T

    def workload(args: list[str]) -> int:
        return subprocess.run(["bash", str(DG / "workload.sh"), *args], stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL).returncode

    def env_of(json_path: Path) -> dict:
        """The WL_ variables workload-env.sh exports for the file (eval'd by a shell, as the consumers do)."""
        p = subprocess.run(["bash", "-c", 'eval "$(bash "$0" "$1")" && env', str(WC / "workload-env.sh"), str(json_path)],
                           stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        if p.returncode != 0:
            return {}
        return dict(ln.split("=", 1) for ln in p.stdout.splitlines() if ln.startswith("WL_") and "=" in ln)

    if workload(["--model", "tpcb", "--sf", "0.01", "--dialect", "tsql", "--out", str(t / "wl")]) != 0:
        return False
    e1 = env_of(t / "wl" / "workload.json")
    if e1.get("WL_NTABLES") != "4" or e1.get("WL_MODEL") != "tpcb" or not Path(e1.get("WL_DDL", "")).is_file() \
            or e1.get("WL_VERIFY"):
        return False
    if workload(["--model", "tpch", "--sf", "0.1", "--dialect", "postgres", "--spec", "--out", str(t / "wl3")]) != 0:
        return False
    e3 = env_of(t / "wl3" / "workload.json")
    verify = Path(e3.get("WL_VERIFY", ""))
    if e3.get("WL_NTABLES") != "8" or e3.get("WL_DIALECT") != "postgres" or not verify.is_file() \
            or "'bad: ship" not in verify.read_text(errors="replace"):
        return False
    return workload(["--model", "tpcb", "--sf", "1", "--dialect", "oracle", "--out", str(t / "wl2")]) != 0


def ddl_matrix_collect_script() -> str:
    """The ddl-matrix collect check's shell: saved events and tables written as Python, then collect and three greps."""
    py = '''import json, sys
d = sys.argv[1]
flow = lambda t: f"main.s.applier_main_s_x_{t}_cdc_flow"
ev = [{'event_type': 'update_progress', 'level': 'INFO', 'message': 'Update u1 is RUNNING.', 'timestamp': '2026-10-26T22:05:00.000Z'},
      {'event_type': 'runtime_details', 'level': 'INFO', 'message': 'Resolved runtime', 'timestamp': '2026-10-26T22:05:01.000Z', 'origin': {'cloud': 'AWS', 'region': 'us-west-2', 'update_id': 'u1'}, 'details': {'runtime_details': {'runtime_version': {'dbr_version': '18.3', 'channel': 'CURRENT', 'image_key': 'dlt:18.3.8-delta-pipelines-aarch64-photon-dlt-release-dp-20261015-rc0-commit-48da011-image-5bafb35'}}}},
      {'event_type': 'flow_progress', 'level': 'WARN', 'message': 'Flow ' + flow('int_to_bigint') + ' has terminated.', 'timestamp': '2026-10-26T22:06:00.000Z', 'origin': {'flow_name': flow('int_to_bigint')},
       'error': {'exceptions': [{'message': '[FLOW_SCHEMA_CHANGED] Flow terminated since it encountered a schema change.'}]}},
      {'event_type': 'flow_progress', 'level': 'ERROR', 'message': 'Failed to resolve flow', 'timestamp': '2026-10-26T22:07:00.000Z', 'origin': {'flow_name': flow('bigint_to_int')},
       'error': {'exceptions': [{'message': "[CDC_INCOMPATIBLE_SCHEMA_CHANGES.UNSUPPORTED_DATA_TYPE_CHANGE] incompatible"}]}},
      {'event_type': 'update_progress', 'level': 'INFO', 'message': 'Update u1 is COMPLETED.', 'timestamp': '2026-10-26T22:20:00.000Z'}]
tables = {'int_to_bigint': {'columns': {'id': 'int', 'n': 'bigint'}, 'row': {'id': '3', 'n': '3000000000'}},
          'bigint_to_int': {'columns': {'id': 'int', 'n': 'bigint'}, 'row': None}}
json.dump(ev, open(d + '/events.json', 'w')); json.dump(tables, open(d + '/tables.json', 'w'))
open(d + '/env.yaml', 'w').write('environment:\\n  engine: sqlserver\\n  product: Azure SQL Database\\n  version: test\\n  mechanism: ct\\n  setup: none\\n  source_setup: test\\n  pipeline_mode: integrated\\n  compute: serverless\\n  workspace: {cloud: AWS, region: us-west-2}\\n  date: 2026-10-26\\n  method: one-case-at-a-time\\n  caveats: [test fixture]\\n')
'''
    tail = ("uv run -q '@DM@/ddl-matrix.py' collect --events-json '@T@/cm/events.json' --tables-json '@T@/cm/tables.json' "
            "--env '@T@/cm/env.yaml' --cases int-to-bigint,bigint-to-int --out '@T@/cm/sqlserver-ct-none-2026-10-26.yaml' "
            "> '@T@/cm/out.txt' && grep -q '^PASS ' '@T@/cm/out.txt' && grep -q '^STOP ' '@T@/cm/out.txt' && "
            "grep -q 'dp_build: dp-20261015' '@T@/cm/sqlserver-ct-none-2026-10-26.yaml'")
    return ("mkdir -p '@T@/cm' && python3 - '@T@/cm' <<'PY'\n" + py + "PY\n" + tail)


def main(argv: list[str]) -> int:
    s = Suite()
    print("== offline")
    for f in sorted(SK.rglob("*.sh")):
        if f.is_file():
            s.run_check(f"bash -n {f.relative_to(SK)}", ["bash", "-n", str(f)])   # milliseconds: never gated
    # and with bash 3.2 (macOS /bin/bash, or BASH32=<path>): the scripts run on it unchanged (lint_scripts.py rule bash32)
    b32 = os.environ.get("BASH32")
    if not b32:
        try:
            b32 = subprocess.run(["/bin/bash", "-c", '[ "${BASH_VERSINFO[0]}" -lt 4 ] && echo /bin/bash'],
                                 stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True).stdout.strip()
        except OSError:
            b32 = ""
    if b32:
        for f in sorted([*SK.rglob("*.sh"), *(R / "bin").rglob("*.sh")]):
            if f.is_file():
                s.run_check(f"bash 3.2 -n {f.relative_to(R)}", [b32, "-n", str(f)])

    s.check("links in every skill", ["python3", R / "bin/check_links.py", SK])
    s.check("links in the contributor skills", ["python3", R / "bin/check_links.py", R / ".claude/skills"])
    s.check("script rules: headers, SKILL.md size, rm guards, first-argument flags, jq for one value",
            ["python3", R / "bin/lint_scripts.py"])
    s.check("lint rule bash32 on fixed bad and good lines", ["python3", "-m", "unittest", "-q", R / "bin/test_lint_bash32.py"])
    s.check("results dir: checkout, LFC_TESTS_RESULTS, an installed plugin copy",
            ["python3", "-m", "unittest", "-q", R / "bin/test_results_dir.py"])
    s.check("SCRIPTS.md is current", ["python3", R / "bin/scripts_index.py", "--check"])
    s.check("layers: every skill and bin script is in layers.yaml, every cross-skill reference is allowed or in layers-baseline.txt",
            ["python3", R / "bin/check_layers.py"])
    s.check("check_layers: unit tests on a fixture tree", ["python3", "-m", "unittest", "-q", R / "bin/test_check_layers.py"])
    s.check("test_gate: a check's inputs (skill, its code references, a bin tool, none) and the $T group",
            ["python3", "-m", "unittest", "-q", R / "bin/test_test_gate.py"])
    s.check("test_skip: fingerprints, passes, failures, TEST_SKIP_AGAIN",
            ["python3", "-m", "unittest", "-q", R / "bin/test_skip/tests/test_test_skip.py"])
    s.check("repo_reuse_hook: the repo's reuse hook on fixtures",
            ["python3", "-m", "unittest", "-q", R / "bin/repo_reuse_hook/tests/test_repo_reuse_hook.py"])
    s.check("shape.py order between columns: offset spread, GREATEST fallback, a rejected offset rule (fixture, every row)",
            ["python3", "-m", "unittest", "-q", R / "bin/test_shape_order.py"])
    for w in ["tpcb", "tpcc", "tpch", "tpcdi", "tpcds", "tpce"]:
        s.check(f"shape.py {w}", s.sh(f"uv run -q '@SS@/shape.py' '@SK@/lfc-source-statschema/schemas/{w}.yaml' --sf 0.1 "
                                      f"--expected '@T@/{w}.exp.json' > '@T@/{w}.yaml'"))
        for d in ["postgres", "mysql", "tsql", "oracle", "db2", "informix", "databricks"]:
            s.check(f"ddl.py {w} {d}", s.sh(f"uv run -q '@SS@/ddl.py' '@T@/{w}.yaml' --dialect {d} | grep -c 'CREATE TABLE' "
                                            "| grep -qv '^0$'"))
        s.check(f"validate.py {w}", ["uv", "run", "-q", SS / "validate.py", s.T / f"{w}.yaml"])
    for w in ["tpcb", "tpcc", "tpch", "tpcds", "tpce", "tpcdi"]:
        # the spec's DDL for tpcb and tpch; the others are statschema's schemas, so their count is the schema file's
        n = {"tpcb": "4", "tpch": "8"}.get(w) or subprocess.run(
            ["python3", str(R / "bin/check_model_counts.py"), "--count", w], stdout=subprocess.PIPE, text=True).stdout.strip()
        s.check(f"tpc-schema.sh {w} (SF 1): {n} tables, through ddl-to-schema and ddl.py to seven dialects",
                lambda w=w: tpc_dialects(s, w))
        count = _count_names(s.T / f"t-{w}.yaml")
        s.check(f"tpc-schema.sh {w} (SF 1): {n} tables in the schema",
                ["sh", "-c", f"test {count} -eq {n}"])
    s.check("ddl-to-schema.py round trip (TPC-H spec)", s.sh(
        "bash '@DG@/tpc/tpc-schema.sh' tpch 0.1 | uv run -q '@SS@/ddl-to-schema.py' --dialect mysql > '@T@/spec.yaml' "
        "&& uv run -q '@SS@/ddl.py' '@T@/spec.yaml' --dialect mysql | uv run -q '@SS@/ddl-to-schema.py' --dialect mysql > "
        "'@T@/spec2.yaml' && python3 -c \"import sys, yaml; f = lambda p: [{k: ([{c: v for c, v in x.items() if c != "
        "'native_type'} for x in v] if k == 'columns' else v) for k, v in t.items()} for t in yaml.safe_load(open(p))"
        "['tables']]; sys.exit(f('@T@/spec.yaml') != f('@T@/spec2.yaml'))\""))
    s.check("python fill planner compiles", ["python3", "-m", "py_compile", DG / "python/fill.py", DG / "python/planner.py",
                                             DG / "python/drivers.py", *sorted(glob.glob(str(DG / "python/engines/*.py")))])
    s.check("results/runs/*.jsonl valid, nothing internal", ["python3", SK / "lfc-test-results/scripts/record.py", "--check"])
    s.check("results/*.md: nothing internal", ["python3", "-c",
            "import sys, glob, re; sys.path.insert(0, '" + str(SK / "lfc-test-results/scripts") + "'); import record; "
            "bad = [(f, w) for f in glob.glob('" + str(R / "results/*.md") + "') for w in record.leaks({'text': open(f).read()})]; "
            "print(bad); sys.exit(bool(bad))"])   # the same check as record.py's, version masking included
    s.check("recall.py runs", s.sh(f"python3 '{SK}/lfc-test-results/scripts/recall.py' > /dev/null"))
    s.check("coverage_page.py cell rules", ["python3", "-m", "unittest", "-q", R / "bin/test_coverage_page.py"])
    s.check("docs/lakeflow-connect-database.md table matches results/runs",
            ["python3", SK / "lfc-test-results/scripts/coverage_page.py", "--check", R / "docs/lakeflow-connect-database.md"])
    s.check("secret-handling scripts on a fake secret", s.sh(f"'{SK}/secret-handling/scripts/test-secret-scripts.sh' > /dev/null"))
    s.check("keep_catalogs.py: connection + its foreign catalogs, never earlier, month out, failures go on (fake REST)",
            ["python3", "-m", "pytest", "-q", SK / "lfc-workspace-setup/tests/test_keep_catalogs.py"])
    s.check("postgres-tpch-cdc.sh verify(): rule counts, a failed or empty verify query is an error (stub client)",
            s.sh(f"'{SK}/lfc-schema-change-test/scripts/test-tpch-verify.sh' > /dev/null"))
    s.check("lib.sh keep at exit and kept-sweep.sh on a fake CLI", s.sh(f"'{SK}/lfc-schema-change-test/scripts/test-kept.sh' > /dev/null"))
    s.check("lfc-slot (lfc_vm zipapp) limits and --park on a fake podman",
            s.sh(f"'{SK}/lfc-source-cloud-vm/scripts/test-slot-limits.sh' > /dev/null"))
    s.check("lfc_vm masters: the SAS file, the engine beside a master, the staging wait, a build's refusals",
            ["python3", "-I", SK / "lfc-source-cloud-vm/scripts/tests/test_masters.py"])
    s.check("lfc_vm: limits and env files, usage hours, the lock's bound (not inherited), the engine table, an env file that can't be replaced",
            ["python3", "-I", SK / "lfc-source-cloud-vm/scripts/tests/test_slots.py"])
    s.check("source contract: source-env.sh on the fixtures and the default clients",
            s.sh(f"'{SK}/lfc-source-contract/scripts/test-source-env.sh' > /dev/null"))
    s.check("statschema.tools: ddl(), ddl_to_schema() and shape() give the wrappers' text (path, YAML text, dict; stats, scale, keep-small); the wheel ships the models and model() finds them; state restored; canon.py; the rows a spec fixes as rules (TPC-DS calendar, time of day, products in SQLite); composite keys: every column varies, unique, read back; lookups take the parent row's value",
            s.sh(f"cd '{SK}/lfc-source-statschema/scripts' && uv run -q --no-project --with pyyaml --with sqlglot python -m unittest -q test_tools.py 2>&1"))
    s.check("lfc_source_contract.jdbc: Maven driver and allow-list, TLS URLs, guarded() DO block, JdbcSql, dbapi reads through remote_query and writes through JDBC (fake spark)",
            ["python3", "-m", "unittest", "-q", SK / "lfc-source-contract/scripts/test_jdbc.py"])
    s.check("lfc_source_contract.slot: ssh words (own key, admin as owner), ssh failures retried, a refused key not, words checked first, the contract",
            ["python3", "-I", SK / "lfc-source-contract/scripts/test_slot.py"])
    s.check("lfc_source_contract.cdc: publication FOR TABLE created, kept, tables added, a failed add explained; the slot (fake cursor)",
            ["python3", "-m", "unittest", "-q", SK / "lfc-source-contract/scripts/test_cdc.py"])
    s.check("lfc-saas-pipeline: package calls (fake API), each notebook and its bundle read the same tables through the same connection",
            ["python3", "-m", "unittest", "-q", SK / "lfc-saas-pipeline/tests/test_lfc_saas_pipeline.py"])
    s.check("lfc_saas_pipeline: the package's pipeline specs, runs and checks (fakes)",
            ["python3", "-m", "unittest", "-q", SK / "lfc-saas-pipeline/tests/test_lfc_saas_pipeline.py"])
    s.check("lakebase-source.py on a mocked workspace: new and existing login and connection, password logins, a missing or new project, no password printed",
            s.sh(f"cd '{SK}/lfc-source-contract/scripts' && uv run -q --no-project --with databricks-sdk --with 'psycopg[binary]' "
                 "python -m unittest -q test_lakebase_source.py 2>&1"))
    s.check("size_query.py: the sizing statement size.sh and a notebook run (each engine's arguments, MariaDB on the MySQL query, missing --database / --service)",
            ["python3", "-m", "unittest", "-q", SK / "lfc-source-sizing/scripts/test_size_query.py"])
    s.check("workload contract: every statschema schema is model format 1 (validate-model.py)",
            ["python3", WC / "validate-model.py", *sorted(glob.glob(str(SK / "lfc-source-statschema/schemas/*.yaml")))])
    s.check("workload contract: workload-env.sh and validate-model.py on their fixtures",
            s.sh(f"'{WC}/test-workload-env.sh' > /dev/null"))
    s.check("copies.sh pg on a fake psql: table names, the DDL order, an existing copy, a failed DDL",
            s.sh(f"'{DG}/test-copies.sh' > /dev/null"))
    s.check("lfc_source_datagen.sources and tpch_sources: parsing, the SQL files, each model's DDL, a new source's steps in order, resume, kept, broken rules (fake connection)",
            s.sh(f"cd '{DG}' && uv run -q --no-project --with 'psycopg[binary]' --with pyyaml --with sqlglot python -m unittest -q test_sources.py 2>&1"))
    s.check("tpc_schema.py: TPC-B and TPC-H text as the shell version wrote it (sha-256), tpc-schema.sh runs it, the scale floor",
            ["python3", "-m", "unittest", "-q", DG / "test_tpc_schema.py"])
    s.check("workload.sh: a tpcb workload at SF 0.01 and a postgres spec tpch one (with verify) that workload-env.sh accepts; oracle refused",
            lambda: wl_offline(s))
    s.check("slot.py source (slot-source.sh): a slot's contract, and its SRC_SQL through slot.py sql and a fake ssh",
            s.sh(f"'{SK}/lfc-source-cloud-vm/scripts/test-slot-source.sh' > /dev/null"))
    s.check("lfc-vm masters export, slot-vm-setup masters (lfc-vm masters import), and the clone admin password change (fake podman, local Blob server)",
            s.sh(f"'{SK}/lfc-source-cloud-vm/scripts/test-slot-masters.sh' > /dev/null"))
    s.check("moved-lines.py on a throwaway repo", s.sh(f"'{R}/.claude/skills/public-skill-conventions/scripts/test-moved-lines.sh' > /dev/null"))
    s.check("conventions-check.py on a throwaway repo",
            s.sh(f"'{R}/.claude/skills/public-skill-conventions/scripts/test-conventions-check.sh' > /dev/null"))
    s.check("eventhubs.sh: never creates, re-tags or deletes the group; only its own namespace; a valid Kafka contract (fake az)",
            s.sh(f"'{SK}/lfc-source-eventhubs/scripts/test-eventhubs.sh' > /dev/null"))
    s.check("lfc-kafka-freshness-test connection.sh, pipeline.sh: from a Kafka source contract (fake databricks)",
            s.sh(f"'{SK}/lfc-kafka-freshness-test/scripts/test-scripts.sh' > /dev/null"))
    s.check("sqldb.sh, mi.sh: delete only what lfc-source-azure-sql tagged; not found 3, failed lookup 1 (fake az)",
            s.sh(f"bash '{SK}/lfc-source-azure-sql/scripts/test-delete.sh' > /dev/null"))
    s.check("flex.sh: delete only what lfc-source-azure-flexserver tagged; not found 3, failed lookup 1 (fake az)",
            s.sh(f"bash '{SK}/lfc-source-azure-flexserver/scripts/test-delete.sh' > /dev/null"))
    s.check("lfc_rest: paging, Files API path, poll_until, the API's own error after retries (fake REST)",
            ["python3", "-m", "pytest", "-q", SK / "lfc-rest/tests/test_rest.py"])
    s.check("lfc_watch: flow names, ready/waiting/failed, fail fast, stall, staging (fake REST)",
            ["python3", "-m", "pytest", "-q", SK / "lfc-pipeline-watch/tests/test_watch.py"])
    s.check("check.py: principal not found, missing grants with their GRANT, a group-owned catalog (fake REST)",
            ["python3", "-m", "pytest", "-q", SK / "lfc-workspace-setup/tests/test_check.py"])
    s.check("ready.py: saved profile and catalog; previews turned on only as admin (fake REST)",
            ["python3", "-m", "pytest", "-q", SK / "lfc-workspace-setup/tests/test_ready.py"])
    s.check("check_defaults.py: the stated defaults (defaults.yaml) still match the code",
            ["python3", R / "bin/check_defaults.py"])
    s.check("check_defaults.py: a drifted phrase fails", s.sh(
        "T=$(mktemp -d); printf 'defaults:\\n  - {id: x, area: a, value: v, why: w, in: [{file: env.sh, has: "
        "no-such-phrase-here}]}\\n' > $T/d.yaml; ! python3 '@R@/bin/check_defaults.py' --file $T/d.yaml > /dev/null; "
        "rc=$?; rm -rf $T; exit $rc"))
    s.check("check_defaults.py: a forbidden pattern found under skills/ fails, with the file and line", s.sh(
        "T=$(mktemp -d); printf 'defaults:\\n  - {id: x, area: a, value: v, why: w, forbid: [{in: [skills], pattern: "
        "\"set -uo pipefail\"}]}\\n' > $T/d.yaml; python3 '@R@/bin/check_defaults.py' --file $T/d.yaml > $T/out; rc=$?; "
        "grep -q 'which the default rules out' $T/out; g=$?; rm -rf $T; [ $rc = 1 ] && [ $g = 0 ]"))
    s.check("settings.sh: set/get quoting, modes, require without CLI calls, workspaces and azure listings (fake CLIs)",
            s.sh(f"bash '{SK}/lfc-test-settings/scripts/test-settings.sh' > /dev/null"))
    s.check("ws-token.sh: TO (perl alarm timeout)", s.sh(f"'{SK}/account-token-fallback/scripts/test-ws-token.sh' > /dev/null"))
    s.check("options-first: ranking, pick, check, index", s.sh(f"'{SK}/options-first/scripts/test-options.sh' > /dev/null"))
    s.check("options-first: every options.yaml in the skills is valid", s.sh(
        f"files=$(find '{SK}' -name options.yaml); [ -z \"$files\" ] || uv run -q '{SK}/options-first/scripts/options.py' check $files"))
    os.environ["WANT"] = "10.0.0.0/24 10.9.0.0/24"
    os.environ["HAVE"] = "10.0.0.0/16"
    s.check("q.sh missing.sql", s.sh(f"'{SK}/shell-json-scripting/scripts/q.sh' '{SK}/lfc-source-firewall/scripts/missing.sql' "
                                     "| grep -qE \"^MISSING='?10.9.0.0/24'?$\""))
    s.check("q.py missing.sql", s.sh(f"uv run -q '{SK}/shell-json-scripting/scripts/q.py' '{SK}/lfc-source-firewall/scripts/missing.sql' "
                                     "| grep -qE \"^MISSING='?10.9.0.0/24'?$\""))
    s.check("zerobus-sdk-freshness-test: producer.py's run file (dups_sent, Kafka errors, a dead stream, a failed flush), report.py's counts, the probe's timeout (fake SDKs)",
            ["python3", "-m", "unittest", "-q", SK / "zerobus-sdk-freshness-test/tests/test_scripts.py"])
    s.check("zerobus-debezium-test canonical_sql: the column SQL the scripts share, against the copies it replaced",
            ["python3", "-m", "unittest", "-q", SK / "zerobus-debezium-test/tests/test_canonical_sql.py"])
    s.check("pipeline-diagram.py on saved pipeline JSON (gateway on an AG, integrated, Kafka)",
            ["python3", "-m", "unittest", "-q", SK / "lfc-pipeline-diagram/tests/test_pipeline_diagram.py"])
    s.check("lfc_medallion merge_route: one route per table across sources, last run's routes, the plan written, released and merged; merged_tables; the entry point (stubs)",
            ["python3", "-m", "unittest", "-q", SK / "lfc-medallion/tests/test_route.py"])
    s.check("lfc_medallion paths and engine: the MERGE optimizer's k and engine by cost, a cold start, forced paths, the refit, the warehouse adapter's values",
            ["python3", "-m", "unittest", "-q", SK / "lfc-medallion/tests/test_paths.py"])
    s.check("lfc_change_source: the contract (positions, last change per key) and the Lakeflow Connect bronze adapter (snapshot, changes with _op and _seq, watermark, stats)",
            ["python3", "-m", "unittest", "-q", SK / "lfc-change-source/tests/test_change_source.py"])
    s.check("lfc_medallion methods: refresh techniques from an event log, each technique's time against its own input, fit_r2",
            ["python3", "-m", "unittest", "-q", SK / "lfc-medallion/tests/test_methods.py"])
    s.check("lfc_medallion merge_all: copy, change-feed MERGE, unchanged, full refresh, new column, SCD type 2 key, stack_all skip (fake spark)",
            ["python3", "-m", "unittest", "-q", SK / "lfc-medallion/tests/test_merge.py"])
    s.check("lfc_fanin: names quoted, what each connector type refuses, folders not written yet, an added connector type (fake spark)",
            ["python3", "-m", "unittest", "-q", SK / "lfc-fanin/tests/test_fanin.py"])
    s.check("lfc_log_silver: build and MERGE SQL from a change source, positions kept per table, the staged apply, a round by method, AUTO CDC lists (fake session)",
            ["python3", "-m", "unittest", "-q", SK / "lfc-log-silver/tests/test_log_silver.py"])
    s.check("lfc_medallion coltypes: the widening rule across sources, schema-union's answers kept, Spark types, aligned() on local Spark (pyspark through uv: Databricks Connect's pyspark opens no local session)",
            ["uv", "run", "-q", "--no-project", "--with", "pyspark", "python", "-m", "unittest", "-q",
             SK / "lfc-medallion/tests/test_coltypes.py"])
    s.check("lfc_medallion evidence: bronze keys, history after the snapshot, inferred foreign keys, the connection's catalog, roles, routes, hints (fake spark)",
            ["python3", "-m", "unittest", "-q", SK / "lfc-medallion/tests/test_evidence.py"])
    for m in ["tpch", "tpcb", "tpcdi"]:   # the rules moved into lfc_medallion.roles: classify.py still gets every role in these right
        s.check(f"classify.py {m}: every role as the schema's role: says",
                ["uv", "run", "-q", SK / "cdc-table-plan/scripts/classify.py", SK / f"lfc-source-statschema/schemas/{m}.yaml",
                 "--truth", "schema"])
    s.check("lfc_introspection: schema.yaml and stats.yaml from the catalog (three statements, types per engine, keys, sizes; fake session)",
            ["uv", "run", "-q", "--no-project", "--with", "pyyaml", "--with", "sqlglot", "python", "-m", "unittest", "-q",
             SK / "lfc-introspection/tests/test_introspect.py"])
    s.check("lfc_introspection history: on demand, kept by run (rows, changes between runs, pending requests, a failed run recorded; fake session)",
            ["python3", "-m", "unittest", "-q", SK / "lfc-introspection/tests/test_history.py"])
    s.check("statschema canonical.convert: a collector's raw document as to-canonical.py wrote it",
            ["uv", "run", "-q", "--no-project", "--with", "pyyaml", "--with", "sqlglot", "python", "-m", "unittest", "-q",
             SK / "lfc-source-statschema/tests/test_canonical.py"])
    s.check("lfc_change_source land filtered: LIST, read once a file and table (a table added to the filter read for), a file being written waits (fake session)",
            ["python3", "-m", "unittest", "-q", SK / "lfc-change-source/tests/test_land_filtered.py"])
    s.check("lfc_change_source moved: a moved table read as its old log up to the cut and its new one after it (the overlap once)",
            ["python3", "-m", "unittest", "-q", SK / "lfc-change-source/tests/test_moved.py"])
    s.check("lfc_change_source producer: a split make before break (steps, order, the cut, refusals), Debezium properties retargeted",
            ["python3", "-m", "unittest", "-q", SK / "lfc-change-source/tests/test_producer.py"])
    s.check("lfc_routing shapes: repeating tables grouped by shape (1,000 tenants: 2 groups, a variant, rows summed, classify once a group)",
            ["uv", "run", "-q", "--no-project", "--with", "pyyaml", "python", "-m", "unittest", "-q", SK / "lfc-routing/tests/test_shapes.py"])
    s.check("lfc_routing snapshots: the provisional snapshot's tables (small, with statistics) and step 7's keep or take",
            ["python3", "-m", "unittest", "-q", SK / "lfc-routing/tests/test_snapshots.py"])
    s.check("lfc_routing rounds: a classify round from the tables (latest introspection, the log against the last round, an override, groups; fake session)",
            ["uv", "run", "-q", "--no-project", "--with", "pyyaml", "--with", "sqlglot", "python", "-m", "unittest", "-q",
             SK / "lfc-routing/tests/test_rounds.py"])
    s.check("lfc_routing ingest: a plan carried out through a landing producer (logs, snapshots recorded, keep or take, resnapshot; fakes)",
            ["uv", "run", "-q", "--no-project", "--with", "pyyaml", "python", "-m", "unittest", "-q", SK / "lfc-routing/tests/test_ingest.py"])
    s.check("lfc_log_silver planned: cursor families, a group a log and family, upto below each log's newest, a round a family (fakes)",
            ["python3", "-m", "unittest", "-q", SK / "lfc-log-silver/tests/test_planned.py"])
    s.check("lfc_routing routing: the four classes, groups and the rules out, the suggestion, cut-overs across logs, route changes, the snapshot method",
            ["python3", "-m", "unittest", "-q", SK / "lfc-routing/tests/test_routing.py"])
    s.check("lfc_routing classify: the plan from files (keys, observed over activity, decisions, round, the routing tables' rows)",
            ["uv", "run", "-q", "--no-project", "--with", "pyyaml", "python", "-m", "unittest", "-q", SK / "lfc-routing/tests/test_classify.py"])
    s.check("classify.py never reads role: (TPC-E's plan is the same without it); --truth schema scores against it",
            ["uv", "run", "-q", "--no-project", "--with", "pyyaml", "python", "-m", "unittest", "-q", SK / "cdc-table-plan/tests/test_classify.py"])
    s.check("lfc_medallion: sources and tables found, one n:1 view per table, a table some sources lack (fake spark)",
            ["python3", "-m", "unittest", "-q", SK / "lfc-medallion/tests/test_stack.py"])
    s.check("lfc_medallion_pack: 20-70-10 tiers, pipelines within the table limit, render, the TPC-B marts against the model, timings and predict",
            s.sh(f"PYTHONPATH='{SK}/lfc-medallion-pack/scripts:{SK}/lfc-medallion/scripts:{SK}/lfc-change-source/scripts:{DG}:{SS}:"
                 f"{SK}/lfc-source-contract/scripts' uv run -q --no-project --with pyyaml --with sqlglot python -W ignore -m "
                 f"unittest -q '{SK}/lfc-medallion-pack/tests/test_pack.py' 2>&1"))
    s.check("history-snapshot-scd2.py: the guards (a reload not finished, an emptied table); with pyspark + Java 17/21 also their DataFrame form and versions",
            ["python3", "-m", "unittest", "-q", SK / "lfc-downstream-history/tests/test_history_snapshot_scd2.py"])
    s.check("medallion.py: layer counts, how each table is written; the medallion box and the Zerobus drawing",
            ["python3", "-m", "unittest", "-q", SK / "lfc-pipeline-diagram/tests/test_medallion.py"])
    s.check("pipeline-diagram.py's model table counts match the statschema schemas", ["python3", R / "bin/check_model_counts.py"])

    s.check("lfc_events, lfc_runtime, compute_report, stop_after: the summary's sections, the runtime snapshot diff, an update's compute facts, the stop poll (fake REST)",
            ["python3", "-m", "unittest", "-q", R / "bin/test_pipeline_tools.py"])
    s.check("workspace file tools: put, get, status; the Genie copy (note after the frontmatter, the marker, a folder not ours left alone); sync's push and its lists (fake REST)",
            ["python3", "-m", "unittest", "-q", R / "bin/test_workspace_tools.py"])
    s.check("run.py and the runners: prompt fill, gate arguments, plugin cache, batch allowlist, matrix batches, the plugin hook, the driver's helpers",
            ["python3", "-m", "unittest", "-q", R / "bin/test_runner_tools.py"])

    # the schema-change matrix: the results validate and render to the committed table; the commands run offline
    s.check("ddl-matrix: every result file validates",
            ["uv", "run", "-q", DM / "ddl-matrix-validate.py", *sorted(glob.glob(str(R / "results/ddl-matrix/*.yaml")))])
    s.check("ddl-matrix: the rendered table is the committed one", s.sh(
        f"uv run -q '{DM}/ddl-matrix.py' render | diff -q - '{SK}/lfc-schema-change-test/references/ddl-matrix-results.md'"))
    s.check("ddl-matrix: sql prints the change-tracking setup and the case", s.sh(
        f"uv run -q '{DM}/ddl-matrix.py' sql sqlserver ct --cases int-to-bigint | grep -q 'ENABLE CHANGE_TRACKING'"))
    s.check("ddl-matrix: bundle has one table per case", s.sh(
        f"uv run -q '{DM}/ddl-matrix.py' bundle --connection c --catalog k --schema s --source-catalog d --cases "
        f"int-to-bigint,drop-column --out '@T@/b.yml' && grep -c source_table '@T@/b.yml' | grep -qx 2"))
    s.check("ddl-matrix: collect on saved events: a restart with its row, a stop, and an unfinished update",
            ["sh", "-c", sub(ddl_matrix_collect_script(), s.T)])
    db_section(s)
    return s.finish()


def db_section(s: Suite) -> None:
    """The database modes: PSQL / MYSQL (a laptop's clients), LIVE_PSQL / LIVE_MYSQL / LIVE_ENGINE (the cloud VM)."""
    pg_dir = os.environ.get("DIR_PG") or str(DG / "postgres")
    my_dir = os.environ.get("DIR_MY") or str(DG / "mysql")
    if os.environ.get("PSQL"):
        s.db("pg", "postgres", os.environ["PSQL"], pg_dir)
    if os.environ.get("MYSQL"):
        s.db("my", "mysql", os.environ["MYSQL"], my_dir)
    if os.environ.get("LIVE_PSQL"):
        s.db("pg", os.environ.get("LIVE_DIALECT") or "postgres", os.environ["LIVE_PSQL"], pg_dir)
    if os.environ.get("LIVE_MYSQL"):
        s.db("my", "mysql", os.environ["LIVE_MYSQL"], my_dir)
    if os.environ.get("LIVE_ENGINE"):
        os.environ.setdefault("PY", "python3")
        s.db("py", os.environ.get("LIVE_DIALECT") or os.environ["LIVE_ENGINE"], os.environ["LIVE_ENGINE"], "")


def _count_names(path: Path) -> int:
    """grep -c '^- name:' on a YAML file (0 when the file is missing, as the shell's grep prints nothing)."""
    if not path.is_file():
        return 0
    return sum(1 for ln in path.read_text(errors="replace").splitlines() if ln.startswith("- name:"))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
