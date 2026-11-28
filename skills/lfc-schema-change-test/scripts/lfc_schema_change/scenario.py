"""scenario: the shared steps of a Lakeflow Connect scenario test (what lib.sh did for the shell scripts), on REST.

    with Run("addcol") as r:                       # pre-flight, names, the source; cleanup at the end (kept 14 days)
        r.db.create("CREATE TABLE lfcddemo.t (id INT PRIMARY KEY, v VARCHAR(20));")
        r.db.utility_objects(); r.db.track("ct", ["t"]); r.db.fill("t", 10)
        r.pipelines_create([r.obj("t")])           # MODE=standard: gateway + ingestion; integrated: one pipeline
        r.update()                                 # follows the chain of updates until the pipeline is idle
        r.claim("loads", "PASS" if r.q(f"SELECT COUNT(*) FROM {r.t}.t") == "10" else "FAIL", "10 rows")
        r.result_md("my-scenario")                 # the report (OUT), and the run recorded (lfc-test-results)

Settings (environment, as the shell scripts took them): PROFILE (the laptop's workspace, or --profile; none inside a
workspace: the notebook's own login), CATALOG, and the source: SOURCE_JSON (a contract,
lfc-source-contract) or SRC_SQL + CONN_USER + CONNECTION. MODE (standard | integrated), GATEWAY_WAIT (90 s after a source
change), GATEWAY_COMPUTE (serverless | classic), PIPELINE_CONF (JSON), PIPELINE_CONTINUOUS, EARLY_STOP (1), PREFIX,
WAREHOUSE, LFC_DEADLINE / LFC_DEADLINE_MIN, KEEP (1: leave everything), KEEP_DAYS (14; 0 deletes at the end),
KEEP_SOURCE, OUT, DIAGRAM (1), RECORD (1), SRC_ENV (a command printing the source's environment, one fact a line).
Only SQL Server sources so far (ENGINE=sqlserver); the PostgreSQL scenarios still run on lib.sh.
"""
from __future__ import annotations

import contextlib
import datetime as dt
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path

from lfc_change_source import cdc_pipeline, pipeline
from lfc_change_source.engine import Warehouse
from lfc_rest import Rest, not_found
from lfc_source_contract import sqlserver
from lfc_source_contract.srcsql import Runner
from lfc_watch import core as watch

from . import (
    REPO,
    SKILLS,
    kept,
)

DONE = ("COMPLETED", "FAILED", "CANCELED")
REFRESH_ERRORS = re.compile("CDC_INCOMPATIBLE_SCHEMA_CHANGES|PRIMARY_KEY_CHANGE|CDC_LOG_TRUNCATED")


class Setup(Exception):
    """A step before the claims failed (exit 2): the run tests nothing."""


class Run:
    Setup = Setup          # a scenario raises r.Setup(...) for a failed set-up step

    def __init__(self, short: str, env=None, api=None, warehouse=None, src=None, clock=time.time, sleep=time.sleep,
                 log=None):
        e = dict(os.environ if env is None else env)
        self.e, self.clock, self.sleep = e, clock, sleep
        self.log = log or (lambda *a: print(*a, flush=True))   # flushed: a log file shows progress as it goes
        self.source_json = e.get("SOURCE_JSON", "")
        try:
            self.src = src or (Runner.from_contract(self.source_json) if self.source_json else Runner(e.get("SRC_SQL", "")))
        except ValueError as x:
            raise Setup(f"{x}: set SOURCE_JSON, or SRC_SQL with CONN_USER and CONNECTION")
        se = self.src.env
        self.profile = e.get("PROFILE") or None   # none in a workspace: the notebook's own login
        self.catalog = need(e, "CATALOG")
        self.connection = e.get("CONNECTION") or se.get("CONNECTION") or need(e, "CONNECTION")
        self.conn_user = e.get("CONN_USER") or se.get("CONN_USER") or need(e, "CONN_USER")
        self.engine = e.get("ENGINE", "sqlserver")
        if self.engine != "sqlserver":
            raise Setup(f"ENGINE={self.engine}: only sqlserver is in Python so far; the PostgreSQL scenarios run on lib.sh")
        self.mode = e.get("MODE", "standard")
        if self.mode not in ("standard", "integrated"):
            raise Setup("MODE must be standard or integrated")
        self.wait = 0 if self.mode == "integrated" else int(e.get("GATEWAY_WAIT") or 90)
        self.keep_all, self.keep_days = e.get("KEEP") == "1", int(e.get("KEEP_DAYS") or 14)
        self.keep_source = e.get("KEEP_SOURCE") == "1"
        self.extra_pipelines, self.extra_schemas, self.extra_sql = [], [], ""
        self.api = api or Rest.connect(self.profile)
        self.host = (getattr(self.api, "host", "") or "").rstrip("/")
        self.results, self.examples, self.metrics, self.conf = [], [], {}, {}
        self.failed = False
        self.ap = self.gw = ""
        self.state, self.errs, self.chain, self.early = "", [], "", False
        self.last_events: list = []
        self.preflight(short, warehouse)

    # ---- set-up
    def preflight(self, short: str, warehouse) -> None:
        me = self.api.get("/api/2.0/preview/scim/v2/Me").get("userName", "")
        self.prefix = self.e.get("PREFIX") or prefix_of(me)
        root = self.api.get(f"/api/2.1/unity-catalog/catalogs/{self.catalog}").get("storage_root") or ""
        try:
            default = self.api.get("/api/2.1/unity-catalog/external-locations/__databricks_managed_storage_location"
                                   ).get("url") or ""
        except Exception:  # noqa: BLE001  most workspaces have none
            default = ""
        if not root:
            self.log(f"catalog {self.catalog} has no storage root of its own: using the metastore's")
        if default and root.startswith(default):
            raise Setup(f"catalog {self.catalog} is on Default Storage: CDC refuses it")
        ct = self.api.get(f"/api/2.1/unity-catalog/connections/{self.connection}").get("connection_type")
        if ct != "SQLSERVER":
            raise Setup(f"{self.connection} is not SQLSERVER ({ct})")
        if warehouse is not None:
            self.wh = warehouse
        else:
            wid = self.e.get("WAREHOUSE") or next((w["id"] for w in self.api.get("/api/2.0/sql/warehouses").get(
                "warehouses") or [] if w.get("enable_serverless_compute")), "")
            if not wid:
                raise Setup("no serverless SQL warehouse: set WAREHOUSE")
            self.wh = Warehouse(wid, self.api)
        self.epoch = int(self.clock())
        dl = self.e.get("LFC_DEADLINE") or (str(self.epoch + int(self.e["LFC_DEADLINE_MIN"]) * 60)
                                            if self.e.get("LFC_DEADLINE_MIN") else "")
        self.deadline = int(dl) if dl else None
        base = f"{self.prefix}_" + re.sub(f"^{self.prefix}_", "", self.connection.replace("-", "_"))
        self.db = sqlserver.Database(self.src, f"{self.prefix}_{short}_{self.epoch}", self.conn_user, log=self.log,
                                     sleep=self.sleep)
        self.schema = f"{base}_{self.epoch}"
        self.staging = f"{self.schema}_stg" if self.mode == "standard" else ""
        self.gwn, self.apn = f"{base}_gw_{self.epoch}", f"{base}_ap_{self.epoch}"
        self.slot = ""
        self.t = f"`{self.catalog}`.{self.schema}"
        self.log(f"run {self.epoch} ({self.mode}): source database {self.db.name}, catalog {self.catalog}, schemas "
                 f"{self.schema}{f' and {self.staging}' if self.staging else ''}")

    @property
    def src_owned(self) -> bool:
        """This run made the source database (its name ends in the run's epoch)."""
        return self.db.name.endswith(f"_{self.epoch}")

    # ---- SQL
    def q(self, sql: str) -> str:
        """The rows tab-separated, one a line (NULL as empty), as the shell printed them; RuntimeError on a failure."""
        rows = self.wh.sql(sql, raw=True).collect()
        return "\n".join("\t".join("" if v is None else str(v) for v in r) for r in rows)

    def q_or(self, sql: str) -> str:
        """q, or the error's text (a claim shows what was seen)."""
        try:
            return self.q(sql)
        except Exception as e:  # noqa: BLE001
            return f"query failed: {str(e)[:200]}"

    def rq(self, sql: str) -> str:
        """sql on the source through the UC connection (what serverless sees)."""
        lit = sql.replace("\\", "\\\\").replace("'", "\\'")   # a Spark SQL literal: '' would join two literals
        return self.q(f"SELECT * FROM remote_query('{self.connection}', database => '{self.db.name}', query => '{lit}')")

    # ---- pipelines
    def obj(self, table: str, scd: str = "SCD_TYPE_1", auto_full_refresh: bool = False, **config) -> dict:
        return cdc_pipeline.table(self.db.name, sqlserver.SCHEMA, table, self.catalog, self.schema, scd,
                                  auto_full_refresh, **config)

    def create_schema(self, name: str, exists_ok: bool = False) -> None:
        try:
            self.api.post("/api/2.1/unity-catalog/schemas", {"name": name, "catalog_name": self.catalog})
        except Exception as e:  # noqa: BLE001
            if not (exists_ok and "ALREADY_EXISTS" in str(e)):
                raise Setup(f"schema {self.catalog}.{name}: {str(e)[:200]}")

    def delete_schema(self, full: str) -> None:
        try:
            self.api.delete(f"/api/2.1/unity-catalog/schemas/{full}", force="true")
        except Exception as e:  # noqa: BLE001
            if not not_found(e):
                self.log(f"schema {full}: not deleted: {str(e)[:200]}")

    def pipelines_create(self, objects: list, soft: bool = False) -> bool:
        """The schemas, then a gateway and an ingestion pipeline (standard) or one pipeline (integrated). soft: a
        refused create returns False with the error in self.create_err (a test asking "is it refused?")."""
        conf = json.loads(self.e.get("PIPELINE_CONF") or "{}")
        if conf:
            self.conf["pipeline_conf"] = json.dumps(conf, separators=(",", ":"))
        self.create_schema(self.schema)
        if self.mode == "standard":
            self.create_schema(self.staging, exists_ok=True)
            try:
                self.gw = pipeline.create(self.api, cdc_pipeline.gateway(
                    self.gwn, self.connection, self.catalog, self.staging,
                    classic=self.e.get("GATEWAY_COMPUTE") == "classic", configuration=conf))
            except Exception as e:  # noqa: BLE001
                raise Setup(f"gateway create failed: {str(e)[:300]}")
        spec = cdc_pipeline.ingestion(self.apn, self.catalog, self.schema, objects, gateway_id=self.gw,
                                      connection="" if self.gw else self.connection,
                                      continuous=self.e.get("PIPELINE_CONTINUOUS") == "1", configuration=conf)
        try:
            self.ap = pipeline.create(self.api, spec)
        except Exception as e:  # noqa: BLE001
            self.create_err = str(e)[:300]
            if soft:
                return False
            raise Setup(f"ingestion pipeline create failed: {self.create_err}")
        self.log(f"LINK pipeline {self.host}/pipelines/{self.ap}")
        if self.e.get("DIAGRAM", "1") == "1":
            subprocess.run(["bash", str(SKILLS / "lfc-pipeline-diagram/scripts/show-diagram.sh"), self.ap],
                           env={**os.environ, **({"PROFILE": self.profile} if self.profile else {})}, check=False)
        return True

    def set_objects(self, objects: list) -> None:
        """Replace the ingestion pipeline's table list (the whole spec goes back)."""
        spec = self.api.get(f"/api/2.0/pipelines/{self.ap}")["spec"]
        spec["ingestion_definition"]["objects"] = list(objects)
        self.api.put(f"/api/2.0/pipelines/{self.ap}", spec)

    # ---- updates
    def past_deadline(self) -> bool:
        return self.deadline is not None and self.clock() >= self.deadline

    def fp(self, tables: list) -> str:
        """Row counts of the tables and the staging table, and the schema's column count: unchanged for 3 polls
        means integrated CDC has nothing left to do."""
        parts = [f"(SELECT COUNT(*) FROM {self.t}.{t})" for t in [*tables, f"__{self.ap.replace('-', '_')}_cdc_staging_table"]]
        parts.append(f"(SELECT COUNT(*) FROM `{self.catalog}`.information_schema.columns WHERE table_schema = "
                     f"'{self.schema}')")
        try:
            return self.q("SELECT " + ", ".join(parts))
        except Exception:  # noqa: BLE001  a table not made yet: not stable
            return ""

    def snaps_done(self, update_id: str, tables: list) -> bool:
        """The extractor finished reading each listed table in that update (its snapshot flow reported COMPLETED)."""
        evs = watch.events(self.api, self.ap, max_events=1000)
        for t in tables:
            pat = re.compile(f"^extractor_.*_{re.escape(t)}_snapshot_flow$")
            if not any(e.get("origin", {}).get("update_id") == update_id and e.get("event_type") == "flow_progress"
                       and ((e.get("details") or {}).get("flow_progress") or {}).get("status") == "COMPLETED"
                       and pat.match(e.get("origin", {}).get("flow_name") or "") for e in evs):
                return False
        return True

    def update(self, refresh: list | None = None, fp_tables: list | None = None, polls: int = 90,
               every: float = 20, _again: bool = True) -> str:
        """Start an update (refresh: the tables to fully refresh) and wait until the pipeline is idle, following the
        updates the pipeline starts itself after a failure (RETRY_ON_FAILURE, SCHEMA_CHANGE). self.state: the last
        one's state; self.errs: the error classes of every update in the chain; self.chain: each one's cause and
        state. Integrated CDC (EARLY_STOP=1): an update that ran 3 minutes is stopped once fp(fp_tables) is
        unchanged for 3 polls (self.early). LFC_DEADLINE: the running update is stopped and reported timed out."""
        self.fp_tables = fp_tables if fp_tables is not None else getattr(self, "fp_tables", None)
        try:
            u = pipeline.start_update(self.api, self.ap, refresh)
        except Exception as e:  # noqa: BLE001  one still running (a retry the pipeline started): follow it
            why = str(e).splitlines()[0][:200] if str(e) else e.__class__.__name__
            u = pipeline.newest_update(self.api, self.ap)
            running = u and pipeline.get_update(self.api, self.ap, u).get("state") not in DONE
            if not running:   # nothing to follow: the refusal is this step's result
                self.state, self.chain, self.early, self.last_events = "REFUSED", f"start refused: {why}", False, []
                self.errs = ["START_UPDATE_REFUSED"]
                self.log(f"update refused: {why}")
                return self.state
            self.log(f"start-update refused ({why}); following {u}")
            self._follow(u, None, polls, every)    # not this step's update: then the requested one, once
            if not _again:
                return self.state
            first = (self.last_events, self.errs, self.chain)
            self.update(refresh, fp_tables, polls, every, _again=False)
            # the followed update is often the one that carries the step's errors (a SCHEMA_CHANGE retry): keep them
            self.last_events = first[0] + self.last_events
            self.errs = sorted(set(first[1]) | set(self.errs))
            self.chain = f"{first[2]} -> {self.chain}" if first[2] else self.chain
            return self.state
        return self._follow(u, refresh, polls, every)

    def _follow(self, u: str, refresh, polls: int, every: float) -> str:
        """Wait for update u and the updates the pipeline starts after it (see update)."""
        ids, chain, self.early = [], [], False
        early_stop = self.mode == "integrated" and self.e.get("EARLY_STOP", "1") == "1"
        while True:
            ids.append(u)
            t0, prev, stable, note, state = self.clock(), None, 0, "", ""
            for _ in range(polls):
                state = pipeline.get_update(self.api, self.ap, u).get("state", "")
                if state in DONE:
                    break
                if self.past_deadline():
                    pipeline.stop(self.api, self.ap)
                    note = " (timed out: deadline)"
                    self.stopped(u, every)
                    break
                if early_stop and state == "RUNNING":
                    f = self.fp(self.fp_tables or [])
                    stable = stable + 1 if f and f == prev else 0
                    prev = f
                    if refresh and not self.snaps_done(u, refresh):
                        stable = 0    # a refresh: count only after the extractor read the tables
                    if stable >= 3 and self.clock() - t0 >= 180:
                        pipeline.stop(self.api, self.ap)
                        self.early = True
                        note = f" (stopped early after {int(self.clock() - t0)}s)"
                        state = self.stopped(u, every)
                        break
                self.sleep(every)
            chain.append(f"{pipeline.get_update(self.api, self.ap, u).get('cause', '')} {state}{note}")
            if state not in DONE:
                break           # timed out: don't follow further
            self.sleep(30)      # a follow-up update starts within seconds of the last one ending
            nxt = pipeline.newest_update(self.api, self.ap)
            if not nxt or nxt == u:
                break
            u = nxt
        self.state, self.chain = state, " -> ".join(chain)
        evs = watch.events(self.api, self.ap, max_events=1000)
        self.last_events = [e for e in evs if (e.get("origin") or {}).get("update_id") in set(ids)]
        self.errs = sorted({x.get("error_class") for e in self.last_events
                            for x in (e.get("error") or {}).get("exceptions") or [] if x.get("error_class")})
        what = f"(full refresh {','.join(refresh)}) " if refresh else ""
        self.log(f"update {what}{state} "
                 f"{' '.join(self.errs)}{' ' if self.errs else ''}[{self.chain}]")
        for m in sorted({json.dumps(x.get("message", "")) for e in self.last_events
                         for x in (e.get("error") or {}).get("exceptions") or [] if x.get("error_class")}):
            self.log(f"  error: {m[:300]}")
        return state

    def has(self, name: str, table: str = "") -> bool:
        """An error of the last update (with table: of that table's flows) whose class contains name: classes come
        qualified (CDC_INCOMPATIBLE_SCHEMA_CHANGES.PRIMARY_KEY_CHANGE), as lib.sh's substring match took them."""
        return any(name in c for c in (self.errs_for(table) if table else self.errs))

    def stopped(self, u: str, every: float, polls: int = 30) -> str:
        """After a stop, the update's state once it has ended (a stopping update refuses the next start)."""
        state = ""
        for _ in range(polls):
            state = pipeline.get_update(self.api, self.ap, u).get("state", "")
            if state in DONE:
                break
            self.sleep(min(every, 10))
        return state

    def ok_state(self) -> bool:
        return self.state == "COMPLETED" or self.early

    def _for(self, table: str):
        pat = re.compile(f"_{table}_")
        return [x for e in self.last_events if pat.search((e.get("origin") or {}).get("flow_name") or "")
                for x in (e.get("error") or {}).get("exceptions") or [] if x.get("error_class")]

    def errs_for(self, table: str) -> list:
        """The error classes the last update reported for that table's flows."""
        return sorted({x["error_class"] for x in self._for(table)})

    def msgs_for(self, table: str) -> list:
        return sorted({(x.get("message") or "")[:200] for x in self._for(table)})

    def needs_refresh(self, table: str) -> bool:
        """An error for that table that asks for a full refresh (the hand-off errors of a schema change don't, and a
        needless refresh resets an SCD type 2 table's history)."""
        return any(REFRESH_ERRORS.search(c) for c in self.errs_for(table))

    def expect(self, want: str, sql: str) -> str:
        """The query's result; after an early stop that didn't give want, one more update first (the last update's
        events are kept, so errs_for still reads the step's)."""
        r = self.q_or(sql)
        if r != want and self.early:
            keep = (self.last_events, self.errs, self.state, self.chain, self.early)   # the step's, read after this
            self.log(f"after an early stop got '{r.replace(chr(9), ' ')}': one more update")
            self.update()             # the step's fingerprint tables (update keeps the last ones)
            r = self.q_or(sql)
            self.last_events, self.errs, self.state, self.chain, self.early = keep
        return r

    def settle(self, table: str, column: str, tries: int = 4, fp_tables=None) -> None:
        """After a source change, updates until one reports an error, or column shows in the destination table."""
        for _ in range(tries):
            self.sleep(self.wait)
            self.update(fp_tables=fp_tables)
            if self.errs:
                return
            with contextlib.suppress(Exception):
                self.q(f"SELECT {column} FROM {self.t}.{table} LIMIT 0")
                return

    def settle_rows(self, sql: str, want: str, table: str = "", tries: int = 4, fp_tables=None) -> bool:
        """After a source DML change, updates until sql gives want, or an update reports an error (with table: that
        table's). self.rows: the last answer."""
        self.rows = ""
        for _ in range(tries):
            self.sleep(self.wait)
            self.update(fp_tables=fp_tables)
            self.rows = self.q_or(sql)
            if self.rows == want:
                return True
            if (self.errs_for(table) if table else self.errs):
                return False
        return False

    # ---- claims and the report
    def claim(self, name: str, result: str, text: str) -> None:
        self.results.append(f"| `{name}` | {result} | {text} |")
        self.failed |= result != "PASS"
        self.log(f"{result} {name}: {text}")

    def check(self, name: str, ok: bool, passed: str, failed: str) -> None:
        self.claim(name, "PASS" if ok else "FAIL", passed if ok else failed)

    def observe(self, name: str, text: str) -> None:
        self.results.append(f"| `{name}` | OBSERVED | {text} |")
        self.log(f"OBSERVED {name}: {text}")

    def example(self, title: str, src_cols: str, src_query: str, dst_cols: str, dst_query: str) -> None:
        """What went in and what came out, side by side in the report (columns "a|b|c"; a few rows each)."""
        def rows(text):
            return [f"| {' | '.join(ln.split(chr(9)))} |" for ln in text.splitlines() if ln]

        def head(cols):
            return [f"| {cols.replace('|', ' | ')} |", "|" + "---|" * (cols.count("|") + 1)]
        try:
            src = self.db.sql("SET NOCOUNT ON; " + src_query + "\n")
        except Exception as e:  # noqa: BLE001
            src = str(e)
        dst = self.q_or(dst_query)
        self.examples += [f"### {title}", "", "Input (source):", "", *head(src_cols), *rows(src), "",
                          "Output (destination):", "", *head(dst_cols), *rows(dst), ""]

    def metric(self, name: str, value: float) -> None:
        self.metrics[name] = value

    def runtime(self, pid: str, summary: bool) -> str:
        if not self.profile:   # bin/lfc-runtime.sh is a laptop CLI tool (in a workspace: not run)
            return ""
        r = subprocess.run([str(REPO / "bin/lfc-runtime.sh"), *(["--summary"] if summary else []), self.profile, pid],
                           capture_output=summary, text=True, check=False)
        return (r.stdout or "").replace("Lakeflow runtime: ", "").splitlines()[0] if summary and r.stdout else ""

    def result_md(self, title: str) -> str:
        """The report: date, mode, each pipeline's runtime, the source, the claims and examples; to stdout and OUT.
        Then the run is recorded."""
        self.runtime(self.ap, summary=False)
        lines = [f"# {title} run {self.epoch}", "", f"- Date (UTC): {dt.datetime.now(dt.timezone.utc).date()}",
                 f"- Mode: standard CDC, {self.e.get('GATEWAY_COMPUTE', 'serverless')} gateway, serverless ingestion "
                 "pipeline" if self.mode == "standard" else "- Mode: integrated CDC, one serverless pipeline",
                 f"- Ingestion pipeline: {self.runtime(self.ap, True)}"]
        if self.gw:
            lines.append(f"- Gateway: {self.runtime(self.gw, True)}")
        if self.e.get("SRC_ENV"):
            r = subprocess.run(shlex.split(self.e["SRC_ENV"]), capture_output=True, text=True, check=False)
            lines += [f"- {ln}" for ln in r.stdout.splitlines() if ln]
        lines += [f"- Source: {sqlserver.version(self.src)}; utility objects {self.db.util or '?'}", "",
                  "| Claim | Result | Observation |", "|---|---|---|", *self.results]
        if self.examples:
            lines += ["", "## Input and output", "", *self.examples]
        text = "\n".join(lines) + "\n"
        print(text, end="")
        if self.e.get("OUT"):
            Path(self.e["OUT"]).write_text(text)
        self.record_run(title)
        return text

    def record_run(self, scenario: str) -> None:
        """One record into results/runs/<date>-lfc-schema-change.jsonl through lfc-test-results' record.py (the
        combination tested and its outcome, no numbers: the repo is public). RECORD=0 skips it."""
        if self.e.get("RECORD", "1") != "1":
            return
        ms = self.api.get("/api/2.1/unity-catalog/metastore_summary")
        today = str(dt.datetime.now(dt.timezone.utc).date())
        counts = {k: sum(f"| {k} |" in r for r in self.results) for k in ("PASS", "FAIL", "OBSERVED")}
        claims = " ".join(f"{m.group(1)}={m.group(2)}" for r in self.results
                          if (m := re.match(r"^\| `([^`]*)` \| ([A-Z]+) \|", r)))
        rec = {"date": today, "run": f"{today}-lfc-schema-change", "test": "icdc" if self.mode == "integrated" else "scdc",
               "subject": {"engine": self.engine, "version": sqlserver.product_version(self.src)},
               "outcome": "fail" if self.failed else "pass",
               "result": f"{scenario}: {counts['PASS']} pass, {counts['FAIL']} fail, {counts['OBSERVED']} observed",
               "workspace": {"cloud": (ms.get("global_metastore_id") or ":").split(":")[0], "region": ms.get("region", "")},
               "setup": {"scenario": scenario, "mode": self.mode, "tool": "lfc-schema-change-test", **self.conf},
               "notes": claims}
        rec_py = SKILLS / "lfc-test-results/scripts/record.py"
        r = subprocess.run(["uv", "run", str(rec_py)], input=json.dumps(rec), text=True, check=False)
        self.log(f"recorded: {scenario} ({rec['result']})" if r.returncode == 0 else "record_run: not recorded (above)")

    # ---- the end
    def drop_slot_for_keep(self) -> str:
        return ""             # PostgreSQL only (its slot holds WAL while the run is kept)

    def drop_source(self):
        """The source database when this run made it (unless KEEP_SOURCE); EXTRA_SQL on one it didn't."""
        if not self.src_owned:
            if self.extra_sql:
                self.db.sql(self.extra_sql)
            return None
        if self.keep_source:
            return None
        return self.db.drop()

    def cleanup(self) -> None:
        if self.keep_all:
            self.log(f"KEEP=1: left {self.db.name}, {self.gwn}, {self.apn}, {self.catalog}.{self.schema}")
        elif self.keep_days:
            kept.keep(self, self.keep_days, log=self.log)
        else:
            kept.delete(self, wait=float(self.e.get("LFC_DELETE_WAIT") or 15), log=self.log)

    def __enter__(self):
        signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))   # a killed run still cleans up
        return self

    def __exit__(self, kind, exc, tb):
        try:
            self.cleanup()
        except Exception as e:  # noqa: BLE001  said; the run's own outcome stands
            self.log(f"cleanup failed: {e}")
        return False


def prefix_of(user: str) -> str:
    """firstnamelastname from the login, as lib.sh made it; a service principal's application id (a job's identity)
    starts with a digit, which a database name can't: sp_ in front."""
    p = re.sub(r"[^a-z0-9_]", "", user.split("@")[0].lower())
    return p if p[:1].isalpha() else f"sp_{p}"


def need(env: dict, key: str) -> str:
    if not env.get(key):
        raise Setup(f"set {key}")
    return env[key]


def main(scenario) -> int:
    """Run a scenario function f(run) -> None in a Run; the exit status: 0, 1 (a claim failed), 2 (set-up)."""
    try:
        with Run(scenario.SHORT) as r:
            try:
                scenario.run(r)
            except Setup:
                raise
            except Exception as e:
                if not r.results:   # before the first claim: a set-up step failed (exit 2, as the shell)
                    raise Setup(f"set-up failed: {e.__class__.__name__}: {str(e)[:400]}") from e
                # after claims: they are kept; the crash is one more FAIL, and the report is written
                r.claim("run-completed", "FAIL", f"the scenario stopped: {e.__class__.__name__}: {str(e)[:300]}")
                with contextlib.suppress(Exception):
                    r.result_md(f"{scenario.SHORT} (stopped)")
            return 1 if r.failed else 0
    except Setup as e:
        print(e, file=sys.stderr)
        return 2
    except Exception as e:  # noqa: BLE001  pre-flight (a REST error before anything was made)
        print(f"set-up failed: {e.__class__.__name__}: {str(e)[:400]}", file=sys.stderr)
        return 2
