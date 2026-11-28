#!/usr/bin/env python3
"""secondary.py <command> [args]: run a Lakeflow Connect SQL Server CDC reader (a gateway, or an integrated CDC
pipeline) on an Always On readable secondary, and move it to the primary for the steps that write to the source
(schema change, full refresh), then back. With GATEWAY_ID unset, the reader is the integrated pipeline PIPELINE_ID.

Commands
  log-delivery                 add cluster log delivery to $LOG_VOLUME to the gateway (saving it starts the gateway);
                               not for integrated CDC: serverless takes no cluster settings
  where                        connection host/port, gateway state, and the gateway's sessions on each node
  switch primary|secondary     point the UC connection at that node, restart the reader, wait until it runs
  stop | start                 stop the reader / start it and wait until it runs
  full-refresh <table>...      start an ingestion update that fully refreshes these destination tables (integrated:
                               stops the running update first; the refresh update is the reader's restart)
  ci <schema.table>            the table's capture-instance state from the source (tracker row + capture instances)
  wait-ci <schema.table> [<ci>]  wait until no old capture instance is left, one capture instance, and (with
                               <ci>) the current one is no longer <ci>: the ADD COLUMN merge or the reset is done
  wait-reset <since-epoch>     wait for "Reinitialization completed successfully" after <since-epoch> (UTC seconds)
                               in the gateway driver log delivered to $LOG_VOLUME
  caught-up                    wait until the secondary has applied everything it received (redo queue 0)
  ddl <schema.table> <file>    planned schema change: switch primary, run <file> on the primary, wait-ci,
                               caught-up, switch secondary
  recover <schema.table>       after an unplanned schema change (the table stopped updating): switch primary,
                               wait-ci, caught-up, switch secondary
  refresh <schema.table> <destination table>  full refresh with the snapshot read from the secondary: switch
                               primary, full-refresh, wait-ci (+ wait-reset when $LOG_VOLUME is set), stop,
                               caught-up, switch secondary; integrated: the pipeline must be triggered, and after the
                               snapshot one more update resets the destination table (waited for here)

Environment
  GATEWAY_ID, PIPELINE_ID      gateway pipeline id (unset for integrated CDC), ingestion pipeline id
  CONNECTION                   the UC connection the gateway uses
  DATABASE                     the source database
  PRIMARY, SECONDARY           <host>,<port> of each node, as the gateway reaches them
  SQL_USER, SQL_PASSWORD       the connection's login (a connection update must repeat them); never printed
  PRIMARY_SQL_CMD, SECONDARY_SQL_CMD  optional: a command that reads T-SQL on stdin and takes sqlcmd's -d/-h/-W/-s/-b
                               flags (default: sqlcmd -C -S <node> with SQL_USER / SQL_PASSWORD)
  LOG_VOLUME                   /Volumes/<catalog>/<schema>/<volume> for the gateway's cluster logs (optional)
  DATABRICKS_CONFIG_PROFILE    CLI profile on the laptop (optional; the SDK reads it); WAIT  seconds before a wait
                               gives up (default 3600)

The Databricks calls go through lfc_rest.Rest (REST paths and JSON), the SQL through sqlcmd (or the commands above).
In a workspace, with no profile, Rest.connect() signs in as the workspace's identity.
Runs with `python3 secondary.py <command> ...`; main(argv) for a notebook.
"""
from __future__ import annotations

import calendar
import datetime as dt
import gzip
import os
import re
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
try:   # installed (pip install the lfc-rest skill's scripts), else beside this skill in a checkout
    from lfc_rest import Rest
except ImportError:
    sys.path.insert(0, str(HERE.parents[1] / "lfc-rest" / "scripts"))
    from lfc_rest import Rest

RUNNING_STATES = ("RUNNING", "INITIALIZING", "SETTING_UP_TABLES", "WAITING_FOR_RESOURCES", "QUEUED", "CREATED", "RESETTING")
DONE_STATES = ("COMPLETED", "CANCELED", "FAILED", "NONE")
LOG_FILE = re.compile(r"log4j-.*\.log(\.gz)?")
RESET_LINE = "Reinitialization completed successfully"

CI_SQL = """SET NOCOUNT ON;
DECLARE @t sysname = (SELECT TOP 1 name FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo')
                      AND name LIKE 'lakeflowCaptureInstanceInfo%' ORDER BY name DESC);
DECLARE @r TABLE (o sysname NULL, n sysname NULL);
IF @t IS NOT NULL INSERT @r EXEC (N'SELECT oldCaptureInstance, newCaptureInstance FROM dbo.' + @t
  + N' WHERE schemaName = ''{sch}'' AND tableName = ''{tbl}''');
SELECT ISNULL((SELECT TOP 1 o FROM @r), 'NULL') + '|'
     + ISNULL((SELECT TOP 1 capture_instance FROM cdc.change_tables
               WHERE source_object_id = OBJECT_ID('[{sch}].[{tbl}]') ORDER BY create_date DESC), 'NULL') + '|'
     + CAST((SELECT COUNT(*) FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('[{sch}].[{tbl}]')) AS varchar(10));
"""
CAUGHT_UP_SQL = """SET NOCOUNT ON;
SELECT synchronization_state_desc + '|' + CAST(ISNULL(log_send_queue_size, 0) AS varchar(20)) + '|'
     + CAST(ISNULL(redo_queue_size, 0) AS varchar(20))
FROM sys.dm_hadr_database_replica_states WHERE is_local = 1 AND database_id = DB_ID();
"""
SESSIONS_SQL = """SET NOCOUNT ON;
SELECT CAST(COUNT(*) AS varchar(10)) + ' gateway sessions|'
     + CAST(DATABASEPROPERTYEX(DB_NAME(), 'Updateability') AS varchar(20))
FROM sys.dm_exec_sessions WHERE program_name LIKE 'lakeflow-connect-%';
"""


class SqlFailed(Exception):
    """A source SQL command that exited nonzero (its own error is on stderr already)."""

    def __init__(self, rc: int):
        super().__init__(f"sql exited {rc}")
        self.rc = rc


def die(msg: str | None, rc: int = 1):
    if msg:
        print(msg, file=sys.stderr)
    raise SystemExit(rc)


def first_row(text: str) -> str:
    """The first output line with a '|' (grep '|' | head -1)."""
    return next((line for line in text.splitlines() if "|" in line), "")


def field2(s: str) -> str:
    """cut -d'|' -f2: the second field (the whole line when it has no '|')."""
    return s.split("|")[1] if "|" in s else s


def jqs(v) -> str:
    """A value as jq's string interpolation shows it: null for none."""
    return "null" if v is None else str(v)


def split_name(name: str) -> tuple[str, str]:
    """schema.table (dbo when there's no schema); the names must be letters, digits and _."""
    if "." in name:
        sch, tbl = name.split(".", 1)
    else:
        sch, tbl = "dbo", name
    if not re.fullmatch(r"[A-Za-z0-9_]*", sch + tbl):
        die(f"bad table name: {name}", 2)
    return sch, tbl


def log_epoch(line: str) -> float | None:
    """The driver log's time (its first 17 characters, "yy/MM/dd HH:mm:ss", UTC) as epoch seconds."""
    try:
        return float(calendar.timegm(time.strptime(line[:17], "%y/%m/%d %H:%M:%S")))
    except ValueError:
        return None


def utc_iso(epoch: int) -> str:
    return dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


class Reader:
    """One reader and its connection, from the environment (see the module docstring). api: a Rest, made on first
    use; sql_run(node, sql) -> stdout is the source's sqlcmd (tests give a fake); sleep and clock: for tests."""

    def __init__(self, env=None, api=None, sql_run=None, sleep=time.sleep, clock=time.monotonic):
        env = os.environ if env is None else env
        self.env = env
        self.v = {k: env.get(k, "") for k in ("GATEWAY_ID", "PIPELINE_ID", "CONNECTION", "DATABASE", "PRIMARY",
                                              "SECONDARY", "SQL_USER", "SQL_PASSWORD", "LOG_VOLUME")}
        self.v["RUN"] = self.v["GATEWAY_ID"] or self.v["PIPELINE_ID"]   # the pipeline that holds the connection
        self.kind = "gateway" if self.v["GATEWAY_ID"] else "pipeline"
        self.wait = int(env.get("WAIT") or 3600)
        self.sql_cmd = {"primary": env.get("PRIMARY_SQL_CMD", ""), "secondary": env.get("SECONDARY_SQL_CMD", "")}
        self._api, self.sleep, self.clock = api, sleep, clock
        self._sql_run = sql_run or self.sqlcmd

    # --- plumbing ---------------------------------------------------------------------------------
    @property
    def api(self) -> Rest:
        if self._api is None:
            self._api = Rest.connect(self.env.get("DATABRICKS_CONFIG_PROFILE") or None)
        return self._api

    def log(self, msg: str) -> None:
        print(f"{time.strftime('%H:%M:%S', time.gmtime())}Z {msg}", file=sys.stderr)

    def need(self, *names: str) -> None:
        for n in names:
            if not self.v.get(n):
                die(f"set {n}", 2)

    def must(self, ok: bool) -> None:
        if not ok:
            raise SystemExit(1)

    def deadline(self) -> float:
        return self.clock() + self.wait

    def gave_up(self, quiet: bool = False) -> bool:
        if not quiet:
            self.log(f"gave up after {self.wait} s")
        return False

    def sqlcmd(self, node: str, sql: str) -> str:
        """sqlcmd on a node, T-SQL on stdin: rows "|" separated, no header. Its errors go to stderr as they are."""
        cmd = self.sql_cmd[node]
        if cmd:   # a command of the user's, through bash, with sqlcmd's flags after it
            argv = ["bash", "-c", f'{cmd} "$@"', "sql-cmd", "-d", self.v["DATABASE"], "-h", "-1", "-W", "-s", "|", "-b"]
            env = None
        else:
            self.need("SQL_USER", "SQL_PASSWORD")
            srv = self.v["PRIMARY"] if node == "primary" else self.v["SECONDARY"]
            argv = ["sqlcmd", "-C", "-S", srv, "-U", self.v["SQL_USER"], "-d", self.v["DATABASE"], "-h", "-1", "-W",
                    "-s", "|", "-b"]
            env = {**os.environ, "SQLCMDPASSWORD": self.v["SQL_PASSWORD"]}
        p = subprocess.run(argv, input=sql, stdout=subprocess.PIPE, text=True, env=env, check=False)
        if p.returncode != 0:
            raise SqlFailed(p.returncode)
        return p.stdout

    def sqlrun(self, node: str, sql: str) -> str:
        return self._sql_run(node, sql)

    # --- source SQL -------------------------------------------------------------------------------
    def ci(self, table: str, node: str = "primary") -> str:
        """"<old>|<current>|<capture instance count>" from the utility objects' tracker, or "" if none."""
        sch, tbl = split_name(table)
        return first_row(self.sqlrun(node, CI_SQL.format(sch=sch, tbl=tbl)))

    def wait_ci(self, table: str, frm: str = "") -> None:
        """Wait until no old capture instance is left, one capture instance, and (frm) the current one is not frm."""
        last, end = "", self.deadline()
        while self.clock() < end:
            try:
                s = self.ci(table, "primary")
            except SqlFailed:
                s = ""
            if s != last:
                self.log(f"{table} capture instances (old|current|count): {s}")
                last = s
            if s.startswith("NULL|") and s.endswith("|1") and len(s) >= 8 and (not frm or s[5:] != f"{frm}|1"):
                return
            self.sleep(10)
        self.log(f"gave up after {self.wait} s")
        raise SystemExit(1)

    def caught_up(self) -> None:
        end = self.deadline()
        while self.clock() < end:
            try:
                s = first_row(self.sqlrun("secondary", CAUGHT_UP_SQL))
            except SqlFailed:
                s = ""
            self.log(f"secondary (state|log send queue KB|redo queue KB): {s or 'no AG state for ' + self.v['DATABASE']}")
            if s.endswith("|0|0"):
                return
            self.sleep(10)
        self.log(f"gave up after {self.wait} s")
        raise SystemExit(1)

    def sessions(self) -> None:
        """Where the gateway's sessions are (program_name lakeflow-connect-<mode>:<replication>), on each node."""
        for node in ("primary", "secondary"):
            try:
                v = first_row(self.sqlrun(node, SESSIONS_SQL)) or "unreachable"
            except SqlFailed:
                v = "unreachable"
            print(f"{node}: {v}")

    # --- Databricks -------------------------------------------------------------------------------
    def gw_state(self) -> str:
        ups = self.api.get(f"/api/2.0/pipelines/{self.v['RUN']}/updates", max_results=1).get("updates") or []
        return (ups[0].get("state") if ups else None) or "NONE"

    def gw_stop(self) -> None:
        self.log(f"stopping {self.kind} {self.v['RUN']}")
        self.api.post(f"/api/2.0/pipelines/{self.v['RUN']}/stop")
        end = self.deadline()
        while self.clock() < end:
            if self.gw_state() in DONE_STATES:
                return
            self.sleep(10)
        raise SystemExit(1)

    def gw_start(self) -> None:
        if self.gw_state() not in RUNNING_STATES:
            self.log(f"starting {self.kind} {self.v['RUN']}")
            self.api.post(f"/api/2.0/pipelines/{self.v['RUN']}/updates", {})
        last, end = "", self.deadline()
        while self.clock() < end:
            s = self.gw_state()
            if s != last:
                self.log(f"{self.kind} {s}")
                last = s
            # a triggered integrated pipeline may finish (COMPLETED) before a poll sees it RUNNING
            if s == "RUNNING":
                return
            if s == "COMPLETED":
                if self.kind == "pipeline":
                    return
                raise SystemExit(1)
            if s in ("FAILED", "CANCELED"):
                raise SystemExit(1)
            self.sleep(15)
        raise SystemExit(1)

    def switch(self, node: str, nostart: bool = False) -> None:
        """Point the connection at a node and restart the reader there. nostart leaves the reader stopped (an
        integrated refresh starts it)."""
        hp = {"primary": self.v["PRIMARY"], "secondary": self.v["SECONDARY"]}.get(node)
        if hp is None:
            die("switch primary|secondary", 2)
        self.need("CONNECTION", "RUN", "SQL_USER", "SQL_PASSWORD")
        host, port = hp.rsplit(",", 1) if "," in hp else (hp, hp)
        url = f"/api/2.1/unity-catalog/connections/{self.v['CONNECTION']}"
        # keep the connection's other options (trustServerCertificate, ...); the API never returns the password
        opts = {k: v for k, v in ((self.api.get(url).get("options") or {}).items()) if "password" not in k.lower()}
        opts.update(host=host, port=port, user=self.v["SQL_USER"], password=self.v["SQL_PASSWORD"])
        self.api.patch(url, {"options": opts})
        self.log(f"connection {self.v['CONNECTION']} -> {node} ({host}:{port})")
        self.gw_stop()
        if not nostart:
            self.gw_start()

    def full_refresh(self, *tables: str) -> None:
        if self.kind == "pipeline":
            # a continuous pipeline refuses a refresh selection ("Refresh selection is not supported for the continuous mode")
            spec = self.api.get(f"/api/2.0/pipelines/{self.v['RUN']}").get("spec") or {}
            if spec.get("continuous"):
                die(f"pipeline {self.v['RUN']} is continuous: set continuous=false for the refresh, or refresh the whole pipeline", 2)
            # the reader is this pipeline; a running update must stop before the refresh update starts
            if self.gw_state() not in DONE_STATES:
                self.gw_stop()
        upd = self.api.post(f"/api/2.0/pipelines/{self.v['PIPELINE_ID']}/updates",
                            {"full_refresh_selection": list(tables)}).get("update_id")
        self.log(f"ingestion update {jqs(upd)}: full refresh of {' '.join(tables)}")

    def log_delivery(self) -> None:
        self.need("LOG_VOLUME")
        if self.kind != "gateway":
            die("integrated CDC runs on serverless, which takes no cluster settings: use wait-ci", 2)
        gw = self.v["GATEWAY_ID"]
        spec = dict(self.api.get(f"/api/2.0/pipelines/{gw}").get("spec") or {})
        clusters = spec.get("clusters")
        if clusters is None:
            clusters = [{"label": "default"}]
        dest = {"volumes": {"destination": self.v["LOG_VOLUME"]}}
        spec["clusters"] = [
            {**c, "cluster_log_conf": dest} if (c.get("label") if c.get("label") is not None else "default") == "default"
            else c for c in clusters]
        self.api.put(f"/api/2.0/pipelines/{gw}", spec)
        self.log(f"gateway {gw}: cluster logs -> {self.v['LOG_VOLUME']}/<cluster id>/driver (copied every few minutes)")

    def download_or_none(self, path: str) -> bytes | None:
        try:
            return self.api.download(path)
        except Exception:  # noqa: BLE001  any failure reading a log file: not there yet
            return None

    def driver_logs_name(self, volume_dir: str) -> list[str]:
        """Names in a folder (empty when it can't be listed)."""
        try:
            return sorted(e.get("name") or "" for e in self.api.ls(volume_dir))
        except Exception:  # noqa: BLE001  a folder that can't be listed is empty (not there yet)
            return []

    def wait_reset(self, since: float) -> None:
        """The driver log's "Reinitialization completed successfully" line at or after `since`, from the log files
        cluster log delivery copies to $LOG_VOLUME (read as they are; a gzipped file is read whole)."""
        self.need("LOG_VOLUME")
        if self.kind != "gateway":
            die("no driver log for integrated CDC (serverless): use wait-ci", 2)
        vol = self.v["LOG_VOLUME"]
        end = self.deadline()
        while self.clock() < end:
            c = self.api.get(f"/api/2.0/pipelines/{self.v['GATEWAY_ID']}").get("cluster_id") or ""
            if not c:
                names = self.driver_logs_name(vol)
                c = names[-1] if names else ""
            for name in self.driver_logs_name(f"{vol}/{c}/driver"):
                if not LOG_FILE.fullmatch(name):
                    continue
                data = self.download_or_none(f"{vol}/{c}/driver/{name}")
                if data is None:   # not readable yet: the next file, or the next poll
                    continue
                text = (gzip.decompress(data) if name.endswith(".gz") else data).decode("utf-8", "replace")
                for line in text.splitlines():
                    if RESET_LINE in line and (log_epoch(line) or 0) >= since:
                        self.log(f"driver log ({c}/driver/{name}): {line[:160]}")
                        return
            self.sleep(30)
        self.log(f"gave up after {self.wait} s")
        raise SystemExit(1)

    def wait_event(self, regex: str, since: str, quiet: bool = False) -> bool:
        """A pipeline event whose message matches regex, after `since` (ISO UTC). False when the wait gives up."""
        end = self.deadline()
        while self.clock() < end:
            evs = self.api.get(f"/api/2.0/pipelines/{self.v['PIPELINE_ID']}/events", max_results=100).get("events") or []
            m = next((e for e in evs if (e.get("timestamp") or "") > since
                      and re.search(regex, e.get("message") or "")), None)
            if m:
                if not quiet:
                    self.log(f"event: {(m['timestamp'][0:19] + ' ' + m['message'])[:160]}")
                return True
            self.sleep(20)
        return self.gave_up(quiet)

    def applied(self, table: str, dst: str, since: str) -> None:
        """Integrated CDC lands the snapshot in staging first and resets the destination table in an update that starts
        after the snapshot finished: wait for the snapshot, then for the reset (restarting once if it isn't there)."""
        sch, tbl = split_name(table)
        self.must(self.wait_event(f"_{sch}_{tbl}_snapshot_flow. has COMPLETED", since))
        reset = rf"STREAMING_TABLE .*\.{dst} has been successfully reset"
        saved, self.wait = self.wait, 25
        try:
            done = self.wait_event(reset, since, quiet=True)
        finally:
            self.wait = saved
        if done:
            return
        self.log(f"restarting so the next update applies the refresh of {dst}")
        self.gw_stop()
        self.gw_start()
        self.must(self.wait_event(reset, since))


def arg(args: list[str], i: int, what: str) -> str:
    """The i-th argument, or the usage error ${n:?what} gives (exit 1)."""
    if len(args) <= i or not args[i]:
        die(what, 1)
    return args[i]


def run(argv: list[str]) -> int:
    cmd, args = (argv[0] if argv else ""), argv[1:]
    r = Reader()
    if cmd == "log-delivery":
        r.need("RUN")
        r.log_delivery()
    elif cmd == "where":
        r.need("CONNECTION", "RUN", "DATABASE")
        conn = r.api.get(f"/api/2.1/unity-catalog/connections/{r.v['CONNECTION']}")
        opts = conn.get("options") or {}
        print(f"connection {jqs(conn.get('name'))}: {jqs(opts.get('host'))}:{jqs(opts.get('port'))}")
        print(f"{r.kind}: {r.gw_state()}")
        r.sessions()
    elif cmd == "switch":
        r.need("CONNECTION", "RUN", "PRIMARY", "SECONDARY")
        r.switch(args[0] if args else "")
    elif cmd == "stop":
        r.need("RUN")
        r.gw_stop()
    elif cmd == "start":
        r.need("RUN")
        r.gw_start()
    elif cmd == "full-refresh":
        r.need("PIPELINE_ID")
        if not args:
            die("full-refresh <table>...", 2)
        r.full_refresh(*args)
    elif cmd == "ci":
        r.need("DATABASE", "PRIMARY")
        print(r.ci(arg(args, 0, "schema.table"), args[1] if len(args) > 1 else "primary"))
    elif cmd == "wait-ci":
        r.need("DATABASE", "PRIMARY")
        r.wait_ci(arg(args, 0, "schema.table"), args[1] if len(args) > 1 else "")
    elif cmd == "wait-reset":
        r.need("RUN")
        r.wait_reset(float(arg(args, 0, "since epoch")))
    elif cmd == "caught-up":
        r.need("DATABASE", "SECONDARY")
        r.caught_up()
    elif cmd == "ddl":
        r.need("CONNECTION", "RUN", "DATABASE", "PRIMARY", "SECONDARY")
        t, f = arg(args, 0, "schema.table"), arg(args, 1, "ddl file")
        if not os.path.isfile(f):
            die(f"no file {f}", 2)
        r.switch("primary")
        before = field2(r.ci(t, "primary"))
        r.log(f"running {f} on the primary")
        with open(f) as fh:
            r.sqlrun("primary", fh.read())
        r.wait_ci(t, before)
        r.caught_up()
        r.switch("secondary")
    elif cmd == "recover":
        r.need("CONNECTION", "RUN", "DATABASE", "PRIMARY", "SECONDARY")
        t = arg(args, 0, "schema.table")
        r.switch("primary")
        r.wait_ci(t)
        r.caught_up()
        r.switch("secondary")
    elif cmd == "refresh":
        r.need("CONNECTION", "RUN", "PIPELINE_ID", "DATABASE", "PRIMARY", "SECONDARY")
        t, dst = arg(args, 0, "schema.table"), arg(args, 1, "destination table")
        # integrated: the refresh update is the restart on the primary
        r.switch("primary", nostart=r.kind == "pipeline")
        before = field2(r.ci(t, "primary"))
        since = int(time.time())
        r.full_refresh(dst)
        r.wait_ci(t, before)
        if r.v["LOG_VOLUME"] and r.kind == "gateway":
            r.wait_reset(since)
        r.gw_stop()
        r.caught_up()
        r.switch("secondary")
        # integrated: snapshot on the secondary, then one more update resets the destination (SCD2 history is replaced)
        if r.kind == "pipeline":
            r.applied(t, dst, utc_iso(since))
    elif cmd:   # an unknown command: the usage, exit 2
        print(__doc__)
        return 2
    else:
        print(__doc__)
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    try:
        return run(argv)
    except SystemExit as e:
        return e.code if isinstance(e.code, int) else (1 if e.code else 0)
    except SqlFailed as e:
        return e.rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
