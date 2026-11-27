# Databricks notebook source
# MAGIC %md
# MAGIC # Debezium Server -> Zerobus, run from a notebook
# MAGIC
# MAGIC Runs Debezium Server (the standalone Debezium distribution) as a child process of this notebook. Its Zerobus sink
# MAGIC writes every change, as Debezium's envelope, into one append-only bronze table, `cdc_log`. Nothing here writes
# MAGIC through Spark: the notebook only starts the process, watches it and stops it.
# MAGIC
# MAGIC `engine` is `postgres`, `sqlserver`, `mysql`, `oracle` or `db2`; the connector settings for each come from the
# MAGIC skill's `zerobus_debezium.engines` (installed from `package`). Prepare the source for capture first (the same
# MAGIC module's `setup` steps: CDC, a publication, ARCHIVELOG, ASN capture).
# MAGIC
# MAGIC ### Requirements
# MAGIC - **Serverless or classic compute.** Debezium Server needs Java 21: the notebook downloads a Java 21 runtime
# MAGIC   (Eclipse Temurin) and the Debezium Server package once into the Volume, then runs from local disk.
# MAGIC - Outbound access to the database, to `repo1.maven.org`, `api.adoptium.net` and GitHub (the first run), and to
# MAGIC   the workspace's Zerobus endpoint.
# MAGIC - The bronze table and a service principal with `USE CATALOG`, `USE SCHEMA` and `SELECT, MODIFY` on it; its OAuth
# MAGIC   secret and the database password in a secret scope.
# MAGIC
# MAGIC ### Stop and start
# MAGIC Each producer (`name`) has its own folder in the Volume: its offsets, schema history, STOP file and `status.json`.
# MAGIC A new run with the same name resumes where the last one stopped. The job loop stops Debezium (SIGTERM, so it
# MAGIC flushes its offsets) on the STOP file, after `idle_timeout_sec` with no new rows from it, after
# MAGIC `max_runtime_sec`, or when the job run is cancelled.

# COMMAND ----------

dbutils.widgets.text("name", "dbz")   # the producer: its folder in the Volume, its topic prefix (- as _)
dbutils.widgets.dropdown("engine", "postgres", ["postgres", "sqlserver", "mysql", "oracle", "db2"])
dbutils.widgets.text("package", "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip"
                                "#subdirectory=skills/zerobus-debezium-test/scripts")
dbutils.widgets.text("catalog", "")   # empty: the workspace default catalog
dbutils.widgets.text("schema", "cdc")
dbutils.widgets.text("volume", "debezium")
dbutils.widgets.text("log_table", "cdc_log")
dbutils.widgets.text("db_host", "<db-host>")
dbutils.widgets.text("db_port", "5432")
dbutils.widgets.text("db_name", "")   # the database (PostgreSQL, SQL Server, MySQL, Db2) or the PDB (Oracle)
dbutils.widgets.text("db_user", "<db-user>")
dbutils.widgets.text("secret_scope", "<scope>")
dbutils.widgets.text("secret_key", "<db-password-key>")
dbutils.widgets.text("zb_client_id", "<service principal application id>")
dbutils.widgets.text("zb_secret_key", "<oauth-secret-key>")
dbutils.widgets.text("zb_endpoint", "")   # empty: https://<workspace id>.zerobus.<region>.<cloud domain>
dbutils.widgets.text("slot_name", "dbz_slot")              # PostgreSQL
dbutils.widgets.text("publication_name", "dbz_publication")  # PostgreSQL
dbutils.widgets.text("heartbeat_ms", "10000")   # keeps the source's confirmed position moving while it's quiet
dbutils.widgets.text("debezium_version", "3.7.0.Final")
dbutils.widgets.text("java_major", "21")
dbutils.widgets.text("idle_timeout_sec", "")
dbutils.widgets.text("max_runtime_sec", "3600")
dbutils.widgets.text("start_timeout_sec", "600")   # not streaming by then: stop, failed, with Debezium's errors

W = {k: dbutils.widgets.get(k) for k in [
    "name", "engine", "package", "catalog", "schema", "volume", "log_table", "db_host", "db_port", "db_name",
    "db_user", "secret_scope", "secret_key", "zb_client_id", "zb_secret_key", "zb_endpoint", "slot_name",
    "publication_name", "heartbeat_ms", "debezium_version", "java_major", "idle_timeout_sec", "max_runtime_sec",
    "start_timeout_sec"]}
if not W["catalog"]:
    W["catalog"] = spark.sql("SELECT current_catalog()").first()[0]

# COMMAND ----------

# The skill's package (engine settings, the Zerobus endpoint): pip from a Python cell, as the source is a widget
import subprocess
import sys

subprocess.run([sys.executable, "-m", "pip", "install", "-q", "--disable-pip-version-check", W["package"]], check=True)
from zerobus_debezium import destination, engines  # noqa: E402

ENGINE = engines.get(W["engine"])


def q(*parts):
    """Backtick-quoted name: catalog names may hold hyphens."""
    return ".".join("`" + p.replace("`", "``") + "`" for p in parts)


LOG_TABLE = q(W["catalog"], W["schema"], W["log_table"])
VOL = f"/Volumes/{W['catalog']}/{W['schema']}/{W['volume']}"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {q(W['catalog'], W['schema'], W['volume'])}")
FOLDER = f"{VOL}/{W['name']}"   # this producer's offsets, schema history, STOP file and status

# COMMAND ----------

# The workspace's URL and Zerobus endpoint, through the notebook's own login
import json
import os

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
HOST = w.config.host.rstrip("/")


class _Api:   # the one call destination.endpoint() makes, on the notebook's client
    def get(self, path):
        return w.api_client.do("GET", path)


W["zb_endpoint"] = W["zb_endpoint"] or destination.endpoint(_Api())
os.makedirs(FOLDER, exist_ok=True)


def status(state: str, **kw):
    """FOLDER/status.json: what a caller (lfc_change_source's WorkspaceDebeziumServer) waits on."""
    with open(f"{FOLDER}/status.json", "w") as f:
        json.dump({"state": state, **kw}, f)


status("starting")
print(f"Engine:  {W['engine']}\nZerobus: {W['zb_endpoint']}\nTable:   {LOG_TABLE}\nFolder:  {FOLDER}")

# COMMAND ----------

# Java 21 and Debezium Server: downloaded once into the Volume, unpacked on local disk
import shutil
import tarfile
import tempfile
import urllib.request
import zipfile

V, J = W["debezium_version"], W["java_major"]
CACHE = f"{VOL}/cache"
os.makedirs(CACHE, exist_ok=True)
SOURCES = {
    f"debezium-server-dist-{V}.tar.gz":
        f"https://repo1.maven.org/maven2/io/debezium/debezium-server-dist/{V}/debezium-server-dist-{V}.tar.gz",
    f"temurin-jre-{J}-linux-x64.tar.gz":
        f"https://api.adoptium.net/v3/binary/latest/{J}/ga/linux/x64/jre/hotspot/normal/eclipse",
}
for fname, url in SOURCES.items():
    if not os.path.exists(f"{CACHE}/{fname}"):
        print(f"downloading {url}")
        tmp = os.path.join(tempfile.gettempdir(), fname)
        req = urllib.request.Request(url, headers={"User-Agent": "debezium-notebook/1.0"})  # urllib's default is refused
        with urllib.request.urlopen(req) as r, open(tmp, "wb") as f:
            shutil.copyfileobj(r, f)
        part = f"{CACHE}/{fname}.{os.getpid()}.part"   # producers started together don't read each other's half copy
        shutil.copy(tmp, part)
        os.replace(part, f"{CACHE}/{fname}")

RUN = tempfile.mkdtemp(prefix="debezium-server-")   # a new folder per run: the compute can be reused
for fname in SOURCES:
    with tarfile.open(f"{CACHE}/{fname}") as t:
        t.extractall(RUN)
subprocess.run(["chmod", "-R", "u+rwX", RUN], check=True)   # the archives' own modes can leave folders unreadable
DBZ_HOME = os.path.join(RUN, "debezium-server")
JAVA_HOME = next(os.path.join(RUN, d) for d in os.listdir(RUN) if d.startswith("jdk"))
zb_jars = [j for j in os.listdir(f"{DBZ_HOME}/lib") if "zerobus" in j]
assert zb_jars, f"Debezium Server {V} has no Zerobus sink in lib/ (it ships from 3.7)"


def class_java(jar: str) -> int:
    """The Java version a jar's classes need (class-file major version - 44)."""
    with zipfile.ZipFile(jar) as z:
        c = next(n for n in z.namelist() if n.endswith(".class") and "module-info" not in n)
        return int.from_bytes(z.read(c)[6:8], "big") - 44


print("java:", JAVA_HOME, "| zerobus sink:", zb_jars)
for j in sorted(os.listdir(f"{DBZ_HOME}/lib")):
    if j.startswith(("debezium-server-core", f"debezium-connector-{ENGINE.connector.split('.')[3]}")):
        print(f"{j}: needs Java {class_java(f'{DBZ_HOME}/lib/{j}')}")

# COMMAND ----------

# application.properties (mode 0600, local disk only: it holds the two secrets)
conn = {"host": W["db_host"], "port": W["db_port"], "user": W["db_user"],
        "password": dbutils.secrets.get(W["secret_scope"], W["secret_key"]),
        "slot": W["slot_name"], "publication": W["publication_name"]}
if W["db_name"]:
    conn["service" if W["engine"] == "oracle" else "database"] = W["db_name"]
# the schema history on local disk (Debezium appends to it, which a Volume refuses: "Illegal seek"), restored from the
# Volume's copy and copied back whenever it changes (sync_history)
HIST, HIST_VOL = f"{RUN}/schema-history.dat", f"{FOLDER}/schema-history.dat"
if os.path.exists(HIST_VOL):
    shutil.copy(HIST_VOL, HIST)
props = {**ENGINE.source_properties(conn, FOLDER, W["name"], int(W["heartbeat_ms"]), history_folder=RUN),
         **engines.sink_properties(W["zb_endpoint"], HOST, W["zb_client_id"],
                                   dbutils.secrets.get(W["secret_scope"], W["zb_secret_key"]),
                                   f"{W['catalog']}.{W['schema']}.{W['log_table']}")}
os.makedirs(f"{DBZ_HOME}/config", exist_ok=True)
conf = f"{DBZ_HOME}/config/application.properties"
fd = os.open(conf, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "w") as f:
    f.write(engines.render(props))
PREFIX = props["debezium.source.topic.prefix"]
del props, conn

# COMMAND ----------

# Run Debezium Server until a stop trigger: STOP file, idle, max runtime, job cancel (SIGTERM), or the process ending
import signal
import time

STOP_FILE = f"{FOLDER}/STOP"
LOG = os.path.join(RUN, "debezium.log")
IDLE = int(W["idle_timeout_sec"]) if W["idle_timeout_sec"] else None
MAX_RUN = int(W["max_runtime_sec"]) if W["max_runtime_sec"] else None
START_S = int(W["start_timeout_sec"] or 600)
stop_requested = False


def _on_sigterm(signum, frame):
    global stop_requested
    stop_requested = True


try:
    signal.signal(signal.SIGTERM, _on_sigterm)
except ValueError as e:
    print(f"SIGTERM handler not set: {e}")
if os.path.exists(STOP_FILE):
    os.remove(STOP_FILE)


def rows() -> int:
    """This producer's change rows (its topic prefix); heartbeats don't count, so idle can run out."""
    return spark.sql(f"SELECT count(*) FROM {LOG_TABLE} WHERE startswith(destination, '{PREFIX}.')").first()[0]


def errors(n: int = 5) -> list:
    """Debezium's last ERROR lines, cut to 500 characters."""
    with open(LOG, errors="replace") as f:
        return [ln[:500] for ln in f.read().splitlines() if " ERROR " in ln][-n:]


_hist_seen = None


def sync_history():
    """Copy the local schema history to the Volume when it has changed (one whole-file write)."""
    global _hist_seen
    if os.path.exists(HIST):
        st = os.stat(HIST)
        if (st.st_size, st.st_mtime) != _hist_seen:
            shutil.copy(HIST, HIST_VOL)
            _hist_seen = (st.st_size, st.st_mtime)


def log_state() -> str:
    """READY or FAILED, whichever Debezium logged last; "" before either."""
    with open(LOG, errors="replace") as f:
        text = f.read()
    hits = [(text.rfind(m), m) for m in (engines.READY, engines.FAILED)]
    pos, m = max(hits)
    return m if pos >= 0 else ""


# the runtime's own Java options are for its Java 8 (PrintGCDateStamps): Java 21 refuses them
JAVA_OPT_VARS = ("JAVA_OPTS", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS")
env = {k: v for k, v in os.environ.items() if k not in JAVA_OPT_VARS}
env.update(JAVA_HOME=JAVA_HOME, PATH=f"{JAVA_HOME}/bin:{os.environ['PATH']}")
log = open(LOG, "w")  # noqa: SIM115 -- the child writes to it; closed in finally
proc = None
why, streaming = "", False
try:   # from here on, finally stops Debezium and removes the settings file (it holds the two secrets)
    last_rows = start_rows = rows()   # before the start: a failing query leaves nothing running
    proc = subprocess.Popen(["bash", "run.sh"], cwd=DBZ_HOME, env=env, stdout=log, stderr=subprocess.STDOUT)
    print(f"Debezium Server started, pid {proc.pid}")
    start = last_change = time.time()
    while not stop_requested:
        time.sleep(10)
        now = time.time()
        if not streaming:
            s = log_state()
            if s == engines.READY:
                streaming = True
                status("streaming", since=int(now))
                last_change = now
            elif s == engines.FAILED:
                why = "Debezium's engine failed"
                break
            elif now - start >= START_S:
                why = f"Debezium not streaming after {START_S}s"
                break
            else:
                status("starting", errors=errors(3))
        sync_history()
        if proc.poll() is not None:
            why = f"Debezium Server exited with {proc.returncode}"
            break
        n = rows()
        if n != last_rows:
            last_rows, last_change = n, now
        if os.path.exists(STOP_FILE):
            os.remove(STOP_FILE)
            why = "STOP file"
            break
        if IDLE and streaming and now - last_change >= IDLE:
            why = f"no new rows for {IDLE}s"
            break
        if MAX_RUN and now - start >= MAX_RUN:
            why = f"max runtime {MAX_RUN}s"
            break
    else:
        why = "SIGTERM (job cancelled)"
finally:
    if proc is not None and proc.poll() is None:
        proc.terminate()   # Debezium flushes its offsets on SIGTERM
        try:
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
    log.close()
    os.remove(conf)

with open(LOG, errors="replace") as f:
    tail = f.read().splitlines()
sync_history()
failed = why.startswith(("Debezium Server exited", "Debezium's engine", "Debezium not streaming"))
errors = [ln for ln in tail if " ERROR " in ln or "Exception" in ln][-40:]
status("failed" if failed else "stopped", why=why, rows=rows() - start_rows, errors=[e[:500] for e in errors[-5:]])
print(f"Stopped: {why}. Rows this run: {rows() - start_rows}. Offsets saved: {os.path.exists(f'{FOLDER}/offsets.dat')}")
print("\n".join(errors)[-4000:] or "no ERROR lines")
if failed:
    print("\n".join(tail[-80:]))
    raise RuntimeError(why)

# COMMAND ----------

# VARIANT columns as text: display() on serverless can't show VARIANT
display(spark.sql(f"""SELECT destination, operation, key::STRING AS key, value:before::STRING AS before,
  value:after::STRING AS after FROM {LOG_TABLE} WHERE startswith(destination, '{PREFIX}.')
ORDER BY value:ts_ms::BIGINT DESC LIMIT 20"""))
