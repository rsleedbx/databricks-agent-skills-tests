# Databricks notebook source
# MAGIC %md
# MAGIC # Debezium Server -> Zerobus, run from a notebook
# MAGIC
# MAGIC Runs Debezium Server (the standalone Debezium distribution) as a child process of this notebook. Its Zerobus sink
# MAGIC writes every change, as Debezium's envelope, into one append-only bronze table, `cdc_log`. Nothing here writes
# MAGIC through Spark: the notebook only starts the process, watches it and stops it.
# MAGIC
# MAGIC ### Requirements
# MAGIC - **Serverless or classic compute.** Debezium Server needs Java 21: the notebook downloads a Java 21 runtime
# MAGIC   (Eclipse Temurin) and the Debezium Server package once into the Volume, then runs from local disk.
# MAGIC - Outbound access to the database, to `repo1.maven.org` and `api.adoptium.net` (the first run only), and to the
# MAGIC   workspace's Zerobus endpoint.
# MAGIC - The bronze table and a service principal with `USE CATALOG`, `USE SCHEMA` and `SELECT, MODIFY` on it; its OAuth
# MAGIC   secret and the database password in a secret scope.
# MAGIC - PostgreSQL: `wal_level = logical`, a user with `REPLICATION`, a publication for the tables.
# MAGIC
# MAGIC ### Stop and start
# MAGIC The offsets file is on the Volume, so a new run resumes where the last one stopped. The job loop stops Debezium
# MAGIC (SIGTERM, so it flushes its offsets) on a STOP file in the Volume, after `idle_timeout_sec` with no new rows in
# MAGIC `cdc_log`, after `max_runtime_sec`, or when the job run is cancelled.

# COMMAND ----------

dbutils.widgets.text("catalog", "")   # empty: the workspace default catalog
dbutils.widgets.text("schema", "cdc")
dbutils.widgets.text("volume", "debezium")
dbutils.widgets.text("log_table", "cdc_log")
dbutils.widgets.text("db_host", "<db-host>")
dbutils.widgets.text("db_port", "5432")
dbutils.widgets.text("db_name", "postgres")
dbutils.widgets.text("db_user", "<db-user>")
dbutils.widgets.text("secret_scope", "<scope>")
dbutils.widgets.text("secret_key", "<db-password-key>")
dbutils.widgets.text("zb_client_id", "<service principal application id>")
dbutils.widgets.text("zb_secret_key", "<oauth-secret-key>")
dbutils.widgets.text("zb_endpoint", "")   # empty: https://<workspace id>.zerobus.<region>.<cloud domain>
dbutils.widgets.text("table_include_list", "public.customers")
dbutils.widgets.text("slot_name", "debezium_server_slot")
dbutils.widgets.text("publication_name", "dbz_publication")
dbutils.widgets.text("topic_prefix", "dbserver1")
dbutils.widgets.text("heartbeat_ms", "10000")   # keeps the slot's confirmed position moving while the source is quiet
dbutils.widgets.text("debezium_version", "3.7.0.Final")
dbutils.widgets.text("java_major", "21")
dbutils.widgets.text("idle_timeout_sec", "")
dbutils.widgets.text("max_runtime_sec", "3600")

W = {k: dbutils.widgets.get(k) for k in [
    "catalog", "schema", "volume", "log_table", "db_host", "db_port", "db_name", "db_user", "secret_scope", "secret_key",
    "zb_client_id", "zb_secret_key", "zb_endpoint", "table_include_list", "slot_name", "publication_name",
    "topic_prefix", "heartbeat_ms", "debezium_version", "java_major", "idle_timeout_sec", "max_runtime_sec"]}
if not W["catalog"]:
    W["catalog"] = spark.sql("SELECT current_catalog()").first()[0]


def q(*parts):
    """Backtick-quoted name: catalog names may hold hyphens."""
    return ".".join("`" + p.replace("`", "``") + "`" for p in parts)


LOG_TABLE = q(W["catalog"], W["schema"], W["log_table"])
VOL = f"/Volumes/{W['catalog']}/{W['schema']}/{W['volume']}"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {q(W['catalog'], W['schema'], W['volume'])}")

# COMMAND ----------

# The workspace's Zerobus endpoint and URL, from the metastore and the notebook's context
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
HOST = w.config.host.rstrip("/")
if not W["zb_endpoint"]:
    ws_id = w.api_client.do("GET", "/api/2.1/unity-catalog/current-metastore-assignment")["workspace_id"]
    ms = w.api_client.do("GET", "/api/2.1/unity-catalog/metastore_summary")
    domain = {"azure": "azuredatabricks.net", "aws": "cloud.databricks.com", "gcp": "gcp.databricks.com"}[ms["cloud"]]
    W["zb_endpoint"] = f"https://{ws_id}.zerobus.{ms['region']}.{domain}"
print(f"Zerobus: {W['zb_endpoint']}\nTable:   {LOG_TABLE}\nVolume:  {VOL}")

# COMMAND ----------

# Java 21 and Debezium Server: downloaded once into the Volume, unpacked on local disk
import os
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request

V, J = W["debezium_version"], W["java_major"]
CACHE = f"{VOL}/cache"
os.makedirs(CACHE, exist_ok=True)
SOURCES = {
    f"debezium-server-dist-{V}.tar.gz":
        f"https://repo1.maven.org/maven2/io/debezium/debezium-server-dist/{V}/debezium-server-dist-{V}.tar.gz",
    f"temurin-jre-{J}-linux-x64.tar.gz":
        f"https://api.adoptium.net/v3/binary/latest/{J}/ga/linux/x64/jre/hotspot/normal/eclipse",
}
for name, url in SOURCES.items():
    if not os.path.exists(f"{CACHE}/{name}"):
        print(f"downloading {url}")
        tmp = os.path.join(tempfile.gettempdir(), name)
        req = urllib.request.Request(url, headers={"User-Agent": "debezium-notebook/1.0"})  # urllib's default is refused
        with urllib.request.urlopen(req) as r, open(tmp, "wb") as f:
            shutil.copyfileobj(r, f)
        shutil.copy(tmp, f"{CACHE}/{name}")

RUN = tempfile.mkdtemp(prefix="debezium-server-")   # a new folder per run: the compute can be reused
for name in SOURCES:
    with tarfile.open(f"{CACHE}/{name}") as t:
        t.extractall(RUN)
subprocess.run(["chmod", "-R", "u+rwX", RUN], check=True)   # the archives' own modes can leave folders unreadable
DBZ_HOME = os.path.join(RUN, "debezium-server")
JAVA_HOME = next(os.path.join(RUN, d) for d in os.listdir(RUN) if d.startswith("jdk"))
print("java:", JAVA_HOME)
zb_jars = [j for j in os.listdir(f"{DBZ_HOME}/lib") if "zerobus" in j]
print("zerobus sink jars:", zb_jars)
assert zb_jars, f"Debezium Server {V} has no Zerobus sink in lib/ (it ships from 3.7)"
import zipfile


def class_java(jar: str) -> int:
    """The Java version a jar's classes need (class-file major version - 44)."""
    with zipfile.ZipFile(jar) as z:
        c = next(n for n in z.namelist() if n.endswith(".class") and "module-info" not in n)
        return int.from_bytes(z.read(c)[6:8], "big") - 44


for j in sorted(os.listdir(f"{DBZ_HOME}/lib")):
    if j.startswith(("debezium-server-core", "debezium-embedded", "debezium-connector-postgres")):
        print(f"{j}: needs Java {class_java(f'{DBZ_HOME}/lib/{j}')}")

# COMMAND ----------

# application.properties (mode 0600, local disk only: it holds the two secrets)
OFFSETS = f"{VOL}/offsets/debezium_server.dat"
os.makedirs(os.path.dirname(OFFSETS), exist_ok=True)
props = {
    "debezium.source.connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "debezium.source.database.hostname": W["db_host"],
    "debezium.source.database.port": W["db_port"],
    "debezium.source.database.user": W["db_user"],
    "debezium.source.database.password": dbutils.secrets.get(W["secret_scope"], W["secret_key"]),
    "debezium.source.database.dbname": W["db_name"],
    "debezium.source.topic.prefix": W["topic_prefix"],
    "debezium.source.table.include.list": W["table_include_list"],
    "debezium.source.plugin.name": "pgoutput",
    "debezium.source.slot.name": W["slot_name"],
    "debezium.source.publication.name": W["publication_name"],
    "debezium.source.publication.autocreate.mode": "disabled",
    "debezium.source.decimal.handling.mode": "string",
    "debezium.source.snapshot.mode": "no_data",
    "debezium.source.heartbeat.interval.ms": W["heartbeat_ms"],
    "debezium.source.tombstones.on.delete": "false",
    "debezium.source.offset.storage": "org.apache.kafka.connect.storage.FileOffsetBackingStore",
    "debezium.source.offset.storage.file.filename": OFFSETS,
    "debezium.source.offset.flush.interval.ms": "1000",
    "debezium.format.key": "json",
    "debezium.format.value": "json",
    "debezium.format.key.schemas.enable": "false",
    "debezium.format.value.schemas.enable": "false",
    # Heartbeats land in the change log as destination __debezium-heartbeat.<prefix>; silver reads by destination.
    # Dropping them before the sink (Kafka's Filter transform) also stops them moving the slot: tested, kept.
    "debezium.sink.type": "zerobus",
    "debezium.sink.zerobus.endpoint": W["zb_endpoint"],
    "debezium.sink.zerobus.workspace.url": HOST,
    "debezium.sink.zerobus.client.id": W["zb_client_id"],
    "debezium.sink.zerobus.client.secret": dbutils.secrets.get(W["secret_scope"], W["zb_secret_key"]),
    "debezium.sink.zerobus.payload.mode": "envelope",
    "debezium.sink.zerobus.table": f"{W['catalog']}.{W['schema']}.{W['log_table']}",
    "debezium.sink.zerobus.record.format": "json",
    "debezium.sink.zerobus.idempotency.mode": "source",
    "debezium.sink.zerobus.tombstone.handling.mode": "drop",
    "debezium.sink.zerobus.json.flexible.fields.encoding": "string",
    "quarkus.log.console.json": "false",
}
os.makedirs(f"{DBZ_HOME}/config", exist_ok=True)
conf = f"{DBZ_HOME}/config/application.properties"
fd = os.open(conf, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "w") as f:
    f.write("".join(f"{k}={str(v).replace(chr(92), chr(92) * 2)}\n" for k, v in props.items()))
del props

# COMMAND ----------

# Run Debezium Server until a stop trigger: STOP file, idle, max runtime, job cancel (SIGTERM), or the process ending
import signal
import subprocess
import time

STOP_FILE = f"{VOL}/STOP"
LOG = os.path.join(RUN, "debezium.log")
IDLE = int(W["idle_timeout_sec"]) if W["idle_timeout_sec"] else None
MAX_RUN = int(W["max_runtime_sec"]) if W["max_runtime_sec"] else None
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
    # change rows only: heartbeats mustn't keep the idle timer from running out
    return spark.sql(f"SELECT count(*) FROM {LOG_TABLE} WHERE destination NOT LIKE '\\_\\_debezium-heartbeat%'").first()[0]


# the notebook's own Java options are for the runtime's Java 8 (PrintGCDateStamps): Java 21 refuses them
JAVA_OPT_VARS = ("JAVA_OPTS", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS")
print("dropped:", {k: os.environ[k][:120] for k in JAVA_OPT_VARS if k in os.environ})
env = {k: v for k, v in os.environ.items() if k not in JAVA_OPT_VARS}
env.update(JAVA_HOME=JAVA_HOME, PATH=f"{JAVA_HOME}/bin:{os.environ['PATH']}")
log = open(LOG, "w")  # noqa: SIM115 -- the child writes to it; closed in finally
proc = subprocess.Popen(["bash", "run.sh"], cwd=DBZ_HOME, env=env, stdout=log, stderr=subprocess.STDOUT)
print(f"Debezium Server started, pid {proc.pid}, log {LOG}")
start = last_change = time.time()
last_rows = rows()
start_rows = last_rows
why = ""
try:
    while not stop_requested:
        time.sleep(10)
        now = time.time()
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
        if IDLE and now - last_change >= IDLE:
            why = f"no new rows for {IDLE}s"
            break
        if MAX_RUN and now - start >= MAX_RUN:
            why = f"max runtime {MAX_RUN}s"
            break
    else:
        why = "SIGTERM (job cancelled)"
finally:
    if proc.poll() is None:
        proc.terminate()   # Debezium flushes its offsets on SIGTERM
        try:
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
    log.close()
    os.remove(conf)

print(f"Stopped: {why}. Rows this run: {rows() - start_rows}. Offsets saved: {os.path.exists(OFFSETS)}")
with open(LOG) as f:
    tail = f.read().splitlines()
print("\n".join(l for l in tail if " ERROR " in l or "Exception" in l)[-4000:] or "no ERROR lines")
if why.startswith("Debezium Server exited"):
    print("\n".join(tail[-60:]))
    raise RuntimeError(why)

# COMMAND ----------

# VARIANT columns as text: display() on serverless can't show VARIANT
display(spark.sql(f"""SELECT destination, operation, key::STRING AS key, value:before::STRING AS before,
  value:after::STRING AS after, value:source:lsn::BIGINT AS lsn
FROM {LOG_TABLE} ORDER BY lsn DESC LIMIT 20"""))
