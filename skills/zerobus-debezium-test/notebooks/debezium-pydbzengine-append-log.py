# Databricks notebook source
# MAGIC %md
# MAGIC # Debezium CDC Append-Only Pipeline (`pydbzengine`)
# MAGIC
# MAGIC Streams live Change Data Capture (CDC) events (skipping the initial full-table snapshot via `snapshot.mode = no_data`) into an append-only Unity Catalog Delta table.
# MAGIC
# MAGIC ### Requirements
# MAGIC - **Serverless, or classic compute in dedicated (single user) access mode on Databricks Runtime 16.4 LTS or later.** Debezium 3.x needs Java 17: the notebook picks the Java 17 `libjvm.so` under `/usr/lib/jvm` (`JAVA_HOME` there can point at Java 8). The engine runs in a JVM inside the Python process.
# MAGIC - The cluster reaches the database's host and port.
# MAGIC - PostgreSQL: `wal_level = logical`, a user with `REPLICATION` and `SELECT` on the tables, and a publication for the tables (`CREATE PUBLICATION dbz_publication FOR TABLE public.customers`), or let Debezium create it if the user owns the tables.
# MAGIC - The database password in a secret scope (`secret_scope` / `secret_key` widgets).
# MAGIC
# MAGIC ### State & Stop/Start Design
# MAGIC - **Persistent Offset File:** Stored on a Unity Catalog Volume (`/Volumes/<catalog>/<schema>/<volume>/dbz_cdc_only.dat`) so replication offsets survive cluster restarts.
# MAGIC - **Interactive Mode:** Use `cdc.start(blocking=False)` and `cdc.stop()`.
# MAGIC - **Databricks Job Mode:** the job-runner cell supports these graceful shutdown triggers that execute `cdc.stop()` and flush offsets before exiting:
# MAGIC   1. Creating a sentinel file at `/Volumes/<catalog>/<schema>/<volume>/STOP`
# MAGIC   2. Idle drain timeout (`idle_timeout_sec`)
# MAGIC   3. Max runtime (`max_runtime_sec`)
# MAGIC   4. `SIGTERM` signal when cancelling a job run in the Databricks UI

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 1: Install pydbzengine and restart the Python interpreter

# COMMAND ----------

# MAGIC %pip install pydbzengine

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Cell 2: Parameters (widgets; a job passes them as base parameters)
dbutils.widgets.text("catalog", "")   # empty: the workspace default catalog
dbutils.widgets.text("schema", "cdc")
dbutils.widgets.text("volume", "offsets")
dbutils.widgets.text("log_table", "cdc_append_log")
dbutils.widgets.text("db_host", "<db-host>")
dbutils.widgets.text("db_port", "5432")
dbutils.widgets.text("db_name", "postgres")
dbutils.widgets.text("db_user", "<db-user>")
dbutils.widgets.text("secret_scope", "<scope>")
dbutils.widgets.text("secret_key", "<key>")
dbutils.widgets.text("table_include_list", "public.customers")
dbutils.widgets.text("slot_name", "databricks_cdc_slot")
dbutils.widgets.text("publication_name", "dbz_publication")
dbutils.widgets.text("topic_prefix", "dbserver1")
dbutils.widgets.text("idle_timeout_sec", "")   # e.g. 60: stop once no new events for 60 s; empty = off
dbutils.widgets.text("max_runtime_sec", "3600")  # safety cap; empty = unlimited
dbutils.widgets.dropdown("run_job_loop", "true", ["true", "false"])  # false: interactive, use cdc.start()/cdc.stop()

W = {k: dbutils.widgets.get(k) for k in [
    "catalog", "schema", "volume", "log_table", "db_host", "db_port", "db_name", "db_user", "secret_scope",
    "secret_key", "table_include_list", "slot_name", "publication_name", "topic_prefix", "idle_timeout_sec",
    "max_runtime_sec", "run_job_loop"]}

def q(*parts):
    """Backtick-quoted name: catalog names may hold hyphens."""
    return ".".join("`" + p.replace("`", "``") + "`" for p in parts)


if not W["catalog"]:
    W["catalog"] = spark.sql("SELECT current_catalog()").first()[0]

TARGET_LOG_TABLE = q(W["catalog"], W["schema"], W["log_table"])
OFFSET_DIR = f"/Volumes/{W['catalog']}/{W['schema']}/{W['volume']}"
print(f"Log table: {TARGET_LOG_TABLE}\nOffsets:   {OFFSET_DIR}")

# COMMAND ----------

# Cell 3: Create Unity Catalog Volume for persistent offsets and target append-only log table
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {q(W['catalog'], W['schema'])}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {q(W['catalog'], W['schema'], W['volume'])}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_LOG_TABLE} (
  ingested_at TIMESTAMP,
  destination STRING,
  op STRING,
  ts_ms BIGINT,
  record_key STRING,
  before_json STRING,
  after_json STRING,
  raw_value STRING
) USING DELTA
""")

# COMMAND ----------

# Cell 4: Configure connection, append-only Delta handler, and start/stop lifecycle controller
import json
import os
import threading
from datetime import datetime, timezone

from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType
from pydbzengine import BasePythonChangeHandler, ChangeEvent, DebeziumEngine

# --- 1. Configuration ---
OFFSET_FILE_PATH = f"{OFFSET_DIR}/dbz_cdc_only.dat"
SCHEMA_HISTORY_PATH = f"{OFFSET_DIR}/schema_history.dat"  # Required for MySQL/SQL Server/Oracle

os.makedirs(OFFSET_DIR, exist_ok=True)

DB_CONFIG = {
    "name": "databricks-cdc-only-engine",
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": W["db_host"],
    "database.port": W["db_port"],
    "database.user": W["db_user"],
    "database.password": dbutils.secrets.get(scope=W["secret_scope"], key=W["secret_key"]),
    "database.dbname": W["db_name"],
    "topic.prefix": W["topic_prefix"],
    "table.include.list": W["table_include_list"],
    "plugin.name": "pgoutput",
    "slot.name": W["slot_name"],
    "publication.name": W["publication_name"],
    # Use the publication as it is; "filtered" creates it for table.include.list (needs table ownership)
    "publication.autocreate.mode": "disabled",

    # CDC only: skip initial data snapshot on first run
    "snapshot.mode": "no_data",

    # Persistent offset storage on Unity Catalog Volume
    "offset.storage": "org.apache.kafka.connect.storage.FileOffsetBackingStore",
    "offset.storage.file.filename": OFFSET_FILE_PATH,
    "offset.flush.interval.ms": "5000",

    # NUMERIC/DECIMAL as text ("21.50"); the default (precise) gives base64 bytes
    "decimal.handling.mode": "string",

    # A delete is one event with op = 'd' and the before image; no extra null-value tombstone after it
    "tombstones.on.delete": "false",
}


def java17_libjvm() -> str | None:
    """Debezium 3.x needs Java 17+. Serverless and recent runtimes ship Zulu 17 under /usr/lib/jvm while JAVA_HOME may
    point at Java 8, which JPype would pick by default: use the newest 17+ libjvm.so found, else JPype's default."""
    import glob
    import re
    found = []
    for path in glob.glob("/usr/lib/jvm/*/lib/server/libjvm.so"):
        m = re.search(r"/(?:zulu|java-|jdk-?)(\d+)", path)
        if m and int(m.group(1)) >= 17:
            found.append((int(m.group(1)), path))
    return max(found)[1] if found else None


JVM_PATH = java17_libjvm()
print(f"JVM: {JVM_PATH or 'JPype default (JAVA_HOME)'}")


def _s(v):
    """JPype hands Java strings back; Spark and json need Python str."""
    return None if v is None else str(v)


# --- 2. Append-Only Delta Handler ---
LOG_SCHEMA = StructType([
    StructField("ingested_at", TimestampType(), False),
    StructField("destination", StringType(), True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True),
    StructField("record_key", StringType(), True),
    StructField("before_json", StringType(), True),
    StructField("after_json", StringType(), True),
    StructField("raw_value", StringType(), True),
])


class AppendOnlyCdcHandler(BasePythonChangeHandler):
    def __init__(self, target_table: str):
        self.target_table = target_table
        self.total_events = 0

    def handleJsonBatch(self, records: list[ChangeEvent]):
        if not records:
            return

        now = datetime.now(timezone.utc)
        rows = []

        for record in records:
            raw_val = _s(record.value())
            op, ts_ms, before_str, after_str = None, None, None, None

            if raw_val:
                parsed = json.loads(raw_val)
                payload = parsed.get("payload", parsed)
                op = payload.get("op")
                ts_ms = payload.get("ts_ms")
                before_str = json.dumps(payload.get("before")) if payload.get("before") is not None else None
                after_str = json.dumps(payload.get("after")) if payload.get("after") is not None else None

            rows.append((
                now,
                _s(record.destination()),
                op,
                ts_ms,
                _s(record.key()),
                before_str,
                after_str,
                raw_val,
            ))

        # Synchronous Delta append: offsets commit only after this completes without error
        df = spark.createDataFrame(rows, schema=LOG_SCHEMA)
        df.write.format("delta").mode("append").saveAsTable(self.target_table)
        self.total_events += len(rows)


# --- 3. Start / Stop Controller ---
class CdcController:
    def __init__(self, props: dict, target_table: str):
        self.props = props
        self.target_table = target_table
        self.engine = None
        self.handler = None
        self.thread = None
        self.error = None

    def _run(self):
        try:
            self.engine.run()
        except Exception as e:  # surfaced by status() and the job loop
            self.error = e
            print(f"CDC engine failed: {e!r}")

    def start(self, blocking: bool = False):
        if self.thread and self.thread.is_alive():
            print("CDC engine is already running.")
            return

        self.error = None
        self.handler = AppendOnlyCdcHandler(self.target_table)
        self.engine = DebeziumEngine(properties=self.props, handler=self.handler, jvm_path=JVM_PATH)

        if blocking:
            print("Starting CDC engine in foreground...")
            try:
                self.engine.run()
            except KeyboardInterrupt:
                print("Interrupt received; shutting down cleanly...")
            finally:
                self.stop()
        else:
            self.thread = threading.Thread(target=self._run, daemon=True)
            self.thread.start()
            print("CDC engine started in background thread.")

    def stop(self):
        if self.engine is None:
            print("CDC engine is not initialized.")
            return

        print("Stopping CDC engine and flushing final offsets to Volume...")
        try:
            self.engine.close()
        except Exception as e:
            print(f"Warning during engine close: {e}")

        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=30)

        count = self.handler.total_events if self.handler else 0
        print(f"CDC engine stopped cleanly. Events written this session: {count}")
        self.engine = None
        self.thread = None

    def status(self):
        running = bool(self.thread and self.thread.is_alive())
        count = self.handler.total_events if self.handler else 0
        offset_exists = os.path.exists(OFFSET_FILE_PATH)
        print(f"Running: {running} | Session events: {count} | Offset file saved: {offset_exists}"
              + (f" | Error: {self.error!r}" if self.error else ""))


cdc = CdcController(props=DB_CONFIG, target_table=TARGET_LOG_TABLE)

# COMMAND ----------

# Cell 5: Job Runner with Graceful Shutdown Triggers (STOP file, Idle Timeout, Max Runtime, SIGTERM)
import signal
import time

STOP_SIGNAL_FILE = f"{OFFSET_DIR}/STOP"
POLL_INTERVAL_SEC = 5
MAX_RUNTIME_SEC = int(W["max_runtime_sec"]) if W["max_runtime_sec"] else None
IDLE_TIMEOUT_SEC = int(W["idle_timeout_sec"]) if W["idle_timeout_sec"] else None

stop_requested = False


def _handle_sigterm(signum, frame):
    global stop_requested
    print("SIGTERM received from Databricks Job cancellation...")
    stop_requested = True


if W["run_job_loop"] == "true":
    try:
        signal.signal(signal.SIGTERM, _handle_sigterm)
    except ValueError as e:  # signal handlers can only be set from the main thread
        print(f"SIGTERM handler not set: {e}")

    # Clear any leftover STOP file from a previous run before starting
    if os.path.exists(STOP_SIGNAL_FILE):
        os.remove(STOP_SIGNAL_FILE)

    cdc.start(blocking=False)

    start_time = time.time()
    last_event_count = 0
    last_activity_time = start_time

    try:
        while not stop_requested:
            time.sleep(POLL_INTERVAL_SEC)
            now = time.time()

            current_count = cdc.handler.total_events if cdc.handler else 0
            if current_count > last_event_count:
                last_event_count = current_count
                last_activity_time = now

            # Trigger A: External STOP file placed on Unity Catalog Volume
            if os.path.exists(STOP_SIGNAL_FILE):
                print(f"Stop file detected at {STOP_SIGNAL_FILE}. Shutting down...")
                os.remove(STOP_SIGNAL_FILE)
                break

            # Trigger B: Idle timeout (all queued CDC changes drained)
            if IDLE_TIMEOUT_SEC and (now - last_activity_time) >= IDLE_TIMEOUT_SEC:
                print(f"No new events for {IDLE_TIMEOUT_SEC}s. Queue drained; shutting down...")
                break

            # Trigger C: Max runtime reached
            if MAX_RUNTIME_SEC and (now - start_time) >= MAX_RUNTIME_SEC:
                print(f"Max runtime of {MAX_RUNTIME_SEC}s reached. Shutting down...")
                break

            # Safety check: exit if background engine thread died unexpectedly
            if not (cdc.thread and cdc.thread.is_alive()):
                print("Warning: CDC background thread exited unexpectedly.")
                break
    finally:
        cdc.stop()

    if cdc.error:
        raise RuntimeError("CDC engine failed; see the error above") from cdc.error

# COMMAND ----------

# Cell 6: Inspect status and recent append-only CDC log rows (for interactive use)
cdc.status()
display(spark.sql(f"SELECT * FROM {TARGET_LOG_TABLE} ORDER BY ingested_at DESC LIMIT 20"))

# COMMAND ----------

# Cell 7: Trigger external shutdown of a running job via STOP sentinel file (run from another notebook)
# with open(f"/Volumes/{catalog}/{schema}/{volume}/STOP", "w") as f:
#     f.write("stop")
