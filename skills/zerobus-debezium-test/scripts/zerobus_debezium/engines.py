"""engines: what each source engine needs for Debezium Server, as plain data. No I/O: the steps are SQL (run as the
engine's admin) or shell (run in the database's container), and the callers decide where they run.

    from zerobus_debezium import engines
    e = engines.get("sqlserver")
    e.setup(conn)            # [("sql", "..."), ...]: the database, the user's rights, CDC, the test table
    e.source_properties(conn, folder="/Volumes/c/s/v/dbz-ms")   # debezium.source.* for application.properties
    e.changes(1, base=100)   # round 1's statements on the test table

conn: {"host", "port", "user", "password"} for the Debezium login, plus "database"/"service" where the engine has them.
The test table is `customers (id, name, balance DECIMAL(12,2))` in the engine's test schema; ids from `base`.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

TABLE = "customers"
# the Debezium settings every engine shares: no snapshot (the change log starts at the producer's first start), no
# tombstones, decimals as text, no DDL events
COMMON = {
    "snapshot.mode": "no_data",
    "tombstones.on.delete": "false",
    "decimal.handling.mode": "string",
    "include.schema.changes": "false",
}


@dataclass(frozen=True)
class Engine:
    name: str
    connector: str
    schema: str                  # the test table's schema, as the connector names it
    table: str                   # the test table, as the connector names it in table.include.list
    key: str                     # the key column's name in the events (Oracle and Db2 fold to upper case)
    seq: str                     # Databricks SQL on cdc_log: the change's position in the source's log (silver's order)
    props: Callable              # (conn, folder) -> engine-specific debezium.source.* (without the prefix)
    setup_steps: Callable        # (conn) -> [(kind, text)]
    dialect_dml: Callable        # (statements) -> one script for the admin client
    history: bool = True         # needs schema.history.internal (every connector but PostgreSQL's)
    lag_s: int = 30              # how long a committed change can take to reach the log (Db2: ASN capture's commit)
    notes: list = field(default_factory=list)
    # the query-based snapshot (Lakeflow Connect, through a UC connection): (conn) -> {"login": user, "service": the
    # connection's Oracle service, "catalog": the pipeline's source_catalog (None: MySQL), "schema", "table"}; None:
    # no query-based connector for the engine (Db2)
    qbc: Callable | None = None
    # the snapshot through a UC JDBC connection instead (Spark's jdbc source, SELECT *: lfc-change-source's
    # notebooks/jdbc_snapshot.py), for an engine with no query-based connector: (conn) -> {"login", "database", "table"
    # as the source names it, "tls": whether the server listens with TLS}
    jdbc: Callable | None = None

    def setup(self, conn: dict) -> list:
        return self.setup_steps(conn)

    def source_properties(self, conn: dict, folder: str, name: str, heartbeat_ms: int = 10000,
                          history_folder: str | None = None) -> dict:
        """debezium.source.* for application.properties; folder: where the offsets live; history_folder: the schema
        history's (default folder). On a UC Volume the history can't be appended to (Debezium reopens it: "Illegal
        seek"), so the workspace notebook keeps it on local disk and copies it to the Volume."""
        p = {"connector.class": self.connector, "name": name, "topic.prefix": name.replace("-", "_"),
             "database.hostname": conn["host"], "database.port": str(conn["port"]),
             "database.user": conn["user"], "database.password": conn["password"],
             "table.include.list": self.table, **COMMON, **self.props(conn),
             "heartbeat.interval.ms": str(heartbeat_ms),
             "offset.storage": "org.apache.kafka.connect.storage.FileOffsetBackingStore",
             "offset.storage.file.filename": f"{folder}/offsets.dat", "offset.flush.interval.ms": "1000"}
        if self.history:
            p["schema.history.internal"] = "io.debezium.storage.file.history.FileSchemaHistory"
            p["schema.history.internal.file.filename"] = f"{history_folder or folder}/schema-history.dat"
        return {f"debezium.source.{k}": v for k, v in p.items()}

    def changes(self, rnd: int, base: int) -> str:
        """The test's statements: 0 the row that's there before the first start; 1 two inserts, an update, a delete
        (of the round-0 row); 2 an insert and an update (while stopped); 3 a delete (after the restart)."""
        t = f"{self.schema}.{TABLE}"
        by_round = {
            0: [f"INSERT INTO {t} (id, name, balance) VALUES ({base}, 'pre-start row', 10.00)"],
            1: [f"INSERT INTO {t} (id, name, balance) VALUES ({base + 1}, 'insert a', 20.00)",
                f"INSERT INTO {t} (id, name, balance) VALUES ({base + 2}, 'insert b', 30.00)",
                f"UPDATE {t} SET name = 'updated a', balance = 21.50 WHERE id = {base + 1}",
                f"DELETE FROM {t} WHERE id = {base}"],
            2: [f"INSERT INTO {t} (id, name, balance) VALUES ({base + 9}, 'while stopped', 10.00)",
                f"UPDATE {t} SET balance = balance + 1 WHERE id = {base + 2}"],
            3: [f"DELETE FROM {t} WHERE id = {base + 9}"],
        }
        return self.dialect_dml(by_round[rnd])

    def state_after(self, rnd: int, base: int) -> dict:
        """{id: (name, balance)}: the test's rows as the source holds them after round rnd."""
        rows = {}
        for r in range(rnd + 1):
            if r == 0:
                rows[base] = ("pre-start row", "10.00")
            elif r == 1:
                rows.update({base + 1: ("updated a", "21.50"), base + 2: ("insert b", "30.00")})
                rows.pop(base, None)
            elif r == 2:
                rows.update({base + 9: ("while stopped", "10.00"), base + 2: ("insert b", "31.00")})
            elif r == 3:
                rows.pop(base + 9, None)
        return rows

    def expected(self, rnd: int, base: int) -> list:
        """(operation, id) the change log must hold after round rnd's changes, in source order."""
        return {0: [], 1: [("create", base + 1), ("create", base + 2), ("update", base + 1), ("delete", base)],
                2: [("create", base + 9), ("update", base + 2)], 3: [("delete", base + 9)]}[rnd]


def _ddl(schema: str, dec: str = "DECIMAL(12,2)", text: str = "VARCHAR(100)") -> str:
    return f"CREATE TABLE {schema}.{TABLE} (id INT NOT NULL PRIMARY KEY, name {text}, balance {dec})"


# --- SQL Server: CDC on a database of its own; the Debezium login is lfc_user, db_owner there -------------------------
def _mssql_setup(conn):
    db, u = conn.get("database", "dbz"), conn["user"]
    return [("sql", f"IF DB_ID('{db}') IS NULL CREATE DATABASE [{db}];\nGO\nUSE [{db}];\nGO\n"
                    f"IF USER_ID('{u}') IS NULL CREATE USER [{u}] FOR LOGIN [{u}];\nGO\n"
                    f"ALTER ROLE db_owner ADD MEMBER [{u}];\nGO\n"
                    f"IF OBJECT_ID('dbo.{TABLE}') IS NULL {_ddl('dbo')};\nGO\n"
                    "IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()) = 0 EXEC sys.sp_cdc_enable_db;\nGO\n"
                    f"IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.{TABLE}'))\n"
                    f"  EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'{TABLE}', "
                    "@role_name = NULL, @supports_net_changes = 0;\nGO\n")]


def _mssql_dml(stmts):
    return "USE [dbz];\nGO\n" + "".join(f"{s};\n" for s in stmts) + "GO\n"


SQLSERVER = Engine(
    "sqlserver", "io.debezium.connector.sqlserver.SqlServerConnector", "dbo", f"dbo.{TABLE}", "id",
    "CAST(conv(split_part(value:source:change_lsn::STRING, ':', 1), 16, 10) AS BIGINT) * 281474976710656 + "
    "CAST(conv(split_part(value:source:change_lsn::STRING, ':', 2), 16, 10) AS BIGINT) * 65536 + "
    "CAST(conv(split_part(value:source:change_lsn::STRING, ':', 3), 16, 10) AS BIGINT)",
    lambda c: {"database.names": c.get("database", "dbz"), "database.encrypt": "true",
               "database.trustServerCertificate": "true"},
    _mssql_setup, _mssql_dml,
    qbc=lambda c: {"login": c["user"], "catalog": c.get("database", "dbz"), "schema": "dbo", "table": TABLE})


# --- MySQL: the binlog in ROW format with full images (the slot's container has it); a database of its own ------------
def _mysql_setup(conn):
    db, u = conn.get("database", "dbz"), conn["user"]
    return [("sql", f"CREATE DATABASE IF NOT EXISTS {db};\nGRANT ALL ON {db}.* TO '{u}'@'%';\n"
                    f"CREATE TABLE IF NOT EXISTS {_ddl(db)[len('CREATE TABLE '):]};\n"
                    "SELECT @@log_bin, @@binlog_format, @@binlog_row_image;\n")]


MYSQL = Engine(
    "mysql", "io.debezium.connector.mysql.MySqlConnector", "dbz", f"dbz.{TABLE}", "id",
    "CAST(regexp_extract(value:source:file::STRING, '([0-9]+)$', 1) AS BIGINT) * 17592186044416 + "
    "value:source:pos::BIGINT * 4096 + COALESCE(value:source:row::BIGINT, 0)",
    lambda c: {"database.server.id": str(c.get("server_id", 5401)), "database.include.list": c.get("database", "dbz"),
               "binary.handling.mode": "hex"},
    _mysql_setup, lambda stmts: "".join(f"{s};\n" for s in stmts),
    qbc=lambda c: {"login": c["user"], "catalog": None, "schema": c.get("database", "dbz"), "table": TABLE})


# --- Oracle: LogMiner. ARCHIVELOG and supplemental logging (restarts the instance once), a common user for Debezium
#     (c##dbzuser, its password conn["password"]), the table in the PDB's LFC_USER schema with all columns logged ----
ORACLE_GRANTS = ("CREATE SESSION, SET CONTAINER, SELECT ANY TABLE, FLASHBACK ANY TABLE, SELECT_CATALOG_ROLE, "
                 "EXECUTE_CATALOG_ROLE, SELECT ANY TRANSACTION, LOGMINING, CREATE TABLE, LOCK ANY TABLE, CREATE SEQUENCE")
ORACLE_VIEWS = ("V_$DATABASE", "V_$LOG", "V_$LOG_HISTORY", "V_$LOGMNR_LOGS", "V_$LOGMNR_CONTENTS", "V_$LOGMNR_PARAMETERS",
                "V_$LOGFILE", "V_$ARCHIVED_LOG", "V_$ARCHIVE_DEST_STATUS", "V_$TRANSACTION", "V_$MYSTAT", "V_$STATNAME")


def _oracle_setup(conn):
    pdb, pw, owner = conn.get("service", "FREEPDB1"), conn["password"], conn.get("owner", "LFC_USER")
    archive = ("WHENEVER SQLERROR CONTINUE\n"   # each step is safe to repeat: an error means it's done already
               "SHUTDOWN IMMEDIATE\nSTARTUP MOUNT\nALTER DATABASE ARCHIVELOG;\nALTER DATABASE OPEN;\n"
               "ALTER PLUGGABLE DATABASE ALL OPEN;\nALTER PLUGGABLE DATABASE ALL SAVE STATE;\n"
               "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;\n"
               "CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/FREE/logminer_tbs.dbf' SIZE 25M REUSE "
               "AUTOEXTEND ON MAXSIZE UNLIMITED;\n"
               f"ALTER SESSION SET CONTAINER = {pdb};\n"
               f"CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/FREE/{pdb}/logminer_tbs.dbf' SIZE 25M "
               "REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;\n"
               "ALTER SESSION SET CONTAINER = CDB$ROOT;\n"
               f'CREATE USER c##dbzuser IDENTIFIED BY "{pw}" DEFAULT TABLESPACE logminer_tbs QUOTA UNLIMITED ON '
               "logminer_tbs CONTAINER = ALL;\n"
               f'ALTER USER c##dbzuser IDENTIFIED BY "{pw}" CONTAINER = ALL;\n'
               f"GRANT {ORACLE_GRANTS} TO c##dbzuser CONTAINER = ALL;\n"
               + "".join(f"GRANT SELECT ON {v} TO c##dbzuser CONTAINER = ALL;\n" for v in ORACLE_VIEWS)
               + "GRANT EXECUTE ON DBMS_LOGMNR TO c##dbzuser CONTAINER = ALL;\n"
               "GRANT EXECUTE ON DBMS_LOGMNR_D TO c##dbzuser CONTAINER = ALL;\n"
               f"ALTER SESSION SET CONTAINER = {pdb};\n"
               f"{_ddl(owner, dec='NUMBER(12,2)', text='VARCHAR2(100)').replace('INT NOT', 'NUMBER(10) NOT')};\n"
               f"ALTER TABLE {owner}.{TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n"
               "WHENEVER SQLERROR EXIT FAILURE\n"
               "SELECT log_mode, supplemental_log_data_min FROM v$database;\n")
    return [("sql", archive)]


def _oracle_dml(stmts, pdb="FREEPDB1"):
    return f"ALTER SESSION SET CONTAINER = {pdb};\n" + "".join(f"{s};\n" for s in stmts) + "COMMIT;\n"


ORACLE = Engine(
    "oracle", "io.debezium.connector.oracle.OracleConnector", "LFC_USER", f"LFC_USER.{TABLE.upper()}", "ID",
    "value:source:scn::BIGINT",
    lambda c: {"database.dbname": "FREE", "database.pdb.name": c.get("service", "FREEPDB1"),
               "schema.include.list": c.get("owner", "LFC_USER"), "log.mining.strategy": "online_catalog"},
    _oracle_setup, _oracle_dml,
    # LogMiner's common user reads for the snapshot too (SELECT ANY TABLE): the CDB's service, the PDB as catalog
    qbc=lambda c: {"login": "C##DBZUSER", "service": "FREE", "catalog": c.get("service", "FREEPDB1"),
                   "schema": c.get("owner", "LFC_USER"), "table": TABLE.upper()})


# --- Db2: ASN capture (Debezium's management scripts, compiled in the container), archive logging, the capture agent
#     started before ADDTABLE, then a reinit (Debezium's documented order). Shell steps run in the container as root
DB2_INIT = ("https://raw.githubusercontent.com/debezium/debezium-examples/8d0f2f0175483853f430cb1574631c2dc3332dcf/"
            "tutorial/debezium-db2-init/db2server")
DB2_FILES = ("asncdc.c", "asncdc_UDF.sql", "asncdcaddremove.sql", "asncdctables.sql", "dbsetup.sh")
DB2_ASN = "capture_schema=asncdc capture_server=LFCDB"


def _db2_as(cmd: str) -> str:
    return f"su - db2inst1 -c {_sq(cmd)}"


def _sq(s: str) -> str:
    return "'" + s.replace("'", "'\"'\"'") + "'"


def _db2_setup(conn):
    u, schema = conn["user"].upper(), "DBZ"
    fetch = "mkdir -p /asncdctools/src && cd /asncdctools/src && " + " && ".join(
        f"curl -sfL -o {f} {DB2_INIT}/{f}" for f in DB2_FILES) + " && chmod -R 755 /asncdctools && chown -R db2inst1 /asncdctools"
    return [
        ("shell", "command -v curl >/dev/null || (yum install -y -q curl >/dev/null 2>&1 || microdnf install -y curl)"),
        ("shell", fetch),
        ("shell", _db2_as("mkdir -p ~/archive && db2 update db cfg for LFCDB using LOGARCHMETH1 DISK:$HOME/archive "
                          ">/dev/null && db2 backup db LFCDB to /dev/null >/dev/null; "
                          "db2 update db cfg for LFCDB using PCKCACHESZ 16384 LOGSECOND 200 >/dev/null; "
                          "bash /asncdctools/src/dbsetup.sh LFCDB")),
        ("sql", f"GRANT DBADM ON DATABASE TO USER {u};\nCREATE SCHEMA {schema};\n"
                f"{_ddl(schema)};\n"),
        ("shell_detached", _db2_as(f"sqllib/bin/asncap {DB2_ASN}")),
        ("sleep", "30"),
        ("sql", f"CALL ASNCDC.ADDTABLE('{schema}', '{TABLE.upper()}');\n"),
        ("sleep", "10"),
        ("shell", _db2_as(f"sqllib/bin/asnccmd {DB2_ASN} reinit")),
    ]


DB2 = Engine(
    "db2", "io.debezium.connector.db2.Db2Connector", "DBZ", f"DBZ.{TABLE.upper()}", "ID",
    "CAST(conv(substring(split_part(COALESCE(value:source:change_lsn, value:source:commit_lsn)::STRING, ':', 3), -15),"
    " 16, 10) AS BIGINT)",
    lambda c: {"database.dbname": c.get("database", "LFCDB")},
    _db2_setup, lambda stmts: "".join(f"{s};\n" for s in stmts), lag_s=90,
    jdbc=lambda c: {"login": c["user"], "database": c.get("database", "LFCDB"), "table": f"DBZ.{TABLE.upper()}",
                    "tls": False},
    notes=["the capture agent asncap must run in the container; a container restart stops it"])


# --- PostgreSQL: pgoutput on a slot of the producer's own (slot.name), a publication for the test table ---------------
def _pg_setup(conn):
    pub, u = conn.get("publication", "dbz_publication"), conn["user"]
    return [("sql", f"CREATE SCHEMA IF NOT EXISTS dbz;\nCREATE TABLE IF NOT EXISTS {_ddl('dbz')[len('CREATE TABLE '):]};\n"
                    f"ALTER TABLE dbz.{TABLE} REPLICA IDENTITY FULL;\nGRANT USAGE ON SCHEMA dbz TO {u};\n"
                    f"GRANT SELECT ON dbz.{TABLE} TO {u};\n"
                    f"DO $$BEGIN IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = '{pub}') THEN "
                    f"CREATE PUBLICATION {pub} FOR TABLE dbz.{TABLE}; END IF; END$$;\nSHOW wal_level;\n")]


POSTGRES = Engine(
    "postgres", "io.debezium.connector.postgresql.PostgresConnector", "dbz", f"dbz.{TABLE}", "id",
    "value:source:lsn::BIGINT",
    lambda c: {"database.dbname": c.get("database", "postgres"), "plugin.name": "pgoutput",
               "slot.name": c.get("slot", "dbz_slot"), "publication.name": c.get("publication", "dbz_publication"),
               "publication.autocreate.mode": "disabled"},
    _pg_setup, lambda stmts: "".join(f"{s};\n" for s in stmts), history=False,
    qbc=lambda c: {"login": c["user"], "catalog": c.get("database", "postgres"), "schema": "dbz", "table": TABLE})


ENGINES = {e.name: e for e in (SQLSERVER, MYSQL, ORACLE, DB2, POSTGRES)}
# Debezium Server's log lines: streaming has started; the engine stopped on an error (lfc_change_source's
# DebeziumServer waits for the same two)
READY, FAILED = "Starting streaming", "Engine has failed"


def get(name: str) -> Engine:
    try:
        return ENGINES[name]
    except KeyError:
        raise ValueError(f"engine is one of {sorted(ENGINES)}: {name}") from None


def sink_properties(endpoint: str, workspace_url: str, client_id: str, client_secret: str, table: str) -> dict:
    """Debezium Server's Zerobus sink: envelope into one change-log table (unquoted catalog.schema.table), VARIANT
    fields sent as JSON text, no tombstones, the source's position as the idempotency key."""
    return {"debezium.sink.type": "zerobus",
            "debezium.sink.zerobus.endpoint": endpoint,
            "debezium.sink.zerobus.workspace.url": workspace_url,
            "debezium.sink.zerobus.client.id": client_id,
            "debezium.sink.zerobus.client.secret": client_secret,
            "debezium.sink.zerobus.payload.mode": "envelope",
            "debezium.sink.zerobus.table": table.replace("`", ""),
            "debezium.sink.zerobus.record.format": "json",
            "debezium.sink.zerobus.idempotency.mode": "source",
            "debezium.sink.zerobus.tombstone.handling.mode": "drop",
            "debezium.sink.zerobus.json.flexible.fields.encoding": "string",
            "debezium.format.key": "json", "debezium.format.value": "json",
            "debezium.format.key.schemas.enable": "false", "debezium.format.value.schemas.enable": "false",
            "quarkus.log.console.json": "false"}


def render(props: dict) -> str:
    """application.properties text: one key=value a line, backslashes escaped (the format's only escape a value
    from a secret needs here)."""
    return "".join(f"{k}={str(v).replace(chr(92), chr(92) * 2)}\n" for k, v in props.items())
