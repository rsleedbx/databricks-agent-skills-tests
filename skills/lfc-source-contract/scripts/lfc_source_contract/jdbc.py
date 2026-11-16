"""lfc_source_contract.jdbc: SQL on a source database through a Unity Catalog connection of type JDBC, from a notebook
or a job, with no password or database driver in the code. The connection holds the login; who may use it is its
USE CONNECTION grant. One pattern for every write a test makes on a source (DDL, DML, publications, slots):

    from lfc_source_contract.jdbc import JdbcSql, guarded
    db = JdbcSql(spark, "my_pg_jdbc")
    db.execute(guarded("SELECT 1 FROM pg_publication WHERE pubname = 'p'", "CREATE PUBLICATION p FOR TABLE s.t"))
    db.query("SELECT count(*) AS n FROM s.t")            # [(n,)]

execute() runs the statement as Spark's JDBC option sessionInitStatement on a read with one partition and one action.
Spark runs that statement on every connection it opens: once here, and again on a task retry. So every statement
must be safe to run twice: guarded() wraps statements in a PostgreSQL DO block that runs them only while a check
finds nothing, all in one transaction; deterministic UPDATEs and INSERTs that check their own keys are safe as they
are. pg_create_logical_replication_slot can't run in a transaction that has written: give it a guard of its own.

The connection: connection_body() is the request for one (`databricks connections create --json`, or the SDK). The
driver comes from Maven (environment_settings.java_dependencies), so no volume or catalog holds it; the allow-list
lets a query set dbtable, query, sessionInitStatement and numPartitions. Give it a login of its own with only what
the writes need: anyone with USE CONNECTION can run any SQL that login may run. Tested 2026-11-15 on serverless with
Lakebase (PostgreSQL 17, pgjdbc 42.7.4, TLSv1.3): a guarded schema, table and rows, and a slot, each run twice, the
second changing nothing; a FOR TABLES IN SCHEMA publication needs a superuser, a FOR TABLE one only the tables' owner.
The drivers and URLs are the ones a long-running demo's connections use, one per engine, each checked with a read.
"""
from __future__ import annotations

MAVEN = {                                               # environment_settings.java_dependencies, per engine
    "postgres": ["org.postgresql:postgresql:42.7.4"],
    "mysql": ["com.mysql:mysql-connector-j:9.1.0"],
    "sqlserver": ["com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11"],
    "oracle": ["com.oracle.database.jdbc:ojdbc11:23.5.0.24.07"],
    "db2": ["com.ibm.db2:jcc:11.5.9.0"],
    "informix": ["com.ibm.informix:jdbc:15.0.1.4", "org.mongodb:bson:3.8.0"],
}
ALLOW = "dbtable,query,sessionInitStatement,numPartitions"
TAG = "$lfc_sql$"                                       # guarded()'s dollar quote


def url(engine: str, host: str, port: int, database: str) -> str:
    """The JDBC URL, with TLS on (lfc-source-setup: trust the server's certificate, never turn TLS off)."""
    if engine == "postgres":
        return f"jdbc:postgresql://{host}:{port}/{database}?sslmode=require"
    if engine == "mysql":
        return f"jdbc:mysql://{host}:{port}/{database}?sslMode=REQUIRED&allowMultiQueries=true"
    if engine == "sqlserver":
        return f"jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=true"
    raise ValueError(f"url: {engine}: postgres, mysql or sqlserver (the others: their driver's own form)")


def connection_body(name: str, engine: str, jdbc_url: str, user: str, password: str, comment: str | None = None) -> dict:
    """A TYPE JDBC connection's create request: the Maven driver, the URL and login, the allow-list."""
    if engine not in MAVEN:
        raise ValueError(f"connection_body: {engine}: one of {', '.join(MAVEN)}")
    b = {"name": name, "connection_type": "JDBC", "environment_settings": {"java_dependencies": list(MAVEN[engine])},
         "options": {"url": jdbc_url, "user": user, "password": password, "externalOptionsAllowList": ALLOW}}
    if comment:
        b["comment"] = comment
    return b


def guarded(exists: str, *statements: str) -> str:
    """A PostgreSQL DO block that runs the statements, in order and in one transaction, only when the query `exists`
    returns no row: safe to run again (the second run finds the row and does nothing)."""
    if not statements:
        raise ValueError("guarded: no statements")
    for s in (exists, *statements):
        if TAG in s:
            raise ValueError(f"guarded: a statement holds {TAG}")
    body = "\n    ".join(f"EXECUTE {TAG}{s.strip().rstrip(';')}{TAG};" for s in statements)
    return f"DO {TAG[:-1]}_do$ BEGIN\n  IF NOT EXISTS ({exists.strip().rstrip(';')}) THEN\n    {body}\n  END IF;\nEND {TAG[:-1]}_do$"


class JdbcSql:
    """SQL through one TYPE JDBC connection: execute() for writes (idempotent statements only), query() for reads."""

    def __init__(self, spark, connection: str):
        self.spark, self.connection = spark, connection

    def _reader(self):
        return self.spark.read.format("jdbc").option("databricks.connection", self.connection)

    def execute(self, statement: str) -> None:
        """Run one statement (or a ;-separated script, one transaction on PostgreSQL) once, plus on a task retry."""
        (self._reader().option("query", "SELECT 1 AS lfc_one").option("numPartitions", "1")
         .option("sessionInitStatement", statement).load().collect())

    def query(self, sql: str) -> list[tuple]:
        """A SELECT's rows as tuples (Spark wraps it as SELECT * FROM (<sql>), so it must be one SELECT)."""
        return [tuple(r) for r in self._reader().option("query", sql).load().collect()]
