"""lfc_source_contract.jdbc: SQL on a source database through a Unity Catalog connection of type JDBC, from a notebook
or a job, with no password or database driver in the code. The connection holds the login; who may use it is its
USE CONNECTION grant. One pattern for every write a test makes on a source (DDL, DML, publications, slots):

    from lfc_source_contract.jdbc import JdbcSql, guarded
    db = JdbcSql(spark, "my_pg_jdbc")
    db.execute(guarded("SELECT 1 FROM pg_publication WHERE pubname = 'p'", "CREATE PUBLICATION p FOR TABLE s.t"))
    db.query("SELECT count(*) AS n FROM s.t")            # [(n,)]
    con = dbapi(spark, "my_pg_jdbc")                     # a DB-API connection over it, for code written for a driver

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

import json
import re

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


def url(engine: str, host: str, port: int, database: str, tls: bool = True) -> str:
    """The JDBC URL, with TLS on (lfc-source-setup: trust the server's certificate, never turn TLS off). tls: Db2's
    only, for a server that has no TLS listener (the others always ask for it)."""
    if engine == "postgres":
        return f"jdbc:postgresql://{host}:{port}/{database}?sslmode=require"
    if engine == "mysql":
        return f"jdbc:mysql://{host}:{port}/{database}?sslMode=REQUIRED&allowMultiQueries=true"
    if engine == "sqlserver":
        return f"jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=true"
    if engine == "db2":   # sslConnection=true when the server has TLS; the Db2 Community image listens without it
        return f"jdbc:db2://{host}:{port}/{database}" + (":sslConnection=true;" if tls else "")
    raise ValueError(f"url: {engine}: postgres, mysql, sqlserver or db2 (the others: their driver's own form)")


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

    def query(self, sql: str, init: str | None = None) -> list[tuple]:
        """A SELECT's rows as tuples (Spark wraps it as SELECT * FROM (<sql>), so it must be one SELECT); init, a
        statement run first on the same connection (e.g. SET search_path), must be safe to repeat."""
        r = self._reader().option("query", sql)
        if init:
            r = r.option("sessionInitStatement", init)
        return [tuple(x) for x in r.load().collect()]


def ident(name: str) -> str:
    """A quoted SQL identifier (a dotted name stays one identifier: quote each part yourself)."""
    return '"' + str(name).replace('"', '""') + '"'


def literal(v) -> str:
    """A SQL literal for a parameter: numbers as they are, text quoted with '' doubled (standard SQL)."""
    if isinstance(v, bool) or v is None:
        return {True: "true", False: "false", None: "NULL"}[v]
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


_WRITE_AFTER_CTE = re.compile(r"\)\s*(INSERT|UPDATE|DELETE|MERGE)\b", re.IGNORECASE)   # WITH d AS (...) UPDATE ...: a write


class _Cursor:
    def __init__(self, con):
        self.con, self.rows, self.description, self.rowcount = con, [], None, -1

    def execute(self, sql: str, params=None):
        if params is not None:                           # the driver's %s and %% (pyformat), filled here
            vals = iter(params)
            sql = re.sub(r"%(%|s)", lambda m: "%" if m.group(1) == "%" else literal(next(vals)), sql)
        head = (sql.lstrip().split(None, 1) or [""])[0].upper()
        init = ";\n".join(self.con.session) or None
        if head == "SET":                                # session state: kept, and run before every later statement
            name = sql.split(None, 2)[1].lower() if len(sql.split()) > 1 else ""
            self.con.session = [s for s in self.con.session if s.split(None, 2)[1].lower() != name] + [sql.strip().rstrip(";")]
            self.rows = []
        elif head in ("SELECT", "VALUES", "TABLE") or (head == "WITH" and not _WRITE_AFTER_CTE.search(sql)):
            self.rows = self.con.read(f"SELECT coalesce(json_agg(row_to_json(row(q.*))), '[]')::text AS j FROM ({sql}) q", init)
        else:
            self.con.db.execute(f"{init};\n{sql}" if init else sql)
            self.rows = []
        return self

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def close(self):
        pass


class _Connection:
    autocommit = True

    def __init__(self, db: JdbcSql, read_connection: str | None = None, database: str | None = None):
        self.db, self.session, self.read_connection, self.database = db, [], read_connection, database

    def read(self, json_query: str, init: str | None) -> list[tuple]:
        """One JSON value's rows: through the regular (federation) connection's remote_query when there is one, with
        parameter markers so nothing is quoted by hand; else through the JDBC connection's query option."""
        if self.read_connection:
            out = [tuple(r) for r in self.db.spark.sql(
                "SELECT * FROM remote_query(:connection, database => :database, query => :query)",
                args={"connection": self.read_connection, "database": self.database, "query": json_query}).collect()]
        else:
            out = self.db.query(json_query, init)
        return [tuple(d.values()) for d in json.loads(out[0][0])] if out else []

    def cursor(self):
        return _Cursor(self)

    def execute(self, sql: str, params=None):
        return self.cursor().execute(sql, params)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def dbapi(spark, connection: str, read_connection: str | None = None, database: str | None = None) -> _Connection:
    """A DB-API connection (PostgreSQL) over a TYPE JDBC connection, so code written for a driver (cursor().execute with
    %s parameters, fetchall) runs through Unity Catalog with no password. With read_connection (the regular POSTGRESQL
    connection, e.g. the one a gateway uses) and database, SELECTs go through its remote_query and only writes through
    the JDBC one: its login must be able to read what the SELECTs name. Each statement is its own JDBC session, in
    autocommit: SET statements are kept and run before every later one, as a session would keep them (a SELECT's
    names, though, resolve before them: Spark reads a query's columns on a connection of its own, so qualify the
    names a SELECT uses rather than rely on SET search_path); a SELECT comes
    back as one JSON value (each row's columns f1..fn), so any Postgres type reads. Writes run as sessionInitStatement:
    they must be safe to run again (a task retry runs them again)."""
    if read_connection and not database:
        raise ValueError("dbapi: read_connection needs database (remote_query names it)")
    return _Connection(JdbcSql(spark, connection), read_connection, database)
