"""catalog: one statement that runs a catalog query through a Unity Catalog connection (remote_query), and the
per-engine size queries (sql/size/<engine>.sql: each table's rows and bytes, catalog only). lfc-source-sizing's
size_query.py and size.sh, and introspect, all build their statements here.
"""
import os
import re

HERE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql", "size")   # <engine>.sql
SQL_FILE = {"mariadb": "mysql"}                          # MariaDB: a MYSQL connection and the MySQL query
NEEDS_DATABASE = ("postgres", "sqlserver", "cockroachdb", "yugabytedb")
ENGINES = ("postgres", "mysql", "mariadb", "sqlserver", "oracle", "cockroachdb", "yugabytedb")


class SizeQueryError(ValueError):
    """A missing engine, file, --database or --service."""


def query_text(engine, database=None, file=None, folder=None, fill=None, jdbc=False):
    """A catalog query's text on one line: the engine's size query, or `file` in `folder`; comments off, __DB__ and
    fill's placeholders filled in (__SCHEMA__ not given: empty, so a size query's schema filter is off). jdbc: for Spark's jdbc source (Db2 and Informix too: a JDBC connection has them)."""
    if engine not in ENGINES and not (jdbc and engine in ("db2", "informix")):
        raise SizeQueryError(f"engine: {', '.join(ENGINES)} (db2 and informix: run lfc-introspection scripts/lfc_introspection/sql/size/db2.sql or informix.sql "
                             "by hand, or through a JDBC connection)")
    path = os.path.join(folder or HERE, (file or SQL_FILE.get(engine, engine)) + ".sql")
    if not os.path.isfile(path):
        raise SizeQueryError(f"no query {path}")
    with open(path) as f:                                 # comments off, __DB__ filled in, one line
        inner = " ".join(re.sub(r"--.*$", "", line.rstrip("\n")).replace("__DB__", database or "") for line in f)
    for k, v in (fill or {}).items():
        inner = inner.replace(k, str(v).replace("'", "''"))
    inner = inner.replace("__SCHEMA__", "")   # not given: a size query covers every schema (whole-database sizing)
    return re.sub(" +", " ", inner + " ").strip().rstrip(";")


def remote_query(connection, engine, database=None, service=None, file=None, folder=None, fill=None):
    """SELECT * FROM remote_query(...) for the engine; file names another query beside this one (mysql-process), or
    in `folder` (another skill's catalog queries); fill: {placeholder: value} replaced in the query, like __DB__."""
    if engine not in ENGINES:
        raise SizeQueryError(f"engine: {', '.join(ENGINES)} (db2 and informix: run lfc-introspection scripts/lfc_introspection/sql/size/db2.sql or informix.sql "
                             "by hand)")
    path = os.path.join(folder or HERE, (file or SQL_FILE.get(engine, engine)) + ".sql")
    if not os.path.isfile(path):
        raise SizeQueryError(f"no query {path}")
    if engine in NEEDS_DATABASE:
        if not database:
            raise SizeQueryError(f"--database is needed for {engine}")
        arg = f", database => '{database}'"
    elif engine == "oracle":
        if not service:
            raise SizeQueryError("--service (the PDB) is needed for oracle")
        arg = f", service_name => '{service}'"
    else:
        arg = ""
    inner = query_text(engine, database, file, folder, fill) + " "
    return f"SELECT * FROM remote_query('{connection}'{arg}, query => \"{inner}\")"


