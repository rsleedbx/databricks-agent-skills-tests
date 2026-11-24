#!/usr/bin/env python3
"""size_query.py: the sizing query for one engine, as one statement: the engine's catalog SQL (<engine>.sql beside
this file) run through the Unity Catalog connection with remote_query. size.sh runs it with the Databricks CLI; in a
notebook or Genie Code, run it with spark.sql (the CLI doesn't run there):

    import sys; sys.path.insert(0, "<skills folder>/lfc-source-sizing/scripts")
    from size_query import remote_query
    display(spark.sql(remote_query("<connection>", "postgres", database="<database>")))

usage: size_query.py <connection> <engine> [--database D | --service S] [--file NAME]   (prints the statement)
"""
import argparse
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SQL_FILE = {"mariadb": "mysql"}                          # MariaDB: a MYSQL connection and the MySQL query
NEEDS_DATABASE = ("postgres", "sqlserver", "cockroachdb", "yugabytedb")
ENGINES = ("postgres", "mysql", "mariadb", "sqlserver", "oracle", "cockroachdb", "yugabytedb")


class SizeQueryError(ValueError):
    """A missing engine, file, --database or --service."""


def remote_query(connection, engine, database=None, service=None, file=None):
    """SELECT * FROM remote_query(...) for the engine; file names another query beside this one (mysql-process)."""
    if engine not in ENGINES:
        raise SizeQueryError(f"engine: {', '.join(ENGINES)} (db2 and informix: run scripts/db2.sql or informix.sql "
                             "by hand)")
    path = os.path.join(HERE, (file or SQL_FILE.get(engine, engine)) + ".sql")
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
    with open(path) as f:                                 # comments off, __DB__ filled in, one line
        inner = " ".join(re.sub(r"--.*$", "", line.rstrip("\n")).replace("__DB__", database or "") for line in f)
    inner = re.sub(" +", " ", inner + " ")
    return f"SELECT * FROM remote_query('{connection}'{arg}, query => \"{inner}\")"


def main(argv=None):
    a = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    a.add_argument("connection"); a.add_argument("engine")
    a.add_argument("--database"); a.add_argument("--service"); a.add_argument("--file")
    x = a.parse_args(argv)
    try:
        print(remote_query(x.connection, x.engine, x.database, x.service, x.file))
    except SizeQueryError as e:
        print(e, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
