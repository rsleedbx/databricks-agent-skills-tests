# One DB-API connection per engine, from a pip-installable driver (references/python-drivers.md), with autocommit on:
# each INSERT commits by itself, so an interrupted fill loses at most the statements still running.
# The password comes from a file (the first line), never from the command line. A JDBC path (JPype) would be another
# function here returning the same kind of connection.
import os, time


def password(a):
    f = a.password_file or os.environ.get("LFC_PASSWORD_FILE")
    if not f: raise SystemExit("give --password-file (or LFC_PASSWORD_FILE): a file whose first line is the password")
    return open(os.path.expanduser(f)).readline().rstrip("\r\n")


def connect(engine, a, tries=10):
    for i in range(tries):                         # a server that's still starting refuses the handshake for a while
        try: return _connect(engine, a)
        except Exception as x:
            if i == tries - 1 or not any(s in str(x).lower() for s in ("handshake", "starting up", "connection refused", "timed out", "sql30081n")): raise
            time.sleep(3)


def _connect(engine, a):
    pw = password(a)
    if engine == "postgres":
        import psycopg                             # psycopg 3 (psycopg[binary]), not psycopg2: psycopg2-binary has SSL and
                                                   # certificate trouble on serverless (one run aborted on import, SIGABRT)
        c = psycopg.connect(host=a.host, port=a.port or 5432, user=a.user, password=pw, dbname=a.database or "postgres",
                            autocommit=True)       # the planner's SQL has no parameters, so its % is sent as is
    elif engine == "mysql":
        import pymysql
        c = pymysql.connect(host=a.host, port=a.port or 3306, user=a.user, password=pw, database=a.database, charset="utf8mb4", autocommit=True)
    elif engine == "sqlserver":
        import mssql_python
        tls = "Encrypt=yes;TrustServerCertificate=yes" if a.trust_server_certificate else "Encrypt=yes"
        c = mssql_python.connect(f"Server={a.host},{a.port or 1433};Database={a.database or 'master'};UID={a.user};PWD={{{pw.replace('}', '}}')}}};{tls}", autocommit=True)
    elif engine == "oracle":
        import oracledb                            # thin mode: no Oracle client
        c = oracledb.connect(user=a.user, password=pw, dsn=f"{a.host}:{a.port or 1521}/{a.database}")
        c.autocommit = True
    elif engine == "db2":
        import ibm_db_dbi                          # ibm-db
        c = ibm_db_dbi.connect(f"DATABASE={a.database};HOSTNAME={a.host};PORT={a.port or 50000};PROTOCOL=TCPIP;UID={a.user};PWD={pw};", "", "")
        c.set_autocommit(True)
    elif engine == "informix":
        import informix_db                         # informix-driver: pure Python
        # autocommit at connect: set afterwards, a transaction is already open (and CREATE DATABASE fails with -377)
        c = informix_db.connect(host=a.host, port=a.port or 9088, user=a.user, password=pw, database=a.database, server=a.server or "informix", autocommit=True)
        if a.database:   # wait for a lock rather than fail at once (-425, ISAM -107: the planner's session and a filling one)
            k = c.cursor(); k.execute("SET LOCK MODE TO WAIT 30"); k.close()
    else:
        raise SystemExit(f"unknown engine {engine}")
    return c
