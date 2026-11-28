#!/usr/bin/env python3
"""lfc-setup.py pg|mysql: the test user and seed tables on an Azure Database for PostgreSQL or MySQL Flexible Server
(after flex.py create set the CDC parameters).
  Needs HOST (the server's FQDN), DB, ADMIN_PW_FILE (lfcadmin's password), LOGIN_PW_FILE (a private file, or keyring:<name>,
  with the test user's password). Optional: LOGIN (lfc_user), SCHEMA (pg: public). Needs psql / mysql; passwords go
  through PGPASSWORD / MYSQL_PWD, never the command line. TLS is required by the server, and used; psql checks the
  server's certificate and name against PG_CA (default /etc/ssl/cert.pem, the OS bundle: a ~/.postgresql/root.crt kept
  for another server would otherwise fail it with "certificate verify failed").
  pg     the user with REPLICATION, owner of intpk (primary key, cursor dt) and dtix (no key, REPLICA IDENTITY FULL), so
         it can make the pipeline's own publication and slot; 10 rows each; a trigger bumps dt on update
  mysql  the user with the CDC grants from the docs (REPLICATION SLAVE and CLIENT, SELECT on *.*, mysql.user plugin),
         the same two tables with dt ON UPDATE CURRENT_TIMESTAMP; 10 rows each
  Idempotent: rerun after a fix. Ends by reading the row counts as the test user.
"""
from __future__ import annotations

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "lfc-source-azure-sql", "scripts"))
from script_common import check_password, exits, need, read_password, tail, tool


def pg_batch(login: str, lpw: str, db: str, sc: str) -> str:
    return f"""DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{login}') THEN CREATE ROLE {login} LOGIN; END IF; END $$;
ALTER ROLE {login} WITH LOGIN REPLICATION PASSWORD '{lpw}';
GRANT CONNECT, CREATE ON DATABASE "{db}" TO {login};
CREATE SCHEMA IF NOT EXISTS {sc};
GRANT USAGE, CREATE ON SCHEMA {sc} TO {login};
CREATE TABLE IF NOT EXISTS {sc}.intpk (pk SERIAL PRIMARY KEY, dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, ops VARCHAR(255) DEFAULT 'insert');
CREATE INDEX IF NOT EXISTS idx_intpk_dt ON {sc}.intpk (dt);
CREATE TABLE IF NOT EXISTS {sc}.dtix (pk BIGINT, dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, ops VARCHAR(255) DEFAULT 'insert');
CREATE INDEX IF NOT EXISTS idx_dtix_dt ON {sc}.dtix (dt);
ALTER TABLE {sc}.dtix REPLICA IDENTITY FULL;
CREATE OR REPLACE FUNCTION {sc}.set_dt() RETURNS trigger LANGUAGE plpgsql AS $f$ BEGIN NEW.dt := CURRENT_TIMESTAMP; RETURN NEW; END $f$;
DROP TRIGGER IF EXISTS intpk_set_dt ON {sc}.intpk; CREATE TRIGGER intpk_set_dt BEFORE UPDATE ON {sc}.intpk FOR EACH ROW EXECUTE FUNCTION {sc}.set_dt();
DROP TRIGGER IF EXISTS dtix_set_dt ON {sc}.dtix; CREATE TRIGGER dtix_set_dt BEFORE UPDATE ON {sc}.dtix FOR EACH ROW EXECUTE FUNCTION {sc}.set_dt();
INSERT INTO {sc}.intpk (ops) SELECT 'insert' FROM generate_series(1, 10) WHERE NOT EXISTS (SELECT 1 FROM {sc}.intpk);
INSERT INTO {sc}.dtix (pk) SELECT g FROM generate_series(1, 10) g WHERE NOT EXISTS (SELECT 1 FROM {sc}.dtix);
-- the test user owns the tables: a publication on a table needs its owner
GRANT {login} TO lfcadmin;
ALTER TABLE {sc}.intpk OWNER TO {login}; ALTER TABLE {sc}.dtix OWNER TO {login}; ALTER SEQUENCE {sc}.intpk_pk_seq OWNER TO {login};
"""


def mysql_batch(login: str, lpw: str, db: str) -> str:
    return f"""CREATE DATABASE IF NOT EXISTS `{db}`;
CREATE USER IF NOT EXISTS '{login}'@'%' IDENTIFIED WITH caching_sha2_password BY '{lpw}';
ALTER USER '{login}'@'%' IDENTIFIED WITH caching_sha2_password BY '{lpw}';
GRANT ALTER, CREATE, DROP, INDEX, SELECT, INSERT, DELETE, UPDATE ON `{db}`.* TO '{login}'@'%';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO '{login}'@'%';
GRANT SELECT (plugin, user) ON mysql.user TO '{login}'@'%';
USE `{db}`;
CREATE TABLE IF NOT EXISTS intpk (pk SERIAL PRIMARY KEY, dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, ops VARCHAR(255) DEFAULT 'insert', INDEX idx_dt (dt));
CREATE TABLE IF NOT EXISTS dtix (pk BIGINT, dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, ops VARCHAR(255) DEFAULT 'insert', INDEX idx_dt (dt));
INSERT INTO intpk (ops) SELECT 'insert' FROM (SELECT 1 FROM information_schema.columns LIMIT 10) t WHERE (SELECT COUNT(*) FROM intpk) = 0;
INSERT INTO dtix (pk) SELECT ROW_NUMBER() OVER () FROM (SELECT 1 FROM information_schema.columns LIMIT 10) t WHERE (SELECT COUNT(*) FROM dtix) = 0;
"""


@exits
def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__.strip(), file=sys.stderr)
        return 1
    engine = argv[0]
    host, db = need("HOST"), need("DB")
    admin_ref, login_ref = need("ADMIN_PW_FILE"), need("LOGIN_PW_FILE")
    login = os.environ.get("LOGIN") or "lfc_user"
    for ref in (admin_ref, login_ref):
        check_password(ref)
    admin_pw = read_password(admin_ref)
    login_pw = read_password(login_ref)
    lpw = login_pw.replace("'", "''")   # the login's password goes into a private batch file, not onto the command line

    with tempfile.TemporaryDirectory() as T:
        if engine == "pg":
            sc = os.environ.get("SCHEMA") or "public"
            tls = f"sslmode=verify-full sslrootcert={os.environ.get('PG_CA') or '/etc/ssl/cert.pem'}"
            admin_conn = f"host={host} port=5432 dbname={db} user=lfcadmin {tls}"
            path = os.path.join(T, "s.sql")
            with open(path, "w") as f:
                f.write(pg_batch(login, lpw, db, sc))
            p = tool(["psql", admin_conn, "-v", "ON_ERROR_STOP=1", "-qAt", "-f", path], env={"PGPASSWORD": admin_pw})
            if p.returncode:
                print(tail(p.stdout, 5))
                return 2
            p = tool(["psql", admin_conn, "-v", "ON_ERROR_STOP=1", "-qAt", "-c", "SHOW wal_level"], env={"PGPASSWORD": admin_pw})
            for line in p.stdout.splitlines():
                print(f"wal_level {line}")
            count = (f"SELECT '{login} reads ' || (SELECT count(*) FROM {sc}.intpk) || ' + ' || "
                     f"(SELECT count(*) FROM {sc}.dtix) || ' rows; replication ' || rolreplication "
                     "FROM pg_roles WHERE rolname = current_user")
            read = tool(["psql", f"host={host} port=5432 dbname={db} user={login} {tls}", "-qAt", "-c", count],
                        env={"PGPASSWORD": login_pw})
            print(read.stdout, end="")
            return read.returncode
        if engine == "mysql":
            def ad(sql_text: str | None = None, *args: str):
                return tool(["mysql", "-h", host, "-u", "lfcadmin", "--ssl-mode=REQUIRED", "-N", *args], env={"MYSQL_PWD": admin_pw},
                            stdin=sql_text, merge_stderr=sql_text is not None)
            p = ad(mysql_batch(login, lpw, db))
            if p.returncode:
                print(tail(p.stdout, 5))
                return 2
            p = ad(None, "-e", "SELECT CONCAT('binlog_row_image ', @@binlog_row_image, ', expire ', "
                               "@@binlog_expire_logs_seconds, ' s, invisible pk ', @@sql_generate_invisible_primary_key)")
            print(p.stdout, end="")
            read = tool(["mysql", "-h", host, "-u", login, "--ssl-mode=REQUIRED", "-N", db, "-e",
                         f"SELECT CONCAT('{login} reads ', (SELECT COUNT(*) FROM intpk), ' + ', (SELECT COUNT(*) FROM dtix), ' rows')"],
                        env={"MYSQL_PWD": login_pw})
            print(read.stdout, end="")
            return read.returncode
    print("pg or mysql")
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
