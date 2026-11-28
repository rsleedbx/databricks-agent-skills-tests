#!/usr/bin/env python3
"""ag-up.py <utility_script.sql>: two SQL Server 2022 containers as an Always On availability group with a readable
secondary (CLUSTER_TYPE = NONE, synchronous commit), a test database with keyed tables, the connection's login with
the same SID on both nodes, the Lakeflow Connect utility objects and CDC on the primary, and an Extended Events
session per node that records the gateway's statements and errors (read them with ag-sql.py <node> --xe).
Runs on a podman host (the source VM, or podman on a laptop). Prints the two endpoints at the end.

Environment (all optional)
  AG_NAME     name prefix for containers, network and AG (default lfc-ag): containers <AG_NAME>-p, <AG_NAME>-s
  AG_DIR      state directory (default $HOME/<AG_NAME>): data, certs, ag.env (passwords, mode 600)
  PORT_P, PORT_S   host ports of the primary and the secondary (default 1533, 1534)
  AG_DB       database (default lfcag); AG_TABLES  keyed tables to create (default "t1 t2 t3"); ROWS (default 3000)
  LFC_USER    the UC connection's login (default lfc_user)
  IMAGE       default mcr.microsoft.com/mssql/server:2022-latest; MEM_MB  memory limit per node (default 4096)
  PODMAN      container CLI (default podman; docker works too)
The container CLI runs sqlcmd inside the nodes; the scripts run on this host and use no workspace (no Databricks call).
"""
from __future__ import annotations

import os
import re
import secrets
import string
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from ag import Ag, valid_name
from podman_host import Fail

SQLCMD = "/opt/mssql-tools18/bin/sqlcmd"
ALNUM = string.ascii_letters + string.digits
USAGE = "usage: ag-up.py <utility_script.sql> (the Lakeflow Connect SQL Server utility objects script)"


def log(msg: str) -> None:
    print(f"{time.strftime('%H:%M:%S', time.gmtime())}Z {msg}", file=sys.stderr)


def password() -> str:
    """Lf + 20 random letters and digits + #9 (the shell's pw())."""
    return "Lf" + "".join(secrets.choice(ALNUM) for _ in range(20)) + "#9"


def make_env(path: str) -> None:
    """ag.env with three new passwords, mode 600, written only when there's none yet."""
    if os.path.exists(path):
        return
    text = f"SA_PW={password()}\nLFC_PW={password()}\nCERT_PW={password()}\n"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(text)


def db_script(db: str, tables: list[str], rows: str) -> str:
    """The database, its keyed tables with ROWS rows each, and a full backup (the AG seeds from it)."""
    lines = [f"IF DB_ID('{db}') IS NULL BEGIN CREATE DATABASE [{db}]; ALTER DATABASE [{db}] SET RECOVERY FULL; END", "GO",
             f"USE [{db}];"]
    for t in tables:
        lines.append(f"IF OBJECT_ID('dbo.{t}') IS NULL BEGIN CREATE TABLE dbo.{t} (id INT PRIMARY KEY, n INT NOT NULL, "
                     "s NVARCHAR(40) NULL);")
        lines.append(f"  INSERT dbo.{t} SELECT TOP ({rows}) ROW_NUMBER() OVER (ORDER BY (SELECT 1)), 1, 'row' "
                     "FROM sys.all_objects a CROSS JOIN sys.all_objects b; END")
    lines += ["GO", f"BACKUP DATABASE [{db}] TO DISK = '/var/opt/mssql/data/{db}.bak' WITH INIT, FORMAT;"]
    return "\n".join(lines) + "\n"


def host_address() -> str:
    try:
        words = subprocess.run(["hostname", "-I"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
                               check=False).stdout.split()
    except FileNotFoundError:
        words = []
    return words[0] if words else "<this host>"


class Setup:
    """The settings from the environment (env_get's defaults), then the steps of the set-up in order."""

    def __init__(self, ag: Ag, util: str):
        self.ag, self.util = ag, util
        self.n = ag.name
        self.d = ag.dir
        self.pp = env_get("PORT_P", "1533")
        self.ps = env_get("PORT_S", "1534")
        self.db = env_get("AG_DB", "lfcag")
        self.tables = env_get("AG_TABLES", "t1 t2 t3").split()
        self.rows = env_get("ROWS", "3000")
        self.user = env_get("LFC_USER", "lfc_user")
        self.image = env_get("IMAGE", "mcr.microsoft.com/mssql/server:2022-latest")
        self.mem = env_get("MEM_MB", "4096")
        self.secrets: dict = {}
        self.hp, self.hs = self.n.replace("-", "") + "p", self.n.replace("-", "") + "s"

    def sq(self, node: str, sql: str = "", db: str = "master", check: bool = True, echo_err: bool = True):
        """T-SQL on stdin as sa in a node (sqlcmd -b: an error is a nonzero exit). check: a failure raises; the
        node's sqlcmd errors are shown unless echo_err is off."""
        s = self.secrets
        args = [self.ag.pm, "exec", "-i", "-e", f"SQLCMDPASSWORD={s['SA_PW']}", f"{self.n}-{node}", SQLCMD, "-C", "-S",
                "localhost", "-U", "sa", "-d", db, "-b", "-h", "-1", "-W", "-v", f"CERT_PW={s['CERT_PW']}",
                f"LFC_PW={s['LFC_PW']}"]
        p = subprocess.run(args, input=sql, capture_output=True, text=True, check=False)
        if echo_err and p.stderr:
            print(p.stderr, end="", file=sys.stderr)
        if check and p.returncode != 0:
            raise Fail(f"sqlcmd on {self.n}-{node} failed: {(p.stderr or p.stdout).strip()[-400:]}", 1)
        return p

    def run(self) -> None:
        ag, n, d, pm = self.ag, self.n, self.d, self.ag.pm
        for sub in ("p", "s", "certs"):
            os.makedirs(os.path.join(d, sub), exist_ok=True)
        make_env(os.path.join(d, "ag.env"))
        self.secrets = ag.env()
        # the containers run as uid 10001 (mssql)
        paths = [os.path.join(d, x) for x in ("p", "s", "certs")]
        if ag.podman(["unshare", "chown", "-R", "10001:0", *paths], quiet=True).returncode != 0:
            subprocess.run(["chown", "-R", "10001:0", *paths], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                           check=False)

        if ag.podman(["network", "inspect", f"{n}-net"], quiet=True).returncode != 0:
            ag.podman(["network", "create", f"{n}-net"], quiet=True, check=True)
        for r in ("p", "s"):
            port = self.pp if r == "p" else self.ps
            # a container left "Created" by an earlier failed start (port taken) is removed and started again
            if ag.podman(["container", "inspect", f"{n}-{r}"], quiet=True).returncode == 0 and \
                    ag.podman(["container", "inspect", "-f", "{{.State.Status}}", f"{n}-{r}"],
                              stdout=subprocess.PIPE, check=False, text=True).stdout.strip() != "running":
                ag.podman(["rm", "-f", f"{n}-{r}"], quiet=True, check=True)
            if ag.podman(["container", "inspect", f"{n}-{r}"], quiet=True).returncode != 0:
                log(f"starting {n}-{r} on port {port}")
                args = ["run", "-d", "--name", f"{n}-{r}", "--hostname", f"{n.replace('-', '')}{r}", "--network",
                        f"{n}-net", "-p", f"{port}:1433", "--label", "lfc.purpose=ag-test", "--label", f"lfc.ag={n}",
                        "-e", "ACCEPT_EULA=Y", "-e", "MSSQL_PID=Developer", "-e", "MSSQL_ENABLE_HADR=1",
                        "-e", "MSSQL_AGENT_ENABLED=true", "-e", f"MSSQL_MEMORY_LIMIT_MB={self.mem}",
                        "-e", f"MSSQL_SA_PASSWORD={self.secrets['SA_PW']}", "-v", f"{d}/{r}:/var/opt/mssql",
                        "-v", f"{d}/certs:/var/opt/mssql/certs", self.image]
                if ag.podman(args, stdout=subprocess.DEVNULL).returncode != 0:
                    ag.podman(["rm", "-f", f"{n}-{r}"], quiet=True)
                    raise Fail(f"could not start {n}-{r}: is port {port} free? (PORT_P / PORT_S)", 1)

        for r in ("p", "s"):
            self.wait_ready(r)

        log("certificates and endpoints")
        if not os.path.exists(os.path.join(d, "certs", "ag_cert.cer")):
            self.sq("p", CERT_SQL)
        self.sq("s", CERT_SECONDARY_SQL)
        for r in ("p", "s"):
            self.sq(r, ENDPOINT_SQL)

        log(f"database {self.db} with tables: {' '.join(self.tables)}")
        self.sq("p", db_script(self.db, self.tables, self.rows), check=True, echo_err=True)

        log(f"availability group {n} (CLUSTER_TYPE = NONE), secondary readable")
        self.sq("p", ag_primary_sql(n, self.db, self.hp, self.hs), echo_err=True)
        self.sq("s", ag_secondary_sql(n))
        self.wait_synchronized()

        log(f"login {self.user} with the same SID on both nodes")
        sid_out = self.sq("p", login_sid_sql(self.user)).stdout
        sid = "".join(sid_out.split())
        self.sq("s", secondary_login_sql(self.user, sid))
        self.sq("p", user_sql(self.user), db=self.db)
        self.sq("p", user_sql(self.user))   # the user in master first: lakeflowFixPermissions grants server scope on the primary

        log("utility objects and CDC on the primary")
        # -x: the utility script is run as is (no sqlcmd $(var) substitution)
        with open(self.util, "rb") as f:
            script = f.read()
        p = subprocess.run([pm, "exec", "-i", "-e", f"SQLCMDPASSWORD={self.secrets['SA_PW']}", f"{n}-p", SQLCMD, "-C", "-S",
                            "localhost", "-U", "sa", "-d", self.db, "-b", "-x"], input=script, stdout=subprocess.DEVNULL,
                           check=False)
        if p.returncode != 0:
            raise Fail(f"utility script failed on {n}-p", p.returncode)
        csv = ",".join(f"dbo.{t}" for t in self.tables)
        out = self.sq("p", f"EXEC dbo.lakeflowSetupChangeDataCapture @Tables = '{csv}', @User = '{self.user}';\n"
                           f"EXEC dbo.lakeflowFixPermissions @User = '{self.user}', @Tables = '{csv}';\n",
                      db=self.db, check=False, echo_err=False)
        for line in out.stdout.splitlines():
            if re.search("✓|⚠|ERROR|RESULT", line):
                print(line, file=sys.stderr)
        # the AG copies the database, not master: the utility script's server-scoped grants are repeated on each node
        for r in ("p", "s"):
            self.sq(r, server_grants_sql(self.user))
        log(f"server-scoped grants for {self.user} in master on both nodes (the availability group doesn't copy master)")

        log("Extended Events session lfc_watch on both nodes (gateway statements and errors)")
        for r in ("p", "s"):
            self.sq(r, XE_SQL)

        host = host_address()
        print("addresses as seen on this host; the UC connection needs the address the workspace reaches "
              "(public IP or DNS name)")
        print(f"primary   {host},{self.pp}  (container {n}-p)")
        print(f"secondary {host},{self.ps}  (container {n}-s, readable)")
        print(f"login     {self.user}; passwords in {d}/ag.env; SQL:  AG_NAME={n} "
              f"{HERE}/ag-sql.py p|s [db] < file.sql")

    def wait_ready(self, r: str) -> None:
        end = time.monotonic() + 300
        while self.sq(r, "SELECT 1\n", check=False, echo_err=False).returncode != 0:
            if time.monotonic() >= end:
                raise Fail(f"{self.n}-{r} not ready after 300 s", 1)
            time.sleep(5)

    def wait_synchronized(self) -> None:
        end = time.monotonic() + 600
        q = ("SET NOCOUNT ON; SELECT synchronization_state_desc FROM sys.dm_hadr_database_replica_states "
             f"WHERE is_local = 1 AND database_id = DB_ID('{self.db}')\n")
        while "".join(self.sq("s", q, check=False, echo_err=False).stdout.split()) != "SYNCHRONIZED":
            if time.monotonic() >= end:
                raise Fail("secondary not SYNCHRONIZED after 600 s", 1)
            time.sleep(5)


def env_get(name: str, default: str) -> str:
    """The variable's value, or the default when it's unset or empty (the shell's ${VAR:-default})."""
    return os.environ.get(name) or default


CERT_SQL = """CREATE MASTER KEY ENCRYPTION BY PASSWORD = '$(CERT_PW)';
CREATE CERTIFICATE ag_cert WITH SUBJECT = 'lfc ag endpoint';
BACKUP CERTIFICATE ag_cert TO FILE = '/var/opt/mssql/certs/ag_cert.cer'
  WITH PRIVATE KEY (FILE = '/var/opt/mssql/certs/ag_cert.pvk', ENCRYPTION BY PASSWORD = '$(CERT_PW)');
"""

CERT_SECONDARY_SQL = """IF NOT EXISTS (SELECT 1 FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
  CREATE MASTER KEY ENCRYPTION BY PASSWORD = '$(CERT_PW)';
IF NOT EXISTS (SELECT 1 FROM sys.certificates WHERE name = 'ag_cert')
  CREATE CERTIFICATE ag_cert FROM FILE = '/var/opt/mssql/certs/ag_cert.cer'
    WITH PRIVATE KEY (FILE = '/var/opt/mssql/certs/ag_cert.pvk', DECRYPTION BY PASSWORD = '$(CERT_PW)');
"""

ENDPOINT_SQL = """IF NOT EXISTS (SELECT 1 FROM sys.endpoints WHERE name = 'hadr_endpoint')
  CREATE ENDPOINT hadr_endpoint STATE = STARTED AS TCP (LISTENER_PORT = 5022)
    FOR DATABASE_MIRRORING (ROLE = ALL, AUTHENTICATION = CERTIFICATE ag_cert, ENCRYPTION = REQUIRED ALGORITHM AES);
"""


def ag_primary_sql(n: str, db: str, hp: str, hs: str) -> str:
    return f"""IF NOT EXISTS (SELECT 1 FROM sys.availability_groups WHERE name = '{n}')
  CREATE AVAILABILITY GROUP [{n}] WITH (CLUSTER_TYPE = NONE) FOR DATABASE [{db}] REPLICA ON
    N'{hp}' WITH (ENDPOINT_URL = N'tcp://{hp}:5022', AVAILABILITY_MODE = SYNCHRONOUS_COMMIT, FAILOVER_MODE = MANUAL,
      SEEDING_MODE = AUTOMATIC, SECONDARY_ROLE (ALLOW_CONNECTIONS = ALL)),
    N'{hs}' WITH (ENDPOINT_URL = N'tcp://{hs}:5022', AVAILABILITY_MODE = SYNCHRONOUS_COMMIT, FAILOVER_MODE = MANUAL,
      SEEDING_MODE = AUTOMATIC, SECONDARY_ROLE (ALLOW_CONNECTIONS = ALL));
"""


def ag_secondary_sql(n: str) -> str:
    return f"""IF NOT EXISTS (SELECT 1 FROM sys.availability_groups WHERE name = '{n}') BEGIN
  ALTER AVAILABILITY GROUP [{n}] JOIN WITH (CLUSTER_TYPE = NONE);
  ALTER AVAILABILITY GROUP [{n}] GRANT CREATE ANY DATABASE; END
"""


def login_sid_sql(user: str) -> str:
    return (f"SET NOCOUNT ON;\nIF SUSER_ID('{user}') IS NULL CREATE LOGIN [{user}] WITH PASSWORD = '$(LFC_PW)', "
            f"CHECK_POLICY = OFF;\nSELECT CONVERT(varchar(200), SUSER_SID('{user}'), 1);\n")


def secondary_login_sql(user: str, sid: str) -> str:
    return (f"IF SUSER_ID('{user}') IS NULL CREATE LOGIN [{user}] WITH PASSWORD = '$(LFC_PW)', SID = {sid}, "
            "CHECK_POLICY = OFF;\n")


def user_sql(user: str) -> str:
    return f"IF USER_ID('{user}') IS NULL CREATE USER [{user}] FOR LOGIN [{user}];\n"


def server_grants_sql(user: str) -> str:
    return (f"IF USER_ID('{user}') IS NULL CREATE USER [{user}] FOR LOGIN [{user}];\n"
            f"GRANT SELECT ON sys.change_tracking_databases TO [{user}];\n"
            f"GRANT EXECUTE ON sys.sp_tables TO [{user}]; GRANT EXECUTE ON sys.sp_columns_100 TO [{user}];\n"
            f"GRANT EXECUTE ON sys.sp_pkeys TO [{user}]; GRANT EXECUTE ON sys.sp_statistics_100 TO [{user}];\n")


XE_SQL = """IF NOT EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = 'lfc_watch') BEGIN
  CREATE EVENT SESSION lfc_watch ON SERVER
    ADD EVENT sqlserver.error_reported (ACTION (sqlserver.client_app_name, sqlserver.sql_text)
      WHERE sqlserver.like_i_sql_unicode_string(sqlserver.client_app_name, N'lakeflow-connect%')),
    ADD EVENT sqlserver.rpc_completed (ACTION (sqlserver.client_app_name)
      WHERE sqlserver.like_i_sql_unicode_string(sqlserver.client_app_name, N'lakeflow-connect%')),
    ADD EVENT sqlserver.sql_batch_completed (ACTION (sqlserver.client_app_name)
      WHERE sqlserver.like_i_sql_unicode_string(sqlserver.client_app_name, N'lakeflow-connect%'))
    ADD TARGET package0.event_file (SET filename = N'/var/opt/mssql/log/lfc_watch.xel', max_file_size = 64)
    WITH (STARTUP_STATE = ON);
  ALTER EVENT SESSION lfc_watch ON SERVER STATE = START; END
"""


def main(argv: list[str]) -> int:
    if not argv:
        print(USAGE, file=sys.stderr)
        return 1
    util = argv[0]
    if not os.path.isfile(util):
        raise Fail(f"no file {util}", 2)
    ag = Ag()
    names = ag.name + env_get("AG_DB", "lfcag") + env_get("LFC_USER", "lfc_user") + env_get("AG_TABLES", "t1 t2 t3").replace(" ", "")
    if not valid_name(names):
        raise Fail("names: letters, digits, _ and - only", 2)
    Setup(ag, util).run()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except Fail as e:
        print(e.msg, file=sys.stderr)
        sys.exit(e.rc)
    except FileNotFoundError as e:   # the container CLI isn't installed
        print(f"{e.filename}: not found", file=sys.stderr)
        sys.exit(127)
    except subprocess.CalledProcessError as e:   # a container CLI call that had to work didn't
        print(f"{' '.join(str(x) for x in e.cmd[:3])} ... failed ({e.returncode})", file=sys.stderr)
        sys.exit(e.returncode or 1)
