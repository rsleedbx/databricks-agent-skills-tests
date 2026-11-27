"""The database engines a source VM runs, one record each: the image and how to start it, the admin's SQL client, the
probe that says it answers, the Lakeflow Connect login (lfc_user), a new database's admin env file and how its admin
password changes. The commands that run inside a container stay shell words for that container's own shell."""
from __future__ import annotations

from dataclasses import dataclass

YB_IP = "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"   # podman inspect: the container's address
SQLCMD = "/opt/mssql-tools18/bin/sqlcmd"


@dataclass(frozen=True)
class Engine:
    name: str
    image: str
    mnt: str                                   # the data path in the container
    cport: int                                 # the port in the container
    args: tuple[str, ...] = ()                 # the image's command
    runopts: tuple[str, ...] = ()              # podman run options
    service: str = ""                          # Oracle: the PDB lfc_user lives in
    dbname: str = ""                           # the database a test uses (the up JSON's "database")
    wait: int = 180                            # seconds to answer after a start
    volopt: str = "Z"                          # the data mount's options
    admin_key: str = ""                        # the env file's admin password key; "" = insecure, none
    env_new: tuple[str, ...] = ()              # a new database's env file; {pw} is the admin password
    data_owner: str = ""                       # uid:gid of a new, empty data folder
    restart_after_rotate: bool = False         # its admin helpers read the container's env: start it again
    takes_database: bool = True                # sql <name> [database]


ENGINES: dict[str, Engine] = {e.name: e for e in (
    Engine("postgres", "docker.io/library/postgres:16", "/var/lib/postgresql/data", 5432,
           args=("-c", "wal_level=logical", "-c", "max_replication_slots=10", "-c", "max_wal_senders=10"),
           runopts=("--shm-size", "1g"), admin_key="POSTGRES_PASSWORD", env_new=("POSTGRES_PASSWORD={pw}",)),
    Engine("mysql", "docker.io/library/mysql:8.4", "/var/lib/mysql", 3306,
           args=("--binlog-format=ROW", "--binlog-row-image=FULL", "--binlog-expire-logs-seconds=604800"),
           admin_key="MYSQL_ROOT_PASSWORD", env_new=("MYSQL_ROOT_PASSWORD={pw}",), restart_after_rotate=True),
    Engine("sqlserver", "mcr.microsoft.com/mssql/server:2022-latest", "/var/opt/mssql", 1433,
           runopts=("--memory", "6g", "-e", "MSSQL_MEMORY_LIMIT_MB=5120", "-e", "MSSQL_AGENT_ENABLED=true",
                    "-e", "MSSQL_PID=Developer"),
           admin_key="MSSQL_SA_PASSWORD", env_new=("ACCEPT_EULA=Y", "MSSQL_SA_PASSWORD={pw}"),
           data_owner="10001:0", restart_after_rotate=True),          # SQL Server runs as mssql (uid 10001)
    # Oracle: the CDB's FREE and the PDB FREEPDB1 (gvenzl); lfc_user is PDB-local, so it connects to FREEPDB1. The first
    # start creates the database (minutes); the data folder belongs to the image's oracle user (uid 54321)
    Engine("oracle", "docker.io/gvenzl/oracle-free:23-slim", "/opt/oracle/oradata", 1521, runopts=("--shm-size", "3g"),
           service="FREEPDB1", wait=600, admin_key="ORACLE_PASSWORD", env_new=("ORACLE_PASSWORD={pw}",),
           data_owner="54321:54321", takes_database=False),
    # MariaDB: no binary log in its image, so it's turned on for the CDC readers (ROW, FULL image, a week kept)
    Engine("mariadb", "docker.io/library/mariadb:11.4", "/var/lib/mysql", 3306,
           args=("--log-bin", "--server-id=1", "--binlog-format=ROW", "--binlog-row-image=FULL",
                 "--binlog-expire-logs-seconds=604800"),
           admin_key="MARIADB_ROOT_PASSWORD", env_new=("MARIADB_ROOT_PASSWORD={pw}",), restart_after_rotate=True),
    # Db2 Community: --privileged; db2_db makes LFCDB after the image's setup; lfc_user is an OS user (Db2 checks the
    # OS password); the image reads DB2INST1_PASSWORD at start
    Engine("db2", "icr.io/db2_community/db2:11.5.9.0", "/database", 50000, runopts=("--privileged",), dbname="LFCDB",
           wait=900, admin_key="DB2INST1_PASSWORD",
           env_new=("LICENSE=accept", "DB2INSTANCE=db2inst1", "DB2INST1_PASSWORD={pw}", "BLU=false",
                    "ENABLE_ORACLE_COMPATIBILITY=false", "PERSISTENT_HOME=true", "ARCHIVE_LOGS=false",
                    "AUTOCONFIG=false", "SAMPLEDB=false"),
           restart_after_rotate=True, takes_database=False),
    # Informix Developer: the server writes its data and config volumes as informix (:U), the config one a folder under
    # the database's; lfc_user is an OS user too (the server authenticates with PAM)
    Engine("informix", "docker.io/ibmcom/informix-developer-database:14.10.FC7W1DE", "/opt/ibm/data", 9088,
           dbname="lfcdb", wait=600, volopt="U,Z", admin_key="INFORMIX_PASSWORD",
           env_new=("LICENSE=accept", "INFORMIX_PASSWORD={pw}")),
    # CockroachDB: one insecure node (no passwords: the stored one isn't checked); cache and SQL memory 2 GiB each
    Engine("cockroachdb", "docker.io/cockroachdb/cockroach:v25.4.13", "/cockroach/cockroach-data", 26257,
           args=("start-single-node", "--insecure", "--cache=2GiB", "--max-sql-memory=2GiB"), dbname="lfc"),
    # YugabyteDB: insecure yugabyted in the foreground; its data folder belongs to uid 10001 (group users). The advertise
    # address is stored in the data (yb-master's current_masters), so every start of a copy uses the one the master was
    # built with: 127.0.0.1. YSQL follows it to loopback only, so the published port (and ysqlsh on the container's
    # address) needs pgsql_proxy_bind_address on all interfaces
    Engine("yugabytedb", "docker.io/yugabytedb/yugabyte:2026.1.2.0-b137", "/yb", 5433,
           args=("bin/yugabyted", "start", "--base_dir=/yb", "--advertise_address=127.0.0.1",
                 "--tserver_flags=pgsql_proxy_bind_address=0.0.0.0:5433", "--background=false"),
           dbname="lfc", wait=300, data_owner="10001:100"),
)}

EDITIONS = ("Developer", "Express", "Standard", "Enterprise", "Evaluation", "EnterpriseCore")   # SQL Server's MSSQL_PID


def get(name: str) -> Engine:
    if name not in ENGINES:
        raise KeyError(f"engine is {', '.join(list(ENGINES)[:-1])} or {list(ENGINES)[-1]}: {name}")
    return ENGINES[name]


def new_env(e: Engine, pw: str) -> str:
    """A new, empty database's env file (insecure engines: empty)."""
    return "".join(line.format(pw=pw) + "\n" for line in e.env_new)


def admin_cmd(e: Engine, ip: str = "") -> tuple[list[str], list[str]]:
    """(the words after `podman exec -i ... <container>`, the exec's options) for stdin as the engine's admin
    (Db2: connected to LFCDB; Informix: in sysmaster). ip: the container's address (YugabyteDB)."""
    n = e.name
    if n == "postgres":
        return ["psql", "-v", "ON_ERROR_STOP=1", "-q", "-U", "postgres", "-d", "postgres"], \
            ["-e", "PGOPTIONS=-c client_min_messages=warning"]
    if n in ("mysql", "mariadb"):
        key, cli = ("MYSQL_ROOT_PASSWORD", "mysql") if n == "mysql" else ("MARIADB_ROOT_PASSWORD", "mariadb")
        return ["sh", "-c", f'MYSQL_PWD="${key}" {cli} -uroot'], []
    if n == "sqlserver":
        return ["sh", "-c", f'f=$(mktemp); cat > $f; {SQLCMD} -C -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -b -i $f; '
                            'r=$?; rm -f $f; exit $r'], []
    if n == "oracle":
        return ["sqlplus", "-s", "-L", "/", "as", "sysdba"], []
    if n == "db2":
        return ["bash", "-c", 'f=$(mktemp); chmod 644 "$f"; { echo "connect to LFCDB;"; cat; } > "$f"; '
                              'su - db2inst1 -c "db2 -x -t -s -f $f"; r=$?; rm -f "$f"; [ $r -lt 4 ]'], []
    if n == "informix":     # the container's user is informix; dbaccess is on its login PATH
        return ["bash", "-lc", "dbaccess sysmaster -"], []
    if n == "cockroachdb":
        return ["/cockroach/cockroach", "sql", "--insecure", "--format=tsv"], []
    return ["bin/ysqlsh", "-h", ip, "-U", "yugabyte", "-v", "ON_ERROR_STOP=1", "-q", "-At"], []


def admin_input(e: Engine, sql: str) -> str:
    """The admin client's stdin for sql (Oracle: stop at the first error, and leave)."""
    if e.name == "oracle":
        return "whenever sqlerror exit failure\n" + sql + ("" if sql.endswith("\n") or not sql else "\n") + "exit\n"
    return sql


def table_cmd(e: Engine, database: str, wide: bool, ip: str = "") -> list[str] | None:
    """`sql <name> [database]`: the admin's client in database, rows tab-separated without headers, stopping at the
    first error. None for the engines that take no database (Oracle, Db2: admin_cmd)."""
    d, n = database, e.name
    if n == "postgres":
        return ["psql", "-U", "postgres", "-d", d or "postgres", "-v", "ON_ERROR_STOP=1", "-qtAX", "-F", "\t"]
    if n == "sqlserver":
        fl = "-b -y 0" if wide else "-b -W -h -1"
        return ["sh", "-c", f'f=$(mktemp); cat > $f; {SQLCMD} -C -S localhost -U sa -P "$MSSQL_SA_PASSWORD" {fl} '
                            f'-d {d or "master"} -i $f; r=$?; rm -f $f; exit $r']
    if n in ("mysql", "mariadb"):
        key, cli = ("MYSQL_ROOT_PASSWORD", "mysql") if n == "mysql" else ("MARIADB_ROOT_PASSWORD", "mariadb")
        return ["sh", "-c", f'MYSQL_PWD="${key}" {cli} -uroot -N -B {d}'.rstrip()]
    if n == "informix":
        return ["bash", "-lc", f"dbaccess {d or 'sysmaster'} -"]
    if n == "cockroachdb":
        return ["/cockroach/cockroach", "sql", "--insecure", "--format=tsv"] + (["-d", d] if d else [])
    if n == "yugabytedb":
        return ["bin/ysqlsh", "-h", ip, "-U", "yugabyte", "-d", d or "yugabyte", "-v", "ON_ERROR_STOP=1", "-qtAX",
                "-F", "\t"]
    return None


def probe(e: Engine) -> str:
    """The query that says the engine answers (Oracle: once its PDB is open too)."""
    return {"oracle": "ALTER SESSION SET CONTAINER = FREEPDB1;\nSELECT 1 FROM dual;\n", "db2": "VALUES 1;\n",
            "informix": "SELECT 1 FROM sysmaster:sysdual;\n"}.get(e.name, "SELECT 1;\n")


READY_LOG = {"oracle": "DATABASE IS READY TO USE",     # the image restarts the database after its setup, then says so
             "db2": "(*) Setup has completed"}         # Db2's setup (instance, then the database) runs after the start

# LFCDB with 4 KB-aligned I/O. A data disk with 4096-byte logical sectors (Azure Premium SSD v2's default) refuses Db2's
# 512-byte direct-I/O writes (SQL0293N on CREATE DATABASE), so the image doesn't make the database. Every start:
# DB2_4K_DEVICE_SUPPORT=ON if it isn't (the instance restarts once to read it), then LFCDB if it's missing.
DB2_DB = ('db2set -all 2>/dev/null | grep -q "DB2_4K_DEVICE_SUPPORT=ON" ||\n'
          '      { db2set DB2_4K_DEVICE_SUPPORT=ON && { db2stop force; db2start; } >/dev/null 2>&1; } || exit 1\n'
          '    db2 list db directory 2>/dev/null | grep -q "= LFCDB$" || db2 create database LFCDB >/dev/null')

OS_USER = 'read l; id -u lfc_user >/dev/null 2>&1 || useradd -m lfc_user; echo "$l" | chpasswd'


def login_sql(e: Engine, pw: str) -> str:
    """The Lakeflow Connect login with replication rights (databases and CDC are per test). Idempotent."""
    p = pw.replace("'", "''")
    pg = ("DO $$BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='lfc_user') THEN CREATE ROLE lfc_user LOGIN "
          f"REPLICATION; END IF; END$$;\nALTER ROLE lfc_user WITH LOGIN REPLICATION PASSWORD '{p}';\n"
          "GRANT pg_read_all_data TO lfc_user;\n")
    grants = ("GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO "
              "'lfc_user'@'%';\n")
    n = e.name
    if n == "postgres":
        return pg
    if n == "mysql":
        return (f"CREATE USER IF NOT EXISTS 'lfc_user'@'%' IDENTIFIED WITH caching_sha2_password BY '{p}';\n"
                f"ALTER USER 'lfc_user'@'%' IDENTIFIED WITH caching_sha2_password BY '{p}';\n" + grants)
    if n == "mariadb":      # no caching_sha2_password: the user gets its default plugin (mysql_native_password)
        return (f"CREATE USER IF NOT EXISTS 'lfc_user'@'%' IDENTIFIED BY '{p}';\n"
                f"ALTER USER 'lfc_user'@'%' IDENTIFIED BY '{p}';\n" + grants)
    if n == "oracle":       # a PDB-local user with the read rights the other logins have (CDC's C## user is separate)
        return ("ALTER SESSION SET CONTAINER = FREEPDB1;\nDECLARE n NUMBER; BEGIN SELECT COUNT(*) INTO n FROM dba_users "
                "WHERE username = 'LFC_USER'; IF n = 0 THEN EXECUTE IMMEDIATE 'CREATE USER lfc_user IDENTIFIED BY "
                f"\"{p}\" DEFAULT TABLESPACE users QUOTA UNLIMITED ON users'; ELSE EXECUTE IMMEDIATE 'ALTER USER "
                f"lfc_user IDENTIFIED BY \"{p}\"'; END IF; END;\n/\n"
                "GRANT CREATE SESSION, SELECT ANY TABLE, SELECT_CATALOG_ROLE TO lfc_user;\n")
    if n == "sqlserver":    # the login, then a user in every user database with the read rights
        return (f"IF SUSER_ID('lfc_user') IS NULL CREATE LOGIN [lfc_user] WITH PASSWORD = N'{p}', CHECK_POLICY = OFF; "
                f"ELSE ALTER LOGIN [lfc_user] WITH PASSWORD = N'{p}', CHECK_POLICY = OFF;\n"
                "DECLARE @d sysname, @s nvarchar(max); DECLARE c CURSOR LOCAL FOR SELECT name FROM sys.databases WHERE "
                "database_id > 4 AND state_desc = 'ONLINE'; OPEN c; FETCH c INTO @d; WHILE @@FETCH_STATUS = 0 BEGIN "
                "SET @s = N'USE ' + QUOTENAME(@d) + N'; IF USER_ID(''lfc_user'') IS NULL CREATE USER [lfc_user] FOR "
                "LOGIN [lfc_user]; ALTER ROLE db_datareader ADD MEMBER [lfc_user]; GRANT VIEW DATABASE STATE, VIEW "
                "DEFINITION TO [lfc_user];'; EXEC (@s); FETCH c INTO @d; END;\n")
    if n == "db2":          # lfc-source-setup's engine-db2.md grant (the login is an OS user: prep made it)
        return "GRANT DBADM ON DATABASE TO USER lfc_user;\n"
    if n == "informix":
        return "DATABASE lfcdb;\nGRANT CONNECT TO lfc_user;\nGRANT RESOURCE TO lfc_user;\nGRANT DBA TO lfc_user;\n"
    if n == "cockroachdb":  # insecure: no password; read rights on the tables now and later
        return ("CREATE DATABASE IF NOT EXISTS lfc;\nCREATE USER IF NOT EXISTS lfc_user;\n"
                "GRANT ALL ON DATABASE lfc TO lfc_user;\nUSE lfc;\nGRANT SELECT ON ALL TABLES IN SCHEMA public TO lfc_user;\n"
                "ALTER DEFAULT PRIVILEGES FOR ROLE root IN SCHEMA public GRANT SELECT ON TABLES TO lfc_user;\n")
    return pg + "SELECT 'CREATE DATABASE lfc' WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'lfc')\\gexec\n"
