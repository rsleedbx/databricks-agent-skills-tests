#!/usr/bin/env bash
# slot.sh [--owner <owner>] <command> [args]: test databases for several people on one VM, one container each, with the
# data on JuiceFS (object storage) so a stopped database costs no disk or CPU. Run as root on the VM.
#   masters                                   the masters a database can start from ($ROOT/masters)
#   ls [--all]                                your databases (--all: everyone's): engine, state, port, created, last up
#   up <engine> <name> [--from <master> | --empty]
#                                             start <name>: resume it if it exists, else clone a master (fs_clone:
#                                             juicefs clone or an XFS reflink) or start empty. Engines: postgres, mysql,
#                                             sqlserver, oracle, mariadb, db2, informix, cockroachdb, yugabytedb.
#                                             Parks your other running database first when you're at MAX_PER_OWNER.
#                                             Prints one JSON line: name, engine, host, port, user, password, container,
#                                             state, and service (oracle: the PDB lfc_user lives in) and database (the
#                                             database a test uses, where the engine has one)
#   park <name>                               stop it; the data stays on JuiceFS
#   clone <name> <new name>                   copy your database <name> to a new parked database <new name> (up starts
#                                             it, with its own port and lfc_user password). JuiceFS: juicefs clone;
#                                             XFS: cp --reflink=always; both share blocks, so it takes seconds. A
#                                             running <name> is stopped for the copy and started again after
#   rm <name>                                 stop it and delete its data
#   usage [--days <n>] [--all]                hours running per owner in the last n days (default 7), from the ledger
#   sql <name> [database] [--wide]            run SQL from stdin as the engine's admin in your running database <name>
#                                             (CDC, change tracking, test tables): tab-separated rows, no headers; the
#                                             exit status is the SQL's. --wide: SQL Server prints long values whole
#                                             (sqlcmd -y 0). Oracle and Db2 take no database (Oracle: ALTER SESSION SET
#                                             CONTAINER in the SQL; Db2: LFCDB)
#   info <name>                               the host OS, Podman, the image and digest, and the VM (size, region, zone)
# --owner: who the database belongs to. A forced SSH command sets it per key (see the skill), so a user can't pick
# another owner; as root without --owner the owner is "admin".
# Settings, from /etc/lfc-slot.env if present: HOST (address printed for connections; set it on Azure, see host()),
# ROOT (/mnt/jfs), STATE (/var/lib/lfc/slots), PORT_MIN/PORT_MAX (30000/30063), MAX_RUNNING (vCPUs / CPUS_PER_DB, at least 1), CPUS_PER_DB (2; 1 on a personal VM),
# MAX_PER_OWNER (1; 0 = no limit, for a personal VM), SECRETS (/root/lfc-src: the admin env files tpc-master.sh used),
# LOCK_WAIT (600 s: how long a command waits for another to finish), LOCK (the lock file for one command at a time; default $STATE/.lock: keep it on a local disk when STATE is on a
# JuiceFS volume, where flock isn't checked here). STATE on /mnt/jfs needs that volume mounted, or the command stops.
set -euo pipefail
[ -r /etc/lfc-slot.env ] && . /etc/lfc-slot.env
ROOT=${ROOT:-/mnt/jfs} STATE=${STATE:-/var/lib/lfc/slots} SECRETS=${SECRETS:-/root/lfc-src}
PORT_MIN=${PORT_MIN:-30000} PORT_MAX=${PORT_MAX:-30063} MAX_PER_OWNER=${MAX_PER_OWNER:-1}
CPUS_PER_DB=${CPUS_PER_DB:-2}
MAX_RUNNING=${MAX_RUNNING:-$(( $(nproc) / CPUS_PER_DB ))}; [ "$MAX_RUNNING" -ge 1 ] || MAX_RUNNING=1
LOCK=${LOCK:-$STATE/.lock}
lock() {   # one command at a time; LOCK_WAIT (600 s) bounds the wait behind another command instead of hanging with it
  flock -w "${LOCK_WAIT:-600}" 9 || die "another slot command still holds $LOCK after ${LOCK_WAIT:-600} s: see it with pgrep -af lfc-slot"
}
usage() { sed -n '2,/^[^#]/p' "$0" | sed '$d' >&2; exit 2; }
die() { echo "slot: $*" >&2; exit 1; }
OWNER="admin"
if [ "${1:-}" = --owner ]; then OWNER=${2:-}; shift 2; fi
[[ $OWNER =~ ^[a-z0-9][a-z0-9-]{0,31}$ ]] || die "bad owner: $OWNER"
[ $# -ge 1 ] || usage
CMD=$1; shift
case $STATE in "$ROOT"/*) mountpoint -q "$ROOT" || die "$ROOT isn't mounted: the state is on it" ;; esac
install -d -m 700 "$STATE" "$STATE/$OWNER" "$(dirname "$LOCK")"
LEDGER=$STATE/ledger.jsonl
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
log() { printf '{"ts":"%s","owner":"%s","name":"%s","event":"%s"}\n' "$(now)" "$OWNER" "$1" "$2" >> "$LEDGER"; }
name_ok() { [[ ${1:-} =~ ^[a-z0-9][a-z0-9-]{0,31}$ ]] || die "bad name: ${1:-} (lowercase letters, digits, -)"; }
cname() { echo "lfc-slot-$1-$2"; }                 # owner, name
meta() { echo "$STATE/$1/$2.json"; }
running() { [ "$(podman inspect -f '{{.State.Running}}' "$(cname "$1" "$2")" 2>/dev/null)" = true ]; }
host() {   # HOST, else the cloud metadata's public IP (Azure leaves it empty for a Standard SKU IP), else the first local IP
  local h=${HOST:-}
  [ -n "$h" ] || h=$(curl -fs -m 5 -H Metadata:true "http://169.254.169.254/metadata/instance/network/interface/0/ipv4/ipAddress/0/publicIpAddress?api-version=2021-02-01&format=text" 2>/dev/null || true)
  [ -n "$h" ] || h=$(curl -fs -m 5 http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || true)
  [ -n "$h" ] || h=$(hostname -I | awk '{print $1}')
  echo "$h"; }
pw() { echo "Lfc1$(openssl rand -hex 12)"; }

# engine: image, data path in the container, port in the container, and what its start needs. SERVICE: the Oracle
# service lfc_user connects to; DBNAME: the database a test uses (in the JSON); WAIT: seconds to answer after start;
# VOLOPT: the data mount's options
engine() {
  SERVICE="" DBNAME="" WAIT=180 VOLOPT=Z
  case $1 in
    postgres) IMAGE=docker.io/library/postgres:16 MNT=/var/lib/postgresql/data CPORT=5432
              ARGS=(-c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10) RUNOPTS=(--shm-size 1g) ;;
    mysql)    IMAGE=docker.io/library/mysql:8.4 MNT=/var/lib/mysql CPORT=3306
              ARGS=(--binlog-format=ROW --binlog-row-image=FULL --binlog-expire-logs-seconds=604800) RUNOPTS=() ;;
    sqlserver) IMAGE=mcr.microsoft.com/mssql/server:2022-latest MNT=/var/opt/mssql CPORT=1433
              ARGS=() RUNOPTS=(--memory 6g -e MSSQL_MEMORY_LIMIT_MB=5120 -e MSSQL_AGENT_ENABLED=true -e MSSQL_PID=Developer) ;;
    # Oracle: the CDB's FREE and the PDB FREEPDB1 (gvenzl); lfc_user is PDB-local, so it connects to FREEPDB1. First start
    # creates the database (minutes); the data dir is owned by the image's oracle user (uid 54321, set in up)
    oracle)   IMAGE=docker.io/gvenzl/oracle-free:23-slim MNT=/opt/oracle/oradata CPORT=1521 SERVICE=FREEPDB1 WAIT=600
              ARGS=() RUNOPTS=(--shm-size 3g) ;;
    # MariaDB: no binary log in its image, so turn it on for the CDC readers (ROW, FULL image, a week kept)
    mariadb)  IMAGE=docker.io/library/mariadb:11.4 MNT=/var/lib/mysql CPORT=3306
              ARGS=(--log-bin --server-id=1 --binlog-format=ROW --binlog-row-image=FULL --binlog-expire-logs-seconds=604800)
              RUNOPTS=() ;;
    # Db2 Community: --privileged; db2_db makes LFCDB after the image's setup; lfc_user is an OS user (Db2 checks the OS password)
    db2)      IMAGE=icr.io/db2_community/db2:11.5.9.0 MNT=/database CPORT=50000 DBNAME=LFCDB WAIT=900
              ARGS=() RUNOPTS=(--privileged) ;;
    # Informix Developer: the server writes its data and config volumes as informix (:U), the config one a folder under the
    # database's; lfc_user is an OS user too (the server authenticates with PAM)
    informix) IMAGE=docker.io/ibmcom/informix-developer-database:14.10.FC7W1DE MNT=/opt/ibm/data CPORT=9088 DBNAME=lfcdb WAIT=600
              VOLOPT=U,Z ARGS=() RUNOPTS=() ;;
    # CockroachDB: one insecure node (no passwords: the stored one isn't checked); cache and SQL memory 2 GiB each
    cockroachdb) IMAGE=docker.io/cockroachdb/cockroach:v25.4.13 MNT=/cockroach/cockroach-data CPORT=26257 DBNAME=lfc
              ARGS=(start-single-node --insecure --cache=2GiB --max-sql-memory=2GiB) RUNOPTS=() ;;
    # YugabyteDB: insecure yugabyted in the foreground; its data dir belongs to uid 10001 (set in up). The advertise
    # address is stored in the data (yb-master's current_masters), so every start of a copy must use the one the master
    # was built with: 127.0.0.1, in every container. YSQL follows the advertise address to loopback only, so the
    # published port (and ysqlsh on the container IP) needs pgsql_proxy_bind_address on all interfaces
    yugabytedb) IMAGE=docker.io/yugabytedb/yugabyte:2026.1.2.0-b137 MNT=/yb CPORT=5433 DBNAME=lfc WAIT=300
              ARGS=(bin/yugabyted start --base_dir=/yb --advertise_address=127.0.0.1 --tserver_flags=pgsql_proxy_bind_address=0.0.0.0:5433 --background=false) RUNOPTS=() ;;
    *) die "engine is postgres, mysql, sqlserver, oracle, mariadb, db2, informix, cockroachdb or yugabytedb: $1" ;;
  esac
}
# sql <container> <engine>: run stdin as the engine's admin (Db2: connected to LFCDB; Informix: in sysmaster)
sql() {   # PROBE_TIMEOUT=<s>: give up on one call after that long (the readiness probe sets it; real work doesn't)
  local t=(); [ -n "${PROBE_TIMEOUT:-}" ] && t=(timeout "$PROBE_TIMEOUT")
  {   # fd 9 is the lock: an exec (and its conmon) must not hold it after this command ends
  case $2 in
    postgres) "${t[@]}" podman exec -i -e PGOPTIONS="-c client_min_messages=warning" "$1" psql -v ON_ERROR_STOP=1 -q -U postgres -d postgres ;;
    mysql) "${t[@]}" podman exec -i "$1" sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot' ;;
    sqlserver) "${t[@]}" podman exec -i "$1" sh -c 'f=$(mktemp); cat > $f; /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -b -i $f; r=$?; rm -f $f; exit $r' ;;
    oracle) { echo "whenever sqlerror exit failure"; cat; echo "exit"; } | "${t[@]}" podman exec -i "$1" sqlplus -s -L / as sysdba ;;
    mariadb) "${t[@]}" podman exec -i "$1" sh -c 'MYSQL_PWD="$MARIADB_ROOT_PASSWORD" mariadb -uroot' ;;
    db2) "${t[@]}" podman exec -i "$1" bash -c 'f=$(mktemp); chmod 644 "$f"; { echo "connect to LFCDB;"; cat; } > "$f"; su - db2inst1 -c "db2 -x -t -s -f $f"; r=$?; rm -f "$f"; [ $r -lt 4 ]' ;;
    informix) "${t[@]}" podman exec -i "$1" bash -lc 'dbaccess sysmaster -' ;;   # the container's user is informix; dbaccess is on its login PATH
    cockroachdb) "${t[@]}" podman exec -i "$1" /cockroach/cockroach sql --insecure --format=tsv ;;
    yugabytedb) ip=$(podman inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$1")
                "${t[@]}" podman exec -i "$1" bin/ysqlsh -h "$ip" -U yugabyte -v ON_ERROR_STOP=1 -q -At ;;
  esac
  } 9>&-
}
probe() {   # the query that says the engine answers (Oracle: once its PDB is open too)
  case $1 in
    oracle) printf 'ALTER SESSION SET CONTAINER = FREEPDB1;\nSELECT 1 FROM dual;\n' ;;
    db2) echo "VALUES 1;" ;;
    informix) echo "SELECT 1 FROM sysmaster:sysdual;" ;;
    *) echo "SELECT 1;" ;;
  esac
}
# db2_db <container>: LFCDB with 4 KB-aligned I/O. A data disk with 4096-byte logical sectors (Azure Premium SSD v2's
# default) refuses Db2's 512-byte direct-I/O writes (pwrite EINVAL: SQL0293N on CREATE DATABASE), so the image doesn't
# make the database (no DBNAME): this sets DB2_4K_DEVICE_SUPPORT=ON (kept in the instance home), restarts the instance
# once and creates it. An existing LFCDB (a clone, or a slot made before) is left as it is.
db2_db() {
  podman exec "$1" su - db2inst1 -c 'db2 list db directory 2>/dev/null | grep -q "= LFCDB$" && exit 0
    db2set DB2_4K_DEVICE_SUPPORT=ON && { db2stop force; db2start; } >/dev/null 2>&1; db2 create database LFCDB >/dev/null' 9>&-
}
ready() {
  # grep -c reads all of the log: with pipefail, grep -q's early exit would make podman logs fail (SIGPIPE)
  if [ "$2" = oracle ]; then   # the image restarts the database after its first-start setup and then prints this line
    [ "$(podman logs "$1" 2>&1 | grep -c 'DATABASE IS READY TO USE')" -gt 0 ] || return 1
  elif [ "$2" = db2 ]; then    # Db2's setup (instance, then the database) runs after the start; the image logs its end
    [ "$(podman logs "$1" 2>&1 | grep -cF '(*) Setup has completed')" -gt 0 ] || return 1
    db2_db "$1" >/dev/null 2>&1 || return 1
  fi
  probe "$2" | PROBE_TIMEOUT=30 sql "$1" "$2" >/dev/null 2>&1
}
# prep <engine> <container> <password> <env file>: what a start needs before lfc_user's login: OS users (Db2, Informix),
# the Informix admin's OS password from its env file, the Informix test database. Every start runs it again (idempotent)
prep() {
  case $1 in
    db2) printf 'lfc_user:%s\n' "$3" | podman exec -i -u root "$2" sh -c 'read l; id -u lfc_user >/dev/null 2>&1 || useradd -m lfc_user; echo "$l" | chpasswd' ;;
    informix)   # the container's default user is informix: the OS accounts need root
      sed -n 's/^INFORMIX_PASSWORD=//p' "$4" | sed 's/^/informix:/' | podman exec -i -u root "$2" chpasswd || return 1
      printf 'lfc_user:%s\n' "$3" | podman exec -i -u root "$2" sh -c 'read l; id -u lfc_user >/dev/null 2>&1 || useradd -m lfc_user; echo "$l" | chpasswd' || return 1
      [ "$(echo "SELECT name FROM sysmaster:sysdatabases WHERE name = 'lfcdb';" | sql "$2" "$1" 2>/dev/null | grep -c lfcdb)" -gt 0 ] \
        || echo "CREATE DATABASE lfcdb WITH LOG;" | sql "$2" "$1" >/dev/null 2>&1 ;;   # dbaccess's messages stay off the JSON line
  esac
}
login_sql() {   # engine, password: the Lakeflow Connect login with replication rights; databases and CDC are per test
  local p=${2//\'/\'\'}
  case $1 in
    postgres) printf "DO \$\$BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='lfc_user') THEN CREATE ROLE lfc_user LOGIN REPLICATION; END IF; END\$\$;\nALTER ROLE lfc_user WITH LOGIN REPLICATION PASSWORD '%s';\nGRANT pg_read_all_data TO lfc_user;\n" "$p" ;;
    mysql) printf "CREATE USER IF NOT EXISTS 'lfc_user'@'%%' IDENTIFIED WITH caching_sha2_password BY '%s';\nALTER USER 'lfc_user'@'%%' IDENTIFIED WITH caching_sha2_password BY '%s';\nGRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'lfc_user'@'%%';\n" "$p" "$p" ;;
    # MariaDB has no caching_sha2_password: the user gets its default plugin (mysql_native_password)
    mariadb) printf "CREATE USER IF NOT EXISTS 'lfc_user'@'%%' IDENTIFIED BY '%s';\nALTER USER 'lfc_user'@'%%' IDENTIFIED BY '%s';\nGRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'lfc_user'@'%%';\n" "$p" "$p" ;;
    # a PDB-local user in FREEPDB1 with the read rights the other engines' logins have (CDC's C## user is set up separately)
    oracle) printf "ALTER SESSION SET CONTAINER = FREEPDB1;\nDECLARE n NUMBER; BEGIN SELECT COUNT(*) INTO n FROM dba_users WHERE username = 'LFC_USER'; IF n = 0 THEN EXECUTE IMMEDIATE 'CREATE USER lfc_user IDENTIFIED BY \"%s\" DEFAULT TABLESPACE users QUOTA UNLIMITED ON users'; ELSE EXECUTE IMMEDIATE 'ALTER USER lfc_user IDENTIFIED BY \"%s\"'; END IF; END;\n/\nGRANT CREATE SESSION, SELECT ANY TABLE, SELECT_CATALOG_ROLE TO lfc_user;\n" "$p" "$p" ;;
    sqlserver) printf "IF SUSER_ID('lfc_user') IS NULL CREATE LOGIN [lfc_user] WITH PASSWORD = N'%s', CHECK_POLICY = OFF; ELSE ALTER LOGIN [lfc_user] WITH PASSWORD = N'%s', CHECK_POLICY = OFF;\n" "$p" "$p"
               # a user in every user database, with the read rights the other engines' logins have
               printf "%s\n" "DECLARE @d sysname, @s nvarchar(max); DECLARE c CURSOR LOCAL FOR SELECT name FROM sys.databases WHERE database_id > 4 AND state_desc = 'ONLINE'; OPEN c; FETCH c INTO @d; WHILE @@FETCH_STATUS = 0 BEGIN SET @s = N'USE ' + QUOTENAME(@d) + N'; IF USER_ID(''lfc_user'') IS NULL CREATE USER [lfc_user] FOR LOGIN [lfc_user]; ALTER ROLE db_datareader ADD MEMBER [lfc_user]; GRANT VIEW DATABASE STATE, VIEW DEFINITION TO [lfc_user];'; EXEC (@s); FETCH c INTO @d; END;" ;;
    # Db2's grant from lfc-source-setup's engine-db2.md (the login is an OS user: prep made it)
    db2) printf "GRANT DBADM ON DATABASE TO USER lfc_user;\n" ;;
    informix) printf "DATABASE lfcdb;\nGRANT CONNECT TO lfc_user;\nGRANT RESOURCE TO lfc_user;\nGRANT DBA TO lfc_user;\n" ;;
    # insecure mode can't set a password, and doesn't check one: the password printed for it isn't used
    # read rights on the tables that exist and on the ones made later (the default privileges, as pg_read_all_data does)
    cockroachdb) printf "CREATE DATABASE IF NOT EXISTS lfc;\nCREATE USER IF NOT EXISTS lfc_user;\nGRANT ALL ON DATABASE lfc TO lfc_user;\nUSE lfc;\nGRANT SELECT ON ALL TABLES IN SCHEMA public TO lfc_user;\nALTER DEFAULT PRIVILEGES FOR ROLE root IN SCHEMA public GRANT SELECT ON TABLES TO lfc_user;\n" ;;
    yugabytedb) printf "DO \$\$BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='lfc_user') THEN CREATE ROLE lfc_user LOGIN REPLICATION; END IF; END\$\$;\nALTER ROLE lfc_user WITH LOGIN REPLICATION PASSWORD '%s';\nGRANT pg_read_all_data TO lfc_user;\n" "$p"
                printf "%s\n" "SELECT 'CREATE DATABASE lfc' WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'lfc')\\gexec" ;;
  esac
}
# admin_sql <container> <engine> <database> <wide>: stdin as the engine's admin in <database>, rows tab-separated without
# headers (the harness reads them), stopping at the first error
admin_sql() {
  local c=$1 e=$2 d=$3
  case $e in
    postgres) podman exec -i "$c" psql -U postgres -d "${d:-postgres}" -v ON_ERROR_STOP=1 -qtAX -F $'\t' ;;
    sqlserver) local fl="-b -W -h -1"; [ "$4" = 1 ] && fl="-b -y 0"
               podman exec -i "$c" sh -c 'f=$(mktemp); cat > $f; /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$MSSQL_SA_PASSWORD" '"$fl"' -d '"${d:-master}"' -i $f; r=$?; rm -f $f; exit $r' ;;
    mysql) podman exec -i "$c" sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -N -B '"$d" ;;
    mariadb) podman exec -i "$c" sh -c 'MYSQL_PWD="$MARIADB_ROOT_PASSWORD" mariadb -uroot -N -B '"$d" ;;
    informix) podman exec -i "$c" bash -lc "dbaccess ${d:-sysmaster} -" ;;
    cockroachdb) podman exec -i "$c" /cockroach/cockroach sql --insecure --format=tsv ${d:+-d "$d"} ;;
    yugabytedb) local ip; ip=$(podman inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$c")
                podman exec -i "$c" bin/ysqlsh -h "$ip" -U yugabyte -d "${d:-yugabyte}" -v ON_ERROR_STOP=1 -qtAX -F $'\t' ;;
    oracle|db2) [ -z "$d" ] || die "$e takes no database argument"; sql "$c" "$e" ;;
  esac
}
all_metas() { find "$STATE" -mindepth 2 -maxdepth 2 -name '*.json' 2>/dev/null; }
field() { jq -r ".$2 // empty" "$1"; }

# fs_clone <from> <to>: a copy that shares blocks with <from>: juicefs clone on a JuiceFS ROOT (metadata only), else
# cp --reflink=always (XFS with reflink, as disk-setup.sh formats it); no silent full copy
fs_clone() {
  if [ "$(findmnt -n -o FSTYPE --target "$ROOT" 2>/dev/null)" = fuse.juicefs ]; then
    juicefs clone --preserve "$1" "$2" >/dev/null
  else
    cp -a --reflink=always "$1" "$2" || { rm -rf "${2:?}"; die "no reflink copy on $ROOT: it must be XFS with reflink=1 (disk-setup.sh)"; }
  fi
}
free_port() {   # the first port in the range that no record holds and nothing listens on (a container started elsewhere)
  local used p
  used=$( { for m in $(all_metas); do field "$m" port; done; ss -Htln | awk '{n = split($4, a, ":"); print a[n]}'; } | sort -n)
  for p in $(seq "$PORT_MIN" "$PORT_MAX"); do grep -qx "$p" <<<"$used" || { echo "$p"; return; }; done
  die "no free port in $PORT_MIN-$PORT_MAX"
}

park_one() {   # owner name
  local c; c=$(cname "$1" "$2")
  if podman container exists "$c"; then podman stop -t 120 "$c" >/dev/null 2>&1 || true; podman rm -f "$c" >/dev/null 2>&1 || true; fi
  OWNER=$1 log "$2" park
}

case $CMD in
  masters)
    for d in "$ROOT"/masters/*/; do [ -d "$d" ] && echo "$(basename "$d")"; done ;;

  ls)
    all=0; [ "${1:-}" = --all ] && all=1
    printf '%-12s %-20s %-10s %-8s %-6s %-20s %s\n' owner name engine state port created from
    for m in $(all_metas); do
      o=$(basename "$(dirname "$m")"); n=$(basename "$m" .json)
      [ $all = 1 ] || [ "$o" = "$OWNER" ] || continue
      st=parked; running "$o" "$n" && st=running
      printf '%-12s %-20s %-10s %-8s %-6s %-20s %s\n' "$o" "$n" "$(field "$m" engine)" "$st" "$(field "$m" port)" "$(field "$m" created)" "$(field "$m" from)"
    done ;;

  up)
    E=${1:-}; N=${2:-}; shift 2 || usage; name_ok "$N"; engine "$E"
    FROM="" EMPTY=0
    while [ $# -gt 0 ]; do case $1 in --from) FROM=${2:-}; shift 2 ;; --empty) EMPTY=1; shift ;; *) usage ;; esac; done
    exec 9>"$LOCK"; lock
    M=$(meta "$OWNER" "$N"); D=$ROOT/dbs/$OWNER/$N; C=$(cname "$OWNER" "$N")
    if [ -f "$M" ]; then
      [ "$(field "$M" engine)" = "$E" ] || die "$N is a $(field "$M" engine) database"
    fi
    if running "$OWNER" "$N"; then :; else
      # one running database per owner (MAX_PER_OWNER), then the VM's limit (MAX_RUNNING)
      if [ "$MAX_PER_OWNER" -gt 0 ]; then
        mine=$(for m in $(all_metas); do o=$(basename "$(dirname "$m")"); n=$(basename "$m" .json); [ "$o" = "$OWNER" ] && [ "$n" != "$N" ] && running "$o" "$n" && echo "$n"; done || true)
        cnt=$(grep -c . <<<"$mine" || true)
        if [ "$cnt" -ge "$MAX_PER_OWNER" ]; then
          for n in $mine; do echo "slot: parking your running database $n first (one at a time on this VM)" >&2; park_one "$OWNER" "$n"; done
        fi
      fi
      run_now=$(podman ps --format '{{.Names}}' | grep -c '^lfc-slot-' || true)
      if [ "$run_now" -ge "$MAX_RUNNING" ]; then
        if [ "$MAX_PER_OWNER" = 0 ]; then next="park one, or a bigger VM (one database per $CPUS_PER_DB vCPU)"; else next="try later, or ask for a VM of your own"; fi
        die "the VM is full: $run_now of $MAX_RUNNING databases are running ($(podman ps --format '{{.Names}}' | grep '^lfc-slot-' | sed 's/^lfc-slot-//' | tr '\n' ' ')); $next"
      fi
      if [ ! -f "$M" ]; then   # a new database: its data, admin env, login password and port
        [ -n "$FROM" ] || [ $EMPTY = 1 ] || die "new database: --from <master> or --empty (slot.sh masters)"
        [ -d "$D" ] && die "$D exists without a record; remove it or pick another name"
        install -d -m 755 "$ROOT/dbs/$OWNER"
        umask 077
        if [ -n "$FROM" ]; then
          [[ $FROM =~ ^[A-Za-z0-9._-]+$ ]] && [ -d "$ROOT/masters/$FROM" ] || die "no master $FROM (slot.sh masters)"
          [ -f "$SECRETS/$E.env" ] || die "no admin env $SECRETS/$E.env for the $E masters"
          fs_clone "$ROOT/masters/$FROM" "$D"
          cp "$SECRETS/$E.env" "$STATE/$OWNER/$N.env"
        else
          install -d "$D"
          [ "$E" = sqlserver ] && chown 10001:0 "$D"   # SQL Server runs as mssql (uid 10001)
          [ "$E" = oracle ] && chown 54321:54321 "$D"  # gvenzl's oracle user
          [ "$E" = yugabytedb ] && chown 10001:100 "$D"  # the image runs as yugabyte (uid 10001, group users)
          [ "$E" = informix ] && install -d "$D/cfg"     # the server's config volume
          case $E in
            postgres) printf 'POSTGRES_PASSWORD=%s\n' "$(pw)" ;;
            mysql) printf 'MYSQL_ROOT_PASSWORD=%s\n' "$(pw)" ;;
            sqlserver) printf 'ACCEPT_EULA=Y\nMSSQL_SA_PASSWORD=%s\n' "$(pw)" ;;
            oracle) printf 'ORACLE_PASSWORD=%s\n' "$(pw)" ;;
            mariadb) printf 'MARIADB_ROOT_PASSWORD=%s\n' "$(pw)" ;;
            db2) printf 'LICENSE=accept\nDB2INSTANCE=db2inst1\nDB2INST1_PASSWORD=%s\nBLU=false\nENABLE_ORACLE_COMPATIBILITY=false\nPERSISTENT_HOME=true\nARCHIVE_LOGS=false\nAUTOCONFIG=false\nSAMPLEDB=false\n' "$(pw)" ;;
            informix) printf 'LICENSE=accept\nINFORMIX_PASSWORD=%s\n' "$(pw)" ;;
            cockroachdb|yugabytedb) ;;   # insecure: no password
          esac > "$STATE/$OWNER/$N.env"
        fi
        pw > "$STATE/$OWNER/$N.login"
        P=$(free_port)
        jq -n --arg e "$E" --arg p "$P" --arg c "$(now)" --arg f "${FROM:-empty}" --arg i "$IMAGE" \
          '{engine: $e, port: ($p | tonumber), created: $c, from: $f, image: $i}' > "$M"
        umask 022
      fi
      P=$(field "$M" port)
      podman rm -f "$C" >/dev/null 2>&1 || true
      VOLS=(-v "$D:$MNT:$VOLOPT")
      [ "$E" = informix ] && VOLS+=(-v "$D/cfg:/opt/ibm/config:U,Z")
      podman run -d -q --name "$C" --label lfc-slot.owner="$OWNER" --label lfc-slot.name="$N" --label lfc.engine="$E" --label lfc.purpose=slot "${RUNOPTS[@]}" \
        --env-file "$STATE/$OWNER/$N.env" -p "$P:$CPORT" "${VOLS[@]}" "$IMAGE" "${ARGS[@]}" >/dev/null 9>&-   # the container must not inherit the lock
    fi
    # every up, started or already running: an up cut short (its ssh dropped during a first start) leaves a running
    # container without lfc_user, so the login runs again here (it's idempotent)
    deadline=$((SECONDS + WAIT))   # wall time: a probe that hangs counts against the wait (each probe gives up after 30 s)
    while [ "$SECONDS" -lt "$deadline" ]; do
      ready "$C" "$E" && break
      [ "$(podman inspect -f '{{.State.Running}}' "$C" 2>/dev/null)" = true ] || die "$E in $C stopped: podman logs $C"
      sleep 2
    done
    ready "$C" "$E" || die "$E in $C didn't answer in $((WAIT / 60)) minutes: podman logs $C"
    prep "$E" "$C" "$(cat "$STATE/$OWNER/$N.login")" "$STATE/$OWNER/$N.env" || { podman rm -f "$C" >/dev/null 2>&1; die "preparing $E in $C failed; run up again"; }
    # a failed login stops the container, so the next up tries again instead of resuming without lfc_user
    login_sql "$E" "$(cat "$STATE/$OWNER/$N.login")" | sql "$C" "$E" >/dev/null || { podman rm -f "$C" >/dev/null 2>&1; die "creating lfc_user in $C failed; run up again"; }
    log "$N" up
    P=$(field "$M" port)
    jq -cn --arg n "$N" --arg e "$E" --arg h "$(host)" --arg p "$P" --rawfile pw "$STATE/$OWNER/$N.login" --arg c "$C" --arg s "$SERVICE" --arg d "$DBNAME" \
      '{name: $n, engine: $e, host: $h, port: ($p | tonumber), user: "lfc_user", password: ($pw | rtrimstr("\n")), container: $c, state: "running"} + (if $s == "" then {} else {service: $s} end) + (if $d == "" then {} else {database: $d} end)' ;;

  park)
    N=${1:-}; name_ok "$N"; [ -f "$(meta "$OWNER" "$N")" ] || die "no database $N"
    exec 9>"$LOCK"; lock
    if running "$OWNER" "$N"; then park_one "$OWNER" "$N"; echo "parked $N: its data stays in $ROOT/dbs/$OWNER/$N"; else echo "$N is already parked"; fi ;;

  clone)
    N=${1:-}; T=${2:-}; name_ok "$N"; name_ok "$T"; [ "$N" != "$T" ] || die "clone to a new name"
    M=$(meta "$OWNER" "$N"); MT=$(meta "$OWNER" "$T"); [ -f "$M" ] || die "no database $N"
    D=$ROOT/dbs/$OWNER/$N; DT=$ROOT/dbs/$OWNER/$T
    exec 9>"$LOCK"; lock
    [ -f "$MT" ] && die "$T exists"
    [ -e "$DT" ] && die "$DT exists without a record; remove it or pick another name"
    E=$(field "$M" engine); was=0
    if running "$OWNER" "$N"; then was=1; echo "slot: stopping $N for a consistent copy" >&2; park_one "$OWNER" "$N"; fi
    t0=$(date +%s)
    umask 077
    fs_clone "$D" "$DT"
    cp "$STATE/$OWNER/$N.env" "$STATE/$OWNER/$T.env"   # the admin password is inside the copied data
    pw > "$STATE/$OWNER/$T.login"                      # up sets lfc_user's password to it
    PT=$(free_port)
    jq --arg p "$PT" --arg c "$(now)" --arg f "clone:$N" '.port = ($p | tonumber) | .created = $c | .from = $f' "$M" > "$MT"
    umask 022
    log "$T" clone
    secs=$(( $(date +%s) - t0 ))
    if [ $was = 1 ]; then exec 9>&-; "$0" --owner "$OWNER" up "$E" "$N" >/dev/null || echo "slot: $N didn't start again: up $E $N" >&2; fi
    jq -cn --arg n "$T" --arg e "$E" --arg f "$N" --arg s "$secs" --arg p "$(field "$MT" port)" \
      '{name: $n, engine: $e, from: $f, port: ($p | tonumber), state: "parked", seconds: ($s | tonumber)}'
    echo "slot: up $E $T starts it" >&2 ;;

  rm)
    N=${1:-}; name_ok "$N"; M=$(meta "$OWNER" "$N"); [ -f "$M" ] || die "no database $N"
    exec 9>"$LOCK"; lock
    running "$OWNER" "$N" && park_one "$OWNER" "$N"
    D=$ROOT/dbs/$OWNER/$N
    if [ -d "$D" ]; then juicefs rmr "$D" >/dev/null 2>&1 || rm -rf "${D:?}"; fi
    rm -f "$M" "$STATE/$OWNER/$N.env" "$STATE/$OWNER/$N.login"
    log "$N" rm; echo "removed $N" ;;

  usage)
    DAYS=7 all=0
    while [ $# -gt 0 ]; do case $1 in --days) DAYS=$2; shift 2 ;; --all) all=1; shift ;; *) usage ;; esac; done
    [[ $DAYS =~ ^[0-9]+$ ]] || die "--days is a number"
    [ -f "$LEDGER" ] || { echo '[]'; exit 0; }
    jq -s --arg since "$(date -u -d "-$DAYS days" +%Y-%m-%dT%H:%M:%SZ)" --arg now "$(now)" --arg me "$OWNER" --argjson all $all '
      def t: fromdateiso8601;
      group_by(.owner + "/" + .name)
      | map(. as $ev | reduce $ev[] as $e ({start: null, secs: 0};
          if $e.event == "up" then (if .start == null then .start = $e.ts else . end)
          elif .start != null then .secs += (([$e.ts | t, ($since | t)] | max) - ([.start | t, ($since | t)] | max) | if . < 0 then 0 else . end) | .start = null
          else . end)
        | {owner: $ev[0].owner, name: $ev[0].name,
           secs: (.secs + (if .start != null then (($now | t) - ([.start | t, ($since | t)] | max)) else 0 end))})
      | map(select($all == 1 or .owner == $me))
      | group_by(.owner) | map({owner: .[0].owner, since: ($since | .[0:10]), hours: ((map(.secs) | add) / 3600 * 10 | round / 10),
                                databases: map({name, hours: (.secs / 3600 * 10 | round / 10)})})' "$LEDGER" ;;

  sql)
    N=${1:-}; name_ok "$N"; shift; M=$(meta "$OWNER" "$N"); [ -f "$M" ] || die "no database $N"
    DB="" WIDE=0
    while [ $# -gt 0 ]; do case $1 in --wide) WIDE=1; shift ;; *) [ -z "$DB" ] || usage; DB=$1; shift ;; esac; done
    [ -z "$DB" ] || [[ $DB =~ ^[A-Za-z0-9_]{1,128}$ ]] || die "bad database: $DB (letters, digits, _)"
    running "$OWNER" "$N" || die "$N is parked: up $(field "$M" engine) $N first"
    admin_sql "$(cname "$OWNER" "$N")" "$(field "$M" engine)" "$DB" "$WIDE" ;;   # no lock: a long fill mustn't block up/park

  info)
    N=${1:-}; name_ok "$N"; M=$(meta "$OWNER" "$N"); [ -f "$M" ] || die "no database $N"
    C=$(cname "$OWNER" "$N")
    PRETTY_NAME=unknown; [ -r /etc/os-release ] && . /etc/os-release; echo "Host: $PRETTY_NAME"; echo "Podman: $(podman --version)"
    if running "$OWNER" "$N"; then podman inspect --format 'Image: {{.ImageName}} ({{.ImageDigest}})' "$C"; else echo "Image: $(field "$M" image) (parked)"; fi
    echo "Data: JuiceFS volume at $ROOT (object storage), database $(field "$M" engine) $N, port $(field "$M" port)"
    curl -fs -m 5 -H Metadata:true "http://169.254.169.254/metadata/instance/compute?api-version=2021-12-13" \
      | jq -r '"VM: Azure \(.vmSize), region \(.location), zone \(if .zone == "" then "none" else .zone end)"' 2>/dev/null || true ;;

  *) usage ;;
esac
