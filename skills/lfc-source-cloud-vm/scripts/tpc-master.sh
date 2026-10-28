#!/usr/bin/env bash
# tpc-master.sh <postgres|mysql|mariadb|sqlserver|oracle|cockroachdb|yugabytedb|db2|informix> <master> [workload ...]: build a
# master holding TPC workloads (default all six: tpcb tpcc tpch tpcds tpce tpcdi) in one engine, filled inside the database by
# lfc-source-datagen, then stop it cleanly and copy it into JuiceFS as $OUT/<master>.
#   Each workload is its own database (MySQL, MariaDB, SQL Server, Informix), schema (Postgres, CockroachDB and YugabyteDB in
#   database lfc, Db2 in database LFCDB), or user (Oracle, in FREEPDB1), named after the workload. Its DDL is
#   $DDL/<workload>-<dialect>.sql, written by lfc-source-statschema's shape.py and ddl.py (dialects postgres, mysql, tsql, oracle,
#   cockroachdb, db2, informix; MariaDB and YugabyteDB take mysql and postgres). Rows come from INSERT ... SELECT over the
#   engine's own generator: generate_series (Postgres, CockroachDB, YugabyteDB: SRC=series), a seed table filled by doubling
#   (MySQL, MariaDB), GENERATE_SERIES (SQL Server 2022), CONNECT BY LEVEL (Oracle). Db2, Informix and CockroachDB fill
#   through fill.py (its planner is Python; PY: a python with ibm-db, informix-driver, psycopg2; references/python-drivers.md),
#   whose INSERT ... SELECT also makes every row in the database: a recursive common table expression (Db2), CONNECT BY
#   LEVEL (Informix), generate_series (CockroachDB).
#   The build runs on local disk ($DATA/build-<master>), tuned for loading (no fsync, no binary log, SIMPLE recovery);
#   the settings are the image defaults again when a clone starts. The lfc tags (lfc_seed.tags, lfc_tags) stay, so a
#   clone can be topped up or appended to with the same planners.
#   Admin passwords come from env files ($SECRETS/<engine>.env, mode 600, the ones slot.sh writes): POSTGRES_PASSWORD;
#   MYSQL_ROOT_PASSWORD or MARIADB_ROOT_PASSWORD; ACCEPT_EULA=Y and MSSQL_SA_PASSWORD; ORACLE_PASSWORD; LICENSE=accept and
#   DB2INST1_PASSWORD (plus slot.sh's Db2 settings); LICENSE=accept and INFORMIX_PASSWORD. CockroachDB and YugabyteDB run
#   insecure: an empty env file. Run it as a systemd unit (watch-progress): it takes an hour or more.
#   The data directory is mounted with :Z, for hosts with SELinux enforcing (Fedora, as in a Lima VM); elsewhere it's a no-op.
#   Db2 and Informix publish their port on 127.0.0.1 (HP, IP) for the drivers; the build stops them cleanly before the copy.
#   Informix's developer image refuses a dbspace of 30 GB ("Demo Version limits exceeded"): 1 GB (IFX_DBSPACE_KB, the default) is accepted,
#   so the Informix master is SF 0.1: DDL = shape.py --sf 0.1, then ddl.py --dialect informix, in $DDL (see lfc-source-cloud-vm's
#   references/engine-masters.md for the measured fit). Name it informix1410-tpc-sf0.1.
set -uo pipefail
E=$1; NAME=$2; shift 2; WL=${*:-tpcb tpcc tpch tpcds tpce tpcdi}
DDL=${DDL:-/tmp/dg/ddl}; DG=${DG:-/tmp/dg}; DATA=${DATA:-/var/lib/containers/lfc-data}; OUT=${OUT:-/mnt/jfs/masters}
SECRETS=${SECRETS:-/root/lfc-src}; JC=${JC:-/var/lib/containers/jfs/cache}; SESSIONS=${SESSIONS:-4}   # MySQL: MY_SESSIONS (default 2)
PY=${PY:-python3}; HP=${HP:-50123}; IP=${IP:-9188}          # Db2 and Informix: the driver's python; their published ports
M=$DATA/build-$NAME; C=build-$NAME; W=$(mktemp -d); trap 'rm -rf "${W:?}"' EXIT
# wait until a check passes, while the container is still running (else show why it stopped, and stop)
wait_for() { until "$@" > /dev/null 2>&1; do
  [ "$(podman container inspect -f '{{.State.Running}}' $C 2>/dev/null)" = true ] || { echo "$C stopped:"; podman logs --tail 20 $C 2>&1; exit 1; }
  sleep 3; done; }
# the DDL's output, checked for errors: sqlcmd and SQL*Plus print them and carry on
ddl_ok() { if grep -E "^(Msg [0-9]+, Level 1[1-9]|ORA-[0-9]+|SP2-[0-9]+)" $W/ddl.out; then echo "DDL for $1 failed (above)"; return 1; fi; }
t() { local s=$(date +%s); "$@"; local r=$?; echo "  [$(( $(date +%s) - s )) s] $*" | cut -c1-120; return $r; }
[ -f "$SECRETS/$E.env" ] || { echo "missing $SECRETS/$E.env"; exit 2; }
[ -e "$OUT/$NAME" ] && { echo "$OUT/$NAME exists: remove it first"; exit 2; }
podman rm -f --depend $C > /dev/null 2>&1; rm -rf "${M:?}"; mkdir -p $M   # --depend: an earlier run's psql sidecar (--network container:) blocks a plain rm -f
case $E in
postgres)
  podman run -d --name $C --label lfc.engine=$E --label lfc.purpose=tpc-master --label lfc.master=$NAME --shm-size 4g --env-file $SECRETS/$E.env -v $M:/var/lib/postgresql/data:Z docker.io/library/postgres:16 \
    -c shared_buffers=4GB -c maintenance_work_mem=1GB -c max_wal_size=16GB -c fsync=off -c synchronous_commit=off -c full_page_writes=off > /dev/null
  wait_for podman exec $C pg_isready -h 127.0.0.1 -q; sleep 2
  PG="podman exec -i $C psql -X -U postgres -d postgres -q -v ON_ERROR_STOP=1"
  load() { { echo "CREATE SCHEMA $1; SET search_path = $1;"; cat $DDL/$1-postgres.sql; } | $PG > /dev/null &&
           PSQL="podman exec -i $C psql -U postgres -d postgres" DIR=$DG/postgres SRC=series SESSIONS=$SESSIONS bash $DG/fill.sh pg $1; }
  finish() { $PG -c "VACUUM ANALYZE" && $PG -c CHECKPOINT && podman stop -t 300 $C > /dev/null &&
             # after a clean stop (no replication slots), pg_resetwal drops the load's WAL to one segment; data is untouched
             podman run --rm --user postgres -v $M:/var/lib/postgresql/data:Z docker.io/library/postgres:16 pg_resetwal -D /var/lib/postgresql/data; } ;;
cockroachdb|yugabytedb)   # PostgreSQL-protocol servers: the Postgres planners through psql, in a sidecar sharing the server's network
  if [ $E = cockroachdb ]; then
    P=26257; U=root; ADMIN=defaultdb; DDLD=cockroachdb; CP=${CRP:-26299}
    : > $W/empty                # insecure: fill.py reads the (empty) password file
    podman run -d --name $C --label lfc.engine=$E --label lfc.purpose=tpc-master --label lfc.master=$NAME -v $M:/cockroach/cockroach-data:Z -p 127.0.0.1:$CP:26257 docker.io/cockroachdb/cockroach:v25.4.13 \
      start-single-node --insecure --cache=2GiB --max-sql-memory=2GiB > /dev/null
    ready_sql() { podman run --rm --network container:$C docker.io/library/postgres:16 psql -X -q -At -h 127.0.0.1 -p $P -U $U -d $ADMIN -c 'SELECT 1'; }
    stop_db() { podman exec $C /cockroach/cockroach node drain --self --insecure --shutdown > /dev/null 2>&1; podman stop -t 120 $C > /dev/null; }
  else
    P=5433; U=yugabyte; ADMIN=yugabyte; DDLD=postgres
    chown 10001:100 $M   # the image runs as yugabyte (uid 10001, group users), as slot.sh does
    podman run -d --name $C --label lfc.engine=$E --label lfc.purpose=tpc-master --label lfc.master=$NAME -v $M:/yb:Z docker.io/yugabytedb/yugabyte:2026.1.2.0-b137 \
      bin/yugabyted start --base_dir=/yb --advertise_address=127.0.0.1 --tserver_flags=pgsql_proxy_bind_address=0.0.0.0:5433 --background=false > /dev/null
    ready_sql() { podman run --rm --network container:$C docker.io/library/postgres:16 psql -X -q -At -h 127.0.0.1 -p $P -U $U -d $ADMIN -c 'SELECT 1'; }
    stop_db() { podman stop -t 120 $C > /dev/null; }
  fi
  wait_for ready_sql; sleep 2
  PGCMD="podman run --rm -i --network container:$C docker.io/library/postgres:16 psql -X -q -h 127.0.0.1 -p $P -U $U"
  $PGCMD -d $ADMIN -v ON_ERROR_STOP=1 -c "CREATE DATABASE lfc" > /dev/null || exit 1   # the database slot.sh uses (a fresh server)
  PG="$PGCMD -d lfc -v ON_ERROR_STOP=1"
  if [ $E = cockroachdb ]; then   # the psql planners need pg_temp functions, which CockroachDB refuses: fill.py builds the same generate_series INSERTs (a statement per 50,000 rows: one
  # 600,000-row INSERT with its foreign-key checks ran for 12 minutes without committing a row)
    load() { { echo "CREATE SCHEMA $1; SET search_path = $1;"; cat $DDL/$1-$DDLD.sql; } | $PG > /dev/null &&
             $PY $DG/python/fill.py pg $1 --database lfc --host 127.0.0.1 --port $CP --user root --password-file $W/empty --sessions $SESSIONS --rows ${CRDB_ROWS:-50000}; }
  else
    load() { { echo "CREATE SCHEMA $1; SET search_path = $1;"; cat $DDL/$1-$DDLD.sql; } | $PG > /dev/null &&
             PSQL="$PGCMD -d lfc" DIR=$DG/postgres SRC=series SESSIONS=$SESSIONS bash $DG/fill.sh pg $1; }
  fi
  finish() { if [ $E = yugabytedb ]; then $PG -c "ANALYZE"; fi; stop_db; } ;;
mysql|mariadb)
  if [ $E = mariadb ]; then   # the MySQL planners; MariaDB's root password variable, and no redo-log capacity setting
    CLI=mariadb; PWV=MARIADB_ROOT_PASSWORD; IMG=docker.io/library/mariadb:11.4
    ARGS="--skip-log-bin --innodb-buffer-pool-size=4G --innodb-flush-log-at-trx-commit=0 --innodb-log-file-size=2G"
  else
    CLI=mysql; PWV=MYSQL_ROOT_PASSWORD; IMG=docker.io/library/mysql:8.4
    ARGS="--skip-log-bin --innodb-buffer-pool-size=4G --innodb-flush-log-at-trx-commit=0 --innodb-redo-log-capacity=2G"
  fi
  podman run -d --name $C --label lfc.engine=$E --label lfc.purpose=tpc-master --label lfc.master=$NAME --env-file $SECRETS/$E.env -v $M:/var/lib/mysql:Z $IMG $ARGS > /dev/null
  wait_for podman exec $C sh -c "MYSQL_PWD=\$$PWV $CLI -uroot -e 'SELECT 1'"
  podman exec $C sh -c 'umask 077; printf "[client]\nuser=root\npassword=%s\n" "$'$PWV'" > /root/.my.cnf'   # not on a command line
  MY="podman exec -i $C $CLI"
  { echo "CREATE DATABASE IF NOT EXISTS lfc_seed; CREATE TABLE lfc_seed.seed (i bigint PRIMARY KEY);"   # 1..2^20, by doubling
    echo "INSERT INTO lfc_seed.seed (i) VALUES $(printf '(%s),' $(seq 1 1024) | sed 's/,$//');"
    for k in $(seq 10); do echo "INSERT INTO lfc_seed.seed (i) SELECT s.i + b.m FROM lfc_seed.seed s, (SELECT MAX(i) AS m FROM lfc_seed.seed) b;"; done
    echo "CREATE TABLE lfc_seed.seed_r (i bigint PRIMARY KEY, r bigint NOT NULL);"       # r: a random permutation, for shuffled keys
    echo "INSERT INTO lfc_seed.seed_r SELECT i, ROW_NUMBER() OVER (ORDER BY RAND()) FROM lfc_seed.seed;"
    echo "DROP TABLE lfc_seed.seed; RENAME TABLE lfc_seed.seed_r TO lfc_seed.seed;"; } | $MY || exit 1
  load() { { echo "CREATE DATABASE $1; USE $1;"; cat $DDL/$1-mysql.sql; } | $MY && MYSQL="$MY" DIR=$DG/mysql SESSIONS=${MY_SESSIONS:-2} bash $DG/fill.sh my $1; }   # 2: MySQL's best on 4 vCPUs (lfc-source-datagen calibrate.md)
  finish() { for w in $WL; do $MY -N -e "SELECT CONCAT('ANALYZE TABLE \`', table_schema, '\`.\`', table_name, '\`;') FROM information_schema.tables WHERE table_schema = '$w'" | $MY > /dev/null; done
             $MY -e "DROP TABLE lfc_seed.seed" && podman stop -t 300 $C > /dev/null; } ;;
sqlserver)
  chown 10001:0 $M
  podman run -d --name $C --label lfc.engine=$E --label lfc.purpose=tpc-master --label lfc.master=$NAME --memory 6g --env-file $SECRETS/$E.env -e MSSQL_MEMORY_LIMIT_MB=5120 -v $M:/var/opt/mssql:Z mcr.microsoft.com/mssql/server:2022-latest > /dev/null
  # the client: a script, since a command in a variable can't hold quotes; sqlcmd gets a file (-i): it splits long stdin lines
  cat > $W/sq <<Q
#!/bin/bash
exec podman exec -i $C bash -c 'f=\$(mktemp); cat > \$f; SQLCMDPASSWORD=\$MSSQL_SA_PASSWORD /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -d "\$0" -y 0 -i \$f; r=\$?; rm -f \$f; exit \$r' "\${1:-master}"
Q
  chmod 700 $W/sq; SQ=$W/sq
  sq_ok() { echo "SELECT 1" | $SQ; }; wait_for sq_ok
  load() { echo "CREATE DATABASE [$1]; ALTER DATABASE [$1] SET RECOVERY SIMPLE;" | $SQ > /dev/null && $SQ $1 < $DDL/$1-tsql.sql > $W/ddl.out && ddl_ok $1 &&
           { printf '#!/bin/bash\nexec %s %s\n' $SQ $1 > $W/sq-$1; chmod 700 $W/sq-$1; MSSQL=$W/sq-$1 DIR=$DG/sqlserver SESSIONS=$SESSIONS bash $DG/fill.sh ms dbo; }; }
  finish() { for w in $WL; do echo "EXEC sp_updatestats;" | $SQ $w > /dev/null || return 1; done; echo "SHUTDOWN;" | $SQ > /dev/null; podman stop -t 120 $C > /dev/null; } ;;
oracle)
  chown 54321:54321 $M
  podman run -d --name $C --label lfc.engine=$E --label lfc.purpose=tpc-master --label lfc.master=$NAME --memory 4g --shm-size 2g --env-file $SECRETS/$E.env -v $M:/opt/oracle/oradata:Z docker.io/gvenzl/oracle-free:23-slim > /dev/null
  cat > $W/sq <<Q
#!/bin/bash
exec podman exec -i $C bash -c '{ echo "SET FEEDBACK OFF"; echo "ALTER SESSION SET CONTAINER = FREEPDB1;"; cat; } | sqlplus -s / as sysdba'
Q
  chmod 700 $W/sq; SQ=$W/sq
  ora_ok() { podman logs $C 2>&1 | grep "DATABASE IS READY TO USE"; }; wait_for ora_ok
  load() { local S=$(echo $1 | tr a-z A-Z)
           { echo "SET DEFINE OFF"; echo "CREATE USER $S NO AUTHENTICATION QUOTA UNLIMITED ON USERS DEFAULT TABLESPACE USERS;"
             echo "ALTER SESSION SET CURRENT_SCHEMA = $S;"; cat $DDL/$1-oracle.sql; } | $SQ > $W/ddl.out
           ddl_ok $1 || return 1
           ORACLE=$SQ DIR=$DG/oracle SESSIONS=$SESSIONS bash $DG/fill.sh ora $S; }
  finish() { for w in $WL; do printf "EXEC DBMS_STATS.GATHER_SCHEMA_STATS('%s')\n" $(echo $w | tr a-z A-Z) | $SQ; done; podman stop -t 120 $C > /dev/null; } ;;
db2)   # Db2 Community: LFCDB made after the image's setup with 4 KB-aligned I/O (a 4096-byte-sector disk refuses Db2's
       # 512-byte direct-I/O writes: SQL0293N); fill.py fills each schema (the Db2 planner isn't SQL)
  install -m 600 /dev/null $W/pw; sed -n 's/^DB2INST1_PASSWORD=//p' $SECRETS/$E.env > $W/pw
  podman run -d --name $C --label lfc.engine=$E --label lfc.purpose=tpc-master --label lfc.master=$NAME --privileged --env-file $SECRETS/$E.env -e DBNAME= -e PERSISTENT_HOME=true -v $M:/database:Z -p 127.0.0.1:$HP:50000 \
    icr.io/db2_community/db2:11.5.9.0 > /dev/null
  db2_ok() { [ "$(podman logs $C 2>&1 | grep -cF '(*) Setup has completed')" -gt 0 ] && echo "SELECT 1 FROM SYSIBM.SYSDUMMY1;" | $PYC; }
  PYC="$PY $DG/python/sq.py db2 --host 127.0.0.1 --port $HP --user db2inst1 --database LFCDB --password-file $W/pw"
  setup_ok() { [ "$(podman logs $C 2>&1 | grep -cF '(*) Setup has completed')" -gt 0 ]; }; wait_for setup_ok
  podman exec $C su - db2inst1 -c 'db2set DB2_4K_DEVICE_SUPPORT=ON && { db2stop force; db2start; } >/dev/null 2>&1; db2 create database LFCDB' > /dev/null || exit 1
  wait_for db2_ok
  printf '%s\n' "CALL SYSPROC.ADMIN_CMD('UPDATE DB CFG FOR LFCDB USING PCKCACHESZ 16384');" "CALL SYSPROC.ADMIN_CMD('UPDATE DB CFG FOR LFCDB USING LOGSECOND 200');" | $PYC > /dev/null || exit 1
  load() { local S=$(echo $1 | tr a-z A-Z)
           { echo "CREATE SCHEMA $S;"; cat $DDL/$1-db2.sql; } | $PYC --schema $S > $W/ddl.out 2>&1 || { cat $W/ddl.out; return 1; }
           $PY $DG/python/fill.py db2 $1 --database LFCDB --host 127.0.0.1 --port $HP --user db2inst1 --password-file $W/pw --sessions ${DB2_SESSIONS:-2}; }
  finish() { podman exec $C su - db2inst1 -c 'db2 force applications all; db2stop force' > /dev/null 2>&1; podman stop -t 120 $C > /dev/null; } ;;
informix)   # Informix Developer: one database per workload in a dbspace of its own (the image's root dbspace is 1.6 GB)
  install -d $M/cfg
  install -m 600 /dev/null $W/ipw; sed -n 's/^INFORMIX_PASSWORD=//p' $SECRETS/$E.env > $W/ipw
  podman run -d --name $C --label lfc.engine=$E --label lfc.purpose=tpc-master --label lfc.master=$NAME --env-file $SECRETS/$E.env -v $M:/opt/ibm/data:U,Z -v $M/cfg:/opt/ibm/config:U,Z \
    -p 127.0.0.1:$IP:9088 docker.io/ibmcom/informix-developer-database:14.10.FC7W1DE > /dev/null
  ifx_ok() { echo "SELECT 1 FROM sysmaster:sysdual;" | $PYS > /dev/null; }
  PYS="$PY $DG/python/sq.py ifx --host 127.0.0.1 --port $IP --user informix --password-file $W/ipw --database sysmaster"
  wait_for ifx_ok
  # the OS password that the server checks (PAM), then the dbspace (the chunk file must exist, owned by informix, mode 660).
  # The developer image refuses a dbspace of 30 GB ("Demo Version limits exceeded"); 1 GB (IFX_DBSPACE_KB) is accepted
  sed -n 's/^INFORMIX_PASSWORD=//p' $SECRETS/$E.env | sed 's/^/informix:/' | podman exec -i -u root $C chpasswd || exit 1
  podman exec -u root $C bash -c "touch /opt/ibm/data/spaces/datadbs.000 && chown informix:informix /opt/ibm/data/spaces/datadbs.000 && chmod 660 /opt/ibm/data/spaces/datadbs.000" || exit 1
  podman exec -u informix $C bash -lc "onspaces -c -d datadbs -p /opt/ibm/data/spaces/datadbs.000 -o 0 -s ${IFX_DBSPACE_KB:-1000000}" > $W/onspaces.out 2>&1 || { cat $W/onspaces.out; exit 1; }
  load() { echo "CREATE DATABASE $1 IN datadbs WITH LOG;" | $PYS > $W/ddl.out 2>&1 || { cat $W/ddl.out; return 1; }
           $PY $DG/python/sq.py ifx --host 127.0.0.1 --port $IP --user informix --password-file $W/ipw --database $1 < $DDL/$1-informix.sql > $W/ddl.out 2>&1 || { cat $W/ddl.out; return 1; }
           $PY $DG/python/fill.py ifx $1 --database $1 --host 127.0.0.1 --port $IP --user informix --password-file $W/ipw --sessions 1; }
  finish() { podman exec -u informix $C bash -lc 'onmode -ky' > /dev/null 2>&1; podman stop -t 120 $C > /dev/null; } ;;
*) echo "engine: postgres, mysql, mariadb, sqlserver, oracle, cockroachdb, yugabytedb, db2 or informix"; exit 2 ;;
esac
for w in $WL; do echo "== $w"; t load $w > $W/load.out 2>&1; r=$?; grep -v -E "^\(?[0-9]+ rows? affected\)?$|^$" $W/load.out | tail -40
                  [ $r = 0 ] || { echo "$w failed"; exit 1; }; done
echo "== statistics, clean stop"; t finish || { echo "statistics or the clean stop failed: not copied; the build is still in $M (container $C)"; exit 1; }
podman rm $C > /dev/null 2>&1
echo "master on disk: $(du -sh $M | cut -f1), files $(find $M -type f | wc -l)"
# into JuiceFS (then wait until its write-back staging is uploaded), or a reflink copy when $OUT is on the same XFS
t cp -a --reflink=auto $M $OUT/$NAME || { echo "copy to $OUT/$NAME failed: the build is still in $M"; exit 1; }
until ! find $JC -path '*rawstaging*' -type f 2>/dev/null | grep . > /dev/null; do sleep 2; done
echo "copied to $OUT/$NAME: $(juicefs info -r $OUT/$NAME 2>/dev/null | awk '/size:/ {print $2 $3; exit}')"
rm -rf "${M:?}"
echo "MASTER DONE $NAME"
