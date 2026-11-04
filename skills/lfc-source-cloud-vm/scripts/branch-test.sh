#!/usr/bin/env bash
# branch-test.sh <engine> <step>: run a database master through both branch layers, on the VM as root (sudo bash).
#   Needs: XFS at /var/lib/containers (disk-setup.sh), rootful Podman, the rclone env file /var/tmp/lfc-rclone.env
#   (master-snapshots.md), and for jfs a formatted JuiceFS volume, /etc/juicefs/azure.env and the jfs-redis container
#   (juicefs-branches.md). The engine password is generated into /root/lfc-eng.pw (mode 600) and passed by env file.
#   engines: mssql oracle db2 crdb yb
#   steps:   init    build a 5M-row master on local XFS ($D/m-<engine>) and stop it
#            rclone  upload to Blob, pull to a cache, reflink-clone, start, check, write, measure a save, discard
#            jfs     copy into JuiceFS, juicefs clone, start, check, write, cold read, discard
#            reflink pull the master from Blob if not cached (read-only), reflink-clone, write on the clone, then show the
#                    master is byte-for-byte unchanged and a second clone has none of the write
set -uo pipefail
E=$1; D=/var/lib/containers/lfc-data; J=/mnt/jfs; JC=/var/lib/containers/jfs/cache; N=5000000
F="--create-empty-src-dirs --metadata --links --transfers 16 --checkers 16"
t() { local s=$(date +%s.%N); "$@"; local rc=$?; echo "  [$(echo "$(date +%s.%N) - $s" | bc | cut -c1-6) s] $*"; return $rc; }
rclone() { podman run --rm --env-file /var/tmp/lfc-rclone.env -v $D:$D docker.io/rclone/rclone:latest "$@"; }
[ -s /root/lfc-eng.pw ] || (umask 077; echo "Lfc1$(openssl rand -hex 12)" > /root/lfc-eng.pw)
ENVF=/root/lfc-$E.env; PW=$(cat /root/lfc-eng.pw); EXTRA=; CMD=; TMO=600
case $E in
mssql) IMG=mcr.microsoft.com/mssql/server:2022-latest; MNT=/var/opt/mssql
  (umask 077; printf 'ACCEPT_EULA=Y\nMSSQL_PID=Developer\nMSSQL_SA_PASSWORD=%s\n' "$PW" > $ENVF)
  sql() { podman exec -i $1 bash -c 'cat > /tmp/q.sql; SQLCMDPASSWORD=$MSSQL_SA_PASSWORD /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -b -h -1 -W -i /tmp/q.sql'; }
  OK="SET NOCOUNT ON; SELECT CASE WHEN DB_ID('lfc') IS NULL OR DATABASEPROPERTYEX('lfc', 'Status') = 'ONLINE' THEN 'ok' ELSE 'wait' END"; COUNT="SET NOCOUNT ON; SELECT CONCAT(COUNT_BIG(*), '|', SUM(CAST(n AS bigint))) FROM lfc.dbo.t"
  WRITE="UPDATE lfc.dbo.t SET n = n + 1 WHERE id <= 100000"
  LOAD="CREATE DATABASE lfc
GO
ALTER DATABASE lfc SET RECOVERY SIMPLE
GO
USE lfc; CREATE TABLE t (id bigint PRIMARY KEY, v varchar(100), n int);
INSERT INTO t WITH (TABLOCK) SELECT TOP ($N) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), REPLICATE('x', 80), 0 FROM sys.all_columns a CROSS JOIN sys.all_columns b" ;;
oracle) IMG=docker.io/gvenzl/oracle-free:23-slim; MNT=/opt/oracle/oradata
  (umask 077; printf 'ORACLE_PASSWORD=%s\nAPP_USER=lfc\nAPP_USER_PASSWORD=%s\n' "$PW" "$PW" > $ENVF)
  sql() { podman exec -i $1 bash -c '{ echo "whenever sqlerror exit failure"; echo "connect lfc/\"$APP_USER_PASSWORD\"@localhost/FREEPDB1"
          echo "set heading off feedback off pagesize 0 linesize 200"; cat; echo; echo "exit"; } | sqlplus -s /nolog'; }
  OK="select 'ok' from dual;"; COUNT="select count(*) || '|' || sum(n) from t;"
  WRITE="update t set n = n + 1 where id <= 100000;
commit;"
  LOAD="create table t (id number primary key, v varchar2(100), n number);
insert /*+ append */ into t select rownum, rpad('x', 80, 'x'), 0 from (select 1 from dual connect by level <= 5000), (select 1 from dual connect by level <= $((N / 5000)));
commit;" ;;
db2) IMG=icr.io/db2_community/db2:11.5.9.0; MNT=/database; EXTRA="--privileged"; TMO=1800
  (umask 077; printf 'LICENSE=accept\nDB2INSTANCE=db2inst1\nDB2INST1_PASSWORD=%s\nDBNAME=LFC\nBLU=false\nENABLE_ORACLE_COMPATIBILITY=false\nPERSISTENT_HOME=true\nARCHIVE_LOGS=false\nAUTOCONFIG=false\nSAMPLEDB=false\n' "$PW" > $ENVF)
  sql() { podman exec -i $1 bash -c '{ echo "connect to LFC;"; cat; } > /tmp/q.sql; chmod 644 /tmp/q.sql; su - db2inst1 -c "db2 -x -t -s -f /tmp/q.sql"; rc=$?; [ $rc -lt 4 ]'; }
  OK="values 'ok';"; COUNT="select varchar(count(*)) || '|' || varchar(sum(bigint(n))) from t;"
  WRITE="update t set n = n + 1 where id <= 100000;"
  LOAD="create table t (id bigint not null primary key, v varchar(100), n int);
update command options using c off;
alter table t activate not logged initially;
insert into t select row_number() over (), repeat('x', 80), 0 from syscat.columns a, syscat.columns b fetch first $N rows only;
commit;" ;;
crdb) IMG=docker.io/cockroachdb/cockroach:latest; MNT=/cockroach/cockroach-data; CMD="start-single-node --insecure --cache=2GiB --max-sql-memory=2GiB"
  : > $ENVF
  sql() { podman exec -i $1 ./cockroach sql --insecure --format=tsv | tail -n +2; }
  OK="select 'ok';"; COUNT="select count(*)::string || '|' || sum(n)::string from lfc.t;"
  WRITE="update lfc.t set n = n + 1 where id <= 100000;"
  LOAD="create database lfc; create table lfc.t (id int primary key, v string, n int);
$(for k in $(seq 0 19); do echo "insert into lfc.t select g, repeat('x', 80), 0 from generate_series($((k * N / 20 + 1)), $(((k + 1) * N / 20))) g;"; done)" ;;
yb) IMG=docker.io/yugabytedb/yugabyte:latest; MNT=/yb; CMD="bin/yugabyted start --base_dir=/yb --advertise_address=127.0.0.1 --background=false"
  : > $ENVF
  sql() { podman exec -i $1 bin/ysqlsh -h 127.0.0.1 -U yugabyte -v ON_ERROR_STOP=1 -At -q; }
  OK="select 'ok';"; COUNT="select count(*) || '|' || sum(n) from t;"
  WRITE="update t set n = n + 1 where id <= 100000;"
  LOAD="create table t (id bigint primary key, v text, n int);
$(for k in $(seq 0 19); do echo "insert into t select g, repeat('x', 80), 0 from generate_series($((k * N / 20 + 1)), $(((k + 1) * N / 20))) g;"; done)" ;;
*) echo "unknown engine $E"; exit 2 ;;
esac
C=lfc-$E
ready() { local i; for i in $(seq $TMO); do echo "$OK" | sql $1 2>/dev/null | grep ok > /dev/null && return 0
          [ "$(podman inspect -f '{{.State.Running}}' $1 2>/dev/null)" = true ] || { echo "  container stopped:"; podman logs $1 2>&1 | tail -5 | sed 's/^/    /'; return 1; }; sleep 1; done
          echo "  not ready after $TMO s:"; podman logs $1 2>&1 | tail -5 | sed 's/^/    /'; return 1; }
run() { podman rm -f $1 > /dev/null 2>&1; podman run -d --name $1 --label lfc.engine=$E --label lfc.purpose=branch-test --hostname lfc-$E $EXTRA --env-file $ENVF -v $2:$MNT $IMG $CMD > /dev/null && ready $1; }
stop() { [ $E = db2 ] && podman exec $1 su - db2inst1 -c "db2 force applications all; db2 terminate; db2stop force" > /dev/null 2>&1 && { podman stop -t 5 $1 > /dev/null 2>&1; return 0; }; [ $E = mssql ] && echo SHUTDOWN | sql $1 > /dev/null 2>&1; [ $E = crdb ] && podman exec $1 ./cockroach node drain --self --insecure --shutdown > /dev/null 2>&1; podman stop -t 120 $1 > /dev/null; }
count() { local i got; for i in $(seq 120); do got=$(echo "$COUNT" | sql $1 2>/dev/null | tr -d ' \r' | grep '|') && { echo "$got"; return 0; }; sleep 1; done; return 1; }   # retries while the database recovers
check() { local got; got=$(count $1); [ "$got" = "$2" ] && echo "  ok: $got" && return 0
          echo "  WRONG: got '$got', want '$2':"; echo "$COUNT" | sql $1 2>&1 | tail -3 | sed 's/^/    /'; return 1; }
owner() { stat -c %u:%g $1; }
dirs_save() { (cd $1 && find . \( -type d -o -perm /7000 \) -printf '%m %U %G %P\n' > .lfc-dirs); }   # object storage keeps no directory owners or modes, nor setuid/setgid/sticky bits
dirs_restore() { (cd $1 && while read -r m u g p; do chown $u:$g "./$p" && chmod $m "./$p"; done < .lfc-dirs); }
flush() { while find $JC -path '*rawstaging*' -type f 2>/dev/null | grep -q .; do sleep 2; done; }   # wait until writeback has uploaded
mount_jfs() { mountpoint -q $J && return 0; podman start jfs-redis > /dev/null; sleep 1; set -a; . /etc/juicefs/azure.env; set +a
  juicefs mount -d --writeback --cache-dir $JC --cache-size 2048 -o allow_other --log /var/log/juicefs.log redis://127.0.0.1:6379/1 $J 2>&1 | grep -v 'latency to database' | tail -1; }
cold() {                                  # stop, drop every cache, start, time a full count
  stop $1; sync; echo 3 > /proc/sys/vm/drop_caches; podman start $1 > /dev/null; ready $1 || return 1
  tc $1 "cold full count"; tc $1 "warm full count"; }
tc() { local s=$(date +%s.%N) got; got=$(count $1); echo "  [$(echo "$(date +%s.%N) - $s" | bc | cut -c1-6) s] $2 ($got)"; }
echo "=== $E $2 ($(date -u +%H:%M:%S))"
case $2 in
init)
  podman image exists $IMG || t podman pull -q $IMG > /dev/null
  U=$(podman run --rm --entrypoint id $IMG -u 2>/dev/null); echo "  image user uid: $U"
  rm -rf "${D:?}/m-${E:?}"; mkdir -p $D/m-$E; chown $U $D/m-$E
  t run $C $D/m-$E || exit 1
  echo "  load $N rows:"; echo "$LOAD" | t sql $C > /tmp/load-$E.out 2>&1 || { tail -5 /tmp/load-$E.out; exit 1; }
  check $C "$N|0"; stop $C; podman rm $C > /dev/null
  du -sh $D/m-$E | cut -f1 | sed 's/^/  master size: /'; echo "  files: $(find $D/m-$E -type f | wc -l), empty dirs: $(find $D/m-$E -type d -empty | wc -l)" ;;
rclone)
  dirs_save $D/m-$E; echo "  upload:"; t rclone sync $D/m-$E az:masters/$E $F -q || exit 1
  rm -rf "${D:?}/cache/${E:?}"; mkdir -p $D/cache
  echo "  pull:"; t rclone copy az:masters/$E $D/cache/$E $F -q || exit 1
  [ "${RESTORE:-1}" = 1 ] && { echo "  restore directory owners and modes:"; t dirs_restore $D/cache/$E; }
  diff <(cd $D/m-$E && find . -printf '%y %m %u %p\n' | sort) <(cd $D/cache/$E && find . -printf '%y %m %u %p\n' | sort) > /tmp/diff-$E.txt \
    && echo "  pulled copy matches (types, modes, owners)" || { echo "  pulled copy differs in $(wc -l < /tmp/diff-$E.txt) lines, e.g.:"; head -4 /tmp/diff-$E.txt | sed 's/^/    /'; }
  rm -rf "${D:?}/run-${E:?}"; echo "  reflink clone:"; t cp -a --reflink=always $D/cache/$E $D/run-$E
  echo "  start the clone as pulled:"
  if ! { t run $C $D/run-$E && check $C "$N|0"; }; then stop $C 2>/dev/null; echo "  retry after chown -R $(owner $D/m-$E):"
    chown -R $(owner $D/m-$E) $D/run-$E; t run $C $D/run-$E && check $C "$N|0" || exit 1; fi
  { echo "$WRITE" | sql $C > /tmp/w.out 2>&1 || { echo "  write failed:"; tail -3 /tmp/w.out; }; }; check $C "$N|100000"
  cold $C; stop $C
  echo "  a save of this clone would upload:"; rclone sync $D/run-$E az:masters/$E $F --dry-run -v 2>&1 | grep -E 'Transferred:' | tail -2 | sed 's/^/    /'
  podman rm $C > /dev/null; t rm -rf "${D:?}/run-${E:?}"; df -h $D | tail -1 | awk '{print "  disk used: " $3}' ;;
jfs)
  mount_jfs; mkdir -p $J/masters $J/clones; rm -rf "${J:?}/masters/${E:?}" "${J:?}/clones/${E:?}"
  echo "  copy the master into JuiceFS:"; t cp -a $D/m-$E $J/masters/$E; echo "  wait for writeback upload:"; t flush
  echo "  juicefs clone:"; t juicefs clone -p $J/masters/$E $J/clones/$E
  echo "  start the clone:"; t run $C $J/clones/$E || { grep -iE 'direct|einval|not supported|error' /var/log/juicefs.log | tail -3; exit 1; }
  check $C "$N|0"; { echo "$WRITE" | sql $C > /tmp/w.out 2>&1 || { echo "  write failed:"; tail -3 /tmp/w.out; }; }; check $C "$N|100000"
  stop $C; flush; umount $J; rm -rf "${JC:?}"/*; mount_jfs; echo "  (JuiceFS cache emptied)"; cold $C; stop $C
  podman rm $C > /dev/null; echo "  discard:"; t rm -rf "${J:?}/clones/${E:?}"
  echo "  a new clone has none of the discarded write:"; juicefs clone -p $J/masters/$E $J/clones/$E-2 > /dev/null
  run $C $J/clones/$E-2 && check $C "$N|0"; stop $C; podman rm $C > /dev/null; rm -rf "${J:?}/clones/${E:?}-2" ;;
reflink)
  mkdir -p $D/cache
  if [ ! -d $D/cache/$E ]; then echo "  pull from Blob:"; t rclone copy az:masters/$E $D/cache/$E $F -q || exit 1
    if [ -f $D/cache/$E/.lfc-dirs ]; then dirs_restore $D/cache/$E
    else chown -R $(stat -c %u:%g "$(find $D/cache/$E -type f ! -name .lfc-dirs | head -1)") $D/cache/$E; fi; fi   # older masters: no .lfc-dirs
  sum() { (cd $1 && find . -type f ! -name .lfc-dirs -print0 | sort -z | xargs -0 sha256sum | sha256sum | cut -c1-16); }
  before=$(sum $D/cache/$E); echo "  master checksum before: $before ($(du -sh $D/cache/$E | cut -f1))"
  used0=$(df --output=used -B1M $D | tail -1)
  rm -rf "${D:?}/run-${E:?}"; echo "  reflink clone:"; t cp -a --reflink=always $D/cache/$E $D/run-$E
  echo "  disk used by the clone: $(( $(df --output=used -B1M $D | tail -1) - used0 )) MB"
  t run $C $D/run-$E || exit 1; check $C "$N|0"
  { echo "$WRITE" | sql $C > /tmp/w.out 2>&1 || { echo "  write failed:"; tail -3 /tmp/w.out; }; }; check $C "$N|100000"
  stop $C; podman rm $C > /dev/null
  after=$(sum $D/cache/$E); [ "$after" = "$before" ] && echo "  master unchanged: $after" || echo "  MASTER CHANGED: $after"
  rm -rf "${D:?}/run-${E:?}-2"; cp -a --reflink=always $D/cache/$E $D/run-$E-2
  echo "  a second clone has none of the write:"; run $C $D/run-$E-2 && check $C "$N|0"; stop $C; podman rm $C > /dev/null
  rm -rf "${D:?}/run-${E:?}" "${D:?}/run-${E:?}-2" ;;
esac
