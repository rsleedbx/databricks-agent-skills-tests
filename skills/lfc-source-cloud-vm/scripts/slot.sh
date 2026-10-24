#!/usr/bin/env bash
# slot.sh [--owner <owner>] <command> [args]: test databases for several people on one VM, one container each, with the
# data on JuiceFS (object storage) so a stopped database costs no disk or CPU. Run as root on the VM.
#   masters                                   the masters a database can start from ($ROOT/masters)
#   ls [--all]                                your databases (--all: everyone's): engine, state, port, created, last up
#   up <engine> <name> [--from <master> | --empty]
#                                             start <name> (postgres | mysql | sqlserver): resume it if it exists, else
#                                             clone a master (juicefs clone) or start empty. Parks your other running
#                                             database first when you're at MAX_PER_OWNER. Prints one JSON line:
#                                             name, engine, host, port, user, password, container, state
#   park <name>                               stop it; the data stays on JuiceFS
#   rm <name>                                 stop it and delete its data
#   usage [--days <n>] [--all]                hours running per owner in the last n days (default 7), from the ledger
# --owner: who the database belongs to. A forced SSH command sets it per key (see the skill), so a user can't pick
# another owner; as root without --owner the owner is "admin".
# Settings, from /etc/lfc-slot.env if present: HOST (address printed for connections; set it on Azure, see host()),
# ROOT (/mnt/jfs), STATE (/var/lib/lfc/slots), PORT_MIN/PORT_MAX (30000/30063), MAX_RUNNING (vCPUs / 2),
# MAX_PER_OWNER (1; 0 = no limit, for a personal VM), SECRETS (/root/lfc-src: the admin env files tpc-master.sh used).
set -euo pipefail
[ -r /etc/lfc-slot.env ] && . /etc/lfc-slot.env
ROOT=${ROOT:-/mnt/jfs} STATE=${STATE:-/var/lib/lfc/slots} SECRETS=${SECRETS:-/root/lfc-src}
PORT_MIN=${PORT_MIN:-30000} PORT_MAX=${PORT_MAX:-30063} MAX_PER_OWNER=${MAX_PER_OWNER:-1}
MAX_RUNNING=${MAX_RUNNING:-$(( $(nproc) / 2 ))}
usage() { sed -n 2,20p "$0" >&2; exit 2; }
die() { echo "slot: $*" >&2; exit 1; }
OWNER="admin"
if [ "${1:-}" = --owner ]; then OWNER=${2:-}; shift 2; fi
[[ $OWNER =~ ^[a-z0-9][a-z0-9-]{0,31}$ ]] || die "bad owner: $OWNER"
[ $# -ge 1 ] || usage
CMD=$1; shift
install -d -m 700 "$STATE" "$STATE/$OWNER"
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

# engine: image, data path in the container, port in the container, extra args
engine() {
  case $1 in
    postgres) IMAGE=docker.io/library/postgres:16 MNT=/var/lib/postgresql/data CPORT=5432
              ARGS=(-c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10) RUNOPTS=(--shm-size 1g) ;;
    mysql)    IMAGE=docker.io/library/mysql:8.4 MNT=/var/lib/mysql CPORT=3306
              ARGS=(--binlog-format=ROW --binlog-row-image=FULL --binlog-expire-logs-seconds=604800) RUNOPTS=() ;;
    sqlserver) IMAGE=mcr.microsoft.com/mssql/server:2022-latest MNT=/var/opt/mssql CPORT=1433
              ARGS=() RUNOPTS=(--memory 6g -e MSSQL_MEMORY_LIMIT_MB=5120 -e MSSQL_AGENT_ENABLED=true -e MSSQL_PID=Developer) ;;
    *) die "engine is postgres, mysql or sqlserver: $1" ;;
  esac
}
# sql <container> <engine> <envfile>: run stdin as admin; ready <container> <engine>: 0 when it answers
sql() {
  case $2 in
    postgres) podman exec -i -e PGOPTIONS="-c client_min_messages=warning" "$1" psql -v ON_ERROR_STOP=1 -q -U postgres -d postgres ;;
    mysql) podman exec -i "$1" sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot' ;;
    sqlserver) podman exec -i "$1" sh -c 'f=$(mktemp); cat > $f; /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -b -i $f; r=$?; rm -f $f; exit $r' ;;
  esac
}
ready() { echo "SELECT 1;" | sql "$1" "$2" >/dev/null 2>&1; }
login_sql() {   # engine, password: the Lakeflow Connect login with replication rights; databases and CDC are per test
  local p=${2//\'/\'\'}
  case $1 in
    postgres) printf "DO \$\$BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='lfc_user') THEN CREATE ROLE lfc_user LOGIN REPLICATION; END IF; END\$\$;\nALTER ROLE lfc_user WITH LOGIN REPLICATION PASSWORD '%s';\nGRANT pg_read_all_data TO lfc_user;\n" "$p" ;;
    mysql) printf "CREATE USER IF NOT EXISTS 'lfc_user'@'%%' IDENTIFIED BY '%s';\nALTER USER 'lfc_user'@'%%' IDENTIFIED BY '%s';\nGRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'lfc_user'@'%%';\n" "$p" "$p" ;;
    sqlserver) printf "IF SUSER_ID('lfc_user') IS NULL CREATE LOGIN [lfc_user] WITH PASSWORD = N'%s', CHECK_POLICY = OFF; ELSE ALTER LOGIN [lfc_user] WITH PASSWORD = N'%s', CHECK_POLICY = OFF;\n" "$p" "$p" ;;
  esac
}
all_metas() { find "$STATE" -mindepth 2 -maxdepth 2 -name '*.json' 2>/dev/null; }
field() { jq -r ".$2 // empty" "$1"; }

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
    exec 9>"$STATE/.lock"; flock 9
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
        die "the VM is full: $run_now of $MAX_RUNNING databases are running ($(podman ps --format '{{.Names}}' | grep '^lfc-slot-' | sed 's/^lfc-slot-//' | tr '\n' ' ')); try later, or ask for a VM of your own"
      fi
      if [ ! -f "$M" ]; then   # a new database: its data, admin env, login password and port
        [ -n "$FROM" ] || [ $EMPTY = 1 ] || die "new database: --from <master> or --empty (slot.sh masters)"
        [ -d "$D" ] && die "$D exists without a record; remove it or pick another name"
        install -d -m 755 "$ROOT/dbs/$OWNER"
        umask 077
        if [ -n "$FROM" ]; then
          [[ $FROM =~ ^[A-Za-z0-9._-]+$ ]] && [ -d "$ROOT/masters/$FROM" ] || die "no master $FROM (slot.sh masters)"
          [ -f "$SECRETS/$E.env" ] || die "no admin env $SECRETS/$E.env for the $E masters"
          juicefs clone --preserve "$ROOT/masters/$FROM" "$D" >/dev/null 2>&1 || cp -a --reflink=auto "$ROOT/masters/$FROM" "$D"
          cp "$SECRETS/$E.env" "$STATE/$OWNER/$N.env"
        else
          install -d "$D"
          [ "$E" = sqlserver ] && chown 10001:0 "$D"   # SQL Server runs as mssql (uid 10001)
          case $E in
            postgres) printf 'POSTGRES_PASSWORD=%s\n' "$(pw)" ;;
            mysql) printf 'MYSQL_ROOT_PASSWORD=%s\n' "$(pw)" ;;
            sqlserver) printf 'ACCEPT_EULA=Y\nMSSQL_SA_PASSWORD=%s\n' "$(pw)" ;;
          esac > "$STATE/$OWNER/$N.env"
        fi
        pw > "$STATE/$OWNER/$N.login"
        used=$(for m in $(all_metas); do field "$m" port; done | sort -n)
        P=""; for p in $(seq "$PORT_MIN" "$PORT_MAX"); do grep -qx "$p" <<<"$used" || { P=$p; break; }; done
        [ -n "$P" ] || die "no free port in $PORT_MIN-$PORT_MAX"
        jq -n --arg e "$E" --arg p "$P" --arg c "$(now)" --arg f "${FROM:-empty}" --arg i "$IMAGE" \
          '{engine: $e, port: ($p | tonumber), created: $c, from: $f, image: $i}' > "$M"
        umask 022
      fi
      P=$(field "$M" port)
      podman rm -f "$C" >/dev/null 2>&1 || true
      podman run -d --name "$C" --label lfc-slot.owner="$OWNER" --label lfc-slot.name="$N" "${RUNOPTS[@]}" \
        --env-file "$STATE/$OWNER/$N.env" -p "$P:$CPORT" -v "$D:$MNT:Z" "$IMAGE" "${ARGS[@]}" >/dev/null 9>&-   # the container must not inherit the lock
      for _ in $(seq 1 90); do ready "$C" "$E" && break; sleep 2; done
      ready "$C" "$E" || die "$E in $C didn't answer in 3 minutes: podman logs $C"
      login_sql "$E" "$(cat "$STATE/$OWNER/$N.login")" | sql "$C" "$E" >/dev/null || die "creating lfc_user in $C failed"
      log "$N" up
    fi
    P=$(field "$M" port)
    jq -cn --arg n "$N" --arg e "$E" --arg h "$(host)" --arg p "$P" --rawfile pw "$STATE/$OWNER/$N.login" --arg c "$C" \
      '{name: $n, engine: $e, host: $h, port: ($p | tonumber), user: "lfc_user", password: ($pw | rtrimstr("\n")), container: $c, state: "running"}' ;;

  park)
    N=${1:-}; name_ok "$N"; [ -f "$(meta "$OWNER" "$N")" ] || die "no database $N"
    exec 9>"$STATE/.lock"; flock 9
    if running "$OWNER" "$N"; then park_one "$OWNER" "$N"; echo "parked $N: its data stays in $ROOT/dbs/$OWNER/$N"; else echo "$N is already parked"; fi ;;

  rm)
    N=${1:-}; name_ok "$N"; M=$(meta "$OWNER" "$N"); [ -f "$M" ] || die "no database $N"
    exec 9>"$STATE/.lock"; flock 9
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

  *) usage ;;
esac
