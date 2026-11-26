# Per-engine Podman startup details

This file contains detailed startup commands, environment variables, readiness timing, and memory/CPU requirements for each supported database engine.

> **Passwords:** each engine below reads its password from a private env file, never from the command line (where it would show in the process list and in `podman inspect`). Create both files once per engine, for example for MySQL:
>
> ```bash
> umask 077
> echo "Lfc1$(openssl rand -hex 12)" > mysql.pw                                  # for SQL clients (MYSQL_PWD / PGPASSWORD / SQLCMDPASSWORD)
> printf 'MYSQL_ROOT_PASSWORD=%s\n' "$(cat mysql.pw)" > mysql.env   # for podman run --env-file (printf is a shell builtin)
> ```
>
> The variable in each `<engine>.env` file: MySQL `MYSQL_ROOT_PASSWORD`, PostgreSQL `POSTGRES_PASSWORD`, SQL Server `MSSQL_SA_PASSWORD`, Oracle `ORACLE_PASSWORD`, MariaDB `MARIADB_ROOT_PASSWORD`, Db2 `DB2INST1_PASSWORD`. Delete both files when you tear the container down.

## MySQL (8.0, 8.4)

**Environment:**
- `MYSQL_ROOT_PASSWORD` — admin password (required; set securely via password file)

**Volume path:** `/var/lib/mysql`

**CPU/Memory:** 2 CPUs, 2 GiB

**Startup:**

```bash
# mysql.pw and mysql.env: as in the note at the top (a generated password, mode 600)

# Run container with password from file
podman run -d \
  --name jane-doe-mysql-8.0-dockerhub-host-local-812 \
  --cpus 2 --memory 2g \
  -p 30001:3306 \
  --env-file mysql.env \
  -v jane-doe-mysql-8.0-dockerhub-host-local-812-data:/var/lib/mysql \
  docker.io/library/mysql:8.0
```

**Readiness:** MySQL accepts TCP connections on port 3306 and responds to `SELECT 1`.

## PostgreSQL (14, 15, 16, 17)

**Environment:**
- `POSTGRES_PASSWORD` — `postgres` user password (required; set securely via password file)
- `POSTGRES_INITDB_ARGS` — init args (optional)

**Volume path:** `/var/lib/postgresql/data`

**CPU/Memory:** 2 CPUs, 2 GiB

**Startup:**

```bash
# postgres.pw and postgres.env: as in the note at the top (a generated password, mode 600)

# Run container with password from file
podman run -d \
  --name jane-doe-pg-16-dockerhub-host-local-812 \
  --cpus 2 --memory 2g \
  -p 30002:5432 \
  --env-file postgres.env \
  -v jane-doe-pg-16-dockerhub-host-local-812-data:/var/lib/postgresql/data \
  docker.io/library/postgres:16
```

**Readiness:** PostgreSQL responds to `pg_isready` health check; verify with `pg_isready -h 127.0.0.1 -p 30002`.

## SQL Server (2017, 2019, 2022, 2025)

> **Tested on macOS:** SQL Server is x86_64-only; on Apple Silicon it runs under Rosetta 2 in the Lima VM. The macOS Homebrew `sqlcmd` reaches it at `127.0.0.1,30003`.

**Image:** `mcr.microsoft.com/mssql/server:{version}-latest` or specific tag (e.g., `2025-CU7-ubuntu-24.04`)

**Environment:**
- `ACCEPT_EULA=Y` — accept the license (required)
- `MSSQL_SA_PASSWORD` — `sa` password (required; minimum 8 chars, uppercase + digit + symbol)

**Volume path:** `/var/opt/mssql` (database files)

**CPU/Memory:** 4 CPUs, 4 GiB minimum

**Startup:**

```bash
# The env file the run reads: a generated password (meets SQL Server's rules), readable only by you
(umask 077; printf 'ACCEPT_EULA=Y\nMSSQL_SA_PASSWORD=Lfc1%s\n' "$(openssl rand -hex 12)" > mssql.env)

# Run container with password from file
podman run -d \
  --name jane-doe-mssql-2022-mcr-host-local-812 \
  --cpus 4 --memory 4g \
  -p 30003:1433 \
  --env-file mssql.env \
  -v jane-doe-mssql-2022-mcr-host-local-812-data:/var/opt/mssql \
  mcr.microsoft.com/mssql/server:2022-latest
```

Keep `mssql.env`: it's how you (and scripts) get the password later, without it ever appearing on a command line.

**Readiness timing:**
- Native amd64 Linux: ~4 s first start, ~90 s timeout
- Rosetta2 (macOS, Lima): 7 s from start to answering a query (tested: 2022 CU27, 16.0.4305.5, `x86_64` inside); the first pull of the 1.7 GB image took 111 s; ~300 s timeout

**SQL Server Always On Availability Groups (Podman only):** Setup involves three containers on a shared Podman network: two replicas (primary + secondary) and a Linux-based load-balancer sidecar. Requires network creation, multi-node cluster configuration, and T-SQL setup for availability group membership. Consult the Lakeflow Connect database connectors reference for current support.

## Oracle (18c, 19c, 21c, 23ai, 26ai)

> **Unverified:** Container arch varies by source. First-run database creation can take 5–60 min depending on edition and VM.

**Image source options:**

- **gvenzl (default):** Free/XE editions, no login required
  - 18c, 21c XE: amd64-only; runs under Rosetta2 on macOS
  - 23ai, 26ai Free: arm64 + amd64; uses native arm64 on Apple Silicon

- **oci:** Official Oracle Enterprise/Standard (login required: `podman login container-registry.oracle.com`)
  - Most versions are x86_64-only; 19c Enterprise is ARM64-only
  - First run: 20–60 min (DBCA creates database from scratch)

**CDB or non-CDB:**
- `gvenzl/oracle-free` (23ai/26ai) and `gvenzl/oracle-xe:18` / `:21` are CDB-only: the CDB plus one PDB (`FREEPDB1` / `XEPDB1`), and `ORACLE_DATABASE` adds more PDBs, never a non-CDB.
- The one gvenzl non-CDB image is `gvenzl/oracle-xe:11` (11.2.0.2, "11g does not support pluggable databases"), older than the connector's oldest version, 12c.
- A non-CDB that the connector supports needs 12c to 19c, since 19c is the last release that allows a non-CDB. That means the OCI Enterprise image with DBCA `createAsContainerDatabase=false` (not tested).

**Image examples:**
- `docker.io/gvenzl/oracle-xe:21` (gvenzl 21c XE)
- `docker.io/gvenzl/oracle-free:latest` (gvenzl 23ai/26ai Free)
- `container-registry.oracle.com/database/enterprise:19.3.0.0` (OCI 19c Enterprise)

**Environment:**
- `ORACLE_PASSWORD` (gvenzl) or `ORACLE_PWD` (OCI): admin password (required; max 30 bytes for Enterprise)
- `ENABLE_ARCHIVELOG=false` (OCI Enterprise): skip ARCHIVELOG in DBCA

**Extra flags (OCI Enterprise):**
```
--ulimit nofile=1024:65536 --ulimit nproc=2047:16384 \
--ulimit stack=10485760:33554432 --ulimit memlock=3221225472
```

**Volume path:** `/opt/oracle/oradata`

**CPU/Memory:** 2 CPUs, 4 GiB; `--shm-size 3g`

**Startup (gvenzl 26ai Free, arm64 native):**

```bash
# The env file the run reads: generated passwords, readable only by you (APP_USER: an ordinary user in FREEPDB1)
(umask 077; printf 'ORACLE_PASSWORD=Lfc1%s\nAPP_USER=lfc\nAPP_USER_PASSWORD=Lfc1%s\n' "$(openssl rand -hex 12)" "$(openssl rand -hex 12)" > oracle.env)

# Run container with password from file
podman run -d \
  --name jane-doe-ora-26ai-gvenzl-host-local-812 \
  --cpus 2 --memory 4g \
  --shm-size 3g \
  -p 30004:1521 \
  --env-file oracle.env \
  -v jane-doe-ora-26ai-gvenzl-host-local-812-data:/opt/oracle/oradata \
  docker.io/gvenzl/oracle-free:latest
```

**Readiness:**
- Fresh start, gvenzl Free: the image ships a created database, so 15 s from start to `DATABASE IS READY TO USE` (tested: `oracle-free:23-slim` on arm64 in Lima, which now reports `26ai Free Release 23.26.3`; the 0.9 GB pull took 23 s)
- Fresh start, Enterprise or XE under Rosetta: 5–60 min (DBCA creates the database)
- Restart from snapshot: ~30 s

**PDB/CDB names (for UC connection):**
- gvenzl 26ai Free: CDB=`FREE`, PDB=`FREEPDB1`
- gvenzl 21c XE: CDB=`XE`, PDB=`XEPDB1`
- OCI 19c Enterprise: CDB=`ORCLCDB`, PDB=`ORCLPDB1`

## MariaDB (11.4, 11.6)

**Environment:**
- `MARIADB_ROOT_PASSWORD` — root password (required; set securely via password file)

**Volume path:** `/var/lib/mysql`

**CPU/Memory:** 2 CPUs, 2 GiB

**Startup:**

```bash
# mariadb.pw and mariadb.env: as in the note at the top (a generated password, mode 600)

# Run container with password from file
podman run -d \
  --name jane-doe-mariadb-11.4-dockerhub-host-local-812 \
  --cpus 2 --memory 2g \
  -p 30005:3306 \
  --env-file mariadb.env \
  -v jane-doe-mariadb-11.4-dockerhub-host-local-812-data:/var/lib/mysql \
  docker.io/library/mariadb:11.4
```

## MongoDB (7, 8)

> **Unverified:** Lakeflow Connect CDC uses oplog; requires replica set mode.

**Environment:** None required (no password by default)

**Volume path:** `/data/db`

**CPU/Memory:** 2 CPUs, 2 GiB

**Extra flags for CDC:** `--replSet rs0` to enable replica set

**Startup:**

```bash
podman run -d \
  --name jane-doe-mongo-8-dockerhub-host-local-812 \
  --cpus 2 --memory 2g \
  --replSet rs0 \
  -p 30006:27017 \
  -v jane-doe-mongo-8-dockerhub-host-local-812-data:/data/db \
  docker.io/library/mongo:8

# Initialize replica set (one-time after first start)
podman exec jane-doe-mongo-8-dockerhub-host-local-812 \
  mongosh --eval "rs.initiate()"
```

## Db2 (11.5, 12.1)

> **Tested on macOS** (Apple Silicon, Lima, Rosetta): `docker.io/ibmcom/db2:11.5.8.0` as x86_64, database created and queried, port reachable from the Mac, data kept across a VM restart. It needs **`--privileged`** and **Rosetta registered with the preserve-argv0 flag** (SKILL.md, macOS), or setup fails with `db2: command not found`.

**Image:** `icr.io/db2_community/db2:<version>` (current, 11.5.9 and 12.1) or `docker.io/ibmcom/db2:11.5.8.0` (the last on Docker Hub, 2022). If `icr.io` times out (seen behind a corporate VPN: `pinging container registry icr.io: ... i/o timeout`), use the Docker Hub image, or pull on another network and `podman save` / `podman load` it.

**Environment:**
- `DB2INST1_PASSWORD`: the instance user's password (in `db2.env`)
- `DBNAME`: a database to create at first start (otherwise there is none)
- `LICENSE=accept` (required)

**Volume path:** `/database` (instance home, data, and config; setgid directories and setuid programs, so keep it a named volume)

**CPU/Memory:** 2 CPUs, 4 GiB minimum

**Startup:**

```bash
# db2.pw and db2.env (with DB2INST1_PASSWORD and DBNAME=lfcdb): as in the note at the top
podman run -d \
  --name jane-doe-db2-11.5-dockerhub-host-local-812 \
  --privileged --platform linux/amd64 \
  --cpus 2 --memory 4g \
  -p 30007:50000 \
  --env-file db2.env \
  -e LICENSE=accept \
  -v jane-doe-db2-11.5-dockerhub-host-local-812-data:/database \
  docker.io/ibmcom/db2:11.5.8.0
```

**Readiness:** `(*) Setup has completed.` in the log: 295 s at first start (Lima, Rosetta, 2 CPUs), and it answers 231 s after a restart (setup rechecks everything). Watch the log for `Failed to`: setup keeps going after an error and the container stays up.

**A disk with 4096-byte logical sectors** (`blockdev --getss <device>`; the cloud VM's XFS container disk, Azure Premium SSD v2's default): the image's `CREATE DATABASE` fails with `SQL0293N Error accessing a table space container.`, and setup still ends with `(*) Setup has completed.`, so a script that waits only for that line waits on a database that isn't there. Start without `DBNAME`, then, as `db2inst1`: `db2set DB2_4K_DEVICE_SUPPORT=ON`, `db2stop force; db2start`, `db2 create database LFCDB` (archive logging, if change capture needs it, is then yours to set: `db2 update db cfg for LFCDB using LOGARCHMETH1 DISK:<folder>`). Check `db2 connect to LFCDB` before going on. lfc-source-cloud-vm's `slot.sh` (`db2_db`) and zerobus-debezium-test do this; details in lfc-source-cloud-vm `references/disk-setup.md`.

**Query** (every `db2` call in one CLP session, from a file: separate `db2` commands don't share a connection):

```bash
podman exec <name> bash -c 'su - db2inst1 -c "printf \"CONNECT TO lfcdb;\nVALUES CURRENT SERVER;\n\" > /tmp/q.sql; db2 -x -tf /tmp/q.sql"'
```

## Informix (14.10)

> **Tested on macOS** (Apple Silicon, Lima, Rosetta): `docker.io/ibmcom/informix-developer-database:14.10.FC7W1DE` as x86_64: On-Line in 40 s at first start and in 22 s when recreated on its volumes, queried with `dbaccess`, port reachable from the Mac.

**Image:** `icr.io/informix/informix-developer-database:latest` (current) or `docker.io/ibmcom/informix-developer-database:14.10.FC7W1DE` (the last on Docker Hub, 2022); `icr.io` as for Db2.

**Environment:**
- `LICENSE=accept` (required)

**Volume paths:** **`/opt/ibm/data`** (the dbspaces: `rootdbs.000` and the rest) and `/opt/ibm/config`, the image's declared volumes, mounted with **`:U`** (a new named volume is owned by root and the server runs as `informix`: without it, `init_status.log: Permission denied` and the server stays offline). Not `/opt/informix/data`: nothing is written there, so with only that mounted the data sits in anonymous volumes and is gone with the container. Tested: a table written, the container removed and run again on the same volumes, On-Line in 22 s with the row there.

**CPU/Memory:** 2 CPUs, 2 GiB minimum

**Startup:**

```bash
podman run -d \
  --name jane-doe-ifx-14.10-dockerhub-host-local-812 \
  --platform linux/amd64 \
  --cpus 2 --memory 2g \
  -p 30008:9088 \
  -e LICENSE=accept \
  -v jane-doe-ifx-14.10-dockerhub-host-local-812-data:/opt/ibm/data:U \
  -v jane-doe-ifx-14.10-dockerhub-host-local-812-config:/opt/ibm/config:U \
  docker.io/ibmcom/informix-developer-database:14.10.FC7W1DE
```

**Readiness:** `On-Line Mode` in the log, or the container's health check (`podman ps` shows `healthy`).

**Query:** `podman exec <name> bash -lc 'echo "select dbinfo(\"version\",\"full\") from sysmaster:sysdual;" | dbaccess sysmaster - 2>&1'`. `dbaccess` writes its messages (`1 row(s) retrieved.`) to stderr: a readiness check that drops stderr never sees them.

## CockroachDB (25.x)

> Verified on an Azure VM (amd64, Podman 4.9) with `cockroachdb/cockroach:latest`. Lakeflow Connect has no CockroachDB connector; use it as a Postgres-wire test target.

**Environment:** none. `--insecure` has no passwords; keep the port unpublished, or bound to 127.0.0.1, and use `podman exec`.

**Volume path:** `/cockroach/cockroach-data` (runs as root)

**CPU/Memory:** 2 CPUs, 4 GiB (give `--cache` and `--max-sql-memory` about a quarter of the memory each)

**Startup:**

```bash
podman run -d \
  --name jane-doe-crdb-25-dockerhub-host-local-812 \
  --cpus 2 --memory 4g \
  -p 127.0.0.1:30009:26257 \
  -v jane-doe-crdb-25-dockerhub-host-local-812-data:/cockroach/cockroach-data \
  docker.io/cockroachdb/cockroach:latest start-single-node --insecure --cache=1GiB --max-sql-memory=1GiB
podman exec -i jane-doe-crdb-25-dockerhub-host-local-812 ./cockroach sql --insecure <<< "SELECT 1"
```

**Readiness:** about 3 s.

**Stop:** `podman exec <name> ./cockroach node drain --self --insecure --shutdown`, then `podman stop`. A plain SIGTERM after a large load can take over 120 s and end in SIGKILL.

**Loading:** insert in batches (for example 250,000 rows per `INSERT ... SELECT ... FROM generate_series(...)`); one huge transaction is slow.

## YugabyteDB (2.25, 2025.x)

> Verified on an Azure VM (amd64, Podman 4.9) with `yugabytedb/yugabyte:latest`. Postgres-compatible (YSQL), so the Postgres datagen scripts mostly apply.

**Environment:** none. Without `--secure`, user `yugabyte` has no password; keep the port unpublished, or bound to 127.0.0.1.

**Volume path:** whatever `--base_dir` points at, for example `/yb`. The image runs as **uid 10001** (`yugabyte`, group `users` 100), so a host directory must be owned by 10001.

**CPU/Memory:** 2 CPUs, 4 GiB

**Startup:**

```bash
mkdir -p yb-data && sudo chown 10001:100 yb-data       # or a named volume
podman run -d \
  --name jane-doe-yb-2025-dockerhub-host-local-812 \
  --cpus 2 --memory 4g \
  -p 127.0.0.1:30010:5433 \
  -v $PWD/yb-data:/yb \
  docker.io/yugabytedb/yugabyte:latest \
  bin/yugabyted start --base_dir=/yb --advertise_address=127.0.0.1 \
    --tserver_flags=pgsql_proxy_bind_address=0.0.0.0:5433 --background=false
podman exec -i jane-doe-yb-2025-dockerhub-host-local-812 bin/ysqlsh -h 127.0.0.1 -U yugabyte -c "SELECT 1"
```

- `--background=false` keeps yugabyted in the foreground; without it the container exits.
- `--advertise_address=127.0.0.1` is stored in the data directory. Pin it, so a copy of the directory still starts in another container with a different IP.
- With loopback advertised, YSQL listens on `127.0.0.1` only, so the published port is refused: `pgsql_proxy_bind_address=0.0.0.0:5433` binds it to every interface (lfc-source-cloud-vm `references/engine-masters.md`, "YugabyteDB's start flags").
- The data directory holds **symlinks** (`data/pg_data`, the log directories); anything that copies it must keep them.

**Readiness:** about 15 s first start, about 6 s restart.
