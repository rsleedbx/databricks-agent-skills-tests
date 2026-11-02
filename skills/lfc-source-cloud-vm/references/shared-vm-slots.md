# Test databases for several people on one VM

`scripts/slot.sh` runs on a VM with a JuiceFS mount ([juicefs-branches.md](juicefs-branches.md)) and gives each person their own Postgres, MySQL or SQL Server container. A database starts empty or as a `juicefs clone -p` of a master ([engine-masters.md](engine-masters.md), `scripts/tpc-master.sh`). Its data lives on JuiceFS. A parked database has no container and no CPU or memory in use, and its data stays in object storage. `scripts/slot-ssh.sh` ties each person's SSH key to their owner name.

## Commands

On the VM as root (`slot.sh [--owner <owner>] <command>`), or from a laptop through the person's key (`ssh slot@<vm> <command>`):

| Command | Does |
|---|---|
| `masters` | lists the masters (`$ROOT/masters`) |
| `up <engine> <name> --from <master>` | clones the master and starts it |
| `up <engine> <name> --empty` | starts an empty database |
| `up <engine> <name>` | resumes a parked database (no change if it's running) |
| `park <name>` | stops the container; the data stays |
| `clone <name> <new name>` | copies your database to a new parked one (`up` starts it, with its own port and `lfc_user` password): `juicefs clone` on a JuiceFS volume, `cp --reflink=always` on XFS, so blocks are shared and it takes seconds; a running database is stopped for the copy and started again |
| `rm <name>` | stops it and deletes its data |
| `ls [--all]` | your databases (`--all`: everyone's): engine, state, port, created, from |
| `usage [--days n] [--all]` | hours running per owner and database in the last n days (default 7) |
| `sql <name> [database] [--wide]` | runs SQL from stdin as the engine's admin in your running database (CDC, change tracking, test tables): tab-separated rows, no headers, the SQL's exit status; `--wide`: SQL Server long values whole. The harness's `SRC_SQL` is `lfc-schema-change-test/scripts/slot-sql.sh` |
| `info <name>` | the host OS, Podman, the image and digest, and the VM (`slot-sql.sh --env`, the harness's `SRC_ENV`) |

Engines: `postgres` (16), `mysql` (8.4), `sqlserver` (2022), `oracle` (`gvenzl/oracle-free:23-slim`: CDB `FREE`, PDB `FREEPDB1`), `mariadb` (11.4), `db2` (Community 11.5.9: database `LFCDB`), `informix` (Developer 14.10: `lfcdb`), `cockroachdb` (v25.4, insecure: `lfc`), `yugabytedb` (2026.1, insecure: `lfc`). Masters exist for the first three; a master works with its own engine only.

`up` prints one JSON line:

```json
{"name":"pg1","engine":"postgres","host":"203.0.113.10","port":30000,"user":"lfc_user","password":"...","container":"lfc-slot-alice-pg1","state":"running"}
```

`service` is added for Oracle only: `FREEPDB1`, the PDB `lfc_user` lives in (the UC connection's `service_name`). `database` is added where a test uses one: `LFCDB`, `lfcdb`, `lfc`, `lfc`.

`lfc_user` is created on every start, with the rights Lakeflow Connect reads with: Postgres `LOGIN REPLICATION` and `pg_read_all_data`; MySQL `SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT`; SQL Server a login and, in every user database, a user in `db_datareader` with `VIEW DATABASE STATE` and `VIEW DEFINITION` (a database made later gets it at the next start; CDC is enabled per test). Oracle: a PDB-local user in `FREEPDB1` with `CREATE SESSION`, `SELECT ANY TABLE` and `SELECT_CATALOG_ROLE`. MariaDB: the MySQL rights, with MariaDB's default plugin (`mysql_native_password`: it has no `caching_sha2_password`). Db2: an OS user in the container (Db2 checks the OS password, so the printed password is that user's) with `DBADM` on `LFCDB`, the grant in [engine-db2.md](../../lfc-source-setup/references/engine-db2.md). Informix: an OS user too (the server uses PAM) with `CONNECT`, `RESOURCE` and `DBA` on `lfcdb`. CockroachDB: `ALL` on `lfc`, and `SELECT` on its tables, existing and later (default privileges); insecure mode neither sets nor checks a password, so the printed one isn't used. YugabyteDB: Postgres's rights (`LOGIN REPLICATION`, `pg_read_all_data`). The engines start with CDC settings: Postgres `wal_level=logical`; MySQL `binlog_format=ROW`, `binlog_row_image=FULL`; MariaDB its binary log on (`ROW`, `FULL`, a week kept; its image has none by default); SQL Server Agent on.

Db2, Informix, CockroachDB and YugabyteDB get no CDC settings here: each test sets its own (Db2's SQL Replication is in engine-db2.md, not done here). **Oracle's CDC is not set up here.** `lfc_user` reads for query-based ingestion. LogMiner CDC needs what [engine-oracle.md](../../lfc-source-setup/references/engine-oracle.md) lists: ARCHIVELOG and a recovery area (50G), supplemental logging, and the `C##` user from the Databricks setup package, run as SYSDBA in the container.

## Limits

| Setting (`/etc/lfc-slot.env`) | Default | When it's reached |
|---|---|---|
| `MAX_PER_OWNER` | 1 (0 = no limit) | the default for `max_running` (below): `up` stops and names your running databases; `up ... --park` parks the one that started first |
| `MAX_RUNNING` | vCPUs / `CPUS_PER_DB`, at least 1 | `up` stops with `the VM is full` and lists what's running |
| `CPUS_PER_DB` | 2; 1 on a personal VM (`KIND=personal`) | follows a resize: the limit is worked out from `nproc` each time |
| `PORT_MIN`-`PORT_MAX` | 30000-30063 | `no free port`; each database keeps its port until `rm`. A port something else listens on (a container started outside slot.sh) is skipped |
| `HOST` | the cloud metadata's public IP, else the first local IP | set it on Azure: the metadata has no public IP for a Standard SKU address |
| `ROOT`, `STATE`, `SECRETS` | `/mnt/jfs`, `/var/lib/lfc/slots`, `/root/lfc-src` | `STATE` under `ROOT` needs `ROOT` mounted, else the command stops |
| `LOCK` | `$STATE/.lock` | the lock for one command at a time; a personal VM's is on the VM's disk |
| `LIMITS` | `/etc/lfc-slot.limits` | per-owner limits, below |

**Per-owner limits** (`/etc/lfc-slot.limits`, read on every command, so a change needs no new slot.sh): one line per owner, and a `default` line for everyone not listed. A key missing from both lines falls back to the setting (`max_running` to `MAX_PER_OWNER`; `hours_per_week` to the client's own default).

```
default max_running=1 hours_per_week=29
alice max_running=4 hours_per_week=60
```

- `max_running`: how many of your databases may run at once (0 = no limit).
- `hours_per_week`: above it, the client suggests a VM of your own. A suggestion, not enforced.

`slot.sh limits` prints yours: `{"owner": "alice", "max_running": 4, "hours_per_week": 60}`.

A clone of a master uses the master's admin env file (`$SECRETS/<engine>.env`, the one `tpc-master.sh` built it with); an empty database gets a new admin password. Passwords and env files stay in `$STATE/<owner>/` (mode 700, root).

## Masters in object storage, and admin passwords

`scripts/masters-export.sh` (root, on a VM that has the masters) writes each master to a Blob container as `<master>.tar.zst` (tar `--numeric-owner`, zstd), each engine's admin env as `<engine>.env`, and `manifest.json` (`masters: {<name>: {engine, bytes, sha256, files}}`, `envs: [{engine, file, bytes, sha256}]`, `created`). Settings: `MASTERS_URL=https://<account>.blob.core.windows.net/<container>`, `MASTERS=all|<comma list>`, `MASTERS_SAS_FILE` (root-only, mode 600, a SAS with read and write; read from the file, never on a command line). The archive goes to a temp file under `EXPORT_TMP` (default `/var/lib/containers/masters-export`, free space checked first). A master whose blob already has its sha256 isn't uploaded again. Each master's engine comes from `<master>.engine` beside it (`tpc-master.sh` writes it) or `MASTER_ENGINES=name=engine,...`.

`scripts/slot-vm-setup.sh` takes them back: `MASTERS_URL`, `MASTERS` (`all`, a comma list, or `none` for the admin envs only) and `MASTERS_SAS_FILE` (a read SAS, mode 600, deleted when the run ends). Each archive streams through sha256 and zstd into a staging folder, which becomes the master only when the sum matches; a mismatch removes it and stops. Every admin env in the manifest goes to `$SECRETS` (mode 600); a different file already there stops the run. To add masters to a VM that's set up: `slot-vm-setup.sh masters` with the same settings, or on a JuiceFS VM the same settings with no `VOLUME` (the volume is never formatted or re-created; the mount must be there).

**Admin password of a clone.** A database `up --from <master>` (and a `clone`) gets a new admin password at its first start: the master's password is the one its data was built with, so the change logs in with it, sets a new random one, writes the env file after the change succeeded, and starts the container again where the engine's admin helpers read the env (MySQL, MariaDB, SQL Server, Db2). A failed change keeps the working password and tries again at the next `up`. Cockroachdb and yugabytedb run insecure: nothing to change. An empty database already has its own password.

## Set up (once per VM)

`scripts/slot-vm-setup.sh` does steps 1 and 2 on a new Ubuntu 24.04 VM with a blank data disk, as root, from a folder holding it, `disk-setup.sh`, `slot.sh` and `slot-ssh.sh`. First write `/etc/juicefs/azure.env` (the connection string, [juicefs-branches.md](juicefs-branches.md), "Access"), then:

```bash
VOLUME=<volume> BUCKET=https://<container>.core.windows.net HOST=<public IP> SLOT_SETTINGS='MAX_PER_OWNER=0' \
  bash slot-vm-setup.sh
```

It installs podman, fuse3, jq and JuiceFS, formats the data disk as XFS at `/var/lib/containers`, starts Redis for the metadata on 127.0.0.1, formats the volume if it's new and mounts it at boot. Safe to rerun. `KIND=personal` (a VM for one person) keeps the state on the volume at `/mnt/jfs/.state` with the lock on the VM's disk; `RESTORE_DUMP=<gzipped juicefs dump>` loads that dump into an empty metadata store instead of formatting it, so a VM rebuilt over its volume's data gets the data back. With `META_URL` (for example `postgres://<role>@<host>:5432/<database>?sslmode=require`, password as `META_PASSWORD` in `azure.env`), the metadata is in that store, with no Redis on the VM: it outlives the VM, and several VMs can each keep their volume's metadata in one managed Postgres (a database per volume). Put masters in `/mnt/jfs/masters` (`tpc-master.sh`, or a copy with `tar --numeric-owner`, which keeps the engines' uids) and their admin env files in `/root/lfc-src`.

By hand:

1. JuiceFS mounted at `/mnt/jfs` ([juicefs-branches.md](juicefs-branches.md)), started at boot after its Redis (systemd unit below), and the masters in `/mnt/jfs/masters`.
2. The scripts and the user people log in as:

```bash
install -m 755 slot.sh /usr/local/sbin/lfc-slot
install -m 755 slot-ssh.sh /usr/local/sbin/lfc-slot-ssh
printf 'HOST=%s\n' "<public IP>" > /etc/lfc-slot.env
useradd -m -s /bin/bash slot
echo 'slot ALL=(root) NOPASSWD: /usr/local/sbin/lfc-slot' > /etc/sudoers.d/lfc-slot && chmod 440 /etc/sudoers.d/lfc-slot
install -d -o slot -g slot -m 700 ~slot/.ssh
```

3. One line per person in `~slot/.ssh/authorized_keys` (owner name: lowercase letters, digits, `-`):

```
command="/usr/local/sbin/lfc-slot-ssh alice",restrict ssh-ed25519 AAAA... alice
```

`slot-ssh.sh` passes the words of the typed command to `slot.sh --owner <owner>`. It refuses `--owner` and any word with characters other than letters, digits, `.`, `_`, `-`; a login without a command runs `ls`.

4. The firewall opens SSH and `PORT_MIN`-`PORT_MAX` to the people's addresses and to the workspace's serverless egress.

JuiceFS mount at boot, waiting for its Redis container (`/etc/systemd/system/lfc-jfs.service`; the env file holds the object storage credentials):

```ini
[Unit]
Description=JuiceFS mount /mnt/jfs
After=network-online.target podman-restart.service
Wants=network-online.target podman-restart.service

[Service]
Type=simple
EnvironmentFile=/etc/juicefs/azure.env
ExecStartPre=/bin/sh -c 'for i in $(seq 1 60); do podman exec jfs-redis redis-cli ping 2>/dev/null | grep -q PONG && exit 0; sleep 2; done; exit 1'
ExecStart=/usr/local/bin/juicefs mount --writeback --cache-dir /var/lib/containers/jfs/cache --cache-size 51200 -o allow_other --log /var/log/juicefs.log redis://127.0.0.1:6379/1 /mnt/jfs
ExecStop=/usr/local/bin/juicefs umount /mnt/jfs
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

`systemctl enable --now podman-restart lfc-jfs` (the Redis container runs with `--restart=always`).

YugabyteDB binds its YSQL port to its advertise address only, so `up` gives its container a static address on the Podman network, `10.88.200.<port - 30000 + 10>` (10.88.200.10 for port 30000): keep that range free of other containers on the VM. Db2's first start takes about two minutes; `up` waits for its setup to finish (15 minutes at most).

## Connect from a workspace

A Unity Catalog connection per database, with the password piped from `up`'s JSON so it isn't on a command line or in a file:

```bash
J=$(ssh slot@<vm> up postgres pg1 --from <master>)
databricks connections create -p <profile> --json @<(jq --arg n pg1_conn '{name: $n, connection_type: "POSTGRESQL",
  options: {host: .host, port: (.port | tostring), user: .user, password: .password}}' <<<"$J")
unset J
```

Connection types: `POSTGRESQL`, `MYSQL`, `SQLSERVER`, and `ORACLE` for Oracle (its `service` field is the `service_name`; that type doesn't take `trustServerCertificate`). MariaDB uses `MYSQL`. Db2 and Informix have no Lakeflow Connect connection type. CockroachDB and YugabyteDB speak the PostgreSQL protocol: a `POSTGRESQL` connection to them worked for a `remote_query` in the tests, and is made only on request. Check it from a SQL warehouse:

```sql
SELECT * FROM remote_query('pg1_conn', database => 'postgres', query => 'select count(*) from tpch.lineitem')
```

## Errors

| Message | Cause, fix |
|---|---|
| `new database: --from <master> or --empty` | the name is new; say what to start from |
| `<name> is a <engine> database` | the name exists with another engine; pick another name |
| `<dir> exists without a record` | data left from an interrupted `up`; `rm` it on the VM or pick another name |
| `<engine> in <container> didn't answer in 3 minutes` (10 for Oracle) | `podman logs <container>`; SQL Server `Permission denied` on `/.system`: the data isn't owned by uid 10001 (clone without `-p`) |
| `creating lfc_user in <container> failed; run up again` | the login SQL failed: the container is stopped, so the next `up` starts it and runs the login again |
| `you have <n> of <n> running databases on this VM (...)` | at your `max_running`: `park` one of them, or `up` again with `--park` to park the one that started first |
| `the VM is full` | park one of yours, wait, or use a VM of your own (`MAX_PER_OWNER=0`) |
| `<engine> in <container> stopped: podman logs <container>` | the engine's container exited while it started; the log says why |
| `preparing <engine> in <container> failed; run up again` | an OS user or the Informix database couldn't be made; the container is stopped, and the next `up` tries again |
| `DBI1264E` in the Db2 log, `already has a sqllib directory` | an earlier first start left its instance in the data folder: `rm` the database and create it again |
