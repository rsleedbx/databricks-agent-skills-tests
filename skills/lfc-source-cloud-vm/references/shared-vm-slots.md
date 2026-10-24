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
| `rm <name>` | stops it and deletes its data |
| `ls [--all]` | your databases (`--all`: everyone's): engine, state, port, created, from |
| `usage [--days n] [--all]` | hours running per owner and database in the last n days (default 7) |

Engines: `postgres` (16), `mysql` (8.4), `sqlserver` (2022). A master works with its own engine only.

`up` prints one JSON line:

```json
{"name":"pg1","engine":"postgres","host":"203.0.113.10","port":30000,"user":"lfc_user","password":"...","container":"lfc-slot-alice-pg1","state":"running"}
```

`lfc_user` is created on every start, with the rights Lakeflow Connect reads with: Postgres `LOGIN REPLICATION` and `pg_read_all_data`; MySQL `SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT`; SQL Server a login and, in every user database, a user in `db_datareader` with `VIEW DATABASE STATE` and `VIEW DEFINITION` (a database made later gets it at the next start; CDC is enabled per test). The engines start with CDC settings: Postgres `wal_level=logical`; MySQL `binlog_format=ROW`, `binlog_row_image=FULL`; SQL Server Agent on.

## Limits

| Setting (`/etc/lfc-slot.env`) | Default | When it's reached |
|---|---|---|
| `MAX_PER_OWNER` | 1 (0 = no limit) | `up` parks your other running database first |
| `MAX_RUNNING` | vCPUs / 2 | `up` stops with `the VM is full` and lists what's running |
| `PORT_MIN`-`PORT_MAX` | 30000-30063 | `no free port`; each database keeps its port until `rm` |
| `HOST` | the cloud metadata's public IP, else the first local IP | set it on Azure: the metadata has no public IP for a Standard SKU address |
| `ROOT`, `STATE`, `SECRETS` | `/mnt/jfs`, `/var/lib/lfc/slots`, `/root/lfc-src` | |

A clone of a master uses the master's admin env file (`$SECRETS/<engine>.env`, the one `tpc-master.sh` built it with); an empty database gets a new admin password. Passwords and env files stay in `$STATE/<owner>/` (mode 700, root).

## Set up (once per VM)

`scripts/slot-vm-setup.sh` does steps 1 and 2 on a new Ubuntu 24.04 VM with a blank data disk, as root, from a folder holding it, `disk-setup.sh`, `slot.sh` and `slot-ssh.sh`. First write `/etc/juicefs/azure.env` (the connection string, [juicefs-branches.md](juicefs-branches.md), "Access"), then:

```bash
VOLUME=<volume> BUCKET=https://<container>.core.windows.net HOST=<public IP> SLOT_SETTINGS='MAX_PER_OWNER=0' \
  bash slot-vm-setup.sh
```

It installs podman, fuse3, jq and JuiceFS, formats the data disk as XFS at `/var/lib/containers`, starts Redis for the metadata on 127.0.0.1, formats the volume if it's new and mounts it at boot. Safe to rerun. With `META_URL` (for example `postgres://<role>@<host>:5432/<database>?sslmode=require`, password as `META_PASSWORD` in `azure.env`), the metadata is in that store, with no Redis on the VM: it outlives the VM, and several VMs can each keep their volume's metadata in one managed Postgres (a database per volume). Put masters in `/mnt/jfs/masters` (`tpc-master.sh`, or a copy with `tar --numeric-owner`, which keeps the engines' uids) and their admin env files in `/root/lfc-src`.

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

## Connect from a workspace

A Unity Catalog connection per database, with the password piped from `up`'s JSON so it isn't on a command line or in a file:

```bash
J=$(ssh slot@<vm> up postgres pg1 --from <master>)
databricks connections create -p <profile> --json @<(jq --arg n pg1_conn '{name: $n, connection_type: "POSTGRESQL",
  options: {host: .host, port: (.port | tostring), user: .user, password: .password}}' <<<"$J")
unset J
```

Connection types: `POSTGRESQL`, `MYSQL`, `SQLSERVER`. Check it from a SQL warehouse:

```sql
SELECT * FROM remote_query('pg1_conn', database => 'postgres', query => 'select count(*) from tpch.lineitem')
```

## Errors

| Message | Cause, fix |
|---|---|
| `new database: --from <master> or --empty` | the name is new; say what to start from |
| `<name> is a <engine> database` | the name exists with another engine; pick another name |
| `<dir> exists without a record` | data left from an interrupted `up`; `rm` it on the VM or pick another name |
| `<engine> in <container> didn't answer in 3 minutes` | `podman logs <container>`; SQL Server `Permission denied` on `/.system`: the data isn't owned by uid 10001 (clone without `-p`) |
| `the VM is full` | park one of yours, wait, or use a VM of your own (`MAX_PER_OWNER=0`) |
