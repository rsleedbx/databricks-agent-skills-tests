---
name: lfc-source-podman
description: "Run source databases locally in Podman containers (Lima VM on macOS, native on Linux). Covers setup, 8 database engines (MySQL, MariaDB, PostgreSQL, SQL Server, Oracle, Db2, Informix, MongoDB), networking for local testing, and UC connection registration."
parent: lfc-source-setup
---

# Source database in local Podman (Lima on macOS, native on Linux)

Local Podman containers provide fast iteration for Lakeflow Connect development and demos. This skill sets up databases on your laptop or Linux machine, configures them for testing, and registers the Unity Catalog connection.

**Reachability limitation:** Local Podman databases **cannot** be reached from Databricks serverless compute by default. Laptop containers are isolated by design (gvproxy on macOS, firewall on Linux). Use this skill for **local testing only**. For Lakeflow Connect to access a database from Databricks, use a **cloud-managed** service or place the database on a public cloud VM with firewall rules.

If you need SQL Server Always On Availability Groups, Db2, Informix, or specific open-source engine versions not in managed services, Podman is the fastest way to get them running for local testing.

## Setup

### macOS

**Tools:** Podman, Lima, and the SQL clients; see **lfc-source-setup** "Tools" (`bash skills/cli-setup/scripts/check-tools.sh local`, and `clients` for the SQL clients). In short:

```bash
brew install podman lima libpq mysql-client sqlcmd mongosh
export PATH="$(brew --prefix libpq)/bin:$(brew --prefix mysql-client)/bin:$PATH"     # both are keg-only
```

Oracle's `sqlplus` isn't in Homebrew: use the one inside the container (`podman exec -it <container> sqlplus ...`).

Create and start the Lima VM with Rosetta and the rootful Podman template (it runs arm64 images natively and x86_64-only images under Rosetta), then register its Podman socket:

```bash
limactl start --tty=false --name lfc-test-podman --arch aarch64 --vm-type vz --rosetta --cpus 4 --memory 10 --disk 60 template:podman-rootful
podman system connection add lfc-test-podman "unix://$HOME/.lima/lfc-test-podman/sock/podman.sock"
```

Size it for what runs at once: SQL Server and Oracle take 4 GB each (`--memory 10` holds both plus a small engine; the default is 4 GiB). `--tty=false` skips the interactive confirmation, for scripts and agents. Tested (macOS 26, Apple Silicon, Lima 2.1): the VM starts in 44 s, rootful Podman on Fedora 44 with kernel 6.19 and Rosetta registered (`/proc/sys/fs/binfmt_misc/rosetta`). Container ports bound in the VM (`-p 30003:1433`) are forwarded to the Mac's `127.0.0.1` with nothing more to set up.

You don't need Podman Desktop for any of this. If it's installed, remove it first: it starts its own `podman machine` VM next to Lima, whose disk keeps growing (**podman-desktop-remove**).

Without `template:podman-rootful` the VM has no Podman and no socket. Don't assume an existing VM has it: an existing Ubuntu 25.10 Lima VM, had none, and no Rosetta registered (2026-10-21). [`scripts/ensure-lima-podman.sh <instance>`](scripts/ensure-lima-podman.sh) makes any Lima VM ready: it starts the instance (or creates it with the command above, `--create`), installs Podman with `apt-get` or `dnf` if it's missing, and checks rootful Podman and Rosetta. It prints one line per step (`PASS`, `FIXED`, `FAIL`, `WARN`). Scripts then run containers as the login user, with plain `podman`. Rosetta off is a `WARN` (x86_64 images then fail with `Exec format error`); `--fix-rosetta` turns it on for a vz VM and restarts the VM ([references/apple-silicon.md](references/apple-silicon.md)).

**One store, rootful, and `sudo` only in setup.** Rootful and rootless containers can run at the same time on one host, Lima included. CPU and RAM are shared: both are processes in the same VM, and the VM's size is fixed whatever runs. **Disk isn't shared:** each mode has its own image store (`/var/lib/containers/storage`, `~/.local/share/containers/storage`), and an image used in both is kept twice, layer for layer. Measured in Lima on 2026-10-21: the same three images (Debezium Server, CockroachDB, Alpine: 1.9 GB) in both stores. So use one mode per host, rootful through the root Podman socket, because database sources need it. Use rootless only on a host that never runs rootful databases.

Checked on the Azure test VM (Ubuntu 24.04) and the Lima VM (Ubuntu 25.10), 2026-10-21:

| Mode | Use it for | Command in scripts | Setup, once (the only `sudo`) |
|---|---|---|---|
| **Rootful** | Database sources that need storage on the XFS data disk (`/var/lib/containers`, **lfc-source-cloud-vm**) for reflink clones and JuiceFS branches of a master; Db2's `--privileged`; Oracle's raised `memlock` and `--shm-size`. Rootless can do none of these | `podman --remote --url unix:///run/podman/podman.sock ...` | [`scripts/ensure-podman-socket.sh --on "<prefix>"`](scripts/ensure-podman-socket.sh): a `podman` group with the user in it, owning the root Podman socket (mode 0660), like the `docker` group |
| **Rootless** | Only on a host that never runs rootful databases: its store would duplicate the rootful one. The Zerobus test can run rootless (`PODMAN=podman`: ports above 1024, nothing privileged, no master snapshot), but by default it uses the rootful socket, in the same store as the databases | `podman ...` | [`scripts/ensure-podman-rootless.sh --on "<prefix>"`](scripts/ensure-podman-rootless.sh): subordinate IDs, linger, `~/.cache` owned by the user |

`<prefix>` is `ssh <user>@<vm>` or `limactl shell <instance> --`. Both scripts print one line per step (`PASS`, `FIXED`, `FAIL`), change only what's missing, and end with a check: a rootful container that runs as uid 0 without `sudo`, or a rootless one with container names resolving on a network.

The disk, the Mac's `podman` connection, the security trade-off, linger, group membership, file ownership, and separate stores: [references/stores.md](references/stores.md).

Verify:

```bash
podman --connection lfc-test-podman info
limactl list
```

To resize the VM (e.g., after a RAM upgrade):

```bash
limactl stop lfc-test-podman
limactl delete lfc-test-podman
# Edit ~/.lima/lfc-test-podman/lima.yaml to adjust cpus + memory, then re-create
```

Which image runs how on Apple Silicon (arm64 or Rosetta, per engine), and the Rosetta `P` flag that Db2 needs: [references/apple-silicon.md](references/apple-silicon.md).

### Linux

**Prerequisites:** sudo access for one-time userspace networking setup.

Install Podman if missing:

```bash
sudo apt update && sudo apt install -y podman uidmap slirp4netns
sudo usermod -a -G podman $(whoami)
newgrp podman
```

Podman connects natively over the unix socket; no VM or SSH tunnel is needed.

## Naming

Container and volume names follow a pattern to avoid collisions and stay under Podman/DNS limits:

```
{prefix}-{engine-short}-{version}-{source}-{target}-{epoch}
```

| Component | Rule | Example |
|-----------|------|---------|
| `{prefix}` | `OWNER` from the saved settings (lfc-test-settings), else the OS user; lowercase, `.`/`_` → `-`, max 12 chars | `jane-doe` |
| `{engine-short}` | 2–4 letter engine ID | `mysql`, `pg`, `mssql`, `ora`, `db2`, `ifx` |
| `{version}` | Major version | `8.0`, `16`, `2022`, `21c` |
| `{source}` | Registry short name | `dockerhub`, `mcr`, `gvenzl`, `oci`, `icr` |
| `{target}` | Deployment target | `host-local` |
| `{epoch}` | Unix timestamp (or `812` for tests) | `1778687039` |

**Maximum length: 63 characters.** Use a shorter prefix if needed (e.g., `jd` instead of `jane-doe-test`).

Data volume appends `-data`: e.g., `jane-doe-mysql-8.0-dockerhub-host-local-812-data`.

## Running containers

**General process for any engine:**

1. **Pull the image** (one-time, cached)
2. **Create and run the container** with:
   - A data volume (persistent across restarts)
   - Port binding (default engine port → a free host port ≥30000)
   - Environment variables (admin password, replication mode, etc.) via `umask 077` + password file
3. **Wait for readiness** (engine startup + initialization)
4. **Configure** (LFC user, grants, CDC, etc.)

**Supported engines:**

| Engine | Version(s) | Default Port | Image | Reference |
|--------|-----------|---|---|---|
| MySQL | 8.0, 8.4 | 3306 | `docker.io/library/mysql:{version}` | [engines.md](references/engines.md) |
| PostgreSQL | 14, 15, 16, 17 | 5432 | `docker.io/library/postgres:{version}` | [engines.md](references/engines.md) |
| SQL Server | 2017, 2019, 2022, 2025 | 1433 | `mcr.microsoft.com/mssql/server:{version}-latest` | [engines.md](references/engines.md) |
| Oracle | 18c, 19c, 21c, 23ai, 26ai | 1521 | `docker.io/gvenzl/oracle-*` or `container-registry.oracle.com/database/*` | [engines.md](references/engines.md) |
| MariaDB | 11.4, 11.6 | 3306 | `docker.io/library/mariadb:{version}` | [engines.md](references/engines.md) |
| MongoDB | 7, 8 | 27017 | `docker.io/library/mongo:{version}` | [engines.md](references/engines.md) |
| Db2 | 11.5, 12.1 | 50000 | `icr.io/db2_community/db2:{version}` | [engines.md](references/engines.md) |
| Informix | 14.10 | 9088 | `icr.io/informix/informix-developer-database:latest` | [engines.md](references/engines.md) |

For per-engine startup commands, environment variables, readiness timing, and memory/CPU requirements, see [references/engines.md](references/engines.md).

**Container registries:**

| Source | Engines | Requires login | Notes |
|--------|---------|---|---|
| `dockerhub` | MySQL, MariaDB, PostgreSQL, MongoDB | No | Official Docker Hub images |
| `mcr` | SQL Server | No | Microsoft Container Registry |
| `gvenzl` | Oracle (default) | No | Community Free/XE; no license agreement |
| `oci` | Oracle (optional) | Yes | Official Enterprise/Standard; requires `podman login container-registry.oracle.com` |
| `icr` | Db2, Informix | No | IBM Container Registry |

Reusable master database on local storage (load once, clone for each test): [references/master.md](references/master.md).

## SQL Server availability group

[`scripts/ag-up.sh <utility_script.sql>`](scripts/ag-up.sh) runs two SQL Server 2022 containers as an Always On availability group with a readable secondary (`CLUSTER_TYPE = NONE`, synchronous commit), CDC and the utility objects on the primary, the same login and SID on both nodes, and an Extended Events session per node for the gateway's statements and errors. [`scripts/ag-sql.sh p|s [db] [--xe ...]`](scripts/ag-sql.sh) runs T-SQL on a node; [`scripts/ag-down.sh`](scripts/ag-down.sh) removes it. Each script prints its usage with no arguments. The Lakeflow test that uses it is the lfc-sqlserver-secondary skill.

## Configure the database for LFC

LFC requires:
- An LFC-specific user with SELECT + network access
- Replication settings (binlog/WAL/CT/CDC) per engine
- Optional audit tables

**See `references/2-database-connectors.md` in the public `databricks-lakeflow-connect` skill (the databricks plugin) for engine-specific configuration details.** The guides cover binlog setup (MySQL/MariaDB), logical replication (PostgreSQL), change tracking (SQL Server), and read-only user setup for query-based connectors (Oracle, Db2, Informix, MongoDB).

**Podman-specific notes:**

- **Container hostname:** SQL clients use `127.0.0.1:<published-port>` (e.g., `30001`).
- **Connect from another container:** Use `{container-name}:{internal-port}` (e.g., `jane-doe-mysql-8.0-dockerhub-host-local-812:3306`).

## Register the Unity Catalog connection

A UC connection on a laptop container is only useful for checking the request body: Databricks serverless can't reach `127.0.0.1` or a laptop's port. To ingest, run the same containers on a cloud VM in the workspace's region (**lfc-source-cloud-vm**), open the published ports to serverless (**lfc-source-firewall**), and register the connection with the VM's public IP, as **lfc-source-setup** "After the database exists" shows. Per engine:

Set `"trustServerCertificate": "true"` on every connection (TLS stays on; the server certificate isn't verified), or give the CA in `userProvidedServerCertificate` instead, never both. Oracle ignores it.

| Engine | `connection_type` | Options beyond `host`, `port`, `user`, `password` | User |
|---|---|---|---|
| MySQL | `MYSQL` | `"trustServerCertificate": "true"`: without it (or a CA in `userProvidedServerCertificate`) a CDC gateway fails with `INGESTION_GATEWAY_CONNECTION_ERROR.TLS_VALIDATION_FAILURE` | `lfc_user` |
| PostgreSQL | `POSTGRESQL` | `"trustServerCertificate": "true"`: the container's certificate is self-signed | `lfc_user` |
| SQL Server | `SQLSERVER` | `"trustServerCertificate": "true"`: the container's certificate is self-signed, and without it serverless fails with `FAILED_JDBC.CONNECTION` | `lfc_user` |
| Oracle | `ORACLE` | `"service_name"`: the CDB's service (`FREE` for `gvenzl/oracle-free`), needed by CDC; pipelines name the PDB in `source_catalog`. For SQL on the PDB through a connection, a second connection with `service_name` `FREEPDB1` | the common user in **uppercase**, for example `C##LFC_USER` |

There is no `databricks connections test`. Verify with a read-only `remote_query` from serverless (**lfc-source-setup**, step 3).

## Lifecycle

### Stop and restart

```bash
podman stop {container-name}
podman start {container-name}
```

The volume persists; the database restarts with the same data.

### Snapshot: export a volume for reuse

```bash
# Export volume to a tar file
podman volume export {volume-name} > {backup-name}.tar

# Later: import into a new volume
podman volume create {new-volume-name}
podman volume import {new-volume-name} {backup-name}.tar
```

### Teardown: remove container and volume

```bash
podman rm {container-name}
podman volume rm {volume-name}
```

All data is lost. To keep the database but remove the container, omit the volume remove step and re-use the volume later.

Removing the VM and getting the laptop's disk back, including the rule about deleting a running VM's disk file: [references/remove-vm.md](references/remove-vm.md).

### Delete UC connection

```bash
databricks connections delete --name owner-engine-podman --profile DEFAULT
```

Common issues and their fixes (name length, Oracle hostnames, a full VM disk, port in use, a frozen Lima VM, slow starts, UC authentication, a disk that keeps filling): [references/troubleshooting.md](references/troubleshooting.md).

---

**Ready for Lakeflow Connect?** Once the UC connection is registered, use the **databricks-lakeflow-connect** skill to configure the pipeline, apply pre-flight checks, and start ingestion.
