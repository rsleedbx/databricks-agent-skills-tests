# Images on Apple Silicon, and Rosetta

**Which image runs how on Apple Silicon.** Every engine runs in the Lima VM. Use the arm64 image wherever one exists, and Rosetta only for x86_64-only images:

| Engine | Image on macOS | Runs as |
|---|---|---|
| PostgreSQL, MySQL, MariaDB, MongoDB | Docker Hub official images | arm64, native |
| Oracle 23ai / 26ai Free | `docker.io/gvenzl/oracle-free:latest` (default), or `container-registry.oracle.com/database/free:latest` | arm64, native |
| Oracle 19c Enterprise | `container-registry.oracle.com/database/enterprise:19.19.0.0` (needs `podman login`) | arm64, native |
| Oracle 18c / 21c XE | `docker.io/gvenzl/oracle-xe:18` / `:21` | x86_64, Rosetta |
| SQL Server 2017–2025 | `mcr.microsoft.com/mssql/server:<version>-latest` | x86_64, Rosetta (2025 needs a Lima kernel of 6.17 or later) |
| Db2, Informix | `icr.io` images | x86_64, Rosetta |
| Azure SQL Edge (retired; SQL syntax checks only, see below) | `mcr.microsoft.com/azure-sql-edge:latest` | arm64, native |

**`Exec format error` at container start** (`exec container process ... Exec format error`; the container exits at once, `podman ps -a` shows `Exited (1)`): the image is x86_64 and the VM has no Rosetta registered. A VM's name doesn't tell: an existing VM with `rosetta` in its name had it off (2026-10-28). Check with `limactl shell <instance> -- ls /proc/sys/fs/binfmt_misc/rosetta`, and turn it on with [`scripts/ensure-lima-podman.py <instance> --fix-rosetta`](../scripts/ensure-lima-podman.py): it runs `limactl stop`, `limactl edit --rosetta`, `limactl start` (the VM restarts, so its containers stop), then checks the registration (45 s). Tested on that VM (vz, Lima 2.1.2): `WARN` with Rosetta off, `FIXED` with `--fix-rosetta`, Rosetta registered with flags `OCF`, and SQL Server 2022 CU27 (`host_architecture` `X64`) answered queries 12 s after `podman run --platform linux/amd64`.

**Register Rosetta with the preserve-argv0 flag** (`P`), once per VM. Lima registers it with `OCF`, which drops a program's `argv[0]`: then `su -` in an x86_64 container isn't a login shell, never reads the user's profile, and Db2's setup (which runs everything through `su - db2inst1`) fails with `db2: command not found`. Check `grep flags /proc/sys/fs/binfmt_misc/rosetta` in the VM (`limactl shell lfc-test-podman`); to fix it at every boot, add this to `~/.lima/lfc-test-podman/lima.yaml` and restart the VM (`limactl stop` / `start`). Tested: `flags: POCF` after the restart, and Db2 then set up cleanly.

```yaml
provision:
- mode: system
  script: |
    #!/bin/bash
    f=/proc/sys/fs/binfmt_misc/rosetta
    for i in $(seq 30); do [ -e $f ] && break; sleep 1; done
    [ -e $f ] && ! grep -q "flags: P" $f || exit 0
    M=$(awk '/^magic/ {print $2}' $f); K=$(awk '/^mask/ {print $2}' $f); I=$(awk '/^interpreter/ {print $2}' $f)
    hex() { echo "$1" | sed 's/../\\x&/g'; }
    echo -1 > $f
    printf ':rosetta:M::%s:%s:%s:POCF' "$(hex $M)" "$(hex $K)" "$I" > /proc/sys/fs/binfmt_misc/register
```

**Tested together** on a MacBook (Apple Silicon, 32 GB) in one 4-CPU, 10 GiB Lima VM: SQL Server 2022, Db2 11.5 and Informix 14.10 under Rosetta and Oracle 26ai Free on arm64 side by side, each reached from the Mac on `127.0.0.1`, and each filled with TPC-H SF 1 (8,660,030 rows) inside the database by **lfc-source-datagen**: SQL Server in 202 s (as fast as a 4-vCPU Azure VM: 179 s), Oracle in 320 s.

Don't run x86_64-only engines in Podman machine (`podman machine`): its Fedora CoreOS kernel (6.12) crashes SQL Server 2025 with an AVX signal-frame bug, and Podman is deprecating its Rosetta support. The Lima VM's newer kernel doesn't have these bugs, and Lima keeps Rosetta: running SQL Server on macOS is the same as running its container in Podman on Lima.

**Azure SQL Edge: a fallback for T-SQL syntax checks, not a source.** Microsoft retired Azure SQL Edge on September 30, 2025 ([overview](https://learn.microsoft.com/en-us/azure/azure-sql-edge/overview)), but the image still pulled from MCR on 2026-10-28. It's arm64, so it runs in a Lima VM without Rosetta; its engine is 15.0.2000.1574 (SQL Server 2019 RTM level). Use it only to check that a T-SQL query parses and reads the catalog views it needs, when Rosetta can't be turned on: on 2026-10-28 it ran a `sys.indexes` query with `has_filter` and `STRING_AGG ... WITHIN GROUP`, with the same result as SQL Server 2022. It gets no fixes, it isn't a SQL Server that Lakeflow Connect lists as a source, and the image has no `sqlcmd` inside: publish the port and use the Mac's `sqlcmd`.

```bash
limactl shell <instance> -- podman run -d --name <name> -p 51433:1433 -e ACCEPT_EULA=1 -e 'MSSQL_SA_PASSWORD=<password>' mcr.microsoft.com/azure-sql-edge:latest
sqlcmd -C -I -S 127.0.0.1,51433 -U sa -P '<password>' -Q 'SELECT @@VERSION'
```

`-I` turns on `QUOTED_IDENTIFIER`, which filtered indexes, indexed views and computed-column indexes need: without it the Mac's `sqlcmd` fails `CREATE INDEX ... WHERE` with error 1934 (on SQL Server too). Where Rosetta can be turned on, run the real SQL Server image instead.
