# IBM Db2 (Community Edition)

> **Lakeflow Connect support:** check the current [connector list](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/) before promising ingestion from this engine. This page covers preparing the database; whether and how Lakeflow Connect ingests from it (query-based, CDC, or a community connector) changes between releases.


## Overview

IBM Db2 Community Edition uses **SQL Replication** (sqlrep) for CDC. Db2 is primarily self-managed / Podman-based; cloud support is limited. **Lakeflow Connect support is experimental / limited.**

## Prerequisites

- Db2 Community Edition (not Enterprise)
- Db2 instance role (default: `db2inst1`) with SYSADM privilege
- Database must exist and be connectable

## User Setup (OS-Level)

Db2 uses **OS-level authentication**: every database user must exist as an OS user in the container. Create the OS user first, then grant SQL privileges:

```sql
-- OS user creation (run on host):
# podman exec <db2-container> useradd -m <lfc_user>
# printf '%s:%s\n' <lfc_user> "$(cat lfc_user.pw)" | podman exec -i <db2-container> chpasswd   # password from a file

-- SQL side: Connect to database and grant privileges
CONNECT TO <database>;
GRANT DBADM ON DATABASE TO USER <lfc_user>;

-- Verify
SELECT GRANTEE, DBADMAUTH FROM SYSCAT.DBAUTH WHERE GRANTEE = UPPER('<lfc_user>');
```

## Seed Tables

```sql
CONNECT TO lfctest;

CREATE TABLE <lfc_user>.intpk (
  pk  INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  dt  TIMESTAMP DEFAULT CURRENT TIMESTAMP,
  ops VARCHAR(255) DEFAULT 'insert'
);

CREATE TABLE <lfc_user>.dtix (
  pk  INTEGER,
  dt  TIMESTAMP DEFAULT CURRENT TIMESTAMP,
  ops VARCHAR(255) DEFAULT 'insert'
);

CREATE INDEX <lfc_user>.idx_intpk_dt ON <lfc_user>.intpk (dt);
CREATE INDEX <lfc_user>.idx_dtix_dt ON <lfc_user>.dtix (dt);

-- Seed data
INSERT INTO <lfc_user>.intpk (dt) VALUES
  (CURRENT TIMESTAMP), (CURRENT TIMESTAMP), (CURRENT TIMESTAMP),
  (CURRENT TIMESTAMP), (CURRENT TIMESTAMP), (CURRENT TIMESTAMP),
  (CURRENT TIMESTAMP), (CURRENT TIMESTAMP), (CURRENT TIMESTAMP),
  (CURRENT TIMESTAMP);

INSERT INTO <lfc_user>.dtix (pk, dt) VALUES
  (1, CURRENT TIMESTAMP), (2, CURRENT TIMESTAMP), (3, CURRENT TIMESTAMP),
  (4, CURRENT TIMESTAMP), (5, CURRENT TIMESTAMP), (6, CURRENT TIMESTAMP),
  (7, CURRENT TIMESTAMP), (8, CURRENT TIMESTAMP), (9, CURRENT TIMESTAMP),
  (10, CURRENT TIMESTAMP);
```

## CDC Requirements

Db2 SQL Replication requires activation on tables and publishing to a replication queue. This is **not commonly used with LFC** due to complexity and limited external tooling support. LFC may use query-based ingestion instead of true CDC.

```sql
-- SQL Replication setup (advanced; rarely used with LFC)
-- Contact Databricks support for Db2 CDC guidance
```

## Gotchas

- **OS user requirement:** Every Db2 user must have a corresponding OS user. Automate via `podman exec useradd`.
- **4096-byte-sector disks (Azure Premium SSD v2, Ultra Disk):** `CREATE DATABASE` fails with `SQL0293N Error accessing a table space container.`: Db2 writes 512 bytes with direct I/O and the disk refuses it (`db2diag.log`: `pwrite`, `OSERR: EINVAL`). Run `db2set DB2_4K_DEVICE_SUPPORT=ON`, `db2stop force`, `db2start`, then `CREATE DATABASE`; or put the database on a 512-byte-sector disk. Check the disk with `blockdev --getss <device>`. Details: lfc-source-cloud-vm `references/disk-setup.md`, "Logical sector size".
- **No cloud managed services:** Db2 CE is self-managed only.
- **Limited CDC tooling:** SQL Replication is less mature than MySQL/PostgreSQL binlog or Oracle LogMiner.
- **Query-based fallback:** LFC may use full-table scans + timestamp columns for "CDC" instead of true change streams.

## LFC Support Status

✓ Query-based ingestion  
✗ Standard CDC (SQL Replication requires extensive configuration; not recommended)  
✗ Integrated CDC (not available)

---

## Troubleshooting

- **Can't connect:** check reachability (`nc -zv <host> <port>`), the firewall / security group, and the credentials.
- **Login fails for a user that exists in the database:** Db2 authenticates against the OS, so the OS user must exist in the container (or host) with the same name and password.
- **Password rotation:** change the OS user's password (`chpasswd` from a file, as above); Db2 has no separate SQL password.

## When to use

Legacy and mainframe-adjacent workloads. For new Lakeflow Connect sources, prefer MySQL, PostgreSQL, SQL Server, or Oracle.
