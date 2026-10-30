# PostgreSQL Source Configuration for Lakeflow Connect

This guide documents the database configuration required for Lakeflow Connect (LFC) query-based ingestion and CDC (Logical Replication) on PostgreSQL servers.

## Quick Reference

| Requirement | Query-Based | CDC (Logical Replication) |
|---|---|---|
| **Server Parameter** | `wal_level=logical` | Same + `max_replication_slots ≥1` |
| **User Privilege** | `CONNECT`, `SELECT` | + `REPLICATION` or `rds_replication` |
| **Cursor Column** | Any non-null `TIMESTAMP` | Not needed; slots track LSN |
| **Primary/Unique Key** | Recommended | Required: set `REPLICA IDENTITY DEFAULT` or `FULL` |

## Overview

PostgreSQL uses logical replication slots and WAL (Write-Ahead Log) decoding for Lakeflow Connect's CDC. All platforms (AWS RDS, GCP Cloud SQL, Azure Database for PostgreSQL, self-managed) support the same core configuration; replication privileges differ slightly between managed and self-managed instances.

## Prerequisites for Query-Based Ingestion

### PostgreSQL Server Parameters

The following parameters control replication and audit logging:

| Parameter | Required Value | Scope | Notes |
|-----------|----------------|-------|-------|
| `max_replication_slots` | ≥1 | Startup | Defines number of logical replication slots; default may be 10 |
| `max_wal_senders` | ≥2 | Startup | Walsender processes for replication; default may be 10 |
| `wal_level` | logical | Startup | Required for logical replication; enables WAL decoding |
| `log_connections` | on | Runtime | Optional; logs connection events |
| `log_disconnections` | on | Runtime | Optional; logs disconnections |
| `log_statement` | 'all' (or 'ddl') | Runtime | Optional; logs SQL statements |

**Check current settings:**
```sql
SELECT name, setting, unit FROM pg_settings 
  WHERE name IN ('max_replication_slots', 'max_wal_senders', 'wal_level', 'log_connections');
```

### Cloud-Specific Parameter Setup

**AWS RDS PostgreSQL:**
```
Parameter group:
  rds.logical_replication = 1  (enables wal_level=logical)
  max_replication_slots = 10  (default; adjust if needed)
  max_wal_senders = 10
```

> **Note:** RDS has a custom parameter `rds.logical_replication`. Setting `wal_level` directly may be ignored.

**GCP Cloud SQL PostgreSQL:**
```
Flags:
  cloudsql.enable_cloudsql_logs = on  (optional, for Cloud Logging audit)
  max_replication_slots = 10
  max_wal_senders = 10
Database flags typically include: cloudsql_logical_replication = on
```

**Azure Database for PostgreSQL:**
```
Server parameters:
  azure.replication_support = on  (enables logical replication)
  max_replication_slots = 10
  max_wal_senders = 10
```

## LFC User Setup

### Create or Update LFC Role

```sql
-- Connect as superuser (postgres) or with CREATEUSER privilege, in psql

-- The database, if it doesn't exist yet; then connect to it (the schema grants apply to the current database)
SELECT 'CREATE DATABASE "<database>"' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '<database>')\gexec
\c <database>

-- Create LFC user (idempotent)
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_user WHERE usename = '<lfc_user>') THEN
    CREATE USER <lfc_user> PASSWORD '<password-from-file>';
  END IF;
END $$;

-- Update password
ALTER USER <lfc_user> WITH PASSWORD '<password-from-file>';

-- Grant connection to target database
GRANT CONNECT ON DATABASE "<database>" TO <lfc_user>;
GRANT ALL PRIVILEGES ON DATABASE "<database>" TO <lfc_user>;

-- Grant schema privileges
GRANT USAGE, CREATE ON SCHEMA public TO <lfc_user>;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA public TO <lfc_user>;

-- Grant default future table privileges
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO <lfc_user>;
```

### Grant Replication Privilege

Replication privilege depends on whether you are superuser or using managed privileges (AWS RDS, GCP Cloud SQL, Azure).

**Self-Managed / Superuser Path:**
```sql
-- Connect as superuser
ALTER ROLE <lfc_user> WITH REPLICATION;
```

**AWS RDS (Non-Superuser) Path:**
```sql
-- If the admin user is not superuser, use rds_replication role
GRANT rds_replication TO <lfc_user>;
```

**Verify replication capability:**
```sql
-- Connect as LFC user
SELECT rolreplication FROM pg_roles WHERE rolname = '<lfc_user>';
-- Should return 't' (true)
```

### Verify LFC User

```sql
-- Connect as LFC user to target database
SELECT 1;
SELECT current_user;
SELECT has_database_privilege('<lfc_user>', '<database>', 'CONNECT');
```

## Seed Tables for Demo/Testing

Two seed tables that work for both query-based ingestion and CDC (tested end to end, Postgres 16). `dt` is the query-based cursor: `NOT NULL`, indexed, and bumped on every update by a trigger (`DEFAULT` alone only covers inserts, and updates would never arrive). Create them as the admin, then grant:

```sql
-- Connect as the admin to <database>

-- intpk: integer primary key
CREATE TABLE IF NOT EXISTS public.intpk (
  pk  SERIAL PRIMARY KEY,
  dt  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ops VARCHAR(255) DEFAULT 'insert'
);
CREATE INDEX IF NOT EXISTS idx_intpk_dt ON public.intpk (dt);

-- dtix: no primary key (query-based: APPEND_ONLY; CDC: needs REPLICA IDENTITY FULL)
CREATE TABLE IF NOT EXISTS public.dtix (
  pk  BIGINT,
  dt  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ops VARCHAR(255) DEFAULT 'insert'
);
CREATE INDEX IF NOT EXISTS idx_dtix_dt ON public.dtix (dt);
ALTER TABLE public.dtix REPLICA IDENTITY FULL;

-- Bump the cursor on every update
CREATE OR REPLACE FUNCTION public.set_dt() RETURNS trigger AS 'BEGIN NEW.dt := now(); RETURN NEW; END' LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER intpk_set_dt BEFORE UPDATE ON public.intpk FOR EACH ROW EXECUTE FUNCTION public.set_dt();
CREATE OR REPLACE TRIGGER dtix_set_dt BEFORE UPDATE ON public.dtix FOR EACH ROW EXECUTE FUNCTION public.set_dt();

-- Seed 10 rows each
INSERT INTO public.intpk (dt) SELECT now() FROM generate_series(1, 10);
INSERT INTO public.dtix (pk, dt) SELECT i, now() FROM generate_series(1, 10) i;
GRANT SELECT ON public.intpk, public.dtix TO <lfc_user>;
```

## CDC Requirements (Logical Replication)

### Enable Logical Replication (Platform-Specific)

Logical replication must be enabled **at the server level** (startup parameter), not per-database.

**Self-Managed PostgreSQL:**

1. Edit `postgresql.conf`:
   ```
   wal_level = logical
   max_replication_slots = 10
   max_wal_senders = 10
   ```

2. Restart PostgreSQL:
   ```bash
   sudo systemctl restart postgresql
   # or
   pg_ctl -D /path/to/data restart
   ```

3. Verify:
   ```sql
   SHOW wal_level;  -- Should return 'logical'
   ```

**AWS RDS PostgreSQL:**
```
1. Parameter group or modify DB instance:
     rds.logical_replication = 1

2. AWS will display a yellow notification to reboot.
   Reboot the DB instance or cluster.

3. Verify post-reboot:
   SELECT setting FROM pg_settings WHERE name = 'wal_level';
   -- Should return 'logical'
```

**GCP Cloud SQL PostgreSQL:**
```
1. Go to Edit instance / Flags
   cloudsql_logical_replication = on

2. Changes apply immediately or on next maintenance window.

3. Verify:
   SELECT setting FROM pg_settings WHERE name = 'wal_level';
```

**Azure Database for PostgreSQL:**
```
1. Server parameters:
     azure.replication_support = on

2. Some changes require server restart (check the UI).

3. Verify:
   SELECT setting FROM pg_settings WHERE name = 'wal_level';
```

### Create Replication Slot

A replication slot tracks the decoder's position in the WAL stream. Create it before the first update. Tested 2026-10-25 on PostgreSQL 16 and 17:
- Pipeline create refuses an empty slot name ("PostgreSQL slot name cannot be empty or null").
- The gateway doesn't create a missing slot; it fails with `CDC_NOT_ENABLED` "Logical replication slot <name> doesn't exist with pgoutput plugin".
- The slot must use `pgoutput`, be `logical`, and live in the database the connection opens.
- A slot that is lost or dropped isn't recreated, even by a full refresh: create it again by hand, then run the full refresh.
- A dropped slot is reported as `CDC_NOT_ENABLED` ("Logical replication slot <name> doesn't exist with pgoutput plugin"), not as a deleted-slot error. A lost slot (WAL kept past `max_slot_wal_keep_size`) fails the next update with `INGESTION_GATEWAY_PG_SLOT_IS_INVALIDATED`. Tested 2026-10-25 on PostgreSQL 16.15.
- Deleting the pipeline doesn't drop a slot it named: the slot was still listed 6 minutes after the delete (tested 2026-10-25, PostgreSQL 16.15). Drop it by hand once the pipeline is gone.

```sql
-- Connect as superuser, in the database the connection opens
SELECT pg_create_logical_replication_slot('<slot_name>', 'pgoutput');
SELECT slot_name, plugin, slot_type, database, active, wal_status FROM pg_replication_slots;
```

`invalidation_reason` exists from PostgreSQL 17; on 16, a lost slot shows as `wal_status = 'lost'`.


### Set Replica Identity on Tables

Replica identity determines how PostgreSQL encodes old row values in the WAL for update/delete operations. LFC uses two strategies:

```sql
-- Connect as LFC user

-- DEFAULT: Use primary key (or unique index) for old row identification
ALTER TABLE public.intpk REPLICA IDENTITY DEFAULT;

-- FULL: Use entire row (for tables without obvious PK)
ALTER TABLE public.dtix REPLICA IDENTITY FULL;

-- Verify:
SELECT schemaname, tablename, reptoptions FROM pg_tables
  WHERE schemaname = 'public' AND tablename IN ('intpk', 'dtix');
```


**A primary key with a TOAST-able column:** the gateway can refuse a table that has a primary key and a text column under `DEFAULT`: `INGESTION_GATEWAY_CDC_NOT_ENABLED` with "does not have primary key or has a TOAST-able type column, and needs FULL REPLICA IDENTITY". The same message says to use `DEFAULT` when there is a primary key. Tested 2026-10-25 on PostgreSQL 16.15 and 17.11. On one gateway pair, `ALTER TABLE <table> REPLICA IDENTITY FULL` cleared it; on others it stayed, and a new gateway and ingestion pipeline pair with the same settings passed. So: set `FULL` on the table; if the same message stays, create a new gateway and ingestion pair. The cause isn't isolated.

## Password Rotation

```sql
-- Rotate LFC user password (via ALTER USER)
-- Connect as superuser or with CREATEROLE privilege

ALTER USER <lfc_user> WITH PASSWORD '<new-password>';

-- Verify by connecting as LFC user:
psql -h <host> -U <lfc_user> -d <database> -c "SELECT 1;"
```

## Troubleshooting

### Logical Replication Not Enabled

**Symptom:** `pg_create_logical_replication_slot()` fails; "logical replication not supported".

**Fix:** Verify `wal_level = logical` (startup parameter; requires restart):
```sql
SHOW wal_level;
```

### Cannot Create Replication Slot (Permission Denied)

**Symptom:** `pg_create_logical_replication_slot()` fails with permission denied.

**Fix:** User must have REPLICATION role:
```sql
ALTER ROLE <lfc_user> WITH REPLICATION;
```

### Replication Slot Not Active (WAL Accumulation)

**Symptom:** WAL files accumulate; slot lagging.

**Fix:** Check and drop stale slots:
```sql
SELECT slot_name, active FROM pg_replication_slots;
SELECT pg_drop_replication_slot('<slot_name>');  -- if inactive
```

### Cannot Alter Replica Identity (Table Owner)

**Symptom:** `ALTER TABLE ... REPLICA IDENTITY` fails due to permission denied.

**Fix:** Ensure LFC user owns the table:
```sql
ALTER TABLE public.intpk OWNER TO <lfc_user>;
ALTER TABLE public.dtix OWNER TO <lfc_user>;
```

### wal_level Is replica

**Symptom:** The gateway fails with `INGESTION_GATEWAY_CDC_ERROR.CDC_NOT_ENABLED`: "CDC is not enabled on User <user> ... wal_level is set to 'replica'. It should be set to 'logical' for CDC to work." The ingestion update can sit in `INITIALIZING` for 20 minutes or more with no events (tested 2026-10-25, PostgreSQL 16.15).

**Fix:** Set `wal_level = logical`, restart (see Enable Logical Replication above), then start the update again.

### Lowering wal_level With Slots Present

**Symptom:** After `wal_level` is lowered, the server won't start: `FATAL: logical replication slot "<slot>" exists, but wal_level < logical`, and the container exits. PostgreSQL 16 doesn't invalidate the slots (tested 2026-10-25, PostgreSQL 16.15).

**Fix:** Stop the gateway, drop the logical slots, then lower `wal_level`. On PostgreSQL 16 check a lost slot with `wal_status`; `invalidation_reason` exists from PostgreSQL 17.

### TLS With a Self-Signed Certificate

**Symptom:** `INGESTION_GATEWAY_CONNECTION_ERROR.TLS_CONNECTION_FAILURE` with the `verify-ca` SSL mode and a self-signed server certificate (tested 2026-10-25, PostgreSQL 17.11, self-hosted).

**Fix:** Add `"trustServerCertificate": "true"` to the connection's options, as SKILL.md step 2 describes for SQL Server and MySQL, or use a certificate from a CA. One run also logged a TLS connection while `pg_stat_ssl` showed no SSL for the gateway's sessions; that wasn't resolved.

### TLS Off on the Server

**Symptom:** `The server does not support SSL` from a JDBC URL with `sslmode=require` (tested 2026-10-30, PostgreSQL 16 container started with the image's defaults, TYPE JDBC connection, pgjdbc 42.7.4), or a gateway's `Authentication failure … The server does not support SSL`. `SHOW ssl;` returns `off`.

**Fix:** turn TLS on at the server rather than lowering the client to `sslmode=prefer` or `disable`, which send the login in clear text. A self-signed certificate is enough when the client trusts it (`trustServerCertificate`, or `sslmode=require`, which encrypts without checking the certificate). As a superuser, in the data directory (`SHOW data_directory;`), from the [PostgreSQL SSL docs](https://www.postgresql.org/docs/16/ssl-tcp.html):

```bash
openssl req -new -x509 -days 365 -nodes -subj /CN=<host> -keyout server.key -out server.crt
chmod 600 server.key            # owned by the server's OS user, or the server refuses the key
```

```sql
ALTER SYSTEM SET ssl = on;      -- server.crt and server.key in the data directory are the defaults
SELECT pg_reload_conf();        -- no restart needed
SHOW ssl;                       -- on
```

Tested 2026-10-30 on a PostgreSQL 16 container that came up with `ssl = off`: after the reload, an append through a TYPE JDBC connection with `sslmode=require` ran on a `TLSv1.3` session (`SELECT ssl, version FROM pg_stat_ssl WHERE pid = pg_backend_pid();` through the same connection). With only SQL access to a container (no shell), a superuser can run the `openssl` step through `COPY <temp table> FROM PROGRAM '...'`.

### Connection Timeout (Network/Security)

**Symptom:** `psql` hangs or connection refused.

**Fix:** Verify network reachability and firewall (port 5432):
```bash
nc -zv <host> 5432
```

## Verification Checklist

Before starting LFC ingestion:

1. **Logical replication enabled:**
   ```sql
   SHOW wal_level;  -- Should return 'logical'
   ```

2. **LFC user has replication privilege:**
   ```sql
   SELECT rolreplication FROM pg_roles WHERE rolname = '<lfc_user>';  -- Should be 't'
   ```

3. **Seed tables exist and populated:**
   ```sql
   SELECT COUNT(*) FROM public.intpk, public.dtix;  -- Both should have 10 rows
   ```

4. **Replica identity correctly set:**
   ```sql
   SELECT tablename, reptoptions FROM pg_tables WHERE tablename IN ('intpk', 'dtix');
   ```

5. **No stale replication slots:**
   ```sql
   SELECT slot_name, active FROM pg_replication_slots;
   ```