# Changes at a fixed rate through a Unity Catalog JDBC connection

A CDC or query-based test needs a steady trickle of inserts, updates and deletes on the source, for example **at most 10 changed rows a minute per table**, while several notebooks, jobs and retries run the same change step without knowing about each other. A statement that caps itself does that, and a Unity Catalog connection of type `JDBC` runs it from Databricks with no password in the code (a federation connection, type `POSTGRESQL`, `MYSQL` or `SQLSERVER`, refuses Spark options).

## How a statement caps itself

1. Every row a change touches gets `dt = now`. The rows whose `dt` falls in the last *W* seconds are the changes already made in the window, by anyone.
2. The budget is `b = max(0, B - rows in the window)`. The statement updates (or deletes) only the `b` rows with the oldest `dt`, outside the window.
3. **A delete is paired with an insert** of one new row per deleted row, so it counts in the window through its replacement (Postgres: one statement, below).
4. Run again inside the window, it finds `b = 0` and changes nothing; after the window it changes up to *B* again. **Repeats are safe, and the cap is a rate.**
5. **Take a lock first when runs can overlap.** Overlapping runs each read the budget before the others commit, and together go over it. Take the engine's named lock, then count, then change:

| Engine | Lock, held to the end of the run |
|---|---|
| PostgreSQL | `SELECT pg_advisory_xact_lock(hashtext('<table>'));` |
| MySQL | `SELECT GET_LOCK('<table>', 60);` … `DO RELEASE_LOCK('<table>');` |
| SQL Server | `EXEC sp_getapplock @Resource = '<table>', @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 60000;` … `sp_releaseapplock` |
| Oracle | `DBMS_LOCK.ALLOCATE_UNIQUE` + `DBMS_LOCK.REQUEST(h, DBMS_LOCK.X_MODE, 60, TRUE)` (released at `COMMIT`; needs `EXECUTE ON DBMS_LOCK`) |

A fixed row order (`ORDER BY dt, pk`) without the lock keeps the window at *B* rows, since overlapping runs pick the same rows, but each run still updates them: CDC sees every update (8 overlapping runs: 10 rows, 80 updates).

## Run it from Databricks

Spark's JDBC source runs a statement only as `sessionInitStatement`, on every connection it opens: once per partition, per action, and per task attempt. It commits before the read's query runs, and stays committed if the read fails. The cap above makes those repeats harmless; still read with one partition and one action:

```python
(spark.read.format("jdbc").option("databricks.connection", "<connection>")
      .option("query", "SELECT 1 AS x")                # Oracle: SELECT 1 AS x FROM dual
      .option("numPartitions", "1")
      .option("sessionInitStatement", capped_update)   # one engine's statement below
      .load().collect())
```

The connection (TYPE JDBC, [append-through-connection.md](append-through-connection.md)) needs:

- `externalOptionsAllowList` with `sessionInitStatement` and `numPartitions` added to `dbtable,query`. Without them Spark refuses: `[DATA_SOURCE_OPTION_NOT_ALLOWED_BY_CONNECTION] Option sessioninitstatement is not allowed by connection ...`.
- On MySQL, `allowMultiQueries=true` in the URL, for a statement with several parts.
- A login with `SELECT, INSERT, UPDATE, DELETE` on the test tables only. Make it its own connection: anyone with `USE CONNECTION` can run any SQL that login may run.

## The statements, B = 10 rows, W = 60 seconds

**PostgreSQL**: half the budget to deletes with their replacement inserts, the rest to updates:

```sql
SELECT pg_advisory_xact_lock(hashtext('public.t'));
WITH del AS (DELETE FROM public.t WHERE pk IN (
               SELECT pk FROM public.t WHERE dt < now() - interval '60 seconds' ORDER BY dt, pk
               LIMIT (SELECT greatest(0, 10 - count(*)) / 2 FROM public.t WHERE dt >= now() - interval '60 seconds'))
             RETURNING pk)
INSERT INTO public.t (dt, ops) SELECT clock_timestamp(), 'insert' FROM del;
UPDATE public.t SET dt = clock_timestamp(), ops = 'update', n = n + 1
WHERE pk IN (SELECT pk FROM public.t WHERE dt < now() - interval '60 seconds' ORDER BY random()
             LIMIT (SELECT greatest(0, 10 - count(*)) FROM public.t WHERE dt >= now() - interval '60 seconds'));
```

The driver runs the `;`-separated statements in one transaction, so the advisory lock is held to the end, and each statement counts what the one before it changed.

**MySQL** (`allowMultiQueries=true`):

```sql
SELECT GET_LOCK('t', 60);
SET @b = (SELECT greatest(0, 10 - count(*)) FROM db.t WHERE dt >= now(6) - INTERVAL 60 SECOND);
UPDATE db.t t JOIN (SELECT pk, row_number() OVER (ORDER BY rand()) AS rn FROM db.t
                    WHERE dt < now(6) - INTERVAL 60 SECOND) o ON o.pk = t.pk AND o.rn <= @b
SET t.dt = now(6), t.ops = 'update', t.n = t.n + 1;
DO RELEASE_LOCK('t');
```

The window function makes MySQL materialize the derived table, so the `UPDATE` may read its own table.

**SQL Server**:

```sql
EXEC sp_getapplock @Resource = 't', @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 60000;
DECLARE @b int = (SELECT CASE WHEN 10 - count(*) > 0 THEN 10 - count(*) ELSE 0 END FROM dbo.t
                  WHERE dt >= dateadd(second, -60, sysutcdatetime()));
WITH o AS (SELECT TOP (@b) * FROM dbo.t WHERE dt < dateadd(second, -60, sysutcdatetime()) ORDER BY newid())
UPDATE o SET dt = sysutcdatetime(), ops = 'update', n = n + 1;
EXEC sp_releaseapplock @Resource = 't', @LockOwner = 'Session';
```

**Oracle**, one PL/SQL block:

```sql
DECLARE b NUMBER; h VARCHAR2(128); r NUMBER;
BEGIN
  DBMS_LOCK.ALLOCATE_UNIQUE('t', h); r := DBMS_LOCK.REQUEST(h, DBMS_LOCK.X_MODE, 60, TRUE);
  SELECT GREATEST(0, 10 - COUNT(*)) INTO b FROM app.t WHERE dt >= LOCALTIMESTAMP - INTERVAL '60' SECOND;
  UPDATE app.t SET dt = LOCALTIMESTAMP, ops = 'update', n = n + 1 WHERE pk IN (SELECT pk FROM (
    SELECT pk FROM app.t WHERE dt < LOCALTIMESTAMP - INTERVAL '60' SECOND ORDER BY DBMS_RANDOM.VALUE) WHERE ROWNUM <= b);
  COMMIT;
END;
```

## PostgreSQL CDC setup through the connection

The slot and publication a Postgres CDC test needs can be made from Databricks too, each guarded so a rerun does nothing:

- **Replication slot**, through `query`. A `SELECT` that calls the function works with the allowlist `dbtable,query`, and needs a login with `REPLICATION`:

  ```sql
  SELECT CASE WHEN EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'test_slot') THEN 'exists'
              ELSE (pg_create_logical_replication_slot('test_slot', 'pgoutput')).slot_name::text END AS s
  ```

  The unguarded call fails the second time: the slot already exists. Drop the slot after the test (`pg_drop_replication_slot`): a slot keeps WAL on the source until it is dropped.
- **Publication**, through `sessionInitStatement`. The login must own the table (`ERROR: must be owner of table ...`) and have `CREATE` on the database:

  ```sql
  DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'test_pub') THEN
    CREATE PUBLICATION test_pub FOR TABLE public.t; END IF; END $$;
  ```

## The allowlist doesn't stop writes; the login's grants do

`dbtable,query` refuses `sessionInitStatement`, but a `query` can still change data. Spark wraps it as `SELECT * FROM (<query>) SPARK_GEN_SUBQ_0`, so a query that closes the parenthesis can carry a second statement: `SELECT 1 AS x) q; UPDATE t SET n = n + 1 WHERE pk = 1; SELECT * FROM (SELECT 1 AS x`.

| Engine | The `UPDATE` in a `query` (allowlist `dbtable,query`) |
|---|---|
| PostgreSQL | Ran once, then Spark failed: `Multiple ResultSets were returned by the query` |
| SQL Server | Ran twice (Spark's schema query and the read), and Spark succeeded |
| MySQL | Refused (syntax error), unless the URL has `allowMultiQueries=true`: then it ran twice |
| Oracle | Refused: `ORA-03405: End of query reached; no additional text should follow.` |

PostgreSQL also runs a function's side effects inside a `SELECT` (the slot above). A connection meant for reads and appends needs a login with only those grants.

## Tested

2026-10-31, Databricks serverless (environment 4), PostgreSQL 16, MySQL 8.4, SQL Server 2022 and Oracle Database 23 Free in containers on one VM, all four at once. Drivers: PostgreSQL 42.7.4, MySQL Connector/J 9.1.0, mssql-jdbc 12.8.1.jre11, ojdbc11 23.5.0.24.07. Tables of 100 rows, all outside the window; B = 10; a 2-second wait inside each run (between reading the budget and the update) to make the overlap certain.

| Run | Rows in the window | Updates run |
|---|---|---|
| Capped, 3 actions in a row, all engines | 10 | 10 |
| Fixed `UPDATE ... LIMIT 10`, 3 actions (PostgreSQL) | 30 | 30 |
| Capped, a failing read retried 4 times (PostgreSQL) | 10 | 10 |
| Capped, 8 overlapping tasks, no lock | 52 to 80 | 80 |
| Capped, 8 overlapping tasks, lock, all engines | 10 | 10 |
| Capped, 64 tasks (8 at a time) on all four engines at once, lock | 10 (19 to 28 in total over the 2 minutes the queued runs took) | 20 to 30 |
| Delete + insert, then update, 3 actions then 8 tasks, lock (PostgreSQL) | 10 (5 updated, 5 replaced) | 5 |
