-- sqlserver-log-state.sql: one batch, run in the source database, returns one JSON document with where SQL Server's
-- change tracking and CDC stand right now, so a reader can compare it with where a capture tool says it is.
--   database:       name, server time (UTC), recovery model, log reuse wait, log space used %, CT and CDC on
--   change_tracking: current version; retention; per tracked table: min valid version, cleanup version, last change
--                   version and its row count (CHANGETABLE since the min valid version)
--   cdc:            max LSN and its time; capture and cleanup job settings; the last log scan and the last one that
--                   captured transactions (latency in s); per capture
--                   instance: min LSN, start LSN, last LSN in its change table (and their times), rows
-- LSNs are hex (0x...). A part the login can't read is null with an "error" next to it; the rest still runs.
-- Rights for every part: VIEW DATABASE STATE, VIEW CHANGE TRACKING and SELECT on the tracked tables, SELECT on the
-- cdc schema; the job settings need msdb (sysadmin, or db_owner of the database for sys.sp_cdc_help_jobs).
-- Set @per_table = 0 to skip the per-table reads (CHANGETABLE and MAX over each change table) on a large source.
SET NOCOUNT ON;
DECLARE @per_table bit = COALESCE(TRY_CONVERT(bit, '$(PER_TABLE)'), 1);

DECLARE @db nvarchar(max), @ct nvarchar(max), @cdc nvarchar(max), @err nvarchar(4000);

-- database
SELECT @db = (
  SELECT d.name, SYSUTCDATETIME() AS server_time_utc, d.recovery_model_desc AS recovery_model,
         d.log_reuse_wait_desc AS log_reuse_wait, CAST(l.used_log_space_in_percent AS decimal(5, 1)) AS log_used_pct,
         CAST(CASE WHEN ctd.database_id IS NULL THEN 0 ELSE 1 END AS bit) AS change_tracking, d.is_cdc_enabled AS cdc
  FROM sys.databases d
  CROSS JOIN sys.dm_db_log_space_usage l
  LEFT JOIN sys.change_tracking_databases ctd ON ctd.database_id = d.database_id
  WHERE d.database_id = DB_ID()
  FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES);

-- change tracking
IF EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID())
BEGIN
  CREATE TABLE #ct (schema_name sysname, table_name sysname, object_id int, min_valid_version bigint,
                    cleanup_version bigint, last_change_version bigint, changed_rows bigint, error nvarchar(4000));
  INSERT #ct (schema_name, table_name, object_id, min_valid_version, cleanup_version)
  SELECT s.name, t.name, t.object_id, CHANGE_TRACKING_MIN_VALID_VERSION(t.object_id), ctt.cleanup_version
  FROM sys.change_tracking_tables ctt JOIN sys.tables t ON t.object_id = ctt.object_id
  JOIN sys.schemas s ON s.schema_id = t.schema_id;
  IF @per_table = 1
  BEGIN
    DECLARE @s sysname, @t sysname, @oid int, @mv bigint, @q nvarchar(max), @v bigint, @n bigint;
    DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT schema_name, table_name, object_id, min_valid_version FROM #ct;
    OPEN c; FETCH c INTO @s, @t, @oid, @mv;
    WHILE @@FETCH_STATUS = 0
    BEGIN
      BEGIN TRY
        SET @q = N'SELECT @v = MAX(SYS_CHANGE_VERSION), @n = COUNT_BIG(*) FROM CHANGETABLE(CHANGES '
                 + QUOTENAME(@s) + N'.' + QUOTENAME(@t) + N', @mv) AS ct';
        EXEC sp_executesql @q, N'@mv bigint, @v bigint OUTPUT, @n bigint OUTPUT', @mv, @v OUTPUT, @n OUTPUT;
        UPDATE #ct SET last_change_version = @v, changed_rows = @n WHERE object_id = @oid;
      END TRY
      BEGIN CATCH
        UPDATE #ct SET error = ERROR_MESSAGE() WHERE object_id = @oid;
      END CATCH;
      FETCH c INTO @s, @t, @oid, @mv;
    END;
    CLOSE c; DEALLOCATE c;
  END;
  SELECT @ct = (
    SELECT CHANGE_TRACKING_CURRENT_VERSION() AS current_version,
           ctd.retention_period, ctd.retention_period_units_desc AS retention_units, ctd.is_auto_cleanup_on AS auto_cleanup,
           (SELECT schema_name AS [schema], table_name AS [table], min_valid_version, cleanup_version,
                   last_change_version, changed_rows, error
            FROM #ct ORDER BY schema_name, table_name FOR JSON PATH, INCLUDE_NULL_VALUES) AS tables
    FROM sys.change_tracking_databases ctd WHERE ctd.database_id = DB_ID()
    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES);
END;

-- CDC
IF (SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()) = 1
BEGIN
  CREATE TABLE #ci (capture_instance sysname, source_schema sysname, source_table sysname, start_lsn binary(10),
                    min_lsn binary(10), last_lsn binary(10), change_rows bigint, error nvarchar(4000));
  INSERT #ci (capture_instance, source_schema, source_table, start_lsn, min_lsn)
  SELECT ct.capture_instance, s.name, t.name, ct.start_lsn, sys.fn_cdc_get_min_lsn(ct.capture_instance)
  FROM cdc.change_tables ct JOIN sys.tables t ON t.object_id = ct.source_object_id
  JOIN sys.schemas s ON s.schema_id = t.schema_id;
  IF @per_table = 1
  BEGIN
    DECLARE @ci sysname, @l binary(10), @r bigint, @q2 nvarchar(max);
    DECLARE c2 CURSOR LOCAL FAST_FORWARD FOR SELECT capture_instance FROM #ci;
    OPEN c2; FETCH c2 INTO @ci;
    WHILE @@FETCH_STATUS = 0
    BEGIN
      BEGIN TRY
        SET @q2 = N'SELECT @l = MAX(__$start_lsn), @r = COUNT_BIG(*) FROM cdc.' + QUOTENAME(@ci + N'_CT');
        EXEC sp_executesql @q2, N'@l binary(10) OUTPUT, @r bigint OUTPUT', @l OUTPUT, @r OUTPUT;
        UPDATE #ci SET last_lsn = @l, change_rows = @r WHERE capture_instance = @ci;
      END TRY
      BEGIN CATCH
        UPDATE #ci SET error = ERROR_MESSAGE() WHERE capture_instance = @ci;
      END CATCH;
      FETCH c2 INTO @ci;
    END;
    CLOSE c2; DEALLOCATE c2;
  END;
  -- job settings: sys.sp_cdc_help_jobs (needs db_owner); null with its error otherwise
  CREATE TABLE #jobs (job_id uniqueidentifier, job_type nvarchar(20), job_name sysname, maxtrans int, maxscans int,
                      continuous bit, pollinginterval bigint, retention bigint, threshold bigint);
  DECLARE @jobs_error nvarchar(4000);
  BEGIN TRY
    INSERT #jobs EXEC sys.sp_cdc_help_jobs;
  END TRY
  BEGIN CATCH
    SET @jobs_error = ERROR_MESSAGE();
  END CATCH;
  DECLARE @max binary(10) = sys.fn_cdc_get_max_lsn();
  SELECT @cdc = (
    SELECT CONVERT(varchar(22), @max, 1) AS max_lsn, sys.fn_cdc_map_lsn_to_time(@max) AS max_lsn_time,
           (SELECT job_type, pollinginterval AS polling_interval_s, maxtrans, maxscans, continuous,
                   retention AS retention_min, threshold
            FROM #jobs FOR JSON PATH, INCLUDE_NULL_VALUES) AS jobs,
           @jobs_error AS jobs_error,
           JSON_QUERY((SELECT TOP 1 start_time, end_time, duration, scan_phase, error_count, latency AS latency_s,
                   '0x' + LEFT(REPLACE(end_lsn, ':', ''), 20) AS end_lsn, tran_count
            FROM sys.dm_cdc_log_scan_sessions WHERE session_id > 0 ORDER BY start_time DESC
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES)) AS last_scan,
           -- the latest scan that captured transactions (the last one is often an idle poll)
           JSON_QUERY((SELECT TOP 1 start_time, end_time, duration, latency AS latency_s,
                   '0x' + LEFT(REPLACE(end_lsn, ':', ''), 20) AS end_lsn, tran_count, command_count
            FROM sys.dm_cdc_log_scan_sessions WHERE session_id > 0 AND tran_count > 0 ORDER BY start_time DESC
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES)) AS last_capture,
           (SELECT capture_instance, source_schema AS [schema], source_table AS [table],
                   CONVERT(varchar(22), min_lsn, 1) AS min_lsn, sys.fn_cdc_map_lsn_to_time(min_lsn) AS min_lsn_time,
                   CONVERT(varchar(22), start_lsn, 1) AS start_lsn,
                   CONVERT(varchar(22), last_lsn, 1) AS last_lsn, sys.fn_cdc_map_lsn_to_time(last_lsn) AS last_lsn_time,
                   change_rows, error
            FROM #ci ORDER BY capture_instance FOR JSON PATH, INCLUDE_NULL_VALUES) AS capture_instances
    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES);
END;

SELECT CAST(CONCAT(N'{"database":', COALESCE(@db, N'null'), N',"change_tracking":', COALESCE(@ct, N'null'),
                   N',"cdc":', COALESCE(@cdc, N'null'), N'}') AS nvarchar(max)) AS log_state;
