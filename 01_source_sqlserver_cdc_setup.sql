/* =====================================================================
   01 - SOURCE: SQL Server CDC setup for Fabric ingestion
   Run on the on-prem SQL Server source database.
   Author: Prathamesh Sable
   ===================================================================== */

USE [YourSourceDb];
GO

/* ---------------------------------------------------------------------
   1. Enable CDC at database level (skip if already enabled)
   --------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME() AND is_cdc_enabled = 1)
    EXEC sys.sp_cdc_enable_db;
GO

/* ---------------------------------------------------------------------
   2. Gating role + read user for the Fabric gateway connection
      The Fabric connection user needs:
        - membership of the CDC gating role (to call the cdc functions)
        - SELECT on the source tables (initial / reload snapshot)
        - SELECT on schema cdc (the pipeline probes cdc.<ci>_CT directly)
   --------------------------------------------------------------------- */
IF DATABASE_PRINCIPAL_ID('cdc_reader') IS NULL
    CREATE ROLE cdc_reader;
GO
-- CREATE USER [svc_fabric_ingest] FOR LOGIN [svc_fabric_ingest];
-- ALTER ROLE cdc_reader     ADD MEMBER [svc_fabric_ingest];
-- ALTER ROLE db_datareader  ADD MEMBER [svc_fabric_ingest];
-- GRANT SELECT ON SCHEMA::cdc TO [svc_fabric_ingest];
GO

/* ---------------------------------------------------------------------
   3. Enable CDC per table
      - @supports_net_changes = 1 is REQUIRED (pipeline uses net changes)
        -> table needs a PK or a unique index (@index_name)
      - Leave @captured_column_list NULL so ALL columns are captured.
        The snapshot load does SELECT *, so a partial column list causes
        drift between full and incremental loads.
      - Name the capture instance explicitly: schema_table_v1.
        Versioning makes schema-change switchovers clean (see README).
   --------------------------------------------------------------------- */
EXEC sys.sp_cdc_enable_table
     @source_schema        = N'dbo',
     @source_name          = N'Customer',
     @role_name            = N'cdc_reader',
     @capture_instance     = N'dbo_Customer_v1',
     @supports_net_changes = 1;
GO

/* ---------------------------------------------------------------------
   4. Retention - default cleanup is 4320 min (3 days).
      Set it comfortably above your worst-case outage window, otherwise
      a missed weekend forces full reloads. 7 days shown.
   --------------------------------------------------------------------- */
EXEC sys.sp_cdc_change_job @job_type = N'cleanup', @retention = 10080;
GO

/* =====================================================================
   HEALTH CHECKS (run any time)
   ===================================================================== */

-- Capture instances, net-change support, captured columns
SELECT  ct.capture_instance,
        OBJECT_SCHEMA_NAME(ct.source_object_id) AS source_schema,
        OBJECT_NAME(ct.source_object_id)        AS source_table,
        ct.supports_net_changes,
        ct.start_lsn,
        ct.create_date
FROM    cdc.change_tables ct
ORDER BY source_schema, source_table, ct.create_date;

-- Capture / cleanup jobs (SQL Agent MUST be running for capture)
EXEC sys.sp_cdc_help_jobs;

-- Capture latency: how far behind the log reader is
SELECT TOP (5) session_id, start_time, end_time, duration, latency, tran_count, command_count
FROM   sys.dm_cdc_log_scan_sessions
ORDER BY session_id DESC;

-- Any capture errors
SELECT TOP (20) * FROM sys.dm_cdc_errors ORDER BY entry_time DESC;

-- Current LSN window for one capture instance
SELECT  CONVERT(varchar(22), sys.fn_cdc_get_min_lsn(N'dbo_Customer_v1'), 1) AS min_lsn_hex,
        CONVERT(varchar(22), sys.fn_cdc_get_max_lsn(), 1)                   AS db_max_lsn_hex;
