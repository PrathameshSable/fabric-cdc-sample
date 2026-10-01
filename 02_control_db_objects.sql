/* =====================================================================
   02 - CONTROL DB: metadata, watermark and run log
   Target: SQL database in Fabric (or Azure SQL DB).

   Why not Fabric Warehouse for this?  Warehouse detects write conflicts
   at TABLE level, so parallel ForEach iterations all updating
   etl.cdc_control will fail each other. SQL database uses row-level
   locking, which is what a watermark table needs.
   Author: Prathamesh Sable
   ===================================================================== */

IF SCHEMA_ID('etl') IS NULL EXEC('CREATE SCHEMA etl');
GO

/* ---------------------------------------------------------------------
   Control table: one row per source table
   --------------------------------------------------------------------- */
CREATE TABLE etl.cdc_control
(
    control_id        int IDENTITY(1,1) NOT NULL CONSTRAINT PK_cdc_control PRIMARY KEY,
    source_system     varchar(50)    NOT NULL,          -- logical name, e.g. 'ERP_PROD'
    source_schema     sysname        NOT NULL,
    source_table      sysname        NOT NULL,
    capture_instance  sysname        NOT NULL,          -- e.g. dbo_Customer_v1
    key_columns       nvarchar(500)  NOT NULL,          -- comma-separated PK columns
    target_table      nvarchar(256)  NOT NULL,          -- lakehouse table, e.g. silver.customer
    scd_type          tinyint        NOT NULL CONSTRAINT DF_cdc_control_scd DEFAULT (1)
                                     CONSTRAINT CK_cdc_control_scd CHECK (scd_type IN (1,2)),
    load_group        varchar(50)    NOT NULL CONSTRAINT DF_cdc_control_grp DEFAULT ('default'),
    last_lsn_hex      varchar(22)    NULL,              -- NULL => next run does a FULL load
    last_load_action  varchar(20)    NULL,
    last_success_utc  datetime2(3)   NULL,
    is_active         bit            NOT NULL CONSTRAINT DF_cdc_control_active DEFAULT (1),
    CONSTRAINT UQ_cdc_control_src UNIQUE (source_system, source_schema, source_table)
);
GO

/* ---------------------------------------------------------------------
   Run log: one row per table per run
   --------------------------------------------------------------------- */
CREATE TABLE etl.cdc_run_log
(
    log_id            bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_cdc_run_log PRIMARY KEY,
    control_id        int            NOT NULL,
    pipeline_run_id   varchar(64)    NOT NULL,
    load_action       varchar(20)    NOT NULL,          -- FULL | INCREMENTAL | NOCHANGE | FAILED
    from_lsn_hex      varchar(22)    NULL,
    to_lsn_hex        varchar(22)    NULL,
    rows_copied       bigint         NULL,
    apply_result      nvarchar(4000) NULL,              -- notebook exit JSON
    status            varchar(20)    NOT NULL,          -- Succeeded | Failed
    error_message     nvarchar(4000) NULL,
    logged_utc        datetime2(3)   NOT NULL CONSTRAINT DF_cdc_run_log_ts DEFAULT (SYSUTCDATETIME())
);
GO
CREATE INDEX IX_cdc_run_log_ctrl ON etl.cdc_run_log (control_id, logged_utc DESC);
GO

/* ---------------------------------------------------------------------
   Success: advance watermark + log, in one transaction.
   The watermark only ever moves forward (guards against a stale
   re-run overwriting a newer LSN).
   --------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE etl.usp_cdc_complete
    @control_id      int,
    @pipeline_run_id varchar(64),
    @load_action     varchar(20),
    @from_lsn_hex    varchar(22) = NULL,
    @to_lsn_hex      varchar(22),
    @rows_copied     bigint      = NULL,
    @apply_result    nvarchar(4000) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    SET @from_lsn_hex = NULLIF(@from_lsn_hex, '');

    IF @to_lsn_hex IS NULL OR LEN(@to_lsn_hex) <> 22
        THROW 50001, 'usp_cdc_complete: @to_lsn_hex must be a 22-char hex LSN (0x + 20).', 1;

    BEGIN TRAN;

        UPDATE etl.cdc_control
           SET last_lsn_hex     = @to_lsn_hex,
               last_load_action = @load_action,
               last_success_utc = SYSUTCDATETIME()
         WHERE control_id = @control_id
           AND (   last_lsn_hex IS NULL
                OR CONVERT(binary(10), @to_lsn_hex, 1) >= CONVERT(binary(10), last_lsn_hex, 1));

        INSERT etl.cdc_run_log (control_id, pipeline_run_id, load_action, from_lsn_hex, to_lsn_hex,
                                rows_copied, apply_result, status)
        VALUES (@control_id, @pipeline_run_id, @load_action, @from_lsn_hex, @to_lsn_hex,
                @rows_copied, @apply_result, 'Succeeded');

    COMMIT;
END;
GO

/* ---------------------------------------------------------------------
   Failure: log only. Watermark untouched => next run retries same window.
   --------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE etl.usp_cdc_log_failure
    @control_id      int,
    @pipeline_run_id varchar(64),
    @load_action     varchar(20) = NULL,
    @from_lsn_hex    varchar(22) = NULL,
    @to_lsn_hex      varchar(22) = NULL,
    @error_message   nvarchar(4000)
AS
BEGIN
    SET NOCOUNT ON;
    INSERT etl.cdc_run_log (control_id, pipeline_run_id, load_action, from_lsn_hex, to_lsn_hex,
                            status, error_message)
    VALUES (@control_id, @pipeline_run_id, ISNULL(@load_action, 'UNKNOWN'), @from_lsn_hex, @to_lsn_hex,
            'Failed', LEFT(@error_message, 4000));
END;
GO

/* ---------------------------------------------------------------------
   Force a full reload of one table on the next run
   --------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE etl.usp_cdc_reset
    @control_id int
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE etl.cdc_control SET last_lsn_hex = NULL WHERE control_id = @control_id;
END;
GO

/* ---------------------------------------------------------------------
   Sample registration
   --------------------------------------------------------------------- */
INSERT etl.cdc_control (source_system, source_schema, source_table, capture_instance,
                        key_columns, target_table, scd_type, load_group)
VALUES ('ERP_PROD', 'dbo', 'Customer',   'dbo_Customer_v1',   'CustomerID',          'silver.customer',    2, 'default'),
       ('ERP_PROD', 'dbo', 'OrderLine',  'dbo_OrderLine_v1',  'OrderID,LineNumber',  'silver.order_line',  1, 'default');
GO

/* Latest status per table */
CREATE OR ALTER VIEW etl.vw_cdc_status
AS
SELECT c.control_id, c.source_system, c.source_schema, c.source_table, c.target_table,
       c.last_lsn_hex, c.last_load_action, c.last_success_utc,
       l.status AS last_run_status, l.load_action AS last_run_action,
       l.rows_copied, l.error_message, l.logged_utc
FROM   etl.cdc_control c
OUTER APPLY (SELECT TOP (1) * FROM etl.cdc_run_log r
             WHERE r.control_id = c.control_id ORDER BY r.logged_utc DESC) l;
GO
