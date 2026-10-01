/* =====================================================================
   04 - BULK REGISTRATION  (run on the SOURCE SQL Server database)
   Reads every CDC capture instance and GENERATES the INSERT statements
   for etl.cdc_control. Copy the generated_sql column output and run it
   in the Fabric SQL database. Also flags tables that will cause trouble.
   Requires SQL Server 2017+ (STRING_AGG).
   Author: Prathamesh Sable
   ===================================================================== */
SET NOCOUNT ON;

DECLARE @source_system  varchar(50) = 'ERP_PROD';
DECLARE @target_schema  sysname     = 'silver';
DECLARE @default_scd    tinyint     = 1;
DECLARE @large_rows     bigint      = 50000000;   -- tables above this go to load_group 'large'

;WITH ci AS (   -- newest capture instance per source table
    SELECT ct.*,
           ROW_NUMBER() OVER (PARTITION BY ct.source_object_id ORDER BY ct.create_date DESC) AS rn
    FROM   cdc.change_tables ct
),
k AS (          -- key columns of the index CDC uses for net changes
    SELECT ic.object_id,
           STRING_AGG(CAST(ic.column_name AS nvarchar(max)), ',') WITHIN GROUP (ORDER BY ic.index_ordinal) AS key_columns
    FROM   cdc.index_columns ic
    GROUP BY ic.object_id
),
cap AS (        -- captured vs total column counts
    SELECT cc.object_id, COUNT(*) AS captured_cols
    FROM   cdc.captured_columns cc
    GROUP BY cc.object_id
),
src AS (
    SELECT c.object_id, COUNT(*) AS table_cols
    FROM   sys.columns c
    WHERE  c.is_computed = 0
    GROUP BY c.object_id
),
sz AS (
    SELECT ps.object_id, SUM(ps.row_count) AS row_count
    FROM   sys.dm_db_partition_stats ps
    WHERE  ps.index_id IN (0,1)
    GROUP BY ps.object_id
)
SELECT
    OBJECT_SCHEMA_NAME(ci.source_object_id)            AS source_schema,
    OBJECT_NAME(ci.source_object_id)                   AS source_table,
    ci.capture_instance,
    k.key_columns,
    sz.row_count,
    ci.supports_net_changes,
    ci.role_name                                       AS gating_role,
    cap.captured_cols, src.table_cols,
    -- problems to fix BEFORE registering
    CONCAT_WS('; ',
        CASE WHEN ci.supports_net_changes = 0 THEN 'NO NET CHANGES (needs PK/unique index, re-enable CDC)' END,
        CASE WHEN k.key_columns IS NULL       THEN 'NO KEY COLUMNS' END,
        CASE WHEN cap.captured_cols < src.table_cols THEN 'PARTIAL COLUMN CAPTURE' END,
        CASE WHEN EXISTS (SELECT 1 FROM sys.columns c JOIN sys.types t ON t.user_type_id = c.user_type_id
                          WHERE c.object_id = ci.source_object_id
                            AND t.name IN ('sql_variant','xml','geography','geometry','hierarchyid'))
             THEN 'UNSUPPORTED TYPES (cast in query)' END,
        CASE WHEN EXISTS (SELECT 1 FROM sys.columns c WHERE c.object_id = ci.source_object_id
                            AND c.name LIKE '%[ ,;{}()=]%')
             THEN 'COLUMN NAMES INVALID FOR DELTA' END
    )                                                  AS issues,
    -- ready-to-run INSERT for the Fabric SQL control DB
    CONCAT(
      'INSERT etl.cdc_control (source_system, source_schema, source_table, capture_instance, key_columns, target_table, scd_type, load_group, is_active) VALUES (',
      '''', @source_system, ''', ',
      '''', OBJECT_SCHEMA_NAME(ci.source_object_id), ''', ',
      '''', OBJECT_NAME(ci.source_object_id), ''', ',
      '''', ci.capture_instance, ''', ',
      '''', k.key_columns, ''', ',
      '''', @target_schema, '.', LOWER(OBJECT_SCHEMA_NAME(ci.source_object_id) + '_' + OBJECT_NAME(ci.source_object_id)), ''', ',
      @default_scd, ', ',
      '''', CASE WHEN sz.row_count >= @large_rows THEN 'large' ELSE 'default' END, ''', ',
      '0);'                                            -- inactive: switch on in waves
    )                                                  AS generated_sql
FROM   ci
LEFT JOIN k   ON k.object_id   = ci.object_id
LEFT JOIN cap ON cap.object_id = ci.object_id
LEFT JOIN src ON src.object_id = ci.source_object_id
LEFT JOIN sz  ON sz.object_id  = ci.source_object_id
WHERE  ci.rn = 1
ORDER BY sz.row_count DESC;

/* Gating roles in use: the Fabric gateway user must be a member of EACH */
SELECT DISTINCT role_name FROM cdc.change_tables WHERE role_name IS NOT NULL;
